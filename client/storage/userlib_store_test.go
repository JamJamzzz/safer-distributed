package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"
)

// These tests cover the storage abstraction boundary itself: that the
// userlib-backed adapter still behaves the way SAFER's business logic has
// always assumed, and that its latches still make raw concurrent access
// safe. They deliberately do not test SAFER semantics; those are covered
// by the inherited suites in client and client_test.

func TestUserlibObjectStoreRoundTrip(t *testing.T) {
	userlib.DatastoreClear()
	defer userlib.DatastoreClear()

	ctx := context.Background()
	store := &UserlibObjectStore{}
	id := uuid.New()
	value := []byte("encrypted-and-authenticated-blob")

	if _, found, err := store.Get(ctx, id); err != nil || found {
		t.Fatalf("absent id: got found=%v err=%v, want found=false err=nil", found, err)
	}

	if err := store.Put(ctx, id, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := store.Get(ctx, id)
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("after Put: got %q found=%v err=%v, want %q found=true err=nil", got, found, err, value)
	}

	// Put overwrites; SAFER relies on in-place object updates.
	updated := []byte("second-version")
	if err := store.Put(ctx, id, updated); err != nil {
		t.Fatalf("overwriting Put: %v", err)
	}
	if got, _, _ := store.Get(ctx, id); !bytes.Equal(got, updated) {
		t.Fatalf("after overwrite: got %q, want %q", got, updated)
	}

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := store.Get(ctx, id); found {
		t.Fatal("id still present after Delete")
	}
	// Deleting an absent id is not an error.
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete of absent id: %v", err)
	}
}

func TestUserlibKeyStoreWriteOnce(t *testing.T) {
	userlib.KeystoreClear()
	defer userlib.KeystoreClear()

	ctx := context.Background()
	store := &UserlibKeyStore{}
	name := "TestUserlibKeyStoreWriteOnce/pke"

	public, _, err := userlib.PKEKeyGen()
	if err != nil {
		t.Fatalf("PKEKeyGen: %v", err)
	}

	if _, found, err := store.Get(ctx, name); err != nil || found {
		t.Fatalf("absent name: got found=%v err=%v, want found=false err=nil", found, err)
	}
	if err := store.Put(ctx, name, public); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, found, err := store.Get(ctx, name); err != nil || !found {
		t.Fatalf("after Put: got found=%v err=%v, want found=true err=nil", found, err)
	}

	// Write-once: SAFER's identity semantics depend on a taken name
	// staying taken.
	other, _, err := userlib.PKEKeyGen()
	if err != nil {
		t.Fatalf("PKEKeyGen: %v", err)
	}
	if err := store.Put(ctx, name, other); err == nil {
		t.Fatal("re-registering an existing key name succeeded, want error")
	}
}

// TestUserlibStoreConcurrentAccess exercises the latches that are the
// entire reason the userlib backend needs a guard: without them, Go's
// runtime kills the process on concurrent map access, and userlib's shared
// bandwidth counter is corrupted even by reads of distinct keys.
//
// The bandwidth assertion is an exact semantic oracle, not a timing or
// throughput assertion.
func TestUserlibStoreConcurrentAccess(t *testing.T) {
	userlib.DatastoreClear()
	defer userlib.DatastoreClear()

	ctx := context.Background()
	store := &UserlibObjectStore{}
	const workers, opsPerWorker = 16, 64
	value := []byte("payload")

	ids := make([]uuid.UUID, workers)
	for i := range ids {
		ids[i] = uuid.New()
		if err := store.Put(ctx, ids[i], value); err != nil {
			t.Fatalf("setup Put: %v", err)
		}
	}
	userlib.DatastoreResetBandwidth()

	start := make(chan struct{})
	valid := make([]bool, workers)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			valid[i] = true
			for n := 0; n < opsPerWorker; n++ {
				got, found, err := store.Get(ctx, ids[i])
				if err != nil || !found || !bytes.Equal(got, value) {
					valid[i] = false
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, ok := range valid {
		if !ok {
			t.Fatalf("worker %d observed a bad read", i)
		}
	}
	if got, want := userlib.DatastoreGetBandwidth(), workers*opsPerWorker*len(value); got != want {
		t.Fatalf("bandwidth accounting: got %d, want %d", got, want)
	}
}

// TestNewUserlibStorageWiring pins the default bundle to the legacy
// backend: SAFER Distributed must keep V1 behavior until a backend is
// explicitly substituted.
func TestNewUserlibStorageWiring(t *testing.T) {
	s := NewUserlibStorage()
	if _, ok := s.Objects.(*UserlibObjectStore); !ok {
		t.Fatalf("Objects: got %T, want *UserlibObjectStore", s.Objects)
	}
	if _, ok := s.Keys.(*UserlibKeyStore); !ok {
		t.Fatalf("Keys: got %T, want *UserlibKeyStore", s.Keys)
	}
}
