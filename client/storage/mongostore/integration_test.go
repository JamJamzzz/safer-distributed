package mongostore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Integration tests for the MongoDB backend.
//
// Every test here calls requireStore, which SKIPS when SAFER_MONGO_URI is
// unset or MongoDB is unreachable. Nothing in this file is required for
// the rest of the suite to run: a machine with no MongoDB still runs every
// unit test in the repository.
//
// To run them:
//
//	docker run -d -p 27017:27017 --name safer-mongo mongo:7
//	SAFER_MONGO_URI=mongodb://localhost:27017 go test ./client/storage/mongostore/
//
// Each test uses its own database name and drops it afterwards, so runs do
// not interfere with each other or with real data.

// testDatabaseName builds a unique, legal database name for one test.
// MongoDB caps database names at 63 characters and forbids several
// punctuation characters, so the test name is sanitized and truncated
// rather than used raw.
func testDatabaseName(testName string) string {
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, testName)
	const maxNameChars = 24
	if len(sanitized) > maxNameChars {
		sanitized = sanitized[:maxNameChars]
	}
	return fmt.Sprintf("safer_it_%s_%d", sanitized, time.Now().UnixNano())
}

func requireStore(t *testing.T) *Store {
	t.Helper()

	cfg, configured, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", EnvURI)
	}
	// Isolate this test's data.
	cfg.Database = testDatabaseName(t.Name())

	ctx := context.Background()
	store, err := Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB at %s is unreachable: %v", cfg.URI, err)
	}
	t.Cleanup(func() {
		if err := store.DropDatabase(context.Background()); err != nil {
			t.Errorf("dropping test database: %v", err)
		}
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	return store
}

func TestMongoObjectRoundTrip(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()
	objects := store.Objects()

	id := uuid.New()
	if _, found, err := objects.Get(ctx, id); err != nil || found {
		t.Fatalf("absent id: got found=%v err=%v, want false/nil", found, err)
	}

	value := []byte{0x00, 0xff, 0x10, 'c', 'i', 'p', 'h', 'e', 'r', 0x00}
	if err := objects.Put(ctx, id, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := objects.Get(ctx, id)
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("Get: got %v found=%v err=%v, want %v/true/nil", got, found, err, value)
	}

	updated := []byte("second-version")
	if err := objects.Put(ctx, id, updated); err != nil {
		t.Fatalf("overwriting Put: %v", err)
	}
	if got, _, _ := objects.Get(ctx, id); !bytes.Equal(got, updated) {
		t.Fatalf("after overwrite: got %q, want %q", got, updated)
	}

	if err := objects.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := objects.Get(ctx, id); found {
		t.Fatal("id still present after Delete")
	}
	if err := objects.Delete(ctx, id); err != nil {
		t.Fatalf("Delete of absent id: got %v, want nil", err)
	}
}

// TestMongoObjectEmptyPayload pins the boundary between "stored, empty"
// and "absent", which SAFER distinguishes.
func TestMongoObjectEmptyPayload(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()
	objects := store.Objects()

	for name, value := range map[string][]byte{"nil": nil, "empty": {}} {
		id := uuid.New()
		if err := objects.Put(ctx, id, value); err != nil {
			t.Fatalf("%s: Put: %v", name, err)
		}
		got, found, err := objects.Get(ctx, id)
		if err != nil || !found {
			t.Fatalf("%s: got found=%v err=%v, want true/nil", name, found, err)
		}
		if len(got) != 0 {
			t.Fatalf("%s: got %v, want an empty payload", name, got)
		}
	}
}

// TestMongoStoresOpaqueBlobsAtSaferUUIDs checks the schema contract: the
// document is keyed by SAFER's own logical UUID and holds SAFER's bytes
// verbatim. MongoDB is persistence, not a re-modeling of SAFER's objects.
func TestMongoStoresOpaqueBlobsAtSaferUUIDs(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()

	id := uuid.New()
	value := []byte{0xde, 0xad, 0xbe, 0xef}
	if err := store.Objects().Put(ctx, id, value); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var raw bson.M
	err := store.objects.collection.FindOne(ctx, bson.M{"_id": id.String()}).Decode(&raw)
	if err != nil {
		t.Fatalf("raw FindOne by SAFER UUID: %v", err)
	}
	binary, ok := raw["value"].(primitive.Binary)
	if !ok {
		t.Fatalf("value field: got %T, want primitive.Binary", raw["value"])
	}
	if !bytes.Equal(binary.Data, value) {
		t.Fatalf("stored bytes: got %v, want %v (verbatim)", binary.Data, value)
	}
}

func TestMongoKeyStoreRoundTripAndWriteOnce(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()
	keys := store.Keys()

	public, private, err := userlib.PKEKeyGen()
	if err != nil {
		t.Fatalf("PKEKeyGen: %v", err)
	}
	name := "alice/pke"

	if _, found, err := keys.Get(ctx, name); err != nil || found {
		t.Fatalf("absent name: got found=%v err=%v, want false/nil", found, err)
	}
	if err := keys.Put(ctx, name, public); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, found, err := keys.Get(ctx, name)
	if err != nil || !found {
		t.Fatalf("Get: got found=%v err=%v, want true/nil", found, err)
	}
	if got.KeyType != public.KeyType {
		t.Fatalf("KeyType: got %q, want %q", got.KeyType, public.KeyType)
	}

	// The real oracle is cryptographic, not structural: a key that
	// survived the round trip must still work.
	message := []byte("round-tripped key must still encrypt")
	ciphertext, err := userlib.PKEEnc(got, message)
	if err != nil {
		t.Fatalf("PKEEnc with round-tripped key: %v", err)
	}
	plaintext, err := userlib.PKEDec(private, ciphertext)
	if err != nil {
		t.Fatalf("PKEDec: %v", err)
	}
	if !bytes.Equal(plaintext, message) {
		t.Fatalf("decrypted %q, want %q", plaintext, message)
	}

	// Write-once, enforced by MongoDB's unique _id index.
	other, _, err := userlib.PKEKeyGen()
	if err != nil {
		t.Fatalf("PKEKeyGen: %v", err)
	}
	if err := keys.Put(ctx, name, other); err == nil {
		t.Fatal("re-registering an existing key name succeeded, want an error")
	}
	// The original registration must be intact.
	after, _, err := keys.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get after rejected overwrite: %v", err)
	}
	if after.PubKey.N.Cmp(public.PubKey.N) != 0 {
		t.Fatal("rejected overwrite still replaced the stored key")
	}
}

// TestMongoVerifyKeyRoundTrip covers the signing half of the keystore.
func TestMongoVerifyKeyRoundTrip(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()

	signKey, verifyKey, err := userlib.DSKeyGen()
	if err != nil {
		t.Fatalf("DSKeyGen: %v", err)
	}
	if err := store.Keys().Put(ctx, "alice/ds", verifyKey); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := store.Keys().Get(ctx, "alice/ds")
	if err != nil || !found {
		t.Fatalf("Get: got found=%v err=%v, want true/nil", found, err)
	}

	message := []byte("authenticated envelope")
	signature, err := userlib.DSSign(signKey, message)
	if err != nil {
		t.Fatalf("DSSign: %v", err)
	}
	if err := userlib.DSVerify(got, message, signature); err != nil {
		t.Fatalf("DSVerify with round-tripped key: %v", err)
	}
	if err := userlib.DSVerify(got, []byte("tampered"), signature); err == nil {
		t.Fatal("round-tripped key verified a tampered message")
	}
}

// TestMongoDurabilityAcrossConnections is the actual durability evidence:
// data written by one client is read back by a completely separate client,
// through a new connection pool, after the first is closed. That is what
// the userlib backend cannot do.
func TestMongoDurabilityAcrossConnections(t *testing.T) {
	cfg, configured, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", EnvURI)
	}
	cfg.Database = testDatabaseName("durability")
	ctx := context.Background()

	writer, err := Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	id := uuid.New()
	value := []byte("survives-a-worker-restart")
	if err := writer.Objects().Put(ctx, id, value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	public, _, err := userlib.PKEKeyGen()
	if err != nil {
		t.Fatalf("PKEKeyGen: %v", err)
	}
	if err := writer.Keys().Put(ctx, "restart/pke", public); err != nil {
		t.Fatalf("key Put: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A second process would look exactly like this.
	reader, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	t.Cleanup(func() {
		_ = reader.DropDatabase(context.Background())
		_ = reader.Close(context.Background())
	})

	got, found, err := reader.Objects().Get(ctx, id)
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("after reconnect: got %q found=%v err=%v, want %q/true/nil", got, found, err, value)
	}
	if _, found, err := reader.Keys().Get(ctx, "restart/pke"); err != nil || !found {
		t.Fatalf("key after reconnect: got found=%v err=%v, want true/nil", found, err)
	}
}

// TestMongoConcurrentAccess checks that the backend tolerates concurrent
// use from many goroutines without a process-wide latch.
//
// This is a storage-layer statement only. It is NOT a claim about SAFER's
// distributed concurrency control: MongoDB does no locking on SAFER's
// behalf, and two workers writing the same object still need the lock
// coordinator (Phase 3) to be ordered correctly.
func TestMongoConcurrentAccess(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()
	objects := store.Objects()

	const workers, opsPerWorker = 8, 10
	ids := make([]uuid.UUID, workers)
	values := make([][]byte, workers)
	for i := range ids {
		ids[i] = uuid.New()
		values[i] = []byte(fmt.Sprintf("worker-%d-payload", i))
	}

	start := make(chan struct{})
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for n := 0; n < opsPerWorker; n++ {
				if err := objects.Put(ctx, ids[i], values[i]); err != nil {
					errs[i] = err
					return
				}
				got, found, err := objects.Get(ctx, ids[i])
				if err != nil {
					errs[i] = err
					return
				}
				if !found || !bytes.Equal(got, values[i]) {
					errs[i] = fmt.Errorf("worker %d read back %q, want %q", i, got, values[i])
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
}

// TestMongoFailureIsReportedAsError pins the most important behavior for
// SAFER's correctness: an unreachable backend must produce an error, never
// a confident "not found". SAFER treats absence as meaningful.
func TestMongoFailureIsReportedAsError(t *testing.T) {
	store := requireStore(t)
	ctx := context.Background()

	id := uuid.New()
	if err := store.Objects().Put(ctx, id, []byte("present")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// An already-cancelled context is a stand-in for a backend that does
	// not answer: the operation cannot complete, so it must report a
	// failure rather than absence.
	dead, cancel := context.WithCancel(ctx)
	cancel()

	value, found, err := store.Objects().Get(dead, id)
	if err == nil {
		t.Fatal("Get on a failed context returned no error")
	}
	if found {
		t.Fatal("Get reported found=true despite failing")
	}
	if value != nil {
		t.Fatalf("Get returned %v alongside an error, want nil", value)
	}

	if err := store.Objects().Put(dead, id, []byte("x")); err == nil {
		t.Fatal("Put on a failed context returned no error")
	}
	if err := store.Objects().Delete(dead, id); err == nil {
		t.Fatal("Delete on a failed context returned no error")
	}
	if _, found, err := store.Keys().Get(dead, "anything"); err == nil || found {
		t.Fatalf("key Get on a failed context: got found=%v err=%v, want false and an error", found, err)
	}
}
