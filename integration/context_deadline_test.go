package integration

// Proves that a caller's context deadline, now that it actually reaches
// SAFER's storage transactions (see client.AppendToFileContext and
// friends, added alongside the context-free legacy API in Phase 4.5),
// aborts a Mongo-backed multi-object mutation cleanly: nothing is left
// half-written, exactly as an injected storage failure already proved in
// rollback_test.go. The mechanism here is a real context.WithTimeout
// against real MongoDB, not a simulated error.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/storage"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// delayObjects wraps a real ObjectStore and sleeps before forwarding the
// Nth Put, so a caller-supplied deadline set shorter than that sleep has
// already passed by the time the real write is attempted. The sleep
// itself deliberately ignores ctx: it exists to guarantee the deadline
// has elapsed before the underlying driver call is made, not to be
// cancellable itself.
type delayObjects struct {
	inner storage.ObjectStore

	mu         sync.Mutex
	delayAfter int // delay before this 1-based Put; 0 disables
	delay      time.Duration
	puts       int
}

func (d *delayObjects) arm(delayAfter int, delay time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.delayAfter = delayAfter
	d.delay = delay
	d.puts = 0
}

func (d *delayObjects) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.puts
}

func (d *delayObjects) shouldDelay() (bool, time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.puts++
	return d.delayAfter > 0 && d.puts == d.delayAfter, d.delay
}

func (d *delayObjects) Get(ctx context.Context, id uuid.UUID) ([]byte, bool, error) {
	return d.inner.Get(ctx, id)
}

func (d *delayObjects) Put(ctx context.Context, id uuid.UUID, value []byte) error {
	if delay, sleep := d.shouldDelay(); delay {
		time.Sleep(sleep)
	}
	return d.inner.Put(ctx, id, value)
}

func (d *delayObjects) Delete(ctx context.Context, id uuid.UUID) error {
	return d.inner.Delete(ctx, id)
}

// deadlineFixture is one test's MongoDB database with delay injection
// installed between SAFER and the real store.
type deadlineFixture struct {
	store   *mongostore.Store
	objects *delayObjects
}

func newDeadlineFixture(t *testing.T) *deadlineFixture {
	t.Helper()

	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_deadline_%d", time.Now().UnixNano())

	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	if err := store.SupportsTransactions(ctx); err != nil {
		_ = store.Close(ctx)
		t.Skipf("skipping: this MongoDB cannot serve transactions (a replica set is required): %v", err)
	}

	base := store.Storage()
	objects := &delayObjects{inner: base.Objects}
	fixture := &deadlineFixture{store: store, objects: objects}

	restore := client.UseStorage(storage.Storage{
		Objects: objects,
		Keys:    base.Keys,
		Atomic:  base.Atomic,
	})
	t.Cleanup(func() {
		restore()
		_ = store.DropDatabase(context.Background())
		_ = store.Close(context.Background())
	})
	return fixture
}

// TestContextDeadline_AbortsAppendCleanly gives AppendToFileContext a
// context whose deadline has already passed by the time the append's
// first storage write is attempted (the delay is longer than the
// deadline), and checks that the whole multi-object mutation aborts:
// no chunk, no metadata update, no version advance, and the file's
// content is exactly what it was before the attempt.
func TestContextDeadline_AbortsAppendCleanly(t *testing.T) {
	fixture := newDeadlineFixture(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	const base = "base-content"
	if err := alice.StoreFile("file", []byte(base)); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	// Unrelated committed state, to prove a failed append does not touch
	// anything outside its own transaction.
	if err := alice.StoreFile("untouched", []byte("untouched content")); err != nil {
		t.Fatalf("StoreFile(untouched): %v", err)
	}

	before, err := alice.ReadFileMetadata("file")
	if err != nil {
		t.Fatalf("ReadFileMetadata: %v", err)
	}
	baseline := objectCount(t, fixture.store)

	// Delay the append's first write (the new chunk) well past a very
	// short deadline, so the deadline has already elapsed by the time the
	// real Mongo write is attempted.
	fixture.objects.arm(1, 300*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = alice.AppendToFileContext(ctx, "file", []byte("late"))
	fixture.objects.arm(0, 0)

	if err == nil {
		t.Fatal("AppendToFileContext with an already-expired deadline succeeded, want an error")
	}

	// Nothing committed.
	if got := objectCount(t, fixture.store); got != baseline {
		t.Fatalf("aborted append left %d objects behind (%d -> %d)", got-baseline, baseline, got)
	}

	after, err := alice.ReadFileMetadata("file")
	if err != nil {
		t.Fatalf("ReadFileMetadata after abort: %v", err)
	}
	if after.Version != before.Version || after.ChunkCount != before.ChunkCount {
		t.Fatalf("aborted append changed metadata: before=%+v after=%+v", before, after)
	}

	content, err := alice.LoadFile("file")
	if err != nil {
		t.Fatalf("LoadFile after abort: %v", err)
	}
	if string(content) != base {
		t.Fatalf("content after aborted append = %q, want %q", content, base)
	}

	untouched, err := alice.LoadFile("untouched")
	if err != nil || string(untouched) != "untouched content" {
		t.Fatalf("aborted append damaged an unrelated file: content=%q err=%v", untouched, err)
	}

	// The lock the aborted attempt held is not leaked: a normal append
	// with an ordinary context succeeds immediately afterward.
	if err := alice.AppendToFileContext(context.Background(), "file", []byte("-ok")); err != nil {
		t.Fatalf("AppendToFileContext after the aborted attempt: %v", err)
	}
	final, err := alice.LoadFile("file")
	if err != nil {
		t.Fatalf("LoadFile after recovery: %v", err)
	}
	if string(final) != base+"-ok" {
		t.Fatalf("content after recovery = %q, want %q", final, base+"-ok")
	}
}
