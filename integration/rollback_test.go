package integration

// Rollback tests for atomic MongoDB persistence.
//
// Each test injects a deterministic failure into the Nth storage mutation
// of a real SAFER operation, against a real MongoDB replica set, and then
// checks that nothing was left half-written.
//
// The failure is injected at the storage boundary rather than simulated,
// so the operation really does fail in the middle of a multi-object
// mutation: some writes have already been issued inside the transaction
// when the next one fails.
//
// Every test asserts four things:
//   - the operation returns an error
//   - no partial object graph is committed
//   - previously valid state is still readable and authenticates
//   - Version / Epoch / authorization invariants still hold
//
// TestRollbackNegativeControl is the control: with the transaction
// boundary bypassed, the same injected failure leaves partial state
// behind, which is what the transaction prevents.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/storage"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

var errInjected = errors.New("injected storage failure")

// failingObjects wraps a real ObjectStore and fails the Nth mutation.
//
// It forwards the context untouched, so the writes it does allow through
// take part in the caller's transaction exactly as they normally would.
// Reads are never failed: the point is to break a mutation part way, not
// to make the operation fail before it starts writing.
type failingObjects struct {
	inner storage.ObjectStore

	mu        sync.Mutex
	failAfter int // fail the mutation with this 1-based index; 0 disables
	mutations int
}

func (f *failingObjects) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}

// arm resets the counter and sets which mutation should fail.
func (f *failingObjects) arm(failAfter int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter = failAfter
	f.mutations = 0
}

func (f *failingObjects) shouldFail() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	return f.failAfter > 0 && f.mutations == f.failAfter
}

func (f *failingObjects) Get(ctx context.Context, id uuid.UUID) ([]byte, bool, error) {
	return f.inner.Get(ctx, id)
}

func (f *failingObjects) Put(ctx context.Context, id uuid.UUID, value []byte) error {
	if f.shouldFail() {
		return errInjected
	}
	return f.inner.Put(ctx, id, value)
}

func (f *failingObjects) Delete(ctx context.Context, id uuid.UUID) error {
	if f.shouldFail() {
		return errInjected
	}
	return f.inner.Delete(ctx, id)
}

// rollbackFixture is one test's MongoDB database with failure injection
// installed between SAFER and the real store.
type rollbackFixture struct {
	store   *mongostore.Store
	objects *failingObjects
	restore func()
}

// newRollbackFixture requires a MongoDB that can actually serve
// transactions. A standalone mongod accepts writes but rejects
// transactions, so it is skipped with an accurate reason rather than
// failing obscurely.
func newRollbackFixture(t *testing.T) *rollbackFixture {
	t.Helper()

	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_rollback_%d", time.Now().UnixNano())

	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	if err := store.SupportsTransactions(ctx); err != nil {
		_ = store.Close(ctx)
		t.Skipf("skipping: this MongoDB cannot serve transactions "+
			"(a replica set is required): %v", err)
	}

	base := store.Storage()
	objects := &failingObjects{inner: base.Objects}
	fixture := &rollbackFixture{store: store, objects: objects}

	fixture.restore = client.UseStorage(storage.Storage{
		Objects: objects,
		Keys:    base.Keys,
		Atomic:  base.Atomic,
	})
	t.Cleanup(func() {
		fixture.restore()
		_ = store.DropDatabase(context.Background())
		_ = store.Close(context.Background())
	})
	return fixture
}

// useWithoutTransactions reinstalls the same stores with the transaction
// capability removed, which is what the negative control needs.
func (f *rollbackFixture) useWithoutTransactions(t *testing.T) {
	t.Helper()
	base := f.store.Storage()
	restore := client.UseStorage(storage.Storage{
		Objects: f.objects,
		Keys:    base.Keys,
		Atomic:  nil, // the boundary under test, deliberately bypassed
	})
	t.Cleanup(restore)
}

// objectCount reports how many objects exist, the direct measure of
// whether a failed operation left anything behind.
func objectCount(t *testing.T, store *mongostore.Store) int {
	t.Helper()
	ids, err := store.ObjectIDs(context.Background())
	if err != nil {
		t.Fatalf("listing objects: %v", err)
	}
	return len(ids)
}

// TestRollbackStoreFileCreate fails each write of a six-object file
// creation in turn and checks that none of them survives.
func TestRollbackStoreFileCreate(t *testing.T) {
	fixture := newRollbackFixture(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	// A committed file to prove unrelated state is untouched by a
	// failed, rolled-back operation.
	if err := alice.StoreFile("existing", []byte("existing content")); err != nil {
		t.Fatalf("StoreFile(existing): %v", err)
	}

	// Count the mutations one creation makes, so every one of them can
	// be failed in turn.
	fixture.objects.arm(0)
	if err := alice.StoreFile("probe", []byte("probe content")); err != nil {
		t.Fatalf("probe StoreFile: %v", err)
	}
	writesPerCreate := fixture.objects.count()
	if writesPerCreate < 6 {
		t.Fatalf("a file creation made %d storage mutations, want at least 6", writesPerCreate)
	}

	baseline := objectCount(t, fixture.store)

	for nth := 1; nth <= writesPerCreate; nth++ {
		filename := fmt.Sprintf("doomed-%d", nth)
		fixture.objects.arm(nth)

		err := alice.StoreFile(filename, []byte("content that must not survive"))
		fixture.objects.arm(0)

		if err == nil {
			t.Fatalf("failure injected at mutation %d: StoreFile succeeded, want an error", nth)
		}

		// Nothing committed: the object count is exactly what it was.
		if got := objectCount(t, fixture.store); got != baseline {
			t.Fatalf("failure at mutation %d left %d objects behind (%d -> %d)",
				nth, got-baseline, baseline, got)
		}
		// And the file is not half-created: it cannot be loaded, and the
		// name is still free.
		if _, err := alice.LoadFile(filename); err == nil {
			t.Fatalf("failure at mutation %d: the file is loadable, so it was partially committed", nth)
		}

		// Previously valid state is still readable and authenticates.
		content, err := alice.LoadFile("existing")
		if err != nil {
			t.Fatalf("failure at mutation %d damaged an unrelated file: %v", nth, err)
		}
		if string(content) != "existing content" {
			t.Fatalf("failure at mutation %d: unrelated file reads %q", nth, content)
		}
	}

	// After the rollbacks, the same filename still works normally: the
	// namespace slot was never consumed.
	if err := alice.StoreFile("doomed-1", []byte("now it works")); err != nil {
		t.Fatalf("StoreFile after rollback: %v", err)
	}
	if content, err := alice.LoadFile("doomed-1"); err != nil || string(content) != "now it works" {
		t.Fatalf("after rollback: got %q err=%v", content, err)
	}
}

// TestRollbackAppendToFile fails each write of an append and checks that
// neither the chunk nor the version advance survives.
func TestRollbackAppendToFile(t *testing.T) {
	fixture := newRollbackFixture(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	const base = "base-content"
	if err := alice.StoreFile("file", []byte(base)); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}

	before, err := alice.ReadFileMetadata("file")
	if err != nil {
		t.Fatalf("ReadFileMetadata: %v", err)
	}
	baselineObjects := objectCount(t, fixture.store)

	// An append writes the chunk and then the metadata.
	for nth := 1; nth <= 2; nth++ {
		fixture.objects.arm(nth)
		err := alice.AppendToFile("file", []byte("-LOST"))
		fixture.objects.arm(0)

		if err == nil {
			t.Fatalf("failure injected at mutation %d: AppendToFile succeeded, want an error", nth)
		}

		if got := objectCount(t, fixture.store); got != baselineObjects {
			t.Fatalf("failure at mutation %d left %d objects behind", nth, got-baselineObjects)
		}

		// Content is exactly the pre-append content: no partial append.
		content, err := alice.LoadFile("file")
		if err != nil {
			t.Fatalf("failure at mutation %d made the file unreadable: %v", nth, err)
		}
		if string(content) != base {
			t.Fatalf("failure at mutation %d: content is %q, want the unchanged %q", nth, content, base)
		}

		// Version did not advance: a failed mutation must not consume a
		// version, or later readers could not tell a lost write from a
		// successful one.
		after, err := alice.ReadFileMetadata("file")
		if err != nil {
			t.Fatalf("failure at mutation %d: ReadFileMetadata: %v", nth, err)
		}
		if after.Version != before.Version {
			t.Fatalf("failure at mutation %d advanced Version from %d to %d",
				nth, before.Version, after.Version)
		}
		if after.ChunkCount != before.ChunkCount {
			t.Fatalf("failure at mutation %d changed ChunkCount from %d to %d",
				nth, before.ChunkCount, after.ChunkCount)
		}
	}

	// A subsequent real append still works and advances exactly once.
	if err := alice.AppendToFile("file", []byte("-KEPT")); err != nil {
		t.Fatalf("AppendToFile after rollback: %v", err)
	}
	content, err := alice.LoadFile("file")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if string(content) != base+"-KEPT" {
		t.Fatalf("after rollback: content %q, want %q", content, base+"-KEPT")
	}
	after, err := alice.ReadFileMetadata("file")
	if err != nil {
		t.Fatalf("ReadFileMetadata: %v", err)
	}
	if after.Version != before.Version+1 {
		t.Fatalf("Version: got %d, want %d (exactly one committed append)", after.Version, before.Version+1)
	}
}

// TestRollbackRevokeAccess is the important one: revocation is an epoch
// rotation touching the most objects, and every partial outcome is a
// security or availability failure.
func TestRollbackRevokeAccess(t *testing.T) {
	fixture := newRollbackFixture(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser alice: %v", err)
	}
	bob, err := client.InitUser("bob", "bob-password")
	if err != nil {
		t.Fatalf("InitUser bob: %v", err)
	}
	const shared = "shared content"
	if err := alice.StoreFile("shared", []byte(shared)); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	invite, err := alice.CreateInvitation("shared", "bob")
	if err != nil {
		t.Fatalf("CreateInvitation: %v", err)
	}
	if err := bob.AcceptInvitation("alice", invite, "from-alice"); err != nil {
		t.Fatalf("AcceptInvitation: %v", err)
	}

	before, err := alice.ReadFileMetadata("shared")
	if err != nil {
		t.Fatalf("ReadFileMetadata: %v", err)
	}
	baselineObjects := objectCount(t, fixture.store)

	// Measure how many mutations a revocation makes, rather than
	// guessing: a guess that is too low silently leaves later mutations
	// untested, and one that is too high fails for the wrong reason.
	if err := alice.StoreFile("probe", []byte(shared)); err != nil {
		t.Fatalf("probe StoreFile: %v", err)
	}
	probeInvite, err := alice.CreateInvitation("probe", "bob")
	if err != nil {
		t.Fatalf("probe CreateInvitation: %v", err)
	}
	if err := bob.AcceptInvitation("alice", probeInvite, "probe-from-alice"); err != nil {
		t.Fatalf("probe AcceptInvitation: %v", err)
	}
	fixture.objects.arm(0)
	if err := alice.RevokeAccess("probe", "bob"); err != nil {
		t.Fatalf("probe RevokeAccess: %v", err)
	}
	writesPerRevoke := fixture.objects.count()
	if writesPerRevoke < 8 {
		t.Fatalf("a revocation made %d storage mutations, want at least 8", writesPerRevoke)
	}
	t.Logf("a revocation makes %d storage mutations; failing each in turn", writesPerRevoke)

	// Re-measure the baseline: the probe changed the object count.
	baselineObjects = objectCount(t, fixture.store)
	before, err = alice.ReadFileMetadata("shared")
	if err != nil {
		t.Fatalf("ReadFileMetadata: %v", err)
	}

	// Fail every mutation of a revocation in turn.
	for nth := 1; nth <= writesPerRevoke; nth++ {
		fixture.objects.arm(nth)
		err := alice.RevokeAccess("shared", "bob")
		fixture.objects.arm(0)

		if err == nil {
			t.Fatalf("failure injected at mutation %d: RevokeAccess succeeded, want an error", nth)
		}

		if got := objectCount(t, fixture.store); got != baselineObjects {
			t.Fatalf("failure at mutation %d changed the object count by %d; "+
				"a rotation was partially committed", nth, got-baselineObjects)
		}

		// The owner's file is intact and authenticates.
		content, err := alice.LoadFile("shared")
		if err != nil {
			t.Fatalf("failure at mutation %d made the owner's file unreadable: %v", nth, err)
		}
		if string(content) != shared {
			t.Fatalf("failure at mutation %d: owner reads %q, want %q", nth, content, shared)
		}

		// Authorization is unchanged: a failed revocation must not
		// half-revoke. Bob still has access, because the revocation did
		// not commit.
		bobContent, err := bob.LoadFile("from-alice")
		if err != nil {
			t.Fatalf("failure at mutation %d: revocation partially applied, "+
				"the recipient lost access even though the operation failed: %v", nth, err)
		}
		if string(bobContent) != shared {
			t.Fatalf("failure at mutation %d: recipient reads %q, want %q", nth, bobContent, shared)
		}

		// Version and epoch bookkeeping did not advance.
		after, err := alice.ReadFileMetadata("shared")
		if err != nil {
			t.Fatalf("failure at mutation %d: ReadFileMetadata: %v", nth, err)
		}
		if after.Version != before.Version {
			t.Fatalf("failure at mutation %d advanced Version from %d to %d",
				nth, before.Version, after.Version)
		}
	}

	// A real revocation still works afterwards, and fully applies.
	if err := alice.RevokeAccess("shared", "bob"); err != nil {
		t.Fatalf("RevokeAccess after rollbacks: %v", err)
	}
	if _, err := bob.LoadFile("from-alice"); err == nil {
		t.Fatal("recipient still has access after a committed revocation")
	}
	content, err := alice.LoadFile("shared")
	if err != nil {
		t.Fatalf("owner LoadFile after revocation: %v", err)
	}
	if string(content) != shared {
		t.Fatalf("owner content after revocation: %q, want %q", content, shared)
	}
}

// TestRollbackInitUser covers the identity case: a failed InitUser must
// not leave a claimed username, because key registration is write-once and
// a half-created account could never be repaired.
func TestRollbackInitUser(t *testing.T) {
	fixture := newRollbackFixture(t)

	// InitUser writes the account object; the two keystore writes go to
	// the key collection, which this injector does not wrap, so failing
	// the first object mutation fails the account write inside the same
	// transaction as the keys.
	fixture.objects.arm(1)
	_, err := client.InitUser("alice", "alice-password")
	fixture.objects.arm(0)
	if err == nil {
		t.Fatal("InitUser succeeded despite an injected failure")
	}
	if got := objectCount(t, fixture.store); got != 0 {
		t.Fatalf("failed InitUser left %d objects behind", got)
	}

	// The username is still free, and claiming it now works completely.
	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser after rollback: %v", err)
	}
	if err := alice.StoreFile("file", []byte("content")); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	revived, err := client.GetUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("GetUser after rollback: %v", err)
	}
	if content, err := revived.LoadFile("file"); err != nil || string(content) != "content" {
		t.Fatalf("after rollback: got %q err=%v", content, err)
	}
}

// TestRollbackNegativeControl is the control for every test above: with
// the transaction boundary removed and the identical failure injected, the
// same operation leaves partial state committed.
//
// Without this, the tests above could be passing because the operations
// happen not to write anything before the failure point, rather than
// because the transaction rolls them back.
func TestRollbackNegativeControl(t *testing.T) {
	fixture := newRollbackFixture(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}

	// Measure how much a creation writes, with transactions on.
	fixture.objects.arm(0)
	if err := alice.StoreFile("probe", []byte("probe")); err != nil {
		t.Fatalf("probe StoreFile: %v", err)
	}
	writesPerCreate := fixture.objects.count()
	if writesPerCreate < 6 {
		t.Fatalf("a file creation made %d mutations, want at least 6", writesPerCreate)
	}

	// With the boundary in place, failing the last write leaves nothing.
	withTransactions := objectCount(t, fixture.store)
	fixture.objects.arm(writesPerCreate)
	if err := alice.StoreFile("atomic-attempt", []byte("content")); err == nil {
		t.Fatal("StoreFile succeeded despite an injected failure")
	}
	fixture.objects.arm(0)
	if got := objectCount(t, fixture.store); got != withTransactions {
		t.Fatalf("with the transaction boundary, a failed create left %d objects behind",
			got-withTransactions)
	}

	// Now bypass the boundary and inject the identical failure.
	fixture.useWithoutTransactions(t)

	beforeBypass := objectCount(t, fixture.store)
	fixture.objects.arm(writesPerCreate)
	if err := alice.StoreFile("leaky-attempt", []byte("content")); err == nil {
		t.Fatal("StoreFile succeeded despite an injected failure")
	}
	fixture.objects.arm(0)

	leaked := objectCount(t, fixture.store) - beforeBypass
	if leaked == 0 {
		t.Fatal("without the transaction boundary the same failure left nothing behind; " +
			"this control proves nothing, and the rollback tests above may be vacuous")
	}
	t.Logf("control: bypassing the transaction boundary leaked %d partially committed objects", leaked)

	// And that partial state is exactly the problem: orphaned objects
	// that SAFER's own API cannot reach or clean up.
	if _, err := alice.LoadFile("leaky-attempt"); err == nil {
		t.Fatal("the partially created file is loadable, which is not the state under test")
	}
}

// TestAtomicCapabilityIsAdvertised pins the wiring: the MongoDB backend
// advertises transactions and the userlib backend does not.
func TestAtomicCapabilityIsAdvertised(t *testing.T) {
	if storage.SupportsAtomic(storage.NewUserlibStorage()) {
		t.Fatal("the userlib backend claims atomic support it does not have")
	}

	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_atomic_%d", time.Now().UnixNano())

	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DropDatabase(context.Background())
		_ = store.Close(context.Background())
	})

	if !storage.SupportsAtomic(store.Storage()) {
		t.Fatal("the MongoDB backend does not advertise atomic support")
	}
	if err := store.SupportsTransactions(ctx); err != nil {
		t.Skipf("skipping the rest: this MongoDB cannot serve transactions: %v", err)
	}

	// Nesting is refused rather than silently flattened, which would
	// widen an inner transaction's scope to the outer one.
	err = store.RunAtomic(ctx, func(inner context.Context) error {
		return store.RunAtomic(inner, func(context.Context) error { return nil })
	})
	if err == nil {
		t.Fatal("nested RunAtomic succeeded, want a refusal")
	}

	// A returned error rolls back; a nil return commits.
	id := uuid.New()
	rollbackErr := store.RunAtomic(ctx, func(txCtx context.Context) error {
		if err := store.Objects().Put(txCtx, id, []byte("should not survive")); err != nil {
			return err
		}
		return errInjected
	})
	if !errors.Is(rollbackErr, errInjected) {
		t.Fatalf("RunAtomic returned %v, want the injected error", rollbackErr)
	}
	if _, found, err := store.Objects().Get(ctx, id); err != nil || found {
		t.Fatalf("rolled-back write is visible: found=%v err=%v", found, err)
	}

	if err := store.RunAtomic(ctx, func(txCtx context.Context) error {
		return store.Objects().Put(txCtx, id, []byte("committed"))
	}); err != nil {
		t.Fatalf("committing RunAtomic: %v", err)
	}
	if _, found, err := store.Objects().Get(ctx, id); err != nil || !found {
		t.Fatalf("committed write is missing: found=%v err=%v", found, err)
	}
}
