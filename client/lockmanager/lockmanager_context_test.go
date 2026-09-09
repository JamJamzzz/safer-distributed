package lockmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Tests for AcquireContext, the cancellation-aware acquisition path added
// for remote coordination.
//
// The property that matters is negative and easy to get wrong: a request
// whose caller has gone away must leave the wait queue and must never be
// granted afterwards. A request that stayed queued would eventually reach
// the front and be granted to a transaction nobody is driving -- a ghost
// lock that blocks the resource indefinitely.
//
// These tests are deterministic. They synchronize on internal bookkeeping
// ("has this request reached the queue yet") rather than on sleeps, using
// the same white-box helpers as the rest of this package's tests.

// heldResourceCount reports how many resources txn currently holds locks
// on, which is the direct oracle for "was this transaction ever granted
// anything".
func (lm *LockManager) heldResourceCount(txn TxnID) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return len(lm.held[txn])
}

func acquireContextAsync(lm *LockManager, ctx context.Context, txn TxnID, resource ResourceID, mode LockMode) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- lm.AcquireContext(ctx, txn, resource, mode)
	}()
	return done
}

// TestAcquireContextCancelledWaiterLeavesQueue is the core property: a
// cancelled pending request is removed from the wait queue and never
// granted, even after the resource becomes free.
func TestAcquireContextCancelledWaiterLeavesQueue(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "cancel-leaves-queue"}

	// txn 1 holds X, so anything else must wait.
	if err := lm.Acquire(1, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 1 Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	waiter := acquireContextAsync(lm, ctx, 2, resource, ExclusiveLock)

	// Wait for the request to actually reach the queue, so the test is
	// cancelling a genuinely pending request rather than racing the
	// enqueue.
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 1 })
	expectBlocked(t, waiter, 20*time.Millisecond, "txn 2 while txn 1 holds X")

	cancel()

	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Acquire returned %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled Acquire did not return")
	}

	// Removed from the queue...
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 0 })
	// ...and holding nothing.
	if got := lm.heldResourceCount(2); got != 0 {
		t.Fatalf("cancelled txn 2 holds %d resources, want 0", got)
	}

	// The decisive step: free the resource. A ghost lock would surface
	// here, as txn 2 being granted after it gave up.
	if err := lm.Release(1, resource); err != nil {
		t.Fatalf("txn 1 Release: %v", err)
	}
	if lm.hasExclusiveHolder(resource) {
		t.Fatal("resource has an exclusive holder after the only waiter was cancelled")
	}
	if got := lm.heldResourceCount(2); got != 0 {
		t.Fatalf("cancelled txn 2 was granted the lock after release: holds %d resources", got)
	}

	// And the resource is genuinely usable by someone else.
	if err := lm.Acquire(3, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 3 Acquire after cancellation: %v", err)
	}
	if err := lm.Release(3, resource); err != nil {
		t.Fatalf("txn 3 Release: %v", err)
	}
}

// TestAcquireContextCancelledWaiterDoesNotStrandOthers checks that
// removing a cancelled request from the middle of the queue wakes the
// requests behind it, rather than leaving them blocked on a predicate
// that already changed.
func TestAcquireContextCancelledWaiterDoesNotStrandOthers(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "cancel-does-not-strand"}

	if err := lm.Acquire(1, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 1 Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	doomed := acquireContextAsync(lm, ctx, 2, resource, ExclusiveLock)
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 1 })

	// txn 3 queues behind the doomed request.
	survivor := acquireAsync(lm, 3, resource, ExclusiveLock)
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 2 })

	cancel()
	select {
	case err := <-doomed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Acquire returned %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled Acquire did not return")
	}

	// txn 3 must still be blocked: txn 1 still holds X. Cancelling the
	// request ahead of it must not grant it early.
	expectBlocked(t, survivor, 20*time.Millisecond, "txn 3 while txn 1 still holds X")
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 1 })

	if err := lm.Release(1, resource); err != nil {
		t.Fatalf("txn 1 Release: %v", err)
	}
	expectGranted(t, survivor, testTimeout, "txn 3 after txn 1 released")
	if got := lm.heldResourceCount(2); got != 0 {
		t.Fatalf("cancelled txn 2 holds %d resources, want 0", got)
	}
	if err := lm.Release(3, resource); err != nil {
		t.Fatalf("txn 3 Release: %v", err)
	}
}

// TestAcquireContextAlreadyCancelledNeverQueues checks the fast path: a
// context that is already done must not enqueue anything at all.
func TestAcquireContextAlreadyCancelledNeverQueues(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "already-cancelled"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lm.AcquireContext(ctx, 1, resource, ExclusiveLock)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if got := lm.queueLen(resource); got != 0 {
		t.Fatalf("queue length %d, want 0", got)
	}
	if got := lm.heldResourceCount(1); got != 0 {
		t.Fatalf("txn 1 holds %d resources, want 0", got)
	}
	// The resource is untouched and immediately usable.
	if err := lm.Acquire(2, resource, ExclusiveLock); err != nil {
		t.Fatalf("Acquire after a rejected request: %v", err)
	}
}

// TestAcquireContextDeadlineExceeded covers the other way a context ends.
func TestAcquireContextDeadlineExceeded(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "deadline"}

	if err := lm.Acquire(1, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 1 Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := lm.AcquireContext(ctx, 2, resource, ExclusiveLock)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 0 })
	if got := lm.heldResourceCount(2); got != 0 {
		t.Fatalf("timed-out txn 2 holds %d resources, want 0", got)
	}
}

// TestAcquireContextGrantsNormally confirms the cancellable path is a
// superset of the old behavior: an uncancelled request is granted, blocks
// others, and releases normally.
func TestAcquireContextGrantsNormally(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: NamespaceResource, Key: "normal-grant"}
	ctx := context.Background()

	if err := lm.AcquireContext(ctx, 1, resource, SharedLock); err != nil {
		t.Fatalf("txn 1 AcquireContext(S): %v", err)
	}
	if err := lm.AcquireContext(ctx, 2, resource, SharedLock); err != nil {
		t.Fatalf("txn 2 AcquireContext(S): %v", err)
	}
	if got := lm.sharedHolderCount(resource); got != 2 {
		t.Fatalf("shared holders: got %d, want 2", got)
	}

	writer := acquireContextAsync(lm, ctx, 3, resource, ExclusiveLock)
	expectBlocked(t, writer, 20*time.Millisecond, "X while two S holders remain")

	if err := lm.ReleaseAll(1); err != nil {
		t.Fatalf("ReleaseAll(1): %v", err)
	}
	expectBlocked(t, writer, 20*time.Millisecond, "X while one S holder remains")
	if err := lm.ReleaseAll(2); err != nil {
		t.Fatalf("ReleaseAll(2): %v", err)
	}
	expectGranted(t, writer, testTimeout, "X after all S holders released")

	if !lm.hasExclusiveHolder(resource) {
		t.Fatal("resource has no exclusive holder after the writer was granted")
	}
	if err := lm.ReleaseAll(3); err != nil {
		t.Fatalf("ReleaseAll(3): %v", err)
	}
}

// TestAcquireContextCancelAfterGrantKeepsLock pins the documented
// ordering: cancelling after a grant has been decided does not silently
// drop the lock. The lock is really held, so it must be reported as held
// and released through the transaction's normal end path -- anything else
// would leak it.
func TestAcquireContextCancelAfterGrantKeepsLock(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "cancel-after-grant"}

	ctx, cancel := context.WithCancel(context.Background())
	if err := lm.AcquireContext(ctx, 1, resource, ExclusiveLock); err != nil {
		t.Fatalf("AcquireContext: %v", err)
	}
	cancel()

	if !lm.hasExclusiveHolder(resource) {
		t.Fatal("granted lock disappeared when the context was cancelled")
	}
	if got := lm.heldResourceCount(1); got != 1 {
		t.Fatalf("txn 1 holds %d resources, want 1", got)
	}
	if err := lm.ReleaseAll(1); err != nil {
		t.Fatalf("ReleaseAll: %v", err)
	}
	if got := lm.heldResourceCount(1); got != 0 {
		t.Fatalf("after ReleaseAll txn 1 holds %d resources, want 0", got)
	}
}

// TestAcquireContextManyCancellationsLeaveNoResidue runs a churn of
// cancelled requests against a held resource and then checks the manager
// is left completely clean -- no stranded queue entries, no leaked
// resource records, no phantom holders.
func TestAcquireContextManyCancellationsLeaveNoResidue(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "churn"}

	if err := lm.Acquire(1, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 1 Acquire: %v", err)
	}

	const waiters = 32
	for i := 0; i < waiters; i++ {
		txn := TxnID(100 + i)
		ctx, cancel := context.WithCancel(context.Background())
		done := acquireContextAsync(lm, ctx, txn, resource, ExclusiveLock)
		waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 1 })
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter %d: got %v, want context.Canceled", i, err)
			}
		case <-time.After(testTimeout):
			t.Fatalf("waiter %d did not return", i)
		}
		waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 0 })
		if got := lm.heldResourceCount(txn); got != 0 {
			t.Fatalf("cancelled waiter %d holds %d resources, want 0", i, got)
		}
	}

	if err := lm.Release(1, resource); err != nil {
		t.Fatalf("txn 1 Release: %v", err)
	}

	// With nothing held and nothing queued, the resource record is swept.
	lm.mu.Lock()
	remaining := len(lm.resources)
	heldTxns := len(lm.held)
	lm.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d resource records left behind, want 0", remaining)
	}
	if heldTxns != 0 {
		t.Fatalf("%d transactions still hold locks, want 0", heldTxns)
	}
}

// TestAcquireContextCancelWinsOverSimultaneousGrant covers the one
// interleaving that cannot be produced from outside the package: the
// request's context is cancelled AND its resource becomes free in the
// same wake.
//
// Cancellation must win. If grantability were checked first, the request
// would be granted to a caller that has already given up and will never
// release it -- a ghost lock holding the resource forever. The test hook
// frees the resource at the instant the cancelled waiter wakes, making
// that coincidence deterministic rather than a race to hope for.
func TestAcquireContextCancelWinsOverSimultaneousGrant(t *testing.T) {
	lm := NewLockManager()
	resource := ResourceID{Type: FileResource, Key: "cancel-vs-grant"}

	if err := lm.Acquire(1, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 1 Acquire: %v", err)
	}

	var fired bool
	testHookAfterWake = func(inner *LockManager) {
		if fired {
			return
		}
		fired = true
		// Free the resource while the woken request is still deciding,
		// so canGrant would answer true if it were consulted first.
		if err := inner.releaseLocked(1, resource); err != nil {
			t.Errorf("hook releaseLocked: %v", err)
		}
	}
	t.Cleanup(func() { testHookAfterWake = nil })

	ctx, cancel := context.WithCancel(context.Background())
	waiter := acquireContextAsync(lm, ctx, 2, resource, ExclusiveLock)
	waitUntil(t, testTimeout, func() bool { return lm.queueLen(resource) == 1 })

	// The only broadcast that can wake the request is the cancellation,
	// so the hook runs with req.cancelled already set and the resource
	// simultaneously free.
	cancel()

	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled: the request was granted "+
				"even though its caller had cancelled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled Acquire did not return")
	}

	if !fired {
		t.Fatal("test hook never ran; the interleaving under test did not occur")
	}
	if got := lm.heldResourceCount(2); got != 0 {
		t.Fatalf("cancelled txn 2 holds %d resources, want 0 (ghost lock)", got)
	}
	if lm.hasExclusiveHolder(resource) {
		t.Fatal("resource still has an exclusive holder (ghost lock)")
	}
	// The resource is free for an honest transaction.
	if err := lm.Acquire(3, resource, ExclusiveLock); err != nil {
		t.Fatalf("txn 3 Acquire: %v", err)
	}
	if err := lm.ReleaseAll(3); err != nil {
		t.Fatalf("txn 3 ReleaseAll: %v", err)
	}
}
