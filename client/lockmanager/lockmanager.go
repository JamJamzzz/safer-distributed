// Package lockmanager implements a generic, in-process, fine-grained
// shared/exclusive lock manager intended to serve as the concurrency-control
// primitive underneath SAFER-CC's strict two-phase-locking layer.
//
// This package deliberately knows nothing about SAFER: no Users, no
// NamespaceEntry, no AccessBox, no FileID semantics. It only understands
// abstract ResourceID keys, S/X lock modes, and caller-supplied transaction
// identifiers. Callers (SAFER API operations, in a later phase) are
// responsible for choosing resource keys and acquisition order.
//
// Concurrency invariants maintained by this package:
//
//   - At most one exclusive (X) holder per resource, and if an X holder
//     exists, the shared-holder set for that resource is empty.
//   - A transaction appears in the holder bookkeeping for a resource if and
//     only if its lock on that resource is currently granted.
//   - A queued exclusive request can never be bypassed indefinitely by
//     later-arriving shared requests: a shared request may only be granted
//     ahead of an earlier queued request if every earlier still-pending
//     request is itself shared.
//   - lm.mu protects ALL holder/queue bookkeeping across every resource.
//     It is held only for short metadata operations and is transparently
//     released while a goroutine is blocked in sync.Cond.Wait(). It must
//     NEVER be held across unrelated work (e.g. future datastore I/O) --
//     callers acquire/release logical S/X locks, which may remain held
//     across an entire SAFER operation, but that is a completely different
//     (and much coarser-grained) notion of "locked" than lm.mu.
package lockmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ResourceType distinguishes broad classes of lockable resources. SAFER-CC
// V1 uses NamespaceResource and FileResource, but this package treats the
// type as an opaque discriminator -- it never branches on its value.
type ResourceType uint8

const (
	NamespaceResource ResourceType = iota
	FileResource
)

// ResourceID identifies one logical lockable resource. It is comparable
// (usable as a map key) and carries no pointer identity: two ResourceID
// values with equal Type and Key always refer to the same logical resource,
// regardless of which goroutine or call site constructed them. Callers are
// responsible for producing a deterministic, unambiguous Key (e.g. a hash
// or structured encoding) so that resource identity cannot collide or be
// spoofed by naive string concatenation.
type ResourceID struct {
	Type ResourceType
	Key  string
}

// LockMode is the mode requested/held on a resource.
type LockMode uint8

const (
	SharedLock LockMode = iota
	ExclusiveLock
)

func (m LockMode) String() string {
	switch m {
	case SharedLock:
		return "S"
	case ExclusiveLock:
		return "X"
	default:
		return "?"
	}
}

// TxnID is an explicit, caller-supplied logical transaction/operation
// identifier. The lock manager never derives ownership from goroutine
// identity (Go does not expose stable goroutine IDs, and doing so would
// tie correctness to an implementation detail of the runtime).
type TxnID uint64

// lockRequest represents one pending-or-granted request for a resource.
// Pointer identity is used to find/remove a specific request within a
// resource's wait queue, and to answer "is anything ahead of ME blocking
// me" without needing a separate sequence number.
type lockRequest struct {
	txn  TxnID
	mode LockMode

	// cancelled is set (under lm.mu) when the waiter's context is done.
	// sync.Cond.Wait cannot itself be interrupted, so cancellation is
	// delivered as state plus a Broadcast: the watcher marks the request
	// and wakes the resource, and the waiting goroutine observes the flag
	// when it re-checks. Only AcquireContext callers can ever see this
	// set; a plain Acquire has no context to cancel.
	cancelled bool
}

// lockState is the per-resource bookkeeping record.
type lockState struct {
	cond *sync.Cond

	sharedHolders   map[TxnID]struct{}
	hasExclusive    bool
	exclusiveHolder TxnID

	// queue holds requests that have not yet been granted, in arrival
	// (FIFO) order. A request is removed from queue the instant it is
	// granted.
	queue []*lockRequest
}

// LockManager is a fine-grained shared/exclusive lock manager keyed by
// ResourceID. All bookkeeping (holders, wait queues) is protected by a
// single internal mutex; each resource has its own condition variable
// (bound to that same mutex) so that Broadcast wakes only the goroutines
// waiting on that specific resource.
type LockManager struct {
	mu        sync.Mutex
	resources map[ResourceID]*lockState
	held      map[TxnID]map[ResourceID]LockMode
}

// NewLockManager constructs an empty LockManager. Multiple independent
// instances may be created (e.g. one per test), and a single package-level
// or application-level instance is expected to coordinate all SAFER
// operations within a process in later phases.
func NewLockManager() *LockManager {
	return &LockManager{
		resources: make(map[ResourceID]*lockState),
		held:      make(map[TxnID]map[ResourceID]LockMode),
	}
}

// Acquire blocks until txn is granted mode on resource, or returns an error
// immediately without blocking if the request is a programming error:
// txn already holds *any* lock (of any mode) on resource. V1 supports
// neither lock upgrades (S -> X) nor reentrant acquisition, so a second
// acquisition attempt for a resource a transaction already holds is always
// rejected rather than silently blocked or silently reused -- callers must
// know the strongest mode they need before entering their critical section.
//
// Acquire never busy-spins: a blocked request waits on the resource's
// condition variable and is woken (and re-checks its grant predicate, to
// correctly handle spurious wakeups) only when the resource's holder/queue
// state changes.
func (lm *LockManager) Acquire(txn TxnID, resource ResourceID, mode LockMode) error {
	// context.Background() has a nil Done channel, so this takes exactly
	// the same code path Acquire always did: no watcher goroutine, no
	// cancellation checks that can ever fire.
	return lm.AcquireContext(context.Background(), txn, resource, mode)
}

// AcquireContext is Acquire, with the wait made cancellable.
//
// It exists because a remote caller can disappear. The RPC coordinator
// serves each Acquire under the caller's context; when that caller
// disconnects or gives up, the request must leave the wait queue. A
// request that stayed queued would eventually reach the front and be
// granted to a transaction nobody is driving any more -- a ghost lock,
// blocking every later transaction on that resource until something
// external cleaned it up.
//
// Cancellation is checked before the grant test on every wake, so a
// request whose context is already done is never granted even if the
// resource happened to become available at the same moment.
//
// There is one unavoidable ordering: a context can be cancelled in the
// instant after a grant has been decided and before the caller learns of
// it. AcquireContext returns nil in that case -- the lock really is held,
// and pretending otherwise would leak it. Callers must therefore always
// end their transaction (see the coordinator's EndTransaction path, which
// runs even when Acquire's response never arrives).
//
// A cancelled request returns ctx.Err() and holds no lock.
func (lm *LockManager) AcquireContext(ctx context.Context, txn TxnID, resource ResourceID, mode LockMode) error {
	if ctx == nil {
		return errors.New("lockmanager: nil context")
	}
	if mode != SharedLock && mode != ExclusiveLock {
		return errors.New("lockmanager: invalid lock mode")
	}
	// Never enqueue a request that is already doomed.
	if err := ctx.Err(); err != nil {
		return err
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if txnLocks, ok := lm.held[txn]; ok {
		if heldMode, already := txnLocks[resource]; already {
			return fmt.Errorf(
				"lockmanager: txn %d already holds %s on resource %+v; "+
					"lock upgrades and reentrant acquisition are not supported in V1",
				txn, heldMode, resource,
			)
		}
	}

	ls := lm.resources[resource]
	if ls == nil {
		ls = &lockState{
			sharedHolders: make(map[TxnID]struct{}),
		}
		ls.cond = sync.NewCond(&lm.mu)
		lm.resources[resource] = ls
	}

	req := &lockRequest{txn: txn, mode: mode}
	ls.queue = append(ls.queue, req)

	// Watch for cancellation only when the context can actually be
	// cancelled. context.Background() has a nil Done channel, so plain
	// Acquire spawns nothing and behaves exactly as it always has.
	if done := ctx.Done(); done != nil {
		stopWatching := make(chan struct{})
		defer close(stopWatching)
		go func() {
			select {
			case <-done:
				lm.mu.Lock()
				req.cancelled = true
				// Wake this resource's waiters so the cancelled one
				// re-checks and unwinds.
				ls.cond.Broadcast()
				lm.mu.Unlock()
			case <-stopWatching:
			}
		}()
	}

	for {
		// Cancellation is checked first, so a request whose context is
		// done is never granted even if the resource is free.
		if req.cancelled {
			ls.removeFromQueue(req)
			// Removing a queued request changes the "what is ahead of
			// me" answer for everything behind it, exactly as a grant
			// does; those waiters must re-check.
			ls.cond.Broadcast()
			lm.sweepIfIdle(resource, ls)
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.Canceled
		}
		if ls.canGrant(req) {
			break
		}
		ls.cond.Wait()
		if hook := testHookAfterWake; hook != nil {
			// Runs with lm.mu held, immediately after a wake and before
			// the re-check above. Test-only; nil in production.
			hook(lm)
		}
	}

	ls.removeFromQueue(req)

	if mode == ExclusiveLock {
		ls.hasExclusive = true
		ls.exclusiveHolder = txn
	} else {
		ls.sharedHolders[txn] = struct{}{}
	}

	if lm.held[txn] == nil {
		lm.held[txn] = make(map[ResourceID]LockMode)
	}
	lm.held[txn][resource] = mode

	// Granting this request removed it from the queue, which can change
	// the "what's ahead of me" answer for requests still waiting behind
	// it -- wake them so they re-check.
	ls.cond.Broadcast()

	return nil
}

// canGrant reports whether req may be granted right now, given the
// resource's current holders and the requests still ahead of it in the
// wait queue. Callers must hold lm.mu.
//
// Fairness policy: a request is grantable only if it is compatible with
// the current holders AND every other request still pending strictly
// ahead of it in the queue is Shared while req itself is also Shared. In
// other words: compatible shared requests may be granted together as a
// batch (even out of strict single-file order relative to each other),
// but nothing may ever be granted ahead of a queued exclusive request, and
// an exclusive request is only grantable once it is the sole thing left
// blocking (no holders, and it has reached the front by virtue of
// everything ahead of it having already drained). This prevents a
// continuous stream of readers from starving a queued writer.
func (ls *lockState) canGrant(req *lockRequest) bool {
	if req.mode == ExclusiveLock {
		if ls.hasExclusive || len(ls.sharedHolders) > 0 {
			return false
		}
	} else {
		if ls.hasExclusive {
			return false
		}
	}

	for _, other := range ls.queue {
		if other == req {
			break
		}
		if !(other.mode == SharedLock && req.mode == SharedLock) {
			return false
		}
	}

	return true
}

// removeFromQueue deletes req from the resource's wait queue by pointer
// identity. Callers must hold lm.mu.
func (ls *lockState) removeFromQueue(req *lockRequest) {
	for i, other := range ls.queue {
		if other == req {
			ls.queue = append(ls.queue[:i], ls.queue[i+1:]...)
			return
		}
	}
}

// Release releases txn's lock on resource. It returns an error, and leaves
// all state unchanged, if txn does not currently hold a lock on resource
// (including if some other transaction holds it) -- ownership is always
// checked before any bookkeeping is mutated.
func (lm *LockManager) Release(txn TxnID, resource ResourceID) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.releaseLocked(txn, resource)
}

// releaseLocked performs the actual release. Callers must hold lm.mu.
func (lm *LockManager) releaseLocked(txn TxnID, resource ResourceID) error {
	txnLocks, ok := lm.held[txn]
	if !ok {
		return fmt.Errorf("lockmanager: txn %d holds no locks", txn)
	}

	mode, ok := txnLocks[resource]
	if !ok {
		return fmt.Errorf("lockmanager: txn %d does not hold a lock on resource %+v", txn, resource)
	}

	ls := lm.resources[resource]
	if ls == nil {
		// Should be unreachable: held bookkeeping and resources map are
		// kept in sync by construction.
		return fmt.Errorf("lockmanager: internal inconsistency: no lock state for resource %+v", resource)
	}

	switch mode {
	case ExclusiveLock:
		if !ls.hasExclusive || ls.exclusiveHolder != txn {
			return fmt.Errorf("lockmanager: internal inconsistency releasing exclusive lock on %+v", resource)
		}
		ls.hasExclusive = false
		ls.exclusiveHolder = TxnID(0)
	case SharedLock:
		if _, held := ls.sharedHolders[txn]; !held {
			return fmt.Errorf("lockmanager: internal inconsistency releasing shared lock on %+v", resource)
		}
		delete(ls.sharedHolders, txn)
	}

	delete(txnLocks, resource)
	if len(txnLocks) == 0 {
		delete(lm.held, txn)
	}

	ls.cond.Broadcast()

	// Only reclaim the lock-state record once nothing holds it AND
	// nothing is waiting on it. Deleting it while a request is still
	// queued would strand that goroutine: it is blocked on THIS *sync.Cond
	// specifically, and a future Acquire for the same resource key would
	// otherwise create a brand-new lockState/cond that the stranded
	// goroutine would never be woken by. We accept retaining a handful of
	// transiently-empty-but-briefly-not-yet-swept records in exchange for
	// never risking that class of bug.
	lm.sweepIfIdle(resource, ls)

	return nil
}

// testHookAfterWake, when non-nil, is invoked with lm.mu held immediately
// after a waiting request wakes and before it re-checks its cancellation
// and grant predicates.
//
// It exists so tests can construct the one interleaving that cannot be
// produced from outside the package: a request whose context was
// cancelled AND whose resource became free in the same wake. That
// coincidence is exactly what the cancellation-before-grant check
// protects against, and without a hook a test can only ever observe the
// easy case where the resource is still held. Always nil in production.
var testHookAfterWake func(lm *LockManager)

// sweepIfIdle reclaims a resource's lock-state record once nothing holds
// it and nothing is waiting on it. Callers must hold lm.mu.
//
// The record must NOT be reclaimed while a request is still queued: that
// waiter is blocked on THIS *sync.Cond specifically, and a later Acquire
// for the same resource key would create a fresh lockState whose
// Broadcasts would never reach the stranded goroutine.
func (lm *LockManager) sweepIfIdle(resource ResourceID, ls *lockState) {
	if !ls.hasExclusive && len(ls.sharedHolders) == 0 && len(ls.queue) == 0 {
		delete(lm.resources, resource)
	}
}

// ReleaseAll releases every lock currently held by txn. It acquires lm.mu
// once (rather than repeatedly via Release) and releases every held
// resource under that single critical section. If releasing one resource
// somehow fails (an internal inconsistency), ReleaseAll still attempts to
// release the remaining resources rather than aborting early and leaking
// locks; it returns the first error encountered, if any.
func (lm *LockManager) ReleaseAll(txn TxnID) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	txnLocks, ok := lm.held[txn]
	if !ok || len(txnLocks) == 0 {
		return nil
	}

	resources := make([]ResourceID, 0, len(txnLocks))
	for resource := range txnLocks {
		resources = append(resources, resource)
	}

	var firstErr error
	for _, resource := range resources {
		if err := lm.releaseLocked(txn, resource); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// LockGuard is a small transaction-scoped convenience wrapper so that
// integration call sites (Phase 4+) can write:
//
//	guard := lockmanager.NewLockGuard(lm, txn)
//	defer guard.ReleaseAll()
//	if err := guard.Acquire(nsResource, lockmanager.SharedLock); err != nil { return err }
//	if err := guard.Acquire(fileResource, lockmanager.ExclusiveLock); err != nil { return err }
//	... perform the operation, using return/defer so every path reaches ReleaseAll ...
//
// so that no error return path can leak a held lock. It intentionally adds
// no behavior beyond "remember which txn, and make ReleaseAll idempotent";
// the LockManager itself remains the source of truth for all locking logic.
type LockGuard struct {
	lm   *LockManager
	txn  TxnID
	once sync.Once
}

// NewLockGuard creates a guard for txn against lm.
func NewLockGuard(lm *LockManager, txn TxnID) *LockGuard {
	return &LockGuard{lm: lm, txn: txn}
}

// Acquire acquires mode on resource on behalf of the guard's transaction.
func (g *LockGuard) Acquire(resource ResourceID, mode LockMode) error {
	return g.lm.Acquire(g.txn, resource, mode)
}

// AcquireContext is Acquire with a cancellable wait.
func (g *LockGuard) AcquireContext(ctx context.Context, resource ResourceID, mode LockMode) error {
	return g.lm.AcquireContext(ctx, g.txn, resource, mode)
}

// ReleaseAll releases every lock held by the guard's transaction. Safe to
// call multiple times (e.g. once explicitly on the success path and once
// via a deferred call) -- only the first call has any effect.
func (g *LockGuard) ReleaseAll() {
	g.once.Do(func() {
		_ = g.lm.ReleaseAll(g.txn)
	})
}
