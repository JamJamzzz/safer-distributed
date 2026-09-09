// Package coordination defines the concurrency-control abstraction SAFER's
// business logic depends on, and the process-local implementation of it.
//
// It is the lock-side analogue of client/storage. SAFER-CC (V1) called one
// process-wide *lockmanager.LockManager directly; SAFER Distributed keeps
// that manager as the single source of locking logic but reaches it
// through an interface, so a remote coordinator can be substituted without
// touching SAFER's operations.
//
// What this abstraction is NOT is a second lock manager. The S/X
// compatibility rules, FIFO fairness, the ResourceID model, and all
// holder/waiter bookkeeping stay in client/lockmanager, unduplicated. A
// remote backend forwards requests to a coordinator that runs that same
// LockManager; it does not reimplement any part of it.
//
// Strict 2PL is preserved by the shape of the interface: a Guard exposes
// no per-resource release. Locks are released only when the transaction
// ends, all at once.
package coordination

import (
	"context"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
)

// Guard is one transaction's lock scope. A SAFER operation obtains a
// guard, acquires what it needs, and ends the transaction exactly once on
// every path.
//
// There is deliberately no Release(resource) method. Under strict 2PL a
// transaction holds every lock it takes until it ends, so exposing early
// per-resource release would offer callers a way to violate the protocol.
type Guard interface {
	// Acquire blocks until the transaction holds mode on resource.
	Acquire(resource lockmanager.ResourceID, mode lockmanager.LockMode) error

	// ReleaseAll ends the transaction, releasing every lock it holds. It
	// must be safe to call more than once, since operations call it both
	// on the success path and from a deferred cleanup.
	ReleaseAll()
}

// FencedGuard is implemented by guards whose grants carry fencing tokens.
//
// The local backend does not implement it: within one process there is no
// stale-writer problem to solve, because a transaction cannot outlive the
// process that is driving it. A remote guard does, and its grants must
// reach the storage transaction that commits the work.
type FencedGuard interface {
	Guard
	// FenceGrants returns the fencing grants this transaction holds, one
	// per exclusive lock.
	FenceGrants() []fencing.Grant
}

// GrantsOf returns a guard's fencing grants, or nil when the guard is not
// fenced.
func GrantsOf(guard Guard) []fencing.Grant {
	fenced, ok := guard.(FencedGuard)
	if !ok {
		return nil
	}
	return fenced.FenceGrants()
}

// Backend hands out guards. Implementations are the process-local lock
// manager and the gRPC coordinator client.
type Backend interface {
	// Begin starts a transaction scope.
	//
	// localTxn is the process-local transaction identifier SAFER already
	// allocates. A local backend uses it directly. A remote backend
	// generates its own globally unique identifier -- a process-local
	// counter is not unique across workers -- and may ignore this value.
	Begin(localTxn lockmanager.TxnID) (Guard, error)

	// Health reports whether the backend is usable.
	Health(ctx context.Context) error

	// Close releases any resources the backend holds. It does not release
	// locks; transactions do that themselves.
	Close() error
}

// LocalBackend coordinates transactions inside one process, using the
// LockManager directly. It is SAFER-CC's V1 behavior, unchanged, and
// remains the default.
//
// Its limitation is the reason Phase 3 exists: two SAFER processes each
// have their own LockManager and so coordinate nothing with each other,
// even when they share one MongoDB.
type LocalBackend struct {
	lm *lockmanager.LockManager
}

var _ Backend = (*LocalBackend)(nil)

// NewLocalBackend wraps an existing LockManager. Callers pass the
// process-wide instance; constructing a second manager would silently
// split coordination in half.
func NewLocalBackend(lm *lockmanager.LockManager) *LocalBackend {
	return &LocalBackend{lm: lm}
}

// Begin returns a guard bound to SAFER's own transaction id.
func (b *LocalBackend) Begin(localTxn lockmanager.TxnID) (Guard, error) {
	return lockmanager.NewLockGuard(b.lm, localTxn), nil
}

// Health always succeeds: an in-process lock manager cannot be
// unreachable.
func (b *LocalBackend) Health(ctx context.Context) error { return nil }

// Close is a no-op. The LockManager has no resources to release, and
// closing must not discard locks that live transactions still hold.
func (b *LocalBackend) Close() error { return nil }

// LockManager exposes the underlying manager, for callers that need it
// directly (the coordinator server embeds one).
func (b *LocalBackend) LockManager() *lockmanager.LockManager { return b.lm }
