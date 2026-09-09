package grpccoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
)

// lifecycle is a transaction's state on the coordinator.
type lifecycle int

const (
	// txnActive is a transaction the worker is driving. It may acquire,
	// renew, and end.
	txnActive lifecycle = iota
	// txnRevoking is a transaction whose lease has passed and whose
	// cleanup has begun. It accepts nothing further: new Acquire and
	// RenewLease are rejected, so a worker that comes back learns it lost
	// its locks rather than continuing as if it had them.
	txnRevoking
	// txnEnding is a transaction the worker itself is ending. Same
	// closed-to-new-work rule, different reason.
	txnEnding
)

func (l lifecycle) String() string {
	switch l {
	case txnActive:
		return "active"
	case txnRevoking:
		return "revoking"
	case txnEnding:
		return "ending"
	default:
		return "unknown"
	}
}

// grantRecord is one lock this transaction holds.
type grantRecord struct {
	resource lockmanager.ResourceID
	mode     lockmanager.LockMode
	// token is the fencing token for an exclusive grant, and zero for a
	// shared one.
	token fencing.Token
}

// transaction is the coordinator's state for one external transaction.
//
// The generic LockManager remains the only place S/X compatibility, FIFO
// fairness, and holder bookkeeping live. This record adds what the
// coordinator needs on top and the manager has no opinion about: who the
// external owner is, whether the transaction is still alive, when its
// lease runs out, and which fencing tokens it was issued.
type transaction struct {
	external uuid.UUID
	internal lockmanager.TxnID

	state         lifecycle
	leaseDeadline time.Time
	// leaseStarted is false until the first lock is actually granted. A
	// transaction that has been created but holds nothing has nothing to
	// reclaim, so there is nothing to time out.
	leaseStarted bool

	// grants is every lock currently held, in acquisition order.
	grants []grantRecord

	// pending cancels in-flight Acquire calls. Revocation cancels them so
	// a queued request cannot be granted to a transaction that is being
	// torn down.
	pending map[uint64]context.CancelFunc
	nextReq uint64

	// finished marks a transaction whose teardown is complete. Its record
	// is deliberately kept for a while afterwards as a tombstone, rather
	// than deleted immediately.
	//
	// Without it, a worker that was revoked and then came back would
	// present the same UUID, find no record, and be handed a brand-new
	// transaction -- silently continuing as though nothing had happened.
	// Its stale writes would still be refused by fencing, but it would
	// never learn it had lost its locks. The tombstone makes that
	// explicit: it gets a failure telling it the transaction is over.
	finished   bool
	finishedAt time.Time
}

// fenceGrants returns the fencing grants for this transaction's exclusive
// locks, which are the ones that must be invalidated before its locks can
// be handed to anyone else.
func (t *transaction) fenceGrants() []fencing.Grant {
	var grants []fencing.Grant
	for _, g := range t.grants {
		if g.mode != lockmanager.ExclusiveLock {
			continue
		}
		grants = append(grants, fencing.Grant{
			Resource: fencing.ResourceKey(uint8(g.resource.Type), g.resource.Key),
			Token:    g.token,
			OwnerTxn: t.external.String(),
		})
	}
	return grants
}

// grantFor returns the existing grant for a resource, if this transaction
// already holds one.
func (t *transaction) grantFor(resource lockmanager.ResourceID) (grantRecord, bool) {
	for _, g := range t.grants {
		if g.resource == resource {
			return g, true
		}
	}
	return grantRecord{}, false
}

// registry is the coordinator's transaction table.
//
// Its mutex protects the table only, and is never held across a blocking
// lock wait. Holding it while a request waits would deadlock the
// coordinator against itself: the EndTransaction that would release the
// lock cannot take the mutex to do so.
type registry struct {
	mu    sync.Mutex
	byID  map[uuid.UUID]*transaction
	next  uint64
	spent bool

	leaseDuration time.Duration
}

func newRegistry(leaseDuration time.Duration) *registry {
	return &registry{
		byID:          make(map[uuid.UUID]*transaction),
		leaseDuration: leaseDuration,
	}
}

// errTxnIDsExhausted is returned once the internal id space is used up.
// Recycling ids would let a new transaction inherit an old one's locks.
var errTxnIDsExhausted = fmt.Errorf("grpccoord: transaction ID space exhausted")

// ensure returns the transaction for an external id, creating it if this
// is its first use.
//
// Lazy creation is why there is no BeginTransaction RPC: the worker's UUID
// is generated locally and costs nothing, and a transaction that never
// acquires anything never appears here at all.
func (r *registry) ensure(external uuid.UUID) (*transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if txn, ok := r.byID[external]; ok {
		if txn.finished {
			return nil, fmt.Errorf("grpccoord: transaction %s has already ended (%s); "+
				"it no longer holds any locks", external, txn.state)
		}
		if txn.state != txnActive {
			return nil, fmt.Errorf("grpccoord: transaction %s is %s", external, txn.state)
		}
		return txn, nil
	}
	if r.spent || r.next == ^uint64(0) {
		r.spent = true
		return nil, errTxnIDsExhausted
	}
	r.next++
	txn := &transaction{
		external: external,
		internal: lockmanager.TxnID(r.next),
		state:    txnActive,
		pending:  make(map[uint64]context.CancelFunc),
	}
	r.byID[external] = txn
	return txn, nil
}

// lookup returns a transaction without creating one.
func (r *registry) lookup(external uuid.UUID) (*transaction, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	txn, ok := r.byID[external]
	return txn, ok
}

// finish marks a transaction's teardown complete, leaving a tombstone.
// It is the last step of both graceful and forced teardown, after fences
// are invalidated and locks are released.
func (r *registry) finish(external uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if txn, ok := r.byID[external]; ok {
		txn.finished = true
		txn.finishedAt = time.Now()
	}
}

// prune drops tombstones older than the retention window, so the table
// does not grow without bound.
//
// The window only has to outlast a worker that might still be holding a
// stale view of its transaction. Once it passes, the same UUID would be
// treated as new -- which is harmless, because a UUID is never reused.
func (r *registry) prune(now time.Time, retention time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, txn := range r.byID {
		if txn.finished && now.Sub(txn.finishedAt) > retention {
			delete(r.byID, id)
		}
	}
}

// startLeaseIfNeeded starts the lease clock on the first actual grant, and
// returns the current deadline.
//
// The lease starts when a lock is granted rather than when the transaction
// first appears, because until something is held there is nothing to
// reclaim -- and a transaction that spent a long time queued behind
// someone else should not be punished for the wait.
func (r *registry) startLeaseIfNeeded(txn *transaction) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !txn.leaseStarted {
		txn.leaseStarted = true
	}
	txn.leaseDeadline = time.Now().Add(r.leaseDuration)
	return txn.leaseDeadline
}

// renew extends a transaction's lease.
//
// It is idempotent: renewing repeatedly just moves the deadline. It
// refuses a transaction that is being revoked or ended, which is how a
// worker discovers it has lost its locks instead of carrying on.
func (r *registry) renew(external uuid.UUID) (time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	txn, ok := r.byID[external]
	if !ok {
		return time.Time{}, fmt.Errorf("grpccoord: transaction %s is not known", external)
	}
	if txn.finished {
		return time.Time{}, fmt.Errorf("grpccoord: transaction %s has already ended (%s)", external, txn.state)
	}
	if txn.state != txnActive {
		return time.Time{}, fmt.Errorf("grpccoord: transaction %s is %s", external, txn.state)
	}
	if !txn.leaseStarted {
		// Nothing granted yet, so there is no lease to move. Report the
		// zero deadline rather than inventing one.
		return time.Time{}, nil
	}
	txn.leaseDeadline = time.Now().Add(r.leaseDuration)
	return txn.leaseDeadline, nil
}

// recordGrant adds a granted lock to a transaction.
func (r *registry) recordGrant(txn *transaction, record grantRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	txn.grants = append(txn.grants, record)
}

// registerPending records an in-flight Acquire's cancel function and
// returns its id, so revocation can cancel it.
func (r *registry) registerPending(txn *transaction, cancel context.CancelFunc) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	txn.nextReq++
	id := txn.nextReq
	txn.pending[id] = cancel
	return id
}

func (r *registry) clearPending(txn *transaction, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(txn.pending, id)
}

// beginTeardown moves a transaction out of the active state and returns a
// snapshot of what has to be cleaned up.
//
// Marking first is the first step of the required revocation order: once a
// transaction is not active it accepts no new Acquire and no RenewLease,
// so nothing can be added to the set being cleaned up while cleanup runs.
//
// It returns ok=false if the transaction is unknown or teardown is already
// under way, so two sweepers -- or a sweeper and a graceful
// EndTransaction -- cannot both tear down the same transaction.
func (r *registry) beginTeardown(external uuid.UUID, state lifecycle) (txn *transaction, cancels []context.CancelFunc, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	txn, exists := r.byID[external]
	if !exists || txn.finished || txn.state != txnActive {
		return nil, nil, false
	}
	txn.state = state
	for id, cancel := range txn.pending {
		cancels = append(cancels, cancel)
		delete(txn.pending, id)
	}
	return txn, cancels, true
}

// expired returns the transactions whose leases have passed and which are
// therefore eligible for revocation.
//
// A passed deadline does not release anything by itself. It only makes a
// transaction a candidate; the coordinator then revokes it in a specific
// order, and only after durably invalidating its fences.
func (r *registry) expired(now time.Time) []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()

	var candidates []uuid.UUID
	for id, txn := range r.byID {
		if txn.finished || txn.state != txnActive || !txn.leaseStarted {
			continue
		}
		if now.After(txn.leaseDeadline) {
			candidates = append(candidates, id)
		}
	}
	return candidates
}

// counts reports table sizes for Health.
func (r *registry) counts() (active, revoking int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, txn := range r.byID {
		if txn.finished {
			// A tombstone is neither active nor being cleaned up.
			continue
		}
		if txn.state == txnRevoking {
			revoking++
		} else {
			active++
		}
	}
	return active, revoking
}
