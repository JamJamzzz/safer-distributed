// Package grpccoord implements the SAFER lock coordinator: a gRPC server
// that serves strict-2PL lock decisions for multiple SAFER worker
// processes, and the client backend workers use to reach it.
//
// The server contains no locking logic of its own. Every decision is made
// by an ordinary *lockmanager.LockManager -- the same generic core SAFER-CC
// has always used, with its S/X compatibility rules, FIFO fairness, and
// holder/waiter bookkeeping unchanged. This package only does three
// things: translate the wire types, map externally-generated transaction
// UUIDs onto the manager's internal uint64 TxnIDs, and make sure a caller
// that disappears does not leave a request queued.
//
// Scope and limits of this phase, stated plainly:
//
//   - There is exactly one coordinator process. It is not replicated, and
//     there is no consensus of any kind.
//   - Lock state is in memory. If the coordinator dies, that state is
//     gone; workers holding locks will not be told, and correctness is
//     lost until everything restarts.
//   - There are no leases and no fencing tokens. A worker that crashes
//     without calling EndTransaction leaks its locks until the
//     coordinator restarts.
//
// So this earns a cross-process coordination claim under normal, graceful
// operation. It earns no fault-tolerance claim.
package grpccoord

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

// Server is the lock coordinator.
type Server struct {
	coordinatorv1.UnimplementedLockCoordinatorServer

	lm *lockmanager.LockManager

	// mu protects the transaction table only. It is never held across an
	// Acquire wait: a blocking acquisition would otherwise stall every
	// other transaction's bookkeeping, including the EndTransaction that
	// would have unblocked it.
	mu sync.Mutex
	// txns maps a worker-generated transaction UUID to the internal
	// lock-manager id. The external identity must be globally unique
	// across workers, which a process-local counter is not; the internal
	// uint64 is unchanged, so the existing LockManager is reused exactly
	// as it is.
	txns      map[uuid.UUID]lockmanager.TxnID
	nextTxn   uint64
	exhausted bool
}

// NewServer builds a coordinator over the given lock manager. Passing an
// existing manager keeps this package free of locking logic; passing nil
// creates a fresh one.
func NewServer(lm *lockmanager.LockManager) *Server {
	if lm == nil {
		lm = lockmanager.NewLockManager()
	}
	return &Server{
		lm:   lm,
		txns: make(map[uuid.UUID]lockmanager.TxnID),
	}
}

// LockManager exposes the underlying manager for tests and diagnostics.
func (s *Server) LockManager() *lockmanager.LockManager { return s.lm }

// internalTxn returns the lock-manager id for an external transaction
// UUID, creating it on first use.
//
// Lazy creation is why there is no BeginTransaction RPC: the worker
// generates its own UUID locally, and the coordinator materializes state
// when the transaction first needs a lock. A transaction that never
// acquires anything costs the coordinator nothing.
func (s *Server) internalTxn(external uuid.UUID) (lockmanager.TxnID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.txns[external]; ok {
		return id, nil
	}
	if s.exhausted || s.nextTxn == math.MaxUint64 {
		// Refuse permanently rather than wrapping to 0, which is the
		// lock manager's reserved invalid id, or recycling an id a live
		// transaction still holds locks under.
		s.exhausted = true
		return 0, errors.New("grpccoord: transaction ID space exhausted")
	}
	s.nextTxn++
	id := lockmanager.TxnID(s.nextTxn)
	s.txns[external] = id
	return id, nil
}

// stillLive reports whether external is still the transaction that owns
// internal id txn -- that is, whether EndTransaction has run in the
// meantime.
func (s *Server) stillLive(external uuid.UUID, txn lockmanager.TxnID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.txns[external]
	return ok && current == txn
}

// Acquire blocks until the transaction holds the requested mode.
//
// It uses AcquireContext, so a caller that disconnects or gives up has its
// request removed from the wait queue instead of being granted a lock
// nobody will release.
func (s *Server) Acquire(ctx context.Context, req *coordinatorv1.AcquireRequest) (*coordinatorv1.AcquireResponse, error) {
	external, err := parseTxnID(req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	resource, err := resourceFromProto(req.GetResource())
	if err != nil {
		return nil, err
	}
	mode, err := modeFromProto(req.GetMode())
	if err != nil {
		return nil, err
	}

	txn, err := s.internalTxn(external)
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}

	if err := s.lm.AcquireContext(ctx, txn, resource, mode); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The caller went away. The request left the queue; nothing
			// is held. The worker will still call EndTransaction.
			return nil, status.FromContextError(ctxErr).Err()
		}
		// A programming error from the caller: re-acquiring a resource
		// this transaction already holds, or an invalid mode.
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// The transaction may have ended while this request sat in the queue.
	// Releasing on EndTransaction alone would not have covered this: the
	// request was not yet granted then, so there was nothing to release,
	// and the grant that arrives afterwards would be held by a
	// transaction nobody will ever end -- a ghost lock. Undo it here
	// instead of handing back a lock that outlives its transaction.
	if !s.stillLive(external, txn) {
		if releaseErr := s.lm.ReleaseAll(txn); releaseErr != nil {
			return nil, status.Errorf(codes.Internal,
				"grpccoord: releasing a lock granted after its transaction ended: %v", releaseErr)
		}
		return nil, status.Error(codes.Aborted, "grpccoord: transaction ended while the request was queued")
	}
	return &coordinatorv1.AcquireResponse{}, nil
}

// EndTransaction releases every lock the transaction holds.
//
// This is the only way locks are released, which is what makes the
// protocol strict 2PL rather than something weaker. It is idempotent:
// ending a transaction the coordinator has never heard of succeeds and
// reports zero released, so a worker can safely call it on every path,
// including after an Acquire whose response never arrived.
func (s *Server) EndTransaction(ctx context.Context, req *coordinatorv1.EndTransactionRequest) (*coordinatorv1.EndTransactionResponse, error) {
	external, err := parseTxnID(req.GetTransactionId())
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	txn, known := s.txns[external]
	if known {
		delete(s.txns, external)
	}
	s.mu.Unlock()

	if !known {
		return &coordinatorv1.EndTransactionResponse{Released: 0}, nil
	}

	released := uint32(s.lm.HeldCount(txn))
	if err := s.lm.ReleaseAll(txn); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("grpccoord: releasing transaction: %v", err))
	}
	return &coordinatorv1.EndTransactionResponse{Released: released}, nil
}

// Health reports that the coordinator is serving, with a point-in-time
// sample of how much state it holds.
func (s *Server) Health(ctx context.Context, _ *coordinatorv1.HealthRequest) (*coordinatorv1.HealthResponse, error) {
	s.mu.Lock()
	active := len(s.txns)
	s.mu.Unlock()

	return &coordinatorv1.HealthResponse{
		ActiveTransactions: uint32(active),
		HeldLocks:          uint32(s.lm.TotalHeldCount()),
	}, nil
}

// ActiveTransactions reports how many transactions the coordinator is
// tracking. Diagnostic only.
func (s *Server) ActiveTransactions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.txns)
}

func parseTxnID(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, status.Error(codes.InvalidArgument, "grpccoord: transaction_id is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "grpccoord: transaction_id %q is not a UUID", raw)
	}
	if id == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "grpccoord: transaction_id must not be the nil UUID")
	}
	return id, nil
}

func resourceFromProto(r *coordinatorv1.ResourceId) (lockmanager.ResourceID, error) {
	if r == nil {
		return lockmanager.ResourceID{}, status.Error(codes.InvalidArgument, "grpccoord: resource is required")
	}
	if r.GetKey() == "" {
		return lockmanager.ResourceID{}, status.Error(codes.InvalidArgument, "grpccoord: resource key is required")
	}
	switch r.GetType() {
	case coordinatorv1.ResourceType_RESOURCE_TYPE_NAMESPACE:
		return lockmanager.ResourceID{Type: lockmanager.NamespaceResource, Key: r.GetKey()}, nil
	case coordinatorv1.ResourceType_RESOURCE_TYPE_FILE:
		return lockmanager.ResourceID{Type: lockmanager.FileResource, Key: r.GetKey()}, nil
	default:
		// An unset or unknown type is rejected rather than defaulted: two
		// different resource types must never collapse onto one lock.
		return lockmanager.ResourceID{}, status.Errorf(codes.InvalidArgument,
			"grpccoord: unknown resource type %v", r.GetType())
	}
}

func resourceToProto(r lockmanager.ResourceID) (*coordinatorv1.ResourceId, error) {
	switch r.Type {
	case lockmanager.NamespaceResource:
		return &coordinatorv1.ResourceId{Type: coordinatorv1.ResourceType_RESOURCE_TYPE_NAMESPACE, Key: r.Key}, nil
	case lockmanager.FileResource:
		return &coordinatorv1.ResourceId{Type: coordinatorv1.ResourceType_RESOURCE_TYPE_FILE, Key: r.Key}, nil
	default:
		return nil, fmt.Errorf("grpccoord: unknown resource type %v", r.Type)
	}
}

func modeFromProto(m coordinatorv1.LockMode) (lockmanager.LockMode, error) {
	switch m {
	case coordinatorv1.LockMode_LOCK_MODE_SHARED:
		return lockmanager.SharedLock, nil
	case coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE:
		return lockmanager.ExclusiveLock, nil
	default:
		// Never default an unset mode to shared: that would silently turn
		// a writer into a reader and lose mutual exclusion.
		return 0, status.Errorf(codes.InvalidArgument, "grpccoord: unknown lock mode %v", m)
	}
}

func modeToProto(m lockmanager.LockMode) (coordinatorv1.LockMode, error) {
	switch m {
	case lockmanager.SharedLock:
		return coordinatorv1.LockMode_LOCK_MODE_SHARED, nil
	case lockmanager.ExclusiveLock:
		return coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE, nil
	default:
		return coordinatorv1.LockMode_LOCK_MODE_UNSPECIFIED, fmt.Errorf("grpccoord: unknown lock mode %v", m)
	}
}
