// Package grpccoord implements the SAFER lock coordinator: a gRPC server
// that serves strict-2PL lock decisions for multiple SAFER worker
// processes, and the client backend workers use to reach it.
//
// The server contains no locking logic of its own. Every decision is made
// by an ordinary *lockmanager.LockManager -- the same generic core SAFER-CC
// has always used, with its S/X compatibility rules, FIFO fairness, and
// holder/waiter bookkeeping unchanged. This package translates wire types,
// maps externally-generated transaction UUIDs onto the manager's internal
// uint64 TxnIDs, keeps the lease and fencing state the manager has no
// opinion about, and makes sure a caller that disappears leaves no request
// queued.
//
// Worker failure (Phase 3C). A transaction's lease starts when its first
// lock is granted, and the worker renews it while it works. A passed
// deadline does not release anything by itself; it makes the transaction
// eligible for revocation, which happens in a fixed order:
//
//  1. mark the transaction revoking, so it accepts no further Acquire or
//     RenewLease
//  2. cancel its in-flight Acquire requests
//  3. durably invalidate every exclusive fence it owns
//  4. only then release its locks through the LockManager
//  5. remove its state
//
// Step 3 before step 4 is the safety property. Releasing locks first would
// let the next holder start writing while the previous holder's fencing
// token was still valid, which is exactly the stale write fencing exists
// to stop. If the fence store is unreachable, the locks STAY HELD and
// cleanup retries: an availability loss is preferable to an unsafe
// handoff.
//
// Scope and limits, stated plainly:
//
//   - There is exactly one coordinator process. It is not replicated, and
//     there is no consensus of any kind. It remains an explicit failure
//     domain: if it dies, its in-memory lock state dies with it.
//   - Fence state is durable, but lock state is not. A coordinator restart
//     loses who held what.
//
// So this earns worker crash recovery, bounded lock reclamation, and
// stale-writer protection. It does not earn coordinator fault tolerance.
package grpccoord

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

// tracer and meter are unconditional and safe with no telemetry backend
// configured (see internal/telemetry's package doc): with no real
// TracerProvider/MeterProvider installed, otel.Tracer/otel.Meter return
// no-op implementations.
var (
	tracer = otel.Tracer("github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord")
	meter  = otel.Meter("github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord")

	// lockAcquireCount and lockWaitDuration answer "is lock contention the
	// bottleneck?" (Phase 5's metrics requirement A). Attributes are
	// mode, resource type, and outcome only -- never a resource key
	// (a namespace or file identifier), which would be unbounded
	// cardinality for no analytical benefit a low-cardinality label
	// does not already provide.
	lockAcquireCount, _ = meter.Int64Counter(
		"safer.coordinator.lock.acquire.count",
		otelmetric.WithDescription("Number of Acquire calls that actually waited on the LockManager, by outcome."),
	)
	lockWaitDuration, _ = meter.Float64Histogram(
		"safer.coordinator.lock.wait.duration",
		otelmetric.WithDescription("Time spent waiting inside LockManager.AcquireContext for a lock, by outcome."),
		otelmetric.WithUnit("s"),
	)

	// leaseRevocationCount and cleanupRetryCount/cleanupFailureCount
	// answer "are leases/fencing entering recovery paths?" (requirement
	// C). A healthy deployment should see these stay at zero; any
	// nonzero rate means workers are dying or stalling often enough for
	// it to matter.
	leaseRevocationCount = mustInt64Counter(meter, "safer.coordinator.lease.revocation.count",
		"Number of transactions revoked because their lease passed without renewal.")
	cleanupRetryCount = mustInt64Counter(meter, "safer.coordinator.fence.cleanup.retry.count",
		"Number of times fence-invalidation cleanup was retried after a failure.")
	cleanupFailureCount = mustInt64Counter(meter, "safer.coordinator.fence.cleanup.failure.count",
		"Number of times fence-invalidation cleanup failed and locks were kept held on purpose.")
)

func mustInt64Counter(m otelmetric.Meter, name, description string) otelmetric.Int64Counter {
	c, _ := m.Int64Counter(name, otelmetric.WithDescription(description))
	return c
}

// resourceTypeLabel is the low-cardinality attribute value for a lock
// resource's type -- never the resource's own key, which would be
// unbounded (one value per namespace/file in the whole deployment).
func resourceTypeLabel(t lockmanager.ResourceType) string {
	switch t {
	case lockmanager.NamespaceResource:
		return "namespace"
	case lockmanager.FileResource:
		return "file"
	default:
		return "unknown"
	}
}

// FenceStore is the durable fencing metadata the coordinator depends on.
// *mongofence.Store implements it; tests substitute failing versions.
type FenceStore interface {
	// Allocate advances a resource's token and records the new owner.
	Allocate(ctx context.Context, resource string, ownerTxn string) (fencing.Token, error)
	// Invalidate clears a transaction's ownership of a resource's fence.
	Invalidate(ctx context.Context, resource string, ownerTxn string) error
}

// Defaults for lease timing.
const (
	// DefaultLeaseDuration is how long a grant survives without a
	// renewal. Long enough to absorb an ordinary GC pause or a slow
	// storage call, short enough that a dead worker's locks come back in
	// seconds rather than minutes.
	DefaultLeaseDuration = 6 * time.Second
	// DefaultSweepInterval is how often expired leases are looked for.
	DefaultSweepInterval = 500 * time.Millisecond
	// DefaultCleanupRetryInterval is how long to wait before retrying a
	// revocation whose fence invalidation failed.
	DefaultCleanupRetryInterval = time.Second
)

// tombstoneRetention is how long a finished transaction's record is kept
// so that a returning worker is told its transaction is over rather than
// silently given a fresh one. It only has to outlast a worker that may
// still believe it holds locks.
func tombstoneRetention(lease time.Duration) time.Duration {
	retention := 10 * lease
	if retention < 5*time.Second {
		retention = 5 * time.Second
	}
	if retention > 5*time.Minute {
		retention = 5 * time.Minute
	}
	return retention
}

// ServerConfig configures a coordinator.
type ServerConfig struct {
	// LockManager is the generic core. Nil creates a fresh one.
	LockManager *lockmanager.LockManager
	// Fences is the durable fence store. Nil disables fencing entirely,
	// which is only appropriate for tests that exercise lock semantics
	// alone -- a coordinator with no fence store cannot protect against
	// stale writers.
	Fences FenceStore

	LeaseDuration        time.Duration
	SweepInterval        time.Duration
	CleanupRetryInterval time.Duration

	// Logf receives cleanup failures. Nil uses the standard logger.
	Logf func(format string, args ...interface{})
}

// Server is the lock coordinator.
type Server struct {
	coordinatorv1.UnimplementedLockCoordinatorServer

	lm       *lockmanager.LockManager
	fences   FenceStore
	registry *registry

	leaseDuration        time.Duration
	sweepInterval        time.Duration
	cleanupRetryInterval time.Duration
	tombstoneRetention   time.Duration
	logf                 func(format string, args ...interface{})

	// retryMu guards the set of transactions whose cleanup failed and is
	// being retried, so a sweeper tick cannot start a second cleanup for
	// one already in progress.
	retryMu  sync.Mutex
	retrying map[uuid.UUID]bool

	stopOnce sync.Once
	stop     chan struct{}
	stopped  sync.WaitGroup
}

// NewServer builds a coordinator with default lease timing and no fence
// store. Prefer NewServerWithConfig for anything that needs fencing.
func NewServer(lm *lockmanager.LockManager) *Server {
	return NewServerWithConfig(ServerConfig{LockManager: lm})
}

// NewServerWithConfig builds a coordinator and starts its lease sweeper.
func NewServerWithConfig(cfg ServerConfig) *Server {
	if cfg.LockManager == nil {
		cfg.LockManager = lockmanager.NewLockManager()
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = DefaultLeaseDuration
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.CleanupRetryInterval <= 0 {
		cfg.CleanupRetryInterval = DefaultCleanupRetryInterval
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}

	s := &Server{
		lm:                   cfg.LockManager,
		fences:               cfg.Fences,
		registry:             newRegistry(cfg.LeaseDuration),
		leaseDuration:        cfg.LeaseDuration,
		sweepInterval:        cfg.SweepInterval,
		cleanupRetryInterval: cfg.CleanupRetryInterval,
		tombstoneRetention:   tombstoneRetention(cfg.LeaseDuration),
		logf:                 cfg.Logf,
		retrying:             make(map[uuid.UUID]bool),
		stop:                 make(chan struct{}),
	}
	s.stopped.Add(1)
	go s.sweepLeases()
	return s
}

// Stop halts the lease sweeper. In-flight cleanups are allowed to finish.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.stopped.Wait()
}

// LockManager exposes the underlying manager for tests and diagnostics.
func (s *Server) LockManager() *lockmanager.LockManager { return s.lm }

// LeaseDuration reports the configured lease length, so a worker can pick
// a renewal interval from it.
func (s *Server) LeaseDuration() time.Duration { return s.leaseDuration }

// Acquire blocks until the transaction holds the requested mode.
//
// It is retry-safe. An identical request for a resource this transaction
// already holds returns the existing grant and the very same fencing
// token, without touching the LockManager and without allocating a second
// token, so a retry after a lost response recovers the original outcome
// rather than inventing a new grant. A request for a DIFFERENT mode on a
// held resource is still rejected: there are no upgrades and no
// reentrancy, exactly as before.
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

	txn, err := s.registry.ensure(external)
	if err != nil {
		if errors.Is(err, errTxnIDsExhausted) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		// The transaction is being revoked or ended: it must not be able
		// to take more locks.
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// Idempotent retry. Checked before going near the LockManager, which
	// would otherwise reject the second request as reentrant acquisition.
	if existing, held := s.heldGrant(txn, resource); held {
		if existing.mode != mode {
			return nil, status.Errorf(codes.FailedPrecondition,
				"grpccoord: transaction %s already holds %s on %v; upgrades are not supported",
				external, existing.mode, resource)
		}
		return &coordinatorv1.AcquireResponse{
			FencingToken:         uint64(existing.token),
			LeaseExpiresUnixNano: s.leaseDeadline(txn).UnixNano(),
		}, nil
	}

	// The wait is cancellable, and its cancel function is registered so
	// revocation can pull the request out of the queue. Registering takes
	// the table mutex briefly; the wait itself happens with no
	// coordinator lock held, or the EndTransaction that would unblock it
	// could never run.
	//
	// This span and the metrics recorded alongside it exist to answer
	// "is lock contention the bottleneck?" (Phase 5's metrics requirement
	// A): attributes are the lock mode, the resource's TYPE, and the
	// outcome -- never the resource's own key (a namespace or file
	// identifier), which is exactly the kind of unbounded-cardinality
	// label that would make this expensive to store and useless to
	// aggregate on.
	waitCtx, span := tracer.Start(ctx, "coordinator.lock_wait", trace.WithAttributes(
		attribute.String("lock.mode", mode.String()),
		attribute.String("resource.type", resourceTypeLabel(resource.Type)),
	))
	waitStart := time.Now()
	waitCtx, cancel := context.WithCancel(waitCtx)
	requestID := s.registry.registerPending(txn, cancel)
	err = s.lm.AcquireContext(waitCtx, txn.internal, resource, mode)
	s.registry.clearPending(txn, requestID)
	cancel()

	recordLockWait := func(outcome string) {
		attrs := otelmetric.WithAttributes(
			attribute.String("lock.mode", mode.String()),
			attribute.String("resource.type", resourceTypeLabel(resource.Type)),
			attribute.String("outcome", outcome),
		)
		lockAcquireCount.Add(ctx, 1, attrs)
		lockWaitDuration.Record(ctx, time.Since(waitStart).Seconds(), attrs)
		span.SetAttributes(attribute.String("lock.outcome", outcome))
		span.End()
	}

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The caller went away. The request left the queue; nothing
			// is held. The worker still calls EndTransaction.
			recordLockWait("caller_cancelled")
			return nil, status.FromContextError(ctxErr).Err()
		}
		if waitCtx.Err() != nil {
			// Cancelled by revocation rather than by the caller.
			recordLockWait("revoked")
			return nil, status.Errorf(codes.Aborted,
				"grpccoord: transaction %s was revoked while waiting for %v", external, resource)
		}
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		recordLockWait("failed")
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	recordLockWait("granted")

	// Granted. From here, any failure must give the lock back rather than
	// leave it held by a transaction the worker does not know succeeded.
	if !s.stillActive(external, txn) {
		s.releaseOne(txn, resource)
		return nil, status.Error(codes.Aborted, "grpccoord: transaction ended while the request was queued")
	}

	var token fencing.Token
	if mode == lockmanager.ExclusiveLock && s.fences != nil {
		// Fail closed: an exclusive grant is only handed out once its
		// fencing token is durable. Returning an unfenced X grant would
		// create a writer nothing could later stop.
		token, err = s.fences.Allocate(ctx,
			fencing.ResourceKey(uint8(resource.Type), resource.Key), external.String())
		if err != nil {
			s.releaseOne(txn, resource)
			return nil, status.Errorf(codes.Unavailable,
				"grpccoord: could not allocate a fencing token for %v, refusing the grant: %v", resource, err)
		}
	}

	s.registry.recordGrant(txn, grantRecord{resource: resource, mode: mode, token: token})
	deadline := s.registry.startLeaseIfNeeded(txn)

	return &coordinatorv1.AcquireResponse{
		FencingToken:         uint64(token),
		LeaseExpiresUnixNano: deadline.UnixNano(),
	}, nil
}

// heldGrant reports an existing grant for a resource under the table lock.
func (s *Server) heldGrant(txn *transaction, resource lockmanager.ResourceID) (grantRecord, bool) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return txn.grantFor(resource)
}

func (s *Server) leaseDeadline(txn *transaction) time.Time {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return txn.leaseDeadline
}

// stillActive reports whether external still maps to this exact
// transaction and is still accepting work.
func (s *Server) stillActive(external uuid.UUID, txn *transaction) bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	current, ok := s.registry.byID[external]
	return ok && current == txn && txn.state == txnActive
}

// releaseOne gives back a single lock that was granted but could not be
// completed. It is not a protocol operation -- there is no per-resource
// Release on the wire -- only an internal undo of a grant the caller never
// learned about.
func (s *Server) releaseOne(txn *transaction, resource lockmanager.ResourceID) {
	if err := s.lm.Release(txn.internal, resource); err != nil {
		s.logf("grpccoord: undoing a grant of %v for %s: %v", resource, txn.external, err)
	}
}

// RenewLease extends a transaction's lease.
//
// It is idempotent: renewing repeatedly just moves the deadline. It fails
// once the transaction is revoking or gone, which is how a worker learns
// it has lost its locks instead of continuing to believe it holds them.
func (s *Server) RenewLease(ctx context.Context, req *coordinatorv1.RenewLeaseRequest) (*coordinatorv1.RenewLeaseResponse, error) {
	external, err := parseTxnID(req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	deadline, err := s.registry.renew(external)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &coordinatorv1.RenewLeaseResponse{LeaseExpiresUnixNano: deadline.UnixNano()}, nil
}

// EndTransaction releases every lock the transaction holds.
//
// This is the only way locks are released, which is what makes the
// protocol strict 2PL. It is idempotent: ending an unknown transaction
// succeeds and reports zero released, so a worker can call it on every
// path including after an Acquire whose response never arrived.
//
// It invalidates the transaction's fences BEFORE releasing its locks, for
// the same reason revocation does: once the next holder can start writing,
// this transaction's token must already be dead.
func (s *Server) EndTransaction(ctx context.Context, req *coordinatorv1.EndTransactionRequest) (*coordinatorv1.EndTransactionResponse, error) {
	external, err := parseTxnID(req.GetTransactionId())
	if err != nil {
		return nil, err
	}

	txn, cancels, ok := s.registry.beginTeardown(external, txnEnding)
	if !ok {
		// Unknown, or already being torn down. Either way there is
		// nothing for this call to do.
		return &coordinatorv1.EndTransactionResponse{Released: 0}, nil
	}
	for _, cancel := range cancels {
		cancel()
	}

	released, err := s.teardown(ctx, txn)
	if err != nil {
		// The locks are deliberately still held; cleanup will retry.
		s.scheduleRetry(txn)
		return nil, status.Errorf(codes.Unavailable,
			"grpccoord: could not invalidate fencing state for %s, locks are still held and cleanup will retry: %v",
			external, err)
	}
	return &coordinatorv1.EndTransactionResponse{Released: released}, nil
}

// teardown invalidates a transaction's fences and then releases its locks.
//
// The order is the safety property of this phase. Fence invalidation must
// be durable before the locks move, because the instant they move another
// worker may begin writing, and this transaction's token must already be
// unusable by then.
//
// A fence-store failure returns an error WITHOUT releasing anything. The
// locks stay held on purpose: an unavailable resource is a much smaller
// problem than two writers who both believe they hold it.
func (s *Server) teardown(ctx context.Context, txn *transaction) (_ uint32, err error) {
	// One span per teardown attempt (there can be several, via
	// scheduleRetry, for one transaction) -- "lease revocation / fence
	// cleanup" from Phase 5's list of useful coordinator spans.
	// grants_count is a count, not an identifier, so it stays
	// low-cardinality even though it varies per call.
	ctx, span := tracer.Start(ctx, "coordinator.teardown")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
		span.End()
	}()

	grants := s.fenceGrantsOf(txn)
	span.SetAttributes(attribute.Int("grants_count", len(grants)))
	if s.fences != nil {
		for _, grant := range grants {
			if err := s.fences.Invalidate(ctx, grant.Resource, grant.OwnerTxn); err != nil {
				return 0, fmt.Errorf("invalidating %s: %w", grant, err)
			}
		}
	}

	released := uint32(s.lm.HeldCount(txn.internal))
	if err := s.lm.ReleaseAll(txn.internal); err != nil {
		// Bookkeeping inconsistency rather than a storage failure; the
		// transaction's state still has to go, or it would be retried
		// forever.
		s.logf("grpccoord: releasing locks for %s: %v", txn.external, err)
	}
	s.registry.finish(txn.external)
	return released, nil
}

func (s *Server) fenceGrantsOf(txn *transaction) []fencing.Grant {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return txn.fenceGrants()
}

// sweepLeases revokes transactions whose leases have passed.
func (s *Server) sweepLeases() {
	defer s.stopped.Done()

	ticker := time.NewTicker(s.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			now := time.Now()
			for _, external := range s.registry.expired(now) {
				s.revoke(external)
			}
			s.registry.prune(now, s.tombstoneRetention)
		}
	}
}

// revoke performs the ordered teardown of a transaction whose lease has
// passed.
func (s *Server) revoke(external uuid.UUID) {
	// 1. Mark it revoking. From here it accepts no Acquire and no
	//    RenewLease, so a worker that comes back learns it lost its
	//    locks, and nothing can be added while cleanup runs.
	txn, cancels, ok := s.registry.beginTeardown(external, txnRevoking)
	if !ok {
		return
	}
	s.logf("grpccoord: lease expired for transaction %s, revoking", external)
	leaseRevocationCount.Add(context.Background(), 1)

	// 2. Cancel its in-flight Acquire requests, so a queued request
	//    cannot be granted to a transaction being torn down.
	for _, cancel := range cancels {
		cancel()
	}

	// 3 and 4: invalidate fences durably, and only then release locks.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := s.teardown(ctx, txn); err != nil {
		s.logf("grpccoord: could not invalidate fencing state for %s; "+
			"KEEPING ITS LOCKS HELD and retrying: %v", external, err)
		cleanupFailureCount.Add(context.Background(), 1)
		s.scheduleRetry(txn)
		return
	}
	s.logf("grpccoord: transaction %s revoked and its locks released", external)
}

// scheduleRetry keeps retrying a teardown whose fence invalidation failed.
//
// The transaction stays in the revoking state throughout, so its locks
// remain held and no one else can take them. This is the deliberate trade:
// SAFER would rather block on a resource than hand it to a second writer
// while the first one's fencing token is still live.
func (s *Server) scheduleRetry(txn *transaction) {
	s.retryMu.Lock()
	if s.retrying[txn.external] {
		s.retryMu.Unlock()
		return
	}
	s.retrying[txn.external] = true
	s.retryMu.Unlock()

	s.stopped.Add(1)
	go func() {
		defer s.stopped.Done()
		defer func() {
			s.retryMu.Lock()
			delete(s.retrying, txn.external)
			s.retryMu.Unlock()
		}()

		ticker := time.NewTicker(s.cleanupRetryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stop:
				s.logf("grpccoord: stopping with transaction %s still uncleaned; "+
					"its locks were never handed over", txn.external)
				return
			case <-ticker.C:
				cleanupRetryCount.Add(context.Background(), 1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := s.teardown(ctx, txn)
				cancel()
				if err == nil {
					s.logf("grpccoord: cleanup for %s succeeded on retry; locks released", txn.external)
					return
				}
				cleanupFailureCount.Add(context.Background(), 1)
				s.logf("grpccoord: cleanup for %s still failing, locks remain held: %v", txn.external, err)
			}
		}
	}()
}

// Health reports that the coordinator is serving, with a point-in-time
// sample of how much state it holds.
//
// RevokingTransactions is the operationally important one: a number that
// stays above zero means fence invalidation is failing and locks are being
// held on purpose rather than leaked by accident.
func (s *Server) Health(ctx context.Context, _ *coordinatorv1.HealthRequest) (*coordinatorv1.HealthResponse, error) {
	active, revoking := s.registry.counts()
	return &coordinatorv1.HealthResponse{
		ActiveTransactions:   uint32(active),
		HeldLocks:            uint32(s.lm.TotalHeldCount()),
		RevokingTransactions: uint32(revoking),
	}, nil
}

// ActiveTransactions reports how many transactions the coordinator is
// tracking. Diagnostic only.
func (s *Server) ActiveTransactions() int {
	active, revoking := s.registry.counts()
	return active + revoking
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
