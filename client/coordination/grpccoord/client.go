package grpccoord

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/coordination"
	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

// Environment variables a worker reads to find its coordinator. As with
// storage, there is no default address: an unset variable means "no remote
// coordinator configured", not "connect to some guessed host".
const (
	EnvAddress = "SAFER_COORDINATOR_ADDR"
	EnvTimeout = "SAFER_COORDINATOR_TIMEOUT"
)

// minRenewalInterval floors the renewal pace, so a very short lease
// cannot turn into a renewal storm.
const minRenewalInterval = 50 * time.Millisecond

// DefaultTimeout bounds connection and Health calls. It deliberately does
// NOT bound Acquire: a transaction may legitimately wait a long time
// behind another transaction's lock, and timing that out would turn
// ordinary contention into spurious failures.
const DefaultTimeout = 10 * time.Second

// Config describes how to reach the coordinator.
type Config struct {
	Address string
	Timeout time.Duration
	// RenewInterval is only the delay before the FIRST renewal, used
	// until the coordinator has told this worker a deadline. After that
	// the pace comes from the lease itself. Defaults to a third of
	// DefaultLeaseDuration.
	RenewInterval time.Duration
}

// ConfigFromEnv builds a Config from the environment. It reports
// configured=false, with no error, when the address is unset.
func ConfigFromEnv() (cfg Config, configured bool, err error) {
	address := os.Getenv(EnvAddress)
	if address == "" {
		return Config{}, false, nil
	}
	cfg = Config{Address: address, Timeout: DefaultTimeout}
	if raw := os.Getenv(EnvTimeout); raw != "" {
		d, parseErr := time.ParseDuration(raw)
		if parseErr != nil {
			return Config{}, false, fmt.Errorf("%s=%q: not a duration: %w", EnvTimeout, raw, parseErr)
		}
		if d <= 0 {
			return Config{}, false, fmt.Errorf("%s=%q: must be positive", EnvTimeout, raw)
		}
		cfg.Timeout = d
	}
	return cfg, true, nil
}

// Backend is the coordination backend a SAFER worker installs to reach a
// remote coordinator. It satisfies coordination.Backend, so SAFER's
// operations are unchanged by using it.
//
// Transport security: this dials insecurely. The deployment model for this
// phase is a coordinator and its workers on a trusted network (one
// machine, or one pod network). Note what that does and does not risk:
// SAFER's objects are encrypted and authenticated before they ever reach
// storage, so this channel carries no plaintext content and no key
// material -- only resource identifiers and lock modes. An attacker on
// this channel could still disrupt availability and correctness by forging
// lock traffic, so it is not safe to expose across an untrusted network as
// it stands.
//
// This is the LOWER-risk of the two insecure gRPC channels in this
// deployment. Contrast cmd/worker's worker.v1 surface (see
// proto/worker/v1/worker.proto's package doc), which carries plaintext
// usernames, passwords, and file content, since it sits before SAFER's
// encryption layer rather than after it -- reaching that channel is a
// meaningfully bigger problem than reaching this one.
type Backend struct {
	conn    *grpc.ClientConn
	client  coordinatorv1.LockCoordinatorClient
	timeout time.Duration
	// renewInterval is how often a transaction renews its lease.
	renewInterval time.Duration
}

var _ coordination.Backend = (*Backend)(nil)

// Dial connects to a coordinator and verifies it is serving, so that a
// misconfigured worker fails at startup rather than at its first
// operation.
func Dial(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("grpccoord: %s is required", EnvAddress)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}

	dialCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, cfg.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("grpccoord: dial %s: %w", cfg.Address, err)
	}

	renewInterval := cfg.RenewInterval
	if renewInterval <= 0 {
		renewInterval = DefaultLeaseDuration / 3
	}
	backend := &Backend{
		conn:          conn,
		client:        coordinatorv1.NewLockCoordinatorClient(conn),
		timeout:       cfg.Timeout,
		renewInterval: renewInterval,
	}
	if err := backend.Health(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return backend, nil
}

// Begin starts a transaction scope.
//
// The local transaction id is ignored: it is a per-process counter, so two
// workers would hand out the same values. The external identity is a UUID
// generated here instead, which is globally unique without a round trip.
// The coordinator maps it onto its own internal id and creates state
// lazily on the first Acquire, which is why there is no BeginTransaction
// RPC.
func (b *Backend) Begin(_ lockmanager.TxnID) (coordination.Guard, error) {
	return &remoteGuard{
		backend:       b,
		txn:           uuid.New(),
		stopHeartbeat: make(chan struct{}),
		lost:          make(chan struct{}),
	}, nil
}

// Health reports whether the coordinator is serving.
func (b *Backend) Health(ctx context.Context) error {
	healthCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	if _, err := b.client.Health(healthCtx, &coordinatorv1.HealthRequest{}); err != nil {
		return fmt.Errorf("grpccoord: health: %w", err)
	}
	return nil
}

// Close closes the connection. It does not release locks; transactions do
// that themselves via EndTransaction.
func (b *Backend) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Close()
}

// suspendHeartbeats, when set, makes every guard in this process stop
// renewing its lease without ending its transaction or dying.
//
// It exists for the stale-writer test, which needs a worker that is still
// running -- still holding computed state it intends to commit -- but
// whose lease is no longer being kept alive. Killing the process would
// test something else entirely, and a sleep would not be deterministic.
var suspendHeartbeats atomic.Bool

// SuspendHeartbeatsForTest stops lease renewal for every transaction in
// this process. Test-only.
func SuspendHeartbeatsForTest(suspended bool) {
	suspendHeartbeats.Store(suspended)
}

// remoteGuard is one transaction's lock scope against the coordinator.
type remoteGuard struct {
	backend *Backend
	txn     uuid.UUID

	mu sync.Mutex
	// acquired records whether this transaction ever asked for a lock.
	// A transaction that never acquired anything has no state on the
	// coordinator, so ending it would be a pointless round trip.
	acquired bool
	ended    bool
	// grants are the fencing tokens this transaction was issued, one per
	// exclusive lock. They are kept for the life of the transaction
	// because the storage commit has to prove every one of them: an
	// operation that took X on several resources loses the whole
	// mutation if it lost any single lock.
	grants []fencing.Grant

	// heartbeat renews the lease. It starts on the first successful
	// grant -- before that there is no lease, because nothing is held --
	// and stops when the transaction ends.
	heartbeatStarted bool
	stopHeartbeat    chan struct{}
	heartbeatDone    sync.WaitGroup
	// leaseExpires is the coordinator's authoritative deadline for this
	// transaction, as of the last response it sent. The renewal interval
	// is derived from it rather than configured locally: the coordinator
	// decides how long a lease lasts, and a worker guessing a longer
	// interval than the real lease would be revoked while perfectly
	// healthy.
	leaseExpires time.Time

	// lost is closed if the coordinator ever tells this transaction its
	// lease is gone, so the worker can find out it no longer holds its
	// locks.
	lostOnce sync.Once
	lost     chan struct{}
}

// noteLeaseDeadline records the coordinator's latest deadline.
func (g *remoteGuard) noteLeaseDeadline(unixNano int64) {
	if unixNano <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leaseExpires = time.Unix(0, unixNano)
}

// renewalDelay is how long to wait before the next renewal.
//
// It is a third of the time actually left on the lease, so two renewals
// can be lost -- to a GC pause, a slow network, a busy coordinator --
// before the coordinator considers the lease passed. It is clamped at the
// bottom so a very short lease cannot turn into a renewal storm, and
// falls back to a fraction of the default lease if the coordinator has
// not told us a deadline yet.
func (g *remoteGuard) renewalDelay() time.Duration {
	g.mu.Lock()
	deadline := g.leaseExpires
	g.mu.Unlock()

	if deadline.IsZero() {
		return DefaultLeaseDuration / 3
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		// Already past: renew immediately and let the coordinator say
		// whether the transaction survives.
		return minRenewalInterval
	}
	delay := remaining / 3
	if delay < minRenewalInterval {
		delay = minRenewalInterval
	}
	return delay
}

// Acquire blocks until the coordinator grants the lock.
//
// It is equivalent to AcquireContext(context.Background(), ...): kept for
// source compatibility with callers that predate context-aware
// acquisition. Prefer AcquireContext for anything driven by an external
// request (a gRPC handler, in particular), so a caller that goes away
// actually stops waiting instead of leaving this goroutine, and the
// coordinator's wait queue entry, parked until the transaction ends.
func (g *remoteGuard) Acquire(resource lockmanager.ResourceID, mode lockmanager.LockMode) error {
	return g.AcquireContext(context.Background(), resource, mode)
}

// AcquireContext is Acquire with cancellation and deadlines.
//
// The call still carries no independent client-side timeout of its own:
// waiting is the normal, correct outcome of contention under strict 2PL,
// and imposing one here would convert ordinary waiting into failure. What
// bounds the wait now is either the holder ending its transaction, or
// ctx being cancelled or expiring -- both are real ways for the wait to
// end.
//
// A cancelled wait leaves no grant behind: cancelling ctx before the
// gRPC call returns cancels the underlying Acquire RPC, which the
// coordinator's own AcquireContext (see client/lockmanager) uses to pull
// the request out of its wait queue rather than granting it to a caller
// no longer listening. If ctx is cancelled after the grant already
// arrived, the lock IS held -- this transaction acquired it fair and
// square -- and it is up to the caller (via guard.ReleaseAll, always run
// through a deferred call, never gated on ctx) to give it back.
func (g *remoteGuard) AcquireContext(ctx context.Context, resource lockmanager.ResourceID, mode lockmanager.LockMode) error {
	protoResource, err := resourceToProto(resource)
	if err != nil {
		return err
	}
	protoMode, err := modeToProto(mode)
	if err != nil {
		return err
	}

	g.mu.Lock()
	if g.ended {
		g.mu.Unlock()
		return fmt.Errorf("grpccoord: transaction %s has already ended", g.txn)
	}
	// Marked before the call, not after: if the response is lost, the
	// coordinator may still have granted the lock, and this transaction
	// must end regardless.
	g.acquired = true
	g.mu.Unlock()

	response, err := g.backend.client.Acquire(ctx, &coordinatorv1.AcquireRequest{
		TransactionId: g.txn.String(),
		Resource:      protoResource,
		Mode:          protoMode,
	})
	if err != nil {
		return fmt.Errorf("grpccoord: acquire %s on %v: %w", mode, resource, err)
	}

	g.noteLeaseDeadline(response.GetLeaseExpiresUnixNano())

	g.mu.Lock()
	if mode == lockmanager.ExclusiveLock && response.GetFencingToken() != 0 {
		g.grants = append(g.grants, fencing.Grant{
			Resource: fencing.ResourceKey(uint8(resource.Type), resource.Key),
			Token:    fencing.Token(response.GetFencingToken()),
			OwnerTxn: g.txn.String(),
		})
	}
	// The lease exists from the first grant onward, so renewal starts
	// here. Before it, the transaction holds nothing to keep alive.
	startHeartbeat := !g.heartbeatStarted && !g.ended
	if startHeartbeat {
		g.heartbeatStarted = true
		g.heartbeatDone.Add(1)
	}
	g.mu.Unlock()

	if startHeartbeat {
		go g.heartbeat()
	}
	return nil
}

// FenceGrants returns the fencing grants this transaction holds.
//
// The caller presents them to its storage transaction, which proves them
// at commit time. Returning a copy keeps a later acquisition from
// mutating a slice an in-flight commit is already validating.
func (g *remoteGuard) FenceGrants() []fencing.Grant {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.grants) == 0 {
		return nil
	}
	grants := make([]fencing.Grant, len(g.grants))
	copy(grants, g.grants)
	return grants
}

// Lost is closed if the coordinator reports that this transaction's lease
// is gone. A worker can watch it to stop early rather than doing work its
// commit will refuse.
func (g *remoteGuard) Lost() <-chan struct{} { return g.lost }

// heartbeat renews the lease until the transaction ends.
//
// The pace comes from the coordinator, not from local configuration: each
// response carries the authoritative deadline, and the next renewal is
// scheduled at a third of the time remaining. A locally configured
// interval longer than the real lease would get a perfectly healthy
// worker revoked, which a cross-process test caught doing exactly that.
func (g *remoteGuard) heartbeat() {
	defer g.heartbeatDone.Done()

	timer := time.NewTimer(g.renewalDelay())
	defer timer.Stop()

	for {
		select {
		case <-g.stopHeartbeat:
			return
		case <-timer.C:
		}

		if suspendHeartbeats.Load() {
			// Deliberately stalled: the lease will pass and the
			// coordinator will revoke, exactly as it would for a worker
			// that stopped responding.
			timer.Reset(minRenewalInterval)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), g.backend.timeout)
		response, err := g.backend.client.RenewLease(ctx, &coordinatorv1.RenewLeaseRequest{
			TransactionId: g.txn.String(),
		})
		cancel()

		if err == nil {
			g.noteLeaseDeadline(response.GetLeaseExpiresUnixNano())
			timer.Reset(g.renewalDelay())
			continue
		}
		if status.Code(err) == codes.FailedPrecondition {
			// The coordinator no longer recognizes this transaction as
			// active: its lease passed and its locks were taken away.
			// Renewing again cannot help, and fencing will refuse the
			// commit regardless.
			g.lostOnce.Do(func() { close(g.lost) })
			fmt.Fprintf(os.Stderr,
				"grpccoord: transaction %s lost its lease: %v\n", g.txn, err)
			return
		}
		// A transient failure. Keep trying, sooner rather than later:
		// the lease may still be alive, and giving up would guarantee
		// losing it.
		fmt.Fprintf(os.Stderr,
			"grpccoord: renewing the lease for %s failed, will retry: %v\n", g.txn, err)
		timer.Reset(minRenewalInterval)
	}
}

// ReleaseAll ends the transaction, releasing every lock it holds.
//
// It runs on every path, including after a failed Acquire, because a
// failure can mean "the grant happened but the response was lost". Ending
// an unknown transaction is harmless on the coordinator, whereas skipping
// it would leak a lock.
//
// It is best-effort by necessity: the interface returns nothing, and a
// deferred cleanup has nowhere to report to. When the call fails, the lock
// stays held on the coordinator until it restarts -- which is exactly the
// gap leases and fencing tokens close in the next phase.
func (g *remoteGuard) ReleaseAll() {
	g.mu.Lock()
	if g.ended {
		g.mu.Unlock()
		return
	}
	g.ended = true
	acquired := g.acquired
	started := g.heartbeatStarted
	g.mu.Unlock()

	// Stop renewing before ending: a renewal racing an EndTransaction
	// would only fail noisily.
	if started {
		close(g.stopHeartbeat)
		g.heartbeatDone.Wait()
	}

	if !acquired {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.backend.timeout)
	defer cancel()

	if _, err := g.backend.client.EndTransaction(ctx, &coordinatorv1.EndTransactionRequest{
		TransactionId: g.txn.String(),
	}); err != nil {
		// Nowhere to return this. Make it visible rather than silent: a
		// failed EndTransaction means locks are still held remotely --
		// though unlike before Phase 3C, no longer held forever, since
		// the lease expires and the coordinator reclaims them.
		fmt.Fprintf(os.Stderr,
			"grpccoord: EndTransaction for %s failed; its lease will expire and the coordinator will reclaim: %v\n",
			g.txn, err)
	}
}

// TransactionID exposes the external transaction identity, for tests and
// diagnostics.
func (g *remoteGuard) TransactionID() uuid.UUID { return g.txn }
