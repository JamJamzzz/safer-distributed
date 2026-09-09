package grpccoord

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/JamJamzzz/safer-distributed/client/coordination"
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

// DefaultTimeout bounds connection and Health calls. It deliberately does
// NOT bound Acquire: a transaction may legitimately wait a long time
// behind another transaction's lock, and timing that out would turn
// ordinary contention into spurious failures.
const DefaultTimeout = 10 * time.Second

// Config describes how to reach the coordinator.
type Config struct {
	Address string
	Timeout time.Duration
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
type Backend struct {
	conn    *grpc.ClientConn
	client  coordinatorv1.LockCoordinatorClient
	timeout time.Duration
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

	backend := &Backend{
		conn:    conn,
		client:  coordinatorv1.NewLockCoordinatorClient(conn),
		timeout: cfg.Timeout,
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
	return &remoteGuard{backend: b, txn: uuid.New()}, nil
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

// remoteGuard is one transaction's lock scope against the coordinator.
type remoteGuard struct {
	backend *Backend
	txn     uuid.UUID

	// acquired records whether this transaction ever asked for a lock.
	// A transaction that never acquired anything has no state on the
	// coordinator, so ending it would be a pointless round trip.
	mu       sync.Mutex
	acquired bool
	ended    bool
}

// Acquire blocks until the coordinator grants the lock.
//
// The call carries no client-side deadline: waiting is the normal,
// correct outcome of contention under strict 2PL, and a timeout here would
// convert ordinary waiting into failure. What bounds the wait is the
// holder ending its transaction.
func (g *remoteGuard) Acquire(resource lockmanager.ResourceID, mode lockmanager.LockMode) error {
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

	_, err = g.backend.client.Acquire(context.Background(), &coordinatorv1.AcquireRequest{
		TransactionId: g.txn.String(),
		Resource:      protoResource,
		Mode:          protoMode,
	})
	if err != nil {
		return fmt.Errorf("grpccoord: acquire %s on %v: %w", mode, resource, err)
	}
	return nil
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
	if g.ended || !g.acquired {
		g.ended = true
		g.mu.Unlock()
		return
	}
	g.ended = true
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), g.backend.timeout)
	defer cancel()

	if _, err := g.backend.client.EndTransaction(ctx, &coordinatorv1.EndTransactionRequest{
		TransactionId: g.txn.String(),
	}); err != nil {
		// Nowhere to return this. Make it visible rather than silent: a
		// failed EndTransaction means locks are still held remotely.
		fmt.Fprintf(os.Stderr, "grpccoord: EndTransaction for %s failed, locks may be leaked: %v\n", g.txn, err)
	}
}

// TransactionID exposes the external transaction identity, for tests and
// diagnostics.
func (g *remoteGuard) TransactionID() uuid.UUID { return g.txn }
