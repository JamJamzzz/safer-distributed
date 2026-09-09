package grpccoord

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

// Tests for leases, fencing, and revocation.
//
// These run a real gRPC server over a real TCP connection with an
// in-memory fence store. They cover the coordinator's behavior; the
// cross-process tests cover what a real killed worker does.

// memoryFences is an in-memory FenceStore with controllable failure, so a
// test can make fence persistence unavailable at an exact moment.
type memoryFences struct {
	mu     sync.Mutex
	tokens map[string]fencing.Token
	owners map[string]string

	// failing makes every operation fail, standing in for an unreachable
	// fence store.
	failing bool
	// allocations counts successful Allocate calls.
	allocations int
	// invalidations counts successful Invalidate calls.
	invalidations int
}

var errFenceStoreDown = errors.New("fence store unavailable")

func newMemoryFences() *memoryFences {
	return &memoryFences{
		tokens: make(map[string]fencing.Token),
		owners: make(map[string]string),
	}
}

func (m *memoryFences) setFailing(failing bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failing = failing
}

func (m *memoryFences) Allocate(ctx context.Context, resource, ownerTxn string) (fencing.Token, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failing {
		return 0, errFenceStoreDown
	}
	m.tokens[resource]++
	m.owners[resource] = ownerTxn
	m.allocations++
	return m.tokens[resource], nil
}

func (m *memoryFences) Invalidate(ctx context.Context, resource, ownerTxn string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failing {
		return errFenceStoreDown
	}
	if m.owners[resource] == ownerTxn {
		delete(m.owners, resource)
	}
	m.invalidations++
	return nil
}

func (m *memoryFences) owner(resource string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.owners[resource]
}

func (m *memoryFences) counts() (allocations, invalidations int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allocations, m.invalidations
}

// startLeaseCoordinator runs a coordinator with short lease timing, so
// expiry tests do not have to wait seconds.
func startLeaseCoordinator(t *testing.T, lease time.Duration) (*Server, *Backend, *memoryFences) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fences := newMemoryFences()
	server := NewServerWithConfig(ServerConfig{
		Fences:               fences,
		LeaseDuration:        lease,
		SweepInterval:        20 * time.Millisecond,
		CleanupRetryInterval: 50 * time.Millisecond,
		Logf:                 func(string, ...interface{}) {}, // quiet in tests
	})
	grpcServer := grpc.NewServer()
	coordinatorv1.RegisterLockCoordinatorServer(grpcServer, server)

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = grpcServer.Serve(listener)
	}()

	backend, err := Dial(context.Background(), Config{
		Address: listener.Addr().String(),
		Timeout: testTimeout,
		// Long enough that nothing renews on its own unless the test
		// wants it to.
		RenewInterval: time.Hour,
	})
	if err != nil {
		grpcServer.Stop()
		t.Fatalf("Dial: %v", err)
	}

	t.Cleanup(func() {
		_ = backend.Close()
		grpcServer.Stop()
		<-served
		server.Stop()
	})
	return server, backend, fences
}

// rawAcquire calls the RPC directly, so a test controls the transaction id
// and can retry a request verbatim.
func rawAcquire(t *testing.T, backend *Backend, txn uuid.UUID, resource lockmanager.ResourceID, mode coordinatorv1.LockMode) (*coordinatorv1.AcquireResponse, error) {
	t.Helper()
	protoResource, err := resourceToProto(resource)
	if err != nil {
		t.Fatalf("resourceToProto: %v", err)
	}
	return backend.client.Acquire(context.Background(), &coordinatorv1.AcquireRequest{
		TransactionId: txn.String(),
		Resource:      protoResource,
		Mode:          mode,
	})
}

// TestExclusiveGrantCarriesFencingToken checks that X grants are fenced
// and S grants are not.
func TestExclusiveGrantCarriesFencingToken(t *testing.T) {
	_, backend, fences := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("fenced")

	writer := uuid.New()
	response, err := rawAcquire(t, backend, writer, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
	if err != nil {
		t.Fatalf("Acquire(X): %v", err)
	}
	if response.GetFencingToken() == 0 {
		t.Fatal("an exclusive grant came back with no fencing token")
	}
	if response.GetLeaseExpiresUnixNano() <= time.Now().UnixNano() {
		t.Fatal("the lease deadline is already in the past")
	}
	key := fencing.ResourceKey(uint8(resource.Type), resource.Key)
	if got := fences.owner(key); got != writer.String() {
		t.Fatalf("fence owner is %q, want %q", got, writer)
	}

	// A shared grant on a different resource gets no token: a stale
	// reader corrupts nothing, and fencing readers would make them
	// conflict on the fence document for no benefit.
	reader := uuid.New()
	sharedResponse, err := rawAcquire(t, backend, reader, fileResource("shared"), coordinatorv1.LockMode_LOCK_MODE_SHARED)
	if err != nil {
		t.Fatalf("Acquire(S): %v", err)
	}
	if sharedResponse.GetFencingToken() != 0 {
		t.Fatalf("a shared grant received fencing token %d, want none", sharedResponse.GetFencingToken())
	}
	allocations, _ := fences.counts()
	if allocations != 1 {
		t.Fatalf("%d tokens allocated, want 1 (the shared grant must not allocate)", allocations)
	}
}

// TestAcquireRetryReturnsTheSameToken is the RPC idempotency property: a
// retry after a lost response must recover the original grant, not invent
// a second one.
func TestAcquireRetryReturnsTheSameToken(t *testing.T) {
	server, backend, fences := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("retry")
	txn := uuid.New()

	first, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	for attempt := 0; attempt < 3; attempt++ {
		retry, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
		if err != nil {
			t.Fatalf("retry %d: %v", attempt, err)
		}
		if retry.GetFencingToken() != first.GetFencingToken() {
			t.Fatalf("retry %d returned token %d, want the original %d",
				attempt, retry.GetFencingToken(), first.GetFencingToken())
		}
	}

	// No extra tokens were allocated and the LockManager was not asked
	// again -- a second call would have been rejected as reentrant.
	if allocations, _ := fences.counts(); allocations != 1 {
		t.Fatalf("%d tokens allocated across retries, want 1", allocations)
	}
	if held := server.LockManager().TotalHeldCount(); held != 1 {
		t.Fatalf("%d locks held after retries, want 1", held)
	}

	// A different mode on a held resource is still refused: no upgrades,
	// no reentrancy.
	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_SHARED); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mode change on a held resource: got %v, want FailedPrecondition", err)
	}
}

// TestRenewLeaseIsIdempotent checks that renewing repeatedly just moves
// the deadline, and that renewal fails once the transaction is gone.
func TestRenewLeaseIsIdempotent(t *testing.T) {
	_, backend, _ := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("renew")
	txn := uuid.New()

	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	var previous int64
	for attempt := 0; attempt < 3; attempt++ {
		response, err := backend.client.RenewLease(context.Background(),
			&coordinatorv1.RenewLeaseRequest{TransactionId: txn.String()})
		if err != nil {
			t.Fatalf("renewal %d: %v", attempt, err)
		}
		if response.GetLeaseExpiresUnixNano() < previous {
			t.Fatalf("renewal %d moved the deadline backwards", attempt)
		}
		previous = response.GetLeaseExpiresUnixNano()
		time.Sleep(2 * time.Millisecond)
	}

	// After the transaction ends, renewal must fail: that is how a worker
	// learns it no longer holds anything.
	if _, err := backend.client.EndTransaction(context.Background(),
		&coordinatorv1.EndTransactionRequest{TransactionId: txn.String()}); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
	if _, err := backend.client.RenewLease(context.Background(),
		&coordinatorv1.RenewLeaseRequest{TransactionId: txn.String()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("renewing an ended transaction: got %v, want FailedPrecondition", err)
	}
}

// TestGracefulEndInvalidatesFencesBeforeReleasing checks the ordering that
// makes a finished transaction's token unusable before anyone else can
// start writing.
func TestGracefulEndInvalidatesFencesBeforeReleasing(t *testing.T) {
	server, backend, fences := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("graceful-end")
	key := fencing.ResourceKey(uint8(resource.Type), resource.Key)
	txn := uuid.New()

	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if fences.owner(key) != txn.String() {
		t.Fatal("the fence was not taken by the acquiring transaction")
	}

	if _, err := backend.client.EndTransaction(context.Background(),
		&coordinatorv1.EndTransactionRequest{TransactionId: txn.String()}); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}

	if owner := fences.owner(key); owner != "" {
		t.Fatalf("the fence is still owned by %q after a graceful end", owner)
	}
	if held := server.LockManager().TotalHeldCount(); held != 0 {
		t.Fatalf("%d locks still held after a graceful end", held)
	}
	if _, invalidations := fences.counts(); invalidations == 0 {
		t.Fatal("no fence was invalidated on a graceful end")
	}
}

// TestExpiredLeaseIsRevokedAndLocksReturn is the coordinator side of
// worker-crash recovery: a transaction that stops renewing loses its locks
// to a waiter, without anyone asking.
func TestExpiredLeaseIsRevokedAndLocksReturn(t *testing.T) {
	server, backend, fences := startLeaseCoordinator(t, 150*time.Millisecond)
	resource := fileResource("expiry")
	key := fencing.ResourceKey(uint8(resource.Type), resource.Key)

	abandoned := uuid.New()
	first, err := rawAcquire(t, backend, abandoned, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// A second transaction queues behind it and must not be granted
	// while the first transaction's lease is alive.
	waiter := uuid.New()
	waiterDone := make(chan *coordinatorv1.AcquireResponse, 1)
	waiterErr := make(chan error, 1)
	go func() {
		response, err := rawAcquire(t, backend, waiter, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
		if err != nil {
			waiterErr <- err
			return
		}
		waiterDone <- response
	}()

	select {
	case <-waiterDone:
		t.Fatal("the waiter was granted while the holder's lease was still alive")
	case err := <-waiterErr:
		t.Fatalf("waiter failed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Nobody renews. The lease passes, and the coordinator revokes.
	select {
	case response := <-waiterDone:
		if response.GetFencingToken() <= first.GetFencingToken() {
			t.Fatalf("the new holder's token %d does not exceed the revoked holder's %d",
				response.GetFencingToken(), first.GetFencingToken())
		}
		if fences.owner(key) != waiter.String() {
			t.Fatalf("the fence is owned by %q, want the new holder", fences.owner(key))
		}
	case err := <-waiterErr:
		t.Fatalf("the waiter failed instead of being granted: %v", err)
	case <-time.After(testTimeout):
		t.Fatal("the waiter never acquired the lock; the expired lease was not reclaimed")
	}

	// The revoked transaction is gone, and cannot come back.
	if _, err := backend.client.RenewLease(context.Background(),
		&coordinatorv1.RenewLeaseRequest{TransactionId: abandoned.String()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("renewing a revoked lease: got %v, want FailedPrecondition", err)
	}
	if _, err := rawAcquire(t, backend, abandoned, fileResource("other"), coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err == nil {
		t.Fatal("a revoked transaction acquired a new lock")
	}
	_ = server
}

// TestRenewalKeepsALeaseAlive is the control for the expiry test: a
// transaction that keeps renewing is NOT revoked, so revocation is driven
// by the missing renewal and not merely by elapsed time.
func TestRenewalKeepsALeaseAlive(t *testing.T) {
	server, backend, _ := startLeaseCoordinator(t, 150*time.Millisecond)
	resource := fileResource("kept-alive")
	txn := uuid.New()

	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = backend.client.RenewLease(context.Background(),
					&coordinatorv1.RenewLeaseRequest{TransactionId: txn.String()})
			}
		}
	}()

	// Several lease lengths pass while renewals continue.
	time.Sleep(600 * time.Millisecond)

	if held := server.LockManager().TotalHeldCount(); held != 1 {
		t.Fatalf("%d locks held, want 1: a renewed lease was revoked anyway", held)
	}
	if _, err := backend.client.RenewLease(context.Background(),
		&coordinatorv1.RenewLeaseRequest{TransactionId: txn.String()}); err != nil {
		t.Fatalf("the transaction was revoked despite renewing: %v", err)
	}
}

// TestFenceAllocationFailureRefusesTheGrant checks the fail-closed rule:
// if a fencing token cannot be made durable, the exclusive grant is not
// handed out at all.
func TestFenceAllocationFailureRefusesTheGrant(t *testing.T) {
	server, backend, fences := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("no-token")

	fences.setFailing(true)
	txn := uuid.New()
	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want Unavailable: an unfenced exclusive grant was handed out", err)
	}

	// The lock was given back, so the resource is not stuck.
	if held := server.LockManager().TotalHeldCount(); held != 0 {
		t.Fatalf("%d locks held after a refused grant, want 0", held)
	}

	fences.setFailing(false)
	other := uuid.New()
	response, err := rawAcquire(t, backend, other, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
	if err != nil {
		t.Fatalf("Acquire after recovery: %v", err)
	}
	if response.GetFencingToken() == 0 {
		t.Fatal("no token after recovery")
	}
}

// TestFenceStoreOutageKeepsLocksHeld is the required availability-over-
// unsafe-handoff behavior: if fencing metadata cannot be invalidated, the
// logical locks stay held rather than being handed to the next writer.
func TestFenceStoreOutageKeepsLocksHeld(t *testing.T) {
	server, backend, fences := startLeaseCoordinator(t, 150*time.Millisecond)
	resource := fileResource("outage")

	holder := uuid.New()
	if _, err := rawAcquire(t, backend, holder, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// The fence store goes away, and the holder stops renewing.
	fences.setFailing(true)

	waiter := uuid.New()
	granted := make(chan *coordinatorv1.AcquireResponse, 1)
	failed := make(chan error, 1)
	go func() {
		response, err := rawAcquire(t, backend, waiter, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
		if err != nil {
			failed <- err
			return
		}
		granted <- response
	}()

	// The lease passes several times over. The coordinator must NOT hand
	// the lock over, because it cannot prove the old holder's token is
	// dead.
	select {
	case <-granted:
		t.Fatal("the lock was handed over while the previous holder's fence could not be invalidated")
	case err := <-failed:
		t.Fatalf("the waiter failed unexpectedly: %v", err)
	case <-time.After(700 * time.Millisecond):
	}

	if held := server.LockManager().TotalHeldCount(); held != 1 {
		t.Fatalf("%d locks held during the outage, want 1 (held on purpose)", held)
	}
	// Health reports the transaction as revoking, so an operator can see
	// that locks are being held deliberately rather than leaked.
	health, err := backend.client.Health(context.Background(), &coordinatorv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.GetRevokingTransactions() == 0 {
		t.Fatal("Health does not report the stuck revocation")
	}

	// Once persistence recovers, cleanup succeeds and the waiter
	// proceeds.
	fences.setFailing(false)
	select {
	case response := <-granted:
		if response.GetFencingToken() == 0 {
			t.Fatal("the new holder received no fencing token")
		}
	case err := <-failed:
		t.Fatalf("the waiter failed after recovery: %v", err)
	case <-time.After(testTimeout):
		t.Fatal("the waiter never acquired after the fence store recovered")
	}

	if _, invalidations := fences.counts(); invalidations == 0 {
		t.Fatal("the old holder's fence was never invalidated")
	}
}

// TestRevocationCancelsPendingAcquires checks step 2 of the revocation
// order: a transaction being torn down must not be granted a lock it was
// still queued for.
func TestRevocationCancelsPendingAcquires(t *testing.T) {
	server, backend, _ := startLeaseCoordinator(t, 200*time.Millisecond)
	held := fileResource("held-by-other")
	contended := fileResource("contended")

	// Another transaction holds the resource our subject will queue for,
	// and keeps renewing so it is never itself revoked.
	blocker := uuid.New()
	if _, err := rawAcquire(t, backend, blocker, contended, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("blocker Acquire: %v", err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = backend.client.RenewLease(context.Background(),
					&coordinatorv1.RenewLeaseRequest{TransactionId: blocker.String()})
			}
		}
	}()

	// The subject takes one lock (starting its lease), then queues for
	// the contended one and stops renewing.
	subject := uuid.New()
	if _, err := rawAcquire(t, backend, subject, held, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("subject Acquire: %v", err)
	}
	queued := make(chan error, 1)
	go func() {
		_, err := rawAcquire(t, backend, subject, contended, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
		queued <- err
	}()

	select {
	case err := <-queued:
		if err == nil {
			t.Fatal("the queued request was granted to a transaction that was being revoked")
		}
		// Correct: the pending request was cancelled by revocation.
	case <-time.After(testTimeout):
		t.Fatal("the queued request was never cancelled when its transaction was revoked")
	}

	// The subject's first lock came back too.
	deadline := time.Now().Add(testTimeout)
	for {
		if server.LockManager().QueueLen(held) == 0 && server.LockManager().TotalHeldCount() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the revoked transaction's locks were not reclaimed: %d held",
				server.LockManager().TotalHeldCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestGuardRetainsFenceGrants checks the worker side: a guard keeps every
// exclusive grant's token, and hands out a copy.
func TestGuardRetainsFenceGrants(t *testing.T) {
	_, backend, _ := startLeaseCoordinator(t, time.Minute)

	guard, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer guard.ReleaseAll()

	namespace := lockmanager.ResourceID{Type: lockmanager.NamespaceResource, Key: "ns"}
	file := fileResource("file")
	if err := guard.Acquire(namespace, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("Acquire(namespace): %v", err)
	}
	if err := guard.Acquire(file, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("Acquire(file): %v", err)
	}
	// A shared lock adds no grant.
	if err := guard.Acquire(fileResource("read-only"), lockmanager.SharedLock); err != nil {
		t.Fatalf("Acquire(shared): %v", err)
	}

	fenced, ok := guard.(*remoteGuard)
	if !ok {
		t.Fatalf("guard is %T, want *remoteGuard", guard)
	}
	grants := fenced.FenceGrants()
	if len(grants) != 2 {
		t.Fatalf("%d fence grants, want 2 (one per exclusive lock, none for shared)", len(grants))
	}
	for _, grant := range grants {
		if grant.Token == 0 {
			t.Fatalf("%s has no token", grant)
		}
		if grant.OwnerTxn != fenced.TransactionID().String() {
			t.Fatalf("%s is owned by the wrong transaction", grant)
		}
	}

	// The returned slice is a copy: mutating it must not affect what a
	// later commit validates.
	grants[0].Token = 999
	if again := fenced.FenceGrants(); again[0].Token == 999 {
		t.Fatal("FenceGrants returned the guard's own slice")
	}
}

// TestHeartbeatKeepsAGuardAlive checks the worker's automatic renewal: a
// guard holding a lock stays alive across several lease lengths with no
// help from the test.
func TestHeartbeatKeepsAGuardAlive(t *testing.T) {
	server, backend, _ := startLeaseCoordinator(t, 200*time.Millisecond)
	// A renew interval well inside the lease, as a real worker uses.
	backend.renewInterval = 40 * time.Millisecond

	guard, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer guard.ReleaseAll()

	if err := guard.Acquire(fileResource("heartbeat"), lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Several lease lengths pass with the guard doing nothing but
	// heartbeating.
	time.Sleep(800 * time.Millisecond)

	if held := server.LockManager().TotalHeldCount(); held != 1 {
		t.Fatalf("%d locks held, want 1: the heartbeat did not keep the lease alive", held)
	}
}

// TestEndTransactionRemainsIdempotent re-checks the Phase 3A property now
// that ending also invalidates fences.
func TestEndTransactionRemainsIdempotent(t *testing.T) {
	_, backend, _ := startLeaseCoordinator(t, time.Minute)
	resource := fileResource("idempotent")
	txn := uuid.New()

	if _, err := rawAcquire(t, backend, txn, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := backend.client.EndTransaction(context.Background(),
			&coordinatorv1.EndTransactionRequest{TransactionId: txn.String()}); err != nil {
			t.Fatalf("EndTransaction attempt %d: %v", attempt, err)
		}
	}
	response, err := backend.client.EndTransaction(context.Background(),
		&coordinatorv1.EndTransactionRequest{TransactionId: uuid.New().String()})
	if err != nil {
		t.Fatalf("ending an unknown transaction: %v", err)
	}
	if response.GetReleased() != 0 {
		t.Fatalf("released %d for an unknown transaction, want 0", response.GetReleased())
	}
}

// TestLeaseStartsOnFirstGrantNotOnFirstContact checks that a transaction
// queued behind someone else is not penalised for waiting: its lease
// starts when it is actually granted something.
func TestLeaseStartsOnFirstGrantNotOnFirstContact(t *testing.T) {
	server, backend, _ := startLeaseCoordinator(t, 200*time.Millisecond)
	resource := fileResource("queued")

	// The holder keeps renewing for a while, so the waiter sits in the
	// queue for longer than a lease.
	holder := uuid.New()
	if _, err := rawAcquire(t, backend, holder, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = backend.client.RenewLease(context.Background(),
					&coordinatorv1.RenewLeaseRequest{TransactionId: holder.String()})
			}
		}
	}()

	waiter := uuid.New()
	granted := make(chan error, 1)
	go func() {
		_, err := rawAcquire(t, backend, waiter, resource, coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE)
		granted <- err
	}()

	// Wait through more than one lease length while queued.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	if _, err := backend.client.EndTransaction(context.Background(),
		&coordinatorv1.EndTransactionRequest{TransactionId: holder.String()}); err != nil {
		t.Fatalf("holder EndTransaction: %v", err)
	}

	select {
	case err := <-granted:
		if err != nil {
			t.Fatalf("the waiter was revoked while queued rather than granted: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("the waiter never acquired the lock")
	}
	if held := server.LockManager().TotalHeldCount(); held != 1 {
		t.Fatalf("%d locks held, want 1", held)
	}
	_ = fmt.Sprint()
}
