package grpccoord

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

// These tests run a real gRPC server over a real TCP connection, in one
// process. That is enough to test the RPC surface and its failure
// boundaries; it is NOT evidence of cross-process coordination, which the
// separate multi-process harness covers.

const testTimeout = 5 * time.Second

// startCoordinator runs a coordinator on a loopback port and returns a
// connected backend.
func startCoordinator(t *testing.T) (*Server, *Backend) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := NewServer(nil)
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
	})
	if err != nil {
		grpcServer.Stop()
		t.Fatalf("Dial: %v", err)
	}

	t.Cleanup(func() {
		_ = backend.Close()
		grpcServer.Stop()
		<-served
	})
	return server, backend
}

func fileResource(key string) lockmanager.ResourceID {
	return lockmanager.ResourceID{Type: lockmanager.FileResource, Key: key}
}

// acquireAsync runs Acquire in a goroutine so a test can distinguish
// "granted" from "still waiting" without a fixed sleep.
func acquireAsync(g interface {
	Acquire(lockmanager.ResourceID, lockmanager.LockMode) error
}, resource lockmanager.ResourceID, mode lockmanager.LockMode) <-chan error {
	done := make(chan error, 1)
	go func() { done <- g.Acquire(resource, mode) }()
	return done
}

func expectBlocked(t *testing.T, done <-chan error, wait time.Duration, msg string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: returned (err=%v) instead of blocking", msg, err)
	case <-time.After(wait):
	}
}

func expectGranted(t *testing.T, done <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
	case <-time.After(testTimeout):
		t.Fatalf("%s: not granted within timeout", msg)
	}
}

func TestCoordinatorGrantsAndBlocks(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("grants-and-blocks")

	first, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := first.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	second, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	blocked := acquireAsync(second, resource, lockmanager.ExclusiveLock)
	expectBlocked(t, blocked, 100*time.Millisecond, "second X while first holds X")

	// Only ending the first transaction releases it -- strict 2PL.
	first.ReleaseAll()
	expectGranted(t, blocked, "second X after first ended")

	second.ReleaseAll()
	if got := server.ActiveTransactions(); got != 0 {
		t.Fatalf("coordinator still tracks %d transactions, want 0", got)
	}
	if got := server.LockManager().TotalHeldCount(); got != 0 {
		t.Fatalf("coordinator still holds %d locks, want 0", got)
	}
}

func TestCoordinatorSharedLocksCoexist(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("shared-coexist")

	readers := make([]interface {
		Acquire(lockmanager.ResourceID, lockmanager.LockMode) error
		ReleaseAll()
	}, 3)
	for i := range readers {
		g, err := backend.Begin(0)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := g.Acquire(resource, lockmanager.SharedLock); err != nil {
			t.Fatalf("reader %d Acquire(S): %v", i, err)
		}
		readers[i] = g
	}
	if got := server.LockManager().TotalHeldCount(); got != 3 {
		t.Fatalf("held locks: got %d, want 3", got)
	}

	writer, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	blocked := acquireAsync(writer, resource, lockmanager.ExclusiveLock)
	expectBlocked(t, blocked, 100*time.Millisecond, "X while readers hold S")

	for i, r := range readers {
		r.ReleaseAll()
		if i < len(readers)-1 {
			expectBlocked(t, blocked, 50*time.Millisecond, "X while a reader still holds S")
		}
	}
	expectGranted(t, blocked, "X after all readers ended")
	writer.ReleaseAll()
}

// TestCoordinatorCancelledAcquireIsNotGranted is the RPC-level version of
// the ghost-lock property: a caller that gives up must not end up holding
// a lock.
func TestCoordinatorCancelledAcquireIsNotGranted(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("cancelled-acquire")

	holder, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := holder.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	// Call the RPC directly so the test controls the context, which is
	// what a disconnecting worker looks like to the server.
	abandoned := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	rpcDone := make(chan error, 1)
	go func() {
		_, err := backend.client.Acquire(ctx, &coordinatorv1.AcquireRequest{
			TransactionId: abandoned.String(),
			Resource:      &coordinatorv1.ResourceId{Type: coordinatorv1.ResourceType_RESOURCE_TYPE_FILE, Key: resource.Key},
			Mode:          coordinatorv1.LockMode_LOCK_MODE_EXCLUSIVE,
		})
		rpcDone <- err
	}()

	// Wait until the request is really queued on the server.
	waitUntil(t, func() bool { return server.LockManager().QueueLen(resource) == 1 })
	cancel()

	select {
	case err := <-rpcDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("cancelled Acquire: got %v, want a Canceled status", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled Acquire never returned")
	}

	waitUntil(t, func() bool { return server.LockManager().QueueLen(resource) == 0 })

	// The decisive step: release the holder. A ghost lock would show up
	// as the abandoned transaction taking the resource.
	holder.ReleaseAll()
	waitUntil(t, func() bool { return server.LockManager().TotalHeldCount() == 0 })

	// And the resource is usable by an honest transaction.
	next, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	granted := acquireAsync(next, resource, lockmanager.ExclusiveLock)
	expectGranted(t, granted, "later transaction after a cancelled request")
	next.ReleaseAll()
}

// TestCoordinatorEndTransactionIsIdempotent covers the path a worker takes
// when an Acquire response is lost: it ends the transaction anyway.
func TestCoordinatorEndTransactionIsIdempotent(t *testing.T) {
	_, backend := startCoordinator(t)
	resource := fileResource("idempotent-end")

	guard, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := guard.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	guard.ReleaseAll()
	guard.ReleaseAll() // must not panic or error

	// Ending a transaction the coordinator has never heard of succeeds
	// and reports nothing released.
	resp, err := backend.client.EndTransaction(context.Background(), &coordinatorv1.EndTransactionRequest{
		TransactionId: uuid.New().String(),
	})
	if err != nil {
		t.Fatalf("EndTransaction for an unknown transaction: %v", err)
	}
	if resp.GetReleased() != 0 {
		t.Fatalf("released %d for an unknown transaction, want 0", resp.GetReleased())
	}
}

// TestCoordinatorRejectsMalformedRequests checks that ambiguous requests
// are refused rather than defaulted. Defaulting an unset mode to shared
// would silently turn a writer into a reader and lose mutual exclusion.
func TestCoordinatorRejectsMalformedRequests(t *testing.T) {
	_, backend := startCoordinator(t)
	ctx := context.Background()
	valid := &coordinatorv1.ResourceId{Type: coordinatorv1.ResourceType_RESOURCE_TYPE_FILE, Key: "k"}

	cases := []struct {
		name string
		req  *coordinatorv1.AcquireRequest
	}{
		{"missing transaction id", &coordinatorv1.AcquireRequest{
			Resource: valid, Mode: coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"non-uuid transaction id", &coordinatorv1.AcquireRequest{
			TransactionId: "not-a-uuid", Resource: valid, Mode: coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"nil uuid transaction id", &coordinatorv1.AcquireRequest{
			TransactionId: uuid.Nil.String(), Resource: valid, Mode: coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"missing resource", &coordinatorv1.AcquireRequest{
			TransactionId: uuid.New().String(), Mode: coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"empty resource key", &coordinatorv1.AcquireRequest{
			TransactionId: uuid.New().String(),
			Resource:      &coordinatorv1.ResourceId{Type: coordinatorv1.ResourceType_RESOURCE_TYPE_FILE},
			Mode:          coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"unspecified resource type", &coordinatorv1.AcquireRequest{
			TransactionId: uuid.New().String(),
			Resource:      &coordinatorv1.ResourceId{Key: "k"},
			Mode:          coordinatorv1.LockMode_LOCK_MODE_SHARED}},
		{"unspecified mode", &coordinatorv1.AcquireRequest{
			TransactionId: uuid.New().String(), Resource: valid}},
	}
	for _, tc := range cases {
		if _, err := backend.client.Acquire(ctx, tc.req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", tc.name, err)
		}
	}
}

// TestCoordinatorRejectsReacquisition preserves V1's rule that a
// transaction may not take a second lock on a resource it already holds:
// no upgrades, no reentrancy.
func TestCoordinatorRejectsReacquisition(t *testing.T) {
	_, backend := startCoordinator(t)
	resource := fileResource("reacquire")

	guard, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer guard.ReleaseAll()

	if err := guard.Acquire(resource, lockmanager.SharedLock); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := guard.Acquire(resource, lockmanager.ExclusiveLock); err == nil {
		t.Fatal("upgrade S->X succeeded, want an error")
	}
}

// TestCoordinatorTransactionsAreIndependentPerGuard checks that two guards
// really are two transactions, and that ending one does not release the
// other's locks.
func TestCoordinatorTransactionsAreIndependentPerGuard(t *testing.T) {
	server, backend := startCoordinator(t)

	first, _ := backend.Begin(0)
	second, _ := backend.Begin(0)
	if first.(*remoteGuard).TransactionID() == second.(*remoteGuard).TransactionID() {
		t.Fatal("two guards share one transaction id")
	}

	if err := first.Acquire(fileResource("a"), lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := second.Acquire(fileResource("b"), lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if got := server.LockManager().TotalHeldCount(); got != 2 {
		t.Fatalf("held locks: got %d, want 2", got)
	}

	first.ReleaseAll()
	if got := server.LockManager().TotalHeldCount(); got != 1 {
		t.Fatalf("after ending one transaction: got %d held locks, want 1", got)
	}
	second.ReleaseAll()
}

// TestCoordinatorFIFOFairnessAcrossConnections confirms the reused
// LockManager's fairness policy is what governs remote requests too: a
// queued writer is not starved by later readers.
func TestCoordinatorFIFOFairnessAcrossConnections(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("fairness")

	reader, _ := backend.Begin(0)
	if err := reader.Acquire(resource, lockmanager.SharedLock); err != nil {
		t.Fatalf("reader Acquire: %v", err)
	}

	writer, _ := backend.Begin(0)
	writerDone := acquireAsync(writer, resource, lockmanager.ExclusiveLock)
	waitUntil(t, func() bool { return server.LockManager().QueueLen(resource) == 1 })

	// A reader arriving after the queued writer must NOT be granted
	// ahead of it.
	lateReader, _ := backend.Begin(0)
	lateDone := acquireAsync(lateReader, resource, lockmanager.SharedLock)
	expectBlocked(t, lateDone, 100*time.Millisecond, "late reader ahead of a queued writer")

	reader.ReleaseAll()
	expectGranted(t, writerDone, "writer after the first reader ended")
	expectBlocked(t, lateDone, 50*time.Millisecond, "late reader while the writer holds X")

	writer.ReleaseAll()
	expectGranted(t, lateDone, "late reader after the writer ended")
	lateReader.ReleaseAll()
}

// TestCoordinatorConcurrentTransactionsSerialize runs many concurrent
// transactions against one resource and checks the coordinator never
// grants X to two of them at once.
func TestCoordinatorConcurrentTransactionsSerialize(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("serialize")

	const workers = 12
	var (
		mu       sync.Mutex
		inside   int
		maxSeen  int
		failures []error
	)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			guard, err := backend.Begin(0)
			if err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
				return
			}
			defer guard.ReleaseAll()
			if err := guard.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
				return
			}
			mu.Lock()
			inside++
			if inside > maxSeen {
				maxSeen = inside
			}
			mu.Unlock()

			mu.Lock()
			inside--
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, err := range failures {
		t.Errorf("worker failure: %v", err)
	}
	if maxSeen > 1 {
		t.Fatalf("%d transactions held the exclusive lock at once, want at most 1", maxSeen)
	}
	if got := server.LockManager().TotalHeldCount(); got != 0 {
		t.Fatalf("%d locks still held after all transactions ended", got)
	}
	if got := server.ActiveTransactions(); got != 0 {
		t.Fatalf("%d transactions still tracked after all ended", got)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EnvAddress, "")
	if _, configured, err := ConfigFromEnv(); err != nil || configured {
		t.Fatalf("unset address: got configured=%v err=%v, want false/nil", configured, err)
	}

	t.Setenv(EnvAddress, "localhost:9999")
	t.Setenv(EnvTimeout, "")
	cfg, configured, err := ConfigFromEnv()
	if err != nil || !configured {
		t.Fatalf("got configured=%v err=%v, want true/nil", configured, err)
	}
	if cfg.Timeout != DefaultTimeout {
		t.Fatalf("Timeout: got %v, want %v", cfg.Timeout, DefaultTimeout)
	}

	t.Setenv(EnvTimeout, "3s")
	if cfg, _, _ = ConfigFromEnv(); cfg.Timeout != 3*time.Second {
		t.Fatalf("Timeout: got %v, want 3s", cfg.Timeout)
	}
	for _, bad := range []string{"soon", "-1s", "0"} {
		t.Setenv(EnvTimeout, bad)
		if _, _, err := ConfigFromEnv(); err == nil {
			t.Fatalf("%s=%q: got nil error, want a rejection", EnvTimeout, bad)
		}
	}
}

func TestDialRequiresAddress(t *testing.T) {
	if _, err := Dial(context.Background(), Config{}); err == nil {
		t.Fatal("Dial with no address succeeded, want an error")
	}
}

// TestDialFailsWhenCoordinatorIsAbsent checks a worker fails at startup
// rather than at its first operation.
func TestDialFailsWhenCoordinatorIsAbsent(t *testing.T) {
	// Port 1 on loopback: reserved, nothing listens there.
	_, err := Dial(context.Background(), Config{Address: "127.0.0.1:1", Timeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("Dial to an absent coordinator succeeded, want an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && status.Code(err) == codes.OK {
		t.Fatalf("unexpected error shape: %v", err)
	}
}

// waitUntil polls a cheap, lock-protected predicate until it holds. It
// synchronizes on server-side bookkeeping ("has this request reached the
// queue"), never on a guess about timing.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}
