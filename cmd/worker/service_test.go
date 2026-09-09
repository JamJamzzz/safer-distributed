package main

// Proves cancellation reaches all the way through a real worker.v1
// SaferWorker RPC: a real gRPC client cancels a real in-flight
// AppendToFile call, blocked behind a real (in-process) coordinator's
// exclusive lock, and the operation terminates, its pending Acquire is
// removed, no lock is leaked, and a later caller proceeds normally.
//
// client/context_cancellation_test.go and
// client/coordination/grpccoord/acquire_context_test.go already prove the
// two halves of this mechanism in isolation (SAFER's *Context entry
// points threading ctx into guard.AcquireContext, and remoteGuard.
// AcquireContext threading ctx into the coordinator RPC). This test is
// the third leg: that cmd/worker's handlers (service.go) actually hand
// the incoming gRPC context to those *Context entry points, rather than
// discarding it the way the pre-Phase-4.5 handlers did.
//
// It needs no MongoDB: the default in-memory userlib storage backend is
// enough, since only lock behavior is under test here.

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

const testTimeout = 5 * time.Second

// startTestCoordinator brings up a real, in-process lock coordinator with
// no fencing (this test only needs strict-2PL lock behavior), and returns
// it alongside a connected backend a worker installs via
// client.UseCoordination.
func startTestCoordinator(t *testing.T) (*grpccoord.Server, *grpccoord.Backend) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	coordinator := grpccoord.NewServer(nil)
	grpcServer := grpc.NewServer()
	coordinatorv1.RegisterLockCoordinatorServer(grpcServer, coordinator)

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		coordinator.Stop()
		grpcServer.Stop()
		<-served
	})

	backend, err := grpccoord.Dial(context.Background(), grpccoord.Config{
		Address: listener.Addr().String(),
		Timeout: testTimeout,
	})
	if err != nil {
		t.Fatalf("dialing coordinator: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return coordinator, backend
}

// startTestWorker brings up a real saferWorkerServer over a real gRPC
// listener and returns a connected client -- the same surface cmd/worker
// serves in production, minus main's MongoDB/coordinator dialing and
// signal handling, which are irrelevant to this test.
func startTestWorker(t *testing.T) workerv1.SaferWorkerClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	workerv1.RegisterSaferWorkerServer(grpcServer, &saferWorkerServer{
		authLimiter: newAuthLimiter(DefaultAuthConcurrency),
	})

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-served
	})

	dialCtx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dialing worker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return workerv1.NewSaferWorkerClient(conn)
}

func TestAppendToFile_RPCCancellationLeavesNoLockBehind(t *testing.T) {
	coordinator, backend := startTestCoordinator(t)
	restoreCoordination := client.UseCoordination(backend)
	defer restoreCoordination()
	client.SetTestPauseHook(nil)
	defer client.SetTestPauseHook(nil)

	worker := startTestWorker(t)
	ctx := context.Background()

	const username, password, filename = "rpccancel-alice", "password", "shared.txt"
	if _, err := worker.InitUser(ctx, &workerv1.InitUserRequest{Username: username, Password: password}); err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	if _, err := worker.StoreFile(ctx, &workerv1.StoreFileRequest{
		Username: username, Password: password, Filename: filename, Content: []byte("base"),
	}); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}

	// Pause the FIRST append after it has acquired File X (the hook fires
	// strictly after that acquire -- see client.AppendToFileContext), so
	// it genuinely holds the lock and the second append below contends
	// for real.
	holding := make(chan struct{})
	release := make(chan struct{})
	var pauseOnce sync.Once
	client.SetTestPauseHook(func(tag string) {
		if tag == "append:metadata-loaded:"+filename {
			pauseOnce.Do(func() {
				close(holding)
				<-release
			})
		}
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := worker.AppendToFile(ctx, &workerv1.AppendToFileRequest{
			Username: username, Password: password, Filename: filename, Content: []byte("-first"),
		})
		firstDone <- err
	}()
	<-holding

	cancelCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := worker.AppendToFile(cancelCtx, &workerv1.AppendToFileRequest{
			Username: username, Password: password, Filename: filename, Content: []byte("-second"),
		})
		secondDone <- err
	}()

	// Deterministically wait for the second RPC to be genuinely pending on
	// File X. Both appends take a compatible Shared lock on the namespace
	// first (AppendToFileContext's lock order is namespace-S, then
	// file-X), so by the time the second is blocked, three grants exist:
	// the first append's namespace-S and file-X (2), and the second
	// append's own namespace-S (1) -- but nothing more, since its file-X
	// request is the one waiting. ActiveTransactions == 2 confirms both
	// transactions are registered on the coordinator.
	waitUntilWorker(t, testTimeout, func() bool {
		return coordinator.ActiveTransactions() == 2 && coordinator.LockManager().TotalHeldCount() == 3
	})

	cancel()

	select {
	case err := <-secondDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("cancelled AppendToFile RPC returned %v, want a Canceled status", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled AppendToFile RPC never returned")
	}

	// No lock leaked: the second transaction's own deferred
	// guard.ReleaseAll (run inside AppendToFileContext before the RPC
	// even returns to this test) gives back its namespace-S grant, so
	// total held count returns to exactly the first append's own two
	// locks (namespace-S, file-X) -- not left at 3, and not dropped below
	// 2 either, which would mean the first append's locks were disturbed.
	waitUntilWorker(t, testTimeout, func() bool { return coordinator.LockManager().TotalHeldCount() == 2 })

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first AppendToFile: %v", err)
	}

	// The decisive step: a later caller can still acquire and proceed.
	thirdCtx, thirdCancel := context.WithTimeout(context.Background(), testTimeout)
	defer thirdCancel()
	if _, err := worker.AppendToFile(thirdCtx, &workerv1.AppendToFileRequest{
		Username: username, Password: password, Filename: filename, Content: []byte("-third"),
	}); err != nil {
		t.Fatalf("third AppendToFile after cancellation: %v", err)
	}

	loaded, err := worker.LoadFile(ctx, &workerv1.LoadFileRequest{Username: username, Password: password, Filename: filename})
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	content := string(loaded.GetContent())
	if strings.Contains(content, "-second") {
		t.Fatalf("cancelled append's content is present: %q", content)
	}
	if !strings.Contains(content, "-first") || !strings.Contains(content, "-third") {
		t.Fatalf("expected both surviving appends' content in %q", content)
	}

	waitUntilWorker(t, testTimeout, func() bool { return coordinator.ActiveTransactions() == 0 })
}

func waitUntilWorker(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition was not met before the timeout")
	}
}
