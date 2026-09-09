// Package workerservice proves that cmd/worker -- the production,
// long-running worker service Phase 4 adds -- actually works as a
// deployable, replicated service: several independent worker processes,
// each with its own MongoDB connection and its own coordinator connection,
// serve one logical SAFER deployment correctly.
//
// This is the automated stand-in for "3 worker replicas behind one
// Kubernetes Service": it does not start a Kubernetes cluster (none is
// available in this environment; see docs/distributed-roadmap.md), but it
// does exercise the same real boundary a Service would sit in front of --
// independent OS processes, dialed over real gRPC, sharing one coordinator
// and one database -- which is what actually determines whether the
// service is safe to replicate.
package workerservice

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

const (
	startupTimeout = 60 * time.Second
	callTimeout    = 30 * time.Second
)

// buildBinaries builds cmd/coordinator and cmd/worker once per test binary
// run. `go build` is far slower than anything else in this test.
var (
	buildOnce      sync.Once
	builtCoord     string
	builtWorker    string
	buildErr       error
)

func buildBinaries(t *testing.T) (coordinator, worker string) {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "safer-workerservice-bin")
		if err != nil {
			buildErr = err
			return
		}
		exeSuffix := ""
		if os.PathSeparator == '\\' && os.PathListSeparator == ';' {
			exeSuffix = ".exe"
		}
		builtCoord = filepath.Join(dir, "coordinator"+exeSuffix)
		builtWorker = filepath.Join(dir, "worker"+exeSuffix)

		for _, target := range []struct{ out, pkg string }{
			{builtCoord, "github.com/JamJamzzz/safer-distributed/cmd/coordinator"},
			{builtWorker, "github.com/JamJamzzz/safer-distributed/cmd/worker"},
		} {
			cmd := exec.Command("go", "build", "-o", target.out, target.pkg)
			if output, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("building %s: %v\n%s", target.pkg, err, output)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatalf("building helper binaries: %v", buildErr)
	}
	return builtCoord, builtWorker
}

// startProcess runs binary and blocks until it prints a "listening <addr>"
// line on stdout (both cmd/coordinator and cmd/worker report their bound
// address this way), or fails the test.
func startProcess(t *testing.T, binary string, args []string, env []string) string {
	t.Helper()

	cmd := exec.Command(binary, args...)
	cmd.Env = env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}

	addressCh := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		var accumulated strings.Builder
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				accumulated.Write(buf[:n])
				for _, line := range strings.Split(accumulated.String(), "\n") {
					if strings.HasPrefix(line, "listening ") {
						select {
						case addressCh <- strings.TrimSpace(strings.TrimPrefix(line, "listening ")):
						default:
						}
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	select {
	case address := <-addressCh:
		return address
	case <-time.After(startupTimeout):
		t.Fatalf("%s did not report an address; stderr:\n%s", binary, stderr.String())
		return ""
	}
}

// dialWorker connects a worker.v1.SaferWorker client to a running worker
// process, exactly as a real client reaching the Kubernetes Service would.
func dialWorker(t *testing.T, address string) workerv1.SaferWorkerClient {
	t.Helper()
	conn := dialGRPC(t, address)
	return workerv1.NewSaferWorkerClient(conn)
}

// dialGRPC opens a blocking, insecure connection to a locally started
// process. Insecure is correct here, not a shortcut: it matches
// grpccoord.Backend's own transport, which this deployment's trust model
// already documents (see client/coordination/grpccoord/client.go).
func dialGRPC(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	dialCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dialing %s: %v", address, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestMultipleWorkerReplicas is Phase 4's core service-level correctness
// claim: independent worker processes, each with its own MongoDB and
// coordinator connections, can service requests for the SAME logical
// SAFER user against ONE coordinator and ONE database -- InitUser on one
// replica, StoreFile on a second, LoadFile on a third -- and see
// consistent results. That is only possible because the worker holds no
// per-user session state: every RPC re-derives the user from its
// username/password (see cmd/worker/service.go), so any replica can serve
// any request.
func TestMultipleWorkerReplicas(t *testing.T) {
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	mongoCfg.Database = fmt.Sprintf("safer_workersvc_%d", time.Now().UnixNano())

	coordinatorBin, workerBin := buildBinaries(t)

	mongoEnv := []string{
		mongostore.EnvURI + "=" + mongoCfg.URI,
		mongostore.EnvDatabase + "=" + mongoCfg.Database,
	}

	coordAddr := startProcess(t, coordinatorBin,
		[]string{"-addr", "127.0.0.1:0", "-lease", "5s", "-sweep", "200ms"},
		append(os.Environ(), mongoEnv...))

	workerEnv := append(append(os.Environ(), mongoEnv...), grpccoord.EnvAddress+"="+coordAddr)

	const replicaCount = 3
	workerAddrs := make([]string, replicaCount)
	for i := 0; i < replicaCount; i++ {
		workerAddrs[i] = startProcess(t, workerBin, []string{"-addr", "127.0.0.1:0"}, workerEnv)
	}

	clients := make([]workerv1.SaferWorkerClient, replicaCount)
	for i, addr := range workerAddrs {
		clients[i] = dialWorker(t, addr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	const username, password = "alice", "correct horse battery staple"
	if _, err := clients[0].InitUser(ctx, &workerv1.InitUserRequest{Username: username, Password: password}); err != nil {
		t.Fatalf("InitUser on replica 0: %v", err)
	}

	const filename, content = "report.txt", "first draft"
	if _, err := clients[1].StoreFile(ctx, &workerv1.StoreFileRequest{
		Username: username, Password: password, Filename: filename, Content: []byte(content),
	}); err != nil {
		t.Fatalf("StoreFile on replica 1: %v", err)
	}

	appended := " -- revision"
	if _, err := clients[2].AppendToFile(ctx, &workerv1.AppendToFileRequest{
		Username: username, Password: password, Filename: filename, Content: []byte(appended),
	}); err != nil {
		t.Fatalf("AppendToFile on replica 2: %v", err)
	}

	loaded, err := clients[0].LoadFile(ctx, &workerv1.LoadFileRequest{
		Username: username, Password: password, Filename: filename,
	})
	if err != nil {
		t.Fatalf("LoadFile on replica 0: %v", err)
	}
	if got, want := string(loaded.GetContent()), content+appended; got != want {
		t.Errorf("LoadFile content = %q, want %q", got, want)
	}

	// A second, independent user on a different replica proves this is not
	// an artifact of hitting the same replica's in-memory state: nothing
	// about "alice" leaked into how "bob" is served.
	const otherUser, otherPassword = "bob", "another passphrase"
	if _, err := clients[2].InitUser(ctx, &workerv1.InitUserRequest{Username: otherUser, Password: otherPassword}); err != nil {
		t.Fatalf("InitUser for a second user on replica 2: %v", err)
	}
	if _, err := clients[1].StoreFile(ctx, &workerv1.StoreFileRequest{
		Username: otherUser, Password: otherPassword, Filename: "notes.txt", Content: []byte("bob's notes"),
	}); err != nil {
		t.Fatalf("StoreFile for a second user on replica 1: %v", err)
	}
	bobLoaded, err := clients[0].LoadFile(ctx, &workerv1.LoadFileRequest{
		Username: otherUser, Password: otherPassword, Filename: "notes.txt",
	})
	if err != nil {
		t.Fatalf("LoadFile for a second user on replica 0: %v", err)
	}
	if got, want := string(bobLoaded.GetContent()), "bob's notes"; got != want {
		t.Errorf("bob's LoadFile content = %q, want %q", got, want)
	}
}

// TestWorkerReadinessReflectsDependencies proves the readiness/liveness
// split cmd/worker implements: a healthy worker reports SERVING on both
// the readiness service (the empty-string default) and the liveness
// service, while its dependencies are reachable.
//
// It does not simulate a MongoDB or coordinator outage -- that would
// require killing a shared container mid-test, which would break every
// other test using it -- so it does not cover the "readiness goes
// NOT_SERVING under a dependency outage" half of the claim. That half is
// implied directly by cmd/worker/health.go's logic (a failed Ping or
// Health call sets NOT_SERVING) and is not re-verified here.
func TestWorkerReadinessReflectsDependencies(t *testing.T) {
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	mongoCfg.Database = fmt.Sprintf("safer_workersvc_health_%d", time.Now().UnixNano())

	coordinatorBin, workerBin := buildBinaries(t)
	mongoEnv := []string{
		mongostore.EnvURI + "=" + mongoCfg.URI,
		mongostore.EnvDatabase + "=" + mongoCfg.Database,
	}
	coordAddr := startProcess(t, coordinatorBin,
		[]string{"-addr", "127.0.0.1:0", "-lease", "5s", "-sweep", "200ms"},
		append(os.Environ(), mongoEnv...))
	workerEnv := append(append(os.Environ(), mongoEnv...), grpccoord.EnvAddress+"="+coordAddr)
	workerAddr := startProcess(t, workerBin, []string{"-addr", "127.0.0.1:0"}, workerEnv)

	conn := dialGRPC(t, workerAddr)
	healthClient := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	// The readiness loop runs once immediately at startup (see
	// startReadinessLoop), but give it a moment in case of a slow CI host.
	deadline := time.Now().Add(10 * time.Second)
	var readiness *healthpb.HealthCheckResponse
	for time.Now().Before(deadline) {
		readiness, err = healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		if err == nil && readiness.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("readiness Check: %v", err)
	}
	if readiness.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("readiness status = %v, want SERVING", readiness.GetStatus())
	}

	liveness, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: "liveness"})
	if err != nil {
		t.Fatalf("liveness Check: %v", err)
	}
	if liveness.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("liveness status = %v, want SERVING", liveness.GetStatus())
	}
}
