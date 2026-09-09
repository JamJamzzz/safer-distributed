package workerservice

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// buildLoadgen builds cmd/loadgen alongside the other helper binaries.
// Separate from buildBinaries in workerservice_test.go because not every
// test in this package needs it, and building it is not free. Go 1.20 (see
// go.mod) predates sync.OnceValue, hence the explicit sync.Once.
var (
	loadgenBuildOnce sync.Once
	loadgenPath      string
	loadgenBuildErr  error
)

func buildLoadgen(t *testing.T) string {
	t.Helper()
	loadgenBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "safer-loadgen-bin")
		if err != nil {
			loadgenBuildErr = err
			return
		}
		exeSuffix := ""
		if os.PathSeparator == '\\' && os.PathListSeparator == ';' {
			exeSuffix = ".exe"
		}
		loadgenPath = filepath.Join(dir, "loadgen"+exeSuffix)
		cmd := exec.Command("go", "build", "-o", loadgenPath, "github.com/JamJamzzz/safer-distributed/cmd/loadgen")
		if output, err := cmd.CombinedOutput(); err != nil {
			loadgenBuildErr = fmt.Errorf("%v\n%s", err, output)
		}
	})
	if loadgenBuildErr != nil {
		t.Fatalf("building loadgen: %v", loadgenBuildErr)
	}
	return loadgenPath
}

// TestLoadgenDrivesWorkerService is Phase 4.6's evidence: the load
// generator, run exactly as a deployment would run it (as its own
// process, talking only to worker addresses over gRPC), successfully
// drives every documented workload type against real worker replicas, a
// real coordinator, and real MongoDB, with zero errors -- and actually
// spreads requests across more than one of them, confirmed from the
// response-header instance identifier cmd/worker attaches to every RPC
// (see internal/workerdiag, cmd/worker/instance.go), not assumed from how
// many worker processes were started.
//
// -addr here is a literal comma-separated address list, which
// cmd/loadgen's manual resolver (dial.go) turns into real client-side
// round_robin balancing across exactly those addresses -- the same
// mechanism, mechanically, that a "dns:///" target against a Kubernetes
// headless Service resolves to a pod address list for (see
// deploy/kubernetes/worker-service-headless.yaml and
// deploy/kubernetes/loadgen-job.yaml, which use that real form). This
// test cannot exercise Kubernetes DNS itself -- there is no cluster in
// this environment (see deploy/kubernetes/README.md) -- but it exercises
// the same gRPC balancing logic loadgen would use there.
//
// This is deliberately a correctness check, not a performance benchmark:
// small counts, run once each. Tuning and throughput numbers are out of
// scope until observability exists (see docs/distributed-roadmap.md).
func TestLoadgenDrivesWorkerService(t *testing.T) {
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	mongoCfg.Database = fmt.Sprintf("safer_loadgen_%d", time.Now().UnixNano())

	coordinatorBin, workerBin := buildBinaries(t)
	loadgenBin := buildLoadgen(t)

	mongoEnv := []string{
		mongostore.EnvURI + "=" + mongoCfg.URI,
		mongostore.EnvDatabase + "=" + mongoCfg.Database,
	}
	coordAddr := startProcess(t, coordinatorBin,
		[]string{"-addr", "127.0.0.1:0", "-lease", "5s", "-sweep", "200ms"},
		append(os.Environ(), mongoEnv...))
	workerEnv := append(append(os.Environ(), mongoEnv...), grpccoord.EnvAddress+"="+coordAddr)

	// Three replicas, matching the Kubernetes worker Deployment's own
	// replica count (deploy/kubernetes/worker-deployment.yaml), so this
	// evidence is not weaker than what a live deployment would have to
	// show.
	var addrs []string
	for i := 0; i < 3; i++ {
		addrs = append(addrs, startProcess(t, workerBin, []string{"-addr", "127.0.0.1:0"}, workerEnv))
	}
	addrFlag := strings.Join(addrs, ",")

	for _, workload := range []string{"independent-writes", "same-file-writes", "reads", "mixed"} {
		workload := workload
		t.Run(workload, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, loadgenBin,
				"-addr", addrFlag,
				"-workload", workload,
				"-concurrency", "8",
				"-count", "80",
				"-content-size", "16",
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("loadgen -workload %s failed: %v\n%s", workload, err, output)
			}
			if !strings.Contains(string(output), "failed=0") {
				t.Errorf("loadgen -workload %s reported failures:\n%s", workload, output)
			}
			if strings.Contains(string(output), "DATA CORRUPTION") {
				t.Errorf("loadgen -workload %s failed its own correctness oracle:\n%s", workload, output)
			}
			if strings.Contains(string(output), "VERIFICATION ERROR") {
				t.Errorf("loadgen -workload %s could not verify its own correctness oracle:\n%s", workload, output)
			}

			replicasServed := parseReplicasServed(t, string(output))
			if replicasServed < 2 {
				t.Errorf("loadgen -workload %s: replicas_served=%d, want at least 2 -- "+
					"requests were not actually spread across worker processes:\n%s",
					workload, replicasServed, output)
			}
		})
	}
}

// parseReplicasServed extracts the count from loadgen's
// "replicas_served=N ..." report line.
func parseReplicasServed(t *testing.T, output string) int {
	t.Helper()
	const marker = "replicas_served="
	idx := strings.Index(output, marker)
	if idx < 0 {
		t.Fatalf("loadgen output has no %q line:\n%s", marker, output)
	}
	rest := output[idx+len(marker):]
	end := strings.IndexAny(rest, " \n")
	if end < 0 {
		end = len(rest)
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil {
		t.Fatalf("parsing replicas_served value %q: %v", rest[:end], err)
	}
	return n
}
