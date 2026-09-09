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
// generator, run exactly as a deployment would run it (as its own process,
// talking only to the worker Service's address), successfully drives
// every documented workload type against real worker replicas, a real
// coordinator, and real MongoDB, with zero errors.
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

	// Two replicas: enough to prove the load generator is not implicitly
	// pinned to one worker process, without paying for a large fleet in
	// every CI run.
	worker1 := startProcess(t, workerBin, []string{"-addr", "127.0.0.1:0"}, workerEnv)
	worker2 := startProcess(t, workerBin, []string{"-addr", "127.0.0.1:0"}, workerEnv)
	addrFlag := worker1 + "," + worker2

	for _, workload := range []string{"independent-writes", "same-file-writes", "reads", "mixed"} {
		workload := workload
		t.Run(workload, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, loadgenBin,
				"-addr", addrFlag,
				"-workload", workload,
				"-concurrency", "4",
				"-count", "40",
				"-content-size", "16",
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("loadgen -workload %s failed: %v\n%s", workload, err, output)
			}
			if !strings.Contains(string(output), "errors=0") {
				t.Errorf("loadgen -workload %s reported errors:\n%s", workload, output)
			}
		})
	}
}
