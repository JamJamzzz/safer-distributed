// Package crossprocess is the harness for SAFER's cross-process
// coordination tests.
//
// It starts real, separate OS processes -- one lock coordinator and two or
// more SAFER workers -- against one MongoDB. Each worker opens its own
// database connection and its own gRPC connection to the coordinator, so
// nothing is shared in-process. Goroutines inside one test binary would
// demonstrate nothing about distribution, because they would share a
// single LockManager by construction.
//
// Overlap between workers is arranged with an explicit barrier. Each
// worker announces itself and blocks; the harness releases all of them at
// once, only after every one has arrived and finished its setup. No test
// depends on a sleep to create a race.
package crossprocess

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

const (
	// startupTimeout bounds process launch and connection, not lock
	// waiting. It is a failure guard, never a synchronization mechanism.
	startupTimeout = 60 * time.Second
	// workerTimeout bounds one worker's whole run. A worker legitimately
	// waits behind another's lock, so this is generous.
	workerTimeout = 90 * time.Second
)

// WorkerResult mirrors the JSON line a worker process prints.
type WorkerResult struct {
	Worker     string `json:"worker"`
	Op         string `json:"op"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	Content    string `json:"content"`
	Version    uint64 `json:"version"`
	ChunkCount uint64 `json:"chunk_count"`
	StartedAt  int64  `json:"started_at_unix_nano"`
	EndedAt    int64  `json:"ended_at_unix_nano"`
}

// Overlapped reports whether two worker runs actually overlapped in time.
// It is an observation used to confirm the barrier did its job, never an
// assertion about which one won.
func Overlapped(a, b WorkerResult) bool {
	return a.StartedAt < b.EndedAt && b.StartedAt < a.EndedAt
}

// Cluster is one test's deployment: a MongoDB database, a coordinator
// process, and the built worker binary.
type Cluster struct {
	t *testing.T

	MongoURI     string
	Database     string
	CoordAddress string

	workerBinary string
	store        *mongostore.Store
}

// binaries are built once per test binary run, not once per test: `go
// build` is far slower than everything else here.
var (
	buildOnce   sync.Once
	builtCoord  string
	builtWorker string
	buildErr    error
)

func buildBinaries() (coordinator, worker string, err error) {
	buildOnce.Do(func() {
		dir, dirErr := os.MkdirTemp("", "safer-crossprocess-bin")
		if dirErr != nil {
			buildErr = dirErr
			return
		}
		exeSuffix := ""
		if isWindows() {
			exeSuffix = ".exe"
		}
		builtCoord = filepath.Join(dir, "coordinator"+exeSuffix)
		builtWorker = filepath.Join(dir, "saferworker"+exeSuffix)

		for _, target := range []struct{ out, pkg string }{
			{builtCoord, "github.com/JamJamzzz/safer-distributed/cmd/coordinator"},
			{builtWorker, "github.com/JamJamzzz/safer-distributed/cmd/saferworker"},
		} {
			cmd := exec.Command("go", "build", "-o", target.out, target.pkg)
			if output, runErr := cmd.CombinedOutput(); runErr != nil {
				buildErr = fmt.Errorf("building %s: %v\n%s", target.pkg, runErr, output)
				return
			}
		}
	})
	return builtCoord, builtWorker, buildErr
}

func isWindows() bool { return os.PathSeparator == '\\' && os.PathListSeparator == ';' }

// Start brings up a cluster, or skips the test when MongoDB is not
// configured or reachable.
func Start(t *testing.T) *Cluster {
	t.Helper()

	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	mongoCfg.Database = fmt.Sprintf("safer_xp_%d", time.Now().UnixNano())

	store, err := mongostore.Open(context.Background(), mongoCfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}

	coordinatorBin, workerBin, err := buildBinaries()
	if err != nil {
		_ = store.Close(context.Background())
		t.Fatalf("building helper binaries: %v", err)
	}

	cluster := &Cluster{
		t:            t,
		MongoURI:     mongoCfg.URI,
		Database:     mongoCfg.Database,
		workerBinary: workerBin,
		store:        store,
	}
	cluster.startCoordinator(coordinatorBin)

	t.Cleanup(func() {
		_ = store.DropDatabase(context.Background())
		_ = store.Close(context.Background())
	})
	return cluster
}

// startCoordinator launches the coordinator process and waits for it to
// report the address it bound, rather than guessing a port.
func (c *Cluster) startCoordinator(binary string) {
	c.t.Helper()

	cmd := exec.Command(binary, "-addr", "127.0.0.1:0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.t.Fatalf("coordinator stdout: %v", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		c.t.Fatalf("starting coordinator: %v", err)
	}

	addressCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "listening ") {
				select {
				case addressCh <- strings.TrimPrefix(line, "listening "):
				default:
				}
			}
		}
	}()

	select {
	case address := <-addressCh:
		c.CoordAddress = address
	case <-time.After(startupTimeout):
		_ = cmd.Process.Kill()
		c.t.Fatalf("coordinator did not report an address; stderr:\n%s", stderr.String())
	}

	c.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
}

// Setup runs a function against the cluster's storage and coordinator from
// the test process itself. It is for preparing state (creating users and
// files) before any worker starts; it is never used to create a race.
func (c *Cluster) Setup(fn func(t *testing.T)) {
	c.t.Helper()

	restoreStorage := client.UseStorage(c.store.Storage())
	defer restoreStorage()

	backend, err := grpccoord.Dial(context.Background(), grpccoord.Config{
		Address: c.CoordAddress,
		Timeout: startupTimeout,
	})
	if err != nil {
		c.t.Fatalf("test process dialing coordinator: %v", err)
	}
	defer func() { _ = backend.Close() }()

	restoreCoordination := client.UseCoordination(backend)
	defer restoreCoordination()

	fn(c.t)
}

// WorkerSpec describes one worker process to run.
type WorkerSpec struct {
	Name     string
	Op       string // append, store, load, or metadata
	User     string
	Password string
	File     string
	Content  string
}

// RunConcurrently starts every worker as its own process and releases them
// all from a barrier at the same moment, so their SAFER operations
// genuinely overlap. It returns each worker's reported result, in the
// order the specs were given.
func (c *Cluster) RunConcurrently(specs ...WorkerSpec) []WorkerResult {
	c.t.Helper()
	if len(specs) == 0 {
		c.t.Fatal("no workers specified")
	}

	barrier := newBarrier(c.t, len(specs))
	defer barrier.close()

	type outcome struct {
		index  int
		result WorkerResult
		err    error
	}
	outcomes := make(chan outcome, len(specs))

	for i, spec := range specs {
		go func(i int, spec WorkerSpec) {
			result, err := c.runWorker(spec, barrier.address)
			outcomes <- outcome{index: i, result: result, err: err}
		}(i, spec)
	}

	// Release only once every worker has finished its own setup and is
	// waiting. This is what makes the overlap deterministic.
	barrier.releaseWhenAllArrived()

	results := make([]WorkerResult, len(specs))
	for range specs {
		select {
		case got := <-outcomes:
			if got.err != nil {
				c.t.Fatalf("worker %q: %v", specs[got.index].Name, got.err)
			}
			results[got.index] = got.result
		case <-time.After(workerTimeout):
			c.t.Fatal("timed out waiting for workers; a lock may be held with nothing to release it")
		}
	}
	return results
}

// runWorker executes one worker process and decodes its result line.
func (c *Cluster) runWorker(spec WorkerSpec, barrierAddress string) (WorkerResult, error) {
	args := []string{
		"-name", spec.Name,
		"-op", spec.Op,
		"-user", spec.User,
		"-password", spec.Password,
		"-file", spec.File,
		"-content", spec.Content,
		"-barrier", barrierAddress,
		"-db", c.Database,
	}
	cmd := exec.Command(c.workerBinary, args...)
	// Each worker gets its own connections, from its own environment.
	cmd.Env = append(os.Environ(),
		mongostore.EnvURI+"="+c.MongoURI,
		mongostore.EnvDatabase+"="+c.Database,
		grpccoord.EnvAddress+"="+c.CoordAddress,
	)

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return WorkerResult{}, fmt.Errorf("running worker: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	line := lastNonEmptyLine(stdout.String())
	if line == "" {
		return WorkerResult{}, fmt.Errorf("worker produced no result line\nstderr:\n%s", stderr.String())
	}
	var result WorkerResult
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		return WorkerResult{}, fmt.Errorf("decoding %q: %w", line, err)
	}
	return result, nil
}

func lastNonEmptyLine(s string) string {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return strings.TrimSpace(lines[i])
		}
	}
	return ""
}

// barrier is a rendezvous point for worker processes. Workers connect and
// announce themselves; none proceeds until all have arrived.
type barrier struct {
	t        *testing.T
	address  string
	listener net.Listener
	arrived  chan net.Conn
	expected int
}

func newBarrier(t *testing.T, expected int) *barrier {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("barrier listen: %v", err)
	}
	b := &barrier{
		t:        t,
		address:  listener.Addr().String(),
		listener: listener,
		arrived:  make(chan net.Conn, expected),
		expected: expected,
	}
	go b.accept()
	return b
}

func (b *barrier) accept() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		go func(conn net.Conn) {
			// Read the announcement so a worker counts as arrived only
			// once it has really reached the barrier.
			_ = conn.SetReadDeadline(time.Now().Add(startupTimeout))
			if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
				_ = conn.Close()
				return
			}
			b.arrived <- conn
		}(conn)
	}
}

// releaseWhenAllArrived blocks until every expected worker has announced
// itself, then releases them all together.
func (b *barrier) releaseWhenAllArrived() {
	b.t.Helper()
	conns := make([]net.Conn, 0, b.expected)
	for len(conns) < b.expected {
		select {
		case conn := <-b.arrived:
			conns = append(conns, conn)
		case <-time.After(startupTimeout):
			b.t.Fatalf("only %d of %d workers reached the barrier", len(conns), b.expected)
		}
	}
	for _, conn := range conns {
		_, _ = fmt.Fprint(conn, "go\n")
	}
}

func (b *barrier) close() { _ = b.listener.Close() }
