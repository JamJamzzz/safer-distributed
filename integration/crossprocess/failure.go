package crossprocess

// Failure-injection support for the Phase 3C tests.
//
// The two failure modes this phase has to demonstrate only exist across
// processes:
//
//   - a worker killed mid-operation, which stops renewing its lease
//     without ever ending its transaction
//   - a worker that stalls, loses its lease, and then wakes up still
//     intending to commit work it computed while it held the lock
//
// Both need a worker stopped at an exact point: holding its locks, having
// read the state it is about to mutate, and not yet committed. The worker
// binary pauses there on a hook and rendezvouses with the harness, so the
// schedule is deterministic rather than a race the test hopes to win.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// PausedWorker is a worker process stopped at its pause point, under the
// harness's control.
type PausedWorker struct {
	cluster *Cluster
	spec    WorkerSpec

	cmd    *exec.Cmd
	conn   net.Conn
	stderr *lockedBuffer
	result chan workerOutcome
}

type workerOutcome struct {
	result WorkerResult
	err    error
}

// lockedBuffer is a string buffer safe to write from the process-reaping
// goroutine while a test reads it.
type lockedBuffer struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.String()
}

// StartPaused launches a worker and returns once it has reached its pause
// point, so the caller controls exactly what happens next.
func (c *Cluster) StartPaused(spec WorkerSpec) *PausedWorker {
	c.t.Helper()
	if spec.PauseAt == "" {
		c.t.Fatal("StartPaused requires a PauseAt")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("pause listener: %v", err)
	}
	defer func() { _ = listener.Close() }()

	paused := &PausedWorker{
		cluster: c,
		spec:    spec,
		stderr:  &lockedBuffer{},
		result:  make(chan workerOutcome, 1),
	}

	cmd := c.workerCommand(spec, "", listener.Addr().String())
	paused.cmd = cmd

	stdout := &lockedBuffer{}
	cmd.Stdout = stdout
	cmd.Stderr = paused.stderr
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("starting worker %q: %v", spec.Name, err)
	}

	go func() {
		runErr := cmd.Wait()
		line := lastNonEmptyLine(stdout.String())
		if line == "" {
			paused.result <- workerOutcome{err: fmt.Errorf(
				"worker %q produced no result line (exit: %v); stderr: %s",
				spec.Name, runErr, paused.stderr.String())}
			return
		}
		var result WorkerResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			paused.result <- workerOutcome{err: fmt.Errorf("decoding %q: %w", line, err)}
			return
		}
		paused.result <- workerOutcome{result: result}
	}()

	// Wait for the worker to announce it has reached the pause point.
	// This is a rendezvous, not a timing guess.
	type accepted struct {
		conn net.Conn
		err  error
	}
	accepts := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepts <- accepted{err: err}
			return
		}
		if _, err := bufio.NewReader(conn).ReadString(byte('\n')); err != nil {
			accepts <- accepted{err: err}
			return
		}
		accepts <- accepted{conn: conn}
	}()

	select {
	case got := <-accepts:
		if got.err != nil {
			_ = cmd.Process.Kill()
			c.t.Fatalf("worker %q never reached its pause point: %v; stderr: %s",
				spec.Name, got.err, paused.stderr.String())
		}
		paused.conn = got.conn
	case outcome := <-paused.result:
		c.t.Fatalf("worker %q exited before pausing: result=%+v err=%v; stderr: %s",
			spec.Name, outcome.result, outcome.err, paused.stderr.String())
	case <-time.After(startupTimeout):
		_ = cmd.Process.Kill()
		c.t.Fatalf("worker %q never reached its pause point; stderr: %s",
			spec.Name, paused.stderr.String())
	}

	c.t.Cleanup(func() {
		if paused.conn != nil {
			_ = paused.conn.Close()
		}
		_ = cmd.Process.Kill()
	})
	return paused
}

// Kill terminates the paused worker abruptly, with no chance to end its
// transaction. To the coordinator this is exactly a crash: the renewals
// simply stop arriving.
func (p *PausedWorker) Kill() {
	p.cluster.t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		p.cluster.t.Fatalf("killing worker %q: %v", p.spec.Name, err)
	}
	// Drain the reaper so it does not leak; a killed worker has no
	// meaningful result.
	select {
	case <-p.result:
	case <-time.After(workerTimeout):
		p.cluster.t.Fatalf("killed worker %q never exited", p.spec.Name)
	}
}

// Resume lets the paused worker continue from where it stopped.
func (p *PausedWorker) Resume() {
	p.cluster.t.Helper()
	if _, err := fmt.Fprint(p.conn, "resume\n"); err != nil {
		p.cluster.t.Fatalf("resuming worker %q: %v", p.spec.Name, err)
	}
}

// Wait returns the worker's reported result once it exits.
func (p *PausedWorker) Wait() WorkerResult {
	p.cluster.t.Helper()
	select {
	case outcome := <-p.result:
		if outcome.err != nil {
			p.cluster.t.Fatalf("worker %q: %v", p.spec.Name, outcome.err)
		}
		return outcome.result
	case <-time.After(workerTimeout):
		p.cluster.t.Fatalf("worker %q never finished after being resumed; stderr: %s",
			p.spec.Name, p.stderr.String())
		return WorkerResult{}
	}
}

// Stderr returns whatever the worker has written to stderr so far, which
// is where the coordination layer reports a lost lease.
func (p *PausedWorker) Stderr() string { return p.stderr.String() }
