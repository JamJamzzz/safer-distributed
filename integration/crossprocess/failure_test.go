package crossprocess

// Worker-failure tests (Phase 3C).
//
// Each starts one coordinator process and independent SAFER worker
// processes against one MongoDB replica set. Workers are stopped at an
// exact point inside their operation -- holding their locks, having read
// the state they are about to mutate, not yet committed -- so the
// schedules are deterministic rather than races the test hopes to win.
//
// What these establish: a crashed worker's locks come back on their own, a
// stalled worker that wakes up after losing its lock cannot commit, and
// the coordinator refuses to hand a lock over while it cannot prove the
// previous holder's token is dead.
//
// What they do NOT establish: anything about the coordinator failing. It
// is a single process with in-memory lock state and remains an explicit
// failure domain.

import (
	"strings"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client"
)

// pauseTag is the hook point used by every test here: an append that has
// acquired its locks and loaded the metadata it is about to replace.
const pauseTag = "append:metadata-loaded"

// TestKilledWorkerLocksAreReclaimed is worker-crash recovery: a worker is
// SIGKILLed while holding an exclusive lock, and nobody ever ends its
// transaction. Its locks must come back on their own.
func TestKilledWorkerLocksAreReclaimed(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	// A pauses holding X on the file, mid-append.
	victim := cluster.StartPaused(WorkerSpec{
		Name: "victim", Op: "append", User: user, Password: password,
		File: file, Content: "-VICTIM", PauseAt: pauseTag,
	})

	// B queues behind it and must not proceed while A is alive and
	// renewing its lease.
	successor := make(chan WorkerResult, 1)
	go func() {
		results := cluster.RunConcurrently(WorkerSpec{
			Name: "successor", Op: "append", User: user, Password: password,
			File: file, Content: "-SUCCESSOR",
		})
		successor <- results[0]
	}()

	select {
	case result := <-successor:
		t.Fatalf("the successor proceeded while the victim held the lock: %+v", result)
	case <-time.After(2 * LeaseDuration):
		// Still blocked, as it should be: the victim is alive and
		// renewing.
	}

	// The victim dies without ending its transaction. Its renewals stop
	// with it.
	victim.Kill()

	select {
	case result := <-successor:
		if !result.OK {
			t.Fatalf("the successor failed after the victim was killed: %s", result.Error)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the killed worker's lock was never reclaimed; it leaked permanently")
	}

	// The successor's append is the only one that landed: the victim
	// never committed.
	content, version := readFinalState(t, cluster)
	if !strings.Contains(content, "-SUCCESSOR") {
		t.Fatalf("final content %q is missing the successor's append", content)
	}
	if strings.Contains(content, "-VICTIM") {
		t.Fatalf("final content %q contains the killed worker's append, which never committed", content)
	}
	if version != 2 {
		t.Fatalf("version: got %d, want 2 (1 initial + 1 committed append)", version)
	}

	// Nothing is left holding the lock: an ordinary operation still
	// works afterwards.
	cluster.Setup(func(t *testing.T) {
		alice, err := client.GetUser(user, password)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if err := alice.AppendToFile(file, []byte("-AFTER")); err != nil {
			t.Fatalf("appending after the crash: %v", err)
		}
	})
}

// TestStaleWriterCannotCommit is stale-writer protection. A worker acquires
// X, is paused before committing, stops renewing, loses its lease, and a
// second worker takes the lock and commits. When the first one wakes up,
// its commit must be refused.
func TestStaleWriterCannotCommit(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	// A holds X and stalls, no longer renewing.
	stale := cluster.StartPaused(WorkerSpec{
		Name: "stale-writer", Op: "append", User: user, Password: password,
		File: file, Content: "-STALE", PauseAt: pauseTag,
		StopRenewalsOnPause: true,
	})

	// Its lease passes and the coordinator revokes it, so B can proceed
	// and commit.
	fresh := cluster.RunConcurrently(WorkerSpec{
		Name: "fresh-writer", Op: "append", User: user, Password: password,
		File: file, Content: "-FRESH",
	})
	if !fresh[0].OK {
		t.Fatalf("the fresh writer failed: %s", fresh[0].Error)
	}

	// A wakes up and tries to commit work it computed while it held the
	// lock.
	stale.Resume()
	result := stale.Wait()

	if result.OK {
		t.Fatal("the stale writer committed after losing its lock")
	}
	// The failure must be the fencing check, not an incidental error.
	if !strings.Contains(result.Error, "stale fencing token") &&
		!strings.Contains(result.Error, "fence") {
		t.Fatalf("the stale writer failed for the wrong reason: %s", result.Error)
	}

	// The fresh writer's state is authoritative and undamaged.
	content, version := readFinalState(t, cluster)
	if !strings.Contains(content, "-FRESH") {
		t.Fatalf("final content %q is missing the fresh writer's append", content)
	}
	if strings.Contains(content, "-STALE") {
		t.Fatalf("final content %q contains the stale writer's append", content)
	}
	if version != 2 {
		t.Fatalf("version: got %d, want 2 (1 initial + 1 committed append)", version)
	}

	// And the file is still fully usable.
	cluster.Setup(func(t *testing.T) {
		alice, err := client.GetUser(user, password)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if err := alice.AppendToFile(file, []byte("-AFTER")); err != nil {
			t.Fatalf("appending after the stale write was refused: %v", err)
		}
		loaded, err := alice.LoadFile(file)
		if err != nil {
			t.Fatalf("LoadFile: %v", err)
		}
		if want := base + "-FRESH-AFTER"; string(loaded) != want {
			t.Fatalf("content %q, want %q", loaded, want)
		}
	})
}

// TestStaleWriterNegativeControl runs the exact schedule of the test above
// with fencing validation bypassed, and requires the bad commit to happen.
//
// Without this, TestStaleWriterCannotCommit could be passing because the
// stale writer fails for some incidental reason -- a dropped connection, a
// timeout -- rather than because fencing caught it.
func TestStaleWriterNegativeControl(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	// Identical to the fencing test, except this worker skips fence
	// validation at commit.
	stale := cluster.StartPaused(WorkerSpec{
		Name: "unfenced-writer", Op: "append", User: user, Password: password,
		File: file, Content: "-STALE", PauseAt: pauseTag,
		StopRenewalsOnPause: true,
		DisableFencing:      true,
	})

	fresh := cluster.RunConcurrently(WorkerSpec{
		Name: "fresh-writer", Op: "append", User: user, Password: password,
		File: file, Content: "-FRESH",
	})
	if !fresh[0].OK {
		t.Fatalf("the fresh writer failed: %s", fresh[0].Error)
	}

	stale.Resume()
	result := stale.Wait()

	if !result.OK {
		t.Fatalf("with fencing bypassed the stale write still failed (%s); "+
			"this control proves nothing, and the fencing test above may be passing "+
			"for an unrelated reason", result.Error)
	}

	// The damage is visible: the stale writer's commit landed on top of
	// the fresh writer's, and the fresh append is gone.
	content, _ := readFinalState(t, cluster)
	if !strings.Contains(content, "-STALE") {
		t.Fatalf("with fencing bypassed the stale write did not land: %q", content)
	}
	if strings.Contains(content, "-FRESH") {
		t.Fatalf("expected the stale write to clobber the fresh one; content is %q", content)
	}
	t.Logf("control: with fencing bypassed, the stale writer clobbered the committed state: %q", content)
}

// TestKilledWorkerDoesNotCommitAfterReclaim combines the two: a killed
// worker's lock is reclaimed and reused, and nothing of its work survives.
func TestKilledWorkerDoesNotCommitAfterReclaim(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	victim := cluster.StartPaused(WorkerSpec{
		Name: "victim", Op: "append", User: user, Password: password,
		File: file, Content: "-VICTIM", PauseAt: pauseTag,
	})
	victim.Kill()

	// Two more workers race for the reclaimed lock, and both must
	// succeed once it comes back.
	results := cluster.RunConcurrently(
		WorkerSpec{Name: "a", Op: "append", User: user, Password: password, File: file, Content: "-A"},
		WorkerSpec{Name: "b", Op: "append", User: user, Password: password, File: file, Content: "-B"},
	)
	requireOK(t, results)

	content, version := readFinalState(t, cluster)
	if strings.Contains(content, "-VICTIM") {
		t.Fatalf("the killed worker's append is present in %q", content)
	}
	if !strings.Contains(content, "-A") || !strings.Contains(content, "-B") {
		t.Fatalf("an append was lost after the crash: %q", content)
	}
	if version != 3 {
		t.Fatalf("version: got %d, want 3 (1 initial + 2 appends)", version)
	}
}

// TestLeaseSurvivesALongOperation is the control for the crash tests: a
// worker that keeps running and renewing is NOT revoked, even when its
// operation takes much longer than a lease.
//
// Without it, the tests above could be passing because leases expire
// indiscriminately rather than because a worker stopped renewing.
func TestLeaseSurvivesALongOperation(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	slow := cluster.StartPaused(WorkerSpec{
		Name: "slow-but-alive", Op: "append", User: user, Password: password,
		File: file, Content: "-SLOW", PauseAt: pauseTag,
		// Renewals continue: this worker is slow, not dead.
	})

	// Several lease lengths pass while it holds the lock.
	time.Sleep(4 * LeaseDuration)

	slow.Resume()
	result := slow.Wait()
	if !result.OK {
		t.Fatalf("a slow but healthy worker was revoked: %s", result.Error)
	}

	content, version := readFinalState(t, cluster)
	if !strings.Contains(content, "-SLOW") {
		t.Fatalf("the slow worker's append is missing from %q", content)
	}
	if version != 2 {
		t.Fatalf("version: got %d, want 2", version)
	}
}
