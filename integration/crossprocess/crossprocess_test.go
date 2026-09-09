package crossprocess

import (
	"strings"
	"testing"

	"github.com/JamJamzzz/safer-distributed/client"
)

// Cross-process coordination tests.
//
// Each test runs one lock coordinator process and two independent SAFER
// worker processes against one MongoDB. The workers rendezvous at a
// barrier and are released together, so their operations really overlap;
// no test uses a sleep to create the race.
//
// What these tests establish: SAFER's strict-2PL semantics hold across
// separate OS processes under normal, graceful operation. What they do NOT
// establish: anything about crashes, partitions, or recovery. A worker
// that dies without ending its transaction still leaks its locks, and a
// coordinator crash still loses all lock state. Leases and fencing tokens
// are the next phase.
//
// To run:
//
//	docker run -d -p 27017:27017 --name safer-mongo mongo:7
//	SAFER_MONGO_URI=mongodb://localhost:27017 go test ./integration/crossprocess/

const (
	user     = "alice"
	password = "alice-password"
	file     = "shared-file"
	base     = "base"
)

// setupFile creates the user and a one-write file, from the test process.
func setupFile(t *testing.T, cluster *Cluster) {
	t.Helper()
	cluster.Setup(func(t *testing.T) {
		alice, err := client.InitUser(user, password)
		if err != nil {
			t.Fatalf("InitUser: %v", err)
		}
		if err := alice.StoreFile(file, []byte(base)); err != nil {
			t.Fatalf("StoreFile: %v", err)
		}
		snapshot, err := alice.ReadFileMetadata(file)
		if err != nil {
			t.Fatalf("ReadFileMetadata: %v", err)
		}
		if snapshot.Version != 1 {
			t.Fatalf("initial version: got %d, want 1", snapshot.Version)
		}
	})
}

// readFinalState reads the committed file from the test process, after
// every worker has exited.
func readFinalState(t *testing.T, cluster *Cluster) (content string, version uint64) {
	t.Helper()
	cluster.Setup(func(t *testing.T) {
		alice, err := client.GetUser(user, password)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		loaded, err := alice.LoadFile(file)
		if err != nil {
			t.Fatalf("LoadFile: %v", err)
		}
		snapshot, err := alice.ReadFileMetadata(file)
		if err != nil {
			t.Fatalf("ReadFileMetadata: %v", err)
		}
		content = string(loaded)
		version = snapshot.Version
	})
	return content, version
}

func requireOK(t *testing.T, results []WorkerResult) {
	t.Helper()
	for _, r := range results {
		if !r.OK {
			t.Fatalf("worker %q (%s) failed: %s", r.Worker, r.Op, r.Error)
		}
	}
}

// TestCrossProcessAppendVsAppend is the central lost-update case: two
// separate processes append to the same file at the same moment.
//
// Both appends must land. The version is the oracle -- SAFER publishes
// exactly one increment per committed content mutation, so a lost update
// shows up as a version that advanced once instead of twice, and as
// content missing one worker's bytes.
func TestCrossProcessAppendVsAppend(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	results := cluster.RunConcurrently(
		WorkerSpec{Name: "appender-a", Op: "append", User: user, Password: password, File: file, Content: "-AAA"},
		WorkerSpec{Name: "appender-b", Op: "append", User: user, Password: password, File: file, Content: "-BBB"},
	)
	requireOK(t, results)

	if !Overlapped(results[0], results[1]) {
		t.Error("the two appends did not overlap in time; the barrier did not force a race")
	}

	content, version := readFinalState(t, cluster)

	// Both appends landed, in one order or the other.
	if !strings.Contains(content, "-AAA") || !strings.Contains(content, "-BBB") {
		t.Fatalf("lost update: final content %q is missing an append", content)
	}
	if !strings.HasPrefix(content, base) {
		t.Fatalf("final content %q does not start with the base content", content)
	}
	if want := base + "-AAA-BBB"; content != want {
		if other := base + "-BBB-AAA"; content != other {
			t.Fatalf("final content %q is neither serial order (%q or %q)", content, want, other)
		}
	}

	// Two committed mutations on top of the initial version.
	if version != 3 {
		t.Fatalf("version: got %d, want 3 (1 initial + 2 appends); a lower value means a lost update", version)
	}
}

// TestCrossProcessAppendVsOverwrite pits an append against a full
// overwrite from another process. Both are exclusive writers, so one must
// be fully ordered before the other; the result must be one of the two
// serial outcomes and never a mixture.
func TestCrossProcessAppendVsOverwrite(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	results := cluster.RunConcurrently(
		WorkerSpec{Name: "appender", Op: "append", User: user, Password: password, File: file, Content: "-APPENDED"},
		WorkerSpec{Name: "overwriter", Op: "store", User: user, Password: password, File: file, Content: "REPLACED"},
	)
	requireOK(t, results)

	if !Overlapped(results[0], results[1]) {
		t.Error("the append and overwrite did not overlap in time")
	}

	content, version := readFinalState(t, cluster)

	// Exactly two serial outcomes are legal:
	//   append then overwrite -> "REPLACED"
	//   overwrite then append -> "REPLACED-APPENDED"
	switch content {
	case "REPLACED", "REPLACED-APPENDED":
	default:
		t.Fatalf("final content %q is not a serial outcome of append and overwrite", content)
	}

	// A half-applied interleaving would show up as content that has the
	// appended bytes attached to the pre-overwrite base.
	if strings.HasPrefix(content, base) {
		t.Fatalf("final content %q still starts with the pre-overwrite base; the overwrite was partially lost", content)
	}
	if version != 3 {
		t.Fatalf("version: got %d, want 3 (1 initial + 2 mutations)", version)
	}
}

// TestCrossProcessLoadVsOverwrite checks the reader side: a read
// overlapping a full overwrite must observe one complete logical state,
// never a half-rewritten file.
//
// Which state it sees depends on lock ordering and is not asserted; that
// it is a whole, self-consistent state is.
func TestCrossProcessLoadVsOverwrite(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	results := cluster.RunConcurrently(
		WorkerSpec{Name: "reader", Op: "load", User: user, Password: password, File: file},
		WorkerSpec{Name: "overwriter", Op: "store", User: user, Password: password, File: file, Content: "COMPLETELY-REPLACED-CONTENT"},
	)
	requireOK(t, results)

	if !Overlapped(results[0], results[1]) {
		t.Error("the load and overwrite did not overlap in time")
	}

	// The reader saw exactly one of the two committed states, whole.
	reader := results[0]
	switch reader.Content {
	case base, "COMPLETELY-REPLACED-CONTENT":
	default:
		t.Fatalf("reader observed %q, which is neither the pre- nor the post-overwrite state; "+
			"it read a partially applied mutation", reader.Content)
	}

	content, version := readFinalState(t, cluster)
	if content != "COMPLETELY-REPLACED-CONTENT" {
		t.Fatalf("final content: got %q, want the overwritten content", content)
	}
	if version != 2 {
		t.Fatalf("version: got %d, want 2 (1 initial + 1 overwrite); a read must not publish a version", version)
	}
}

// TestCrossProcessManyAppendersNoLostWrites raises the contention: five
// independent processes append to one file simultaneously. Every append
// must land exactly once.
func TestCrossProcessManyAppendersNoLostWrites(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	const appenders = 5
	specs := make([]WorkerSpec, 0, appenders)
	fragments := make([]string, 0, appenders)
	for i := 0; i < appenders; i++ {
		fragment := "-W" + string(rune('A'+i))
		fragments = append(fragments, fragment)
		specs = append(specs, WorkerSpec{
			Name: "appender-" + string(rune('A'+i)), Op: "append",
			User: user, Password: password, File: file, Content: fragment,
		})
	}

	results := cluster.RunConcurrently(specs...)
	requireOK(t, results)

	content, version := readFinalState(t, cluster)

	for _, fragment := range fragments {
		if got := strings.Count(content, fragment); got != 1 {
			t.Fatalf("fragment %q appears %d times in %q, want exactly 1", fragment, got, content)
		}
	}
	if want := len(base) + appenders*3; len(content) != want {
		t.Fatalf("final content %q has length %d, want %d; content was lost or duplicated",
			content, len(content), want)
	}
	if want := uint64(1 + appenders); version != want {
		t.Fatalf("version: got %d, want %d (1 initial + %d appends)", version, want, appenders)
	}
}

// TestCrossProcessWorkersShareOneCoordinator is the control for these
// tests: it confirms the workers really are coordinating through the
// shared coordinator, by observing that concurrent exclusive writers on
// the same file do not overlap their critical sections.
//
// Without shared coordination, two processes would enter their critical
// sections simultaneously; the lost-update assertions above would then be
// passing by luck rather than by locking.
func TestCrossProcessWorkersShareOneCoordinator(t *testing.T) {
	cluster := Start(t)
	setupFile(t, cluster)

	results := cluster.RunConcurrently(
		WorkerSpec{Name: "writer-a", Op: "append", User: user, Password: password, File: file, Content: "-A"},
		WorkerSpec{Name: "writer-b", Op: "append", User: user, Password: password, File: file, Content: "-B"},
	)
	requireOK(t, results)

	// The workers were released together, so their runs overlap; the
	// mutations inside them must still have been serialized, which the
	// version count proves.
	_, version := readFinalState(t, cluster)
	if version != 3 {
		t.Fatalf("version: got %d, want 3; the two writers were not serialized", version)
	}
	if !Overlapped(results[0], results[1]) {
		t.Error("workers did not overlap; this test asserts nothing without contention")
	}
}
