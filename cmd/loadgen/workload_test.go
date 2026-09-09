package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"

	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

// fakeWorkerClient is an in-memory workerv1.SaferWorkerClient, so
// checkOracles and runOne can be tested against controlled, deterministic
// storage behavior without a real worker, coordinator, or MongoDB --
// including the one behavior that matters most and is hardest to force
// against a real stack on demand: an AppendToFile call that reports
// success without its bytes actually landing.
type fakeWorkerClient struct {
	mu    sync.Mutex
	files map[string][]byte

	// dropAppendNumber, if positive, makes the dropAppendNumber-th
	// AppendToFile call across the whole fake (1-based) report success
	// without writing anything -- a synthetic lost update.
	dropAppendNumber int32
	appendCalls      int32
}

func newFakeWorkerClient() *fakeWorkerClient {
	return &fakeWorkerClient{files: make(map[string][]byte)}
}

func (f *fakeWorkerClient) InitUser(context.Context, *workerv1.InitUserRequest, ...grpc.CallOption) (*workerv1.InitUserResponse, error) {
	return &workerv1.InitUserResponse{}, nil
}

func (f *fakeWorkerClient) StoreFile(_ context.Context, in *workerv1.StoreFileRequest, _ ...grpc.CallOption) (*workerv1.StoreFileResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[in.GetUsername()] = append([]byte(nil), in.GetContent()...)
	return &workerv1.StoreFileResponse{}, nil
}

func (f *fakeWorkerClient) AppendToFile(_ context.Context, in *workerv1.AppendToFileRequest, _ ...grpc.CallOption) (*workerv1.AppendToFileResponse, error) {
	n := atomic.AddInt32(&f.appendCalls, 1)
	if f.dropAppendNumber > 0 && n == f.dropAppendNumber {
		// The lost update itself: report success, write nothing.
		return &workerv1.AppendToFileResponse{}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[in.GetUsername()] = append(f.files[in.GetUsername()], in.GetContent()...)
	return &workerv1.AppendToFileResponse{}, nil
}

func (f *fakeWorkerClient) LoadFile(_ context.Context, in *workerv1.LoadFileRequest, _ ...grpc.CallOption) (*workerv1.LoadFileResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &workerv1.LoadFileResponse{Content: append([]byte(nil), f.files[in.GetUsername()]...)}, nil
}

func testUsers(t *testing.T, client workerv1.SaferWorkerClient, cfg Config, n int) []*user {
	t.Helper()
	users, err := setup(context.Background(), cfg, client, newReplicaTally())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if len(users) != n {
		t.Fatalf("setup returned %d users, want %d", len(users), n)
	}
	return users
}

// TestCheckOracles_PassesWhenEveryAppendLands is the control: with no
// lost updates, checkOracles must find nothing wrong.
func TestCheckOracles_PassesWhenEveryAppendLands(t *testing.T) {
	client := newFakeWorkerClient()
	cfg := Config{Workload: WorkloadIndependentWrites, Users: 2, ContentSize: 8}
	users := testUsers(t, client, cfg, 2)
	content := make([]byte, cfg.ContentSize)
	tally := newReplicaTally()

	for _, u := range users {
		for i := 0; i < 3; i++ {
			if err := runOne(context.Background(), cfg, client, tally, u, content, content); err != nil {
				t.Fatalf("runOne: %v", err)
			}
		}
	}

	failures := checkOracles(context.Background(), cfg, client, users)
	if len(failures) != 0 {
		t.Fatalf("checkOracles reported failures with no lost updates: %v", failures)
	}
}

// TestCheckOracles_DetectsLostUpdate is the property this file exists
// for: an AppendToFile call that reports success while silently not
// writing its bytes -- indistinguishable from a real lost update under
// broken concurrency control -- must make checkOracles fail, even though
// runOne saw no error at all.
func TestCheckOracles_DetectsLostUpdate(t *testing.T) {
	client := newFakeWorkerClient()
	cfg := Config{Workload: WorkloadIndependentWrites, Users: 1, ContentSize: 8}
	users := testUsers(t, client, cfg, 1)
	content := make([]byte, cfg.ContentSize)
	tally := newReplicaTally()

	// Drop the second append across the fake: the third AppendToFile call
	// this test makes (setup's StoreFile does not go through
	// AppendToFile, so the counter starts at zero here).
	client.dropAppendNumber = 2

	for i := 0; i < 4; i++ {
		if err := runOne(context.Background(), cfg, client, tally, users[0], content, content); err != nil {
			t.Fatalf("runOne %d: %v", i, err)
		}
	}

	// runOne reported all 4 as successful -- including the dropped one,
	// exactly like a real RPC that returns success while a concurrent
	// writer's commit clobbers it.
	if got := atomic.LoadInt64(&users[0].successfulAppends); got != 4 {
		t.Fatalf("successfulAppends = %d, want 4 (runOne must not itself detect the drop)", got)
	}

	failures := checkOracles(context.Background(), cfg, client, users)
	if len(failures) != 1 {
		t.Fatalf("checkOracles found %d failures, want exactly 1: %v", len(failures), failures)
	}
	if !strings.Contains(failures[0], users[0].username) {
		t.Errorf("failure message does not name the affected user: %q", failures[0])
	}
	// 4 reported successes at 8 bytes each, but only 3 actually landed:
	// the file is 8 bytes short of what checkOracles expects.
	wantLen := cfg.ContentSize + 4*cfg.ContentSize
	gotLen := wantLen - cfg.ContentSize
	if !strings.Contains(failures[0], strconv.Itoa(gotLen)) || !strings.Contains(failures[0], strconv.Itoa(wantLen)) {
		t.Errorf("failure message %q does not mention both the actual (%d) and expected (%d) length",
			failures[0], gotLen, wantLen)
	}
}

// TestRunOne_ReadsWorkload_DetectsWrongContent proves the OTHER oracle:
// WorkloadReads checks every read inline rather than at the end, since
// nothing about that workload ever changes the file again to check later.
func TestRunOne_ReadsWorkload_DetectsWrongContent(t *testing.T) {
	client := newFakeWorkerClient()
	cfg := Config{Workload: WorkloadReads, Users: 1, ContentSize: 8}
	users := testUsers(t, client, cfg, 1)
	tally := newReplicaTally()
	initialContent := make([]byte, cfg.ContentSize)

	// A correct read must not error.
	if err := runOne(context.Background(), cfg, client, tally, users[0], nil, initialContent); err != nil {
		t.Fatalf("runOne with correct stored content: %v", err)
	}

	// Corrupt the backing store directly, simulating a worker that
	// returned the wrong bytes for a read -- no RPC-level error, just
	// wrong data, which is exactly what a status-code-only check would
	// miss.
	client.mu.Lock()
	client.files[users[0].username] = []byte("wrong!!!")
	client.mu.Unlock()

	if err := runOne(context.Background(), cfg, client, tally, users[0], nil, initialContent); err == nil {
		t.Fatal("runOne did not detect a read returning the wrong content")
	}
}
