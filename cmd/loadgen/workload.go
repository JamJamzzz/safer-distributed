package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/JamJamzzz/safer-distributed/internal/workerdiag"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

// callTimeout bounds one RPC. It is generous: under WorkloadSameFileWrites
// a caller may legitimately wait behind another caller's exclusive lock,
// and the coordinator's Acquire itself has no client-side deadline (see
// client/coordination/grpccoord/client.go) -- but a load-generation run
// still needs a bound so one wedged call cannot hang the whole tool.
const callTimeout = 30 * time.Second

// Report summarizes one run.
type Report struct {
	Workload    Workload
	Requested   int // Count, or 0 if the run was duration-bounded
	Completed   int64
	Errors      int64
	Elapsed     time.Duration
	FirstErrors []string // up to a handful of distinct error messages, for diagnosis

	// Replicas counts completed RPCs by the worker instance that served
	// them (see internal/workerdiag and cmd/worker/instance.go), keyed by
	// hostname-pid. This is the evidence for whatever balancing claim the
	// caller made when choosing -addr: more than one key here means more
	// than one worker process actually served this run, not just that
	// more than one address was configured.
	Replicas map[string]int64
}

func (r Report) String() string {
	rate := float64(r.Completed) / r.Elapsed.Seconds()
	s := fmt.Sprintf("workload=%s completed=%d errors=%d elapsed=%s throughput=%.1f ops/s",
		r.Workload, r.Completed, r.Errors, r.Elapsed.Round(time.Millisecond), rate)
	s += fmt.Sprintf("\nreplicas_served=%d %v", len(r.Replicas), r.Replicas)
	for _, e := range r.FirstErrors {
		s += fmt.Sprintf("\n  error: %s", e)
	}
	return s
}

// user is one SAFER identity the run drives requests as.
type user struct {
	username string
	password string
	filename string
}

// replicaTally counts, per worker instance, how many RPCs it actually
// served -- read from the response header cmd/worker's
// instanceHeaderInterceptor attaches to every call, never assumed from
// how many addresses were dialed.
type replicaTally struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newReplicaTally() *replicaTally {
	return &replicaTally{counts: make(map[string]int64)}
}

func (rt *replicaTally) record(header metadata.MD) {
	ids := header.Get(workerdiag.InstanceHeaderKey)
	if len(ids) == 0 {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.counts[ids[0]]++
}

func (rt *replicaTally) snapshot() map[string]int64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make(map[string]int64, len(rt.counts))
	for k, v := range rt.counts {
		out[k] = v
	}
	return out
}

// setup prepares the users and initial file content a workload needs
// before the timed run starts, so setup latency (InitUser is intentionally
// expensive -- see client.InitUser -- and StoreFile is a full multi-object
// mutation) is never counted as load.
func setup(ctx context.Context, cfg Config, client workerv1.SaferWorkerClient, tally *replicaTally) ([]user, error) {
	users := make([]user, cfg.Users)
	initialContent := make([]byte, cfg.ContentSize)

	for i := range users {
		u := user{
			username: fmt.Sprintf("loadgen-%d-%d", time.Now().UnixNano(), i),
			password: fmt.Sprintf("password-%d", i),
			filename: "loadgen.dat",
		}
		var header metadata.MD
		if _, err := client.InitUser(ctx, &workerv1.InitUserRequest{
			Username: u.username, Password: u.password,
		}, grpc.Header(&header)); err != nil {
			return nil, fmt.Errorf("setup: InitUser %d: %w", i, err)
		}
		tally.record(header)

		switch cfg.Workload {
		case WorkloadSameFileWrites, WorkloadReads, WorkloadMixed, WorkloadIndependentWrites:
			header = nil
			if _, err := client.StoreFile(ctx, &workerv1.StoreFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename, Content: initialContent,
			}, grpc.Header(&header)); err != nil {
				return nil, fmt.Errorf("setup: StoreFile %d: %w", i, err)
			}
			tally.record(header)
		}
		users[i] = u
	}
	return users, nil
}

// callerUser assigns each concurrent caller a user according to the
// workload's sharing shape: independent-writes and mixed give every
// caller its own user (round-robin over cfg.Users, which may be fewer than
// cfg.Concurrency), while same-file-writes and reads use the single shared
// user every caller was already forced onto in parseConfig.
func callerUser(users []user, callerIndex int) user {
	return users[callerIndex%len(users)]
}

// runOne performs a single operation for the given caller against its
// user, per the workload's definition, records which replica served it,
// and reports whether it succeeded.
func runOne(ctx context.Context, cfg Config, client workerv1.SaferWorkerClient, tally *replicaTally, u user, content []byte) error {
	var header metadata.MD
	var err error

	switch cfg.Workload {
	case WorkloadIndependentWrites, WorkloadSameFileWrites:
		_, err = client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename, Content: content,
		}, grpc.Header(&header))
	case WorkloadReads:
		_, err = client.LoadFile(ctx, &workerv1.LoadFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename,
		}, grpc.Header(&header))
	case WorkloadMixed:
		if rand.Intn(2) == 0 {
			_, err = client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename, Content: content,
			}, grpc.Header(&header))
		} else {
			_, err = client.LoadFile(ctx, &workerv1.LoadFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename,
			}, grpc.Header(&header))
		}
	default:
		return fmt.Errorf("loadgen: unknown workload %q", cfg.Workload)
	}

	tally.record(header)
	return err
}

// run executes cfg's workload against conn and returns a Report.
//
// Work is divided by a shared atomic counter, not by giving each goroutine
// a fixed slice of Count: a caller stalled behind another's exclusive lock
// (WorkloadSameFileWrites, by design) should not leave its share of the
// work undone while idle goroutines have nothing left to do.
func run(ctx context.Context, cfg Config, conn *grpc.ClientConn) (Report, error) {
	client := workerv1.NewSaferWorkerClient(conn)
	tally := newReplicaTally()

	users, err := setup(ctx, cfg, client, tally)
	if err != nil {
		return Report{}, err
	}

	content := make([]byte, cfg.ContentSize)

	var completed, failed int64
	var errMu sync.Mutex
	var firstErrors []string
	recordErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if len(firstErrors) < 5 {
			firstErrors = append(firstErrors, err.Error())
		}
	}

	deadline := time.Time{}
	if cfg.Duration > 0 {
		deadline = time.Now().Add(cfg.Duration)
	}
	var remaining int64
	if cfg.Duration <= 0 {
		remaining = int64(cfg.Count)
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(callerIndex int) {
			defer wg.Done()
			u := callerUser(users, callerIndex)
			for {
				if cfg.Duration > 0 {
					if time.Now().After(deadline) {
						return
					}
				} else if atomic.AddInt64(&remaining, -1) < 0 {
					return
				}

				callCtx, cancel := context.WithTimeout(ctx, callTimeout)
				err := runOne(callCtx, cfg, client, tally, u, content)
				cancel()

				atomic.AddInt64(&completed, 1)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					recordErr(err)
				}
			}
		}(i)
	}
	wg.Wait()

	return Report{
		Workload:    cfg.Workload,
		Requested:   cfg.Count,
		Completed:   completed,
		Errors:      failed,
		Elapsed:     time.Since(start),
		FirstErrors: firstErrors,
		Replicas:    tally.snapshot(),
	}, nil
}
