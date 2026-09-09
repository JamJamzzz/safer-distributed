package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

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
}

func (r Report) String() string {
	rate := float64(r.Completed) / r.Elapsed.Seconds()
	s := fmt.Sprintf("workload=%s completed=%d errors=%d elapsed=%s throughput=%.1f ops/s",
		r.Workload, r.Completed, r.Errors, r.Elapsed.Round(time.Millisecond), rate)
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

// connPicker round-robins across the dialed worker addresses. A real
// deployment passes one address (the Kubernetes Service, which Kubernetes
// itself load-balances); this exists so the same tool is useful for local
// testing without a Service in front of anything, and so a single loadgen
// process is not pinned to one worker replica by construction.
type connPicker struct {
	clients []workerv1.SaferWorkerClient
	next    uint64
}

func newConnPicker(conns []*grpc.ClientConn) *connPicker {
	clients := make([]workerv1.SaferWorkerClient, len(conns))
	for i, c := range conns {
		clients[i] = workerv1.NewSaferWorkerClient(c)
	}
	return &connPicker{clients: clients}
}

func (p *connPicker) pick() workerv1.SaferWorkerClient {
	i := atomic.AddUint64(&p.next, 1) - 1
	return p.clients[i%uint64(len(p.clients))]
}

// setup prepares the users and initial file content a workload needs
// before the timed run starts, so setup latency (InitUser is intentionally
// expensive -- see client.InitUser -- and StoreFile is a full multi-object
// mutation) is never counted as load.
func setup(ctx context.Context, cfg Config, picker *connPicker) ([]user, error) {
	users := make([]user, cfg.Users)
	initialContent := make([]byte, cfg.ContentSize)

	for i := range users {
		u := user{
			username: fmt.Sprintf("loadgen-%d-%d", time.Now().UnixNano(), i),
			password: fmt.Sprintf("password-%d", i),
			filename: "loadgen.dat",
		}
		if _, err := picker.pick().InitUser(ctx, &workerv1.InitUserRequest{
			Username: u.username, Password: u.password,
		}); err != nil {
			return nil, fmt.Errorf("setup: InitUser %d: %w", i, err)
		}

		switch cfg.Workload {
		case WorkloadSameFileWrites, WorkloadReads, WorkloadMixed, WorkloadIndependentWrites:
			if _, err := picker.pick().StoreFile(ctx, &workerv1.StoreFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename, Content: initialContent,
			}); err != nil {
				return nil, fmt.Errorf("setup: StoreFile %d: %w", i, err)
			}
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
// user, per the workload's definition, and reports whether it succeeded.
func runOne(ctx context.Context, cfg Config, picker *connPicker, u user, content []byte) error {
	client := picker.pick()

	switch cfg.Workload {
	case WorkloadIndependentWrites, WorkloadSameFileWrites:
		_, err := client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename, Content: content,
		})
		return err
	case WorkloadReads:
		_, err := client.LoadFile(ctx, &workerv1.LoadFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename,
		})
		return err
	case WorkloadMixed:
		if rand.Intn(2) == 0 {
			_, err := client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename, Content: content,
			})
			return err
		}
		_, err := client.LoadFile(ctx, &workerv1.LoadFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename,
		})
		return err
	default:
		return fmt.Errorf("loadgen: unknown workload %q", cfg.Workload)
	}
}

// run executes cfg's workload against picker and returns a Report.
//
// Work is divided by a shared atomic counter, not by giving each goroutine
// a fixed slice of Count: a caller stalled behind another's exclusive lock
// (WorkloadSameFileWrites, by design) should not leave its share of the
// work undone while idle goroutines have nothing left to do.
func run(ctx context.Context, cfg Config, picker *connPicker) (Report, error) {
	users, err := setup(ctx, cfg, picker)
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
				err := runOne(callCtx, cfg, picker, u, content)
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
	}, nil
}
