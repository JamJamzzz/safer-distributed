package main

import (
	"bytes"
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
//
// Attempted, Succeeded, and Failed are kept distinct on purpose: a run
// that attempted 1000 operations and failed 400 of them has a throughput
// of 600 operations, not 1000, and reporting Attempted as if it were a
// success count would overstate what actually happened -- exactly the
// mistake a later performance-tuning pass could otherwise inherit.
type Report struct {
	Workload  Workload
	Requested int // Count, or 0 if the run was duration-bounded
	Attempted int64
	Succeeded int64
	Failed    int64
	Elapsed   time.Duration

	FirstErrors []string // up to a handful of distinct error messages, for diagnosis

	// Replicas counts completed RPCs by the worker instance that served
	// them (see internal/workerdiag and cmd/worker/instance.go), keyed by
	// hostname-pid. This is the evidence for whatever balancing claim the
	// caller made when choosing -addr: more than one key here means more
	// than one worker process actually served this run, not just that
	// more than one address was configured.
	Replicas map[string]int64

	// OracleFailures is non-empty when a completed run's actual data
	// disagrees with what the recorded successful operations say should
	// be there -- see checkOracles. This is real evidence of corrupted or
	// lost data: the verification LoadFile succeeded, and the bytes it
	// returned are provably wrong.
	//
	// This is deliberately NOT the same bucket as VerificationErrors
	// below. An earlier version of checkOracles put both under one
	// "DATA CORRUPTION" label, which overstated what a failed
	// verification call actually shows: a worker that is merely
	// unavailable when checkOracles tries to LoadFile (a live OOM kill
	// mid-run, for instance) proves nothing about whether the data itself
	// is intact -- it might be perfectly fine and simply unreachable at
	// that moment.
	OracleFailures []string

	// VerificationErrors is non-empty when checkOracles could not
	// complete its check at all -- the verification LoadFile call itself
	// failed (RPC error, unavailable worker, timeout). This is still a
	// real problem worth a non-zero exit code, since a run this tool
	// cannot verify is not a run it can vouch for, but it is a weaker
	// claim than OracleFailures: it says "unknown", not "wrong".
	VerificationErrors []string
}

func (r Report) String() string {
	rate := float64(r.Succeeded) / r.Elapsed.Seconds()
	s := fmt.Sprintf("workload=%s attempted=%d succeeded=%d failed=%d elapsed=%s throughput=%.1f ops/s",
		r.Workload, r.Attempted, r.Succeeded, r.Failed, r.Elapsed.Round(time.Millisecond), rate)
	s += fmt.Sprintf("\nreplicas_served=%d %v", len(r.Replicas), r.Replicas)
	for _, e := range r.FirstErrors {
		s += fmt.Sprintf("\n  error: %s", e)
	}
	for _, f := range r.OracleFailures {
		s += fmt.Sprintf("\n  DATA CORRUPTION: %s", f)
	}
	for _, e := range r.VerificationErrors {
		s += fmt.Sprintf("\n  VERIFICATION ERROR: %s", e)
	}
	return s
}

// user is one SAFER identity the run drives requests as. successfulAppends
// is read and written with the atomic package, since several goroutines
// can share one user (WorkloadSameFileWrites forces exactly one shared
// user; independent-writes and mixed round-robin cfg.Users < cfg.Concurrency
// callers over the same set).
type user struct {
	username string
	password string
	filename string

	successfulAppends int64
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
func setup(ctx context.Context, cfg Config, client workerv1.SaferWorkerClient, tally *replicaTally) ([]*user, error) {
	users := make([]*user, cfg.Users)
	initialContent := make([]byte, cfg.ContentSize)

	for i := range users {
		u := &user{
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
func callerUser(users []*user, callerIndex int) *user {
	return users[callerIndex%len(users)]
}

// runOne performs a single operation for the given caller against its
// user, per the workload's definition, records which replica served it
// and, for a successful append, that it happened (u.successfulAppends is
// the oracle in checkOracles' input) -- and, for a read, checks the
// returned content against the one value this workload's file can ever
// legitimately hold, since WorkloadReads never mutates anything after
// setup. It returns an error for either an RPC failure or a read that
// came back wrong; both are real correctness failures a caller cares
// about, whatever the RPC status said.
func runOne(ctx context.Context, cfg Config, client workerv1.SaferWorkerClient, tally *replicaTally, u *user, content, initialContent []byte) error {
	var header metadata.MD
	var err error

	switch cfg.Workload {
	case WorkloadIndependentWrites, WorkloadSameFileWrites:
		_, err = client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename, Content: content,
		}, grpc.Header(&header))
		if err == nil {
			atomic.AddInt64(&u.successfulAppends, 1)
		}
	case WorkloadReads:
		var resp *workerv1.LoadFileResponse
		resp, err = client.LoadFile(ctx, &workerv1.LoadFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename,
		}, grpc.Header(&header))
		if err == nil && !bytes.Equal(resp.GetContent(), initialContent) {
			err = fmt.Errorf("read returned %d bytes not matching the file's only ever-written content (%d bytes)",
				len(resp.GetContent()), len(initialContent))
		}
	case WorkloadMixed:
		if rand.Intn(2) == 0 {
			_, err = client.AppendToFile(ctx, &workerv1.AppendToFileRequest{
				Username: u.username, Password: u.password, Filename: u.filename, Content: content,
			}, grpc.Header(&header))
			if err == nil {
				atomic.AddInt64(&u.successfulAppends, 1)
			}
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

// checkOracles is what makes this tool's correctness claim real rather
// than assumed from RPC status codes.
//
// For any workload that writes (independent-writes, same-file-writes,
// mixed), every user's file must, after the run, be exactly its initial
// content plus one contentSize-sized chunk per append this run recorded
// as successful for that user:
//
//	final length = initial length + successful append count * append size
//
// A silent lost update -- an append whose RPC reported success but whose
// bytes never actually landed, or that a concurrent write clobbered --
// changes the file's length and is caught here. This is the one point in
// the tool that asks "is the data really there", instead of only "did the
// RPC return an error"; nothing about a clean error count from run()
// implies the data is correct without this also passing.
//
// WorkloadReads needs no separate check here: runOne already verifies
// every read inline (a mismatch there is a Failed RPC, exactly where it
// belongs, not a separate oracle pass), since a read that returns the
// wrong bytes is detected the moment it happens, with no need to wait for
// the run to finish.
// checkOracles returns (dataFailures, verificationErrors). The two are
// kept separate because they are different claims: a dataFailure means
// checkOracles successfully read the file and the bytes are provably
// wrong (real corruption or a lost/duplicated write); a verificationError
// means the check itself could not run -- most commonly the verification
// LoadFile call failing because a worker is unavailable -- which says
// nothing about whether the data is actually intact. Folding a failed
// verification call into "data corruption" (an earlier version of this
// function did) overstates what was observed: a worker that is down
// cannot have its file's bytes inspected at all, corrupted or not.
func checkOracles(ctx context.Context, cfg Config, client workerv1.SaferWorkerClient, users []*user) (dataFailures, verificationErrors []string) {
	if cfg.Workload == WorkloadReads {
		return nil, nil
	}

	seen := make(map[string]bool, len(users))
	for _, u := range users {
		if seen[u.username] {
			continue
		}
		seen[u.username] = true

		successes := atomic.LoadInt64(&u.successfulAppends)
		want := int64(cfg.ContentSize) + successes*int64(cfg.ContentSize)

		loadCtx, cancel := context.WithTimeout(ctx, callTimeout)
		resp, err := client.LoadFile(loadCtx, &workerv1.LoadFileRequest{
			Username: u.username, Password: u.password, Filename: u.filename,
		})
		cancel()
		if err != nil {
			verificationErrors = append(verificationErrors,
				fmt.Sprintf("user %s: could not verify final content: %v", u.username, err))
			continue
		}

		if got := int64(len(resp.GetContent())); got != want {
			dataFailures = append(dataFailures, fmt.Sprintf(
				"user %s: file is %d bytes, want %d (initial %d + %d successful append(s) x %d bytes)",
				u.username, got, want, cfg.ContentSize, successes, cfg.ContentSize))
		}
	}
	return dataFailures, verificationErrors
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
	initialContent := make([]byte, cfg.ContentSize) // all-zero, exactly what setup wrote

	var attempted, succeeded, failed int64
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
				err := runOne(callCtx, cfg, client, tally, u, content, initialContent)
				cancel()

				atomic.AddInt64(&attempted, 1)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					recordErr(err)
				} else {
					atomic.AddInt64(&succeeded, 1)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Oracle verification happens after the timed run, exactly like
	// setup: it is a correctness check, not load, and must not be
	// counted as either.
	oracleFailures, verificationErrors := checkOracles(ctx, cfg, client, users)

	return Report{
		Workload:           cfg.Workload,
		Requested:          cfg.Count,
		Attempted:          attempted,
		Succeeded:          succeeded,
		Failed:             failed,
		Elapsed:            elapsed,
		FirstErrors:        firstErrors,
		Replicas:           tally.snapshot(),
		OracleFailures:     oracleFailures,
		VerificationErrors: verificationErrors,
	}, nil
}
