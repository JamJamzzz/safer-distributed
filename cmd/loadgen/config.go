package main

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// Workload is the mix of SAFER operations one loadgen run performs.
type Workload string

const (
	// WorkloadIndependentWrites gives each caller its own user and file, so
	// callers do not contend with each other. It exercises throughput
	// under concurrency with no lock waiting.
	WorkloadIndependentWrites Workload = "independent-writes"
	// WorkloadSameFileWrites has every caller append to ONE shared file
	// owned by ONE shared user. This is the workload that actually
	// exercises cross-replica strict 2PL: callers may be talking to
	// different worker replicas, and correctness depends entirely on the
	// coordinator serializing them, not on anything the workers share
	// in-process.
	WorkloadSameFileWrites Workload = "same-file-writes"
	// WorkloadReads has every caller repeatedly load one pre-populated
	// shared file. Reads take shared locks and do not contend with each
	// other.
	WorkloadReads Workload = "reads"
	// WorkloadMixed interleaves independent writes and reads.
	WorkloadMixed Workload = "mixed"
)

func (w Workload) valid() bool {
	switch w {
	case WorkloadIndependentWrites, WorkloadSameFileWrites, WorkloadReads, WorkloadMixed:
		return true
	default:
		return false
	}
}

// Config describes one load-generation run.
type Config struct {
	// Addresses are one or more worker Service addresses to dial. Real
	// deployments pass one address -- the Kubernetes Service DNS name,
	// which Kubernetes itself load-balances across worker replica pods.
	// More than one is accepted so this tool is also useful for local
	// testing without a Service in front of anything.
	Addresses []string

	Concurrency int
	// Count is the total number of operations across every caller. It is
	// ignored when Duration is positive.
	Count int
	// Duration, if positive, runs until it elapses instead of running a
	// fixed Count.
	Duration    time.Duration
	Workload    Workload
	Users       int
	ContentSize int
}

// parseConfig builds a Config from command-line flags.
func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	addr := fs.String("addr", "",
		"comma-separated worker Service address(es) to dial, e.g. safer-worker:50052")
	concurrency := fs.Int("concurrency", 10, "number of concurrent callers")
	count := fs.Int("count", 100,
		"total number of operations to run across all callers; ignored if -duration is positive")
	duration := fs.Duration("duration", 0, "run for this long instead of a fixed operation count")
	workload := fs.String("workload", string(WorkloadMixed),
		"workload type: independent-writes, same-file-writes, reads, or mixed")
	users := fs.Int("users", 0,
		"distinct SAFER users to create; defaults to -concurrency for independent-writes/mixed, "+
			"and is forced to 1 for same-file-writes/reads since those workloads share one user by design")
	contentSize := fs.Int("content-size", 64, "bytes written per StoreFile/AppendToFile call")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(*addr) == "" {
		return Config{}, fmt.Errorf("loadgen: -addr is required")
	}
	var addresses []string
	for _, a := range strings.Split(*addr, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addresses = append(addresses, a)
		}
	}
	if len(addresses) == 0 {
		return Config{}, fmt.Errorf("loadgen: -addr must contain at least one non-empty address")
	}

	if *concurrency <= 0 {
		return Config{}, fmt.Errorf("loadgen: -concurrency must be positive")
	}
	if *count <= 0 && *duration <= 0 {
		return Config{}, fmt.Errorf("loadgen: either -count or -duration must be positive")
	}
	if *contentSize <= 0 {
		return Config{}, fmt.Errorf("loadgen: -content-size must be positive")
	}

	wl := Workload(*workload)
	if !wl.valid() {
		return Config{}, fmt.Errorf("loadgen: unknown -workload %q", *workload)
	}

	userCount := *users
	switch wl {
	case WorkloadSameFileWrites, WorkloadReads:
		// These workloads are defined around one shared user and file;
		// a caller-supplied -users would just create users the run
		// never touches.
		userCount = 1
	default:
		if userCount <= 0 {
			userCount = *concurrency
		}
	}

	return Config{
		Addresses:   addresses,
		Concurrency: *concurrency,
		Count:       *count,
		Duration:    *duration,
		Workload:    wl,
		Users:       userCount,
		ContentSize: *contentSize,
	}, nil
}
