package main

// Tests for the unsampled per-operation latency measurement added for the
// distributed evidence phase. The point of that measurement is that
// Datadog's indexed APM spans are a retained subset and cannot support
// benchmark-wide percentiles, so these numbers must come from the load
// generator itself -- which makes "which operations are counted" a
// correctness property worth pinning, not a detail.

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }

	// 1ms..100ms, already sorted: the indices are easy to reason about, so
	// a wrong quantile convention shows up as an obviously wrong number.
	hundred := make([]time.Duration, 0, 100)
	for i := 1; i <= 100; i++ {
		hundred = append(hundred, ms(i))
	}

	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{"empty has no percentile", nil, 0.50, 0},
		{"empty p99", []time.Duration{}, 0.99, 0},
		{"single sample is every percentile p50", []time.Duration{ms(7)}, 0.50, ms(7)},
		{"single sample is every percentile p95", []time.Duration{ms(7)}, 0.95, ms(7)},
		{"single sample is every percentile p99", []time.Duration{ms(7)}, 0.99, ms(7)},
		// Two samples: the truncated-index convention puts p95 and p99 on
		// the lower element. Pinned deliberately so the small-sample
		// behavior is documented rather than discovered later.
		{"two samples p50", []time.Duration{ms(1), ms(2)}, 0.50, ms(1)},
		{"two samples p95", []time.Duration{ms(1), ms(2)}, 0.95, ms(1)},
		{"two samples p99", []time.Duration{ms(1), ms(2)}, 0.99, ms(1)},
		{"hundred samples p50", hundred, 0.50, ms(50)},
		{"hundred samples p95", hundred, 0.95, ms(95)},
		{"hundred samples p99", hundred, 0.99, ms(99)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.p); got != tc.want {
				t.Errorf("percentile(p=%.2f) = %s, want %s", tc.p, got, tc.want)
			}
		})
	}
}

// TestPercentile_CallerSortsFirst documents percentile's contract: it takes
// an already-sorted slice (runWorkloadClient sorts once after collection
// rather than per sample), and sorting an unordered set first produces the
// same answers as the ordered case above.
func TestPercentile_CallerSortsFirst(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }

	unsorted := []time.Duration{ms(50), ms(1), ms(99), ms(95), ms(2)}
	sorted := append([]time.Duration(nil), unsorted...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	// sorted is [1, 2, 50, 95, 99]; for n=5 the index is int(p*4), so
	// p50 -> 2, and both p95 and p99 -> 3.
	if got, want := percentile(sorted, 0.50), ms(50); got != want {
		t.Errorf("p50 = %s, want %s", got, want)
	}
	if got, want := percentile(sorted, 0.95), ms(95); got != want {
		t.Errorf("p95 = %s, want %s", got, want)
	}
	if got, want := percentile(sorted, 0.99), ms(95); got != want {
		t.Errorf("p99 = %s, want %s", got, want)
	}
}

// TestReport_IncludesLatencyPercentiles pins the report line an evidence
// table will later be pasted from, and confirms the pre-existing fields
// were not displaced by it.
func TestReport_IncludesLatencyPercentiles(t *testing.T) {
	report := Report{
		Workload:       WorkloadSameFileWrites,
		Attempted:      200,
		Succeeded:      200,
		LatencySamples: 200,
		P50Latency:     12 * time.Millisecond,
		P95Latency:     480 * time.Millisecond,
		P99Latency:     1500 * time.Millisecond,
		Replicas:       map[string]int64{"worker-a-1": 200},
		Elapsed:        time.Second,
	}

	s := report.String()
	for _, want := range []string{
		"latency_samples=200",
		"p50=12ms",
		"p95=480ms",
		"p99=1.5s",
		// Pre-existing output must survive unchanged.
		"attempted=200",
		"succeeded=200",
		"failed=0",
		"replicas_served=1",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("report output missing %q:\n%s", want, s)
		}
	}
}

// TestReport_ZeroSamplesFormatsCleanly covers the degenerate run (every
// operation failed, or none ran): the line must still render rather than
// panic or print something unparseable.
func TestReport_ZeroSamplesFormatsCleanly(t *testing.T) {
	report := Report{Replicas: map[string]int64{}, Elapsed: time.Second}
	s := report.String()
	if !strings.Contains(s, "latency_samples=0") {
		t.Errorf("zero-sample report does not report a zero sample count:\n%s", s)
	}
	if !strings.Contains(s, "p50=0s") {
		t.Errorf("zero-sample report does not render a zero p50:\n%s", s)
	}
}

// TestRunWorkloadClient_SuccessfulOperationsProduceLatencySamples is the
// core invariant: one sample per successful timed operation, and no others.
func TestRunWorkloadClient_SuccessfulOperationsProduceLatencySamples(t *testing.T) {
	client := newFakeWorkerClient()
	cfg := Config{
		Workload:    WorkloadIndependentWrites,
		Users:       2,
		Concurrency: 2,
		Count:       10,
		ContentSize: 8,
	}

	report, err := runWorkloadClient(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("runWorkloadClient: %v", err)
	}

	if report.Attempted != 10 || report.Succeeded != 10 || report.Failed != 0 {
		t.Fatalf("attempted=%d succeeded=%d failed=%d, want 10/10/0",
			report.Attempted, report.Succeeded, report.Failed)
	}
	if report.LatencySamples != int(report.Succeeded) {
		t.Errorf("LatencySamples = %d, want %d (one per successful operation)",
			report.LatencySamples, report.Succeeded)
	}
	// Deliberately not asserting the percentiles are non-zero: against an
	// in-memory fake an operation can legitimately complete inside the
	// clock's resolution, and a test that depended on it being slower
	// would be a sleep test in disguise. Ordering is the real invariant.
	if report.P50Latency > report.P95Latency || report.P95Latency > report.P99Latency {
		t.Errorf("percentiles are not monotonic: p50=%s p95=%s p99=%s",
			report.P50Latency, report.P95Latency, report.P99Latency)
	}

	// Pre-existing semantics must be untouched by the latency work.
	if len(report.OracleFailures) != 0 {
		t.Errorf("clean run reported data corruption: %v", report.OracleFailures)
	}
	if len(report.VerificationErrors) != 0 {
		t.Errorf("clean run reported verification errors: %v", report.VerificationErrors)
	}
	if report.Replicas == nil {
		t.Error("Replicas map is nil")
	}
}

// TestRunWorkloadClient_FailedOperationsDoNotProduceLatencySamples is the
// property that keeps the distribution honest: a failed operation is
// frequently a timeout, and letting it into a "successful operation
// latency" distribution would silently dominate the tail. It must still be
// counted and still be visible in FirstErrors -- just not sampled.
func TestRunWorkloadClient_FailedOperationsDoNotProduceLatencySamples(t *testing.T) {
	client := newFakeWorkerClient()
	// Concurrency 1 makes the append numbering strictly sequential, so
	// exactly calls 2, 4, 6, 8 and 10 fail.
	client.failAppendEvery = 2
	cfg := Config{
		Workload:    WorkloadIndependentWrites,
		Users:       1,
		Concurrency: 1,
		Count:       10,
		ContentSize: 8,
	}

	report, err := runWorkloadClient(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("runWorkloadClient: %v", err)
	}

	if report.Attempted != 10 || report.Succeeded != 5 || report.Failed != 5 {
		t.Fatalf("attempted=%d succeeded=%d failed=%d, want 10/5/5",
			report.Attempted, report.Succeeded, report.Failed)
	}
	if report.LatencySamples != 5 {
		t.Errorf("LatencySamples = %d, want 5 -- failed operations must not be sampled",
			report.LatencySamples)
	}
	if report.LatencySamples != int(report.Succeeded) {
		t.Errorf("LatencySamples = %d, want %d (== Succeeded)", report.LatencySamples, report.Succeeded)
	}
	if int64(report.LatencySamples)+report.Failed != report.Attempted {
		t.Errorf("samples (%d) + failed (%d) != attempted (%d)",
			report.LatencySamples, report.Failed, report.Attempted)
	}

	// Failure accounting itself must be unchanged.
	if len(report.FirstErrors) == 0 {
		t.Error("failed operations left no entry in FirstErrors")
	}
	// Five successful 8-byte appends onto an 8-byte initial file: the
	// oracle must still agree, i.e. the failed calls really wrote nothing.
	if len(report.OracleFailures) != 0 {
		t.Errorf("oracle reported corruption for correctly-failed appends: %v", report.OracleFailures)
	}
	if len(report.VerificationErrors) != 0 {
		t.Errorf("oracle could not verify: %v", report.VerificationErrors)
	}

	if !strings.Contains(report.String(), "latency_samples=5") {
		t.Errorf("report does not surface the sample count:\n%s", report.String())
	}
}
