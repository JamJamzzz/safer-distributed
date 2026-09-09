package main

// Coverage for safer.worker.auth_compute.duration, the post-admission
// authentication compute histogram.
//
// This test installs a real MeterProvider backed by a ManualReader into
// OpenTelemetry's global state. That is deliberate and needs no production
// refactoring: the instruments in authlimit.go are created from
// otel.Meter(...) at package init, and OTel's global implementation
// re-binds already-created instruments to the real provider the first time
// SetMeterProvider is called, so records made afterwards land in the
// reader. The trade-off is that the provider is process-global and cannot
// be uninstalled, so this is the only test in the package that sets one --
// everything it affects afterwards (other tests recording admission
// metrics nobody reads) is harmless.

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/JamJamzzz/safer-distributed/client"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

const authComputeMetricName = "safer.worker.auth_compute.duration"

// collectAuthCompute reads the histogram back and returns observation
// counts keyed by the outcome attribute. The ManualReader is cumulative,
// so these totals accumulate across calls within one test.
func collectAuthCompute(t *testing.T, reader sdkmetric.Reader) (counts map[string]uint64, found bool) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	counts = make(map[string]uint64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != authComputeMetricName {
				continue
			}
			found = true
			if m.Unit != "s" {
				t.Errorf("%s unit = %q, want %q", m.Name, m.Unit, "s")
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s has data type %T, want metricdata.Histogram[float64]", m.Name, m.Data)
			}
			for _, dp := range hist.DataPoints {
				// Low cardinality is a property worth enforcing, not
				// assuming: exactly one attribute, and it must be outcome.
				// A username, filename or error string leaking in here
				// would be both a cardinality and a privacy problem.
				if dp.Attributes.Len() != 1 {
					t.Errorf("datapoint carries %d attributes, want exactly 1 (outcome): %v",
						dp.Attributes.Len(), dp.Attributes.Encoded(attribute.DefaultEncoder()))
				}
				value, ok := dp.Attributes.Value("outcome")
				if !ok {
					t.Errorf("datapoint has no outcome attribute: %v",
						dp.Attributes.Encoded(attribute.DefaultEncoder()))
					continue
				}
				counts[value.AsString()] += dp.Count
			}
		}
	}
	return counts, found
}

func totalObservations(counts map[string]uint64) uint64 {
	var total uint64
	for _, n := range counts {
		total += n
	}
	return total
}

// TestAuthComputeMetric covers the whole contract in one test because the
// ManualReader is cumulative and the global MeterProvider can only be
// installed once: splitting these into separate tests would make them
// order-dependent on a shared counter.
func TestAuthComputeMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	restoreStorage := client.UseUserlibStorage()
	defer restoreStorage()

	const username, password = "authcompute-alice", "correct-password"
	if _, err := client.InitUser(username, password); err != nil {
		t.Fatalf("InitUser: %v", err)
	}

	limiter := newAuthLimiter(DefaultAuthConcurrency)
	ctx, cancel := context.WithTimeout(context.Background(), authTestTimeout)
	defer cancel()

	// 1. A successful post-admission authentication records exactly one
	//    observation, under the exact metric name and unit.
	if _, err := authenticate(ctx, limiter, username, password); err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	counts, found := collectAuthCompute(t, reader)
	if !found {
		t.Fatalf("metric %q was never recorded", authComputeMetricName)
	}
	if counts["success"] != 1 {
		t.Errorf("outcome=success observations = %d, want 1", counts["success"])
	}
	if got := totalObservations(counts); got != 1 {
		t.Errorf("total observations = %d, want 1", got)
	}

	// 2. GetUserContext returning an error still records -- the work ran,
	//    and how long a failed credential check takes is exactly as
	//    interesting as a successful one.
	if _, err := authenticate(ctx, limiter, username, "wrong-password"); err == nil {
		t.Fatal("authenticate with a wrong password unexpectedly succeeded")
	}
	counts, _ = collectAuthCompute(t, reader)
	if counts["success"] != 1 || counts["error"] != 1 {
		t.Errorf("outcome counts = %v, want success=1 error=1", counts)
	}
	if got := totalObservations(counts); got != 2 {
		t.Errorf("total observations = %d, want 2", got)
	}

	// 3. InitUser must NOT be mixed in. Its handler takes an authLimiter
	//    slot directly rather than going through authenticate, and it
	//    performs different (RSA/DS) key generation, so folding it in would
	//    make this histogram describe two different populations at once.
	server := &saferWorkerServer{authLimiter: limiter}
	if _, err := server.InitUser(ctx, &workerv1.InitUserRequest{
		Username: "authcompute-initonly",
		Password: "init-password",
	}); err != nil {
		t.Fatalf("InitUser handler: %v", err)
	}
	counts, _ = collectAuthCompute(t, reader)
	if got := totalObservations(counts); got != 2 {
		t.Errorf("total observations = %d after an InitUser, want 2 -- InitUser must not record auth_compute", got)
	}

	// 4. A request cancelled while queued for a slot never starts the
	//    compute section, so it must record nothing here (its admission
	//    wait is recorded as cancelled by the existing metric). The
	//    limiter is deliberately held to capacity so acquire's select can
	//    only take the ctx.Done() branch.
	full := newAuthLimiter(1)
	if err := full.acquire(context.Background()); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	defer full.release()

	cancelledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := authenticate(cancelledCtx, full, username, password); err == nil {
		t.Fatal("authenticate with a cancelled context and a full limiter unexpectedly succeeded")
	}
	counts, _ = collectAuthCompute(t, reader)
	if got := totalObservations(counts); got != 2 {
		t.Errorf("total observations = %d, want 2 -- a request cancelled at admission must not fabricate a compute sample", got)
	}
}
