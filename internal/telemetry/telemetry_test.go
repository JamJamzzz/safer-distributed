package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func clearOTelEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{EnvEndpoint, EnvTracesEndpoint, EnvMetricsEndpoint} {
		t.Setenv(key, "")
	}
}

// TestSetup_DisabledWithoutEndpoint is the core "observability is
// optional" property: with none of the standard OTLP endpoint
// environment variables set, Setup must not install real providers (it
// must not even attempt to dial anywhere -- there is nothing to point
// at), and the returned Shutdown must be a safe no-op.
func TestSetup_DisabledWithoutEndpoint(t *testing.T) {
	clearOTelEnv(t)

	shutdown, err := Setup(context.Background(), Config{ServiceName: "safer-test"})
	if err != nil {
		t.Fatalf("Setup with no endpoint configured: %v", err)
	}

	// The global TracerProvider must still be the SDK's own no-op
	// implementation -- Setup must not have called otel.SetTracerProvider.
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		t.Fatal("Setup installed a real TracerProvider with no OTLP endpoint configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("no-op shutdown returned an error: %v", err)
	}
}

// TestSetup_MalformedConfigInstallsNothing is the transactional half of
// Setup's contract, exercised with a real (not simulated) malformed
// configuration: an OTEL_RESOURCE_ATTRIBUTES value missing "=" is exactly
// what resource.WithFromEnv()'s own parser rejects (see
// go.opentelemetry.io/otel/sdk/resource's fromEnv detector), so
// buildResource fails deterministically with no network involved -- the
// OTLP exporters themselves are deliberately built to never fail
// synchronously on a bad endpoint, so a malformed *resource* attribute is
// the one realistic way to make Setup itself return an error here.
//
// Both a traces and a metrics endpoint are configured so this also
// proves the "transactional" half of Setup's contract: even though both
// signals were requested, a failure this early must leave neither a real
// TracerProvider nor a real MeterProvider installed -- never one real
// provider alongside a returned error. This is what lets cmd/worker,
// cmd/coordinator, and cmd/loadgen's fail-open wrappers safely treat any
// Setup error the same way: log it and keep running with telemetry off,
// without needing to know which providers, if any, partially came up.
func TestSetup_MalformedConfigInstallsNothing(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv(EnvTracesEndpoint, "127.0.0.1:1")
	t.Setenv(EnvMetricsEndpoint, "127.0.0.1:1")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "not-a-key-value-pair")

	shutdown, err := Setup(context.Background(), Config{ServiceName: "safer-test"})
	if err == nil {
		t.Fatal("Setup with a malformed OTEL_RESOURCE_ATTRIBUTES unexpectedly succeeded")
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		t.Fatal("Setup installed a real TracerProvider despite failing to build its Resource")
	}
	if _, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider); ok {
		t.Fatal("Setup installed a real MeterProvider despite failing to build its Resource")
	}

	// The Shutdown Setup hands back on failure must itself be the safe
	// no-op -- a caller that continues running (per the fail-open policy
	// above) still calls it unconditionally at shutdown time.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("Setup's returned shutdown after a failed Setup should be a safe no-op, got: %v", err)
	}
}

// TestSetup_EnabledWithTracesEndpoint proves the other half: when an
// endpoint IS configured, Setup does install a real TracerProvider (so
// that the rest of this repository's instrumentation actually produces
// spans), and Shutdown still completes -- and completes within its own
// bounded timeout even though nothing in this test is listening on the
// configured (bogus) endpoint, proving a backend outage does not hang
// shutdown.
func TestSetup_EnabledWithTracesEndpoint(t *testing.T) {
	clearOTelEnv(t)
	// A syntactically valid, guaranteed-unreachable endpoint: this test
	// is about Setup's own behavior, not about actually exporting
	// anything (see the OTLP round-trip proven live against a real
	// Datadog Agent instead -- see docs/distributed-roadmap.md's Phase 5
	// section).
	t.Setenv(EnvTracesEndpoint, "127.0.0.1:1")

	// This is the only test in this package that installs a real global
	// TracerProvider (Go runs a package's tests sequentially, in source
	// order, so it deliberately runs last and leaves no global state for
	// a later test to be surprised by).
	shutdown, err := Setup(context.Background(), Config{ServiceName: "safer-test"})
	if err != nil {
		t.Fatalf("Setup with a traces endpoint configured: %v", err)
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("Setup did not install a real TracerProvider with a traces endpoint configured; got %T", otel.GetTracerProvider())
	}

	done := make(chan error, 1)
	go func() { done <- shutdown(context.Background()) }()
	select {
	case <-done:
		// Whether it reports an error trying to flush to an unreachable
		// endpoint is not the point; that it returned at all, promptly,
		// is.
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("shutdown did not return within its own bounded timeout against an unreachable backend")
	}
}
