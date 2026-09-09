package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
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
