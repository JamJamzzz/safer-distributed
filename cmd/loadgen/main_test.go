package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/JamJamzzz/safer-distributed/internal/telemetry"
)

// TestSetupTelemetry_FailsOpenOnMalformedConfig is the Phase 5 hardening
// review's core requirement for this binary: a malformed OTLP
// configuration must never stop a load run from executing. It sets
// exactly the same malformed OTEL_RESOURCE_ATTRIBUTES that
// internal/telemetry's own tests use to make Setup fail deterministically
// (no network involved -- see that package's
// TestSetup_MalformedConfigInstallsNothing), then proves setupTelemetry
// absorbs the error rather than propagating it: it returns a usable,
// safely callable Shutdown, and no real TracerProvider gets installed.
func TestSetupTelemetry_FailsOpenOnMalformedConfig(t *testing.T) {
	for _, key := range []string{telemetry.EnvEndpoint, telemetry.EnvTracesEndpoint, telemetry.EnvMetricsEndpoint} {
		t.Setenv(key, "")
	}
	t.Setenv(telemetry.EnvTracesEndpoint, "127.0.0.1:1")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "not-a-key-value-pair")

	shutdown := setupTelemetry(context.Background(), telemetry.Config{ServiceName: "safer-loadgen"})
	if shutdown == nil {
		t.Fatal("setupTelemetry returned a nil Shutdown on a Setup failure -- callers defer-call this unconditionally")
	}

	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		t.Fatal("setupTelemetry left a real TracerProvider installed despite a malformed configuration")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned by setupTelemetry after a failed Setup should be a safe no-op, got: %v", err)
	}
}
