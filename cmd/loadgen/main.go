// Command loadgen drives functional and correctness load against a SAFER
// worker Service.
//
// It talks to the worker Service over gRPC -- the same way any real client
// would -- rather than calling the client package directly, and balances
// requests across worker replicas itself, client-side, using gRPC's
// round_robin policy (see dial.go for exactly what that means and why a
// Kubernetes ClusterIP Service alone does not give you this). Phase 4
// needs it to be functional and to validate correctness under
// concurrency; it deliberately does not try to be a tuned throughput/
// latency benchmark (that is a later phase, once observability exists --
// see docs/distributed-roadmap.md).
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/JamJamzzz/safer-distributed/internal/telemetry"
)

func main() {
	failed, err := run()
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	if failed {
		os.Exit(1)
	}
}

// run is main's body, structured as an ordinary function (rather than
// main calling log.Fatalf/os.Exit directly at each step) so that Go's
// normal `defer` unwinding -- in particular, flushing telemetry -- always
// runs before the process exits, including on the failure paths.
func run() (failed bool, err error) {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		return false, err
	}

	// Telemetry is optional (see internal/telemetry's package doc) and
	// fails OPEN, never closed: a malformed or unreachable OTLP
	// configuration is an observability problem, not a reason to abort a
	// load run. setupTelemetry logs any Setup failure as a warning and
	// returns a safe no-op shutdown instead of propagating the error.
	shutdownTelemetry := setupTelemetry(context.Background(), telemetry.Config{ServiceName: "safer-loadgen"})
	defer func() {
		if shutdownErr := shutdownTelemetry(context.Background()); shutdownErr != nil {
			log.Printf("loadgen: flushing telemetry: %v", shutdownErr)
		}
	}()

	conn, err := dialTarget(context.Background(), cfg.Addresses)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()

	report, err := runWorkload(context.Background(), cfg, conn)
	if err != nil {
		return false, err
	}
	fmt.Println(report.String())
	return report.Failed > 0 || len(report.OracleFailures) > 0 || len(report.VerificationErrors) > 0, nil
}

// setupTelemetry wraps telemetry.Setup with this binary's fail-open
// policy: telemetry is an optional add-on (see internal/telemetry's
// package doc), so a malformed OTEL_EXPORTER_OTLP_* configuration, or a
// backend that Setup cannot reach while constructing itself, must never
// stop a load run from executing. Any Setup error is logged clearly --
// it is not hidden -- and this returns a safe no-op Shutdown in its
// place; internal/telemetry.Setup itself guarantees that a failed call
// never leaves a real TracerProvider/MeterProvider installed for a
// caller in this position to worry about.
func setupTelemetry(ctx context.Context, cfg telemetry.Config) telemetry.Shutdown {
	shutdownTelemetry, err := telemetry.Setup(ctx, cfg)
	if err != nil {
		log.Printf("loadgen: telemetry setup failed, continuing with telemetry disabled: %v", err)
		return func(context.Context) error { return nil }
	}
	return shutdownTelemetry
}
