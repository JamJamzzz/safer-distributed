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

	// Telemetry is optional (see internal/telemetry's package doc): with
	// no OTEL_EXPORTER_OTLP_* endpoint configured, Setup does nothing and
	// shutdownTelemetry is a no-op.
	shutdownTelemetry, err := telemetry.Setup(context.Background(), telemetry.Config{ServiceName: "safer-loadgen"})
	if err != nil {
		return false, fmt.Errorf("telemetry setup: %w", err)
	}
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
