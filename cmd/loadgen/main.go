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
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}

	conn, err := dialTarget(context.Background(), cfg.Addresses)
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	defer func() { _ = conn.Close() }()

	report, err := run(context.Background(), cfg, conn)
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	fmt.Println(report.String())
	if report.Failed > 0 || len(report.OracleFailures) > 0 || len(report.VerificationErrors) > 0 {
		os.Exit(1)
	}
}
