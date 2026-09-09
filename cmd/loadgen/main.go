// Command loadgen drives functional and correctness load against a SAFER
// worker Service.
//
// It talks to the worker Service over gRPC -- the same way any real client
// would -- rather than calling the client package directly, so it
// exercises the same boundary a Kubernetes Service load-balances traffic
// across. Phase 4 needs it to be functional and to validate correctness
// under concurrency; it deliberately does not try to be a tuned
// throughput/latency benchmark (that is a later phase, once observability
// exists -- see docs/distributed-roadmap.md).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}

	conns, closeConns, err := dialAll(cfg.Addresses)
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	defer closeConns()

	picker := newConnPicker(conns)

	report, err := run(context.Background(), cfg, picker)
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	fmt.Println(report.String())
	if report.Errors > 0 {
		os.Exit(1)
	}
}

// dialAll connects to every address up front, so a misconfigured or
// unreachable worker fails the run immediately rather than on its first
// request.
func dialAll(addresses []string) (conns []*grpc.ClientConn, closeAll func(), err error) {
	for _, addr := range addresses {
		dialCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		conn, dialErr := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if dialErr != nil {
			for _, c := range conns {
				_ = c.Close()
			}
			return nil, nil, fmt.Errorf("dialing %s: %w", addr, dialErr)
		}
		conns = append(conns, conn)
	}
	return conns, func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}, nil
}
