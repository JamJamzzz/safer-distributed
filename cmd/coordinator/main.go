// Command coordinator runs the SAFER lock coordinator.
//
// One coordinator serves strict-2PL lock decisions for every SAFER worker
// in the deployment. Workers reach it over gRPC and point at it with
// SAFER_COORDINATOR_ADDR.
//
// Limits, deliberately not hidden: this process is the single point of
// coordination, its lock state is in memory, and there are no leases or
// fencing tokens yet. If it dies, that state is gone and workers holding
// locks are not told. If a worker dies without ending its transaction, its
// locks are leaked until this process restarts.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

func main() {
	address := flag.String("addr", "127.0.0.1:0",
		"address to listen on; port 0 picks a free port and prints it")
	flag.Parse()

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("coordinator: listen on %s: %v", *address, err)
	}

	server := grpc.NewServer()
	coordinatorv1.RegisterLockCoordinatorServer(server, grpccoord.NewServer(nil))

	// The bound address goes to stdout on its own line, so a supervisor
	// or a test harness can start this with port 0 and learn where it
	// landed instead of guessing a port and racing for it.
	fmt.Printf("listening %s\n", listener.Addr().String())
	_ = os.Stdout.Sync()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signals
		log.Printf("coordinator: %v received, shutting down", sig)
		// GracefulStop lets in-flight calls finish. It does not save
		// lock state -- there is nowhere to save it to in this phase.
		server.GracefulStop()
	}()

	if err := server.Serve(listener); err != nil {
		log.Fatalf("coordinator: serve: %v", err)
	}
}
