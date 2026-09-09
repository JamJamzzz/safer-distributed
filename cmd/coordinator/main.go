// Command coordinator runs the SAFER lock coordinator.
//
// One coordinator serves strict-2PL lock decisions for every SAFER worker
// in the deployment. Workers reach it over gRPC and point at it with
// SAFER_COORDINATOR_ADDR.
//
// Fencing requires durable state, so this process needs the same MongoDB
// deployment the workers use (SAFER_MONGO_URI / SAFER_MONGO_DB). It stores
// only coordination metadata there -- resource ids, counters, owner ids --
// never SAFER object content. Started without it, the coordinator still
// serves locks but issues no fencing tokens, which means no stale-writer
// protection; it says so loudly at startup rather than appearing healthy.
//
// Limits, deliberately not hidden: this process is the single point of
// coordination and its lock state is in memory. If it dies, that state is
// gone and workers holding locks are not told. Leases bound how long a
// dead worker's locks are held, but nothing bounds the loss of the
// coordinator itself.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/fencing/mongofence"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

func main() {
	address := flag.String("addr", "127.0.0.1:0",
		"address to listen on; port 0 picks a free port and prints it")
	lease := flag.Duration("lease", grpccoord.DefaultLeaseDuration,
		"how long a transaction's locks survive without a renewal")
	sweep := flag.Duration("sweep", grpccoord.DefaultSweepInterval,
		"how often to look for expired leases")
	flag.Parse()

	config := grpccoord.ServerConfig{LeaseDuration: *lease, SweepInterval: *sweep}

	// The fence store is what makes stale-writer protection possible.
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		log.Fatalf("coordinator: MongoDB configuration: %v", err)
	}
	if configured {
		fences, err := mongofence.Open(context.Background(), mongofence.Config{
			URI:      mongoCfg.URI,
			Database: mongoCfg.Database,
			Timeout:  mongoCfg.Timeout,
		})
		if err != nil {
			log.Fatalf("coordinator: opening the fence store: %v", err)
		}
		defer func() { _ = fences.Close(context.Background()) }()
		config.Fences = fences
		log.Printf("coordinator: fencing enabled, using database %q", mongoCfg.Database)
	} else {
		log.Printf("coordinator: WARNING: %s is not set, so no fencing tokens will be issued "+
			"and there is NO stale-writer protection", mongostore.EnvURI)
	}

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("coordinator: listen on %s: %v", *address, err)
	}

	coordinator := grpccoord.NewServerWithConfig(config)
	defer coordinator.Stop()

	server := grpc.NewServer()
	coordinatorv1.RegisterLockCoordinatorServer(server, coordinator)

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
