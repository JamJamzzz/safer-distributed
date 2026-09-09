// Command coordinator runs the SAFER lock coordinator.
//
// One coordinator serves strict-2PL lock decisions for every SAFER worker
// in the deployment. Workers reach it over gRPC and point at it with
// SAFER_COORDINATOR_ADDR.
//
// Fencing requires durable state, so this process needs the same MongoDB
// deployment the workers use (SAFER_MONGO_URI / SAFER_MONGO_DB). By
// default this is not optional: a coordinator that cannot durably fence
// exclusive grants offers no stale-writer protection at all (see
// client/fencing), so startup fails rather than serving an unsafe
// deployment that merely looks healthy. The -allow-unfenced flag exists to
// opt out of that for local development or lock-semantics-only testing; it
// is never appropriate in production, and it says so loudly at startup
// when used.
//
// Limits, deliberately not hidden: this process is the single point of
// coordination and its lock state is in memory. If it dies, that state is
// gone and workers holding locks are not told. Leases bound how long a
// dead worker's locks are held, but nothing bounds the loss of the
// coordinator itself.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/fencing/mongofence"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
	coordinatorv1 "github.com/JamJamzzz/safer-distributed/proto/coordinator/v1"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("coordinator: %v", err)
	}
	if err := run(cfg); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}

// run starts the coordinator and serves until it is asked to stop.
func run(cfg Config) error {
	fences, closeFences, err := openFenceStore(cfg)
	if err != nil {
		return err
	}
	if closeFences != nil {
		defer closeFences()
	}

	serverConfig := grpccoord.ServerConfig{
		LeaseDuration: cfg.LeaseDuration,
		SweepInterval: cfg.SweepInterval,
		Fences:        fences,
	}

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Address, err)
	}

	coordinator := grpccoord.NewServerWithConfig(serverConfig)
	defer coordinator.Stop()

	server := grpc.NewServer()
	coordinatorv1.RegisterLockCoordinatorServer(server, coordinator)

	// The gRPC health-checking protocol, for Kubernetes' native grpc
	// probes (see deploy/kubernetes/coordinator). This is a coarser signal
	// than LockCoordinator.Health -- that RPC reports operational detail
	// (active/revoking transaction counts, held locks) for a worker or
	// operator to inspect, while this just answers "is this process
	// serving". It is set SERVING once, here, and never toggled: by this
	// point openFenceStore has already either succeeded or this process
	// would have exited (fail-closed, Phase 4.0), so there is no
	// meaningful degraded state to report afterward the way the worker's
	// readiness loop reports a downstream outage.
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

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
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// openFenceStore opens the durable fencing store MongoDB configuration
// describes, or fails closed if none is configured or reachable.
//
// This is the Phase 4.0 boundary: a production coordinator must not serve
// remote X-lock coordination without durable fencing, because an unfenced
// exclusive grant is a writer nothing can later stop (see client/fencing
// and docs/distributed-roadmap.md). cfg.AllowUnfenced is the one escape
// hatch, meant for local development and tests that only exercise lock
// semantics; it is logged loudly precisely so it cannot be mistaken for a
// supported production mode.
func openFenceStore(cfg Config) (fences grpccoord.FenceStore, closeFn func(), err error) {
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		return nil, nil, fmt.Errorf("MongoDB configuration: %w", err)
	}

	if !configured {
		if !cfg.AllowUnfenced {
			return nil, nil, fmt.Errorf(
				"%s is not set; a production coordinator refuses to start without a durable "+
					"fencing store (pass -allow-unfenced to override for local development or "+
					"lock-semantics-only testing -- this gives NO stale-writer protection)",
				mongostore.EnvURI)
		}
		log.Printf("coordinator: WARNING: -allow-unfenced set and %s is not set, so no fencing "+
			"tokens will be issued and there is NO stale-writer protection; this is not a "+
			"supported production configuration", mongostore.EnvURI)
		return nil, nil, nil
	}

	store, err := mongofence.Open(context.Background(), mongofence.Config{
		URI:      mongoCfg.URI,
		Database: mongoCfg.Database,
		Timeout:  mongoCfg.Timeout,
	})
	if err != nil {
		if !cfg.AllowUnfenced {
			return nil, nil, fmt.Errorf("opening the fence store: %w", err)
		}
		log.Printf("coordinator: WARNING: -allow-unfenced set and the fence store is unreachable "+
			"(%v); starting anyway with NO stale-writer protection; this is not a supported "+
			"production configuration", err)
		return nil, nil, nil
	}

	log.Printf("coordinator: fencing enabled, using database %q", mongoCfg.Database)
	return store, func() { _ = store.Close(context.Background()) }, nil
}
