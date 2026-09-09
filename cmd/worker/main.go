// Command worker is the production, long-running SAFER worker service.
//
// It is a stateless adapter: at startup it installs a MongoDB storage
// backend and a remote lock coordinator into the existing SAFER client
// package (see client.UseStorage / client.UseCoordination), then serves a
// small gRPC surface (worker.v1.SaferWorker) that calls straight into
// SAFER's existing InitUser / StoreFile / AppendToFile / LoadFile logic.
// None of SAFER's cryptography, storage, or locking semantics are
// reimplemented here.
//
// Unlike cmd/saferworker -- a single-operation test helper the
// integration/crossprocess harness spawns and kills for one call at a time
// -- this process is meant to run indefinitely behind a Kubernetes Service,
// alongside other replicas of itself, all sharing one MongoDB and one lock
// coordinator. Crash recovery for a killed replica already comes from
// Phase 3C's leases and fencing; the graceful shutdown this command
// performs on SIGTERM is an operational optimization on top of that, not a
// second correctness mechanism.
//
// Both MongoDB and the coordinator are required at startup: a worker with
// neither has nothing to persist to and nothing to coordinate through, and
// parseConfig fails closed rather than starting in a degraded mode.
//
// SECURITY: the worker.v1 gRPC surface this process serves is not
// suitable for exposure to an untrusted network -- see
// proto/worker/v1/worker.proto's package doc and
// deploy/kubernetes/networkpolicy.yaml for the full explanation. In
// short: unlike the lock coordinator's channel, which only ever carries
// resource identifiers and lock modes, this one carries plaintext
// usernames, passwords, and file content, over a dial/serve boundary that
// uses insecure.NewCredentials() -- no transport encryption, no endpoint
// authentication. Keep this Service ClusterIP/headless and behind a
// restrictive NetworkPolicy; do not put it behind a LoadBalancer, a
// NodePort, or an Ingress without a TLS-terminating, authenticating proxy
// in front of it, none of which this repository provides.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
	"github.com/JamJamzzz/safer-distributed/internal/telemetry"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("worker: %v", err)
	}
	if err := run(cfg); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

// run brings up one worker replica and serves until it is asked to stop.
//
// Startup order matters: MongoDB is opened (and pinged) before the
// coordinator is dialed (and health-checked), and both must succeed before
// the gRPC listener opens, so a misconfigured or unreachable deployment
// fails at startup rather than accepting traffic it cannot actually serve.
func run(cfg Config) error {
	ctx := context.Background()

	// Telemetry is optional (see internal/telemetry's package doc) and
	// fails OPEN, never closed: a malformed or unreachable OTLP
	// configuration is an observability problem, not a reason to refuse
	// to serve SAFER traffic. setupTelemetry logs any Setup failure as a
	// warning and returns a safe no-op shutdown instead of propagating
	// the error -- unlike the MongoDB/coordinator dependencies below,
	// which fail this startup closed on purpose, because SAFER cannot
	// actually do anything useful without them. Registered before the
	// other deferred cleanups below, so it runs last -- flushing whatever
	// telemetry the run produced only after everything else has already
	// shut down.
	shutdownTelemetry := setupTelemetry(ctx, telemetry.Config{ServiceName: "safer-worker"})
	defer func() {
		if err := shutdownTelemetry(context.Background()); err != nil {
			log.Printf("worker: flushing telemetry: %v", err)
		}
	}()

	store, err := mongostore.Open(ctx, cfg.Mongo)
	if err != nil {
		return fmt.Errorf("opening mongo: %w", err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			log.Printf("worker: closing mongo: %v", err)
		}
	}()

	backend, err := grpccoord.Dial(ctx, cfg.Coordinator)
	if err != nil {
		return fmt.Errorf("dialing coordinator: %w", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			log.Printf("worker: closing coordinator connection: %v", err)
		}
	}()

	restoreStorage := client.UseStorage(store.Storage())
	defer restoreStorage()
	restoreCoordination := client.UseCoordination(backend)
	defer restoreCoordination()

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}

	// The instance-header interceptor is diagnostic only (see instance.go):
	// it lets a caller observe which replica served a request, which is
	// what makes claims about cross-replica load balancing (see dial.go
	// and cmd/loadgen) checkable instead of asserted.
	//
	// otelgrpc's server stats handler creates one span per incoming
	// worker.v1.SaferWorker RPC and, when the caller (cmd/loadgen)
	// propagated trace context, makes this span a child of the caller's
	// rather than the root of a new trace. It composes independently of
	// the interceptor above -- StatsHandler and ChainUnaryInterceptor are
	// separate grpc.ServerOptions -- and is unconditional and safe with no
	// telemetry backend configured; see internal/telemetry's package doc.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(instanceHeaderInterceptor(instanceID())),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	workerv1.RegisterSaferWorkerServer(grpcServer, &saferWorkerServer{
		authLimiter: newAuthLimiter(cfg.AuthConcurrency),
	})

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	// Liveness is set once, here, and never touched again: it answers "is
	// this process's serve loop running", not "are my dependencies up".
	healthServer.SetServingStatus(livenessServiceName, healthpb.HealthCheckResponse_SERVING)

	stopReadiness := startReadinessLoop(store, backend, healthServer, cfg.ReadinessInterval)
	defer stopReadiness()

	// The bound address goes to stdout on its own line, exactly as
	// cmd/coordinator does, so a supervisor or test harness started with
	// port 0 can learn where it landed instead of guessing.
	fmt.Printf("listening %s\n", listener.Addr().String())
	_ = os.Stdout.Sync()

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-signals:
		log.Printf("worker: %v received, shutting down", sig)
		shutdown(grpcServer, healthServer, cfg.ShutdownTimeout)
		return nil
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	}
}

// shutdown drains the worker without an unbounded wait.
//
//  1. Mark unready immediately, before anything else, so Kubernetes stops
//     sending new requests to this replica as soon as termination begins
//     -- there is no reason to wait for the drain to start that.
//  2. GracefulStop: stop accepting new RPCs, let in-flight ones finish.
//  3. If that has not finished within shutdownTimeout, force it: a graceful
//     shutdown that never completes is not graceful, it is a hang, and
//     Kubernetes will SIGKILL the process anyway once its own
//     terminationGracePeriodSeconds elapses.
func shutdown(grpcServer *grpc.Server, healthServer *health.Server, shutdownTimeout time.Duration) {
	healthServer.SetServingStatus(readinessServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		log.Printf("worker: graceful shutdown did not finish within %s, forcing stop", shutdownTimeout)
		grpcServer.Stop()
		<-stopped
	}
}

// setupTelemetry wraps telemetry.Setup with this binary's fail-open
// policy: telemetry is an optional add-on (see internal/telemetry's
// package doc), so a malformed OTEL_EXPORTER_OTLP_* configuration, or a
// backend that Setup cannot reach while constructing itself, must never
// stop this worker from serving SAFER traffic. Any Setup error is logged
// clearly -- it is not hidden -- and this returns a safe no-op Shutdown
// in its place; internal/telemetry.Setup itself guarantees that a failed
// call never leaves a real TracerProvider/MeterProvider installed for a
// caller in this position to worry about.
func setupTelemetry(ctx context.Context, cfg telemetry.Config) telemetry.Shutdown {
	shutdownTelemetry, err := telemetry.Setup(ctx, cfg)
	if err != nil {
		log.Printf("worker: telemetry setup failed, continuing with telemetry disabled: %v", err)
		return func(context.Context) error { return nil }
	}
	return shutdownTelemetry
}
