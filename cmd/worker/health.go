package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// Readiness and liveness are deliberately different checks, registered as
// two different gRPC health-checking-protocol service names on the same
// health.Server.
//
//   - readinessServiceName ("", the health-checking protocol's default
//     service name, and Kubernetes' native gRPC probe's default `service`
//     field) reflects whether this replica's dependencies -- MongoDB and
//     the lock coordinator -- are currently reachable. A worker becomes
//     NOT_SERVING here when either is unreachable, so Kubernetes stops
//     routing new requests to it. That is exactly what a Service in front
//     of several replicas needs: route around a replica whose downstream
//     is having a bad day.
//   - livenessServiceName reports only that this process's own serve loop
//     is running. It is set SERVING once at startup and never toggled by
//     downstream health, on purpose: if a transient MongoDB or coordinator
//     outage made every replica's liveness probe fail too, Kubernetes
//     would kill and restart all of them at once, which cannot fix an
//     outage in a dependency and can only make it worse by adding a
//     thundering restart on top. Liveness answers "is the process itself
//     alive and able to make progress", not "are my dependencies healthy".
const (
	readinessServiceName = ""
	livenessServiceName  = "liveness"
)

// readinessCheckTimeout bounds one round of dependency checks, so a wedged
// dependency cannot wedge the readiness loop itself.
const readinessCheckTimeout = 5 * time.Second

// startReadinessLoop periodically checks MongoDB and coordinator
// reachability and updates the readiness service's status accordingly. It
// runs one check immediately, so readiness reflects reality from the first
// probe rather than defaulting to SERVING (or NOT_SERVING) until the first
// tick.
//
// The returned stop function blocks until the loop has actually exited, so
// callers can rely on no further SetServingStatus calls happening once it
// returns.
func startReadinessLoop(store *mongostore.Store, backend *grpccoord.Backend, hs *health.Server, interval time.Duration) (stop func()) {
	if interval <= 0 {
		interval = DefaultReadinessInterval
	}
	stopCh := make(chan struct{})
	done := make(chan struct{})

	check := func() {
		ctx, cancel := context.WithTimeout(context.Background(), readinessCheckTimeout)
		defer cancel()

		mongoErr := store.Ping(ctx)
		coordErr := backend.Health(ctx)
		if mongoErr == nil && coordErr == nil {
			hs.SetServingStatus(readinessServiceName, healthpb.HealthCheckResponse_SERVING)
			return
		}
		hs.SetServingStatus(readinessServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
		log.Printf("worker: not ready: mongo=%v coordinator=%v", mongoErr, coordErr)
	}

	go func() {
		defer close(done)
		check()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				check()
			}
		}
	}()

	return func() {
		close(stopCh)
		<-done
	}
}
