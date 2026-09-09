package main

// Bounded admission control for SAFER's expensive authentication/key-
// derivation path.
//
// Live measurement (kind, Phase 4.6; see docs/distributed-roadmap.md's
// Phase 4.6 section for the full write-up) found that every
// StoreFile/AppendToFile/LoadFile RPC re-authenticates its caller through
// client.GetUserContext -> deriveAccountKey -> userlib.Argon2Key, and
// InitUser performs its own expensive key generation (RSA/DS keygen).
// userlib's Argon2Key calls golang.org/x/crypto/argon2.IDKey with a
// memory parameter of 64*1024 KiB (64 MiB) per call -- a deliberate,
// fixed cost of the memory-hard KDF, not a bug and not something this
// package weakens. A single worker process observed under real
// concurrent load settled at roughly 150-165 MB of resident memory after
// any burst of this work (Go's allocator does not hand pages back to the
// OS quickly, so that level persists between bursts rather than an
// isolated spike). Measured at controlled loadgen concurrency levels
// against the same worker process: 2 overlapping sections reached
// ~217 MB observed, with no OOM at that level in the measured run; 4 or
// more overlapping sections reliably exceeded the original 256Mi
// container limit and were OOMKilled. Repeating low-concurrency runs
// found that level stable rather than climbing further run over run --
// i.e. concurrency pressure, not a leak.
//
// The fix is not "give the container more memory and hope": an
// unbounded number of concurrent authentications in one process has no
// ceiling at all, so no static memory limit is actually safe against it.
// authLimiter puts an explicit, small, context-aware ceiling on how many
// of these sections may run at once in this process, so memory sizing
// (see deploy/kubernetes/worker-deployment.yaml) can be reasoned about
// instead of guessed.
//
// This is scoped to exactly the expensive section, not the whole RPC:
// once a request has authenticated, the rest of its work (lock
// acquisition, the MongoDB transaction) is not memory-hard the same way
// and is not gated here.

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meter, authAdmissionWait, and authInFlight are unconditional and safe
// with no telemetry backend configured (see internal/telemetry's package
// doc): with no real MeterProvider installed, otel.Meter returns a no-op
// implementation.
var (
	authMeter = otel.Meter("github.com/JamJamzzz/safer-distributed/cmd/worker")

	// authAdmissionWait and authInFlight together answer "is the auth
	// admission limiter causing latency?" (Phase 5's metrics requirement
	// B). authAdmissionWait is how long a request actually waited for a
	// slot (zero whenever one was immediately free); authInFlight is a
	// live saturation signal -- if it sits at authLimiter's configured
	// capacity, requests are queueing on it right now.
	authAdmissionWait, _ = authMeter.Float64Histogram(
		"safer.worker.auth_admission.wait.duration",
		metric.WithDescription("Time a request waited to acquire an authLimiter slot before its expensive auth/key-derivation section could start."),
		metric.WithUnit("s"),
	)
	authInFlight, _ = authMeter.Int64UpDownCounter(
		"safer.worker.auth_admission.in_flight",
		metric.WithDescription("Number of GetUserContext/InitUserContext calls currently holding an authLimiter slot."),
	)
)

// authLimiter bounds how many expensive authentication/key-derivation
// sections run concurrently in this worker process.
//
// It adds no server-side session state: every acquire/release pair
// brackets exactly one call to client.GetUserContext or
// client.InitUserContext, the same per-call re-authentication SAFER
// already does. Nothing about a password or a derived account key is
// cached or reused across requests, and no crypto parameter here is
// weakened -- this only delays when a request is allowed to start that
// work, never what the work computes.
type authLimiter struct {
	sem chan struct{}
}

// newAuthLimiter builds a limiter admitting at most n concurrent sections.
// n <= 0 is treated as 1: a limiter that admits zero would deadlock every
// request, which is never the intent of a misconfigured value here.
func newAuthLimiter(n int) *authLimiter {
	if n <= 0 {
		n = 1
	}
	return &authLimiter{sem: make(chan struct{}, n)}
}

// acquire blocks until a slot is free or ctx is done, whichever comes
// first. On success the caller MUST call release exactly once -- normally
// via `defer` immediately after a successful acquire -- or the slot is
// never given back. On failure (ctx.Err()) no slot was taken and there is
// nothing to release.
func (a *authLimiter) acquire(ctx context.Context) error {
	start := time.Now()
	select {
	case a.sem <- struct{}{}:
		authAdmissionWait.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(attribute.String("outcome", "acquired")))
		authInFlight.Add(ctx, 1)
		return nil
	case <-ctx.Done():
		authAdmissionWait.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(attribute.String("outcome", "cancelled")))
		return ctx.Err()
	}
}

// release gives back a slot acquired by acquire. Calling it without a
// matching successful acquire blocks forever if the semaphore is already
// at capacity (an empty channel has nothing to receive) -- which is a
// bug in the caller, not a state authLimiter tries to tolerate.
func (a *authLimiter) release() {
	<-a.sem
	authInFlight.Add(context.Background(), -1)
}
