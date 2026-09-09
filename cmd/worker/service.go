package main

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

// tracer is unconditional and safe with no telemetry backend configured
// (see internal/telemetry's package doc): with no real TracerProvider
// installed, otel.Tracer returns a no-op implementation.
var tracer = otel.Tracer("github.com/JamJamzzz/safer-distributed/cmd/worker")

// saferWorkerServer adapts SAFER's existing client API
// (github.com/JamJamzzz/safer-distributed/client) to the worker.v1.SaferWorker
// gRPC surface. It is a thin translation layer: no cryptography, storage,
// or locking logic lives here, all of it stays in the client package this
// process installed a MongoDB backend and a remote coordinator into at
// startup (see run in main.go).
//
// It holds no per-user state between calls. Every RPC carries a username
// and password, and SAFER derives that user's keys fresh from them each
// time (client.GetUserContext) rather than keeping a server-side session,
// so any worker replica can serve any request and a replica can be added,
// removed, or restarted without losing anything a client needs.
//
// Every handler calls the *Context variant of the SAFER operation it
// needs, passing the incoming gRPC context straight through. That is what
// lets a client's cancellation or deadline actually reach lock acquisition
// and the MongoDB transaction underneath it, instead of stopping at this
// boundary the way it did before those variants existed (see
// client.InitUserContext and friends, and their package doc for why the
// legacy context-free methods -- still used by V1 callers -- could not
// simply be changed in place). It is also the substrate distributed
// tracing rides on: main.go's otelgrpc server handler extracts whatever
// trace context the caller (cmd/loadgen) propagated onto ctx, and every
// span this file starts from that ctx -- worker.authenticate,
// safer.<Operation> -- becomes a child of it, and ctx carries that span
// context further into guard.AcquireContext (grpccoord's client span)
// and the MongoDB transaction (mongostore's span), all in one trace.
type saferWorkerServer struct {
	workerv1.UnimplementedSaferWorkerServer
	authLimiter *authLimiter
}

func (s *saferWorkerServer) InitUser(ctx context.Context, req *workerv1.InitUserRequest) (*workerv1.InitUserResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: username is required")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: password is required")
	}
	if err := s.authLimiter.acquire(ctx); err != nil {
		return nil, translateErr(ctx, "InitUser", err)
	}
	defer s.authLimiter.release()

	err := traced(ctx, "safer.InitUser", func(ctx context.Context) error {
		_, err := client.InitUserContext(ctx, req.GetUsername(), req.GetPassword())
		return err
	})
	if err != nil {
		return nil, translateErr(ctx, "InitUser", err)
	}
	return &workerv1.InitUserResponse{}, nil
}

func (s *saferWorkerServer) StoreFile(ctx context.Context, req *workerv1.StoreFileRequest) (*workerv1.StoreFileResponse, error) {
	user, err := authenticate(ctx, s.authLimiter, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	err = traced(ctx, "safer.StoreFile", func(ctx context.Context) error {
		return user.StoreFileContext(ctx, req.GetFilename(), req.GetContent())
	})
	if err != nil {
		return nil, translateErr(ctx, "StoreFile", err)
	}
	return &workerv1.StoreFileResponse{}, nil
}

func (s *saferWorkerServer) AppendToFile(ctx context.Context, req *workerv1.AppendToFileRequest) (*workerv1.AppendToFileResponse, error) {
	user, err := authenticate(ctx, s.authLimiter, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	err = traced(ctx, "safer.AppendToFile", func(ctx context.Context) error {
		return user.AppendToFileContext(ctx, req.GetFilename(), req.GetContent())
	})
	if err != nil {
		return nil, translateErr(ctx, "AppendToFile", err)
	}
	return &workerv1.AppendToFileResponse{}, nil
}

func (s *saferWorkerServer) LoadFile(ctx context.Context, req *workerv1.LoadFileRequest) (*workerv1.LoadFileResponse, error) {
	user, err := authenticate(ctx, s.authLimiter, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	var content []byte
	err = traced(ctx, "safer.LoadFile", func(ctx context.Context) error {
		var err error
		content, err = user.LoadFileContext(ctx, req.GetFilename())
		return err
	})
	if err != nil {
		return nil, translateErr(ctx, "LoadFile", err)
	}
	return &workerv1.LoadFileResponse{Content: content}, nil
}

// authenticate validates the request's credentials fields and derives the
// caller's SAFER user, exactly as every SAFER operation already requires.
//
// The GetUserContext call itself -- Argon2 key derivation -- is gated by
// limiter, not the validation around it: an empty username/password is
// cheap to reject and should not wait for an admission slot meant for
// expensive work. The whole gated section is one span
// ("worker.authenticate"), the same span Phase 5's metrics requirement B
// is about: how much of a request's latency this section -- admission
// wait plus the Argon2 call itself -- actually accounts for, visible
// directly in the trace alongside the lock-wait and Mongo-transaction
// spans elsewhere in the same request.
//
// No password, derived key, or file content ever becomes a span
// attribute here or anywhere else in this file -- see this repository's
// Phase 5 telemetry policy in docs/distributed-roadmap.md. Nor does the
// username: it identifies a specific SAFER account, which is exactly the
// kind of high-cardinality, potentially sensitive identifier Phase 5
// deliberately keeps out of span attributes.
func authenticate(ctx context.Context, limiter *authLimiter, username, password string) (*client.User, error) {
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: username is required")
	}
	if password == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: password is required")
	}

	var user *client.User
	err := traced(ctx, "worker.authenticate", func(ctx context.Context) error {
		if err := limiter.acquire(ctx); err != nil {
			return err
		}
		defer limiter.release()
		// Timed separately from the span above, and starting only here:
		// safer.worker.auth_compute.duration is defined as the
		// post-admission work alone, so the admission wait that
		// limiter.acquire just finished is deliberately outside it (see
		// authlimit.go). A caller cancelled while queued returns above and
		// records no compute observation.
		computeStart := time.Now()
		var err error
		user, err = client.GetUserContext(ctx, username, password)
		recordAuthCompute(ctx, computeStart, err)
		return err
	})
	if err != nil {
		return nil, translateErr(ctx, "GetUser", err)
	}
	return user, nil
}

// traced runs fn inside a span named name, recording fn's error (if any)
// on the span before returning it unchanged. It exists so every manual
// span in this file follows the same shape -- start, run, record outcome,
// end -- rather than repeating that boilerplate at each call site.
func traced(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()
	if err := fn(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return err
	}
	return nil
}

// translateErr reports a SAFER operation failure as a gRPC status.
//
// Cancellation and deadlines are reported precisely -- Canceled or
// DeadlineExceeded -- because ctx now actually reaches the operation (see
// the package doc above): a client that cancelled its own call, or hit its
// own deadline, should see that reflected accurately, not folded into the
// generic bucket below.
//
// Everything else is deliberately coarse: the client package returns plain
// errors with no exported sentinel types (wrong password, a missing file,
// a lost race under strict 2PL, a stale fencing token, and an unreachable
// backend are all just `error`), so there is no reliable signal here to
// split into finer-grained codes like NotFound or PermissionDenied without
// fragile string matching against messages that are not part of any API
// contract. FailedPrecondition -- "the operation could not be completed
// given the current state" -- is accurate for all of them; the original
// message is preserved so a caller or operator can read what actually
// happened.
func translateErr(ctx context.Context, op string, err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Errorf(codes.Canceled, "worker: %s: %v", op, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Errorf(codes.DeadlineExceeded, "worker: %s: %v", op, err)
	}
	// ctx itself may already carry the answer even if err's wrapping chain
	// does not surface it (a driver or RPC layer that returns its own
	// error type instead of propagating ctx.Err() through, for instance).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	return status.Errorf(codes.FailedPrecondition, "worker: %s: %v", op, err)
}
