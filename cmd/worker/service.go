package main

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client"
	workerv1 "github.com/JamJamzzz/safer-distributed/proto/worker/v1"
)

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
// simply be changed in place).
//
// authLimiter bounds how many of this process's calls are inside
// InitUserContext's key generation or GetUserContext's Argon2 key
// derivation at once -- see authlimit.go for why: each such call is
// genuinely memory-heavy, and an unbounded number of them running at
// once in one process has no ceiling a static memory limit can be sized
// against.
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
	if _, err := client.InitUserContext(ctx, req.GetUsername(), req.GetPassword()); err != nil {
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
	if err := user.StoreFileContext(ctx, req.GetFilename(), req.GetContent()); err != nil {
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
	if err := user.AppendToFileContext(ctx, req.GetFilename(), req.GetContent()); err != nil {
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
	content, err := user.LoadFileContext(ctx, req.GetFilename())
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
// expensive work.
func authenticate(ctx context.Context, limiter *authLimiter, username, password string) (*client.User, error) {
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: username is required")
	}
	if password == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: password is required")
	}
	if err := limiter.acquire(ctx); err != nil {
		return nil, translateErr(ctx, "GetUser", err)
	}
	defer limiter.release()
	user, err := client.GetUserContext(ctx, username, password)
	if err != nil {
		return nil, translateErr(ctx, "GetUser", err)
	}
	return user, nil
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
