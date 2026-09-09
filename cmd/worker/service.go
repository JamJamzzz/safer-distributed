package main

import (
	"context"

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
// time (client.GetUser) rather than keeping a server-side session, so any
// worker replica can serve any request and a replica can be added, removed,
// or restarted without losing anything a client needs.
type saferWorkerServer struct {
	workerv1.UnimplementedSaferWorkerServer
}

func (s *saferWorkerServer) InitUser(_ context.Context, req *workerv1.InitUserRequest) (*workerv1.InitUserResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: username is required")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: password is required")
	}
	if _, err := client.InitUser(req.GetUsername(), req.GetPassword()); err != nil {
		return nil, translateErr("InitUser", err)
	}
	return &workerv1.InitUserResponse{}, nil
}

func (s *saferWorkerServer) StoreFile(_ context.Context, req *workerv1.StoreFileRequest) (*workerv1.StoreFileResponse, error) {
	user, err := authenticate(req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	if err := user.StoreFile(req.GetFilename(), req.GetContent()); err != nil {
		return nil, translateErr("StoreFile", err)
	}
	return &workerv1.StoreFileResponse{}, nil
}

func (s *saferWorkerServer) AppendToFile(_ context.Context, req *workerv1.AppendToFileRequest) (*workerv1.AppendToFileResponse, error) {
	user, err := authenticate(req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	if err := user.AppendToFile(req.GetFilename(), req.GetContent()); err != nil {
		return nil, translateErr("AppendToFile", err)
	}
	return &workerv1.AppendToFileResponse{}, nil
}

func (s *saferWorkerServer) LoadFile(_ context.Context, req *workerv1.LoadFileRequest) (*workerv1.LoadFileResponse, error) {
	user, err := authenticate(req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	if req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: filename is required")
	}
	content, err := user.LoadFile(req.GetFilename())
	if err != nil {
		return nil, translateErr("LoadFile", err)
	}
	return &workerv1.LoadFileResponse{Content: content}, nil
}

// authenticate validates the request's credentials fields and derives the
// caller's SAFER user, exactly as every SAFER operation already requires.
func authenticate(username, password string) (*client.User, error) {
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: username is required")
	}
	if password == "" {
		return nil, status.Error(codes.InvalidArgument, "worker: password is required")
	}
	user, err := client.GetUser(username, password)
	if err != nil {
		return nil, translateErr("GetUser", err)
	}
	return user, nil
}

// translateErr reports a SAFER operation failure as FailedPrecondition.
//
// This is deliberately coarse: the client package returns plain errors
// with no exported sentinel types (wrong password, a missing file, a lost
// race under strict 2PL, a stale fencing token, and an unreachable backend
// are all just `error`), so there is no reliable signal here to split into
// finer-grained codes like NotFound or PermissionDenied without fragile
// string matching against messages that are not part of any API contract.
// FailedPrecondition -- "the operation could not be completed given the
// current state" -- is accurate for all of them; the original message is
// preserved so a caller or operator can read what actually happened.
func translateErr(op string, err error) error {
	return status.Errorf(codes.FailedPrecondition, "worker: %s: %v", op, err)
}
