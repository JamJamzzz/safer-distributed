package main

// Instance identification: a tiny diagnostic surface, entirely separate
// from worker.v1's business RPCs, that lets a caller (cmd/loadgen, or a
// test) observe which worker replica actually served a given request. See
// internal/workerdiag's package doc for why this rides on gRPC response
// metadata rather than a protobuf field.
//
// This exists because of a real gap Phase 4.5 closes: cmd/loadgen and
// docs/distributed-roadmap.md previously asserted that pointing loadgen at
// the worker Service exercised all replicas, without any way to check
// that claim. A single long-lived HTTP/2 connection through a Kubernetes
// ClusterIP Service is generally pinned to one backend pod by kube-proxy;
// nothing about that setup actually spreads RPCs across replicas. See
// cmd/loadgen/dial.go for the fix (client-side round_robin balancing) and
// this file for the evidence that it worked.

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/JamJamzzz/safer-distributed/internal/workerdiag"
)

// instanceID identifies this worker process, computed once at startup.
//
// In Kubernetes, a pod's hostname is its pod name by default -- exactly
// the operationally meaningful answer to "which replica handled this".
// The PID is appended so that multiple worker processes on one machine
// (every local test in this repository) are still distinguishable, since
// they all share one hostname.
func instanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// instanceHeaderInterceptor attaches id to every RPC's response header.
//
// A unary server interceptor is the right layer for this: it runs for
// every RPC method without service.go's handlers needing to know or care
// about it, keeping the diagnostic concern fully separate from SAFER
// business logic.
func instanceHeaderInterceptor(id string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Best-effort: a header that fails to set does not fail the RPC
		// itself, since this is diagnostic information, not part of the
		// service's actual contract.
		_ = grpc.SetHeader(ctx, metadata.Pairs(workerdiag.InstanceHeaderKey, id))
		return handler(ctx, req)
	}
}
