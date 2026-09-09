// Package workerdiag defines the small diagnostic surface a SAFER worker
// uses to identify which replica served a given RPC.
//
// This is deliberately NOT part of worker.v1's protobuf messages
// (proto/worker/v1/worker.proto). "Which pod handled this" is operational
// metadata about the deployment, not part of SAFER's business API -- a
// caller of InitUser/StoreFile/AppendToFile/LoadFile has no reason to
// carry a replica identifier in its request or response types, and adding
// one there would contaminate SAFER's semantics with a Phase-4-only
// deployment concern. gRPC response metadata (a header, not a message
// field) carries it instead, entirely outside the wire contract the
// service methods define.
//
// It is a package of its own, rather than living in cmd/worker or
// cmd/loadgen, so both the server side (cmd/worker, which sets the
// header) and the client side (cmd/loadgen and tests, which read it) agree
// on the exact key without duplicating a string literal that would
// silently drift if edited in only one place.
package workerdiag

// InstanceHeaderKey is the gRPC response header a worker attaches to
// every RPC it serves, naming the replica that served it (see
// cmd/worker's instance ID, derived from its hostname -- a pod's own name
// in Kubernetes -- plus its PID, so that multiple replicas sharing a
// hostname in local testing are still distinguishable).
//
// gRPC lower-cases and requires metadata keys to be valid HTTP/2 header
// names; this one already is.
const InstanceHeaderKey = "x-safer-worker-instance"
