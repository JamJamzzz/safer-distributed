# Docker images

Three images, one per deployable command, each a multi-stage build: a
`golang:1.20-bookworm` build stage (matching `go.mod`'s `go 1.20`, no
`toolchain` directive) producing a static (`CGO_ENABLED=0`) binary, copied
into a `gcr.io/distroless/static-debian12:nonroot` runtime stage. Distroless
has no shell and no package manager, and `:nonroot` runs as uid/gid 65532
without any extra `USER` setup. None of the three needs `protoc` at build
time: the generated protobuf sources under `proto/` are checked in (see
`proto/README.md`).

Build from the repository root, so each Dockerfile's build context includes
the whole module:

```bash
docker build -f docker/worker/Dockerfile       -t safer-worker:local       .
docker build -f docker/coordinator/Dockerfile  -t safer-coordinator:local  .
docker build -f docker/loadgen/Dockerfile      -t safer-loadgen:local      .
```

## Manual smoke test

This is the same shape `deploy/kubernetes` runs, just with `docker run`
instead of pods:

```bash
docker network create safer-net
docker run -d --name safer-mongo --network safer-net -p 27017:27017 \
  mongo:7 --replSet rs0 --bind_ip_all
# initiate the replica set (transactions need one; see the CI workflow for
# the wait/health-check loop this needs in practice)
docker exec safer-mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

docker run -d --name safer-coordinator --network safer-net \
  -e SAFER_MONGO_URI="mongodb://safer-mongo:27017/?replicaSet=rs0&directConnection=true" \
  safer-coordinator:local

docker run -d --name safer-worker --network safer-net \
  -e SAFER_MONGO_URI="mongodb://safer-mongo:27017/?replicaSet=rs0&directConnection=true" \
  -e SAFER_COORDINATOR_ADDR="safer-coordinator:50051" \
  safer-worker:local

docker run --rm --network safer-net safer-loadgen:local \
  -addr safer-worker:50052 -workload mixed -concurrency 4 -count 40
```

This exact sequence was run while building Phase 4: the coordinator started
with fencing enabled, the worker connected to both dependencies, and the
load generator completed all four workload types (`independent-writes`,
`same-file-writes`, `reads`, `mixed`) against it with zero errors. Running
`safer-coordinator:local` with no `SAFER_MONGO_URI` set was also verified to
exit non-zero rather than starting unfenced (Phase 4.0).

This single-worker version of the smoke test says nothing about balancing
across replicas -- there is only one worker container -- see the next
section for that.

## Proving cross-replica balancing without a Kubernetes cluster

`cmd/loadgen` balances per-RPC across every address its target resolves to,
client-side, using gRPC's `round_robin` policy (see `cmd/loadgen/dial.go`).
Pointed at a Kubernetes headless Service (`deploy/kubernetes/worker-service-headless.yaml`),
that address resolves to every ready worker pod's own IP. Docker's embedded
DNS does the same thing for containers sharing one `--network-alias`, which
makes it possible to exercise the exact same code path -- gRPC's `dns:///`
resolver plus `round_robin` -- without a Kubernetes cluster at all:

```bash
docker network create safer-net    # if not already created above
docker run -d --name safer-mongo --network safer-net -p 27017:27017 \
  mongo:7 --replSet rs0 --bind_ip_all
docker exec safer-mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

docker run -d --name safer-coordinator --network safer-net \
  -e SAFER_MONGO_URI="mongodb://safer-mongo:27017/?replicaSet=rs0&directConnection=true" \
  safer-coordinator:local

for i in 1 2 3; do
  docker run -d --name "safer-worker-$i" --network safer-net \
    --network-alias safer-worker-multi \
    -e SAFER_MONGO_URI="mongodb://safer-mongo:27017/?replicaSet=rs0&directConnection=true" \
    -e SAFER_COORDINATOR_ADDR="safer-coordinator:50051" \
    safer-worker:local
done

docker run --rm --network safer-net safer-loadgen:local \
  -addr "dns:///safer-worker-multi:50052" -workload mixed -concurrency 8 -count 80
```

Run while building Phase 4.5, this produced:

```
workload=mixed attempted=80 succeeded=80 failed=0 elapsed=1.853s throughput=43.2 ops/s
replicas_served=3 map[6965216501c4-1:31 953e10d280f3-1:33 a85adadc3243-1:32]
```

`replicas_served=3` -- read from the response-header instance identifier
`cmd/worker` attaches to every RPC (`internal/workerdiag`,
`cmd/worker/instance.go`), not assumed from how many containers were
started -- with a near-even 31/32/33 split, confirms the `dns:///` +
`round_robin` mechanism actually spreads load across distinct worker
processes. Pointing loadgen at a single worker's own address (or, before
Phase 4.5, at a container reached through one pinned connection) would
show `replicas_served=1` instead; that was the gap this evidence closes.
See `deploy/kubernetes/README.md` for why the plain `worker-service.yaml`
ClusterIP Service does not give you this on its own.

`succeeded=80 failed=0` alone is not evidence the writes actually landed
correctly -- an RPC reporting success says nothing about whether a
concurrent write silently clobbered it. `attempted`/`succeeded`/`failed`
count RPC outcomes; the absence of a `DATA CORRUPTION` line is the
separate, actual data check (`cmd/loadgen/workload.go`'s `checkOracles`):
for write workloads, every user's final file length must equal its
initial length plus one `-content-size`-sized chunk per successful
append that user made, verified with a `LoadFile` after the run, and
`WorkloadReads` verifies every single read's bytes inline as it happens.
A lost or duplicated write changes the length; a wrong read is caught the
moment it comes back. Either failure exits non-zero, and prints under
`DATA CORRUPTION` rather than being folded into `failed`, which counts
RPC errors, not this.

## What is deliberately not here

MongoDB does not get an image in this repository -- it is a managed
dependency, run from the upstream `mongo:7` image, exactly as CI and every
other integration test in this repository already do. Building a custom
MongoDB image would not change SAFER's storage or concurrency-control
semantics and is out of scope.
