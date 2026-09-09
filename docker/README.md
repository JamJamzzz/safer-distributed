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

## What is deliberately not here

MongoDB does not get an image in this repository -- it is a managed
dependency, run from the upstream `mongo:7` image, exactly as CI and every
other integration test in this repository already do. Building a custom
MongoDB image would not change SAFER's storage or concurrency-control
semantics and is out of scope.
