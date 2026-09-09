# syntax=docker/dockerfile:1
#
# Builds cmd/worker: the production, long-running SAFER worker service.
#
# Build from the repository root so the module's go.sum and every package
# the binary imports are in the build context:
#
#   docker build -f docker/worker/Dockerfile -t safer-worker:local .
#
# The generated proto/worker/v1/*.pb.go files are checked in (see
# proto/README.md), so this build needs no protobuf toolchain.

FROM golang:1.20-bookworm AS build
WORKDIR /src

# Cached separately from the rest of the source so an application code
# change does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/worker ./cmd/worker

# distroless/static: no shell, no package manager, nothing beyond what a
# statically linked Go binary needs -- which is nothing, since
# CGO_ENABLED=0 means no libc dependency either. The "nonroot" variant
# runs as uid/gid 65532 by default, so the container needs no separate
# USER setup to avoid running as root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /usr/local/bin/worker

# The worker's own default listen address (see cmd/worker/config.go)
# already binds every interface on this port; EXPOSE is documentation for
# `docker run -P` and orchestrators, not a security boundary.
EXPOSE 50052

ENTRYPOINT ["/usr/local/bin/worker"]
