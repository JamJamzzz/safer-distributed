# syntax=docker/dockerfile:1
#
# Builds cmd/coordinator: the SAFER lock coordinator.
#
#   docker build -f docker/coordinator/Dockerfile -t safer-coordinator:local .
#
# See docker/worker/Dockerfile for the shared build-stage rationale
# (checked-in generated proto sources, no protobuf toolchain needed,
# CGO_ENABLED=0 static binary, distroless nonroot runtime).

FROM golang:1.20-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/coordinator ./cmd/coordinator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/coordinator /usr/local/bin/coordinator

EXPOSE 50051

# cmd/coordinator's own default (-addr 127.0.0.1:0) is meant for local
# tests that print the bound address and dial it in the same process; a
# container needs a fixed, externally reachable port instead. This is the
# image's default, not a hidden production policy -- override it with
# container args (Kubernetes does, in deploy/kubernetes/coordinator).
ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["-addr", "0.0.0.0:50051"]
