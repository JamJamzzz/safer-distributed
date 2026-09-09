# syntax=docker/dockerfile:1
#
# Builds cmd/loadgen: the SAFER worker-service load generator.
#
#   docker build -f docker/loadgen/Dockerfile -t safer-loadgen:local .
#
# See docker/worker/Dockerfile for the shared build-stage rationale.
# Unlike worker and coordinator, loadgen listens on nothing -- it is
# purely a client -- so there is no EXPOSE and no fixed default port.

FROM golang:1.20-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/loadgen /usr/local/bin/loadgen

ENTRYPOINT ["/usr/local/bin/loadgen"]
