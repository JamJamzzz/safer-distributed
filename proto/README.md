# Protobuf definitions

`coordinator/v1/coordinator.proto` is the wire surface of the SAFER lock
coordinator, and `worker/v1/worker.proto` is the wire surface of the
production worker service (`cmd/worker`). The generated `.pb.go` files are
checked in for both, so building and testing this repository needs no
protobuf toolchain, and neither does building the Docker images in
`docker/` (see `docker/README.md`).

To regenerate after editing a `.proto`:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/JamJamzzz/safer-distributed \
  --go-grpc_out=. --go-grpc_opt=module=github.com/JamJamzzz/safer-distributed \
  proto/coordinator/v1/coordinator.proto proto/worker/v1/worker.proto
```

`protoc-gen-go-grpc` is pinned to v1.3.0 deliberately: v1.5 emits code
requiring a newer `grpc-go` than this module's Go 1.20 line supports.
