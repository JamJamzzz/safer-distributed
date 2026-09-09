## SAFER Distributed

This repository is the distributed evolution of SAFER-CC (V1), which remains the
canonical single-process implementation. See
[docs/distributed-roadmap.md](docs/distributed-roadmap.md) for the fork's origin,
phase plan, and current limitations.

Storage now sits behind an `ObjectStore`/`KeyStore` abstraction
([client/storage](client/storage)) with two backends: the legacy in-memory `userlib`
one (the default, preserving V1 behavior) and a durable MongoDB one
([client/storage/mongostore](client/storage/mongostore)).

To run the suite against MongoDB:

```bash
docker run -d -p 27017:27017 --name safer-mongo mongo:7
SAFER_MONGO_URI=mongodb://localhost:27017 go test -count=1 ./...
```

Without `SAFER_MONGO_URI`, MongoDB integration tests skip cleanly and every other test
still runs. MongoDB provides durable shared persistence only -- it is not the
concurrency-control mechanism, and SAFER's locking is still process-local, so multiple
workers sharing one database are not yet safely serialized.

## CI and concurrency verification

To run all packages, including the white-box, black-box, lock-manager, and
benchmark UI tests, run `go test -count=1 ./...` from the repository root.
Running only inside `client_test` does not run the other packages' tests.

[CI workflow](.github/workflows/ci.yml) runs on pull requests and pushes to
`main`, with read-only repository permissions. Two independent GitHub-hosted
`ubuntu-latest` jobs run the complete suite:

```bash
go test -mod=readonly -count=1 -v -timeout=10m ./...
CGO_ENABLED=1 CC=gcc go test -race -mod=readonly -count=1 -v -timeout=15m ./...
```

Go is selected from `go.mod` (`go 1.20`, no separate `toolchain` directive),
using the latest patch of that release series. No module-version change is
part of this CI work. The normal job disables cgo; this project does not
require it. The race job verifies Go, `CGO_ENABLED=1`, GCC, and the package
list before testing. `-count=1` disables test-result caching and
`-mod=readonly` prevents implicit dependency changes. The limits bound hung
tests; they are not performance thresholds.

The original Windows environment lacked a C compiler and had cgo disabled.
The full race suite passed on Linux after fixing a shared datastore
bandwidth-counter race, in
[hosted run 33740706536](https://github.com/JamJamzzz/safer-with-concurrency-control/actions/runs/33740706536)
(Go 1.20.14, GCC 13.3.0). Linux CI supplies the missing toolchain; it does
not change the local Windows environment. See
[the evidence/status section](review.md#8-race-detector-evidencestatus).
To reproduce the race command locally, use Linux with Go and GCC (or another
Go-supported race-detector platform with its required C toolchain).

The race detector checks memory races in executed paths. The existing
forced-interleaving, strict-2PL, lost-update, and authorization-invariant
tests check logical outcomes. These are complementary: neither proves all
schedules correct or establishes system-wide serializability by itself.
Locks are in-process; crash recovery and cross-process coordination are
outside V1's scope. Benchmarks remain a separate manual tool, not CI
throughput/latency gates or GitHub-runner performance evidence.
The checked-in benchmark tables predate the datastore race fix and are
historical measurements, not current-code performance claims.

[Latest main-branch CI runs](https://github.com/JamJamzzz/safer-with-concurrency-control/actions/workflows/ci.yml?query=branch%3Amain)

## Concurrency benchmark

Run the SAFER-CC correctness and performance benchmark with:

```bash
go run ./cmd/benchmark
```

The command shows workload progress, correctness status, throughput, p95
latency, and lost-write counts in the terminal. It writes the complete results
to `benchmarks/benchmark-results.json` and `benchmarks/benchmark-report.md`.

Useful options:

```bash
go run ./cmd/benchmark -out ./results
go run ./cmd/benchmark -quiet
go run ./cmd/benchmark -color never
```
