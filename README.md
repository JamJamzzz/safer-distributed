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
concurrency-control mechanism.

Lock coordination is likewise pluggable ([client/coordination](client/coordination)):
the default is V1's process-local `LockManager`, and a gRPC lock coordinator
([cmd/coordinator](cmd/coordinator)) extends the same strict-2PL semantics across
separate worker processes. `integration/crossprocess` exercises that with real
multi-process races.

On MongoDB, every multi-object SAFER mutation runs in one transaction, so a failure
part way through commits nothing; `integration/rollback_test.go` verifies this by
failing each mutation of a real operation in turn, with a negative control showing the
same failure leaks partial state when the boundary is bypassed. Transactions need a
replica set, which is what CI runs.

Worker failure is handled: transactions carry a lease, a crashed worker's locks are
reclaimed automatically, and exclusive grants carry fencing tokens validated by a
conditional write inside the commit transaction, so a stalled worker that wakes after
losing its lock cannot commit. `integration/crossprocess` proves both with real killed
and stalled processes, alongside a negative control showing the clobbering write that
fencing prevents.

The single coordinator remains an explicit failure domain: fence state is durable, but
lock state is in memory, so a coordinator crash still loses who held what.

Phase 4 turns this into a deployable service. `cmd/worker` is a real, long-running,
stateless worker process (distinct from `cmd/saferworker`, the single-operation test
helper the crossprocess harness spawns and kills per call), serving
[worker.v1.SaferWorker](proto/worker/v1/worker.proto) over gRPC. `cmd/coordinator` now
fails closed rather than warning-and-continuing when no durable fencing store is
configured. `cmd/loadgen` drives functional/correctness load against the worker
Service. `docker/` containerizes all three, and `deploy/kubernetes/` deploys 3 worker
replicas behind one Service and a singleton coordinator with a no-overlap rollout
strategy, with distinct readiness/liveness health checks and NetworkPolicy-based
isolation. See [docs/distributed-roadmap.md](docs/distributed-roadmap.md#cloud-native-deployment-phase-4)
and each directory's own README for what was actually run versus statically validated.

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
The full race suite first passed on Linux after fixing a shared datastore
bandwidth-counter race, in SAFER-CC's
[hosted run 33740706536](https://github.com/JamJamzzz/safer-with-concurrency-control/actions/runs/33740706536)
(Go 1.20.14, GCC 13.3.0) -- that link is V1 evidence in the upstream
repository, not a run of this one; this repository's own runs are linked
below. Linux CI supplies the missing toolchain; it does
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

[Latest main-branch CI runs](https://github.com/JamJamzzz/safer-distributed/actions/workflows/ci.yml?query=branch%3Amain)

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

## Final distributed evidence

The distributed system (3 stateless workers, a centralized gRPC strict-2PL lock
coordinator, MongoDB with atomic transactions, leases and fencing, OpenTelemetry
into Datadog) was measured in a controlled campaign on a single-node `kind`
cluster. Full report and raw evidence:
[`benchmarks/distributed-final/2026-09-09/final-campaign-report.md`](benchmarks/distributed-final/2026-09-09/final-campaign-report.md).

The headline results, at commit `3fa018e`:

- **Correctness held throughout.** 33 valid runs and more than 18,000 operations
  with zero failures, zero lost updates caught by the load generator's own data
  oracle, and no worker or coordinator restarts. All three replicas served every
  run, verified per-RPC rather than assumed.
- **A matched A/B controlled for operation type while contrasting contention
  shape.** `same-file-writes` (all callers serialized on one exclusive file lock)
  was compared against `independent-writes` (a file per caller) under otherwise
  identical settings. At concurrency ≤ 8 the contended workload showed no
  repeatable throughput penalty distinguishable from run-to-run variation.
- **Observability showed why that is not the whole story.** Datadog recorded
  lock-manager wait rising about three orders of magnitude under contention, from
  tens of microseconds to tens of milliseconds. Coordination cost was real; it
  simply was not the binding throughput constraint at this scale. Every workload
  shape — including read-only traffic that never takes an exclusive lock —
  converged on the same throughput ceiling, and workers were measurably
  CPU-quota-throttled throughout.

That combination is the point: the component that looked expensive was not the one
setting the limit, which removed the evidence basis for adding coordinator
replication or sharding.

**Limitations.** These are single-node `kind` measurements at development resource
limits, on a host shared with MongoDB, the coordinator and the Datadog Agent. They
support relative comparison and bottleneck attribution within this deployment. They
are **not** production capacity, multi-node, availability or fault-tolerance claims;
the report is explicit about what the evidence does not earn.

Note that the older files directly under `benchmarks/` are separate **V1** evidence
measuring process-local concurrency strategies over in-memory storage, and are not
comparable with the distributed results above.
