# SAFER Distributed — Roadmap

## Origin

This repository was forked (source copy, no shared git history) from **SAFER-CC**
(`safer-with-concurrency-control`), the completed single-process concurrency-control
implementation. SAFER-CC is the canonical V1 and is treated here as an **immutable
baseline**: it is not modified by this project. `safer-distributed` is an independent
distributed evolution of that code.

The import excluded `.git` and carried over the full source tree, tests, CI workflow,
documentation (`docs/concurrency-control.md`, `review.md`, `CHANGELOG.md`), and the
intentionally versioned benchmark evidence under `benchmarks/`.

## Baseline verification (Phase 0)

`go test -mod=readonly -count=1 ./...` on the imported tree:

```
ok  .../client            19.3s
ok  .../client/lockmanager 1.3s
ok  .../client_test       17.6s
ok  .../cmd/benchmark      0.4s
```

`go vet ./...` and `go build ./...` are clean. The benchmark tool builds; it was not
run during import, because running it rewrites the versioned evidence files in
`benchmarks/`.

**Race detector: not runnable in this environment.** `go test -race` requires cgo, and
this Windows host has no C compiler:

```
$ CGO_ENABLED=1 go test -race ./client/lockmanager
cgo: C compiler "gcc" not found: exec: "gcc": executable file not found in %PATH%
```

This is the same environment limitation SAFER-CC's README documents. The inherited
`.github/workflows/ci.yml` runs the race suite on Linux with `CGO_ENABLED=1 CC=gcc`.
The check is not weakened or simulated locally; it is deferred to Linux CI.

## What V1 is, and where it stops

V1 is correct and well tested **within one process**. Its two structural limits:

1. **Process-local locking.** `client/lockmanager` implements strict 2PL with S/X
   locks, FIFO fairness, and deadlock handling — but entirely in-process, over Go
   mutexes and condition variables. Two SAFER processes over the same data share no
   lock state at all, so nothing prevents them from interleaving destructively. V1
   makes no cross-process correctness claim.
2. **Non-durable `userlib` storage.** The datastore and keystore are unsynchronized
   in-memory Go maps inside the process. Nothing survives a restart, and nothing is
   visible to a second process. The `datastoreMu`/`keystoreMu` latches in `client.go`
   exist only because those maps are not concurrency-safe; they are a storage-engine
   detail of the `userlib` backend, not part of SAFER's transaction semantics.

## Phases

- **Phase 0 — Import and verify the baseline.** Done. Tree copied, suite green, race
  detector limitation documented rather than worked around.
- **Phase 1 — Storage abstractions.** Done. `client/storage` defines `ObjectStore` and
  `KeyStore`; `UserlibObjectStore` / `UserlibKeyStore` implement them over `userlib`
  and preserve V1 behavior exactly, including the `datastoreMu`/`keystoreMu` latches,
  which moved out of `client.go` into the backend they belong to. `client.go` keeps its
  five wrapper functions (so ~40 call sites are untouched) but delegates through the
  interfaces. `NewUserlibStorage()` remains the default backend.

  Two known gaps, both deliberate:
  - The wrappers still have V1's error-free signatures. A backend failure is surfaced
    through `noteStorageFailure` (logged and recorded, never silently dropped) and a
    failed read reports absence rather than handing unverified bytes to the crypto
    layer. Plumbing real error returns through the wrappers and their call sites is
    the first task of Phase 2, done against a backend that can actually fail.
  - Storage calls carry `context.Background()`. Threading per-operation contexts
    through the public SAFER API would change that API, which Phase 1 does not.
- **Phase 2 — MongoDB backend.** Next. Shared durable persistence of the *same* encrypted,
  authenticated blobs under the *same* logical UUIDs. Minimal schema (`objects`,
  `public_keys`), configuration via environment, integration tests that skip cleanly
  when MongoDB is absent. MongoDB is persistence only — **not** the concurrency-control
  mechanism.
- **Phase 3 — Remote coordination.** A single Lock Coordinator wrapping the existing
  generic `LockManager` core, reached over gRPC by multiple SAFER workers. Small,
  strict-2PL-oriented surface: `BeginTransaction`, `Acquire`, `RenewLease`,
  `EndTransaction`, `Health`. Leases and fencing tokens. No consensus, no replication,
  no lock sharding.

Cryptography, authenticated envelopes, UUID addressing, authorization/capability
semantics, Namespace/File resources, Version/Epoch behavior, and the public SAFER API
are preserved unchanged across every phase.

## Explicitly out of scope

Kafka, Redis, Raft/Paxos or any custom consensus, hand-rolled 2PC, MongoDB sharding,
multi-region deployment, MVCC, and distributed lock sharding.

## Claims not yet earned

Recorded here so they are not asserted prematurely:

- **No distributed correctness claim** — there are no cross-process tests yet.
- **No durability claim** — storage is still the in-memory `userlib` maps.
- **No fault-tolerance claim** — there are no failure-injection tests yet.
- The checked-in benchmark tables are inherited historical SAFER-CC measurements, not
  performance claims about this repository.
