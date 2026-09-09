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
- **Phase 2 — MongoDB backend.** Done. `client/storage/mongostore` implements the
  Phase 1 interfaces on MongoDB, and `client.UseStorage` installs a backend at
  startup. See "Running SAFER on MongoDB" below.

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

## Running SAFER on MongoDB

```bash
docker run -d -p 27017:27017 --name safer-mongo mongo:7
SAFER_MONGO_URI=mongodb://localhost:27017 go test ./...
```

Configuration comes from the environment; no credentials are hard-coded and there is
no default URI, so a missing `SAFER_MONGO_URI` is an error rather than a silent
connection to some default host.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SAFER_MONGO_URI` | *(none)* | Connection string, including credentials. Unset means "no MongoDB configured". |
| `SAFER_MONGO_DB` | `safer` | Database name. |
| `SAFER_MONGO_TIMEOUT` | `10s` | Bounds connect and per-operation work when the caller has no deadline. Accepts `30` as well as `30s`. |

A process opts in explicitly:

```go
store, err := mongostore.Open(ctx, cfg)
restore := client.UseStorage(store.Storage())
```

The default backend remains the in-memory `userlib` one, so V1 behavior is what you
get unless a backend is installed deliberately.

### Schema

```
objects      { _id: "<uuid>", value: <binary> }
public_keys  { _id: "<key name>", key_type: "PKE"|"DS", public_key: <PKIX DER> }
```

Objects are addressed by the same logical UUIDs SAFER already derives, and hold the
encrypted, authenticated envelope verbatim. MongoDB sees ciphertext and nothing else:
the cryptographic object layout was not redesigned to look more database-like. Key
registration is write-once, enforced by the unique `_id` index, so SAFER's identity
semantics now hold across every worker rather than within one process. No sharding, no
additional indexes.

### What MongoDB is not

MongoDB is persistence. It performs no locking on SAFER's behalf and knows nothing
about SAFER's logical resources. Strict 2PL, S/X locks, and FIFO fairness remain in
SAFER's lock layer, which is still process-local. Two SAFER workers sharing one
MongoDB are **not** safely serialized today; that is exactly what Phase 3 is for.

The userlib backend's global datastore latch is deliberately not carried over. It
exists because userlib's Go maps are unsynchronized in-process state; imposing it here
would serialize a backend whose purpose is concurrent shared access, and would not
coordinate anything across processes anyway.

### Known limitation: multi-object writes are not atomic

Several SAFER operations write several objects in sequence (file creation writes a
chunk, metadata, an access box, a status record, a structure record, and a namespace
entry). With the `userlib` backend these writes could not fail. With MongoDB they can,
and there is currently no transaction around them: a backend failure part-way through
leaves some objects written and others not.

This is a real gap, not a theoretical one. The operation reports the error rather than
claiming success, and SAFER's authenticated envelopes mean a partial write cannot be
passed off as valid content — but the stored state can be left incomplete. Wrapping
these sequences in MongoDB transactions is the natural next storage-layer step; it
needs a replica set, since standalone `mongod` does not support them.

## Claims not yet earned

Recorded here so they are not asserted prematurely:

- **No distributed correctness claim** — locking is still process-local, and there are
  no cross-process tests. A shared database does not make concurrent workers safe.
- **Durability, narrowly.** Verified: data written through one SAFER client and
  connection pool is readable through a separate one afterwards, which is what a worker
  restart looks like from storage's point of view. Not verified: crash consistency, or
  behavior under a mid-operation failure — see the atomicity limitation above.
- **No fault-tolerance claim** — the only failure tests are an injected failing backend
  and an unreachable one. There is no kill-the-database or partition testing.
- The checked-in benchmark tables are inherited historical SAFER-CC measurements, not
  performance claims about this repository.
