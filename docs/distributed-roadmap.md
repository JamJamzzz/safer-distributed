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
   locks, and FIFO fairness — but entirely in-process, over Go mutexes and condition
   variables. There is no deadlock *detector*: no wait-for-graph, no victim selection.
   Deadlock is *avoided* by a fixed acquisition order (Namespace before File) that each
   API operation follows; the lock manager itself has no opinion about ordering. Two SAFER processes over the same data share no
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

  One gap remains from this phase: storage calls carry `context.Background()`.
  Threading per-operation contexts through the public SAFER API would change that API.
  (The other Phase 1 gap, wrappers with error-free signatures, was closed in Phase 2:
  the wrappers now return backend errors and every call site handles them.)
- **Phase 2 — MongoDB backend.** Done. `client/storage/mongostore` implements the
  Phase 1 interfaces on MongoDB, and `client.UseStorage` installs a backend at
  startup. See "Running SAFER on MongoDB" below.

- **Phase 2.5 — Repository and CI hygiene.** Done. Module path is now
  `github.com/JamJamzzz/safer-distributed`; CI runs a third job against a MongoDB
  single-node replica set (a replica set because the next storage phase needs
  transactions, which a standalone `mongod` cannot serve) and fails if integration
  tests skip.
- **Phase 3A — Remote coordination.** Done. See "Cross-process coordination" below.
- **Phase 3B — Leases and fencing.** Next. `RenewLease`, lease expiry so a crashed
  worker's locks are reclaimed, and fencing tokens so a revived worker cannot act on a
  lock it has lost. This is what turns Phase 3A's graceful-operation claim into
  something that survives failure.

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

## Cross-process coordination (Phase 3A)

Multiple SAFER worker processes now share one lock coordinator, so strict 2PL holds
across OS processes and not merely across goroutines.

```
worker process   worker process   worker process
       \               |               /
        \              |              /       gRPC
         +------ lock coordinator ----+
                       |
              (the same generic
               client/lockmanager)
       ...all sharing one MongoDB for storage
```

Run it:

```bash
docker run -d -p 27017:27017 --name safer-mongo mongo:7
go run ./cmd/coordinator -addr 127.0.0.1:50051
SAFER_MONGO_URI=mongodb://localhost:27017 SAFER_COORDINATOR_ADDR=127.0.0.1:50051 go test ./integration/crossprocess/
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `SAFER_COORDINATOR_ADDR` | *(none)* | Coordinator address. Unset means "no remote coordinator"; there is no guessed default. |
| `SAFER_COORDINATOR_TIMEOUT` | `10s` | Bounds dialing and `Health`. It deliberately does **not** bound `Acquire`. |

### Design

- **The LockManager is reused, not reimplemented.** S/X compatibility, FIFO fairness,
  the ResourceID model, and holder/waiter bookkeeping all stay in `client/lockmanager`.
  The coordinator translates wire types and maps external transaction UUIDs onto the
  existing internal `uint64` TxnIDs, which are unchanged.
- **Three RPCs: `Acquire`, `EndTransaction`, `Health`.** No `BeginTransaction`: a worker
  generates its transaction UUID locally, and the coordinator creates state lazily on
  first use, so a Begin would only add a round trip and a failure mode. No per-resource
  `Release`: under strict 2PL locks are released only when the transaction ends.
- **Cancellation is real.** `LockManager.AcquireContext` removes a cancelled request
  from the wait queue, so a worker that disconnects cannot be granted a ghost lock
  later. Cancellation is checked before grantability, and a request still queued when
  its transaction ends is released immediately if it is granted afterwards.
- **`Acquire` has no client-side deadline.** Waiting is the correct outcome of
  contention under strict 2PL; a timeout would turn ordinary contention into failure.
- **Benchmark strategies stay local.** `StrategyGlobalLock` and `StrategyNoCC` measure
  this process's alternatives to fine-grained 2PL, so they do not route through the
  coordination backend.

### Evidence

`integration/crossprocess` starts one coordinator process and two or more independent
worker processes -- each with its own MongoDB connection, its own gRPC connection, and
its own SAFER client state -- against one database. Workers rendezvous at an explicit
barrier and are released together, so operations genuinely overlap; no test uses a
sleep to create a race. Covered: concurrent append vs append, append vs overwrite,
load vs overwrite, five simultaneous appenders, and a control asserting the writers
were actually serialized.

The suite was validated by negative control: with the workers' coordinator wiring
removed so each falls back to its own process-local LockManager, the same tests fail
with real lost updates (`base-AAA` missing the second append; four of five fragments
lost). The tests detect the failure they claim to.

### What this does not cover

- A worker that dies without calling `EndTransaction` **leaks its locks** until the
  coordinator restarts. There are no leases yet.
- A coordinator crash **loses all lock state**, and workers holding locks are not told.
  It is a single process with in-memory state, not replicated.
- There are no fencing tokens, so nothing stops a stalled worker from acting after its
  lock should have been considered lost.
- Multi-object MongoDB writes are still not atomic (see the storage limitation above).
- The gRPC channel is not encrypted or authenticated. SAFER's objects are encrypted and
  authenticated before reaching storage, so this channel carries no plaintext content
  and no key material -- only resource identifiers and lock modes -- but an attacker on
  it could forge lock traffic. It is meant for a trusted network.

## Claims not yet earned

Recorded here so they are not asserted prematurely:

- **Cross-process coordination, under graceful operation only.** Earned: strict 2PL
  across separate OS processes sharing one coordinator and one database, verified by
  multi-process tests and a negative control. Not earned: any behavior under crashes,
  partitions, or coordinator failure.
- **Durability, narrowly.** Verified: data written through one SAFER client and
  connection pool is readable through a separate one afterwards, which is what a worker
  restart looks like from storage's point of view. Not verified: crash consistency, or
  behavior under a mid-operation failure — see the atomicity limitation above.
- **No fault-tolerance claim** — failure testing covers an injected failing storage
  backend, an unreachable one, an unreachable coordinator at startup, and cancelled or
  abandoned lock requests. There is no kill-the-database, kill-the-coordinator,
  kill-the-worker, or partition testing, and no lease or fencing machinery to make such
  tests meaningful yet. That is Phase 3B.
- The checked-in benchmark tables are inherited historical SAFER-CC measurements, not
  performance claims about this repository.
