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
- **Phase 3B — Atomic MongoDB persistence.** Done. See "Multi-object writes are
  atomic" below.
- **Phase 3C — Leases and fencing.** Next. `RenewLease`, lease expiry so a crashed
  worker's locks are reclaimed, and fencing tokens so a revived worker cannot act on a
  lock it has lost. Fencing has to be validated atomically with the writes it guards,
  which is why the transaction substrate came first. This is what turns the
  graceful-operation claim into one that survives failure.

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
about SAFER's logical resources: it never sees a Namespace or a File, only opaque
encrypted blobs at UUIDs.

Strict 2PL, S/X locks, and FIFO fairness live in SAFER's lock layer, which since Phase
3A reaches across processes through the lock coordinator rather than being confined to
one process. The two layers stay separate and are not substitutes for each other:

- **MongoDB** makes a multi-object mutation atomic and durable (Phase 3B). It does not
  decide who may mutate, or in what order.
- **The coordinator** decides who holds which logical resource, and in what order
  (Phase 3A). It stores no SAFER data.

So workers sharing one MongoDB are serialized by the coordinator, not by the database.
Two workers pointed at the same database but at *different* coordinators — or at none —
are still not serialized with respect to each other, because nothing in MongoDB
enforces SAFER's logical locking.

The userlib backend's global datastore latch is deliberately not carried over. It
exists because userlib's Go maps are unsynchronized in-process state; imposing it here
would serialize a backend whose purpose is concurrent shared access, and would not
coordinate anything across processes anyway.

### Multi-object writes are atomic (Phase 3B)

Several SAFER operations write several objects. On MongoDB each such mutation now runs
inside one transaction, so it commits entirely or not at all.

The boundary is scoped by **context**, not by anything global. `storage.RunAtomic`
hands the callback a context carrying a MongoDB session; storage calls made with that
context — and only those — join the transaction. The session travels from the
`operationContext()` at the top of a public operation down that operation's own call
stack, so two concurrent operations each get their own transaction with no shared
mutable state between them. There is no process-global "current session", no goroutine
identity, and no backend swapping, each of which would be wrong the moment two
operations overlap. Nesting is refused rather than silently flattened.

The order around it is deliberate:

```
acquire logical SAFER locks -> revalidate state -> Mongo transaction -> commit -> end transaction
```

A Mongo transaction is never opened and then made to wait for a lock; that would pin
database resources for the length of another worker's critical section.

The `userlib` backend has no transaction capability, so `RunAtomic` runs the callback
directly there. That is V1's behavior — not a weaker transaction but no transaction —
and it is left honest rather than faked, since a transaction that silently commits
partial state would be worse than none.

Transactions require a replica set. A standalone `mongod` accepts writes but rejects
transactions, which is why CI runs a single-node replica set.

#### Mutation boundaries

Each operation's commit point, and what a partial commit would mean:

| Operation | Objects written in one transaction | Partial commit would mean |
| --- | --- | --- |
| `InitUser` | 2 public keys + account record | An unrepairable account: key registration is write-once, so a claimed username with missing keys can never be fixed |
| `StoreFile` (create) | chunk, metadata, access box, status, structure, namespace entry | A namespace entry pointing at a missing access box, or orphaned ciphertext nothing references — both unreachable through SAFER's API, so never cleaned up |
| `StoreFile` (overwrite) | new chunk, metadata switch, old-chunk reclamation | Metadata naming a chunk that was never written, or old chunks reclaimed while metadata still references them — an unreadable file |
| `AppendToFile` | new chunk + metadata | The append silently dropped while consuming a Version, or a tail that cannot be read |
| `CreateInvitation` | branch access box, structure entry, invitation | A capability pointing at nothing, or a grant `RevokeAccess` cannot find — a permanently unrevokable share |
| `AcceptInvitation` | namespace entry + invitation consumption | An invitation that can be accepted twice, or a recipient with no route to the file |
| `RevokeAccess` | new chunk, new metadata, owner box, **every surviving branch box**, structure, status, and reclamation of the revoked box, old metadata and old chunks — a variable count that grows with the number of surviving recipients and old chunks | The worst case: a file unreadable to everyone, or an owner who believes access was withdrawn while the revoked box is still in place |

#### Rollback evidence

`integration/rollback_test.go` fails the Nth storage mutation inside a real operation
against a real replica set. `StoreFile` create and `RevokeAccess` measure their own
mutation counts at runtime and fail **every** mutation in turn rather than assuming a
number — `RevokeAccess`'s count is not fixed, since it scales with the surviving
recipients and old chunks of the file being revoked.
Each iteration asserts the operation errors, the object count is unchanged, previously
valid state still loads and authenticates, and Version/ChunkCount and authorization are
unchanged — including that a failed revocation does not half-revoke.

The negative control is what makes those assertions mean something: with the
transaction capability removed and the identical failure injected, the same `StoreFile`
leaves **5 partially committed objects** behind. It fails loudly if it ever stops
leaking.

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
- Multi-object MongoDB writes are atomic as of Phase 3B, but that covers a mutation
  that fails; it does nothing about a worker that dies still holding locks.
- The gRPC channel is not encrypted or authenticated. SAFER's objects are encrypted and
  authenticated before reaching storage, so this channel carries no plaintext content
  and no key material -- only resource identifiers and lock modes -- but an attacker on
  it could forge lock traffic. It is meant for a trusted network.

## Claims not yet earned

Recorded here so they are not asserted prematurely:

- **Cross-process strict 2PL plus atomic durable MongoDB mutations, under graceful
  operation only.** Earned: strict 2PL across separate OS processes sharing one
  coordinator and one database, and all-or-nothing multi-object persistence, both
  verified by real multi-process/failure-injection tests with negative controls. Not
  earned: any behavior under worker crashes, coordinator crashes, or partitions.
- **No worker crash tolerance, no lease recovery, no stale-writer protection, and no
  coordinator fault tolerance.** Atomic storage means a mutation that *fails* leaves
  nothing behind. It does not help a worker that *dies* mid-operation still holding
  locks: those locks stay held until the coordinator restarts.
- **Durability, plus atomicity of failed mutations.** Verified: data written through
  one SAFER client and connection pool is readable through a separate one afterwards,
  and a mutation that fails part way leaves no committed partial state. Not verified:
  crash consistency — a killed process mid-commit is not the same as a returned error,
  and there are no kill tests.
- **No fault-tolerance claim** — failure testing covers an injected failing storage
  backend, an unreachable one, an unreachable coordinator at startup, and cancelled or
  abandoned lock requests. There is no kill-the-database, kill-the-coordinator,
  kill-the-worker, or partition testing, and no lease or fencing machinery to make such
  tests meaningful yet. That is Phase 3C.
- The checked-in benchmark tables are inherited historical SAFER-CC measurements, not
  performance claims about this repository.
