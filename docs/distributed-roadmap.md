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
- **Phase 3C — Worker-failure recovery.** Done. See "Worker failure" below.
- **Phase 4 — Cloud-native deployment.** Done. See "Cloud-native deployment" below.

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

- A coordinator crash **loses all lock state**, and workers holding locks are not told.
  It is a single process with in-memory state, not replicated. A *worker* crash is
  handled as of Phase 3C; a *coordinator* crash is not.
- Multi-object MongoDB writes are atomic as of Phase 3B, but that covers a mutation
  that fails; it does nothing about a worker that dies still holding locks.
- The gRPC channel is not encrypted or authenticated. SAFER's objects are encrypted and
  authenticated before reaching storage, so this channel carries no plaintext content
  and no key material -- only resource identifiers and lock modes -- but an attacker on
  it could forge lock traffic. It is meant for a trusted network.

## Worker failure (Phase 3C)

A worker can die. Phase 3C makes that survivable: its locks come back on their own, and
if it wakes up later it cannot act on a lock it has lost.

### Leases

A transaction's lease starts when its **first lock is granted**, not when the
transaction first appears - a transaction queued behind someone else should not be
punished for waiting. The worker renews while it works, and the coordinator is
authoritative for the deadline: every response carries it, and the worker schedules its
next renewal at a third of the time actually remaining. That last part was a bug the
cross-process tests caught - a worker pacing renewals from a locally configured
interval got revoked while perfectly healthy, because the coordinator's lease was
shorter than the worker assumed.

A passed deadline **releases nothing by itself**. It makes a transaction eligible for
revocation, which runs in a fixed order:

1. mark it revoking - no further `Acquire`, no further `RenewLease`
2. cancel its in-flight `Acquire` requests
3. durably invalidate every exclusive fence it owns
4. **only then** release its locks through the LockManager
5. leave a tombstone, so a returning worker is told its transaction is over rather than
   silently handed a fresh one

Step 3 before step 4 is the safety property: releasing first would let the next holder
begin writing while the previous holder's token was still valid.

If the fence store is unreachable during step 3, the locks **stay held** and cleanup
retries. `Health` reports the stuck transaction, so an operator can see that locks are
held deliberately rather than leaked. This is a deliberate choice of availability loss
over unsafe handoff.

### Fencing

Exclusive grants carry a fencing token; shared grants do not, because a stale reader
corrupts nothing and fencing readers would make them conflict for no benefit. Tokens
only move forward, and allocation **fails closed**: if a token cannot be made durable
the grant is refused and the lock given back, since an unfenced exclusive grant is a
writer nothing could later stop.

The validation at commit is a **conditional write, not a read**, and that distinction
is the whole design. SAFER's storage transactions use MongoDB snapshot reads, so a
transaction that merely *read* the fence document would see its own token as of the
snapshot it started with - still apparently valid no matter what happened since - and
commit anyway. Updating the same document the coordinator writes creates a real
write-conflict boundary, so the database itself refuses the stale writer. The filter is
the grant's full identity (resource, exact token, owning transaction), it runs after
the mutation's writes and immediately before commit, and **every** grant is checked,
since an operation holding X on several resources loses the whole mutation if it lost
any one of them.

Grants reach the storage transaction on the operation-scoped context that already
carries the MongoDB session - no globals, no goroutine identity, consistent with the
Phase 3B design.

### RPC surface

`Acquire`, `EndTransaction`, `RenewLease`, `Health`. Still no `BeginTransaction`, and
still no per-resource `Release`.

`Acquire` is retry-safe: an identical request for a resource the transaction already
holds returns the existing grant and the **same** token, without calling the
LockManager and without allocating a second token, so a retry after a lost response
recovers the original outcome. A different mode on a held resource is still
`FailedPrecondition` - no upgrades, no reentrancy. `RenewLease` and `EndTransaction`
are idempotent.

### Evidence

| Test | Shows |
| --- | --- |
| Killed worker | A is SIGKILLed holding X with no `EndTransaction`; B is blocked while A lives, then proceeds. A's write is absent, B's present, version advanced once, no permanent leak |
| Stale writer | A stalls, loses its lease, B commits with a newer token; A wakes and its commit is refused by fence validation specifically |
| **Negative control** | The identical schedule with validation bypassed **must** produce the bad commit - and does: the stale writer clobbered committed state, leaving `base-STALE` with the fresh append gone |
| Lease survives a long operation | A worker that keeps renewing is **not** revoked across several lease lengths - revocation follows the missing renewal, not elapsed time |
| Fence-store outage | Invalidation fails, locks stay held, `Health` reports it; on recovery, cleanup succeeds and the waiter proceeds |
| Retry / idempotency | The same `Acquire` returns the same token; `RenewLease` and `EndTransaction` are idempotent; shared locks allocate no token; a graceful end invalidates before handing over |

### Liveness

`RunAtomic` applies one explicit transaction-level deadline (`SAFER_MONGO_TXN_TIMEOUT`,
default 30s) when the caller supplied none. Phase 3B's decision to add no
*per-statement* timeouts inside a transaction stands - expiring one statement aborts
the whole transaction, so ordinary contention would look like failure - but an
unbounded transaction could hold a lease alive indefinitely, and now cannot.

## Cloud-native deployment (Phase 4)

Phases 1-3C built the correctness machinery: durable storage, remote strict-2PL
coordination, atomic multi-object mutations, and worker-crash recovery with fencing.
Phase 4 turns that into something actually deployable -- Docker images and a
Kubernetes topology -- without adding any new distributed algorithm. Nothing here
changes SAFER's cryptography, storage, or locking semantics; everything is an
additive adapter around the client package Phases 1-3C already built.

### The coordinator fails closed

Before Phase 4, `cmd/coordinator` started with no fencing store logged a warning and
served remote X-lock coordination anyway -- functional, but with no stale-writer
protection at all. That is no longer the default: `cmd/coordinator` now refuses to
start unless it has a durable, reachable fence store. `-allow-unfenced` is the
explicit, loudly-logged escape hatch for local development and tests that only
exercise lock semantics; it is never appropriate in production. Library-level
constructors (`grpccoord.NewServer` / `NewServerWithConfig`) are unchanged and still
accept a nil `FenceStore`, since some of their own tests need exactly that.

### A real worker service

`cmd/saferworker` was always a test helper: the crossprocess harness spawns one
process per operation and reads a single JSON result line off stdout. It was never
meant to be what a deployment actually runs. `cmd/worker` is: a long-running process
that installs a MongoDB backend and a remote coordinator into the client package once
at startup and serves `worker.v1.SaferWorker` (`InitUser`, `StoreFile`,
`AppendToFile`, `LoadFile` -- the minimum needed to drive a realistic load test, not a
mechanical export of every SAFER method) over gRPC indefinitely.

It holds no per-user session state between calls: every RPC carries a username and
password, and SAFER derives that user's keys fresh from them each time
(`client.GetUser`), exactly as it always has. That is what makes the worker safely
replicable -- any replica can serve any request, and a replica can be added, removed,
or restarted without losing anything a client needs. Startup fails closed exactly
like the coordinator: no MongoDB or no coordinator configured means the process does
not start in some degraded mode, it exits.

Readiness and liveness are deliberately different checks, both served over the
standard `grpc.health.v1` health-checking protocol so Kubernetes' native gRPC probes
can target them directly:

- **Readiness** (the protocol's default, empty-string service name) tracks whether
  MongoDB and the coordinator are currently reachable. An unready worker is exactly
  what should happen when a downstream dependency is having a bad day -- Kubernetes
  stops routing new requests to that replica.
- **Liveness** (service name `"liveness"`) is set once at startup and never touched
  again. It answers "is this process's serve loop alive", not "are my dependencies
  healthy". If it mirrored readiness, a shared MongoDB or coordinator outage would
  fail every replica's liveness probe simultaneously, and Kubernetes would kill and
  restart the entire worker fleet at once -- which cannot fix an outage in a
  dependency and can only add a thundering restart on top of it.

On SIGTERM the worker marks itself unready first (so Kubernetes stops sending new
traffic immediately, before anything else happens), then `GracefulStop`s its gRPC
server so in-flight requests finish, with a bounded forced-stop fallback
(`-shutdown-timeout`, default 25s) so a wedged request cannot hang shutdown forever.
This is an operational optimization layered on top of Phase 3C, not a second
correctness mechanism: a worker that is SIGKILLed with no graceful shutdown at all is
exactly the "killed worker" scenario Phase 3C's leases and fencing already handle.

### Containerization

Three images (`docker/worker`, `docker/coordinator`, `docker/loadgen`), each a
`golang:1.20-bookworm` build stage producing a static (`CGO_ENABLED=0`) binary copied
into a `gcr.io/distroless/static-debian12:nonroot` runtime stage -- no shell, no
package manager, non-root by construction. None needs `protoc`: the generated
protobuf sources under `proto/` are checked in. See `docker/README.md`.

### Kubernetes topology

```
                          dns:///, round_robin
                          (gRPC client-side balancing --
                           see "Load generator" below)
                                    |
                                    v
                     +----------------------------+
  clients / loadgen  | worker HEADLESS Service     | ---> 3x worker pod
                ---->| (DNS returns every pod IP)  |        |  |  |
                     +----------------------------+         v  v  v
                                                     +--------------------+
                                                     | coordinator Service|
                                                     +--------------------+
                                                                |
                                                     +--------------------+
                                                     | coordinator pod (x1)|
                                                     +--------------------+
                                                                |
                                                     shared MongoDB (external;
                                                     not deployed by this repo)
```

The plain `safer-worker` ClusterIP Service (`worker-service.yaml`) still
exists for anything that just wants a stable virtual IP -- but it resolves
to one IP, and kube-proxy pins a client's connection to one backend pod
for that connection's whole life, so it is not what a client wanting
cross-replica balancing should use. See "Load generator" below and
`deploy/kubernetes/worker-service.yaml`'s own header comment.

Workers: `replicas: 3`, ordinary `RollingUpdate` (the Kubernetes default) is fine,
because correctness under concurrent replicas comes entirely from the shared
coordinator's strict 2PL and fencing, not from anything workers coordinate among
themselves.

Coordinator: `replicas: 1`, and -- unlike the default -- `strategy: Recreate`,
**never** `RollingUpdate`. Two coordinator pods running at once would each hold an
independent in-memory lock table, silently splitting coordination between workers
with no error to signal it. `Recreate` tears the old pod down before the new one
starts, so every coordinator rollout has a brief real coordination outage. That is
the deliberate trade: deployment availability for coordination safety.

**Precisely what `Recreate` does and does not earn, stated plainly so it is not
overread:** it earns exactly one property -- that no two coordinator lock tables ever
run at once, which is what a rolling update could not guarantee. It earns nothing
else. It does **not** make a coordinator restart fault-tolerant, and it does **not**
preserve in-flight lock state across the restart -- the old pod's in-memory lock
table is gone the moment it terminates, same as an unplanned crash, whether that
termination came from `kubectl rollout` or from something killing the pod
unexpectedly. A coordinator restart, planned or not, remains the same explicit,
unsupported failure boundary Phase 3A through 3C already documented: this is not high
availability, and nothing added in Phase 4 -- no leader election, no consensus, no
Raft -- changes that. `Recreate` is a rollout-safety property, not a substitute for
the coordinator fault tolerance this repository deliberately does not build.

Configuration: a `ConfigMap` for non-sensitive settings (coordinator address,
database name, timeout settings) and a `Secret` for MongoDB credentials. The
checked-in `secret.yaml` is a local/dev placeholder, deliberately excluded from
`kustomization.yaml`; `deploy/kubernetes/README.md` documents creating the real one
out-of-band. CPU/memory requests and limits are explicit but conservative
development defaults, not derived from any benchmark -- `cmd/loadgen` is
functional/correctness tooling at this phase, not a sizing tool.

Network isolation: a `default-deny-ingress` NetworkPolicy plus two scoped allows, so
the coordinator is reachable only from worker pods, and workers only from pods
carrying an opt-in `safer-client: "true"` label (`loadgen-job.yaml` carries it) --
not from the whole namespace, which an earlier version of this policy allowed. That
distinction matters because the two channels are not equally risky: the coordinator's
channel (unchanged from Phase 3A -- see "Cross-process coordination" above) carries
only resource identifiers and lock modes, since SAFER encrypts and authenticates its
objects before anything reaches storage or the lock layer. The worker's channel
(`proto/worker/v1/worker.proto`) is a NEW, higher-risk boundary Phase 4 introduced: it
sits *before* that encryption layer, so every request carries the caller's plaintext
username and password, and StoreFile/AppendToFile carry plaintext file content, over
a transport that -- like the coordinator's -- is neither encrypted nor authenticated
at the application layer. **The worker gRPC surface is not suitable for exposure to
an untrusted network.** NetworkPolicy restricts *which pods* can reach it; it is not
cryptographic authentication of what an already-admitted pod sends, and building mTLS
or any other service-authentication layer remains out of scope for this phase. An
optional, unapplied NetworkPolicy template covers the case where MongoDB itself runs
as an in-cluster pod; this repository does not deploy MongoDB at all (it is a
managed/external dependency, the same way CI runs it from the upstream `mongo:7`
image rather than a custom build).

### Load generator

`cmd/loadgen` talks to worker addresses over gRPC -- never the client package
directly. Four workloads: independent-file writes, same-file writes (the one
that actually exercises cross-replica strict 2PL, since concurrent callers
may land on different worker pods and correctness depends entirely on the
shared coordinator, not on anything workers share in-process), reads, and a
mixed interleaving. It is functional/correctness tooling for this phase,
deliberately not a tuned throughput/latency benchmark -- that comes later,
once observability exists.

**Balancing across replicas is done by the gRPC client, not by Kubernetes.**
An earlier version of this tool, and this document, asserted that pointing
loadgen at the worker Service exercised all three replicas. That was never
actually checked, and it does not hold: a gRPC client that dials a
ClusterIP Service once gets one HTTP/2 connection that kube-proxy has
pinned to whichever pod it first reached, and every RPC multiplexed over
that one connection lands on that same pod for as long as the connection
lives -- no matter how many replicas exist behind the Service. The fix
(`cmd/loadgen/dial.go`) is gRPC's own `round_robin` load-balancing policy
against a target that actually resolves to more than one address: a
HEADLESS Service (`clusterIP: None`, `worker-service-headless.yaml`)
makes DNS return every ready pod's IP directly instead of hiding them
behind one virtual IP, gRPC's built-in DNS resolver (registered
automatically, no extra import needed) fetches that address list, and
`round_robin` picks a different one per RPC. `deploy/kubernetes/loadgen-job.yaml`
targets `dns:///safer-worker-headless:50052` accordingly. A comma-separated
list of literal addresses is also accepted, using gRPC's manual resolver
instead of DNS, for local testing against processes with no DNS name
unifying them (see `integration/workerservice`).

The claim that requests actually reached more than one replica is no
longer asserted without evidence: every worker attaches a response-header
instance identifier to each RPC it serves (`internal/workerdiag`,
`cmd/worker/instance.go`, deliberately kept out of `worker.v1`'s protobuf
messages, since which pod handled a request is deployment metadata, not
part of SAFER's business API), and `cmd/loadgen`'s report includes a
`replicas_served=N` line built from it. `integration/workerservice` fails
the build if a run against three real worker processes reports fewer than
two.

**A clean RPC error count is not a correctness claim, and the report no
longer implies it is one.** The report now separates `attempted`,
`succeeded`, and `failed` RPC outcomes -- a run that attempted 1000
operations and failed 400 has succeeded at 600, not 1000, and throughput
is computed from `succeeded` alone. More importantly, `succeeded` by
itself never proved the data was actually right: an RPC can report success
while a concurrent write silently clobbers it. `checkOracles`
(`cmd/loadgen/workload.go`) is the actual data check, run once after the
timed portion of a write workload: every user's file must, at the end, be
exactly its initial content plus one `-content-size`-sized chunk per
append this run recorded as successful for that user -- a lost or
duplicated write changes that length and is caught. `WorkloadReads`
verifies inline, on every single read, that the bytes returned are the
only content the file has ever legitimately held. Either kind of failure
prints under a `DATA CORRUPTION` line and exits the process non-zero,
distinct from `failed`, which counts RPC errors, not data that disagrees
with what the RPCs said happened.

### Evidence

| Check | How |
| --- | --- |
| Coordinator fails closed | `cmd/coordinator`'s `openFenceStore` unit-tested directly (no MongoDB configured, and MongoDB configured but unreachable); also verified against the actual built Docker image, which exits non-zero with no `SAFER_MONGO_URI` (now a CI job: `.github/workflows/ci.yml`'s `docker`) |
| Worker refuses bad/missing config | `cmd/worker`'s `parseConfig` unit-tested: missing MongoDB, missing coordinator, missing both, non-positive timeouts |
| Multiple worker replicas serve one deployment correctly | `integration/workerservice`: real worker and coordinator processes, `InitUser` on one replica, `StoreFile`/`AppendToFile`/`LoadFile` on others, for two independent users, against one MongoDB |
| Readiness/liveness semantics | `integration/workerservice`: a live worker reports `SERVING` on both the default (readiness) and `"liveness"` gRPC health services |
| Load generator drives worker replicas, balancing is real, and the data is actually checked | `integration/workerservice`: all four workload types run against three real worker processes with zero failed RPCs and no correctness-oracle failure, asserting `replicas_served >= 2` parsed from loadgen's own report; the `dns:///` + `round_robin` mechanism itself verified separately against three Docker containers sharing one DNS name -- `replicas_served=3`, a near-even 31/32/33 split, `failed=0` and no `DATA CORRUPTION` line (see `docker/README.md`) |
| Docker images build and interoperate | All three images built and smoke-tested together on a Docker network against a real MongoDB replica set (`docker/README.md`); now also built (not run) on every CI push (`.github/workflows/ci.yml`'s `docker` job) |
| Kubernetes manifests are internally consistent | `deploy/kubernetes/manifest_test.go`: `kubectl kustomize` builds the full manifest set and the files intentionally excluded from it, catching a broken cross-reference (e.g. a Service selector that stops matching a Deployment's labels) |
| Existing Phase 1-3C correctness | Full suite (`go test ./...`, with and without `SAFER_MONGO_URI`) re-run clean after every Phase 4 change, including `integration/crossprocess` and `integration/rollback_test.go` |

**Live Kubernetes evidence exists as of Phase 4.6** (see that section below) via a
disposable `kind` cluster. Everything above this point in Phase 4's own section was
still statically validated only at the time it was written -- no cluster was
available yet -- and that limitation is what the first live run in Phase 4.6
immediately exposed: the manifests as originally sized OOM-killed every worker
replica under real load. See `deploy/kubernetes/README.md` for the day-to-day
boundary between what is exercised statically versus live.

## Cloud-native deployment hardening (Phase 4.6)

Phase 4.5's first live `kind` run -- the first time any of Phase 4's manifests had
actually touched a running Kubernetes API server -- immediately found a real problem
static validation could not have caught: all three worker replicas were OOM-killed
(`exitCode: 137`, `reason: OOMKilled`) within about 45 seconds of the checked-in
`loadgen-job.yaml` starting against them at its own default settings (`workload=mixed`,
`concurrency=8`, `count=200`). This section is that incident's investigation and fix,
not something to gloss over: a real deployment problem, found by actually deploying,
is exactly what static manifest validation cannot substitute for.

### Root cause: concurrent Argon2 key derivation, not sequential setup

The first hypothesis -- that `cmd/loadgen`'s `setup()` phase issues 8 concurrent
`InitUser` calls -- was wrong and was corrected before any code changed.
`cmd/loadgen/workload.go`'s `setup()` is a single sequential loop; it does not spawn
goroutines. The actual hot path is every ordinary `StoreFile`/`AppendToFile`/`LoadFile`
RPC re-authenticating its caller through `client.GetUserContext` ->
`deriveAccountKey` -> `userlib.Argon2Key`, which is *by design*, stateless, and exactly
what makes any worker replica able to serve any request (see cmd/worker's package
doc). `userlib.Argon2Key` calls `golang.org/x/crypto/argon2.IDKey` with a memory
parameter of `64*1024` KiB (64 MiB) per call -- a fixed, deliberate cost of the
memory-hard KDF the SAFER-CC crypto layer already used, not a bug and not something
this phase weakens. `InitUser` separately performs RSA/DS key generation, its own
expensive crypto -- also bounded by the same admission control introduced below,
since it is comparably costly, but it is **not** what this incident's measured
concurrent load exercised: the measurements in this section drove
`StoreFile`/`AppendToFile`/`LoadFile` traffic (`workload=mixed`), which authenticates
through `GetUserContext`/Argon2, not through `InitUser`.

Measuring a real worker process under `crictl stats` at controlled `-concurrency`
levels (1/2/4/8, same `workload=mixed`/`count=200` shape as the checked-in Job)
against the same three live pods found:

| Concurrency | Worker restarted? | Peak observed memory | Result |
| --- | --- | --- | --- |
| 1 | No | ~148-163 MB | `attempted=200 succeeded=200 failed=0` |
| 2 | No (this run) | up to ~217 MB on one replica | `attempted=200 succeeded=200 failed=0` |
| 4 | Yes -- OOMKilled | ~268 MB observed before the kill | `succeeded=24 failed=176`, mixed `DATA CORRUPTION`/RPC-error output (see below) |
| 8 | Yes -- OOMKilled, repeatedly | not captured (killed before the ~300ms poller could sample it) | loadgen could not even complete its `dns:///` dial -- every worker was crash-looping |

Peak memory at low concurrency is a *sampling* high-water mark (polling every ~300ms
via `crictl stats`, not a continuous trace), stated as a limitation rather than an
exact figure. Repeating the concurrency-1 run three times back to back found idle
memory settling at a stable ~150-165 MB plateau each time -- not climbing further run
over run -- which is Go's allocator retaining committed heap pages rather than
returning them to the OS quickly (normal Go runtime behavior once a large allocation
like Argon2's has happened), not a memory leak in this repository's code. No leak
evidence was found, and none is claimed.

Answering the three questions this investigation was scoped to:

1. **Does worker memory increase materially with concurrent request/auth load?** Yes,
   substantially -- from a ~150 MB single-caller plateau toward and past 256 MB as
   concurrent overlap increases.
2. **At what concurrency does 256Mi fail?** Between 2 and 4: 2 succeeded in the
   measured run but left little headroom (~217 MB observed against a 256 MB limit);
   4 reliably failed.
3. **Is the pressure consistent with concurrent Argon2/GetUser work rather than a
   monotonic leak?** Yes -- confirmed by the repeated-run plateau test above.

### Bounded admission control, not just a bigger memory limit

Raising the memory limit alone would not have fixed anything: an unbounded number of
concurrent `GetUserContext`/`InitUserContext` calls in one process has no upper bound
on memory at all, so no static limit is actually safe against it, only less likely to
be hit soon. `cmd/worker/authlimit.go` adds `authLimiter`, a small context-aware
semaphore bounding how many of those two calls may run at once in one worker process
(`-auth-concurrency`, default `DefaultAuthConcurrency = 2`, chosen directly from the
measurements above: it permits real concurrency while keeping a worker's expected
peak -- the observed ~217 MB total for 2 overlapping `GetUserContext` calls, not an
OOM in the measured run -- inside the revised memory limit). It wraps only the
expensive section -- lock acquisition and the MongoDB transaction that follow
authentication are not gated -- and changes no SAFER semantics: no server-side
session, no cached password or derived key, no weakened Argon2 parameters, and
`GetUserContext`'s authentication itself is never bypassed. `InitUser` uses the
identical limiter, since its key generation is the same class of cost, even though
it was not what this incident's own measured load exercised (see above).
`cmd/worker/authlimit_test.go` and `cmd/worker/authenticate`'s tests prove,
deterministically: at most N sections run concurrently; a waiter whose context is
cancelled while queued for a slot returns promptly without taking one; a failed
authentication still releases its slot (checked with a limiter of size 1, where a
leaked permit would deadlock the very next call); and ordinary requests are
unaffected.

### Kubernetes memory limit, re-derived from evidence

`deploy/kubernetes/worker-deployment.yaml`'s worker resources changed from
`request: 64Mi` / `limit: 256Mi` to `request: 192Mi` / `limit: 512Mi`, sized against
the measurements above with `-auth-concurrency=2` capping the worst case: the request
covers the ~150-165 MB steady-state plateau with margin, and the limit comfortably
covers the ~217 MB two-concurrent peak plus headroom for the sampling limitation
already noted. These remain development/load-test defaults, now evidence-informed
rather than an unverified guess -- **not** production capacity planning, which would
need a longer, more thorough load-test regime this phase did not run. The checked-in
Job's own default concurrency (8) was deliberately left unchanged rather than lowered
to make the symptom disappear: the fix is admission control and correct sizing, not
hiding the workload that found the bug.

### Oracle error classification

`cmd/loadgen`'s `checkOracles` used to report a verification `LoadFile` call that
failed outright (an unavailable worker, e.g. mid-OOM-kill) under the same
`"DATA CORRUPTION"` label as an actual length mismatch -- overstating what was
observed, since a worker that cannot be reached proves nothing about whether its data
is actually wrong. `Report` now separates `OracleFailures` (the verification call
succeeded and the bytes are provably wrong -- real corruption or a lost/duplicated
write) from `VerificationErrors` (the check itself could not run), printed under
distinct `DATA CORRUPTION` and `VERIFICATION ERROR` labels respectively. Both still
exit non-zero -- an unverifiable run is not one this tool can vouch for -- but only
the former is evidence of corrupted or lost data. `cmd/loadgen/workload_test.go`
covers both classifications with a fake worker client that can fail `LoadFile`
outright, independent of any data mismatch.

### Live `kind` result, after the fix

Rebuilt images, loaded into the same disposable cluster, manifests re-applied,
bounded `kubectl rollout status` waits throughout (never an unbounded wait):

- Coordinator: `1/1` Ready. Workers: `3/3` Ready, `restartCount: 0` for all three
  before, during, and after every subsequent run in this section -- no OOM at the
  revised limits under the unmodified, checked-in `loadgen-job.yaml`.
- The headless Service's `dns:///` target resolved all three ready worker endpoints;
  `mixed` (the checked-in Job as-is) reported
  `attempted=200 succeeded=200 failed=0`, `replicas_served=3` with a near-even
  71/72/73 split, no `DATA CORRUPTION`, no `VERIFICATION ERROR`.
- `same-file-writes` (`-concurrency=8 -count=200`) reported
  `attempted=200 succeeded=200 failed=0`, `replicas_served=3` (67/67/68), its
  lost-write oracle clean -- real cross-replica strict 2PL correctness under load,
  against real pods.
- **Worker failure exercise**: one worker pod deleted directly (`kubectl delete pod`)
  while the Deployment was otherwise healthy. Kubernetes scheduled a replacement
  pod immediately; the Deployment returned to `3/3` Ready within the same bounded
  wait used throughout. A fresh `mixed` run afterward reported
  `attempted=200 succeeded=200 failed=0` with `replicas_served=3` including the new
  replacement pod actively serving traffic (72/72/72) -- this is worker
  replaceability under a normal Kubernetes Deployment, **not** coordinator fault
  tolerance, which remains explicitly out of scope (see "No coordinator fault
  tolerance" below).
- **NetworkPolicy enforcement was actually verified, not just assumed**: kindnet in
  this cluster does enforce `NetworkPolicy`. A pod without the `safer-client: "true"`
  label timed out reaching the worker Service (traffic silently dropped); an
  otherwise-identical pod carrying that label connected immediately (a gRPC-protocol
  reset from a plain HTTP client, not a network-level block). This is the intended
  allow/deny boundary actually holding on a live cluster, not a static reading of the
  YAML.

This was run once, on one disposable `kind` cluster, on one host. It is real evidence
that the fix works and that the deployment is functionally correct under a worker
failure; it is not a substitute for a longer soak test, a real multi-node cluster, or
production capacity planning, none of which this phase attempted.

## OpenTelemetry -> Datadog observability (Phase 5)

Deliberately narrow: `worker`/`coordinator`/`loadgen` export OpenTelemetry traces and
metrics over OTLP/gRPC to the existing Datadog Agent's OTLP receiver. No OTel
Collector, no Prometheus/Grafana/Jaeger, no service mesh, no new microservice. The
application depends only on `go.opentelemetry.io/otel` and its SDK/exporter packages
(`sdk`, `sdk/metric`, `exporters/otlp/otlptrace/otlptracegrpc`,
`exporters/otlp/otlpmetric/otlpmetricgrpc`, `contrib.../otelgrpc` @ v1.24.0/v0.49.0,
chosen to keep `go.mod`'s existing `go 1.20` directive) -- never a Datadog-specific
tracing library.

### Bootstrap: optional by construction

`internal/telemetry.Setup` is the one place any of the three binaries touch OTel setup.
It reads the standard `OTEL_EXPORTER_OTLP_ENDPOINT`/`_TRACES_ENDPOINT`/
`_METRICS_ENDPOINT` variables itself (rather than letting the exporters silently fall
back to the spec-default `localhost:4317`), builds a `Resource` via
`resource.WithFromEnv()` plus `service.name`/`service.instance.id`/`service.version`,
and installs a `TracerProvider`/`MeterProvider` with a bounded (5s) shutdown. If no
endpoint is configured, `Setup` does nothing and returns a no-op shutdown -- OTel's own
global no-op Tracer/Meter stays installed, so every instrumentation call site below is
unconditionally safe whether or not telemetry is turned on anywhere. `internal/telemetry_test.go`
proves both paths: disabled-by-default with no endpoint, and a real
`*sdktrace.TracerProvider` installed (with bounded shutdown against an unreachable
backend) when one is set.

### Tracing on real gRPC boundaries

Every gRPC boundary in the system carries trace context automatically via
`otelgrpc.NewServerHandler()`/`NewClientHandler()` (the modern `stats.Handler` API):
worker server, coordinator server, worker-to-coordinator client
(`client/coordination/grpccoord`), and loadgen-to-worker client (`cmd/loadgen/dial.go`).
This is what makes a trace follow one connected shape rather than several disconnected
ones. For `StoreFile`/`AppendToFile`/`LoadFile`, that shape is (indentation = child of
the line above; siblings share a parent and do not nest under each other):

```
loadgen client RPC
  -> worker server RPC
       -> worker.authenticate
       -> safer.<Operation>
            -> worker -> coordinator gRPC
                 -> coordinator server Acquire
                      -> coordinator.lock_wait
            -> mongostore.run_atomic
```

`worker.authenticate` and `safer.<Operation>` are **siblings** under the worker RPC
span, not nested inside each other -- authentication (`cmd/worker/service.go`'s
`authenticate` helper) runs to completion, ending its own span, before
`safer.<Operation>` starts its own from the same parent. `InitUser` is the one
exception: `cmd/worker/authlimit.go`'s admission limiter still gates it (the same
`authLimiter.acquire`/`release` pair), but `InitUser`'s handler calls that directly
rather than through the `authenticate` helper, so it does **not** currently produce a
`worker.authenticate` span -- only its own `safer.InitUser` span. `mongostore.run_atomic`
and the worker-to-coordinator `Acquire` call are both reached from inside
`safer.<Operation>`, not from `worker.authenticate`, and are siblings of each other, not
nested inside one another.

- Worker (`cmd/worker/service.go`, `cmd/worker/authlimit.go`): `worker.authenticate`
  wraps the auth-admission wait plus `GetUserContext` (StoreFile/AppendToFile/LoadFile
  only -- see above); a `safer.<Operation>` span (`safer.InitUser`, `safer.StoreFile`,
  `safer.AppendToFile`, `safer.LoadFile`) wraps each handler's actual SAFER call.
- Coordinator (`client/coordination/grpccoord/server.go`): `coordinator.lock_wait` wraps
  the real `AcquireContext` wait (not the idempotent-retry fast path) with `lock.mode`/
  `resource.type` attributes; `coordinator.teardown` wraps lease/lock release with a
  `grants_count` attribute.
- Storage (`client/storage/mongostore/atomic.go`): `mongostore.run_atomic` wraps the
  whole atomic transaction boundary, recording a `mongostore.outcome`
  (`committed`/`error`) attribute and the error itself on failure.

No span anywhere carries a password, plaintext file content, a derived key, Mongo
credentials, or a high-cardinality identifier (raw username, filename, FileID, or
Namespace key) -- `resourceTypeLabel` maps lock resources down to `"namespace"`/
`"file"`/`"unknown"` before it ever becomes a span attribute, matching the same
trust-boundary reasoning Phase 4.5 applied to the worker gRPC channel.

### Metrics

| Metric | Kind | Attributes | Question it answers |
|---|---|---|---|
| `safer.coordinator.lock.acquire.count` | counter | `lock.mode`, `resource.type`, `outcome` | lock contention volume |
| `safer.coordinator.lock.wait.duration` | histogram (s) | `lock.mode`, `resource.type`, `outcome` | how long callers actually wait for a lock |
| `safer.worker.auth_admission.wait.duration` | histogram (s) | `outcome` | authLimiter queueing delay |
| `safer.worker.auth_admission.in_flight` | up-down counter | -- | live auth-admission saturation |
| `safer.coordinator.lease.revocation.count` | counter | -- | stale-lease revocations |
| `safer.coordinator.fence.cleanup.retry.count` | counter | -- | fence cleanup retry pressure |
| `safer.coordinator.fence.cleanup.failure.count` | counter | -- | fence cleanup giving up |
| `safer.storage.transaction.duration` | histogram (s) | `outcome` | Mongo atomic transaction time |
| `safer.storage.fence.rejection.count` | counter | -- | stale-fence rejections inside storage |

`outcome` values are deliberately small, fixed enums (`granted`/`caller_cancelled`/
`revoked`/`failed`, `acquired`/`cancelled`, `committed`/`error`), never a raw error
string or identifier.

### Logging

No custom logging framework was added; each binary's existing `log.Printf`-based
output is left exactly as it was before Phase 5, going to the container's ordinary
stdout/stderr where `kubectl logs` already reads it. This section originally claimed
that output was "picked up by the Datadog Agent's standard container log collection";
that was wrong and has been corrected here. The DatadogAgent CR this deployment
actually uses does **not** enable `spec.features.logCollection` -- only infrastructure
telemetry and (as of this phase) OTLP trace/metric ingestion are on. Enabling
cluster-wide log collection is a real scope expansion of what the Agent does (a new
`features` toggle on a CR this repository does not own the lifecycle of, plus whatever
volume/cost implications follow for every pod in the cluster, not just SAFER's three),
and Phase 5 was never asked to make that change, so it was not made. Datadog log
ingestion for this repository's binaries was **not** enabled and **not** verified in
this phase; trace/span-ID log correlation was, separately, also **not** implemented --
doing that properly means routing every log line through a context-aware logger, which
none of the three binaries have today, and retrofitting one was judged out of this
phase's narrow scope regardless of where the logs end up.

### Datadog Agent: inspected, not reinstalled

The already-running Datadog Operator/Cluster Agent/node Agent in the `datadog`
namespace of the `safer-test` kind cluster (site `us3.datadoghq.com`, `env:dev`) was
never reinstalled. `kubectl -n datadog get datadogagent datadog -o yaml` showed no
`otlp:` block under `spec.features`, confirming OTLP ingestion was genuinely not yet
enabled despite infrastructure telemetry already working -- the CR was not assumed
either way. The only change made was a `kubectl patch --type merge` adding
`spec.features.otlp.receiver.protocols.grpc.enabled: true`; every other field (the
`datadog-secret` API-key reference, `clusterName`, `site`, `env:dev` tag,
`kubelet.tlsVerify: false`, APM instrumentation, cluster checks) was confirmed
byte-for-byte unchanged by diffing the full CR before and after. The Agent rolled and
returned to `Running (1/1/1)`. See `deploy/kubernetes/telemetry/README.md` for the
exact commands. `deploy/kubernetes/telemetry/otel-config.yaml` is a separate,
optionally-referenced ConfigMap pointing `OTEL_EXPORTER_OTLP_ENDPOINT` at the Agent's
in-cluster Service (`datadog-agent.datadog.svc.cluster.local:4317`) -- deliberately
kept out of SAFER's own `configmap.yaml`, the same separation-of-concerns choice
already made for MongoDB.

### Evidence gathered locally

Both workloads run against the same live `kind` cluster, worker/coordinator images
rebuilt with the Phase 5 instrumentation, `safer-otel-config` applied, Deployments
restarted:

- `mixed`: `attempted=200 succeeded=200 failed=0`, `replicas_served=3` (72/71/73).
- `same-file-writes` (`-concurrency=8 -count=200`): `attempted=200 succeeded=200
  failed=0`, `replicas_served=3` (67/68/67).
- Zero worker restarts across both runs (`kubectl get pods -l app=safer-worker` showed
  `RESTARTS: 0` throughout); no `DATA CORRUPTION` or `VERIFICATION ERROR` from either
  run's oracle checks; no export errors or panics in worker/coordinator pod logs.
- `agent status` inside the Agent pod shows `OTLP: Status: Enabled, Collector status:
  Running`, and the `datadog-agent` Service exposes `4317/TCP` alongside its existing
  `8126/TCP,8125/UDP`.
- The trace-agent's own per-minute stats (`agent status`'s "APM Agent" section) show
  direct, repeated correlation with the exact traffic generated: the priority-sampling
  rate for `service:safer-loadgen,env:dev` (a stat the trace-agent computes only from
  traces it has actually received and processed for that service) changed value after
  each fresh run in this session (69.4% -> 89.3% -> 80.6%), and the "Writer (previous
  minute)" stats-payload counters were non-zero immediately after a run. This is local,
  Agent-side evidence that OTLP-derived trace data tagged with this repository's exact
  service names is reaching and being processed by the Agent. It is not the same as
  visually confirming traces and metrics in the Datadog UI -- that verification is
  manual and belongs to whoever has access to it, not something this repository can
  fabricate. See the corresponding session handoff for the exact service names, span
  names, and metric names to check. Datadog log ingestion was not enabled in this
  phase (see "Logging" above), so there is nothing to check there yet.

### What Phase 5 does not attempt

Datadog log ingestion was not enabled or verified (see "Logging" above); no log/trace
correlation, for the same reason. No answer yet to the "same-file-writes latency
breakdown" question Phase 5 set out to use telemetry for: the actual observed
`safer.coordinator.lock.wait.duration`, `safer.worker.auth_admission.wait.duration`,
and `safer.storage.transaction.duration` values only exist in Datadog once ingested,
and reading them back needs Datadog UI or API access this repository's automation does
not have and must not acquire on its own (the plaintext Datadog API key is explicitly
off-limits). That analysis is deferred until those numbers are reported back.

## Claims not yet earned

Recorded here so they are not asserted prematurely:

- **Cross-process strict 2PL plus atomic durable MongoDB mutations.** Earned: strict
  2PL across separate OS processes sharing one coordinator and one database, and
  all-or-nothing multi-object persistence, both verified by real multi-process/
  failure-injection tests with negative controls. This bullet originally added "under
  graceful operation only" and said worker-crash behavior was not earned; Phase 3C
  earned it (see the next bullet), so that qualifier no longer applies here. What is
  still not earned by anything in this repository: coordinator crashes or network
  partitions (see the following bullets).
- **Worker crash recovery, bounded lock reclamation, and stale-writer protection are
  earned** as of Phase 3C, verified with real killed and stalled processes and a
  negative control. "Bounded" means bounded by the lease, plus however long fence
  invalidation takes to succeed - an unreachable fence store extends it indefinitely,
  on purpose.
- **No coordinator fault tolerance.** The single coordinator remains an explicit
  failure domain. Fence state is durable, but lock state is in memory: if the
  coordinator dies, who held what is lost, and workers holding locks are not told.
  Solving that needs replication and consensus, deliberately out of scope.
- **No claim about network partitions.** Nothing has been tested against a partition
  between a worker and the coordinator while storage stays reachable.
- **Durability, plus atomicity of failed mutations — but not MongoDB crash
  consistency.** Verified: data written through one SAFER client and connection pool
  is readable through a separate one afterwards, and a mutation that fails part way
  (an injected storage error, or -- as of Phase 4.5 -- a context deadline; see
  `integration/context_deadline_test.go`) leaves no committed partial state. Not
  verified: a killed **MongoDB** process mid-commit, which is a different scope from
  worker crashes and is not the same as a returned error. This bullet is about the
  storage layer specifically; it does not contradict worker-crash recovery below,
  which Phase 3C does cover.
- **No kill-the-database or kill-the-coordinator testing.** Kill-the-**worker**
  testing, and the lease/fencing machinery that makes it meaningful, are done (Phase
  3C -- see the bullet above and "Worker failure" earlier in this document). An
  earlier version of this bullet said there was no kill-worker testing and no lease/
  fencing machinery at all, ending "That is Phase 3C" as if the phase were still
  pending; it was written before Phase 3C existed and was never updated once it did.
  What remains genuinely untested is narrower: a killed MongoDB process or a killed
  coordinator process specifically (network partitions are the separate bullet
  above).
- The checked-in benchmark tables are inherited historical SAFER-CC measurements, not
  performance claims about this repository.
- **Live Kubernetes verification exists as of Phase 4.6, but is limited in scope.** A
  disposable `kind` cluster ran the actual manifests: coordinator and all 3 workers
  reached Ready, readiness/liveness probes fired for real against a kubelet, a worker
  pod was deleted and Kubernetes replaced it with the Deployment returning to `3/3`
  Ready, and NetworkPolicy enforcement was confirmed directly (an unlabeled pod's
  traffic to the worker Service was silently dropped; a labeled one connected). See
  "Cloud-native deployment hardening (Phase 4.6)" above for the full run. What
  remains unverified: the coordinator's `Recreate` rollout has still not been observed
  in a live rollout (only worker-pod replacement was exercised, which is not
  coordinator fault tolerance); this was one run, on one single-node `kind` cluster,
  on one host, not a multi-node cluster, a longer soak test, or anything resembling
  production capacity validation.
- **No cryptographic service authentication, and the worker channel is genuinely
  higher-risk than the coordinator's.** Neither gRPC transport is authenticated or
  encrypted at the application layer. The coordinator's channel, unchanged from Phase
  3A, carries only resource identifiers and lock modes. The worker's channel
  (`proto/worker/v1/worker.proto`, added in Phase 4.1) is a different and worse
  exposure: it sits before SAFER's encryption layer, so it carries the caller's
  plaintext username and password on every request, and plaintext file content on
  writes. Phase 4.5's NetworkPolicies restrict *which pods* can reach each channel
  (an opt-in label for the worker's, tighter than the coordinator's own-namespace
  scope, reflecting that difference in risk); they do not authenticate or encrypt
  *what* an already-allowed pod sends. The worker gRPC surface is explicitly not
  suitable for exposure to an untrusted network as a result. Building mTLS or another
  service-authentication layer remains out of scope for this phase.
- **No production sizing.** Most CPU/memory requests and limits in `deploy/kubernetes/`
  remain conservative development defaults, not derived from any load test -- this is
  still true of the coordinator's and loadgen's, and of every CPU request/limit in this
  repository. The worker's *memory* request/limit is the one exception: Phase 4.6
  re-derived it directly from the OOM measurements above ("Kubernetes memory limit,
  re-derived from evidence"), so it is evidence-informed rather than an unverified
  guess. Neither status is production capacity planning, which needs a longer,
  multi-node soak-test regime this repository has not run. `cmd/loadgen` validates
  functional correctness under concurrency in this phase, not throughput or capacity.
