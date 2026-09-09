# Distributed final evidence — 2026-09-09

Raw evidence and the written report for the final measurement campaign of
the distributed system (3 workers, gRPC lock coordinator, MongoDB, leases
and fencing, OpenTelemetry → Datadog).

**Measured code SHA: `3fa018e67790e748c9f4e12b0987fd32a618573f`**
**Measurement date: 2026-09-09 (UTC)**

Start with [`final-campaign-report.md`](final-campaign-report.md).

## Not the same evidence as `benchmarks/*.md` / `benchmarks/*.json`

The files directly under `benchmarks/` (`benchmark-report.md`,
`benchmark-results.json`) are **inherited V1 evidence**: they measure
*process-local* concurrency-control strategies (No-CC, Global-lock,
SAFER-CC) through the legacy in-process API over `userlib` in-memory
storage. No gRPC, no coordinator, no MongoDB, no Kubernetes, no leases or
fencing is involved in them.

This directory measures the **distributed system**. The two bodies of
evidence describe different machines doing different work and their numbers
must never be placed in the same table or compared to each other.

## Environment

| Item | Value |
|---|---|
| Cluster | `kind-safer-test`, single node |
| Workers | 3 replicas, `-auth-concurrency=2`, CPU limit `500m`, memory limit `512Mi` |
| Coordinator | 1 replica |
| Storage | MongoDB in-cluster |
| Loadgen target | `dns:///safer-worker-headless:50052` (headless Service + gRPC `round_robin`) |
| Content size | 64 bytes (binary default) |
| Telemetry | OpenTelemetry → OTLP → in-cluster Datadog Agent |

Everything shares one developer host. These are **development-cluster
measurements, not production capacity claims**.

## Experiment design

The primary experiment is a **matched A/B**: `independent-writes` versus
`same-file-writes`. Both execute `AppendToFile` with identical concurrency,
operation count, content size, worker count, auth settings and resource
limits. The single deliberate difference is the sharing shape — a separate
user and file per caller, versus one shared user and file across all callers
(which forces serialization on one exclusive file lock).

- 4 concurrency levels (1, 2, 4, 8) × 2 workloads × 3 repetitions =
  **24 primary runs**, `-count=500` each
- **6 secondary runs**: `mixed` and `reads` at c=8, 3 repetitions each
- **1 discarded warm-up** (`mixed` c=8, count=100) used only as an
  instrumentation gate
- **2 sustained attribution windows** (`-duration=120s`, c=8), each fenced by
  ≥90 s of idle, for clean Datadog metric buckets

Run order was balanced at each concurrency level (rep 1
independent-then-same-file, rep 2 reversed, rep 3 as rep 1) to reduce
temporal bias.

## Layout

```
final-campaign-report.md   The report. All numbers regenerated from raw/.
SHA256SUMS.txt             Manifest of every file in this directory.
raw/
  logs/     34 files  loadgen stdout, one per run (the primary result)
  meta/     40 files  per-run metadata + CPU counter snapshots
  yaml/     34 files  the exact rendered Job manifest applied per run
  scripts/   4 files  the runner and the analysis helpers
```

### What each artifact means

- **`raw/logs/<run>.log`** — loadgen's own output. Contains the
  `workload= attempted= succeeded= failed= elapsed= throughput=` line, the
  `latency_samples= p50= p95= p99=` line, and `replicas_served=` with the
  per-replica RPC counts. The percentiles are computed client-side from
  **unsampled** per-operation samples covering only successful operations,
  so `latency_samples == succeeded` on a clean run. Any `DATA CORRUPTION` or
  `VERIFICATION ERROR` line would appear here; none does.
- **`raw/meta/<run>.meta`** — run name, workload, concurrency, mode/value,
  UTC start and end with millisecond precision, Job status, and worker pod
  restart counts captured before and after the run.
- **`raw/meta/cpu-{before,after}-win{A,B}.txt`** — cgroup v2 `cpu.stat`
  snapshots per worker pod (`nr_periods`, `nr_throttled`, `throttled_usec`,
  `usage_usec`) taken immediately around each sustained attribution window.
  Deltas between the before/after pairs produce the throttling table in the
  report.
- **`raw/meta/cpu-{before,after}-windowA.txt`** — snapshots around the
  *excluded* first attempt at window A (see below). Retained for honesty:
  they show no load occurred.
- **`raw/yaml/<run>.yaml`** — the exact Job manifest applied. Rendered from
  the checked-in `deploy/kubernetes/loadgen-job.yaml` with only
  `metadata.name`, `-workload=`, `-concurrency=` and `-count=`/`-duration=`
  substituted; the image, `safer-client` label, telemetry `envFrom`,
  headless-Service target, resources, namespace and restart policy are
  inherited unchanged. `-content-size` is never passed, so the 64-byte
  binary default applies.
- **`raw/scripts/run_one.sh`** — the runner: renders the manifest, records
  UTC start, applies the Job, polls for completion, captures logs, records
  UTC end and restart deltas, then deletes the Job. It refuses to overwrite
  an existing log, so raw evidence cannot be silently replaced.
- **`raw/scripts/cpu_stat.sh`** — read-only cgroup v2 capture. The kubelet
  `/metrics/cadvisor` and `/metrics/resource` proxy endpoints returned
  `NotFound` on this cluster, so counters were read directly from the kind
  node container's `/sys/fs/cgroup`. It modifies nothing and uses no
  Datadog credentials.
- **`raw/scripts/aggregate.py`, `raw/scripts/cpu_delta.py`** — the analysis
  used to produce the report's tables from `raw/logs/` and `raw/meta/`.

### One excluded run, deliberately retained

`attrib-A-ind-c8-120s` was a **harness failure, not a SAFER result**: the Job
name contained an uppercase letter, which Kubernetes rejects as an invalid
RFC 1123 name, so the Job was never created and no workload ran. Its log,
meta and CPU snapshots are kept (`attrib-A-…`, `cpu-*-windowA.txt`) because
the surrounding CPU counters are themselves the evidence that no load
occurred. The window was re-run as `attrib-win-a-ind-c8-120s`. No valid run
was discarded or repeated for being slow.

## Reproducing the report's numbers

The tables in the report are derived, not hand-copied:

```bash
python raw/scripts/aggregate.py    # tables C, D, E, F, G, H  (edit LOGS path)
python raw/scripts/cpu_delta.py    # table I                  (edit BASE path)
```

Both scripts contain absolute paths from the machine that ran the campaign;
point them at `raw/logs` and `raw/meta` in this directory.

## Checksum verification

`SHA256SUMS.txt` covers every other file in this directory, sorted by
relative path. From this directory:

```bash
sha256sum -c SHA256SUMS.txt
```

## Secret audit

All committed files were scanned for credential patterns (Datadog API keys,
`Authorization`/`Bearer` headers, `mongodb://` URIs with credentials,
passwords, private keys, cloud and GitHub tokens) and for high-entropy
values. **No secrets were found and nothing was redacted.** The evidence
consists of loadgen stdout, run metadata, integer CPU counters, rendered
manifests that reference ConfigMaps and Secrets only by name, and the
helper scripts.

## Relationship to the private archive

A lossless copy of the campaign directory is held privately outside this
repository. **Every file in that archive is committed here** — the
committed tree and the private archive have identical contents (112 raw
files), so nothing material is available only in private. The private copy
exists as a durability backup, not because anything was withheld.

## Scope of the claims

These measurements support **relative** comparisons between workloads,
bottleneck attribution within this deployment, correctness under controlled
concurrent load, and contention trends. They do **not** support production
capacity, multi-node performance, cloud-network behaviour, availability or
fault-tolerance claims. The report's "Not earned" section is explicit about
this.
