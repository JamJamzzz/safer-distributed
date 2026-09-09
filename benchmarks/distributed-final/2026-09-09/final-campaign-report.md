# Final distributed measurement campaign — 2026-09-09

Measured implementation SHA: **`3fa018e67790e748c9f4e12b0987fd32a618573f`**

Every number in this report was regenerated from the raw logs under `raw/`
(see `raw/scripts/aggregate.py` and `raw/scripts/cpu_delta.py`), not
transcribed from a live console session.

These are measurements of a **single-node `kind` development cluster** at
development resource limits. They are internally comparable to each other.
They are **not** production capacity figures. See "Not earned" below.

---

## A. Starting state

| Item | Value |
|---|---|
| Code SHA measured | `3fa018e67790e748c9f4e12b0987fd32a618573f` |
| Worktree | Clean before and after the campaign; no commits ahead of `origin/main` |
| Cluster | `kind-safer-test`, single node |
| Worker replicas | 3, all Ready |
| Coordinator | 1, Ready |
| Worker args | `-auth-concurrency=2` |
| Worker limits | CPU `500m`, memory `512Mi` (requests `100m` / `192Mi`) |
| Loadgen target | `dns:///safer-worker-headless:50052` |
| Content size | 64 bytes (binary default; never overridden) |
| Images rebuilt | `safer-worker:local`, `safer-loadgen:local`, both loaded into kind; running digest verified to match the loaded digest |
| Coordinator rebuild | Not rebuilt — its code was unchanged in the commits under test |
| Pre-campaign restarts | Workers 0, coordinator 0 |

### Measurement semantics (verified by reading the code at this SHA)

- **Loadgen latency** brackets exactly the `runOne` call. `setup` (`InitUser`
  plus the initial `StoreFile`) runs before the timer starts; the oracle
  verification runs after it stops. Both are excluded.
- **Only successful operations** contribute a latency sample, so
  `latency_samples == succeeded` on a clean run. These samples are
  **unsampled**, unlike Datadog indexed spans.
- `safer.worker.auth_admission.wait.duration` = **queue wait only**; it is
  recorded at the instant a limiter slot is acquired, before
  `GetUserContext` runs.
- `safer.worker.auth_compute.duration` = **post-admission
  `client.GetUserContext` wall time**. This is *not* an Argon2-only figure —
  `GetUserContext` also performs account retrieval and decryption.
- The `worker.authenticate` span = admission wait + `GetUserContext` for one
  **retained** trace.

### Known contamination caveat

Loadgen `setup` issues `StoreFile` RPCs, and the worker's `StoreFile` handler
calls `authenticate()`. Setup therefore **does emit** `auth_compute` and
`auth_admission` observations even though it is excluded from loadgen
latency and throughput. The leading metric bucket of any run is
setup-contaminated; only the middle of a sustained window is clean.
(Setup's `InitUser` takes a limiter slot directly and emits no
`auth_compute`.)

---

## B. Warm-up gate — passed, discarded

`mixed`, c=8, count=100: `attempted=100 succeeded=100 failed=0`,
`latency_samples=100 p50=586.542ms p95=2.200194s p99=4.074177s`,
`replicas_served=3`, no DATA CORRUPTION, no VERIFICATION ERROR, no restarts.

Not counted in any aggregate. Raw: `raw/logs/warmup-mixed-c8.log`.

---

## C. Raw results — 30 fixed-count runs

All runs: `attempted=500 succeeded=500 failed=0`, `latency_samples=500`,
`replicas_served=3`, no DATA CORRUPTION, no VERIFICATION ERROR, no restarts.

| run | workload | tput ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| p-ind-c1-r1 | independent-writes | 9.7 | 105.2 | 126.8 | 217.2 |
| p-ind-c1-r2 | independent-writes | 9.3 | 112.1 | 134.5 | 221.4 |
| p-ind-c1-r3 | independent-writes | 9.8 | 92.5 | 128.0 | 169.7 |
| p-sf-c1-r1 | same-file-writes | 9.8 | 95.6 | 125.4 | 192.4 |
| p-sf-c1-r2 | same-file-writes | 9.7 | 98.6 | 127.8 | 219.9 |
| p-sf-c1-r3 | same-file-writes | 9.3 | 112.0 | 130.1 | 310.6 |
| p-ind-c2-r1 | independent-writes | 14.9 | 122.0 | 209.2 | 308.2 |
| p-ind-c2-r2 | independent-writes | 16.3 | 115.9 | 201.5 | 300.3 |
| p-ind-c2-r3 | independent-writes | 13.3 | 133.5 | 217.7 | 373.9 |
| p-sf-c2-r1 | same-file-writes | 13.7 | 131.1 | 226.7 | 333.5 |
| p-sf-c2-r2 | same-file-writes | 16.3 | 115.7 | 192.2 | 330.5 |
| p-sf-c2-r3 | same-file-writes | 14.4 | 128.6 | 208.5 | 232.3 |
| p-ind-c4-r1 | independent-writes | 12.4 | 189.5 | 774.7 | 1140.6 |
| p-ind-c4-r2 | independent-writes | 14.2 | 130.2 | 739.2 | 1149.2 |
| p-ind-c4-r3 | independent-writes | 14.4 | 170.5 | 739.1 | 1051.0 |
| p-sf-c4-r1 | same-file-writes | 12.2 | 208.8 | 880.8 | 1311.2 |
| p-sf-c4-r2 | same-file-writes | 13.8 | 212.0 | 710.1 | 1168.5 |
| p-sf-c4-r3 | same-file-writes | 15.4 | 189.3 | 620.0 | 769.8 |
| p-ind-c8-r1 | independent-writes | 13.9 | 482.0 | 1392.0 | 1775.6 |
| p-ind-c8-r2 | independent-writes | 14.0 | 227.5 | 1571.3 | 1733.9 |
| p-ind-c8-r3 | independent-writes | 14.6 | 514.6 | 1274.5 | 1499.9 |
| p-sf-c8-r1 | same-file-writes | 14.2 | 524.6 | 1103.9 | 1339.8 |
| p-sf-c8-r2 | same-file-writes | 14.2 | 526.3 | 1020.5 | 1159.5 |
| p-sf-c8-r3 | same-file-writes | 13.8 | 531.3 | 1051.9 | 1325.1 |
| s-mixed-c8-r1 | mixed | 14.3 | 362.9 | 1504.3 | 1696.8 |
| s-mixed-c8-r2 | mixed | 14.7 | 193.7 | 1490.8 | 1658.0 |
| s-mixed-c8-r3 | mixed | 14.1 | 408.4 | 1557.3 | 1726.5 |
| s-reads-c8-r1 | reads | 15.3 | 526.8 | 1007.7 | 1272.6 |
| s-reads-c8-r2 | reads | 14.8 | 517.8 | 1046.1 | 1196.5 |
| s-reads-c8-r3 | reads | 14.6 | 193.0 | 1494.9 | 1572.6 |

Run order was balanced to reduce temporal bias: at each concurrency,
rep 1 ran independent-then-same-file, rep 2 same-file-then-independent,
rep 3 independent-then-same-file.

---

## D. Aggregated cells — median [min–max] across 3 repetitions

| workload | c | tput ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| independent-writes | 1 | 9.7 [9.3–9.8] | 105 [92–112] | 128 [127–134] | 217 [170–221] |
| independent-writes | 2 | 14.9 [13.3–16.3] | 122 [116–133] | 209 [201–218] | 308 [300–374] |
| independent-writes | 4 | 14.2 [12.4–14.4] | 170 [130–189] | 739 [739–775] | 1141 [1051–1149] |
| independent-writes | 8 | 14.0 [13.9–14.6] | 482 [228–515] | 1392 [1274–1571] | 1734 [1500–1776] |
| same-file-writes | 1 | 9.7 [9.3–9.8] | 99 [96–112] | 128 [125–130] | 220 [192–311] |
| same-file-writes | 2 | 14.4 [13.7–16.3] | 129 [116–131] | 209 [192–227] | 331 [232–334] |
| same-file-writes | 4 | 13.8 [12.2–15.4] | 209 [189–212] | 710 [620–881] | 1169 [770–1311] |
| same-file-writes | 8 | 14.2 [13.8–14.2] | 526 [525–531] | 1052 [1021–1104] | 1325 [1159–1340] |

These are **median run-level percentiles across three repetitions**, not
operation-level global percentiles. Three repetitions demonstrate
repeatability; they are not a publication-grade confidence interval.

---

## E. Matched contention analysis (cell medians)

`independent-writes` vs `same-file-writes` — both execute `AppendToFile`
with identical concurrency, count, content size, worker count, auth
settings and resource limits. The deliberate difference is the sharing
shape: separate user/file per caller versus one shared user/file.

| c | ind tput | sf tput | ratio | throughput penalty | p95 inflation | p99 inflation |
|---|---|---|---|---|---|---|
| 1 | 9.7 | 9.7 | 1.000 | 0.0% | 1.00 | 1.01 |
| 2 | 14.9 | 14.4 | 0.966 | 3.4% | 1.00 | 1.07 |
| 4 | 14.2 | 13.8 | 0.972 | 2.8% | 0.96 | 1.02 |
| 8 | 14.0 | 14.2 | 1.014 | −1.4% | 0.76 | 0.76 |

**At c ≤ 8 in this single-node development deployment, same-file contention
produced no repeatable throughput penalty distinguishable from run-to-run
variation.** The spread within a single cell (for example independent-writes
at c=2 ranged 13.3–16.3 ops/s across its three repetitions) is comparable to
or larger than the cross-workload difference.

This is a statement about the **binding throughput constraint at the tested
scale**, not a claim that coordination is free. Section J records that
same-file contention materially increased lock-manager wait time.

---

## F. Scaling, c=1 → c=8

| workload | tput | speedup for 8× concurrency | p50 | p99 |
|---|---|---|---|---|
| independent-writes | 9.7 → 14.0 | 1.44× | 105 → 482 ms | 217 → 1734 ms |
| same-file-writes | 9.7 → 14.2 | 1.46× | 99 → 526 ms | 220 → 1325 ms |

`c=1 → c=2` is the only clear throughput scaling step (9.7 → ~14.5). From
c=2 onward throughput is flat (14.9 → 14.2 → 14.0 for independent-writes)
while p50 grows roughly 4–5× and p99 roughly 6–8×. Past that point,
additional concurrency converts almost entirely into queueing latency.

The fixed-count benchmark alone does not isolate the exact saturating
resource. The shared per-request authentication path and the measured
worker CPU throttling (section I) are the leading explanations.

---

## G. Mixed / reads characterization (c=8, 3 repetitions)

| workload | tput ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|
| mixed | 14.3 [14.1–14.7] | 363 [194–408] | 1504 [1491–1557] | 1697 [1658–1726] |
| reads | 14.8 [14.6–15.3] | 518 [193–527] | 1046 [1008–1495] | 1273 [1196–1573] |

`reads` is the most informative secondary result. It takes only shared
locks (no write serialization) and performs no write transaction, yet lands
at 14.8 ops/s — the same ceiling as both write workloads. All four workload
shapes converge near ~14–15 ops/s at c=8.

---

## H. Correctness and stability evidence

Across all 33 valid runs (30 fixed-count + warm-up + 2 sustained windows),
totalling more than 18,000 operations:

- **0** workload failures (`failed=0` in every run)
- **0** DATA CORRUPTION findings (loadgen's length oracle: final file length
  must equal initial + successful appends × append size)
- **0** VERIFICATION ERROR findings
- `latency_samples == succeeded` in **every** run
- `replicas_served == 3` in **every** run, with near-uniform distribution
  (for example 172/172/172 and 167/167/168) — measured per-RPC from a
  response header, not inferred from replica count
- **0** worker restarts, **0** coordinator restarts, **0** OOMKills

### Excluded run (harness failure, honestly recorded)

The first attempt at attribution window A used a Job name containing an
uppercase letter, which Kubernetes rejects as an invalid RFC 1123 name. The
Job was never created and the workload never executed — the CPU counters
across that interval show no load. This is a harness failure before workload
execution, not a SAFER result, so it was excluded and the window re-run. The
runner was then hardened to surface `kubectl apply` errors instead of
swallowing them.

Artifacts of the excluded attempt are retained deliberately:
`raw/logs/attrib-A-ind-c8-120s.log`, `raw/meta/attrib-A-ind-c8-120s.meta`,
`raw/meta/cpu-before-windowA.txt`, `raw/meta/cpu-after-windowA.txt`. No
valid run was discarded or repeated for being slow.

---

## I. CPU throttling evidence (direct cgroup v2 measurement)

The kubelet `/metrics/cadvisor` and `/metrics/resource` proxy endpoints
returned `NotFound` on this cluster. Counters were instead read directly
from the kind node container's own `/sys/fs/cgroup` tree — a pure read that
modified nothing and used no Datadog credentials
(`raw/scripts/cpu_stat.sh`).

**Method validation:** during a 90-second idle period `nr_throttled` did not
advance while `nr_periods` did, confirming the counters respond to load
rather than to elapsed time.

| Window | pod | periods | throttled | throttled % | throttled_s (aggregate) | cpu_used_s | avg cores |
|---|---|---|---|---|---|---|---|
| A independent c8 | …-2nl2c | 1304 | 915 | 70.2% | 120.0 | 53.9 | 0.413 |
| A | …-hgnfg | 1314 | 1228 | 93.5% | 374.6 | 61.4 | 0.467 |
| A | …-m8c8s | 1304 | 988 | 75.8% | 178.6 | 55.5 | 0.426 |
| B same-file c8 | …-2nl2c | 1272 | 1050 | 82.5% | 185.3 | 56.1 | 0.441 |
| B | …-hgnfg | 1283 | 1174 | 91.5% | 309.6 | 59.5 | 0.464 |
| B | …-m8c8s | 1256 | 956 | 76.1% | 156.6 | 53.2 | 0.424 |

Workers ran at **0.41–0.47 cores against a 0.5-core limit (roughly 83–93% of
quota)** and were **throttled in roughly 70–94% of CFS periods**. The two
windows are closely similar. Aggregate `throttled_usec` exceeds wall-clock
because Argon2 is configured with `p=4` threads, so several threads can be
throttled in the same period.

**Safe conclusion: worker CPU quota was genuinely binding under load.**

Deliberately **not** concluded: that CPU throttling caused all authentication
latency, that throttling alone set the throughput ceiling, or any statement
about intrinsic Argon2 duration. No CPU-limit A/B was run, so the causal
share was not isolated.

---

## J. Sustained attribution windows and manual Datadog observations

*(This section records a manual Datadog UI inspection performed **after** the
automated campaign. The values below are magnitudes read from UI buckets,
not computed percentiles.)*

| Window | Workload | UTC start | UTC end |
|---|---|---|---|
| **A** | `independent-writes` c=8, 120 s | `2026-09-09T09:45:47.430Z` | `2026-09-09T09:48:25.632Z` |
| **B** | `same-file-writes` c=8, 120 s | `2026-09-09T09:50:24.334Z` | `2026-09-09T09:52:59.761Z` |

Each window was preceded and followed by ≥90 s of idle so metric export and
aggregation windows would not smear across runs. Window A: 1829/1829 ops,
15.2 ops/s, p50 524 ms, p95 1.179 s, p99 1.421 s. Window B: 1781/1781 ops,
14.8 ops/s, p50 503 ms, p95 988 ms, p99 1.179 s. Both clean.

Only the **middle** of each window is usable for authentication attribution —
the leading edge contains setup `StoreFile` traffic (see the contamination
caveat in section A).

### `safer.worker.auth_compute.duration` — post-admission `GetUserContext`

Approximately **0.2–0.4 s** magnitude in steady-state buckets in **both**
windows. Post-admission authentication cost stayed in the same broad
magnitude regardless of workload shape. This metric is **not Argon2-only**;
`GetUserContext` also retrieves and decrypts the account record.

### `safer.worker.auth_admission.wait.duration` — limiter queue wait

- independent c=8: commonly ~0.1–0.2 s in steady-state buckets, with one
  observed spike near 0.39 s
- same-file c=8: mostly ~0.01–0.1 s

Admission queueing contributed latency, most visibly in the independent-write
window, but was not a stable shared dominant cost across both shapes. This
metric must **not** be summed with `auth_compute` bucket averages — they have
different denominators and distributions.

### `safer.coordinator.lock.wait.duration` — the decisive contrast

- independent c=8: ≈ **4.6 × 10⁻⁵ s** (≈ 46 microseconds)
- same-file c=8: ≈ **0.063 s** (≈ 63 milliseconds)

Roughly **three orders of magnitude higher** under deliberate same-file
contention. **Same-file contention genuinely exercised the centralized
locking path and materially increased lock-manager wait time.** Despite that
increase, the matched throughput campaign (section E) showed no repeatable
throughput penalty at c ≤ 8 — so coordination cost was real, but was not the
binding throughput constraint at the tested scale.

### `safer.storage.transaction.duration`

Both windows remained in the tens-of-milliseconds range, with observed
buckets broadly around **~20–60 ms**. No same-file-specific transaction
duration explosion was observed. No precise average is claimed from
eyeballed UI buckets.

### Retained traces

For a slow example, filter APM to `service:safer-worker` on the worker
`AppendToFile` RPC within either window above and sort by duration. A
retained trace should show loadgen client → worker server →
`worker.authenticate` and `safer.AppendToFile` as siblings →
coordinator `Acquire` → `coordinator.lock_wait` → `mongostore.run_atomic`.

A retained trace is **one sampled example**, not the benchmark-wide p99.
Indexed spans are a retained subset; the unsampled loadgen distribution in
sections C–D is the benchmark latency source. These two evidence levels must
not be mixed.

---

## K. Evidence interpretation

### Supported

- Correctness held across the entire final campaign: 33 valid runs, >18,000
  operations, zero failures, zero lost updates, zero verification errors.
- All 3 worker replicas actually served every run, in near-uniform
  proportion, verified per-RPC.
- No worker restarts, no coordinator restarts, no OOMKills.
- Throughput saturated at roughly **14–15 ops/s** in this development
  deployment, across all four workload shapes.
- Going from c=1 to c=8 added little throughput (1.44–1.46×) and much more
  latency (p99 roughly 6–8×).
- Same-file contention **materially increased lock-manager wait**, from tens
  of microseconds to tens of milliseconds (section J).
- The matched same-file vs independent comparison showed **no repeatable
  throughput penalty at c ≤ 8** distinguishable from run-to-run variation.
- Workers were **strongly CPU-quota-throttled** under load: 70–94% of CFS
  periods, at 83–93% of a 0.5-core limit.
- Mongo transaction duration remained tens-of-milliseconds scale in the
  observed attribution windows.

### Strongly supported / suggestive

- The **shared per-request authentication path** (plus the measured CPU
  quota pressure) is the leading explanation for the common throughput
  ceiling that all four workload shapes converge on. The strongest
  supporting observation is that `reads` — which takes no exclusive lock and
  runs no write transaction — reaches the same ceiling.
- **Coordinator lock management was not the binding throughput constraint at
  the tested scale**, even under a workload designed to maximise contention.

### Not earned

- Argon2-only duration. `auth_compute` measures `GetUserContext`, which
  includes account retrieval and decryption; Argon2 was never isolated.
- The exact share of authentication latency attributable to CPU throttling.
  No CPU-limit A/B was run.
- "The coordinator can never bottleneck." Only c ≤ 8, one contended file,
  one node were tested.
- "Lock contention is free." Lock wait rose ~1000× under contention; it was
  simply not the binding throughput constraint here, and could surface if the
  upstream ceiling were raised.
- Production throughput or capacity figures of any kind.
- High availability, coordinator fault tolerance, or linear scalability.
- Multi-node performance, real cloud-network behaviour, or production SLOs.
- "Datadog p99 equals loadgen p99" — sampled spans versus unsampled
  client-side samples are different populations.

---

## L. Engineering finding

Reading the code made centralized exclusive-lock contention the obvious
suspect: the same-file workload serializes every write through one exclusive
lock, and that path is the reason the coordinator exists. The matched
experiment showed something subtler and more useful. Contention was
unambiguously real — Datadog showed lock wait rising from roughly 46
microseconds to roughly 63 milliseconds between the two sustained windows,
about three orders of magnitude. Yet same-file throughput stayed within
run-to-run variation of independent writes at every concurrency tested, and
read-only and mixed traffic converged on the same ~14–15 ops/s ceiling.
Meanwhile workers sat at 83–93% of their CPU quota with most scheduling
periods throttled, and post-admission `GetUserContext` stayed in the
hundreds-of-milliseconds range in both windows.

The practical consequence is that measurement changed the optimization
decision. A plausible-sounding next step — sharding or replicating the
coordinator, or weakening concurrency semantics to make a benchmark number
larger — had no evidence behind it: the component that looked expensive was
not the one setting the ceiling, and the ceiling appeared in workloads that
never touch the contended path at all. Instrumentation did not just locate a
bottleneck; it falsified the intuition that would otherwise have driven the
next architecture change.

---

## M. Freeze recommendation

**FREEZE.** No correctness defect was found. The evidence is sufficient for
this project's intended scope, and the remaining performance findings are
characteristics of the tested development deployment — a deliberately
expensive per-request key-derivation path running under development CPU
limits on a single-node cluster that also hosts MongoDB, the coordinator and
the Datadog Agent.

Reopening technical development should require a genuine correctness defect
or a new explicit project requirement — **not** the observation that a
benchmark number could be higher.
