import os, re, glob, statistics

LOGS = "/c/Users/duanh/AppData/Local/Temp/claude/campaign/logs"
LOGS = os.path.expanduser(r"C:\Users\duanh\AppData\Local\Temp\claude\campaign\logs")


def dur_to_ms(s):
    """Parse a Go time.Duration string into milliseconds."""
    s = s.strip()
    m = re.fullmatch(r'(?:(\d+)m)?(?:([\d.]+)s)?', s)
    if m and (m.group(1) or m.group(2)):
        mins = float(m.group(1) or 0)
        secs = float(m.group(2) or 0)
        return mins * 60000 + secs * 1000
    if s.endswith("ms"):
        return float(s[:-2])
    if s.endswith("us") or s.endswith("\u00b5s"):
        return float(s[:-2]) / 1000
    raise ValueError("unparsed duration: %r" % s)


runs = []
for path in sorted(glob.glob(os.path.join(LOGS, "*.log"))):
    name = os.path.basename(path)[:-4]
    txt = open(path, encoding="utf-8", errors="replace").read()
    m = re.search(r'workload=(\S+) attempted=(\d+) succeeded=(\d+) failed=(\d+) '
                  r'elapsed=(\S+) throughput=([\d.]+) ops/s', txt)
    if not m:
        continue
    l = re.search(r'latency_samples=(\d+) p50=(\S+) p95=(\S+) p99=(\S+)', txt)
    r = re.search(r'replicas_served=(\d+) (map\[[^\]]*\])', txt)
    runs.append(dict(
        name=name, workload=m.group(1),
        attempted=int(m.group(2)), succeeded=int(m.group(3)), failed=int(m.group(4)),
        elapsed=m.group(5), tput=float(m.group(6)),
        samples=int(l.group(1)), p50=dur_to_ms(l.group(2)),
        p95=dur_to_ms(l.group(3)), p99=dur_to_ms(l.group(4)),
        replicas=int(r.group(1)) if r else 0,
        rmap=r.group(2) if r else "",
        corruption="DATA CORRUPTION" in txt,
        verifyerr="VERIFICATION ERROR" in txt,
    ))

# ---- raw table -------------------------------------------------------
print("== RAW RUNS ==")
hdr = "%-26s %-18s %5s %5s %6s %7s %8s %9s %9s %9s %3s %s"
print(hdr % ("run", "workload", "att", "succ", "fail", "tput", "samples",
             "p50ms", "p95ms", "p99ms", "rep", "flags"))
for x in runs:
    flags = []
    if x["corruption"]:
        flags.append("CORRUPTION")
    if x["verifyerr"]:
        flags.append("VERIFY_ERR")
    if x["samples"] != x["succeeded"]:
        flags.append("SAMPLE_MISMATCH")
    print(hdr % (x["name"], x["workload"], x["attempted"], x["succeeded"], x["failed"],
                 "%.1f" % x["tput"], x["samples"], "%.1f" % x["p50"],
                 "%.1f" % x["p95"], "%.1f" % x["p99"], x["replicas"],
                 ",".join(flags) if flags else "clean"))

# ---- cells -----------------------------------------------------------
def cell(prefix):
    return [x for x in runs if x["name"].startswith(prefix)]


def med(vals):
    return statistics.median(vals)


print()
print("== AGGREGATED CELLS (median [min-max] over 3 reps) ==")
cells = {}
for wl, tag in (("independent-writes", "ind"), ("same-file-writes", "sf")):
    for c in (1, 2, 4, 8):
        rs = cell("p-%s-c%d-" % (tag, c))
        if len(rs) != 3:
            print("!! cell p-%s-c%d has %d runs" % (tag, c, len(rs)))
        t = [x["tput"] for x in rs]
        p50 = [x["p50"] for x in rs]
        p95 = [x["p95"] for x in rs]
        p99 = [x["p99"] for x in rs]
        cells[(tag, c)] = dict(t=med(t), p50=med(p50), p95=med(p95), p99=med(p99),
                               tmin=min(t), tmax=max(t),
                               p50min=min(p50), p50max=max(p50),
                               p95min=min(p95), p95max=max(p95),
                               p99min=min(p99), p99max=max(p99), n=len(rs))
        cc = cells[(tag, c)]
        print("%-20s c=%d n=%d  tput %.1f [%.1f-%.1f]  p50 %.0f [%.0f-%.0f]  "
              "p95 %.0f [%.0f-%.0f]  p99 %.0f [%.0f-%.0f]"
              % (wl, c, cc["n"], cc["t"], cc["tmin"], cc["tmax"],
                 cc["p50"], cc["p50min"], cc["p50max"],
                 cc["p95"], cc["p95min"], cc["p95max"],
                 cc["p99"], cc["p99min"], cc["p99max"]))

print()
print("== MATCHED CONTENTION ANALYSIS (same-file vs independent, cell medians) ==")
print("%-4s %10s %10s %10s %10s %10s %10s" %
      ("c", "ind_tput", "sf_tput", "ratio", "penalty", "p95_infl", "p99_infl"))
for c in (1, 2, 4, 8):
    i, s = cells[("ind", c)], cells[("sf", c)]
    ratio = s["t"] / i["t"]
    print("%-4d %10.1f %10.1f %10.3f %9.1f%% %10.2f %10.2f"
          % (c, i["t"], s["t"], ratio, (1 - ratio) * 100,
             s["p95"] / i["p95"], s["p99"] / i["p99"]))

print()
print("== SCALING c=1 -> c=8 ==")
for tag, wl in (("ind", "independent-writes"), ("sf", "same-file-writes")):
    a, b = cells[(tag, 1)], cells[(tag, 8)]
    print("%-20s tput %.1f -> %.1f  (%.2fx for 8x concurrency)   p50 %.0f -> %.0f ms   p99 %.0f -> %.0f ms"
          % (wl, a["t"], b["t"], b["t"] / a["t"], a["p50"], b["p50"], a["p99"], b["p99"]))

print()
print("== SECONDARY (c=8, 3 reps) ==")
for pref, label in (("s-mixed-c8-", "mixed"), ("s-reads-c8-", "reads")):
    rs = cell(pref)
    t = [x["tput"] for x in rs]
    p50 = [x["p50"] for x in rs]
    p95 = [x["p95"] for x in rs]
    p99 = [x["p99"] for x in rs]
    print("%-8s n=%d tput %.1f [%.1f-%.1f]  p50 %.0f [%.0f-%.0f]  p95 %.0f [%.0f-%.0f]  p99 %.0f [%.0f-%.0f]"
          % (label, len(rs), med(t), min(t), max(t), med(p50), min(p50), max(p50),
             med(p95), min(p95), max(p95), med(p99), min(p99), max(p99)))

print()
print("== CORRECTNESS SUMMARY ==")
bad = [x for x in runs if x["failed"] or x["corruption"] or x["verifyerr"]
       or x["samples"] != x["succeeded"]]
print("total runs parsed: %d" % len(runs))
print("runs with failures/corruption/verify-errors/sample-mismatch: %d" % len(bad))
print("min replicas_served across all runs: %d" % min(x["replicas"] for x in runs))
