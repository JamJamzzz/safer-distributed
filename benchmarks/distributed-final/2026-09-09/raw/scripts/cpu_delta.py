import re, os

BASE = r"C:\Users\duanh\AppData\Local\Temp\claude\campaign\meta"


def load(fn):
    out = {}
    for line in open(os.path.join(BASE, fn), encoding="utf-8", errors="replace"):
        m = re.match(r'(\S+) nr_periods=(\d+) nr_throttled=(\d+) '
                     r'throttled_usec=(\d+) usage_usec=(\d+)', line.strip())
        if m:
            out[m.group(1)] = dict(np=int(m.group(2)), nt=int(m.group(3)),
                                   tu=int(m.group(4)), uu=int(m.group(5)))
    return out


for label, before_f, after_f in (
    ("WINDOW A  independent-writes c8 120s", "cpu-before-winA.txt", "cpu-after-winA.txt"),
    ("WINDOW B  same-file-writes   c8 120s", "cpu-before-winB.txt", "cpu-after-winB.txt"),
):
    b, a = load(before_f), load(after_f)
    print("== %s ==" % label)
    print("%-32s %8s %8s %9s %12s %12s %10s" %
          ("pod", "periods", "thrtld", "thrtld%", "throttled_s", "cpu_used_s", "cores"))
    for pod in sorted(b):
        if pod not in a:
            continue
        dnp = a[pod]["np"] - b[pod]["np"]
        dnt = a[pod]["nt"] - b[pod]["nt"]
        dtu = (a[pod]["tu"] - b[pod]["tu"]) / 1e6
        duu = (a[pod]["uu"] - b[pod]["uu"]) / 1e6
        pct = 100.0 * dnt / dnp if dnp else 0.0
        # CFS period is 100ms by default, so dnp*0.1s approximates the
        # wall-clock the cgroup had runnable tasks in.
        cores = duu / (dnp * 0.1) if dnp else 0.0
        print("%-32s %8d %8d %8.1f%% %12.1f %12.1f %10.3f" %
              (pod, dnp, dnt, pct, dtu, duu, cores))
    print()
