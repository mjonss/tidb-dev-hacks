#!/usr/bin/env python3
"""Attribute resource usage (RSS, heap, goroutines, and optionally CPU) to
ANALYZE phases, so memory/CPU can be compared per phase rather than as a
single whole-run peak.

Phases (boundaries from profile_result.json + tidb.log + analyze_jobs):
  collection : analyze_start        -> global_stats_start
               (per-partition scan + build; identical code both branches)
  merge      : global_stats_start   -> analyze_end
               (per-partition stats load from storage + the global merge;
                this is the phase the PR rewrites)

global_stats_start = first "use async merge global stats" /
"use blocking merge global stats" line in tidb.log — present on both
branches. On part-single, collection is ~0s (one partition), so nearly the
whole run is the merge phase.

Memory/goroutines come from the tidb_metrics time series (~2s cadence);
each sample is bucketed by timestamp into a phase. CPU (--cpu) maps the
cpu_profile_*.pb.gz snapshots to phases by file mtime and aggregates the
merge-phase profiles with `go tool pprof -top`.

Usage:
  resource-by-phase.py <cell_dir> [--cpu]
"""
import sys, os, re, glob, json, subprocess
from datetime import datetime, timezone

TS = re.compile(r'^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+\-]\d{2}:\d{2})\]')

def log_ts(s): return datetime.strptime(s, "%Y/%m/%d %H:%M:%S.%f %z")
def json_ts(s):
    m = re.match(r'(.*\.\d{6})\d*([+\-]\d{2}:\d{2})$', s)
    if m: s = m.group(1) + m.group(2)
    return datetime.fromisoformat(s)
def gb(b): return b / 1e9

def first_marker(logpath, needles):
    with open(logpath, errors="replace") as f:
        for line in f:
            if any(n in line for n in needles):
                m = TS.match(line)
                if m: return log_ts(m.group(1))
    return None

def phase_of(t, a_start, gs_start, a_end):
    if gs_start is None:
        return "merge"  # part-single: collection negligible
    return "collection" if t < gs_start else "merge"

def analyze(cell, do_cpu):
    runs = [r for r in sorted(glob.glob(os.path.join(cell, "run_*")))
            if os.path.exists(os.path.join(r, "profile_result.json"))]
    if not runs:
        print("no run in", cell); return
    run = runs[-1]
    d = json.load(open(os.path.join(run, "profile_result.json")))
    a_start = json_ts(d["analyze_start_time"]); a_end = json_ts(d["analyze_end_time"])
    log = os.path.join(run, "tidb-logs", "tidb.log")
    gs_start = first_marker(log, ("use async merge global stats",
                                  "use blocking merge global stats")) \
               if os.path.exists(log) else None

    # bucket the metric samples
    buckets = {}
    for s in d.get("tidb_metrics", []):
        t = json_ts(s["timestamp"])
        ph = phase_of(t, a_start, gs_start, a_end)
        m = s.get("metrics", {})
        rss = m.get("process_resident_memory_bytes", 0)
        heap = m.get("go_memstats_heap_alloc_bytes", 0)
        buckets.setdefault(ph, []).append((rss, heap))
    goro = {}
    for s in d.get("goroutine_samples", []):
        t = json_ts(s["timestamp"])
        ph = phase_of(t, a_start, gs_start, a_end)
        goro.setdefault(ph, []).append(s.get("total", 0))

    print(f"# {os.path.relpath(cell)}")
    dur = (a_end - a_start).total_seconds()
    coll = (gs_start - a_start).total_seconds() if gs_start else 0.0
    print(f"  total {dur:.0f}s | collection {coll:.0f}s | merge {dur-coll:.0f}s\n")
    print(f"  {'phase':<11} {'samples':>7} {'peakRSS':>8} {'meanRSS':>8} "
          f"{'peakHeap':>9} {'meanHeap':>9} {'maxGoro':>8}")
    for ph in ("collection", "merge"):
        b = buckets.get(ph)
        if not b: continue
        rss = [x[0] for x in b]; heap = [x[1] for x in b]
        g = goro.get(ph, [0])
        print(f"  {ph:<11} {len(b):>7} {gb(max(rss)):>7.2f}G {gb(sum(rss)/len(rss)):>7.2f}G "
              f"{gb(max(heap)):>8.2f}G {gb(sum(heap)/len(heap)):>8.2f}G {max(g):>8}")

    if do_cpu:
        cpu_by_phase(run, a_start, gs_start, a_end, cell)

def cpu_by_phase(run, a_start, gs_start, a_end, cell):
    """Aggregate merge-phase CPU profiles. Snapshots are ~fixed-interval
    pprof captures; map each to a phase by mtime, sum top functions over the
    merge phase."""
    binflag = "tidb-server.pr" if "/PR/" in cell else "tidb-server.master"
    binp = os.path.join(os.path.dirname(__file__), binflag)
    profs = sorted(glob.glob(os.path.join(run, "cpu_profile_*.pb.gz")))
    merge_profs = []
    for p in profs:
        mt = datetime.fromtimestamp(os.path.getmtime(p), tz=timezone.utc)
        if gs_start is None or mt >= gs_start:
            merge_profs.append(p)
    print(f"\n  CPU: {len(merge_profs)}/{len(profs)} snapshots fall in the merge phase")
    if not merge_profs or not os.path.exists(binp):
        print("  (no merge profiles or binary missing)"); return
    # aggregate a sample of merge-phase profiles (every Nth to bound cost)
    step = max(1, len(merge_profs) // 20)
    sample = merge_profs[::step]
    agg = {}
    for p in sample:
        try:
            out = subprocess.run(["go", "tool", "pprof", "-top", "-nodecount=8",
                                  "-unit=ms", binp, p],
                                 capture_output=True, text=True, timeout=60).stdout
        except Exception:
            continue
        for line in out.splitlines():
            m = re.match(r'\s*[\d.]+m?s\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)(m?s)\s+[\d.]+%\s+(\S+)', line)
            if m:
                v = float(m.group(1)) * (1 if m.group(2) == "ms" else 1000)
                agg[m.group(3)] = agg.get(m.group(3), 0) + v
    print(f"  merge-phase hot functions (summed cum ms over {len(sample)} sampled snapshots):")
    for fn, v in sorted(agg.items(), key=lambda x: -x[1])[:8]:
        print(f"    {v:>10.0f}ms  {fn}")

if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    analyze(args[0], "--cpu" in sys.argv)
