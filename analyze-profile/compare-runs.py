#!/usr/bin/env python3
"""
Compare ANALYZE TABLE profile runs between two TiDB versions.

Usage:
    python3 compare-runs.py <new_run1> <new_run2> <old_run1> <old_run2>

Example:
    python3 compare-runs.py \
        output/run_20260301_231159 output/run_20260301_233043 \
        output/run_20260302_000926 output/run_20260302_001727

Prompt / methodology:
    This script extracts and compares the following from profile_result.json:
    1. TiDB version (from tidb_version.txt)
    2. Session variables (flag any differences between new and old groups)
    3. ANALYZE wall-clock duration
    4. Partition job summary: count by state, duration stats for finished jobs
       (only jobs whose start_time falls within this run's analyze window)
    5. TiDB metrics time series breakdown:
       - RSS, heap alloc, CPU rate, goroutines at ~30s intervals
       - Peak values for each metric
       - Phase detection: scan phase (~first half, low memory) vs
         merge phase (~second half, high memory)
    6. Slow query data: query_time, mem_max, total_keys
    7. File counts (heap profiles, cpu profiles) as a proxy for run length
    8. Stats TSV row counts (stats_topn, stats_histograms, stats_buckets, stats_meta)

    The goal is to surface regressions or improvements between the "new" and "old" versions.
"""

import json
import os
import statistics
import sys
from datetime import datetime


def parse_duration_to_seconds(d):
    """Parse Go duration string like '6m23.143s' or '382ms' to float seconds."""
    if not d:
        return 0.0
    s = d
    total = 0.0
    if "h" in s:
        parts = s.split("h", 1)
        total += float(parts[0]) * 3600
        s = parts[1]
    if "m" in s and "ms" not in s:
        parts = s.split("m", 1)
        total += float(parts[0]) * 60
        s = parts[1]
    if s.endswith("ms"):
        total += float(s[:-2]) / 1000
    elif s.endswith("s"):
        total += float(s[:-1])
    elif s:
        try:
            total += float(s)
        except ValueError:
            pass
    return total


def parse_ts(ts_str):
    """Parse ISO timestamp to datetime, handling both 'T' and space separators."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)  # normalize to naive for comparison
    except ValueError:
        try:
            return datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def load_run(run_dir):
    """Load all relevant data from a run directory."""
    result = {"dir": run_dir}

    # TiDB version
    ver_path = os.path.join(run_dir, "tidb_version.txt")
    if os.path.exists(ver_path):
        with open(ver_path) as f:
            result["version"] = f.read().strip()
    else:
        result["version"] = "(not available)"

    # Profile result JSON
    json_path = os.path.join(run_dir, "profile_result.json")
    with open(json_path) as f:
        data = json.load(f)

    result["session_vars"] = data.get("session_vars", {})
    result["analyze_duration"] = data.get("analyze_duration", "")
    # Prefer the ns field — ms-rounded string loses precision for close runs.
    if isinstance(data.get("analyze_duration_ns"), (int, float)):
        result["analyze_duration_s"] = data["analyze_duration_ns"] / 1e9
    else:
        result["analyze_duration_s"] = parse_duration_to_seconds(result["analyze_duration"])
    result["analyze_start"] = data.get("analyze_start_time", "")
    result["analyze_end"] = data.get("analyze_end_time", "")

    # Partition jobs - filter to this run's time window.
    # SHOW ANALYZE STATUS accumulates history, so we must match by start_time.
    analyze_start = parse_ts(data.get("analyze_start_time", ""))
    analyze_end = parse_ts(data.get("analyze_end_time", ""))
    jobs = data.get("partition_jobs", [])
    current_jobs = []
    for j in jobs:
        jt = parse_ts(j.get("start_time"))
        if jt and analyze_start and analyze_end:
            # Include if job started within 2s of analyze window
            from datetime import timedelta
            if jt >= analyze_start - timedelta(seconds=2) and jt <= analyze_end + timedelta(seconds=2):
                current_jobs.append(j)
        elif j.get("state") == "running":
            current_jobs.append(j)
    result["jobs_total"] = len(current_jobs)
    result["jobs_by_state"] = {}
    for j in current_jobs:
        state = j.get("state", "unknown")
        result["jobs_by_state"][state] = result["jobs_by_state"].get(state, 0) + 1

    finished_durations = []
    for j in current_jobs:
        if j.get("state") == "finished" and j.get("duration"):
            d = parse_duration_to_seconds(j["duration"])
            if d > 0:
                finished_durations.append(d)
    result["job_durations"] = finished_durations
    if finished_durations:
        result["job_dur_min"] = min(finished_durations)
        result["job_dur_max"] = max(finished_durations)
        result["job_dur_median"] = statistics.median(finished_durations)
    else:
        result["job_dur_min"] = result["job_dur_max"] = result["job_dur_median"] = 0

    # TiDB metrics - full time series + peaks
    tidb_metrics = data.get("tidb_metrics", [])
    result["metrics_raw"] = tidb_metrics
    peak_rss = 0
    peak_heap = 0
    peak_goroutines = 0
    prev_cpu = None
    time_series = []  # list of dicts with elapsed_s, rss_gb, heap_gb, cpu_rate_pct, goroutines

    for i, sample in enumerate(tidb_metrics):
        m = sample.get("metrics", {})
        cpu = m.get("process_cpu_seconds_total", 0)
        rss = m.get("process_resident_memory_bytes", 0)
        heap = m.get("go_memstats_heap_alloc_bytes", 0)
        heap_inuse = m.get("go_memstats_heap_inuse_bytes", 0)
        goroutines = m.get("go_goroutines", 0)

        cpu_rate = 0.0
        if prev_cpu is not None:
            cpu_rate = (cpu - prev_cpu) / 2.0 * 100  # ~2s interval, as percentage
        prev_cpu = cpu

        rss_gb = rss / (1024**3)
        heap_gb = heap / (1024**3)

        time_series.append({
            "elapsed_s": i * 2,  # ~2s per sample
            "rss_gb": rss_gb,
            "heap_gb": heap_gb,
            "heap_inuse_gb": heap_inuse / (1024**3),
            "cpu_rate_pct": cpu_rate,
            "goroutines": goroutines,
        })

        if rss > peak_rss:
            peak_rss = rss
        if heap > peak_heap:
            peak_heap = heap
        if goroutines > peak_goroutines:
            peak_goroutines = goroutines

    result["time_series"] = time_series
    result["peak_rss_gb"] = peak_rss / (1024**3)
    result["peak_heap_gb"] = peak_heap / (1024**3)
    result["peak_goroutines"] = peak_goroutines

    # Slow queries
    slow = data.get("slow_queries", [])
    result["slow_queries"] = slow
    if slow:
        result["slow_query_time"] = slow[0].get("query_time", 0)
        result["slow_mem_max"] = slow[0].get("mem_max", 0)
        result["slow_total_keys"] = slow[0].get("total_keys", 0)
    else:
        result["slow_query_time"] = result["slow_mem_max"] = result["slow_total_keys"] = 0

    # File counts
    files = os.listdir(run_dir)
    result["cpu_profiles"] = len([f for f in files if f.startswith("cpu_profile_")])
    result["heap_profiles"] = len([f for f in files if f.startswith("heap_")])

    # Stats TSV row counts (minus 1 for header)
    for tsv in ["stats_topn.tsv", "stats_histograms.tsv", "stats_buckets.tsv", "stats_meta.tsv"]:
        tsv_path = os.path.join(run_dir, tsv)
        if os.path.exists(tsv_path):
            with open(tsv_path) as f:
                lines = sum(1 for _ in f)
            result[tsv] = max(0, lines - 1)
        else:
            result[tsv] = "(missing)"

    return result


def fmt_gb(gb):
    return f"{gb:.2f} GB"


def fmt_secs(s):
    # Keep ms precision at every magnitude so sub-second differences between
    # PR and base are visible when averaging across many iterations.
    if s < 60:
        return f"{s:.3f}s"
    m = int(s) // 60
    sec = s - m * 60
    return f"{m}m{sec:06.3f}s"


def fmt_bytes(b):
    return f"{b / (1024**3):.2f} GB"


def print_run(r, label):
    print(f"\n{'='*70}")
    print(f"  {label}: {r['dir']}")
    print(f"{'='*70}")
    ver_lines = r["version"].split("\n")
    for vl in ver_lines[:3]:
        print(f"  {vl}")
    print(f"  ANALYZE duration:     {r['analyze_duration']} ({r['analyze_duration_s']:.6f}s)")
    print(f"  Partition jobs:       {r['jobs_total']} total, by state: {r['jobs_by_state']}")
    if r["job_durations"]:
        print(f"  Job durations:        min={fmt_secs(r['job_dur_min'])}, median={fmt_secs(r['job_dur_median'])}, max={fmt_secs(r['job_dur_max'])}")
    print(f"  Peak RSS:             {fmt_gb(r['peak_rss_gb'])}")
    print(f"  Peak heap alloc:      {fmt_gb(r['peak_heap_gb'])}")
    print(f"  Peak goroutines:      {r['peak_goroutines']}")
    print(f"  CPU/heap profiles:    {r['cpu_profiles']} / {r['heap_profiles']}")
    if r["slow_queries"]:
        print(f"  Slow query time:      {r['slow_query_time']:.3f}s")
        print(f"  Slow query mem_max:   {fmt_bytes(r['slow_mem_max'])}")
        print(f"  Slow query keys:      {r['slow_total_keys']}")
    for tsv in ["stats_topn.tsv", "stats_histograms.tsv", "stats_buckets.tsv", "stats_meta.tsv"]:
        print(f"  {tsv}:  {r[tsv]} rows")

    # Timing breakdown: show metrics at ~30s intervals
    ts = r["time_series"]
    if ts:
        print(f"\n  Timing breakdown (~30s intervals):")
        print(f"  {'elapsed':>8}  {'RSS':>8}  {'heap':>8}  {'CPU%':>6}  {'goro':>6}")
        print(f"  {'-------':>8}  {'---':>8}  {'----':>8}  {'----':>6}  {'----':>6}")
        step = max(1, len(ts) // 13)  # ~13 rows
        indices = list(range(0, len(ts), step))
        if indices[-1] != len(ts) - 1:
            indices.append(len(ts) - 1)
        for i in indices:
            s = ts[i]
            print(f"  {fmt_secs(s['elapsed_s']):>8}  {fmt_gb(s['rss_gb']):>8}  {fmt_gb(s['heap_gb']):>8}  {s['cpu_rate_pct']:>5.0f}%  {s['goroutines']:>6}")


def print_comparison(new_runs, old_runs):
    print(f"\n{'='*70}")
    print(f"  COMPARISON: NEW vs OLD")
    print(f"{'='*70}")

    def avg(runs, key):
        vals = [r[key] for r in runs if isinstance(r.get(key), (int, float))]
        return sum(vals) / len(vals) if vals else 0

    def pct_change(new, old):
        if old == 0:
            return "N/A"
        return f"{(new - old) / old * 100:+.1f}%"

    def vals(runs, key):
        return [r[key] for r in runs if isinstance(r.get(key), (int, float))]

    # Duration
    print(f"\n  --- Duration ---")
    for r in new_runs:
        print(f"    NEW {r['dir']}: {r['analyze_duration']}")
    for r in old_runs:
        print(f"    OLD {r['dir']}: {r['analyze_duration']}")
    new_dur = avg(new_runs, "analyze_duration_s")
    old_dur = avg(old_runs, "analyze_duration_s")
    print(f"    Avg: NEW={fmt_secs(new_dur)}  OLD={fmt_secs(old_dur)}  ({pct_change(new_dur, old_dur)})")

    # Peak memory
    print(f"\n  --- Peak Memory ---")
    print(f"    {'Run':<45} {'RSS':>10} {'Heap':>10}")
    for r in new_runs:
        print(f"    NEW {os.path.basename(r['dir']):<40} {fmt_gb(r['peak_rss_gb']):>10} {fmt_gb(r['peak_heap_gb']):>10}")
    for r in old_runs:
        print(f"    OLD {os.path.basename(r['dir']):<40} {fmt_gb(r['peak_rss_gb']):>10} {fmt_gb(r['peak_heap_gb']):>10}")
    new_rss = avg(new_runs, "peak_rss_gb")
    old_rss = avg(old_runs, "peak_rss_gb")
    new_heap = avg(new_runs, "peak_heap_gb")
    old_heap = avg(old_runs, "peak_heap_gb")
    print(f"    Avg:{'':40} {fmt_gb(new_rss):>10} {fmt_gb(new_heap):>10}  (NEW)")
    print(f"    Avg:{'':40} {fmt_gb(old_rss):>10} {fmt_gb(old_heap):>10}  (OLD)")
    print(f"    Change:{'':37} {pct_change(new_rss, old_rss):>10} {pct_change(new_heap, old_heap):>10}")

    # Goroutines
    new_gor = avg(new_runs, "peak_goroutines")
    old_gor = avg(old_runs, "peak_goroutines")
    print(f"\n  --- Peak Goroutines ---")
    print(f"    NEW={new_gor:.0f}  OLD={old_gor:.0f}")

    # Slow queries
    new_qt = avg(new_runs, "slow_query_time")
    old_qt = avg(old_runs, "slow_query_time")
    if new_qt or old_qt:
        print(f"\n  --- Slow Query ---")
        print(f"    {'Run':<45} {'Time':>10} {'Mem':>10} {'Keys':>12}")
        for r in new_runs:
            print(f"    NEW {os.path.basename(r['dir']):<40} {r['slow_query_time']:>9.1f}s {fmt_bytes(r['slow_mem_max']):>10} {r['slow_total_keys']:>12}")
        for r in old_runs:
            print(f"    OLD {os.path.basename(r['dir']):<40} {r['slow_query_time']:>9.1f}s {fmt_bytes(r['slow_mem_max']):>10} {r['slow_total_keys']:>12}")

    # Partition jobs
    print(f"\n  --- Partition Jobs ---")
    for r in new_runs + old_runs:
        label = "NEW" if r in new_runs else "OLD"
        print(f"    {label} {os.path.basename(r['dir'])}: {r['jobs_total']} jobs, states={r['jobs_by_state']}")
        if r["job_durations"]:
            print(f"         durations: min={fmt_secs(r['job_dur_min'])} median={fmt_secs(r['job_dur_median'])} max={fmt_secs(r['job_dur_max'])}")

    # Session var differences
    all_keys = set()
    for r in new_runs + old_runs:
        all_keys.update(r["session_vars"].keys())
    diffs = []
    for k in sorted(all_keys):
        new_vals = set(r["session_vars"].get(k, "(unset)") for r in new_runs)
        old_vals = set(r["session_vars"].get(k, "(unset)") for r in old_runs)
        if new_vals != old_vals:
            diffs.append((k, new_vals, old_vals))
    if diffs:
        print(f"\n  --- Session Variable Differences ---")
        for k, nv, ov in diffs:
            print(f"    {k}: NEW={nv}  OLD={ov}")
    else:
        print(f"\n  Session variables: identical across all runs")

    # Stats TSV differences
    print(f"\n  --- Stats TSV Row Counts ---")
    for tsv in ["stats_topn.tsv", "stats_histograms.tsv", "stats_buckets.tsv", "stats_meta.tsv"]:
        new_vals = [r[tsv] for r in new_runs]
        old_vals = [r[tsv] for r in old_runs]
        marker = " <-- DIFF" if new_vals != old_vals else ""
        print(f"    {tsv}:  NEW={new_vals}  OLD={old_vals}{marker}")

    # Side-by-side timing: compare memory trajectory at matching elapsed times
    print(f"\n  --- Memory Trajectory Comparison (NEW#1 vs OLD#1) ---")
    ts_new = new_runs[0]["time_series"]
    ts_old = old_runs[0]["time_series"]
    max_len = max(len(ts_new), len(ts_old))
    step = max(1, max_len // 13)
    print(f"  {'elapsed':>8}  {'NEW RSS':>8}  {'NEW heap':>9}  {'OLD RSS':>8}  {'OLD heap':>9}")
    print(f"  {'-------':>8}  {'-------':>8}  {'--------':>9}  {'-------':>8}  {'--------':>9}")
    for i in range(0, max_len, step):
        elapsed = i * 2
        n = ts_new[i] if i < len(ts_new) else None
        o = ts_old[i] if i < len(ts_old) else None
        n_rss = fmt_gb(n['rss_gb']) if n else "---"
        n_heap = fmt_gb(n['heap_gb']) if n else "---"
        o_rss = fmt_gb(o['rss_gb']) if o else "---"
        o_heap = fmt_gb(o['heap_gb']) if o else "---"
        print(f"  {fmt_secs(elapsed):>8}  {n_rss:>8}  {n_heap:>9}  {o_rss:>8}  {o_heap:>9}")

    print(f"\n  --- Memory Trajectory Comparison (NEW#2 vs OLD#2) ---")
    ts_new = new_runs[1]["time_series"]
    ts_old = old_runs[1]["time_series"]
    max_len = max(len(ts_new), len(ts_old))
    step = max(1, max_len // 13)
    print(f"  {'elapsed':>8}  {'NEW RSS':>8}  {'NEW heap':>9}  {'OLD RSS':>8}  {'OLD heap':>9}")
    print(f"  {'-------':>8}  {'-------':>8}  {'--------':>9}  {'-------':>8}  {'--------':>9}")
    for i in range(0, max_len, step):
        elapsed = i * 2
        n = ts_new[i] if i < len(ts_new) else None
        o = ts_old[i] if i < len(ts_old) else None
        n_rss = fmt_gb(n['rss_gb']) if n else "---"
        n_heap = fmt_gb(n['heap_gb']) if n else "---"
        o_rss = fmt_gb(o['rss_gb']) if o else "---"
        o_heap = fmt_gb(o['heap_gb']) if o else "---"
        print(f"  {fmt_secs(elapsed):>8}  {n_rss:>8}  {n_heap:>9}  {o_rss:>8}  {o_heap:>9}")


def print_multi_comparison(groups):
    """Compare N groups side by side."""
    labels = [g[0] for g in groups]

    def avg(runs, key):
        vals = [r[key] for r in runs if isinstance(r.get(key), (int, float))]
        return sum(vals) / len(vals) if vals else 0

    def pct_vs_first(val, baseline):
        if baseline == 0:
            return ""
        return f"({(val - baseline) / baseline * 100:+.1f}%)"

    print(f"\n{'='*70}")
    print(f"  COMPARISON: {' vs '.join(labels)}")
    print(f"{'='*70}")

    # Duration
    print(f"\n  --- Duration ---")
    avgs = []
    for label, runs in groups:
        for r in runs:
            print(f"    {label:>12} {os.path.basename(r['dir'])}: {r['analyze_duration']}")
        a = avg(runs, "analyze_duration_s")
        avgs.append(a)
    for i, (label, _) in enumerate(groups):
        pct = pct_vs_first(avgs[i], avgs[0]) if i > 0 else ""
        print(f"    {label:>12} avg: {fmt_secs(avgs[i])}  {pct}")

    # Peak memory
    print(f"\n  --- Peak Memory ---")
    print(f"    {'Group':>12} {'Run':<28} {'RSS':>10} {'Heap':>10}")
    rss_avgs = []
    heap_avgs = []
    for label, runs in groups:
        for r in runs:
            print(f"    {label:>12} {os.path.basename(r['dir']):<28} {fmt_gb(r['peak_rss_gb']):>10} {fmt_gb(r['peak_heap_gb']):>10}")
        rss_avgs.append(avg(runs, "peak_rss_gb"))
        heap_avgs.append(avg(runs, "peak_heap_gb"))
    print(f"    {'':>12} {'Averages:':>28}")
    for i, (label, _) in enumerate(groups):
        rss_pct = pct_vs_first(rss_avgs[i], rss_avgs[0]) if i > 0 else ""
        heap_pct = pct_vs_first(heap_avgs[i], heap_avgs[0]) if i > 0 else ""
        print(f"    {label:>12} {'avg':<28} {fmt_gb(rss_avgs[i]):>10} {fmt_gb(heap_avgs[i]):>10}  {rss_pct}  {heap_pct}")

    # Goroutines
    print(f"\n  --- Peak Goroutines ---")
    for label, runs in groups:
        print(f"    {label:>12}: {avg(runs, 'peak_goroutines'):.0f}")

    # Slow queries
    print(f"\n  --- Slow Query Time ---")
    for label, runs in groups:
        for r in runs:
            print(f"    {label:>12} {os.path.basename(r['dir'])}: {r['slow_query_time']:.1f}s")
        a = avg(runs, "slow_query_time")
        print(f"    {label:>12} avg: {a:.1f}s")

    # Partition jobs
    print(f"\n  --- Partition Jobs ---")
    for label, runs in groups:
        for r in runs:
            dur_info = ""
            if r["job_durations"]:
                dur_info = f" dur: min={fmt_secs(r['job_dur_min'])} med={fmt_secs(r['job_dur_median'])} max={fmt_secs(r['job_dur_max'])}"
            print(f"    {label:>12} {os.path.basename(r['dir'])}: {r['jobs_total']} jobs {r['jobs_by_state']}{dur_info}")

    # Session var differences
    all_keys = set()
    all_runs = [r for _, runs in groups for r in runs]
    for r in all_runs:
        all_keys.update(r["session_vars"].keys())
    diffs = []
    for k in sorted(all_keys):
        vals_per_group = {}
        for label, runs in groups:
            vals_per_group[label] = set(r["session_vars"].get(k, "(unset)") for r in runs)
        all_same = len(set(frozenset(v) for v in vals_per_group.values())) == 1
        if not all_same:
            diffs.append((k, vals_per_group))
    if diffs:
        print(f"\n  --- Session Variable Differences ---")
        for k, vals_per_group in diffs:
            parts = [f"{label}={v}" for label, v in vals_per_group.items()]
            print(f"    {k}: {', '.join(parts)}")
    else:
        print(f"\n  Session variables: identical across all groups")

    # Stats TSV
    print(f"\n  --- Stats TSV Row Counts ---")
    for tsv in ["stats_topn.tsv", "stats_histograms.tsv", "stats_buckets.tsv", "stats_meta.tsv"]:
        parts = []
        for label, runs in groups:
            vals = [r[tsv] for r in runs]
            parts.append(f"{label}={vals}")
        print(f"    {tsv}: {', '.join(parts)}")

    # Memory trajectory: compare first run from each group
    print(f"\n  --- Memory Trajectory (run #1 from each group) ---")
    first_runs = [(label, runs[0]) for label, runs in groups]
    max_ts = max(len(r["time_series"]) for _, r in first_runs)
    step = max(1, max_ts // 13)
    header = f"  {'elapsed':>8}"
    for label, _ in first_runs:
        header += f"  {label+' RSS':>12} {label+' heap':>12}"
    print(header)
    print(f"  {'-------':>8}" + f"  {'-------':>12} {'--------':>12}" * len(first_runs))
    for i in range(0, max_ts, step):
        elapsed = i * 2
        line = f"  {fmt_secs(elapsed):>8}"
        for _, r in first_runs:
            ts = r["time_series"]
            if i < len(ts):
                line += f"  {fmt_gb(ts[i]['rss_gb']):>12} {fmt_gb(ts[i]['heap_gb']):>12}"
            else:
                line += f"  {'---':>12} {'---':>12}"
        print(line)


def main():
    # Parse --group arguments: --group label dir1 dir2 ... [--group label dir3 ...]
    # Or legacy mode: dir1 dir2 ... (split in half)
    args = sys.argv[1:]

    if "--group" in args:
        groups = []
        i = 0
        while i < len(args):
            if args[i] == "--group":
                label = args[i + 1]
                i += 2
                dirs = []
                while i < len(args) and args[i] != "--group":
                    dirs.append(args[i])
                    i += 1
                groups.append((label, [load_run(d) for d in dirs]))
        if len(groups) < 2:
            print("Need at least 2 groups")
            sys.exit(1)
    else:
        if len(args) < 4 or len(args) % 2 != 0:
            print(f"Usage: {sys.argv[0]} --group LABEL dir1 [dir2...] --group LABEL dir3 [dir4...]")
            print(f"  Or: {sys.argv[0]} dir1 dir2 ... (even count, split in half: NEW vs OLD)")
            sys.exit(1)
        mid = len(args) // 2
        groups = [
            ("NEW", [load_run(d) for d in args[:mid]]),
            ("OLD", [load_run(d) for d in args[mid:]]),
        ]

    for label, runs in groups:
        for r in runs:
            print_run(r, label)
    print_multi_comparison(groups)


if __name__ == "__main__":
    main()
