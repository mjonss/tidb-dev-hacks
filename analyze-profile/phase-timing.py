#!/usr/bin/env python3
"""Decompose ANALYZE wall-clock into phases, to isolate the phase the PR
actually changes (the global merge) from the phases it does not touch
(per-partition collection, and per-partition stats load from storage).

The PR (improve-global-stats-3) rewrites ONLY the global-merge step
(mergeGlobalStatsTopN / MergePartitionHist2GlobalHist -> the combined
MergePartTopNAndHistToGlobal). It does not change:
  - per-partition sample collection + per-partition TopN/histogram build
    (the ANALYZE scan phase, identical executor code on both branches),
  - saving/loading per-partition stats to/from mysql.stats_* (the async
    load loop `for _, partitionID := range a.partitionIDs` is byte-identical
    between branches, per the v4 code audit).

So total-ANALYZE speedup understates the merge speedup, especially on
part-full where ~15 min of per-partition collection dilutes it. This script
extracts the phase boundaries that exist on BOTH branches:

  analyze_start ........ profile_result.json analyze_start_time
  global_stats_start ... first "use async merge global stats" (async=ON) or
                         "use blocking merge global stats" (async=OFF) in
                         tidb.log — marks end of collection, start of the
                         load+merge global phase. Present on both branches.
  analyze_end .......... profile_result.json analyze_end_time

  collection_phase = global_stats_start - analyze_start
                     (part-full: scan+build all 8192 partitions;
                      part-single: collect the one re-analyzed partition)
  globalstats_phase = analyze_end - global_stats_start
                     (load all partitions from storage + merge)

On the PR only, diagnostic logs further split globalstats_phase:
  merge_start ...... first "MergePartTopNAndHistToGlobal start"
  merge_end ........ last  "MergePartTopNAndHistToGlobal step 1d"
  merge_proper = merge_end - merge_start        (the rewritten algorithm)
  load_lead    = merge_start - global_stats_start (load before first merge)

Because the load loop is identical between branches, BASE's globalstats_phase
minus a comparable load lead approximates BASE's merge-proper; we report
BASE globalstats_phase as the upper bound and note the PR's measured load
lead as the shared, unchanged component.

Usage: phase-timing.py <output-root>   (defaults to ./output-bench-v5)
"""
import sys, os, re, glob, json
from datetime import datetime

ROOT = sys.argv[1] if len(sys.argv) > 1 else "output-bench-v5"

TS_RE = re.compile(r'^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+\-]\d{2}:\d{2})\]')

def parse_log_ts(s):
    # "2026/07/16 01:43:40.161 +02:00" -> datetime
    return datetime.strptime(s, "%Y/%m/%d %H:%M:%S.%f %z")

def parse_json_ts(s):
    # "2026-07-16T01:29:16.13537003+02:00" -> datetime (trim ns to us)
    m = re.match(r'(.*\.\d{6})\d*([+\-]\d{2}:\d{2})$', s)
    if m:
        s = m.group(1) + m.group(2)
    return datetime.fromisoformat(s)

def first_last_ts(logpath, needle):
    first = last = None
    with open(logpath, errors="replace") as f:
        for line in f:
            if needle in line:
                m = TS_RE.match(line)
                if not m:
                    continue
                ts = parse_log_ts(m.group(1))
                if first is None:
                    first = ts
                last = ts
    return first, last

def fmt(secs):
    if secs is None:
        return "     n/a"
    m, s = divmod(secs, 60)
    h, m = divmod(int(m), 60)
    if h:
        return f"{h}h{m:02d}m{s:04.1f}s"
    if m:
        return f"{m}m{s:04.1f}s"
    return f"{s:.1f}s"

def analyze_cell(cell_dir):
    runs = sorted(glob.glob(os.path.join(cell_dir, "run_*")))
    runs = [r for r in runs if os.path.exists(os.path.join(r, "profile_result.json"))]
    if not runs:
        return None
    run = runs[-1]
    d = json.load(open(os.path.join(run, "profile_result.json")))
    a_start = parse_json_ts(d["analyze_start_time"])
    a_end = parse_json_ts(d["analyze_end_time"])
    total = (a_end - a_start).total_seconds()
    log = os.path.join(run, "tidb-logs", "tidb.log")
    gs_start = merge_first = merge_last = None
    if os.path.exists(log):
        for needle in ("use async merge global stats", "use blocking merge global stats"):
            f, _ = first_last_ts(log, needle)
            if f:
                gs_start = f
                break
        merge_first, merge_last = first_last_ts(log, "MergePartTopNAndHistToGlobal start")
        _, step1d_last = first_last_ts(log, "MergePartTopNAndHistToGlobal step 1d")
        if step1d_last:
            merge_last = step1d_last
    collection = (gs_start - a_start).total_seconds() if gs_start else None
    globalstats = (a_end - gs_start).total_seconds() if gs_start else None
    merge_proper = (merge_last - merge_first).total_seconds() if (merge_first and merge_last) else None
    load_lead = (merge_first - gs_start).total_seconds() if (merge_first and gs_start) else None
    return dict(total=total, collection=collection, globalstats=globalstats,
                merge_proper=merge_proper, load_lead=load_lead)

def main():
    scenarios = ["part-single", "part-full"]
    asyncs = ["ON", "OFF"]
    print(f"{'scenario':<12} {'async':<5} {'branch':<5} {'total':>10} "
          f"{'collect':>10} {'globalstats':>11} {'merge':>9} {'loadlead':>9}")
    print("-" * 74)
    agg = {}
    for scn in scenarios:
        for asy in asyncs:
            for br in ("BASE", "PR"):
                cell = os.path.join(ROOT, br, f"{scn}_existing_async{asy}_iter1")
                r = analyze_cell(cell)
                if not r:
                    print(f"{scn:<12} {asy:<5} {br:<5} {'(pending)':>10}")
                    continue
                agg[(scn, asy, br)] = r
                print(f"{scn:<12} {asy:<5} {br:<5} {fmt(r['total']):>10} "
                      f"{fmt(r['collection']):>10} {fmt(r['globalstats']):>11} "
                      f"{fmt(r['merge_proper']):>9} {fmt(r['load_lead']):>9}")
            # ratios if both present
            b = agg.get((scn, asy, "BASE"))
            p = agg.get((scn, asy, "PR"))
            if b and p:
                def ratio(x, y):
                    return f"{x/y:.2f}x" if (x and y) else "n/a"
                print(f"{'':<12} {'':<5} {'RATIO':<5} "
                      f"{ratio(b['total'], p['total']):>10} "
                      f"{ratio(b['collection'], p['collection']):>10} "
                      f"{ratio(b['globalstats'], p['globalstats']):>11} "
                      f"(merge-proper PR={fmt(p['merge_proper'])})")
            print()

if __name__ == "__main__":
    main()
