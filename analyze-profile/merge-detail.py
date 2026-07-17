#!/usr/bin/env python3
"""Merge-phase detail, combining two sources:

1. mysql.analyze_jobs (profile_result.json -> analyze_status_after):
   authoritative, STRUCTURED, and present on BOTH branches. The
   "merge global stats for <table> columns" job plus one
   "merge global stats for <table>'s index <idx>" job each carry
   start_time/end_time, so the global-merge phase decomposes into
   columns-merge + per-index-merge on BASE and PR alike. This is the
   cross-branch merge comparison (no dependency on PR-only logs).

2. tidb.log PR diagnostic lines (PR only): within the merge phase, each
   histID logs "MergePartTopNAndHistToGlobal start" -> "step 1a/1b/1c/1d".
   Merges run SEQUENTIALLY (start_i -> step1d_i -> start_{i+1}), so:
     merge-fn per column = start -> step1d   (the rewritten algorithm)
     load  per column     = step1d -> next start   (per-partition stats
                            read from storage; identical code on BASE)
   step1d also prints globalTopN=N, revealing the singleton filter's
   effect per column (0 on uniform high-NDV columns).

Usage: merge-detail.py <cell_dir>
  e.g. merge-detail.py output-bench-v5/PR/part-full_existing_asyncON_iter1
"""
import sys, os, re, glob, json
from datetime import datetime

TS = re.compile(r'^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+\-]\d{2}:\d{2})\]')
JOBFMT = "%Y-%m-%d %H:%M:%S"

# NOTE: histID does NOT map to spec column order (c1..c20) — TiDB assigns
# column IDs that interleave the clustered PK, so a by-position label is
# wrong (it put globalTopN=0 on low-NDV columns, which cannot have empty
# TopN). We therefore label each merged histogram by histID + logged type
# only. The authoritative per-column TopN / singleton-filter check comes
# from accuracy-diff (report.txt), which carries real column names.

def log_ts(s): return datetime.strptime(s, "%Y/%m/%d %H:%M:%S.%f %z")
def job_dur(a, b):
    return (datetime.strptime(b, JOBFMT) - datetime.strptime(a, JOBFMT)).total_seconds()
def fmt(s):
    if s is None: return "n/a"
    m, s = divmod(s, 60); h, m = divmod(int(m), 60)
    return f"{h}h{m:02d}m{s:04.1f}s" if h else (f"{m}m{s:04.1f}s" if m else f"{s:.2f}s")

def analyze_jobs_merge(prof):
    jobs = [j for j in prof.get("analyze_status_after", [])
            if j["job_info"].startswith("merge global")]
    out = []
    for j in sorted(jobs, key=lambda x: x["start_time"]):
        tag = "columns" if " columns" in j["job_info"] else \
              "index " + j["job_info"].split("index ")[-1]
        out.append((tag, job_dur(j["start_time"], j["end_time"])))
    return out

def pr_log_percolumn(logpath):
    events = []  # (kind, time, globalTopN)
    for l in open(logpath, errors="replace"):
        m = TS.match(l)
        if not m: continue
        t = log_ts(m.group(1))
        if "MergePartTopNAndHistToGlobal start" in l:
            h = re.search(r'histID=(\d+)', l); tp = re.search(r'tp=(\w+)', l)
            ix = re.search(r'isIndex=(\w+)', l)
            label = f"h{h.group(1) if h else '?'} {tp.group(1) if tp else '?'}" \
                    + (" [idx]" if (ix and ix.group(1) == 'true') else "")
            events.append(("start", t, label))
        elif "MergePartTopNAndHistToGlobal step 1d" in l:
            g = re.search(r'globalTopN=(\d+)', l)
            events.append(("1d", t, int(g.group(1)) if g else -1))
    # pair sequential start -> next 1d; load = 1d -> next start
    cols = []
    i = 0
    while i < len(events):
        if events[i][0] == "start":
            label = events[i][2]
            # find next 1d
            j = i + 1
            while j < len(events) and events[j][0] != "1d":
                j += 1
            if j < len(events):
                merge_fn = (events[j][1] - events[i][1]).total_seconds()
                gtn = events[j][2]
                # load lead to NEXT start
                k = j + 1
                load = None
                while k < len(events):
                    if events[k][0] == "start":
                        load = (events[k][1] - events[j][1]).total_seconds()
                        break
                    k += 1
                cols.append((label, merge_fn, gtn, load))
                i = j + 1
                continue
        i += 1
    return cols

def main():
    cell = sys.argv[1]
    runs = [r for r in sorted(glob.glob(os.path.join(cell, "run_*")))
            if os.path.exists(os.path.join(r, "profile_result.json"))]
    if not runs:
        print("no completed run in", cell); return
    run = runs[-1]
    prof = json.load(open(os.path.join(run, "profile_result.json")))
    is_pr = "/PR/" in cell or "\\PR\\" in cell

    print(f"# {cell}\n")
    print("## mysql.analyze_jobs — global-merge phase (both branches)")
    total = 0
    for tag, d in analyze_jobs_merge(prof):
        print(f"  {fmt(d):>10}  merge: {tag}")
        total += d
    print(f"  {fmt(total):>10}  merge TOTAL (sum of jobs)\n")

    log = os.path.join(run, "tidb-logs", "tidb.log")
    if is_pr and os.path.exists(log):
        cols = pr_log_percolumn(log)
        print("## PR diagnostic logs — per-histogram merge-fn / load / globalTopN")
        print("   (labelled histID+type; histID != spec column order — see note)")
        print(f"  {'hist':<14} {'merge_fn':>9} {'load_next':>10} {'gTopN':>6}")
        tot_merge = tot_load = 0.0
        for label, mf, gtn, load in cols:
            tot_merge += mf or 0
            if load: tot_load += load
            print(f"  {label:<14} {fmt(mf):>9} {fmt(load):>10} {gtn:>6}")
        print(f"\n  merge-fn sum: {fmt(tot_merge)}   load-gap sum: {fmt(tot_load)}")
        print("  (merge-fn = the PR's rewritten algorithm; load = per-partition")
        print("   stats read from storage, identical code on BASE)")

if __name__ == "__main__":
    main()
