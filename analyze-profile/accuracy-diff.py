#!/usr/bin/env python3
"""
Compare stats accuracy (row count, NDV, null count, TopN, histogram buckets)
between groups of runs.

Reads `accuracy` from each run's profile_result.json (produced by
analyze-profile with --check-accuracy).

Reports per group:
  - stats_meta row count vs actual (ratio)
  - NDV per column (averaged across runs if >1)
  - null count per column
  - bucket count per column, histogram CV (coefficient of variation),
    repeats distribution
  - TopN length and HEX-encoded first-value prefix

Then shows PR vs BASE deltas.

Usage:
  accuracy-diff.py --group PR dirA dirB --group BASE dirC dirD
"""

import argparse
import json
import os
import statistics
import sys


def load(run_dir: str) -> dict | None:
    p = os.path.join(run_dir, "profile_result.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        data = json.load(f)
    return data.get("accuracy")


def cv(values: list[int]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    if mean == 0:
        return 0.0
    return statistics.pstdev(values) / mean


def summarize_entry(e: dict) -> dict:
    buckets = e.get("buckets") or []
    bucket_counts = [b.get("count", 0) for b in buckets]
    # Convert cumulative counts to per-bucket counts.
    per_bucket = []
    prev = 0
    for c in bucket_counts:
        per_bucket.append(c - prev)
        prev = c
    repeats = [b.get("repeats", 0) for b in buckets]
    topn = e.get("topn") or []
    return {
        "ndv": e.get("ndv", 0),
        "null_count": e.get("null_count", 0),
        "avg_col_size": e.get("avg_col_size", 0.0),
        "correlation": e.get("correlation", 0.0),
        "n_buckets": len(buckets),
        "bucket_cv": cv(per_bucket) if per_bucket else 0.0,
        "bucket_mean": statistics.mean(per_bucket) if per_bucket else 0,
        "bucket_min": min(per_bucket) if per_bucket else 0,
        "bucket_max": max(per_bucket) if per_bucket else 0,
        "unique_repeats": len(set(repeats)) if repeats else 0,
        "max_repeats": max(repeats) if repeats else 0,
        "n_topn": len(topn),
        "topn_top_value": (topn[0].get("value", "") if topn else "")[:30],
        "topn_top_count": topn[0].get("count", 0) if topn else 0,
    }


def fold_runs(runs: list[dict]) -> dict:
    """Fold N run accuracy dicts into per-column averages."""
    # Row count: average stats/actual and ratio.
    rc_stats = [r["row_count"]["stats_count"] for r in runs if r]
    rc_actual = [r["row_count"]["actual_count"] for r in runs if r]
    rc_ratio = [r["row_count"]["ratio"] for r in runs if r]

    # Per-column / per-index: build name → list of per-run summaries.
    per_col: dict[str, list[dict]] = {}
    per_idx: dict[str, list[dict]] = {}

    for r in runs:
        if not r:
            continue
        for s in r.get("column_stats") or []:
            per_col.setdefault(s["name"], []).append(summarize_entry(s))
        for s in r.get("index_stats") or []:
            per_idx.setdefault(s["name"], []).append(summarize_entry(s))

    def avg_dicts(dicts: list[dict]) -> dict:
        out = {}
        keys = dicts[0].keys() if dicts else []
        for k in keys:
            vals = [d[k] for d in dicts]
            if isinstance(vals[0], (int, float)):
                out[k] = statistics.mean(vals)
            else:
                out[k] = vals[0]
        out["_runs"] = len(dicts)
        return out

    return {
        "row_count_stats": statistics.mean(rc_stats) if rc_stats else 0,
        "row_count_actual": statistics.mean(rc_actual) if rc_actual else 0,
        "row_count_ratio": statistics.mean(rc_ratio) if rc_ratio else 0,
        "columns": {n: avg_dicts(v) for n, v in per_col.items()},
        "indexes": {n: avg_dicts(v) for n, v in per_idx.items()},
    }


def print_group(label: str, g: dict):
    print(f"\n  [{label}] rows: stats={g['row_count_stats']:.0f} "
          f"actual={g['row_count_actual']:.0f} ratio={g['row_count_ratio']:.4f}")

    def print_section(title: str, entries: dict):
        if not entries:
            return
        print(f"  [{label}] {title}:")
        print(f"    {'name':<16} {'NDV':>12} {'nulls':>10} {'bkts':>5} "
              f"{'bktCV':>6} {'topN':>5} {'maxRep':>7}")
        for name, e in sorted(entries.items()):
            print(f"    {name:<16} {e['ndv']:>12.0f} {e['null_count']:>10.0f} "
                  f"{e['n_buckets']:>5.0f} {e['bucket_cv']:>6.3f} "
                  f"{e['n_topn']:>5.0f} {e['max_repeats']:>7.0f}")

    print_section("columns", g["columns"])
    print_section("indexes", g["indexes"])


def print_diff(label_a: str, label_b: str, a: dict, b: dict):
    print(f"\n  === {label_a} vs {label_b} delta ===")
    print(f"  row_count: {label_a}_ratio={a['row_count_ratio']:.4f} "
          f"{label_b}_ratio={b['row_count_ratio']:.4f} "
          f"Δ={a['row_count_ratio']-b['row_count_ratio']:+.4f}")

    def diff_section(title: str, ea: dict, eb: dict):
        names_a = set(ea)
        names_b = set(eb)
        both = sorted(names_a & names_b)
        only_a = sorted(names_a - names_b)
        only_b = sorted(names_b - names_a)
        if not (both or only_a or only_b):
            return

        if both:
            # Accuracy diff — only for columns present in both groups, so
            # values reflect genuine stats drift rather than coverage gaps.
            print(f"\n  {title} accuracy (Δ = {label_a} - {label_b}, "
                  f"columns analyzed by both):")
            print(f"    {'name':<16} {'ΔNDV':>12} {'NDV ratio':>10} "
                  f"{'ΔnullCnt':>10} {'Δbkts':>6} {'ΔbktCV':>7} "
                  f"{'ΔtopN':>6}")
            rows = []
            for name in both:
                x, y = ea[name], eb[name]
                ratio = (x["ndv"] / y["ndv"]) if y["ndv"] else float("nan")
                rows.append((
                    name,
                    x["ndv"] - y["ndv"],
                    ratio,
                    x["null_count"] - y["null_count"],
                    x["n_buckets"] - y["n_buckets"],
                    x["bucket_cv"] - y["bucket_cv"],
                    x["n_topn"] - y["n_topn"],
                ))
            rows.sort(key=lambda r: -abs(r[1]))
            for name, dn, nr, dnc, db_, dcv, dtn in rows:
                nr_s = f"{nr:.4f}" if nr == nr else "n/a"
                print(f"    {name:<16} {dn:>+12.0f} {nr_s:>10} "
                      f"{dnc:>+10.0f} {db_:>+6.0f} {dcv:>+7.3f} "
                      f"{dtn:>+6.0f}")

        if only_a or only_b:
            # Coverage gap — not an accuracy problem; one branch just
            # didn't analyze these columns at all.
            print(f"\n  {title} coverage gap:")
            if only_a:
                print(f"    only in {label_a}: {', '.join(only_a)}")
            if only_b:
                print(f"    only in {label_b}: {', '.join(only_b)}")

    diff_section("columns", a["columns"], b["columns"])
    diff_section("indexes", a["indexes"], b["indexes"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", action="append", nargs="+", required=True)
    args = ap.parse_args()

    groups = []
    for g in args.group:
        if len(g) < 2:
            sys.exit("--group needs LABEL DIR [DIR...]")
        label, dirs = g[0], g[1:]
        runs = [load(d) for d in dirs]
        runs = [r for r in runs if r]
        if not runs:
            print(f"  (warn: group {label} has no accuracy data)", file=sys.stderr)
            continue
        groups.append((label, fold_runs(runs)))

    for label, g in groups:
        print_group(label, g)

    if len(groups) >= 2:
        print_diff(groups[0][0], groups[1][0], groups[0][1], groups[1][1])


if __name__ == "__main__":
    main()
