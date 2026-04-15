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


def load_stats_dump(run_dir: str) -> dict | None:
    """Load stats_dump.json — authoritative on-disk stats, unaffected by TiDB
    in-memory cache eviction (which makes SHOW STATS_HISTOGRAMS return partial
    results on non-partitioned tables)."""
    p = os.path.join(run_dir, "stats_dump.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def from_stats_dump(run_dir: str) -> dict | None:
    """Build an accuracy-shaped dict from stats_dump.json. Mirrors the shape
    produced by accuracy.go so downstream code treats both sources identically.
    Returns None if the dump isn't present."""
    dump = load_stats_dump(run_dir)
    if not dump:
        return None

    def extract(entries: dict) -> list[dict]:
        out = []
        for name, c in (entries or {}).items():
            hist = c.get("histogram") or {}
            buckets_raw = hist.get("buckets") or []
            # stats_dump's buckets are {count, repeats, lower_bound, upper_bound, ndv}
            buckets = []
            for i, b in enumerate(buckets_raw):
                buckets.append({
                    "bucket_id": i,
                    "count": b.get("count", 0),
                    "repeats": b.get("repeats", 0),
                    "lower_bound": str(b.get("lower_bound", "")),
                    "upper_bound": str(b.get("upper_bound", "")),
                    "ndv": b.get("ndv", 0),
                })
            cm = c.get("cm_sketch") or {}
            topn_raw = cm.get("top_n") or []
            topn = [{"value": str(t.get("data", "")), "count": t.get("count", 0)}
                    for t in topn_raw]
            out.append({
                "name": name,
                "ndv": hist.get("ndv", 0),
                "null_count": c.get("null_count", 0),
                "avg_col_size": hist.get("tot_col_size", 0.0),
                "correlation": hist.get("correlation", 0.0),
                "buckets": buckets,
                "topn": topn,
            })
        return out

    columns = extract(dump.get("columns") or {})
    indexes = extract(dump.get("indices") or {})
    return {
        # Row count still comes from the profile_result.json accuracy field.
        "row_count": {"stats_count": dump.get("count", 0),
                      "actual_count": dump.get("count", 0),
                      "ratio": 1.0},
        "column_stats": columns,
        "index_stats": indexes,
    }


def load(run_dir: str) -> dict | None:
    # Prefer stats_dump.json (authoritative, never evicted). Fall back to the
    # accuracy block from profile_result.json only when the dump is missing.
    dump = from_stats_dump(run_dir)
    if dump:
        # Row count comparison should use stats_meta vs actual count from the
        # profile_result.json accuracy block if available.
        p = os.path.join(run_dir, "profile_result.json")
        if os.path.exists(p):
            with open(p) as f:
                acc = json.load(f).get("accuracy") or {}
            if acc.get("row_count"):
                dump["row_count"] = acc["row_count"]
        return dump

    p = os.path.join(run_dir, "profile_result.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        data = json.load(f)
    return data.get("accuracy")


# KS distance only needs a total order on bucket bounds. ISO dates/datetimes
# already lex-sort chronologically, so string ordering is fine for them.
# Only numeric strings need coercion ("10" < "9" lex would be wrong).
# Parse ladder: int → float → string. The first successful rank for the
# first bucket fixes the column's type; if ranks disagree between branches
# we fall back to string comparison.


def _decode_bound(s):
    """Base64-decode a stats_dump bound. Falls through to the raw string if
    decoding fails (handles both stats_dump.json and SHOW output)."""
    import base64
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    try:
        return base64.b64decode(s, validate=True).decode("utf-8", "replace").strip()
    except Exception:
        return s


def _parse_ordered_bound(s, rank=None):
    """Parse a decoded bound at a specific rank, or try int → float → string.
    Returns (rank, sort_key), or (None, None) on empty input."""
    decoded = _decode_bound(s)
    if decoded == "":
        return (None, None)
    if rank in (None, "int"):
        try:
            return ("int", int(decoded))
        except ValueError:
            if rank == "int":
                return (None, None)
    if rank in (None, "float"):
        try:
            return ("float", float(decoded))
        except ValueError:
            if rank == "float":
                return (None, None)
    return ("string", decoded)


def cdf_from_buckets(buckets):
    """Build (rank, [(key, cum_frac), ...]) from a histogram. The first
    bucket's parse fixes the rank; remaining buckets reuse it."""
    if not buckets:
        return (None, None)
    last_count = buckets[-1].get("count", 0)
    if last_count <= 0:
        return (None, None)
    first_rank, _ = _parse_ordered_bound(buckets[0].get("upper_bound"))
    if first_rank is None:
        return (None, None)
    points = []
    for b in buckets:
        _, key = _parse_ordered_bound(b.get("upper_bound"), rank=first_rank)
        if key is None:
            # Shouldn't happen for well-formed columns, but fall back
            # to string ordering to stay useful.
            return _cdf_as_strings(buckets, last_count)
        points.append((key, b.get("count", 0) / last_count))
    points.sort(key=lambda p: p[0])
    return (first_rank, points)


def _cdf_as_strings(buckets, last_count):
    points = [(_decode_bound(b.get("upper_bound")),
               b.get("count", 0) / last_count) for b in buckets]
    points.sort(key=lambda p: p[0])
    return ("string", points)


def ks_distance(a, b):
    """Kolmogorov-Smirnov distance: max |F_A(x) - F_B(x)|. Takes
    (rank, points) tuples from cdf_from_buckets. If ranks disagree, both
    sides are recomputed lexicographically."""
    rank_a, pts_a = a
    rank_b, pts_b = b
    if not pts_a or not pts_b:
        return 0.0

    if rank_a != rank_b:
        pts_a = sorted(((str(k), v) for k, v in pts_a), key=lambda p: p[0])
        pts_b = sorted(((str(k), v) for k, v in pts_b), key=lambda p: p[0])

    xs = sorted({p[0] for p in pts_a} | {p[0] for p in pts_b})

    def cdf_at(points, x):
        if x < points[0][0]:
            return 0.0
        if x >= points[-1][0]:
            return points[-1][1]
        lo, hi = 0, len(points) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if points[mid][0] <= x:
                lo = mid
            else:
                hi = mid - 1
        return points[lo][1]

    return max(abs(cdf_at(pts_a, x) - cdf_at(pts_b, x)) for x in xs)


def histogram_jaccard(a, b):
    """Jaccard similarity on bucket upper-bound strings (works for any type,
    numeric or not). 1 = same bucket boundaries, 0 = disjoint."""
    sa = {str(x.get("upper_bound", "")) for x in (a or [])}
    sb = {str(x.get("upper_bound", "")) for x in (b or [])}
    union = sa | sb
    if not union:
        return 1.0
    return len(sa & sb) / len(union)


def _norm_topn_value(v):
    """TopN values come base64-encoded in stats_dump.json. Decode when
    possible so two branches agree on the same value even if one source
    decoded already. Non-decodable strings are kept as-is."""
    import base64
    s = str(v or "")
    try:
        return base64.b64decode(s, validate=True).decode("utf-8", "replace")
    except Exception:
        return s


def topn_compare(a, b):
    """Compare two TopN arrays. Returns {jaccard, common, only_a, only_b,
    max_rel_count_diff}. max_rel_count_diff is the worst relative count
    disagreement over values present in both arrays."""
    map_a = {_norm_topn_value(t.get("value", "")): t.get("count", 0) for t in (a or [])}
    map_b = {_norm_topn_value(t.get("value", "")): t.get("count", 0) for t in (b or [])}
    sa, sb = set(map_a), set(map_b)
    common = sa & sb
    union = sa | sb
    max_rel = 0.0
    for v in common:
        ca, cb = map_a[v], map_b[v]
        denom = max(ca, cb)
        if denom > 0:
            rel = abs(ca - cb) / denom
            if rel > max_rel:
                max_rel = rel
    return {
        "jaccard": len(common) / len(union) if union else 1.0,
        "common": len(common),
        "only_a": len(sa - sb),
        "only_b": len(sb - sa),
        "max_rel_count_diff": max_rel,
    }


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
        # Raw buckets + topn, used by the distribution comparator. Folded
        # across runs by picking the first run's arrays (they're large;
        # averaging bucket boundaries would be meaningless).
        "_buckets": buckets,
        "_topn": topn,
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
            if k.startswith("_"):
                # Raw arrays: keep the first run's — averaging buckets/topn
                # across runs doesn't make sense.
                out[k] = vals[0]
            elif isinstance(vals[0], (int, float)):
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

            # Histogram distribution similarity. KS distance is meaningful
            # only for numerically-bounded columns; Jaccard on bucket bounds
            # works for any type. Skip the row entirely when there are no
            # buckets on either side.
            dist_rows = []
            for name in both:
                x, y = ea[name], eb[name]
                bx = x.get("_buckets") or []
                by = y.get("_buckets") or []
                if not bx and not by:
                    continue
                cx = cdf_from_buckets(bx)   # (rank, points) or (None, None)
                cy = cdf_from_buckets(by)
                ks = ks_distance(cx, cy) if (cx[0] and cy[0]) else None
                jac = histogram_jaccard(bx, by)
                dist_rows.append((name, ks, jac, len(bx), len(by)))
            if dist_rows:
                print(f"\n  {title} histogram distribution (0=identical):")
                print(f"    {'name':<16} {'KS dist':>9} {'bndsJac':>8} "
                      f"{label_a+' bkts':>10} {label_b+' bkts':>10}")
                dist_rows.sort(key=lambda r: -((r[1] or 0.0) + (1 - r[2])))
                for name, ks, jac, na, nb in dist_rows:
                    ks_s = f"{ks:.4f}" if ks is not None else "n/a"
                    print(f"    {name:<16} {ks_s:>9} {jac:>8.4f} "
                          f"{na:>10d} {nb:>10d}")

            # TopN distribution similarity.
            topn_rows = []
            for name in both:
                x, y = ea[name], eb[name]
                tx = x.get("_topn") or []
                ty = y.get("_topn") or []
                if not tx and not ty:
                    continue
                c = topn_compare(tx, ty)
                topn_rows.append((name, c, len(tx), len(ty)))
            if topn_rows:
                print(f"\n  {title} TopN distribution:")
                print(f"    {'name':<16} {'jaccard':>8} {'common':>6} "
                      f"{'only '+label_a:>10} {'only '+label_b:>10} "
                      f"{'maxΔcnt':>8}")
                topn_rows.sort(key=lambda r: r[1]["jaccard"])
                for name, c, na, nb in topn_rows:
                    print(f"    {name:<16} {c['jaccard']:>8.4f} "
                          f"{c['common']:>6d} {c['only_a']:>10d} "
                          f"{c['only_b']:>10d} "
                          f"{c['max_rel_count_diff']:>8.4f}")

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
