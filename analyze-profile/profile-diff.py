#!/usr/bin/env python3
"""
Compare pprof profiles (CPU or heap) across groups of runs.

For each run dir, picks the largest / latest profile, runs `go tool pprof -top`,
parses the top functions, averages their flat/cum percentages within each group,
then shows:
  - top-K functions per group
  - top movers: functions whose flat% changed most between groups

Usage:
  profile-diff.py cpu  --group PR dirA dirB --group BASE dirC dirD
  profile-diff.py heap --group PR dirA      --group BASE dirC

Notes:
  - For CPU profiles, picks the cpu_profile_<N>.pb.gz with the largest N.
  - For heap profiles, picks the final heap_<N>.pb.gz (post-ANALYZE).
  - Parses `go tool pprof -top -cum=false -nodecount=30` output.
"""

import argparse
import glob
import os
import re
import statistics
import subprocess
import sys


PPROF_LINE = re.compile(
    r"^\s*([\-\d\.]+)\s*\S+\s+([\d\.]+)%\s+([\d\.]+)%\s+\S+\s+([\d\.]+)%\s+(.*)$"
)


def pick_profile(run_dir: str, kind: str) -> str | None:
    if kind == "cpu":
        prefix = "cpu_profile_"
    elif kind == "heap":
        prefix = "heap_"
    else:
        raise ValueError(kind)

    files = glob.glob(os.path.join(run_dir, f"{prefix}*.pb.gz"))
    if not files:
        return None

    def idx(p):
        m = re.search(rf"{prefix}(\d+)\.pb\.gz$", p)
        return int(m.group(1)) if m else -1

    files.sort(key=idx)
    return files[-1]  # highest index = latest


def pprof_top(path: str, nodecount: int = 30) -> list[tuple[str, float, float]]:
    """Return [(symbol, flat_pct, cum_pct)] sorted by flat desc."""
    cmd = ["go", "tool", "pprof", "-top", "-nodecount", str(nodecount), path]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True)
    except subprocess.CalledProcessError:
        return []
    rows = []
    for line in out.splitlines():
        m = PPROF_LINE.match(line)
        if not m:
            continue
        flat_pct = float(m.group(2))
        cum_pct = float(m.group(4))
        sym = m.group(5).strip()
        rows.append((sym, flat_pct, cum_pct))
    return rows


def aggregate(group_rows: list[list[tuple[str, float, float]]]) -> dict[str, dict]:
    """Average flat% and cum% per symbol across runs."""
    acc: dict[str, dict] = {}
    for run_rows in group_rows:
        seen = set()
        for sym, flat, cum in run_rows:
            if sym in seen:
                continue
            seen.add(sym)
            d = acc.setdefault(sym, {"flat": [], "cum": []})
            d["flat"].append(flat)
            d["cum"].append(cum)
    # For symbols missing from some runs, pad with 0 so averages reflect
    # real presence.
    n_runs = len(group_rows)
    out = {}
    for sym, d in acc.items():
        flats = d["flat"] + [0.0] * (n_runs - len(d["flat"]))
        cums = d["cum"] + [0.0] * (n_runs - len(d["cum"]))
        out[sym] = {
            "flat_mean": statistics.mean(flats),
            "cum_mean": statistics.mean(cums),
            "runs_seen": len(d["flat"]),
        }
    return out


def print_top(label: str, agg: dict[str, dict], n: int = 15):
    print(f"\n  {label} — top {n} by flat%:")
    print(f"  {'flat%':>6} {'cum%':>6} {'runs':>5}  function")
    items = sorted(agg.items(), key=lambda kv: -kv[1]["flat_mean"])
    for sym, d in items[:n]:
        print(f"  {d['flat_mean']:>6.2f} {d['cum_mean']:>6.2f} {d['runs_seen']:>5}  {sym}")


def print_diff(label_a: str, label_b: str, agg_a: dict, agg_b: dict, n: int = 20):
    symbols = set(agg_a) | set(agg_b)
    rows = []
    for s in symbols:
        a = agg_a.get(s, {"flat_mean": 0.0, "cum_mean": 0.0})
        b = agg_b.get(s, {"flat_mean": 0.0, "cum_mean": 0.0})
        rows.append((s, a["flat_mean"], b["flat_mean"], a["flat_mean"] - b["flat_mean"]))
    rows.sort(key=lambda r: -abs(r[3]))
    print(f"\n  Top movers ({label_a} flat% - {label_b} flat%):")
    print(f"  {label_a+' flat%':>12} {label_b+' flat%':>12} {'Δ':>8}  function")
    for sym, a, b, d in rows[:n]:
        print(f"  {a:>12.2f} {b:>12.2f} {d:>+8.2f}  {sym}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["cpu", "heap"])
    ap.add_argument("--group", action="append", nargs="+", metavar="LABEL DIR [DIR...]",
                    required=True)
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    groups = []
    for g in args.group:
        if len(g) < 2:
            sys.exit("--group needs LABEL and at least one DIR")
        label, dirs = g[0], g[1:]
        groups.append((label, dirs))

    if len(groups) < 2:
        sys.exit("need at least two --group arguments")

    aggs = {}
    for label, dirs in groups:
        rows = []
        for d in dirs:
            p = pick_profile(d, args.kind)
            if p is None:
                print(f"  (warn: no {args.kind} profile in {d})", file=sys.stderr)
                continue
            rows.append(pprof_top(p))
        aggs[label] = aggregate(rows)
        print_top(label, aggs[label], n=args.top)

    # Diff first two groups (typically PR vs BASE).
    labels = [g[0] for g in groups]
    print_diff(labels[0], labels[1], aggs[labels[0]], aggs[labels[1]])


if __name__ == "__main__":
    main()
