# Benchmark Findings: improve-global-stats-3 PR

Date: 2026-05-07
PR: `improve-global-stats-3` (HEAD `d4c1a0d90c`) vs BASE master at PR's parent (`33ae9e3cb5`)
Hardware: macOS M3 Max
Data: freshly generated, 8000 HASH partitions, 30M rows, 21 columns, 2 indexes

## Headline result

**The PR's global-stats merge rewrite is dramatically faster and uses dramatically less memory than BASE on the part-single existing-stats scenario, with no accuracy regression.**

| Cell | BASE (avg of 2 iters) | PR (avg of 2 iters) | PR speedup |
|---|---|---|---|
| part-single existing async=ON  | **40m26s** wall, 6.77 GB RSS, 6.41 GB heap | **8m05s** wall, 3.03 GB RSS, 2.52 GB heap | **5.0× wall, 0.45× RSS, 0.39× heap** |
| part-single existing async=OFF | **1h01m36s** wall, 26.6 GB RSS, 24.9 GB heap | **15m18s** wall, 26.3 GB RSS, 24.6 GB heap | **4.0× wall, parity RSS / heap** |

Memory savings only manifest on async=ON because async=OFF holds all
partition stats in memory before merging — the PR optimizes the merge
working set, not the load phase.

## Per-function CPU (the algorithmic gap, isolated)

Cumulative CPU time spent inside the merge functions across all 28 CPU
profiles per cell (`MergePartitionHist2GlobalHist` + `MergePartTopN2GlobalTopN`
on BASE vs `MergePartTopNAndHistToGlobal` on PR):

| Cell | BASE merge fn CPU | PR merge fn CPU | **Function speedup** |
|---|---|---|---|
| async=ON iter1  | 1443 s (24 min) | 39 s | **37×** |
| async=ON iter2  | 1528 s (25 min) | 37 s | **42×** |
| async=OFF iter1 | 2072 s (35 min) | 41 s | **51×** |
| async=OFF iter2 | 2085 s (35 min) | 32 s | **65×** |

The wall-clock speedup (5×) is smaller than the function speedup (37–65×)
because the run wall-clock includes other phases identical between branches
(BR restore, partition data scan for p5, async pipeline overhead, codec
decoding, gRPC roundtrips for `mysql.stats_*` reads).

## Accuracy — no regression, slight improvement

Per the `compare-runs.py` / `accuracy-diff.py` output:

| Metric | Result |
|---|---|
| row_count ratio (PR vs BASE) | 1.0000 |
| Per-column NDV ratio (all 21 columns + pk) | **1.0000 exactly** |
| Per-column null_count delta | **+0 for all columns** |
| Histogram bucket count | PR has +143 to +229 more buckets on some columns (denser) |
| Bucket coefficient of variation (lower = more uniform) | PR is **−0.110 to −0.316** vs BASE (better quality) |
| TopN entries | PR adds +65 to +100 more on c3, c7, c15, c16, pk (richer) |
| Index stats accuracy | NDV identical, idx_c2_c19 has +81 TopN entries on PR |

PR produces *strictly more informative* stats (richer TopN on
high-cardinality columns, more uniform bucket sizes) at the same NDV/null
counts.

## How the test was structured

Each iter:
1. `playground_reload BIN` — stop/start tiup playground with the iter's binary.
2. `disable_auto_analyze` — `SET GLOBAL tidb_enable_auto_analyze = OFF` and
   `SET GLOBAL innodb_lock_wait_timeout = 600`.
3. `br restore full` from `combined-backup-v3` — restores data + stats
   (see "Critical bug" below).
4. `warmup` — `SELECT COUNT(*) FROM t_partitioned`.
5. `run_one_profile` — `analyze-profile profile --partition p5 --check-accuracy`,
   which runs `ANALYZE TABLE … PARTITION p5 ALL COLUMNS` while collecting
   heap/CPU/mutex/goroutine snapshots and `mysql.analyze_jobs` polling.

`SCENARIOS_FILTER=part-single`, `STATES_FILTER=existing` — restricted to
the only scenario that meaningfully exercises cross-partition merge with
existing-stats inputs (the scenario customers hit in production).

In dynamic prune mode, `ANALYZE TABLE … PARTITION p5` re-collects p5's stats
and re-merges global stats from all 8000 partition-level stats — which is
exactly the merge code path under test.

## Critical bug found and fixed: BR was silently dropping stats

The first matrix run (overnight, attempt 1) showed PR ≈ BASE parity at
all levels. Investigation revealed:

- `br backup db --db analyze_profile --ignore-stats=false` writes JSON
  schema.stats sidecar files into the backup but does **not** include the
  `mysql.stats_*` system tables themselves.
- `br restore full --load-stats=true` (default) on such a backup creates
  *empty placeholder* rows in `mysql.stats_meta` and `mysql.stats_histograms`
  (Row_count=0, distinct_count=0, Last_analyze_time=NULL) without
  populating `mysql.stats_buckets` / `mysql.stats_top_n` at all.

Confirmed by post-restore queries:

```
mysql.stats_meta:        8004 rows (all Row_count=0)
mysql.stats_histograms:  184036 rows (all distinct_count=0)
mysql.stats_buckets:     0 rows  ← payload missing
mysql.stats_top_n:       0 rows  ← payload missing
```

So every "existing-stats" iter was actually merging p5's freshly-collected
stats with 7999 *empty* partition stats. The merge function did near-zero
work, and PR vs BASE looked identical. Both branches' merge function CPU
came in at ~1 s and global stats came out as 3750 rows (just p5).

**Fix:** switch from `br backup db --db X --ignore-stats=false` to
`br backup full --ignore-stats=false`. `backup full` includes the actual
`mysql.stats_*` tables as ordinary user data; `restore full` writes them
back. Verified with a small (10-partition / 1M-row) round-trip:

```
After br backup full --ignore-stats=false → br restore full:
mysql.stats_meta:        11 rows, table=116 count=1000000, p0..p9 count=100000
mysql.stats_histograms:  253 rows
mysql.stats_buckets:     48276 rows  ← payload preserved
mysql.stats_top_n:       16958 rows  ← payload preserved
```

The corrected backup at scale is 11.54 GB compressed (vs 6.7 GB for the
broken `backup db` version) — the difference is the actual stats payload.

## Side issues fixed during the run

- `tidb_lock_wait_timeout` doesn't exist in v9.0.0-beta.2. The
  pessimistic-lock wait timeout is `innodb_lock_wait_timeout` (default
  50 s). Used the right name now.
- An earlier `flush_stats_delta_with_retry` helper was added to avoid
  ANALYZE TABLE's preflight `FLUSH STATS_DELTA` lock-timing-out against
  the background stats updater after a 30M-row bulk INSERT. With the
  raised global `innodb_lock_wait_timeout=600` it became redundant and
  was removed.
- bench.sh `disable_auto_analyze` heredoc-fed mysql command had a
  `set -e`-incompatible structure that exited the pipeline immediately
  after "playground ready". Switched to `mysql … <<SQL || { log … }` so
  the failure becomes part of a control expression.

## Seed-full-analyze (PR full ANALYZE on freshly-generated data)

Captured during prepare phase as the "no-prior-stats" baseline for PR:

| Metric | Value |
|---|---|
| Duration | 41m9s |
| Per-partition phase wallclock | 8m21s (avg 18.8 s/partition, max 7m15s) |
| Peak RSS | 8.55 GB |
| Peak heap alloc | 5.50 GB |
| Stats produced | row_count=30M; e.g. c11 NDV=9.55M, c15 NDV=29.6M, all NDVs in expected ranges |

Output: `output-bench-v3/PR/seed-full-analyze/run_20260507_142206/`.

## Open work

- **Per-column timing breakdown by (type, distribution):** the bench
  harness's per-iter TiDB log copy step didn't preserve any `tidb*.log`
  files (none in any run dir). To split the 24-35 min BASE merge CPU and
  the 30-40 s PR merge CPU across the 21 columns, we need the TiDB diagnostic
  logs that PR commit `d4c1a0d90c` added. Plan: fix the harness's log copy,
  then re-run a single PR async=ON cell and a single BASE async=ON cell
  to capture per-column timings.
- **BASE seed-full-analyze for direct PR-vs-BASE comparison on the
  full-table path:** only PR's seed run was captured during prepare.
  A matched BASE run is needed to compare on the full-table merge path,
  not just the partial-merge path.

## Raw artifact paths

- `output-bench-v3/manifest.tsv` — 8 cells × per-cell run dirs
- `output-bench-v3/PR/seed-full-analyze/run_20260507_142206/` — PR's
  no-prior-stats baseline
- `output-bench-v3/report.txt` — `compare-runs.py` + `profile-diff.py` +
  `accuracy-diff.py` output for both async=ON and async=OFF
- `combined-backup-v3/` (16 GB) — usable BR backup with stats
- `combined-backup-v3.broken-loadstats-20260507_125941/` — kept for
  reference, the `backup db --ignore-stats=false` artifact whose stats
  are present in schema.stats files but not restorable into the
  persistent tables
- Older v2 results: `../FINDINGS.md` (memory-focused, on different
  hardware, partly different workload)
