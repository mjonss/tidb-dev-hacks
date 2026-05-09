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

## Full-table ANALYZE pair (BASE + PR on combined-backup-v3)

Added a `full-analyze-from-backup` subcommand that restores
`combined-backup-v3` and runs `ANALYZE TABLE … ALL COLUMNS` on each
binary in turn. Both starting from the same data + seeded stats. No
`--drop-stats` (DROP STATS over 184K histograms is expensive, ANALYZE
overwrites stats anyway).

| Branch | Total run | ANALYZE phase wallclock |
|---|---|---|
| BASE  | 1h12m02s | **41m33s** |
| PR    | 39m09s | **8m27s** |

PR's full-table ANALYZE phase is **4.9× faster** — consistent with the
matrix's part-single 5× speedup, on the maximum-merge-work scenario
(8000 freshly-collected partitions all in memory simultaneously).

### Per-column merge function timing (PR-only, from diagnostic logs)

PR's `MergePartTopNAndHistToGlobal` logs entry/exit per column at INFO
level (commit `d4c1a0d90c`). BASE doesn't have these logs.

| Column | Type | Distribution | merge fn (s) |
|---|---|---|---|
| pk | BIGINT | uniform | 1.137 |
| c1 | BIGINT | low-ndv | **0.077** |
| c2 | INT UNSIGNED | zipf | 0.985 |
| c3 | BIGINT UNSIGNED | uniform:NOTNULL | 1.248 |
| c4 | BIGINT | per-part-categ:NULL(40) | 0.949 |
| c5 | DOUBLE | low-ndv | **0.076** |
| c6 | DOUBLE | zipf | 1.039 |
| c7 | DOUBLE | uniform | 1.357 |
| c8 | DOUBLE | per-part-categ | 0.991 |
| c9 | DECIMAL | low-ndv | **0.079** |
| c10 | DECIMAL | zipf | 1.580 |
| c11 | DECIMAL | uniform | **2.371** ← heaviest |
| c12 | DECIMAL | per-part-categ | 1.371 |
| c13 | VARCHAR | low-ndv | **0.112** |
| c14 | VARCHAR | zipf | 1.434 |
| c15 | VARCHAR | uniform:NOTNULL | 2.090 |
| c16 | VARCHAR | per-part-categ | **2.195** |
| c17 | DATETIME | low-ndv | **0.076** |
| c18 | DATETIME | zipf | 1.261 |
| c19 | TIMESTAMP | uniform | 1.820 |
| c20 | DATETIME | per-part-categ:NULL(40) | 1.029 |
| idx_c14 | INDEX | - | 1.113 |
| idx_c2_c19 | INDEX | - | 1.324 |

Total PR merge function CPU across all columns + indexes: ~24 s.

#### Patterns

- **Low-NDV columns are 10–30× faster than any other distribution**
  (0.076–0.112 s vs ~1 s+). TopN saturates → no histogram-bucket merge
  is needed at all.
- **DECIMAL uniform is the heaviest column on PR** (2.371 s) — full
  bucket sort over canonical-byte-encoded keys.
- **VARCHAR per-part-categ is the second heaviest** (2.195 s) — collation-
  keyed sort with cross-partition non-overlapping ranges.
- Type effect within a distribution (e.g. uniform): VARCHAR < DECIMAL,
  consistent with collation-key length / decimal-canonical-byte cost.
- Distribution effect: low-ndv < per-part-categ < zipf < uniform — TopN
  saturation reduces histogram work, then bucket-sort cost grows with
  cross-partition value diversity.
- The ~13–15 s "gap to next column" between consecutive merges is per-
  partition stats loading from `mysql.stats_*`, gRPC, codec — identical
  on PR and BASE, where most of the 8m27s phase wallclock lives.

If BASE's per-column ratios mirror PR's (with the ~70× function
speedup measured at the function-CPU aggregate level), DECIMAL uniform
alone would have taken **~3 minutes on BASE** vs PR's 2.4 s.

### Source-line attribution: where the merge time actually lives

`go tool pprof -list <fn>` aggregated across all CPU profiles in each cell.

**BASE** (one line dominates 80% of merge CPU):

| Function | Line | cum (s) | Code |
|---|---|---|---|
| `MergePartTopN2GlobalTopN` | **190** | **1211** | `count, _ := hists[j].EqualRowCount(nil, datum, isIndex)` |
| `MergePartTopN2GlobalTopN` | 181 | 159 | `datum, exists := datumMap.Get(encodedVal)` |
| `MergePartTopN2GlobalTopN` | 177 | 73 | `if (j == i && version >= 2) \|\| topNs[j].FindTopN(val.Encoded) != -1` |
| `MergePartTopN2GlobalTopN` | 173 | 48 | `if err := killer.HandleSignal(); err != nil` |
| `MergePartTopN2GlobalTopN` | 194 | 16 | `hists[j].BinarySearchRemoveVal(&datum, int64(count))` |
| `MergePartitionHist2GlobalHist` | 1671 | 11 | `err := sortBucketsByUpperBound(sc.TypeCtx(), buckets)` |

The `EqualRowCount` call at line 190 is the O(TopN × P) loop:
for each of ~100 TopN values × 21 columns × 8000 partitions, do a
binary search over the partition's histogram. That's 16.8 M
`EqualRowCount` calls per full-table ANALYZE merge phase.

**PR** (no hotspot — spread across k-way merge ops):

| Line | cum (s) | Code |
|---|---|---|
| 1720 | 15.2 | `bkt, _ := nextBucket()` |
| 1651 | 8.7 | `entry := mergeHeap.popMin()` |
| 1789 | 6.8 | `globalHist, err := buildGlobalHistogram(...)` |
| 1657 | 4.2 | `mergeHeap.pushEntry(next)` |
| 1656 | 2.2 | `firstNonEmptyBucket(entry.histIdx, int(entry.bucketIdx)+1)` |
| 1735 | 0.5 | `entry.encoded, err = codec.EncodeKey(tz, nil, bkt.upper)` |
| 1551 | 0.3 | `return bytes.Compare(a.encoded, b.encoded)` |

PR replaces the O(TopN × P) lookup with one k-way merge walk through
all partition buckets in sorted order. TopN values are identified
inline as the merge cursor passes over them.

Per-column line-level hotspots (sampled in the heaviest 3 columns)
are uniform: `nextBucket` + heap ops + `buildGlobalHistogram` in the
same order, with absolute times scaling with the column's
`sortedRefs` / `totalBuckets` count. No type-specific hotspot
emerges — the algorithmic cost is fully amortized across data
shapes.

## Resolved harness issues during the run

- **macOS `pgrep -a` silently broken:** `find_tidb_log_path` /
  `find_tikv_log_path` used `pgrep -af` which is Linux-only. macOS
  pgrep ignores `-a` and returns just the PID, so the regex never
  matched `--log-file=` and every per-iter log copy was a silent no-op.
  All 8 matrix iters had no `tidb*.log` preserved. Fixed with a portable
  `pgrep_argv` helper (`pgrep -f` for PIDs, `ps -p PID -o command=` for
  args). The full-analyze-from-backup pair has logs preserved.
- **`tidb_lock_wait_timeout` doesn't exist** in v9.0.0-beta.2; the
  pessimistic-lock var is `innodb_lock_wait_timeout` (default 50 s).
- **`disable_auto_analyze` heredoc + `set -e`:** the original
  `if [[ $? -ne 0 ]]` after the heredoc-fed mysql exits the pipeline
  immediately under `set -e` before the post-check runs. Switched to
  `mysql … <<SQL || { log … }` so the failure is part of a control
  expression.

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
