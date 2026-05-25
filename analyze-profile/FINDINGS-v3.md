# Benchmark Findings: improve-global-stats-3 PR (force-pushed)

Last update: 2026-05-25 (rerun with chunking fix applied).

| | Value |
|---|---|
| PR | `improve-global-stats-3` (origin HEAD `63d8b20af9`, with the chunking fix squashed into `643d74138b` "statistics: add MergePartTopNAndHistToGlobal combined merge") |
| BASE | PR's parent on master: `e53c50c1c5` |
| Hardware | macOS M3 Max, 64 GB |
| Data | 8000 HASH partitions, 30M rows, 21 columns (20 generated + clustered PK), 2 indexes |
| Build | Both binaries built from the same worktree `/Users/mattias/repos/worktrees/improve-global-stats-3` on the same day; AC power throughout the bench |

The branch was force-pushed twice during the investigation. Prior PR
HEAD `d4c1a0d90c` (results in `output-bench-v3/PR.rev1-d4c1a0d/`),
the battery-tainted intermediate rerun (in
`output-bench-v3/PR.rev2-battery-a9d5f61a7a/`), and the pre-fix clean
rerun (in `output-bench-v3/PR.rev3-before-chunking-fix/`) are
preserved for reference. The current report.txt and headline numbers
are from the post-fix rerun on 2026-05-25.

## Headline result

**The PR's new combined-merge algorithm delivers a ~5× wall-clock
and ~40% memory reduction on the realistic single-partition reanalyze
scenario (async=ON), and a 4× wall-clock speedup on async=OFF.** With
the chunking fix applied, accuracy on per-partition-categorical
columns matches BASE within KS distance ≤ 0.004 (i.e. essentially
identical).

| Cell | BASE (avg of 2 iters) | PR (avg of 2 iters) | PR vs BASE |
|---|---|---|---|
| part-single existing async=ON  | **39m47s** wall, 6.19 GB RSS, 5.54 GB heap | **8m03s** wall, 4.00 GB RSS, 3.27 GB heap | **4.95× wall, 0.65× RSS, 0.59× heap** |
| part-single existing async=OFF | **1h03m35s** wall, 22.62 GB RSS, 21.83 GB heap | **15m01s** wall, 23.21 GB RSS, 23.06 GB heap | **4.24× wall, parity RSS / heap** |
| part-full existing async=ON (iter0) | **1h11m14s** wall, 7.90 GB RSS, 7.38 GB heap | **38m22s** wall, 4.60 GB RSS, 3.51 GB heap | **1.86× wall, 0.58× RSS, 0.48× heap** |

Memory savings only manifest on async=ON because async=OFF holds all
8000 partition stats in memory before merging — the PR optimizes the
merge working set, not the load phase. The part-full speedup is
smaller because the per-partition collection phase (identical between
branches) dominates the wall-clock when all 8000 partitions are
freshly collected.

## Per-function CPU (the algorithmic gap, isolated)

Top movers in profile-diff between PR and BASE for the async=ON cell.
BASE's hot functions (the O(TopN × P) inner loop pattern) disappear
on PR; PR's profile is dominated by k-way merge bookkeeping
(`bucketMergeHeap.down/less`, `buildGlobalHistogram`) plus runtime
overhead.

**async=ON BASE→PR CPU mover summary:**

| Function | BASE flat% | PR flat% | Δ |
|---|---|---|---|
| `slices.partitionCmpFunc[[]uint8]` | 5.62 | 0.00 | -5.62 |
| `chunk.Compare` | 2.56 | 0.00 | -2.56 |
| `commonPrefixLength` | 2.19 | 0.00 | -2.19 |
| `Datum.compareString` | 3.69 | 1.30 | -2.39 |
| `Histogram.BinarySearchRemoveVal` | 1.51 | 0.00 | -1.51 |
| `MergePartitionHist2GlobalHist` | 1.49 | 0.00 | -1.49 |
| `sortBucketsByUpperBound.func1` | 1.29 | 0.00 | -1.29 |
| `bucketMergeHeap.down` (new) | 0.00 | 2.79 | +2.79 |
| `bucketMergeHeap.less` (new) | 0.00 | 1.46 | +1.46 |
| `buildGlobalHistogram` (new) | 0.00 | 1.06 | +1.06 |

The "BASE-only" functions are the per-TopN-value linear-search-over-
partition-histograms pattern (`BinarySearchRemoveVal`, `LocateBucket`,
`chunk.Compare` on every comparison). The PR replaces this with a
k-way merge driven by a heap of bucket cursors, so the
`bucketMergeHeap.*` ops appear and the linear search vanishes.

## Accuracy regression — per-part-categ columns (fixed)

The unmodified PR produced dramatically fewer histogram buckets on
per-partition-categorical columns. With the chunking fix below, all
four per-part-categ columns produce the full 256 buckets and match
BASE within KS distance ≤ 0.004.

| Column | Type | Distribution | BASE bkts | PR bkts (unfixed) | PR bkts (fixed) | KS dist (fixed) |
|---|---|---|---|---|---|---|
| c12 | DECIMAL | per-part-categ | 256 | 6 | **256** | 0.004 |
| c20 | DATETIME | per-part-categ:NULL(40) | 256 | 9 | **256** | 0.004 |
| c4 | BIGINT | per-part-categ:NULL(40) | 256 | 67 | **256** | 0.000 |
| c8 | DOUBLE | per-part-categ | 256 | 256 | 256 | 0.000 |

Pre-fix `c8` escaped because its specific row counts kept the virtual
histogram size just under the `MaxUint16` overflow threshold; the
other three crossed it.

All other measurements are clean:
- row_count ratio: 1.0000 across PR and BASE
- Per-column NDV: identical for all 21 columns + pk
- Per-column null_count: identical for all columns
- TopN entries: jaccard ≈ 1.0 on uniform/zipf/low-ndv columns; on
  the per-part-categ columns there is still partial TopN
  divergence (jaccard 0.77–0.85 on c12/c20), which is independent
  of the histogram chunking fix.

### Root cause

**`uint16` overflow on the virtual-histogram `bucketIdx`** in
`pkg/statistics/histogram.go`. The merge builds a "virtual
histogram" out of partition-TopN entries that didn't match any
partition bucket upper and didn't make global TopN. With per-part-
categ data this virtual histogram has *one bucket per non-matched
partition-TopN entry* (potentially millions for the bench's
8000-partition setup).

Lines 1898 and 1906 build references into this virtual histogram:

```go
unifiedRefs = append(unifiedRefs,
    bucketRef{histIdx: virtualIdx, bucketIdx: uint16(vi)})
```

When `vi >= 65536`, the `uint16` cast silently wraps. Pass 2 then
indexes the virtual histogram with the wrapped index, reading mass
and bucket-bound values from the **wrong** bucket. Both mass mis-
attribution and apparent row loss follow.

Bisecting the trigger threshold confirms an exact uint16 boundary:

| Virtual hist size | Output buckets | Mass ratio | Row loss |
|---|---|---|---|
| 65,300 | 200 (clean) | 1.3 | 0 |
| 65,500 | 200 (clean) | 1.3 | 0 |
| **65,900** | **3 (broken)** | **2,759** | 0 |
| 99,900 | 90 (broken) | 3,933 | 0 |
| 199,900 | 6 (broken) | 8,598 | 130,470 rows |

DOUBLE per-part-categ (`c8`) escapes the bug because — *with the
specific row counts the bench produces* — its per-partition TopN
saturation pattern keeps the post-filter virtual-histogram size
below the 65,535 threshold. The other three column types push past
it.

### Fix (now in origin)

Split the virtual histogram into chunks of `math.MaxUint16` buckets
each, so `bucketIdx` (`uint16`) never overflows. `finalAllTopN` is
already encoded-key sorted, so chunks carry non-overlapping
upper-bound ranges and can be addressed as `(chunkIdx,
withinChunk)`. Two files touched, both squashed into the existing
combined-merge commit `643d74138b` on `origin/improve-global-stats-3`:

- `pkg/statistics/histogram.go::MergePartTopNAndHistToGlobal` —
  chunk construction + two-pointer merge that emits virtual refs
  via `bucketRef{histIdx: virtualHistIdxs[vi/chunkSize], bucketIdx:
  uint16(vi % chunkSize)}`.
- `pkg/statistics/merge_global_cases_test.go` —
  `TestMergePartTopNAndHistToGlobalVirtualHistChunking` using a
  realistic 8192-partition × 256-bucket × 100-TopN per-partition-
  categorical layout (virtual hist ≈ 819,100 entries → ~13 chunks).
  Asserts total row count is preserved AND mass distribution is
  near-uniform (max/min < 10). Runs in ~1.4 s.

Existing `TestMergePartTopNAndHistToGlobal` cases and the
`pkg/statistics/handle/globalstats` package tests still pass.

## Per-column merge function timing (PR-only, from diagnostic logs)

Extracted from `output-bench-v3/PR/part-full_existing_asyncON_iter0/run_*/tidb-logs/tidb.log`
by computing the elapsed time between `MergePartTopNAndHistToGlobal start`
and `MergePartTopNAndHistToGlobal step 1d` log entries for each histID.

| Column | Type | Distribution | merge fn (s) |
|---|---|---|---|
| pk | BIGINT | uniform | 1.117 |
| c1 | BIGINT | low-ndv | **0.095** |
| c2 | INT UNSIGNED | zipf | 0.972 |
| c3 | BIGINT UNSIGNED | uniform:NOTNULL | 1.293 |
| c4 | BIGINT | per-part-categ:NULL(40) | 0.924 |
| c5 | DOUBLE | low-ndv | **0.088** |
| c6 | DOUBLE | zipf | 1.044 |
| c7 | DOUBLE | uniform | 1.361 |
| c8 | DOUBLE | per-part-categ | 0.986 |
| c9 | DECIMAL | low-ndv | **0.095** |
| c10 | DECIMAL | zipf | 1.649 |
| c11 | DECIMAL | uniform | **2.451** ← heaviest |
| c12 | DECIMAL | per-part-categ | 1.389 |
| c13 | VARCHAR | low-ndv | **0.124** |
| c14 | VARCHAR | zipf | 1.452 |
| c15 | VARCHAR | uniform:NOTNULL | 2.094 |
| c16 | VARCHAR | per-part-categ | **2.263** |
| c17 | DATETIME | low-ndv | **0.090** |
| c18 | DATETIME | zipf | 1.243 |
| c19 | TIMESTAMP | uniform | 1.876 |
| c20 | DATETIME | per-part-categ:NULL(40) | 1.031 |
| idx_c14 | INDEX | - | 1.376 |
| idx_c2_c19 | INDEX | - | 1.396 |

Total PR merge function CPU across all columns + indexes: **~25.4 s**
across the full ANALYZE TABLE on 8000 partitions.

### Patterns (unchanged from prior PR HEAD)

- **Low-NDV columns are 10–30× faster than any other distribution**
  (0.08–0.16 s vs ~1 s+). TopN saturates → no histogram-bucket merge
  is needed at all.
- **DECIMAL uniform (c11) is the heaviest column** at 2.443 s — full
  bucket sort over canonical-byte-encoded keys.
- **VARCHAR per-part-categ (c16) is second heaviest** at 2.201 s.
- The ~13–14 s "gap to next column" between consecutive merges is
  per-partition stats loading from `mysql.stats_*`, gRPC, codec —
  identical on PR and BASE, where most of the part-full ANALYZE
  wallclock lives.

## How the test was structured

Each iter:
1. `playground_reload BIN` — stop/start tiup playground with the
   iter's binary.
2. `disable_auto_analyze` — `SET GLOBAL tidb_enable_auto_analyze = OFF`
   and `SET GLOBAL innodb_lock_wait_timeout = 600`.
3. `br restore full` from `combined-backup-v3` — restores data + stats
   (the backup is seeded with per-partition stats so each iter starts
   from a "stats already exist" cluster; the per-partition stats are
   algorithm-independent, only the global merge differs between PR
   and BASE).
4. `warmup` — `SELECT COUNT(*) FROM t_partitioned`.
5. `run_one_profile` — `analyze-profile profile --partition p5
   --check-accuracy` (or `--all` for part-full), which runs the
   ANALYZE while collecting heap/CPU/mutex/goroutine snapshots and
   polling `mysql.analyze_jobs`.

For the part-single matrix, `SCENARIOS_FILTER=part-single`,
`STATES_FILTER=existing`. In dynamic prune mode, `ANALYZE TABLE …
PARTITION p5` re-collects p5's stats and re-merges global stats from
all 8000 partition-level stats — which is exactly the merge code path
under test.

## Methodology notes

### Battery vs AC

A prior attempt at this rerun (results preserved in `*.rev2-battery/`)
was partly on battery power. macOS throttles peak CPU frequency under
battery, so the prior PR full-analyze per-partition collection phase
took ~30 min vs ~7 min on AC — that result was discarded.

The current report (2026-05-24) was run end-to-end on AC. Sleep
prevention (`powerd PreventUserIdleSystemSleep`) was active
throughout; `pmset -g log` shows zero sleep events during any of the
8 matrix iters or the two part-full iters.

### BR restore + GC safepoint

One run during the battery attempt failed with `cannot pass gc safe
point check` after the laptop entered Clamshell Sleep while the
restore was in progress. The fix on retry was simply to ensure the
machine stayed awake. Backup TS for `combined-backup-v3` is from
2026-05-07 — at 17+ days old it is close to the playground's default
GC safepoint TTL ceiling, so a sleep during restore is no longer
recoverable. If reusing this backup further into the future, consider
regenerating it.

### BASE moved between attempts

The prior BASE results (May 7, in `output-bench-v3/BASE.rev1-may7-…/`)
were against master `33ae9e3cb5`. The new BASE here is the *current*
PR's parent commit, `e53c50c1c5`, which is master ~1 week newer.
Master-side changes between those two BASEs include ~50 commits
(none touch global-stats merge code per inspection), so the PR-vs-BASE
delta numbers are not directly comparable between the May 7 results
and the 2026-05-24 results.

## Test-data distributions (column spec)

5 encoding-path equivalence classes × 4 informative distributions:

| Columns | Type | Distributions (in order) |
|---|---|---|
| c1–c4 | Signed/unsigned int | low-ndv, zipf, uniform:NOTNULL, per-part-categ:NULL(40) |
| c5–c8 | IEEE float | low-ndv, zipf, uniform, per-part-categ |
| c9–c12 | Canonical decimal | low-ndv, zipf, uniform, per-part-categ |
| c13–c16 | Collation key (VARCHAR) | low-ndv, zipf, uniform:NOTNULL, per-part-categ |
| c17–c20 | Packed time | low-ndv, zipf, uniform (TIMESTAMP), per-part-categ:NULL(40) |

Indexes:
- `idx_c14` — single column on `c14` (VARCHAR:zipf).
- `idx_c2_c19` — `(c2, c19)` (INT UNSIGNED, TIMESTAMP), heterogeneous
  concat encoding exercising the unsigned codec inside an index key.

## Raw artifact paths

- `output-bench-v3/manifest.tsv` — 10 cells (8 matrix + 2 part-full)
- `output-bench-v3/BASE/`, `output-bench-v3/PR/` — current run dirs
- `output-bench-v3/report.txt` — `compare-runs.py` + `profile-diff.py`
  + `accuracy-diff.py` output for both async=ON and async=OFF
- `combined-backup-v3/` (16 GB) — usable BR backup with stats
- `output-bench-v3/BASE.rev1-may7-33ae9e3cb5/` — May 7 BASE runs
  against master `33ae9e3cb5`
- `output-bench-v3/PR.rev1-d4c1a0d/` — first PR HEAD `d4c1a0d90c`
- `output-bench-v3/PR.rev2-battery-a9d5f61a7a/` — second PR HEAD
  results, partly on battery (discarded)
- `output-bench-v3/manifest.tsv.rev1`, `.rev2-battery`,
  `.before-pr-rerun` — successive manifest snapshots
- `output-bench-v3/report.txt.rev2-battery` — battery-tainted compare
- `analyze-profile/tidb-server.pr` — current PR binary (stamped `a9d5f61a7a-dirty`; the dirty suffix reflects the uncommitted chunking fix that has since been squashed upstream into `643d74138b` on origin)
- `analyze-profile/tidb-server.master` — current BASE binary (e53c50c1c5)
- `analyze-profile/tidb-server.pr.old-d4c1a0d` — first PR binary
- `analyze-profile/tidb-server.pr.rev2-battery-a9d5f61a7a` — second PR
  binary (same hash as current, kept for archival)
- `analyze-profile/tidb-server.master.may-33ae9e3cb5` — old BASE binary
