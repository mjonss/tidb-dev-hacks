# Benchmark Findings v4: improve-global-stats-3 PR

Date: 2026-05-25
PR: `improve-global-stats-3` HEAD `a9d5f61a7a` vs BASE = its merge-base on
master `749c973e5a` (PR's parent).
Hardware: Linux x86, 16 cores, 60 GB RAM (host `minisum`).
Data: freshly generated — **8192 HASH partitions, 40M rows, 20 columns
(5 type classes × 4 distributions) + 2 indexes** (c14; c2,c19).
Both binaries rebuilt from source; combined backup seeded with PR-produced
per-partition stats (BR full + stats, 25 GB / 14.28 GB compressed).

Matrix: {part-single, part-full} × existing-stats × {async ON, OFF} × 2 iters
× {BASE, PR} = 16 cells. (Wider than v3, which was part-single only.)

## Headline result

| scenario | async | BASE dur | PR dur | speedup | BASE RSS | PR RSS |
|---|---|---|---|---|---|---|
| **part-full** | ON  | 2h15m | **24m50s** | **5.47×** | 8.67 GB | 5.72 GB |
| **part-full** | OFF | 2h53m | **35m28s** | **4.90×** | 24.74 GB | 29.82 GB |
| part-single | ON  | 4m50s | 10m13s | 0.47× (PR slower) | 5.79 GB | 4.25 GB |
| part-single | OFF | 19m46s | 19m03s | 1.04× (parity) | 11.04 GB | 29.09 GB |

On the full-table ANALYZE (max-merge-work case the PR targets), **PR is
~5× faster** at 8192 partitions / 40M rows — consistent with v3's 5× at
8000/30M, now confirmed at larger scale and on different hardware.

## The merge phase is constant on PR; that explains everything

Measured from PR's per-histogram diagnostic logs
(`MergePartTopNAndHistToGlobal start` → last `step 1d`):

| cell | PR merge-phase wall |
|---|---|
| PR part-full async=ON  | 6m23s (00:37:51 → 00:44:14) |
| PR part-single async=ON | 9m26s (23:05:24 → 23:14:50) |

PR's global merge always rebuilds from all 8192 partition stats, so it costs
~6–9 min **regardless of how many partitions were re-collected**. The total
ANALYZE time = per-partition collection + this ~constant merge.

- **part-full:** collect all 8192 (~18 min) + merge (~6 min) = ~25 min.
  BASE here pays the full O(TopN×P) merge ≈ **~2 h**, so PR wins ~5×.
- **part-single:** collect only p5 (~30 s) + merge (~9 min) = ~10 min.
  **BASE part-single is only 4m50s** — i.e. BASE does *not* do a full
  8192-partition merge for a single-partition ANALYZE; it does a lighter
  incremental global update. PR does the full rebuild, so PR is ~2× slower
  here.

**Open question for the PR author:** is PR doing a redundant full global
rebuild on single-partition ANALYZE where BASE did an incremental update?
If the incremental path is still correct, PR may be leaving an optimization
on the table for the `ANALYZE … PARTITION p` case. Accuracy is identical
either way (below), so this is a cost question, not a correctness one.

## Memory: async=OFF regression on PR

async=OFF holds all partition stats in memory before merging. PR's working
set is **larger** than BASE there (29–30 GB vs 11–25 GB). On async=ON PR uses
*less* than BASE (4.25–5.72 vs 5.79–8.67 GB). The PR optimizes the async=ON
merge working set but not the async=OFF load path — and appears to inflate it.
Worth profiling the async=OFF heap on PR (heap snapshots are captured per cell).

## Accuracy — no cardinality regression

From `accuracy-diff.py` (all 4 scenario sections):

- `row_count` ratio PR/BASE = **1.0000** in every cell.
- Per-column **NDV ratio = 1.0000 and ΔnullCount = +0 for all 20 columns +
  pk + both indexes** — identical cardinalities.
- TopN: PR adds entries on high-cardinality columns (c3 +100, c7 +100,
  c15 +43, c16 +75, pk +100) — richer, as in v3.

**One structural difference:** the per-part-categ columns **c4, c12, c20**
(values cluster within partitions) get far fewer histogram buckets on PR
(5–71 vs BASE's 256; KS distance 0.73–0.98, bucket CV +2 to +6). NDV is
unchanged, but PR's merged histogram shape for that distribution diverges
markedly from BASE. Worth checking whether PR's k-way merge collapses
buckets for cross-partition non-overlapping categorical ranges.

## Harness notes (this run)

- **Overnight kill #1:** matrix launched with `nohup … & disown` died on
  Claude session teardown — `nohup` blocks SIGHUP, not the SIGTERM the
  harness sends to its child process group. 4/16 cells done at that point.
- **Kill #2:** relaunched under `systemd-run --user` but `Linger=no`, so the
  user manager stopped the service on a login-session change. Fixed with
  `loginctl enable-linger`.
- Final stable launch: `systemd-run --user` + linger + an idempotent
  `resume-v4.sh` that skips already-complete (branch, scenario, async)
  groups and rebuilds `manifest.tsv` from disk before comparing.
- Per-cell BR restore overhead ≈ 3.5 min (25 GB backup). Each cell runs in a
  fresh playground; data dirs cleaned on clean shutdown (one 30 GB orphan
  from kill #1 was reclaimed manually).

## Re-run on force-pushed PR (2026-05-25)

PR branch force-pushed `a9d5f61a7a` → `63d8b20af9` (merge-base unchanged at
`749c973e5a`, so BASE results + combined-backup-v4 reused as-is). Rebuilt
`tidb-server.pr`, re-ran all 8 PR cells against the same backup.

**Result: timings identical within noise — the new commit changed nothing
measurable.**

| scenario | async | BASE | new PR | speedup | old PR |
|---|---|---|---|---|---|
| part-full | ON  | 2h15m | 24m53s | 5.46× | 24m50s |
| part-full | OFF | 2h53m | 35m46s | 4.86× | 35m28s |
| part-single | ON  | 4m50s | 10m12s | 0.48× | 10m13s |
| part-single | OFF | 19m46s | 18m37s | 1.06× | 19m03s |

In particular **part-single async=ON is still 10m12s** — the full
8192-partition rebuild behavior persists in `63d8b20af9`. Old PR cells
archived under `output-bench-v4/_archive_PR_old_20260525_100456/`.

## Investigation: the part-single full-rebuild path

**Question:** why is PR's `ANALYZE … PARTITION p5` (10m12s) ~2× slower than
BASE's (4m50s), when PR's full-table merge is ~5× *faster*?

### Confirmed facts (from logs + slow log)

| | global-merge phase wall | ANALYZE stmt |
|---|---|---|
| BASE part-single | `use async merge` 21:55:45 → last stats 21:59:45 = **~4m** | 4m52s (slow-log Query_time 292.8s) |
| BASE part-full   | 12:43:02 → 14:47:31 = **~2h04m** | 2h13–18m |
| PR part-single   | merge pipeline **~9m26s** (load+merge overlap) | 10m12s |
| PR part-full     | merge pipeline **~6m23s** | 24m50s |

- The executor trigger is **identical** between branches: `needGlobalStats =
  (pruneMode == Dynamic)` (analyze.go:317), `handleGlobalStats` builds one
  global-merge entry per table (analyze_global_stats.go — **zero diff**
  BASE→PR). Both branches do a full-table global merge on single-partition
  analyze; PR logs confirm `partitions=8192, skipped=0, missing=0`.
- The async path (`global_stats_async.go`) loads **all** partitions from
  storage (`for _, partitionID := range a.partitionIDs`) — loop unchanged
  between branches. Only the merge algorithm changed: BASE's
  `mergeGlobalStatsTopN` + `MergePartitionHist2GlobalHist` →
  PR's combined `MergePartTopNAndHistToGlobal`.

### The mechanism: bucket count tracks (input source × algorithm)

Per-part-categ columns (c4, c12, c20 — values cluster within a partition,
non-overlapping across partitions), final global histogram bucket counts:

| col | BASE part-single | PR part-single | BASE part-full | PR part-full |
|---|---|---|---|---|
| c20 | 5   | 256 | 256 | 5   |
| c12 | 12  | 256 | 256 | 12  |
| c4  | 71  | 256 | 256 | 71  |

NDV ratio = 1.0000 in all cases — **cardinality always correct; only
histogram shape differs.** The merge cost is dominated by these heavy
columns, and cost scales with the bucket work, so the bucket-count pattern
### ROOT CAUSE (pinned via histogram.go + diagnostic logs)

Corrected bucket attribution (each report group prints PR-row then BASE-row;
the earlier draft had PR/BASE swapped):

| col | PR part-single | PR part-full | BASE part-single | BASE part-full |
|---|---|---|---|---|
| c12 | 256 | 256 | **12** | 256 |
| c20 | 256 | 256 | **5**  | 256 |
| c4  | 256 | 256 | **71** | 256 |

**PR is fully consistent (256 buckets in both scenarios); BASE is the
outlier (degenerate 5–71 buckets on part-single).**

Decisive evidence — PR's per-column merge-input logs are **byte-identical**
between part-single and part-full (e.g. c11 `totalBuckets=2007035`,
`topNEntries=0` in both; every column matches). Reason: ANALYZE is
deterministic on the fixed seed-42 data, so the per-partition stats a fresh
analyze writes are identical to the seed-backup stats the merge reloads.
The async merge always reloads all partitions from storage in both
scenarios, so PR sees identical input → identical output (256 buckets) and
identical merge work (~2M input buckets/column) → identical merge cost.

How `MergePartTopNAndHistToGlobal` builds buckets (`histogram.go:1493`,
`buildGlobalHistogram:1966`): global TopN = the `numTopN`(=100) most frequent
values by `totalRepeat`; their bucket-uppers are absorbed (count→0,
`histogram.go:2059`, skipped at `:2109`); the rest is equal-frequency
bucketed to `expBucketNumber`(=256) (`:2138`). The merge **walks every
partition bucket** (`sortedRefs`, ~2M/column) regardless of final bucket
count, so cost is O(total input buckets), not O(output buckets).

**Conclusions:**

1. **PR's merge cost is the same for part-single and part-full** (~6–9 min;
   the 9.5-vs-6.4 gap is I/O variance, not algorithmic). The part-single
   *total* (10m) vs part-full *total* (25m) gap is entirely the per-partition
   **collection** phase (1 vs 8192 partitions), not the merge.

2. **BASE part-single was anomalously fast (4m) because BASE's old merge
   produces degenerate histograms** (5–71 buckets, bucket-CV 2–6 = wildly
   uneven) for the clustered-categorical columns on that path — it does less
   work and yields lower-quality stats. BASE part-full does the full
   256-bucket merge (2h). So "PR 2× slower on part-single" is misleading:
   PR delivers full-quality, consistent histograms; BASE part-single was
   fast by under-merging. NDV is 1.0000 throughout, so it's a histogram-
   *quality* gap, not a cardinality bug — and PR is the better one.

3. **The genuine optimization opportunity** (shared by both branches): any
   single-partition `ANALYZE … PARTITION p` in dynamic prune mode triggers a
   full 8192-partition reload+merge (`analyze.go:317 needGlobalStats =
   Dynamic`; async path loads all partitions). Only `p` changed, so an
   incremental merge (reuse the existing global histogram, fold in `p`) would
   cut the single-partition cost without losing PR's full-merge quality.

## Raw artifact paths

- `output-bench-v4/manifest.tsv` — 16 cells
- `output-bench-v4/report.txt` — compare-runs + profile-diff + accuracy-diff
  for all 4 scenario×async groups
- `output-bench-v4/PR/seed-full-analyze/run_*` — PR no-prior-stats baseline
  (seed ANALYZE, 31m30s)
- `combined-backup-v4/` (25 GB) — BR full backup with stats
- `bench-config-v4.sh`, `bench-v4-smoke.sh`, `resume-v4.sh` — drivers
- v3 results for comparison: `FINDINGS-v3.md`
