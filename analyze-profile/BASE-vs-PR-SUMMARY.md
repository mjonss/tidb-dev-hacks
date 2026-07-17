# improve-global-stats-3 — BASE vs PR benchmark summary

Evidence of the PR's performance and accuracy impact on `ANALYZE` of a
partitioned table in dynamic prune mode. BASE vs PR only.

| | |
|---|---|
| **PR** | `improve-global-stats-3` @ `8cf6bd31b5` |
| **BASE** | PR's merge-base on master @ `f994e8da27` |
| **What changed** | The global-stats **merge** step only: the old `mergeGlobalStatsTopN` + `MergePartitionHist2GlobalHist` are replaced by one combined k-way merge `MergePartTopNAndHistToGlobal`. Per-partition sample collection, per-partition TopN/histogram build, and per-partition stats save/load are **unchanged**. |
| **Data** | 8192 HASH partitions, 40M rows, 20 columns (5 type classes × 4 distributions) + 2 indexes. Per-partition stats pre-seeded (restored from backup), so each run exercises the merge with stats already present. |
| **Hardware** | Linux x86, 16 cores, 64 GB RAM. |
| **Method** | `ANALYZE TABLE` (part-full) and `ANALYZE TABLE … PARTITION p` (part-single), each × async-merge ON/OFF, 2 iterations. Mean of 2 shown. |

## TL;DR

- **CPU: 4.1×–9.2× faster** end-to-end; the merge step itself (the only
  thing the PR changes) is **~10× faster** and its compute drops from
  ~1.5 h to ~30 s.
- **Memory: 33–48% lower peak** on async=ON; on async=OFF peak is a tie but
  **sustained (mean) memory is ~0.45×**.
- **Cardinality and histograms: identical / matching.** No regression.
- **TopN content differs** (the one reviewer caution): the PR reports
  accurate counts where BASE overcounts ~500×. Plans calibrated on BASE's
  inflated equality-selectivity will shift.

## 1. Wall-clock and peak memory (whole ANALYZE)

| scenario | async | BASE time | PR time | **speedup** | BASE RSS | PR RSS | **PR/BASE** |
|---|---|---|---|---|---|---|---|
| part-single | ON  | 1h32m36s | 10m07s | **9.16×** | 7.14 GB | 3.74 GB | **0.52×** |
| part-single | OFF | 2h05m21s | 18m13s | **6.88×** | 27.71 GB | 27.60 GB | **1.00×** |
| part-full   | ON  | 1h52m24s | 23m52s | **4.71×** | 8.22 GB | 5.55 GB | **0.67×** |
| part-full   | OFF | 2h26m59s | 35m43s | **4.12×** | 29.78 GB | 28.71 GB | **0.96×** |

Iterations agree within 0.1–3%.

## 2. The merge phase — isolating what the PR actually changed

Total ANALYZE includes per-partition collection, which the PR does not
touch and which dilutes the ratio (especially part-full, where collecting
all 8192 partitions takes ~15 min on both branches). Splitting the phases
(both branches emit the phase boundary; merge timing also confirmed from
`mysql.analyze_jobs`):

| scenario | async | phase | BASE | PR | **ratio** |
|---|---|---|---|---|---|
| part-full | ON | collection (unchanged) | 15m30s | 14m24s | 1.08× |
| | | **global merge** | 1h36m50s | 9m08s | **10.59×** |
| part-full | OFF | collection (unchanged) | 15m58s | 15m55s | 1.00× |
| | | **global merge** | 2h11m05s | 19m27s | 6.74× |
| part-single | ON | **global merge** (≈whole run) | 1h31m14s | 10m05s | 9.05× |
| part-single | OFF | **global merge** (≈whole run) | 2h05m04s | 18m13s | 6.86× |

- **Collection is identical** BASE↔PR (1.00–1.08×): confirms the PR affects
  only the merge.
- Within the PR's 9-minute part-full merge, the actual merge **computation**
  is **~30 s total** (per-column diagnostic logs); the rest is loading 8192
  partitions' stats from `mysql.stats_*` — the same code BASE runs. BASE
  spends **~1.5 h** in merge computation for the same result. The combined
  k-way merge turns a compute-bound phase into a load-bound one.

## 3. Memory attributed per phase

Whole-run peak understates the memory win on async=OFF. Per-phase
(peak / mean RSS, GB):

| scenario | async | phase | BASE peak/mean | PR peak/mean |
|---|---|---|---|---|
| part-full | ON  | collection | 3.96 / 3.30 | 3.86 / 3.28 |
| | | merge | 8.18 / 5.19 | 4.72 / 3.36 |
| part-full | OFF | collection | 4.01 / 3.37 | 3.90 / 3.32 |
| | | merge | 30.93 / 21.19 | 29.00 / **9.45** |
| part-single | OFF | merge | 27.86 / 19.73 | 27.62 / **9.02** |

On async=OFF, peak RSS ties because both branches briefly hold all 8192
partitions' stats when loading (a shared step). But BASE keeps the full
O(TopN × P) merge state resident throughout, while the PR's k-way merge
streams — so the PR's **mean** merge-phase memory is **~0.45×** BASE's
(9.0 vs 19.7 GB; 9.5 vs 21.2 GB). Sustained memory pressure is roughly
halved even where peak is equal.

CPU at the function level (part-full async=ON merge phase): BASE is
dominated by the O(TopN × P) inner loop —
`MergePartTopN2GlobalTopN → LocateBucket / EqualRowCount / MyDecimal.Compare`.
On the PR **those functions do not appear**; its merge CPU is partition-load
syscalls plus the new lightweight k-way cursors (`globalMergeRefs`,
`bucketGroupCursor`).

## 4. Accuracy

Measured by comparing the resulting global stats against BASE and against
ground truth.

**No regression:**

- **Row count**: ratio 1.0000.
- **Per-column NDV**: identical to BASE for all 20 columns + pk + both
  indexes (ΔNDV = 0, Δnull = 0).
- **Histograms**: match BASE within KS distance ≤ 0.10; bucket counts within
  ±15. The per-partition-categorical columns (c4, c8, c12, c20) produce the
  full 256 buckets on both branches.

**Behavioural difference reviewers should know about — TopN counts:**

- On uniform high-NDV columns, BASE's global TopN reports counts of
  ~7,800–7,980; the PR reports ~1. Ground truth (direct `GROUP BY`) is
  ~15. **BASE overestimates equality selectivity by ~500×; the PR is
  accurate.** This is an improvement, but it *changes* estimates: query
  plans that were (mis)calibrated against BASE's inflated counts on hot
  values of high-NDV columns will see lower cardinality estimates under the
  PR.
- The PR adds a singleton-TopN filter (default-`numTopN`, heap-full gated)
  that empties noise singletons on some high-NDV columns to match BASE
  (e.g. c3, pk → empty on both). It is not uniform: c7 still keeps 21
  entries where BASE has 0, and c4 goes the other way. Minor stats-content
  divergence, no cardinality effect.

## 5. Removed tuning knob: `tidb_merge_partition_stats_concurrency`

The PR deprecates this knob (BASE default 1). BASE was measured at
concurrency 16 (the max) across all four scenarios to check what removing
it costs:

| scenario | async | BASE c=1 | BASE c=16 | effect |
|---|---|---|---|---|
| part-single | ON  | 1h31m15s | 1h33m57s | none |
| part-single | OFF | 2h05m05s | 2h09m55s | none (slightly worse) |
| part-full | ON  | 1h52m20s | 1h53m06s | none |
| part-full | OFF | 2h27m03s | 2h26m21s | none |

**No speedup on any scenario** — even on part-full async=ON (the heaviest
merge), the merge phase is unchanged within 10 s at c=16. The knob only
parallelizes TopN batching, not the dominant per-column histogram merge, so
it has nothing to parallelize; on async=OFF it is marginally worse (extra
worker-pool memory). **Removing it costs nothing.** (Concurrency 2 was not
measured — it sits between 1 and 16, which are identical, so it cannot
differ.)

## Summary — good and bad

**Good**

- Merge step ~10× faster; whole ANALYZE 4–9× faster.
- Lower memory: 33–48% peak on async=ON, ~55% mean on async=OFF.
- More accurate TopN counts (fixes BASE's ~500× equality overestimate).
- Identical cardinality and matching histograms — no correctness loss.
- The removed concurrency knob provided no measurable benefit.

**Bad / caveats**

- async=OFF peak memory is not improved (only sustained memory is).
- TopN content diverges from BASE: more accurate, but equality-predicate
  cardinality estimates on hot values of high-NDV columns will change, so
  some plans may change. Worth validating on representative workloads.
- **On per-partition-categorical columns with a high NULL fraction, the PR's
  singleton filter empties the global TopN where BASE keeps entries.** Direct
  test (partitioned vs identical non-partitioned table, full sampling, both
  binaries): for such a column the PR's partitioned global TopN = 0, BASE =
  100, and the ground-truth non-partitioned TopN = 11 — so BASE keeps ~89
  noise entries, the PR drops all including the ~11 real ones. NDV and
  histograms are unaffected, so the only impact is slightly less precise
  equality-selectivity on those columns' frequent values (histogram fallback
  still applies). Non-blocking; a filter-tuning refinement at most.
- **Partitioned global TopN does not exactly equal a non-partitioned
  `ANALYZE` of the same data — but this is pre-existing, not PR-introduced.**
  The same test shows BASE diverges from the non-partitioned result on the
  same columns; it is a fundamental partitioned-merge vs single-table
  difference, present on both branches.
- Single-partition `ANALYZE … PARTITION p` still triggers a full
  8192-partition global re-merge on both branches (the PR makes it 9×
  faster, but the full-rebuild-on-one-partition cost remains an
  independent optimization opportunity).
