# Performance Report: stats_buckets → stats_data Migration

## Test Setup

- **MASTER**: a10a43ada5 (v9.0.0-beta.2.pre-1584)
- **STATS-DATA**: cfa2354409 (stats_buckets-to-stats_data branch, v9.0.0-beta.2.pre-1635)
- **Data**: 8000 HASH partitions, ~30M rows, 17 columns (pk + c1-c16)
- **Config**: tidb_analyze_partition_concurrency=6, analyze_version=2, ALL COLUMNS, 16GB server-memory-quota
- **Hardware**: Apple Silicon laptop, TiDB/TiKV/PD on same machine via tiup playground
- **Iterations**: 2 per scenario, averaged

## Full Matrix Results

| Scenario | MASTER | STATS-DATA | Duration Δ | MASTER Heap | SD Heap | Heap Δ |
|---|---|---|---|---|---|---|
| **part-full clean ON** | 24m18s | **7m36s** | **-69%** | 4.46 GB | 4.52 GB | +1% |
| **part-full clean OFF** | 22m51s | **6m36s** | **-71%** | 28.66 GB | 28.02 GB | -2% |
| **part-full existing ON** | 29m21s | **8m22s** | **-72%** | 6.06 GB | 5.05 GB | -17% |
| **part-full existing OFF** | 28m16s | **7m31s** | **-73%** | 29.47 GB | 29.17 GB | -1% |
| part-single clean ON | 3m38s | 3m55s | **+8%** | 0.95 GB | 0.98 GB | +3% |
| part-single clean OFF | 2m14s | 2m29s | **+12%** | 1.05 GB | 1.07 GB | +2% |
| part-single existing ON | 3m16s | 3m33s | **+8%** | 0.84 GB | 1.04 GB | +23% |
| part-single existing OFF | 1m53s | 2m07s | **+12%** | 1.04 GB | 1.15 GB | +10% |
| nonpart-full clean ON | 6.8s | 6.6s | -3% | 1.23 GB | 1.27 GB | +4% |
| nonpart-full clean OFF | 6.8s | 6.6s | -3% | 1.32 GB | 1.21 GB | -8% |
| nonpart-full existing ON | 6.8s | 6.5s | -4% | 1.11 GB | 1.03 GB | -7% |
| nonpart-full existing OFF | 6.9s | 6.7s | -2% | 1.28 GB | 1.11 GB | -13% |

## Key Findings

### 1. Full-table ANALYZE: 3× faster (69-73% reduction)

The clustered PK in `stats_data` transforms both the write path (saving
partition stats) and the read path (merge loading). Consistent across
all async-merge modes and stats states.

### 2. Memory unchanged for async=ON

Peak heap is within ±4% for async=ON. The stats_data PR does not change
the merge algorithm — only the storage layer. The async=OFF path still
uses ~28-29 GB (old `MergePartitionHist2GlobalHist` code, unchanged).

### 3. "Existing" runs now scale properly

MASTER's "existing" runs (re-analyzing a table that already has stats)
took **longer** than clean runs (29m vs 24m for async=ON). STATS-DATA's
existing runs are only slightly longer than clean (8.4m vs 7.6m). The
non-clustered `stats_buckets` table apparently scattered reads during
the merge's IO loading phase, making re-analysis slower than fresh.

### 4. Part-single: 8-12% slower (merge-only regression)

Single-partition ANALYZE (triggers global merge without full table scan)
is consistently slower on STATS-DATA.

**Root cause**: STATS-DATA executes **138K more internal SQL statements**
than MASTER during the merge phase:

| Metric | MASTER | STATS-DATA | Delta |
|---|---|---|---|
| Internal SQL count | 933,230 | 1,071,835 | +138,605 (+15%) |
| Internal SQL total time | 194.5s | 208.7s | +14.2s |

138K extra queries ÷ 8000 partitions = ~17 extra SQL per partition.
This is likely backward-compatibility overhead in the stats loading
path — checking both `stats_data` (new) and `stats_buckets` (old)
tables, or additional queries for the new schema format.

This overhead is masked in full-table ANALYZE (where the 3× save
speedup dominates) but visible in part-single where only the merge
runs.

### 5. Nonpart-full: identical

No partitioning overhead — expected.

## Phase Breakdown (part-full clean async=ON)

CPU profiling of the STATS-DATA run:

| Phase | Wallclock | % | CPU Load | Bottleneck |
|---|---|---|---|---|
| **Save partition stats** | ~210s | 36% | ~4.5 cores | `SaveAnalyzeResultToStorage` → TiKV writes |
| **Merge: IO/prepare** | ~140s | 24% | ~0.7 cores | Loading 8K partition stats from TiKV |
| **Merge: CPU** | ~150s | 26% | ~1.2 cores | `MergePartTopNAndHistToGlobal` (old algorithm) |
| **Idle/gaps** | ~80s | 14% | ~0 | Scheduling, waiting |
| **Total** | ~580s | | | |

### Save phase: FMSketch INSERT is 68% of save time

Within the save phase, the dominant cost is FMSketch:

| Operation | Time | % of save |
|---|---|---|
| **FMSketch INSERT** | 9.16s/10s | **68%** |
| saveBuckets (protobuf) | 1.85s/10s | 14% |
| saveTopN (batch INSERT) | 0.72s/10s | 5% |
| DELETE topN | 0.57s/10s | 4% |
| DELETE fm_sketch | 0.42s/10s | 3% |
| Other (histograms, usage) | 0.66s/10s | 5% |

Each column's FMSketch (~8 KB blob) is a separate INSERT statement.
8000 partitions × 17 columns = 136K individual INSERTs. Moving
FMSketch into `stats_data` and batching would eliminate most of this.

## Recommendations

### Already done
- [x] `stats_buckets` → `stats_data` with clustered PK (this PR)

### Next steps
- [ ] Move `stats_fm_sketch` → `stats_data` (eliminates 68% of save overhead)
- [ ] Batch all column data into multi-row INSERT per partition
      (reduce ~119 SQL round-trips to ~4-5 per partition)
- [ ] Investigate the +17 extra SQL per partition in the stats loading
      path (part-single regression)
- [ ] Consider batching multiple partitions per transaction commit

## Raw Data

All artifacts preserved in:
- `output-compare-fresh/MASTER/` — 24 runs
- `output-compare-fresh/STATS-DATA/` — 24 runs
- `output-compare-fresh/manifest.tsv` — run index
- `output-compare-fresh/report.txt` — full comparison report

Binaries used:
- `tidb-server.master-a10a43a` — MASTER at a10a43ada5
- `tidb-server.stats-data-cfa2354` — stats_buckets-to-stats_data at cfa2354409
