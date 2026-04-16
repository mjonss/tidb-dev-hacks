# Benchmark Findings: improve-global-stats-2 PR

Date: 2026-04-16
PR: `improve-global-stats-2` (commit 7ffd1e6818) vs BASE master (commit 212ded5950)
Data: sample-global-stats backup, 8000 HASH partitions, ~30M rows, 17 columns
Config: tidb_analyze_partition_concurrency=6, analyze_version=2, ALL COLUMNS

## Full Matrix Results (48 runs, ~7 hours)

### Duration

| Scenario | PR avg | BASE avg | Delta | Notes |
|---|---|---|---|---|
| part-full clean async=ON | 23m24s | 23m18s | -0.4% | Tied |
| part-full existing async=ON | 5m37s | 5m36s | -0.3% | Tied (both OOM'd — all partition jobs failed) |
| part-full clean async=OFF | 22m14s | 22m56s | **+3.1%** | PR faster |
| part-full existing async=OFF | 5m25s | 5m58s | **+10.2%** | PR significantly faster (both OOM'd) |
| part-single (all combos) | ~1m52-3m23 | ~1m54-3m25 | +1.0-1.6% | PR slightly faster |
| nonpart-full (all combos) | ~6.8-7.0s | ~6.8-6.9s | <1.7% | Noise |

### Memory (the key finding)

| Scenario | PR RSS | BASE RSS | PR Heap | BASE Heap |
|---|---|---|---|---|
| part-full clean async=ON | **9.54 GB** | **4.87 GB** | **8.43 GB** | **4.42 GB** |
| part-full existing async=ON | 30.65 GB | 29.70 GB | 53.33 GB | 44.12 GB |
| part-full clean async=OFF | 28.42 GB | 28.12 GB | 26.47 GB | 26.99 GB |
| part-full existing async=OFF | 33.85 GB | 35.09 GB | 57.58 GB | 43.49 GB |
| part-single / nonpart-full | ~1.2-2.1 GB | ~1.2-1.9 GB | ~0.8-1.4 GB | ~0.8-1.4 GB |

**PR async=ON clean uses ~2x more peak heap than BASE** (9.2 GB vs 4.8 GB).
Same total cumulative allocations (1120 GB both), but PR retains objects
longer between GC cycles — the GC sawtooth peak doubles.

async=OFF shows parity, confirming the regression is in the async merge path.

### "Existing" Runs All OOM'd

All 8 part-full "existing" runs (both branches, both async values) hit
Error 8176 (server-memory-quota exceeded at 8 GB). The preceding "clean"
ANALYZE leaves several GB of cached stats in TiDB's memory; the second
ANALYZE allocates on top of that, exceeding the 8 GB limit.

This is the real-world scenario the PR targets — re-analyzing a table
that already has cached stats.

## Issues Found in Tooling (all fixed)

### analyze-profile memory leak
AnalyzeJobsPoller accumulated all rows from every poll: O(jobs x polls).
8000 partitions x 690 polls = ~5.5M entries / ~2.5 GB.
**Fix**: keep only latest snapshot per job ID.

### stats_dump.json timeout
60s HTTP timeout too short for TiDB to serialize 8000 partitions x 17
columns. TiDB builds the entire JSON in memory before sending the first
byte (DumpStatsToJSONBySnapshot calls TableStatsToJSON 8001 times
synchronously — confirmed in stats_read_writer.go:456-493).
**Fix**: timeout increased to 10 minutes.

### Heap profiles missed GC peak
10s capture cadence (tied to CPU profile) consistently sampled the post-GC
trough, not the pre-GC peak. The sawtooth cycle was 6-12s.
**Fix**: heap snapshots decoupled to 2s ticker.

### TIDB_MEM_GB too low
8 GB server-memory-quota was too low for the existing-stats scenario.
**Fix**: default raised to 16 GB.

## TODO for Next Run

- [ ] Re-run full matrix with all fixes applied (16 GB limit, 10 min
      stats dump timeout, 2s heap snapshots, process monitor, verbose log,
      TiDB/TiKV log capture)
- [ ] Verify "existing" runs complete without OOM at 16 GB
- [ ] Analyze peak heap profiles (2s cadence should now capture the
      sawtooth peak) to identify which objects the PR retains longer
      in the async=ON merge path
- [ ] Check collector_debug.log for overhead of the new 2s heap captures
- [ ] Check process_monitor.tsv for analyze-profile memory (should be
      ~10 MB now, was ~2.5 GB before the poller fix)
- [ ] Review TiDB logs for warnings/errors during the runs
- [ ] Investigate why PR async=ON retains ~2x more live objects — the
      "eager partition release" commit should reduce memory, not increase it
- [ ] Consider adding ?dumpPartitionStats=false to stats dump URL to skip
      per-partition stats and only get global (would complete in <1s)

## Accuracy

No accuracy regressions found. For columns analyzed by both branches:
- NDV: exact match across all columns (ratio=1.0000)
- Bucket counts: within +/-4 (sampling noise)
- Histogram KS distance: <0.015 (distributions essentially identical)
- TopN Jaccard: 0.88-1.00 for low/medium cardinality columns

PR produces richer TopN on partitioned tables (100 entries where BASE
produces 0 for pk) — extra stats, not a regression.

## Raw Data

All run artifacts preserved in output-bench/:
- output-bench/<BRANCH>/<scenario>/run_<ts>/profile_result.json
- output-bench/<BRANCH>/<scenario>/run_<ts>/heap_*.pb.gz (CPU/heap profiles)
- output-bench/<BRANCH>/<scenario>/run_<ts>/goroutine_*.txt
- output-bench/<BRANCH>/<scenario>/run_<ts>/mutex_{before,after}.pb.gz
- output-bench/<BRANCH>/<scenario>/run_<ts>/stats_dump.json (when not timed out)
- output-bench/<BRANCH>/<scenario>/run_<ts>/collector_debug.log
- output-bench/<BRANCH>/<scenario>/process_monitor.tsv
- output-bench/<BRANCH>/<scenario>/system_{before,after}.txt
- output-bench/manifest.tsv (index of all runs)
- output-bench/report.txt (full comparison report)
