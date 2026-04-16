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

## Memory Investigation (quick run, 2s heap snapshots)

Using 2s heap snapshots we captured the peak at sample 716 (~1432s) for
both branches.

### Peak heap at-a-glance

| | BASE | PR | Delta |
|---|---|---|---|
| Peak heap (metrics) | 4.45 GB | 11.63 GB | +7.18 GB (2.6×) |
| Peak RSS | 5.11 GB | 12.61 GB | +7.50 GB |
| in-use at pprof peak | 768 MB | 2000 MB | +1232 MB |
| GC trough | ~1.1 GB | ~2.1 GB | |

### Root cause #1: codec.EncodeKey per bucket (267 MB)

Phase 2 calls `codec.EncodeKey(tz, nil, *hist.GetUpper(i))` for every
non-index histogram bucket with Repeat > 0 to match the TopN counter's
encoded-byte key format. With ~2M buckets, each allocating ~130 bytes,
this produces 267 MB of `reallocBytes`.

**Fix** (committed as 7a7a50c7f4): reuse a single `encodeBuf` via
`codec.EncodeKey(tz, encodeBuf[:0], ...)`. The buffer grows once to
~130 bytes and stays there. The map copies the key on insert.

Longer-term: avoid encoding entirely by decoding the ~few-thousand
TopN keys to Datums and binary-searching for each histogram bound
using Datum comparison (same approach BASE's `EqualRowCount` uses).

### Root cause #2: double bucket4Merging for Repeat values (434 MB)

The PR splits each histogram bucket's row count into TWO bucket4Merging
entries that are alive simultaneously:

```
Phase 2: bucket4Merging with Count = bucket.Count - Repeat    (~393 MB)
Phase 4: bucket4Merging with Count = Repeat (from leftTopN)   (~434 MB)
```

These represent the **same rows**, split into two objects. Both are held
in the `buckets` slice when Phase 5's sort runs, doubling the working set.

BASE creates only ONE bucket4Merging per bucket (with Repeat already
handled inline by `BinarySearchRemoveVal`), plus ~21 MB of actual TopN
leftovers.

**Why Phase 2 extracts Repeat into the counter at all**: a value can
have the highest global frequency without ever appearing in any single
partition's TopN. Example: value "X" has count 20 in each of 8000
partitions (total 160,000) but each partition has 100 other values with
count 21 that fill its TopN. Phase 2 catches "X" by summing its Repeat
across partition histograms. This is correct and necessary.

**Why the double-representation is NOT necessary**: the Repeat extraction
serves to identify global TopN candidates. Once Phase 3 determines the
100 global TopN values, we only need to subtract THOSE 100 values'
counts from the histogram data. The other ~550K leftTopN entries' counts
are already represented in the Phase 2 bucket4Merging entries.

**Fix** (to be implemented):
1. Phase 2: keep full bucket counts (don't subtract Repeat). Still
   accumulate counter entries for TopN candidate identification.
2. Phase 3: determine global TopN (100 entries) — unchanged.
3. Phase 4: only subtract the 100 global TopN values' counts from
   affected bucket4Merging entries (binary search, trivial). Do NOT
   convert leftTopN to bucket4Merging — their counts are already in
   the Phase 2 buckets.
4. Adjust `totCount` only for the 100 global TopN entries.

Eliminates ~434 MB of Phase 4 bucket4Merging, ~550K `topNMetaToDatum` +
`buildBucket4Merging` calls, and simplifies the data flow.

### Root cause #3: Datum.Clone (+59 MB)

Visible in the diff profile. Likely from `d.Copy(res.lower)` and
`d.Copy(res.upper)` in `TopNMeta.buildBucket4Merging`. Eliminated if
Phase 4 bucket creation is removed (fix #2 above).

### Allocation summary at peak (pprof source-annotated)

```
PR MergePartTopNAndHistToGlobal at peak (heap_716.pb.gz):

Line 1623:  267 MB cum — codec.EncodeKey (fix #1: buffer reuse)
Line 1641:  393 MB cum — Phase 2 bucket4Merging (histogram buckets)
Line 1655:  491 MB flat — Phase 3 sorted slice + string copies (mis-attributed by pprof inlining)
Line 1669:  434 MB cum — Phase 4 leftTopN → bucket4Merging (fix #2: eliminate)
Line 1674:  150 MB cum — Phase 5 mergeByUpperBound

BASE MergePartitionHist2GlobalHist at peak:
Histogram.buildBucket4Merging: 427 MB (single representation, no Phase 4)
NewHistogram: 53 MB
```

### Expected impact of fixes #1 + #2

| | Current PR | After fixes | BASE |
|---|---|---|---|
| codec allocs | 267 MB | ~0 MB | — |
| Phase 2 buckets | 393 MB | ~400 MB (full count) | 427 MB |
| Phase 4 buckets | 434 MB | ~0 MB | ~21 MB |
| Other | ~906 MB | ~906 MB | ~320 MB |
| **Total in-use** | **2000 MB** | **~1300 MB** | **768 MB** |
| **GC peak** | **11.6 GB** | **~7-8 GB** (est.) | **4.5 GB** |

Remaining gap (~530 MB vs BASE) is from the `sorted` TopNMeta slice
(line 1655, ~491 MB of string copies from counter map iteration) and
sync.Pool overhead. Addressable by reusing counter keys instead of
copying them.

## TODO for Next Run

- [x] Investigate why PR async=ON retains ~2x more live objects
- [x] Add ?dumpPartitionStats=false to stats dump URL
- [ ] Re-run with fix #1 (buffer reuse, committed) and fix #2 (deferred
      Repeat subtraction, to be implemented) to verify memory reduction
- [ ] Re-run full matrix with all tooling fixes applied
- [ ] Verify "existing" runs complete without OOM at 16 GB
- [ ] Check collector_debug.log for overhead of 2s heap captures
- [ ] Review TiDB logs for warnings/errors

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
