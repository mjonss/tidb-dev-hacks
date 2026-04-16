# TODO: ANALYZE TABLE for Partitioned Tables

Issues and improvement opportunities found during benchmarking of
`improve-global-stats-2` PR against master, on 8000 HASH partitions /
30M rows / 17 columns.

## Current PR Focus: Global Stats Merge Efficiency

The `improve-global-stats-2` PR rewrites the global-stats merge path
(equi-depth upper-bound merge, eager partition release, async merge
changes). Benchmark findings:

- **async=OFF**: PR is 3-10% faster, no memory regression.
- **async=ON**: PR uses ~2x peak heap (9.2 GB vs 4.8 GB) during merge.
  Same total allocations — objects retained longer between GC cycles.
  The "eager partition release" may not be releasing as intended.
- **Accuracy**: no regressions — NDV exact match, histograms
  distributionally identical (KS < 0.015).

### TODO (this PR)
- [ ] Investigate why async=ON retains ~2x more live objects.
      The GC sawtooth peak doubles (4.8 GB → 9.2 GB) despite identical
      cumulative allocations. 2s heap snapshots from the next full run
      should capture the peak for `pprof -inuse_space` diagnosis.
- [ ] Verify the eager partition release actually frees partition stats
      before starting the next column merge.
- [ ] Consider whether the async merge worker pool holds too many
      partition results in-flight simultaneously.

---

## Per-Partition Sampling Overhead

Each partition is analyzed as if it were a full table — configured to
collect up to 110K samples regardless of actual partition size. For
small partitions the sample count is capped at the partition row count,
but for large tables the overhead compounds:

- A 1-billion-row table with 8000 partitions (125K rows each) would
  collect 8000 × 110K = **880M samples**, nearly the entire table.
- Even our 30M-row / 8000-partition test scans all rows per partition
  since each partition (3,750 rows) is smaller than the 110K target.
- Each sample goes through proto serialization on TiKV and
  deserialization on TiDB (`tipb.RowSample.Unmarshal` +
  `grpc.BufferSlice.Materialize` are top heap allocators in profiles).
- The sample target should scale with partition size, not use the
  global table default.

### TODO (separate PR)
- [ ] Scale sample count per partition proportionally to partition row
      count, with a minimum floor.
- [ ] Investigate whether the per-partition sample collector can share
      memory with the global merge path instead of building independent
      histograms.

---

## FMSketch Storage per Partition

Each partition saves a full FMSketch per column to `mysql.stats_fm_sketch`.
FMSketch is used for NDV estimation and is large (~8 KB per column per
partition). With 8000 partitions × 17 columns:

- **136K FMSketch entries** written per ANALYZE.
- `statistics.NewFMSketch` is the #1 heap allocator in profiles
  (~108 GB cumulative over a 23-minute ANALYZE).

### TODO (separate PR)
- [ ] Evaluate whether per-partition FMSketches can be merged
      incrementally (stream-merge) instead of stored independently
      and re-read for the global merge.
- [ ] Consider whether FMSketch is even needed per partition when
      global NDV can be computed from the merged histogram.

---

## stats_* Table Design

The `mysql.stats_histograms`, `stats_buckets`, `stats_top_n`, and
`stats_fm_sketch` tables lack clustered primary keys. For 8000
partitions × 17 columns:

- Writes scatter across many non-clustered index entries.
- Each partition's stats write is a separate transaction to TiKV.
- The write amplification from non-clustered PKs compounds with the
  number of partitions.

### TODO (separate PR)
- [ ] Evaluate adding clustered PKs to the stats tables (migration
      concern — these are system tables).
- [ ] Batch stats writes per partition to reduce transaction count.

---

## Stats Cache Pressure on Re-Analyze

When re-analyzing a table that already has cached stats ("existing"
scenario), the old stats remain in TiDB's in-memory cache while the
new ANALYZE allocates memory for sampling + merging. This caused OOM
(Error 8176) on all "existing" part-full runs at 8 GB server-memory-quota.

The cache does not proactively evict the table being re-analyzed.

### TODO (separate PR)
- [ ] Evict the target table's cached stats at the start of ANALYZE
      to free memory before the new analysis begins.
- [ ] Consider a "stats cache memory budget" that accounts for
      in-progress ANALYZE allocations.

---

## Partition Concurrency Default

`tidb_analyze_partition_concurrency` defaults to 2. On a 10-core
machine analyzing 8000 partitions, this serializes work into 1.66
effective cores — 75% of machine capacity is idle. The wall-clock
is dominated by serialization wait, not computation.

### TODO (separate PR)
- [ ] Raise the default to match available cores, or auto-tune based
      on `runtime.NumCPU()`.
- [ ] Consider separate concurrency controls for the scan phase
      (CPU+IO bound) vs the merge phase (CPU+memory bound).

---

## stats_dump HTTP Endpoint Scalability

The `/stats/dump/{db}/{table}` endpoint calls `TableStatsToJSON` once
per partition (8001 times) synchronously before sending the first HTTP
response byte. For 8000 partitions this takes >60s and times out.

Confirmed in source: `stats_read_writer.go:456-493` builds the entire
`*JSONTable` tree in memory before returning to the HTTP handler.

### TODO (separate PR)
- [ ] Stream the JSON response partition-by-partition instead of
      building the full tree in memory.
- [ ] Add `?dumpPartitionStats=false` as the default for monitoring
      tools that only need global stats.

---

## DROP STATS Scalability

Dropping statistics for a partitioned table requires per-partition
`DROP STATS ... PARTITION` calls batched to avoid OOM. At 8000
partitions even the batch-drop path takes significant time.

### TODO (separate PR)
- [ ] Support `DROP STATS table` that drops all partition stats in
      one operation server-side without per-partition SQL.
