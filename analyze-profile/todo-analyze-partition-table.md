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

## stats_* Table Design and Save Performance

The `mysql.stats_histograms`, `stats_buckets`, `stats_top_n`, and
`stats_fm_sketch` tables lack clustered primary keys. For 8000
partitions × 17 columns:

- Writes scatter across many non-clustered index entries.
- Each partition's stats write is a separate transaction to TiKV.
- The write amplification from non-clustered PKs compounds with the
  number of partitions.

### stats_buckets → stats_data migration (in progress)

PR `stats-tables-with-clustered-pk` replaces `stats_buckets` with
`stats_data` (clustered PK). Full ANALYZE dropped from 23m to 9.7m
on 8K partitions. See pingcap/tidb#66751.

### FMSketch: dominant save cost

CPU profiling shows FMSketch INSERT is **68% of the save phase** —
9.16s out of 13.4s per 10s profile window. Each column's FMSketch
(~8 KB protobuf blob) is a separate `INSERT INTO mysql.stats_fm_sketch`
statement. With 8K partitions × 17 columns = 136K individual INSERTs.

Moving FMSketch into `stats_data` (same clustered-PK table as buckets)
and batching the inserts would eliminate most of this overhead.

### SQL round-trips within save transaction

Each partition save is one transaction but **~119 individual SQL
statements**: 17× (DELETE topn + INSERT topn + DELETE fm_sketch +
INSERT fm_sketch + REPLACE histograms + INSERT/UPDATE stats_data +
INSERT column_stats_usage). Each statement goes through TiDB's
parse → plan → execute → TiKV write cycle.

With all data in `stats_data`, this could be reduced to ~4-5 SQL
statements per partition: one batch DELETE + one batch INSERT for
stats_data + a few for stats_histograms and column_stats_usage.

### Batching multiple partitions per transaction

Currently: one transaction per partition (8K commits for 8K partitions).
Each 2PC commit has ~1-2ms fixed overhead.

Batching 10-20 partitions per transaction would reduce commits from
8K to ~400-800 while keeping transaction size small (~3-6 MB per
batch for 17 columns). Transaction size is safe up to ~50 partitions
even for wide tables (100 columns × 18 KB/col = 1.7 MB/partition).

### TODO
- [x] Migrate stats_buckets → stats_data with clustered PK (#66751)
- [ ] Migrate stats_fm_sketch → stats_data (next PR)
- [ ] Batch all column data (buckets + fm_sketch + topn) into one
      multi-row INSERT per partition instead of per-column INSERTs
- [ ] Evaluate batching multiple partitions per transaction commit
- [ ] Migrate stats_top_n → stats_data

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
