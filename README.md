# tidb-dev-hacks
Various hacks and tools used during TiDB development, debugging, benchmarking, troubleshooting etc.

## analyze-profile

Profiles `ANALYZE TABLE` on partitioned TiDB tables. Measures wall-clock time, CPU, memory, and per-partition breakdown to help identify bottlenecks.

### Build

```sh
cd analyze-profile
go build -o analyze-profile .
```

Or run directly with `go run .`.

### Usage

Two subcommands: `setup` (create table + insert data) and `profile` (run ANALYZE and collect performance data).

#### 1. Setup — create a partitioned table with test data

```sh
go run ./analyze-profile setup [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--host` | 127.0.0.1 | TiDB host |
| `--port` | 4000 | TiDB SQL port |
| `--user` | root | DB user |
| `--password` | "" | DB password |
| `--db` | analyze_profile | Database name |
| `--table` | t_partitioned | Table name |
| `--partitions` | 256 | Number of HASH partitions |
| `--rows` | 10000000 | Number of rows to insert |
| `--columns` | 50 | Number of columns (cycles through the selected column types) |
| `--column-types` | mixed | Column types: `mixed` (INT, BIGINT, CHAR, VARCHAR, DECIMAL, FLOAT, DOUBLE, DATE, DATETIME, TIMESTAMP) or `int` (INT, BIGINT only) |
| `--batch-size` | 5000 | INSERT batch size |
| `--insert-concurrency` | 8 | Number of parallel partition inserters |
| `--seed` | 0 | Random seed for data generation (0 = random, printed for reproducibility) |
| `--partition-profile` | uniform | Data distribution across partitions: `uniform`, `range-like`, `size-skew` |
| `--index` | | Secondary index columns (repeatable, e.g. `--index "c1,c4" --index "c3"`); creates `KEY idx_c1_c4 (c1, c4)` etc. |
| `--max-string-length` | 60 | Maximum length for generated VARCHAR values; if > 255, widens the column type to `VARCHAR(N)` |

**Column layout:** Columns cycle through types and distributions in order. The full mapping is printed during `setup`. With `--column-types mixed` (default):

| Column | Type | Distribution |
|--------|------|-------------|
| c1 | INT | uniform |
| c2 | BIGINT | zipf |
| c3 | CHAR(32) | normal |
| c4 | VARCHAR(255) | low-ndv |
| c5 | DECIMAL(10,2) | sequential |
| c6 | FLOAT | per-part-range |
| c7 | DOUBLE | growth |
| c8 | DATE | per-part-categ |
| c9 | DATETIME | uniform |
| c10 | TIMESTAMP | zipf |
| c11+ | (cycle repeats) | (cycle repeats) |

Use this to choose `--index` columns by type, e.g. `--index "c1,c4"` for a composite INT+VARCHAR index.

#### 2. Profile — run ANALYZE and collect data

```sh
go run ./analyze-profile profile [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--host` | 127.0.0.1 | TiDB host |
| `--port` | 4000 | TiDB SQL port |
| `--user` | root | DB user |
| `--password` | "" | DB password |
| `--status-port` | 10080 | TiDB status port (pprof/metrics) |
| `--db` | analyze_profile | Database name |
| `--table` | t_partitioned | Table name |
| `--partition` | "" | Comma-separated partition names to analyze (e.g. `p0,p1`); empty = all partitions |
| `--analyze-columns` | "" | Column selection for ANALYZE: `all`, `predicate`, or comma-separated list (e.g. `c1,c2,c3`); empty = server default |
| `--set-variable` | | Set a session+global variable before ANALYZE (repeatable, e.g. `--set-variable "tidb_enable_sample_based_global_stats=ON"`) |
| `--output-dir` | ./output | Base directory for results |
| `--cpu-profile-seconds` | 10 | Duration per pprof CPU profile capture |
| `--tikv-status-port` | 20180 | TiKV status port (for metrics) |
| `--tidb-log` | "" | Path to TiDB log file to tail during ANALYZE |
| `--drop-stats` | false | Drop table statistics before running ANALYZE |
| `--truncate-stats` | false | Truncate all `mysql.stats_*` tables before running ANALYZE (affects all tables in cluster) |
| `--check-accuracy` | false | After ANALYZE, compare stats estimates vs actual counts (row count + range queries on up to 5 columns) |

Each run creates a timestamped subdirectory (e.g. `output/run_20260226_153045/`) containing:
- `profile_result.json` — full structured results (config, per-partition jobs, metric time series, slow queries, goroutine samples, session variables)
- `stats_topn.tsv`, `stats_histograms.tsv`, `stats_buckets.tsv`, `stats_meta.tsv` — table statistics from SQL
- `stats_dump.json` — full statistics dump from TiDB HTTP API (authoritative, immune to in-memory cache eviction)
- `heap_*.pb.gz` — heap profiles every 2s (captures GC sawtooth peaks)
- `cpu_profile_*.pb.gz` — CPU profiles (viewable with `go tool pprof`)
- `goroutine_*.txt` — goroutine state snapshots every 2s (shows what TiDB is blocked on)
- `mutex_before.pb.gz`, `mutex_after.pb.gz` — mutex contention delta (TiDB already samples at 1/10)
- `collector_debug.log` — timing of every collection operation (when `--verbose`)

#### Data collected during profiling

- **analyze_jobs** — polled every 2s for per-partition start/end times (latest snapshot per job ID)
- **TiDB & TiKV metrics** — scraped every 2s (CPU, RSS, heap, goroutines, DistSQL, coprocessor, GC duration, scheduler latency, compaction, region splits)
- **pprof** — heap snapshots every 2s, CPU profiles in a loop during ANALYZE
- **Goroutine snapshots** — `/debug/pprof/goroutine?debug=2` every 2s, parsed into per-state counts (chan receive, semacquire, IO wait, runnable, etc.)
- **Mutex profile** — before/after delta from TiDB's built-in `SetMutexProfileFraction(10)`
- **Slow query** — extended fields from `information_schema.slow_query` (process_time, wait_time, backoff_time, cop_time, cop_proc_avg, cop_wait_avg, etc.)
- **TiDB log** — tailed for analyze/statistics/merge-related lines (when `--tidb-log` is set)
- **Session variables** — statistics-related variables captured for reference

### bench.sh — PR vs base comparison harness

Orchestrates full-matrix benchmarks comparing two TiDB binaries (PR vs base)
across scenarios, stats states, and async-merge settings. Uses `tiup playground`
for cluster lifecycle and BR for data restore.

#### Quick start

```sh
cd analyze-profile

# Smoke test (generates 1M-row table, ~4 min end-to-end):
BIN_PR=./tidb-server.pr BIN_BASE=./tidb-server.master ./bench.sh smoke

# Full matrix (BR-restore from existing backup, ~7 hours):
BIN_PR=./tidb-server.pr BIN_BASE=./tidb-server.master ./bench.sh all

# On a fresh machine (no BR backup needed — generates data):
BIN_PR=/path/to/pr/bin/tidb-server \
BIN_BASE=/path/to/base/bin/tidb-server \
GENERATE_DATA=1 GEN_ROWS=30000000 GEN_COLUMNS=16 GEN_PARTITIONS=8000 \
./bench.sh all
```

#### Subcommands

| Command | Description |
|---|---|
| `build` | Build analyze-profile + sanity-check binaries |
| `setup` | Start playground + restore data (interactive use) |
| `run [scope]` | Run the matrix. Scopes: `all`, `pr`, `base`, `quick`, `escalate` |
| `compare` | Re-run comparison report from existing data |
| `prepare` | Rebuild the combined backup |
| `stop` | Tear down playground |
| `smoke` | End-to-end minimal test (~4 min) |
| `all` | build + run all + compare |

#### Matrix dimensions

- **2 branches**: PR vs BASE (configured via `BIN_PR` / `BIN_BASE`)
- **3 scenarios**: `part-full`, `part-single`, `nonpart-full`
- **2 stats states**: `clean` (drop stats first), `existing` (stats from prior run)
- **2 async-merge settings**: `tidb_enable_async_merge_global_stats` ON/OFF
- **N iterations**: default 2

#### Per-run output

Each cell produces a run directory with all `analyze-profile` output plus:
- `process_monitor.tsv` — CPU%, RSS, VSZ per process (tidb/tikv/pd/analyze-profile) every 2s
- `system_before.txt` / `system_after.txt` — load average, disk space, memory pressure
- `tidb-logs/` — TiDB log files (current + rotated)
- `tikv-logs/` — TiKV log files (current + rotated)

#### Configuration

Edit `bench-config.sh` or override via environment variables:

```sh
# Memory limits (rendered into per-component tomls):
TIDB_MEM_GB=16 TIKV_MEM_GB=8 TIKV_BLOCK_CACHE_GB=3 ./bench.sh all

# Session variables, scenarios, iterations:
# Edit COMMON_SET_VARS, SCENARIOS, STATES, ASYNC_MERGE_VALUES in bench-config.sh
```

### Comparison tools

#### compare-runs.py

Compares profile runs between groups. Reports duration, peak memory, goroutine
states, mutex contention, TiKV coprocessor durations, partition job timeline,
TiKV noise indicators (region splits, compaction), memory trajectory.

```sh
python3 compare-runs.py \
  --group PR  output-bench/PR/part-full_clean_asyncON_iter*/run_* \
  --group BASE output-bench/BASE/part-full_clean_asyncON_iter*/run_*
```

#### accuracy-diff.py

Compares stats accuracy between groups. Reads `stats_dump.json` (authoritative,
immune to TiDB cache eviction). Reports NDV, null count, bucket count/uniformity,
histogram KS distance, TopN Jaccard similarity.

```sh
python3 accuracy-diff.py \
  --group PR  output-bench/PR/part-full_clean_asyncON_iter*/run_* \
  --group BASE output-bench/BASE/part-full_clean_asyncON_iter*/run_*
```

#### profile-diff.py

Compares pprof CPU or heap profiles between groups. Extracts top functions,
averages across runs, reports top movers.

```sh
python3 profile-diff.py cpu \
  --group PR  output-bench/PR/part-full_clean_asyncON_iter*/run_* \
  --group BASE output-bench/BASE/part-full_clean_asyncON_iter*/run_*
```

### Quick start (standalone, no bench.sh)

```sh
# Start a TiDB playground
tiup playground nightly

# Create a small test table
go run ./analyze-profile setup --partitions=4 --rows=10000 --columns=10

# Profile ANALYZE on all partitions
go run ./analyze-profile profile --check-accuracy --verbose --analyze-columns all

# Profile ANALYZE on specific partitions only
go run ./analyze-profile profile --partition "p0,p1"

# View CPU profile
go tool pprof output/run_*/cpu_profile_0.pb.gz
```

### Related docs

- [FINDINGS.md](analyze-profile/FINDINGS.md) — benchmark results from the improve-global-stats-2 PR
- [todo-analyze-partition-table.md](analyze-profile/todo-analyze-partition-table.md) — catalog of ANALYZE TABLE issues for partitioned tables
