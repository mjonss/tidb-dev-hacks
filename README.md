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

Each run creates a timestamped subdirectory (e.g. `output/run_20260226_153045/`) containing:
- `profile_result.json` — full structured results (config, per-partition jobs, metric time series, slow queries, session variables)
- `stats_topn.tsv`, `stats_histograms.tsv`, `stats_buckets.tsv`, `stats_meta.tsv` — table statistics from SQL
- `stats_dump.json` — full statistics dump from TiDB HTTP API
- `heap_*.pb.gz` — heap profiles (snapshots during and after ANALYZE)
- `cpu_profile_*.pb.gz` — CPU profiles (viewable with `go tool pprof`)

#### Data collected during profiling

- **analyze_jobs** — polled every 500ms for per-partition start/end times
- **TiDB & TiKV metrics** — scraped every 2s (CPU, RSS, heap, goroutines, DistSQL, coprocessor)
- **pprof** — heap snapshots before/after, CPU profiles in a loop during ANALYZE
- **TiDB log** — tailed for analyze/statistics/merge-related lines (when `--tidb-log` is set)
- **Slow query** — queried from `information_schema.slow_query` after ANALYZE completes
- **Session variables** — statistics-related variables captured for reference

### Quick start

```sh
# Start a TiDB playground
tiup playground

# Create a small test table
go run ./analyze-profile setup --partitions=4 --rows=10000 --columns=10

# Profile ANALYZE on all partitions
go run ./analyze-profile profile

# Profile ANALYZE on specific partitions only (faster, useful for merge profiling)
go run ./analyze-profile profile --partition "p0,p1"

# Set TiDB session/global variables before running ANALYZE
go run ./analyze-profile profile \
  --set-variable "tidb_enable_sample_based_global_stats=ON" \
  --set-variable "tidb_build_stats_concurrency=4"

# View CPU profile
go tool pprof output/run_*/cpu_profile_0.pb.gz
```
