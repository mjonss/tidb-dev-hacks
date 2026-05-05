#!/usr/bin/env bash
# Wrapper that runs bench.sh with the v3 benchmark config (improve-global-stats-3
# PR vs upstream): a freshly generated 8000-partition / 30M-row / 20-column
# table covering all merge-relevant (type × distribution × nullability)
# combinations, plus one single-column and one multi-column secondary index.
#
# Usage:
#   ./bench-config-v3.sh build
#   ./bench-config-v3.sh prepare        # build the combined-backup-v3 once
#   ./bench-config-v3.sh run quick      # 1 cell each branch
#   ./bench-config-v3.sh run all        # full matrix
#   ./bench-config-v3.sh compare > output-bench-v3/report.txt
#
# All bench.sh subcommands are forwarded with these env-var overrides applied.
#
# ============================================================================
# Order of operations (what bench.sh actually does for an `all` run)
# ============================================================================
#
# 1. cmd_build           — compiles ./analyze-profile (Go), checks BIN_PR/BASE.
#
# 2. cmd_run all         — calls run_matrix_for_label twice (BASE then PR).
#
# 3. run_matrix_for_label LABEL BIN — for each (scenario, async, iter):
#
#      a) playground_reload BIN
#           - On the very first call: prepare_combined_backup BIN
#               i)   playground_stop   (no-op if nothing running)
#               ii)  playground_start BIN  (fresh tiup playground)
#               iii) generate_data           ← creates t_partitioned via
#                                              `analyze-profile setup` with
#                                              --column-spec + --index
#               iv)  clone_nonpartitioned    ← INSERT…SELECT into t_nonpart
#               v)   br_backup_combined      ← BR backup → combined-backup-v3/
#               vi)  playground_stop
#           - Subsequent calls: skip prepare; just stop, start fresh, restore
#             from combined-backup-v3 (so every iter starts byte-identical).
#           - Restore order:
#               i)   playground_stop
#               ii)  playground_start BIN
#               iii) br_restore combined-backup-v3
#
#      b) For each state in (clean, existing):
#           i)   warmup            — SELECT COUNT(*) on both tables, primes
#                                    block cache so first ANALYZE isn't I/O-
#                                    bound.
#           ii)  run_one_profile   — see step 4.
#
#         Both states run on the SAME cluster instance — `existing` deliberately
#         keeps prior `clean` stats in TiDB's memory cache, which is the
#         realistic re-analyze scenario the merge optimization targets.
#
# 4. run_one_profile LABEL SCENARIO STATE ASYNC ITER:
#
#      a) start_process_monitor          — background ps sampler, 2s cadence
#      b) capture_system_snapshot before — disk/load/mem
#      c) ./analyze-profile profile … (the heavy lifter):
#         - SET session/global vars (--set-variable)
#         - DROP STATS if --drop-stats (clean state)
#         - ANALYZE TABLE … with the requested scope
#         - During ANALYZE, in parallel:
#             * heap snapshots every 2s
#             * CPU profile (CPU_PROFILE_SECONDS, default 10s)
#             * goroutine + mutex snapshots before/after
#             * mysql.analyze_jobs polling for per-partition timing
#             * tidb_metrics scrape every 1s
#         - Post-ANALYZE: dump stats_dump.json, run accuracy check
#      d) stop_process_monitor
#      e) capture_system_snapshot after
#      f) Append run_<ts> dir to manifest.tsv
#      g) Copy tidb-logs/ and tikv-logs/ into the run dir (rotated logs glob)
#         — must happen before the next playground_reload destroys the
#         tiup data dir.
#
# 5. cmd_compare         — for each (scenario, state, async) cell, runs
#                          compare-runs.py (durations/RSS/heap), profile-diff.py
#                          (CPU/heap top-diff), accuracy-diff.py against the
#                          manifest groups; writes report.txt.
#
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Generation: 8000 HASH partitions, 30M rows, fresh seed -------------------
export GENERATE_DATA=1
export GEN_PARTITIONS=8000
export GEN_ROWS=30000000
export GEN_SEED=42
# GEN_COLUMNS is overridden by COLUMN_SPEC's length; kept as a fallback.
export GEN_COLUMNS=20

# --- Per-column spec ---------------------------------------------------------
# 20 columns covering 5 encoding-path equivalence classes × 4 informative
# distributions, with NOT NULL and high-NULL paths represented:
#   Signed/unsigned int  : c1-c4   (BIGINT, INT UNSIGNED, BIGINT UNSIGNED, BIGINT)
#   IEEE float           : c5-c8   (DOUBLE)
#   Canonical decimal    : c9-c12  (DECIMAL(10,2))
#   Collation key        : c13-c16 (VARCHAR(255))
#   Packed time          : c17-c20 (DATETIME × 3, TIMESTAMP × 1 for tz coverage)
# Distributions:
#   low-ndv         — TopN saturates, exercises TopN merge
#   zipf            — hot tail + long histogram, both paths
#   uniform         — high-NDV, histogram-heavy
#   per-part-categ  — values cluster within partition, cross-partition merge
export COLUMN_SPEC="BIGINT:low-ndv,INT UNSIGNED:zipf,BIGINT UNSIGNED:uniform:NOTNULL,BIGINT:per-part-categ:NULL(40),\
DOUBLE:low-ndv,DOUBLE:zipf,DOUBLE:uniform,DOUBLE:per-part-categ,\
DECIMAL:low-ndv,DECIMAL:zipf,DECIMAL:uniform,DECIMAL:per-part-categ,\
VARCHAR:low-ndv,VARCHAR:zipf,VARCHAR:uniform:NOTNULL,VARCHAR:per-part-categ,\
DATETIME:low-ndv,DATETIME:zipf,TIMESTAMP:uniform,DATETIME:per-part-categ:NULL(40)"

# --- Indexes -----------------------------------------------------------------
# Single-column on c14 (VARCHAR:zipf)        — string index, realistic skew.
# Multi-column on (c2, c19) (INT UNSIGNED, TIMESTAMP) — heterogeneous concat
# encoding plus exercises the unsigned codec inside an index key.
export INDEXES="c14 c2,c19"

# --- Isolated artifacts so v3 doesn't clobber v2 -----------------------------
export COMBINED_BACKUP_DIR="${SCRIPT_DIR}/combined-backup-v3"
export OUTPUT_ROOT="${SCRIPT_DIR}/output-bench-v3"

exec "${SCRIPT_DIR}/bench.sh" "$@"
