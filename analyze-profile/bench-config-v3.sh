#!/usr/bin/env bash
# Wrapper that runs bench.sh with the v3 benchmark config (improve-global-stats-3
# PR vs upstream).
#
# Shape of the v3 run:
#   - 8000-partition, 30M-row, 20-column mixed-type t_partitioned table.
#   - No t_nonpart twin (SKIP_NONPART_CLONE=1).
#   - Combined backup is seeded with PR-produced per-partition stats: the
#     prepare phase runs a full ANALYZE TABLE … ALL COLUMNS with the PR
#     binary, profiled and captured as the "no previous stats" baseline,
#     then BR-backs up data + stats together.
#   - Auto-analyze is forced OFF after every playground start.
#   - Test matrix is restricted to PARTITION-SINGLE × EXISTING-stats only,
#     async=ON. No DROP STATS ever happens. Each test cell starts from the
#     seeded backup and runs ANALYZE TABLE … PARTITION p5, which (in
#     dynamic prune mode) reads the 7999 existing partition stats and does
#     a full N=8000 global merge — which is exactly what we want to
#     compare between PR and BASE.
#
# Usage:
#   ./bench-config-v3.sh build
#   ./bench-config-v3.sh prepare        # one-time: generate + seed-ANALYZE + BR backup
#   ./bench-config-v3.sh run quick      # 1 cell each branch
#   ./bench-config-v3.sh run all        # full part-single matrix
#   ./bench-config-v3.sh compare > output-bench-v3/report.txt
#
# All bench.sh subcommands are forwarded.
#
# ============================================================================
# Order of operations for `./bench-config-v3.sh all` (or run all)
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
#               i)   playground_stop                (no-op if nothing running)
#               ii)  playground_start ${BIN_PR}     ← seed binary, NOT BIN
#                       └─ disable_auto_analyze     (always)
#               iii) generate_data                  ← analyze-profile setup
#                                                     --column-spec (20 cols)
#                                                     --index c14
#                                                     --index c2,c19
#               iv)  (clone_nonpartitioned skipped — SKIP_NONPART_CLONE=1)
#               v)   seed_analyze                   ← profiled ANALYZE TABLE
#                                                     t_partitioned ALL COLUMNS
#                                                     using BIN_PR; output in
#                                                     output-bench-v3/PR/
#                                                     seed-full-analyze/run_<ts>/
#                                                     (NOT in manifest.tsv —
#                                                      this is the baseline
#                                                      reference)
#               vi)  br_backup_combined             ← combined-backup-v3/
#                                                     (now contains data + stats)
#               vii) playground_stop
#           - Subsequent calls: stop / start BIN / disable_auto_analyze /
#             br_restore from combined-backup-v3 (every iter starts from
#             byte-identical seeded state).
#
#      b) for each state in (existing):
#           i)   warmup            — SELECT COUNT(*) on t_partitioned only
#                                    (t_nonpart doesn't exist).
#           ii)  run_one_profile   — see step 4. NEVER passes --drop-stats
#                                    because state ≠ clean.
#
# 4. run_one_profile LABEL part-single existing ON ITER:
#
#      a) start_process_monitor          — background ps sampler @ 2s
#      b) capture_system_snapshot before
#      c) ./analyze-profile profile … :
#         - SET vars (--set-variable, including tidb_enable_async_merge_global_stats=ON)
#         - --partition p5
#         - ANALYZE TABLE … PARTITION p5 ALL COLUMNS
#           ↳ in dynamic prune mode, this re-collects p5's per-partition
#             stats AND merges all 8000 partitions' stats into fresh global
#             stats — which is the full N=8000 merge under test.
#         - During ANALYZE, in parallel:
#             heap snapshots @ 2s,
#             CPU profile (CPU_PROFILE_SECONDS, default 10s),
#             goroutine + mutex snapshots before/after,
#             mysql.analyze_jobs polling,
#             tidb metrics scrape @ 1s.
#         - Post-ANALYZE: dump stats_dump.json, accuracy check.
#      d) stop_process_monitor
#      e) capture_system_snapshot after
#      f) Append run_<ts> to manifest.tsv (only the matrix runs, NOT the seed)
#      g) Copy tidb-logs/ + tikv-logs/ into run dir
#
# 5. cmd_compare         — for each (scenario, state, async) cell:
#                          compare-runs.py / profile-diff.py / accuracy-diff.py
#                          → writes output-bench-v3/report.txt.
#
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Generation: 8000 HASH partitions, 30M rows, fresh seed -------------------
# Each var honors the caller's env so smaller verification runs (e.g.
# GEN_PARTITIONS=10 GEN_ROWS=1000000 ./bench-config-v3.sh prepare) work.
export GENERATE_DATA=1
export GEN_PARTITIONS="${GEN_PARTITIONS:-8000}"
export GEN_ROWS="${GEN_ROWS:-30000000}"
export GEN_SEED="${GEN_SEED:-42}"
# GEN_COLUMNS is overridden by COLUMN_SPEC's length; kept as a fallback.
export GEN_COLUMNS="${GEN_COLUMNS:-20}"

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

# --- Backup shape ------------------------------------------------------------
# No non-partitioned twin: only t_partitioned is in the combined backup.
export SKIP_NONPART_CLONE=1

# Seed the combined backup with PR-produced per-partition stats. Two effects:
#   1. Subsequent test cells start from a cluster that already has stats,
#      so part-single ANALYZE re-merges 7999 pre-existing + 1 fresh
#      partition (the realistic re-analyze pattern).
#   2. The seed-analyze run itself is profiled and captured as the
#      "no previous stats" baseline for PR — output-bench-v3/PR/seed-full-analyze/.
export SEED_ANALYZE_BINARY="${SCRIPT_DIR}/tidb-server.pr"

# --- Test matrix restriction -------------------------------------------------
# Only single-partition ANALYZEs, only with existing stats. Never DROP STATS.
export SCENARIOS_FILTER="part-single"
export STATES_FILTER="existing"

# --- Isolated artifacts so v3 doesn't clobber v2. Env-overridable so smaller
#     verification runs land in their own dirs.
export COMBINED_BACKUP_DIR="${COMBINED_BACKUP_DIR:-${SCRIPT_DIR}/combined-backup-v3}"
export OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output-bench-v3}"

exec "${SCRIPT_DIR}/bench.sh" "$@"
