#!/usr/bin/env bash
# v4 benchmark config: PR improve-global-stats-3 (a9d5f61a7a) vs its merge-base
# on master (749c973e5a). Same shape as v3 but with bigger data and a wider
# matrix.
#
# Shape of the v4 run:
#   - 8192-partition (TiDB HASH max), 40M-row, 20-column mixed-type table.
#     Same column spec as v3 — 5 encoding-path equivalence classes × 4
#     informative distributions.
#   - No t_nonpart twin (SKIP_NONPART_CLONE=1).
#   - Combined backup seeded with PR-produced per-partition stats via
#     `seed_analyze` during prepare.
#   - Matrix expanded vs v3: SCENARIOS_FILTER="part-single part-full"
#     (still existing-stats only, both async values, 2 iters).
#     2 scenarios × 1 state × 2 async × 2 iters × 2 branches = 16 cells.
#
# Usage:
#   ./bench-config-v4.sh build      # build analyze-profile + sanity-check binaries
#   ./bench-config-v4.sh prepare    # one-time generate + seed-ANALYZE + BR backup
#   ./bench-config-v4.sh run all    # full matrix
#   ./bench-config-v4.sh compare > output-bench-v4/report.txt
#   ./bench-config-v4.sh all        # build + run + compare
#
# Smoke screening:
#   The wrapper honours GEN_ROWS, GEN_PARTITIONS, COLUMN_SPEC, INDEXES,
#   OUTPUT_ROOT, COMBINED_BACKUP_DIR, ITERATIONS and ASYNC_MERGE env
#   overrides — see bench-v4-smoke.sh for a small validation run that
#   exercises the full pipeline in minutes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Generation: 8192 HASH partitions (TiDB max), 40M rows ------------------
export GENERATE_DATA=1
export GEN_PARTITIONS="${GEN_PARTITIONS:-8192}"
export GEN_ROWS="${GEN_ROWS:-40000000}"
export GEN_SEED="${GEN_SEED:-42}"
export GEN_COLUMNS="${GEN_COLUMNS:-20}"

# --- Per-column spec (identical to v3) ---------------------------------------
# 20 columns covering 5 encoding-path equivalence classes × 4 distributions,
# with NOT NULL and high-NULL paths represented.
export COLUMN_SPEC="${COLUMN_SPEC:-BIGINT:low-ndv,INT UNSIGNED:zipf,BIGINT UNSIGNED:uniform:NOTNULL,BIGINT:per-part-categ:NULL(40),\
DOUBLE:low-ndv,DOUBLE:zipf,DOUBLE:uniform,DOUBLE:per-part-categ,\
DECIMAL:low-ndv,DECIMAL:zipf,DECIMAL:uniform,DECIMAL:per-part-categ,\
VARCHAR:low-ndv,VARCHAR:zipf,VARCHAR:uniform:NOTNULL,VARCHAR:per-part-categ,\
DATETIME:low-ndv,DATETIME:zipf,TIMESTAMP:uniform,DATETIME:per-part-categ:NULL(40)}"

# --- Indexes (same as v3) ----------------------------------------------------
export INDEXES="${INDEXES:-c14 c2,c19}"

# --- Backup shape ------------------------------------------------------------
export SKIP_NONPART_CLONE=1

# Seed the combined backup with PR-produced per-partition stats.
export SEED_ANALYZE_BINARY="${SEED_ANALYZE_BINARY:-${SCRIPT_DIR}/tidb-server.pr}"

# --- Test matrix -------------------------------------------------------------
# Wider than v3 — adds part-full (full-table ANALYZE) to the existing
# part-single. Still existing-stats only.
export SCENARIOS_FILTER="${SCENARIOS_FILTER:-part-single part-full}"
export STATES_FILTER="${STATES_FILTER:-existing}"
# ASYNC_MERGE_VALUES default is ON + OFF — honor caller's ASYNC_MERGE env.

# --- Isolated artifacts ------------------------------------------------------
export COMBINED_BACKUP_DIR="${COMBINED_BACKUP_DIR:-${SCRIPT_DIR}/combined-backup-v4}"
export OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output-bench-v4}"

exec "${SCRIPT_DIR}/bench.sh" "$@"
