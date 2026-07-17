#!/usr/bin/env bash
# v5 benchmark config: PR improve-global-stats-3 (8cf6bd31b5) vs its TRUE
# merge-base on master (f994e8da27, 2026-07-15).
#
# Why v5 exists — the v4 BASE was wrong:
#   v4 built its BASE binary from master 749c973e5a (2025-11-04) and documented
#   it as "the PR's merge-base". It never was. Both PR HEADs benchmarked in v4
#   (a9d5f61a7a, 63d8b20af9) had merge-base e53c50c1c5 (2026-05-22) — 1037
#   commits newer, 63 of them touching pkg/statistics. So v4's PR-vs-BASE delta
#   credited the PR with ~6.5 months of unrelated master work, notably
#   296420ee5b "use clustered PRIMARY KEY for stats system tables" (#68324),
#   which speeds the per-partition stats load that dominates part-full
#   wallclock. v4's 5.47x part-full headline is not trustworthy; v3, which used
#   the correct base, measured 1.86x on the same path.
#
#   Root cause of the mistake: /home/mattias/repos/tidb-base has a *local*
#   branch `master` pinned at 749c973e5a while pingcap/master is current, so
#   `git merge-base <pr> master` in that worktree silently returns a stale
#   commit. ALWAYS resolve the base against pingcap/master, and confirm with
#   `tidb-server -V` that BASE and PR report a similar `pre-NNNN` count
#   (v4's skew was visible as BASE pre-716 vs PR pre-1759).
#
# Shape (unchanged from v4, so results stay comparable):
#   - 8192 HASH partitions (TiDB max), 40M rows, 20 columns
#     (5 encoding-path equivalence classes x 4 distributions) + 2 indexes.
#   - No t_nonpart twin (SKIP_NONPART_CLONE=1).
#   - Combined backup seeded with per-partition stats. Per-partition stats are
#     algorithm-independent (only the global merge differs BASE->PR), so both
#     branches restoring the same backup is a fair comparison.
#   - Matrix: 2 scenarios x 1 state x 2 async x 2 iters x 2 branches = 16 cells.
#
# Backup reuse:
#   COMBINED_BACKUP_DIR defaults to combined-backup-v4 (2026-05-22, 25 GB) for
#   a reuse attempt. It is ~54 days old and v3's notes warned that a 17-day-old
#   backup was already near the playground's GC safepoint TTL ceiling. Validate
#   with a restore before committing to the matrix; if it fails, regenerate:
#     COMBINED_BACKUP_DIR=$PWD/combined-backup-v5 ./bench-config-v5.sh prepare
#
# Usage:
#   ./bench-config-v5.sh build      # build analyze-profile + sanity-check binaries
#   ./bench-config-v5.sh prepare    # ONLY if regenerating the backup
#   ./bench-config-v5.sh run all    # full matrix
#   ./bench-config-v5.sh compare > output-bench-v5/report.txt

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Generation: 8192 HASH partitions (TiDB max), 40M rows ------------------
export GENERATE_DATA=1
export GEN_PARTITIONS="${GEN_PARTITIONS:-8192}"
export GEN_ROWS="${GEN_ROWS:-40000000}"
export GEN_SEED="${GEN_SEED:-42}"
export GEN_COLUMNS="${GEN_COLUMNS:-20}"

# --- Per-column spec (identical to v3/v4) ------------------------------------
export COLUMN_SPEC="${COLUMN_SPEC:-BIGINT:low-ndv,INT UNSIGNED:zipf,BIGINT UNSIGNED:uniform:NOTNULL,BIGINT:per-part-categ:NULL(40),\
DOUBLE:low-ndv,DOUBLE:zipf,DOUBLE:uniform,DOUBLE:per-part-categ,\
DECIMAL:low-ndv,DECIMAL:zipf,DECIMAL:uniform,DECIMAL:per-part-categ,\
VARCHAR:low-ndv,VARCHAR:zipf,VARCHAR:uniform:NOTNULL,VARCHAR:per-part-categ,\
DATETIME:low-ndv,DATETIME:zipf,TIMESTAMP:uniform,DATETIME:per-part-categ:NULL(40)}"

# --- Indexes (same as v3/v4) -------------------------------------------------
export INDEXES="${INDEXES:-c14 c2,c19}"

# --- Backup shape ------------------------------------------------------------
export SKIP_NONPART_CLONE=1

# Only consulted by `prepare` (i.e. when regenerating the backup).
export SEED_ANALYZE_BINARY="${SEED_ANALYZE_BINARY:-${SCRIPT_DIR}/tidb-server.pr}"

# --- Test matrix -------------------------------------------------------------
export SCENARIOS_FILTER="${SCENARIOS_FILTER:-part-single part-full}"
export STATES_FILTER="${STATES_FILTER:-existing}"
# ASYNC_MERGE_VALUES default is ON + OFF — honor caller's ASYNC_MERGE env.

# --- Isolated artifacts ------------------------------------------------------
# Reuse the v4 backup by default (see "Backup reuse" above); fresh output root.
export COMBINED_BACKUP_DIR="${COMBINED_BACKUP_DIR:-${SCRIPT_DIR}/combined-backup-v4}"
export OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output-bench-v5}"

exec "${SCRIPT_DIR}/bench.sh" "$@"
