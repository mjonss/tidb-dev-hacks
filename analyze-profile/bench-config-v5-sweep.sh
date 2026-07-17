#!/usr/bin/env bash
# v5 concurrency sweep config: BASE only, varying
# tidb_merge_partition_stats_concurrency.
#
# Why this exists:
#   The PR (821ab3de59) deprecates tidb_merge_partition_stats_concurrency to a
#   no-op. BASE's default for it is 1 (DefTiDBMergePartitionStatsConcurrency),
#   and globalstats/topn.go dispatches `if mergeConcurrency < 2` to the
#   single-threaded MergePartTopN2GlobalTopN. So the main v5 matrix measures
#   BASE pinned to ONE core of a 16-core box, against a PR that deletes the very
#   knob that would parallelize it. Claiming a speedup on that basis would be
#   the same class of error as v4's stale BASE: flattering the PR for reasons
#   unrelated to its algorithm.
#
#   This sweep answers "what was the knob worth?" — in wall-clock AND in memory
#   (BASE's worker pool holds per-worker partial merge state, so high
#   concurrency should cost RAM; that is the honest baseline for the PR's
#   memory win).
#
# Shape: BASE only, 2 scenarios x 2 async x c={2,16}, 1 iteration = 8 cells.
#   c=1 is NOT swept — it is the default, so the main matrix's 8 BASE groups
#   already cover it (at 2 iters).
#
# Uses bench-sweep.sh, a SNAPSHOT fork of bench.sh taken 2026-07-15 while the
# v5 matrix was mid-run — bash re-reads a script from a file offset as it
# executes, so editing the live bench.sh would have risked corrupting the
# running matrix. The fork adds only the set_merge_concurrency hook. Fold that
# hook back into bench.sh and delete this fork once the matrix is done.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Data shape: identical to v5 so cells are comparable ---------------------
export GENERATE_DATA=1
export GEN_PARTITIONS="${GEN_PARTITIONS:-8192}"
export GEN_ROWS="${GEN_ROWS:-40000000}"
export GEN_SEED="${GEN_SEED:-42}"
export GEN_COLUMNS="${GEN_COLUMNS:-20}"

export COLUMN_SPEC="${COLUMN_SPEC:-BIGINT:low-ndv,INT UNSIGNED:zipf,BIGINT UNSIGNED:uniform:NOTNULL,BIGINT:per-part-categ:NULL(40),\
DOUBLE:low-ndv,DOUBLE:zipf,DOUBLE:uniform,DOUBLE:per-part-categ,\
DECIMAL:low-ndv,DECIMAL:zipf,DECIMAL:uniform,DECIMAL:per-part-categ,\
VARCHAR:low-ndv,VARCHAR:zipf,VARCHAR:uniform:NOTNULL,VARCHAR:per-part-categ,\
DATETIME:low-ndv,DATETIME:zipf,TIMESTAMP:uniform,DATETIME:per-part-categ:NULL(40)}"

export INDEXES="${INDEXES:-c14 c2,c19}"
export SKIP_NONPART_CLONE=1

export SCENARIOS_FILTER="${SCENARIOS_FILTER:-part-single part-full}"
export STATES_FILTER="${STATES_FILTER:-existing}"

# One iteration per cell: this is a scaling probe, and the effect being measured
# (1 -> 2 -> 16 workers) should dwarf run-to-run noise. Re-run a specific cell
# with ITERATIONS=2 if any result lands close enough to matter.
export ITERATIONS="${ITERATIONS:-1}"

# --- Artifacts: same 25 GB backup, separate output root ----------------------
export COMBINED_BACKUP_DIR="${COMBINED_BACKUP_DIR:-${SCRIPT_DIR}/combined-backup-v4}"
export OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output-bench-v5-sweep}"

exec "${SCRIPT_DIR}/bench-sweep.sh" "$@"
