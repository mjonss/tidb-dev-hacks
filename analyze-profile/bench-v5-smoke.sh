#!/usr/bin/env bash
# Smoke screening for the v5 setup. Same pipeline (generate -> seed-analyze ->
# combined backup -> matrix) but tiny: 1M rows, 10 partitions, 4 columns,
# 1 iteration. Exercises every step end-to-end in minutes rather than hours.
#
# Run before launching the v5 matrix — both binaries are freshly rebuilt
# (BASE f994e8da27, PR 8cf6bd31b5) and neither has been exercised yet.
#
# Note this generates its OWN tiny backup, so it does not tell you whether the
# 54-day-old combined-backup-v4 still restores. That is a separate check.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Tiny data ----------------------------------------------------------------
export GEN_ROWS=1000000
export GEN_PARTITIONS=10
export GEN_COLUMNS=4
export GEN_SEED=42

# 4 columns covering 4 distinct type x distribution shapes — enough to
# exercise the merge code path on each encoding family.
export COLUMN_SPEC="BIGINT:low-ndv,DOUBLE:uniform,DECIMAL:zipf,VARCHAR:per-part-categ"

export INDEXES="c1 c2,c3"

export ITERATIONS=1
export CPU_PROFILE_SECONDS=3

# Isolated dirs so smoke artifacts don't collide with the real v5 run.
export COMBINED_BACKUP_DIR="${SCRIPT_DIR}/combined-backup-v5-smoke"
export OUTPUT_ROOT="${SCRIPT_DIR}/output-bench-v5-smoke"

exec "${SCRIPT_DIR}/bench-config-v5.sh" "$@"
