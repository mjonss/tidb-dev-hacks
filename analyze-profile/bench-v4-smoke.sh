#!/usr/bin/env bash
# Smoke screening for the v4 setup. Same pipeline (generate → seed-analyze →
# combined backup → matrix) but tiny: 1M rows, 10 partitions, 4 columns,
# 1 iteration. Exercises every step end-to-end in minutes rather than hours.
#
# Run before bench-config-v4.sh prepare/run to validate the pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Tiny data ----------------------------------------------------------------
export GEN_ROWS=1000000
export GEN_PARTITIONS=10
export GEN_COLUMNS=4
export GEN_SEED=42

# 4 columns covering 4 distinct type×distribution shapes — enough to
# exercise the merge code path on each encoding family.
export COLUMN_SPEC="BIGINT:low-ndv,DOUBLE:uniform,DECIMAL:zipf,VARCHAR:per-part-categ"

# One single-column and one multi-column index, on small types so we don't
# trip auto-prefix logic.
export INDEXES="c1 c2,c3"

# 1 iter, both async values, both scenarios — same matrix shape as v4 just
# minus the iteration multiplier (= 2 scenarios × 2 async × 1 iter × 2 branches
# = 8 cells, each tiny).
export ITERATIONS=1
export CPU_PROFILE_SECONDS=3

# Isolated dirs so smoke artifacts don't collide with the real v4 run.
export COMBINED_BACKUP_DIR="${SCRIPT_DIR}/combined-backup-v4-smoke"
export OUTPUT_ROOT="${SCRIPT_DIR}/output-bench-v4-smoke"

exec "${SCRIPT_DIR}/bench-config-v4.sh" "$@"
