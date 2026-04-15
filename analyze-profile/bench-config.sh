#!/usr/bin/env bash
# Config for bench.sh — fill in the paths and tweak the matrix, then run bench.sh.
# bench.sh sources this file.

# --- Binaries ---------------------------------------------------------------
# Absolute paths to the two tidb-server binaries to compare.
# Build with: cd <worktree> && make && ls bin/tidb-server
BIN_PR="${BIN_PR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tidb-server.pr}"
BIN_BASE="${BIN_BASE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tidb-server.master}"

LABEL_PR="${LABEL_PR:-PR}"
LABEL_BASE="${LABEL_BASE:-BASE}"

# --- tiup playground topology ----------------------------------------------
# Keep cluster small and deterministic. Data persists across tidb binary swaps
# because TiKV/PD keep running between iterations.
PLAYGROUND_TAG="${PLAYGROUND_TAG:-analyze-bench}"
PD_NUM=1
KV_NUM=1
TIDB_NUM=1
# Give TiDB enough memory; ANALYZE on 8000 partitions can use >8GB.
TIDB_CONFIG="${TIDB_CONFIG:-}"   # optional path to tidb.toml

# --- Data / tables ----------------------------------------------------------
DB_NAME="${DB_NAME:-analyze_profile}"
# Partitioned table: restored from BR backup. Non-partitioned twin is cloned via
# ALTER ... REMOVE PARTITIONING after restore. Same data, same values.
PART_TABLE="${PART_TABLE:-t_partitioned}"
NONPART_TABLE="${NONPART_TABLE:-t_nonpart}"

# BR backup directory (relative to this script, or absolute).
# Looking at the repo: t_partitioned/ is a 3750-row-per-partition / small backup,
# t_partitioned-8000/ is the 8000-partition / 30M-row backup, sample-global-stats/
# is the big sample-based test backup. Pick one.
BR_BACKUP_DIR="${BR_BACKUP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sample-global-stats}"

# Combined backup directory — built once on first reload (or via
# `./bench.sh prepare`) by restoring BR_BACKUP_DIR, cloning the non-partitioned
# twin, and re-backing up both tables together. All subsequent reloads restore
# from this combined backup, skipping the slow INSERT...SELECT clone step.
COMBINED_BACKUP_DIR="${COMBINED_BACKUP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/combined-backup}"

# Which partition to use for the "single partition" tests.
SINGLE_PARTITION="${SINGLE_PARTITION:-p5}"

# --- Profile matrix ---------------------------------------------------------
# Iterations per (branch, scenario, async-merge) combination.
ITERATIONS="${ITERATIONS:-2}"

# Test both on and off for the PR's relevant variable.
# Override to just ("ON") for a faster matrix; use "run escalate" to only add
# OFF if ON shows a regression.
ASYNC_MERGE_VALUES=("ON" "OFF")

# Scenarios to run, in order. Override to shrink the matrix, e.g.
# SCENARIOS=("part-full" "nonpart-full") to skip single-partition.
# Valid values: part-full, part-single, nonpart-full.
SCENARIOS=("part-full" "part-single" "nonpart-full")

# Stats-state values. Override to e.g. ("clean") to skip warm-stats runs.
STATES=("clean" "existing")

# --- Minimal test config ----------------------------------------------------
# Set MINIMAL=1 (or run `./bench.sh run smoke`) to use a tiny matrix that
# exercises the entire pipeline end-to-end in a few minutes. Useful for
# validating the workflow before committing to the real matrix.
#
# The minimal config:
#   - uses the small t_partitioned backup (not t_partitioned-8000)
#   - runs a single iteration
#   - tests only async=ON
#   - only two scenarios (part-full, nonpart-full), clean state only
#   - 3-second CPU profile (vs 10s)
if [[ "${MINIMAL:-0}" == "1" ]]; then
  # Generate fresh data instead of restoring a BR backup — way faster for a
  # smoke test. analyze-profile setup builds a tiny table directly.
  GENERATE_DATA=1
  GEN_ROWS=1000000
  GEN_COLUMNS=10
  GEN_PARTITIONS=20
  GEN_SEED=42
  ITERATIONS=1
  ASYNC_MERGE_VALUES=("ON")
  SCENARIOS=("part-full" "nonpart-full")
  STATES=("clean")
  CPU_PROFILE_SECONDS=3
  OUTPUT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/output-bench-minimal"
  COMBINED_BACKUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/combined-backup-minimal"
fi

# When GENERATE_DATA=1, `prepare_combined_backup` calls
# `analyze-profile setup` with these parameters instead of BR-restoring
# BR_BACKUP_DIR.
GENERATE_DATA="${GENERATE_DATA:-0}"
GEN_ROWS="${GEN_ROWS:-1000000}"
GEN_COLUMNS="${GEN_COLUMNS:-10}"
GEN_PARTITIONS="${GEN_PARTITIONS:-20}"
GEN_SEED="${GEN_SEED:-42}"

# Session vars applied to every run (before ANALYZE). One per line, "name=value".
# Defaults match the sample-based-accuracy.md setup.
COMMON_SET_VARS=(
  "tidb_analyze_version=2"
  "tidb_partition_prune_mode=dynamic"
  "tidb_analyze_partition_concurrency=2"
  "tidb_build_stats_concurrency=2"
  "tidb_build_sampling_stats_concurrency=2"
  "tidb_merge_partition_stats_concurrency=1"
)

# --- Profile collection -----------------------------------------------------
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-10}"

# Output root — each run gets its own timestamped subdir under a per-branch
# folder, e.g. output-bench/PR/run_20260415_.../
OUTPUT_ROOT="${OUTPUT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/output-bench}"
