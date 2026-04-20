#!/usr/bin/env bash
# Fast merge-iteration benchmark.
#
# One-time setup (bench-merge-iter.sh setup):
#   1. Start playground, generate 8K-partition table
#   2. ANALYZE TABLE (full) — populates all partition stats
#   3. BR backup full (data + stats)
#   4. Stop
#
# Each iteration (bench-merge-iter.sh run <binary> [label]):
#   1. Start fresh playground with the given binary
#   2. BR restore full (data + stats already present)
#   3. ANALYZE TABLE PARTITION p5 ALL COLUMNS — triggers global merge
#   4. Collect profiles, save results
#   5. Stop
#
# The full ANALYZE (~25 min) only runs once during setup. Each iteration
# takes ~2-3 min (restore + single-partition analyze + merge).
#
# Usage:
#   ./bench-merge-iter.sh setup                          # one-time (~30 min)
#   ./bench-merge-iter.sh run ./tidb-server.master BASE  # benchmark BASE
#   ./bench-merge-iter.sh run ./tidb-server.pr PR        # benchmark PR
#   ./bench-merge-iter.sh compare                        # diff BASE vs PR

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/bench-config.sh"

MERGE_ITER_DIR="${SCRIPT_DIR}/merge-iter"
MERGE_BACKUP="${MERGE_ITER_DIR}/backup"
MERGE_OUTPUT="${MERGE_ITER_DIR}/output"

TIUP="${TIUP:-tiup}"
PARTITION="${PARTITION:-p5}"
PARTITIONS="${GEN_PARTITIONS:-8000}"
ROWS="${GEN_ROWS:-30000000}"
COLUMNS="${GEN_COLUMNS:-17}"
SEED="${GEN_SEED:-42}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# --- Playground lifecycle (reuse from bench.sh) ---

find_playground_pid() {
  pgrep -f '/tiup-playground ' 2>/dev/null | head -1 || true
}

playground_stop() {
  local pg_pid
  pg_pid=$(find_playground_pid)
  if [[ -z "${pg_pid}" ]]; then
    log "No tiup-playground running"
    return
  fi
  log "Stopping tiup-playground (pid=${pg_pid})"
  kill -INT "${pg_pid}" 2>/dev/null || true
  local i
  for i in $(seq 1 60); do
    if ! kill -0 "${pg_pid}" 2>/dev/null; then
      log "tiup-playground exited after ~$((i*2))s"
      return
    fi
    sleep 2
  done
  kill -INT "${pg_pid}" 2>/dev/null || true
  sleep 10
  kill -KILL "${pg_pid}" 2>/dev/null || true
}

playground_start() {
  local bin="$1"
  [[ -x "$bin" ]] || die "binary not executable: $bin"
  playground_stop
  sleep 2

  mkdir -p "${MERGE_ITER_DIR}/configs"
  cat > "${MERGE_ITER_DIR}/configs/tidb.toml" <<EOF
[performance]
server-memory-quota = $((TIDB_MEM_GB * 1024 * 1024 * 1024))
EOF
  cat > "${MERGE_ITER_DIR}/configs/tikv.toml" <<EOF
[memory]
memory-usage-limit = "${TIKV_MEM_GB}GB"
[storage]
reserve-raft-space = 0
reserve-space = 0
low-space-threshold = "10GiB"
[storage.block-cache]
capacity = "${TIKV_BLOCK_CACHE_GB}GB"
EOF
  cat > "${MERGE_ITER_DIR}/configs/pd.toml" <<EOF
server-memory-limit = ${PD_MEM_FRACTION}
EOF

  log "Starting playground (tidb=${bin})"
  "${TIUP}" playground nightly \
    --db 1 --kv 1 --pd 1 \
    --db.binpath "${bin}" \
    --db.config "${MERGE_ITER_DIR}/configs/tidb.toml" \
    --kv.config "${MERGE_ITER_DIR}/configs/tikv.toml" \
    --pd.config "${MERGE_ITER_DIR}/configs/pd.toml" \
    >"${MERGE_ITER_DIR}/playground.log" 2>&1 &

  local i
  for i in $(seq 1 90); do
    if mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1" >/dev/null 2>&1; then
      log "playground ready"
      return
    fi
    sleep 2
  done
  die "playground did not start in 180s"
}

# --- Setup: one-time full ANALYZE + backup ---

cmd_setup() {
  local bin="${1:-${BIN_BASE}}"
  [[ -x "$bin" ]] || die "need a binary: ./bench-merge-iter.sh setup [binary]"

  mkdir -p "${MERGE_ITER_DIR}"

  if [[ -f "${MERGE_BACKUP}/backupmeta" ]]; then
    die "Backup already exists at ${MERGE_BACKUP}. Remove it to re-setup."
  fi

  # Reuse the existing combined-backup (has 8K-partition table + nonpart twin).
  local src_backup="${SCRIPT_DIR}/combined-backup"
  [[ -f "${src_backup}/backupmeta" ]] || die "No combined-backup found at ${src_backup}. Run bench.sh setup first."

  log "=== Setup: restore existing data → drop nonpart → full ANALYZE → backup with stats ==="
  playground_start "$bin"

  # Restore existing data
  log "Restoring from ${src_backup}"
  "${TIUP}" br:nightly restore full \
    --pd 127.0.0.1:2379 \
    --storage "local://${src_backup}" \
    --log-file "${MERGE_ITER_DIR}/br-restore-setup.log" \
    || die "BR restore failed"

  # Drop the non-partitioned table — not needed for merge iteration
  log "Dropping non-partitioned table"
  mysql -h 127.0.0.1 -P 4000 -u root -e \
    "DROP TABLE IF EXISTS analyze_profile.t_nonpart" 2>/dev/null || true

  # Full ANALYZE — populates all partition + global stats
  log "Running full ANALYZE TABLE ALL COLUMNS (this takes ~25 min for 8K partitions)"
  mysql -h 127.0.0.1 -P 4000 -u root <<SQL
SET GLOBAL tidb_analyze_version = 2;
SET GLOBAL tidb_partition_prune_mode = 'dynamic';
SET GLOBAL tidb_analyze_partition_concurrency = ${TIDB_ANALYZE_PARTITION_CONCURRENCY:-6};
SET GLOBAL tidb_build_stats_concurrency = 2;
SET GLOBAL tidb_build_sampling_stats_concurrency = 2;
ANALYZE TABLE analyze_profile.t_partitioned ALL COLUMNS;
SQL
  log "Full ANALYZE complete"

  # BR backup full (includes data + mysql.stats_* tables with fresh stats)
  log "Taking BR backup (data + stats)"
  "${TIUP}" br:nightly backup full \
    --pd 127.0.0.1:2379 \
    --storage "local://${MERGE_BACKUP}" \
    --ignore-stats=false \
    --log-file "${MERGE_ITER_DIR}/br-backup.log" \
    || die "BR backup failed"
  log "Backup saved to ${MERGE_BACKUP}"

  playground_stop
  log "=== Setup complete ==="
}

# --- Run: restore + single-partition ANALYZE ---

cmd_run() {
  local bin="${1:?Usage: bench-merge-iter.sh run <binary> [label]}"
  local label="${2:-$(basename "$bin" | sed 's/tidb-server[.-]*//')}"
  [[ -x "$bin" ]] || die "binary not executable: $bin"
  [[ -f "${MERGE_BACKUP}/backupmeta" ]] || die "No backup at ${MERGE_BACKUP}. Run setup first."

  local out_dir="${MERGE_OUTPUT}/${label}"
  mkdir -p "${out_dir}"

  log "=== Run: ${label} — restore + ANALYZE PARTITION ${PARTITION} ==="
  playground_start "$bin"

  # Restore (data + stats)
  log "Restoring backup (data + stats)"
  "${TIUP}" br:nightly restore full \
    --pd 127.0.0.1:2379 \
    --storage "local://${MERGE_BACKUP}" \
    --log-file "${out_dir}/br-restore.log" \
    || die "BR restore failed"

  # Disable auto-analyze to prevent TiDB from re-analyzing after restore
  mysql -h 127.0.0.1 -P 4000 -u root -e \
    "SET GLOBAL tidb_enable_auto_analyze = OFF" 2>/dev/null || true

  # Verify stats were restored
  local stats_count
  stats_count=$(mysql -N -h 127.0.0.1 -P 4000 -u root -e \
    "SELECT COUNT(*) FROM mysql.stats_meta sm JOIN information_schema.partitions p ON sm.table_id = p.TIDB_PARTITION_ID WHERE p.TABLE_SCHEMA = 'analyze_profile' AND p.TABLE_NAME = 't_partitioned' AND sm.count > 0" 2>/dev/null || echo "0")
  log "Stats restored: ${stats_count} partitions with stats"
  if [[ "${stats_count}" -eq 0 ]]; then
    die "No stats found after restore — backup may not include stats. Check --ignore-stats=false on backup and --with-sys-table=true on restore."
  fi

  # Warmup
  log "Warming caches"
  mysql -h 127.0.0.1 -P 4000 -u root -e \
    "SET SESSION tidb_mem_quota_query = 0; SELECT COUNT(*) FROM analyze_profile.t_partitioned" \
    >/dev/null

  # Profile the single-partition ANALYZE (triggers global stats merge)
  log "Running: ANALYZE TABLE PARTITION ${PARTITION} ALL COLUMNS"
  "${SCRIPT_DIR}/analyze-profile" profile \
    --db analyze_profile --table t_partitioned \
    --partition "${PARTITION}" \
    --analyze-columns all \
    --check-accuracy \
    --verbose \
    --cpu-profile-seconds 10 \
    --output-dir "${out_dir}" \
    --set-variable "tidb_analyze_version=2" \
    --set-variable "tidb_partition_prune_mode=dynamic" \
    --set-variable "tidb_analyze_partition_concurrency=6" \
    --set-variable "tidb_build_stats_concurrency=2" \
    --set-variable "tidb_build_sampling_stats_concurrency=2" \
    --set-variable "tidb_enable_async_merge_global_stats=ON" \
    2>"${out_dir}/run.log" || log "WARN: analyze-profile returned non-zero"

  # Save TiDB + TiKV logs before stopping (playground cleanup deletes them).
  # Copy into both out_dir and the run_* subdir (alongside profiles).
  local run_dir_for_logs
  run_dir_for_logs=$(find "${out_dir}" -maxdepth 1 -type d -name 'run_*' | head -1)
  local tidb_log tikv_log
  tidb_log=$(pgrep -af 'tidb-server.*--log-file=' 2>/dev/null | head -1 | sed -nE 's|.*--log-file=([^ ]+).*|\1|p' || true)
  if [[ -n "${tidb_log}" && -f "${tidb_log}" ]]; then
    local log_dir log_stem
    log_dir=$(dirname "${tidb_log}")
    log_stem=$(basename "${tidb_log}" .log)
    for dest in "${out_dir}/tidb-logs" "${run_dir_for_logs:+${run_dir_for_logs}/tidb-logs}"; do
      [[ -n "${dest}" ]] || continue
      mkdir -p "${dest}"
      cp "${log_dir}/${log_stem}"*.log* "${dest}/" 2>/dev/null || true
    done
  fi
  tikv_log=$(pgrep -af 'tikv-server.*--log-file=' 2>/dev/null | head -1 | sed -nE 's|.*--log-file=([^ ]+).*|\1|p' || true)
  if [[ -n "${tikv_log}" && -f "${tikv_log}" ]]; then
    local log_dir log_stem
    log_dir=$(dirname "${tikv_log}")
    log_stem=$(basename "${tikv_log}" .log)
    for dest in "${out_dir}/tikv-logs" "${run_dir_for_logs:+${run_dir_for_logs}/tikv-logs}"; do
      [[ -n "${dest}" ]] || continue
      mkdir -p "${dest}"
      cp "${log_dir}/${log_stem}"*.log* "${dest}/" 2>/dev/null || true
    done
  fi

  playground_stop

  # Print summary
  local run_dir
  run_dir=$(find "${out_dir}" -maxdepth 1 -type d -name 'run_*' | head -1)
  if [[ -n "${run_dir}" ]]; then
    python3 -c "
import json
with open('${run_dir}/profile_result.json') as f: d=json.load(f)
ns = d.get('analyze_duration_ns', 0)
ts = d.get('tidb_metrics', [])
peak_h = max((s.get('metrics',{}).get('go_memstats_heap_alloc_bytes',0) for s in ts), default=0)
peak_r = max((s.get('metrics',{}).get('process_resident_memory_bytes',0) for s in ts), default=0)
print(f'  ${label}: dur={ns/1e9:.3f}s  peak_heap={peak_h/1e9:.2f}GB  peak_rss={peak_r/1e9:.2f}GB')
# Check merge timing from analyze_jobs
jobs = d.get('partition_jobs', [])
merge_jobs = [j for j in jobs if 'merge global' in (j.get('partition_name','') + j.get('job_info','')).lower()]
for j in merge_jobs:
    print(f'  merge job: dur={j.get(\"duration\",\"?\")} state={j.get(\"state\",\"?\")}')
"
  fi

  log "=== ${label} complete. Output: ${out_dir} ==="
}

# --- Compare ---

cmd_compare() {
  log "=== Comparison ==="
  for d in "${MERGE_OUTPUT}"/*/; do
    label=$(basename "$d")
    run_dir=$(find "$d" -maxdepth 1 -type d -name 'run_*' | head -1)
    [[ -n "${run_dir}" ]] || continue
    python3 -c "
import json
with open('${run_dir}/profile_result.json') as f: d=json.load(f)
ns = d.get('analyze_duration_ns', 0)
ts = d.get('tidb_metrics', [])
peak_h = max((s.get('metrics',{}).get('go_memstats_heap_alloc_bytes',0) for s in ts), default=0)
peak_r = max((s.get('metrics',{}).get('process_resident_memory_bytes',0) for s in ts), default=0)
print(f'${label}:')
print(f'  duration:  {ns/1e9:.3f}s')
print(f'  peak_heap: {peak_h/1e9:.2f} GB')
print(f'  peak_rss:  {peak_r/1e9:.2f} GB')
jobs = d.get('partition_jobs', [])
for j in jobs:
    ji = j.get('job_info','')
    if 'merge' in ji.lower():
        print(f'  merge: dur={j.get(\"duration\",\"?\")} state={j.get(\"state\",\"?\")}')
        print(f'         {ji}')
"
  done
}

case "${1:-}" in
  setup)   shift; cmd_setup "$@" ;;
  run)     shift; cmd_run "$@" ;;
  compare) cmd_compare ;;
  stop)    playground_stop ;;
  *)
    echo "Usage:"
    echo "  $0 setup [binary]      # one-time: generate + full analyze + backup (~30 min)"
    echo "  $0 run <binary> [label] # restore + single-partition analyze (~2-3 min)"
    echo "  $0 compare              # show BASE vs PR results"
    echo "  $0 stop                 # stop playground"
    ;;
esac
