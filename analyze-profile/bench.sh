#!/usr/bin/env bash
# Benchmark ANALYZE TABLE on a PR vs its base, across scenarios.
#
# Matrix:
#   2 branches  (PR, BASE)
# × 3 scenarios (partitioned-full, partitioned-single, nonpartitioned-full)
# × 2 stats states (clean = drop-stats before run, existing = keep prior stats)
# × 2 async_merge values (ON, OFF)
# × ITERATIONS
#
# Fill in bench-config.sh first. Then:
#   ./bench.sh build           # build analyze-profile + sanity-check binaries
#   ./bench.sh setup           # start playground + restore backup + clone nonpart
#   ./bench.sh run [scope]     # run the matrix
#                              #   scope: all|pr|base|quick|smoke|escalate
#   ./bench.sh compare         # run compare-runs.py + profile-diff + accuracy-diff
#   ./bench.sh stop            # tear down playground
#   ./bench.sh all             # build + setup + run all + compare
#   ./bench.sh smoke           # shorthand: MINIMAL=1 build + setup + run + compare
#
# Scopes:
#   all      — every cell in the matrix (both branches, both async values)
#   pr       — PR branch only
#   base     — base branch only
#   quick    — one cell (part-full clean async=ON) on each branch
#   smoke    — triggers the MINIMAL=1 config (small backup, few cells)
#   escalate — runs async=ON only; if PR vs BASE shows regression, adds async=OFF

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/bench-config.sh"

RUN_MANIFEST="${OUTPUT_ROOT}/manifest.tsv"   # record of every run

TIUP="${TIUP:-tiup}"
PPROF="${PPROF:-go tool pprof}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# ---------------------------------------------------------------------------
# Playground lifecycle
# ---------------------------------------------------------------------------
# Each clean run gets a completely fresh playground (no --tag). BR refuses to
# restore into a non-fresh cluster, so reuse is not an option without
# destructive per-table drops. The PID file is the single source of truth for
# what's running.

PLAYGROUND_PID_FILE="${OUTPUT_ROOT}/playground.pid"
PLAYGROUND_LOG="${OUTPUT_ROOT}/playground.log"

playground_running() {
  [[ -f "${PLAYGROUND_PID_FILE}" ]] || return 1
  local pid
  pid=$(cat "${PLAYGROUND_PID_FILE}")
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

render_memory_configs() {
  local cfg_dir="${OUTPUT_ROOT}/configs"
  mkdir -p "${cfg_dir}"
  if [[ -z "${TIDB_CONFIG}" ]]; then
    TIDB_CONFIG="${cfg_dir}/tidb.toml"
    cat > "${TIDB_CONFIG}" <<EOF
# Rendered by bench.sh — limits TiDB memory to ${TIDB_MEM_GB} GB.
[performance]
server-memory-quota = $(( TIDB_MEM_GB * 1024 * 1024 * 1024 ))
EOF
  fi
  if [[ -z "${TIKV_CONFIG}" ]]; then
    TIKV_CONFIG="${cfg_dir}/tikv.toml"
    cat > "${TIKV_CONFIG}" <<EOF
# Rendered by bench.sh — limits TiKV memory to ${TIKV_MEM_GB} GB and relaxes
# disk-space reservations so it runs on nearly-full volumes.
[memory]
memory-usage-limit = "${TIKV_MEM_GB}GB"

[storage]
reserve-raft-space = ${TIKV_RESERVE_RAFT_SPACE}
reserve-space = ${TIKV_RESERVE_SPACE}
low-space-threshold = "${TIKV_LOW_SPACE_THRESHOLD}"

[storage.block-cache]
capacity = "${TIKV_BLOCK_CACHE_GB}GB"
EOF
  fi
  if [[ -z "${PD_CONFIG}" ]]; then
    PD_CONFIG="${cfg_dir}/pd.toml"
    cat > "${PD_CONFIG}" <<EOF
# Rendered by bench.sh — caps PD's share of system memory.
server-memory-limit = ${PD_MEM_FRACTION}
EOF
  fi
  log "Memory limits: TiDB=${TIDB_MEM_GB}GB, TiKV=${TIKV_MEM_GB}GB (cache ${TIKV_BLOCK_CACHE_GB}GB), PD=${PD_MEM_FRACTION}"
}

playground_start() {
  local bin="$1"
  [[ -x "$bin" ]] || die "tidb-server binary not executable: $bin"
  if playground_running; then
    die "playground still running (pid $(cat "${PLAYGROUND_PID_FILE}")) — stop first"
  fi
  render_memory_configs
  log "Starting fresh playground (tidb=${bin})"
  "${TIUP}" playground nightly \
    --db "${TIDB_NUM}" --kv "${KV_NUM}" --pd "${PD_NUM}" \
    --db.binpath "${bin}" \
    --db.config "${TIDB_CONFIG}" \
    --kv.config "${TIKV_CONFIG}" \
    --pd.config "${PD_CONFIG}" \
    >"${PLAYGROUND_LOG}" 2>&1 &
  echo $! > "${PLAYGROUND_PID_FILE}"
  # Wait up to 180s for TiDB to accept connections (fresh cluster takes longer).
  local i
  for i in $(seq 1 90); do
    if mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1" >/dev/null 2>&1; then
      log "playground ready"
      return
    fi
    sleep 2
  done
  die "playground did not come up in 180s (see ${PLAYGROUND_LOG})"
}

# Return the tiup-playground binary PID (the actual component that spawns
# pd/tikv/tidb and owns the data dir), not the tiup shim or any child.
# Signal delivery must target this PID directly — propagation through the
# `tiup` shim is not reliable.
#
# `|| true` guards against set -o pipefail + set -e: when pgrep finds nothing
# it exits 1, which under pipefail would abort the caller's assignment.
find_playground_pid() {
  pgrep -f '/tiup-playground ' 2>/dev/null | head -1 || true
}

# Parse the data dir (e.g. .tiup/data/VGxxxxx) out of a pd/tikv child's argv,
# for diagnostics in the log.
find_playground_data_dir() {
  local argv
  argv=$(pgrep -af 'pd-server.*\.tiup/data/' 2>/dev/null | head -1 || true)
  [[ -z "${argv}" ]] && return 0
  printf '%s\n' "${argv}" | sed -nE 's|.*(\.tiup/data/[^/]+)/.*|\1|p' | head -1 || true
}

# Find the TiDB log file path from the running tidb-server's --log-file arg.
find_tidb_log_path() {
  local argv
  argv=$(pgrep -af 'tidb-server.*--log-file=' 2>/dev/null | head -1 || true)
  [[ -z "${argv}" ]] && return 0
  printf '%s\n' "${argv}" | sed -nE 's|.*--log-file=([^ ]+).*|\1|p' | head -1 || true
}

# Find the TiKV log file path from the running tikv-server's --log-file arg.
find_tikv_log_path() {
  local argv
  argv=$(pgrep -af 'tikv-server.*--log-file=' 2>/dev/null | head -1 || true)
  [[ -z "${argv}" ]] && return 0
  printf '%s\n' "${argv}" | sed -nE 's|.*--log-file=([^ ]+).*|\1|p' | head -1 || true
}

# Wait for port 4000 to be released so the next playground_start doesn't
# collide. Returns 0 when free, non-zero on timeout.
wait_port_4000_free() {
  local i
  for i in $(seq 1 30); do
    (exec 3<>/dev/tcp/127.0.0.1/4000) 2>/dev/null && { exec 3>&- 3<&-; sleep 1; continue; }
    return 0
  done
  return 1
}

# Graceful shutdown, matching the signal contract in tiup's
# components/playground/main.go:
#   * SIGHUP/INT/TERM/QUIT all trigger p.terminate(sig), which signals each
#     child (pd/tikv/tidb) and SIGKILLs any that don't exit within 10s
#     (forceKillAfterDuration in playground.go).
#   * A SECOND SIGINT during shutdown calls terminate(SIGKILL) — skips the
#     10s grace and kills children immediately. Data dir is STILL cleaned up.
#   * Only SIGKILL'ing the tiup-playground process itself bypasses removeData
#     and orphans the data dir.
#
# So our ladder is: SIGINT → wait → SIGINT again (fast-path) → wait → SIGKILL
# tiup-playground as last resort. We do NOT pkill pd/tikv/tidb ourselves —
# that would pre-empt tiup's own shutdown logic and could leave instances
# half-stopped. We do not remove any data dirs.
playground_stop() {
  local pg_pid data_dir
  pg_pid=$(find_playground_pid)
  data_dir=$(find_playground_data_dir)

  if [[ -z "${pg_pid}" ]]; then
    log "No tiup-playground process running"
    rm -f "${PLAYGROUND_PID_FILE}"
    wait_port_4000_free || true
    return
  fi

  log "Stopping tiup-playground (pid=${pg_pid}, data_dir=${data_dir:-unknown})"

  # SIGINT — tiup's graceful path (10s per-child grace, then SIGKILL child).
  # With 1 pd + 1 tikv + 1 tidb this should complete in under 30s; give it 2
  # minutes for safety in case a tikv flush blocks its signal handler.
  kill -INT "${pg_pid}" 2>/dev/null || true
  local i
  for i in $(seq 1 60); do         # 60 × 2s = 2 minutes
    if ! kill -0 "${pg_pid}" 2>/dev/null; then
      log "tiup-playground exited cleanly after ~$((i*2))s"
      rm -f "${PLAYGROUND_PID_FILE}"
      wait_port_4000_free || true
      return
    fi
    sleep 2
  done

  # Second SIGINT — tiup treats this as "terminate(SIGKILL)", which
  # immediately SIGKILLs children while still running removeData afterward.
  log "SIGINT did not exit in 2m; sending second SIGINT (fast-kill children, still cleans dir)"
  kill -INT "${pg_pid}" 2>/dev/null || true
  for i in $(seq 1 20); do         # 20 × 2s = 40s
    if ! kill -0 "${pg_pid}" 2>/dev/null; then
      log "tiup-playground exited after second SIGINT"
      rm -f "${PLAYGROUND_PID_FILE}"
      wait_port_4000_free || true
      return
    fi
    sleep 2
  done

  # Last resort — SIGKILL tiup-playground itself. removeData will NOT run;
  # data dir is left behind for manual cleanup.
  log "Second SIGINT did not exit in 40s; SIGKILL'ing tiup-playground"
  log "  data dir ${data_dir:-unknown} will NOT be auto-cleaned — remove it manually"
  kill -KILL "${pg_pid}" 2>/dev/null || true
  rm -f "${PLAYGROUND_PID_FILE}"
  wait_port_4000_free || log "port 4000 still busy after stop"
}

# Full reload: stop any running cluster, start fresh with the given binary,
# restore the combined backup (built lazily on first call). Callers are
# responsible for warming caches — the matrix loop does it per iteration,
# and cmd_setup does it once for interactive use.
playground_reload() {
  local bin="$1"
  if [[ ! -f "${COMBINED_BACKUP_DIR}/backupmeta" ]]; then
    prepare_combined_backup "${bin}"
  fi
  playground_stop
  playground_start "${bin}"
  br_restore "${COMBINED_BACKUP_DIR}"
}

warmup() {
  log "Warming caches (SELECT COUNT(*) on both tables)"
  mysql -h 127.0.0.1 -P 4000 -u root <<SQL >/dev/null
SET SESSION tidb_mem_quota_query = 0;
SELECT COUNT(*) FROM \`${DB_NAME}\`.\`${PART_TABLE}\`;
SELECT COUNT(*) FROM \`${DB_NAME}\`.\`${NONPART_TABLE}\`;
SQL
}

# ---------------------------------------------------------------------------
# Data setup (one-time, once playground is running)
# ---------------------------------------------------------------------------

br_restore() {
  local src="$1"
  [[ -d "${src}" ]] || die "BR backup dir not found: ${src}"
  log "Restoring BR backup from ${src}"
  "${TIUP}" br:nightly restore full \
    --pd 127.0.0.1:2379 \
    --storage "local://${src}" \
    --log-file "${OUTPUT_ROOT}/br-restore.log" \
    || die "BR restore failed (see ${OUTPUT_ROOT}/br-restore.log)"
}

br_backup_combined() {
  mkdir -p "$(dirname "${COMBINED_BACKUP_DIR}")"
  # BR refuses to back up into a non-empty dir. Don't delete anything from
  # here ourselves — if a prior backup is present, surface it and let the
  # operator decide what to remove.
  if [[ -d "${COMBINED_BACKUP_DIR}" ]] && [[ -n "$(ls -A "${COMBINED_BACKUP_DIR}" 2>/dev/null)" ]]; then
    die "COMBINED_BACKUP_DIR is non-empty: ${COMBINED_BACKUP_DIR}
    Remove it (or use 'bench.sh prepare' after removing) before re-backing up."
  fi
  log "Backing up ${DB_NAME}.{${PART_TABLE},${NONPART_TABLE}} → ${COMBINED_BACKUP_DIR}"
  "${TIUP}" br:nightly backup db \
    --pd 127.0.0.1:2379 \
    --db "${DB_NAME}" \
    --storage "local://${COMBINED_BACKUP_DIR}" \
    --log-file "${OUTPUT_ROOT}/br-backup.log" \
    || die "BR backup failed (see ${OUTPUT_ROOT}/br-backup.log)"
}

generate_data() {
  log "Generating table via analyze-profile setup (rows=${GEN_ROWS} cols=${GEN_COLUMNS} parts=${GEN_PARTITIONS})"
  "${SCRIPT_DIR}/analyze-profile" setup \
    --db "${DB_NAME}" \
    --table "${PART_TABLE}" \
    --rows "${GEN_ROWS}" \
    --columns "${GEN_COLUMNS}" \
    --partitions "${GEN_PARTITIONS}" \
    --seed "${GEN_SEED}" \
    2> "${OUTPUT_ROOT}/generate.log" \
    || die "analyze-profile setup failed (see ${OUTPUT_ROOT}/generate.log)"
}

# Build the combined backup once: either restore the source backup or generate
# data via analyze-profile setup, then clone the non-partitioned twin, then
# back both tables up together. All subsequent reloads restore from the
# combined backup instead of rebuilding every time.
prepare_combined_backup() {
  local bin="$1"
  if [[ -f "${COMBINED_BACKUP_DIR}/backupmeta" ]]; then
    log "Combined backup already exists at ${COMBINED_BACKUP_DIR}"
    return
  fi
  log "Preparing combined backup (one-time)"
  playground_stop
  playground_start "${bin}"
  if [[ "${GENERATE_DATA}" == "1" ]]; then
    generate_data
  else
    br_restore "${BR_BACKUP_DIR}"
  fi
  clone_nonpartitioned
  br_backup_combined
  playground_stop
}

clone_nonpartitioned() {
  log "Creating non-partitioned twin ${DB_NAME}.${NONPART_TABLE}"
  # Use tidb_dml_type='bulk' + unlimited per-query mem quota so INSERT...SELECT
  # of 10M+ rows doesn't hit error 8175. The bulk DML mode streams rows in
  # batches without buffering the whole set in memory.
  mysql -h 127.0.0.1 -P 4000 -u root <<SQL
SET SESSION tidb_mem_quota_query = 0;
SET SESSION tidb_dml_type = 'bulk';
CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\`;
USE \`${DB_NAME}\`;
DROP TABLE IF EXISTS \`${NONPART_TABLE}\`;
CREATE TABLE \`${NONPART_TABLE}\` LIKE \`${PART_TABLE}\`;
ALTER TABLE \`${NONPART_TABLE}\` REMOVE PARTITIONING;
INSERT INTO \`${NONPART_TABLE}\` SELECT * FROM \`${PART_TABLE}\`;
SQL
  local n
  n=$(mysql -N -h 127.0.0.1 -P 4000 -u root -e \
       "SELECT COUNT(*) FROM ${DB_NAME}.${NONPART_TABLE}")
  log "Non-partitioned twin row count: ${n}"
}

# ---------------------------------------------------------------------------
# Profile runs
# ---------------------------------------------------------------------------

build_analyzer() {
  log "Building analyze-profile"
  (cd "${SCRIPT_DIR}" && go build -o analyze-profile .)
  [[ -x "${SCRIPT_DIR}/analyze-profile" ]] || die "analyze-profile build failed"
}

# Run a single profile. Args:
#   $1 label     (PR or BASE — used for output subdir)
#   $2 scenario  (part-full | part-single | nonpart-full)
#   $3 state     (clean | existing)
#   $4 async     (ON | OFF)
#   $5 iter      (1-based iteration number)
run_one_profile() {
  local label="$1" scenario="$2" state="$3" async="$4" iter="$5"
  local out_dir="${OUTPUT_ROOT}/${label}/${scenario}_${state}_async${async}_iter${iter}"
  mkdir -p "${out_dir}"

  local table part_flag=""
  case "${scenario}" in
    part-full)     table="${PART_TABLE}" ;;
    part-single)   table="${PART_TABLE}"; part_flag="--partition ${SINGLE_PARTITION}" ;;
    nonpart-full)  table="${NONPART_TABLE}" ;;
    *) die "unknown scenario ${scenario}" ;;
  esac

  local stats_flag=""
  if [[ "${state}" == "clean" ]]; then
    stats_flag="--drop-stats"
  fi

  local set_args=()
  for sv in "${COMMON_SET_VARS[@]}"; do
    set_args+=(--set-variable "${sv}")
  done
  set_args+=(--set-variable "tidb_enable_async_merge_global_stats=${async}")

  local analyze_columns_flag=()
  if [[ -n "${ANALYZE_COLUMNS}" ]]; then
    analyze_columns_flag=(--analyze-columns "${ANALYZE_COLUMNS}")
  fi

  log "RUN ${label} ${scenario} ${state} async=${async} iter=${iter} → ${out_dir}"
  "${SCRIPT_DIR}/analyze-profile" profile \
    --db "${DB_NAME}" --table "${table}" \
    ${part_flag} \
    ${stats_flag} \
    --check-accuracy \
    --verbose \
    --cpu-profile-seconds "${CPU_PROFILE_SECONDS}" \
    --output-dir "${out_dir}" \
    "${analyze_columns_flag[@]}" \
    "${set_args[@]}" \
    2> "${out_dir}/run.log" || log "WARN: analyze-profile returned non-zero (see ${out_dir}/run.log)"

  # analyze-profile creates a run_<ts> subdir under output-dir; capture it.
  local inner
  inner=$(find "${out_dir}" -mindepth 1 -maxdepth 1 -type d -name 'run_*' | head -1)
  if [[ -n "${inner}" ]]; then
    printf '%s\t%s\t%s\t%s\t%d\t%s\n' \
      "${label}" "${scenario}" "${state}" "${async}" "${iter}" "${inner}" \
      >> "${RUN_MANIFEST}"
    # Copy TiDB + TiKV logs into the run dir before the next reload
    # destroys the tiup data dir. Grab current + rotated segments.
    # Lumberjack (TiDB) rotates tidb.log → tidb-<timestamp>.log, so we
    # glob on the stem (tidb*) rather than the exact name (tidb.log*).
    local tidb_log tikv_log
    tidb_log=$(find_tidb_log_path)
    if [[ -n "${tidb_log}" && -f "${tidb_log}" ]]; then
      local log_dir log_stem
      log_dir=$(dirname "${tidb_log}")
      log_stem=$(basename "${tidb_log}" .log)   # "tidb" from "tidb.log"
      mkdir -p "${inner}/tidb-logs"
      cp "${log_dir}/${log_stem}"*.log* "${inner}/tidb-logs/" 2>/dev/null || true
    fi
    tikv_log=$(find_tikv_log_path)
    if [[ -n "${tikv_log}" && -f "${tikv_log}" ]]; then
      local log_dir log_stem
      log_dir=$(dirname "${tikv_log}")
      log_stem=$(basename "${tikv_log}" .log)   # "tikv" from "tikv.log"
      mkdir -p "${inner}/tikv-logs"
      cp "${log_dir}/${log_stem}"*.log* "${inner}/tikv-logs/" 2>/dev/null || true
    fi
  else
    log "WARN: no run_* dir under ${out_dir}"
  fi
}

# Run the matrix for one label (PR or BASE). For each cell we restart+reload
# the cluster before any "clean" run; "existing" runs immediately follow on
# the same cluster so prior stats exist. Matrix dimensions come from the
# SCENARIOS, STATES, ASYNC_MERGE_VALUES, ITERATIONS config.
#
# Inner loop per (scenario, async, iter):
#   1. reload (fresh playground + restore + clone + warmup)
#   2. run clean
#   3. warmup (drops caches via fresh BR? no — just touch pages again)
#   4. run existing
run_matrix_for_label() {
  local label="$1"
  local bin="$2"
  local i
  for scenario in "${SCENARIOS[@]}"; do
    for async in "${ASYNC_MERGE_VALUES[@]}"; do
      for i in $(seq 1 "${ITERATIONS}"); do
        # Decide whether this iteration needs a fresh cluster. If "clean" is
        # requested, it always does. If only "existing" is requested, still
        # reload so we start from a known state (ANALYZE once, discard, then
        # the second ANALYZE runs against those stats).
        local needs_reload=0
        local state
        for state in "${STATES[@]}"; do
          if [[ "${state}" == "clean" ]]; then needs_reload=1; fi
        done
        if [[ "${needs_reload}" -eq 1 || " ${STATES[*]} " != *" clean "* ]]; then
          playground_reload "${bin}"
        fi
        # Walk states in this order: clean first (so existing sees stats from
        # it), then existing.
        local have_stats=0
        for state in "${STATES[@]}"; do
          if [[ "${state}" == "existing" && "${have_stats}" -eq 0 ]]; then
            # No preceding clean run in STATES — build stats with an untimed
            # ANALYZE so the "existing" measurement has something to update.
            log "Priming stats for 'existing' run"
            mysql -h 127.0.0.1 -P 4000 -u root -e \
              "ANALYZE TABLE \`${DB_NAME}\`.\`${PART_TABLE}\`" >/dev/null
            mysql -h 127.0.0.1 -P 4000 -u root -e \
              "ANALYZE TABLE \`${DB_NAME}\`.\`${NONPART_TABLE}\`" >/dev/null
            have_stats=1
          fi
          warmup
          run_one_profile "${label}" "${scenario}" "${state}" "${async}" "${i}"
          have_stats=1
        done
      done
    done
  done
}

# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

# Build --group args for compare-runs.py from the manifest, filtered by
# scenario/state/async so each comparison is apples-to-apples.
compare_group() {
  local scenario="$1" state="$2" async="$3"
  local header="=== ${scenario} ${state} async=${async} ==="
  echo ""
  echo "${header}"
  local pr_dirs=() base_dirs=()
  while IFS=$'\t' read -r label scn st asy it dir; do
    [[ "${scn}" == "${scenario}" && "${st}" == "${state}" && "${asy}" == "${async}" ]] || continue
    if [[ "${label}" == "${LABEL_PR}" ]]; then pr_dirs+=("${dir}"); fi
    if [[ "${label}" == "${LABEL_BASE}" ]]; then base_dirs+=("${dir}"); fi
  done < "${RUN_MANIFEST}"
  if [[ ${#pr_dirs[@]} -eq 0 || ${#base_dirs[@]} -eq 0 ]]; then
    echo "  (skipped — missing runs: pr=${#pr_dirs[@]}, base=${#base_dirs[@]})"
    return
  fi
  python3 "${SCRIPT_DIR}/compare-runs.py" \
    --group "${LABEL_PR}" "${pr_dirs[@]}" \
    --group "${LABEL_BASE}" "${base_dirs[@]}" \
    | sed 's/^/  /'
  echo ""
  echo "  -- profile top-diff (CPU) --"
  python3 "${SCRIPT_DIR}/profile-diff.py" cpu \
    --group "${LABEL_PR}" "${pr_dirs[@]}" \
    --group "${LABEL_BASE}" "${base_dirs[@]}" \
    | sed 's/^/  /' || true
  echo ""
  echo "  -- profile top-diff (HEAP) --"
  python3 "${SCRIPT_DIR}/profile-diff.py" heap \
    --group "${LABEL_PR}" "${pr_dirs[@]}" \
    --group "${LABEL_BASE}" "${base_dirs[@]}" \
    | sed 's/^/  /' || true
  echo ""
  echo "  -- accuracy diff --"
  python3 "${SCRIPT_DIR}/accuracy-diff.py" \
    --group "${LABEL_PR}" "${pr_dirs[@]}" \
    --group "${LABEL_BASE}" "${base_dirs[@]}" \
    | sed 's/^/  /' || true
}

do_compare() {
  [[ -f "${RUN_MANIFEST}" ]] || die "no manifest at ${RUN_MANIFEST} — run 'run' first"
  for scenario in "${SCENARIOS[@]}"; do
    for async in "${ASYNC_MERGE_VALUES[@]}"; do
      for state in "${STATES[@]}"; do
        compare_group "${scenario}" "${state}" "${async}"
      done
    done
  done
}

# Check whether PR regressed vs BASE in the async=ON results. Returns 0 if a
# regression is detected (duration or peak RSS > 5% worse), non-zero otherwise.
detect_regression() {
  [[ -f "${RUN_MANIFEST}" ]] || return 1
  python3 - "${RUN_MANIFEST}" "${LABEL_PR}" "${LABEL_BASE}" <<'PY'
import json, os, statistics, sys
manifest, label_pr, label_base = sys.argv[1], sys.argv[2], sys.argv[3]
pr_dur, base_dur, pr_rss, base_rss = [], [], [], []
with open(manifest) as f:
    for line in f:
        label, scn, st, asy, it, d = line.rstrip().split("\t")
        if asy != "ON": continue
        p = os.path.join(d, "profile_result.json")
        if not os.path.exists(p): continue
        with open(p) as g:
            data = json.load(g)
        dur = data.get("analyze_duration", "0s")
        # crude parse: seconds from last numeric token
        import re
        tot = 0.0
        for m, v in re.findall(r"([\d\.]+)([a-z]+)", dur):
            v = {"h": 3600, "m": 60, "s": 1, "ms": 0.001}.get(v, 0)
            tot += float(m) * v
        rss = max((s.get("metrics", {}).get("process_resident_memory_bytes", 0)
                   for s in data.get("tidb_metrics", [])), default=0)
        (pr_dur if label == label_pr else base_dur).append(tot)
        (pr_rss if label == label_pr else base_rss).append(rss)
if not pr_dur or not base_dur:
    sys.exit(2)
pd_ = statistics.mean(pr_dur); bd = statistics.mean(base_dur)
pr_ = statistics.mean(pr_rss); br = statistics.mean(base_rss)
dur_pct = (pd_ - bd) / bd * 100 if bd else 0
rss_pct = (pr_ - br) / br * 100 if br else 0
print(f"async=ON: duration {label_pr}={pd_:.1f}s vs {label_base}={bd:.1f}s ({dur_pct:+.1f}%)", file=sys.stderr)
print(f"async=ON: peak RSS {label_pr}={pr_/1e9:.2f}GB vs {label_base}={br/1e9:.2f}GB ({rss_pct:+.1f}%)", file=sys.stderr)
sys.exit(0 if (dur_pct > 5 or rss_pct > 5) else 1)
PY
}

# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

cmd_build() {
  [[ -x "${BIN_PR}" ]] || die "BIN_PR not executable: ${BIN_PR}"
  [[ -x "${BIN_BASE}" ]] || die "BIN_BASE not executable: ${BIN_BASE}"
  build_analyzer
  log "Binaries OK:"
  log "  PR:   ${BIN_PR}"
  log "  BASE: ${BIN_BASE}"
  "${BIN_PR}" -V 2>&1 | head -4 | sed 's/^/    /' >&2 || true
  "${BIN_BASE}" -V 2>&1 | head -4 | sed 's/^/    /' >&2 || true
}

cmd_setup() {
  mkdir -p "${OUTPUT_ROOT}"
  build_analyzer
  # Leave a cluster up on BASE so users can poke at the data interactively.
  # Chain commands (all / smoke / run) do NOT go through cmd_setup — they
  # let run_matrix_for_label manage its own cluster lifecycle to avoid a
  # redundant stop/start/restore cycle before the first iteration.
  playground_reload "${BIN_BASE}"
  warmup
  log "Setup complete. Data in ${DB_NAME}.{${PART_TABLE},${NONPART_TABLE}}"
}

cmd_run() {
  local scope="${1:-all}"
  mkdir -p "${OUTPUT_ROOT}"
  # Truncate the manifest — run_matrix_for_label appends one line per cell,
  # and we want each top-level `run` invocation to start a fresh record.
  # Callers that want to preserve prior runs can skip cmd_run and invoke
  # run_matrix_for_label directly.
  : > "${RUN_MANIFEST}"
  build_analyzer
  case "${scope}" in
    all|both)
      run_matrix_for_label "${LABEL_BASE}" "${BIN_BASE}"
      run_matrix_for_label "${LABEL_PR}"   "${BIN_PR}"
      playground_stop
      ;;
    pr)   run_matrix_for_label "${LABEL_PR}"   "${BIN_PR}";   playground_stop ;;
    base) run_matrix_for_label "${LABEL_BASE}" "${BIN_BASE}"; playground_stop ;;
    quick)
      # Smoke: one scenario, one iter, clean state, each branch.
      playground_reload "${BIN_BASE}"
      warmup
      run_one_profile "${LABEL_BASE}" part-full clean ON 1
      playground_reload "${BIN_PR}"
      warmup
      run_one_profile "${LABEL_PR}"   part-full clean ON 1
      playground_stop
      ;;
    # (`all`/`pr`/`base`/`escalate` handled above)
    escalate)
      # Phase 1: async=ON only, full matrix.
      local saved_vals=("${ASYNC_MERGE_VALUES[@]}")
      ASYNC_MERGE_VALUES=("ON")
      run_matrix_for_label "${LABEL_BASE}" "${BIN_BASE}"
      run_matrix_for_label "${LABEL_PR}"   "${BIN_PR}"
      ASYNC_MERGE_VALUES=("${saved_vals[@]}")
      if [[ " ${ASYNC_MERGE_VALUES[*]} " == *" OFF "* ]] && detect_regression; then
        log "Regression detected in async=ON → running async=OFF follow-up"
        ASYNC_MERGE_VALUES=("OFF")
        run_matrix_for_label "${LABEL_BASE}" "${BIN_BASE}"
        run_matrix_for_label "${LABEL_PR}"   "${BIN_PR}"
      else
        log "No regression in async=ON — skipping async=OFF runs"
      fi
      playground_stop
      ;;
    *) die "unknown scope ${scope}" ;;
  esac
}

cmd_compare() { do_compare; }

cmd_stop() { playground_stop; }

cmd_prepare() {
  # Build the combined backup. If one already exists, surface it — don't
  # remove anything ourselves. Operator can rm the dir and re-run.
  mkdir -p "${OUTPUT_ROOT}"
  if [[ -f "${COMBINED_BACKUP_DIR}/backupmeta" ]]; then
    die "Combined backup already at ${COMBINED_BACKUP_DIR}.
    Remove it manually if you want to rebuild: rm -rf '${COMBINED_BACKUP_DIR}'"
  fi
  prepare_combined_backup "${BIN_BASE}"
  log "Combined backup ready at ${COMBINED_BACKUP_DIR}"
}

cmd_all() {
  cmd_build
  # Skip cmd_setup — cmd_run calls playground_reload itself, and a separate
  # setup reload would throw away the fresh cluster just to rebuild it.
  cmd_run all
  cmd_compare > "${OUTPUT_ROOT}/report.txt"
  log "Report: ${OUTPUT_ROOT}/report.txt"
}

cmd_smoke() {
  # End-to-end minimal test. Uses t_partitioned (small backup), 1 iter,
  # async=ON only, 2 scenarios, clean state only — see MINIMAL block in
  # bench-config.sh.
  if [[ "${MINIMAL:-0}" != "1" ]]; then
    log "Re-exec with MINIMAL=1 for smoke run"
    exec env MINIMAL=1 "$0" smoke
  fi
  log "SMOKE RUN — minimal matrix at ${OUTPUT_ROOT}"
  cmd_build
  cmd_run all
  cmd_compare | tee "${OUTPUT_ROOT}/report.txt"
  log "Smoke report: ${OUTPUT_ROOT}/report.txt"
}

case "${1:-}" in
  build)   shift; cmd_build "$@" ;;
  setup)   shift; cmd_setup "$@" ;;
  run)     shift; cmd_run "$@" ;;
  compare) shift; cmd_compare "$@" ;;
  stop)    shift; cmd_stop "$@" ;;
  prepare) shift; cmd_prepare "$@" ;;
  all)     shift; cmd_all "$@" ;;
  smoke)   shift; cmd_smoke "$@" ;;
  ""|help|-h|--help)
    sed -n '1,/^set -euo/p' "$0" | head -n -2 | sed 's/^# \{0,1\}//'
    ;;
  *) die "unknown command: $1 (try: build|setup|run|compare|stop|all)" ;;
esac
