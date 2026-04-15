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

playground_start() {
  local bin="$1"
  [[ -x "$bin" ]] || die "tidb-server binary not executable: $bin"
  if playground_running; then
    die "playground still running (pid $(cat "${PLAYGROUND_PID_FILE}")) — stop first"
  fi
  log "Starting fresh playground (tidb=${bin})"
  local extra=()
  [[ -n "${TIDB_CONFIG}" ]] && extra+=(--db.config "${TIDB_CONFIG}")
  "${TIUP}" playground nightly \
    --db "${TIDB_NUM}" --kv "${KV_NUM}" --pd "${PD_NUM}" \
    --db.binpath "${bin}" \
    "${extra[@]}" \
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

playground_stop() {
  local pid=""
  if [[ -f "${PLAYGROUND_PID_FILE}" ]]; then
    pid=$(cat "${PLAYGROUND_PID_FILE}")
  fi
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    log "Stopping playground (pid ${pid})"
    # SIGINT asks tiup playground to shut down cleanly (PD + TiKV + TiDB).
    kill -INT "${pid}" 2>/dev/null || true
    local i
    for i in $(seq 1 60); do
      if ! kill -0 "${pid}" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "${pid}" 2>/dev/null; then
      log "playground did not exit on SIGINT; sending SIGKILL"
      kill -9 "${pid}" 2>/dev/null || true
    fi
  else
    log "No tracked playground pid; checking for stragglers"
  fi
  # Unconditional sweep — `tiup playground` spawns `tiup-playground`
  # (the real component binary) plus pd/tikv/tidb children, any of which may
  # outlive the shim we tracked. Match on the .tiup/data/ path shared by every
  # tiup-launched process, since the actual tidb binary name is configurable
  # (BIN_PR/BIN_BASE) and won't match a fixed string like 'bin/tidb-server'.
  pkill -INT  -f 'tiup-playground'       2>/dev/null || true
  pkill -INT  -f '\.tiup/data/'          2>/dev/null || true
  pkill -INT  -f 'tidb-server.*--store=tikv' 2>/dev/null || true
  pkill -INT  -f 'tikv-server'           2>/dev/null || true
  pkill -INT  -f 'pd-server'             2>/dev/null || true
  sleep 3
  pkill -KILL -f 'tiup-playground'       2>/dev/null || true
  pkill -KILL -f '\.tiup/data/'          2>/dev/null || true
  pkill -KILL -f 'tidb-server.*--store=tikv' 2>/dev/null || true
  pkill -KILL -f 'tikv-server'           2>/dev/null || true
  pkill -KILL -f 'pd-server'             2>/dev/null || true
  rm -f "${PLAYGROUND_PID_FILE}"
  # Wait for port 4000 to be free so the next start doesn't collide.
  local j
  for j in $(seq 1 30); do
    if ! (exec 3<>/dev/tcp/127.0.0.1/4000) 2>/dev/null; then break; fi
    sleep 1
  done
  sleep 2
}

# Full reload: stop any running cluster, start fresh with the given binary,
# restore the combined backup (built lazily on first call), warm caches.
playground_reload() {
  local bin="$1"
  if [[ ! -f "${COMBINED_BACKUP_DIR}/backupmeta" ]]; then
    prepare_combined_backup "${bin}"
  fi
  playground_stop
  playground_start "${bin}"
  br_restore "${COMBINED_BACKUP_DIR}"
  warmup
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
  # BR refuses to back up into a non-empty dir.
  rm -rf "${COMBINED_BACKUP_DIR}"
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

  log "RUN ${label} ${scenario} ${state} async=${async} iter=${iter} → ${out_dir}"
  "${SCRIPT_DIR}/analyze-profile" profile \
    --db "${DB_NAME}" --table "${table}" \
    ${part_flag} \
    ${stats_flag} \
    --check-accuracy \
    --cpu-profile-seconds "${CPU_PROFILE_SECONDS}" \
    --output-dir "${out_dir}" \
    "${set_args[@]}" \
    2> "${out_dir}/run.log" || log "WARN: analyze-profile returned non-zero (see ${out_dir}/run.log)"

  # analyze-profile creates a run_<ts> subdir under output-dir; capture it.
  local inner
  inner=$(find "${out_dir}" -mindepth 1 -maxdepth 1 -type d -name 'run_*' | head -1)
  if [[ -n "${inner}" ]]; then
    printf '%s\t%s\t%s\t%s\t%d\t%s\n' \
      "${label}" "${scenario}" "${state}" "${async}" "${iter}" "${inner}" \
      >> "${RUN_MANIFEST}"
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
  : > "${RUN_MANIFEST}"   # truncate; runs re-append
  build_analyzer
  # Do one reload on BASE so users can poke at the cluster interactively.
  # The real matrix will reload again per iteration.
  playground_reload "${BIN_BASE}"
  log "Setup complete. Data in ${DB_NAME}.{${PART_TABLE},${NONPART_TABLE}}"
}

cmd_run() {
  local scope="${1:-all}"
  mkdir -p "${OUTPUT_ROOT}"
  touch "${RUN_MANIFEST}"
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
  # Rebuild the combined backup from scratch (idempotent: deletes existing).
  mkdir -p "${OUTPUT_ROOT}"
  rm -rf "${COMBINED_BACKUP_DIR}"
  prepare_combined_backup "${BIN_BASE}"
  log "Combined backup ready at ${COMBINED_BACKUP_DIR}"
}

cmd_all() {
  cmd_build
  cmd_setup
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
  cmd_setup
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
