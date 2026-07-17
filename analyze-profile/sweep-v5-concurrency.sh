#!/usr/bin/env bash
# BASE-only sweep of tidb_merge_partition_stats_concurrency, queued to start
# once matrix-v5 finishes. See bench-config-v5-sweep.sh for the rationale.
#
# 2 scenarios x 2 async x c={2,16}, 1 iter, BASE only = 8 cells.
# c=1 is the default and is already covered by matrix-v5's BASE groups.
#
# Idempotent per (scenario, async, concurrency) cell, so it doubles as its own
# resume after a kill.
#
# Launch (waits for matrix-v5 on its own — safe to start immediately):
#   systemd-run --user --unit=sweep-v5 --collect \
#     --working-directory=$PWD bash sweep-v5-concurrency.sh
#
# Watch:
#   journalctl --user -u sweep-v5 -f --no-pager

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"
OUT="${SCRIPT_DIR}/output-bench-v5-sweep"

export PATH="/home/mattias/go/bin:/home/mattias/.tiup/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export GOROOT="/home/mattias/go1.26.3"
export GOPATH="/home/mattias/go"

echo "=== SWEEP queued $(date) — waiting for matrix-v5 to finish ==="

# Wait for the matrix. Match the state EXACTLY: `systemctl is-active | grep
# active` is a trap, since "inactive" contains the substring "active".
while true; do
  state="$(systemctl --user is-active matrix-v5.service 2>/dev/null || true)"
  case "${state}" in
    active|activating|reloading|deactivating) sleep 60 ;;
    *) echo "matrix-v5 state='${state}' → proceeding $(date)"; break ;;
  esac
done

# The matrix may have exited leaving a playground up (e.g. if it was killed).
# playground_start refuses to run if one is already alive, which would fail
# every cell, so clear it first. Ignore failure: usually there is nothing here.
./bench-config-v5-sweep.sh stop >/dev/null 2>&1 || true

echo "=== SWEEP start $(date) ==="
./tidb-server.master -V | head -3

cell_done() {
  local scn="$1" asy="$2" conc="$3"
  compgen -G "${OUT}/c${conc}/BASE/${scn}_existing_async${asy}_iter1/run_*/profile_result.json" >/dev/null 2>&1
}

run_cell() {
  local scn="$1" asy="$2" conc="$3"
  if cell_done "$scn" "$asy" "$conc"; then
    echo "=== SKIP BASE ${scn} async=${asy} concurrency=${conc} (already done) ==="
    return 0
  fi
  echo "=== RUN BASE ${scn} async=${asy} concurrency=${conc} $(date) ==="
  # Separate OUTPUT_ROOT per concurrency so cells never collide and each level
  # can be compared independently.
  OUTPUT_ROOT="${OUT}/c${conc}" \
  MERGE_STATS_CONCURRENCY="${conc}" \
  SCENARIOS_FILTER="${scn}" STATES_FILTER="existing" ASYNC_MERGE="${asy}" \
    ./bench-config-v5-sweep.sh run base
  echo "BASE ${scn} async=${asy} c=${conc} exit=$? $(date)"
}

# Cheapest / most informative first: part-single at high concurrency is the
# fastest cell and proves the knob engages at all before we spend hours on it.
for conc in 16 2; do
  run_cell part-single ON  "${conc}"
  run_cell part-single OFF "${conc}"
  run_cell part-full   ON  "${conc}"
  run_cell part-full   OFF "${conc}"
done

echo "=== SWEEP summary $(date) ==="
python3 - "$OUT" <<'PY'
import os, glob, json, sys, re
out = sys.argv[1]
print(f"{'conc':>4} {'scenario':<12} {'async':<5} {'duration':>10} {'peakRSS_GB':>11}")
rows = []
for cdir in sorted(glob.glob(os.path.join(out, "c*"))):
    conc = os.path.basename(cdir)[1:]
    for cell in sorted(glob.glob(os.path.join(cdir, "BASE", "*_iter1"))):
        m = re.match(r'(.+)_existing_async(ON|OFF)_iter1$', os.path.basename(cell))
        if not m:
            continue
        js = sorted(glob.glob(os.path.join(cell, "run_*", "profile_result.json")))
        if not js:
            continue
        try:
            d = json.load(open(js[-1]))
        except Exception:
            continue
        rss = max((s.get('metrics', {}).get('process_resident_memory_bytes', 0)
                   for s in d.get('tidb_metrics', [])), default=0)
        rows.append((int(conc), m.group(1), m.group(2), d.get('analyze_duration'), rss / 1e9))
for r in sorted(rows):
    print(f"{r[0]:>4} {r[1]:<12} {r[2]:<5} {str(r[3]):>10} {r[4]:>11.2f}")
print("\nCompare against matrix-v5's BASE cells for c=1 (the default).")
PY
echo "=== SWEEP done $(date) ==="
