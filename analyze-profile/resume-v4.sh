#!/usr/bin/env bash
# Resume the v4 matrix. Idempotent at (branch, scenario, async) group
# granularity: a group whose both iters already have profile_result.json on
# disk is skipped, so a mid-run kill costs at most the in-flight 2-cell group.
# bench.sh's `run` truncates manifest.tsv, so we rebuild it from disk at the
# end before comparing.
#
# Launch (survives session teardown now that linger is enabled):
#   systemd-run --user --unit=matrix-v4-resume --collect \
#     --working-directory=$PWD bash resume-v4.sh

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"
OUT="${SCRIPT_DIR}/output-bench-v4"

# systemd --user starts with a minimal PATH that lacks go / tiup. Restore the
# interactive PATH so build_analyzer (go build) and tiup playground/br work.
export PATH="/home/mattias/go/bin:/home/mattias/.tiup/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export GOROOT="/home/mattias/go1.26.3"
export GOPATH="/home/mattias/go"

echo "=== RESUME start $(date) ==="

# Is a (label, scenario, async) group complete? Both iter1 and iter2 must have
# a profile_result.json.
group_done() {
  local label="$1" scn="$2" asy="$3" it n=0
  for it in 1 2; do
    local cell="${OUT}/${label}/${scn}_existing_async${asy}_iter${it}"
    if compgen -G "${cell}/run_*/profile_result.json" >/dev/null 2>&1; then
      n=$((n+1))
    fi
  done
  [[ "$n" -eq 2 ]]
}

# Run one (label, scenario, async) group (2 iters) unless already complete.
run_group() {
  local label="$1" scn="$2" asy="$3"
  local scope; [[ "$label" == "BASE" ]] && scope="base" || scope="pr"
  if group_done "$label" "$scn" "$asy"; then
    echo "=== SKIP ${label} ${scn} async=${asy} (already complete) ==="
    return 0
  fi
  echo "=== RUN ${label} ${scn} async=${asy} (2 iters) ==="
  SCENARIOS_FILTER="${scn}" STATES_FILTER="existing" ASYNC_MERGE="${asy}" \
    ./bench-config-v4.sh run "${scope}"
  echo "${label} ${scn} async=${asy} exit=$?"
}

# Remaining groups: BASE part-full (part-single already done), all PR.
run_group BASE part-full   ON
run_group BASE part-full   OFF
run_group PR   part-single ON
run_group PR   part-single OFF
run_group PR   part-full   ON
run_group PR   part-full   OFF

# Rebuild manifest.tsv from every matrix run dir on disk.
echo "=== Rebuilding manifest from disk ==="
python3 - "$OUT" <<'PY'
import os, re, sys, glob
out = sys.argv[1]; rows = []
for label in ("BASE", "PR"):
    base = os.path.join(out, label)
    if not os.path.isdir(base): continue
    for cell in sorted(os.listdir(base)):
        if cell == "seed-full-analyze": continue
        m = re.match(r'(?P<scn>.+)_(?P<state>existing|clean)_async(?P<asy>ON|OFF)_iter(?P<it>\d+)$', cell)
        if not m: continue
        runs = [r for r in sorted(glob.glob(os.path.join(base, cell, "run_*")))
                if os.path.exists(os.path.join(r, "profile_result.json"))]
        if not runs: continue
        rows.append((label, m['scn'], m['state'], m['asy'], m['it'], runs[-1]))
rows.sort(key=lambda r: (r[0], r[1], r[3], int(r[4])))
with open(os.path.join(out, "manifest.tsv"), "w") as f:
    for r in rows: f.write("\t".join(r) + "\n")
print(f"manifest rebuilt: {len(rows)}/16 cells")
for r in rows: print("  " + "\t".join(r[:5]))
PY

echo "=== Compare ==="
./bench-config-v4.sh compare > "${OUT}/report.txt" 2>&1
echo "compare exit=$? — report at ${OUT}/report.txt"
echo "=== RESUME done $(date) ==="
