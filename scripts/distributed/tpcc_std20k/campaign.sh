#!/usr/bin/env bash
# Final_Results TPC-C matrix (v2 method, 20k transactions) with the spec-standard
# workload: warehouses 5..100 at 32 workers, workers 8..64 at W=100; pg, det, merkle.
# Usage: campaign.sh <ROOT> [trials=3]   (ranking only; launch detached)
# Within each point: trial-outer, modes interleaved (t1 pg det merkle, t2 ...).
# Existing accepted runs are reused, so a restarted campaign only fills gaps.
set -uo pipefail
ROOT=${1:?ROOT}; TRIALS=${2:-3}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$ROOT"
exec 9>"$HOME/claude_checks/v3/B_campaign.lock"; flock -n 9 || { echo 'lock busy' >&2; exit 2; }
log() { echo "$(date -Is) $*" | tee -a "$ROOT/status_history.txt"; }
point() {  # W workers...
  local W=$1; shift
  for kind in plain merkle; do
    local b=$ROOT/base_w${W}_${kind}
    if [[ -e $b && ! -d $b/pgdata ]]; then mv "$b" "$b.used_$(date +%s)"; fi
    [[ -f $b/BASE_OK ]] || { log "base W=$W $kind"; bash "$HERE/std20k.sh" base "$ROOT" "$W" "$kind" 9>&- >"$ROOT/base_w${W}_${kind}.log" 2>&1 || log "BASE_FAILED W=$W $kind"; }
  done
  for ((TRIAL=1; TRIAL<=TRIALS; TRIAL++)); do
   for k in "$@"; do
    for mode in pg det merkle; do
      local r=$ROOT/runs/${mode}_w${W}_k${k}_t${TRIAL}
      if grep -qx 'accepted=1' "$r/acceptance.txt" 2>/dev/null; then continue; fi
      [[ -e $r ]] && mv "$r" "$r.failed_$(date +%s)"
      log "run W=$W k=$k $mode t=$TRIAL"
      bash "$HERE/std20k.sh" run "$ROOT" "$W" "$k" "$mode" "$TRIAL" 9>&- >"$ROOT/run_${mode}_w${W}_k${k}_t${TRIAL}.log" 2>&1 || log "RUN_FAILED W=$W k=$k $mode t=$TRIAL"
      log "$(tr '\n' ' ' < "$r/result.txt" 2>/dev/null | grep -Eo 'completed_tps=[0-9.]+|accepted=[01]' | tr '\n' ' ')"
    done
   done
  done
  rm -rf "$ROOT/base_w${W}_plain/pgdata" "$ROOT/base_w${W}_merkle/pgdata"
}
# WAREHOUSES / W100_WORKERS override the default matrix (empty W100_WORKERS skips W=100).
for W in ${WAREHOUSES-5 10 20 30 50 75}; do point "$W" 32; done
[[ -n ${W100_WORKERS-8 16 24 32 48 64} ]] && point 100 ${W100_WORKERS-8 16 24 32 48 64}
python3 "$HERE/collect.py" "$ROOT" >"$ROOT/summary.md" 2>&1 || true
log COMPLETE
