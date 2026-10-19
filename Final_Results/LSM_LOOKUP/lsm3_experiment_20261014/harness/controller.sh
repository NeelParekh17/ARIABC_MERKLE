#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_ctl/lsm_lookup_20261014
export TMPDIR=$root/scratch PYTHONDONTWRITEBYTECODE=1
source "$root/arguments.sh"
mkdir -p "$LSM_OUT/stages"
exec 9> "$LSM_OUT/controller.lock"
flock -n 9 || exit 1
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LSM_OUT/status.txt"; }
stage=init
trap 'rc=$?; log "FAILED stage=$stage rc=$rc line=$LINENO"; exit "$rc"' ERR
cd "$LSM_REPO"
test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
log CONTROLLER_START
stage=wait_baselines
until ssh neel@10.129.148.247 'test -f ~/claude_checks/lsm_lookup_20261014/baselines_normalized/status.txt && grep -q NORMALIZED_FF90_BASELINES_PASS_STOPPED ~/claude_checks/lsm_lookup_20261014/baselines_normalized/status.txt'; do
 ssh neel@10.129.148.247 'test ! -f ~/claude_checks/lsm_lookup_20261014/baselines/FAILED.txt && test ! -f ~/claude_checks/lsm_lookup_20261014/baselines_normalized/FAILED.txt'
 log WAITING_NORMALIZED_BASELINES
 sleep 60
done
if [[ ! -f "$LSM_OUT/stages/smoke.OK" ]]; then
 stage=smoke
 log SMOKE_START_A_THETA0_W8
 python3 -u scripts/distributed/run_oom_100m_benchmark.py "${LSM_COMMON[@]}" --skip-gen --auto-resume --trials 1 --workers 8 --combos a:0.0 --out-dir "$LSM_OUT/smoke" > "$LSM_OUT/smoke.log" 2>&1
 touch "$LSM_OUT/stages/smoke.OK"
 log SMOKE_PASS_3_CASES
fi
stage=smoke_compare
python3 "$root/compare_smoke.py" > "$LSM_OUT/smoke_comparison.log" 2>&1
log SMOKE_DRIFT_WITHIN_10_PERCENT
stage=ff90
log FF90_GRID_START_72_CASES
for attempt in 1 2 3; do
 if python3 -u scripts/distributed/run_oom_100m_benchmark.py "${LSM_COMMON[@]}" --skip-gen --auto-resume --out-dir "$LSM_OUT/ff90" >> "$LSM_OUT/ff90.log" 2>&1; then
  touch "$LSM_OUT/stages/ff90.OK"
  log FF90_GRID_DONE_72_CASES
  exit 0
 fi
 log "FF90_AUTO_RESUME_WAIT attempt=$attempt"
 sleep 60
done
log FF90_FAILED_THREE_CONTROLLER_ATTEMPTS
exit 1
