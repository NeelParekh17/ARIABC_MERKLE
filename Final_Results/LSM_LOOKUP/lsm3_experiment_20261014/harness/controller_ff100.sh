#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_ctl/lsm_lookup_20261014
export TMPDIR=$root/scratch PYTHONDONTWRITEBYTECODE=1
source "$root/arguments.sh"
mkdir -p "$LSM_OUT/stages"
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LSM_OUT/status_ff100.txt"; }
stage=wait_ff90
trap 'rc=$?; log "FAILED stage=$stage rc=$rc line=$LINENO"; exit "$rc"' ERR
until test -f "$LSM_OUT/stages/ff90.OK"; do
 # A live controller or successful ff90 marker is required before advancing.
 log WAITING_FF90_GRID
 sleep 60
done
exec 9> "$LSM_OUT/controller.lock"
flock -n 9 || exit 1
stage=ff100_build
if ! ssh neel@10.129.148.247 'test -f ~/claude_checks/lsm_lookup_20261014/baselines_ff100/status.txt && grep -q FF100_BASELINES_PASS_STOPPED ~/claude_checks/lsm_lookup_20261014/baselines_ff100/status.txt'; then
 log FF100_BUILD_START
 ssh neel@10.129.148.247 'export TMPDIR=$HOME/claude_checks/lsm_lookup_20261014/scratch; nohup python3 -u ~/claude_checks/lsm_lookup_20261014/prepare_ff100.py > ~/claude_checks/lsm_lookup_20261014/ff100_launcher.log 2>&1 < /dev/null &'
 until ssh neel@10.129.148.247 'test -f ~/claude_checks/lsm_lookup_20261014/baselines_ff100/status.txt && grep -q FF100_BASELINES_PASS_STOPPED ~/claude_checks/lsm_lookup_20261014/baselines_ff100/status.txt'; do
  ssh neel@10.129.148.247 'test ! -f ~/claude_checks/lsm_lookup_20261014/baselines_ff100/FAILED.txt'
  log WAITING_FF100_BASELINES
  sleep 60
 done
fi
cd "$LSM_REPO"
if [[ ! -f "$LSM_OUT/stages/ff100.OK" ]]; then
 stage=ff100
 log FF100_GRID_START_36_CASES
 python3 -u scripts/distributed/run_oom_100m_benchmark.py "${LSM_COMMON[@]}" --skip-gen --auto-resume --base-dir-name base_ff100 --usertable-fillfactor 100 --combos a:0.0 --out-dir "$LSM_OUT/ff100" > "$LSM_OUT/ff100.log" 2>&1
 touch "$LSM_OUT/stages/ff100.OK"
 log FF100_GRID_DONE_36_CASES
fi
if [[ ! -f "$LSM_OUT/stages/sustained.OK" ]]; then
 stage=sustained
 log SUSTAINED_START_6_CASES_600K_STATEMENTS
 python3 -u scripts/distributed/run_oom_100m_benchmark.py "${LSM_COMMON[@]}" --skip-gen --auto-resume --base-dir-name base_ff100 --usertable-fillfactor 100 --combos a:0.0 --workers 16 --trials 2 --txs 600000 --out-dir "$LSM_OUT/sustained" > "$LSM_OUT/sustained.log" 2>&1
 touch "$LSM_OUT/stages/sustained.OK"
 log SUSTAINED_DONE_6_CASES
fi
stage=analysis
python3 "$root/analyze.py" --root "$LSM_OUT" --out "$LSM_OUT/analysis" > "$LSM_OUT/analysis.log" 2>&1
log ALL_GRIDS_DONE_ANALYSIS_PASS
