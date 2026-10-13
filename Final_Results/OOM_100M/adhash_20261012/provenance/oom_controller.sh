#!/bin/bash
# .111 only. Same file/arguments auto-resume the detached 112-case grid.
set -Eeuo pipefail
root=/home/neel/claude_ctl/f15_adhash_20261012
export TMPDIR="$root/scratch"
export PYTHONDONTWRITEBYTECODE=1
source "$root/oom_arguments.sh"
mkdir -p "$F15_OUT/stages"
exec 9> "$F15_OUT/controller.lock"
flock -n 9 || exit 1
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$F15_OUT/status.txt"; }
stage=init
trap 'rc=$?; log "FAILED stage=$stage rc=$rc line=$LINENO"; exit "$rc"' ERR
cd "$F15_REPO"
test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
log CONTROLLER_START
stage=baseline
until ssh neel@10.129.148.247 'grep -q BASELINE_PASS_STOPPED ~/claude_checks/f15_adhash_20261012/oom_baseline/status.txt'; do
    ssh neel@10.129.148.247 'test ! -f ~/claude_checks/f15_adhash_20261012/oom_baseline/FAILED.txt'
    log WAITING_BASELINE
    sleep 30
done
if [[ ! -f "$F15_OUT/stages/baseline.OK" ]]; then
    rsync -a neel@10.129.148.247:claude_checks/f15_adhash_20261012/oom_baseline/ "$F15_OUT/baseline_evidence/"
    scp "$F15_OUT/baseline_evidence/adhash_manifest.json" neel@10.129.148.247:"$F15_REMOTE/.ariabc_golden_manifest_pgdata_adhash.json"
    ssh neel@10.129.148.247 'cp ~/ariabc_data/paper_20261010_oom/.ariabc_golden_manifest_pgdata_base_v2_ff90_plain.json ~/ariabc_data/f15_adhash_20261012/oom/.ariabc_golden_manifest_pgdata_adhash_plain.json'
    touch "$F15_OUT/stages/baseline.OK"
fi
stage=workload_preflight
if [[ ! -f "$F15_OUT/stages/workload_preflight.OK" ]]; then
    python3 -u scripts/distributed/run_oom_100m_benchmark.py "${F15_COMMON[@]}" --skip-gen --preflight-only --out-dir "$F15_OUT/preflight" > "$F15_OUT/preflight.log" 2>&1
    python3 scripts/distributed/oom_v2/check_workloads.py scripts/distributed/oom_v2/published_workloads.json "$F15_OUT/preflight" > "$F15_OUT/workload_hash_check.txt"
    touch "$F15_OUT/stages/workload_preflight.OK"
    log PREFLIGHT_AND_7_WORKLOAD_HASHES_PASS
fi
stage=smoke
if [[ ! -f "$F15_OUT/stages/smoke.OK" ]]; then
    log SMOKE_START
    python3 -u scripts/distributed/run_oom_100m_benchmark.py "${F15_COMMON[@]}" --skip-gen --auto-resume --trials 1 --workers 1 --combos a:0.99 --out-dir "$F15_OUT/smoke" > "$F15_OUT/smoke.log" 2>&1
    touch "$F15_OUT/stages/smoke.OK"
    log SMOKE_PASS_2_CASES
fi
stage=campaign
log GRID_START_112_CASES
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${F15_COMMON[@]}" --skip-gen --auto-resume --out-dir "$F15_OUT/campaign" > "$F15_OUT/campaign.log" 2>&1
touch "$F15_OUT/stages/campaign.OK"
log GRID_DONE_112_CASES
