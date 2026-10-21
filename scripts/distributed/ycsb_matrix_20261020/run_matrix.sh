#!/bin/bash
# Reference run_d.sh settings; 15 campaigns amortize >=300s calibration each.
set -Eeuo pipefail
root=/home/neel/claude_ctl/ycsb_matrix_20261020
dbroot=/home/neel/claude_checks/ycsb_matrix_20261020
export TMPDIR=$root/scratch PYTHONDONTWRITEBYTECODE=1
mkdir -p "$TMPDIR" "$root/results"
exec 9> "$root/controller.lock"
flock -n 9 || { echo 'Another controller holds the lock' >&2; exit 1; }
stage=${1:-matrix}
[[ $stage == smoke || $stage == matrix ]] || exit 2
out=$root/results/$stage
mkdir -p "$out"
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$out/status.txt"; }
trap 'log CONTROLLER_FAILED rc=$?' ERR
test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
ssh -o ControlMaster=no neel@10.129.148.247 "test \"\$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)\" = tsc"
if [[ $stage == matrix ]]; then
    test -f "$root/evidence/READY"
    # All smoke and regression instances have stopped; require a clean DB host.
    foreign=$(ssh -o ControlMaster=no neel@10.129.148.247 'ps -C postgres -o pid=,args= || true')
    if [[ -n $foreign ]]; then log LAUNCH_BLOCKED "$foreign"; exit 3; fi
    for host in 10.129.148.247 10.129.27.111; do
        ssh -o ControlMaster=no neel@"$host" 'test "$(df -B1 --output=avail /home/neel | tail -1)" -ge 30000000000'
    done
fi
ssh -o ControlMaster=no neel@10.129.148.247 "rm -f '$dbroot/sampler.stop'; (setsid nohup python3 -u '$dbroot/lsm_sampler.py' > '$dbroot/evidence/sampler_driver.log' 2>&1 < /dev/null & )"
cd "$root/src"
common=(--remote-host 10.129.148.247 --remote-user neel
 --remote-dir /home/neel/ariabc_data/lsm_lookup_20261014
 --install-dir "$dbroot/install" --cluster-dir "$dbroot/cluster"
 --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo "$root/src"
 --db-port 5488 --server-port 8088 --db-rows 100000000 --shared-buffers 32MB --seed 42
 --workers 16 --trials 1 --auto-resume
 --merkle-lookup-levels 3 --merkle-lookup-size 1024 --merkle-lookup-size-ratio 16
 --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
 --reset-mode delta --delta-content-check sampled --gateway-timeout 3600 --reset-timeout 7200 --verify-timeout 7200
 --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
 --usertable-fillfactor 90 --gen-shared-buffers 512MB --gen-maintenance-work-mem 256MB
 --gen-parallel-maintenance-workers 0 --gen-min-mem-available-mb 4608 --generation-timeout 21600 --skip-gen
 --require-existing-baselines --socket-dir "$dbroot/socket" --wal-sampler-root "$dbroot"
 --gateway-workload-dir "$root/scratch/workloads")
txs=2400000
workloads=(d c b a f)
if [[ $stage == smoke ]]; then txs=20000; workloads=(d); fi
failures=0
for wl in "${workloads[@]}"; do
    combos="$wl:0,$wl:0.5,$wl:0.99,$wl:1.2"
    if [[ $stage == smoke ]]; then combos="$wl:0.99"; fi
    for kind in plain lsm btree; do
        modes=(bcdb_merkle); base=base_ff90; am=btree
        if [[ $kind == plain ]]; then modes=(bcdb_det pg); fi
        if [[ $kind == lsm ]]; then base=base_ff90_mlsm; am=merkle_lsm; fi
        name=${wl}_${kind}
        log CAMPAIGN_START "$name" txs="$txs" combos="$combos"
        begin=$SECONDS
        if python3 -u scripts/distributed/run_oom_100m_benchmark.py "${common[@]}" --txs "$txs" --modes "${modes[@]}" --base-dir-name "$base" --merkle-lookup-am "$am" --combos "$combos" --out-dir "$out/$name" >> "$out/$name.log" 2>&1; then
            log CAMPAIGN_END "$name" elapsed_s=$((SECONDS-begin))
        else
            rc=$?; failures=$((failures+1))
            log CAMPAIGN_FAILURE "$name" rc="$rc" elapsed_s=$((SECONDS-begin))
        fi
    done
    if [[ $stage == matrix && $wl == d ]]; then
        log D_GROUP_REVIEW_WAIT failures="$failures"
        while [[ ! -f "$root/evidence/d_group.approved" ]]; do sleep 15; done
        log D_GROUP_REVIEW_APPROVED
    fi
done
ssh -o ControlMaster=no neel@10.129.148.247 "touch '$dbroot/sampler.stop'"
log CONTROLLER_END failed_campaigns="$failures"
test "$failures" -eq 0
