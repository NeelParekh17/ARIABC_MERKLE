#!/bin/bash
# merkle_lsm YCSB D s0.99 w16 on 100M ff90 rows: same settings as the 2026-10-15
# B-tree/lsm3 D runs, except ~2.4M statements per run (~3.5 min) so merges occur.
set -Eeuo pipefail
root=/home/neel/claude_ctl/lsm_bench_20261019
export TMPDIR=$root/scratch PYTHONDONTWRITEBYTECODE=1
out=$root/results/ycsb_d
mkdir -p $out
exec 9> $out/controller.lock; flock -n 9 || exit 1
log() { printf "%s %s\n" "$(date -u +%FT%TZ)" "$*" | tee -a $out/status.txt; }
trap 'log FAILED rc=$?' ERR
cd $root/src
test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
ARGS=(--remote-host 10.129.148.247 --remote-user neel
 --remote-dir /home/neel/ariabc_data/lsm_lookup_20261014
 --install-dir /home/neel/claude_checks/lsm_bench_20261019/install
 --cluster-dir /home/neel/claude_checks/lsm_bench_20261019/cluster
 --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo $root/src
 --db-port 5488 --server-port 8088 --base-dir-name base_ff90_mlsm
 --db-rows 100000000 --shared-buffers 32MB --txs ${TXS:-2400000} --seed 42
 --modes bcdb_merkle --workers 16 --combos d:0.99
 --merkle-lookup-am merkle_lsm --merkle-lookup-levels 3 --merkle-lookup-size 1024 --merkle-lookup-size-ratio 16
 --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
 --reset-mode delta --delta-content-check sampled --gateway-timeout 1800 --reset-timeout 7200 --verify-timeout 7200
 --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
 --usertable-fillfactor 90 --gen-shared-buffers 512MB --gen-maintenance-work-mem 256MB
 --gen-parallel-maintenance-workers 0 --gen-min-mem-available-mb 4608 --generation-timeout 21600 --skip-gen)
if [[ ${1:-} == preflight ]]; then
  python3 -u scripts/distributed/run_oom_100m_benchmark.py "${ARGS[@]}" --trials 1 --preflight-only --out-dir $out/preflight
  exit
fi
log START_D_s0.99_w16_merkle_lsm trials=${TRIALS:-3} txs=${TXS:-2400000}
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${ARGS[@]}" --trials ${TRIALS:-3} --auto-resume --out-dir $out/run >> $out/run.log 2>&1
log DONE
