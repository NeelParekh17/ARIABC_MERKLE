#!/usr/bin/env bash
cd /work/ARIABC/AriaBC
B=/work/ARIABC/AriaBC/scripts/bench_full_results/e2e_window_sweep_20261006
for win in 4096 1024 256 64 16; do
  echo "=== window=$win $(date +%H:%M:%S)"
  mkdir -p $B/win$win
  CLUSTER_DET_WINDOW=$win python3 -u scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb \
    --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo /home/neel/ARIABC/AriaBC \
    --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 \
    --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt --workers 1,16 --modes cluster \
    --run-cluster --db-shared-buffers 32MB --cold-runs --order-seed 42 --trials 1 \
    --out-dir $B/win$win > $B/win$win/runner.log 2>&1
  grep -E "PASS: TPS|FAIL|Error" $B/win$win/runner.log
done
echo "ALL WINDOWS DONE $(date +%H:%M:%S)"
