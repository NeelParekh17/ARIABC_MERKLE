#!/usr/bin/env bash
# Final WORKER_THREAD_LATENCY runs: exact YCSB cluster config, YCSB-A theta=0,
# fixed gateway; det window 1024 (3 trials) and 65536 (1 trial, hand-off check).
cd /work/ARIABC/AriaBC
B=/work/ARIABC/AriaBC/scripts/bench_full_results
run() {
  local win="$1" trials="$2" out="$3"
  mkdir -p "$out"
  CLUSTER_DET_WINDOW=$win python3 -u scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb \
    --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo /home/neel/ARIABC/AriaBC \
    --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 \
    --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt --workers 1,4,8,16 --modes cluster \
    --run-cluster --db-shared-buffers 32MB --cold-runs --order-seed 42 --trials "$trials" \
    --out-dir "$out" > "$out/runner.log" 2>&1
  grep -E "PASS: TPS|FAIL|Error" "$out/runner.log"
}
run 1024 3 $B/e2e_win1024_20261006
run 65536 1 $B/e2e_win65536_fixed_20261006
echo "FINAL RUNS DONE"
