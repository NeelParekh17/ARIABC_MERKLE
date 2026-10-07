#!/usr/bin/env bash
# Fixed offered load (open loop) per worker count: does per-tx latency rise with W
# once queueing is taken out? Exact YCSB cluster config, det window 1024 (default).
cd /work/ARIABC/AriaBC
B=/work/ARIABC/AriaBC/scripts/bench_full_results
for tps in 1000 2500; do
  out=$B/e2e_fixed_load_${tps}tps_20261006
  mkdir -p "$out"
  GATEWAY_TARGET_TPS=$tps python3 -u scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb \
    --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo /home/neel/ARIABC/AriaBC \
    --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 \
    --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt --workers 1,4,8,16 --modes cluster \
    --run-cluster --db-shared-buffers 32MB --cold-runs --order-seed 42 --trials 1 \
    --out-dir "$out" > "$out/runner.log" 2>&1
  grep -E "PASS: TPS|FAIL|Error" "$out/runner.log"
done
echo "FIXED LOAD RUNS DONE"
