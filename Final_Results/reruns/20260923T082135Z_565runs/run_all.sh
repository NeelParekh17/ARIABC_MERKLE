#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=/work/ARIABC/AriaBC
RESULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

YCSB_TRIALS=1
OOM_TRIALS=1
TPCC_TRIALS=3
RECOVERY_REPETITIONS=10
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS

finish_run() {
  local exit_status=$?
  if [[ "$exit_status" -eq 0 ]]; then
    printf 'status=completed\nfinished_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
  else
    printf 'status=failed\nexit_code=%s\nfinished_utc=%s\n' "$exit_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
  fi
}
trap finish_run EXIT

run_stage() {
  local stage_name="$1"
  shift
  local stage_dir="$RESULT_ROOT/$stage_name"
  local benchmark_rc
  local tee_rc
  local -a stage_statuses
  mkdir -p "$stage_dir"
  printf '%q ' "$@" > "$stage_dir/command.txt"
  printf '\n' >> "$stage_dir/command.txt"
  printf 'START %s %s\n' "$stage_name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$@" 2>&1 | tee "$stage_dir/runner.log"
  stage_statuses=("${PIPESTATUS[@]}")
  set -e
  benchmark_rc="${stage_statuses[0]}"
  tee_rc="${stage_statuses[1]}"
  printf '%s\n' "$benchmark_rc" > "$stage_dir/benchmark_exit_code.txt"
  printf '%s\n' "$tee_rc" > "$stage_dir/tee_exit_code.txt"
  if [[ "$benchmark_rc" -ne 0 || "$tee_rc" -ne 0 ]]; then
    printf 'FAILED %s benchmark_exit=%s tee_exit=%s\n' "$stage_name" "$benchmark_rc" "$tee_rc"
    return 1
  fi
  printf 'PASS %s %s\n' "$stage_name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

# Non-mutating OOM preflight checks remote binaries, storage, ports, and sudo.
run_stage OOM_100M/preflight python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --remote-host 10.129.148.247 \
  --remote-user neel \
  --remote-dir /tmp/ariabc_oom_100m \
  --install-dir /home/neel/Desktop/ariabc_install \
  --cluster-dir /home/neel/Desktop/ariabc_cluster \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-port 5438 \
  --server-port 8000 \
  --db-rows 100000000 \
  --shared-buffers 32MB \
  --txs 20000 \
  --seed 42 \
  --workloads a \
  --skews 0.0 0.99 \
  --workers 1 8 16 \
  --modes pg bcdb_det bcdb_merkle \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --out-dir "$RESULT_ROOT/OOM_100M/preflight" \
  --preflight-only

run_stage YCSB python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark ycsb \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-host 10.129.148.247 \
  --db-user neel \
  --db-port 5438 \
  --server-port 8000 \
  --workloads abcdf_4skews \
  --workers 1,4,8,16 \
  --modes cluster,pg,bcdb_det,bcdb_merkle \
  --run-cluster \
  --db-shared-buffers 32MB \
  --cold-runs \
  --order-seed 42 \
  --trials "$YCSB_TRIALS" \
  --out-dir "$RESULT_ROOT/YCSB"

run_stage OOM_100M/skews_0_and_0_99 python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --remote-host 10.129.148.247 \
  --remote-user neel \
  --remote-dir /tmp/ariabc_oom_100m \
  --install-dir /home/neel/Desktop/ariabc_install \
  --cluster-dir /home/neel/Desktop/ariabc_cluster \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-port 5438 \
  --server-port 8000 \
  --db-rows 100000000 \
  --shared-buffers 32MB \
  --txs 20000 \
  --seed 42 \
  --workloads a \
  --skews 0.0 0.99 \
  --workers 1 8 16 \
  --modes pg bcdb_det bcdb_merkle \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_and_0_99"

run_stage OOM_100M/skews_0_5_and_1_2 python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --remote-host 10.129.148.247 \
  --remote-user neel \
  --remote-dir /tmp/ariabc_oom_100m \
  --install-dir /home/neel/Desktop/ariabc_install \
  --cluster-dir /home/neel/Desktop/ariabc_cluster \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-port 5438 \
  --server-port 8000 \
  --db-rows 100000000 \
  --shared-buffers 32MB \
  --txs 20000 \
  --seed 42 \
  --workloads a \
  --skews 0.5 1.2 \
  --workers 1 8 16 \
  --modes pg bcdb_det bcdb_merkle \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_5_and_1_2"

run_stage TPCC/workers_w100 python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark tpcc \
  --db-host 10.129.7.57 \
  --db-user protectdr \
  --db-port 5438 \
  --server-port 8000 \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --modes pg,bcdb_det,bcdb_merkle \
  --warehouses 100 \
  --tpcc-workers 8,16,24,32 \
  --trials "$TPCC_TRIALS" \
  --tpcc-tx-count 20000 \
  --tpcc-seed 42 \
  --tpcc-remote-payment-pct 15.0 \
  --tpcc-remote-new-order-pct 1.0 \
  --tpcc-merkle-fanout 32 \
  --tpcc-merkle-partitions 200 \
  --tpcc-merkle-split-threshold 32 \
  --tpcc-merkle-merge-threshold 8 \
  --db-shared-buffers 32GB \
  --cold-runs \
  --order-seed 42 \
  --out-dir "$RESULT_ROOT/TPCC/workers_w100"

run_stage TPCC/warehouses_w32 python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark tpcc \
  --db-host 10.129.7.57 \
  --db-user protectdr \
  --db-port 5438 \
  --server-port 8000 \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --modes pg,bcdb_det,bcdb_merkle \
  --warehouses 5,10,20,30,50,75,100 \
  --tpcc-workers 32 \
  --trials "$TPCC_TRIALS" \
  --tpcc-tx-count 20000 \
  --tpcc-seed 42 \
  --tpcc-remote-payment-pct 15.0 \
  --tpcc-remote-new-order-pct 1.0 \
  --tpcc-merkle-fanout 32 \
  --tpcc-merkle-partitions 200 \
  --tpcc-merkle-split-threshold 32 \
  --tpcc-merkle-merge-threshold 8 \
  --db-shared-buffers 32GB \
  --cold-runs \
  --order-seed 42 \
  --out-dir "$RESULT_ROOT/TPCC/warehouses_w32"

run_stage Recovery env RECOVERY_LOG_TEE_ACTIVE=1 bash scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh \
  --host ranking \
  --ssh-user protectdr \
  --remote-root /home/protectdr/merkle_recovery_runs \
  --remote-python /usr/bin/python3 \
  --profile size-scaling-k75-c300 \
  --build-profile release \
  --fanout 32 \
  --geometry-label fanout_f32_l16 \
  --partitions 200 \
  --levels-per-batch 1 \
  --leaf-fetch-batch-size 64 \
  --corruption-mode mixed \
  --repetitions "$RECOVERY_REPETITIONS" \
  --profiling off \
  --track-counts on \
  --artifact-mode summary \
  --audit-mode full \
  --synchronous-commit off \
  --cpu-affinity 176-183 \
  --warmup-cycles 6

RECOVERY_FETCHED="$(tail -n 1 "$RESULT_ROOT/Recovery/runner.log")"
if [[ ! -d "$RECOVERY_FETCHED" ]]; then
  printf 'Fetched recovery artifact path is missing: %s\n' "$RECOVERY_FETCHED" >&2
  exit 1
fi
cp -a --reflink=never "$RECOVERY_FETCHED" "$RESULT_ROOT/Recovery/"
printf 'copied_recovery_artifacts=%s\n' "$RECOVERY_FETCHED" >> "$RESULT_ROOT/run_metadata.txt"
