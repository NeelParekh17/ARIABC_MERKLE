#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=/home/neel/ARIABC/AriaBC
RESULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

YCSB_TRIALS=1
OOM_TRIALS=1
TPCC_TRIALS=3
RECOVERY_REPETITIONS=10
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"
export LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/home/neel/ARIABC/install}"

echo "=== ARIA BC FULL 565-RUN CAMPAIGN ==="
echo "Result Root: $RESULT_ROOT"
echo "Start Time:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"

update_status() {
  local stage="$1"
  local state="$2"
  printf 'status=%s\ncurrent_stage=%s\nupdated_utc=%s\n' "$state" "$stage" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
}

finish_run() {
  local exit_status=$?
  if [[ "$exit_status" -eq 0 ]]; then
    printf 'status=completed\nfinished_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
    echo "=== CAMPAIGN COMPLETED SUCCESSFULLY AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  else
    printf 'status=failed\nexit_code=%s\nfinished_utc=%s\n' "$exit_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
    echo "=== CAMPAIGN FAILED (exit code: $exit_status) AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  fi
}
trap finish_run EXIT

run_stage() {
  local stage_name="$1"
  shift
  if [[ -f "$RESULT_ROOT/stages_completed.txt" ]] && grep -Fxq "$stage_name" "$RESULT_ROOT/stages_completed.txt"; then
    printf 'SKIP %s (already completed)\n' "$stage_name"
    return 0
  fi
  local stage_dir="$RESULT_ROOT/$stage_name"
  local benchmark_rc
  local tee_rc
  local -a stage_statuses
  mkdir -p "$stage_dir"
  printf '%q ' "$@" > "$stage_dir/command.txt"
  printf '\n' >> "$stage_dir/command.txt"
  update_status "$stage_name" "running"
  printf 'START %s %s\n' "$stage_name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$@" 2>&1 | tee -a "$stage_dir/runner.log"
  stage_statuses=("${PIPESTATUS[@]}")
  set -e
  benchmark_rc="${stage_statuses[0]}"
  tee_rc="${stage_statuses[1]}"
  printf '%s\n' "$benchmark_rc" > "$stage_dir/benchmark_exit_code.txt"
  printf '%s\n' "$tee_rc" > "$stage_dir/tee_exit_code.txt"
  if [[ "$benchmark_rc" -ne 0 || "$tee_rc" -ne 0 ]]; then
    printf 'FAILED %s benchmark_exit=%s tee_exit=%s\n' "$stage_name" "$benchmark_rc" "$tee_rc"
    update_status "$stage_name" "failed"
    return 1
  fi
  printf 'PASS %s %s\n' "$stage_name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\n' "$stage_name" >> "$RESULT_ROOT/stages_completed.txt"
  update_status "$stage_name" "passed"
}

# --- PRE-SWEEP VALIDATION: Binary provenance and source fingerprint verification ---
preflight_verify_all_nodes() {
  echo "=== Running Pre-sweep Binary Provenance & Source Fingerprint Verification ==="
  local expected_fp
  expected_fp="$(python3 scripts/distributed/source_fingerprint.py --repo . --ring-capacity 2048)"
  echo "Local source fingerprint: $expected_fp"

  local n1_fp
  n1_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no neel@10.129.148.247 'grep "^source_fingerprint=" /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server.manifest 2>/dev/null | cut -d= -f2 || true')"
  if [[ "$n1_fp" != "$expected_fp" ]]; then
    echo "ERROR: Node 1 (10.129.148.247) manifest fingerprint ($n1_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "Node 1 (10.129.148.247): VERIFIED ($n1_fp)"

  local n2_fp
  n2_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no neel@10.129.148.246 'grep "^source_fingerprint=" /home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server.manifest 2>/dev/null | cut -d= -f2 || true')"
  if [[ "$n2_fp" != "$expected_fp" ]]; then
    echo "ERROR: Node 2 (10.129.148.246) manifest fingerprint ($n2_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "Node 2 (10.129.148.246): VERIFIED ($n2_fp)"

  local n4_fp
  n4_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no neel@10.129.148.248 'grep "^source_fingerprint=" /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server.manifest 2>/dev/null | cut -d= -f2 || true')"
  if [[ "$n4_fp" != "$expected_fp" ]]; then
    echo "ERROR: Node 4 (10.129.148.248) manifest fingerprint ($n4_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "Node 4 (10.129.148.248): VERIFIED ($n4_fp)"

  local tpcc_fp
  tpcc_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no protectdr@10.129.7.57 'grep "^source_fingerprint=" /home/protectdr/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server.manifest 2>/dev/null | cut -d= -f2 || true')"
  if [[ "$tpcc_fp" != "$expected_fp" ]]; then
    echo "ERROR: TPC-C Host (10.129.7.57) manifest fingerprint ($tpcc_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "TPC-C Host (10.129.7.57): VERIFIED ($tpcc_fp)"
  echo "All cluster and benchmark hosts verified matching active source fingerprint."
}
preflight_verify_all_nodes

# --- STAGE 0: OOM 100M PREFLIGHT ---
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
  --pg-exec-mode event \
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

# --- STAGE 1: YCSB ---
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

# --- STAGE 2: OOM 100M (SKEWS 0.0, 0.99) ---
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
  --pg-exec-mode event \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --auto-resume \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_and_0_99"

# --- STAGE 3: OOM 100M (SKEWS 0.5, 1.2) ---
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
  --pg-exec-mode event \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --auto-resume \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_5_and_1_2"

# --- STAGE 4: TPC-C (WORKER SCALING AT 100 WAREHOUSES) ---
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

# --- STAGE 5: TPC-C (WAREHOUSE SCALING AT 32 WORKERS) ---
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

# --- STAGE 6: RECOVERY SIZE SCALING ---
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

RECOVERY_FETCHED="$(grep -E '^/.*scripts/benchmark/recovery/fetched/' "$RESULT_ROOT/Recovery/runner.log" | tail -n 1 || true)"
if [[ -z "$RECOVERY_FETCHED" || ! -d "$RECOVERY_FETCHED" ]]; then
  RECOVERY_FETCHED="$(tail -n 1 "$RESULT_ROOT/Recovery/runner.log")"
fi
if [[ -z "$RECOVERY_FETCHED" || ! -d "$RECOVERY_FETCHED" ]]; then
  echo "ERROR: Recovery artifacts directory missing or invalid: '$RECOVERY_FETCHED'" >&2
  update_status "Recovery/copy" "failed"
  exit 1
fi
cp -a --reflink=never "$RECOVERY_FETCHED" "$RESULT_ROOT/Recovery/"
printf 'copied_recovery_artifacts=%s\n' "$RECOVERY_FETCHED" >> "$RESULT_ROOT/run_metadata.txt"

update_status "completed" "completed"
