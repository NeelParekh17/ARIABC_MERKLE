#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_ROOT="$SCRIPT_DIR"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$REPO_DIR"

TPCC_TRIALS="${TPCC_TRIALS:-3}"
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"
export LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-$(test -d /work/ARIABC/install && echo /work/ARIABC/install || echo "$REPO_DIR/../install")}"

echo "=== ARIA BC TPC-C BENCHMARK SUITE ==="
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
    echo "=== TPC-C BENCHMARK SUITE COMPLETED SUCCESSFULLY AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  else
    printf 'status=failed\nexit_code=%s\nfinished_utc=%s\n' "$exit_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
    echo "=== TPC-C BENCHMARK SUITE FAILED (exit code: $exit_status) AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
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
preflight_verify_tpcc_nodes() {
  echo "=== Running Pre-sweep Binary Provenance & Source Fingerprint Verification for TPC-C ==="
  local expected_fp
  expected_fp="$(python3 scripts/distributed/source_fingerprint.py --repo . --ring-capacity 2048)"
  echo "Local source fingerprint: $expected_fp"

  local db_fp
  db_fp="$(ssh -o BatchMode=yes protectdr@10.129.7.57 'python3 /home/protectdr/Desktop/ariabc_cluster/scripts/distributed/source_fingerprint.py --repo /home/protectdr/Desktop/ariabc_cluster --ring-capacity 2048 2>/dev/null || echo "FP_FAIL"')"
  if [[ "$db_fp" != "$expected_fp" ]]; then
    echo "ERROR: DB Node (10.129.7.57) source fingerprint ($db_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "DB Node (10.129.7.57): VERIFIED ($db_fp)"

  local gw_fp
  if [[ -f "$REPO_DIR/ariabc_pg/build/bin/ariabc_pg_gateway.manifest" ]]; then
    gw_fp="$(grep "^source_fingerprint=" "$REPO_DIR/ariabc_pg/build/bin/ariabc_pg_gateway.manifest" 2>/dev/null | cut -d= -f2 || true)"
  else
    gw_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no neel@10.129.27.111 'grep "^source_fingerprint=" /home/neel/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway.manifest 2>/dev/null | cut -d= -f2 || true')"
  fi
  if [[ "$gw_fp" != "$expected_fp" ]]; then
    echo "ERROR: Gateway (10.129.27.111) manifest fingerprint ($gw_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "Gateway (10.129.27.111): VERIFIED ($gw_fp)"
  echo "All TPC-C hosts verified matching active source fingerprint."
}
preflight_verify_tpcc_nodes

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
  --tpcc-prewarm \
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
  --tpcc-prewarm \
  --order-seed 42 \
  --out-dir "$RESULT_ROOT/TPCC/warehouses_w32"

# --- POST-SWEEP PLOT AND REPORT COMPILATION ---
echo "=== Generating TPC-C Master Plots and Comprehensive Report ==="
python3 scripts/distributed/orchestrate_tpcc_full_evaluation.py \
  --report-only \
  --trials "$TPCC_TRIALS" \
  --workers-dir "$RESULT_ROOT/TPCC/workers_w100" \
  --warehouses-dir "$RESULT_ROOT/TPCC/warehouses_w32"

mkdir -p "$RESULT_ROOT/TPCC"
if [[ -f "$RESULT_ROOT/TPCC/workers_w100/tpcc_tps_vs_workers.png" ]]; then
  cp "$RESULT_ROOT/TPCC/workers_w100/tpcc_tps_vs_workers.png" "$RESULT_ROOT/TPCC/tpcc_workers_scaling.png"
fi
if [[ -f "$RESULT_ROOT/TPCC/warehouses_w32/tpcc_tps_vs_warehouses.png" ]]; then
  cp "$RESULT_ROOT/TPCC/warehouses_w32/tpcc_tps_vs_warehouses.png" "$RESULT_ROOT/TPCC/tpcc_warehouses_scaling.png"
fi

latest_tpcc_report="$(ls -t "$RESULT_ROOT/TPCC"/TPCC_ANALYSIS_*.md 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_tpcc_report" && -f "$latest_tpcc_report" ]]; then
  cp "$latest_tpcc_report" "$RESULT_ROOT/TPCC/Report.md"
  echo "TPC-C Report saved to $RESULT_ROOT/TPCC/Report.md"
fi

update_status "completed" "completed"
