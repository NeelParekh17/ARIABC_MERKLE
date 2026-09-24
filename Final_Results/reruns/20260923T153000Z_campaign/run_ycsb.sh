#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=/work/ARIABC/AriaBC
RESULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

YCSB_TRIALS=1
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"
export LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"

echo "=== ARIA BC YCSB SWEEP ==="
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
    echo "=== YCSB SWEEP COMPLETED SUCCESSFULLY AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  else
    printf 'status=failed\nexit_code=%s\nfinished_utc=%s\n' "$exit_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
    echo "=== YCSB SWEEP FAILED (exit code: $exit_status) AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
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

  local gw_fp
  gw_fp="$(sshpass -p clusterinfolab123 ssh -o StrictHostKeyChecking=no neel@10.129.27.111 'grep "^source_fingerprint=" /home/neel/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway.manifest 2>/dev/null | cut -d= -f2 || true')"
  if [[ "$gw_fp" != "$expected_fp" ]]; then
    echo "ERROR: Gateway (10.129.27.111) manifest fingerprint ($gw_fp) does not match expected ($expected_fp)" >&2
    return 1
  fi
  echo "Gateway (10.129.27.111): VERIFIED ($gw_fp)"
  echo "All cluster and gateway hosts verified matching active source fingerprint."
}
preflight_verify_all_nodes

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
