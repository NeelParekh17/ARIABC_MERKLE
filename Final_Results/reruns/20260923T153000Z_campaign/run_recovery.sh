#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=/work/ARIABC/AriaBC
RESULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

RECOVERY_REPETITIONS="${RECOVERY_REPETITIONS:-10}"
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"
export LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"

echo "=== ARIA BC RECOVERY BENCHMARK ==="
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
    echo "=== RECOVERY BENCHMARK COMPLETED SUCCESSFULLY AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  else
    printf 'status=failed\nexit_code=%s\nfinished_utc=%s\n' "$exit_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$RESULT_ROOT/run_status.txt"
    echo "=== RECOVERY BENCHMARK FAILED (exit code: $exit_status) AT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
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
echo "Copying fetched recovery artifacts from $RECOVERY_FETCHED to $RESULT_ROOT/Recovery/..."
cp -a --reflink=never "$RECOVERY_FETCHED" "$RESULT_ROOT/Recovery/"
RUN_ID="$(basename "$RECOVERY_FETCHED")"
printf 'copied_recovery_artifacts=%s\n' "$RECOVERY_FETCHED" >> "$RESULT_ROOT/run_metadata.txt"

# Copy generated single-run plots if available
if [[ -d "$REPO_DIR/Dynamic_merkle_docs/run_reports/${RUN_ID}/plots" ]]; then
  echo "Copying single-run visualization plots..."
  mkdir -p "$RESULT_ROOT/Recovery/plots"
  cp -r "$REPO_DIR/Dynamic_merkle_docs/run_reports/${RUN_ID}/plots/"* "$RESULT_ROOT/Recovery/plots/" 2>/dev/null || true
fi

update_status "completed" "completed"
