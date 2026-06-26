#!/usr/bin/env bash
# Deterministic Raft + Kafka tuning campaign.
#
# Run this script from any directory. It locates run_4node_raft_cluster.sh,
# executes a controlled sequence of experiments, streams live output, and
# gathers every native cluster result directory into one campaign folder.
#
# Default campaign:
#   A. Direct per-transaction path, p=64, 3 interleaved repetitions each:
#      linked 256 workers, decoupled 256, 64, 48, 32, 24, 16 workers.
#   B. For the winning worker configuration, test p=48 for 3 repetitions.
#   C. One traced baseline and one traced winning-direct run.
#   D. Block-fastpath exploration at the winning worker configuration:
#      p=1 and p=2, 3 repetitions each.
#
# Safety / comparability invariants:
#   - Raft + Kafka majority_async_all3 remains enabled.
#   - Every run restores identical data, resets Kafka topic, and validates all
#     replicas with the runner's post-workload marker + Merkle check.
#   - No unsafe consistency flags are enabled.
#   - Failed runs are retained and the campaign continues, so diagnostics are
#     never silently discarded.

set -Eeuo pipefail
shopt -s nullglob

# -----------------------------------------------------------------------------
# User-tunable environment overrides. Defaults are intentionally conservative.
# -----------------------------------------------------------------------------
RUN_TIMEOUT_SECONDS="${RUN_TIMEOUT_SECONDS:-1200}"
REPETITIONS="${REPETITIONS:-3}"
RUN_FASTPATH="${RUN_FASTPATH:-1}"
RUN_PHASE_TRACES="${RUN_PHASE_TRACES:-1}"
PREPARE_FIRST_RUN="${PREPARE_FIRST_RUN:-1}"

if [[ "$REPETITIONS" -lt 2 ]]; then
  echo "ERROR: REPETITIONS must be at least 2 (default: 3)." >&2
  exit 2
fi
if [[ "$RUN_TIMEOUT_SECONDS" -lt 60 ]]; then
  echo "ERROR: RUN_TIMEOUT_SECONDS must be at least 60." >&2
  exit 2
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Prefer an explicit RUNNER path, then a runner beside this campaign script,
# then the standard repository location relative to this script's directory.
RUNNER="${RUNNER:-}"
if [[ -z "$RUNNER" ]]; then
  if [[ -x "$SCRIPT_DIR/run_4node_raft_cluster.sh" || -f "$SCRIPT_DIR/run_4node_raft_cluster.sh" ]]; then
    RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
  elif [[ -x "$PWD/run_4node_raft_cluster.sh" || -f "$PWD/run_4node_raft_cluster.sh" ]]; then
    RUNNER="$PWD/run_4node_raft_cluster.sh"
  else
    candidate="$SCRIPT_DIR/../distributed/run_4node_raft_cluster.sh"
    if [[ -f "$candidate" ]]; then
      RUNNER="$candidate"
    fi
  fi
fi

if [[ -z "$RUNNER" || ! -f "$RUNNER" ]]; then
  cat >&2 <<'ERR'
ERROR: could not locate run_4node_raft_cluster.sh.
Place this script in scripts/distributed beside the runner, or invoke it as:
  RUNNER=/absolute/path/to/run_4node_raft_cluster.sh ./run_det_tuning_campaign.sh
ERR
  exit 2
fi
RUNNER="$(cd -- "$(dirname -- "$RUNNER")" && pwd)/$(basename -- "$RUNNER")"

# Runner lives in <repo>/scripts/distributed. Resolve repository root from it.
RUNNER_DIR="$(cd -- "$(dirname -- "$RUNNER")" && pwd)"
REPO_ROOT="$(cd -- "$RUNNER_DIR/../.." && pwd)"
RESULT_BASE="$REPO_ROOT/scripts/bench_full_results"
CAMPAIGN_ID="det_tuning_$(date +%Y%m%d_%H%M%S)"
OUT_ROOT="$RESULT_BASE/$CAMPAIGN_ID"
RUNS_DIR="$OUT_ROOT/runs"
LIVE_DIR="$OUT_ROOT/live"
CONFIG_DIR="$OUT_ROOT/configs"
SUMMARY_CSV="$OUT_ROOT/summary.csv"
MASTER_LOG="$OUT_ROOT/campaign.log"

mkdir -p "$RUNS_DIR" "$LIVE_DIR" "$CONFIG_DIR"

# Mirror all wrapper messages to the terminal and the campaign master log.
exec > >(tee -a "$MASTER_LOG") 2>&1

log() {
  printf '[%s] %s\n' "$(date +'%F %T')" "$*"
}

fatal() {
  log "ERROR: $*"
  exit 1
}

on_interrupt() {
  log "Interrupted. Completed and failed run folders are preserved in: $OUT_ROOT"
  exit 130
}
trap on_interrupt INT TERM

[[ -x "$RUNNER" ]] || chmod +x "$RUNNER" 2>/dev/null || true
[[ -x "$RUNNER" ]] || fatal "runner is not executable: $RUNNER"
[[ -d "$RESULT_BASE" ]] || mkdir -p "$RESULT_BASE"

WORKLOAD_FILE="$REPO_ROOT/scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
[[ -f "$WORKLOAD_FILE" ]] || fatal "expected workload is missing: $WORKLOAD_FILE"

# The supplied workload has one statement per non-comment line. The runner also
# logs its own query count; this value is only a fallback for partially failed runs.
WORKLOAD_QUERIES="$(awk 'NF && $1 !~ /^--/ { n++ } END { print n+0 }' "$WORKLOAD_FILE")"
[[ "$WORKLOAD_QUERIES" -gt 0 ]] || fatal "could not determine workload query count"

# Record immutable campaign provenance before any run changes the environment.
cp -f "$RUNNER" "$OUT_ROOT/run_4node_raft_cluster.used.sh"
cp -f "${BASH_SOURCE[0]}" "$OUT_ROOT/run_det_tuning_campaign.used.sh"
{
  echo "campaign_id=$CAMPAIGN_ID"
  echo "started_at=$(date --iso-8601=seconds)"
  echo "repo_root=$REPO_ROOT"
  echo "runner=$RUNNER"
  echo "workload_file=$WORKLOAD_FILE"
  echo "workload_queries=$WORKLOAD_QUERIES"
  echo "repetitions=$REPETITIONS"
  echo "run_timeout_seconds=$RUN_TIMEOUT_SECONDS"
  echo "run_fastpath=$RUN_FASTPATH"
  echo "run_phase_traces=$RUN_PHASE_TRACES"
  echo "prepare_first_run=$PREPARE_FIRST_RUN"
  echo "runner_sha256=$(sha256sum "$RUNNER" | awk '{print $1}')"
  echo "campaign_sha256=$(sha256sum "${BASH_SOURCE[0]}" | awk '{print $1}')"
  echo "git_head=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_status_begin"
  git -C "$REPO_ROOT" status --short 2>/dev/null || true
  echo "git_status_end"
} > "$OUT_ROOT/manifest.txt"

cat > "$OUT_ROOT/README.txt" <<EOF_README
Deterministic Raft + Kafka tuning campaign: $CAMPAIGN_ID

This folder is self-contained. Send this entire folder after the campaign ends.

Important files:
  summary.csv                    One row per experiment run.
  medians.txt                    Median wall-clock TPS by comparable configuration.
  final_choice.env               Winning direct-path configuration selected automatically.
  campaign.log                   Wrapper-level live log for the complete campaign.
  live/*.console.log             Full terminal output for each runner invocation.
  configs/*.command.txt          Exact command and environment for each invocation.
  runs/<label>/                  Native runner result folder, including gateway/server/
                                 PostgreSQL/NuRaft/OS logs and phase traces when enabled.

Correctness is PASS only when the runner exits successfully and the captured
output includes zero divergence, zero permanent failures, zero async-all3
failures, pre-marker consistency PASS, and final Merkle consistency PASS.

The result folders are intentionally moved under runs/ only after each runner
invocation exits. The runner has already completed its own log collection by
then, so the move does not affect the benchmark.
EOF_README

printf '%s\n' \
  'run_index,phase,config_id,repetition,worker_count,decouple_workers,det_block_parallel,fastpath,phase_trace,os_profile,exit_code,correctness,queries,wall_ms,wall_tps,runner_result_dir,console_log' \
  > "$SUMMARY_CSV"

# -----------------------------------------------------------------------------
# Fixed benchmark configuration. Keep these values identical across direct runs.
# -----------------------------------------------------------------------------
COMMON_ARGS=(
  --ordering-mode raft-kafka
  --kafka-completion-mode majority-async-all3
  --threads 8
  --preferred-leader-id 1
  --pool-size 256
  --det-window 8192
  --det-batch-size 256
  --det-pipeline-depth 1024
  --pg-exec-mode event
  --submit-mode event
  --det-raw-sql 0
  --bcdb-dt-conflict-tracking 1
  --bcdb-dt-light-snapshot 0
  --bcdb-dt-skip-readonly-gate 0
  --bcdb-dt-completion-only-skip-reads 0
  --bcdb-dt-hashtab-switch-threshold 1500
  --bcdb-det-queue-high-wm 1024
  --bcdb-det-queue-low-wm 512
  --bcdb-poll-max-us 8
  --bcdb-serial-gate-mode 1
  --bcdb-serial-gate-source 0
  --bcdb-block-enqueue-yield-every 0
)

# Direct path candidates. The two 256-worker controls isolate the effect of
# merely enabling worker decoupling from the effect of actually reducing workers.
declare -a DIRECT_CONFIGS=(
  direct_w256_linked
  direct_w256_decoupled
  direct_w064_decoupled
  direct_w048_decoupled
  direct_w032_decoupled
  direct_w024_decoupled
  direct_w016_decoupled
)
declare -A CFG_WORKERS=(
  [direct_w256_linked]=256
  [direct_w256_decoupled]=256
  [direct_w064_decoupled]=64
  [direct_w048_decoupled]=48
  [direct_w032_decoupled]=32
  [direct_w024_decoupled]=24
  [direct_w016_decoupled]=16
)
declare -A CFG_DECOUPLE=(
  [direct_w256_linked]=0
  [direct_w256_decoupled]=1
  [direct_w064_decoupled]=1
  [direct_w048_decoupled]=1
  [direct_w032_decoupled]=1
  [direct_w024_decoupled]=1
  [direct_w016_decoupled]=1
)

RUN_INDEX=0
FIRST_RUN=1
declare -A PREEXISTING_DIRS=()

snapshot_native_runner_dirs() {
  PREEXISTING_DIRS=()
  local d
  for d in "$RESULT_BASE"/cluster4_*; do
    [[ -d "$d" ]] || continue
    PREEXISTING_DIRS["$d"]=1
  done
}

extract_native_runner_dir() {
  local console_log="$1"
  local candidate=""

  # Preferred method: the runner prints the exact directory in "Server stdout".
  candidate="$(sed -nE 's|.*Server stdout[[:space:]]*:[[:space:]]*(.*)/server_node[^/]*\.log.*|\1|p' "$console_log" | tail -n 1 || true)"
  if [[ -n "$candidate" && -d "$candidate" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi

  # Failure-path fallback: identify the one cluster4 directory created by this run.
  local d fallback=""
  for d in "$RESULT_BASE"/cluster4_*; do
    [[ -d "$d" ]] || continue
    if [[ -z "${PREEXISTING_DIRS[$d]+present}" ]]; then
      fallback="$d"
    fi
  done
  printf '%s\n' "$fallback"
}

field_from_profile() {
  local profile_line="$1"
  local key="$2"
  sed -nE "s/.*(^|[[:space:]])${key}=([^[:space:]]+).*/\\2/p" <<<"$profile_line" | tail -n 1
}

classify_correctness() {
  local console_log="$1"
  local rc="$2"

  if [[ "$rc" -eq 0 ]] && \
     grep -q 'Pre-marker consistency: PASS' "$console_log" && \
     grep -q 'usertable_small consistency: PASS' "$console_log" && \
     grep -q '^divergence_count=0$' "$console_log" && \
     grep -q '^permanent_failures=0$' "$console_log" && \
     grep -q 'async_all3_failure_count=0' "$console_log"; then
    printf 'PASS\n'
  else
    printf 'FAIL\n'
  fi
}

write_command_file() {
  local path="$1"
  shift
  {
    echo '# Exact command executed by run_det_tuning_campaign.sh'
    printf '%q ' "$@"
    printf '\n'
  } > "$path"
}

append_result_row() {
  local run_index="$1" phase="$2" config_id="$3" repetition="$4"
  local workers="$5" decouple="$6" det_parallel="$7" fastpath="$8"
  local phase_trace="$9" os_profile="${10}" exit_code="${11}" correctness="${12}"
  local queries="${13}" wall_ms="${14}" wall_tps="${15}" native_dir="${16}" console_log="${17}"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$run_index" "$phase" "$config_id" "$repetition" "$workers" "$decouple" \
    "$det_parallel" "$fastpath" "$phase_trace" "$os_profile" "$exit_code" \
    "$correctness" "$queries" "$wall_ms" "$wall_tps" "$native_dir" "$console_log" \
    >> "$SUMMARY_CSV"
}

median_tps_for_config() {
  local config_id="$1"
  local phase_filter="${2:-}"
  local -a values=()

  if [[ -n "$phase_filter" ]]; then
    mapfile -t values < <(awk -F, -v c="$config_id" -v p="$phase_filter" \
      '$2 == p && $3 == c && $12 == "PASS" && $15 ~ /^[0-9]+(\.[0-9]+)?$/ { print $15 }' \
      "$SUMMARY_CSV" | sort -n)
  else
    mapfile -t values < <(awk -F, -v c="$config_id" \
      '$3 == c && $12 == "PASS" && $15 ~ /^[0-9]+(\.[0-9]+)?$/ { print $15 }' \
      "$SUMMARY_CSV" | sort -n)
  fi

  local n="${#values[@]}"
  (( n > 0 )) || return 1
  if (( n % 2 == 1 )); then
    printf '%s\n' "${values[$((n / 2))]}"
  else
    awk -v a="${values[$((n / 2 - 1))]}" -v b="${values[$((n / 2))]}" 'BEGIN { printf "%.2f\n", (a+b)/2 }'
  fi
}

write_medians() {
  local output="$OUT_ROOT/medians.txt"
  {
    echo "Campaign: $CAMPAIGN_ID"
    echo "Generated: $(date --iso-8601=seconds)"
    echo
    echo "Comparable direct-path medians [wall-clock TPS, PASS runs only]"
    printf '%-34s %10s %10s %10s\n' 'config' 'samples' 'median_TPS' 'worker/p'
    local cfg med samples workers decouple parallel
    for cfg in "${DIRECT_CONFIGS[@]}"; do
      med="$(median_tps_for_config "$cfg" "direct-worker-sweep" 2>/dev/null || true)"
      samples="$(awk -F, -v c="$cfg" '$2 == "direct-worker-sweep" && $3 == c && $12 == "PASS" {n++} END {print n+0}' "$SUMMARY_CSV")"
      workers="${CFG_WORKERS[$cfg]}"
      decouple="${CFG_DECOUPLE[$cfg]}"
      [[ -n "$med" ]] || med='n/a'
      printf '%-34s %10s %10s %8s/%s p64\n' "$cfg" "$samples" "$med" "$workers" "$decouple"
    done

    echo
    echo "All run rows"
    column -s, -t "$SUMMARY_CSV" 2>/dev/null || cat "$SUMMARY_CSV"
  } > "$output"
}

select_best_direct_config() {
  local best_cfg="" best_med=""
  local cfg med
  for cfg in "${DIRECT_CONFIGS[@]}"; do
    med="$(median_tps_for_config "$cfg" "direct-worker-sweep" 2>/dev/null || true)"
    [[ -n "$med" ]] || continue
    if [[ -z "$best_med" ]] || awk -v a="$med" -v b="$best_med" 'BEGIN { exit !(a > b) }'; then
      best_cfg="$cfg"
      best_med="$med"
    fi
  done
  [[ -n "$best_cfg" ]] || return 1
  printf '%s,%s\n' "$best_cfg" "$best_med"
}

# Arguments:
#   phase config repetition workers decouple det_parallel fastpath phase_trace os_profile extra runner args...
run_one() {
  local phase="$1" config_id="$2" repetition="$3" workers="$4" decouple="$5"
  local det_parallel="$6" fastpath="$7" phase_trace="$8" os_profile="$9"
  shift 9
  local -a extra_args=("$@")

  RUN_INDEX=$((RUN_INDEX + 1))
  local label
  printf -v label '%02d_%s_r%02d' "$RUN_INDEX" "$config_id" "$repetition"
  local console_log="$LIVE_DIR/${label}.console.log"
  local command_file="$CONFIG_DIR/${label}.command.txt"
  local config_file="$CONFIG_DIR/${label}.env"
  local native_dir=""
  local final_native_dir=""

  local skip_sync=1 skip_build=1
  if [[ "$FIRST_RUN" -eq 1 && "$PREPARE_FIRST_RUN" -eq 1 ]]; then
    # First invocation performs the runner's normal source-hash/build/sync path.
    # Subsequent repetitions use exactly the binaries proven by this first run.
    skip_sync=0
    skip_build=0
  fi

  local -a args=("${COMMON_ARGS[@]}")
  args+=(
    --bcdb-worker-count "$workers"
    --bcdb-decouple-workers "$decouple"
    --det-block-parallel "$det_parallel"
  )

  if [[ "$fastpath" -eq 1 ]]; then
    args+=(
      --det-prefixed-direct-parallel 0
      --det-event-block-fastpath 1
      --det-completion-only-success 0
      --det-block-pipeline 1
      --det-block-max 256
      --det-partial-block-max-wait-us 0
      --bcdb-block-wait-watermark 1
      --bcdb-block-profile 1
      --bcdb-dt-parse-barrier 0
    )
  else
    args+=(
      --det-prefixed-direct-parallel 1
      --det-event-block-fastpath 0
      --det-completion-only-success 0
      --bcdb-block-wait-watermark 0
      --bcdb-block-profile 0
      --bcdb-dt-parse-barrier 0
    )
  fi

  if [[ "$phase_trace" -eq 1 ]]; then
    args+=(--bcdb-phase-trace 1)
  else
    args+=(--bcdb-phase-trace 0)
  fi

  args+=("${extra_args[@]}")

  local -a env_pairs=(
    "SKIP_RDKAFKA_SETUP=1"
    "SKIP_SYNC=$skip_sync"
    "SKIP_BUILD=$skip_build"
    "FORCE_BUILD=0"
    "FORCE_PG_RESTART=1"
    "KAFKA_COMPLETION_MODE=majority-async-all3"
    "ARIABC_FULL_RESULT_REPLICA_LIMIT=2"
    "ARIABC_RESULT_PUBLISH_REPLICA_LIMIT=0"
    "ARIABC_OS_PROFILE=$os_profile"
  )

  {
    echo "label=$label"
    echo "phase=$phase"
    echo "config_id=$config_id"
    echo "repetition=$repetition"
    echo "worker_count=$workers"
    echo "decouple_workers=$decouple"
    echo "det_block_parallel=$det_parallel"
    echo "fastpath=$fastpath"
    echo "phase_trace=$phase_trace"
    echo "os_profile=$os_profile"
    echo "skip_sync=$skip_sync"
    echo "skip_build=$skip_build"
    printf 'environment='; printf '%q ' "${env_pairs[@]}"; printf '\n'
    printf 'runner_args='; printf '%q ' "${args[@]}"; printf '\n'
  } > "$config_file"

  write_command_file "$command_file" timeout --foreground "$RUN_TIMEOUT_SECONDS" env "${env_pairs[@]}" bash "$RUNNER" "${args[@]}"

  log ""
  log "=============================================================================="
  log "RUN $RUN_INDEX | $phase | $config_id | repetition=$repetition"
  log "workers=$workers decouple=$decouple detBlockParallel=$det_parallel fastpath=$fastpath trace=$phase_trace osProfile=$os_profile"
  log "Live log: $console_log"
  log "=============================================================================="

  snapshot_native_runner_dirs

  local runner_rc
  set +e
  timeout --foreground "$RUN_TIMEOUT_SECONDS" \
    env "${env_pairs[@]}" \
    bash "$RUNNER" "${args[@]}" \
    2>&1 | tee "$console_log"
  runner_rc="${PIPESTATUS[0]}"
  set -e
  FIRST_RUN=0

  native_dir="$(extract_native_runner_dir "$console_log")"
  if [[ -n "$native_dir" && -d "$native_dir" ]]; then
    final_native_dir="$RUNS_DIR/$label"
    if mv "$native_dir" "$final_native_dir"; then
      log "Native runner folder captured: $final_native_dir"
    else
      log "WARNING: could not move native runner folder: $native_dir"
      final_native_dir="$native_dir"
    fi
  else
    log "WARNING: no native runner folder could be identified for $label"
    final_native_dir=""
  fi

  local profile_line queries wall_ms wall_tps correctness
  profile_line="$(grep 'PROFILE_GATEWAY .*overall_wall_ms=' "$console_log" | head -n 1 || true)"
  queries="$(sed -nE 's/.*Queries[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' "$console_log" | head -n 1 || true)"
  [[ -n "$queries" ]] || queries="$WORKLOAD_QUERIES"
  wall_ms="$(field_from_profile "$profile_line" 'overall_wall_ms' || true)"
  if [[ -n "$wall_ms" ]] && awk -v v="$wall_ms" 'BEGIN { exit !(v > 0) }'; then
    wall_tps="$(awk -v q="$queries" -v ms="$wall_ms" 'BEGIN { printf "%.2f", q * 1000.0 / ms }')"
  else
    wall_tps=""
  fi
  correctness="$(classify_correctness "$console_log" "$runner_rc")"

  append_result_row \
    "$RUN_INDEX" "$phase" "$config_id" "$repetition" "$workers" "$decouple" \
    "$det_parallel" "$fastpath" "$phase_trace" "$os_profile" "$runner_rc" \
    "$correctness" "$queries" "$wall_ms" "$wall_tps" "$final_native_dir" "$console_log"

  log "RUN $RUN_INDEX result: exit=$runner_rc correctness=$correctness wall_ms=${wall_ms:-n/a} wall_tps=${wall_tps:-n/a}"
  write_medians
  return 0
}

run_direct_config() {
  local config_id="$1" repetition="$2"
  run_one \
    'direct-worker-sweep' "$config_id" "$repetition" \
    "${CFG_WORKERS[$config_id]}" "${CFG_DECOUPLE[$config_id]}" \
    64 0 0 0
}

# -----------------------------------------------------------------------------
# Phase A: worker-count sweep. Each round rotates ordering to avoid giving one
# configuration all cold-cache or all late-run positions.
# -----------------------------------------------------------------------------
log "Campaign output folder: $OUT_ROOT"
log "Workload: $WORKLOAD_FILE [$WORKLOAD_QUERIES statements]"
log "Phase A: direct per-transaction worker-count sweep, $REPETITIONS repetitions per configuration."

ROUND_ORDERS=(
  'direct_w256_linked direct_w032_decoupled direct_w256_decoupled direct_w048_decoupled direct_w024_decoupled direct_w064_decoupled direct_w016_decoupled'
  'direct_w064_decoupled direct_w256_linked direct_w024_decoupled direct_w048_decoupled direct_w256_decoupled direct_w016_decoupled direct_w032_decoupled'
  'direct_w024_decoupled direct_w048_decoupled direct_w256_linked direct_w016_decoupled direct_w032_decoupled direct_w064_decoupled direct_w256_decoupled'
)

for ((rep = 1; rep <= REPETITIONS; rep++)); do
  # For repetitions beyond three, rotate the first order deterministically.
  order_string="${ROUND_ORDERS[$(((rep - 1) % ${#ROUND_ORDERS[@]}))]}"
  read -r -a order <<< "$order_string"
  for config_id in "${order[@]}"; do
    run_direct_config "$config_id" "$rep"
  done
  write_medians
  log "Completed direct sweep round $rep/$REPETITIONS. Current medians: $OUT_ROOT/medians.txt"
done

BEST_LINE="$(select_best_direct_config || true)"
if [[ -z "$BEST_LINE" ]]; then
  log "ERROR: no correctness-PASS direct configuration was available. Skipping refinement, trace, and fastpath phases."
  {
    echo 'status=no_valid_direct_configuration'
    echo "finished_at=$(date --iso-8601=seconds)"
  } > "$OUT_ROOT/final_choice.env"
  write_medians
  exit 1
fi

IFS=',' read -r BEST_CONFIG BEST_MEDIAN_TPS <<< "$BEST_LINE"
BEST_WORKERS="${CFG_WORKERS[$BEST_CONFIG]}"
BEST_DECOUPLE="${CFG_DECOUPLE[$BEST_CONFIG]}"
FINAL_CONFIG="$BEST_CONFIG"
FINAL_PARALLEL=64
FINAL_MEDIAN_TPS="$BEST_MEDIAN_TPS"

log "Phase A winner: $BEST_CONFIG [workers=$BEST_WORKERS decouple=$BEST_DECOUPLE p64 median=${BEST_MEDIAN_TPS} TPS]"

# -----------------------------------------------------------------------------
# Phase B: one focused direct-path refinement. Existing data already gives p64
# three repetitions for the winning worker setup. Three p48 runs reveal whether
# p64 remains optimal after worker decoupling without launching a broad lottery.
# -----------------------------------------------------------------------------
REFINE_CONFIG="direct_refine_w$(printf '%03d' "$BEST_WORKERS")_d${BEST_DECOUPLE}_p048"
log "Phase B: direct refinement for winning worker setting: p=48, $REPETITIONS repetitions."
for ((rep = 1; rep <= REPETITIONS; rep++)); do
  run_one \
    'direct-parallel-refine' "$REFINE_CONFIG" "$rep" \
    "$BEST_WORKERS" "$BEST_DECOUPLE" \
    48 0 0 0
done

REFINE_MEDIAN_TPS="$(median_tps_for_config "$REFINE_CONFIG" 'direct-parallel-refine' 2>/dev/null || true)"
if [[ -n "$REFINE_MEDIAN_TPS" ]] && awk -v a="$REFINE_MEDIAN_TPS" -v b="$FINAL_MEDIAN_TPS" 'BEGIN { exit !(a > b) }'; then
  FINAL_CONFIG="$REFINE_CONFIG"
  FINAL_PARALLEL=48
  FINAL_MEDIAN_TPS="$REFINE_MEDIAN_TPS"
fi

{
  echo "status=valid_direct_configuration_selected"
  echo "selected_config=$FINAL_CONFIG"
  echo "selected_workers=$BEST_WORKERS"
  echo "selected_decouple_workers=$BEST_DECOUPLE"
  echo "selected_det_block_parallel=$FINAL_PARALLEL"
  echo "selected_median_wall_tps=$FINAL_MEDIAN_TPS"
  echo "initial_p64_winner=$BEST_CONFIG"
  echo "initial_p64_median_wall_tps=$BEST_MEDIAN_TPS"
  echo "p48_refinement_config=$REFINE_CONFIG"
  echo "p48_refinement_median_wall_tps=${REFINE_MEDIAN_TPS:-n/a}"
  echo "selected_at=$(date --iso-8601=seconds)"
} > "$OUT_ROOT/final_choice.env"

write_medians
log "Selected direct configuration: $FINAL_CONFIG [workers=$BEST_WORKERS decouple=$BEST_DECOUPLE p=$FINAL_PARALLEL median=${FINAL_MEDIAN_TPS} TPS]"

# -----------------------------------------------------------------------------
# Phase C: diagnostics. These are not used for headline TPS. They exist to
# isolate the phase which improved or regressed and to collect OS snapshots.
# -----------------------------------------------------------------------------
if [[ "$RUN_PHASE_TRACES" -eq 1 ]]; then
  log "Phase C: one baseline trace and one selected-direct trace. These may be slower because instrumentation is enabled."
  run_one \
    'diagnostic-phase-trace' 'trace_baseline_w256_linked_p064' 1 \
    256 0 64 0 1 1
  run_one \
    'diagnostic-phase-trace' "trace_selected_${FINAL_CONFIG}" 1 \
    "$BEST_WORKERS" "$BEST_DECOUPLE" "$FINAL_PARALLEL" 0 1 1
else
  log "Phase C skipped because RUN_PHASE_TRACES=0"
fi

# -----------------------------------------------------------------------------
# Phase D: structural comparison. The block fast path amortizes per-request
# libpq / backend setup. It is deliberately kept separate from direct-path
# medians because its batching path differs, while the same correctness checks
# still apply.
# -----------------------------------------------------------------------------
if [[ "$RUN_FASTPATH" -eq 1 ]]; then
  log "Phase D: block-fastpath exploration at selected worker count, p=1 and p=2, $REPETITIONS repetitions each."
  for ((rep = 1; rep <= REPETITIONS; rep++)); do
    run_one \
      'block-fastpath' "fastpath_w$(printf '%03d' "$BEST_WORKERS")_d${BEST_DECOUPLE}_p001" "$rep" \
      "$BEST_WORKERS" "$BEST_DECOUPLE" \
      1 1 0 0
    run_one \
      'block-fastpath' "fastpath_w$(printf '%03d' "$BEST_WORKERS")_d${BEST_DECOUPLE}_p002" "$rep" \
      "$BEST_WORKERS" "$BEST_DECOUPLE" \
      2 1 0 0
  done
else
  log "Phase D skipped because RUN_FASTPATH=0"
fi

write_medians
{
  echo
  echo "Campaign complete: $(date --iso-8601=seconds)"
  echo "Selected direct configuration: $FINAL_CONFIG"
  echo "Selected direct median wall TPS: $FINAL_MEDIAN_TPS"
  echo "Campaign folder: $OUT_ROOT"
} | tee -a "$OUT_ROOT/README.txt"

log "=============================================================================="
log "CAMPAIGN COMPLETE"
log "Send this folder for analysis: $OUT_ROOT"
log "Summary: $SUMMARY_CSV"
log "Medians: $OUT_ROOT/medians.txt"
log "Chosen direct setup: $OUT_ROOT/final_choice.env"
log "=============================================================================="
