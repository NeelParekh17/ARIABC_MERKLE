#!/usr/bin/env bash
# cluster_raft_kafka_fixed_window_sweep.sh
#
# Fair Raft+Kafka logical-lane sweep for the current known-good cluster path.
#
# Design:
#   - Every measured case uses the SAME total DET window (default 8192).
#   - Only the number of logical terminal lanes changes.
#   - An 8-lane sentinel runs before and after each repetition to detect drift.
#   - Each finished cluster4_* result directory is COPIED into the campaign,
#     so an archive contains server/PostgreSQL/NuRaft logs, not broken symlinks.
#   - Runner configuration matches the known-good manual command exactly,
#     except for --threads/--det-window/--det-pipeline-depth.
#
# Usage:
#   cd /work/ARIABC/AriaBC/scripts/distributed
#   chmod +x cluster_raft_kafka_fixed_window_sweep.sh
#   REPEATS=3 ./cluster_raft_kafka_fixed_window_sweep.sh
#
# Optional:
#   THREADS="1 2 4 8 16 32 64" WINDOW=8192 REPEATS=3 CASE_COOLDOWN_S=15 \
#     ./cluster_raft_kafka_fixed_window_sweep.sh

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results"

THREADS="${THREADS:-1 2 4 8 16 32 64}"
WINDOW="${WINDOW:-8192}"
REPEATS="${REPEATS:-3}"
CASE_COOLDOWN_S="${CASE_COOLDOWN_S:-15}"
SENTINEL_THREADS="${SENTINEL_THREADS:-8}"
KAFKA_DELAY_US="${KAFKA_DELAY_US:-}"

[[ -x "$RUNNER" ]] || { echo "ERROR: runner not executable: $RUNNER" >&2; exit 2; }
[[ "$WINDOW" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: WINDOW must be a positive integer" >&2; exit 2; }
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: REPEATS must be a positive integer" >&2; exit 2; }

for t in $THREADS "$SENTINEL_THREADS"; do
  [[ "$t" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: invalid thread count: $t" >&2; exit 2; }
  (( WINDOW % t == 0 )) || {
    echo "ERROR: WINDOW=$WINDOW must divide exactly by threads=$t." >&2
    echo "       Choose thread counts that divide the fixed total window." >&2
    exit 2
  }
done

STAMP="$(date +%Y%m%d_%H%M%S)"
CAMPAIGN_DIR="$RESULT_ROOT/raft_kafka_fixed_window_sweep_${STAMP}"
mkdir -p "$CAMPAIGN_DIR/cases"

cat > "$CAMPAIGN_DIR/README.txt" <<EOF
Fair fixed-window logical-lane campaign.
total_det_window=$WINDOW
repeats=$REPEATS
threads=$THREADS
sentinel_threads=$SENTINEL_THREADS
case_cooldown_s=$CASE_COOLDOWN_S

Each measured point holds total in-flight work fixed at $WINDOW.
Only logical gateway-lane count and per-lane depth vary.
An 8-lane sentinel is repeated before and after each round to detect
runtime drift independent of lane-count effects.
EOF

printf 'round\tposition\tthreads\tdet_window\tdepth\tstatus\ttps\tmerkle\tall3\trun_dir\n' \
  > "$CAMPAIGN_DIR/summary.tsv"

# Exact known-good manual configuration, excluding the three variables
# deliberately swept below: threads, total window, per-lane pipeline depth.
BASE_ARGS=(
  --preferred-leader-id 1
  --det-batch-size 256
  --pool-size 256
  --bcdb-worker-count 24
  --bcdb-decouple-workers 1
  --det-block-parallel 64
  --det-event-block-fastpath 0
  --det-prefixed-direct-parallel 1
  --det-completion-only-success 0
  --bcdb-dt-parse-barrier 0
  --bcdb-block-profile 0
  --bcdb-phase-trace 0
  --bcdb-block-wait-watermark 0
  --bcdb-serial-gate-mode 1
  --bcdb-serial-gate-source 0
  --bcdb-det-queue-high-wm 1024
  --bcdb-det-queue-low-wm 512
  --ordering-mode raft-kafka
  --pg-exec-mode event
  --submit-mode event
  --det-raw-sql 0
  --bcdb-dt-conflict-tracking 1
  --bcdb-dt-light-snapshot 0
  --bcdb-dt-skip-readonly-gate 0
  --bcdb-dt-completion-only-skip-reads 0
  --bcdb-dt-hashtab-switch-threshold 1500
  --bcdb-poll-max-us 8
  --bcdb-block-enqueue-yield-every 0
)

find_new_run_dir() {
  local marker="$1"
  find "$RESULT_ROOT" -maxdepth 1 -mindepth 1 -type d -name 'cluster4_*' -newer "$marker" \
    -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-
}

run_case() {
  local round="$1"
  local position="$2"
  local threads="$3"
  local depth=$(( WINDOW / threads ))
  local label="r${round}_${position}_t${threads}_w${WINDOW}_d${depth}"
  local case_dir="$CAMPAIGN_DIR/cases/$label"
  local console="$case_dir/runner_console.log"
  local marker="$case_dir/.before_run"

  mkdir -p "$case_dir"
  touch "$marker"

  cat > "$case_dir/command.txt" <<EOF
env ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US='' FORCE_BUILD=0 SKIP_SYNC=1 SKIP_BUILD=1 \\
  SKIP_RDKAFKA_SETUP=1 KAFKA_COMPLETION_MODE=majority-async-all3 \\
  ARIABC_FULL_RESULT_REPLICA_LIMIT=2 ARIABC_RESULT_PUBLISH_REPLICA_LIMIT=0 \\
  ARIABC_OS_PROFILE=0 timeout -k 45s 900s ./run_4node_raft_cluster.sh \\
  --threads $threads --det-window $WINDOW --det-pipeline-depth $depth \\
  ${BASE_ARGS[*]}
EOF

  echo
  echo "=============================================================================="
  echo "CASE $label"
  echo "threads=$threads total_window=$WINDOW per_lane_depth=$depth"
  echo "=============================================================================="

  set +e
  env \
    ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US="$KAFKA_DELAY_US" \
    FORCE_BUILD=0 SKIP_SYNC=1 SKIP_BUILD=1 SKIP_RDKAFKA_SETUP=1 \
    KAFKA_COMPLETION_MODE=majority-async-all3 \
    ARIABC_FULL_RESULT_REPLICA_LIMIT=2 \
    ARIABC_RESULT_PUBLISH_REPLICA_LIMIT=0 \
    ARIABC_OS_PROFILE=0 \
    timeout -k 45s 900s "$RUNNER" \
      --threads "$threads" \
      --det-window "$WINDOW" \
      --det-pipeline-depth "$depth" \
      "${BASE_ARGS[@]}" \
      2>&1 | tee "$console"
  local rc=${PIPESTATUS[0]}
  set -e

  local run_dir
  run_dir="$(find_new_run_dir "$marker")"
  if [[ -n "$run_dir" && -d "$run_dir" ]]; then
    # Copy real logs. Absolute symlinks become useless after zip/tar upload.
    cp -a "$run_dir" "$case_dir/cluster4_result"
  fi

  local tps merkle all3 status
  tps="$(grep -E 'TPS \(gateway\)[[:space:]]*:' "$console" | tail -1 | sed -nE 's/.*~([0-9]+(\.[0-9]+)?).*/\1/p')"
  [[ -n "$tps" ]] || tps="NA"

  merkle="FAIL"
  grep -q 'usertable_small consistency: PASS' "$console" && merkle="PASS"

  all3="FAIL"
  grep -Eq 'async_all3_verified_count=20513.*async_all3_failure_count=0.*async_all3_timeout_count=0.*async_all3_missing_count=0' \
    "$console" && all3="PASS"

  status="PASS"
  (( rc == 0 )) || status="EXIT_${rc}"
  [[ "$merkle" == "PASS" ]] || status="${status}_MERKLE_FAIL"
  [[ "$all3" == "PASS" ]] || status="${status}_ALL3_FAIL"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" "$position" "$threads" "$WINDOW" "$depth" "$status" "$tps" "$merkle" "$all3" \
    "${run_dir:-MISSING}" >> "$CAMPAIGN_DIR/summary.tsv"

  if (( CASE_COOLDOWN_S > 0 )); then
    echo "Cooling down for ${CASE_COOLDOWN_S}s before next case..."
    sleep "$CASE_COOLDOWN_S"
  fi
}

# One excluded warm-up lets the Kafka broker and all machines settle.
run_case 0 warmup "$SENTINEL_THREADS"

for round in $(seq 1 "$REPEATS"); do
  run_case "$round" pre_sentinel "$SENTINEL_THREADS"

  # Rotate order each round. It prevents a fixed early/late position from being
  # mistaken for a lane-count effect without introducing a random dependency.
  ordered=($THREADS)
  shift_by=$(( (round - 1) % ${#ordered[@]} ))
  rotated=( "${ordered[@]:$shift_by}" "${ordered[@]:0:$shift_by}" )

  for threads in "${rotated[@]}"; do
    # The pre/post sentinels already measure the 8-lane point twice per round.
    [[ "$threads" == "$SENTINEL_THREADS" ]] && continue
    run_case "$round" measured "$threads"
  done

  run_case "$round" post_sentinel "$SENTINEL_THREADS"
done

echo
echo "Campaign complete."
echo "Summary: $CAMPAIGN_DIR/summary.tsv"
column -ts $'\t' "$CAMPAIGN_DIR/summary.tsv" 2>/dev/null || cat "$CAMPAIGN_DIR/summary.tsv"
