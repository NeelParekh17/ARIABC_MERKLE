#!/usr/bin/env bash
# Sweep Raft+Kafka deterministic throughput across logical gateway lanes.
# Use with the current known-good binaries. Every case keeps the post-run
# marker + Merkle verification enabled.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results"

[[ -x "$RUNNER" ]] || { echo "ERROR: runner not executable: $RUNNER" >&2; exit 2; }

# Logical terminal counts. Override, e.g. THREADS='1 2 4 8 16'.
THREADS="${THREADS:-1 2 4 8 16 32 64}"
REPEATS="${REPEATS:-1}"
MAX_TOTAL_WINDOW="${MAX_TOTAL_WINDOW:-8192}"
PER_LANE_WINDOW="${PER_LANE_WINDOW:-1024}"
KAFKA_DELAY_US="${KAFKA_DELAY_US:-}"

STAMP="$(date +%Y%m%d_%H%M%S)"
CAMPAIGN_DIR="$RESULT_ROOT/raft_kafka_thread_sweep_${STAMP}"
mkdir -p "$CAMPAIGN_DIR/runner_console"
printf 'repeat\tthreads\tdet_window\tpipeline_depth\tstatus\ttps\tmerkle\trun_dir\n' \
  > "$CAMPAIGN_DIR/summary.tsv"

# Known-good base configuration from the stable ~8k run.
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
  --parallelism-mode pipeline
)

find_new_run_dir() {
  local marker="$1"
  find "$RESULT_ROOT" -maxdepth 1 -mindepth 1 -type d -name 'cluster4_*' -newer "$marker" \
    -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-
}

for rep in $(seq 1 "$REPEATS"); do
  for threads in $THREADS; do
    [[ "$threads" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: bad thread value: $threads" >&2; exit 2; }

    # Increase offered in-flight work up to the proven 8,192-request cap,
    # then split that same cap across more terminal lanes.
    desired_window=$((threads * PER_LANE_WINDOW))
    if (( desired_window > MAX_TOTAL_WINDOW )); then
      det_window="$MAX_TOTAL_WINDOW"
    else
      det_window="$desired_window"
    fi
    depth=$((det_window / threads))
    (( depth >= 1 )) || { echo "ERROR: pipeline depth became zero" >&2; exit 2; }
    det_window=$((depth * threads))  # keep lane count × per-lane depth exact

    label="r${rep}_t${threads}_w${det_window}_d${depth}"
    console="$CAMPAIGN_DIR/runner_console/${label}.log"
    marker="$CAMPAIGN_DIR/.before_${label}"
    touch "$marker"

    echo
    echo "========================================================================"
    echo "CASE $label: threads=$threads detWindow=$det_window depth=$depth"
    echo "========================================================================"

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
        --det-window "$det_window" \
        --det-pipeline-depth "$depth" \
        "${BASE_ARGS[@]}" \
        2>&1 | tee "$console"
    rc=${PIPESTATUS[0]}
    set -e

    run_dir="$(find_new_run_dir "$marker")"
    tps="$(grep -E 'TPS \(gateway\)[[:space:]]*:' "$console" | tail -1 | sed -nE 's/.*~([0-9]+(\.[0-9]+)?).*/\1/p')"
    [[ -n "$tps" ]] || tps="NA"

    merkle="FAIL"
    grep -q 'usertable_small consistency: PASS' "$console" && merkle="PASS"
    status="PASS"
    (( rc == 0 )) || status="EXIT_${rc}"
    [[ "$merkle" == "PASS" ]] || status="${status}_MERKLE_FAIL"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$rep" "$threads" "$det_window" "$depth" "$status" "$tps" "$merkle" "${run_dir:-MISSING}" \
      >> "$CAMPAIGN_DIR/summary.tsv"

    if [[ -n "$run_dir" && -d "$run_dir" ]]; then
      ln -sfn "$run_dir" "$CAMPAIGN_DIR/$label"
    fi
  done
done

echo
echo "Campaign complete."
echo "Summary: $CAMPAIGN_DIR/summary.tsv"
echo "Console logs: $CAMPAIGN_DIR/runner_console/"
echo
column -ts $'\t' "$CAMPAIGN_DIR/summary.tsv" 2>/dev/null || cat "$CAMPAIGN_DIR/summary.tsv"
