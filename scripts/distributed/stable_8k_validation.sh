#!/usr/bin/env bash
# Stable near-8k validation for raft-kafka + Kafka-majority deterministic mode.
# Runs three configurations in an interleaved order, five repetitions each,
# captures the runner's native result folders, live console logs, and CSV summary.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results"
REPS="${REPS:-5}"
TIMEOUT_S="${TIMEOUT_S:-900}"
FIRST_RUN=1

[[ -x "$RUNNER" ]] || { echo "ERROR: runner not found/executable: $RUNNER" >&2; exit 2; }
mkdir -p "$RESULT_ROOT"

OUT="$RESULT_ROOT/stable_8k_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT" "$OUT/live" "$OUT/runs" "$OUT/configs"

printf 'started_at=%s\nrepo_root=%s\nrunner=%s\nrepetitions=%s\ntimeout_s=%s\n' \
  "$(date --iso-8601=seconds)" "$REPO_ROOT" "$RUNNER" "$REPS" "$TIMEOUT_S" \
  > "$OUT/manifest.env"
printf 'label,workers,det_block_parallel,batch_delay_us,exit_code,wall_ms,wall_tps,correctness,node1_kafka_delivery_pending_max,node1_result_flush_count,node1_kafka_delivery_ms_avg,gateway_consume_to_ready_ms_p95,gateway_vote_store_mutex_wait_us_p95,gateway_vote_store_mutex_hold_us_p95,gateway_kafka_messages,runner_result_dir,classification\n' > "$OUT/summary.csv"


log() { printf '[%(%F %T)T] %s\n' -1 "$*" | tee -a "$OUT/campaign.log"; }

newest_runner_dir() {
  find "$RESULT_ROOT" -maxdepth 1 -mindepth 1 -type d -name 'cluster4_*' \
    -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-
}

extract_metrics() {
  local copied="$1"
  local console_log="$2"
  local gw="$copied/gateway_test.log"
  [[ -f "$gw" ]] || gw="$copied/runner.log"
  [[ -f "$gw" ]] || gw="$console_log"

  WALL_MS=""
  WALL_TPS=""
  CORRECTNESS="FAIL"
  KAFKA_PENDING_MAX=""
  RESULT_FLUSH_COUNT=""
  KAFKA_DELIVERY_MS_AVG=""
  GW_P95=""
  GW_MUTEX_WAIT_P95=""
  GW_MUTEX_HOLD_P95=""
  GW_KAFKA_MSGS=""

  WALL_MS="$(sed -n -E 's/.*overall wall time including drains \(millisec\) = ([0-9]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  if [[ -z "$WALL_MS" ]]; then
    WALL_MS="$(sed -n -E 's/.*overall_wall_ms=([0-9]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  fi
  if [[ -z "$WALL_MS" && -f "$console_log" ]]; then
    WALL_MS="$(sed -n -E 's/.*overall wall time including drains \(millisec\) = ([0-9]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
    if [[ -z "$WALL_MS" ]]; then
      WALL_MS="$(sed -n -E 's/.*overall_wall_ms=([0-9]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
    fi
  fi

  if [[ "$WALL_MS" =~ ^[0-9]+$ ]] && (( WALL_MS > 0 )); then
    WALL_TPS="$(awk -v n=20513 -v ms="$WALL_MS" 'BEGIN { printf "%.2f", n*1000/ms }')"
  fi

  local has_div=0
  local has_perm=0
  local has_async_fail=0
  local has_pre_marker=0
  local has_usertable_small=0
  local has_full_completion=0

  check_correctness_markers() {
    local f="$1"
    [[ -f "$f" ]] || return 0
    if grep -q 'divergence_count=0' "$f"; then has_div=1; fi
    if grep -q 'permanent_failures=0' "$f"; then has_perm=1; fi
    if grep -q 'async_all3_failure_count=0' "$f"; then has_async_fail=1; fi
    if grep -q 'Pre-marker consistency: PASS' "$f"; then has_pre_marker=1; fi
    if grep -q 'usertable_small consistency: PASS' "$f"; then has_usertable_small=1; fi
    if grep -q 'client_quorum_complete_count=20513' "$f"; then has_full_completion=1; fi
    return 0
  }

  check_correctness_markers "$gw"
  check_correctness_markers "$copied/runner.log"
  check_correctness_markers "$console_log"

  if [[ "$has_div" -eq 1 &&
        "$has_perm" -eq 1 &&
        "$has_async_fail" -eq 1 &&
        "$has_pre_marker" -eq 1 &&
        "$has_usertable_small" -eq 1 &&
        "$has_full_completion" -eq 1 ]]; then
    CORRECTNESS="PASS"
  fi

  local node1_log
  node1_log="$(find "$copied" -name 'server_node1_*.log' 2>/dev/null | head -1)"
  if [[ -n "$node1_log" && -f "$node1_log" ]]; then
    KAFKA_PENDING_MAX="$(sed -n -E 's/.*kafka_delivery_pending_max=([0-9]+).*/\1/p' "$node1_log" 2>/dev/null | tail -1)"
    RESULT_FLUSH_COUNT="$(sed -n -E 's/.*result_flush_count=([0-9]+).*/\1/p' "$node1_log" 2>/dev/null | tail -1)"
    KAFKA_DELIVERY_MS_AVG="$(sed -n -E 's/.*kafka_delivery_ms_avg=([0-9.]+).*/\1/p' "$node1_log" 2>/dev/null | tail -1)"
  fi

  GW_P95="$(sed -n -E 's/.*consume_to_ready_ms_p95=([0-9.]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  if [[ -z "$GW_P95" && -f "$console_log" ]]; then
    GW_P95="$(sed -n -E 's/.*consume_to_ready_ms_p95=([0-9.]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
  fi

  GW_MUTEX_WAIT_P95="$(sed -n -E 's/.*vote_store_mutex_wait_us_p95=([0-9.]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  if [[ -z "$GW_MUTEX_WAIT_P95" && -f "$console_log" ]]; then
    GW_MUTEX_WAIT_P95="$(sed -n -E 's/.*vote_store_mutex_wait_us_p95=([0-9.]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
  fi

  GW_MUTEX_HOLD_P95="$(sed -n -E 's/.*vote_store_mutex_hold_us_p95=([0-9.]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  if [[ -z "$GW_MUTEX_HOLD_P95" && -f "$console_log" ]]; then
    GW_MUTEX_HOLD_P95="$(sed -n -E 's/.*vote_store_mutex_hold_us_p95=([0-9.]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
  fi

  GW_KAFKA_MSGS="$(sed -n -E 's/.*kafka_msgs=([0-9]+).*/\1/p' "$gw" 2>/dev/null | tail -1)"
  if [[ -z "$GW_KAFKA_MSGS" && -f "$console_log" ]]; then
    GW_KAFKA_MSGS="$(sed -n -E 's/.*kafka_msgs=([0-9]+).*/\1/p' "$console_log" 2>/dev/null | tail -1)"
  fi

  if [[ -z "$KAFKA_PENDING_MAX" ]]; then KAFKA_PENDING_MAX="NA"; fi
  if [[ -z "$RESULT_FLUSH_COUNT" ]]; then RESULT_FLUSH_COUNT="NA"; fi
  if [[ -z "$KAFKA_DELIVERY_MS_AVG" ]]; then KAFKA_DELIVERY_MS_AVG="NA"; fi
  if [[ -z "$GW_P95" ]]; then GW_P95="NA"; fi
  if [[ -z "$GW_MUTEX_WAIT_P95" ]]; then GW_MUTEX_WAIT_P95="NA"; fi
  if [[ -z "$GW_MUTEX_HOLD_P95" ]]; then GW_MUTEX_HOLD_P95="NA"; fi
  if [[ -z "$GW_KAFKA_MSGS" ]]; then GW_KAFKA_MSGS="NA"; fi
}

run_one() {
  local run_no="$1" workers="$2" parallel="$3" batch_delay_us="$4" in_campaign="$5"
  local label
  if [[ "$in_campaign" -eq 1 ]]; then
    label="$(printf '%02d_w%03d_p%03d' "$run_no" "$workers" "$parallel")"
  else
    label="$(printf 'select_%04d_r%d_w%03d_p%03d' "$batch_delay_us" "$run_no" "$workers" "$parallel")"
  fi
  local console="$OUT/live/${label}.console.log"
  local cfg="$OUT/configs/${label}.env"
  local before after copied

  cat > "$cfg" <<CFG
workers=$workers
det_block_parallel=$parallel
pool_size=256
det_window=8192
det_batch_size=256
det_pipeline_depth=1024
threads=8
completion_mode=majority-async-all3
prefixed_direct_parallel=1
fastpath=0
parse_barrier=0
serial_gate_mode=1
serial_gate_source=0
queue_high_wm=1024
queue_low_wm=512
ordering_mode=raft-kafka
pg_exec_mode=event
submit_mode=event
det_raw_sql=0
bcdb_dt_conflict_tracking=1
bcdb_dt_light_snapshot=0
bcdb_dt_skip_readonly_gate=0
bcdb_dt_completion_only_skip_reads=0
bcdb_dt_hashtab_switch_threshold=1500
bcdb_poll_max_us=8
bcdb_block_enqueue_yield_every=0
batch_delay_us=$batch_delay_us
CFG

  local skip_sync=1
  local skip_build=1
  local force_build=0

  if [[ "$FIRST_RUN" -eq 1 ]]; then
    skip_sync=0
    skip_build=0
    force_build=1
    FIRST_RUN=0
  fi

  before="$(newest_runner_dir || true)"
  log "START $label workers=$workers parallel=$parallel batch_delay_us=$batch_delay_us skip_sync=$skip_sync skip_build=$skip_build"

  set +e
  timeout -k 45s "${TIMEOUT_S}s" env \
    SKIP_SYNC="$skip_sync" \
    SKIP_BUILD="$skip_build" \
    FORCE_BUILD="$force_build" \
    SKIP_RDKAFKA_SETUP=1 \
    KAFKA_COMPLETION_MODE=majority-async-all3 \
    ARIABC_FULL_RESULT_REPLICA_LIMIT=2 \
    ARIABC_RESULT_PUBLISH_REPLICA_LIMIT=0 \
    ARIABC_OS_PROFILE=0 \
    BCDB_GATE_TELEMETRY=0 \
    BCDB_GATE_SNAPSHOT_EACH_BLOCK=0 \
    ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US="$batch_delay_us" \
    "$RUNNER" \
      --threads 8 \
      --preferred-leader-id 1 \
      --det-window 8192 \
      --det-batch-size 256 \
      --det-pipeline-depth 1024 \
      --pool-size 256 \
      --bcdb-worker-count "$workers" \
      --bcdb-decouple-workers 1 \
      --det-block-parallel "$parallel" \
      --det-event-block-fastpath 0 \
      --det-prefixed-direct-parallel 1 \
      --det-completion-only-success 0 \
      --bcdb-dt-parse-barrier 0 \
      --bcdb-block-profile 0 \
      --bcdb-phase-trace 0 \
      --bcdb-block-wait-watermark 0 \
      --bcdb-serial-gate-mode 1 \
      --bcdb-serial-gate-source 0 \
      --bcdb-det-queue-high-wm 1024 \
      --bcdb-det-queue-low-wm 512 \
      --ordering-mode raft-kafka \
      --pg-exec-mode event \
      --submit-mode event \
      --det-raw-sql 0 \
      --bcdb-dt-conflict-tracking 1 \
      --bcdb-dt-light-snapshot 0 \
      --bcdb-dt-skip-readonly-gate 0 \
      --bcdb-dt-completion-only-skip-reads 0 \
      --bcdb-dt-hashtab-switch-threshold 1500 \
      --bcdb-poll-max-us 8 \
      --bcdb-block-enqueue-yield-every 0 \
      2>&1 | tee "$console"
  rc=${PIPESTATUS[0]}
  set -e

  after="$(newest_runner_dir || true)"
  copied="$OUT/runs/${label}"
  if [[ -n "$after" && "$after" != "$before" && -d "$after" ]]; then
    cp -a "$after" "$copied"
  else
    mkdir -p "$copied"
    cp -a "$console" "$copied/console.log"
    cp -a "$cfg" "$copied/config.env"
  fi

  extract_metrics "$copied" "$console"

  local classification="FAIL"
  if [[ "$rc" -eq 0 && "$CORRECTNESS" == "PASS" ]]; then
    local is_gte_8k
    is_gte_8k=$(awk -v tps="${WALL_TPS:-0}" 'BEGIN { if (tps >= 8000) print 1; else print 0 }')
    if [[ "$is_gte_8k" -eq 1 ]]; then
      classification="PASS"
    else
      classification="SLOW_PASS"
    fi
  fi

  if [[ "$in_campaign" -eq 1 ]]; then
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$label" "$workers" "$parallel" "$batch_delay_us" "$rc" "${WALL_MS:-}" "${WALL_TPS:-}" "$CORRECTNESS" \
      "$KAFKA_PENDING_MAX" "$RESULT_FLUSH_COUNT" "$KAFKA_DELIVERY_MS_AVG" "$GW_P95" \
      "$GW_MUTEX_WAIT_P95" "$GW_MUTEX_HOLD_P95" "$GW_KAFKA_MSGS" "$after" "$classification" \
      >> "$OUT/summary.csv"
  else
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$label" "$workers" "$parallel" "$batch_delay_us" "$rc" "${WALL_MS:-}" "${WALL_TPS:-}" "$CORRECTNESS" \
      "$KAFKA_PENDING_MAX" "$RESULT_FLUSH_COUNT" "$KAFKA_DELIVERY_MS_AVG" "$GW_P95" \
      "$GW_MUTEX_WAIT_P95" "$GW_MUTEX_HOLD_P95" "$GW_KAFKA_MSGS" "$after" "$classification" \
      >> "$OUT/selection.csv"
  fi
  log "END $label rc=$rc correctness=$CORRECTNESS wall_ms=${WALL_MS:-NA} wall_tps=${WALL_TPS:-NA} classification=$classification"
}

log "Output directory: $OUT"
log "Starting selection stage to find the best batch_delay_us (1000, 2000, 4000) for workers=24 parallel=64"
printf 'label,workers,det_block_parallel,batch_delay_us,exit_code,wall_ms,wall_tps,correctness,node1_kafka_delivery_pending_max,node1_result_flush_count,node1_kafka_delivery_ms_avg,gateway_consume_to_ready_ms_p95,gateway_vote_store_mutex_wait_us_p95,gateway_vote_store_mutex_hold_us_p95,gateway_kafka_messages,runner_result_dir,classification\n' > "$OUT/selection.csv"

# Run selection candidates in interleaved order
# Rep 1: 1000 -> 2000 -> 4000
run_one 1 24 64 1000 0
tps_1000_1="${WALL_TPS:-0}"
correct_1000_1="$CORRECTNESS"
rc_1000_1="$rc"
pending_1000_1="${KAFKA_PENDING_MAX:-0}"
p95_1000_1="${GW_P95:-0}"

run_one 1 24 64 2000 0
tps_2000_1="${WALL_TPS:-0}"
correct_2000_1="$CORRECTNESS"
rc_2000_1="$rc"
pending_2000_1="${KAFKA_PENDING_MAX:-0}"
p95_2000_1="${GW_P95:-0}"

run_one 1 24 64 4000 0
tps_4000_1="${WALL_TPS:-0}"
correct_4000_1="$CORRECTNESS"
rc_4000_1="$rc"
pending_4000_1="${KAFKA_PENDING_MAX:-0}"
p95_4000_1="${GW_P95:-0}"

# Rep 2: 4000 -> 2000 -> 1000
run_one 2 24 64 4000 0
tps_4000_2="${WALL_TPS:-0}"
correct_4000_2="$CORRECTNESS"
rc_4000_2="$rc"
pending_4000_2="${KAFKA_PENDING_MAX:-0}"
p95_4000_2="${GW_P95:-0}"

run_one 2 24 64 2000 0
tps_2000_2="${WALL_TPS:-0}"
correct_2000_2="$CORRECTNESS"
rc_2000_2="$rc"
pending_2000_2="${KAFKA_PENDING_MAX:-0}"
p95_2000_2="${GW_P95:-0}"

run_one 2 24 64 1000 0
tps_1000_2="${WALL_TPS:-0}"
correct_1000_2="$CORRECTNESS"
rc_1000_2="$rc"
pending_1000_2="${KAFKA_PENDING_MAX:-0}"
p95_1000_2="${GW_P95:-0}"

## Rep 3: 2000 -> 1000 -> 4000
run_one 3 24 64 2000 0
tps_2000_3="${WALL_TPS:-0}"
correct_2000_3="$CORRECTNESS"
rc_2000_3="$rc"
pending_2000_3="${KAFKA_PENDING_MAX:-0}"
p95_2000_3="${GW_P95:-0}"

run_one 3 24 64 1000 0
tps_1000_3="${WALL_TPS:-0}"
correct_1000_3="$CORRECTNESS"
rc_1000_3="$rc"
pending_1000_3="${KAFKA_PENDING_MAX:-0}"
p95_1000_3="${GW_P95:-0}"

run_one 3 24 64 4000 0
tps_4000_3="${WALL_TPS:-0}"
correct_4000_3="$CORRECTNESS"
rc_4000_3="$rc"
pending_4000_3="${KAFKA_PENDING_MAX:-0}"
p95_4000_3="${GW_P95:-0}"

log "Selection results and metrics (median of PASS runs):"
selected_delay=$(awk -v t1000_1="$tps_1000_1" -v c1000_1="$correct_1000_1" -v r1000_1="$rc_1000_1" -v p1000_1="$pending_1000_1" -v l1000_1="$p95_1000_1" \
                     -v t1000_2="$tps_1000_2" -v c1000_2="$correct_1000_2" -v r1000_2="$rc_1000_2" -v p1000_2="$pending_1000_2" -v l1000_2="$p95_1000_2" \
                     -v t1000_3="$tps_1000_3" -v c1000_3="$correct_1000_3" -v r1000_3="$rc_1000_3" -v p1000_3="$pending_1000_3" -v l1000_3="$p95_1000_3" \
                     -v t2000_1="$tps_2000_1" -v c2000_1="$correct_2000_1" -v r2000_1="$rc_2000_1" -v p2000_1="$pending_2000_1" -v l2000_1="$p95_2000_1" \
                     -v t2000_2="$tps_2000_2" -v c2000_2="$correct_2000_2" -v r2000_2="$rc_2000_2" -v p2000_2="$pending_2000_2" -v l2000_2="$p95_2000_2" \
                     -v t2000_3="$tps_2000_3" -v c2000_3="$correct_2000_3" -v r2000_3="$rc_2000_3" -v p2000_3="$pending_2000_3" -v l2000_3="$p95_2000_3" \
                     -v t4000_1="$tps_4000_1" -v c4000_1="$correct_4000_1" -v r4000_1="$rc_4000_1" -v p4000_1="$pending_4000_1" -v l4000_1="$p95_4000_1" \
                     -v t4000_2="$tps_4000_2" -v c4000_2="$correct_4000_2" -v r4000_2="$rc_4000_2" -v p4000_2="$pending_4000_2" -v l4000_2="$p95_4000_2" \
                     -v t4000_3="$tps_4000_3" -v c4000_3="$correct_4000_3" -v r4000_3="$rc_4000_3" -v p4000_3="$pending_4000_3" -v l4000_3="$p95_4000_3" '
BEGIN {
  process_delay(1000, t1000_1, c1000_1, r1000_1, p1000_1, l1000_1, t1000_2, c1000_2, r1000_2, p1000_2, l1000_2, t1000_3, c1000_3, r1000_3, p1000_3, l1000_3);
  process_delay(2000, t2000_1, c2000_1, r2000_1, p2000_1, l2000_1, t2000_2, c2000_2, r2000_2, p2000_2, l2000_2, t2000_3, c2000_3, r2000_3, p2000_3, l2000_3);
  process_delay(4000, t4000_1, c4000_1, r4000_1, p4000_1, l4000_1, t4000_2, c4000_2, r4000_2, p4000_2, l4000_2, t4000_3, c4000_3, r4000_3, p4000_3, l4000_3);
  
  # Print candidates summary
  for (d in delays) {
    if (correct_cnt[d] == 3) {
      printf "  %d us: valid_runs=3 median_TPS=%.2f median_pending=%.1f median_p95=%.1f\n", \
        d, med_tps[d], med_pending[d], med_p95[d] > "/dev/stderr";
    } else {
      printf "  %d us: valid_runs=%d (invalid/failed)\n", d, correct_cnt[d] > "/dev/stderr";
    }
  }

  best_delay = "";
  best_tps = 0;
  
  # Try to select the best safe candidate first (pending <= 32 and p95 <= 50) among valid (correct_cnt == 3)
  for (d in delays) {
    if (correct_cnt[d] == 3) {
      is_safe = (med_pending[d] <= 32 && med_p95[d] <= 50.0);
      if (is_safe) {
        if (med_tps[d] > best_tps) {
          best_tps = med_tps[d];
          best_delay = d;
        }
      }
    }
  }
  
  # Fallback to the best valid correct candidate if none are "safe"
  if (best_delay == "") {
    for (d in delays) {
      if (correct_cnt[d] == 3) {
        if (med_tps[d] > best_tps) {
          best_tps = med_tps[d];
          best_delay = d;
        }
      }
    }
  }
  
  
  if (best_delay != "") {
    print best_delay;
  } else {
    print "ERROR";
  }
}

function process_delay(d, t1, c1, r1, p1, l1, t2, c2, r2, p2, l2, t3, c3, r3, p3, l3) {
  delays[d] = 1;
  cnt = 0;
  
  t1 += 0; t2 += 0; t3 += 0;
  p1 = (p1 == "NA" || p1 == "") ? 999999 : p1 + 0;
  p2 = (p2 == "NA" || p2 == "") ? 999999 : p2 + 0;
  p3 = (p3 == "NA" || p3 == "") ? 999999 : p3 + 0;
  l1 = (l1 == "NA" || l1 == "") ? 999999 : l1 + 0;
  l2 = (l2 == "NA" || l2 == "") ? 999999 : l2 + 0;
  l3 = (l3 == "NA" || l3 == "") ? 999999 : l3 + 0;
  
  if (c1 == "PASS" && r1 == 0) {
    tps_arr[cnt] = t1;
    pending_arr[cnt] = p1;
    p95_arr[cnt] = l1;
    cnt++;
  }
  if (c2 == "PASS" && r2 == 0) {
    tps_arr[cnt] = t2;
    pending_arr[cnt] = p2;
    p95_arr[cnt] = l2;
    cnt++;
  }
  if (c3 == "PASS" && r3 == 0) {
    tps_arr[cnt] = t3;
    pending_arr[cnt] = p3;
    p95_arr[cnt] = l3;
    cnt++;
  }
  
  correct_cnt[d] = cnt;
  
  # Sort arrays to compute medians
  for (i = 0; i < cnt; i++) {
    for (j = i + 1; j < cnt; j++) {
      if (tps_arr[i] > tps_arr[j]) {
        tmp = tps_arr[i]; tps_arr[i] = tps_arr[j]; tps_arr[j] = tmp;
      }
      if (pending_arr[i] > pending_arr[j]) {
        tmp = pending_arr[i]; pending_arr[i] = pending_arr[j]; pending_arr[j] = tmp;
      }
      if (p95_arr[i] > p95_arr[j]) {
        tmp = p95_arr[i]; p95_arr[i] = p95_arr[j]; p95_arr[j] = tmp;
      }
    }
  }
  
  if (cnt == 3) {
    med_tps[d] = tps_arr[1];
    med_pending[d] = pending_arr[1];
    med_p95[d] = p95_arr[1];
  } else if (cnt == 2) {
    med_tps[d] = (tps_arr[0] + tps_arr[1]) / 2.0;
    med_pending[d] = (pending_arr[0] + pending_arr[1]) / 2.0;
    med_p95[d] = (p95_arr[0] + p95_arr[1]) / 2.0;
  } else if (cnt == 1) {
    med_tps[d] = tps_arr[0];
    med_pending[d] = pending_arr[0];
    med_p95[d] = p95_arr[0];
  } else {
    med_tps[d] = 0;
    med_pending[d] = 999999;
    med_p95[d] = 999999;
  }
}
\')

if [[ "$selected_delay" == "ERROR" ]]; then
  log "ERROR: all batch-delay candidates failed correctness; refusing main campaign."
  exit 1
fi

log "Selected batch_delay_us: $selected_delay us"
log "Starting the $REPS repetition main stability campaign with selected batch_delay_us=$selected_delay"

run_no=0
for rep in $(seq 1 "$REPS"); do
  run_no=$((run_no + 1)); run_one "$run_no" 16 64 "$selected_delay" 1
  run_no=$((run_no + 1)); run_one "$run_no" 24 64 "$selected_delay" 1
  run_no=$((run_no + 1)); run_one "$run_no" 64 48 "$selected_delay" 1
done

{
  echo "config,total_runs,valid_runs,fail_runs,mean_tps,median_tps,min_tps,stddev_tps,cov_pct,acceptance_gate"
  awk -F, '
  NR==1 {next}
  {
    key=$2 "/p" $3;
    n[key]++;
    tps=$7+0;
    class=$17;
    
    if (class == "PASS" || class == "SLOW_PASS") {
      valid_cnt[key]++;
      sum[key] += tps;
      vals[key, valid_cnt[key]] = tps;
    } else {
      fail_cnt[key]++;
    }
  }
  END {
    for (k in n) {
      total = n[k];
      v = valid_cnt[k] + 0;
      f = fail_cnt[k] + 0;
      
      mean = 0;
      median_tps = 0;
      min_tps = 0;
      stddev = 0;
      cov = 0;
      gate = "REJECTED";
      
      if (v > 0) {
        # Sort vals for min and median calculation
        for (i = 1; i <= v; i++) {
          for (j = i + 1; j <= v; j++) {
            if (vals[k, i] > vals[k, j]) {
              tmp = vals[k, i];
              vals[k, i] = vals[k, j];
              vals[k, j] = tmp;
            }
          }
        }
        
        min_tps = vals[k, 1];
        if (v % 2 == 1) {
          median_tps = vals[k, int(v / 2) + 1];
        } else {
          median_tps = (vals[k, v / 2] + vals[k, v / 2 + 1]) / 2.0;
        }
        
        mean = sum[k] / v;
        if (v > 1) {
          sq_sum = 0;
          for (i = 1; i <= v; i++) {
            sq_sum += (vals[k, i] - mean) ^ 2;
          }
          stddev = sqrt(sq_sum / (v - 1));
          cov = stddev / mean;
        }
      }
      
      cov_pct = cov * 100;
      
      # Enforce acceptance gate criteria
      if (v == 5 && f == 0 && cov_pct <= 8.0 && mean >= 7800) {
        if (median_tps >= 8000 && min_tps >= 7300) {
          gate = "8K_CLASS";
        } else if (median_tps >= 7800 && min_tps >= 7000) {
          gate = "7.8K_CLASS";
        }
      }
      
      printf "%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f%%,%s\n", k, total, v, f, mean, median_tps, min_tps, stddev, cov_pct, gate;
    }
  }' "$OUT/summary.csv" | sort
} > "$OUT/aggregate.csv"

log "Completed. Read: $OUT/summary.csv and $OUT/aggregate.csv"
printf '\n%s\n' "$OUT"
