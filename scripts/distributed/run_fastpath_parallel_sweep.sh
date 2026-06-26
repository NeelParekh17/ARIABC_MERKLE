#!/usr/bin/env bash
set -u -o pipefail

ARIABC_CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
export ARIABC_CLUSTER_PASSWORD

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$SCRIPT_DIR/../bench_full_results"
STAMP="$(date +%Y%m%d_%H%M%S)"
SWEEP_DIR="$BENCH_ROOT/fastpath_parallel_sweep_$STAMP"
mkdir -p "$SWEEP_DIR"

TOTAL=20513
RING_SAFE_WINDOW=2048
THREADS=8
PS=(1 2 4 8 16 32 64)

unset ARIABC_TEST_FAIL_DET_BLOCK_SEND_ONCE
unset ARIABC_TEST_FAIL_DET_BLOCK_SEND_NODE

printf 'p\twindow\tlane_depth\trc\tstatus\ttps\tcompleted\tdivergence\tpermanent_failures\tall3_failures\tmerkle\twatchdog\trun_dir\n' \
  > "$SWEEP_DIR/summary.tsv"

for p in "${PS[@]}"; do
  requested_window=$((p * 256))
  window=$requested_window
  if (( window > RING_SAFE_WINDOW )); then
    window=$RING_SAFE_WINDOW
  fi
  lane_depth=$((window / THREADS))

  echo
  echo "================================================================"
  echo "FASTPATH SWEEP: p=$p window=$window lane_depth=$lane_depth"
  if (( requested_window > RING_SAFE_WINDOW )); then
    echo "NOTE: requested p=$p exceeds 2048-slot safe window; capped at 2048."
  fi
  echo "================================================================"

  marker="$SWEEP_DIR/.before_p${p}"
  touch "$marker"
  console="$SWEEP_DIR/p${p}.console.log"

  timeout -k 45s 900s env \
    SKIP_SYNC=1 \
    SKIP_BUILD=1 \
    SKIP_RDKAFKA_SETUP=1 \
    ENABLE_FASTPATH_WATCHDOG=1 \
    KAFKA_COMPLETION_MODE=majority-async-all3 \
    ARIABC_FULL_RESULT_REPLICA_LIMIT=2 \
    ARIABC_RESULT_PUBLISH_REPLICA_LIMIT=0 \
    ARIABC_OS_PROFILE=0 \
    ./run_4node_raft_cluster.sh \
      --threads "$THREADS" \
      --preferred-leader-id 1 \
      --det-window "$window" \
      --det-batch-size 256 \
      --det-pipeline-depth "$lane_depth" \
      --pool-size 256 \
      --bcdb-worker-count 64 \
      --bcdb-decouple-workers 1 \
      --det-block-parallel "$p" \
      --det-block-pipeline 1 \
      --det-block-max 256 \
      --det-event-block-fastpath 1 \
      --det-prefixed-direct-parallel 0 \
      --det-completion-only-success 0 \
      --bcdb-block-wait-watermark 1 \
      --bcdb-dt-parse-barrier 0 \
      --bcdb-block-profile 0 \
      --bcdb-phase-trace 0 \
      --bcdb-serial-gate-mode 1 \
      --bcdb-serial-gate-source 0 \
      2>&1 | tee "$console"

  rc=${PIPESTATUS[0]}

  run_dir="$(
    find "$BENCH_ROOT" -maxdepth 1 -mindepth 1 -type d \
      -name 'cluster4_*' -newer "$marker" -printf '%T@ %p\n' 2>/dev/null |
      sort -n | tail -1 | cut -d' ' -f2-
  )"
  rm -f "$marker"

  tps="$(grep -E 'TPS \(gateway\)[[:space:]]*:[[:space:]]*~[0-9]+' "$console" | tail -1 | sed -E 's/.*~([0-9]+).*/\1/' || true)"
  completed="$(grep -E "PROGRESS_GATEWAY_DET .*total=${TOTAL} .*completed=${TOTAL} .*final=1" "$console" | tail -1 | sed -E 's/.*completed=([0-9]+).*/\1/' || true)"
  divergence="$(grep -E '^divergence_count=' "$console" | head -1 | cut -d= -f2 || true)"
  failures="$(grep -E '^permanent_failures=' "$console" | head -1 | cut -d= -f2 || true)"
  all3_failures="$(grep -E "client_quorum_complete_count=${TOTAL} .*async_all3_failure_count=" "$console" | tail -1 | sed -E 's/.*async_all3_failure_count=([0-9]+).*/\1/' || true)"

  merkle="NO"
  grep -q 'usertable_small consistency: PASS' "$console" && merkle="YES"

  watchdog="NO"
  [[ -n "$run_dir" && -f "$run_dir/WATCHDOG_TRIGGERED" ]] && watchdog="YES"

  status="FAIL"
  if [[ "$rc" == "0" &&
        "$completed" == "$TOTAL" &&
        "$divergence" == "0" &&
        "$failures" == "0" &&
        "$all3_failures" == "0" &&
        "$merkle" == "YES" &&
        "$watchdog" == "NO" ]]; then
    status="PASS"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$p" "$window" "$lane_depth" "$rc" "$status" \
    "${tps:-?}" "${completed:-?}" "${divergence:-?}" \
    "${failures:-?}" "${all3_failures:-?}" \
    "$merkle" "$watchdog" "${run_dir:-missing}" \
    | tee -a "$SWEEP_DIR/summary.tsv"

  echo "Finished p=$p: $status"
done

echo
echo "================ FASTPATH SWEEP SUMMARY ================"
column -t -s $'\t' "$SWEEP_DIR/summary.tsv" || cat "$SWEEP_DIR/summary.tsv"
echo
echo "Artifacts: $SWEEP_DIR"
