#!/bin/bash
cd /work/ARIABC/AriaBC
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_sweep.sh [options]

Sweep options:
  --threads N              Client deterministic lanes and det client workers
                           unless --det-client-workers is also set.
  --det-client-workers N   Gateway deterministic threadpool workers.
  --executor-workers LIST  Server executor worker counts to sweep.
                           Accepts comma-separated or quoted space-separated values.
                           Default: "1 2 4 8"
  --workloads LIST         Workload SQL files to sweep.
                           Accepts comma-separated or quoted space-separated values.
                           Default: "scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
  --reps LIST              Repetition labels to run for each executor worker.
                           Accepts comma-separated or quoted space-separated values.
                           Default: "1 2 3"
  --kafka-completion-mode MODE Completion mode: majority, majority_async_all3, or async.
                           Default: "majority_async_all3"
  --raft-ordering-policy POLICY
                           Raft ordering policy: leader-assigned or preassigned.
                           Default: "leader-assigned"

Cluster topology options forwarded to run_4node_raft_cluster.sh:
  --node-ids CSV
  --node-ips CSV
  --node-names CSV
  --node-users CSV
  --node-is-u22 CSV
  --node-client-ports CSV
  --raft-port N
  --db-port N
  --db-user USER
  --db-name NAME
  --kafka-host HOST
  --kafka-port N
  --kafka-home-remote DIR

Other:
  -h, --help

Example:
  ./scripts/distributed/run_sweep.sh \
    --threads 96 \
    --executor-workers 1,2,4,8 \
    --reps 1,2 \
    --raft-ordering-policy preassigned \
    --node-ids 1,2,3 \
    --node-ips 10.10.0.11,10.10.0.12,10.10.0.13 \
    --node-names node-a,node-b,node-c \
    --node-users neel,neel,neel \
    --node-is-u22 0,0,1 \
    --node-client-ports 8000,8000,8001 \
    --kafka-host 10.10.0.11
EOF
}

normalize_list() {
  printf '%s\n' "${1//,/ }"
}

THREADS=96
DET_CLIENT_WORKERS=""
EXECUTOR_WORKERS="1 2 4 8"
DEFAULT_WORKLOADS="scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
WORKLOADS="$DEFAULT_WORKLOADS"
REPS="1 2 3"
KAFKA_COMPLETION_MODE="${KAFKA_COMPLETION_MODE:-majority_async_all3}"
EXECUTION_PROFILE="${EXECUTION_PROFILE:-event-direct}"
RAFT_ORDERING_POLICY="${RAFT_ORDERING_POLICY:-leader-assigned}"
CLUSTER_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)
      THREADS="${2:?missing value for --threads}"
      shift 2
      ;;
    --det-client-workers)
      DET_CLIENT_WORKERS="${2:?missing value for --det-client-workers}"
      shift 2
      ;;
    --executor-workers)
      EXECUTOR_WORKERS="$(normalize_list "${2:?missing value for --executor-workers}")"
      shift 2
      ;;
    --workloads)
      WORKLOADS="$(normalize_list "${2:?missing value for --workloads}")"
      shift 2
      ;;
    --reps)
      REPS="$(normalize_list "${2:?missing value for --reps}")"
      shift 2
      ;;
    --kafka-completion-mode)
      KAFKA_COMPLETION_MODE="${2:?missing value for --kafka-completion-mode}"
      shift 2
      ;;
    --raft-ordering-policy)
      RAFT_ORDERING_POLICY="${2:?missing value for --raft-ordering-policy}"
      shift 2
      ;;
    --execution-profile)
      EXECUTION_PROFILE="${2:?missing value for --execution-profile}"
      CLUSTER_ARGS+=("$1" "$2")
      shift 2
      ;;
    --node-ids|--node-ips|--node-names|--node-users|--node-is-u22|--node-client-ports|\
    --raft-port|--db-port|--db-user|--db-name|--kafka-host|--kafka-port|--kafka-home-remote|\
    --det-client-inflight|--raft-ordered-batch-linger-us|--enable-merkle-index|--det-client-mode|\
    --raft-ordered-batch-append|--raft-ordered-batch-target-entries|--raft-ordered-coalesce-log|\
    --raft-ordered-fanout|--conn-fanout|--det-window|--bcdb-decouple-workers)
      CLUSTER_ARGS+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$DET_CLIENT_WORKERS" ]]; then
  DET_CLIENT_WORKERS="$THREADS"
fi

echo "=== Building targets ==="
cmake --build ariabc_pg/build \
  --target ariabc_pg_gateway ariabc_pg_server \
  -j"$(nproc)"

echo "=== Checking script syntax ==="
bash -n scripts/distributed/run_4node_raft_cluster.sh

echo "=== Checking git style rules ==="
# If this fails, the script will stop cleanly instead of killing your terminal
git diff --check

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="scripts/bench_full_results/pg_executor_sweep_${STAMP}"
mkdir -p "$OUT"
: > "$OUT/run_dirs.txt"
: > "$OUT/summary.csv"

printf 'workload,pg_executor_workers,rep,artifact\n' > "$OUT/runs.csv"
{
  printf 'threads=%s\n' "$THREADS"
  printf 'det_client_workers=%s\n' "$DET_CLIENT_WORKERS"
  printf 'executor_workers=%s\n' "$EXECUTOR_WORKERS"
  printf 'workloads=%s\n' "$WORKLOADS"
  printf 'reps=%s\n' "$REPS"
  printf 'kafka_completion_mode=%s\n' "$KAFKA_COMPLETION_MODE"
  printf 'raft_ordering_policy=%s\n' "$RAFT_ORDERING_POLICY"
  printf 'cluster_args='
  printf '%q ' "${CLUSTER_ARGS[@]}"
  printf '\n'
} > "$OUT/campaign.env"

# Wrapped execution sequence to funnel into out.txt
{
  echo "Campaign: threads=$THREADS det_client_workers=$DET_CLIENT_WORKERS executor_workers=[$EXECUTOR_WORKERS] workloads=[$WORKLOADS] reps=[$REPS] kafka_completion_mode=$KAFKA_COMPLETION_MODE raft_ordering_policy=$RAFT_ORDERING_POLICY"
  if [[ "${#CLUSTER_ARGS[@]}" -gt 0 ]]; then
    printf 'Cluster args: '
    printf '%q ' "${CLUSTER_ARGS[@]}"
    printf '\n'
  fi

  for WL_RAW in $WORKLOADS; do
    if [[ "$WL_RAW" = /* ]]; then
      WL="$WL_RAW"
    else
      WL="/work/ARIABC/AriaBC/$WL_RAW"
    fi
    wl_base="$(basename "$WL")"
    wl_tag="${wl_base%.*}"
    : > "$OUT/summary_${wl_tag}.csv"

    echo
    echo "=============================================================="
    echo "=== WORKLOAD: $wl_base"
    echo "=============================================================="

    for E in $EXECUTOR_WORKERS; do
      for REP in $REPS; do
        echo
        echo "--------------------------------------------------------------"
        echo "WORKLOAD=$wl_base | PG/EXECUTOR WORKERS=$E | REP=$REP"
        echo "--------------------------------------------------------------"

        BEFORE="$(mktemp)"
        find scripts/bench_full_results \
          -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
          -printf '%f\n' | sort > "$BEFORE"

        set +e
        env \
          FORCE_BUILD="${FORCE_BUILD:-0}" \
          SKIP_RDKAFKA_SETUP=1 \
          ARIABC_PREFERRED_LEADER_ID=1 \
          ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1 \
          ARIABC_RAFT_STREAM_GAP=512 \
          ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1 \
          BCDB_DET_QUEUE_HIGH_WM=65536 \
          BCDB_DET_QUEUE_LOW_WM=32768 \
          ./scripts/distributed/run_4node_raft_cluster.sh \
            "${CLUSTER_ARGS[@]}" \
            --workload "$WL" \
            --ordering-mode raft-kafka \
            --enable-merkle-index 1 \
            --raft-apply-ledger-mode off \
            --threads "$THREADS" \
            --det-client-workers "${DET_CLIENT_WORKERS:-$THREADS}" \
            --det-client-inflight "${DET_CLIENT_INFLIGHT:-16}" \
            --server-exec-workers "$E" \
            --server-pg-connections "${SERVER_PG_CONNECTIONS:-$E}" \
            --pool-size "${SERVER_PG_CONNECTIONS:-$E}" \
            --bcdb-workers "${BCDB_WORKERS:-$E}" \
            --bcdb-init-block-size "$E" \
            --bcdb-decouple-workers 1 \
            --conn-fanout "${CONN_FANOUT:-1}" \
            --raft-ordered-fanout "${RAFT_ORDERED_FANOUT:-1}" \
            --raft-ordering-policy "${RAFT_ORDERING_POLICY:-leader-assigned}" \
            --raft-ordered-batch-append 1 \
            --raft-ordered-batch-target-entries "${RAFT_ORDERED_BATCH_TARGET_ENTRIES:-64}" \
            --raft-ordered-batch-linger-us "${RAFT_ORDERED_LINGER_US:-1000}" \
            --raft-ordered-coalesce-log "${RAFT_ORDERED_COALESCE_LOG:-1}" \
            --kafka-completion-mode "$KAFKA_COMPLETION_MODE" \
            --det-window "${DET_WINDOW:-65536}"
        RUN_RC=$?
        set -e

        RUN_NAME="$(
          comm -13 "$BEFORE" \
            <(find scripts/bench_full_results \
              -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
              -printf '%f\n' | sort) | tail -n1
        )"
        rm -f "$BEFORE"

        if [[ -n "$RUN_NAME" && -d "scripts/bench_full_results/$RUN_NAME" ]]; then
          RUN="scripts/bench_full_results/$RUN_NAME"
          printf '%s,%s,%s,%s\n' "$wl_base" "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
          printf '%s\n' "$RUN" >> "$OUT/run_dirs.txt"

          python3 scripts/distributed/summarize_raft_profile.py "$RUN" | tee -a "$OUT/summary.csv" "$OUT/summary_${wl_tag}.csv" || true
        else
          echo "WARNING: Could not locate fresh benchmark artifact for WORKLOAD=$wl_base E=$E REP=$REP (cluster run exited with code $RUN_RC)"
        fi
      done
    done

    echo
    echo "Generating TPS graph for $wl_base..."
    python3 scripts/distributed/plot_executor_sweep.py \
      --input-csv "$OUT/summary_${wl_tag}.csv" \
      --output-img "$OUT/executor_sweep_${wl_tag}_tps.png" \
      --title "Executor Sweep ($wl_base): TPS vs Workers (Threads=$THREADS)" || true
  done

  echo
  echo "Done."
  echo "Artifacts: $OUT"

} > out.txt 2>&1
