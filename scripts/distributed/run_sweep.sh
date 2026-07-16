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
                           Default: "1 2 4 8 12 16"
  --reps LIST              Repetition labels to run for each executor worker.
                           Accepts comma-separated or quoted space-separated values.
                           Default: "1 2 3"

Dynamic Merkle options forwarded to run_4node_raft_cluster.sh:
  --restore-sql FILE       SQL used to restore table state before each run.
                           Default: scripts/distributed/sql/
                           restore_usertable_small_dynamic.sql.
  --verify-table TABLE     Table used for post-run root comparison (default: usertable_small).
  --enable-merkle-index N  Set Merkle index maintenance: 0|1 (default: 1).
  --merkle-verify-mode M   Post-run equality check mode: legacy|dynamic|auto
                           (default: dynamic).
                           In "dynamic" mode the runner computes SHA-256 digests of
                           partition roots, physical topology, and leaf-item assignments
                           and requires them to match across all replicas.
  --dynamic-index NAME     Fully-qualified index name for dynamic verification
                           (default: public.usertable_small_dynamic_merkle_idx).

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

Example (dynamic Merkle sweep):
  ./scripts/distributed/run_sweep.sh \
    --threads 96 \
    --executor-workers 1,2,4,8,12,16 \
    --reps 1,2,3 \
    --restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql \
    --verify-table usertable_small \
    --enable-merkle-index 1 \
    --merkle-verify-mode dynamic \
    --dynamic-index public.usertable_small_dynamic_merkle_idx

Example (legacy sweep):
  ./scripts/distributed/run_sweep.sh \
    --threads 96 \
    --executor-workers 4,8,16 \
    --reps 1,2 \
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
EXECUTOR_WORKERS="1 2 4 8 12 16"
REPS="1 2 3"
CLUSTER_ARGS=(
  --restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql
  --verify-table usertable_small
  --enable-merkle-index 1
  --merkle-verify-mode dynamic
  --dynamic-index public.usertable_small_dynamic_merkle_idx
)

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
    --reps)
      REPS="$(normalize_list "${2:?missing value for --reps}")"
      shift 2
      ;;
    # --- dynamic Merkle options ---
    --restore-sql|--verify-table|--enable-merkle-index|\
    --merkle-verify-mode|--dynamic-index)
      CLUSTER_ARGS+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    # --- cluster topology options ---
    --node-ids|--node-ips|--node-names|--node-users|--node-is-u22|--node-client-ports|\
    --raft-port|--db-port|--db-user|--db-name|--kafka-host|--kafka-port|--kafka-home-remote)
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
git diff --check

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="scripts/bench_full_results/pg_executor_sweep_${STAMP}"
mkdir -p "$OUT"
: >"$OUT/run_dirs.txt"
: >"$OUT/summary.csv"

printf 'pg_executor_workers,rep,artifact,status\n' >"$OUT/runs.csv"
{
  printf 'threads=%s\n' "$THREADS"
  printf 'det_client_workers=%s\n' "$DET_CLIENT_WORKERS"
  printf 'executor_workers=%s\n' "$EXECUTOR_WORKERS"
  printf 'reps=%s\n' "$REPS"
  printf 'cluster_args='
  printf '%q ' "${CLUSTER_ARGS[@]}"
  printf '\n'
} >"$OUT/campaign.env"

{
  echo "Campaign: threads=$THREADS det_client_workers=$DET_CLIENT_WORKERS executor_workers=[$EXECUTOR_WORKERS] reps=[$REPS]"
  if [[ "${#CLUSTER_ARGS[@]}" -gt 0 ]]; then
    printf 'Cluster args: '
    printf '%q ' "${CLUSTER_ARGS[@]}"
    printf '\n'
  fi

  for E in $EXECUTOR_WORKERS; do
    for REP in $REPS; do
      echo
      echo "=============================================================="
      echo "PG/EXECUTOR WORKERS=$E | REP=$REP"
      echo "=============================================================="

      BEFORE="$(mktemp)"
      find scripts/bench_full_results \
        -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
        -printf '%f\n' | sort >"$BEFORE"

      RUN_PASS=1
      env \
        SKIP_RDKAFKA_SETUP=1 \
        ARIABC_PREFERRED_LEADER_ID=1 \
        ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1 \
        ARIABC_RAFT_STREAM_GAP=512 \
        ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1 \
        BCDB_DET_QUEUE_HIGH_WM=128 \
        BCDB_DET_QUEUE_LOW_WM=64 \
        ./scripts/distributed/run_4node_raft_cluster.sh \
          "${CLUSTER_ARGS[@]}" \
          --ordering-mode raft-kafka \
          --execution-profile threaded-raft-direct \
          --threads "$THREADS" \
          --det-client-workers "$DET_CLIENT_WORKERS" \
          --det-client-inflight 1 \
          --server-exec-workers "$E" \
          --server-pg-connections "$E" \
          --bcdb-workers 8 \
          --bcdb-init-block-size 8 \
          --bcdb-decouple-workers 1 \
          --raft-ordered-fanout 1 \
          --raft-ordering-policy leader-assigned \
          --raft-ordered-batch-append 1 \
          --raft-ordered-batch-target-entries 32 \
          --raft-ordered-batch-linger-us 1000 \
          --raft-ordered-coalesce-log 0 \
          --kafka-completion-mode async \
          --det-window 65536 || RUN_PASS=0

      RUN_NAME="$(
        comm -13 "$BEFORE" \
          <(find scripts/bench_full_results \
            -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
            -printf '%f\n' | sort) | tail -n1
      )"
      rm -f "$BEFORE"

      RUN="scripts/bench_full_results/$RUN_NAME"
      [[ -d "$RUN" ]] || { echo "Could not locate fresh benchmark artifact"; exit 1; }

      # Dynamic equality gate: TPS results without matching digest are invalid.
      if [[ "$RUN_PASS" -eq 1 ]]; then
        if grep -q 'DYNAMIC_MERKLE_THREE_REPLICA_EQUALITY_PASS=1' \
            "$RUN/run_summary.env" 2>/dev/null; then
          echo "DYNAMIC_MERKLE equality gate: PASS (E=$E rep=$REP)"
        elif grep -qE 'merkle-verify-mode.+dynamic|MERKLE_VERIFY_MODE.+dynamic' \
            "$OUT/campaign.env" 2>/dev/null; then
          echo "WARNING: dynamic equality gate was expected but not found — marking run INVALID"
          RUN_PASS=0
        fi
      fi

      if [[ "$RUN_PASS" -eq 0 ]]; then
        echo "INVALID run (E=$E rep=$REP): equality gate or runner failed — excluded from summary"
        printf '%s,%s,%s,INVALID\n' "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
        continue
      fi

      printf '%s,%s,%s,PASS\n' "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
      printf '%s\n' "$RUN" >>"$OUT/run_dirs.txt"

      python3 scripts/distributed/summarize_raft_profile.py "$RUN" | tee -a "$OUT/summary.csv"
    done
  done

  echo
  echo "Generating TPS graph..."
  python3 scripts/distributed/plot_executor_sweep.py \
    --input-csv "$OUT/summary.csv" \
    --output-img "$OUT/executor_sweep_tps.png" \
    --title "Executor Worker Sweep: TPS vs Executor Workers (Threads=$THREADS)"

  echo
  echo "Done."
  echo "Artifacts: $OUT"

} >out.txt 2>&1
