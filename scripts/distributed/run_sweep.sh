#!/bin/bash
cd /work/ARIABC/AriaBC
set -euo pipefail

ARIABC_CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
export ARIABC_CLUSTER_PASSWORD
# This runner targets the dedicated disposable AriaBC lab cluster.  Keep the
# destructive-reset opt-in internal so the normal invocation remains simply
# `./run_sweep.sh`; callers can still set it to 0 to refuse destructive runs.
ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET="${ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET:-1}"
export ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET

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
                           Default: "1"

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
  --dynamic-structure-gate N
                           Untimed merge/re-split/key-route gate (default: 0).
  --dynamic-structure-crash-gate N
                           Crash/restart one replica with pending transitions
                           during the untimed gate (default: 0).
  --dynamic-structure-profile N
                           Opt-in native split/merge counters and per-replica
                           equality check (default: 1; use 0 to disable profiling).

Workload phase options forwarded to run_4node_raft_cluster.sh:
  --warmup-queries N       Untimed state-preserving warm-up updates (default: 1000;
                           0 disables warm-up).
  --warmup-workload FILE   Explicit untimed warm-up SQL file.

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
    --dynamic-index public.usertable_small_dynamic_merkle_idx \
    --warmup-queries 1000

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
REPS="1"
CLUSTER_ARGS=(
  --restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql
  --verify-table usertable_small
  --enable-merkle-index 1
  --merkle-verify-mode dynamic
  --dynamic-index public.usertable_small_dynamic_merkle_idx
  --dynamic-structure-gate 0
  --dynamic-structure-crash-gate 0
  --dynamic-structure-profile 1
  --warmup-queries 1000
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
    --merkle-verify-mode|--dynamic-index|--dynamic-structure-gate|--dynamic-structure-crash-gate|--dynamic-structure-profile)
      CLUSTER_ARGS+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    # --- workload phase options ---
    --warmup-queries|--warmup-workload)
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

: "${ARIABC_CLUSTER_PASSWORD:?resolved ARIABC_CLUSTER_PASSWORD must not be empty}"
if [[ "${ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET:-0}" != "1" ]]; then
  echo "ERROR: export ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 only for a dedicated benchmark database" >&2
  exit 2
fi

cluster_arg_value() {
  local flag="$1"
  local default_value="$2"
  local value="$default_value"
  local idx
  for ((idx = 0; idx < ${#CLUSTER_ARGS[@]}; ++idx)); do
    if [[ "${CLUSTER_ARGS[$idx]}" == "$flag" ]]; then
      value="${CLUSTER_ARGS[$((idx + 1))]}"
    fi
  done
  printf '%s\n' "$value"
}

source_fingerprint() {
  {
    find src ariabc_pg \
      \( -name '*.c' -o -name '*.cpp' -o -name '*.cxx' -o -name '*.h' -o -name 'CMakeLists.txt' \) \
      -not -path '*/build/*' -not -path '*/.git/*' \
      -exec sha256sum {} \; 2>/dev/null | sort
    echo 'RESULT_RING_CAPACITY=2048'
  } | sha256sum | awk '{print $1}'
}

file_sha256() {
  [[ -f "$1" ]] && sha256sum "$1" | awk '{print $1}' || printf 'missing\n'
}

RESTORE_INPUT="$(cluster_arg_value --restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql)"
WORKLOAD_INPUT="$(cluster_arg_value --workload scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt)"
NODE_IDS_INPUT="$(cluster_arg_value --node-ids 1,2,4)"
read -r -a NODE_ID_VALUES <<< "${NODE_IDS_INPUT//,/ }"
NODE_COUNT="${#NODE_ID_VALUES[@]}"
if (( NODE_COUNT < 1 )); then
  echo "ERROR: topology must contain at least one node" >&2
  exit 2
fi
# Majority result completion must use the Raft majority for the configured topology.
# For the normal three-node cluster this is 2; custom topologies derive the
# corresponding floor(N/2)+1 quorum automatically.
RESULT_COMPLETION_QUORUM=$(( NODE_COUNT / 2 + 1 ))

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

campaign_snapshot() {
  local destination="$1"
  {
    printf 'git_head=%s\n' "$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    printf 'source_fingerprint=%s\n' "$(source_fingerprint)"
    printf 'gateway_sha256=%s\n' "$(file_sha256 ariabc_pg/build/bin/ariabc_pg_gateway)"
    printf 'server_sha256=%s\n' "$(file_sha256 ariabc_pg/build/bin/ariabc_pg_server)"
    printf 'postgres_sha256=%s\n' "$(file_sha256 /work/ARIABC/install/bin/postgres)"
    printf 'restore_sql=%s\n' "$RESTORE_INPUT"
    printf 'restore_sha256=%s\n' "$(file_sha256 "$RESTORE_INPUT")"
    printf 'restore_base_sha256=%s\n' "$(file_sha256 scripts/restore_usertable_small.sql)"
    printf 'dynamic_index_sql_sha256=%s\n' "$(file_sha256 scripts/distributed/sql/create_usertable_small_dynamic_index.sql)"
    printf 'workload=%s\n' "$WORKLOAD_INPUT"
    printf 'workload_sha256=%s\n' "$(file_sha256 "$WORKLOAD_INPUT")"
    printf 'cluster_runner_sha256=%s\n' "$(file_sha256 scripts/distributed/run_4node_raft_cluster.sh)"
    printf 'sweep_runner_sha256=%s\n' "$(file_sha256 scripts/distributed/run_sweep.sh)"
  } >"$destination"
}

campaign_snapshot "$OUT/campaign_provenance.env"

printf 'ordinal,rep,pg_executor_workers\n' >"$OUT/schedule.csv"
ordinal=0
rep_ordinal=0
read -r -a worker_values <<< "$EXECUTOR_WORKERS"
for REP in $REPS; do
  ((rep_ordinal += 1))
  if (( rep_ordinal % 2 == 1 )); then
    scheduled_workers=("${worker_values[@]}")
  else
    scheduled_workers=()
    for ((idx = ${#worker_values[@]} - 1; idx >= 0; --idx)); do
      scheduled_workers+=("${worker_values[$idx]}")
    done
  fi
  for E in "${scheduled_workers[@]}"; do
    ((ordinal += 1))
    printf '%s,%s,%s\n' "$ordinal" "$REP" "$E" >>"$OUT/schedule.csv"
  done
done

printf 'pg_executor_workers,rep,artifact,status\n' >"$OUT/runs.csv"
{
  printf 'threads=%s\n' "$THREADS"
  printf 'det_client_workers=%s\n' "$DET_CLIENT_WORKERS"
  printf 'executor_workers=%s\n' "$EXECUTOR_WORKERS"
  printf 'reps=%s\n' "$REPS"
  printf 'node_count=%s\n' "$NODE_COUNT"
  printf 'result_completion_quorum=%s\n' "$RESULT_COMPLETION_QUORUM"
  printf 'schedule=interleaved_alternating_by_rep\n'
  printf 'tps_semantics=raft_majority_result_completion_async_all3_validation\n'
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

  SUMMARY_HEADER_WRITTEN=0
  while IFS=, read -r ORDINAL REP E; do
      [[ "$ORDINAL" == "ordinal" ]] && continue
      echo
      echo "=============================================================="
      echo "PG/EXECUTOR WORKERS=$E | REP=$REP"
      echo "=============================================================="

      BEFORE="$(mktemp)"
      BEFORE_INPUTS="$(mktemp)"
      campaign_snapshot "$BEFORE_INPUTS"
      if ! cmp -s "$OUT/campaign_provenance.env" "$BEFORE_INPUTS"; then
        diff -u "$OUT/campaign_provenance.env" "$BEFORE_INPUTS" || true
        rm -f "$BEFORE_INPUTS"
        echo "ERROR: campaign source, binaries, or inputs changed before run $ORDINAL" >&2
        exit 1
      fi
      rm -f "$BEFORE_INPUTS"
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
          --kafka-completion-mode majority_async_all3 \
          --det-window 65536 \
          </dev/null || RUN_PASS=0

      RUN_NAME="$(
        comm -13 "$BEFORE" \
          <(find scripts/bench_full_results \
            -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
            -printf '%f\n' | sort) | tail -n1
      )"
      rm -f "$BEFORE"

      AFTER_INPUTS="$(mktemp)"
      campaign_snapshot "$AFTER_INPUTS"
      if ! cmp -s "$OUT/campaign_provenance.env" "$AFTER_INPUTS"; then
        diff -u "$OUT/campaign_provenance.env" "$AFTER_INPUTS" | tee "$OUT/input_drift_run_${ORDINAL}.diff" || true
        rm -f "$AFTER_INPUTS"
        echo "INVALID run: campaign source, binaries, or inputs changed during run $ORDINAL" >&2
        RUN_PASS=0
      fi
      rm -f "$AFTER_INPUTS"

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

      if [[ "$RUN_PASS" -eq 1 ]] && ! grep -q '^CAMPAIGN_INPUT_FREEZE_PASS=1$' \
          "$RUN/build_provenance.env" 2>/dev/null; then
        echo "WARNING: campaign input freeze proof missing — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && ! grep -q '^warmup_included_in_tps=0$' \
          "$RUN/run_summary.env" 2>/dev/null; then
        echo "WARNING: warm-up/TPS separation proof missing — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && ! grep -q '^post_run_equality_verification_ms=' \
          "$RUN/run_summary.env" 2>/dev/null; then
        echo "WARNING: post-run equality timing/proof missing — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && grep -q -- '--dynamic-structure-gate 1' \
          "$OUT/campaign.env" 2>/dev/null &&
          ! grep -q '^DYNAMIC_DISTRIBUTED_STRUCTURE_GATE_PASS=1$' \
          "$RUN/run_summary.env" 2>/dev/null; then
        echo "WARNING: distributed dynamic structure gate missing — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && grep -q -- '--dynamic-structure-crash-gate 1' \
          "$OUT/campaign.env" 2>/dev/null &&
          ! grep -q '^DYNAMIC_DISTRIBUTED_PENDING_CRASH_RESTART_PASS=1$' \
          "$RUN/run_summary.env" 2>/dev/null; then
        echo "WARNING: distributed pending-transition crash/restart gate missing — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && grep -q -- '--dynamic-structure-profile 1' \
          "$OUT/campaign.env" 2>/dev/null &&
          ! grep -q '^DYNAMIC_NATIVE_PROFILE_PASS=1$' \
          "$RUN/run_summary.env" 2>/dev/null; then
        echo "WARNING: native dynamic split/merge profile missing or failed — marking run INVALID"
        RUN_PASS=0
      fi
      if [[ "$RUN_PASS" -eq 1 ]] && ! grep -qE 'request_latency_count=[1-9][0-9]*' \
          "$RUN/gateway_test.log" 2>/dev/null; then
        echo "WARNING: client request latency samples missing — marking run INVALID"
        RUN_PASS=0
      fi

      if [[ "$RUN_PASS" -eq 0 ]]; then
        echo "INVALID run (E=$E rep=$REP): equality gate or runner failed — excluded from summary"
        printf '%s,%s,%s,INVALID\n' "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
        continue
      fi

      printf '%s,%s,%s,PASS\n' "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
      printf '%s\n' "$RUN" >>"$OUT/run_dirs.txt"

      SUMMARY_ARGS=()
      if [[ "$SUMMARY_HEADER_WRITTEN" -eq 1 ]]; then
        SUMMARY_ARGS+=(--no-header)
      fi
      python3 scripts/distributed/summarize_raft_profile.py "${SUMMARY_ARGS[@]}" "$RUN" | tee -a "$OUT/summary.csv"
      SUMMARY_HEADER_WRITTEN=1
  done <"$OUT/schedule.csv"

  echo
  echo "Generating TPS graph..."
  python3 scripts/distributed/plot_executor_sweep.py \
    --input-csv "$OUT/summary.csv" \
    --output-img "$OUT/executor_sweep_tps.png" \
    --title "All-replica-audit-drained TPS (Raft majority completion, Threads=$THREADS)"

  echo
  echo "Done."
  echo "Artifacts: $OUT"

} 2>&1 | tee out.txt "$OUT/console.log"
