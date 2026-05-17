#!/usr/bin/env bash
set -euo pipefail

#
# Run one graph-ready YCSB-skew comparison:
#   - single machine majority-pivot node: PG and unsigned DET
#   - full trusted 4-node system: Kafka majority + Raft + BCDB
#
# Outputs under scripts/bench_full_results/ycsb_skew_compare_<timestamp>/:
#   results.csv, summary.csv, overhead.csv, ycsb_skew_tps_comparison.png
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TARGET_NODE="${TARGET_NODE:-neel@10.129.148.236}"
TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL:-}"
REMOTE_REPO="${REMOTE_REPO:-/home/neel/Desktop/ariabc_cluster}"
REMOTE_INSTALL="${REMOTE_INSTALL:-/home/neel/Desktop/ariabc_install}"
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"
TEMPLATE_CONF_LOCAL="${TEMPLATE_CONF_LOCAL:-/work/ARIABC/pgdata/postgresql.conf}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_PORT="${SSH_PORT:-22}"

THREADS="${THREADS:-1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}"
RUNS="${RUNS:-3}"
WORKLOAD="${WORKLOAD:-ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
DB_PORT="${DB_PORT:-5438}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"

# The deterministic gateway must preserve a single global request order, so
# full-system "threads" are modeled as the ordered deterministic concurrency
# budget: gateway pool size, deterministic batch size, and deterministic window.
FULL_THREAD_KNOB="${FULL_THREAD_KNOB:-concurrency}"
FULL_POOL_SIZE_MODE="${FULL_POOL_SIZE_MODE:-fixed}" # fixed|sweep
FULL_FIXED_POOL_SIZE="${FULL_FIXED_POOL_SIZE:-256}"
FULL_DET_BATCH_SIZE="${FULL_DET_BATCH_SIZE:-256}"
FULL_DET_WINDOW="${FULL_DET_WINDOW:-4096}"
FULL_DET_WINDOW_MULTIPLIER="${FULL_DET_WINDOW_MULTIPLIER:-256}"
FULL_DET_WINDOW_MAX="${FULL_DET_WINDOW_MAX:-3072}"
FULL_DET_BLOCK_PARALLEL="${FULL_DET_BLOCK_PARALLEL:-1}"
FULL_DET_BLOCK_PIPELINE="${FULL_DET_BLOCK_PIPELINE:-8}"
FULL_DET_BLOCK_MAX="${FULL_DET_BLOCK_MAX:-256}"
FULL_BCDB_WORKER_COUNT="${FULL_BCDB_WORKER_COUNT:-512}"
FULL_BCDB_DECOUPLE_WORKERS="${FULL_BCDB_DECOUPLE_WORKERS:-1}"
FULL_TEST_QUERIES="${FULL_TEST_QUERIES:-20512}"
FULL_BCDB_BLOCK_PROFILE="${FULL_BCDB_BLOCK_PROFILE:-0}"
FULL_BCDB_BLOCK_WAIT_WATERMARK="${FULL_BCDB_BLOCK_WAIT_WATERMARK:-0}"
FULL_BCDB_SERIAL_GATE_MODE="${FULL_BCDB_SERIAL_GATE_MODE:-1}"
FULL_BCDB_DT_PARSE_BARRIER="${FULL_BCDB_DT_PARSE_BARRIER:-0}"
FULL_BCDB_DT_SKIP_READONLY_GATE="${FULL_BCDB_DT_SKIP_READONLY_GATE:-1}"
FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS="${FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS:-1}"
FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD="${FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD:-65536}"
FULL_RESULT_REPLICA_LIMIT="${FULL_RESULT_REPLICA_LIMIT:-1}"
FULL_CASE_TIMEOUT_S="${FULL_CASE_TIMEOUT_S:-900}"
FULL_SKIP_SYNC="${FULL_SKIP_SYNC:-0}"
FULL_SKIP_BUILD="${FULL_SKIP_BUILD:-0}"
FULL_SKIP_RDKAFKA_SETUP="${FULL_SKIP_RDKAFKA_SETUP:-1}"
POLL_COUNT="${POLL_COUNT:-120000}"
RESULT_RING_CAPACITY="${RESULT_RING_CAPACITY:-32768}"
FULL_CONTINUE_ON_ERROR="${FULL_CONTINUE_ON_ERROR:-0}"

SKIP_SYNC="${SKIP_SYNC:-0}"
SINGLE_ONLY="${SINGLE_ONLY:-0}"
FULL_ONLY="${FULL_ONLY:-0}"
ANALYZE_ONLY="${ANALYZE_ONLY:-0}"
NO_RESUME="${NO_RESUME:-0}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --threads CSV       Default: $THREADS
  --runs N            Default: $RUNS
  --workload FILE     Default: $WORKLOAD
  --target NODE       Default: $TARGET_NODE
  --target-label NAME Label used in CSV/graph for the single-node target.
  --skip-sync
  --single-only
  --full-only
  --analyze-only      Rebuild combined CSVs/graph from existing manifest/files.
  --no-resume         Pass --no-resume to the single-machine runner.

Environment:
  FULL_THREAD_KNOB=concurrency maps each x-axis thread value to the outstanding
  deterministic window while keeping the proven batch size and backend capacity
  normalized.  --num-terminals remains 1 because the deterministic/Raft path has
  one global sequence order.
  FULL_DET_WINDOW_MAX caps the mapped deterministic window; set 0 to disable.
  FULL_POOL_SIZE_MODE=sweep maps x-axis thread value to --pool-size, with a
  minimum of 2 because bcdb_init requires at least two workers.
  FULL_CONTINUE_ON_ERROR=1 keeps sweeping after an invalid full-system case.
  By default the script stops the full-system sweep after the first invalid
  case so a poisoned replica cannot make the remaining x-axis points bogus.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="${2:-}"; shift 2 ;;
    --runs) RUNS="${2:-3}"; shift 2 ;;
    --workload) WORKLOAD="${2:-}"; shift 2 ;;
    --target) TARGET_NODE="${2:-}"; shift 2 ;;
    --target-label) TARGET_MACHINE_LABEL="${2:-}"; shift 2 ;;
    --skip-sync) SKIP_SYNC=1; shift ;;
    --single-only) SINGLE_ONLY=1; shift ;;
    --full-only) FULL_ONLY=1; shift ;;
    --analyze-only) ANALYZE_ONLY=1; shift ;;
    --no-resume) NO_RESUME=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$FULL_THREAD_KNOB" != "pool-size" && "$FULL_THREAD_KNOB" != "concurrency" ]]; then
  echo "ERROR: FULL_THREAD_KNOB=$FULL_THREAD_KNOB is not supported; use pool-size or concurrency" >&2
  exit 2
fi
if [[ "$FULL_POOL_SIZE_MODE" != "fixed" && "$FULL_POOL_SIZE_MODE" != "sweep" ]]; then
  echo "ERROR: FULL_POOL_SIZE_MODE=$FULL_POOL_SIZE_MODE is not supported; use fixed or sweep" >&2
  exit 2
fi

if [[ -z "$TARGET_MACHINE_LABEL" ]]; then
  TARGET_MACHINE_LABEL="${TARGET_NODE##*@}"
  TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL%%:*}"
fi

ts="$(date +%Y%m%d_%H%M%S)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/scripts/bench_full_results/ycsb_skew_compare_${ts}}"
RUN_LOG_DIR="$OUT_ROOT/_run_logs"
SINGLE_LOCAL_DIR="$OUT_ROOT/single_${TARGET_MACHINE_LABEL//./_}"
FULL_MANIFEST="$OUT_ROOT/full_system_manifest.csv"
mkdir -p "$RUN_LOG_DIR" "$SINGLE_LOCAL_DIR"

log() { echo "[$(date +'%F %T')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

ssh_run() {
  ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT" "$TARGET_NODE" "$@"
}

rsync_to_target() {
  local rc

  set +e
  rsync -az "$@" -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT"
  rc=$?
  set -e

  # Autoconf/build scratch files such as ./conftest can disappear while rsync
  # walks the tree. Code 24 means "vanished source files"; the target copy is
  # still usable for this benchmark sync.
  if [[ "$rc" == "24" ]]; then
    log "WARNING: rsync saw vanished source files; continuing"
    return 0
  fi
  return "$rc"
}

local_install_has_bcdb_gucs() {
  local postgres_bin="$LOCAL_INSTALL_DIR/bin/postgres"
  [[ -x "$postgres_bin" ]] || return 1
  LD_LIBRARY_PATH="$LOCAL_INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}" \
    "$postgres_bin" --describe-config 2>/dev/null \
    | grep -qE '^bcdb_worker_count([[:space:]]|\|)'
}

append_full_manifest_header() {
  if [[ ! -f "$FULL_MANIFEST" ]]; then
    echo "thread,run,artifact_dir,exit_code,thread_knob,pool_size,bcdb_worker_count,det_batch_size,det_window,det_block_pipeline,det_block_max,req_id_offset,notes" > "$FULL_MANIFEST"
  fi
}

sync_single_target() {
  [[ "$FULL_ONLY" == "1" ]] && { log "Single-node sync skipped (--full-only)"; return; }
  [[ "$SKIP_SYNC" == "1" ]] && { log "Single-node sync skipped"; return; }
  log "Syncing source/install to $TARGET_NODE"
  ssh_run "mkdir -p '$REMOTE_REPO' '$REMOTE_INSTALL' '$REMOTE_REPO/.bench_tmp' '$REMOTE_REPO/.bench_tmp/deps/lib'"
  rsync_to_target --delete \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='.bench_tmp' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='conftest' \
    --exclude='conftest.*' \
    --exclude='scripts/bench_full_results' \
    --exclude='scripts/bench_results' \
    "$REPO_ROOT/" "$TARGET_NODE:$REMOTE_REPO/"
  if local_install_has_bcdb_gucs; then
    log "Local install looks BCDB-capable; syncing install tree"
    rsync_to_target --delete "$LOCAL_INSTALL_DIR/" "$TARGET_NODE:$REMOTE_INSTALL/"
  else
    log "WARNING: local install at $LOCAL_INSTALL_DIR is not BCDB-capable; skipping install sync to preserve remote custom install"
  fi
  rsync_to_target "$TEMPLATE_CONF_LOCAL" "$TARGET_NODE:$REMOTE_REPO/.bench_tmp/shared_postgresql.conf"
}

run_single_node() {
  [[ "$FULL_ONLY" == "1" ]] && return
  log "Running single-node PG/DET benchmark on $TARGET_NODE"
  remote_out="$REMOTE_REPO/scripts/bench_results/ycsb_skew_compare_${ts}"
  remote_log="$REMOTE_REPO/.bench_tmp/ycsb_skew_compare_single_${ts}.log"
  local log_file="$RUN_LOG_DIR/single_node_${TARGET_MACHINE_LABEL//./_}.log"
  local no_resume_arg=""
  local single_extra_gucs="bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD"
  [[ "$NO_RESUME" == "1" ]] && no_resume_arg="--no-resume"
  if [[ -n "${SINGLE_BCDB_EXTRA_GUCS:-}" ]]; then
    single_extra_gucs="$SINGLE_BCDB_EXTRA_GUCS,$single_extra_gucs"
  fi

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO/scripts'
bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export BCDB_EXTRA_GUCS='$single_extra_gucs'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
PYTHON_BIN=''
if [[ -x '$REMOTE_REPO/.venv/bin/python3' ]] && '$REMOTE_REPO/.venv/bin/python3' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
elif [[ -x '$REMOTE_REPO/.venv/bin/python' ]] && '$REMOTE_REPO/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
  python3 -m venv --clear '$REMOTE_REPO/.venv'
fi
export ARIABC_PYTHON=\"\$PYTHON_BIN\"
pgdata_line=\$(bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] && export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
if ! \$PYTHON_BIN -c 'import psycopg' >/dev/null 2>&1; then
  \$PYTHON_BIN -m pip install -q --disable-pip-version-check 'psycopg[binary]' psycopg >/dev/null
fi
echo \"ARIABC_PYTHON=\$ARIABC_PYTHON\"
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes pg,det \
  --signing-modes 0 \
  --enforce-signatures 0 \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --workloads '$WORKLOAD' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out' $no_resume_arg
")" > "$log_file" 2>&1

  log "Collecting single-node results from $TARGET_NODE:$remote_out"
  rsync_to_target "$TARGET_NODE:$remote_out/" "$SINGLE_LOCAL_DIR/"
}

run_full_system() {
  [[ "$SINGLE_ONLY" == "1" ]] && return
  append_full_manifest_header
  log "Running full-system Kafka+Raft+BCDB sweep"
  log "Full-system x-axis mapping: thread value -> ordered concurrency budget; --num-terminals remains 1"

  local first=1
  IFS=',' read -ra thread_arr <<< "$THREADS"
  for th in "${thread_arr[@]}"; do
    th="${th//[[:space:]]/}"
    [[ -z "$th" ]] && continue
    for run in $(seq 1 "$RUNS"); do
      if [[ "$FULL_POOL_SIZE_MODE" == "fixed" ]]; then
        full_pool_size="$FULL_FIXED_POOL_SIZE"
      else
        full_pool_size="$th"
        if [[ "$full_pool_size" -lt 2 ]]; then
          full_pool_size=2
        fi
      fi
      if [[ "$FULL_THREAD_KNOB" == "concurrency" ]]; then
        full_det_batch_size="$FULL_DET_BATCH_SIZE"
        full_det_window=$(( th * FULL_DET_WINDOW_MULTIPLIER ))
        if [[ "$full_det_window" -lt "$full_det_batch_size" ]]; then
          full_det_window="$full_det_batch_size"
        fi
        if [[ "$FULL_DET_WINDOW_MAX" -gt 0 && "$full_det_window" -gt "$FULL_DET_WINDOW_MAX" ]]; then
          full_det_window="$FULL_DET_WINDOW_MAX"
        fi
      else
        full_det_batch_size="$FULL_DET_BATCH_SIZE"
        full_det_window="$FULL_DET_WINDOW"
      fi
      if [[ -n "$FULL_BCDB_WORKER_COUNT" ]]; then
        full_bcdb_worker_count="$FULL_BCDB_WORKER_COUNT"
      else
        full_bcdb_worker_count="$full_pool_size"
      fi
      req_id_offset=$(( (run * 1000000) + (th * 10000) + 1 ))
      log "Full-system case thread=$th run=$run (pool-size=$full_pool_size worker-count=$full_bcdb_worker_count det-batch=$full_det_batch_size det-window=$full_det_window det-block-parallel=$FULL_DET_BLOCK_PARALLEL det-block-pipeline=$FULL_DET_BLOCK_PIPELINE det-block-max=$FULL_DET_BLOCK_MAX serial-gate=$FULL_BCDB_SERIAL_GATE_MODE completion-only-skip-reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS full-result-replica-limit=$FULL_RESULT_REPLICA_LIMIT)"
      before_file="$RUN_LOG_DIR/full_before_${th}_${run}.txt"
      after_file="$RUN_LOG_DIR/full_after_${th}_${run}.txt"
      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$before_file" || true

      extra_skip=()
      [[ "$FULL_SKIP_SYNC" == "1" || "$first" == "0" ]] && extra_skip+=(--skip-sync)
      [[ "$FULL_SKIP_BUILD" == "1" || "$first" == "0" ]] && extra_skip+=(--skip-build)
      [[ "$FULL_SKIP_RDKAFKA_SETUP" == "1" || "$first" == "0" ]] && extra_skip+=(--skip-rdkafka-setup)

      set +e
      timeout "$FULL_CASE_TIMEOUT_S" env POLL_COUNT="$POLL_COUNT" RESULT_RING_CAPACITY="$RESULT_RING_CAPACITY" \
      "$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh" \
        "${extra_skip[@]}" \
        --workload "$REPO_ROOT/scripts/$WORKLOAD" \
        --test-queries "$FULL_TEST_QUERIES" \
        --req-id-offset "$req_id_offset" \
        --pool-size "$full_pool_size" \
        --bcdb-worker-count "$full_bcdb_worker_count" \
        --bcdb-decouple-workers "$FULL_BCDB_DECOUPLE_WORKERS" \
        --det-batch-size "$full_det_batch_size" \
        --det-window "$full_det_window" \
        --det-block-pipeline "$FULL_DET_BLOCK_PIPELINE" \
        --det-block-parallel "$FULL_DET_BLOCK_PARALLEL" \
        --det-block-max "$FULL_DET_BLOCK_MAX" \
        --num-terminals 1 \
        --bcdb-block-profile "$FULL_BCDB_BLOCK_PROFILE" \
        --bcdb-block-wait-watermark "$FULL_BCDB_BLOCK_WAIT_WATERMARK" \
        --bcdb-serial-gate-mode "$FULL_BCDB_SERIAL_GATE_MODE" \
        --bcdb-dt-parse-barrier "$FULL_BCDB_DT_PARSE_BARRIER" \
        --bcdb-dt-skip-readonly-gate "$FULL_BCDB_DT_SKIP_READONLY_GATE" \
        --bcdb-dt-completion-only-skip-reads "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" \
        --bcdb-dt-hashtab-switch-threshold "$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD" \
        --full-result-replica-limit "$FULL_RESULT_REPLICA_LIMIT" \
        > "$RUN_LOG_DIR/full_thread_${th}_run_${run}.log" 2>&1
      rc=$?
      set -e
      first=0

      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$after_file" || true
      artifact="$(grep -vxF -f "$before_file" "$after_file" | head -n 1 || true)"
      if [[ -z "$artifact" ]]; then
        artifact="$(head -n 1 "$after_file" || true)"
      fi
      [[ -n "$artifact" ]] || artifact="$RUN_LOG_DIR/missing_full_artifact_thread_${th}_run_${run}"
      notes="full_system_thread_knob=$FULL_THREAD_KNOB;full_pool_size_mode=$FULL_POOL_SIZE_MODE;num_terminals=1;trusted_gate=kafka_majority_merkle;pool_size_min=2;det_window_multiplier=$FULL_DET_WINDOW_MULTIPLIER;det_window_max=$FULL_DET_WINDOW_MAX;det_block_parallel=$FULL_DET_BLOCK_PARALLEL;det_block_pipeline=$FULL_DET_BLOCK_PIPELINE;det_block_max=$FULL_DET_BLOCK_MAX;bcdb_block_wait_watermark=$FULL_BCDB_BLOCK_WAIT_WATERMARK;bcdb_serial_gate_mode=$FULL_BCDB_SERIAL_GATE_MODE;bcdb_dt_skip_readonly_gate=$FULL_BCDB_DT_SKIP_READONLY_GATE;bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;det_block_skip_readonly=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD;full_result_replica_limit=$FULL_RESULT_REPLICA_LIMIT;backend_capacity=normalized"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$th" "$run" "$artifact" "$rc" "$FULL_THREAD_KNOB" "$full_pool_size" "$full_bcdb_worker_count" "$full_det_batch_size" "$full_det_window" "$FULL_DET_BLOCK_PIPELINE" "$FULL_DET_BLOCK_MAX" "$req_id_offset" "$notes" \
        >> "$FULL_MANIFEST"
      if [[ "$rc" != "0" ]]; then
        if [[ "$FULL_CONTINUE_ON_ERROR" == "1" ]]; then
          log "WARNING: full-system case thread=$th run=$run exited rc=$rc; continuing because FULL_CONTINUE_ON_ERROR=1"
        else
          log "WARNING: full-system case thread=$th run=$run exited rc=$rc; stopping full-system sweep so the failed replica cannot poison later points"
          return 0
        fi
      fi
    done
  done
}

generate_outputs() {
  local single_results="$SINGLE_LOCAL_DIR/results.csv"
  [[ "$FULL_ONLY" == "1" ]] && single_results="${SINGLE_RESULTS_CSV:-$single_results}"
  if [[ ! -f "$single_results" ]]; then
    if [[ "$FULL_ONLY" == "1" ]]; then
      log "Single-node results unavailable in --full-only mode; generating full-system-only outputs"
      single_results="/dev/null"
    else
      die "missing single-node results.csv: $single_results"
    fi
  fi
  [[ -f "$FULL_MANIFEST" ]] || die "missing full-system manifest: $FULL_MANIFEST"
  MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" \
    python3 "$SCRIPT_DIR/plot_ycsb_skew_tps_comparison.py" \
      --single-results "$single_results" \
      --full-manifest "$FULL_MANIFEST" \
      --out-dir "$OUT_ROOT" \
      --workload "$WORKLOAD" \
      --machine "$TARGET_MACHINE_LABEL" \
      --threads "$THREADS"
}

log "=== YCSB skew TPS comparison ==="
log "Out root : $OUT_ROOT"
log "Threads  : $THREADS"
log "Runs     : $RUNS"
log "Workload : $WORKLOAD"
log "Target   : $TARGET_NODE"

if [[ "$ANALYZE_ONLY" != "1" ]]; then
  sync_single_target
  run_single_node
  run_full_system
fi
generate_outputs

log "Done"
log "Results : $OUT_ROOT/results.csv"
log "Summary : $OUT_ROOT/summary.csv"
log "Overhead: $OUT_ROOT/overhead.csv"
log "Graph   : $OUT_ROOT/ycsb_skew_tps_comparison.png"
