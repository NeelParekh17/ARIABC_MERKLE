#!/usr/bin/env bash
set -euo pipefail

#
# Run one graph-ready YCSB-skew comparison:
#   - single machine majority-pivot node: PG and unsigned DET
#   - trusted 4-node cluster modes:
#       * Kafka-only: preordered direct broadcast + Kafka majority + BCDB
#       * Raft+Kafka: Raft ordering + Kafka majority + BCDB
#
# Outputs under scripts/bench_full_results/ycsb_skew_compare_<timestamp>/:
#   results.csv, summary.csv, overhead.csv, ycsb_skew_tps_comparison.png
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TARGET_NODE="${TARGET_NODE:-neel@10.129.148.236}"
TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL:-}"
TARGETS_DEFAULT="neel@10.129.148.236,neel@10.129.27.54,neel@10.129.148.179,neel@10.129.148.248"
TARGETS="${TARGETS:-}"
REMOTE_REPO="${REMOTE_REPO:-/home/neel/Desktop/ariabc_cluster}"
REMOTE_INSTALL="${REMOTE_INSTALL:-/home/neel/Desktop/ariabc_install}"
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"
TEMPLATE_CONF_LOCAL="${TEMPLATE_CONF_LOCAL:-/work/ARIABC/pgdata/postgresql.conf}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_PORT="${SSH_PORT:-22}"

THREADS="${THREADS:-1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}"
RUNS="${RUNS:-3}"
DEFAULT_WORKLOADS="ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt,ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt"
WORKLOADS="${WORKLOADS:-${WORKLOAD:-$DEFAULT_WORKLOADS}}"
DB_PORT="${DB_PORT:-5438}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"
EXPERIMENT_MODE="${EXPERIMENT_MODE:-pipeline-saturation}" # pipeline-saturation|strict-overhead

# The fair full-system x-axis is real deterministic gateway client terminals.
# The gateway still preserves one global order internally, but --num-terminals
# now controls terminal lanes and --det-pipeline-depth controls per-lane
# outstanding work.
FULL_THREAD_KNOB="${FULL_THREAD_KNOB:-client-pipeline}"
FULL_POOL_SIZE_MODE="${FULL_POOL_SIZE_MODE:-fixed}" # fixed|sweep
FULL_FIXED_POOL_SIZE="${FULL_FIXED_POOL_SIZE:-256}"
FULL_DET_BATCH_SIZE="${FULL_DET_BATCH_SIZE:-160}"
FULL_DET_PIPELINE_DEPTH_WAS_SET=0
if [[ -n "${FULL_DET_PIPELINE_DEPTH+x}" ]]; then
  FULL_DET_PIPELINE_DEPTH_WAS_SET=1
fi
FULL_DET_PIPELINE_DEPTH="${FULL_DET_PIPELINE_DEPTH:-80}"
FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY="${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY:-128}"
# Bumped 64 -> 128 to match kafka-only baseline. With raft-kafka's prior depth=64
# the cluster_raft_kafka series was structurally capped at half the kafka-only
# effective_inflight at every thread, producing a misleading 30-35% TPS gap at
# low threads that was just a knob mismatch (not a real Raft overhead).
FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA="${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA:-128}"
# At low thread counts the per-lane det_pipeline_depth is also the in-flight cap
# (SELECTED_DET_WINDOW = th * depth, det_pipeline_cap = min(window, terminals*depth)).
# With depth=128 and 1 terminal, only ONE batch can be in flight at a time, so the
# kafka-result roundtrip (~6-7 ms per batch) sits on the critical path and the
# kafka-only / raft-kafka modes lose ~30% at thread=1 vs gateway-direct.
# Bumping the per-lane depth at thread=1 to 512 lets the lane hold ~3.2 batches
# in flight and hides the kafka roundtrip behind concurrent work, dropping the
# overhead at thread=1 from ~32% to ~15% (kafka) and from ~34% to ~3% (raft).
# At thread>=4 each terminal naturally provides its own pipelining, so the map
# only targets thread=1.
FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP="${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP:-1:512}"
FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP="${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP:-1:512}"
FULL_DET_WINDOW="${FULL_DET_WINDOW:-4096}"
FULL_DET_WINDOW_MULTIPLIER="${FULL_DET_WINDOW_MULTIPLIER:-256}"
FULL_DET_WINDOW_MAX="${FULL_DET_WINDOW_MAX:-3072}"
FULL_DET_BATCH_SIZE_MAP="${FULL_DET_BATCH_SIZE_MAP:-}"
FULL_DET_WINDOW_MAP="${FULL_DET_WINDOW_MAP:-}"
FULL_DET_BLOCK_PARALLEL="${FULL_DET_BLOCK_PARALLEL:-1}"
FULL_DET_BLOCK_PIPELINE="${FULL_DET_BLOCK_PIPELINE:-8}"
FULL_DET_BLOCK_MAX="${FULL_DET_BLOCK_MAX:-256}"
FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US="${FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US:-0}"
FULL_BCDB_WORKER_COUNT="${FULL_BCDB_WORKER_COUNT:-512}"
FULL_BCDB_DECOUPLE_WORKERS="${FULL_BCDB_DECOUPLE_WORKERS:-1}"
FULL_TEST_QUERIES="${FULL_TEST_QUERIES:-20512}"
FULL_BCDB_BLOCK_PROFILE="${FULL_BCDB_BLOCK_PROFILE:-0}"
FULL_BCDB_BLOCK_WAIT_WATERMARK="${FULL_BCDB_BLOCK_WAIT_WATERMARK:-0}"
FULL_BCDB_SERIAL_GATE_MODE="${FULL_BCDB_SERIAL_GATE_MODE:-1}"
FULL_BCDB_SERIAL_GATE_SOURCE="${FULL_BCDB_SERIAL_GATE_SOURCE:-0}"
FULL_BCDB_DT_PARSE_BARRIER="${FULL_BCDB_DT_PARSE_BARRIER:-1}"
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
FULL_CLUSTER_MODES="${FULL_CLUSTER_MODES:-kafka-only,raft-kafka}"
SINGLE_GATEWAY_DIRECT="${SINGLE_GATEWAY_DIRECT:-1}"
SINGLE_GATEWAY_CLIENT_PORT_BASE="${SINGLE_GATEWAY_CLIENT_PORT_BASE:-19100}"
SINGLE_GATEWAY_RAFT_PORT_BASE="${SINGLE_GATEWAY_RAFT_PORT_BASE:-19200}"
SINGLE_GATEWAY_POOL_SIZE="${SINGLE_GATEWAY_POOL_SIZE:-}"
SINGLE_GATEWAY_BCDB_WORKER_COUNT="${SINGLE_GATEWAY_BCDB_WORKER_COUNT:-}"
SINGLE_GATEWAY_CASE_TIMEOUT_S="${SINGLE_GATEWAY_CASE_TIMEOUT_S:-$FULL_CASE_TIMEOUT_S}"
SINGLE_BCDB_SERIAL_GATE_MODE="${SINGLE_BCDB_SERIAL_GATE_MODE:-0}"
SINGLE_BCDB_SERIAL_GATE_SOURCE="${SINGLE_BCDB_SERIAL_GATE_SOURCE:-0}"
SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK="${SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK:-on}"
SINGLE_BCDB_POLL_MAX_US="${SINGLE_BCDB_POLL_MAX_US:-1}"
SINGLE_BCDB_DT_PARSE_BARRIER="${SINGLE_BCDB_DT_PARSE_BARRIER:-0}"
SINGLE_BCDB_DT_LIGHT_SNAPSHOT="${SINGLE_BCDB_DT_LIGHT_SNAPSHOT:-0}"
SINGLE_BCDB_DT_SKIP_READONLY_GATE="${SINGLE_BCDB_DT_SKIP_READONLY_GATE:-1}"
SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS="${SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS:-on}"
SINGLE_BCDB_RESULT_RING_SLOTS="${SINGLE_BCDB_RESULT_RING_SLOTS:-$RESULT_RING_CAPACITY}"
SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS="${SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS:-0}"
SINGLE_BCDB_FLOW_DEBUG="${SINGLE_BCDB_FLOW_DEBUG:-0}"
SINGLE_BCDB_GATE_DEBUG="${SINGLE_BCDB_GATE_DEBUG:-0}"
SINGLE_BCDB_APPLY_WAIT_DEBUG="${SINGLE_BCDB_APPLY_WAIT_DEBUG:-0}"
SINGLE_TIMEOUT_WORKLOAD_S="${SINGLE_TIMEOUT_WORKLOAD_S:-0}"
SINGLE_TIMEOUT_WORKLOAD_DET_S="${SINGLE_TIMEOUT_WORKLOAD_DET_S:-1800}"
SINGLE_REMOTE_KILL_STALE="${SINGLE_REMOTE_KILL_STALE:-0}"
SINGLE_BCDB_PHASE_TRACE_PREFIX="${SINGLE_BCDB_PHASE_TRACE_PREFIX:-}"
SINGLE_PYTHON_BIN="${SINGLE_PYTHON_BIN:-}"

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
  --workload FILE     Single workload (back-compat).
  --workloads CSV     Comma-separated list of workload files; each produces its
                      own subdir + graph under OUT_ROOT. Default: $WORKLOADS
  --target NODE       Single-target back-compat shortcut for --targets NODE.
  --targets CSV       Comma-separated user@host list to run the single-node bench
                      on in parallel. Default: $TARGETS_DEFAULT
                      One single_<label>/ subdir + graph per target. The 3rd-
                      fastest node (by peak DET median TPS across the thread
                      sweep) is then used for the gateway-direct baseline and
                      the combined det-vs-cluster comparison plot.
  --target-label NAME Label used in CSV/graph for the chosen comparison target.
  --skip-sync
  --single-only
  --full-only
  --cluster-modes CSV
                      Cluster series to run for the full-system side.
                      Default: $FULL_CLUSTER_MODES
                      Values: kafka-only,raft-kafka
  --experiment-mode MODE
                      Default: $EXPERIMENT_MODE
                      pipeline-saturation keeps the gateway pipeline surface and
                      reports effective in-flight work. strict-overhead forces
                      full-system detPipelineDepth=1 so x-axis concurrency is
                      comparable to direct single-node threads.
  --analyze-only      Rebuild combined CSVs/graph from existing manifest/files.
  --no-resume         Pass --no-resume to the single-machine runner.

Environment:
  FULL_THREAD_KNOB=client-pipeline maps each x-axis thread value to
  --num-terminals and uses a per-terminal outstanding depth.  If
  FULL_DET_PIPELINE_DEPTH is set, it applies to every cluster mode.  Otherwise
  kafka-only defaults to FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY=128. raft-kafka
  defaults to one fixed FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA=64 across thread
  points unless an explicit map is supplied.  The full-system detWindow is set
  to thread * selected_depth unless FULL_DET_WINDOW_MAP overrides it.
  This is the proper terminal-aware graph shape from plan.txt.
  FULL_THREAD_KNOB=concurrency is the older capacity-calibration mode that maps
  each x-axis value to deterministic window while keeping --num-terminals 1.
  FULL_THREAD_KNOB=fixed-window keeps detWindow fixed for all x-axis values.
  FULL_DET_WINDOW_MAX caps the mapped deterministic window; set 0 to disable.
  FULL_DET_WINDOW_MAP and FULL_DET_BATCH_SIZE_MAP accept comma-separated
  thread:value overrides, e.g. FULL_DET_WINDOW_MAP=1:96,2:192.
  FULL_POOL_SIZE_MODE=sweep maps x-axis thread value to --pool-size, with a
  minimum of 2 because bcdb_init requires at least two workers.
  FULL_CONTINUE_ON_ERROR=1 keeps sweeping after an invalid full-system case.
  By default the script stops the full-system sweep after the first invalid
  case so a poisoned replica cannot make the remaining x-axis points bogus.
  EXPERIMENT_MODE=pipeline-saturation is the default comparison mode.
  EXPERIMENT_MODE=strict-overhead forces FULL_THREAD_KNOB=client-pipeline and
  FULL_DET_PIPELINE_DEPTH=1 when a one-outstanding-request comparison is the
  question instead of a near-saturation comparison.
  SINGLE_GATEWAY_DIRECT=1 runs the apples-to-apples single-node
  gateway-direct baseline by sending the same workload through
  ariabc_pg_gateway --completionPath direct --totalNodes 1 against one
  bypass-Raft server. Set SINGLE_GATEWAY_DIRECT=0 only for legacy output.
  SINGLE_PYTHON_BIN=/path/to/python3 forces the single-node matrix runner to
  use that remote Python when the executable can import psycopg.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="${2:-}"; shift 2 ;;
    --runs) RUNS="${2:-3}"; shift 2 ;;
    --workload) WORKLOADS="${2:-}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --target) TARGET_NODE="${2:-}"; TARGETS="$TARGET_NODE"; shift 2 ;;
    --targets) TARGETS="${2:-}"; shift 2 ;;
    --target-label) TARGET_MACHINE_LABEL="${2:-}"; shift 2 ;;
    --skip-sync) SKIP_SYNC=1; shift ;;
    --single-only) SINGLE_ONLY=1; shift ;;
    --full-only) FULL_ONLY=1; shift ;;
    --cluster-modes) FULL_CLUSTER_MODES="${2:-}"; shift 2 ;;
    --experiment-mode) EXPERIMENT_MODE="${2:-}"; shift 2 ;;
    --analyze-only) ANALYZE_ONLY=1; shift ;;
    --no-resume) NO_RESUME=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$EXPERIMENT_MODE" != "pipeline-saturation" && "$EXPERIMENT_MODE" != "strict-overhead" ]]; then
  echo "ERROR: EXPERIMENT_MODE=$EXPERIMENT_MODE is not supported; use pipeline-saturation or strict-overhead" >&2
  exit 2
fi
if [[ "$EXPERIMENT_MODE" == "strict-overhead" ]]; then
  FULL_THREAD_KNOB="client-pipeline"
  FULL_DET_PIPELINE_DEPTH_WAS_SET=1
  FULL_DET_PIPELINE_DEPTH=1
  FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY=1
  FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA=1
  FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP=""
  FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP=""
  FULL_DET_WINDOW_MAX=0
fi

if [[ "$FULL_THREAD_KNOB" != "client-pipeline" && "$FULL_THREAD_KNOB" != "pool-size" && "$FULL_THREAD_KNOB" != "concurrency" && "$FULL_THREAD_KNOB" != "fixed-window" ]]; then
  echo "ERROR: FULL_THREAD_KNOB=$FULL_THREAD_KNOB is not supported; use client-pipeline, pool-size, concurrency, or fixed-window" >&2
  exit 2
fi
if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
  for depth_name in FULL_DET_PIPELINE_DEPTH FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA; do
    depth_value="${!depth_name}"
    if [[ "$depth_value" -lt 1 ]]; then
      echo "ERROR: $depth_name must be >= 1 for FULL_THREAD_KNOB=client-pipeline" >&2
      exit 2
    fi
  done
fi
if [[ "$FULL_POOL_SIZE_MODE" != "fixed" && "$FULL_POOL_SIZE_MODE" != "sweep" ]]; then
  echo "ERROR: FULL_POOL_SIZE_MODE=$FULL_POOL_SIZE_MODE is not supported; use fixed or sweep" >&2
  exit 2
fi

normalize_cluster_mode() {
  case "$1" in
    raft|raft-kafka|raft_kafka)
      printf '%s\n' "raft-kafka"
      ;;
    kafka|kafka-only|kafka_only|preordered-direct-broadcast|preordered_direct_broadcast)
      printf '%s\n' "kafka-only"
      ;;
    *)
      return 1
      ;;
  esac
}

if [[ -z "$FULL_CLUSTER_MODES" ]]; then
  echo "ERROR: --cluster-modes cannot be empty" >&2
  exit 2
fi
IFS=',' read -ra full_mode_check_arr <<< "$FULL_CLUSTER_MODES"
for mode_check in "${full_mode_check_arr[@]}"; do
  mode_check="${mode_check//[[:space:]]/}"
  [[ -z "$mode_check" ]] && continue
  if ! normalize_cluster_mode "$mode_check" >/dev/null; then
    echo "ERROR: unsupported cluster mode '$mode_check'; use kafka-only or raft-kafka" >&2
    exit 2
  fi
done

if [[ -z "$TARGETS" ]]; then
  TARGETS="$TARGETS_DEFAULT"
fi

declare -a TARGETS_ARR=()
declare -a LABELS_ARR=()
IFS=',' read -ra _targets_split <<< "$TARGETS"
for _t in "${_targets_split[@]}"; do
  _t="${_t//[[:space:]]/}"
  [[ -z "$_t" ]] && continue
  TARGETS_ARR+=("$_t")
  _lbl="${_t##*@}"
  _lbl="${_lbl%%:*}"
  _lbl="${_lbl//./_}"
  LABELS_ARR+=("$_lbl")
done
if [[ "${#TARGETS_ARR[@]}" -eq 0 ]]; then
  echo "ERROR: no targets parsed from TARGETS=$TARGETS" >&2
  exit 2
fi

# TARGET_NODE / TARGET_MACHINE_LABEL default to the first target. They are
# later reassigned to the 3rd-fastest node before the gateway-direct step.
TARGET_NODE="${TARGETS_ARR[0]}"
if [[ -z "$TARGET_MACHINE_LABEL" ]]; then
  TARGET_MACHINE_LABEL="${LABELS_ARR[0]}"
fi

ts="$(date +%Y%m%d_%H%M%S)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/scripts/bench_full_results/ycsb_skew_compare_${ts}}"
mkdir -p "$OUT_ROOT/_run_logs"

# Per-workload paths. set_workload_paths <workload-file> populates:
#   WORKLOAD, WORKLOAD_SLUG, OUT_DIR, SINGLE_LOCAL_DIR,
#   SINGLE_GATEWAY_LOCAL_DIR, SINGLE_GATEWAY_MANIFEST, FULL_MANIFEST
set_workload_paths() {
  WORKLOAD="$1"
  WORKLOAD_SLUG="$(printf '%s' "${WORKLOAD%.*}" | tr -cs '[:alnum:]' '_' | sed 's/^_*//; s/_*$//')"
  [[ -z "$WORKLOAD_SLUG" ]] && WORKLOAD_SLUG="workload"
  OUT_DIR="$OUT_ROOT/$WORKLOAD_SLUG"
  SINGLE_LOCAL_DIR="$OUT_DIR/single_${TARGET_MACHINE_LABEL//./_}"
  SINGLE_GATEWAY_LOCAL_DIR="$OUT_DIR/single_gateway_direct"
  SINGLE_GATEWAY_MANIFEST="$OUT_DIR/single_gateway_direct_manifest.csv"
  FULL_MANIFEST="$OUT_DIR/full_system_manifest.csv"
  RUN_LOG_DIR="$OUT_ROOT/_run_logs/$WORKLOAD_SLUG"
  SINGLE_LOCAL_DIRS=()
  for _lbl in "${LABELS_ARR[@]}"; do
    SINGLE_LOCAL_DIRS+=("$OUT_DIR/single_${_lbl//./_}")
    mkdir -p "$OUT_DIR/single_${_lbl//./_}"
  done
  mkdir -p "$OUT_DIR" "$SINGLE_LOCAL_DIR" "$SINGLE_GATEWAY_LOCAL_DIR" "$RUN_LOG_DIR"
}

single_dir_for_label() {
  echo "$OUT_DIR/single_${1//./_}"
}

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
  # Capture output first, then grep. Piping into `grep -q` causes SIGPIPE on
  # postgres after the first match, which under `set -o pipefail` flips the
  # pipeline's exit status to non-zero — making the function race with itself
  # when called concurrently from multiple background sync jobs.
  local desc
  desc="$(LD_LIBRARY_PATH="$LOCAL_INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}" "$postgres_bin" --describe-config 2>/dev/null)" || return 1
  printf '%s\n' "$desc" | grep -qE '^bcdb_worker_count([[:space:]]|\|)'
}

append_full_manifest_header() {
  if [[ ! -f "$FULL_MANIFEST" ]]; then
    echo "series,mode,experiment_mode,ordering_mode,ordering_path,completion_path,server_bypass_raft,gateway_broadcast_to_all,thread,run,artifact_dir,exit_code,thread_knob,pool_size,bcdb_worker_count,det_batch_size,det_window,num_terminals,det_pipeline_depth,effective_inflight,det_block_pipeline,det_block_max,req_id_offset,tps_denominator_policy,notes" > "$FULL_MANIFEST"
  fi
}

append_single_gateway_manifest_header() {
  if [[ ! -f "$SINGLE_GATEWAY_MANIFEST" ]]; then
    echo "series,mode,experiment_mode,ordering_mode,ordering_path,completion_path,server_bypass_raft,gateway_broadcast_to_all,thread,run,artifact_dir,exit_code,thread_knob,pool_size,bcdb_worker_count,det_batch_size,det_window,num_terminals,det_pipeline_depth,effective_inflight,det_block_pipeline,det_block_max,req_id_offset,tps_denominator_policy,notes" > "$SINGLE_GATEWAY_MANIFEST"
  fi
}

lookup_thread_override() {
  local map="$1"
  local thread="$2"
  local entry key value
  [[ -n "$map" ]] || return 1
  IFS=',' read -ra entries <<< "$map"
  for entry in "${entries[@]}"; do
    entry="${entry//[[:space:]]/}"
    [[ -n "$entry" ]] || continue
    key="${entry%%:*}"
    value="${entry#*:}"
    if [[ "$key" == "$thread" && "$value" != "$entry" && -n "$value" ]]; then
      printf '%s\n' "$value"
      return 0
    fi
  done
  return 1
}

select_full_case_params() {
  local cluster_mode="$1"
  local th="$2"
  local override

  if [[ "$FULL_POOL_SIZE_MODE" == "fixed" ]]; then
    SELECTED_POOL_SIZE="$FULL_FIXED_POOL_SIZE"
  else
    SELECTED_POOL_SIZE="$th"
    if [[ "$SELECTED_POOL_SIZE" -lt 2 ]]; then
      SELECTED_POOL_SIZE=2
    fi
  fi

  SELECTED_NUM_TERMINALS=1
  SELECTED_DET_PIPELINE_DEPTH=0
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    SELECTED_NUM_TERMINALS="$th"
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH"
    elif [[ "$cluster_mode" == "kafka-only" ]]; then
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY"
      if override="$(lookup_thread_override "$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP" "$th")"; then
        SELECTED_DET_PIPELINE_DEPTH="$override"
      fi
    else
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA"
      if override="$(lookup_thread_override "$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP" "$th")"; then
        SELECTED_DET_PIPELINE_DEPTH="$override"
      fi
    fi
    if [[ "$SELECTED_DET_PIPELINE_DEPTH" -lt 1 ]]; then
      die "selected det-pipeline-depth must be >= 1 (mode=$cluster_mode thread=$th value=$SELECTED_DET_PIPELINE_DEPTH)"
    fi
    SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE"
    SELECTED_DET_WINDOW=$(( th * SELECTED_DET_PIPELINE_DEPTH ))
  elif [[ "$FULL_THREAD_KNOB" == "concurrency" ]]; then
    SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE"
    SELECTED_DET_WINDOW=$(( th * FULL_DET_WINDOW_MULTIPLIER ))
    if [[ "$SELECTED_DET_WINDOW" -lt "$SELECTED_DET_BATCH_SIZE" ]]; then
      SELECTED_DET_WINDOW="$SELECTED_DET_BATCH_SIZE"
    fi
    if [[ "$FULL_DET_WINDOW_MAX" -gt 0 && "$SELECTED_DET_WINDOW" -gt "$FULL_DET_WINDOW_MAX" ]]; then
      SELECTED_DET_WINDOW="$FULL_DET_WINDOW_MAX"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE"
    SELECTED_DET_WINDOW="$FULL_DET_WINDOW"
  else
    SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE"
    SELECTED_DET_WINDOW="$FULL_DET_WINDOW"
  fi

  if override="$(lookup_thread_override "$FULL_DET_BATCH_SIZE_MAP" "$th")"; then
    SELECTED_DET_BATCH_SIZE="$override"
  fi
  if override="$(lookup_thread_override "$FULL_DET_WINDOW_MAP" "$th")"; then
    SELECTED_DET_WINDOW="$override"
  fi

  SELECTED_EFFECTIVE_INFLIGHT="$SELECTED_DET_WINDOW"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    SELECTED_EFFECTIVE_INFLIGHT=$(( SELECTED_NUM_TERMINALS * SELECTED_DET_PIPELINE_DEPTH ))
  fi
  if [[ -n "$FULL_BCDB_WORKER_COUNT" ]]; then
    SELECTED_BCDB_WORKER_COUNT="$FULL_BCDB_WORKER_COUNT"
  else
    SELECTED_BCDB_WORKER_COUNT="$SELECTED_POOL_SIZE"
  fi
}

local_glibc_version() {
  ldd --version 2>/dev/null | awk 'NR==1{print $NF; exit}'
}

remote_glibc_version() {
  local TARGET_NODE="$1"
  ssh_run "ldd --version 2>/dev/null | awk 'NR==1{print \$NF; exit}'" 2>/dev/null || true
}

# Returns 0 if remote GLIBC is at least as new as local, non-zero otherwise.
remote_can_run_local_install() {
  local TARGET_NODE="$1"
  local lv rv
  lv="$(local_glibc_version)"
  rv="$(remote_glibc_version "$TARGET_NODE")"
  [[ -z "$lv" || -z "$rv" ]] && return 0  # unknown -> assume compatible
  # Compare as dotted versions.
  printf '%s\n%s\n' "$lv" "$rv" | sort -V -C
}

single_node_can_trust_synced_install() {
  local TARGET_NODE="$1"
  # With --skip-sync there is no fresh local-install compatibility decision to
  # carry into the remote ensure step. Let the existing trust/fallback path
  # check whatever install is already present on that host.
  [[ "$SKIP_SYNC" == "1" ]] && return 0
  remote_can_run_local_install "$TARGET_NODE"
}

_sync_one_target() {
  local TARGET_NODE="$1"
  local local_install_bcdb="$2"
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
  if [[ "$local_install_bcdb" != "1" ]]; then
    log "WARNING: local install at $LOCAL_INSTALL_DIR is not BCDB-capable; skipping install sync to $TARGET_NODE to preserve remote custom install"
  elif ! remote_can_run_local_install "$TARGET_NODE"; then
    log "WARNING: $TARGET_NODE has older GLIBC than local ($(local_glibc_version) vs $(remote_glibc_version "$TARGET_NODE")); skipping install sync and letting remote rebuild from source"
    # Wipe any previously-synced (now-broken) binaries so the remote
    # ensure_custom_install_from_repo.sh rebuilds from source.
    ssh_run "rm -rf '$REMOTE_INSTALL'/bin '$REMOTE_INSTALL'/lib 2>/dev/null || true"
  else
    log "Local install looks BCDB-capable; syncing install tree to $TARGET_NODE"
    rsync_to_target --delete "$LOCAL_INSTALL_DIR/" "$TARGET_NODE:$REMOTE_INSTALL/"
  fi
  rsync_to_target "$TEMPLATE_CONF_LOCAL" "$TARGET_NODE:$REMOTE_REPO/.bench_tmp/shared_postgresql.conf"
}

sync_single_target() {
  [[ "$FULL_ONLY" == "1" ]] && { log "Single-node sync skipped (--full-only)"; return; }
  [[ "$SKIP_SYNC" == "1" ]] && { log "Single-node sync skipped"; return; }
  # Evaluate the local install's BCDB capability ONCE here, then pass the
  # cached answer to each parallel subshell. Running the check 4× concurrently
  # used to race (one subshell's pipe SIGPIPE'd postgres under pipefail, so one
  # target arbitrarily decided "not BCDB-capable" and skipped its install sync).
  local local_install_bcdb=0
  if local_install_has_bcdb_gucs; then
    local_install_bcdb=1
  fi
  log "Syncing source/install to ${#TARGETS_ARR[@]} target(s) in parallel: ${TARGETS_ARR[*]} (local_install_bcdb=$local_install_bcdb)"
  local pids=() rc=0
  for tgt in "${TARGETS_ARR[@]}"; do
    ( _sync_one_target "$tgt" "$local_install_bcdb" ) &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      log "WARNING: sync subprocess pid=$pid failed"
      rc=1
    fi
  done
  if [[ "$rc" != "0" ]]; then
    log "WARNING: one or more sync targets failed; per-node single bench will skip nodes that did not sync"
  fi
}

_run_single_node_one() {
  local TARGET_NODE="$1"
  local TARGET_MACHINE_LABEL="$2"
  local SINGLE_LOCAL_DIR
  SINGLE_LOCAL_DIR="$(single_dir_for_label "$TARGET_MACHINE_LABEL")"
  [[ "$FULL_ONLY" == "1" ]] && return
  log "Running single-node PG/DET benchmark on $TARGET_NODE (label=$TARGET_MACHINE_LABEL workload=$WORKLOAD)"
  local remote_out="$REMOTE_REPO/scripts/bench_results/ycsb_skew_compare_${ts}_${WORKLOAD_SLUG}"
  local remote_log="$REMOTE_REPO/.bench_tmp/ycsb_skew_compare_single_${ts}_${WORKLOAD_SLUG}.log"
  local log_file="$RUN_LOG_DIR/single_node_${TARGET_MACHINE_LABEL//./_}_${WORKLOAD_SLUG}.log"
  local no_resume_arg=""
  local single_extra_gucs="bcdb_serial_gate_mode=$SINGLE_BCDB_SERIAL_GATE_MODE,bcdb_serial_gate_source=$SINGLE_BCDB_SERIAL_GATE_SOURCE,bcdb_advance_commit_watermark=$SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK,bcdb_dt_completion_only_skip_reads=$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS,bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD,bcdb_result_ring_slots=$SINGLE_BCDB_RESULT_RING_SLOTS"
  local trust_synced_install=0
  [[ "$NO_RESUME" == "1" ]] && no_resume_arg="--no-resume"
  if [[ -n "${SINGLE_BCDB_EXTRA_GUCS:-}" ]]; then
    single_extra_gucs="$SINGLE_BCDB_EXTRA_GUCS,$single_extra_gucs"
  fi
  if single_node_can_trust_synced_install "$TARGET_NODE"; then
    trust_synced_install=1
  fi

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO/scripts'
if [[ '$trust_synced_install' == '1' ]]; then
  if ! bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
       --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild --trust-install; then
    echo \"[INFO] --trust-install failed on this host; retrying with a source rebuild\" >&2
    bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
  fi
else
  echo \"[INFO] remote cannot consume the freshly synced local install; ensuring an on-host build\" >&2
  bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
fi
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export BCDB_EXTRA_GUCS='$single_extra_gucs'
export BCDB_BLOCK_RETURN_ACTUAL_RESULTS='$SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS'
export BCDB_POLL_MAX_US='$SINGLE_BCDB_POLL_MAX_US'
export BCDB_DT_PARSE_BARRIER='$SINGLE_BCDB_DT_PARSE_BARRIER'
export BCDB_DT_LIGHT_SNAPSHOT='$SINGLE_BCDB_DT_LIGHT_SNAPSHOT'
export BCDB_DT_SKIP_READONLY_GATE='$SINGLE_BCDB_DT_SKIP_READONLY_GATE'
export BCDB_FLOW_DEBUG='$SINGLE_BCDB_FLOW_DEBUG'
export BCDB_GATE_DEBUG='$SINGLE_BCDB_GATE_DEBUG'
export BCDB_APPLY_WAIT_DEBUG='$SINGLE_BCDB_APPLY_WAIT_DEBUG'
if [[ -n '$SINGLE_BCDB_PHASE_TRACE_PREFIX' ]]; then
  export BCDB_PHASE_TRACE='$remote_out/$SINGLE_BCDB_PHASE_TRACE_PREFIX'
fi
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
export ARIABC_PSYCOPG_CLIENT_CURSOR=1
echo \"SINGLE_NODE_BCDB_KNOBS=serial_gate_mode=$SINGLE_BCDB_SERIAL_GATE_MODE serial_gate_source=$SINGLE_BCDB_SERIAL_GATE_SOURCE advance_commit_watermark=$SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK completion_only_skip_reads=$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS result_ring_slots=$SINGLE_BCDB_RESULT_RING_SLOTS block_return_actual_results=$SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS poll_max_us=$SINGLE_BCDB_POLL_MAX_US dt_parse_barrier=$SINGLE_BCDB_DT_PARSE_BARRIER dt_light_snapshot=$SINGLE_BCDB_DT_LIGHT_SNAPSHOT dt_skip_readonly_gate=$SINGLE_BCDB_DT_SKIP_READONLY_GATE flow_debug=$SINGLE_BCDB_FLOW_DEBUG gate_debug=$SINGLE_BCDB_GATE_DEBUG apply_wait_debug=$SINGLE_BCDB_APPLY_WAIT_DEBUG timeout_workload_s=$SINGLE_TIMEOUT_WORKLOAD_S timeout_workload_det_s=$SINGLE_TIMEOUT_WORKLOAD_DET_S phase_trace_prefix=$SINGLE_BCDB_PHASE_TRACE_PREFIX extra_gucs=$single_extra_gucs\"
if [[ '$SINGLE_REMOTE_KILL_STALE' == '1' ]]; then
  for pid in \$(pgrep -f '$REMOTE_REPO/scripts/generic-saicopg-traffic-load+logSkip-safedb+pg.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -TERM \"\$pid\" 2>/dev/null || true
  done
  for pid in \$(pgrep -f 'python3 -u bench_threads_matrix.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -TERM \"\$pid\" 2>/dev/null || true
  done
  sleep 1
  for pid in \$(pgrep -f '$REMOTE_REPO/scripts/generic-saicopg-traffic-load+logSkip-safedb+pg.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -KILL \"\$pid\" 2>/dev/null || true
  done
  for pid in \$(pgrep -f 'python3 -u bench_threads_matrix.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -KILL \"\$pid\" 2>/dev/null || true
  done
fi
PYTHON_BIN=''
if [[ -n '$SINGLE_PYTHON_BIN' ]]; then
  if [[ -x '$SINGLE_PYTHON_BIN' ]] && '$SINGLE_PYTHON_BIN' -c 'import psycopg' >/dev/null 2>&1; then
    PYTHON_BIN='$SINGLE_PYTHON_BIN'
  else
    echo \"ERROR: SINGLE_PYTHON_BIN=$SINGLE_PYTHON_BIN is not executable or cannot import psycopg\" >&2
    exit 1
  fi
elif [[ -x '$REMOTE_REPO/.venv/bin/python3' ]] && '$REMOTE_REPO/.venv/bin/python3' -c 'import psycopg' >/dev/null 2>&1; then
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
pip_install() {
  # Bounded so a slow/hung PyPI fetch can't stall the whole bench.
  timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check \"\$@\" >/dev/null 2>&1 \
    || timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check --user \"\$@\" >/dev/null 2>&1 \
    || timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check --break-system-packages \"\$@\" >/dev/null 2>&1
}
if ! \$PYTHON_BIN -c 'import psycopg' >/dev/null 2>&1; then
  pip_install 'psycopg[binary]' psycopg || echo \"WARNING: failed to install psycopg into \$PYTHON_BIN\" >&2
fi
# Note: matplotlib intentionally not installed here. The runner generates the
# per-machine TPS-vs-threads graphs locally after collecting summary.csv, so
# the remote does not need matplotlib. A previous version pip-installed it on
# every node in parallel which sometimes hung indefinitely on slow PyPI mirrors.
echo \"ARIABC_PYTHON=\$ARIABC_PYTHON\"
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes pg,det \
  --signing-modes 0 \
  --enforce-signatures 0 \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --workloads '$WORKLOAD' \
  --timeout-workload-s '$SINGLE_TIMEOUT_WORKLOAD_S' \
  --timeout-workload-det-s '$SINGLE_TIMEOUT_WORKLOAD_DET_S' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out' $no_resume_arg
")" > "$log_file" 2>&1

  log "Collecting single-node results from $TARGET_NODE:$remote_out"
  rsync_to_target "$TARGET_NODE:$remote_out/" "$SINGLE_LOCAL_DIR/"
}

generate_local_per_machine_graph() {
  local single_dir="$1"
  local summary="$single_dir/summary.csv"
  if [[ ! -f "$summary" ]]; then
    log "WARNING: skipping local graph for $single_dir (no summary.csv)"
    return
  fi
  log "Generating local per-machine graph from $summary"
  MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" python3 - "$REPO_ROOT" "$summary" "$single_dir" <<'PYEOF' \
    >> "$RUN_LOG_DIR/local_graphs.log" 2>&1 \
    || log "WARNING: local graph generation failed for $single_dir (see $RUN_LOG_DIR/local_graphs.log)"
import sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / "scripts"))
from bench_threads_matrix import _generate_tps_graphs
out = _generate_tps_graphs(Path(sys.argv[2]), Path(sys.argv[3]))
for p in out:
    print("wrote", p)
PYEOF
}

run_single_node() {
  [[ "$FULL_ONLY" == "1" ]] && return
  log "Launching single-node bench in parallel across ${#TARGETS_ARR[@]} target(s): ${TARGETS_ARR[*]}"
  local pids=() pid_targets=() pid_labels=()
  for i in "${!TARGETS_ARR[@]}"; do
    local tgt="${TARGETS_ARR[$i]}"
    local lbl="${LABELS_ARR[$i]}"
    ( _run_single_node_one "$tgt" "$lbl" ) &
    pids+=($!)
    pid_targets+=("$tgt")
    pid_labels+=("$lbl")
  done
  for j in "${!pids[@]}"; do
    if wait "${pids[$j]}"; then
      log "Single-node bench succeeded on ${pid_targets[$j]} (label=${pid_labels[$j]})"
    else
      log "WARNING: single-node bench failed on ${pid_targets[$j]} (label=${pid_labels[$j]}); see $RUN_LOG_DIR/single_node_${pid_labels[$j]//./_}_${WORKLOAD_SLUG}.log"
    fi
  done
  for lbl in "${LABELS_ARR[@]}"; do
    generate_local_per_machine_graph "$(single_dir_for_label "$lbl")"
  done
}

pick_third_fastest_node() {
  local out
  out="$(python3 - "$OUT_DIR" "${LABELS_ARR[@]}" "--" "${TARGETS_ARR[@]}" <<'PYEOF' 2> >(tee -a "$RUN_LOG_DIR/third_fastest_pick.log" >&2)
import csv, os, sys
out_dir = sys.argv[1]
sep = sys.argv.index("--", 2)
labels = sys.argv[2:sep]
targets = sys.argv[sep+1:]
results = []
for label, target in zip(labels, targets):
    summary = os.path.join(out_dir, f"single_{label}", "summary.csv")
    if not os.path.exists(summary):
        print(f"  miss: {summary}", file=sys.stderr)
        continue
    peak = 0.0
    with open(summary, newline='') as f:
        for row in csv.DictReader(f):
            if (row.get('mode') or '').strip().lower() != 'det':
                continue
            try:
                tps = float(row.get('median_throughput_tps') or 0)
            except (ValueError, TypeError):
                continue
            if tps > peak:
                peak = tps
    if peak > 0:
        results.append((peak, label, target))
# Rank by peak DET median TPS descending; pick the 3rd entry. If fewer than 3
# nodes produced data, fall back to the slowest one we have.
results.sort(reverse=True)
if not results:
    sys.exit(1)
print("RANKING:", file=sys.stderr)
for r in results:
    print(f"  peak_det_tps={r[0]:.2f}  label={r[1]}  target={r[2]}", file=sys.stderr)
idx = min(2, len(results) - 1)
peak, label, target = results[idx]
print(f"{label}\t{target}\t{peak:.2f}")
PYEOF
  )"
  if [[ -z "$out" ]]; then
    die "could not pick 3rd-fastest node — no successful single-node summary.csv files under $OUT_DIR"
  fi
  IFS=$'\t' read -r THIRD_FASTEST_LABEL THIRD_FASTEST_NODE THIRD_FASTEST_TPS <<< "$out"
  log "3rd-fastest node selected: $THIRD_FASTEST_NODE (label=$THIRD_FASTEST_LABEL, peak DET TPS=$THIRD_FASTEST_TPS)"
  # Reassign the globals used by run_single_gateway_direct and generate_outputs
  TARGET_NODE="$THIRD_FASTEST_NODE"
  TARGET_MACHINE_LABEL="$THIRD_FASTEST_LABEL"
  SINGLE_LOCAL_DIR="$(single_dir_for_label "$THIRD_FASTEST_LABEL")"
}

run_single_gateway_direct() {
  [[ "$FULL_ONLY" == "1" ]] && return
  [[ "$SINGLE_ONLY" == "1" ]] && return
  [[ "$SINGLE_GATEWAY_DIRECT" == "1" ]] || { log "Single-node gateway-direct baseline skipped (SINGLE_GATEWAY_DIRECT=$SINGLE_GATEWAY_DIRECT)"; return; }

  append_single_gateway_manifest_header
  log "Running single-node gateway-direct baseline on $TARGET_NODE (workload=$WORKLOAD)"

  local remote_out="$REMOTE_REPO/scripts/bench_results/ycsb_skew_gateway_direct_${ts}_${WORKLOAD_SLUG}"
  local log_file="$RUN_LOG_DIR/single_gateway_direct_${WORKLOAD_SLUG}.log"
  local trust_synced_install=0
  : > "$log_file"
  if single_node_can_trust_synced_install "$TARGET_NODE"; then
    trust_synced_install=1
  fi

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO'
if [[ '$trust_synced_install' == '1' ]]; then
  if ! bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
       --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild --trust-install; then
    echo \"[INFO] --trust-install failed on this host; retrying with a source rebuild\" >&2
    bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
  fi
else
  echo \"[INFO] remote cannot consume the freshly synced local install; ensuring an on-host build\" >&2
  bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
fi
srv_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_server'
gw_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_gateway'
if [[ ! -x \"\$srv_bin\" || ! -x \"\$gw_bin\" ]]; then
  if command -v cmake >/dev/null 2>&1 && [[ -d '$REMOTE_REPO/ariabc_pg/build' ]]; then
    cmake --build '$REMOTE_REPO/ariabc_pg/build' --target ariabc_pg_server ariabc_pg_gateway -j\$(nproc)
  fi
fi
[[ -x \"\$srv_bin\" ]] || { echo \"ERROR: missing ariabc_pg_server at \$srv_bin\" >&2; exit 1; }
[[ -x \"\$gw_bin\" ]] || { echo \"ERROR: missing ariabc_pg_gateway at \$gw_bin\" >&2; exit 1; }
")" >> "$log_file" 2>&1

  IFS=',' read -ra thread_arr <<< "$THREADS"
  for th in "${thread_arr[@]}"; do
    th="${th//[[:space:]]/}"
    [[ -z "$th" ]] && continue
    for run in $(seq 1 "$RUNS"); do
      # Match cluster_kafka's pipeline depth: the gateway-direct baseline is
      # the apples-to-apples reference for cluster_kafka minus replication, so
      # it must use the same per-terminal pipeline depth (kafka-only's depth)
      # — not raft-kafka's. Without this match, single-node-gateway-direct was
      # artificially capped below cluster_kafka and the graph ordering looked
      # wrong.
      select_full_case_params "kafka-only" "$th"
      local gateway_pool_size="${SINGLE_GATEWAY_POOL_SIZE:-$SELECTED_POOL_SIZE}"
      # Keep worker_count at the cluster's value (512). Bumping it higher
      # explodes the per-process open-file count and tripped
      #   bcdb_init skipped: Cannot start worker: could not create socket:
      #   Too many open files
      # which silently falls back to the serialized det compat path and caps
      # TPS at ~2.7k. Stay within ulimit -n.
      local gateway_worker_count="${SINGLE_GATEWAY_BCDB_WORKER_COUNT:-$SELECTED_BCDB_WORKER_COUNT}"
      local gateway_num_terminals="$SELECTED_NUM_TERMINALS"
      # Use kafka-only's pipeline depth directly (128). Effective in-flight
      # then scales linearly with numTerminals (128, 256, 512, 1024, ...).
      # Without the BCDB_DET_QUEUE_HIGH_WM bump exported below, the server
      # would stall on admission control above the formula default
      # (2 * conn_pool_size = 512) — capping single-node throughput.
      local gateway_det_pipeline_depth="$SELECTED_DET_PIPELINE_DEPTH"
      local gateway_det_batch_size="$SELECTED_DET_BATCH_SIZE"
      local gateway_det_window=$(( gateway_num_terminals * gateway_det_pipeline_depth ))
      local gateway_effective_inflight="$gateway_det_window"
      local req_id_offset=$(( 500000000 + (run * 1000000) + (th * 10000) + 1 ))
      local client_port=$(( SINGLE_GATEWAY_CLIENT_PORT_BASE + (th * 20) + run ))
      local raft_port=$(( SINGLE_GATEWAY_RAFT_PORT_BASE + (th * 20) + run ))
      local required_max_connections=$(( gateway_pool_size * 3 + 64 ))
      local worker_required_max_connections=$(( gateway_worker_count + gateway_pool_size + 64 ))
      if [[ "$required_max_connections" -lt "$worker_required_max_connections" ]]; then
        required_max_connections="$worker_required_max_connections"
      fi
      if [[ "$required_max_connections" -lt 256 ]]; then
        required_max_connections=256
      fi
      local completion_only_skip_reads="off"
      if [[ "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" == "1" ]]; then
        completion_only_skip_reads="on"
      fi

      local remote_case="$remote_out/thread_${th}_run_${run}"
      local local_case="$SINGLE_GATEWAY_LOCAL_DIR/thread_${th}_run_${run}"
      mkdir -p "$local_case"
      log "Single gateway-direct case thread=$th run=$run experiment=$EXPERIMENT_MODE (num-terminals=$gateway_num_terminals det-pipeline-depth=$gateway_det_pipeline_depth effective-inflight=$gateway_effective_inflight pool-size=$gateway_pool_size worker-count=$gateway_worker_count det-batch=$gateway_det_batch_size det-window=$gateway_det_window completion_path=direct)"

      set +e
      ssh_run "timeout '$SINGLE_GATEWAY_CASE_TIMEOUT_S' bash -lc $(printf '%q' "
set -euo pipefail
case_dir='$remote_case'
mkdir -p \"\$case_dir\"
cd '$REMOTE_REPO'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
# Decouple BCDB worker queue count from bcdb_init's block_size argument.
# Must be exported BEFORE postgres (re)starts so it's in postgres's env.
# Without this, bcdb_init() rejects with
#   \"bcdb_worker_count mismatch: existing=512 requested=256; restart required\"
# and the server falls back to the serialized det compat path — capping
# single-node-gateway-direct at ~2.7k TPS regardless of fanout, threads, or
# pipeline depth. This mirrors the cluster runner's BCDB_DECOUPLE_WORKERS=1.
export BCDB_DECOUPLE_WORKERS='$FULL_BCDB_DECOUPLE_WORKERS'
# Raise the BCDB det-mode admission-control watermarks for the single-node
# gateway-direct backend so it can absorb the full gateway pipeline
# (numTerminals × detPipelineDepth × detBatchSize outstanding txs) without
# tripping append_stall and capping throughput. The cluster spreads load
# across 4 servers; the single-node case must scale watermarks up to match.
export BCDB_DET_QUEUE_HIGH_WM='${SINGLE_BCDB_DET_QUEUE_HIGH_WM:-4096}'
export BCDB_DET_QUEUE_LOW_WM='${SINGLE_BCDB_DET_QUEUE_LOW_WM:-2048}'
pgdata_line=\$(bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] || { echo \"bad PGDATA line: \$pgdata_line\" >&2; exit 1; }
PGDATA=\${pgdata_line#PGDATA=}
psql_bin='$REMOTE_INSTALL/bin/psql'
pg_ctl_bin='$REMOTE_INSTALL/bin/pg_ctl'
srv_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_server'
gw_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_gateway'

\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_worker_count = '$gateway_worker_count';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_mode = '$FULL_BCDB_SERIAL_GATE_MODE';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_source = '$FULL_BCDB_SERIAL_GATE_SOURCE';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_conflict_tracking = 'on';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_completion_only_skip_reads = '$completion_only_skip_reads';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_hashtab_switch_threshold = '$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_result_ring_slots = '$RESULT_RING_CAPACITY';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET max_connections = '$required_max_connections';\"
\"\$pg_ctl_bin\" -D \"\$PGDATA\" -w -t 120 restart -l \"\$case_dir/postgres.log\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"DO \\\$\\\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'neel') THEN CREATE ROLE neel LOGIN SUPERUSER; END IF; END \\\$\\\$;\"

\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -f '$REMOTE_REPO/scripts/restore_usertable_small.sql' >\"\$case_dir/restore.log\" 2>&1
if command -v fuser >/dev/null 2>&1; then
  fuser -k -TERM '${client_port}/tcp' '${raft_port}/tcp' >/dev/null 2>&1 || true
  sleep 1
  fuser -k '${client_port}/tcp' '${raft_port}/tcp' >/dev/null 2>&1 || true
fi

export ARIABC_PROFILE=1
export ARIABC_DET_BLOCK_PARALLEL='$FULL_DET_BLOCK_PARALLEL'
export ARIABC_DET_BLOCK_PIPELINE='$FULL_DET_BLOCK_PIPELINE'
export ARIABC_DET_BLOCK_MAX='$FULL_DET_BLOCK_MAX'
export ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US='$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US'
export ARIABC_DET_BLOCK_SKIP_READONLY='$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS'
export ARIABC_FULL_RESULT_REPLICA_LIMIT='$FULL_RESULT_REPLICA_LIMIT'
export BCDB_DT_COMPLETION_ONLY_SKIP_READS='$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS'
nohup \"\$srv_bin\" \
  --id 1 \
  --raftEndpoint 127.0.0.1:'$raft_port' \
  --clientPort '$client_port' \
  --raftMembers '1=127.0.0.1:$raft_port' \
  --dbName '$DB_NAME' \
  --dbHost 127.0.0.1 \
  --dbPort '$DB_PORT' \
  --dbUser '$DB_USER' \
  --dbType 1 \
  --safedb 1 \
  --dbConnPoolSize '$gateway_pool_size' \
  --pgExecMode event \
  --bypassRaft 1 \
  >\"\$case_dir/server.log\" 2>&1 &
srv_pid=\$!
cleanup() {
  kill -TERM \"\$srv_pid\" >/dev/null 2>&1 || true
  wait \"\$srv_pid\" >/dev/null 2>&1 || true
}
trap cleanup EXIT

ready=0
for attempt in \$(seq 1 60); do
  if ss -tlnp 2>/dev/null | grep -q ':$client_port'; then
    ready=1
    break
  fi
  if ! kill -0 \"\$srv_pid\" >/dev/null 2>&1; then
    cat \"\$case_dir/server.log\" >&2 || true
    exit 1
  fi
  sleep 1
done
if [[ \"\$ready\" -ne 1 ]]; then
  cat \"\$case_dir/server.log\" >&2 || true
  echo 'server did not open client port' >&2
  exit 1
fi

printf 'SELECT 1;\n' >\"\$case_dir/precheck.sql\"
\"\$gw_bin\" \
  --nodes '127.0.0.1:$client_port' \
  --queryFrom \"\$case_dir/precheck.sql\" \
  --dbType 1 \
  --detStartSeq 99000000 \
  --reqIdOffset 99000000 \
  --detWindow 1 \
  --detBatchSize 1 \
  --dbConnPoolSize '$gateway_pool_size' \
  --submitMode blocking \
  --detSubmitPipeline 0 \
  --detPipelineDepth 1 \
  --clientId 'single-gateway-direct-probe' \
  --numTerminals 1 \
  --waitMajority 0 \
  --completionPath direct \
  --totalNodes 1 \
  >\"\$case_dir/precheck.log\" 2>&1

set +e
\"\$gw_bin\" \
  --nodes '127.0.0.1:$client_port' \
  --queryFrom '$REMOTE_REPO/scripts/$WORKLOAD' \
  --dbType 1 \
  --detStartSeq 1 \
  --reqIdOffset '$req_id_offset' \
  --detWindow '$gateway_det_window' \
  --detBatchSize '$gateway_det_batch_size' \
  --dbConnPoolSize '$gateway_pool_size' \
  --submitMode event \
  --detSubmitPipeline 1 \
  --detPipelineDepth '$gateway_det_pipeline_depth' \
  --pollCount '$POLL_COUNT' \
  --clientId 'single-gateway-direct' \
  --numTerminals '$gateway_num_terminals' \
  --waitMajority 0 \
  --completionPath direct \
  --totalNodes 1 \
  --connFanout '$gateway_num_terminals' \
  2>&1 | tee \"\$case_dir/gateway_test.log\"
gw_rc=\${PIPESTATUS[0]}
set -e

cnt=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc 'SELECT count(*) FROM usertable_small' | tr -d '[:space:]')
root=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"SELECT merkle_root_hash('usertable_small')\" | tr -d '[:space:]')
verify=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"SELECT merkle_verify('usertable_small')\" | tr -d '[:space:]')
{
  echo \"rows=\$cnt\"
  echo \"root=\$root\"
  echo \"verify=\$verify\"
  echo \"pool_size=$gateway_pool_size\"
  echo \"bcdb_worker_count=$gateway_worker_count\"
  echo \"max_connections=$required_max_connections\"
} >\"\$case_dir/verify.txt\"

if [[ \"\$gw_rc\" -ne 0 ]]; then
  exit \"\$gw_rc\"
fi
if [[ \"\$verify\" != 't' ]]; then
  echo \"merkle_verify failed: \$verify\" >&2
  exit 1
fi
")" >> "$log_file" 2>&1
      rc=$?
      set -e

      set +e
      rsync_to_target "$TARGET_NODE:$remote_case/" "$local_case/"
      rsync_rc=$?
      set -e
      if [[ "$rsync_rc" != "0" ]]; then
        log "WARNING: failed to collect single gateway-direct artifact thread=$th run=$run rsync_rc=$rsync_rc"
      fi

      notes="single_node_gateway_direct;completion_path=direct;experiment_mode=$EXPERIMENT_MODE;thread_knob=$FULL_THREAD_KNOB;num_terminals=$gateway_num_terminals;det_pipeline_depth=$gateway_det_pipeline_depth;effective_inflight=$gateway_effective_inflight;pool_size=$gateway_pool_size;bcdb_worker_count=$gateway_worker_count;det_block_parallel=$FULL_DET_BLOCK_PARALLEL;det_block_pipeline=$FULL_DET_BLOCK_PIPELINE;det_block_max=$FULL_DET_BLOCK_MAX;bcdb_serial_gate_mode=$FULL_BCDB_SERIAL_GATE_MODE;bcdb_serial_gate_source=$FULL_BCDB_SERIAL_GATE_SOURCE;bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD;result_ring_capacity=$RESULT_RING_CAPACITY;remote_case=$remote_case"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "single_node_gateway_direct" "gateway_direct_bcdb" "$EXPERIMENT_MODE" "direct" "single_node_gateway" "direct" "1" "0" "$th" "$run" "$local_case" "$rc" "$FULL_THREAD_KNOB" "$gateway_pool_size" "$gateway_worker_count" "$gateway_det_batch_size" "$gateway_det_window" "$gateway_num_terminals" "$gateway_det_pipeline_depth" "$gateway_effective_inflight" "$FULL_DET_BLOCK_PIPELINE" "$FULL_DET_BLOCK_MAX" "$req_id_offset" "completed_or_accepted_or_loaded" "$notes" \
        >> "$SINGLE_GATEWAY_MANIFEST"

      if [[ "$rc" != "0" ]]; then
        die "single gateway-direct case failed (thread=$th run=$run rc=$rc); see $log_file and $local_case"
      fi
      if [[ "$rsync_rc" != "0" ]]; then
        die "single gateway-direct artifact collection failed (thread=$th run=$run rsync_rc=$rsync_rc)"
      fi
    done
  done
}

FULL_SYNC_FIRST=1

run_full_system() {
  [[ "$SINGLE_ONLY" == "1" ]] && return
  append_full_manifest_header
  log "Running cluster sweep (modes=$FULL_CLUSTER_MODES workload=$WORKLOAD)"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      log "Full-system x-axis mapping: thread value -> --num-terminals; detPipelineDepth=$FULL_DET_PIPELINE_DEPTH per terminal (forced for every mode)"
    else
      log "Full-system x-axis mapping: thread value -> --num-terminals; detPipelineDepth by mode: kafka-only=$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY kafka-only-map=${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP:-none} raft-kafka=$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA raft-kafka-map=${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP:-none}"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    log "Full-system x-axis mapping: thread labels with fixed detWindow=$FULL_DET_WINDOW; --num-terminals remains 1"
  else
    log "Full-system x-axis mapping: thread value -> ordered concurrency budget; --num-terminals remains 1"
  fi

  IFS=',' read -ra full_mode_arr <<< "$FULL_CLUSTER_MODES"
  IFS=',' read -ra thread_arr <<< "$THREADS"
  for requested_mode in "${full_mode_arr[@]}"; do
    requested_mode="${requested_mode//[[:space:]]/}"
    [[ -z "$requested_mode" ]] && continue
    cluster_mode="$(normalize_cluster_mode "$requested_mode")" || die "unsupported cluster mode: $requested_mode"
    if [[ "$cluster_mode" == "kafka-only" ]]; then
      cluster_series="cluster_kafka"
      cluster_mode_label="kafka_only_bcdb"
      ordering_path="preordered_direct_broadcast"
      server_bypass_raft=1
      gateway_broadcast_to_all=1
    else
      cluster_series="cluster_raft_kafka"
      cluster_mode_label="kafka_raft_bcdb"
      ordering_path="raft"
      server_bypass_raft=0
      gateway_broadcast_to_all=0
    fi
    log "Cluster mode: $cluster_mode (series=$cluster_series ordering_path=$ordering_path)"
    for th in "${thread_arr[@]}"; do
      th="${th//[[:space:]]/}"
      [[ -z "$th" ]] && continue
      for run in $(seq 1 "$RUNS"); do
      select_full_case_params "$cluster_mode" "$th"
      full_pool_size="$SELECTED_POOL_SIZE"
      full_num_terminals="$SELECTED_NUM_TERMINALS"
      full_det_pipeline_depth="$SELECTED_DET_PIPELINE_DEPTH"
      full_det_batch_size="$SELECTED_DET_BATCH_SIZE"
      full_det_window="$SELECTED_DET_WINDOW"
      full_effective_inflight="$SELECTED_EFFECTIVE_INFLIGHT"
      full_bcdb_worker_count="$SELECTED_BCDB_WORKER_COUNT"
      req_id_offset=$(( (run * 1000000) + (th * 10000) + 1 ))
      log "Cluster case mode=$cluster_mode thread=$th run=$run experiment=$EXPERIMENT_MODE (num-terminals=$full_num_terminals det-pipeline-depth=$full_det_pipeline_depth effective-inflight=$full_effective_inflight pool-size=$full_pool_size worker-count=$full_bcdb_worker_count det-batch=$full_det_batch_size det-window=$full_det_window det-block-parallel=$FULL_DET_BLOCK_PARALLEL det-block-pipeline=$FULL_DET_BLOCK_PIPELINE det-block-max=$FULL_DET_BLOCK_MAX det-partial-block-max-wait-us=$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US serial-gate=$FULL_BCDB_SERIAL_GATE_MODE serial-gate-source=$FULL_BCDB_SERIAL_GATE_SOURCE dt-parse-barrier=$FULL_BCDB_DT_PARSE_BARRIER completion-only-skip-reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS full-result-replica-limit=$FULL_RESULT_REPLICA_LIMIT ordering_path=$ordering_path)"
      before_file="$RUN_LOG_DIR/full_${cluster_mode}_before_${th}_${run}.txt"
      after_file="$RUN_LOG_DIR/full_${cluster_mode}_after_${th}_${run}.txt"
      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$before_file" || true

      extra_skip=()
      [[ "$FULL_SKIP_SYNC" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-sync)
      [[ "$FULL_SKIP_BUILD" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-build)
      [[ "$FULL_SKIP_RDKAFKA_SETUP" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-rdkafka-setup)

      set +e
      timeout "$FULL_CASE_TIMEOUT_S" env POLL_COUNT="$POLL_COUNT" RESULT_RING_CAPACITY="$RESULT_RING_CAPACITY" \
      "$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh" \
        "${extra_skip[@]}" \
        --ordering-mode "$cluster_mode" \
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
        --det-partial-block-max-wait-us "$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US" \
        --num-terminals "$full_num_terminals" \
        --det-pipeline-depth "$full_det_pipeline_depth" \
        --bcdb-block-profile "$FULL_BCDB_BLOCK_PROFILE" \
        --bcdb-block-wait-watermark "$FULL_BCDB_BLOCK_WAIT_WATERMARK" \
        --bcdb-serial-gate-mode "$FULL_BCDB_SERIAL_GATE_MODE" \
        --bcdb-serial-gate-source "$FULL_BCDB_SERIAL_GATE_SOURCE" \
        --bcdb-dt-parse-barrier "$FULL_BCDB_DT_PARSE_BARRIER" \
        --bcdb-dt-skip-readonly-gate "$FULL_BCDB_DT_SKIP_READONLY_GATE" \
        --bcdb-dt-completion-only-skip-reads "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" \
        --bcdb-dt-hashtab-switch-threshold "$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD" \
        --full-result-replica-limit "$FULL_RESULT_REPLICA_LIMIT" \
        > "$RUN_LOG_DIR/full_${cluster_mode}_thread_${th}_run_${run}.log" 2>&1
      rc=$?
      set -e
      FULL_SYNC_FIRST=0

      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$after_file" || true
      artifact="$(grep -vxF -f "$before_file" "$after_file" | head -n 1 || true)"
      if [[ -z "$artifact" ]]; then
        artifact="$(head -n 1 "$after_file" || true)"
      fi
      [[ -n "$artifact" ]] || artifact="$RUN_LOG_DIR/missing_full_artifact_thread_${th}_run_${run}"
      notes="cluster_mode=$cluster_mode;ordering_path=$ordering_path;completion_path=kafka_majority;experiment_mode=$EXPERIMENT_MODE;full_system_thread_knob=$FULL_THREAD_KNOB;full_pool_size_mode=$FULL_POOL_SIZE_MODE;num_terminals=$full_num_terminals;det_pipeline_depth=$full_det_pipeline_depth;effective_inflight=$full_effective_inflight;trusted_gate=kafka_majority_merkle;pool_size_min=2;det_window_multiplier=$FULL_DET_WINDOW_MULTIPLIER;det_window_max=$FULL_DET_WINDOW_MAX;det_window_map=$FULL_DET_WINDOW_MAP;det_batch_size_map=$FULL_DET_BATCH_SIZE_MAP;det_block_parallel=$FULL_DET_BLOCK_PARALLEL;det_block_pipeline=$FULL_DET_BLOCK_PIPELINE;det_block_max=$FULL_DET_BLOCK_MAX;det_partial_block_max_wait_us=$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US;bcdb_block_wait_watermark=$FULL_BCDB_BLOCK_WAIT_WATERMARK;bcdb_serial_gate_mode=$FULL_BCDB_SERIAL_GATE_MODE;bcdb_serial_gate_source=$FULL_BCDB_SERIAL_GATE_SOURCE;bcdb_dt_parse_barrier=$FULL_BCDB_DT_PARSE_BARRIER;bcdb_dt_skip_readonly_gate=$FULL_BCDB_DT_SKIP_READONLY_GATE;bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;det_block_skip_readonly=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD;full_result_replica_limit=$FULL_RESULT_REPLICA_LIMIT;backend_capacity=normalized"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$cluster_series" "$cluster_mode_label" "$EXPERIMENT_MODE" "$cluster_mode" "$ordering_path" "kafka_majority" "$server_bypass_raft" "$gateway_broadcast_to_all" "$th" "$run" "$artifact" "$rc" "$FULL_THREAD_KNOB" "$full_pool_size" "$full_bcdb_worker_count" "$full_det_batch_size" "$full_det_window" "$full_num_terminals" "$full_det_pipeline_depth" "$full_effective_inflight" "$FULL_DET_BLOCK_PIPELINE" "$FULL_DET_BLOCK_MAX" "$req_id_offset" "completed_or_accepted_or_loaded" "$notes" \
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
  single_gateway_args=()
  if [[ -f "$SINGLE_GATEWAY_MANIFEST" ]]; then
    single_gateway_args=(--single-gateway-manifest "$SINGLE_GATEWAY_MANIFEST")
  fi
  local x_label="Threads"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      x_label="Single-node client threads / full-system numTerminals (detPipelineDepth=$FULL_DET_PIPELINE_DEPTH, experiment=$EXPERIMENT_MODE)"
    else
      x_label="Single-node client threads / full-system numTerminals (mode-specific detPipelineDepth, experiment=$EXPERIMENT_MODE)"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "concurrency" ]]; then
    x_label="Single-node client threads / full-system ordered concurrency budget"
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    x_label="Single-node client threads / full-system labeled points (fixed detWindow=$FULL_DET_WINDOW)"
  fi
  MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" \
    python3 "$SCRIPT_DIR/plot_ycsb_skew_tps_comparison.py" \
      --single-results "$single_results" \
      "${single_gateway_args[@]}" \
      --full-manifest "$FULL_MANIFEST" \
      --out-dir "$OUT_DIR" \
      --workload "$WORKLOAD" \
      --machine "$TARGET_MACHINE_LABEL" \
      --threads "$THREADS" \
      --x-label "$x_label"
}

log "=== YCSB skew TPS comparison ==="
log "Out root  : $OUT_ROOT"
log "Threads   : $THREADS"
log "Runs      : $RUNS"
log "Workloads : $WORKLOADS"
log "Targets   : ${TARGETS_ARR[*]}"
log "Cluster modes: $FULL_CLUSTER_MODES"
log "Experiment mode: $EXPERIMENT_MODE"

IFS=',' read -ra workload_arr <<< "$WORKLOADS"

if [[ "$ANALYZE_ONLY" != "1" ]]; then
  sync_single_target
fi

for wl in "${workload_arr[@]}"; do
  wl="${wl//[[:space:]]/}"
  [[ -z "$wl" ]] && continue
  set_workload_paths "$wl"
  log "--- Workload: $WORKLOAD (out=$OUT_DIR) ---"
  if [[ "$ANALYZE_ONLY" != "1" ]]; then
    run_single_node
  fi
  if [[ "$SINGLE_ONLY" == "1" ]]; then
    log "Workload single-only done: $WORKLOAD"
    for _lbl in "${LABELS_ARR[@]}"; do
      _d="$(single_dir_for_label "$_lbl")"
      log "  $_lbl: $_d/results.csv  summary=$_d/summary.csv"
    done
    continue
  fi
  if [[ "$FULL_ONLY" != "1" ]]; then
    pick_third_fastest_node
  fi
  if [[ "$ANALYZE_ONLY" != "1" ]]; then
    run_single_gateway_direct
    run_full_system
  fi
  generate_outputs
  log "Workload done: $WORKLOAD"
  log "  Results : $OUT_DIR/results.csv"
  log "  Summary : $OUT_DIR/summary.csv"
  log "  Overhead: $OUT_DIR/overhead.csv"
  log "  Graph   : $OUT_DIR/ycsb_skew_pg_vs_det.png"
  log "  Graph   : $OUT_DIR/ycsb_skew_det_vs_cluster.png"
done

log "Done"
log "Out root : $OUT_ROOT"
