#!/usr/bin/env bash
# bench_cluster_threads.sh — Multi-thread TPS scaling sweep for the 4-node AriaBC cluster.
#
# Mirrors what bench_threads_matrix.py does for the single-machine benchmark:
#   For each thread count in THREAD_COUNTS, run run_4node_raft_cluster.sh with
#   --threads N. The cluster runner maps this to N deterministic client lanes
#   and keeps a bounded per-thread pipeline so Raft/Kafka majority latency is
#   amortised instead of making every statement serial. In raft-kafka mode the
#   submit stream remains ordered by default; kafka-only can safely use fanout
#   sockets because bypass-raft servers reorder deterministic ranges.
#
# Parallelism mode (--parallelism-mode):
#   pipeline   (default) — N terminal lanes on a single gateway process; deepens
#                          the DET window.  This is pipeline-depth scaling, NOT
#                          OS-thread parallelism.  Comparable to sequential depth,
#                          not to the single-node Python benchmark's threads.
#   os-threads           — Splits the workload into N sequential shards and runs
#                          N independent gateway processes in parallel (background &).
#                          Wall time = max shard wall time.  This IS comparable to
#                          the single-node Python ThreadPoolExecutor(max_workers=N).
#
# Usage:
#   ./bench_cluster_threads.sh [options] [-- <extra run_4node args>]
#
# Options:
#   --threads "1,4,8"       Comma-separated terminal counts to sweep (default: 1,4,8)
#   --parallelism-mode M    pipeline|os-threads  (default: pipeline)
#                           Controls how --threads maps to actual concurrency.
#   --skip-cluster-setup    Pass --skip-sync --skip-build --skip-kafka to every run
#                           (skips package install + binary build ONLY; server restart,
#                           Raft bring-up, and table restore still run every iteration
#                           to ensure clean state — use --skip-restore to opt out)
#   --skip-restore          Skip table restore between runs (saves time; correctness risk)
#   --runs N                Measured runs per thread count (default: 1)
#   --out-dir DIR           Parent directory for per-run result dirs (default: scripts/bench_full_results/cluster_sweep_<timestamp>)
#   --workload FILE         Workload SQL file passed to run_4node_raft_cluster.sh
#   --per-thread-window N   Deterministic in-flight depth per client lane (default: 256)
#   --det-window N          Backward-compatible alias for --per-thread-window
#   --det-batch-size N      Deterministic batch size (default: 256)
#   --pool-size N           Gateway dbConnPoolSize and bcdb_init block size (default: 256)
#   --conn-fanout N         Submit sockets per node (default: raft-kafka=1,
#                           kafka-only=threads)
#   --det-pipeline-depth N  Per-terminal DET pipeline depth; 0=auto (default: 0)
#   --det-block-parallel N  Parallel PG conns per det block on database nodes (default: 16)
#                           1=serial legacy, 4-16=parallel block execution for higher TPS.
#                           IMPORTANT: set >= 4 to unlock multi-threaded execution on each
#                           cluster node (otherwise raft-kafka connFanout=1 limits each node
#                           to its 1-thread performance, ~4k TPS instead of ~9k TPS).
#   --det-event-block-fastpath N
#                           1=use BCDB block-submit fast path for deterministic event-mode
#                           scaling (default: 1). 0 is a diagnostic per-statement path.
#   --submit-mode M         blocking|event (default: event)
#   --ordering-mode M       raft-kafka|kafka-only (default: raft-kafka)
#   --kafka-completion-mode M majority|async (default: majority)
#   --no-kafka              Disable Kafka; run direct-only
#   --dry-run               Print commands without executing
#   -h, --help
#
# All options after -- are forwarded verbatim to every run_4node_raft_cluster.sh call.
#
# Environment variables (passthrough):
#   Any env var accepted by run_4node_raft_cluster.sh (SKIP_SYNC, WORKLOAD_FILE, etc.)
#   is inherited automatically.
#
# Scaling validation:
#   After all runs the script prints a TPS table and exits non-zero if TPS is
#   no higher thread count improves over the baseline at all.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_SCRIPT="$SCRIPT_DIR/run_4node_raft_cluster.sh"

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Canonical Campaign Environment Variables (matching YCSB campaign)
# ---------------------------------------------------------------------------
export ARIABC_PREFERRED_LEADER_ID="${ARIABC_PREFERRED_LEADER_ID:-1}"
export ARIABC_RAFT_DURABLE_ASYNC_FLUSH="${ARIABC_RAFT_DURABLE_ASYNC_FLUSH:-1}"
export ARIABC_RAFT_STREAM_GAP="${ARIABC_RAFT_STREAM_GAP:-512}"
export ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER="${ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER:-1}"
export ARIABC_GATEWAY_DISPATCH_WORKERS="${ARIABC_GATEWAY_DISPATCH_WORKERS:-8}"
export ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US="${ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US:-3000}"
export ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS="${ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS:-128}"
export ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS="${ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS:-128}"
export ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS="${ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS:-256}"
export ARIABC_FULL_RESULT_REPLICA_LIMIT="${ARIABC_FULL_RESULT_REPLICA_LIMIT:--1}"
export ARIABC_KAFKA_PAYLOAD_FORMAT="${ARIABC_KAFKA_PAYLOAD_FORMAT:-text}"
export BCDB_DET_QUEUE_HIGH_WM="${BCDB_DET_QUEUE_HIGH_WM:-65536}"
export BCDB_DET_QUEUE_LOW_WM="${BCDB_DET_QUEUE_LOW_WM:-32768}"
export GATEWAY_STALL_WATCHDOG="${GATEWAY_STALL_WATCHDOG:-1}"
export GATEWAY_STALL_POLL_SECONDS="${GATEWAY_STALL_POLL_SECONDS:-5}"
export GATEWAY_STALL_MAX_CYCLES="${GATEWAY_STALL_MAX_CYCLES:-12}"
export KAFKA_FAST_RESET="${KAFKA_FAST_RESET:-1}"
export DUMP_VERIFY_CSV="${DUMP_VERIFY_CSV:-0}"
export ARIABC_SOURCE_FINGERPRINT="${ARIABC_SOURCE_FINGERPRINT:-068290a61f18a35b1429ef395f4489404f65df0800cc4fba147afa5704425f61}"
export TX_SIGN="${TX_SIGN:-blake3}"
export BENCH_COLD_CACHE="${BENCH_COLD_CACHE:-0}"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SWEEP_TARGET="workers"
WORKER_COUNTS="1,4,8,16"
CLIENT_TERMINALS="96"
CLIENT_WORKERS="96"
CLIENT_INFLIGHT="16"
SKIP_CLUSTER_SETUP=1
SKIP_RESTORE_BETWEEN_RUNS=0
RUNS=1
OUT_DIR=""
WORKLOAD_ARG="/work/ARIABC/AriaBC/Final_Results/reruns/20260923T153000Z_campaign/YCSB/workloads_v5/ycsb_workload_a_skew_0_00_20k.txt"
PER_THREAD_WINDOW="256"
DET_BATCH_SIZE="256"
CONN_FANOUT="1"
DET_PIPELINE_DEPTH="0"
DET_BLOCK_PARALLEL="64"
DET_EVENT_BLOCK_FASTPATH="0"
SUBMIT_MODE="event"
ORDERING_MODE_ARG="raft-kafka"
KAFKA_COMPLETION_MODE_ARG="majority_async_all3"
NO_KAFKA=0
DRY_RUN=0
PARALLELISM_MODE="pipeline"   # pipeline|os-threads
EXTRA_ARGS=()

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
usage() {
  sed -n '/^# Usage:/,/^set -/{ /^set -/d; s/^# \?//; p }' "$0"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers)          WORKER_COUNTS="${2:-1,4,8,16}"; SWEEP_TARGET="workers"; shift 2 ;;
    --threads)          WORKER_COUNTS="${2:-1,4,8,16}"; SWEEP_TARGET="workers"; shift 2 ;;
    --client-terminals) CLIENT_TERMINALS="${2:-96}";         shift 2 ;;
    --client-workers)   CLIENT_WORKERS="${2:-96}";           shift 2 ;;
    --client-inflight)  CLIENT_INFLIGHT="${2:-16}";          shift 2 ;;
    --parallelism-mode) PARALLELISM_MODE="${2:-pipeline}";   shift 2 ;;
    --skip-cluster-setup) SKIP_CLUSTER_SETUP=1;              shift ;;
    --skip-restore)     SKIP_RESTORE_BETWEEN_RUNS=1;         shift ;;
    --runs)             RUNS="${2:-1}";                       shift 2 ;;
    --out-dir)          OUT_DIR="${2:-}";                     shift 2 ;;
    --workload)         WORKLOAD_ARG="${2:-}";                shift 2 ;;
    --det-window)       PER_THREAD_WINDOW="${2:-256}";        shift 2 ;;
    --per-thread-window) PER_THREAD_WINDOW="${2:-256}";       shift 2 ;;
    --det-batch-size)   DET_BATCH_SIZE="${2:-256}";           shift 2 ;;
    --pool-size)        POOL_SIZE="${2:-}";                   shift 2 ;;
    --conn-fanout)      CONN_FANOUT="${2:-1}";                shift 2 ;;
    --det-pipeline-depth) DET_PIPELINE_DEPTH="${2:-0}";      shift 2 ;;
    --det-block-parallel) DET_BLOCK_PARALLEL="${2:-64}";     shift 2 ;;
    --det-event-block-fastpath) DET_EVENT_BLOCK_FASTPATH="${2:-0}"; shift 2 ;;
    --submit-mode)      SUBMIT_MODE="${2:-event}";            shift 2 ;;
    --ordering-mode)    ORDERING_MODE_ARG="${2:-raft-kafka}"; shift 2 ;;
    --kafka-completion-mode) KAFKA_COMPLETION_MODE_ARG="${2:-majority_async_all3}"; shift 2 ;;
    --no-kafka)         NO_KAFKA=1;                           shift ;;
    --dry-run)          DRY_RUN=1;                            shift ;;
    -h|--help)          usage ;;
    --)                 shift; EXTRA_ARGS+=("$@"); break ;;
    *)  echo "Unknown arg: $1" >&2; usage ;;
  esac
done

# Parse worker counts into an array
IFS=',' read -ra WORKERS <<< "$WORKER_COUNTS"
for w in "${WORKERS[@]}"; do
  if ! [[ "$w" =~ ^[0-9]+$ ]] || [[ "$w" -lt 1 ]]; then
    echo "ERROR: --workers must be a comma-separated list of positive integers (got '$w')" >&2
    exit 2
  fi
done

if [[ "$RUNS" -lt 1 ]]; then
  echo "ERROR: --runs must be >= 1" >&2
  exit 2
fi

if [[ ! -x "$CLUSTER_SCRIPT" ]]; then
  echo "ERROR: cluster script not found or not executable: $CLUSTER_SCRIPT" >&2
  exit 1
fi

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$(cd "$SCRIPT_DIR/../bench_full_results" 2>/dev/null && pwd || echo "$SCRIPT_DIR/../bench_full_results")/cluster_sweep_${TIMESTAMP}"
fi
mkdir -p "$OUT_DIR"

RESULTS_CSV="$OUT_DIR/thread_sweep_results.csv"

log() { echo "[$(date +'%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Build common args shared across all runs into the COMMON_ARGS global array.
# ---------------------------------------------------------------------------
build_common_args() {
  local w="$1"
  COMMON_ARGS=(
    --per-thread-window "$PER_THREAD_WINDOW"
    --det-batch-size  "$DET_BATCH_SIZE"
    --pool-size       "$w"
    --server-exec-workers "$w"
    --server-pg-connections "$w"
    --bcdb-workers    "$w"
    --bcdb-init-block-size "$w"
    --bcdb-decouple-workers 1
    --conn-fanout 1
    --raft-ordered-fanout 1
    --raft-ordering-policy leader-assigned
    --raft-ordered-batch-append 1
    --raft-ordered-batch-target-entries 64
    --raft-ordered-batch-linger-us 1000
    --raft-ordered-coalesce-log 1
    --kafka-completion-mode majority_async_all3
    --det-window      65536
    --enable-merkle-index 1
    --tx-sign blake3
    --raft-apply-ledger-mode off
    --db-shared-buffers 32MB
    --det-pipeline-depth "$DET_PIPELINE_DEPTH"
    --det-block-parallel "$DET_BLOCK_PARALLEL"
    --det-event-block-fastpath "$DET_EVENT_BLOCK_FASTPATH"
    --submit-mode     "$SUBMIT_MODE"
    --parallelism-mode "$PARALLELISM_MODE"
  )
  if [[ -n "$WORKLOAD_ARG" ]]; then COMMON_ARGS+=(--workload "$WORKLOAD_ARG"); fi
  if [[ -n "$ORDERING_MODE_ARG" ]]; then COMMON_ARGS+=(--ordering-mode "$ORDERING_MODE_ARG"); fi
  if [[ "$NO_KAFKA" -eq 1 ]]; then COMMON_ARGS+=(--no-kafka); fi
}


# ---------------------------------------------------------------------------
# Extract TPS from a gateway log or cluster-run stdout log.
# Priority:
#   0. OS_THREADS_AGGREGATE line (os-threads mode — uses max shard wall_ms)
#   1. "overall time taken (millisec) = N" (gateway binary output)
#      In os-threads mode the gateway_test.log is a concatenation of shard
#      logs; head -1 would give the FASTEST shard (lowest ms = highest TPS,
#      wrong). Use the OS_THREADS_AGGREGATE line instead (written by
#      run_4node_raft_cluster.sh with the correct max_shard_wall_ms).
#   2. "TPS (gateway) : ~N tx/s" line printed by run_4node_raft_cluster.sh
#   3. "Est TPS : ~N tx/s" wall-clock fallback from run_4node_raft_cluster.sh
# Returns (echo): "<tps_int> <workload_lines> <gateway_ms> <lat_mean> <lat_p50> <lat_p95> <lat_p99>"
# ---------------------------------------------------------------------------
extract_tps() {
  local gw_log="$1"
  local stdout_log="$2"
  local elapsed_s="${3:-0}"
  local concurrency="${4:-1}"

  local lat_mean="0" lat_p50="0" lat_p95="0" lat_p99="0"

  if [[ ! -f "$gw_log" ]]; then
    echo "0 0 0 0 0 0 0"
    return
  fi

  # Attempt to parse from run_summary.env first
  local env_file="$(dirname "$gw_log")/run_summary.env"
  if [[ -f "$env_file" ]]; then
    lat_mean="$(grep -E '^latency_empirical_mean_ms=' "$env_file" | cut -d= -f2 || true)"
    lat_p50="$(grep -E '^latency_empirical_p50_ms=' "$env_file" | cut -d= -f2 || true)"
    lat_p95="$(grep -E '^latency_empirical_p95_ms=' "$env_file" | cut -d= -f2 || true)"
    lat_p99="$(grep -E '^latency_empirical_p99_ms=' "$env_file" | cut -d= -f2 || true)"
    if [[ -z "$lat_mean" || "$lat_mean" == "N/A" ]]; then
      lat_mean="$(grep -E '^latency_majority_per_tx_ms=' "$env_file" | cut -d= -f2 || true)"
      lat_p50="$lat_mean"
      lat_p95="$lat_mean"
      lat_p99="$lat_mean"
    fi
    if [[ -z "$lat_mean" || "$lat_mean" == "N/A" ]]; then
      lat_mean="$(grep -E '^latency_all3_per_tx_ms=' "$env_file" | cut -d= -f2 || true)"
      lat_p50="$lat_mean"
      lat_p95="$lat_mean"
      lat_p99="$lat_mean"
    fi

    local tps_maj="" tps_all3="" q="" ms="" mode=""
    tps_maj="$(grep -E '^tps_majority_visible=' "$env_file" | cut -d= -f2 || true)"
    tps_all3="$(grep -E '^tps_all3_audit_drained=' "$env_file" | cut -d= -f2 || true)"
    q="$(grep -E '^workload_transactions=' "$env_file" | cut -d= -f2 || true)"
    ms="$(grep -E '^majority_visible_ms=' "$env_file" | cut -d= -f2 || true)"
    mode="$(grep -E '^validation_mode=' "$env_file" | cut -d= -f2 || true)"

    if [[ "$tps_maj" != "N/A" && -n "$tps_maj" ]]; then
      local tps_int
      tps_int="$(printf "%.0f" "$tps_maj" 2>/dev/null || echo "${tps_maj%.*}")"
      echo "$tps_int ${q:-0} ${ms:-0} ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
      return
    fi
    if [[ "$tps_all3" != "N/A" && "$tps_all3" != "INVALID" && -n "$tps_all3" ]]; then
      local tps_int
      tps_int="$(printf "%.0f" "$tps_all3" 2>/dev/null || echo "${tps_all3%.*}")"
      echo "$tps_int ${q:-0} ${ms:-0} ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
      return
    fi
    if [[ -n "$ms" && "$ms" -gt 0 && -n "$q" && "$q" -gt 0 ]]; then
      if [[ "$mode" != "async_hash" && "$mode" != "async" ]]; then
        local tps_int=$(( q * 1000 / ms ))
        echo "$tps_int $q $ms ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
        return
      fi
    fi
  fi

  # Check for empirical latency line in gateway log or stdout
  local emp_line
  emp_line="$(grep -m1 '^TX_LATENCY_EMPIRICAL ' "$gw_log" 2>/dev/null || grep -m1 '^TX_LATENCY_EMPIRICAL ' "$stdout_log" 2>/dev/null || true)"
  if [[ -n "$emp_line" ]]; then
    lat_mean="$(echo "$emp_line" | grep -oP 'mean_ms=\K[0-9.]+' || echo "0")"
    lat_p50="$(echo "$emp_line" | grep -oP 'p50_ms=\K[0-9.]+' || echo "0")"
    lat_p95="$(echo "$emp_line" | grep -oP 'p95_ms=\K[0-9.]+' || echo "0")"
    lat_p99="$(echo "$emp_line" | grep -oP 'p99_ms=\K[0-9.]+' || echo "0")"
  fi

  local gw_ms="" workload_lines="" tps

  # 0. os-threads aggregate line — most accurate for os-threads mode.
  #    Format: "OS_THREADS_AGGREGATE queries=N max_shard_wall_ms=N aggregate_tps=N ..."
  #    This is appended by run_4node_raft_cluster.sh after all shards finish,
  #    using OSTH_MAX_MS (the slowest shard) which is the correct wall clock.
  local osth_line
  osth_line="$(grep -m1 '^OS_THREADS_AGGREGATE ' "$gw_log" 2>/dev/null || \
               grep -oP '\[os-threads\] Aggregate TPS\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
  if [[ -n "$osth_line" ]]; then
    local osth_tps osth_ms osth_q
    osth_tps="$(echo "$osth_line" | grep -oP 'aggregate_tps=\K[0-9]+' || true)"
    osth_ms="$(echo  "$osth_line" | grep -oP 'max_shard_wall_ms=\K[0-9]+' || true)"
    osth_q="$(echo  "$osth_line"  | grep -oP 'queries=\K[0-9]+'           || true)"
    if [[ -n "$osth_tps" && "$osth_tps" -gt 0 ]]; then
      if [[ "$lat_mean" == "0" ]]; then
        lat_mean="$(awk -v t="$osth_tps" -v w="$concurrency" 'BEGIN{if (t>0) printf "%.3f", w*1000/t; else print "0"}')"
        lat_p50="$lat_mean"; lat_p95="$lat_mean"; lat_p99="$lat_mean"
      fi
      echo "$osth_tps ${osth_q:-0} ${osth_ms:-0} ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
      return
    fi
  fi

  # 1. Gateway-reported wall time (most accurate for pipeline mode)
  if [[ "$PARALLELISM_MODE" == "os-threads" ]]; then
    gw_ms="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$gw_log" 2>/dev/null | tail -1 || true)"
  else
    gw_ms="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$gw_log" 2>/dev/null | head -1 || true)"
  fi
  if [[ -f "$stdout_log" ]]; then
    workload_lines="$(grep -oP 'Queries\s*:\s*\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
  fi
  if [[ -z "$workload_lines" ]]; then
    workload_lines="$(grep -oP 'loaded \K[0-9]+(?= queries)' "$gw_log" 2>/dev/null | head -1 || true)"
  fi
  if [[ "$PARALLELISM_MODE" == "os-threads" && -z "$workload_lines" ]]; then
    local total_q=0
    while IFS= read -r q; do (( total_q += q )) || true; done < \
      <(grep -oP 'loaded \K[0-9]+(?= queries)' "$gw_log" 2>/dev/null || true)
    [[ "$total_q" -gt 0 ]] && workload_lines="$total_q"
  fi

  if [[ -n "$gw_ms" && "${gw_ms:-0}" -gt 0 && -n "$workload_lines" && "${workload_lines:-0}" -gt 0 ]]; then
    tps=$(( workload_lines * 1000 / gw_ms ))
    if [[ "$lat_mean" == "0" ]]; then
      lat_mean="$(awk -v t="$tps" -v w="$concurrency" 'BEGIN{if (t>0) printf "%.3f", w*1000/t; else print "0"}')"
      lat_p50="$lat_mean"; lat_p95="$lat_mean"; lat_p99="$lat_mean"
    fi
    echo "$tps $workload_lines $gw_ms ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
    return
  fi

  # 2. TPS line printed by run_4node_raft_cluster.sh (gateway-ms based)
  local reported_tps=""
  if [[ -f "$stdout_log" ]]; then
    reported_tps="$(grep -oP '(TPS_majority_visible|TPS_strict_majority|TPS_direct)\s*:\s*\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    if [[ -z "$reported_tps" ]]; then
      reported_tps="$(grep -oP 'TPS_all3_audit_drained\s*:\s*\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$reported_tps" && "$PARALLELISM_MODE" == "os-threads" ]]; then
      reported_tps="$(grep -oP '\[os-threads\] Aggregate TPS\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$reported_tps" ]]; then
      reported_tps="$(grep -oP 'TPS \(gateway\)\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$reported_tps" ]]; then
      reported_tps="$(grep -oP 'Est TPS\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
  fi
  if [[ -n "$reported_tps" && "${reported_tps:-0}" -gt 0 ]]; then
    workload_lines="${workload_lines:-0}"
    if [[ "$lat_mean" == "0" ]]; then
      lat_mean="$(awk -v t="$reported_tps" -v w="$concurrency" 'BEGIN{if (t>0) printf "%.3f", w*1000/t; else print "0"}')"
      lat_p50="$lat_mean"; lat_p95="$lat_mean"; lat_p99="$lat_mean"
    fi
    echo "$reported_tps $workload_lines 0 ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
    return
  fi

  # 3. Last resort: wall-clock (very approximate)
  if [[ "$elapsed_s" -gt 0 && -n "$workload_lines" && "${workload_lines:-0}" -gt 0 ]]; then
    tps=$(( workload_lines / elapsed_s ))
    if [[ "$lat_mean" == "0" ]]; then
      lat_mean="$(awk -v t="$tps" -v w="$concurrency" 'BEGIN{if (t>0) printf "%.3f", w*1000/t; else print "0"}')"
      lat_p50="$lat_mean"; lat_p95="$lat_mean"; lat_p99="$lat_mean"
    fi
    echo "$tps $workload_lines 0 ${lat_mean:-0} ${lat_p50:-0} ${lat_p95:-0} ${lat_p99:-0}"
    return
  fi

  echo "0 0 0 0 0 0 0"
}

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Initialize results CSV
# ---------------------------------------------------------------------------
echo "workers,terminals,parallelism_mode,det_batch_size,per_thread_window,ordering_mode,run,tps,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,workload_lines,gateway_ms,elapsed_s,run_dir,status" > "$RESULTS_CSV"

# ---------------------------------------------------------------------------
# Accumulate per-worker TPS and Latency for the summary table
# ---------------------------------------------------------------------------
declare -A WORKER_TPS_SUM
declare -A WORKER_TPS_COUNT
declare -A WORKER_TPS_ALL
declare -A WORKER_LAT_SUM
declare -A WORKER_LAT_P50_SUM
declare -A WORKER_LAT_P95_SUM

for w in "${WORKERS[@]}"; do
  WORKER_TPS_SUM[$w]=0
  WORKER_TPS_COUNT[$w]=0
  WORKER_TPS_ALL[$w]=""
  WORKER_LAT_SUM[$w]=0
  WORKER_LAT_P50_SUM[$w]=0
  WORKER_LAT_P95_SUM[$w]=0
done

FIRST_RUN_EVER=1

log "=== Cluster Worker Thread Sweep ==="
log "  Worker counts   : ${WORKERS[*]}"
log "  Client terminals: $CLIENT_TERMINALS (inflight: $CLIENT_INFLIGHT)"
log "  Runs per count  : $RUNS"
log "  Workload        : $WORKLOAD_ARG"
log "  Output dir      : $OUT_DIR"
if [[ -n "$CONN_FANOUT" ]]; then
  CONN_FANOUT_LABEL="$CONN_FANOUT"
else
  CONN_FANOUT_LABEL="1"
fi

EFFECTIVE_ORDERING="${ORDERING_MODE_ARG:-raft-kafka}"
log "  Ordering mode   : $EFFECTIVE_ORDERING"
log ""

for w in "${WORKERS[@]}"; do
  log "--- Server Workers: $w (Client Terminals: $CLIENT_TERMINALS) ---"

  for run_idx in $(seq 1 "$RUNS"); do
    RUN_LABEL="workers=${w}_run${run_idx}"
    RUN_DIR="$OUT_DIR/${RUN_LABEL}"
    mkdir -p "$RUN_DIR"

    STDOUT_LOG="$RUN_DIR/cluster_run.log"

    # Build per-run args: fixed 96 client terminals with 16 in-flight per terminal
    RUN_ARGS=(
      --threads "$CLIENT_TERMINALS"
      --det-client-workers "$CLIENT_WORKERS"
      --det-client-inflight "$CLIENT_INFLIGHT"
    )

    # Cluster setup (sync / build / kafka) — skip on all runs after the first
    if [[ "$SKIP_CLUSTER_SETUP" -eq 1 ]]; then
      RUN_ARGS+=(--skip-sync --skip-build --skip-kafka --skip-rdkafka-setup)
    elif [[ "$FIRST_RUN_EVER" -eq 0 ]]; then
      RUN_ARGS+=(--skip-sync --skip-build --skip-rdkafka-setup)
    fi

    # Restore: always restore at the start of each worker-count group (run_idx==1),
    # skip within the same worker count for run_idx>1 if explicitly requested.
    if [[ "$SKIP_RESTORE_BETWEEN_RUNS" -eq 1 && "$run_idx" -gt 1 ]]; then
      RUN_ARGS+=(--skip-restore)
    fi
    if [[ "$FIRST_RUN_EVER" -eq 0 && "$run_idx" -gt 1 ]]; then
      RUN_ARGS+=(--skip-cleanup)
    fi

    # Common args; maps $w to server-exec-workers, pool-size, bcdb-workers, bcdb-init-block-size
    build_common_args "$w"
    RUN_ARGS+=("${COMMON_ARGS[@]}")
    [[ ${#EXTRA_ARGS[@]} -gt 0 ]] && RUN_ARGS+=("${EXTRA_ARGS[@]}")

    log "  [w=$w run=$run_idx] Command: $CLUSTER_SCRIPT ${RUN_ARGS[*]+${RUN_ARGS[*]}}"
    log "  [w=$w run=$run_idx] Log: $STDOUT_LOG"

    ELAPSED_S=0
    STATUS="ok"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      log "  [w=$w run=$run_idx] DRY-RUN — skipping execution"
      STATUS="dry-run"
    else
      T_START="$(date +%s)"
      if ! bash "$CLUSTER_SCRIPT" "${RUN_ARGS[@]}" 2>&1 | tee "$STDOUT_LOG"; then
        STATUS="failed"
        log "  [w=$w run=$run_idx] FAILED (non-zero exit) — see $STDOUT_LOG"
      fi
      T_END="$(date +%s)"
      ELAPSED_S=$(( T_END - T_START ))
    fi

    # Find the gateway_test.log inside the timestamped LOG_DIR created by run_4node_raft_cluster.sh.
    GW_LOG=""
    BENCH_RESULTS_DIR="$(cd "$SCRIPT_DIR/../bench_full_results" 2>/dev/null && pwd || echo "")"
    if [[ -n "$BENCH_RESULTS_DIR" && -d "$BENCH_RESULTS_DIR" ]]; then
      GW_LOG="$(find "$BENCH_RESULTS_DIR" -name "gateway_test.log" -newer "$RUN_DIR" 2>/dev/null | sort -t_ -k2 | tail -1 || true)"
    fi
    if [[ -z "$GW_LOG" || ! -f "$GW_LOG" ]]; then
      GW_LOG="$STDOUT_LOG"
    fi
    # Copy tx_latency.csv into RUN_DIR if available
    if [[ -n "$GW_LOG" && -f "$(dirname "$GW_LOG")/tx_latency.csv" ]]; then
      cp "$(dirname "$GW_LOG")/tx_latency.csv" "$RUN_DIR/tx_latency.csv"
    fi

    # Parse TPS and Latency
    read -r TPS WORKLOAD_LINES GW_MS LAT_MEAN LAT_P50 LAT_P95 LAT_P99 <<< "$(extract_tps "$GW_LOG" "$STDOUT_LOG" "$ELAPSED_S" "$CLIENT_TERMINALS")"
    TPS="${TPS:-0}"
    WORKLOAD_LINES="${WORKLOAD_LINES:-0}"
    GW_MS="${GW_MS:-0}"
    LAT_MEAN="${LAT_MEAN:-0}"
    LAT_P50="${LAT_P50:-0}"
    LAT_P95="${LAT_P95:-0}"
    LAT_P99="${LAT_P99:-0}"

    log "  [w=$w run=$run_idx] TPS=$TPS lat_mean=${LAT_MEAN}ms lat_p50=${LAT_P50}ms lat_p95=${LAT_P95}ms workload_lines=$WORKLOAD_LINES gw_ms=${GW_MS} elapsed=${ELAPSED_S}s status=$STATUS"

    # Append to CSV
    echo "$w,$CLIENT_TERMINALS,$PARALLELISM_MODE,$DET_BATCH_SIZE,$PER_THREAD_WINDOW,${EFFECTIVE_ORDERING},$run_idx,$TPS,$LAT_MEAN,$LAT_P50,$LAT_P95,$LAT_P99,$WORKLOAD_LINES,$GW_MS,$ELAPSED_S,$RUN_DIR,$STATUS" >> "$RESULTS_CSV"

    # Accumulate for summary
    if [[ "$STATUS" == "ok" && "$TPS" -gt 0 ]]; then
      WORKER_TPS_SUM[$w]=$(( WORKER_TPS_SUM[$w] + TPS ))
      WORKER_TPS_COUNT[$w]=$(( WORKER_TPS_COUNT[$w] + 1 ))
      WORKER_TPS_ALL[$w]+=" $TPS"
      WORKER_LAT_SUM[$w]="$(awk -v s="${WORKER_LAT_SUM[$w]}" -v v="$LAT_MEAN" 'BEGIN{print s+v}')"
      WORKER_LAT_P50_SUM[$w]="$(awk -v s="${WORKER_LAT_P50_SUM[$w]}" -v v="$LAT_P50" 'BEGIN{print s+v}')"
      WORKER_LAT_P95_SUM[$w]="$(awk -v s="${WORKER_LAT_P95_SUM[$w]}" -v v="$LAT_P95" 'BEGIN{print s+v}')"
    fi

    FIRST_RUN_EVER=0
  done
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------
log ""
log "=== Worker Thread Scaling Summary (Client Terminals: $CLIENT_TERMINALS) ==="
log "$(printf '%-10s %-10s %-10s %-14s %-14s %-14s %-12s' 'workers' 'terminals' 'avg_tps' 'lat_mean(ms)' 'lat_p50(ms)' 'lat_p95(ms)' 'scaling_tps')"
log "$(printf '%-10s %-10s %-10s %-14s %-14s %-14s %-12s' '-------' '---------' '-------' '------------' '------------' '------------' '-----------')"

BASE_TPS=0
SAW_IMPROVEMENT=0
SAW_REGRESSION=0

for w in "${WORKERS[@]}"; do
  count="${WORKER_TPS_COUNT[$w]}"
  sum="${WORKER_TPS_SUM[$w]}"
  all="${WORKER_TPS_ALL[$w]}"

  if [[ "$count" -gt 0 ]]; then
    avg=$(( sum / count ))
    lat_mean_avg="$(awk -v s="${WORKER_LAT_SUM[$w]}" -v c="$count" 'BEGIN{if (c>0) printf "%.3f", s/c; else print "0"}')"
    lat_p50_avg="$(awk -v s="${WORKER_LAT_P50_SUM[$w]}" -v c="$count" 'BEGIN{if (c>0) printf "%.3f", s/c; else print "0"}')"
    lat_p95_avg="$(awk -v s="${WORKER_LAT_P95_SUM[$w]}" -v c="$count" 'BEGIN{if (c>0) printf "%.3f", s/c; else print "0"}')"
  else
    avg=0
    lat_mean_avg="0"
    lat_p50_avg="0"
    lat_p95_avg="0"
  fi

  if [[ "$BASE_TPS" -eq 0 && "$avg" -gt 0 ]]; then
    BASE_TPS="$avg"
  fi

  if [[ "$BASE_TPS" -gt 0 && "$avg" -gt 0 ]]; then
    ratio=$(( avg * 100 / BASE_TPS ))
    ratio_str="${ratio}%"
  else
    ratio_str="N/A"
  fi

  log "$(printf '%-10s %-10s %-10s %-14s %-14s %-14s %-12s' "$w" "$CLIENT_TERMINALS" "$avg" "$lat_mean_avg" "$lat_p50_avg" "$lat_p95_avg" "$ratio_str")"

  if [[ "$BASE_TPS" -gt 0 && "$avg" -gt 0 && "$avg" -gt "$BASE_TPS" ]]; then
    SAW_IMPROVEMENT=1
  fi
  if [[ "$BASE_TPS" -gt 0 && "$avg" -gt 0 && "$avg" -lt "$BASE_TPS" ]]; then
    SAW_REGRESSION=1
  fi
done

log ""
log "Results CSV: $RESULTS_CSV"

# ---------------------------------------------------------------------------
# Scaling validation
# ---------------------------------------------------------------------------
SUCCESSFUL_COUNTS=0
for w in "${WORKERS[@]}"; do
  [[ "${WORKER_TPS_COUNT[$w]}" -gt 0 ]] && (( SUCCESSFUL_COUNTS++ )) || true
done

if [[ "$SUCCESSFUL_COUNTS" -ge 2 && "$SAW_IMPROVEMENT" -eq 0 ]]; then
  log "WARNING: No higher worker count improved over the baseline."
  log "         Check gateway_test.log files in sub-dirs under $OUT_DIR"
fi

if [[ "$SUCCESSFUL_COUNTS" -eq 0 ]]; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "=== Dry run complete ==="
    exit 0
  fi
  log "ERROR: No successful runs completed. Check logs in $OUT_DIR"
  exit 1
fi

log "=== Sweep complete ==="

# ---------------------------------------------------------------------------
# Generate Graph
# ---------------------------------------------------------------------------
if command -v python3 >/dev/null 2>&1; then
  GRAPH_FILE="$OUT_DIR/tps_vs_threads.png"
  LAT_GRAPH_FILE="$OUT_DIR/latency_vs_threads.png"
  DUAL_GRAPH_FILE="$OUT_DIR/cluster_scaling_tps_and_latency.png"
  cat << 'EOF' > "$OUT_DIR/plot.py"
import sys
import pandas as pd
import matplotlib.pyplot as plt

csv_file = sys.argv[1]
tps_out = sys.argv[2]
lat_out = sys.argv[3]
dual_out = sys.argv[4]

try:
    df = pd.read_csv(csv_file)
    df = df[df['status'] == 'ok']
    if df.empty:
        sys.exit(0)

    for c in ['tps', 'latency_mean_ms', 'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    col = 'workers' if 'workers' in df.columns else 'threads'
    x_vals = sorted(df[col].unique())
    tps_grp = df.groupby(col)['tps'].agg(['mean', 'std']).reset_index().fillna(0)
    lat_grp = df.groupby(col)['latency_mean_ms'].agg(['mean', 'std']).reset_index().fillna(0)
    p50_grp = df.groupby(col)['latency_p50_ms'].agg(['mean']).reset_index().fillna(0)
    p95_grp = df.groupby(col)['latency_p95_ms'].agg(['mean']).reset_index().fillna(0)

    terminals = df['terminals'].iloc[0] if 'terminals' in df.columns else 96

    # 1. TPS Plot
    plt.figure(figsize=(9, 5.5))
    plt.errorbar(tps_grp[col], tps_grp['mean'], yerr=tps_grp['std'],
                 fmt='-o', color='#1f77b4', lw=2.5, markersize=8, capsize=4, label='Throughput (TPS)')
    for _, row in tps_grp.iterrows():
        plt.annotate(f"{int(row['mean'])} TPS", (row[col], row['mean']),
                     textcoords="offset points", xytext=(0, 10), ha='center', fontweight='bold', fontsize=10)
    plt.title(f'Cluster Throughput Scaling vs Worker Threads (Terminals={terminals})', fontsize=14, fontweight='bold', pad=12)
    plt.xlabel('Server Worker Threads', fontsize=12, fontweight='bold')
    plt.ylabel('Throughput (tx/sec)', fontsize=12, fontweight='bold')
    plt.xticks(x_vals)
    plt.ylim(bottom=0, top=tps_grp['mean'].max() * 1.25)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(tps_out, dpi=200)
    plt.close()

    # 2. Latency Plot (Mean, p50, p95)
    plt.figure(figsize=(9, 5.5))
    plt.plot(lat_grp[col], lat_grp['mean'], '-o', color='#d62728', lw=2.5, markersize=8, label='Mean Latency')
    if (p50_grp['mean'] > 0).any():
        plt.plot(p50_grp[col], p50_grp['mean'], '-s', color='#2ca02c', lw=2.0, markersize=7, label='p50 (Median) Latency')
    if (p95_grp['mean'] > 0).any():
        plt.plot(p95_grp[col], p95_grp['mean'], '-^', color='#ff7f0e', lw=2.0, markersize=7, label='p95 Latency')
    for _, row in lat_grp.iterrows():
        plt.annotate(f"{row['mean']:.3f} ms", (row[col], row['mean']),
                     textcoords="offset points", xytext=(0, 10), ha='center', fontweight='bold', fontsize=10)
    plt.title(f'Cluster Transaction Latency vs Worker Threads (Terminals={terminals})', fontsize=14, fontweight='bold', pad=12)
    plt.xlabel('Server Worker Threads', fontsize=12, fontweight='bold')
    plt.ylabel('Elapsed Latency per Transaction (ms)', fontsize=12, fontweight='bold')
    plt.xticks(x_vals)
    plt.ylim(bottom=0)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(loc='best', frameon=True)
    plt.tight_layout()
    plt.savefig(lat_out, dpi=200)
    plt.close()

    # 3. Dual-Panel Plot: TPS (Left) and Latency (Right)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    ax1.plot(tps_grp[col], tps_grp['mean'], '-o', color='#1f77b4', lw=2.5, markersize=8)
    for _, row in tps_grp.iterrows():
        ax1.annotate(f"{int(row['mean'])}", (row[col], row['mean']),
                     textcoords="offset points", xytext=(0, 10), ha='center', fontweight='bold')
    ax1.set_title('Throughput Scaling (TPS)', fontsize=13, fontweight='bold')
    ax1.set_xlabel('Server Worker Threads', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Throughput (tx/s)', fontsize=11, fontweight='bold')
    ax1.set_xticks(x_vals)
    ax1.set_ylim(bottom=0, top=tps_grp['mean'].max() * 1.25)
    ax1.grid(True, linestyle='--', alpha=0.6)

    ax2.plot(lat_grp[col], lat_grp['mean'], '-o', color='#d62728', lw=2.5, markersize=8, label='Mean')
    if (p50_grp['mean'] > 0).any():
        ax2.plot(p50_grp[col], p50_grp['mean'], '-s', color='#2ca02c', lw=2.0, markersize=7, label='p50')
    if (p95_grp['mean'] > 0).any():
        ax2.plot(p95_grp[col], p95_grp['mean'], '-^', color='#ff7f0e', lw=2.0, markersize=7, label='p95')
    for _, row in lat_grp.iterrows():
        ax2.annotate(f"{row['mean']:.3f} ms", (row[col], row['mean']),
                     textcoords="offset points", xytext=(0, 10), ha='center', fontweight='bold')
    ax2.set_title('Per-Transaction Latency Scaling', fontsize=13, fontweight='bold')
    ax2.set_xlabel('Server Worker Threads', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
    ax2.set_xticks(x_vals)
    ax2.set_ylim(bottom=0)
    ax2.grid(True, linestyle='--', alpha=0.6)
    ax2.legend(loc='best', frameon=True)

    fig.suptitle(f'4-Node AriaBC Cluster: Throughput & Latency vs Worker Threads (Terminals={terminals})', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(dual_out, dpi=200)
    plt.close()

    print(f"Graphs successfully generated: {tps_out}, {lat_out}, {dual_out}")
except Exception as e:
    print(f"Failed to generate graphs: {e}")
EOF
  log "Generating Python graphs (TPS, Latency, Dual-Panel)..."
  python3 "$OUT_DIR/plot.py" "$RESULTS_CSV" "$GRAPH_FILE" "$LAT_GRAPH_FILE" "$DUAL_GRAPH_FILE" || log "Failed to generate Python graph."
else
  log "python3 not found, skipping Python graph generation."
fi

# Gnuplot generator
if command -v gnuplot >/dev/null 2>&1; then
  log "Generating gnuplot graph..."
  GNUPLOT_SCRIPT="$OUT_DIR/plot.plt"
  GNUPLOT_GRAPH="$OUT_DIR/tps_vs_threads_gnuplot.png"
  cat << EOF > "$GNUPLOT_SCRIPT"
set datafile separator ","
set terminal pngcairo size 1024,768 enhanced font "sans,12"
set output "$GNUPLOT_GRAPH"
set title "Cluster Throughput: TPS vs Worker Threads" font "sans,16"
set xlabel "Server Worker Threads" font "sans,14"
set ylabel "Throughput (TPS)" font "sans,14"
set grid xtics ytics ls 12 lc rgb '#dddddd' lt 1 lw 1
set style line 12 lc rgb '#dddddd' lt 0 lw 1
set style line 1 lc rgb '#0060ad' lt 1 lw 2 pt 7 ps 1.5
set style fill transparent solid 0.2 noborder
set yrange [0:*]
set xtics 1
plot "$RESULTS_CSV" using 1:8:xtic(1) with points ls 1 title "TPS Runs"
EOF
  gnuplot "$GNUPLOT_SCRIPT" || log "Failed to generate gnuplot graph."
else
  log "gnuplot not found, skipping gnuplot graph generation."
fi

exit 0
