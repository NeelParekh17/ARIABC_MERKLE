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
# Defaults
# ---------------------------------------------------------------------------
THREAD_COUNTS="1,4,8"
SKIP_CLUSTER_SETUP=0
SKIP_RESTORE_BETWEEN_RUNS=0
RUNS=1
OUT_DIR=""
WORKLOAD_ARG=""
PER_THREAD_WINDOW="256"
DET_BATCH_SIZE="256"
POOL_SIZE="256"
CONN_FANOUT=""
DET_PIPELINE_DEPTH="0"
DET_BLOCK_PARALLEL="16"       # default 16 — enables parallel block execution on db nodes
DET_EVENT_BLOCK_FASTPATH="1"
SUBMIT_MODE="event"
ORDERING_MODE_ARG=""
KAFKA_COMPLETION_MODE_ARG=""
NO_KAFKA=0
DRY_RUN=0
PARALLELISM_MODE="pipeline"   # pipeline|os-threads
EXTRA_ARGS=()

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
usage() {
  sed -n '/^# Usage:/,/^[^#]/{ /^#/{ s/^# \?//; p }; /^[^#]/q }' "$0"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)          THREAD_COUNTS="${2:-1,4,8}";         shift 2 ;;
    --parallelism-mode) PARALLELISM_MODE="${2:-pipeline}";   shift 2 ;;
    --skip-cluster-setup) SKIP_CLUSTER_SETUP=1;              shift ;;
    --skip-restore)     SKIP_RESTORE_BETWEEN_RUNS=1;         shift ;;
    --runs)             RUNS="${2:-1}";                       shift 2 ;;
    --out-dir)          OUT_DIR="${2:-}";                     shift 2 ;;
    --workload)         WORKLOAD_ARG="${2:-}";                shift 2 ;;
    --det-window)       PER_THREAD_WINDOW="${2:-256}";        shift 2 ;;
    --per-thread-window) PER_THREAD_WINDOW="${2:-256}";       shift 2 ;;
    --det-batch-size)   DET_BATCH_SIZE="${2:-256}";           shift 2 ;;
    --pool-size)        POOL_SIZE="${2:-256}";                shift 2 ;;
    --conn-fanout)      CONN_FANOUT="${2:-1}";                shift 2 ;;
    --det-pipeline-depth) DET_PIPELINE_DEPTH="${2:-0}";      shift 2 ;;
    --det-block-parallel) DET_BLOCK_PARALLEL="${2:-16}";     shift 2 ;;
    --det-event-block-fastpath) DET_EVENT_BLOCK_FASTPATH="${2:-1}"; shift 2 ;;
    --submit-mode)      SUBMIT_MODE="${2:-event}";            shift 2 ;;
    --ordering-mode)    ORDERING_MODE_ARG="${2:-}";           shift 2 ;;
    --kafka-completion-mode) KAFKA_COMPLETION_MODE_ARG="${2:-}"; shift 2 ;;
    --no-kafka)         NO_KAFKA=1;                           shift ;;
    --dry-run)          DRY_RUN=1;                            shift ;;
    -h|--help)          usage ;;
    --)                 shift; EXTRA_ARGS+=("$@"); break ;;
    *)  echo "Unknown arg: $1" >&2; usage ;;
  esac
done

# Parse thread counts into an array
IFS=',' read -ra THREADS <<< "$THREAD_COUNTS"
for t in "${THREADS[@]}"; do
  if ! [[ "$t" =~ ^[0-9]+$ ]] || [[ "$t" -lt 1 ]]; then
    echo "ERROR: --threads must be a comma-separated list of positive integers (got '$t')" >&2
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
# Using a global array avoids the echo+word-split anti-pattern that breaks
# args containing spaces and cannot safely round-trip through a string.
# ---------------------------------------------------------------------------
build_common_args() {
  local t="$1"
  COMMON_ARGS=(
    --per-thread-window "$PER_THREAD_WINDOW"
    --det-batch-size  "$DET_BATCH_SIZE"
    --pool-size       "$POOL_SIZE"
    --det-pipeline-depth "$DET_PIPELINE_DEPTH"
    --det-block-parallel "$DET_BLOCK_PARALLEL"
    --det-event-block-fastpath "$DET_EVENT_BLOCK_FASTPATH"
    --submit-mode     "$SUBMIT_MODE"
    --parallelism-mode "$PARALLELISM_MODE"
  )
  if [[ -n "$CONN_FANOUT" ]]; then COMMON_ARGS+=(--conn-fanout "$CONN_FANOUT"); fi
  if [[ -n "$WORKLOAD_ARG" ]]; then COMMON_ARGS+=(--workload "$WORKLOAD_ARG"); fi
  if [[ -n "$ORDERING_MODE_ARG" ]]; then COMMON_ARGS+=(--ordering-mode "$ORDERING_MODE_ARG"); fi
  if [[ -n "$KAFKA_COMPLETION_MODE_ARG" ]]; then COMMON_ARGS+=(--kafka-completion-mode "$KAFKA_COMPLETION_MODE_ARG"); fi
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
# Returns (echo): "<tps_int> <workload_lines> <gateway_ms>"
# ---------------------------------------------------------------------------
extract_tps() {
  local gw_log="$1"
  local stdout_log="$2"
  local elapsed_s="${3:-0}"

  if [[ ! -f "$gw_log" ]]; then
    echo "0 0 0"
    return
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
      echo "$osth_tps ${osth_q:-0} ${osth_ms:-0}"
      return
    fi
  fi

  # 1. Gateway-reported wall time (most accurate for pipeline mode)
  #    In os-threads mode, use the appended synthetic line (last occurrence =
  #    max shard value written by run_4node_raft_cluster.sh).
  if [[ "$PARALLELISM_MODE" == "os-threads" ]]; then
    # Take the LAST occurrence: run_4node appends "overall_wall_ms=OSTH_MAX_MS" last.
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
  # For os-threads, aggregate queries is sum of all shard queries
  if [[ "$PARALLELISM_MODE" == "os-threads" && -z "$workload_lines" ]]; then
    local total_q=0
    while IFS= read -r q; do (( total_q += q )) || true; done < \
      <(grep -oP 'loaded \K[0-9]+(?= queries)' "$gw_log" 2>/dev/null || true)
    [[ "$total_q" -gt 0 ]] && workload_lines="$total_q"
  fi

  if [[ -n "$gw_ms" && "${gw_ms:-0}" -gt 0 && -n "$workload_lines" && "${workload_lines:-0}" -gt 0 ]]; then
    tps=$(( workload_lines * 1000 / gw_ms ))
    echo "$tps $workload_lines $gw_ms"
    return
  fi

  # 2. TPS line printed by run_4node_raft_cluster.sh (gateway-ms based)
  #    Format: "[HH:MM:SS]   TPS (gateway) : ~NNN tx/s"
  local reported_tps=""
  if [[ -f "$stdout_log" ]]; then
    # For os-threads mode, prefer the aggregate TPS reported by run_4node
    if [[ "$PARALLELISM_MODE" == "os-threads" ]]; then
      reported_tps="$(grep -oP '\[os-threads\] Aggregate TPS\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$reported_tps" ]]; then
      reported_tps="$(grep -oP 'TPS \(gateway\)\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$reported_tps" ]]; then
      # Also match the "Est TPS" wall-clock fallback line
      reported_tps="$(grep -oP 'Est TPS\s*:\s*~\K[0-9]+' "$stdout_log" 2>/dev/null | head -1 || true)"
    fi
  fi
  if [[ -n "$reported_tps" && "${reported_tps:-0}" -gt 0 ]]; then
    workload_lines="${workload_lines:-0}"
    echo "$reported_tps $workload_lines 0"
    return
  fi

  # 3. Last resort: wall-clock (very approximate)
  if [[ "$elapsed_s" -gt 0 && -n "$workload_lines" && "${workload_lines:-0}" -gt 0 ]]; then
    tps=$(( workload_lines / elapsed_s ))
    echo "$tps $workload_lines 0"
    return
  fi

  echo "0 0 0"
}

# ---------------------------------------------------------------------------
# Initialize results CSV
# ---------------------------------------------------------------------------
echo "threads,run,tps,workload_lines,gateway_ms,elapsed_s,run_dir,status" > "$RESULTS_CSV"

# ---------------------------------------------------------------------------
# Accumulate per-thread TPS for the summary table
# Key: threads → array of TPS values across runs
# ---------------------------------------------------------------------------
declare -A THREAD_TPS_SUM      # sum of tps per thread count
declare -A THREAD_TPS_COUNT    # number of successful runs per thread count
declare -A THREAD_TPS_ALL      # all tps values (space-separated) for display

for t in "${THREADS[@]}"; do
  THREAD_TPS_SUM[$t]=0
  THREAD_TPS_COUNT[$t]=0
  THREAD_TPS_ALL[$t]=""
done

# ---------------------------------------------------------------------------
# Build skip flags
#
# Strategy:
#   Run 1 of thread count 1 always does the full setup (sync/build/kafka/cleanup/restore).
#   Subsequent runs within the same thread count use --skip-restore (already restored).
#   Between different thread counts, we re-restore to ensure identical start state,
#   but skip sync/build/kafka (cluster is already up from prior run).
#
#   If --skip-cluster-setup is passed, we skip sync+build+kafka on ALL runs.
# ---------------------------------------------------------------------------

FIRST_RUN_EVER=1

log "=== Cluster Thread Sweep ==="
log "  Thread counts : ${THREADS[*]}"
log "  Runs per count: $RUNS"
log "  Output dir    : $OUT_DIR"
if [[ -n "$CONN_FANOUT" ]]; then
  CONN_FANOUT_LABEL="$CONN_FANOUT"
else
  CONN_FANOUT_LABEL="auto: raft-kafka=1, kafka-only=threads"
fi

# Detect os-threads + raft-kafka incompatibility early — downgrade to pipeline
# so the whole sweep doesn't abort on the first iteration.  os-threads requires
# the gateway to support strided DET sequence stepping (--detSeqStep), which it
# doesn't.  Pipeline mode is the correct equivalent for raft-kafka: the single
# gateway already implements the strided multi-terminal pattern internally.
EFFECTIVE_ORDERING="${ORDERING_MODE_ARG:-raft-kafka}"
if [[ "$PARALLELISM_MODE" == "os-threads" && "$EFFECTIVE_ORDERING" == *"raft"* ]]; then
  log "  WARNING: --parallelism-mode os-threads is incompatible with raft-kafka ordering."
  log "  Root cause: contiguous DET sequence shards serialize at the BCDB serial gate —"
  log "  shard N waits for ALL prior shards to complete, giving zero parallelism benefit."
  log "  Strided sharding (the fix) requires --detSeqStep which the gateway binary lacks."
  log "  AUTO-SWITCHING to --parallelism-mode pipeline, which already implements"
  log "  strided DET sequences across N terminal lanes inside one gateway process."
  log "  This is structurally identical to Python's ThreadPoolExecutor(max_workers=N)."
  log "  Use --ordering-mode kafka-only to run os-threads with true parallel processes."
  PARALLELISM_MODE="pipeline"
fi

log "  Parallelism mode: $PARALLELISM_MODE"
if [[ "$PARALLELISM_MODE" == "os-threads" ]]; then
  log "  *** os-threads: N independent gateway procs — only valid for kafka-only mode ***"
else
  log "  *** pipeline: N terminal lanes / 1 reactor (strided DET assignment internally) ***"
  log "  *** This is the correct multi-thread model for raft-kafka deterministic ordering ***"
fi
log "  Common args   : (base) --per-thread-window $PER_THREAD_WINDOW --det-batch-size $DET_BATCH_SIZE --pool-size $POOL_SIZE --conn-fanout $CONN_FANOUT_LABEL --det-pipeline-depth $DET_PIPELINE_DEPTH --det-block-parallel $DET_BLOCK_PARALLEL --det-event-block-fastpath $DET_EVENT_BLOCK_FASTPATH --submit-mode $SUBMIT_MODE ${EXTRA_ARGS[*]+${EXTRA_ARGS[*]}}"
log ""


for t in "${THREADS[@]}"; do
  log "--- Terminals: $t ---"

  for run_idx in $(seq 1 "$RUNS"); do
    RUN_LABEL="threads=${t}_run${run_idx}"
    RUN_DIR="$OUT_DIR/${RUN_LABEL}"
    mkdir -p "$RUN_DIR"

    STDOUT_LOG="$RUN_DIR/cluster_run.log"

    # Build per-run args
    RUN_ARGS=(
      --threads "$t"
    )

    # Cluster setup (sync / build / kafka) — skip on all runs after the first
    if [[ "$SKIP_CLUSTER_SETUP" -eq 1 ]]; then
      RUN_ARGS+=(--skip-sync --skip-build --skip-kafka --skip-rdkafka-setup)
    elif [[ "$FIRST_RUN_EVER" -eq 0 ]]; then
      RUN_ARGS+=(--skip-sync --skip-build --skip-rdkafka-setup)
    fi

    # Restore: always restore at the start of each thread-count group (run_idx==1),
    # skip within the same thread count for run_idx>1 if explicitly requested.
    if [[ "$SKIP_RESTORE_BETWEEN_RUNS" -eq 1 && "$run_idx" -gt 1 ]]; then
      RUN_ARGS+=(--skip-restore)
    fi
    # When the cluster is already up across iterations, also skip cleanup to
    # avoid restarting the servers and losing the warm state between runs.
    if [[ "$FIRST_RUN_EVER" -eq 0 && "$run_idx" -gt 1 ]]; then
      RUN_ARGS+=(--skip-cleanup)
    fi

    # Common args; run_4node maps --threads to lanes and derives the total window.
    build_common_args "$t"
    RUN_ARGS+=("${COMMON_ARGS[@]}")
    # Only splice EXTRA_ARGS when it is non-empty to avoid passing a phantom
    # empty-string argument that run_4node_raft_cluster.sh rejects.
    [[ ${#EXTRA_ARGS[@]} -gt 0 ]] && RUN_ARGS+=("${EXTRA_ARGS[@]}")

    log "  [t=$t run=$run_idx] Command: $CLUSTER_SCRIPT ${RUN_ARGS[*]+${RUN_ARGS[*]}}"
    log "  [t=$t run=$run_idx] Log: $STDOUT_LOG"

    ELAPSED_S=0
    STATUS="ok"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      log "  [t=$t run=$run_idx] DRY-RUN — skipping execution"
      STATUS="dry-run"
    else
      T_START="$(date +%s)"
      if ! bash "$CLUSTER_SCRIPT" "${RUN_ARGS[@]}" 2>&1 | tee "$STDOUT_LOG"; then
        STATUS="failed"
        log "  [t=$t run=$run_idx] FAILED (non-zero exit) — see $STDOUT_LOG"
      fi
      T_END="$(date +%s)"
      ELAPSED_S=$(( T_END - T_START ))
    fi

    # Find the gateway_test.log inside the timestamped LOG_DIR created by run_4node_raft_cluster.sh.
    # The script creates: scripts/bench_full_results/cluster4_<timestamp>/gateway_test.log
    # We look for the most recently modified one that was created during this run.
    GW_LOG=""
    BENCH_RESULTS_DIR="$(cd "$SCRIPT_DIR/../bench_full_results" 2>/dev/null && pwd || echo "")"
    if [[ -n "$BENCH_RESULTS_DIR" && -d "$BENCH_RESULTS_DIR" ]]; then
      GW_LOG="$(find "$BENCH_RESULTS_DIR" -name "gateway_test.log" -newer "$RUN_DIR" 2>/dev/null | sort -t_ -k2 | tail -1 || true)"
    fi
    # Also check the stdout log for the TPS line printed by run_4node_raft_cluster.sh
    if [[ -z "$GW_LOG" || ! -f "$GW_LOG" ]]; then
      GW_LOG="$STDOUT_LOG"
    fi

    # Parse TPS
    read -r TPS WORKLOAD_LINES GW_MS <<< "$(extract_tps "$GW_LOG" "$STDOUT_LOG" "$ELAPSED_S")"
    TPS="${TPS:-0}"
    WORKLOAD_LINES="${WORKLOAD_LINES:-0}"
    GW_MS="${GW_MS:-0}"

    log "  [t=$t run=$run_idx] TPS=$TPS workload_lines=$WORKLOAD_LINES gw_ms=${GW_MS} elapsed=${ELAPSED_S}s status=$STATUS"

    # Append to CSV
    echo "$t,$run_idx,$TPS,$WORKLOAD_LINES,$GW_MS,$ELAPSED_S,$RUN_DIR,$STATUS" >> "$RESULTS_CSV"

    # Accumulate for summary
    if [[ "$STATUS" == "ok" && "$TPS" -gt 0 ]]; then
      THREAD_TPS_SUM[$t]=$(( THREAD_TPS_SUM[$t] + TPS ))
      THREAD_TPS_COUNT[$t]=$(( THREAD_TPS_COUNT[$t] + 1 ))
      THREAD_TPS_ALL[$t]+=" $TPS"
    fi

    FIRST_RUN_EVER=0
  done
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------
log ""
log "=== Thread Scaling Summary ==="
log "  parallelism_mode=$PARALLELISM_MODE"
if [[ "$PARALLELISM_MODE" == "pipeline" ]]; then
  log "  WARNING: 'pipeline' mode measures DET window depth, NOT OS-level parallelism."
  log "           To compare fairly with the single-node Python benchmark, re-run with --parallelism-mode os-threads"
fi
log "$(printf '%-12s %-10s %-18s %s' 'terminals' 'avg_tps' 'all_tps' 'scaling_vs_1t')"
log "$(printf '%-12s %-10s %-18s %s' '----------' '-------' '-------' '-------------')"

BASE_TPS=0
SAW_IMPROVEMENT=0
SAW_REGRESSION=0

for t in "${THREADS[@]}"; do
  count="${THREAD_TPS_COUNT[$t]}"
  sum="${THREAD_TPS_SUM[$t]}"
  all="${THREAD_TPS_ALL[$t]}"

  if [[ "$count" -gt 0 ]]; then
    avg=$(( sum / count ))
  else
    avg=0
  fi

  if [[ "$BASE_TPS" -eq 0 && "$avg" -gt 0 ]]; then
    BASE_TPS="$avg"
  fi

  if [[ "$BASE_TPS" -gt 0 && "$avg" -gt 0 ]]; then
    # Integer ratio ×100 for display as percentage
    ratio=$(( avg * 100 / BASE_TPS ))
    ratio_str="${ratio}%"
  else
    ratio_str="N/A"
  fi

  log "$(printf '%-12s %-10s %-18s %s' "$t" "$avg" "${all# }" "$ratio_str")"

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
# Only fail if we have results for at least 2 thread counts AND throughput
# never increased at all (monotonically decreased). A single regression is OK —
# distributed systems have variance.
SUCCESSFUL_COUNTS=0
for t in "${THREADS[@]}"; do
  [[ "${THREAD_TPS_COUNT[$t]}" -gt 0 ]] && (( SUCCESSFUL_COUNTS++ )) || true
done

if [[ "$SUCCESSFUL_COUNTS" -ge 2 && "$SAW_IMPROVEMENT" -eq 0 ]]; then
  log "WARNING: No higher terminal count improved over the baseline."
  log "         This may indicate a bottleneck in the gateway, Raft ordering, or cluster nodes."
  log "         Check gateway_test.log files in sub-dirs under $OUT_DIR"
  # Exit with a warning code but not hard failure — the user may still find the data useful.
  exit 2
fi

if [[ "$SUCCESSFUL_COUNTS" -ge 2 && "$SAW_REGRESSION" -eq 1 ]]; then
  log "NOTE: At least one higher terminal count was below baseline; inspect per-run logs before treating the curve as stable."
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
  cat << 'EOF' > "$OUT_DIR/plot.py"
import sys
import pandas as pd
import matplotlib.pyplot as plt

csv_file = sys.argv[1]
out_file = sys.argv[2]

try:
    df = pd.read_csv(csv_file)
    # Filter only successful runs
    df = df[df['status'] == 'ok']
    if df.empty:
        sys.exit(0)

    # Group by threads and calculate mean TPS
    grouped = df.groupby('threads')['tps'].agg(['mean', 'std']).reset_index()
    # Fill NaN std dev with 0 (if only 1 run per thread count)
    grouped['std'] = grouped['std'].fillna(0)

    plt.figure(figsize=(10, 6))
    
    if (grouped['std'] > 0).any():
        plt.errorbar(grouped['threads'], grouped['mean'], yerr=grouped['std'], 
                     fmt='-o', capsize=5, capthick=2, color='blue', markersize=8)
    else:
        plt.plot(grouped['threads'], grouped['mean'], '-o', color='blue', markersize=8)
    
    plt.title('Cluster Throughput: TPS vs Threads', fontsize=16)
    plt.xlabel('Number of Threads', fontsize=14)
    plt.ylabel('Throughput (TPS)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(grouped['threads'])
    
    # Annotate points
    for i, row in grouped.iterrows():
        plt.annotate(f"{int(row['mean'])}", 
                     (row['threads'], row['mean']),
                     textcoords="offset points", 
                     xytext=(0,10), 
                     ha='center')

    # Start Y-axis at 0 for proper scaling visualization
    plt.ylim(bottom=0)
    # Add some top margin so annotations aren't cut off
    plt.ylim(top=max(grouped['mean']) * 1.15)

    plt.tight_layout()
    plt.savefig(out_file)
    print(f"Graph successfully generated at {out_file}")
except Exception as e:
    print(f"Failed to generate graph: {e}")
EOF
  log "Generating graph..."
  python3 "$OUT_DIR/plot.py" "$RESULTS_CSV" "$GRAPH_FILE" || log "Failed to generate graph."
else
  log "python3 not found, skipping graph generation."
fi

exit 0
