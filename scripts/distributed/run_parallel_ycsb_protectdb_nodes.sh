#!/usr/bin/env bash
set -euo pipefail

#
# run_parallel_ycsb_protectdb_nodes.sh
#
# Parallel YCSB benchmark runner for the dram-cache cluster machines
# accessible as protectdb@<ip> (sl1/sl2 lab nodes).
#
# Nodes (default):
#   protectdb@10.130.154.112  sl2-112  (NVMe, 16t)
#
# Usage:
#   scripts/distributed/run_parallel_ycsb_protectdb_nodes.sh [options]
#
#   --modes <det>           Benchmark mode (det|pg|det,pg)
#   --threads <csv>         Thread counts  [default: 1,2,4,8,16]
#   --runs <3>              Runs per workload/thread combination
#   --workloads <csv>       Workload filenames (default: bench_threads_matrix.py defaults)
#   --poll-interval <60>    Seconds between monitoring polls
#   --hang-timeout <1200>   Seconds of no log change before hang warning
#   --skip-sync             Skip rsync phase (reuse last-synced source)
#   --nodes <csv>           Comma-separated IPs to restrict (default: all 7)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

# ---- Node config ----
declare -a ALL_IPS=(
  "10.130.154.112"
)
REMOTE_USER="protectdb"
REMOTE_REPO_BASE="/home/protectdb/ariabc_bench/ariabc_cluster"
REMOTE_INSTALL_BASE="/home/protectdb/ariabc_bench/ariabc_install"
LOCAL_INSTALL_DIR="/work/ARIABC/install"
TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"

# ---- Tunable defaults ----
SSH_PORT=22
MODES="det"
THREADS="1,2,4,8,16"
RUNS=3
WORKLOADS=""
RATES=""
DB_NAME="postgres"
DB_USER="postgres"
DB_PORT=5438
POLL_INTERVAL_S=60
HANG_TIMEOUT_S=1200
SKIP_SYNC=0
FILTER_IPS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --modes)         MODES="${2:-det}"; shift 2 ;;
    --threads)       THREADS="${2:-}"; shift 2 ;;
    --runs)          RUNS="${2:-3}"; shift 2 ;;
    --workloads)     WORKLOADS="${2:-}"; shift 2 ;;
    --rates)         RATES="${2:-}"; shift 2 ;;
    --poll-interval) POLL_INTERVAL_S="${2:-60}"; shift 2 ;;
    --hang-timeout)  HANG_TIMEOUT_S="${2:-1200}"; shift 2 ;;
    --skip-sync)     SKIP_SYNC=1; shift 1 ;;
    --nodes)         FILTER_IPS="${2:-}"; shift 2 ;;
    -h|--help)
      sed -n '/^# Usage/,/^[^#]/p' "$0" | head -n 20; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ---- Helpers ----
_ts()  { date '+%F %T'; }
lmsg() { echo "[$(_ts)] $*"; }

safe_label() {
  local s="$1"
  s="${s//@/_at_}"; s="${s//./_}"; s="${s//-/_}"
  printf '%s' "$s"
}

ssh_run() {
  local node="$1"; shift
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no \
      -o ConnectTimeout=15 -p "$SSH_PORT" "$node" "$@"
}

rsync_e="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT"

# ---- Build active node list ----
declare -a ACTIVE_IPS=()
if [[ -n "$FILTER_IPS" ]]; then
  IFS=',' read -ra ACTIVE_IPS <<< "$FILTER_IPS"
else
  ACTIVE_IPS=("${ALL_IPS[@]}")
fi

# ---- Pre-flight ----
for req in \
  "$TEMPLATE_CONF_LOCAL" \
  "$REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh" \
  "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
  "$REPO_ROOT/scripts/bench_threads_matrix.py" \
  "$REPO_ROOT/scripts/restore_usertable_small.sql"; do
  [[ ! -f "$req" ]] && { echo "ERROR: missing: $req" >&2; exit 1; }
done

ts="$(date +%Y%m%d_%H%M%S)"
LOCAL_RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results/protectdb_ycsb_${ts}"
LOG_DIR="$LOCAL_RESULT_ROOT/_run_logs"
mkdir -p "$LOG_DIR"

lmsg "=== Parallel YCSB — protectdb cluster nodes ==="
lmsg "Timestamp   : $ts"
lmsg "Modes       : $MODES"
lmsg "Threads     : $THREADS"
lmsg "Runs        : $RUNS"
lmsg "Workloads   : ${WORKLOADS:-<defaults>}"
lmsg "Result root : $LOCAL_RESULT_ROOT"
lmsg "Nodes       : ${ACTIVE_IPS[*]}"
echo

# ============================================================
# PHASE 1: PARALLEL SYNC
# ============================================================
declare -A SYNC_STATUS=()

if [[ "$SKIP_SYNC" == "1" ]]; then
  lmsg "=== Phase 1: SKIPPED (--skip-sync) ==="
  for ip in "${ACTIVE_IPS[@]}"; do
    SYNC_STATUS["$ip"]="OK"
  done
else
  lmsg "=== Phase 1: Syncing source to all nodes in parallel ==="
  declare -A SYNC_BGPIDS=()
  declare -A SYNC_LOGS=()

  for ip in "${ACTIVE_IPS[@]}"; do
    node="${REMOTE_USER}@${ip}"
    repo="$REMOTE_REPO_BASE"
    inst="$REMOTE_INSTALL_BASE"
    slog="$LOG_DIR/sync_${ip//./_}.log"
    SYNC_LOGS["$ip"]="$slog"
    SYNC_STATUS["$ip"]="running"

    (
      set -euo pipefail
      echo "[SYNC] $node  repo=$repo  install=$inst" > "$slog"
      echo "Started: $(_ts)" >> "$slog"

      ssh_run "$node" "mkdir -p '$repo' '$inst' '$repo/.bench_tmp' '$repo/.bench_tmp/deps/lib'" >> "$slog" 2>&1

      # Stage OpenSSL headers for Ubuntu 22.04 on-host rebuild
      if [[ -f /usr/include/openssl/sha.h ]]; then
        ssh_run "$node" "mkdir -p '$repo/.bench_tmp/deps/include/openssl'" >> "$slog" 2>&1
        rsync -az --delete -e "$rsync_e" \
          /usr/include/openssl/ "$node:$repo/.bench_tmp/deps/include/openssl/" >> "$slog" 2>&1
        if [[ -d /usr/include/x86_64-linux-gnu/openssl ]]; then
          rsync -az -e "$rsync_e" \
            /usr/include/x86_64-linux-gnu/openssl/ \
            "$node:$repo/.bench_tmp/deps/include/openssl/" >> "$slog" 2>&1
        fi
      fi

      # libcrypto.so shim for nodes missing libssl-dev
      ssh_run "$node" "
        for cand in \
          /usr/lib/x86_64-linux-gnu/libcrypto.so \
          /lib/x86_64-linux-gnu/libcrypto.so \
          /usr/lib/x86_64-linux-gnu/libcrypto.so.3 \
          /lib/x86_64-linux-gnu/libcrypto.so.3; do
          if [[ -e \$cand ]]; then
            ln -sf \"\$cand\" '$repo/.bench_tmp/deps/lib/libcrypto.so'
            break
          fi
        done
      " >> "$slog" 2>&1

      # Sync source tree (exclude large SQL data files and result dirs)
      rsync -az --delete \
        --exclude='.git' --exclude='.venv' --exclude='.bench_tmp' \
        --exclude='__pycache__' --exclude='*.pyc' \
        --exclude='scripts/bench_full_results' --exclude='scripts/bench_results' \
        --exclude='scripts/bench_results_tpcc' \
        --exclude='scripts/big_usertable.sql' \
        --exclude='scripts/big_workload.sql' \
        --exclude='scripts/big_workload_undo.sql' \
        --exclude='scripts/big_workload' \
        -e "$rsync_e" \
        "$REPO_ROOT/" "$node:$repo/" >> "$slog" 2>&1

      # Sync compiled install tree
      rsync -az --delete -e "$rsync_e" \
        /work/ARIABC/install/ "$node:$inst/" >> "$slog" 2>&1

      # Canonical postgresql.conf template — patch en_IN locale to C for portability
      rsync -az -e "$rsync_e" \
        "$TEMPLATE_CONF_LOCAL" "$node:$repo/.bench_tmp/shared_postgresql.conf" >> "$slog" 2>&1
      ssh_run "$node" "sed -i \"s/lc_messages = 'en_IN'/lc_messages = 'C'/g;
                               s/lc_monetary = 'en_IN'/lc_monetary = 'C'/g;
                               s/lc_numeric  = 'en_IN'/lc_numeric  = 'C'/g;
                               s/lc_numeric = 'en_IN'/lc_numeric = 'C'/g;
                               s/lc_time = 'en_IN'/lc_time = 'C'/g\" \
                       '$repo/.bench_tmp/shared_postgresql.conf'" >> "$slog" 2>&1

      # Sync psycopg wheels (both py3.10 and py3.12 binary builds)
      ssh_run "$node" "mkdir -p '$repo/.bench_tmp/psycopg_wheels'" >> "$slog" 2>&1
      rsync -az -e "$rsync_e" \
        /tmp/psycopg_wheels/ "$node:$repo/.bench_tmp/psycopg_wheels/" >> "$slog" 2>&1
      # Also sync local py3.12 venv psycopg package dirs for direct copy on 3.12 machines
      rsync -az -e "$rsync_e" \
        /tmp/psycopg_wheels/ "$node:$repo/.bench_tmp/psycopg_wheels/" >> "$slog" 2>&1

      echo "Finished OK: $(_ts)" >> "$slog"
    ) &
    SYNC_BGPIDS["$ip"]=$!
    lmsg "  Started sync for $ip (bg PID=${SYNC_BGPIDS[$ip]})"
  done

  lmsg "  Waiting for all syncs..."
  for ip in "${!SYNC_BGPIDS[@]}"; do
    if wait "${SYNC_BGPIDS[$ip]}" 2>/dev/null; then
      SYNC_STATUS["$ip"]="OK"
      lmsg "  [OK]   $ip"
    else
      SYNC_STATUS["$ip"]="FAIL"
      lmsg "  [FAIL] $ip – see ${SYNC_LOGS[$ip]}"
    fi
  done
fi

echo

# ============================================================
# PHASE 2: PARALLEL BENCH LAUNCH
# ============================================================
lmsg "=== Phase 2: Launching benchmarks on all nodes in parallel ==="

declare -A NODE_PID=()
declare -A NODE_LOG=()
declare -A NODE_REMOTE_OUT=()
declare -A NODE_STATUS=()
declare -A NODE_LAST_HASH=()
declare -A NODE_LAST_CHG=()

_extra_flags() {
  local e=""
  [[ -n "$WORKLOADS" ]] && e+=" --workloads '$WORKLOADS'"
  [[ -n "$RATES" ]]     && e+=" --rates '$RATES'"
  printf '%s' "$e"
}

for ip in "${ACTIVE_IPS[@]}"; do
  node="${REMOTE_USER}@${ip}"
  repo="$REMOTE_REPO_BASE"
  inst="$REMOTE_INSTALL_BASE"

  NODE_STATUS["$ip"]="pending"
  NODE_LAST_HASH["$ip"]=""
  NODE_LAST_CHG["$ip"]="$(date +%s)"

  sync_ok="${SYNC_STATUS[$ip]:-FAIL}"
  if [[ "$sync_ok" != "OK" ]]; then
    lmsg "  [SKIP] $ip – sync failed"
    NODE_STATUS["$ip"]="skipped"
    continue
  fi

  safe_ip="${ip//./_}"
  remote_out="$repo/scripts/bench_results/protectdb_ycsb_${safe_ip}_${ts}"
  remote_log="$repo/.bench_tmp/bench_protectdb_${ts}.log"
  NODE_LOG["$ip"]="$remote_log"
  NODE_REMOTE_OUT["$ip"]="$remote_out"

  local_node_dir="$LOCAL_RESULT_ROOT/protectdb_at_${safe_ip}"
  mkdir -p "$local_node_dir"

  extra="$(_extra_flags)"
  remote_cmd="set -euo pipefail
mkdir -p '$remote_out' '$repo/.bench_tmp'
cd '$repo/scripts'
bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
  --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$inst/bin/psql'
export ARIABC_INSTALL_DIR='$inst'
export ARIABC_DIR='$repo'
export ARIABC_PGPORT='$DB_PORT'
export LD_LIBRARY_PATH='$inst/lib:\${LD_LIBRARY_PATH:-}'
pgdata_line=\$(bash '$repo/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$repo' --install-dir '$inst' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$repo/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] && export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
WHEELS_DIR='$repo/.bench_tmp/psycopg_wheels'
if ! python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYVER=\$(python3 -c 'import sys; print(f\"{sys.version_info.major}{sys.version_info.minor}\")')
  USERSITE=\$(python3 -c 'import site; print(site.getusersitepackages())' 2>/dev/null || \
             python3 -c 'import sysconfig; print(sysconfig.get_path(\"purelib\"))')
  mkdir -p \"\$USERSITE\"
  if [[ \"\$PYVER\" == \"312\" && -d \"\$WHEELS_DIR/psycopg\" ]]; then
    cp -a \"\$WHEELS_DIR/psycopg\" \"\$USERSITE/\" 2>/dev/null || true
    cp -a \"\$WHEELS_DIR/psycopg_binary\" \"\$USERSITE/\" 2>/dev/null || true
    cp -a \"\$WHEELS_DIR/psycopg_binary.libs\" \"\$USERSITE/\" 2>/dev/null || true
  fi
  if ! python3 -c 'import psycopg' >/dev/null 2>&1; then
    pip3 install -q --user --no-index --find-links \"\$WHEELS_DIR\" psycopg psycopg_binary 2>/dev/null || \
    pip3 install -q --user --no-index --find-links \"\$WHEELS_DIR\" --break-system-packages psycopg psycopg_binary 2>/dev/null || true
  fi
fi
if ! python3 -c 'import psycopg' >/dev/null 2>&1; then
  echo 'ERROR: psycopg unavailable after offline wheel install' >&2; exit 2
fi
PYTHON_BIN=python3
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes '$MODES' --threads '$THREADS' --runs '$RUNS' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out'${extra}"

  launch_err="$LOG_DIR/launch_err_${safe_ip}.log"
  remote_pid=$(ssh_run "$node" "
    mkdir -p '$repo/.bench_tmp' '$remote_out'
    nohup bash -lc $(printf '%q' "$remote_cmd") > '$remote_log' 2>&1 &
    echo \$!
  " 2>"$launch_err" | tail -n 1) || true

  if [[ -z "$remote_pid" ]] || ! [[ "$remote_pid" =~ ^[0-9]+$ ]]; then
    lmsg "  [FAIL] $ip – bad remote PID ('$remote_pid') – see $launch_err"
    NODE_STATUS["$ip"]="failed"
    continue
  fi

  NODE_PID["$ip"]="$remote_pid"
  NODE_STATUS["$ip"]="running"
  lmsg "  [LAUNCHED] $ip  remote_pid=$remote_pid  log=$remote_log"
done

echo
running_at_start=0
for ip in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$ip]}" == "running" ]] && ((running_at_start++)) || true
done

[[ "$running_at_start" -eq 0 ]] && {
  lmsg "ERROR: No nodes launched." >&2; exit 1
}

lmsg "All benchmarks launched ($running_at_start running). Polling every ${POLL_INTERVAL_S}s."
echo

# ============================================================
# PHASE 3: MONITOR LOOP
# ============================================================

_active_count() {
  local c=0
  for ip in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$ip]}" == "running" ]] && ((c++)) || true
  done
  echo "$c"
}

_collect_node() {
  local ip="$1"
  local remote_out="${NODE_REMOTE_OUT[$ip]}"
  local node="${REMOTE_USER}@${ip}"
  local safe_ip="${ip//./_}"
  local local_node_dir="$LOCAL_RESULT_ROOT/protectdb_at_${safe_ip}"
  mkdir -p "$local_node_dir"

  rsync -az -e "$rsync_e" "$node:$remote_out/" "$local_node_dir/" 2>/dev/null || \
    lmsg "  WARNING: rsync of results failed for $ip"

  if [[ -f "$local_node_dir/results.csv" && -f "$local_node_dir/summary.csv" ]]; then
    NODE_STATUS["$ip"]="done"
    lmsg "  [DONE] $ip – results → $local_node_dir"
    _gen_graphs "$local_node_dir" "$ip"
  else
    NODE_STATUS["$ip"]="failed"
    lmsg "  [FAIL] $ip – missing results.csv/summary.csv in $local_node_dir"
  fi
}

_gen_graphs() {
  local out_dir="$1" node="$2"
  [[ -f "$out_dir/summary.csv" ]] || return 0
  [[ "$(find "$out_dir" -maxdepth 1 -name '*.png' | wc -l)" -gt 0 ]] && return 0
  ARIABC_REPO_ROOT="$REPO_ROOT" python3 - "$out_dir/summary.csv" "$out_dir" <<'PYEOF' \
    && lmsg "  [GRAPH] $node – graphs generated" \
    || lmsg "  [GRAPH] WARNING: graph generation failed for $node"
import importlib.util, os, sys
from pathlib import Path
repo = os.environ["ARIABC_REPO_ROOT"]
spec = importlib.util.spec_from_file_location("btm", f"{repo}/scripts/bench_threads_matrix.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
paths = mod._generate_tps_graphs(Path(sys.argv[1]), Path(sys.argv[2]))
print(f"  generated {len(paths)} graph(s)")
PYEOF
}

_check_node() {
  local ip="$1"
  local node="${REMOTE_USER}@${ip}"
  local pid="${NODE_PID[$ip]}"
  local bench_log="${NODE_LOG[$ip]}"

  local alive=0
  ssh_run "$node" "kill -0 $pid 2>/dev/null" 2>/dev/null && alive=1 || true

  if [[ "$alive" == "1" ]]; then
    local tail_out=""
    tail_out=$(ssh_run "$node" "tail -10 '$bench_log' 2>/dev/null || true" 2>/dev/null || true)

    local hash; hash=$(printf '%s' "$tail_out" | md5sum | cut -d' ' -f1)
    local now; now="$(date +%s)"
    if [[ "$hash" != "${NODE_LAST_HASH[$ip]:-}" ]]; then
      NODE_LAST_HASH["$ip"]="$hash"
      NODE_LAST_CHG["$ip"]="$now"
    else
      local stale=$(( now - NODE_LAST_CHG["$ip"] ))
      [[ "$stale" -ge "$HANG_TIMEOUT_S" ]] && \
        echo "  *** HANG WARNING: $ip – no log change for ${stale}s (pid=$pid) ***"
    fi

    echo "  -- $ip [RUNNING pid=$pid] --"
    [[ -n "$tail_out" ]] && \
      while IFS= read -r line; do echo "     $line"; done <<< "$tail_out" || \
      echo "     (no output yet)"
  else
    lmsg "  Process finished on $ip – collecting..."
    _collect_node "$ip"
  fi
}

while [[ "$(_active_count)" -gt 0 ]]; do
  echo "=========================================="
  echo "  Poll $(_ts)  |  Running: $(_active_count)"
  echo "=========================================="
  for ip in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$ip]}" == "running" ]] || continue
    _check_node "$ip"
  done

  running_nodes=(); done_nodes=(); fail_nodes=()
  for ip in "${!NODE_STATUS[@]}"; do
    case "${NODE_STATUS[$ip]}" in
      running) running_nodes+=("$ip") ;;
      done)    done_nodes+=("$ip") ;;
      failed)  fail_nodes+=("$ip") ;;
    esac
  done
  echo
  echo "  Running (${#running_nodes[@]}): ${running_nodes[*]:-—}"
  echo "  Done    (${#done_nodes[@]}):    ${done_nodes[*]:-—}"
  echo "  Failed  (${#fail_nodes[@]}):    ${fail_nodes[*]:-—}"
  echo
  [[ "$(_active_count)" -gt 0 ]] && sleep "$POLL_INTERVAL_S"
done

# ============================================================
# PHASE 4: FINAL REPORT
# ============================================================
lmsg "=== Phase 4: All nodes finished ==="
echo

printf "%-20s %-8s %s\n" "Node (IP)" "Status" "Local results dir"
printf "%-20s %-8s %s\n" "---------" "------" "-----------------"
for ip in "${!NODE_STATUS[@]}"; do
  safe_ip="${ip//./_}"
  local_node_dir="$LOCAL_RESULT_ROOT/protectdb_at_${safe_ip}"
  printf "%-20s %-8s %s\n" "$ip" "${NODE_STATUS[$ip]}" "$local_node_dir"
done
echo
lmsg "Logs:    $LOG_DIR"
lmsg "Results: $LOCAL_RESULT_ROOT"
