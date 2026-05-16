#!/usr/bin/env bash
set -euo pipefail

#
# run_three_mode_bench_all_nodes.sh
#
# Syncs the updated AriaBC source + install to all lab nodes, then runs
# run_three_mode_bench.sh on EVERY node IN PARALLEL.
#
# The three benchmark modes are:
#   paper      – bcdb_serial_gate_source=1, bcdb_advance_commit_watermark=off
#                (paper Algorithm 1: gate on last_committed before conflict-check)
#   leverd     – bcdb_serial_gate_source=0, bcdb_advance_commit_watermark=off
#                (Lever D v2: published_max gate + blocking finish)
#   leverd_t3  – bcdb_serial_gate_source=0, bcdb_advance_commit_watermark=on
#                (Lever D v2 + T3 CAS finish — current default)
#
# Usage:
#   bash run_three_mode_bench_all_nodes.sh [OPTIONS]
#
#   --threads <1,4,8,12,16>  Thread counts csv
#   --runs <1>               Runs per cell
#   --workloads <csv>        Workload filenames
#   --poll-interval <60>     Seconds between monitoring polls
#   --hang-timeout <1800>    Seconds of no log change before hang warning
#   --skip-sync              Skip rsync phase (reuse last-synced source)
#   --no-pg-baseline         Skip plain PG baseline pass
#   --ssh-key <path>         SSH private key (optional)
#   --ssh-port <22>          SSH port
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

# ---- Static node config (same as run_parallel_ycsb_all_nodes.sh) ----
NEEL_NODES=(
  "neel@10.129.148.248"
  "neel@10.129.27.54"
  "neel@10.129.148.236"
  "neel@10.129.148.179"
)
NEEL_REMOTE_REPO="/home/neel/Desktop/ariabc_cluster"
NEEL_REMOTE_INSTALL="/home/neel/Desktop/ariabc_install"
TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"

declare -A NODE_PASSWORDS=()

# ---- Tunable defaults ----
SSH_KEY=""
SSH_PORT=22
THREADS="1,4,8,12,16"
RUNS=1
WORKLOADS=""
POLL_INTERVAL_S=60
HANG_TIMEOUT_S=1800
SKIP_SYNC=0
NO_PG_BASELINE=0
DB_PORT=5438
DB_NAME="postgres"
DB_USER="postgres"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh-key)        SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port)       SSH_PORT="${2:-22}"; shift 2 ;;
    --threads)        THREADS="${2:-}"; shift 2 ;;
    --runs)           RUNS="${2:-1}"; shift 2 ;;
    --workloads)      WORKLOADS="${2:-}"; shift 2 ;;
    --poll-interval)  POLL_INTERVAL_S="${2:-60}"; shift 2 ;;
    --hang-timeout)   HANG_TIMEOUT_S="${2:-1800}"; shift 2 ;;
    --skip-sync)      SKIP_SYNC=1; shift 1 ;;
    --no-pg-baseline) NO_PG_BASELINE=1; shift 1 ;;
    --port)           DB_PORT="${2:-5438}"; shift 2 ;;
    --db)             DB_NAME="${2:-postgres}"; shift 2 ;;
    --user)           DB_USER="${2:-postgres}"; shift 2 ;;
    -h|--help)
      sed -n '/^# Usage/,/^[^#]/p' "$0" | head -20
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ---- Helpers ----
_ts()   { date '+%F %T'; }
lmsg()  { echo "[$(_ts)] $*"; }

safe_label() {
  local s="$1"
  s="${s//@/_at_}"; s="${s//./_}"; s="${s//-/_}"
  printf '%s' "$s"
}

_node_password() { printf '%s' "${NODE_PASSWORDS[$1]:-}"; }

ssh_run() {
  local node="$1"; shift
  local pass; pass="$(_node_password "$node")"
  local ssh_arr=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT")
  [[ -n "$SSH_KEY" ]] && ssh_arr+=(-i "$SSH_KEY")
  if [[ -n "$pass" ]]; then
    sshpass -p "$pass" "${ssh_arr[@]/BatchMode=yes/}" "$node" "$@"
  else
    "${ssh_arr[@]}" "$node" "$@"
  fi
}

_rsync_e_for_node() {
  local node="$1"
  local pass; pass="$(_node_password "$node")"
  local e="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT"
  if [[ -n "$pass" ]]; then
    [[ -n "$SSH_KEY" ]] && e+=" -i $SSH_KEY"
    printf 'sshpass -p %q %s' "$pass" "$e"
  else
    e+=" -o BatchMode=yes"
    [[ -n "$SSH_KEY" ]] && e+=" -i $SSH_KEY"
    printf '%s' "$e"
  fi
}

_rsync_allow_vanished() {
  local rc
  if rsync "$@"; then return 0; fi
  rc=$?
  [[ "$rc" -eq 24 ]] && return 0
  return "$rc"
}

ts="$(date +%Y%m%d_%H%M%S)"
LOCAL_RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results/three_mode_${ts}"
LOG_DIR="$LOCAL_RESULT_ROOT/_run_logs"
mkdir -p "$LOG_DIR"

lmsg "=== Three-mode bench — all nodes ==="
lmsg "Timestamp    : $ts"
lmsg "Threads      : $THREADS"
lmsg "Runs         : $RUNS"
lmsg "Workloads    : ${WORKLOADS:-<defaults>}"
lmsg "Result root  : $LOCAL_RESULT_ROOT"
echo

# ---- Build node registry ----
declare -a ALL_NODES=()
for n in "${NEEL_NODES[@]}"; do
  ALL_NODES+=("$n|${NEEL_REMOTE_REPO}|${NEEL_REMOTE_INSTALL}|neel")
done

# ---- Node tracking ----
declare -A NODE_PID=()
declare -A NODE_LOG=()
declare -A NODE_REMOTE_OUT=()
declare -A NODE_TYPE=()
declare -A NODE_REPO=()
declare -A NODE_INST=()
declare -A NODE_STATUS=()
declare -A NODE_LAST_HASH=()
declare -A NODE_LAST_CHG=()
declare -A NODE_RSYNC_PID=()

_abort_run() {
  local reason="$1"; shift
  echo ""; echo "BENCHMARK ABORTED: $reason"
  for n in "$@"; do echo "  ✗  $n"; done
  for n in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$n]:-}" != "running" ]] && continue
    local pid="${NODE_PID[$n]:-}"
    [[ -z "$pid" ]] && continue
    ssh_run "$n" "kill $pid 2>/dev/null" 2>/dev/null || true
  done
  exit 1
}

# ============================================================
# PREFLIGHT
# ============================================================
lmsg "=== Preflight: SSH reachability ==="
declare -a _unreachable=()
for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  if ssh_run "$node" "echo alive" >/dev/null 2>&1; then
    lmsg "  [OK]          $node"
  else
    lmsg "  [UNREACHABLE] $node"
    _unreachable+=("$node")
  fi
done
if [[ "${#_unreachable[@]}" -gt 0 ]]; then
  _abort_run "${#_unreachable[@]} node(s) unreachable" "${_unreachable[@]}"
fi
echo

# ============================================================
# PHASE 0: KILL STALE PROCESSES + WIPE OLD PGDATA
# ============================================================
lmsg "=== Phase 0: Kill stale processes and wipe old pgdata ==="
for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  ssh_run "$node" "
    pkill -f 'bench_threads_matrix.py' 2>/dev/null || true
    pkill -f 'run_three_mode_bench' 2>/dev/null || true
    pkill -f 'generic-saicopg' 2>/dev/null || true
    # Clean stop of any running postgres on bench port to avoid corruption
    pgd='$repo/.bench_tmp/single_node_pgdata'
    if [[ -x '$inst/bin/pg_ctl' && -d \"\$pgd\" ]]; then
      '$inst/bin/pg_ctl' -D \"\$pgd\" stop -m fast -w -t 20 2>/dev/null || true
    fi
    # Wipe pgdata so each run starts fresh (avoids stale WAL/checkpoint)
    rm -rf \"\$pgd\"
  " 2>/dev/null || true
  lmsg "  [CLEAN] $node"
done
sleep 2
echo

# ============================================================
# PHASE 1: SYNC
# ============================================================
declare -A SYNC_STATUS=()

if [[ "$SKIP_SYNC" == "1" ]]; then
  lmsg "=== Phase 1: SKIPPED (--skip-sync) ==="
  for entry in "${ALL_NODES[@]}"; do
    IFS='|' read -r node repo inst type <<< "$entry"
    SYNC_STATUS["$node"]="OK"
  done
else
  lmsg "=== Phase 1: Syncing source + install to all nodes ==="
  declare -A SYNC_BGPIDS=()
  declare -A SYNC_LOGS=()

  for entry in "${ALL_NODES[@]}"; do
    IFS='|' read -r node repo inst type <<< "$entry"
    slog="$LOG_DIR/sync_$(safe_label "$node").log"
    SYNC_LOGS["$node"]="$slog"
    SYNC_STATUS["$node"]="running"

    (
      set -euo pipefail
      echo "[SYNC] $node" > "$slog"
      ssh_run "$node" "mkdir -p '$repo' '$inst' '$repo/.bench_tmp'" >> "$slog" 2>&1

      # Sync source — exclude .bench_tmp so we don't clobber remote build artifacts
      _rsync_allow_vanished -az --delete \
        --exclude='.git' --exclude='.venv' --exclude='__pycache__' \
        --exclude='*.pyc' --exclude='.bench_tmp' \
        --exclude='scripts/big_usertable.sql' \
        --exclude='scripts/bench_full_results' \
        --exclude='scripts/bench_results' \
        -e "$(_rsync_e_for_node "$node")" \
        "$REPO_ROOT/" "$node:$repo/" >> "$slog" 2>&1

      # Sync install
      _rsync_allow_vanished -az --delete \
        -e "$(_rsync_e_for_node "$node")" \
        /work/ARIABC/install/ "$node:$inst/" >> "$slog" 2>&1

      # Sync postgresql.conf template
      if [[ -f "$TEMPLATE_CONF_LOCAL" ]]; then
        _rsync_allow_vanished -az \
          -e "$(_rsync_e_for_node "$node")" \
          "$TEMPLATE_CONF_LOCAL" "$node:$repo/.bench_tmp/shared_postgresql.conf" >> "$slog" 2>&1
      fi

      # Stage OpenSSL headers AFTER source sync (Ubuntu 22.04 on-host rebuild)
      # Must be after source sync to avoid being deleted by --delete flag
      ssh_run "$node" "mkdir -p '$repo/.bench_tmp/deps/lib'" >> "$slog" 2>&1
      if [[ -f /usr/include/openssl/sha.h ]]; then
        ssh_run "$node" "mkdir -p '$repo/.bench_tmp/deps/include/openssl'" >> "$slog" 2>&1
        _rsync_allow_vanished -az --delete \
          -e "$(_rsync_e_for_node "$node")" \
          /usr/include/openssl/ "$node:$repo/.bench_tmp/deps/include/openssl/" >> "$slog" 2>&1
        if [[ -d /usr/include/x86_64-linux-gnu/openssl ]]; then
          _rsync_allow_vanished -az \
            -e "$(_rsync_e_for_node "$node")" \
            /usr/include/x86_64-linux-gnu/openssl/ \
            "$node:$repo/.bench_tmp/deps/include/openssl/" >> "$slog" 2>&1
        fi
      fi

      # libcrypto.so symlink shim for nodes missing libssl-dev
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

      echo "Finished OK: $(_ts)" >> "$slog"
    ) &
    SYNC_BGPIDS["$node"]=$!
    lmsg "  Started sync for $node (bg PID=${SYNC_BGPIDS[$node]})"
  done

  lmsg "  Waiting for syncs..."
  for node in "${!SYNC_BGPIDS[@]}"; do
    if wait "${SYNC_BGPIDS[$node]}" 2>/dev/null; then
      SYNC_STATUS["$node"]="OK"; lmsg "  [OK]   $node"
    else
      SYNC_STATUS["$node"]="FAIL"; lmsg "  [FAIL] $node — see ${SYNC_LOGS[$node]}"
    fi
  done

  declare -a _sync_failed=()
  for node in "${!SYNC_STATUS[@]}"; do
    [[ "${SYNC_STATUS[$node]}" != "OK" ]] && _sync_failed+=("$node")
  done
  [[ "${#_sync_failed[@]}" -gt 0 ]] && _abort_run "Sync failed on ${#_sync_failed[@]} node(s)" "${_sync_failed[@]}"
fi
echo

# ============================================================
# PHASE 2: LAUNCH BENCHMARKS IN PARALLEL
# ============================================================
lmsg "=== Phase 2: Launching three-mode bench on all nodes ==="

for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"

  NODE_REPO["$node"]="$repo"
  NODE_INST["$node"]="$inst"
  NODE_TYPE["$node"]="$type"
  NODE_STATUS["$node"]="pending"
  NODE_LAST_HASH["$node"]=""
  NODE_LAST_CHG["$node"]="$(date +%s)"

  safe_node="$(safe_label "$node")"
  local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  mkdir -p "$local_node_dir"

  sync_ok="${SYNC_STATUS[$node]:-FAIL}"
  if [[ "$sync_ok" != "OK" ]]; then
    lmsg "  [SKIP] $node – sync failed"
    NODE_STATUS["$node"]="skipped"
    continue
  fi

  remote_out="$repo/scripts/bench_results/three_mode_$(safe_label "$node")_${ts}"
  remote_log="$repo/.bench_tmp/three_mode_bench_${ts}.log"
  NODE_LOG["$node"]="$remote_log"
  NODE_REMOTE_OUT["$node"]="$remote_out"

  no_pg_flag=""; [[ "$NO_PG_BASELINE" == "1" ]] && no_pg_flag="--no-pg-baseline"
  wl_flag=""; [[ -n "$WORKLOADS" ]] && wl_flag="--workloads '$WORKLOADS'"

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
PYTHON_BIN=python3
if [[ -x '$repo/.venv/bin/python' ]] && '$repo/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$repo/.venv/bin/python'
fi
export ARIABC_PYTHON=\"\$PYTHON_BIN\"
bash '$repo/scripts/distributed/run_three_mode_bench.sh' \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --out-dir '$remote_out' \
  --port '$DB_PORT' \
  --db '$DB_NAME' \
  --user '$DB_USER' \
  $no_pg_flag $wl_flag"

  launch_err="$LOG_DIR/launch_err_$(safe_label "$node").log"
  remote_pid=$(ssh_run "$node" "
    mkdir -p '$repo/.bench_tmp' '$remote_out'
    nohup bash -lc $(printf '%q' "$remote_cmd") > '$remote_log' 2>&1 &
    echo \$!
  " 2>"$launch_err" | tail -n 1) || true

  if [[ -z "$remote_pid" ]] || ! [[ "$remote_pid" =~ ^[0-9]+$ ]]; then
    lmsg "  [FAIL] $node – no remote PID (got: '$remote_pid') — see $launch_err"
    NODE_STATUS["$node"]="failed"
    continue
  fi

  NODE_PID["$node"]="$remote_pid"
  NODE_STATUS["$node"]="running"
  lmsg "  [LAUNCHED] $node  pid=$remote_pid  log=$remote_log"
done
echo

declare -a _launch_failed=()
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" == "failed" ]] && _launch_failed+=("$node")
done
if [[ "${#_launch_failed[@]}" -gt 0 ]]; then
  lmsg "WARNING: Launch failed on ${#_launch_failed[@]} node(s): ${_launch_failed[*]} — continuing with remaining nodes"
fi

_active_count() {
  local c=0
  for n in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$n]}" == "running" ]] && ((c++)) || true
  done; echo "$c"
}

running_at_start="$(_active_count)"
[[ "$running_at_start" -eq 0 ]] && { lmsg "ERROR: No nodes launched successfully"; exit 1; }
lmsg "All benchmarks launched ($running_at_start running). Monitoring every ${POLL_INTERVAL_S}s."
echo

# ============================================================
# PHASE 3: MONITOR
# ============================================================
_live_sync_node() {
  local node="$1"
  local remote_out="${NODE_REMOTE_OUT[$node]}"
  local safe_node; safe_node="$(safe_label "$node")"
  local local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  local prev_pid="${NODE_RSYNC_PID[$node]:-}"
  [[ -n "$prev_pid" ]] && kill -0 "$prev_pid" 2>/dev/null && return 0
  mkdir -p "$local_node_dir"
  (
    ssh_run "$node" "test -d '$remote_out'" 2>/dev/null || exit 0
    rsync -az -e "$(_rsync_e_for_node "$node")" \
      "$node:$remote_out/" "$local_node_dir/" >/dev/null 2>&1
  ) &
  NODE_RSYNC_PID["$node"]=$!
}

_collect_node() {
  local node="$1"
  local remote_out="${NODE_REMOTE_OUT[$node]}"
  local safe_node; safe_node="$(safe_label "$node")"
  local local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  mkdir -p "$local_node_dir"
  rsync -az -e "$(_rsync_e_for_node "$node")" \
    "$node:$remote_out/" "$local_node_dir/" 2>/dev/null || \
    lmsg "  WARNING: final rsync failed for $node"
  if [[ -f "$local_node_dir/combined_results.csv" ]]; then
    NODE_STATUS["$node"]="done"
    lmsg "  [DONE] $node → $local_node_dir"
  else
    NODE_STATUS["$node"]="failed"
    lmsg "  [FAIL] $node — missing combined_results.csv in $local_node_dir"
  fi
}

_check_node() {
  local node="$1"
  local pid="${NODE_PID[$node]}"
  local bench_log="${NODE_LOG[$node]}"
  local alive=0
  ssh_run "$node" "kill -0 $pid 2>/dev/null" 2>/dev/null && alive=1 || true

  if [[ "$alive" == "1" ]]; then
    local tail_out=""
    tail_out=$(ssh_run "$node" "tail -8 '$bench_log' 2>/dev/null || true" 2>/dev/null || true)
    local hash; hash=$(printf '%s' "$tail_out" | md5sum | cut -d' ' -f1)
    local prev="${NODE_LAST_HASH[$node]:-}"
    local now; now="$(date +%s)"
    if [[ "$hash" != "$prev" ]]; then
      NODE_LAST_HASH["$node"]="$hash"
      NODE_LAST_CHG["$node"]="$now"
    else
      local stale=$(( now - NODE_LAST_CHG["$node"] ))
      [[ "$stale" -ge "$HANG_TIMEOUT_S" ]] && echo "  *** HANG WARNING: $node — no change for ${stale}s ***"
    fi
    echo "  -- $node [RUNNING pid=$pid] --"
    [[ -n "$tail_out" ]] && while IFS= read -r line; do echo "     $line"; done <<< "$tail_out"
    _live_sync_node "$node"
  else
    lmsg "  Process finished on $node — collecting..."
    local prev_pid="${NODE_RSYNC_PID[$node]:-}"
    [[ -n "$prev_pid" ]] && wait "$prev_pid" 2>/dev/null || true
    _collect_node "$node"
  fi
}

while [[ "$(_active_count)" -gt 0 ]]; do
  echo "=============================="
  echo "  Poll $(_ts) | Running: $(_active_count)"
  echo "=============================="
  for node in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$node]}" == "running" ]] && _check_node "$node"
  done
  running_nodes=(); done_nodes=(); fail_nodes=()
  for node in "${!NODE_STATUS[@]}"; do
    case "${NODE_STATUS[$node]}" in
      running) running_nodes+=("$node") ;;
      done)    done_nodes+=("$node") ;;
      failed)  fail_nodes+=("$node") ;;
    esac
  done
  echo "  Running(${#running_nodes[@]}): ${running_nodes[*]:-—}"
  echo "  Done   (${#done_nodes[@]}):    ${done_nodes[*]:-—}"
  echo "  Failed (${#fail_nodes[@]}):    ${fail_nodes[*]:-—}"
  if [[ "${#fail_nodes[@]}" -gt 0 ]]; then
    lmsg "WARNING: ${#fail_nodes[@]} node(s) failed — continuing with remaining nodes: ${fail_nodes[*]}"
  fi
  [[ "$(_active_count)" -gt 0 ]] && sleep "$POLL_INTERVAL_S"
done

# ============================================================
# PHASE 4: COLLECT + CROSS-NODE GRAPHS
# ============================================================
lmsg "=== Phase 4: All nodes finished ==="
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" != "done" ]] && continue
  local_node_dir="$LOCAL_RESULT_ROOT/$(safe_label "$node")"
  [[ ! -f "$local_node_dir/combined_results.csv" ]] && _collect_node "$node"
done

echo "=== Final Status ==="
printf "%-52s %-8s %s\n" "Node" "Status" "Local dir"
for node in "${!NODE_STATUS[@]}"; do
  printf "%-52s %-8s %s\n" "$node" "${NODE_STATUS[$node]}" "$LOCAL_RESULT_ROOT/$(safe_label "$node")"
done
echo

# Cross-node combined graph
lmsg "=== Generating cross-node comparison graphs ==="
PLOT_PY="$SCRIPT_DIR/plot_three_modes_comparison.py"

# Merge all per-node combined_results.csv into one master CSV
MASTER_CSV="$LOCAL_RESULT_ROOT/all_nodes_combined.csv"
python3 - "$LOCAL_RESULT_ROOT" "$MASTER_CSV" <<'PYEOF'
import csv, sys
from pathlib import Path

root = Path(sys.argv[1])
out_csv = Path(sys.argv[2])
all_rows = []
fieldnames = None

for node_dir in sorted(root.iterdir()):
    if not node_dir.is_dir():
        continue
    csv_path = node_dir / "combined_results.csv"
    if not csv_path.exists():
        continue
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        if fieldnames is None:
            fieldnames = list(reader.fieldnames or [])
            if "node" not in fieldnames:
                fieldnames = ["node"] + fieldnames
        for row in reader:
            if "node" not in row:
                row["node"] = node_dir.name
            all_rows.append(row)

if not all_rows:
    print("WARNING: no rows to merge", file=sys.stderr)
    sys.exit(0)

with out_csv.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)
print(f"Master CSV: {len(all_rows)} rows → {out_csv}")
PYEOF

if [[ -f "$PLOT_PY" && -f "$MASTER_CSV" ]]; then
  python3 "$PLOT_PY" "$MASTER_CSV" "$LOCAL_RESULT_ROOT" \
    && lmsg "Graphs: $LOCAL_RESULT_ROOT" \
    || lmsg "WARN: graph generation failed"
fi

# Also generate per-node graphs
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" != "done" ]] && continue
  local_node_dir="$LOCAL_RESULT_ROOT/$(safe_label "$node")"
  csv_path="$local_node_dir/combined_results.csv"
  [[ ! -f "$csv_path" ]] && continue
  if [[ -f "$PLOT_PY" ]]; then
    python3 "$PLOT_PY" "$csv_path" "$local_node_dir" \
      && lmsg "Per-node graphs: $local_node_dir" \
      || lmsg "WARN: per-node graph failed for $node"
  fi
done

lmsg "Logs:    $LOG_DIR"
lmsg "Results: $LOCAL_RESULT_ROOT"
lmsg "=== Done ==="
