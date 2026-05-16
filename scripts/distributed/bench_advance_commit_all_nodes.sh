#!/usr/bin/env bash
set -euo pipefail

#
# bench_advance_commit_all_nodes.sh
#
# Benchmarks bcdb_advance_commit_watermark ON vs OFF on all 4 lab nodes,
# collecting one result dir per node per GUC state, then generating
# 4 per-node comparison graphs.
#
# GUC: bcdb_advance_commit_watermark
#   ON  (default) — advance_last_committed_txid()  lock-free CAS
#   OFF            — bcdb_wait_for_prev_committed() + set_last_committed_txid()
#
# Usage:
#   bash scripts/distributed/bench_advance_commit_all_nodes.sh [options]
#
#   --threads <1,4,8,12,16,24>    Thread counts csv
#   --runs <2>                    Runs per config
#   --skip-sync                   Skip rsync (reuse last sync)
#   --skip-local                  Don't include local node
#   --out-dir <path>              Local result root (default: bench_full_results/advance_commit_<ts>)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

REMOTE_REPO="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL="/home/neel/Desktop/ariabc_install"
LOCAL_INSTALL="/work/ARIABC/install"
TEMPLATE_CONF="/work/ARIABC/pgdata/postgresql.conf"

NEEL_NODES=(
  "neel@10.129.148.236"
  "neel@10.129.27.54"
  "neel@10.129.148.179"
  "neel@10.129.148.248"
)

THREADS="1,4,8,12,16,24"
RUNS=2
MODES="det"
SIGNING_MODES="0"
DB_PORT=5438
DB_USER="postgres"
DB_NAME="postgres"
SKIP_SYNC=0
SKIP_REBUILD=0
SKIP_LOCAL=1
POLL_INTERVAL_S=120
OUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)     THREADS="${2:-}";  shift 2 ;;
    --runs)        RUNS="${2:-2}";    shift 2 ;;
    --skip-sync)     SKIP_SYNC=1;    shift   ;;
    --skip-rebuild)  SKIP_REBUILD=1; shift   ;;
    --skip-local)    SKIP_LOCAL=1;   shift   ;;
    --out-dir)     OUT_DIR="${2:-}";  shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

ts="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/scripts/bench_full_results/advance_commit_${ts}}"
LOG_DIR="$OUT_DIR/_logs"
mkdir -p "$LOG_DIR"

_ts()  { date '+%F %T'; }
log()  { echo "[$(_ts)] $*"; }
err()  { echo "[$(_ts)] ERROR: $*" >&2; }

ssh_run() {
  local node="$1"; shift
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 "$node" "$@"
}

safe() {
  local s="$1"
  s="${s//@/_}"; s="${s//./_}"; s="${s//-/_}"
  printf '%s' "$s"
}

log "=== bench_advance_commit_all_nodes  ts=$ts ==="
log "Threads : $THREADS"
log "Runs    : $RUNS"
log "Out     : $OUT_DIR"
echo

# ── Build all nodes list ────────────────────────────────────────────────────
declare -a ALL_NODES=()
[[ "$SKIP_LOCAL" == "0" ]] && ALL_NODES+=("local")
for n in "${NEEL_NODES[@]}"; do ALL_NODES+=("$n"); done

# ── Preflight SSH check ─────────────────────────────────────────────────────
log "=== Preflight: SSH reachability ==="
for n in "${ALL_NODES[@]}"; do
  [[ "$n" == "local" ]] && { log "  [OK] local"; continue; }
  if ssh_run "$n" "echo alive" >/dev/null 2>&1; then
    log "  [OK] $n"
  else
    err "$n unreachable"
    exit 1
  fi
done
echo

# ── Phase 1: Local build + install ─────────────────────────────────────────
log "=== Phase 1: Local build + install ==="
(cd "$REPO_ROOT" && make -j"$(nproc)" 2>&1 | tail -4)
(cd "$REPO_ROOT" && make install 2>&1 | tail -4)
log "  postgres binary: $(ls -lh $LOCAL_INSTALL/bin/postgres | awk '{print $5, $6, $7, $8}')"
echo

# ── Phase 2: Parallel rsync ─────────────────────────────────────────────────
if [[ "$SKIP_SYNC" == "1" ]]; then
  log "=== Phase 2: Skipped (--skip-sync) ==="
else
  log "=== Phase 2: Parallel rsync to all nodes ==="
  declare -A SYNC_PID=()
  for n in "${NEEL_NODES[@]}"; do
    slog="$LOG_DIR/sync_$(safe "$n").log"
    (
      set -euo pipefail
      echo "[SYNC] $n  started: $(_ts)" > "$slog"
      ssh_run "$n" "mkdir -p '$REMOTE_REPO' '$REMOTE_INSTALL' '$REMOTE_REPO/.bench_tmp'" >> "$slog" 2>&1
      rsync -az --delete \
        --exclude='.git' --exclude='.venv' --exclude='.bench_tmp' \
        --exclude='__pycache__' --exclude='*.pyc' \
        --exclude='conftest' --exclude='conftest.*' \
        --exclude='scripts/bench_full_results' \
        --exclude='scripts/bench_results' \
        -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
        "$REPO_ROOT/" "$n:$REMOTE_REPO/" >> "$slog" 2>&1 || [[ $? -eq 24 ]]
      rsync -az --delete \
        -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
        "$LOCAL_INSTALL/" "$n:$REMOTE_INSTALL/" >> "$slog" 2>&1
      rsync -az \
        -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
        "$TEMPLATE_CONF" "$n:$REMOTE_REPO/.bench_tmp/shared_postgresql.conf" >> "$slog" 2>&1
      echo "[SYNC] $n  done: $(_ts)" >> "$slog"
    ) &
    SYNC_PID["$n"]=$!
    log "  Syncing $n (bg PID=${SYNC_PID[$n]})"
  done
  for n in "${NEEL_NODES[@]}"; do
    if wait "${SYNC_PID[$n]}"; then
      log "  [OK]   $n"
    else
      err "Sync failed for $n – see $LOG_DIR/sync_$(safe "$n").log"
      exit 1
    fi
  done
fi
echo

# ── Phase 3: Remote on-host rebuild (Ubuntu 22.04 only) ────────────────────
if [[ "$SKIP_REBUILD" == "1" ]]; then
  log "=== Phase 3: Skipped (--skip-rebuild) ==="
else
log "=== Phase 3: ensure_custom_install on all nodes ==="
declare -A REBUILD_PID=()
for n in "${NEEL_NODES[@]}"; do
  rlog="$LOG_DIR/rebuild_$(safe "$n").log"
  (
    ssh_run "$n" "
      set -euo pipefail
      export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
      bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
        --clean-when-rebuild 2>&1
    " > "$rlog" 2>&1
  ) &
  REBUILD_PID["$n"]=$!
  log "  Rebuilding $n (bg PID=${REBUILD_PID[$n]})"
done
for n in "${NEEL_NODES[@]}"; do
  if wait "${REBUILD_PID[$n]}"; then
    log "  [OK]   $n"
  else
    err "Rebuild failed for $n – see $LOG_DIR/rebuild_$(safe "$n").log"
    cat "$LOG_DIR/rebuild_$(safe "$n").log" | tail -20 >&2 || true
    exit 1
  fi
done
fi  # end skip-rebuild check
echo

# ── Helper: flip GUC on one remote node ────────────────────────────────────
# Requires postgres to be running on the node.
flip_guc_remote() {
  local node="$1"
  local value="$2"   # on | off
  local psql="$REMOTE_INSTALL/bin/psql"
  ssh_run "$node" "
    set -euo pipefail
    export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
    '$psql' -h 127.0.0.1 -p $DB_PORT -U $DB_USER -d $DB_NAME \
      -c \"ALTER SYSTEM SET bcdb_advance_commit_watermark = '$value';\" 2>&1
  "
}

# Helper: verify GUC on one remote node (after pg_ctl restart by bench script)
verify_guc_remote() {
  local node="$1"
  local expected="$2"
  local psql="$REMOTE_INSTALL/bin/psql"
  local actual
  actual=$(ssh_run "$node" "
    export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
    '$psql' -h 127.0.0.1 -p $DB_PORT -U $DB_USER -d $DB_NAME \
      -tAc 'SHOW bcdb_advance_commit_watermark;' 2>/dev/null || echo ERROR
  " 2>/dev/null | tr -d '[:space:]')
  if [[ "$actual" == "$expected" ]]; then
    log "  [GUC-OK] $node  bcdb_advance_commit_watermark=$actual"
    return 0
  else
    err "$node  GUC expected=$expected got=$actual"
    return 1
  fi
}

# ── Helper: ensure single-node postgres is running, return PGDATA ───────────
start_postgres_remote() {
  local node="$1"
  ssh_run "$node" "
    set -euo pipefail
    export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
    export ARIABC_REQUIRE_CUSTOM_PG=1
    export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
    export ARIABC_DIR='$REMOTE_REPO'
    export ARIABC_PGPORT='$DB_PORT'
    bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
      --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
      --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
      --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
      --require-custom 2>&1 | tail -1
  " 2>/dev/null | grep "^PGDATA=" | tail -1
}

# ── Phase 4: Start postgres on all nodes and set GUC=ON ────────────────────
log "=== Phase 4: Start postgres on all nodes and set GUC=on ==="
declare -A START_PID=()
for n in "${NEEL_NODES[@]}"; do
  slog="$LOG_DIR/pgstart_on_$(safe "$n").log"
  (
    echo "[START] $n" > "$slog"
    pgdata_line=$(start_postgres_remote "$n" 2>>"$slog") || true
    echo "  pgdata_line=$pgdata_line" >> "$slog"
    flip_guc_remote "$n" "on" >> "$slog" 2>&1
    echo "[DONE] $n" >> "$slog"
  ) &
  START_PID["$n"]=$!
done
for n in "${NEEL_NODES[@]}"; do
  wait "${START_PID[$n]}" && log "  [OK]   $n" || { err "$n start/GUC-on failed"; cat "$LOG_DIR/pgstart_on_$(safe "$n").log" >&2; exit 1; }
done
echo

# ── Phase 5: Bench ADVANCE_ON in parallel ──────────────────────────────────
log "=== Phase 5: Running bench advance_on on all nodes in parallel ==="
BENCH_FLAGS="--modes $MODES --threads '$THREADS' --runs $RUNS --signing-modes $SIGNING_MODES"
BENCH_FLAGS+=" --db $DB_NAME --user $DB_USER --port $DB_PORT"

declare -A BENCH_ON_PID=()
for n in "${NEEL_NODES[@]}"; do
  remote_out="$REMOTE_REPO/scripts/bench_results/advance_on"
  remote_log="$REMOTE_REPO/.bench_tmp/bench_advance_on_$(safe "$n").log"
  blog="$LOG_DIR/bench_on_launch_$(safe "$n").log"
  remote_cmd="
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO/scripts'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export ARIABC_PGDATA='$REMOTE_REPO/.bench_tmp/single_node_pgdata'
PYTHON_BIN=''
if [[ -x '$REMOTE_REPO/.venv/bin/python' ]] && '$REMOTE_REPO/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
fi
[[ -z \"\$PYTHON_BIN\" ]] && { echo 'ERROR: no python with psycopg' >&2; exit 2; }
\$PYTHON_BIN -u bench_threads_matrix.py \
  $BENCH_FLAGS --no-resume --out-dir '$remote_out'
"
  remote_pid=$(ssh_run "$n" "
    mkdir -p '$REMOTE_REPO/.bench_tmp' '$remote_out'
    nohup bash -lc $(printf '%q' "$remote_cmd") > '$remote_log' 2>&1 &
    echo \$!
  " 2>"$blog" | tail -1)
  if [[ -z "$remote_pid" ]] || ! [[ "$remote_pid" =~ ^[0-9]+$ ]]; then
    err "$n: could not launch advance_on bench (pid='$remote_pid')" ; cat "$blog" >&2 ; exit 1
  fi
  BENCH_ON_PID["$n"]="$remote_pid"
  log "  [LAUNCHED] $n  pid=$remote_pid  log=$remote_log"
done

log "  Waiting for all advance_on benches to finish (poll every ${POLL_INTERVAL_S}s)..."
declare -A BENCH_ON_DONE=()
while true; do
  all_done=1
  for n in "${NEEL_NODES[@]}"; do
    [[ -n "${BENCH_ON_DONE[$n]:-}" ]] && continue
    pid="${BENCH_ON_PID[$n]}"
    if ssh_run "$n" "kill -0 $pid 2>/dev/null" 2>/dev/null; then
      all_done=0
      remote_log="$REMOTE_REPO/.bench_tmp/bench_advance_on_$(safe "$n").log"
      tail_out=$(ssh_run "$n" "tail -3 '$remote_log' 2>/dev/null || true" 2>/dev/null || true)
      log "  [RUNNING] $n | ${tail_out//$'\n'/ | }"
    else
      BENCH_ON_DONE["$n"]=1
      log "  [DONE]    $n advance_on bench finished"
    fi
  done
  [[ "$all_done" == "1" ]] && break
  sleep "$POLL_INTERVAL_S"
done
echo

# ── Phase 6: Set GUC=OFF on all nodes ──────────────────────────────────────
log "=== Phase 6: Set bcdb_advance_commit_watermark=off on all nodes ==="
declare -A GUC_OFF_PID=()
for n in "${NEEL_NODES[@]}"; do
  (
    flip_guc_remote "$n" "off"
    log "  [GUC-OFF-SET] $n"
  ) &
  GUC_OFF_PID["$n"]=$!
done
for n in "${NEEL_NODES[@]}"; do
  wait "${GUC_OFF_PID[$n]}" || { err "GUC flip failed for $n"; exit 1; }
done
log "  GUC=off written to auto.conf on all nodes."
log "  (bench_threads_matrix.py will restart postgres before each det run, picking it up)"
echo

# ── Phase 7: Bench ADVANCE_OFF in parallel ─────────────────────────────────
log "=== Phase 7: Running bench advance_off on all nodes in parallel ==="

declare -A BENCH_OFF_PID=()
for n in "${NEEL_NODES[@]}"; do
  remote_out="$REMOTE_REPO/scripts/bench_results/advance_off"
  remote_log="$REMOTE_REPO/.bench_tmp/bench_advance_off_$(safe "$n").log"
  blog="$LOG_DIR/bench_off_launch_$(safe "$n").log"
  remote_cmd="
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO/scripts'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export ARIABC_PGDATA='$REMOTE_REPO/.bench_tmp/single_node_pgdata'
PYTHON_BIN=''
if [[ -x '$REMOTE_REPO/.venv/bin/python' ]] && '$REMOTE_REPO/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
fi
[[ -z \"\$PYTHON_BIN\" ]] && { echo 'ERROR: no python with psycopg' >&2; exit 2; }
\$PYTHON_BIN -u bench_threads_matrix.py \
  $BENCH_FLAGS --no-resume --out-dir '$remote_out'
"
  remote_pid=$(ssh_run "$n" "
    mkdir -p '$REMOTE_REPO/.bench_tmp' '$remote_out'
    nohup bash -lc $(printf '%q' "$remote_cmd") > '$remote_log' 2>&1 &
    echo \$!
  " 2>"$blog" | tail -1)
  if [[ -z "$remote_pid" ]] || ! [[ "$remote_pid" =~ ^[0-9]+$ ]]; then
    err "$n: could not launch advance_off bench (pid='$remote_pid')"; cat "$blog" >&2; exit 1
  fi
  BENCH_OFF_PID["$n"]="$remote_pid"
  log "  [LAUNCHED] $n  pid=$remote_pid  log=$remote_log"
done

log "  Waiting for all advance_off benches to finish (poll every ${POLL_INTERVAL_S}s)..."
declare -A BENCH_OFF_DONE=()
while true; do
  all_done=1
  for n in "${NEEL_NODES[@]}"; do
    [[ -n "${BENCH_OFF_DONE[$n]:-}" ]] && continue
    pid="${BENCH_OFF_PID[$n]}"
    if ssh_run "$n" "kill -0 $pid 2>/dev/null" 2>/dev/null; then
      all_done=0
      remote_log="$REMOTE_REPO/.bench_tmp/bench_advance_off_$(safe "$n").log"
      tail_out=$(ssh_run "$n" "tail -3 '$remote_log' 2>/dev/null || true" 2>/dev/null || true)
      log "  [RUNNING] $n | ${tail_out//$'\n'/ | }"
    else
      BENCH_OFF_DONE["$n"]=1
      log "  [DONE]    $n advance_off bench finished"
    fi
  done
  [[ "$all_done" == "1" ]] && break
  sleep "$POLL_INTERVAL_S"
done
echo

# ── Phase 8: Reset GUC=ON on all nodes ─────────────────────────────────────
log "=== Phase 8: Resetting bcdb_advance_commit_watermark=on on all nodes ==="
for n in "${NEEL_NODES[@]}"; do
  flip_guc_remote "$n" "on" 2>/dev/null && log "  [RESET] $n" || log "  [WARN] Could not reset GUC on $n"
done
echo

# ── Phase 9: Collect results from all nodes ─────────────────────────────────
log "=== Phase 9: Collecting results from all nodes ==="
for n in "${NEEL_NODES[@]}"; do
  safe_n="$(safe "$n")"
  node_dir="$OUT_DIR/$safe_n"
  mkdir -p "$node_dir/advance_on" "$node_dir/advance_off"

  rsync -az \
    -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
    "$n:$REMOTE_REPO/scripts/bench_results/advance_on/" \
    "$node_dir/advance_on/" 2>/dev/null \
    && log "  [COLLECTED] $n advance_on → $node_dir/advance_on" \
    || log "  [WARN] could not rsync advance_on from $n"

  rsync -az \
    -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
    "$n:$REMOTE_REPO/scripts/bench_results/advance_off/" \
    "$node_dir/advance_off/" 2>/dev/null \
    && log "  [COLLECTED] $n advance_off → $node_dir/advance_off" \
    || log "  [WARN] could not rsync advance_off from $n"
done
echo

# ── Phase 10: Generate per-node comparison graphs ───────────────────────────
log "=== Phase 10: Generating per-node comparison graphs ==="
GRAPH_SCRIPT="$REPO_ROOT/scripts/plot_advance_commit_compare.py"

for n in "${NEEL_NODES[@]}"; do
  safe_n="$(safe "$n")"
  node_dir="$OUT_DIR/$safe_n"
  on_dir="$node_dir/advance_on"
  off_dir="$node_dir/advance_off"
  out_png="$OUT_DIR/${safe_n}_advance_commit_compare.png"

  # Build summaries if needed
  for d in "$on_dir" "$off_dir"; do
    if [[ -f "$d/results.csv" && ! -f "$d/summary.csv" ]]; then
      python3 - "$d" <<'PYEOF'
import sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]).parent.parent.parent / "scripts"))
import importlib.util
spec = importlib.util.spec_from_file_location("btm",
    str(Path(sys.argv[1]).parent.parent.parent / "scripts/bench_threads_matrix.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
d = Path(sys.argv[1])
mod._write_summary(d / "results.csv", d / "summary.csv")
print(f"  summary written: {d}/summary.csv")
PYEOF
    fi
  done

  if [[ -f "$on_dir/summary.csv" && -f "$off_dir/summary.csv" ]]; then
    python3 "$GRAPH_SCRIPT" \
      --on-dir  "$on_dir" \
      --off-dir "$off_dir" \
      --node-label "$n" \
      --out "$out_png" \
      && log "  [GRAPH] $n → $out_png" \
      || log "  [WARN]  graph failed for $n"
  else
    log "  [SKIP] $n – missing summary CSV(s)"
  fi
done
echo

log "=== Done ==="
log "Results : $OUT_DIR"
log "Graphs  :"
find "$OUT_DIR" -maxdepth 1 -name '*.png' | sort | while IFS= read -r f; do
  log "  $f"
done
