#!/usr/bin/env bash
set -euo pipefail

#
# run_parallel_ycsb_all_nodes.sh
#
# Syncs updated AriaBC source to all lab nodes simultaneously, then launches
# the YCSB det-mode benchmark on every node IN PARALLEL (no waiting for one
# to finish before starting the next).
#
# Active nodes:
#   neel@10.129.148.248      utkarsh-MS-7C96
#   neel@10.129.148.246      kartik-MS-7C96  (Ubuntu 22.04 – on-host rebuild)
#   neel@10.129.148.247      neel-MS-7C96
#   protectdr@ranking.cse.iitb.ac.in  user-MZ73-LM0-000 (AMD EPYC 9654 96-Core)
#
# Default benchmark profiles (runs 3 × modes in this order):
#   1. pg mode                   – plain PostgreSQL baseline (no BCDB)
#   2. bcdb_det mode sign=0      – deterministic BCDB, no Merkle
#   3. bcdb_merkle mode sign=0   – deterministic BCDB + synchronous Merkle
#
# Signing key for sign=1 runs lives at scripts/bench_signing_privkey.pem (EC P-256).
#
# Flow:
#   Phase 1 – Sync source + install to all remote nodes in parallel (rsync bg)
#   Phase 2 – Launch bench on every node in parallel (nohup SSH, capture PIDs)
#   Phase 3 – Monitor loop: poll every POLL_INTERVAL_S seconds, hang detection
#   Phase 4 – Collect results from all nodes, print final summary
#
# Usage:
#   scripts/distributed/run_parallel_ycsb_all_nodes.sh [options]
#
#   --nodes <csv>           Comma-separated list of target user@host nodes
#   --ssh-key <path>        SSH private key (optional if key-auth is default)
#   --ssh-port <22>         SSH port
#   --modes <...>          Comma-separated modes: pg, bcdb_det, bcdb_merkle  [default: pg,bcdb_det,bcdb_merkle]
#   --threads <csv>         Thread counts csv  [default: 1,2,4,8]
#   --runs <3>              Runs per workload/thread combination
#   --workloads <csv>       Workload filenames (default: bench_threads_matrix.py defaults)
#   --rates <csv>           Rate limits csv (optional)
#   --signing-modes <0,1>   Signing modes for det runs: 0=unsigned, 1=signed  [default: 0,1]
#   --signing-privkey <p>   Signing key path (relative to repo root)  [default: scripts/bench_signing_privkey.pem]
#   --enforce-signatures <1> 0|1 — set bcdb_enforce_signatures in workload sessions  [default: 1]
#   --poll-interval <60>    Seconds between monitoring polls
#   --warmup-runs <1>       Unmeasured warmup executions before the matrix  [default: 1]
#   --merkle-partitions <200> Number of hash-routed Merkle partitions  [default: 200]
#   --merkle-fanout <4>     Merkle tree branching factor  [default: 4]
#   --merkle-split-threshold <32> Node tuple count triggering a split  [default: 32]
#   --merkle-merge-threshold <8>  Node tuple count triggering a merge  [default: 8]
#   --timeout-db-s <900>    Timeout for restore/apply/verification SQL; 0 disables it
#   --hang-timeout <300>    Seconds of no log change before long-stage warning
#   --skip-sync             Skip the rsync phase (reuse last-synced remote source)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

# ---- Static node config ----
DEFAULT_NODES=(
  "neel@10.129.148.248"
  "neel@10.129.148.246"
  "neel@10.129.148.247"
)
NODES_OVERRIDE=""
TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"
LOCAL_INSTALL_DIR="${ARIABC_INSTALL_DIR:-/work/ARIABC/install}"

# Node-specific password auth (all nodes now use key auth; add entries here only
# if a node requires sshpass).
declare -A NODE_PASSWORDS=()

# ---- Tunable defaults ----
SSH_KEY=""
SSH_PORT=22
MODES="pg,bcdb_det,bcdb_merkle"
THREADS="${ARIABC_DEFAULT_FULL_THREADS}"
RUNS=3
WORKLOADS=""
RATES=""
SIGNING_MODES="0"
SIGNING_PRIVKEY="scripts/bench_signing_privkey.pem"
ENFORCE_SIGNATURES="1"
PG_ISOLATION="serializable"
WARMUP_RUNS="1"
MERKLE_PARTITIONS="200"
MERKLE_FANOUT="4"
MERKLE_SPLIT_THRESHOLD="32"
MERKLE_MERGE_THRESHOLD="8"
DB_STAGE_TIMEOUT_S="900"
DB_NAME="postgres"
DB_USER="postgres"
DB_PORT=5438
POLL_INTERVAL_S=60
HANG_TIMEOUT_S=300
DB_READY_TIMEOUT_S=120
SKIP_SYNC=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)         NODES_OVERRIDE="${2:-}"; shift 2 ;;
    --ssh-key)       SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port)      SSH_PORT="${2:-22}"; shift 2 ;;
    --modes)         MODES="${2:-pg,bcdb_det,bcdb_merkle}"; shift 2 ;;
    --threads)       THREADS="${2:-}"; shift 2 ;;
    --runs)          RUNS="${2:-3}"; shift 2 ;;
    --workloads)     WORKLOADS="${2:-}"; shift 2 ;;
    --rates)         RATES="${2:-}"; shift 2 ;;
    --signing-modes) SIGNING_MODES="${2:-}"; shift 2 ;;
    --signing-privkey) SIGNING_PRIVKEY="${2:-}"; shift 2 ;;
    --enforce-signatures) ENFORCE_SIGNATURES="${2:-}"; shift 2 ;;
    --pg-isolation)  PG_ISOLATION="${2:-serializable}"; shift 2 ;;
    --warmup-runs)  WARMUP_RUNS="${2:-1}"; shift 2 ;;
    --merkle-partitions) MERKLE_PARTITIONS="${2:-200}"; shift 2 ;;
    --merkle-fanout)     MERKLE_FANOUT="${2:-4}"; shift 2 ;;
    --merkle-split-threshold) MERKLE_SPLIT_THRESHOLD="${2:-32}"; shift 2 ;;
    --merkle-merge-threshold) MERKLE_MERGE_THRESHOLD="${2:-8}"; shift 2 ;;
    --timeout-db-s) DB_STAGE_TIMEOUT_S="${2:-900}"; shift 2 ;;
    --poll-interval) POLL_INTERVAL_S="${2:-60}"; shift 2 ;;
    --hang-timeout)  HANG_TIMEOUT_S="${2:-300}"; shift 2 ;;
    --skip-sync)     SKIP_SYNC=1; shift 1 ;;
    -h|--help)
      sed -n '/^# Usage/,/^[^#]/p' "$0" | head -n 20
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

SIGNING_PRIVKEY_LOCAL=""
if [[ -n "$SIGNING_PRIVKEY" ]]; then
  if [[ "$SIGNING_PRIVKEY" == /* ]]; then
    SIGNING_PRIVKEY_LOCAL="$SIGNING_PRIVKEY"
  else
    SIGNING_PRIVKEY_LOCAL="$REPO_ROOT/${SIGNING_PRIVKEY#./}"
  fi

  if [[ ! -f "$SIGNING_PRIVKEY_LOCAL" ]]; then
    echo "ERROR: signing private key not found: $SIGNING_PRIVKEY_LOCAL" >&2
    exit 1
  fi

  if [[ "$SIGNING_PRIVKEY_LOCAL" != "$REPO_ROOT/"* ]]; then
    echo "ERROR: --signing-privkey must be inside repo root for remote runs: $REPO_ROOT" >&2
    echo "       provided: $SIGNING_PRIVKEY_LOCAL" >&2
    exit 1
  fi
fi

# ---- Helpers ----
_ts()   { date '+%F %T'; }
lmsg()  { echo "[$(_ts)] $*"; }

# Node tracking arrays — declared here so _abort_run() can reference them
# even if called before Phase 2 populates them.
declare -A NODE_PID=()
declare -A NODE_LOG=()
declare -A NODE_REMOTE_OUT=()
declare -A NODE_TYPE=()
declare -A NODE_REPO=()
declare -A NODE_INST=()
declare -A NODE_STATUS=()
declare -A NODE_LAST_HASH=()
declare -A NODE_LAST_CHG=()
declare -A NODE_LAST_WARN=()
declare -A NODE_RSYNC_PID=()

# _abort_run REASON [node1 node2 ...]
# Prints which nodes have issues, kills any in-flight bench processes, exits 1.
_abort_run() {
  local reason="$1"; shift
  local -a bad_nodes=("$@")

  echo ""
  echo "╔══════════════════════════════════════════════════════════╗"
  echo "║  BENCHMARK ABORTED                                       ║"
  echo "╚══════════════════════════════════════════════════════════╝"
  lmsg "Reason: $reason"

  if [[ "${#bad_nodes[@]}" -gt 0 ]]; then
    lmsg "Nodes with issues (${#bad_nodes[@]}):"
    for n in "${bad_nodes[@]}"; do
      lmsg "  ✗  $n"
    done
  fi
  echo ""

  # Kill any bench processes that are still running.
  local killed_any=0
  for n in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$n]:-}" != "running" ]] && continue
    local pid="${NODE_PID[$n]:-}"
    [[ -z "$pid" ]] && continue
    if ssh_run "$n" "kill $pid 2>/dev/null" 2>/dev/null; then
      lmsg "  Killed remote bench pid=$pid on $n"
      killed_any=1
    fi
  done
  [[ "$killed_any" == "0" ]] && lmsg "  (no running bench processes to kill)"

  lmsg "Logs: $LOG_DIR"
  exit 1
}

safe_label() {
  local s="$1"
  s="${s//@/_at_}"; s="${s//./_}"; s="${s//-/_}"
  printf '%s' "$s"
}

_ssh_base() {
  local arr=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT")
  [[ -n "$SSH_KEY" ]] && arr+=(-i "$SSH_KEY")
  printf '%q ' "${arr[@]}"
}

_node_password() {
  local node="$1"
  printf '%s' "${NODE_PASSWORDS[$node]:-}"
}

ssh_run() {
  local node="$1"; shift
  local pass
  pass="$(_node_password "$node")"
  local ssh_arr=(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT")
  if [[ -n "$pass" ]]; then
    command -v sshpass >/dev/null 2>&1 || {
      echo "ERROR: sshpass is required for password-auth node $node" >&2
      return 127
    }
    ssh_arr=(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT")
    [[ -n "$SSH_KEY" ]] && ssh_arr+=(-i "$SSH_KEY")
    sshpass -p "$pass" "${ssh_arr[@]}" "$node" "$@"
  else
    ssh_arr=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT")
    [[ -n "$SSH_KEY" ]] && ssh_arr+=(-i "$SSH_KEY")
    "${ssh_arr[@]}" "$node" "$@"
  fi
}

_rsync_e_for_node() {
  local node="$1"
  local pass
  pass="$(_node_password "$node")"
  local e="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT"
  if [[ -n "$pass" ]]; then
    command -v sshpass >/dev/null 2>&1 || {
      echo "ERROR: sshpass is required for password-auth node $node" >&2
      return 127
    }
    [[ -n "$SSH_KEY" ]] && e+=" -i $SSH_KEY"
    printf 'sshpass -p %q %s' "$pass" "$e"
  else
    e+=" -o BatchMode=yes"
    [[ -n "$SSH_KEY" ]] && e+=" -i $SSH_KEY"
    printf '%s' "$e"
  fi
}

# ---- Pre-flight validation ----
for req in \
  "$TEMPLATE_CONF_LOCAL" \
  "$REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh" \
  "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
  "$REPO_ROOT/scripts/start_server.sh" \
  "$REPO_ROOT/scripts/bench_threads_matrix.py" \
  "$REPO_ROOT/scripts/restore_usertable_small.sql"; do
  if [[ ! -f "$req" ]]; then
    echo "ERROR: required file missing: $req" >&2; exit 1
  fi
done

# A source edit is not runnable evidence until the local install has been
# rebuilt/installed from that exact checkout.  This gate runs before rsync so
# the install tree copied to execution-only nodes cannot lag the source tree.
# --skip-sync intentionally bypasses this gate because it explicitly requests
# reuse of the last synchronized source/install pair.
if [[ "$SKIP_SYNC" == "0" ]]; then
  lmsg "=== Preflight: refreshing local custom install from current source ==="
  if ! bash "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
      --repo-root "$REPO_ROOT" --install-dir "$LOCAL_INSTALL_DIR" \
      --clean-when-rebuild; then
    echo "ERROR: local custom install is missing or stale; source was not benchmarked" >&2
    exit 1
  fi
  lmsg "Preflight: local custom install matches current source checkout."
  echo
fi

for node in "${!NODE_PASSWORDS[@]}"; do
  if [[ -n "${NODE_PASSWORDS[$node]}" ]]; then
    command -v sshpass >/dev/null 2>&1 || {
      echo "ERROR: sshpass is required because password auth is configured for $node" >&2
      exit 1
    }
  fi
done

ts="$(date +%Y%m%d_%H%M%S)"
LOCAL_RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results/parallel_ycsb_${ts}"
LOG_DIR="$LOCAL_RESULT_ROOT/_run_logs"
mkdir -p "$LOG_DIR"

lmsg "=== Parallel YCSB benchmark – all nodes ==="
lmsg "Timestamp    : $ts"
lmsg "Modes        : $MODES"
lmsg "Threads      : $THREADS"
lmsg "Runs         : $RUNS"
lmsg "Workloads    : ${WORKLOADS:-<bench_threads_matrix.py defaults>}"
lmsg "SigningModes : ${SIGNING_MODES:-<default>}"
lmsg "SigningKey   : ${SIGNING_PRIVKEY_LOCAL:-<not set>}"
lmsg "EnforceSig   : ${ENFORCE_SIGNATURES:-<default>}"
lmsg "PG Isolation : $PG_ISOLATION"
lmsg "WarmupRuns   : $WARMUP_RUNS"
lmsg "DB timeout   : ${DB_STAGE_TIMEOUT_S}s (0=disabled)"
lmsg "Result root  : $LOCAL_RESULT_ROOT"
echo

# ---- Build node registry ----
# Entry format: "node|repo_root|install_dir|type"
declare -a ALL_NODES=()
declare -a _target_nodes=()

if [[ -n "$NODES_OVERRIDE" ]]; then
  IFS=',' read -ra _target_nodes <<< "$NODES_OVERRIDE"
else
  _target_nodes=("${DEFAULT_NODES[@]}")
fi

for n in "${_target_nodes[@]}"; do
  n="$(echo "$n" | xargs)"
  [[ -z "$n" ]] && continue
  user="${n%@*}"
  host="${n#*@}"
  repo="/home/$user/Desktop/ariabc_cluster"
  inst="/home/$user/Desktop/ariabc_install"
  ALL_NODES+=("$n|$repo|$inst|$user")
done

lmsg "Nodes (${#ALL_NODES[@]}):"
for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  lmsg "  [$type] $node"
done
echo

# ============================================================
# PREFLIGHT: SSH REACHABILITY CHECK
# Every remote node must be reachable before any work starts.
# Fail fast with a clear list of unreachable nodes.
# ============================================================
lmsg "=== Preflight: SSH reachability check ==="
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
  _abort_run "${#_unreachable[@]} node(s) unreachable via SSH" "${_unreachable[@]}"
fi
lmsg "Preflight: all nodes reachable."
echo

# ============================================================
# PHASE 0: KILL STALE BENCHMARK PROCESSES ON ALL NODES
# ============================================================
lmsg "=== Phase 0: Killing any stale benchmark processes ==="

# Patterns to kill (bench orchestrator + workload traffic loader).
# We do NOT touch postgres itself – ensure_single_node_postgres.sh handles that.
_KILL_PATTERNS=(
  "bench_threads_matrix.py"
  "generic-saicopg"
  "saicopg-traffic"
)

_kill_on_node() {
  local node="$1"
  local type="$2"
  local killed=0

  for pat in "${_KILL_PATTERNS[@]}"; do
    local result
    result=$(ssh_run "$node" "pkill -9 -f '$pat' 2>/dev/null && echo KILLED || true" 2>/dev/null || true)
    if [[ "$result" == *"KILLED"* ]]; then
      lmsg "  [KILL] $node – terminated processes matching '$pat'"
      killed=1
    fi
  done

  if [[ "$killed" == "0" ]]; then
    lmsg "  [CLEAN] ${node} – no stale processes found"
  fi
}

for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  _kill_on_node "$node" "$type"
done

# Give processes a moment to fully exit before we sync/launch
sleep 2
lmsg "Phase 0 complete."
echo

# ============================================================
# PHASE 1: PARALLEL SOURCE SYNC
# ============================================================
declare -A SYNC_STATUS=()

if [[ "$SKIP_SYNC" == "1" ]]; then
  lmsg "=== Phase 1: SKIPPED (--skip-sync) ==="
  for entry in "${ALL_NODES[@]}"; do
    IFS='|' read -r node repo inst type <<< "$entry"
    SYNC_STATUS["$node"]="OK"
  done
else
  lmsg "=== Phase 1: Syncing source to all remote nodes in parallel ==="

  # The local PGDATA config is also used as the remote fresh-cluster template.
  # Do not inherit the legacy blocking commit-watermark setting from an older
  # experiment: with concurrent DET workers it can deadlock when a successor
  # holds a tuple/Merkle lock while waiting for its predecessor to publish.
  # The CAS prefix advancement is the required liveness path for this runner.
  SHARED_TEMPLATE_LOCAL="$REPO_ROOT/.bench_tmp/shared_postgresql.conf"
  mkdir -p "$REPO_ROOT/.bench_tmp"
  cp "$TEMPLATE_CONF_LOCAL" "$SHARED_TEMPLATE_LOCAL"
  sed -i -E \
    "s|^[[:space:]]*bcdb_advance_commit_watermark[[:space:]]*=.*$|bcdb_advance_commit_watermark = 'on'|" \
    "$SHARED_TEMPLATE_LOCAL"
  if ! grep -Eq '^[[:space:]]*bcdb_advance_commit_watermark[[:space:]]*=' "$SHARED_TEMPLATE_LOCAL"; then
    printf "\nbcdb_advance_commit_watermark = 'on'\n" >> "$SHARED_TEMPLATE_LOCAL"
  fi
  sed -i -E \
    "s|^[[:space:]]*bcdb_serial_gate_source[[:space:]]*=.*$|bcdb_serial_gate_source = 0|" \
    "$SHARED_TEMPLATE_LOCAL"
  if ! grep -Eq '^[[:space:]]*bcdb_serial_gate_source[[:space:]]*=' "$SHARED_TEMPLATE_LOCAL"; then
    printf 'bcdb_serial_gate_source = 0\n' >> "$SHARED_TEMPLATE_LOCAL"
  fi
  sed -i -E \
    "s|^[[:space:]]*default_transaction_isolation[[:space:]]*=.*$|default_transaction_isolation = '${PG_ISOLATION}'|" \
    "$SHARED_TEMPLATE_LOCAL"
  if ! grep -Eq '^[[:space:]]*default_transaction_isolation[[:space:]]*=' "$SHARED_TEMPLATE_LOCAL"; then
    printf "\ndefault_transaction_isolation = '%s'\n" "$PG_ISOLATION" >> "$SHARED_TEMPLATE_LOCAL"
  fi
  lmsg "  Benchmark template: default_transaction_isolation=$PG_ISOLATION, bcdb_advance_commit_watermark=on, bcdb_serial_gate_source=0"

  declare -A SYNC_BGPIDS=()
  declare -A SYNC_LOGS=()

  for entry in "${ALL_NODES[@]}"; do
    IFS='|' read -r node repo inst type <<< "$entry"
    slog="$LOG_DIR/sync_$(safe_label "$node").log"
    SYNC_LOGS["$node"]="$slog"
    SYNC_STATUS["$node"]="running"

    (
      set -euo pipefail
      echo "[SYNC] $node  repo=$repo  install=$inst" > "$slog"
      echo "Started: $(_ts)" >> "$slog"

      # rsync may return 24 when transient build artifacts disappear mid-transfer.
      # Treat that specific code as a warning; keep all other failures fatal.
      _rsync_allow_vanished() {
        local rc
        if rsync "$@"; then
          return 0
        fi
        rc=$?
        if [[ "$rc" -eq 24 ]]; then
          echo "WARN: rsync exited 24 (vanished source files) — continuing" >> "$slog"
          return 0
        fi
        return "$rc"
      }

      # Ensure remote dirs exist
      ssh_run "$node" "mkdir -p '$repo' '$inst' '$repo/.bench_tmp' '$repo/.bench_tmp/deps/lib'" >> "$slog" 2>&1

      # Stage OpenSSL headers (needed for Ubuntu 22.04 on-host rebuild)
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

      # Sync source tree (excludes .git, .venv, .bench_tmp, bench results, and compiled binaries/configs)
      _rsync_allow_vanished -az --delete \
        --exclude='.git' \
        --exclude='.venv' \
        --exclude='.bench_tmp' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='*.o' \
        --exclude='*.a' \
        --exclude='*.so' \
        --exclude='*.so.*' \
        --exclude='config.status' \
        --exclude='config.log' \
        --exclude='config.cache' \
        --exclude='GNUmakefile' \
        --exclude='src/Makefile.global' \
        --exclude='src/include/pg_config.h' \
        --exclude='src/include/pg_config_ext.h' \
        --exclude='src/interfaces/ecpg/include/ecpg_config.h' \
        --exclude='scripts/big_usertable.sql' \
        --exclude='scripts/bench_full_results' \
        --exclude='scripts/bench_results' \
        --exclude='scripts/bench_results_tpcc' \
        -e "$(_rsync_e_for_node "$node")" \
        "$REPO_ROOT/" "$node:$repo/" >> "$slog" 2>&1

      # Sync compiled install tree (Ubuntu 24.04 nodes may use it directly;
      # ensure_custom_install_from_repo.sh will rebuild on Ubuntu 22.04 if glibc differs)
      _rsync_allow_vanished -az --delete \
        -e "$(_rsync_e_for_node "$node")" \
        /work/ARIABC/install/ "$node:$inst/" >> "$slog" 2>&1

      # Copy canonical postgresql.conf template
      _rsync_allow_vanished -az \
        -e "$(_rsync_e_for_node "$node")" \
        "$SHARED_TEMPLATE_LOCAL" "$node:$repo/.bench_tmp/shared_postgresql.conf" >> "$slog" 2>&1

      echo "Finished OK: $(_ts)" >> "$slog"
    ) &
    SYNC_BGPIDS["$node"]=$!
    lmsg "  Started sync for $node (bg PID=${SYNC_BGPIDS[$node]})"
  done

  lmsg "  Waiting for all syncs to complete..."
  for node in "${!SYNC_BGPIDS[@]}"; do
    if wait "${SYNC_BGPIDS[$node]}" 2>/dev/null; then
      SYNC_STATUS["$node"]="OK"
      lmsg "  [OK]   $node"
    else
      SYNC_STATUS["$node"]="FAIL"
      lmsg "  [FAIL] $node  – see ${SYNC_LOGS[$node]}"
    fi
  done

  declare -a _sync_failed=()
  for node in "${!SYNC_STATUS[@]}"; do
    [[ "${SYNC_STATUS[$node]}" != "OK" ]] && _sync_failed+=("$node") || true
  done
  if [[ "${#_sync_failed[@]}" -gt 0 ]]; then
    lmsg "Sync logs for failed nodes:"
    for node in "${_sync_failed[@]}"; do
      lmsg "  -- $node: ${SYNC_LOGS[$node]:-<no log>} --"
      tail -10 "${SYNC_LOGS[$node]:-/dev/null}" 2>/dev/null | while IFS= read -r l; do lmsg "     $l"; done
    done
    _abort_run "Sync failed on ${#_sync_failed[@]} node(s)" "${_sync_failed[@]}"
  fi
fi

echo

# ============================================================
# PHASE 1.5: VERIFY/REBUILD CUSTOM INSTALL BEFORE LAUNCH
# ============================================================
lmsg "=== Phase 1.5: Verifying custom install on all nodes ==="

declare -A INSTALL_STATUS=()
declare -A INSTALL_LOGS=()

for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  ilog="$LOG_DIR/install_$(safe_label "$node").log"
  INSTALL_LOGS["$node"]="$ilog"
  INSTALL_STATUS["$node"]="running"

  if [[ "${SYNC_STATUS[$node]:-FAIL}" != "OK" ]]; then
    INSTALL_STATUS["$node"]="SKIP"
    lmsg "  [SKIP] $node – sync failed"
    continue
  fi

  if ssh_run "$node" "
    set -euo pipefail
    mkdir -p '$repo/.bench_tmp'
    chmod +x '$repo/scripts/distributed/ensure_custom_install_from_repo.sh'
    if [[ '$SKIP_SYNC' == '0' ]]; then
      echo '[INFO] source/install were just synced; checking install freshness against source'
      if bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild; then
        exit 0
      fi
      echo '[INFO] synced install is missing or stale; attempting local rebuild'
      if ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
        echo 'ERROR: synced install is stale/missing and make/gcc are unavailable for rebuild' >&2
        exit 1
      fi
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild
    elif ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
      echo '[INFO] make/gcc missing; trusting existing install if it verifies'
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
        --trust-install
    else
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild
    fi
  " >"$ilog" 2>&1; then
    INSTALL_STATUS["$node"]="OK"
    if grep -q '^TRUST_INSTALL=1' "$ilog"; then
      lmsg "  [OK] $node – synced install verified"
    else
      lmsg "  [OK] $node – custom install ready"
    fi
  else
    INSTALL_STATUS["$node"]="FAIL"
    lmsg "  [FAIL] $node – custom install not ready; see $ilog"
  fi
done

declare -a _install_failed=()
for node in "${!INSTALL_STATUS[@]}"; do
  [[ "${INSTALL_STATUS[$node]}" != "OK" ]] && _install_failed+=("$node") || true
done
if [[ "${#_install_failed[@]}" -gt 0 ]]; then
  lmsg "Install logs for failed nodes:"
  for node in "${_install_failed[@]}"; do
    lmsg "  -- $node: ${INSTALL_LOGS[$node]:-<no log>} --"
    tail -12 "${INSTALL_LOGS[$node]:-/dev/null}" 2>/dev/null | while IFS= read -r l; do lmsg "     $l"; done
  done
  _abort_run "Custom install verification failed on ${#_install_failed[@]} node(s)" "${_install_failed[@]}"
fi
lmsg "Phase 1.5 complete: custom install ready on all nodes."
echo

# ============================================================
# PHASE 2: PARALLEL BENCHMARK LAUNCH
# ============================================================
lmsg "=== Phase 2: Launching benchmarks on all nodes in parallel ==="

declare -A NODE_PID=()       # remote PID (string) or local bash PID
declare -A NODE_LOG=()       # path to bench log on the machine where it runs
declare -A NODE_REMOTE_OUT=() # path to bench_threads_matrix --out-dir on remote
declare -A NODE_TYPE=()
declare -A NODE_REPO=()
declare -A NODE_INST=()
declare -A NODE_STATUS=()    # running | done | failed | skipped
declare -A NODE_LAST_HASH=()
declare -A NODE_LAST_CHG=()
declare -A NODE_RSYNC_PID=() # PID of in-flight live-sync rsync (remote nodes only)

_build_bench_flags() {
  local repo_root="$1"
  local extra=""
  [[ -n "$WORKLOADS" ]] && extra+=" --workloads '$WORKLOADS'"
  [[ -n "$RATES" ]]     && extra+=" --rates '$RATES'"
  [[ -n "$SIGNING_MODES" ]] && extra+=" --signing-modes '$SIGNING_MODES'"
  if [[ -n "$SIGNING_PRIVKEY_LOCAL" ]]; then
    if [[ "$repo_root" == "$REPO_ROOT" ]]; then
      extra+=" --signing-privkey '$SIGNING_PRIVKEY_LOCAL'"
    else
      extra+=" --signing-privkey '$repo_root/${SIGNING_PRIVKEY_LOCAL#$REPO_ROOT/}'"
    fi
  fi
  [[ -n "$ENFORCE_SIGNATURES" ]] && extra+=" --enforce-signatures '$ENFORCE_SIGNATURES'"
  [[ -n "$PG_ISOLATION" ]] && extra+=" --pg-isolation '$PG_ISOLATION'"
  [[ -n "$MERKLE_PARTITIONS" ]] && extra+=" --merkle-partitions '$MERKLE_PARTITIONS'"
  [[ -n "$MERKLE_FANOUT" ]] && extra+=" --merkle-fanout '$MERKLE_FANOUT'"
  [[ -n "$MERKLE_SPLIT_THRESHOLD" ]] && extra+=" --merkle-split-threshold '$MERKLE_SPLIT_THRESHOLD'"
  [[ -n "$MERKLE_MERGE_THRESHOLD" ]] && extra+=" --merkle-merge-threshold '$MERKLE_MERGE_THRESHOLD'"
  extra+=" --warmup-runs '$WARMUP_RUNS' --timeout-db-s '$DB_STAGE_TIMEOUT_S'"
  printf '%s' "$extra"
}

_signing_needs_crypto() {
  [[ ",$SIGNING_MODES," == *,1,* ]]
}

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

  # ---- Remote node ----
  {
    sync_ok="${SYNC_STATUS[$node]:-FAIL}"
    if [[ "$sync_ok" != "OK" ]]; then
      lmsg "  [SKIP] $node – sync failed"
      NODE_STATUS["$node"]="skipped"
      continue
    fi

    remote_out="$repo/scripts/bench_results/parallel_ycsb_$(safe_label "$node")_${ts}"
    remote_log="$repo/.bench_tmp/bench_parallel_${ts}.log"
    NODE_LOG["$node"]="$remote_log"
    NODE_REMOTE_OUT["$node"]="$remote_out"

    extra_flags="$(_build_bench_flags "$repo")"
    remote_cmd="set -euo pipefail
mkdir -p '$remote_out' '$repo/.bench_tmp'
cd '$repo/scripts'
ensure_install_args=()
if [[ '$SKIP_SYNC' == '0' ]]; then
  if ! bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild; then
    if ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
      echo 'ERROR: synced install is stale/missing and make/gcc are unavailable for rebuild' >&2
      exit 1
    fi
    bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild
  fi
elif ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
  ensure_install_args+=(--trust-install)
  bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
    \"\${ensure_install_args[@]}\"
else
  bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild
fi
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$inst/bin/psql'
export ARIABC_INSTALL_DIR='$inst'
export ARIABC_DIR='$repo'
export ARIABC_PGPORT='$DB_PORT'
export PG_ISOLATION_LEVEL='$PG_ISOLATION'
export LD_LIBRARY_PATH='$inst/lib:\${LD_LIBRARY_PATH:-}'
pgdata_line=\$(bash '$repo/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$repo' --install-dir '$inst' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$repo/.bench_tmp/shared_postgresql.conf' \
  --require-custom --fresh-pgdata | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] && export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
PYTHON_BIN=''
if [[ -x '$repo/.venv/bin/python' ]] && '$repo/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$repo/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN='$repo/.venv/bin/python'
  [[ ! -x \"\$PYTHON_BIN\" ]] && python3 -m venv '$repo/.venv'
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    dest=\$(\"\$PYTHON_BIN\" -c 'import sysconfig; print(sysconfig.get_path(\"purelib\"))')
    for oroot in '$repo/.venv' '/home/neel/Desktop/ariabc_cluster/.venv' '/home/neel/.local'; do
      csite=\$(find \"\$oroot/lib\" -maxdepth 2 -type d -name site-packages 2>/dev/null | head -n1 || true)
      [[ -z \"\$csite\" ]] && continue
      [[ -d \"\$csite/psycopg\" ]] && cp -a \"\$csite/psycopg\" \"\$dest/\" 2>/dev/null || true
      for x in \"\$csite\"/psycopg_binary* \"\$csite\"/typing_extensions.py \"\$csite\"/psycopg-*.dist-info; do
        [[ -e \"\$x\" ]] && cp -a \"\$x\" \"\$dest/\" 2>/dev/null || true
      done
      \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1 && break || true
    done
  fi
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    if ! "\$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
      "\$PYTHON_BIN" -m ensurepip --upgrade >/dev/null 2>&1 || true
    fi
    if "\$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
      "\$PYTHON_BIN" -m pip install -q --disable-pip-version-check 'psycopg[binary]' || \
        "\$PYTHON_BIN" -m pip install -q --disable-pip-version-check psycopg
    elif [[ -x '$repo/.venv/bin/pip' ]]; then
      '$repo/.venv/bin/pip' install -q --disable-pip-version-check 'psycopg[binary]' || \
        '$repo/.venv/bin/pip' install -q --disable-pip-version-check psycopg
    else
      echo 'ERROR: pip unavailable to install psycopg on remote' >&2; exit 2
    fi
  fi
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    echo 'ERROR: psycopg unavailable on remote' >&2; exit 2
  fi
fi
export ARIABC_PYTHON="\$PYTHON_BIN"
if $(_signing_needs_crypto && echo true || echo false); then
  if ! "\$PYTHON_BIN" -c 'import cryptography' >/dev/null 2>&1; then
    if python3 -c 'import psycopg, cryptography' >/dev/null 2>&1; then
      PYTHON_BIN=python3
    fi
  fi
  if ! "\$PYTHON_BIN" -c 'import cryptography' >/dev/null 2>&1; then
    if "\$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
      "\$PYTHON_BIN" -m pip install -q --disable-pip-version-check cryptography
    elif [[ -x '$repo/.venv/bin/pip' ]]; then
      '$repo/.venv/bin/pip' install -q --disable-pip-version-check cryptography
    else
      "\$PYTHON_BIN" -m ensurepip --upgrade >/dev/null 2>&1 || true
      "\$PYTHON_BIN" -m pip install -q --disable-pip-version-check cryptography
    fi
  fi
  "\$PYTHON_BIN" -c 'import cryptography' >/dev/null 2>&1 || {
    echo 'ERROR: cryptography unavailable for signing mode' >&2; exit 2
  }
fi
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes '$MODES' --threads '$THREADS' --runs '$RUNS' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out'${extra_flags}"

    launch_err="$LOG_DIR/launch_err_$(safe_label "$node").log"
    # setsid creates a new session detached from the terminal so the bench
    # survives SSH disconnect even on hosts where bare `nohup` is killed when
    # the parent SSH session closes (observed on Ubuntu 22.04). stdin from
    # /dev/null prevents the bg shell from blocking on tty read.
    remote_pid=$(ssh_run "$node" "
      mkdir -p '$repo/.bench_tmp' '$remote_out'
      setsid nohup bash -lc $(printf '%q' "$remote_cmd") > '$remote_log' 2>&1 < /dev/null &
      echo \$!
    " 2>"$launch_err" | tail -n 1) || true

    if [[ -z "$remote_pid" ]] || ! [[ "$remote_pid" =~ ^[0-9]+$ ]]; then
      lmsg "  [FAIL] $node – could not capture remote PID (got: '$remote_pid') – see $launch_err"
      NODE_STATUS["$node"]="failed"
      continue
    fi

    NODE_PID["$node"]="$remote_pid"
    NODE_STATUS["$node"]="running"
    lmsg "  [LAUNCHED] $node  remote_pid=$remote_pid  log=$remote_log"
  }
done

echo

# Abort if any expected node failed to launch.
declare -a _launch_failed=()
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" == "failed" ]] && _launch_failed+=("$node")
done
if [[ "${#_launch_failed[@]}" -gt 0 ]]; then
  _abort_run "Bench launch failed on ${#_launch_failed[@]} node(s)" "${_launch_failed[@]}"
fi

running_at_start=0
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" == "running" ]] && ((running_at_start++)) || true
done

if [[ "$running_at_start" -eq 0 ]]; then
  _abort_run "No nodes were launched successfully — check sync/launch logs in $LOG_DIR"
fi

lmsg "All benchmarks launched ($running_at_start running). Monitoring every ${POLL_INTERVAL_S}s."
lmsg "Long-stage threshold: ${HANG_TIMEOUT_S}s of no parent-log change."
echo

# ============================================================
# PHASE 2.5: LIVE GUC VALIDATION
# Verify GUC parameters match dynamic Merkle contract
# ============================================================
lmsg "=== Phase 2.5: Verifying synchronous_commit and merkle_apply_synchronous_direct on all nodes ==="
declare -a _sc_bad=()
_check_synchronous_commit() {
  local node="$1"
  local repo="$2"
  local inst="$3"
  local type="$4"
  local psql_bin="$inst/bin/psql"
  local sc_val=""
  local merkle_sync_val=""
  local advance_val=""
  local gate_source_val=""

  for ((attempt=1; attempt <= DB_READY_TIMEOUT_S / 2; attempt++)); do
    sc_val=$(ssh_run "$node" \
      "LD_LIBRARY_PATH='$inst/lib' '$psql_bin' -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' \
       -tAc 'SHOW synchronous_commit;' 2>/dev/null || true" 2>/dev/null || true)
    sc_val="${sc_val//[[:space:]]/}"

    merkle_sync_val=$(ssh_run "$node" \
      "PGOPTIONS='-c merkle_apply_synchronous_direct=on' LD_LIBRARY_PATH='$inst/lib' '$psql_bin' -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' \
       -tAc 'SHOW merkle_apply_synchronous_direct;' 2>/dev/null || true" 2>/dev/null || true)
    merkle_sync_val="${merkle_sync_val//[[:space:]]/}"

    advance_val=$(ssh_run "$node" \
      "LD_LIBRARY_PATH='$inst/lib' '$psql_bin' -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' \
       -tAc 'SHOW bcdb_advance_commit_watermark;' 2>/dev/null || true" 2>/dev/null || true)
    advance_val="${advance_val//[[:space:]]/}"

    gate_source_val=$(ssh_run "$node" \
      "LD_LIBRARY_PATH='$inst/lib' '$psql_bin' -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' \
       -tAc 'SHOW bcdb_serial_gate_source;' 2>/dev/null || true" 2>/dev/null || true)
    gate_source_val="${gate_source_val//[[:space:]]/}"

    if [[ "$sc_val" == "on" && "$merkle_sync_val" == "on" && \
          "$advance_val" == "on" && "$gate_source_val" == "0" ]]; then
      lmsg "  [OK]  $node  synchronous_commit=on, merkle_apply_synchronous_direct=on, bcdb_advance_commit_watermark=on, bcdb_serial_gate_source=0"
      return 0
    elif [[ -n "$sc_val" && -n "$merkle_sync_val" && -n "$advance_val" && -n "$gate_source_val" ]]; then
      lmsg "  [FAIL] $node  synchronous_commit=$sc_val merkle_apply_synchronous_direct=$merkle_sync_val bcdb_advance_commit_watermark=$advance_val bcdb_serial_gate_source=$gate_source_val (expected: on, on, on, 0)"
      _sc_bad+=("$node")
      return 0
    fi
    sleep 2
  done

  lmsg "  [FAIL] $node  Postgres GUCs could not be read within ${DB_READY_TIMEOUT_S}s"
  _sc_bad+=("$node")
}

for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  [[ "${NODE_STATUS[$node]:-}" == "running" ]] || continue
  _check_synchronous_commit "$node" "$repo" "$inst" "$type"
done

if [[ "${#_sc_bad[@]}" -gt 0 ]]; then
  _abort_run "GUC verification failed on ${#_sc_bad[@]} node(s)" "${_sc_bad[@]}"
fi
lmsg "Phase 2.5 complete: synchronous commit, direct Merkle apply, and non-blocking DET watermark verified on all nodes."
echo

# ============================================================
# PHASE 3: MONITOR LOOP
# ============================================================

_active_count() {
  local c=0
  for n in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$n]}" == "running" ]] && ((c++)) || true
  done
  echo "$c"
}

_collect_node() {
  local node="$1"
  local remote_out="${NODE_REMOTE_OUT[$node]}"
  local safe_node
  safe_node="$(safe_label "$node")"
  local local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  local collect_log="$LOG_DIR/collect_${safe_node}.log"

  mkdir -p "$local_node_dir"
  rsync -az \
    -e "$(_rsync_e_for_node "$node")" \
    "$node:$remote_out/" "$local_node_dir/" \
    > "$collect_log" 2>&1 || true

  # Also pull the main bench nohup log
  local remote_bench_log="${NODE_LOG[$node]}"
  rsync -az \
    -e "$(_rsync_e_for_node "$node")" \
    "$node:$remote_bench_log" "$LOG_DIR/bench_${safe_node}.log" \
    >> "$collect_log" 2>&1 || true

  if [[ -f "$local_node_dir/results.csv" && -f "$local_node_dir/summary.csv" ]]; then
    NODE_STATUS["$node"]="done"
    lmsg "  [DONE] $node – results collected → $local_node_dir"
    _generate_graphs "$local_node_dir" "$node"
  else
    NODE_STATUS["$node"]="failed"
    lmsg "  [FAIL] $node – missing results.csv or summary.csv in $local_node_dir"
  fi
}

_generate_graphs() {
  local out_dir="$1"
  local node="$2"
  local summary="$out_dir/summary.csv"
  [[ -f "$summary" ]] || return 0
  local existing
  existing=$(find "$out_dir" -maxdepth 1 -name '*.png' | wc -l)
  if [[ "$existing" -gt 0 ]]; then
    lmsg "  [GRAPH] $node – $existing PNG(s) already present, skipping"
    return 0
  fi
  ARIABC_REPO_ROOT="$REPO_ROOT" python3 - "$summary" "$out_dir" <<'PYEOF' \
    && lmsg "  [GRAPH] $node – graphs generated in $out_dir" \
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

_live_sync_node() {
  # Non-blocking rsync of partial results from a running remote node.
  # Skips if a previous rsync for this node is still in flight.
  local node="$1"
  local remote_out="${NODE_REMOTE_OUT[$node]}"
  local safe_node
  safe_node="$(safe_label "$node")"
  local local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  local sync_log="$LOG_DIR/livesync_${safe_node}.log"

  # If a previous live-sync rsync is still running, skip this poll cycle
  local prev_pid="${NODE_RSYNC_PID[$node]:-}"
  if [[ -n "$prev_pid" ]] && kill -0 "$prev_pid" 2>/dev/null; then
    return 0
  fi

  mkdir -p "$local_node_dir"
  (
    # Check if the remote output directory exists yet (bench may still be in setup)
    if ! ssh_run "$node" "test -d '$remote_out'" 2>>"$sync_log"; then
      echo "[$(_ts)] live-sync: remote dir not ready yet: $remote_out" >> "$sync_log"
      exit 0
    fi

    rsync -az \
      -e "$(_rsync_e_for_node "$node")" \
      "$node:$remote_out/" "$local_node_dir/" \
      >> "$sync_log" 2>&1
    echo "[$(_ts)] live-sync OK: $(find "$local_node_dir" -name '*.csv' | wc -l) csv(s) present" \
      >> "$sync_log"
  ) &
  NODE_RSYNC_PID["$node"]=$!
}

_check_node() {
  local node="$1"
  local pid="${NODE_PID[$node]}"
  local bench_log="${NODE_LOG[$node]}"

  local alive=0
  ssh_run "$node" "kill -0 $pid 2>/dev/null" 2>/dev/null && alive=1 || true

  if [[ "$alive" == "1" ]]; then
    local tail_out=""
    tail_out=$(ssh_run "$node" "tail -10 '$bench_log' 2>/dev/null || true" 2>/dev/null || true)

    # A workload case can legitimately spend several minutes in restore or
    # synchronous Merkle materialization without changing the parent log. Treat this as a
    # long-stage warning, not proof of a deadlock; bench_threads_matrix.py has
    # the bounded DB-stage timeout passed in the launch command.
    local hash; hash=$(printf '%s' "$tail_out" | md5sum | cut -d' ' -f1)
    local prev="${NODE_LAST_HASH[$node]:-}"
    local now; now="$(date +%s)"
    if [[ "$hash" != "$prev" ]]; then
      NODE_LAST_HASH["$node"]="$hash"
      NODE_LAST_CHG["$node"]="$now"
    else
      local stale=$(( now - NODE_LAST_CHG["$node"] ))
      if [[ "$stale" -ge "$HANG_TIMEOUT_S" && "${NODE_LAST_WARN[$node]:-0}" != "$(( stale / HANG_TIMEOUT_S ))" ]]; then
        local case_info
        case_info=$(ssh_run "$node" "cat '${NODE_REMOTE_OUT[$node]}/current_case.json' 2>/dev/null || true" 2>/dev/null || true)
        echo "  *** LONG-STAGE WARNING: $node – no parent-log change for ${stale}s (pid=$pid) ***"
        [[ -n "$case_info" ]] && echo "      active case: $case_info"
        echo "      This is not a hang verdict; DB-stage timeout=${DB_STAGE_TIMEOUT_S}s."
        NODE_LAST_WARN["$node"]="$(( stale / HANG_TIMEOUT_S ))"
      fi
    fi

    echo "  -- $node [RUNNING pid=$pid] --"
    if [[ -n "$tail_out" ]]; then
      while IFS= read -r line; do echo "     $line"; done <<< "$tail_out"
    else
      echo "     (no output yet)"
    fi

    _live_sync_node "$node"
  else
    lmsg "  Process finished on $node – collecting results..."
    # Wait for any in-flight live-sync to finish before the final collect
    local prev_pid="${NODE_RSYNC_PID[$node]:-}"
    [[ -n "$prev_pid" ]] && wait "$prev_pid" 2>/dev/null || true
    _collect_node "$node"
  fi
}

while [[ "$(_active_count)" -gt 0 ]]; do
  echo "=========================================="
  echo "  Poll $(_ts)  |  Running: $(_active_count)"
  echo "=========================================="

  for node in "${!NODE_STATUS[@]}"; do
    [[ "${NODE_STATUS[$node]}" == "running" ]] || continue
    _check_node "$node"
  done

  # Summary line
  running_nodes=(); done_nodes=(); fail_nodes=()
  for node in "${!NODE_STATUS[@]}"; do
    case "${NODE_STATUS[$node]}" in
      running)  running_nodes+=("$node") ;;
      done)     done_nodes+=("$node") ;;
      failed)   fail_nodes+=("$node") ;;
    esac
  done
  echo
  echo "  Running  (${#running_nodes[@]}): ${running_nodes[*]:-—}"
  echo "  Done     (${#done_nodes[@]}):    ${done_nodes[*]:-—}"
  echo "  Failed   (${#fail_nodes[@]}):    ${fail_nodes[*]:-—}"
  echo

  # Abort immediately if any node failed during the run.
  if [[ "${#fail_nodes[@]}" -gt 0 ]]; then
    lmsg "Bench log tails for failed nodes:"
    for node in "${fail_nodes[@]}"; do
      local_bench_log="${NODE_LOG[$node]:-}"
      if [[ -n "$local_bench_log" ]]; then
        lmsg "  -- $node: $local_bench_log (remote) --"
        ssh_run "$node" "tail -15 '$local_bench_log' 2>/dev/null || true" 2>/dev/null \
          | while IFS= read -r l; do lmsg "     $l"; done
      fi
    done
    _abort_run "Bench process failed on ${#fail_nodes[@]} node(s) during monitoring" "${fail_nodes[@]}"
  fi

  [[ "$(_active_count)" -gt 0 ]] && sleep "$POLL_INTERVAL_S"
done

# ============================================================
# PHASE 4: FINAL REPORT
# ============================================================
lmsg "=== Phase 4: All nodes finished ==="
echo

# Final collect pass for any that finished right before the loop ended
for node in "${!NODE_STATUS[@]}"; do
  [[ "${NODE_STATUS[$node]}" != "done" ]] && continue
  local_node_dir="$LOCAL_RESULT_ROOT/$(safe_label "$node")"
  if [[ ! -f "$local_node_dir/results.csv" ]]; then
    _collect_node "$node"
  fi
done

echo "=== Final Status ==="
printf "%-52s %-8s %s\n" "Node" "Status" "Local results dir"
printf "%-52s %-8s %s\n" "----" "------" "-----------------"
for node in "${!NODE_STATUS[@]}"; do
  safe_node="$(safe_label "$node")"
  local_node_dir="$LOCAL_RESULT_ROOT/$safe_node"
  status="${NODE_STATUS[$node]}"
  printf "%-52s %-8s %s\n" "$node" "$status" "$local_node_dir"
done
echo

# ============================================================
# PHASE 5: DEDICATED PER-NODE & COMBINED GRAPHS
# ============================================================
lmsg "=== Phase 5: Generating per-node and combined graphs ==="
python3 - "$LOCAL_RESULT_ROOT" <<'PYEOF'
import csv
import sys
import re
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    print("  [GRAPH] matplotlib not found, skipping graph generation.")
    sys.exit(0)

result_root = Path(sys.argv[1])
node_dirs = [d for d in result_root.iterdir() if d.is_dir() and d.name != "_run_logs"]

def as_int(v):
    try: return int((v or "").strip())
    except: return None

def as_float(v):
    try: return float((v or "").strip())
    except: return None

mode_norm = {
    "postgres": "pg", "pg": "pg",
    "bcdb_det": "bcdb_det", "bcdb": "bcdb_det",
    "bcdb_merkle": "bcdb_merkle", "det": "bcdb_merkle", "safedb": "bcdb_merkle",
    "aria": "nondet", "nondet": "nondet",
}

mode_meta = {
    "pg": {"label": "Plain PostgreSQL (pg)", "color": "#1f77b4", "marker": "o", "linestyle": "-"},
    "bcdb_det": {"label": "BCDB Deterministic (bcdb_det)", "color": "#ff7f0e", "marker": "s", "linestyle": "--"},
    "bcdb_merkle": {"label": "BCDB Dynamic Merkle (bcdb_merkle)", "color": "#2ca02c", "marker": "D", "linestyle": "-"},
}

node_title_map = {
    "neel_at_10_129_148_248": "Node 1: 10.129.148.248 (utkarsh-MS-7C96)",
    "neel_at_10_129_148_246": "Node 2: 10.129.148.246 (kartik-MS-7C96)",
    "neel_at_10_129_148_247": "Node 3: 10.129.148.247 (neel-MS-7C96)",
    "protectdr_at_ranking_cse_iitb_ac_in": "Node 4: ranking.cse.iitb.ac.in (AMD EPYC 9654 96-Core)",
}

groups = {}
node_summaries = {}

for ndir in node_dirs:
    node_name = ndir.name
    summary_csv = ndir / "summary.csv"
    if not summary_csv.exists():
        continue
    node_rows = []
    with summary_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_rows.append(row)
            wl = (row.get("workload") or "").strip()
            if not wl: continue
            rate = as_int(row.get("rate", "")) or 0
            groups.setdefault((wl, rate), []).append((node_name, row))
    if node_rows:
        node_summaries[node_name] = node_rows

# 1. Generate dedicated per-node comparison graphs
for node_name, rows in node_summaries.items():
    workloads_in_node = sorted(list({(r.get("workload") or "").strip() for r in rows if (r.get("workload") or "").strip()}))
    if not workloads_in_node:
        continue

    num_wl = len(workloads_in_node)
    fig_w = 7.5 * num_wl
    fig, axes = plt.subplots(1, num_wl, figsize=(fig_w, 5.8), dpi=160, squeeze=False)
    display_node = node_title_map.get(node_name, node_name.replace("_at_", "@").replace("_", "."))
    fig.suptitle(f"AriaBC Throughput (TPS vs Threads)\n{display_node}", fontsize=13, fontweight="bold", y=0.98)

    for ax_idx, wl in enumerate(workloads_in_node):
        ax = axes[0][ax_idx]
        stem = re.sub(r'[^A-Za-z0-9_.-]', '_', wl)
        title = f"{wl}"
        if "skew0-99" in wl or "skew0.99" in wl:
            title = f"YCSB Skew 0.99 (Point + Inserts)\n{wl}"
        elif "skew-01" in wl or "skew0.1" in wl:
            title = f"YCSB Skew 0.1 (Clean Txns)\n{wl}"
        ax.set_title(title, fontsize=10, fontweight="bold", pad=8)
        ax.set_xlabel("Concurrent Threads", fontsize=9, fontweight="bold")
        ax.set_ylabel("Throughput (TPS)", fontsize=9, fontweight="bold")
        ax.grid(True, linestyle="--", alpha=0.5, color="#cccccc")

        for mode_key in ["pg", "bcdb_det", "bcdb_merkle"]:
            meta = mode_meta[mode_key]
            sub = [
                r for r in rows
                if (r.get("workload") or "").strip() == wl and mode_norm.get((r.get("mode") or "").strip().lower()) == mode_key
            ]
            sub = sorted(sub, key=lambda x: as_int(x.get("threads")) or 0)
            if not sub:
                continue
            xs = [as_int(r.get("threads")) for r in sub if as_int(r.get("threads")) is not None]
            ys = [as_float(r.get("median_throughput_tps")) or as_float(r.get("mean_throughput_tps")) for r in sub]
            valid_pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
            if not valid_pairs:
                continue
            pxs = [p[0] for p in valid_pairs]
            pys = [p[1] for p in valid_pairs]

            ax.plot(
                pxs, pys,
                label=meta["label"],
                color=meta["color"],
                marker=meta["marker"],
                markersize=5.5,
                linewidth=2.0,
                linestyle=meta["linestyle"]
            )

            # Peak label
            if pys:
                max_idx = pys.index(max(pys))
                ax.annotate(
                    f"{pys[max_idx]:.0f}",
                    xy=(pxs[max_idx], pys[max_idx]),
                    xytext=(0, 6),
                    textcoords="offset points",
                    ha="center",
                    fontsize=8,
                    color=meta["color"],
                    fontweight="bold"
                )

        ax.set_xticks(sorted(list({as_int(r.get("threads")) for r in rows if as_int(r.get("threads")) is not None})))
        ax.set_ylim(bottom=0)
        ax.legend(frameon=True, facecolor="white", edgecolor="#e0e0e0", fontsize=8.5, loc="upper left")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    out_path = result_root / f"{node_name}_tps.png"
    fig.savefig(out_path, bbox_inches='tight')
    plt.close(fig)
    print(f"  [GRAPH] Generated per-node graph: {out_path.name}")

# 2. Generate combined cross-node graphs
for (workload, rate), items in groups.items():
    series = {}
    for node_name, r in items:
        raw_mode = (r.get("mode") or "").strip().lower()
        mode = mode_norm.get(raw_mode, raw_mode)

        if mode not in ("pg", "bcdb_det", "bcdb_merkle"):
            continue

        label = f"{node_name}_{mode}"
        th = as_int(r.get("threads", ""))
        tps = as_float(r.get("median_throughput_tps", ""))
        if tps is None:
            tps = as_float(r.get("mean_throughput_tps", ""))
        if th is None or tps is None:
            continue

        series.setdefault(label, []).append((th, tps))

    if not any(series.values()):
        continue

    fig, ax = plt.subplots(figsize=(10, 6), dpi=130)

    for label in sorted(series.keys()):
        points = sorted(series[label], key=lambda x: x[0])
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]

        ls = "-"
        if "_bcdb_det" in label: ls = "--"
        if "_bcdb_merkle" in label: ls = "-"
        if "_nondet" in label: ls = ":"

        ax.plot(xs, ys, marker="o", linewidth=1.5, markersize=4.0, linestyle=ls, label=label)

    ax.set_xlabel("Threads")
    ax.set_ylabel("TPS")
    title = f"Combined TPS vs Threads - {workload}"
    if rate > 0:
        title += f" - rate={rate}"
    ax.set_title(title)
    ax.grid(True, linestyle="--", alpha=0.6)

    if len(series) > 6:
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.)
    else:
        ax.legend()

    def safe_name(s):
        s = s.replace('@', '_at_')
        return re.sub(r'[^A-Za-z0-9_.-]', '_', s)

    stem = safe_name(workload)
    out_name = f"combined_tps_{stem}"
    if rate > 0:
        out_name += f"_rate-{rate}"
    out_name += ".png"

    out_path = result_root / out_name
    fig.savefig(out_path, bbox_inches='tight')
    plt.close(fig)
    print(f"  [GRAPH] Generated combined graph: {out_name}")
PYEOF

echo

lmsg "Logs:    $LOG_DIR"
lmsg "Results: $LOCAL_RESULT_ROOT"
