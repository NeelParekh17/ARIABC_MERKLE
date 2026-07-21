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
#   neel@10.129.148.248    utkarsh-MS-7C96
#   neel@10.129.148.246      kartik-MS-7C96  (Ubuntu 22.04 – on-host rebuild)
#   neel@10.129.148.247    neel-MS-7C96
#
# Default benchmark profiles (one run × modes in this order):
#   1. pg mode             – plain PostgreSQL baseline (no BCDB)
#   2. det mode sign=0     – deterministic, unsigned, enforce_signatures=1
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
#   --ssh-key <path>        SSH private key (optional if key-auth is default)
#   --ssh-port <22>         SSH port
#   --modes <pg,det>        Comma-separated modes: pg, det, nondet  [default: pg,det]
#   --threads <csv>         Thread counts csv  [default: 1,2,4,8,12,16]
#   --runs <1>              Runs per workload/thread combination
#   --transaction-isolation <level>  Client isolation for both modes [default: serializable]
#   --workloads <csv>       Workload filenames (default: ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt)
#   --rates <csv>           Rate limits csv (optional)
#   --signing-modes <0,1>   Signing modes for det runs: 0=unsigned, 1=signed  [default: 0]
#   --signing-privkey <p>   Signing key path (relative to repo root)  [default: scripts/bench_signing_privkey.pem]
#   --enforce-signatures <1> 0|1 — set bcdb_enforce_signatures in workload sessions  [default: 1]
#   --max-retries <50>      Per-statement retry budget for transient serialization conflicts
#   --legacy-merkle         Use the legacy static Merkle restore (default: native dynamic)
#   --dynamic-structure-profile <0|1>  Profile native DET split/merge counters  [default: 1]
#                            Dynamic runs require native layout v6, logical fanout
#                            32 over physical fanout 2, and record tree depth.
#   --poll-interval <60>    Seconds between monitoring polls
#   --hang-timeout <60>     Seconds of no log change before hang warning
#   --stall-timeout <1800>  Abort if a live benchmark has no log progress this long
#   --skip-sync             Skip the rsync phase (reuse last-synced remote source)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

# ---- Static node config ----
NEEL_NODES=(
  "neel@10.129.148.248"
  "neel@10.129.148.246"
  "neel@10.129.148.247"
)
NEEL_REMOTE_REPO="/home/neel/Desktop/ariabc_cluster"
NEEL_REMOTE_INSTALL="/home/neel/Desktop/ariabc_install"
TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"

# Node-specific password auth (all nodes now use key auth; add entries here only
# if a node requires sshpass).
declare -A NODE_PASSWORDS=()

# ---- Tunable defaults ----
SSH_KEY=""
SSH_PORT=22
MODES="pg,det"
THREADS="${ARIABC_DEFAULT_FULL_THREADS}"
RUNS=1
TRANSACTION_ISOLATION="serializable"
WORKLOADS="ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
RATES=""
SIGNING_MODES="0"
SIGNING_PRIVKEY="scripts/bench_signing_privkey.pem"
ENFORCE_SIGNATURES="1"
BENCH_MAX_RETRIES=50
DYNAMIC_MERKLE=1
DYNAMIC_STRUCTURE_PROFILE=1
DB_NAME="postgres"
DB_USER="postgres"
DB_PORT=5438
POLL_INTERVAL_S=60
HANG_TIMEOUT_S=60
STALL_TIMEOUT_S=1800
SKIP_SYNC=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh-key)       SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port)      SSH_PORT="${2:-22}"; shift 2 ;;
    --modes)         MODES="${2:-pg,det}"; shift 2 ;;
    --threads)       THREADS="${2:-}"; shift 2 ;;
    --runs)          RUNS="${2:-1}"; shift 2 ;;
    --transaction-isolation) TRANSACTION_ISOLATION="${2:-}"; shift 2 ;;
    --workloads)     WORKLOADS="${2:-}"; shift 2 ;;
    --rates)         RATES="${2:-}"; shift 2 ;;
    --signing-modes) SIGNING_MODES="${2:-}"; shift 2 ;;
    --signing-privkey) SIGNING_PRIVKEY="${2:-}"; shift 2 ;;
    --enforce-signatures) ENFORCE_SIGNATURES="${2:-}"; shift 2 ;;
    --max-retries)   BENCH_MAX_RETRIES="${2:-50}"; shift 2 ;;
    --legacy-merkle)  DYNAMIC_MERKLE=0; shift 1 ;;
    --dynamic-structure-profile) DYNAMIC_STRUCTURE_PROFILE="${2:-1}"; shift 2 ;;
    --poll-interval) POLL_INTERVAL_S="${2:-60}"; shift 2 ;;
    --hang-timeout)  HANG_TIMEOUT_S="${2:-60}"; shift 2 ;;
    --stall-timeout) STALL_TIMEOUT_S="${2:-1800}"; shift 2 ;;
    --skip-sync)     SKIP_SYNC=1; shift 1 ;;
    -h|--help)
      sed -n '/^# Usage/,/^[^#]/p' "$0" | head -n 20
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ "$DYNAMIC_STRUCTURE_PROFILE" =~ ^[01]$ ]] || {
  echo "ERROR: --dynamic-structure-profile must be 0 or 1" >&2
  exit 2
}

case "$TRANSACTION_ISOLATION" in
  "read committed"|"repeatable read"|"serializable") ;;
  *) echo "ERROR: unsupported --transaction-isolation: $TRANSACTION_ISOLATION" >&2; exit 2 ;;
esac

for numeric in POLL_INTERVAL_S HANG_TIMEOUT_S STALL_TIMEOUT_S BENCH_MAX_RETRIES; do
  value="${!numeric}"
  [[ "$value" =~ ^[0-9]+$ ]] && (( value > 0 )) || {
    echo "ERROR: $numeric must be a positive integer (got: $value)" >&2
    exit 2
  }
done
if (( STALL_TIMEOUT_S < HANG_TIMEOUT_S )); then
  echo "ERROR: --stall-timeout must be >= --hang-timeout" >&2
  exit 2
fi

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

if [[ "$DYNAMIC_MERKLE" == "1" && ! -f "$REPO_ROOT/scripts/distributed/sql/restore_usertable_small_dynamic.sql" ]]; then
  echo "ERROR: native dynamic Merkle restore SQL is missing" >&2
  exit 1
fi

for node in "${!NODE_PASSWORDS[@]}"; do
  if [[ -n "${NODE_PASSWORDS[$node]}" ]]; then
    command -v sshpass >/dev/null 2>&1 || {
      echo "ERROR: sshpass is required because password auth is configured for $node" >&2
      exit 1
    }
  fi
done

LOCAL_SOURCE_FINGERPRINT="$(
  bash "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
    --repo-root "$REPO_ROOT" --install-dir /work/ARIABC/install \
    --build-profile release --print-source-fingerprint
)"
[[ "$LOCAL_SOURCE_FINGERPRINT" =~ ^[0-9a-f]{64}$ ]] || {
  echo "ERROR: failed to compute local PostgreSQL/BCDB source fingerprint" >&2
  exit 1
}

ts="$(date +%Y%m%d_%H%M%S)"
LOCAL_RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results/parallel_ycsb_${ts}"
LOG_DIR="$LOCAL_RESULT_ROOT/_run_logs"
mkdir -p "$LOG_DIR"
{
  printf 'source_fingerprint=%s\n' "$LOCAL_SOURCE_FINGERPRINT"
  printf 'source_fingerprint_contract=source_and_install_stamp_match\n'
} > "$LOCAL_RESULT_ROOT/source_provenance.env"

lmsg "=== Parallel YCSB benchmark – all nodes ==="
lmsg "Timestamp    : $ts"
lmsg "Modes        : $MODES"
lmsg "DET profile  : $([[ "$DYNAMIC_STRUCTURE_PROFILE" == "1" && "$DYNAMIC_MERKLE" == "1" ]] && echo enabled || echo disabled)"
lmsg "Threads      : $THREADS"
lmsg "Runs         : $RUNS"
lmsg "Tx isolation : $TRANSACTION_ISOLATION"
lmsg "Merkle       : $([[ "$DYNAMIC_MERKLE" == "1" ]] && echo native-dynamic/synchronous_cow || echo legacy-static)"
lmsg "Stall abort  : ${STALL_TIMEOUT_S}s (warning at ${HANG_TIMEOUT_S}s)"
lmsg "Workloads    : ${WORKLOADS:-<bench_threads_matrix.py defaults>}"
lmsg "SigningModes : ${SIGNING_MODES:-<default>}"
lmsg "SigningKey   : ${SIGNING_PRIVKEY_LOCAL:-<not set>}"
lmsg "EnforceSig   : ${ENFORCE_SIGNATURES:-<default>}"
lmsg "Max retries  : $BENCH_MAX_RETRIES"
lmsg "Source FP    : $LOCAL_SOURCE_FINGERPRINT"
lmsg "Result root  : $LOCAL_RESULT_ROOT"
echo

# ---- Build node registry ----
# Entry format: "node|repo_root|install_dir|type"
declare -a ALL_NODES=()

for n in "${NEEL_NODES[@]}"; do
  ALL_NODES+=("$n|${NEEL_REMOTE_REPO}|${NEEL_REMOTE_INSTALL}|neel")
done

lmsg "Nodes:"
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

if [[ "$SKIP_SYNC" == "0" ]]; then
  lmsg "=== Preflight: Verifying canonical local install provenance ==="
  if ! bash "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
      --repo-root "$REPO_ROOT" --install-dir /work/ARIABC/install \
      --clean-when-rebuild --build-profile release \
      --require-source-fingerprint "$LOCAL_SOURCE_FINGERPRINT"; then
    _abort_run "Canonical local PostgreSQL install is stale or failed to rebuild"
  fi
  lmsg "  [OK] local install matches source fingerprint $LOCAL_SOURCE_FINGERPRINT"
  echo
fi

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

      # rsync success freezes the exact orchestrator source identity for this
      # node. The host-native install must carry the same fingerprint before
      # any benchmark process is allowed to launch.
      ssh_run "$node" \
        "printf '%s\\n' '$LOCAL_SOURCE_FINGERPRINT' > '$repo/.ariabc_synced_source_fingerprint'" \
        >> "$slog" 2>&1

      # Ubuntu 24.04 nodes can use the orchestrator's install directly.  The
      # Ubuntu 22.04 host is ABI-incompatible, so preserve its host-native
      # install here. Phase 1.5 compares its build stamp with the synchronized
      # source fingerprint and rebuilds on-host whenever they differ.
      if [[ "$node" == "neel@10.129.148.246" ]]; then
        echo "[INFO] preserving host-native release install on $node" >> "$slog"
      else
        _rsync_allow_vanished -az --delete \
          -e "$(_rsync_e_for_node "$node")" \
          /work/ARIABC/install/ "$node:$inst/" >> "$slog" 2>&1
      fi

      # Copy canonical postgresql.conf template
      _rsync_allow_vanished -az \
        -e "$(_rsync_e_for_node "$node")" \
        "$TEMPLATE_CONF_LOCAL" "$node:$repo/.bench_tmp/shared_postgresql.conf" >> "$slog" 2>&1

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
      echo '[INFO] source/install were just synced; trusting synced install if it verifies'
      if bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
        --build-profile release --trust-install \
        --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'; then
        exit 0
      fi
      echo '[INFO] synced install did not verify on this host; attempting local rebuild'
      if ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
        echo 'ERROR: synced install did not verify and make/gcc are unavailable for rebuild' >&2
        exit 1
      fi
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
        --build-profile release \
        --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'
    elif ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
      echo '[INFO] make/gcc missing; trusting existing install if it verifies'
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
        --build-profile release --trust-install \
        --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'
    else
      bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
        --build-profile release \
        --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'
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
  [[ "$DYNAMIC_MERKLE" == "1" ]] && extra+=" --dynamic-merkle --dynamic-merkle-index 'public.usertable_small_dynamic_merkle_idx'"
  if [[ "$DYNAMIC_MERKLE" == "1" && "$DYNAMIC_STRUCTURE_PROFILE" == "1" && ",${MODES}," == *,det,* ]]; then
    extra+=" --dynamic-merkle-profile"
  fi
  extra+=" --transaction-isolation '$TRANSACTION_ISOLATION'"
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
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
    --build-profile release --trust-install \
    --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'; then
    if ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
      echo 'ERROR: synced install did not verify and make/gcc are unavailable for rebuild' >&2
      exit 1
    fi
    bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
      --build-profile release \
      --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'
  fi
elif ! command -v make >/dev/null 2>&1 || ! command -v gcc >/dev/null 2>&1; then
  ensure_install_args+=(--trust-install)
  bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
    --build-profile release --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT' \
    \"\${ensure_install_args[@]}\"
else
  bash '$repo/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$repo' --install-dir '$inst' --clean-when-rebuild \
    --build-profile release \
    --require-source-fingerprint '$LOCAL_SOURCE_FINGERPRINT'
fi
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$inst/bin/psql'
export ARIABC_INSTALL_DIR='$inst'
export ARIABC_DIR='$repo'
export ARIABC_PGPORT='$DB_PORT'
export ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1
export MAX_RETRIES='$BENCH_MAX_RETRIES'
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
lmsg "Hang detection threshold: ${HANG_TIMEOUT_S}s of no log change."
echo

# Nounset-safe state for the optional failed-node log-tail report.
local_bench_log=""

# ============================================================
# PHASE 2.5: VERIFY WAL DURABILITY SETTINGS ON ALL NODES
# Query the live running Postgres on every node right now,
# while the benchmark is active, to confirm the setting has
# not been flipped by auto.conf, ALTER SYSTEM, or a session SET.
# ============================================================
lmsg "=== Phase 2.5: Verifying WAL durability settings on all nodes ==="
declare -a _sc_bad=()
_check_synchronous_commit() {
  local node="$1"
  local type="$2"
  local inst="${NODE_INST[$node]:-}"
  local psql_bin="$inst/bin/psql"
  local settings=""
  local attempt
  for attempt in $(seq 1 15); do
    settings=$(ssh_run "$node" \
      "LD_LIBRARY_PATH='$inst/lib' '$psql_bin' -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' \
       -tAc \"select current_setting('synchronous_commit') || '|' || current_setting('fsync') || '|' || current_setting('full_page_writes') || '|' || current_setting('wal_level');\" 2>/dev/null || true" 2>/dev/null || true)
    settings="${settings//[[:space:]]/}"
    [[ -n "$settings" ]] && break
    sleep 2
  done

  local sc_val fsync_val fpw_val wal_level
  IFS='|' read -r sc_val fsync_val fpw_val wal_level <<< "$settings"
  if [[ "$sc_val" == "on" && "$fsync_val" == "on" && "$fpw_val" == "on" && "$wal_level" != "minimal" && -n "$wal_level" ]]; then
    lmsg "  [OK]  $node  synchronous_commit=$sc_val fsync=$fsync_val full_page_writes=$fpw_val wal_level=$wal_level"
  elif [[ -z "$settings" ]]; then
    lmsg "  [FAIL] $node  could not read WAL durability settings after 30s"
    _sc_bad+=("$node")
  else
    lmsg "  [FAIL] $node  WAL settings: synchronous_commit=$sc_val fsync=$fsync_val full_page_writes=$fpw_val wal_level=$wal_level"
    _sc_bad+=("$node")
  fi
}

for entry in "${ALL_NODES[@]}"; do
  IFS='|' read -r node repo inst type <<< "$entry"
  [[ "${NODE_STATUS[$node]:-}" == "running" ]] || continue
  _check_synchronous_commit "$node" "$type"
done

if [[ "${#_sc_bad[@]}" -gt 0 ]]; then
  _abort_run "WAL durability settings invalid on ${#_sc_bad[@]} node(s)" "${_sc_bad[@]}"
fi
lmsg "Phase 2.5 complete: synchronous_commit/fsync/full_page_writes/WAL level validated on all nodes."
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
  local local_node_dir="$LOCAL_RESULT_ROOT/$(safe_label "$node")"

  mkdir -p "$local_node_dir"
  rsync -az \
    -e "$(_rsync_e_for_node "$node")" \
    "$node:$remote_out/" "$local_node_dir/" 2>/dev/null || \
    lmsg "  WARNING: rsync of results failed for $node"

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
    # Keep poll output focused on benchmark progress.  PostgreSQL emits
    # harmless idempotent bootstrap notices for dropped constraints/triggers;
    # retain them in the remote log but omit them from the live dashboard.
    tail_out="$(printf '%s\n' "$tail_out" | grep -vE '^psql:.*NOTICE:.*skipping$' || true)"

    # Hang detection
    local hash; hash=$(printf '%s' "$tail_out" | md5sum | cut -d' ' -f1)
    local prev="${NODE_LAST_HASH[$node]:-}"
    local now; now="$(date +%s)"
    if [[ "$hash" != "$prev" ]]; then
      NODE_LAST_HASH["$node"]="$hash"
      NODE_LAST_CHG["$node"]="$now"
    else
      local stale=$(( now - NODE_LAST_CHG["$node"] ))
      if [[ "$stale" -ge "$HANG_TIMEOUT_S" ]]; then
        echo "  *** HANG WARNING: $node – no log change for ${stale}s (pid=$pid) ***"
      fi
      if [[ "$stale" -ge "$STALL_TIMEOUT_S" ]]; then
        _abort_run "benchmark stalled on $node for ${stale}s with no log progress" "$node"
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

if [[ "$DYNAMIC_STRUCTURE_PROFILE" == "1" && "$DYNAMIC_MERKLE" == "1" && ",${MODES}," == *,det,* ]]; then
  lmsg "=== Native DET dynamic split/merge profile validation ==="
  if ! python3 - "$LOCAL_RESULT_ROOT" "${!NODE_STATUS[@]}" <<'PYEOF'
import csv, sys
from pathlib import Path

root = Path(sys.argv[1])
nodes = sys.argv[2:]
maps = {}
for node in nodes:
    safe = node.replace('@', '_at_').replace('.', '_').replace('-', '_')
    ndir = root / safe
    summary = ndir / "summary.csv"
    if not summary.exists():
        raise SystemExit(f"missing summary.csv for {node}")
    current = {}
    with summary.open(newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("mode") or "").strip().lower() not in {"det", "safedb"}:
                continue
            key = tuple((row.get(k) or "").strip() for k in
                        ("workload", "threads", "rate", "signing", "enforce_signatures"))
            splits = (row.get("dynamic_profile_splits") or "").strip()
            merges = (row.get("dynamic_profile_merges") or "").strip()
            logical = (row.get("dynamic_logical_fanout") or "").strip()
            physical = (row.get("dynamic_physical_node_fanout") or "").strip()
            layout = (row.get("dynamic_layout_version") or "").strip()
            max_depth = (row.get("dynamic_max_depth") or "").strip()
            if not splits.isdigit() or not merges.isdigit():
                raise SystemExit(f"missing DET profile counters for {node}: {key}")
            if layout != "6" or logical != "32" or physical != "2" or not max_depth.isdigit():
                raise SystemExit(
                    f"layout contract failed for {node}: {key} "
                    f"layout={layout or 'missing'} logical={logical or 'missing'} "
                    f"physical={physical or 'missing'} max_depth={max_depth or 'missing'}"
                )
            current[key] = (
                int(splits), int(merges), int(layout), int(logical),
                int(physical), int(max_depth)
            )
    if not current:
        raise SystemExit(f"no DET profile rows for {node}")
    maps[node] = current

baseline_node = nodes[0]
baseline = maps[baseline_node]
for node, current in maps.items():
    if current != baseline:
        raise SystemExit(f"profile mismatch: {baseline_node}={baseline} {node}={current}")
for key, value in sorted(baseline.items()):
    print(
        f"  profile key={key} splits={value[0]} merges={value[1]} "
        f"layout_version={value[2]} logical_fanout={value[3]} "
        f"physical_node_fanout={value[4]} max_depth={value[5]}"
    )
print(f"DYNAMIC_NATIVE_PROFILE_PASS=1 nodes={len(maps)}")
print("DYNAMIC_LAYOUT_CONTRACT_PASS=1 layout_version=6")
print("DYNAMIC_FANOUT_CONTRACT_PASS=1 logical_fanout=32 physical_node_fanout=2")
PYEOF
  then
    _abort_run "Native DET split/merge profile missing or mismatched across nodes"
  fi
  {
    printf 'DYNAMIC_LAYOUT_VERSION=6\n'
    printf 'DYNAMIC_LOGICAL_FANOUT=32\n'
    printf 'DYNAMIC_PHYSICAL_NODE_FANOUT=2\n'
    printf 'DYNAMIC_LAYOUT_CONTRACT_PASS=1\n'
    printf 'DYNAMIC_FANOUT_CONTRACT_PASS=1\n'
  } >"$LOCAL_RESULT_ROOT/dynamic_fanout_contract.env"
fi

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
# PHASE 5: COMBINED GRAPHS
# ============================================================
lmsg "=== Phase 5: Generating combined graphs ==="
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
    print("  [GRAPH] matplotlib not found, skipping combined graphs.")
    sys.exit(0)

result_root = Path(sys.argv[1])
node_dirs = [d for d in result_root.iterdir() if d.is_dir() and d.name != "_run_logs"]

def as_int(v):
    try: return int((v or "").strip())
    except: return None

def as_float(v):
    try: return float((v or "").strip())
    except: return None

groups = {}
for ndir in node_dirs:
    node_name = ndir.name
    summary_csv = ndir / "summary.csv"
    if not summary_csv.exists():
        continue
    with summary_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wl = (row.get("workload") or "").strip()
            if not wl: continue
            rate = as_int(row.get("rate", "")) or 0
            groups.setdefault((wl, rate), []).append((node_name, row))

if not groups:
    sys.exit(0)

mode_norm = {"postgres": "pg", "pg": "pg", "safedb": "det", "det": "det", "aria": "nondet", "nondet": "nondet"}

for (workload, rate), items in groups.items():
    series = {}
    for node_name, r in items:
        raw_mode = (r.get("mode") or "").strip().lower()
        mode = mode_norm.get(raw_mode, raw_mode)
        
        if mode != "det":
            continue
        
        label = f"{node_name}_det"
        
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
    
    # Sort labels for consistent coloring
    for label in sorted(series.keys()):
        points = sorted(series[label], key=lambda x: x[0])
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        
        # simple heuristic for line style
        ls = "-"
        if "_det" in label: ls = "--"
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
