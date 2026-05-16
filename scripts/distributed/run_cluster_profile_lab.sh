#!/usr/bin/env bash
set -euo pipefail

# Fixed cluster profile for current 4-node lab setup.
# Default role split:
# - PG1+RAFT1: 10.129.148.248 (neel)               - utkarsh-MS-7C96 (Intel SSD on /)
# - PG2+RAFT2: 10.129.27.54 (neel)  - user4-MS-7C96
# - PG3+RAFT3: 10.129.148.236 (neel)               - admin123 (Ubuntu 24.04, co-located with GW)
# - GW only  : 10.129.148.236 (neel)               - admin123
# NOTE: 10.129.27.54 (user4-MS-7C96) is Ubuntu 22.04 (glibc 2.35) and cannot run prebuilt
#       ariabc_pg binaries (require glibc 2.38). Use admin123 (Ubuntu 24.04) as PG3 instead.
#
# NOTE: 10.129.148.248 (utkarsh-MS-7C96) is RETIRED; use 10.129.27.54 (user4-MS-7C96) for PG3.
# NOTE: user4 (10.129.27.54) is Ubuntu 22.04; runner rebuilds postgres on-host automatically.
#       explicitly (for example under /home/bibrank/project/data/ariabc_bench).
#
# Rationale:
# - Co-locate ariabc_pg_server + PostgreSQL to avoid SQL network hop in serial gate path.
# - Keep gateway on dedicated host for cleaner client/coordination isolation.
# - AUTO_TUNNEL_BLOCKED_PG=1 probes connectivity at runtime and tunnels
#   any PG host not directly reachable from the gateway.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PG_HOSTS="${PROFILE_PG_HOSTS:-10.129.148.248,10.129.27.54,10.129.148.236}"
PG_USERS="${PROFILE_PG_USERS:-neel,neel,neel}"

# New preferred vars:
RAFT_HOSTS="${PROFILE_RAFT_HOSTS:-$PG_HOSTS}"
RAFT_MEMBER_HOSTS="${PROFILE_RAFT_MEMBER_HOSTS:-$RAFT_HOSTS}"
RAFT_USERS="${PROFILE_RAFT_USERS:-$PG_USERS}"

# Legacy compatibility: PROFILE_RAFT_HOST / PROFILE_RAFT_USER
if [[ -z "${PROFILE_RAFT_HOSTS:-}" && -n "${PROFILE_RAFT_HOST:-}" ]]; then
  RAFT_HOSTS="${PROFILE_RAFT_HOST},${PROFILE_RAFT_HOST},${PROFILE_RAFT_HOST}"
  if [[ -z "${PROFILE_RAFT_MEMBER_HOSTS:-}" ]]; then
    RAFT_MEMBER_HOSTS="$RAFT_HOSTS"
  fi
fi
if [[ -z "${PROFILE_RAFT_USERS:-}" && -n "${PROFILE_RAFT_USER:-}" ]]; then
  RAFT_USERS="${PROFILE_RAFT_USER},${PROFILE_RAFT_USER},${PROFILE_RAFT_USER}"
fi

GATEWAY_HOST="${PROFILE_GATEWAY_HOST:-10.129.148.236}"
GATEWAY_USER="${PROFILE_GATEWAY_USER:-neel}"

SSH_USER="neel"    # default fallback
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_PORT="${SSH_PORT:-22}"

# Keep benchmark workspace isolated and writable for different users.
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
LOCAL_INSTALL_DIR="/work/ARIABC/install"

# Setup strategy toggles (env-overridable):
# - default keeps fast path (sync local prebuilt app)
# - set PROFILE_SETUP_BUILD_APP=1 PROFILE_SETUP_SYNC_BUILT_APP=0 to build on remote hosts
SETUP_BUILD_APP="${PROFILE_SETUP_BUILD_APP:-0}"
SETUP_SYNC_BUILT_APP="${PROFILE_SETUP_SYNC_BUILT_APP:-1}"
SETUP_INSTALL_PACKAGES="${PROFILE_SETUP_INSTALL_PACKAGES:-0}"

# Deterministic topology tuning.
BCDB_WORKER_COUNTS="${PROFILE_BCDB_WORKER_COUNTS:-8,8,8}"
SHARED_BUFFERS="${PROFILE_SHARED_BUFFERS:-512MB,2GB,512MB}"
MAX_CONNECTIONS="${PROFILE_MAX_CONNECTIONS:-300}"
BCDB_SERIAL_GATE_MODE="${PROFILE_BCDB_SERIAL_GATE_MODE:-1}"
BCDB_RESULT_RING_SLOTS="${PROFILE_BCDB_RESULT_RING_SLOTS:-256}"
DB_CONN_POOL_CAP="${PROFILE_DB_CONN_POOL_CAP:-8}"
DB_CONN_POOL_SIZE="${PROFILE_DB_CONN_POOL_SIZE:-8}"
DET_WINDOW="${PROFILE_DET_WINDOW:-16}"

RUN_FULL=0
RUN_SETUP=1
DB_PORT_BASE="${PROFILE_DB_PORT_BASE:-5438}"
CLIENT_PORT_BASE="${PROFILE_CLIENT_PORT_BASE:-26000}"
AUTO_TUNNEL_BLOCKED_PG="${PROFILE_AUTO_TUNNEL_BLOCKED_PG:-1}"
FORCE_PG_TUNNELS="${PROFILE_FORCE_PG_TUNNELS:-0}"
FORCE_RAFT_TUNNELS="${PROFILE_FORCE_RAFT_TUNNELS:-0}"
GATEWAY_SSH_KEY="${PROFILE_GATEWAY_SSH_KEY:-~/.ssh/id_rsa}"
if [[ "$GATEWAY_SSH_KEY" == "~/"* ]]; then
  GATEWAY_SSH_KEY="/home/$GATEWAY_USER/${GATEWAY_SSH_KEY#"~/"}"
fi

trim() {
  local s="$1"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf '%s' "$s"
}

split_csv() {
  local csv="$1"
  local -n out_ref="$2"
  out_ref=()
  IFS=',' read -r -a raw <<< "$csv"
  for x in "${raw[@]}"; do
    x="$(trim "$x")"
    [[ -n "$x" ]] && out_ref+=("$x")
  done
}

join_csv() {
  local -n arr_ref="$1"
  local IFS=,
  printf '%s' "${arr_ref[*]}"
}

usage() {
  cat <<'EOF_HELP'
Usage:
  run_cluster_profile_lab.sh [--full] [--no-setup]

Options:
  --full       Run full benchmark after preflight+smoke
  --no-setup   Skip setup sync/build step

Environment overrides:
  SSH_KEY, SSH_PORT
  PROFILE_PG_HOSTS, PROFILE_PG_USERS
  PROFILE_RAFT_HOSTS, PROFILE_RAFT_MEMBER_HOSTS, PROFILE_RAFT_USERS
  PROFILE_GATEWAY_HOST, PROFILE_GATEWAY_USER
  PROFILE_DB_PORT_BASE, PROFILE_CLIENT_PORT_BASE
  PROFILE_AUTO_TUNNEL_BLOCKED_PG
  PROFILE_GATEWAY_SSH_KEY
  PROFILE_SETUP_BUILD_APP, PROFILE_SETUP_SYNC_BUILT_APP, PROFILE_SETUP_INSTALL_PACKAGES
  PROFILE_BCDB_WORKER_COUNTS, PROFILE_SHARED_BUFFERS, PROFILE_MAX_CONNECTIONS
  PROFILE_BCDB_SERIAL_GATE_MODE, PROFILE_BCDB_RESULT_RING_SLOTS
  PROFILE_DB_CONN_POOL_CAP, PROFILE_DB_CONN_POOL_SIZE, PROFILE_DET_WINDOW
EOF_HELP
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --full) RUN_FULL=1; shift ;;
    --no-setup) RUN_SETUP=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

echo "== Profile =="
echo "PG hosts/users     : $PG_HOSTS / $PG_USERS"
echo "Raft hosts/users   : $RAFT_HOSTS / $RAFT_USERS"
echo "Raft member hosts  : $RAFT_MEMBER_HOSTS"
echo "Gateway host/user  : $GATEWAY_HOST / $GATEWAY_USER"
echo "Remote repo/install: $REMOTE_REPO_ROOT / $REMOTE_INSTALL_DIR"
echo "SSH key            : $SSH_KEY"
echo "Tuning             : workers=$BCDB_WORKER_COUNTS shared_buffers=$SHARED_BUFFERS dbpool=${DB_CONN_POOL_SIZE}/${DB_CONN_POOL_CAP} detWindow=$DET_WINDOW"
echo

if [[ "$RUN_SETUP" == "1" ]]; then
  "$SCRIPT_DIR/setup_4node_cluster.sh" \
    --pg-hosts "$PG_HOSTS" \
    --pg-users "$PG_USERS" \
    --raft-hosts "$RAFT_HOSTS" \
    --raft-users "$RAFT_USERS" \
    --gateway-host "$GATEWAY_HOST" \
    --gateway-user "$GATEWAY_USER" \
    --ssh-user "$SSH_USER" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --remote-repo-root "$REMOTE_REPO_ROOT" \
    --remote-install-dir "$REMOTE_INSTALL_DIR" \
    --local-install-dir "$LOCAL_INSTALL_DIR" \
    --install-packages "$SETUP_INSTALL_PACKAGES" \
    --sync-code 1 \
    --sync-install 1 \
    --build-app "$SETUP_BUILD_APP" \
    --prepare-venv 0 \
    --sync-built-app "$SETUP_SYNC_BUILT_APP"
fi

declare -a PG_HOST_ARR=()
declare -a PG_USER_ARR=()
declare -a PG_CLIENT_HOST_ARR=()
declare -a RAFT_HOST_ARR=()
declare -a RAFT_USER_ARR=()
declare -a RAFT_CLIENT_HOST_ARR=()
split_csv "$PG_HOSTS" PG_HOST_ARR
split_csv "$PG_USERS" PG_USER_ARR
split_csv "$RAFT_HOSTS" RAFT_HOST_ARR
split_csv "$RAFT_USERS" RAFT_USER_ARR
if [[ "${#PG_HOST_ARR[@]}" -ne 3 || "${#PG_USER_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: PROFILE_PG_HOSTS and PROFILE_PG_USERS must each contain exactly 3 entries." >&2
  exit 2
fi
if [[ "${#RAFT_HOST_ARR[@]}" -ne 3 || "${#RAFT_USER_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: PROFILE_RAFT_HOSTS and PROFILE_RAFT_USERS must each contain exactly 3 entries." >&2
  exit 2
fi
PG_CLIENT_HOST_ARR=("${PG_HOST_ARR[@]}")
RAFT_CLIENT_HOST_ARR=("${RAFT_HOST_ARR[@]}")

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
fi

if [[ "$AUTO_TUNNEL_BLOCKED_PG" == "1" ]]; then
  for i in 0 1 2; do
    host="${PG_HOST_ARR[$i]}"
    host_user="${PG_USER_ARR[$i]}"
    port=$((DB_PORT_BASE + i))

    need_tunnel=0
    if [[ "$FORCE_PG_TUNNELS" == "1" ]]; then
      need_tunnel=1
    else
      probe_cmd=$(cat <<EOF_PROBE
set -euo pipefail
python3 - <<'PY'
import socket, sys

host = "${host}"
port = ${port}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(4)
try:
    rc = s.connect_ex((host, port))
finally:
    s.close()

# 0: listener up. 111/61: host reachable but listener not up yet.
sys.exit(0 if rc in (0, 111, 61) else 1)
PY
EOF_PROBE
)
      if ! "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$probe_cmd")"; then
        need_tunnel=1
      fi
    fi

    if [[ "$need_tunnel" == "0" ]]; then
      continue
    fi

    echo "[tunnel] Gateway cannot reach ${host}:${port} directly; enabling SSH tunnel on gateway"
    tunnel_cmd=$(cat <<EOF_TUNNEL
set -euo pipefail
PORT=${port}
TARGET_HOST=${host}
TARGET_USER=${host_user}
TARGET_KEY=${GATEWAY_SSH_KEY}
pkill -f "ssh.*-L \${PORT}:127.0.0.1:\${PORT}.*\${TARGET_USER}@\${TARGET_HOST}" >/dev/null 2>&1 || true
mkdir -p "\$HOME/.ariabc_tunnel_logs"
nohup ssh -i "\${TARGET_KEY}" -o StrictHostKeyChecking=no -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -N -L "\${PORT}:127.0.0.1:\${PORT}" "\${TARGET_USER}@\${TARGET_HOST}" \
  >"\$HOME/.ariabc_tunnel_logs/ariabc_pg_tunnel_\${PORT}.log" 2>&1 &
sleep 1
python3 - <<'PY'
import socket, sys

port = int("${port}")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(4)
try:
    rc = s.connect_ex(("127.0.0.1", port))
finally:
    s.close()

# 0: listener up. 111/61: tunnel is up but listener not up yet.
if rc not in (0, 111, 61):
    print(f"tunnel_probe_failed_rc={rc}", file=sys.stderr)
    sys.exit(1)
PY
echo "tunnel_ready=127.0.0.1:\${PORT} -> \${TARGET_HOST}:\${PORT}"
EOF_TUNNEL
)
    "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$tunnel_cmd")"
    PG_CLIENT_HOST_ARR[$i]="127.0.0.1"
  done

  for i in 0 1 2; do
    host="${RAFT_HOST_ARR[$i]}"
    host_user="${RAFT_USER_ARR[$i]}"
    port=$((CLIENT_PORT_BASE + i))

    need_tunnel=0
    if [[ "$FORCE_RAFT_TUNNELS" == "1" ]]; then
      need_tunnel=1
    else
      probe_cmd=$(cat <<EOF_PROBE
set -euo pipefail
python3 - <<'PY'
import socket, sys

host = "${host}"
port = ${port}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(4)
try:
    rc = s.connect_ex((host, port))
finally:
    s.close()

# 0: listener up. 111/61: host reachable but listener not up yet.
sys.exit(0 if rc in (0, 111, 61) else 1)
PY
EOF_PROBE
)
      if ! "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$probe_cmd")"; then
        need_tunnel=1
      fi
    fi

    if [[ "$need_tunnel" == "0" ]]; then
      continue
    fi

    echo "[tunnel] Gateway cannot reach Raft client ${host}:${port} directly; enabling SSH tunnel on gateway"
    tunnel_cmd=$(cat <<EOF_TUNNEL
set -euo pipefail
PORT=${port}
TARGET_HOST=${host}
TARGET_USER=${host_user}
TARGET_KEY=${GATEWAY_SSH_KEY}
pkill -f "ssh.*-L \${PORT}:127.0.0.1:\${PORT}.*\${TARGET_USER}@\${TARGET_HOST}" >/dev/null 2>&1 || true
mkdir -p "\$HOME/.ariabc_tunnel_logs"
nohup ssh -i "\${TARGET_KEY}" -o StrictHostKeyChecking=no -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -N -L "\${PORT}:127.0.0.1:\${PORT}" "\${TARGET_USER}@\${TARGET_HOST}" \
  >"\$HOME/.ariabc_tunnel_logs/ariabc_raft_tunnel_\${PORT}.log" 2>&1 &
sleep 1
python3 - <<'PY'
import socket, sys

port = int("${port}")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(4)
try:
    rc = s.connect_ex(("127.0.0.1", port))
finally:
    s.close()

# 0: listener up. 111/61: tunnel is up but listener not up yet.
if rc not in (0, 111, 61):
    print(f"raft_tunnel_probe_failed_rc={rc}", file=sys.stderr)
    sys.exit(1)
PY
echo "raft_tunnel_ready=127.0.0.1:\${PORT} -> \${TARGET_HOST}:\${PORT}"
EOF_TUNNEL
)
    "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$tunnel_cmd")"
    RAFT_CLIENT_HOST_ARR[$i]="127.0.0.1"
  done
fi

PG_CLIENT_HOSTS="$(join_csv PG_CLIENT_HOST_ARR)"
RAFT_CLIENT_HOSTS="$(join_csv RAFT_CLIENT_HOST_ARR)"
echo "Benchmark client hosts (from gateway): $PG_CLIENT_HOSTS"
echo "Raft client hosts (from gateway): $RAFT_CLIENT_HOSTS"

common_args=(
  --pg-hosts "$PG_HOSTS"
  --pg-client-hosts "$PG_CLIENT_HOSTS"
  --pg-users "$PG_USERS"
  --raft-hosts "$RAFT_HOSTS"
  --raft-member-hosts "$RAFT_MEMBER_HOSTS"
  --raft-client-hosts "$RAFT_CLIENT_HOSTS"
  --raft-users "$RAFT_USERS"
  --gateway-host "$GATEWAY_HOST"
  --gateway-user "$GATEWAY_USER"
  --ssh-user "$SSH_USER"
  --ssh-key "$SSH_KEY"
  --ssh-port "$SSH_PORT"
  --remote-repo-root "$REMOTE_REPO_ROOT"
  --remote-install-dir "$REMOTE_INSTALL_DIR"
  --bcdb-worker-counts "$BCDB_WORKER_COUNTS"
  --shared-buffers "$SHARED_BUFFERS"
  --max-connections "$MAX_CONNECTIONS"
  --bcdb-serial-gate-mode "$BCDB_SERIAL_GATE_MODE"
  --bcdb-result-ring-slots "$BCDB_RESULT_RING_SLOTS"
  --db-conn-pool-cap "$DB_CONN_POOL_CAP"
  --db-conn-pool-size "$DB_CONN_POOL_SIZE"
  --det-window "$DET_WINDOW"
)

if [[ "$RUN_FULL" == "1" ]]; then
  "$SCRIPT_DIR/preflight_then_run_full.sh" "${common_args[@]}"
else
  "$SCRIPT_DIR/preflight_then_run_full.sh" "${common_args[@]}" --skip-full
fi
