#!/usr/bin/env bash
set -euo pipefail

# Fast distributed preflight checks (no benchmark workload execution).
# Verifies SSH, required paths/binaries, and network reachability.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PG_HOSTS=""
PG_CLIENT_HOSTS=""
RAFT_HOSTS=""
RAFT_HOST=""
GATEWAY_HOST=""
SSH_USER="${USER:-}"
PG_USERS=""
RAFT_USERS=""
RAFT_USER=""
GATEWAY_USER=""
SSH_KEY=""
SSH_PORT=22
REMOTE_REPO_ROOT="/work/ARIABC/AriaBC"
REMOTE_INSTALL_DIR="/work/ARIABC/install"
DB_PORT_BASE=5438
NODES=3

usage() {
  cat <<'EOF_HELP'
Usage:
  preflight_cluster_checks.sh \
    --pg-hosts <h1,h2,h3> \
    [--pg-client-hosts <h1,h2,h3>] \
    [--raft-hosts <r1,r2,r3> | --raft-host <r>] \
    [--gateway-host <g>] \
    [--ssh-user <default_user>] \
    [--pg-users <u1,u2,u3>] [--raft-users <u1,u2,u3> | --raft-user <u>] [--gateway-user <u>] \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </work/ARIABC/AriaBC>] \
    [--remote-install-dir </work/ARIABC/install>] \
    [--db-port-base <5438>] [--nodes <3>]

Checks:
1) SSH reachability to all unique hosts.
2) Required repo files on each host.
3) Required binaries on each host role.
4) installDir postgres tools on each host.
5) From gateway host, TCP reachability to each PG client host:dbPort.
EOF_HELP
}

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

index_of_host() {
  local host="$1"
  local -n hosts_ref="$2"
  local i
  for i in "${!hosts_ref[@]}"; do
    if [[ "${hosts_ref[$i]}" == "$host" ]]; then
      printf '%s' "$i"
      return 0
    fi
  done
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-hosts) PG_HOSTS="${2:-}"; shift 2 ;;
    --pg-client-hosts) PG_CLIENT_HOSTS="${2:-}"; shift 2 ;;
    --raft-hosts) RAFT_HOSTS="${2:-}"; shift 2 ;;
    --raft-host) RAFT_HOST="${2:-}"; shift 2 ;;
    --gateway-host) GATEWAY_HOST="${2:-}"; shift 2 ;;
    --ssh-user) SSH_USER="${2:-}"; shift 2 ;;
    --pg-users) PG_USERS="${2:-}"; shift 2 ;;
    --raft-users) RAFT_USERS="${2:-}"; shift 2 ;;
    --raft-user) RAFT_USER="${2:-}"; shift 2 ;;
    --gateway-user) GATEWAY_USER="${2:-}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:-22}"; shift 2 ;;
    --remote-repo-root) REMOTE_REPO_ROOT="${2:-}"; shift 2 ;;
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --db-port-base) DB_PORT_BASE="${2:-5438}"; shift 2 ;;
    --nodes) NODES="${2:-3}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$PG_HOSTS" ]]; then
  read -r -p "Enter PG hosts (comma-separated, exactly 3 entries): " PG_HOSTS
fi
if [[ -z "$PG_CLIENT_HOSTS" ]]; then
  PG_CLIENT_HOSTS="$PG_HOSTS"
fi
if [[ -z "$RAFT_HOSTS" ]]; then
  if [[ -n "$RAFT_HOST" ]]; then
    RAFT_HOSTS="$RAFT_HOST,$RAFT_HOST,$RAFT_HOST"
  else
    RAFT_HOSTS="$PG_HOSTS"
  fi
fi
if [[ "$NODES" != "3" ]]; then
  echo "ERROR: this preflight currently expects --nodes=3." >&2
  exit 2
fi

declare -a PG_HOST_ARR=()
split_csv "$PG_HOSTS" PG_HOST_ARR
if [[ "${#PG_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --pg-hosts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a PG_CLIENT_HOST_ARR=()
split_csv "$PG_CLIENT_HOSTS" PG_CLIENT_HOST_ARR
if [[ "${#PG_CLIENT_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --pg-client-hosts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a RAFT_HOST_ARR=()
split_csv "$RAFT_HOSTS" RAFT_HOST_ARR
if [[ "${#RAFT_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --raft-hosts must contain exactly 3 entries." >&2
  exit 2
fi

if [[ -z "$GATEWAY_HOST" ]]; then
  GATEWAY_HOST="${RAFT_HOST_ARR[0]}"
fi

declare -a PG_USER_ARR=()
if [[ -n "$PG_USERS" ]]; then
  split_csv "$PG_USERS" PG_USER_ARR
  if [[ "${#PG_USER_ARR[@]}" -ne 3 ]]; then
    echo "ERROR: --pg-users must contain exactly 3 entries." >&2
    exit 2
  fi
else
  if [[ -z "$SSH_USER" ]]; then
    echo "ERROR: provide --ssh-user default or explicit --pg-users/--raft-users/--gateway-user." >&2
    exit 2
  fi
  PG_USER_ARR=("$SSH_USER" "$SSH_USER" "$SSH_USER")
fi

declare -a RAFT_USER_ARR=()
if [[ -n "$RAFT_USERS" ]]; then
  split_csv "$RAFT_USERS" RAFT_USER_ARR
  if [[ "${#RAFT_USER_ARR[@]}" -ne 3 ]]; then
    echo "ERROR: --raft-users must contain exactly 3 entries." >&2
    exit 2
  fi
else
  if [[ -z "$RAFT_USER" ]]; then
    RAFT_USER="$SSH_USER"
  fi
  for host in "${RAFT_HOST_ARR[@]}"; do
    if idx="$(index_of_host "$host" PG_HOST_ARR 2>/dev/null)"; then
      RAFT_USER_ARR+=("${PG_USER_ARR[$idx]}")
    elif [[ -n "$RAFT_USER" ]]; then
      RAFT_USER_ARR+=("$RAFT_USER")
    elif [[ -n "$SSH_USER" ]]; then
      RAFT_USER_ARR+=("$SSH_USER")
    else
      echo "ERROR: could not derive raft user for host $host" >&2
      exit 2
    fi
  done
fi

if [[ -z "$GATEWAY_USER" ]]; then
  if idx="$(index_of_host "$GATEWAY_HOST" PG_HOST_ARR 2>/dev/null)"; then
    GATEWAY_USER="${PG_USER_ARR[$idx]}"
  elif idx="$(index_of_host "$GATEWAY_HOST" RAFT_HOST_ARR 2>/dev/null)"; then
    GATEWAY_USER="${RAFT_USER_ARR[$idx]}"
  elif [[ -n "$RAFT_USER" ]]; then
    GATEWAY_USER="$RAFT_USER"
  elif [[ -n "$SSH_USER" ]]; then
    GATEWAY_USER="$SSH_USER"
  else
    echo "ERROR: missing user mapping for gateway host $GATEWAY_HOST" >&2
    exit 2
  fi
fi

declare -A HOST_USER=()
set_host_user() {
  local host="$1"
  local user="$2"
  local role="$3"
  if [[ -n "${HOST_USER[$host]:-}" && "${HOST_USER[$host]}" != "$user" ]]; then
    echo "ERROR: conflicting users for host $host (${HOST_USER[$host]} vs $user) role=$role" >&2
    exit 2
  fi
  HOST_USER[$host]="$user"
}

for i in 0 1 2; do
  set_host_user "${PG_HOST_ARR[$i]}" "${PG_USER_ARR[$i]}" "pg$((i+1))"
  set_host_user "${RAFT_HOST_ARR[$i]}" "${RAFT_USER_ARR[$i]}" "raft$((i+1))"
done
set_host_user "$GATEWAY_HOST" "$GATEWAY_USER" "gateway"

declare -a ALL_HOSTS=("${PG_HOST_ARR[@]}" "${RAFT_HOST_ARR[@]}" "$GATEWAY_HOST")
declare -A seen=()
declare -a UNIQUE_HOSTS=()
for h in "${ALL_HOSTS[@]}"; do
  if [[ -z "${seen[$h]:-}" ]]; then
    seen[$h]=1
    UNIQUE_HOSTS+=("$h")
  fi
done

RAFT_HOSTS_CSV="$(join_csv RAFT_HOST_ARR)"
RAFT_USERS_CSV="$(join_csv RAFT_USER_ARR)"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
fi

echo "== Preflight: Topology =="
echo "PG hosts       : ${PG_HOST_ARR[*]}"
echo "PG client hosts: ${PG_CLIENT_HOST_ARR[*]}"
echo "Raft hosts     : $RAFT_HOSTS_CSV"
echo "Gateway host   : $GATEWAY_HOST"
echo "PG users       : ${PG_USER_ARR[*]}"
echo "Raft users     : $RAFT_USERS_CSV"
echo "Gateway user   : $GATEWAY_USER"
echo "SSH port       : $SSH_PORT"
echo "Repo root      : $REMOTE_REPO_ROOT"
echo "Install dir    : $REMOTE_INSTALL_DIR"
echo

for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  echo "[1/5] SSH check: $host"
  "${ssh_base[@]}" "$host_user@$host" "echo ssh_ok"
done

for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  echo "[2/5] Repo path + core files check: $host"
  remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
cd "$REMOTE_REPO_ROOT"
test -f scripts/bench_nuraft_kafka_matrix.py
test -f scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt
test -f scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt
echo repo_ok
EOF_REMOTE
)
  "${ssh_base[@]}" "$host_user@$host" "bash -lc $(printf '%q' "$remote_cmd")"
done

for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  echo "[3/5] Binary + runtime check: $host"
  need_server_bin=0
  need_gateway_bin=0

  if idx="$(index_of_host "$host" RAFT_HOST_ARR 2>/dev/null)"; then
    need_server_bin=1
  fi
  if [[ "$host" == "$GATEWAY_HOST" ]]; then
    need_gateway_bin=1
  fi

  remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
cd "$REMOTE_REPO_ROOT"
if [[ "$need_server_bin" == "1" ]]; then
  test -x ariabc_pg/build/bin/ariabc_pg_server
fi
if [[ "$need_gateway_bin" == "1" ]]; then
  test -x ariabc_pg/build/bin/ariabc_pg_gateway
fi
test -x "$REMOTE_INSTALL_DIR/bin/initdb"
test -x "$REMOTE_INSTALL_DIR/bin/pg_ctl"
if [[ -d .venv ]]; then
  . .venv/bin/activate
fi
python3 -c 'import sys; print("python_ok", sys.version.split()[0])'
echo binaries_ok
EOF_REMOTE
)
  "${ssh_base[@]}" "$host_user@$host" "bash -lc $(printf '%q' "$remote_cmd")"
done

echo "[4/5] Gateway -> PG TCP connectivity check"
for i in 0 1 2; do
  h="${PG_CLIENT_HOST_ARR[$i]}"
  p=$((DB_PORT_BASE + i))
  remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
python3 - <<'PY'
import socket, sys

host = "$h"
port = $p
timeout_s = 3

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(timeout_s)
try:
    rc = s.connect_ex((host, port))
finally:
    s.close()

# 0: connected (listener is up)
# 111/61: connection refused (host reachable, listener not up yet)
if rc == 0:
    print(f"tcp_ok_{host}:{port}")
    sys.exit(0)
if rc in (111, 61):
    print(f"tcp_reachable_no_listener_{host}:{port}")
    sys.exit(0)

print(f"tcp_unreachable_{host}:{port}_rc={rc}", file=sys.stderr)
sys.exit(1)
PY
EOF_REMOTE
)
  "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$remote_cmd")"
done

echo "[5/5] Optional local disk free on all hosts"
for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  "${ssh_base[@]}" "$host_user@$host" "df -h '$REMOTE_REPO_ROOT' | tail -n 1"
done

echo
echo "Preflight checks PASSED."
