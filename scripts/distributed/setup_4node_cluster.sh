#!/usr/bin/env bash
set -euo pipefail

# Distributed AriaBC setup helper.
# Preferred topology target:
# - 3 PostgreSQL nodes on 3 separate machines
# - Raft server co-located on each PG node
# - 1 dedicated gateway machine (optional; may overlap with a PG node)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PG_HOSTS=""
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
LOCAL_REPO_ROOT="$REPO_ROOT"
REMOTE_INSTALL_DIR="/work/ARIABC/install"
LOCAL_INSTALL_DIR="/work/ARIABC/install"
INSTALL_PACKAGES=1
SYNC_CODE=1
SYNC_INSTALL=1
BUILD_APP=1
PREPARE_VENV=1
SYNC_BUILT_APP=0

usage() {
  cat <<'EOF_HELP'
Usage:
  setup_4node_cluster.sh \
    --pg-hosts <h1,h2,h3> \
    [--raft-hosts <r1,r2,r3> | --raft-host <r>] \
    [--gateway-host <g>] \
    [--ssh-user <default_user>] \
    [--pg-users <u1,u2,u3>] [--raft-users <u1,u2,u3> | --raft-user <u>] [--gateway-user <u>] \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </work/ARIABC/AriaBC>] \
    [--local-repo-root </work/ARIABC/AriaBC>] \
    [--remote-install-dir </work/ARIABC/install>] \
    [--local-install-dir </work/ARIABC/install>] \
    [--install-packages 0|1] [--sync-code 0|1] [--sync-install 0|1] \
    [--build-app 0|1] [--prepare-venv 0|1] [--sync-built-app 0|1]

Notes:
- Preferred topology is co-located: --raft-hosts == --pg-hosts.
- Legacy topology is still supported via --raft-host <single-host>.
- If --raft-hosts/--raft-host are omitted, raft hosts default to --pg-hosts.
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
    --local-repo-root) LOCAL_REPO_ROOT="${2:-}"; shift 2 ;;
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --local-install-dir) LOCAL_INSTALL_DIR="${2:-}"; shift 2 ;;
    --install-packages) INSTALL_PACKAGES="${2:-1}"; shift 2 ;;
    --sync-code) SYNC_CODE="${2:-1}"; shift 2 ;;
    --sync-install) SYNC_INSTALL="${2:-1}"; shift 2 ;;
    --build-app) BUILD_APP="${2:-1}"; shift 2 ;;
    --prepare-venv) PREPARE_VENV="${2:-1}"; shift 2 ;;
    --sync-built-app) SYNC_BUILT_APP="${2:-0}"; shift 2 ;;
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
if [[ -z "$RAFT_HOSTS" ]]; then
  if [[ -n "$RAFT_HOST" ]]; then
    RAFT_HOSTS="$RAFT_HOST,$RAFT_HOST,$RAFT_HOST"
  else
    RAFT_HOSTS="$PG_HOSTS"
  fi
fi

if [[ ! -d "$LOCAL_REPO_ROOT" ]]; then
  echo "ERROR: local repo root not found: $LOCAL_REPO_ROOT" >&2
  exit 2
fi

if [[ "$SYNC_INSTALL" == "1" && ! -d "$LOCAL_INSTALL_DIR" ]]; then
  echo "WARNING: local install dir not found ($LOCAL_INSTALL_DIR); forcing --sync-install=0" >&2
  SYNC_INSTALL=0
fi

if [[ "$SYNC_BUILT_APP" == "1" && ! -d "$LOCAL_REPO_ROOT/ariabc_pg/build" ]]; then
  echo "ERROR: --sync-built-app=1 but local build dir not found: $LOCAL_REPO_ROOT/ariabc_pg/build" >&2
  exit 2
fi

declare -a PG_HOST_ARR=()
split_csv "$PG_HOSTS" PG_HOST_ARR
if [[ "${#PG_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --pg-hosts must contain exactly 3 entries." >&2
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
    echo "ERROR: provide --gateway-user or --ssh-user default." >&2
    exit 2
  fi
fi

declare -A HOST_USER=()
set_host_user() {
  local host="$1"
  local user="$2"
  local role="$3"
  if [[ -z "$host" || -z "$user" ]]; then
    echo "ERROR: empty host/user for role $role" >&2
    exit 2
  fi
  if [[ -n "${HOST_USER[$host]:-}" && "${HOST_USER[$host]}" != "$user" ]]; then
    echo "ERROR: conflicting users for host $host (${HOST_USER[$host]} vs $user)." >&2
    echo "       Use the same user for all roles on the same machine." >&2
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
rsync_ssh="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p $SSH_PORT"
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
fi

echo "== Cluster Setup Summary =="
echo "PG hosts      : ${PG_HOST_ARR[*]}"
echo "Raft hosts    : $RAFT_HOSTS_CSV"
echo "Gateway host  : $GATEWAY_HOST"
echo "Unique hosts  : ${UNIQUE_HOSTS[*]}"
echo "SSH users     : PG=${PG_USER_ARR[*]} RAFT=$RAFT_USERS_CSV GATEWAY=$GATEWAY_USER"
echo "SSH port      : $SSH_PORT"
echo "Repo sync     : $SYNC_CODE"
echo "Install sync  : $SYNC_INSTALL"
echo "Build app     : $BUILD_APP"
echo "Sync built app: $SYNC_BUILT_APP"
echo

for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  echo "[1/4] Checking SSH on $host"
  "${ssh_base[@]}" "$host_user@$host" "echo ok >/dev/null"
done

if [[ "$SYNC_CODE" == "1" ]]; then
  for host in "${UNIQUE_HOSTS[@]}"; do
    host_user="${HOST_USER[$host]}"
    echo "[2/4] Syncing codebase to $host:$REMOTE_REPO_ROOT"
    "${ssh_base[@]}" "$host_user@$host" "mkdir -p '$REMOTE_REPO_ROOT'"
    if [[ "$SYNC_BUILT_APP" == "1" ]]; then
      rsync -a --delete \
        --exclude='.git/' \
        --exclude='.venv/' \
        --exclude='.bench_tmp/' \
        --exclude='scripts/bench_full_results/' \
        --exclude='scripts/bench_results/' \
        --exclude='scripts/big_workload/' \
        --exclude='scripts/benchmark/' \
        --exclude='scripts/big_usertable.sql' \
        -e "$rsync_ssh" \
        "$LOCAL_REPO_ROOT/" "$host_user@$host:$REMOTE_REPO_ROOT/"
    else
      rsync -a --delete \
        --exclude='.git/' \
        --exclude='.venv/' \
        --exclude='.bench_tmp/' \
        --exclude='NuRaft/build/' \
        --exclude='ariabc_pg/build/' \
        --exclude='scripts/bench_full_results/' \
        --exclude='scripts/bench_results/' \
        --exclude='scripts/big_workload/' \
        --exclude='scripts/benchmark/' \
        --exclude='scripts/big_usertable.sql' \
        -e "$rsync_ssh" \
        "$LOCAL_REPO_ROOT/" "$host_user@$host:$REMOTE_REPO_ROOT/"
    fi
  done
fi

if [[ "$SYNC_BUILT_APP" == "1" ]]; then
  for host in "${UNIQUE_HOSTS[@]}"; do
    host_user="${HOST_USER[$host]}"
    echo "[2b/4] Syncing local prebuilt ariabc_pg/build to $host:$REMOTE_REPO_ROOT/ariabc_pg/build"
    "${ssh_base[@]}" "$host_user@$host" "mkdir -p '$REMOTE_REPO_ROOT/ariabc_pg/build'"
    rsync -a --delete -e "$rsync_ssh" \
      "$LOCAL_REPO_ROOT/ariabc_pg/build/" \
      "$host_user@$host:$REMOTE_REPO_ROOT/ariabc_pg/build/"
  done
fi

if [[ "$SYNC_INSTALL" == "1" ]]; then
  for host in "${UNIQUE_HOSTS[@]}"; do
    host_user="${HOST_USER[$host]}"
    echo "[3/4] Syncing install payload to $host:$REMOTE_INSTALL_DIR"
    "${ssh_base[@]}" "$host_user@$host" "mkdir -p '$REMOTE_INSTALL_DIR'"
    rsync -a --delete -e "$rsync_ssh" "$LOCAL_INSTALL_DIR/" "$host_user@$host:$REMOTE_INSTALL_DIR/"
  done
fi

for host in "${UNIQUE_HOSTS[@]}"; do
  host_user="${HOST_USER[$host]}"
  echo "[4/4] Preparing runtime/build env on $host"
  remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if [[ "$INSTALL_PACKAGES" == "1" ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -y
    sudo apt-get install -y \
      build-essential cmake ninja-build pkg-config \
      libssl-dev zlib1g-dev libreadline-dev bison flex \
      liblz4-dev libzstd-dev libxml2-dev libxslt1-dev libicu-dev libkrb5-dev \
      libpq-dev \
      postgresql-client \
      python3 python3-venv python3-pip \
      openjdk-17-jre-headless \
      rsync gnuplot sysstat
  else
    echo "WARNING: apt-get not found on host $host. Install packages manually." >&2
  fi
fi
cd "$REMOTE_REPO_ROOT"
if [[ "$PREPARE_VENV" == "1" ]]; then
  python3 -m venv .venv || true
  . .venv/bin/activate
  python -m pip install --upgrade pip wheel
  python -m pip install matplotlib pandas numpy
fi
if [[ "$BUILD_APP" == "1" ]]; then
  # Build librdkafka v2.3.0 from source first so all nodes use the same version
  # regardless of what the OS package manager provides.
  ENSURE_SCRIPT="$REMOTE_REPO_ROOT/scripts/distributed/ensure_rdkafka.sh"
  if [[ -x "\$ENSURE_SCRIPT" ]]; then
    "\$ENSURE_SCRIPT"
  fi
  cmake -S ariabc_pg -B ariabc_pg/build
  cmake --build ariabc_pg/build -j "\$(nproc)"
fi
test -x ariabc_pg/build/bin/ariabc_pg_server
test -x ariabc_pg/build/bin/ariabc_pg_gateway
echo "host_ready=$host"
EOF_REMOTE
)
  "${ssh_base[@]}" "$host_user@$host" "bash -lc $(printf '%q' "$remote_cmd")"
done

echo
echo "Cluster setup finished successfully."
echo "Next: run scripts/distributed/run_distributed_benchmark.sh with the same host lists."
