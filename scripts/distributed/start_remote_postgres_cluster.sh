#!/usr/bin/env bash
set -euo pipefail

# Start (or initialize and start) Postgres on each distributed PG host.
# This is required because bench_nuraft_kafka_matrix.py in --distributed mode
# expects Postgres instances to already be reachable.

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
REMOTE_INSTALL_DIR="/work/ARIABC/install"
DB_PORT_BASE=5438
PG_CONFIG_MODE="${PROFILE_PG_CONFIG_MODE:-canonical}"
PG_CONFIG_TEMPLATE_LOCAL="${PROFILE_PG_CONFIG_TEMPLATE_LOCAL:-/work/ARIABC/pgdata/postgresql.conf}"

# Deterministic runtime tuning (per-node lists are PG1,PG2,PG3)
BCDB_WORKER_COUNTS="${PROFILE_BCDB_WORKER_COUNTS:-4,8,4}"
SHARED_BUFFERS="${PROFILE_SHARED_BUFFERS:-512MB,2GB,512MB}"
MAX_CONNECTIONS="${PROFILE_MAX_CONNECTIONS:-300}"
BCDB_SERIAL_GATE_MODE="${PROFILE_BCDB_SERIAL_GATE_MODE:-1}"
BCDB_RESULT_RING_SLOTS="${PROFILE_BCDB_RESULT_RING_SLOTS:-256}"

usage() {
  cat <<'EOF_HELP'
Usage:
  start_remote_postgres_cluster.sh \
    --pg-hosts <h1,h2,h3> \
    [--raft-hosts <r1,r2,r3> | --raft-host <r>] \
    [--gateway-host <g>] \
    [--ssh-user <default_user>] \
    [--pg-users <u1,u2,u3>] [--raft-users <u1,u2,u3> | --raft-user <u>] [--gateway-user <u>] \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </work/ARIABC/AriaBC>] \
    [--remote-install-dir </work/ARIABC/install>] \
    [--db-port-base <5438>] \
    [--bcdb-worker-counts <w1,w2,w3>] [--shared-buffers <b1,b2,b3>] \
    [--max-connections <n>] [--bcdb-serial-gate-mode <0|1>] [--bcdb-result-ring-slots <n>]

Notes:
- --raft-host(s) and gateway options are accepted for wrapper compatibility,
  but this script starts Postgres only on --pg-hosts.
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

extract_last_setting() {
  local file="$1"
  local key="$2"
  local default_value="$3"
  local value
  value="$(
    awk -v key="$key" '
      {
        line = $0
        sub(/\r$/, "", line)
        if (line ~ /^[[:space:]]*#/) {
          next
        }
        if (line !~ ("^[[:space:]]*" key "[[:space:]]*=")) {
          next
        }
        sub(/^[[:space:]]*[^=]+=[[:space:]]*/, "", line)
        sub(/[[:space:]]*#.*/, "", line)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
        if (line != "") {
          val = line
        }
      }
      END {
        if (val != "") {
          print val
        }
      }
    ' "$file"
  )"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$default_value"
  fi
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
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --db-port-base) DB_PORT_BASE="${2:-5438}"; shift 2 ;;
    --bcdb-worker-counts) BCDB_WORKER_COUNTS="${2:-}"; shift 2 ;;
    --shared-buffers) SHARED_BUFFERS="${2:-}"; shift 2 ;;
    --max-connections) MAX_CONNECTIONS="${2:-300}"; shift 2 ;;
    --bcdb-serial-gate-mode) BCDB_SERIAL_GATE_MODE="${2:-1}"; shift 2 ;;
    --bcdb-result-ring-slots) BCDB_RESULT_RING_SLOTS="${2:-256}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$PG_HOSTS" ]]; then
  usage
  echo "ERROR: --pg-hosts is required." >&2
  exit 2
fi

declare -a PG_HOST_ARR=()
split_csv "$PG_HOSTS" PG_HOST_ARR
if [[ "${#PG_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --pg-hosts must contain exactly 3 entries." >&2
  exit 2
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
    echo "ERROR: provide --ssh-user default or explicit --pg-users." >&2
    exit 2
  fi
  PG_USER_ARR=("$SSH_USER" "$SSH_USER" "$SSH_USER")
fi

declare -a BCDB_WORKER_ARR=()
split_csv "$BCDB_WORKER_COUNTS" BCDB_WORKER_ARR
if [[ "${#BCDB_WORKER_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --bcdb-worker-counts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a SHARED_BUFFERS_ARR=()
split_csv "$SHARED_BUFFERS" SHARED_BUFFERS_ARR
if [[ "${#SHARED_BUFFERS_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --shared-buffers must contain exactly 3 entries." >&2
  exit 2
fi

if [[ "$PG_CONFIG_MODE" != "canonical" && "$PG_CONFIG_MODE" != "tuned" ]]; then
  echo "ERROR: PROFILE_PG_CONFIG_MODE must be canonical or tuned (got '$PG_CONFIG_MODE')." >&2
  exit 2
fi

if [[ "$PG_CONFIG_MODE" == "canonical" && ! -f "$PG_CONFIG_TEMPLATE_LOCAL" ]]; then
  echo "ERROR: canonical PG config template not found: $PG_CONFIG_TEMPLATE_LOCAL" >&2
  exit 2
fi

template_max_connections="$MAX_CONNECTIONS"
template_shared_buffers="${SHARED_BUFFERS_ARR[0]}"
template_synchronous_commit="on"
template_fsync="on"
template_full_page_writes="on"
template_wal_level="replica"
template_log_min_messages="warning"
template_merkle_update_detection="on"
template_enable_merkle_index="on"
template_merkle_update_detection_suppress="on"
template_bcdb_worker_count="${BCDB_WORKER_ARR[0]}"
template_bcdb_serial_gate_mode="$BCDB_SERIAL_GATE_MODE"
template_bcdb_dt_conflict_tracking="on"
template_bcdb_result_ring_slots="$BCDB_RESULT_RING_SLOTS"

if [[ "$PG_CONFIG_MODE" == "canonical" ]]; then
  template_max_connections="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "max_connections" "$template_max_connections")"
  template_shared_buffers="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "shared_buffers" "$template_shared_buffers")"
  template_synchronous_commit="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "synchronous_commit" "$template_synchronous_commit")"
  template_fsync="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "fsync" "$template_fsync")"
  template_full_page_writes="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "full_page_writes" "$template_full_page_writes")"
  template_wal_level="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "wal_level" "$template_wal_level")"
  template_log_min_messages="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "log_min_messages" "$template_log_min_messages")"
  template_merkle_update_detection="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "merkle_update_detection" "$template_merkle_update_detection")"
  template_enable_merkle_index="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "enable_merkle_index" "$template_enable_merkle_index")"
  template_merkle_update_detection_suppress="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "merkle_update_detection_suppress" "$template_merkle_update_detection_suppress")"
  template_bcdb_worker_count="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "bcdb_worker_count" "$template_bcdb_worker_count")"
  template_bcdb_serial_gate_mode="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "bcdb_serial_gate_mode" "$template_bcdb_serial_gate_mode")"
  template_bcdb_dt_conflict_tracking="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "bcdb_dt_conflict_tracking" "$template_bcdb_dt_conflict_tracking")"
  template_bcdb_result_ring_slots="$(extract_last_setting "$PG_CONFIG_TEMPLATE_LOCAL" "bcdb_result_ring_slots" "$template_bcdb_result_ring_slots")"
fi

# Optional role args are accepted for compatibility with wrappers.
# Validate only if provided to catch obvious mismatches early.
if [[ -n "$RAFT_HOSTS" ]]; then
  declare -a _raft_tmp=()
  split_csv "$RAFT_HOSTS" _raft_tmp
  if [[ "${#_raft_tmp[@]}" -ne 3 ]]; then
    echo "ERROR: --raft-hosts must contain exactly 3 entries when provided." >&2
    exit 2
  fi
fi
if [[ -n "$RAFT_USERS" ]]; then
  declare -a _raft_user_tmp=()
  split_csv "$RAFT_USERS" _raft_user_tmp
  if [[ "${#_raft_user_tmp[@]}" -ne 3 ]]; then
    echo "ERROR: --raft-users must contain exactly 3 entries when provided." >&2
    exit 2
  fi
fi

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
fi

echo "== Starting remote Postgres cluster =="
echo "PG hosts           : ${PG_HOST_ARR[*]}"
echo "PG users           : ${PG_USER_ARR[*]}"
echo "pg_config_mode     : ${PG_CONFIG_MODE}"
if [[ "$PG_CONFIG_MODE" == "canonical" ]]; then
  echo "pg_config_template : ${PG_CONFIG_TEMPLATE_LOCAL}"
fi
echo "bcdb_worker_count  : ${BCDB_WORKER_ARR[*]}"
echo "shared_buffers     : ${SHARED_BUFFERS_ARR[*]}"
echo "max_connections    : ${MAX_CONNECTIONS}"
echo "serial_gate_mode   : ${BCDB_SERIAL_GATE_MODE}"
echo "result_ring_slots  : ${BCDB_RESULT_RING_SLOTS}"
echo

for i in 0 1 2; do
  host="${PG_HOST_ARR[$i]}"
  host_user="${PG_USER_ARR[$i]}"
  node_idx=$((i + 1))
  port=$((DB_PORT_BASE + i))
  worker_count="${BCDB_WORKER_ARR[$i]}"
  shared_buffers_node="${SHARED_BUFFERS_ARR[$i]}"
  max_connections_node="$MAX_CONNECTIONS"
  synchronous_commit_node="off"
  fsync_node="off"
  full_page_writes_node="off"
  wal_level_node="replica"
  log_min_messages_node="warning"
  merkle_update_detection_node="on"
  enable_merkle_index_node="on"
  merkle_update_detection_suppress_node="on"
  bcdb_serial_gate_mode_node="$BCDB_SERIAL_GATE_MODE"
  bcdb_dt_conflict_tracking_node="on"
  bcdb_result_ring_slots_node="$BCDB_RESULT_RING_SLOTS"
  if [[ "$PG_CONFIG_MODE" == "canonical" ]]; then
    worker_count="$template_bcdb_worker_count"
    shared_buffers_node="$template_shared_buffers"
    max_connections_node="$template_max_connections"
    synchronous_commit_node="$template_synchronous_commit"
    fsync_node="$template_fsync"
    full_page_writes_node="$template_full_page_writes"
    wal_level_node="$template_wal_level"
    log_min_messages_node="$template_log_min_messages"
    merkle_update_detection_node="$template_merkle_update_detection"
    enable_merkle_index_node="$template_enable_merkle_index"
    merkle_update_detection_suppress_node="$template_merkle_update_detection_suppress"
    bcdb_serial_gate_mode_node="$template_bcdb_serial_gate_mode"
    bcdb_dt_conflict_tracking_node="$template_bcdb_dt_conflict_tracking"
    bcdb_result_ring_slots_node="$template_bcdb_result_ring_slots"
  fi
  max_worker_processes_node="$(( worker_count + 32 ))"
  if [[ "$max_worker_processes_node" -lt 64 ]]; then
    max_worker_processes_node=64
  fi

  echo "[PG node ${node_idx}] $host:$port (user=$host_user workers=$worker_count shared_buffers=$shared_buffers_node max_connections=$max_connections_node mode=$PG_CONFIG_MODE)"

  remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
BIN_DIR=""
USE_CUSTOM_BIN=0

# Try transferred custom postgres first (needed for BCDB deterministic SQL path)
# and fall back to host-native binaries when it is not runnable.
if [[ -x "$REMOTE_INSTALL_DIR/bin/initdb" && -x "$REMOTE_INSTALL_DIR/bin/pg_ctl" && -x "$REMOTE_INSTALL_DIR/bin/pg_isready" && -x "$REMOTE_INSTALL_DIR/bin/postgres" ]]; then
  if LD_LIBRARY_PATH="$REMOTE_INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}" \
       "$REMOTE_INSTALL_DIR/bin/initdb" --version >/dev/null 2>&1 \
     && LD_LIBRARY_PATH="$REMOTE_INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}" \
       "$REMOTE_INSTALL_DIR/bin/postgres" --version >/dev/null 2>&1; then
    BIN_DIR="$REMOTE_INSTALL_DIR/bin"
    USE_CUSTOM_BIN=1
  fi
fi

if [[ -z "\$BIN_DIR" ]]; then
  for c in /usr/lib/postgresql/*/bin; do
    if [[ -x "\$c/initdb" && -x "\$c/pg_ctl" && -x "\$c/pg_isready" && -x "\$c/postgres" ]]; then
      if "\$c/initdb" --version >/dev/null 2>&1 && "\$c/postgres" --version >/dev/null 2>&1; then
        BIN_DIR="\$c"
        USE_CUSTOM_BIN=0
        break
      fi
    fi
  done
fi

if [[ -z "\$BIN_DIR" ]]; then
  echo "No usable postgres binaries found (checked /usr/lib/postgresql/*/bin and $REMOTE_INSTALL_DIR/bin)" >&2
  exit 1
fi

echo "node${node_idx}: using postgres bin_dir=\$BIN_DIR custom_bin=\$USE_CUSTOM_BIN port=$port" >&2

INITDB="\$BIN_DIR/initdb"
PG_CTL="\$BIN_DIR/pg_ctl"
PG_ISREADY="\$BIN_DIR/pg_isready"
PSQL="\$BIN_DIR/psql"
export LD_LIBRARY_PATH="\${LD_LIBRARY_PATH:-}"
if [[ "\$USE_CUSTOM_BIN" == "1" ]]; then
  export LD_LIBRARY_PATH="$REMOTE_INSTALL_DIR/lib:\$LD_LIBRARY_PATH"
fi

BASE_DIR="$REMOTE_REPO_ROOT/.bench_tmp/distributed_pg"
PGDATA="\$BASE_DIR/node${node_idx}"
SOCK_DIR="\$BASE_DIR/sockets/node${node_idx}"
LOG_DIR="$REMOTE_REPO_ROOT/scripts/bench_full_results/postgres_boot"
LOG_FILE="\$LOG_DIR/node${node_idx}.log"

mkdir -p "\$PGDATA" "\$SOCK_DIR" "\$LOG_DIR"

# If a previous run initialized this PGDATA with a different PostgreSQL major
# version (for example system PG16 vs custom PG13devel), force re-init so
# startup does not fail with version-incompatible data files.
bin_major="\$("\$INITDB" --version 2>/dev/null | awk '{print \$NF}' | sed -E 's/^([0-9]+).*/\1/' || true)"
if [[ -f "\$PGDATA/PG_VERSION" ]]; then
  data_major="\$(head -n 1 "\$PGDATA/PG_VERSION" | tr -d '\r\n' | sed -E 's/^([0-9]+).*/\1/' || true)"
  if [[ -n "\$bin_major" && -n "\$data_major" && "\$bin_major" != "\$data_major" ]]; then
    echo "node${node_idx}: PGDATA version mismatch data_major=\$data_major bin_major=\$bin_major; reinitializing" >&2
    rm -rf "\$PGDATA"/*
  fi
fi

if [[ ! -f "\$PGDATA/PG_VERSION" ]]; then
  rm -rf "\$PGDATA"/*
  if ! timeout 120 "\$INITDB" -D "\$PGDATA" -U postgres --auth=trust >/dev/null 2>"\$LOG_DIR/node${node_idx}_initdb.err"; then
    echo "ERROR: initdb failed/timed out for node${node_idx} ($host:$port)" >&2
    tail -n 40 "\$LOG_DIR/node${node_idx}_initdb.err" >&2 || true
    exit 1
  fi
fi

AUTO_CONF="\$PGDATA/bench_auto.conf"
cat > "\$AUTO_CONF" <<CFG
# generated by start_remote_postgres_cluster.sh (mode=$PG_CONFIG_MODE)
port = $port
listen_addresses = '*'
unix_socket_directories = '\$SOCK_DIR'
max_connections = $max_connections_node
max_worker_processes = $max_worker_processes_node
max_parallel_workers = 0
max_parallel_maintenance_workers = 0
shared_buffers = '$shared_buffers_node'
log_min_messages = $log_min_messages_node
synchronous_commit = $synchronous_commit_node
fsync = $fsync_node
full_page_writes = $full_page_writes_node
wal_level = $wal_level_node
CFG

if [[ "\$USE_CUSTOM_BIN" == "1" ]]; then
  cat >> "\$AUTO_CONF" <<CFG
merkle_update_detection = $merkle_update_detection_node
enable_merkle_index = $enable_merkle_index_node
merkle_update_detection_suppress = $merkle_update_detection_suppress_node
bcdb_worker_count = $worker_count
bcdb_serial_gate_mode = $bcdb_serial_gate_mode_node
bcdb_dt_conflict_tracking = $bcdb_dt_conflict_tracking_node
bcdb_result_ring_slots = $bcdb_result_ring_slots_node
CFG
fi

if ! grep -Fq "include_if_exists = 'bench_auto.conf'" "\$PGDATA/postgresql.conf"; then
  echo "include_if_exists = 'bench_auto.conf'" >> "\$PGDATA/postgresql.conf"
fi

if ! grep -Eq '^host\s+all\s+all\s+0\.0\.0\.0/0\s+trust$' "\$PGDATA/pg_hba.conf"; then
  echo "host all all 0.0.0.0/0 trust" >> "\$PGDATA/pg_hba.conf"
fi

if "\$PG_CTL" -D "\$PGDATA" status >/dev/null 2>&1; then
  # listen_addresses and socket dir changes are postmaster-level settings.
  # A reload is insufficient; enforce restart so remote reachability is correct.
  "\$PG_CTL" -D "\$PGDATA" -w -t 120 stop -m fast || true
fi

# A previous single-node benchmark run may leave a different postmaster bound
# to this benchmark port (same host, different PGDATA). Clear any stale
# listeners so the distributed node can bind all required interfaces.
stale_pids="\$( (ss -ltnp 2>/dev/null | grep -E \":$port[[:space:]]\" | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u | tr '\n' ' ') || true )"
if [[ -n "\${stale_pids// }" ]]; then
  echo "node${node_idx}: clearing stale listeners on port $port pids=\$stale_pids" >&2
  kill -TERM \$stale_pids >/dev/null 2>&1 || true
  sleep 2
  stale_pids2="\$( (ss -ltnp 2>/dev/null | grep -E \":$port[[:space:]]\" | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u | tr '\n' ' ') || true )"
  if [[ -n "\${stale_pids2// }" ]]; then
    kill -KILL \$stale_pids2 >/dev/null 2>&1 || true
    sleep 1
  fi
fi

if ! timeout 140 "\$PG_CTL" -D "\$PGDATA" -l "\$LOG_FILE" -w -t 120 start; then
  echo "ERROR: pg_ctl start failed for node${node_idx} ($host:$port)" >&2
  tail -n 80 "\$LOG_FILE" >&2 || true
  exit 1
fi

ready_ok=0
for _ in 1 2 3 4 5; do
  if timeout 8 "\$PG_ISREADY" -h 127.0.0.1 -p $port -U postgres -d postgres -t 5 >/dev/null 2>&1; then
    ready_ok=1
    break
  fi
  sleep 2
done
if [[ "\$ready_ok" != "1" ]]; then
  echo "ERROR: postgres readiness probe failed for node${node_idx} ($host:$port)" >&2
  tail -n 80 "\$LOG_FILE" >&2 || true
  exit 1
fi

CONFIG_SNAPSHOT="\$LOG_DIR/node${node_idx}.effective.conf"
{
  echo "pg_config_mode=$PG_CONFIG_MODE"
  echo "custom_bin=\$USE_CUSTOM_BIN"
  echo "bin_dir=\$BIN_DIR"
  echo "port=$port"
  echo "unix_socket_directories=\$SOCK_DIR"
  if [[ -x "\$PSQL" ]]; then
    for key in max_connections shared_buffers synchronous_commit fsync full_page_writes wal_level log_min_messages; do
      value="\$("\$PSQL" -X -q -h 127.0.0.1 -p $port -U postgres -d postgres -At -c "show \$key;" 2>/dev/null | tail -n 1 | tr -d '\r' || true)"
      echo "\$key=\$value"
    done
    if [[ "\$USE_CUSTOM_BIN" == "1" ]]; then
      for key in merkle_update_detection enable_merkle_index merkle_update_detection_suppress bcdb_worker_count bcdb_serial_gate_mode bcdb_dt_conflict_tracking bcdb_result_ring_slots; do
        value="\$("\$PSQL" -X -q -h 127.0.0.1 -p $port -U postgres -d postgres -At -c "show \$key;" 2>/dev/null | tail -n 1 | tr -d '\r' || true)"
        echo "\$key=\$value"
      done
    fi
  fi
} > "\$CONFIG_SNAPSHOT"

snapshot_get() {
  local key="\$1"
  awk -F= -v k="\$key" '\$1==k {print substr(\$0, index(\$0, "=")+1); exit}' "\$CONFIG_SNAPSHOT"
}

normalize_setting() {
  local v="\${1:-}"
  v="\$(printf '%s' "\$v" | tr -d "[:space:]'\"" | tr '[:upper:]' '[:lower:]')"
  printf '%s' "\$v"
}

assert_setting_equals() {
  local key="\$1"
  local expected="\$2"
  local actual
  actual="\$(snapshot_get "\$key")"
  if [[ -z "\$actual" ]]; then
    echo "ERROR: canonical setting missing on node${node_idx}: key=\$key snapshot=\$CONFIG_SNAPSHOT" >&2
    return 1
  fi
  if [[ "\$(normalize_setting "\$actual")" != "\$(normalize_setting "\$expected")" ]]; then
    echo "ERROR: canonical setting mismatch on node${node_idx}: key=\$key expected='\$expected' actual='\$actual'" >&2
    return 1
  fi
  return 0
}

if [[ "$PG_CONFIG_MODE" == "canonical" ]]; then
  canon_fail=0
  if [[ "\$USE_CUSTOM_BIN" != "1" ]]; then
    echo "ERROR: canonical mode requires custom benchmark postgres binaries on node${node_idx}; got custom_bin=\$USE_CUSTOM_BIN" >&2
    canon_fail=1
  fi
  assert_setting_equals "max_connections" "$max_connections_node" || canon_fail=1
  assert_setting_equals "shared_buffers" "$shared_buffers_node" || canon_fail=1
  assert_setting_equals "synchronous_commit" "$synchronous_commit_node" || canon_fail=1
  assert_setting_equals "fsync" "$fsync_node" || canon_fail=1
  assert_setting_equals "full_page_writes" "$full_page_writes_node" || canon_fail=1
  assert_setting_equals "wal_level" "$wal_level_node" || canon_fail=1
  assert_setting_equals "log_min_messages" "$log_min_messages_node" || canon_fail=1
  assert_setting_equals "merkle_update_detection" "$merkle_update_detection_node" || canon_fail=1
  assert_setting_equals "enable_merkle_index" "$enable_merkle_index_node" || canon_fail=1
  assert_setting_equals "merkle_update_detection_suppress" "$merkle_update_detection_suppress_node" || canon_fail=1
  assert_setting_equals "bcdb_worker_count" "$worker_count" || canon_fail=1
  assert_setting_equals "bcdb_serial_gate_mode" "$bcdb_serial_gate_mode_node" || canon_fail=1
  assert_setting_equals "bcdb_dt_conflict_tracking" "$bcdb_dt_conflict_tracking_node" || canon_fail=1
  assert_setting_equals "bcdb_result_ring_slots" "$bcdb_result_ring_slots_node" || canon_fail=1
  if [[ "\$canon_fail" != "0" ]]; then
    echo "ERROR: canonical PG config validation failed for node${node_idx}. Effective snapshot:" >&2
    sed -n '1,200p' "\$CONFIG_SNAPSHOT" >&2 || true
    exit 1
  fi
fi

echo "pg_ready_node${node_idx}=$host:$port bin_dir=\$BIN_DIR custom_bin=\$USE_CUSTOM_BIN workers=$worker_count shared_buffers=$shared_buffers_node"
EOF_REMOTE
)

  "${ssh_base[@]}" "$host_user@$host" "bash -lc $(printf '%q' "$remote_cmd")"
done

echo
echo "Remote Postgres cluster is ready."
