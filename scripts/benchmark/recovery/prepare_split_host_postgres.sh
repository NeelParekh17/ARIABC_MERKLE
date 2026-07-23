#!/usr/bin/env bash
# Start one isolated AriaBC PostgreSQL instance for split-host recovery.
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: prepare_split_host_postgres.sh OPTIONS

Required:
  --role healthy|damaged
  --install-dir DIR
  --runtime-root DIR
  --ledger-sql FILE
  --port PORT
  --db-name NAME
  --db-user NAME
  --allowed-client-ip IPv4

The cluster is persistent at RUNTIME_ROOT/postgres-ROLE. Existing clusters at
that exact path are reused; no existing PGDATA is deleted. A listener already
occupying PORT outside that PGDATA causes a fail-closed error.
USAGE
}

ROLE=""
INSTALL_DIR=""
RUNTIME_ROOT=""
LEDGER_SQL=""
PORT=""
DB_NAME=""
DB_USER=""
ALLOWED_CLIENT_IP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role) ROLE="${2:?}"; shift 2 ;;
    --install-dir) INSTALL_DIR="${2:?}"; shift 2 ;;
    --runtime-root) RUNTIME_ROOT="${2:?}"; shift 2 ;;
    --ledger-sql) LEDGER_SQL="${2:?}"; shift 2 ;;
    --port) PORT="${2:?}"; shift 2 ;;
    --db-name) DB_NAME="${2:?}"; shift 2 ;;
    --db-user) DB_USER="${2:?}"; shift 2 ;;
    --allowed-client-ip) ALLOWED_CLIENT_IP="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$ROLE" =~ ^(healthy|damaged)$ ]] || { echo "invalid --role" >&2; exit 2; }
[[ "$PORT" =~ ^[1-9][0-9]*$ ]] || { echo "invalid --port" >&2; exit 2; }
[[ "$DB_NAME" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || { echo "invalid --db-name" >&2; exit 2; }
[[ "$DB_USER" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || { echo "invalid --db-user" >&2; exit 2; }
[[ "$ALLOWED_CLIENT_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "--allowed-client-ip must be an IPv4 address" >&2; exit 2
}
[[ -f "$LEDGER_SQL" ]] || { echo "missing ledger SQL: $LEDGER_SQL" >&2; exit 1; }

BIN_DIR="$INSTALL_DIR/bin"
for binary in initdb pg_ctl pg_isready psql createdb postgres; do
  [[ -x "$BIN_DIR/$binary" ]] || {
    echo "missing AriaBC binary: $BIN_DIR/$binary" >&2; exit 1
  }
done
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}"
"$BIN_DIR/postgres" --version >/dev/null

PGDATA="$RUNTIME_ROOT/postgres-$ROLE"
LOG_DIR="$RUNTIME_ROOT/logs-$ROLE"
mkdir -p "$RUNTIME_ROOT" "$LOG_DIR"

if [[ -f "$PGDATA/postmaster.pid" ]]; then
  existing_pid="$(sed -n '1p' "$PGDATA/postmaster.pid")"
  if [[ "$existing_pid" =~ ^[0-9]+$ ]] && kill -0 "$existing_pid" 2>/dev/null; then
    "$BIN_DIR/pg_ctl" -D "$PGDATA" -m fast -w -t 60 stop 2>/dev/null || \
      "$BIN_DIR/pg_ctl" -D "$PGDATA" -m immediate -w -t 30 stop
  fi
fi

if ss -ltn 2>/dev/null | awk -v port=":$PORT" '$4 ~ (port "$") {found=1} END {exit !found}'; then
  echo "port $PORT is occupied by a process outside $PGDATA" >&2
  exit 1
fi

if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
  if [[ -e "$PGDATA" && -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "refusing to initialize non-empty directory without PG_VERSION: $PGDATA" >&2
    exit 1
  fi
  mkdir -p "$PGDATA"
  "$BIN_DIR/initdb" -D "$PGDATA" -U "$DB_USER" --auth=trust \
    >"$LOG_DIR/initdb.log" 2>&1
fi

hba_rule="host all all $ALLOWED_CLIENT_IP/32 trust"
grep -Fqx "$hba_rule" "$PGDATA/pg_hba.conf" || printf '%s\n' "$hba_rule" >>"$PGDATA/pg_hba.conf"

start_options="-p $PORT -c listen_addresses='*' -c unix_socket_directories='/tmp' -c max_wal_size=8GB -c min_wal_size=2GB -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9"
"$BIN_DIR/pg_ctl" -D "$PGDATA" -l "$LOG_DIR/postgres.log" \
  -o "$start_options" -w -t 120 start >"$LOG_DIR/pg_ctl_start.log" 2>&1
"$BIN_DIR/pg_isready" -h 127.0.0.1 -p "$PORT" -U "$DB_USER" -d postgres -t 10

db_exists="$("$BIN_DIR/psql" -X -qAt -h 127.0.0.1 -p "$PORT" -U "$DB_USER" \
  -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")"
if [[ "$db_exists" != 1 ]]; then
  "$BIN_DIR/createdb" -h 127.0.0.1 -p "$PORT" -U "$DB_USER" "$DB_NAME"
fi
"$BIN_DIR/psql" -X -v ON_ERROR_STOP=1 -q -h 127.0.0.1 -p "$PORT" \
  -U "$DB_USER" -d "$DB_NAME" -f "$LEDGER_SQL" >"$LOG_DIR/ledger_schema.log" 2>&1

printf 'SPLIT_HOST_POSTGRES_READY role=%s pgdata=%s port=%s database=%s\n' \
  "$ROLE" "$PGDATA" "$PORT" "$DB_NAME"
