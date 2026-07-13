#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=""
INSTALL_DIR=""
DB_PORT=5438
DB_USER="postgres"
DB_NAME="postgres"
TEMPLATE_CONFIG=""
REQUIRE_CUSTOM=0
FRESH_PGDATA=0

usage() {
  cat <<'EOF'
Usage:
  ensure_single_node_postgres.sh \
    --repo-root </home/neel/Desktop/ariabc_cluster> \
    --install-dir </home/neel/Desktop/ariabc_install> \
    [--db-port <5438>] [--db-user <postgres>] [--db-name <postgres>] \
    --template-config </home/neel/Desktop/ariabc_cluster/.bench_tmp/shared_postgresql.conf> \
    [--require-custom] [--fresh-pgdata]

Ensures a local single-node postgres cluster exists and is running on given port.
Prints: PGDATA=<path>
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) REPO_ROOT="${2:-}"; shift 2 ;;
    --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
    --db-port) DB_PORT="${2:-5438}"; shift 2 ;;
    --db-user) DB_USER="${2:-postgres}"; shift 2 ;;
    --db-name) DB_NAME="${2:-postgres}"; shift 2 ;;
    --template-config) TEMPLATE_CONFIG="${2:-}"; shift 2 ;;
    --require-custom) REQUIRE_CUSTOM=1; shift 1 ;;
    --fresh-pgdata) FRESH_PGDATA=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$REPO_ROOT" || -z "$INSTALL_DIR" || -z "$TEMPLATE_CONFIG" ]]; then
  usage
  echo "ERROR: --repo-root, --install-dir, and --template-config are required" >&2
  exit 2
fi

BIN_DIR="$INSTALL_DIR/bin"
if [[ -x "$BIN_DIR/initdb" && -x "$BIN_DIR/postgres" ]]; then
  if ! (LD_LIBRARY_PATH="$INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}" "$BIN_DIR/initdb" --version >/dev/null 2>&1 && \
        LD_LIBRARY_PATH="$INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}" "$BIN_DIR/postgres" --version >/dev/null 2>&1); then
    BIN_DIR=""
  fi
fi

if [[ "$REQUIRE_CUSTOM" != "1" && ( -z "$BIN_DIR" || ! -x "$BIN_DIR/initdb" || ! -x "$BIN_DIR/pg_ctl" || ! -x "$BIN_DIR/pg_isready" || ! -x "$BIN_DIR/postgres" ) ]]; then
  for c in /usr/lib/postgresql/*/bin; do
    if [[ -x "$c/initdb" && -x "$c/pg_ctl" && -x "$c/pg_isready" && -x "$c/postgres" ]]; then
      if "$c/initdb" --version >/dev/null 2>&1 && "$c/postgres" --version >/dev/null 2>&1; then
        BIN_DIR="$c"
        break
      fi
    fi
  done
fi

if [[ "$REQUIRE_CUSTOM" == "1" && ( -z "$BIN_DIR" || ! -x "$BIN_DIR/initdb" || ! -x "$BIN_DIR/pg_ctl" || ! -x "$BIN_DIR/pg_isready" || ! -x "$BIN_DIR/postgres" ) ]]; then
  echo "ERROR: runnable custom postgres binaries not found under $INSTALL_DIR" >&2
  exit 1
fi

if [[ ! -x "$BIN_DIR/initdb" || ! -x "$BIN_DIR/pg_ctl" ]]; then
  echo "ERROR: usable postgres binaries not found" >&2
  exit 1
fi

if [[ "$BIN_DIR" == "$INSTALL_DIR/bin" && -d "$INSTALL_DIR/lib" ]]; then
  export LD_LIBRARY_PATH="$INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}"
fi

PGDATA_DIR="$REPO_ROOT/.bench_tmp/single_node_pgdata"

if [[ "$FRESH_PGDATA" == "1" && -f "$PGDATA_DIR/PG_VERSION" ]]; then
  echo "[ensure_pg] Reinitializing benchmark PGDATA for a clean campaign..."
  "$BIN_DIR/pg_ctl" -D "$PGDATA_DIR" stop -m immediate -w -t 30 >/dev/null 2>&1 || true
  stray_pids=$(pgrep -f "postgres.*-D $PGDATA_DIR" || true)
  if [[ -n "$stray_pids" ]]; then
    kill -TERM $stray_pids >/dev/null 2>&1 || true
    sleep 2
  fi
  rm -rf "$PGDATA_DIR"
fi

mkdir -p "$PGDATA_DIR"

if [[ ! -f "$PGDATA_DIR/PG_VERSION" ]]; then
  rm -rf "$PGDATA_DIR"/*
  "$BIN_DIR/initdb" -D "$PGDATA_DIR" -U postgres --auth=trust >/dev/null
fi

if [[ ! -f "$TEMPLATE_CONFIG" ]]; then
  echo "ERROR: template config not found: $TEMPLATE_CONFIG" >&2
  exit 1
fi

cp "$TEMPLATE_CONFIG" "$PGDATA_DIR/postgresql.conf"

# If we had to fall back to system postgres binaries (non-BCDB build),
# drop BCDB-only GUCs so postmaster can start with the canonical template.
if [[ "$BIN_DIR" != "$INSTALL_DIR/bin" ]]; then
  sed -i -E '/^[[:space:]]*(bcdb_[a-zA-Z0-9_]+|merkle_update_detection|enable_merkle_index|merkle_update_detection_suppress)[[:space:]]*=.*/d' \
    "$PGDATA_DIR/postgresql.conf"
  if grep -Eq '^[[:space:]]*unix_socket_directories[[:space:]]*=' "$PGDATA_DIR/postgresql.conf"; then
    sed -i -E "s|^[[:space:]]*unix_socket_directories[[:space:]]*=.*$|unix_socket_directories = '/tmp'|" \
      "$PGDATA_DIR/postgresql.conf"
  else
    echo "unix_socket_directories = '/tmp'" >> "$PGDATA_DIR/postgresql.conf"
  fi
fi

# Hard lock: never allow benchmark override includes to alter canonical config.
sed -i "/include_if_exists = 'bench_single_auto.conf'/d" "$PGDATA_DIR/postgresql.conf"
rm -f "$PGDATA_DIR/bench_single_auto.conf"
# Reset ALTER SYSTEM state between runs. This avoids startup failures when
# previous runs wrote custom GUCs that are not recognized by the current binary.
rm -f "$PGDATA_DIR/postgresql.auto.conf"
if ! grep -Eq '^host\s+all\s+all\s+127\.0\.0\.1/32\s+trust$' "$PGDATA_DIR/pg_hba.conf"; then
  echo "host all all 127.0.0.1/32 trust" >> "$PGDATA_DIR/pg_hba.conf"
fi

if "$BIN_DIR/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; then
  "$BIN_DIR/pg_ctl" -D "$PGDATA_DIR" -w -t 90 stop -m fast || true
fi

# If some other leftover postmaster is still holding DB_PORT, stop it so the
# isolated single-node benchmark instance can bind deterministically.
port_pids="$(ss -ltnp 2>/dev/null | awk -v p=":$DB_PORT" '$4 ~ p {print $NF}' | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u | tr '\n' ' ')"
if [[ -n "${port_pids// }" ]]; then
  kill -TERM $port_pids >/dev/null 2>&1 || true
  sleep 2
  still_pids="$(ss -ltnp 2>/dev/null | awk -v p=":$DB_PORT" '$4 ~ p {print $NF}' | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u | tr '\n' ' ')"
  if [[ -n "${still_pids// }" ]]; then
    kill -KILL $still_pids >/dev/null 2>&1 || true
    sleep 1
  fi
fi

# Also forcibly kill any stray postgres processes running from this exact PGDATA_DIR
# to prevent "pre-existing shared memory block" errors if postmaster.pid was deleted but
# the process is still alive.
stray_pids=$(pgrep -f "postgres.*-D $PGDATA_DIR" || true)
if [[ -n "$stray_pids" ]]; then
  kill -TERM $stray_pids >/dev/null 2>&1 || true
  sleep 2
  stray_pids=$(pgrep -f "postgres.*-D $PGDATA_DIR" || true)
  if [[ -n "$stray_pids" ]]; then
    kill -KILL $stray_pids >/dev/null 2>&1 || true
    sleep 1
  fi
fi

_try_start_postgres() {
  ulimit -c unlimited
  "$BIN_DIR/pg_ctl" -D "$PGDATA_DIR" -w -t 120 start -l "$REPO_ROOT/server.log" 2>&1
}

_strip_bcdb_only_gucs() {
  sed -i -E '/^[[:space:]]*(bcdb_[a-zA-Z0-9_]+|merkle_update_detection|enable_merkle_index|merkle_update_detection_suppress)[[:space:]]*=.*/d' \
    "$PGDATA_DIR/postgresql.conf"
}

_reinit_pgdata() {
  echo "[ensure_pg] Detected corrupted pgdata — wiping and reinitialising..."
  "$BIN_DIR/pg_ctl" -D "$PGDATA_DIR" stop -m immediate 2>/dev/null || true
  rm -rf "$PGDATA_DIR"
  mkdir -p "$PGDATA_DIR"
  "$BIN_DIR/initdb" -D "$PGDATA_DIR" -U postgres --auth=trust >/dev/null
  cp "$TEMPLATE_CONFIG" "$PGDATA_DIR/postgresql.conf"
  if [[ "$BIN_DIR" != "$INSTALL_DIR/bin" ]]; then
    _strip_bcdb_only_gucs
  fi
  sed -i "/include_if_exists = 'bench_single_auto.conf'/d" "$PGDATA_DIR/postgresql.conf"
  rm -f "$PGDATA_DIR/bench_single_auto.conf" "$PGDATA_DIR/postgresql.auto.conf"
  if ! grep -Eq '^host\s+all\s+all\s+127\.0\.0\.1/32\s+trust$' "$PGDATA_DIR/pg_hba.conf"; then
    echo "host all all 127.0.0.1/32 trust" >> "$PGDATA_DIR/pg_hba.conf"
  fi
}

if ! _try_start_postgres; then
  if grep -qE "could not locate a valid checkpoint|invalid primary checkpoint|invalid checkpoint" \
      "$REPO_ROOT/server.log" 2>/dev/null; then
    _reinit_pgdata
    _try_start_postgres
  elif grep -qE 'unrecognized configuration parameter "(bcdb_[a-zA-Z0-9_]+|merkle_update_detection|enable_merkle_index|merkle_update_detection_suppress)"' \
      "$REPO_ROOT/server.log" 2>/dev/null; then
    echo "[ensure_pg] Detected non-BCDB postgres binary; removing BCDB-only GUCs and retrying..."
    _strip_bcdb_only_gucs
    _try_start_postgres
  else
    echo "ERROR: pg_ctl start failed" >&2
    exit 1
  fi
fi
"$BIN_DIR/pg_isready" -h 127.0.0.1 -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t 5 >/dev/null

if [[ "$FRESH_PGDATA" == "1" ]]; then
  BOOTSTRAP_SCRIPT="$REPO_ROOT/scripts/distributed/bootstrap_raft_apply_ledger.sh"
  if [[ ! -f "$BOOTSTRAP_SCRIPT" ]]; then
    echo "ERROR: missing Merkle bootstrap script: $BOOTSTRAP_SCRIPT" >&2
    exit 1
  fi
  PATH="$BIN_DIR:$PATH" bash "$BOOTSTRAP_SCRIPT" \
    --db "$DB_NAME" --port "$DB_PORT" --host 127.0.0.1 --user "$DB_USER" \
    --schema-only --reset-for-restore >/dev/null
fi

echo "PGDATA=$PGDATA_DIR"
