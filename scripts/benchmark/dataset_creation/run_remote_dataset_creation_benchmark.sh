#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUT_FILE="$ROOT/out.txt"

# Tee all stdout and stderr live to out.txt at the repository root
if [[ -z "${DATASET_BENCH_LOG_TEE_ACTIVE:-}" ]]; then
  export DATASET_BENCH_LOG_TEE_ACTIVE=1
  : > "$OUT_FILE"
  exec > >(tee -a "$OUT_FILE") 2> >(tee -a "$OUT_FILE" >&2)
fi

usage() {
  cat <<'USAGE'
Usage: run_remote_dataset_creation_benchmark.sh [options]

Options:
  --host HOST                  default: ranking (10.129.148.247 / ranking.cse.iitb.ac.in)
  --ssh-user USER              default: protectdr
  --ssh-port PORT              default: 22
  --ssh-key PATH               path to SSH private key
  --remote-root PATH           default: /home/protectdr/dataset_creation_bench_runs
  --remote-python PATH         default: /usr/bin/python3
  --build-profile debug|release  default: release
  --scales SCALES              default: 1000,10000,100000,1000000 (1k, 10k, 100k, 1M)
  --repetitions N              default: 3
  --fanout N                   default: 4
  --split-threshold N          default: 32
  --merge-threshold N          default: 8
  --partitions N               default: 200
  --synchronous-commit on|off  default: off
  --min-free-gib N             default: 20
  --ssh-timeout SECONDS        default: 15
USAGE
}

HOST="ranking"
SSH_USER="protectdr"
SSH_PORT="22"
SSH_KEY=""
SSH_PASSWORD="${SSH_PASSWORD:-}"
REMOTE_ROOT="/home/protectdr/dataset_creation_bench_runs"
REMOTE_PYTHON="/usr/bin/python3"
BUILD_PROFILE="release"
SCALES="1000,10000,100000,1000000"
REPETITIONS="3"
FANOUT="4"
SPLIT_THRESHOLD="32"
MERGE_THRESHOLD="8"
PARTITIONS="200"
SYNCHRONOUS_COMMIT="off"
MIN_FREE_GIB="20"
SSH_TIMEOUT="${SSH_TIMEOUT:-15}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="${2:?}"; shift 2 ;;
    --ssh-user) SSH_USER="${2:?}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:?}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:?}"; shift 2 ;;
    --remote-root) REMOTE_ROOT="${2:?}"; shift 2 ;;
    --remote-python) REMOTE_PYTHON="${2:?}"; shift 2 ;;
    --build-profile) BUILD_PROFILE="${2:?}"; shift 2 ;;
    --scales) SCALES="${2:?}"; shift 2 ;;
    --repetitions) REPETITIONS="${2:?}"; shift 2 ;;
    --fanout) FANOUT="${2:?}"; shift 2 ;;
    --split-threshold) SPLIT_THRESHOLD="${2:?}"; shift 2 ;;
    --merge-threshold) MERGE_THRESHOLD="${2:?}"; shift 2 ;;
    --partitions) PARTITIONS="${2:?}"; shift 2 ;;
    --synchronous-commit) SYNCHRONOUS_COMMIT="${2:?}"; shift 2 ;;
    --min-free-gib) MIN_FREE_GIB="${2:?}"; shift 2 ;;
    --ssh-timeout) SSH_TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

resolve_host_ip() {
  case "$1" in
    admin123) printf '%s\n' "10.129.148.247" ;;
    user4) printf '%s\n' "10.129.148.246" ;;
    utkarsh) printf '%s\n' "10.129.148.248" ;;
    ranking|ranking.cse.iitb.ac.in) printf '%s\n' "ranking.cse.iitb.ac.in" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

progress() {
  printf '[%s] [local] %s\n' "$(date --iso-8601=seconds)" "$*" >&2
}

SSH_TARGET="$(resolve_host_ip "$HOST")"
if [[ -n "$SSH_USER" ]]; then
  SSH_TARGET="$SSH_USER@$SSH_TARGET"
fi

SSH_COMMON_OPTS=(
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout="$SSH_TIMEOUT"
  -o ConnectionAttempts=1
  -o ServerAliveInterval="$SSH_TIMEOUT"
  -o ServerAliveCountMax=1
  -o LogLevel=ERROR
)

if [[ -n "$SSH_PASSWORD" ]]; then
  command -v sshpass >/dev/null 2>&1 || { echo "SSH_PASSWORD was set but sshpass is unavailable" >&2; exit 1; }
  export SSHPASS="$SSH_PASSWORD"
  SSH_CMD=(sshpass -e ssh -p "$SSH_PORT" "${SSH_COMMON_OPTS[@]}")
  SCP_CMD=(sshpass -e scp -P "$SSH_PORT" "${SSH_COMMON_OPTS[@]}")
  RSYNC_RSH="sshpass -e ssh -p $SSH_PORT"
else
  SSH_CMD=(ssh -p "$SSH_PORT" -o BatchMode=yes "${SSH_COMMON_OPTS[@]}")
  SCP_CMD=(scp -P "$SSH_PORT" -o BatchMode=yes "${SSH_COMMON_OPTS[@]}")
  RSYNC_RSH="ssh -p $SSH_PORT -o BatchMode=yes"
fi

if [[ -n "$SSH_KEY" ]]; then
  SSH_CMD+=(-i "$SSH_KEY")
  SCP_CMD+=(-i "$SSH_KEY")
  RSYNC_RSH="$RSYNC_RSH -i $SSH_KEY"
fi

remote_ssh_cmd() {
  "${SSH_CMD[@]}" "$SSH_TARGET" "$@"
}

remote_ssh_cmd_stdinless() {
  remote_ssh_cmd "$@" </dev/null
}

RUN_ID="dataset-creation-$(date -u +%Y%m%dT%H%M%SZ)-$(printf '%06x' "$((RANDOM << 1 ^ RANDOM))")"
REMOTE_RUN_DIR="$REMOTE_ROOT/runs/$RUN_ID"
REMOTE_SRC_DIR="$REMOTE_RUN_DIR/src"
REMOTE_INSTALL_DIR="$REMOTE_RUN_DIR/install"
REMOTE_PGDATA="$REMOTE_RUN_DIR/pgdata"
REMOTE_RESULTS_DIR="$REMOTE_RUN_DIR/results"
REMOTE_LOG_DIR="$REMOTE_RUN_DIR/logs"
LOCAL_RESULTS_DIR="$ROOT/scripts/benchmark/dataset_creation/results/$RUN_ID"
ROOTS_FILE="$ROOT/scripts/benchmark/recovery/sync_source_roots.txt"

progress "Starting dataset creation benchmark run $RUN_ID on $SSH_TARGET (scales=$SCALES reps=$REPETITIONS)"

# Check SSH connectivity
if ! remote_ssh_cmd_stdinless "printf '%s\n' ssh-ok" >/dev/null; then
  echo "SSH connectivity check failed for $SSH_TARGET" >&2
  exit 1
fi

progress "Creating remote run directories under $REMOTE_RUN_DIR"
remote_ssh_cmd_stdinless "mkdir -p '$REMOTE_RUN_DIR' '$REMOTE_SRC_DIR' '$REMOTE_LOG_DIR' '$REMOTE_RESULTS_DIR'"

# Clean up remote run directory on exit if error
cleanup_needed=1
cleanup_local() {
  local rc=$?
  if [[ "$cleanup_needed" -eq 1 ]]; then
    progress "Cleaning remote run directory: $REMOTE_RUN_DIR"
    remote_ssh_cmd_stdinless "rm -rf '$REMOTE_RUN_DIR'" >/dev/null 2>&1 || true
  fi
  return "$rc"
}
trap cleanup_local EXIT

# Sync source files with complete exclusions of object files, generated headers, and stamps
progress "Syncing source files to remote host"
while IFS= read -r rel; do
  [[ -z "$rel" || "$rel" == \#* ]] && continue
  src="$ROOT/$rel"
  dest="$REMOTE_SRC_DIR/$rel"
  if [[ -d "$src" && ! -L "$src" ]]; then
    remote_ssh_cmd_stdinless "mkdir -p '$(dirname "$dest")' '$dest'"
    rsync -aL -q --delete -e "$RSYNC_RSH" \
      --exclude 'results/' --exclude 'fetched/' --exclude '__pycache__/' \
      --exclude '*.pyc' --exclude '*.pyo' \
      --exclude '*.o' --exclude '*.a' --exclude '*.so' --exclude '*.so.*' \
      --exclude '*.d' --exclude '*.gcda' --exclude '*.gcno' \
      --exclude '*.copybin' --exclude '*.tar' --exclude '*.tar.gz' --exclude '*.zip' \
      --exclude 'bin/initdb/postgres' --exclude 'bin/pg_ctl/postgres' \
      --exclude '*.pc' --exclude '*.list' \
      --exclude '*_d.h' --exclude '*_d.dat' \
      --exclude 'schemapg.h' --exclude 'errcodes.h' \
      --exclude 'fmgroids.h' --exclude 'fmgrprotos.h' \
      --exclude 'lwlocknames.h' --exclude 'lwlocknames.c' \
      --exclude 'probes.h' --exclude 'plerrcodes.h' \
      --exclude 'pg_config.h' --exclude 'pg_config_ext.h' \
      --exclude 'pg_config_os.h' --exclude 'ecpg_config.h' \
      --exclude 'pg_config_paths.h' \
      --exclude 'objfiles.txt' --exclude 'exports.list' \
      --exclude 'snowball_create.sql' \
      --exclude '*.stamp' --exclude 'bki-stamp' --exclude 'header-stamp' \
      "$src/" "$SSH_TARGET:$dest/"
  else
    remote_ssh_cmd_stdinless "mkdir -p '$(dirname "$dest")'"
    rsync -aL -q -e "$RSYNC_RSH" "$src" "$SSH_TARGET:$dest"
  fi
done < "$ROOTS_FILE"

# Sync OpenSSL headers if present
if [[ -d "/usr/include/openssl" ]]; then
  local_tmp_headers="/tmp/openssl_compat_headers_$$"
  mkdir -p "$local_tmp_headers"
  cp -rL /usr/include/openssl/* "$local_tmp_headers/"
  if [[ -d "/usr/include/x86_64-linux-gnu/openssl" ]]; then
    cp -rL /usr/include/x86_64-linux-gnu/openssl/* "$local_tmp_headers/"
  fi
  remote_ssh_cmd_stdinless "mkdir -p '$REMOTE_SRC_DIR/src/include/openssl'"
  rsync -aL -q -e "$RSYNC_RSH" "$local_tmp_headers/" "$SSH_TARGET:$REMOTE_SRC_DIR/src/include/openssl/"
  rm -rf "$local_tmp_headers"
fi

remote_env_prefix=$(printf 'RUN_ID=%q REMOTE_RUN_DIR=%q REMOTE_SRC_DIR=%q REMOTE_INSTALL_DIR=%q REMOTE_PGDATA=%q REMOTE_RESULTS_DIR=%q REMOTE_LOG_DIR=%q REMOTE_PYTHON=%q BUILD_PROFILE=%q SCALES=%q REPETITIONS=%q FANOUT=%q SPLIT_THRESHOLD=%q MERGE_THRESHOLD=%q PARTITIONS=%q SYNCHRONOUS_COMMIT=%q' \
  "$RUN_ID" "$REMOTE_RUN_DIR" "$REMOTE_SRC_DIR" "$REMOTE_INSTALL_DIR" "$REMOTE_PGDATA" "$REMOTE_RESULTS_DIR" "$REMOTE_LOG_DIR" "$REMOTE_PYTHON" "$BUILD_PROFILE" "$SCALES" "$REPETITIONS" "$FANOUT" "$SPLIT_THRESHOLD" "$MERGE_THRESHOLD" "$PARTITIONS" "$SYNCHRONOUS_COMMIT")

progress "Uploading runner payload to $REMOTE_RUN_DIR/run_payload.sh"
remote_ssh_cmd "cat > '$REMOTE_RUN_DIR/run_payload.sh' && chmod +x '$REMOTE_RUN_DIR/run_payload.sh'" <<'REMOTE_PAYLOAD'
#!/usr/bin/env bash
set -Eeuo pipefail

remote_progress() {
  printf '[%s] [remote:%s] %s\n' "$(date --iso-8601=seconds)" "$(hostname -s 2>/dev/null || hostname)" "$*" >&2
}

tail_log() {
  local log_file="$1"
  if [[ -s "$log_file" ]]; then
    remote_progress "Last 40 lines from $log_file:"
    tail -n 40 "$log_file" >&2 || true
  fi
}

fail_with_log() {
  local reason="$1"
  local log_file="${2:-}"
  remote_progress "FAILURE: $reason"
  if [[ -n "$log_file" ]]; then
    tail_log "$log_file"
  fi
  exit 1
}

cd "$REMOTE_SRC_DIR"

if [[ "$BUILD_PROFILE" == "release" ]]; then
  CONFIGURE_ARGS=(--prefix="$REMOTE_INSTALL_DIR" --without-readline)
  BUILD_CFLAGS="-O2 -g"
else
  CONFIGURE_ARGS=(--prefix="$REMOTE_INSTALL_DIR" --enable-debug --enable-cassert --without-readline)
  BUILD_CFLAGS="-O0 -g3"
fi

local_lib_dir="$REMOTE_SRC_DIR/openssl_compat_libs"
mkdir -p "$local_lib_dir"
ssl_so=$(find /usr/lib /lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu -name "libssl.so.*" -print -quit 2>/dev/null)
crypto_so=$(find /usr/lib /lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu -name "libcrypto.so.*" -print -quit 2>/dev/null)
if [[ -n "$ssl_so" ]]; then ln -sf "$ssl_so" "$local_lib_dir/libssl.so"; fi
if [[ -n "$crypto_so" ]]; then ln -sf "$crypto_so" "$local_lib_dir/libcrypto.so"; fi

remote_progress "Configuring PostgreSQL ($BUILD_PROFILE build)..."
if ! (ac_cv_exeext= CPPFLAGS="${CPPFLAGS:-}" LDFLAGS="-L$local_lib_dir ${LDFLAGS:-}" \
  CFLAGS="$BUILD_CFLAGS" \
  ./configure "${CONFIGURE_ARGS[@]}" >"$REMOTE_LOG_DIR/configure.log" 2>&1); then
  fail_with_log "configure failed" "$REMOTE_LOG_DIR/configure.log"
fi

remote_progress "Cleaning stale generated headers..."
rm -f \
  src/backend/catalog/bki-stamp \
  src/backend/catalog/header-stamp \
  src/include/catalog/header-stamp \
  src/backend/utils/header-stamp \
  src/include/catalog/*_d.h \
  src/backend/catalog/*_d.h
make clean >/dev/null 2>&1 || true

remote_progress "Building generated headers..."
if ! make -B -C src/backend generated-headers >"$REMOTE_LOG_DIR/generated_headers.log" 2>&1; then
  fail_with_log "generated headers failed" "$REMOTE_LOG_DIR/generated_headers.log"
fi

remote_progress "Building PostgreSQL with $(nproc) cores..."
if ! make -j"$(nproc)" >"$REMOTE_LOG_DIR/build.log" 2>&1; then
  fail_with_log "PostgreSQL build failed" "$REMOTE_LOG_DIR/build.log"
fi

remote_progress "Installing PostgreSQL to $REMOTE_INSTALL_DIR..."
if ! make install prefix="$REMOTE_INSTALL_DIR" >"$REMOTE_LOG_DIR/install.log" 2>&1; then
  fail_with_log "install failed" "$REMOTE_LOG_DIR/install.log"
fi
export LD_LIBRARY_PATH="$REMOTE_INSTALL_DIR/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

remote_progress "Initializing database cluster (initdb)..."
if ! "$REMOTE_INSTALL_DIR/bin/initdb" -D "$REMOTE_PGDATA" >"$REMOTE_LOG_DIR/initdb.log" 2>&1; then
  fail_with_log "initdb failed" "$REMOTE_LOG_DIR/initdb.log"
fi

SOCKET_TEMPLATE="${TMPDIR:-/tmp}/ariabc-db.XXXXXX"
REMOTE_SOCKET_DIR="$(mktemp -d "$SOCKET_TEMPLATE")"
chmod 700 "$REMOTE_SOCKET_DIR"

cleanup_pg() {
  remote_progress "Shutting down database..."
  "$REMOTE_INSTALL_DIR/bin/pg_ctl" -D "$REMOTE_PGDATA" stop -m fast >/dev/null 2>&1 || true
  rm -rf "$REMOTE_SOCKET_DIR" || true
}
trap cleanup_pg EXIT

remote_progress "Starting PostgreSQL with high-performance memory settings on socket $REMOTE_SOCKET_DIR..."
if ! "$REMOTE_INSTALL_DIR/bin/pg_ctl" -D "$REMOTE_PGDATA" -l "$REMOTE_LOG_DIR/postgres.log" \
  -o "-k $REMOTE_SOCKET_DIR -p 55432 -c listen_addresses='' -c shared_buffers=32GB -c effective_cache_size=160GB -c maintenance_work_mem=16GB -c work_mem=256MB -c max_wal_size=128GB -c checkpoint_timeout=60min -c autovacuum=off -c synchronous_commit=off -c wal_buffers=256MB -c max_worker_processes=128 -c max_parallel_workers=96 -c max_parallel_maintenance_workers=32" \
  -w start >"$REMOTE_LOG_DIR/pg_ctl_start.log" 2>&1; then
  fail_with_log "pg_ctl start failed" "$REMOTE_LOG_DIR/pg_ctl_start.log"
fi

if ! "$REMOTE_INSTALL_DIR/bin/pg_isready" -h "$REMOTE_SOCKET_DIR" -p 55432 -d postgres -U "$(id -un)" >/dev/null; then
  fail_with_log "pg_isready failed" "$REMOTE_LOG_DIR/postgres.log"
fi

remote_progress "Bootstrapping Raft/Merkle ledger schema..."
if ! "$REMOTE_INSTALL_DIR/bin/psql" -X -v ON_ERROR_STOP=1 \
  -h "$REMOTE_SOCKET_DIR" -p 55432 -d postgres -U "$(id -un)" \
  -f "$REMOTE_SRC_DIR/scripts/distributed/sql/raft_apply_ledger_schema.sql" >"$REMOTE_LOG_DIR/ledger_schema.log" 2>&1; then
  fail_with_log "ledger schema bootstrap failed" "$REMOTE_LOG_DIR/ledger_schema.log"
fi

BENCH_DSN="host=$REMOTE_SOCKET_DIR port=55432 dbname=postgres user=$(id -un)"

remote_progress "Launching Python dataset creation benchmark suite..."
PYTHONUNBUFFERED=1 "$REMOTE_PYTHON" \
  "$REMOTE_SRC_DIR/scripts/benchmark/dataset_creation/benchmark_dataset_creation.py" \
  --dsn "$BENCH_DSN" \
  --scales "$SCALES" \
  --repetitions "$REPETITIONS" \
  --fanout "$FANOUT" \
  --split-threshold "$SPLIT_THRESHOLD" \
  --merge-threshold "$MERGE_THRESHOLD" \
  --partitions "$PARTITIONS" \
  --synchronous-commit "$SYNCHRONOUS_COMMIT" \
  --output-dir "$REMOTE_RESULTS_DIR"

remote_progress "Benchmark execution finished successfully!"
REMOTE_PAYLOAD

progress "Executing remote dataset creation benchmark..."
remote_ssh_cmd "env $remote_env_prefix bash '$REMOTE_RUN_DIR/run_payload.sh'"

progress "Fetching benchmark artifacts to local directory $LOCAL_RESULTS_DIR"
mkdir -p "$LOCAL_RESULTS_DIR"
"${SCP_CMD[@]}" -r "$SSH_TARGET:$REMOTE_RESULTS_DIR/*" "$LOCAL_RESULTS_DIR/"
"${SCP_CMD[@]}" -r "$SSH_TARGET:$REMOTE_LOG_DIR" "$LOCAL_RESULTS_DIR/"

progress "Cleaning up remote run directory: $REMOTE_RUN_DIR"
remote_ssh_cmd_stdinless "rm -rf '$REMOTE_RUN_DIR'" >/dev/null 2>&1 || true
cleanup_needed=0

# Run local plotting if python + matplotlib available
LOCAL_PYTHON="$ROOT/.venv/bin/python3"
if [[ ! -x "$LOCAL_PYTHON" ]]; then LOCAL_PYTHON="$(command -v python3)"; fi

if [[ -x "$LOCAL_PYTHON" ]] && "$LOCAL_PYTHON" -c "import matplotlib, numpy" >/dev/null 2>&1; then
  progress "Generating visualization plots from results..."
  "$LOCAL_PYTHON" "$ROOT/scripts/benchmark/dataset_creation/plot_dataset_creation.py" \
    --csv "$LOCAL_RESULTS_DIR/dataset_creation_results.csv" \
    --output-dir "$LOCAL_RESULTS_DIR"
fi

progress "Dataset creation benchmark complete! Results saved in $LOCAL_RESULTS_DIR"
cat "$LOCAL_RESULTS_DIR/dataset_creation_results.csv"
