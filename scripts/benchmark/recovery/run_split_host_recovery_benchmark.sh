#!/usr/bin/env bash
# True split-host recovery: damaged PostgreSQL on admin123, healthy on user4.
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: run_split_host_recovery_benchmark.sh [options]

Default placement:
  recovery client: admin123 (neel@10.129.148.247)
  damaged schema:  admin123:55432
  healthy schema:  user4:55432 (10.129.148.246)

By default the runner starts/reuses isolated AriaBC PostgreSQL clusters under
~/ariabc_split_host_recovery on both database hosts. It never deletes PGDATA.
Use --no-prepare-postgres only for already-running dedicated databases.

Safety:
  --allow-destructive-dataset-reset   required

Placement and SSH:
  --client-host HOST          default: admin123
  --client-user USER          default: neel
  --client-port PORT          default: 22
  --client-root DIR           default: ~/ariabc_split_host_recovery
  --client-python PATH        default: /usr/bin/python3
  --ssh-key PATH
  --db-ssh-port PORT          default: 22
  --healthy-host HOST         default: user4
  --healthy-ssh-user USER     default: neel
  --healthy-port PORT         default: 55432
  --healthy-user USER         default: neel
  --healthy-db NAME           default: merkle_recovery_bench
  --healthy-install-dir DIR   default: /home/neel/Desktop/ariabc_install
  --healthy-runtime-root DIR  default: ~/ariabc_split_host_recovery
  --damaged-host HOST         default: admin123
  --damaged-ssh-user USER     default: neel
  --damaged-port PORT         default: 55432
  --damaged-user USER         default: neel
  --damaged-db NAME           default: merkle_recovery_bench
  --damaged-install-dir DIR   default: /home/neel/Desktop/ariabc_install
  --damaged-runtime-root DIR  default: ~/ariabc_split_host_recovery
  --no-prepare-postgres       use externally managed dedicated databases
  --sslmode MODE              default: prefer

Benchmark:
  --tuple-count CSV           default: 1000000,3000000,5000000,10000000
  --full-scale                use 1M,3M,5M,7M,10M,15M,20M,25M,30M,40M,50M
  --fanout N                  2,4,8,16,32; default: 32
  --repetitions N             default: 1
  --network-probe-samples N   default: 20
  --no-warmup                 disable the untimed sparse-read warmup
  --local-results DIR         default: scripts/bench_full_results/split_host_recovery
USAGE
}

resolve_host() {
  case "$1" in
    admin123) printf '%s\n' 10.129.148.247 ;;
    user4) printf '%s\n' 10.129.148.246 ;;
    utkarsh) printf '%s\n' 10.129.148.248 ;;
    *) printf '%s\n' "$1" ;;
  esac
}

resolve_ipv4() {
  local value
  value="$(resolve_host "$1")"
  if [[ "$value" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s\n' "$value"
    return
  fi
  getent ahostsv4 "$value" | awk 'NR == 1 {print $1}'
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CLIENT_HOST=admin123
CLIENT_USER=neel
CLIENT_PORT=22
CLIENT_ROOT=""
CLIENT_PYTHON=/usr/bin/python3
SSH_KEY=""
DB_SSH_PORT=22
HEALTHY_HOST=user4
HEALTHY_SSH_USER=neel
HEALTHY_PORT=55432
HEALTHY_USER=neel
HEALTHY_DB=merkle_recovery_bench
HEALTHY_INSTALL_DIR=/home/neel/Desktop/ariabc_install
HEALTHY_RUNTIME_ROOT='~/ariabc_split_host_recovery'
DAMAGED_HOST=admin123
DAMAGED_SSH_USER=neel
DAMAGED_PORT=55432
DAMAGED_USER=neel
DAMAGED_DB=merkle_recovery_bench
DAMAGED_INSTALL_DIR=/home/neel/Desktop/ariabc_install
DAMAGED_RUNTIME_ROOT='~/ariabc_split_host_recovery'
PREPARE_POSTGRES=1
SSLMODE=prefer
TUPLE_COUNT=""
FULL_SCALE=0
FANOUT=32
REPETITIONS=1
NETWORK_PROBE_SAMPLES=20
NO_WARMUP=0
LOCAL_RESULTS="$REPO_ROOT/scripts/bench_full_results/split_host_recovery"
ALLOW_RESET=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-host) CLIENT_HOST="${2:?}"; shift 2 ;;
    --client-user) CLIENT_USER="${2:?}"; shift 2 ;;
    --client-port) CLIENT_PORT="${2:?}"; shift 2 ;;
    --client-root) CLIENT_ROOT="${2:?}"; shift 2 ;;
    --client-python) CLIENT_PYTHON="${2:?}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:?}"; shift 2 ;;
    --db-ssh-port) DB_SSH_PORT="${2:?}"; shift 2 ;;
    --healthy-host) HEALTHY_HOST="${2:?}"; shift 2 ;;
    --healthy-ssh-user) HEALTHY_SSH_USER="${2:?}"; shift 2 ;;
    --healthy-port) HEALTHY_PORT="${2:?}"; shift 2 ;;
    --healthy-user) HEALTHY_USER="${2:?}"; shift 2 ;;
    --healthy-db) HEALTHY_DB="${2:?}"; shift 2 ;;
    --healthy-install-dir) HEALTHY_INSTALL_DIR="${2:?}"; shift 2 ;;
    --healthy-runtime-root) HEALTHY_RUNTIME_ROOT="${2:?}"; shift 2 ;;
    --damaged-host) DAMAGED_HOST="${2:?}"; shift 2 ;;
    --damaged-ssh-user) DAMAGED_SSH_USER="${2:?}"; shift 2 ;;
    --damaged-port) DAMAGED_PORT="${2:?}"; shift 2 ;;
    --damaged-user) DAMAGED_USER="${2:?}"; shift 2 ;;
    --damaged-db) DAMAGED_DB="${2:?}"; shift 2 ;;
    --damaged-install-dir) DAMAGED_INSTALL_DIR="${2:?}"; shift 2 ;;
    --damaged-runtime-root) DAMAGED_RUNTIME_ROOT="${2:?}"; shift 2 ;;
    --no-prepare-postgres) PREPARE_POSTGRES=0; shift ;;
    --sslmode) SSLMODE="${2:?}"; shift 2 ;;
    --tuple-count) TUPLE_COUNT="${2:?}"; shift 2 ;;
    --full-scale) FULL_SCALE=1; shift ;;
    --fanout) FANOUT="${2:?}"; shift 2 ;;
    --repetitions) REPETITIONS="${2:?}"; shift 2 ;;
    --network-probe-samples) NETWORK_PROBE_SAMPLES="${2:?}"; shift 2 ;;
    --no-warmup) NO_WARMUP=1; shift ;;
    --local-results) LOCAL_RESULTS="${2:?}"; shift 2 ;;
    --allow-destructive-dataset-reset) ALLOW_RESET=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$ALLOW_RESET" -eq 1 ]] || {
  echo "refusing destructive benchmark without --allow-destructive-dataset-reset" >&2
  exit 2
}
[[ "$FANOUT" =~ ^(2|4|8|16|32)$ ]] || {
  echo "--fanout must be one of 2,4,8,16,32" >&2; exit 2
}
if [[ "$FULL_SCALE" -eq 1 && -n "$TUPLE_COUNT" ]]; then
  echo "--full-scale and --tuple-count are mutually exclusive" >&2; exit 2
fi
for value in "$CLIENT_PORT" "$DB_SSH_PORT" "$HEALTHY_PORT" "$DAMAGED_PORT" "$REPETITIONS" "$NETWORK_PROBE_SAMPLES"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || {
    echo "ports, repetitions, and sample counts must be positive integers" >&2
    exit 2
  }
done

client_ip="$(resolve_host "$CLIENT_HOST")"
healthy_ip="$(resolve_host "$HEALTHY_HOST")"
damaged_ip="$(resolve_host "$DAMAGED_HOST")"
client_allowed_ip="$(resolve_ipv4 "$CLIENT_HOST")"
[[ -n "$client_allowed_ip" ]] || {
  echo "could not resolve recovery client to IPv4: $CLIENT_HOST" >&2; exit 2
}
if [[ "$healthy_ip" == "$damaged_ip" ]]; then
  echo "healthy and damaged hosts must be distinct" >&2; exit 2
fi
client_target="$CLIENT_USER@$client_ip"
ssh_args=(-p "$CLIENT_PORT" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)
rsync_ssh="ssh -p $CLIENT_PORT -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"
if [[ -n "$SSH_KEY" ]]; then
  ssh_args+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
fi

prepare_remote_postgres() {
  local role="$1" host="$2" ssh_user="$3" db_port="$4" db_user="$5" db_name="$6"
  local install_dir="$7" runtime_root="$8"
  local target="$ssh_user@$host"
  local db_ssh_args=(-p "$DB_SSH_PORT" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)
  local db_rsync_ssh="ssh -p $DB_SSH_PORT -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"
  if [[ -n "$SSH_KEY" ]]; then
    db_ssh_args+=(-i "$SSH_KEY")
    db_rsync_ssh+=" -i $SSH_KEY"
  fi
  local remote_home resolved_root setup_dir
  remote_home="$(ssh "${db_ssh_args[@]}" "$target" 'printf %s "$HOME"')"
  resolved_root="$runtime_root"
  if [[ "$resolved_root" == "~/"* ]]; then
    resolved_root="$remote_home/${resolved_root:2}"
  fi
  setup_dir="$resolved_root/setup"
  ssh "${db_ssh_args[@]}" "$target" mkdir -p "$setup_dir"
  rsync -az -e "$db_rsync_ssh" \
    "$SCRIPT_DIR/prepare_split_host_postgres.sh" \
    "$REPO_ROOT/scripts/distributed/sql/raft_apply_ledger_schema.sql" \
    "$target:$setup_dir/"
  ssh "${db_ssh_args[@]}" "$target" bash "$setup_dir/prepare_split_host_postgres.sh" \
    --role "$role" --install-dir "$install_dir" --runtime-root "$resolved_root" \
    --ledger-sql "$setup_dir/raft_apply_ledger_schema.sql" --port "$db_port" \
    --db-name "$db_name" --db-user "$db_user" --allowed-client-ip "$client_allowed_ip"
}

if [[ "$PREPARE_POSTGRES" -eq 1 ]]; then
  echo "[split-host] preparing damaged PostgreSQL on $damaged_ip:$DAMAGED_PORT"
  prepare_remote_postgres damaged "$damaged_ip" "$DAMAGED_SSH_USER" \
    "$DAMAGED_PORT" "$DAMAGED_USER" "$DAMAGED_DB" \
    "$DAMAGED_INSTALL_DIR" "$DAMAGED_RUNTIME_ROOT"
  echo "[split-host] preparing healthy PostgreSQL on $healthy_ip:$HEALTHY_PORT"
  prepare_remote_postgres healthy "$healthy_ip" "$HEALTHY_SSH_USER" \
    "$HEALTHY_PORT" "$HEALTHY_USER" "$HEALTHY_DB" \
    "$HEALTHY_INSTALL_DIR" "$HEALTHY_RUNTIME_ROOT"
fi

client_home="$(ssh "${ssh_args[@]}" "$client_target" 'printf %s "$HOME"')"
if [[ -z "$CLIENT_ROOT" ]]; then
  CLIENT_ROOT="$client_home/ariabc_split_host_recovery"
elif [[ "$CLIENT_ROOT" == "~/"* ]]; then
  CLIENT_ROOT="$client_home/${CLIENT_ROOT:2}"
fi
run_tag="split_host_recovery_$(date -u +%Y%m%dT%H%M%SZ)_$$"
remote_source="$CLIENT_ROOT/work/$run_tag/recovery"
remote_results="$CLIENT_ROOT/results/$run_tag"

ssh "${ssh_args[@]}" "$client_target" mkdir -p "$remote_source" "$remote_results"
rsync -az --delete \
  --exclude='fetched/' --exclude='results/' --exclude='tests/' \
  --exclude='__pycache__/' --exclude='.pytest_cache/' --exclude='*.pyc' \
  -e "$rsync_ssh" "$SCRIPT_DIR/" "$client_target:$remote_source/"

remote_args=(
  "$CLIENT_PYTHON" "$remote_source" "$remote_results"
  "$healthy_ip" "$HEALTHY_PORT" "$HEALTHY_USER" "$HEALTHY_DB"
  "$damaged_ip" "$DAMAGED_PORT" "$DAMAGED_USER" "$DAMAGED_DB"
  "$SSLMODE" "$FANOUT" "$REPETITIONS" "$NETWORK_PROBE_SAMPLES"
  "$FULL_SCALE" "$TUPLE_COUNT" "$NO_WARMUP"
)

set +e
remote_output="$(ssh "${ssh_args[@]}" "$client_target" bash -s -- "${remote_args[@]}" <<'REMOTE' 2>&1
set -Eeuo pipefail
python_bin="$1"; source_dir="$2"; result_dir="$3"
healthy_host="$4"; healthy_port="$5"; healthy_user="$6"; healthy_db="$7"
damaged_host="$8"; damaged_port="$9"; damaged_user="${10}"; damaged_db="${11}"
sslmode="${12}"; fanout="${13}"; repetitions="${14}"; probe_samples="${15}"
full_scale="${16}"; tuple_count="${17}"; no_warmup="${18}"

"$python_bin" -c 'import psycopg' >/dev/null || {
  echo "client Python lacks psycopg: $python_bin" >&2; exit 1
}
healthy_dsn="host=$healthy_host port=$healthy_port dbname=$healthy_db user=$healthy_user sslmode=$sslmode connect_timeout=15"
damaged_dsn="host=$damaged_host port=$damaged_port dbname=$damaged_db user=$damaged_user sslmode=$sslmode connect_timeout=15"
args=(
  --healthy-dsn "$healthy_dsn"
  --damaged-dsn "$damaged_dsn"
  --healthy-placement "$healthy_host"
  --damaged-placement "$damaged_host"
  --client-placement "$(hostname -f 2>/dev/null || hostname)"
  --fanout "$fanout"
  --repetitions "$repetitions"
  --network-probe-samples "$probe_samples"
  --result-dir "$result_dir"
)
if [[ "$full_scale" == 1 ]]; then args+=(--full-scale); fi
if [[ -n "$tuple_count" ]]; then args+=(--tuple-count "$tuple_count"); fi
if [[ "$no_warmup" == 1 ]]; then args+=(--no-warmup); fi
ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 "$python_bin" \
  "$source_dir/run_split_host_dynamic_recovery.py" "${args[@]}"
REMOTE
)"
remote_status=$?
set -e
printf '%s\n' "$remote_output"
[[ "$remote_status" -eq 0 ]] || exit "$remote_status"

remote_artifact="$(printf '%s\n' "$remote_output" | sed -n 's/^SPLIT_HOST_RECOVERY_ARTIFACT=//p' | tail -1)"
[[ -n "$remote_artifact" ]] || {
  echo "split-host driver did not report an artifact path" >&2; exit 1
}
mkdir -p "$LOCAL_RESULTS/$run_tag"
rsync -az -e "$rsync_ssh" "$client_target:$remote_artifact/" "$LOCAL_RESULTS/$run_tag/"
git -C "$REPO_ROOT" rev-parse HEAD >"$LOCAL_RESULTS/$run_tag/source_git_head.txt" 2>/dev/null || true
git -C "$REPO_ROOT" diff -- src scripts/benchmark/recovery \
  >"$LOCAL_RESULTS/$run_tag/source_diff.patch" 2>/dev/null || true
printf 'SPLIT_HOST_RECOVERY_LOCAL_ARTIFACT=%s\n' "$LOCAL_RESULTS/$run_tag"
