#!/usr/bin/env bash
# Run recovery from a client machine against PostgreSQL on a different host.
# Every benchmark SQL phase therefore includes the real client/server network path;
# network_probe.json records endpoint identity and baseline SQL RTT separately.
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: run_networked_recovery_benchmark.sh --client-host HOST --db-host HOST [options]

Required safety flag:
  --allow-destructive-dataset-reset
      Confirms that --db-name is a dedicated benchmark database. Recovery drops
      and recreates healthy/damaged schemas and Merkle indexes.

Connection and placement:
  --client-host HOST          machine that executes the Python recovery client
  --client-user USER          default: current user
  --client-port PORT          default: 22
  --client-root DIR           default: ~/ariabc_network_recovery
  --client-python PATH        default: /usr/bin/python3
  --ssh-key PATH
  --db-host HOST              PostgreSQL/Merkle server, distinct from client
  --db-port PORT              default: 5432
  --db-user USER              default: current user
  --db-name NAME              default: postgres
  --sslmode MODE              default: prefer
  --ssh-reverse-tunnel-port N
      Optional client-loopback port. The orchestrator opens an SSH reverse
      tunnel from that port to --db-host:--db-port. Use when PostgreSQL is not
      directly exposed; the artifact records transport_mode=ssh_reverse_tunnel.

Benchmark:
  --profile NAME              default: dynamic-size-scaling-k75-c300
  --tuple-count CSV           default: 1000000,5000000
  --fanout N                  dynamic logical fanout: 2,4,8,16,32 (default: 32)
  --repetitions N             default: 1
  --network-probe-samples N   default: 20
  --audit-mode full|skip      default: skip
  --artifact-mode summary|debug  default: summary
  --local-results DIR         default: scripts/bench_full_results/network_recovery

Authentication uses the normal libpq mechanisms on the client machine
(prefer ~/.pgpass); passwords are intentionally not accepted on the command line.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CLIENT_HOST=""
CLIENT_USER="${USER:-neel}"
CLIENT_PORT=22
CLIENT_ROOT=""
CLIENT_PYTHON=/usr/bin/python3
SSH_KEY=""
DB_HOST=""
DB_PORT=5432
DB_USER="${USER:-neel}"
DB_NAME=postgres
SSLMODE=prefer
REVERSE_TUNNEL_PORT=""
PROFILE=dynamic-size-scaling-k75-c300
TUPLE_COUNT=1000000,5000000
FANOUT=32
REPETITIONS=1
NETWORK_PROBE_SAMPLES=20
AUDIT_MODE=skip
ARTIFACT_MODE=summary
LOCAL_RESULTS="$REPO_ROOT/scripts/bench_full_results/network_recovery"
ALLOW_RESET=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-host) CLIENT_HOST="${2:?}"; shift 2 ;;
    --client-user) CLIENT_USER="${2:?}"; shift 2 ;;
    --client-port) CLIENT_PORT="${2:?}"; shift 2 ;;
    --client-root) CLIENT_ROOT="${2:?}"; shift 2 ;;
    --client-python) CLIENT_PYTHON="${2:?}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:?}"; shift 2 ;;
    --db-host) DB_HOST="${2:?}"; shift 2 ;;
    --db-port) DB_PORT="${2:?}"; shift 2 ;;
    --db-user) DB_USER="${2:?}"; shift 2 ;;
    --db-name) DB_NAME="${2:?}"; shift 2 ;;
    --sslmode) SSLMODE="${2:?}"; shift 2 ;;
    --ssh-reverse-tunnel-port) REVERSE_TUNNEL_PORT="${2:?}"; shift 2 ;;
    --profile) PROFILE="${2:?}"; shift 2 ;;
    --tuple-count) TUPLE_COUNT="${2:?}"; shift 2 ;;
    --fanout) FANOUT="${2:?}"; shift 2 ;;
    --repetitions) REPETITIONS="${2:?}"; shift 2 ;;
    --network-probe-samples) NETWORK_PROBE_SAMPLES="${2:?}"; shift 2 ;;
    --audit-mode) AUDIT_MODE="${2:?}"; shift 2 ;;
    --artifact-mode) ARTIFACT_MODE="${2:?}"; shift 2 ;;
    --local-results) LOCAL_RESULTS="${2:?}"; shift 2 ;;
    --allow-destructive-dataset-reset) ALLOW_RESET=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$CLIENT_HOST" ]] || { echo "--client-host is required" >&2; exit 2; }
[[ -n "$DB_HOST" ]] || { echo "--db-host is required" >&2; exit 2; }
[[ "$ALLOW_RESET" -eq 1 ]] || {
  echo "refusing destructive benchmark without --allow-destructive-dataset-reset" >&2
  exit 2
}
[[ "$FANOUT" =~ ^(2|4|8|16|32)$ ]] || {
  echo "--fanout must be one of 2,4,8,16,32" >&2; exit 2;
}
for numeric in "$CLIENT_PORT" "$DB_PORT" "$REPETITIONS" "$NETWORK_PROBE_SAMPLES"; do
  [[ "$numeric" =~ ^[1-9][0-9]*$ ]] || { echo "ports/counts must be positive integers" >&2; exit 2; }
done
if [[ -n "$REVERSE_TUNNEL_PORT" && ! "$REVERSE_TUNNEL_PORT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--ssh-reverse-tunnel-port must be a positive integer" >&2; exit 2
fi
[[ "$AUDIT_MODE" == full || "$AUDIT_MODE" == skip ]] || {
  echo "--audit-mode must be full or skip" >&2; exit 2;
}
[[ "$ARTIFACT_MODE" == summary || "$ARTIFACT_MODE" == debug ]] || {
  echo "--artifact-mode must be summary or debug" >&2; exit 2;
}

ssh_args=(-p "$CLIENT_PORT" -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=15)
rsync_ssh="ssh -p $CLIENT_PORT -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=15"
if [[ -n "$SSH_KEY" ]]; then
  ssh_args+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
fi
client_target="$CLIENT_USER@$CLIENT_HOST"
client_home="$(ssh "${ssh_args[@]}" "$client_target" 'printf %s "$HOME"')"
if [[ -z "$CLIENT_ROOT" ]]; then
  CLIENT_ROOT="$client_home/ariabc_network_recovery"
elif [[ "$CLIENT_ROOT" == "~/"* ]]; then
  CLIENT_ROOT="$client_home/${CLIENT_ROOT#~/}"
fi
run_tag="network_recovery_$(date -u +%Y%m%dT%H%M%SZ)_$$"

remote_base="$CLIENT_ROOT/work/$run_tag"
remote_source="$remote_base/recovery"
remote_results="$CLIENT_ROOT/results/$run_tag"

ssh "${ssh_args[@]}" "$client_target" mkdir -p "$remote_source" "$remote_results"
rsync -az --delete \
  --exclude='fetched/' --exclude='results/' --exclude='tests/' \
  --exclude='__pycache__/' --exclude='.pytest_cache/' --exclude='*.pyc' \
  -e "$rsync_ssh" "$SCRIPT_DIR/" "$client_target:$remote_source/"

transport_mode=direct_tcp
effective_db_host="$DB_HOST"
effective_db_port="$DB_PORT"
tunnel_pid=""
cleanup_tunnel() {
  [[ -z "$tunnel_pid" ]] || kill "$tunnel_pid" 2>/dev/null || true
}
trap cleanup_tunnel EXIT
if [[ -n "$REVERSE_TUNNEL_PORT" ]]; then
  transport_mode=ssh_reverse_tunnel
  effective_db_host=127.0.0.1
  effective_db_port="$REVERSE_TUNNEL_PORT"
  ssh "${ssh_args[@]}" -o ExitOnForwardFailure=yes -N \
    -R "127.0.0.1:$REVERSE_TUNNEL_PORT:$DB_HOST:$DB_PORT" \
    "$client_target" &
  tunnel_pid=$!
  for _ in $(seq 1 30); do
    if ssh "${ssh_args[@]}" "$client_target" \
         "nc -z 127.0.0.1 '$REVERSE_TUNNEL_PORT'" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  ssh "${ssh_args[@]}" "$client_target" \
    "nc -z 127.0.0.1 '$REVERSE_TUNNEL_PORT'" >/dev/null 2>&1 || {
      echo "SSH reverse tunnel did not become reachable" >&2; exit 1;
    }
fi

set +e
remote_output="$({
  ssh "${ssh_args[@]}" "$client_target" bash -s -- \
    "$CLIENT_PYTHON" "$remote_source" "$remote_results" \
    "$effective_db_host" "$effective_db_port" "$DB_USER" "$DB_NAME" "$SSLMODE" \
    "$PROFILE" "$TUPLE_COUNT" "$FANOUT" "$REPETITIONS" \
    "$NETWORK_PROBE_SAMPLES" "$AUDIT_MODE" "$ARTIFACT_MODE" \
    "$transport_mode" "$DB_HOST" "$DB_PORT" "$CLIENT_HOST" <<'REMOTE'
set -Eeuo pipefail
python_bin="$1"; source_dir="$2"; result_root="$3"
db_host="$4"; db_port="$5"; db_user="$6"; db_name="$7"; sslmode="$8"
profile="$9"; tuple_count="${10}"; fanout="${11}"; repetitions="${12}"
probe_samples="${13}"; audit_mode="${14}"; artifact_mode="${15}"
transport_mode="${16}"; logical_db_host="${17}"; logical_db_port="${18}"
logical_client_host="${19}"

"$python_bin" -c 'import psycopg' >/dev/null || {
  echo "client Python lacks psycopg: $python_bin" >&2; exit 1;
}
dsn="host=$db_host port=$db_port dbname=$db_name user=$db_user sslmode=$sslmode connect_timeout=15"
ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 "$python_bin" \
  "$source_dir/run_merkle_recovery_benchmark.py" \
  --dsn "$dsn" \
  --profile "$profile" \
  --tuple-count "$tuple_count" \
  --fanout "$fanout" \
  --repetitions "$repetitions" \
  --network-probe-samples "$probe_samples" \
  --audit-mode "$audit_mode" \
  --artifact-mode "$artifact_mode" \
  --result-dir "$result_root" \
  --scratch-dir "$result_root"

result_path="$(find "$result_root" -mindepth 1 -maxdepth 1 -type d ! -name 'tmp_*' -print -quit)"
[[ -n "$result_path" && -f "$result_path/network_probe.json" ]] || {
  echo "benchmark completed without network_probe.json" >&2; exit 1;
}
"$python_bin" - "$result_path/network_probe.json" "$transport_mode" \
  "$logical_db_host" "$logical_db_port" "$logical_client_host" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
probe = json.loads(path.read_text())
mode, logical_db_host, logical_db_port, logical_client_host = sys.argv[2:]
probe.update({
    "transport_mode": mode,
    "logical_db_host": logical_db_host,
    "logical_db_port": int(logical_db_port),
    "logical_client_host": logical_client_host,
})
path.write_text(json.dumps(probe, indent=2) + "\n")
server = probe.get("server_addr")
client = probe.get("client_addr_seen_by_server")
if not server or not client:
    raise SystemExit("network proof failed: PostgreSQL did not report TCP endpoint addresses")
if mode == "direct_tcp" and server == client:
    raise SystemExit(f"network proof failed: server and client addresses are both {server}")
if mode == "ssh_reverse_tunnel" and logical_db_host == logical_client_host:
    raise SystemExit("network proof failed: tunnel DB and client hosts are identical")
print(f"NETWORK_RECOVERY_PASS transport={mode} server={server} client={client} median_rtt_ms={probe['round_trip_median_ms']:.3f} p95_rtt_ms={probe['round_trip_p95_ms']:.3f}")
PY
printf 'RESULT_PATH=%s\n' "$result_path"
REMOTE
} 2>&1)"
remote_status=$?
set -e
printf '%s\n' "$remote_output"
[[ "$remote_status" -eq 0 ]] || exit "$remote_status"
remote_result_path="$(printf '%s\n' "$remote_output" | sed -n 's/^RESULT_PATH=//p' | tail -1)"
[[ -n "$remote_result_path" ]] || { echo "could not determine remote result path" >&2; exit 1; }

mkdir -p "$LOCAL_RESULTS/$run_tag"
rsync -az -e "$rsync_ssh" "$client_target:$remote_result_path/" "$LOCAL_RESULTS/$run_tag/"
git -C "$REPO_ROOT" rev-parse HEAD \
  >"$LOCAL_RESULTS/$run_tag/source_git_head.txt" 2>/dev/null || true
git -C "$REPO_ROOT" diff -- src scripts/benchmark/recovery scripts/distributed \
  >"$LOCAL_RESULTS/$run_tag/source_diff.patch" 2>/dev/null || true
(cd "$SCRIPT_DIR" &&
  find . -type f ! -path './fetched/*' ! -path './results/*' \
    ! -path './tests/*' ! -path '*/__pycache__/*' ! -name '*.pyc' -print0 |
  sort -z | xargs -0 sha256sum) \
  >"$LOCAL_RESULTS/$run_tag/recovery_source_files.sha256"
printf 'NETWORK_RECOVERY_ARTIFACT=%s\n' "$LOCAL_RESULTS/$run_tag"
