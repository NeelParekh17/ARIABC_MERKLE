#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
source "$ROOT/scripts/distributed/benchmark_defaults.sh"

# Reachable topology: 10.129.148.248 is down.
# Node1=10.129.148.236 is LAN-local to gateway for fast nondet connections.
# Node2=10.129.148.246, Node3=10.129.148.236 (co-located).
# Gateway=10.129.148.236.
PG_HOSTS="10.129.148.236,10.129.148.246,10.129.148.236"
RAFT_HOSTS="$PG_HOSTS"
RAFT_MEMBER_HOSTS="$PG_HOSTS"
RAFT_CLIENT_HOSTS="$PG_HOSTS"
PG_USERS="neel,neel,neel"
RAFT_USERS="neel,neel,neel"
GW_HOST="10.129.148.236"
GW_USER="neel"
SSH_USER="neel"
SSH_KEY="/home/neel/.ssh/id_rsa"
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
SINGLE_MATRIX_SCRIPT="$ROOT/scripts/distributed/run_single_machine_matrix_all_nodes.sh"
SINGLE_MIN_AGG_SCRIPT="$ROOT/scripts/distributed/aggregate_single_machine_min_profile.py"
FULL_THREADS="${PROFILE_THREADS:-$ARIABC_DEFAULT_FULL_THREADS}"
FULL_RUNS="${PROFILE_RUNS:-3}"
WORKLOADS="${PROFILE_WORKLOADS:-ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
RATES="${PROFILE_RATES:-0}"

# --------------------------------------------------------------------------
# Global knobs — kept constant across all profiles for fair comparison.
# --------------------------------------------------------------------------
BENCH_PRESET="${PROFILE_BENCH_PRESET:-canonical}"
if [[ "$BENCH_PRESET" != "canonical" && "$BENCH_PRESET" != "tuned" ]]; then
  echo "ERROR: PROFILE_BENCH_PRESET must be canonical or tuned (got '$BENCH_PRESET')" >&2
  exit 2
fi
export ARIABC_SSH_USER_MAP="10.129.148.246=neel"
export PROFILE_PG_CONFIG_MODE="${PROFILE_PG_CONFIG_MODE:-$BENCH_PRESET}"
export PROFILE_AUTO_DET_MODE="${PROFILE_AUTO_DET_MODE:-1}"
export PROFILE_DET_RUNTIME_MODE="${PROFILE_DET_RUNTIME_MODE:-throughput}"
export PROFILE_GW_DET_RAW_SQL="${PROFILE_GW_DET_RAW_SQL:-0}"
export PROFILE_REQUIRE_DET_PARSER="${PROFILE_REQUIRE_DET_PARSER:-1}"
export PROFILE_GW_SUBMIT_MODE="${PROFILE_GW_SUBMIT_MODE:-event}"
export PROFILE_GW_DET_SUBMIT_PIPELINE="${PROFILE_GW_DET_SUBMIT_PIPELINE:-1}"
export PROFILE_SRV_PG_EXEC_MODE="${PROFILE_SRV_PG_EXEC_MODE:-event}"
export PROFILE_ABORT_ON_INVALID_CASE=0
export PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S="${PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S:-5}"
export PROFILE_POSTCHECK_CONVERGENCE_POLL_MS="${PROFILE_POSTCHECK_CONVERGENCE_POLL_MS:-100}"
export PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS:-2}"
export PROFILE_SKIP_SMOKE=1
# Default: no raft bypass, no broadcast
export PROFILE_SERVER_BYPASS_RAFT=0
export PROFILE_GW_BROADCAST_ALL=0
export PROFILE_WAIT_MAJORITY=0
if [[ "$BENCH_PRESET" == "canonical" ]]; then
  export PROFILE_DB_CONN_POOL_SIZE="${PROFILE_DB_CONN_POOL_SIZE:-8}"
  export PROFILE_DB_CONN_POOL_CAP="${PROFILE_DB_CONN_POOL_CAP:-8}"
  export PROFILE_DET_WINDOW="${PROFILE_DET_WINDOW:-64}"
else
  export PROFILE_DB_CONN_POOL_SIZE="${PROFILE_DB_CONN_POOL_SIZE:-16}"
  export PROFILE_DB_CONN_POOL_CAP="${PROFILE_DB_CONN_POOL_CAP:-16}"
  export PROFILE_DET_WINDOW="${PROFILE_DET_WINDOW:-128}"
fi

RUN_TS="$(date +%Y%m%d_%H%M%S)"
BASE_LOG="$ROOT/scripts/bench_full_results/overhead_full_reachable_${RUN_TS}"
mkdir -p "$BASE_LOG"

ensure_kafka_ready() {
  # Kafka runs on the gateway host (GW_HOST) in distributed mode.
  # SSH in and check/start it.
  local kafka_home="/tmp/kafka_2.13-3.7.0"
  echo "Ensuring Kafka is ready on gateway $GW_USER@$GW_HOST ..."
  local remote_cmd
  remote_cmd=$(cat <<'REMOTE_EOF'
set -euo pipefail
KAFKA_HOME="/tmp/kafka_2.13-3.7.0"
TOPICS_SH="$KAFKA_HOME/bin/kafka-topics.sh"
STORAGE_SH="$KAFKA_HOME/bin/kafka-storage.sh"
SERVER_SH="$KAFKA_HOME/bin/kafka-server-start.sh"
SERVER_PROPS="$KAFKA_HOME/config/kraft/server.properties"

if [[ ! -x "$TOPICS_SH" || ! -x "$STORAGE_SH" || ! -x "$SERVER_SH" || ! -f "$SERVER_PROPS" ]]; then
  echo "ERROR: Kafka runtime missing under $KAFKA_HOME" >&2
  exit 1
fi

if "$TOPICS_SH" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  echo "Kafka already running at localhost:9092"
  exit 0
fi

cluster_id="$("$STORAGE_SH" random-uuid 2>/dev/null | tail -n 1 | tr -d '\r')"
if [[ -z "$cluster_id" ]]; then
  echo "ERROR: failed to generate Kafka cluster ID" >&2
  exit 1
fi

"$STORAGE_SH" format -t "$cluster_id" -c "$SERVER_PROPS" --ignore-formatted >/dev/null 2>&1 || true
"$SERVER_SH" -daemon "$SERVER_PROPS"

for i in $(seq 1 60); do
  if "$TOPICS_SH" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka broker ready at localhost:9092 (after ${i}s)"
    exit 0
  fi
  sleep 1
done

echo "ERROR: Kafka broker did not become ready at localhost:9092" >&2
exit 1
REMOTE_EOF
)
  # Use bash (non-login) with stdin to avoid login-shell profile noise.
  ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT" \
      "$GW_USER@$GW_HOST" bash <<< "$remote_cmd"
}

assert_profile_outputs() {
  local profile="$1"
  local out_dir="$2"
  local run_start_epoch="$3"
  local log="$4"

  if [[ ! -d "$out_dir" || ! -f "$out_dir/summary.csv" || ! -f "$out_dir/results.csv" ]]; then
    echo "ERROR: profile '$profile' missing required artifacts in: $out_dir" >&2
    return 1
  fi
  if [[ "$(wc -l < "$out_dir/summary.csv")" -le 1 || "$(wc -l < "$out_dir/results.csv")" -le 1 ]]; then
    echo "ERROR: profile '$profile' produced empty summary/results CSV" >&2
    return 1
  fi

  local dir_mtime
  dir_mtime="$(stat -c %Y "$out_dir" 2>/dev/null || echo 0)"
  if [[ "$dir_mtime" -lt "$run_start_epoch" ]]; then
    echo "ERROR: profile '$profile' out dir appears stale (mtime older than run start): $out_dir" >&2
    return 1
  fi
  if ! rg -q "Benchmark completed\. Artifacts:" "$log"; then
    echo "ERROR: profile '$profile' did not reach benchmark completion marker" >&2
    return 1
  fi
}

assert_local_profile_outputs() {
  local profile="$1"
  local out_dir="$2"
  local run_start_epoch="$3"

  if [[ ! -d "$out_dir" || ! -f "$out_dir/summary.csv" || ! -f "$out_dir/results.csv" ]]; then
    echo "ERROR: local single-machine aggregate for '$profile' missing required artifacts in: $out_dir" >&2
    return 1
  fi
  if [[ "$(wc -l < "$out_dir/summary.csv")" -le 1 || "$(wc -l < "$out_dir/results.csv")" -le 1 ]]; then
    echo "ERROR: local single-machine aggregate for '$profile' produced empty summary/results CSV" >&2
    return 1
  fi
  local dir_mtime
  dir_mtime="$(stat -c %Y "$out_dir" 2>/dev/null || echo 0)"
  if [[ "$dir_mtime" -lt "$run_start_epoch" ]]; then
    echo "ERROR: local single-machine aggregate for '$profile' appears stale: $out_dir" >&2
    return 1
  fi
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

build_unique_node_csv() {
  local hosts_csv="$1"
  local users_csv="$2"
  local -n out_csv_ref="$3"
  local -n out_count_ref="$4"

  local -a hosts=()
  local -a users=()
  local -a nodes=()
  local i
  declare -A seen_host=()
  split_csv "$hosts_csv" hosts
  split_csv "$users_csv" users
  if [[ "${#hosts[@]}" -ne "${#users[@]}" ]]; then
    echo "ERROR: hosts/users length mismatch while building single-machine node list" >&2
    exit 1
  fi
  for i in "${!hosts[@]}"; do
    local h="${hosts[$i]}"
    local u="${users[$i]}"
    if [[ -z "$h" || -z "$u" ]]; then
      continue
    fi
    if [[ -n "${seen_host[$h]:-}" ]]; then
      continue
    fi
    seen_host["$h"]=1
    nodes+=("$u@$h")
  done
  if [[ "${#nodes[@]}" -eq 0 ]]; then
    echo "ERROR: no nodes derived for single-machine profile runs" >&2
    exit 1
  fi
  local IFS=,
  out_csv_ref="${nodes[*]}"
  out_count_ref="${#nodes[@]}"
}

ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT" "$GW_USER@$GW_HOST" \
  "mkdir -p '$REMOTE_REPO_ROOT/scripts'"

run_one() {
  local profile="$1"
  local no_kafka="$2"
  local kafka_home="$3"
  local modes="${4:-det}"
  local log="$BASE_LOG/${profile}.log"
  local run_start_epoch
  run_start_epoch="$(date +%s)"

  echo "=== Running profile: $profile (modes=$modes) ===" | tee "$log" >&2
  export PROFILE_COMPARISON_PROFILE="$profile"
  export PROFILE_NO_KAFKA="$no_kafka"
  export PROFILE_KAFKA_HOME="$kafka_home"
  export PROFILE_MODES="$modes"
  echo "Profile modes    : $modes" | tee -a "$log" >&2

  rsync -a -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -p $SSH_PORT" \
    "$ROOT/scripts/bench_nuraft_kafka_matrix.py" \
    "$GW_USER@$GW_HOST:$REMOTE_REPO_ROOT/scripts/bench_nuraft_kafka_matrix.py" \
    >>"$log" 2>&1

  /usr/bin/time -p "$ROOT/scripts/distributed/preflight_then_run_full.sh" \
    --pg-hosts "$PG_HOSTS" \
    --pg-client-hosts "$PG_HOSTS" \
    --raft-hosts "$RAFT_HOSTS" \
    --raft-member-hosts "$RAFT_MEMBER_HOSTS" \
    --raft-client-hosts "$RAFT_CLIENT_HOSTS" \
    --pg-users "$PG_USERS" \
    --raft-users "$RAFT_USERS" \
    --gateway-host "$GW_HOST" \
    --gateway-user "$GW_USER" \
    --ssh-user "$SSH_USER" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --remote-repo-root "$REMOTE_REPO_ROOT" \
    --remote-install-dir "$REMOTE_INSTALL_DIR" \
    --bcdb-worker-counts 4,8,4 \
    --shared-buffers 512MB,2GB,512MB \
    --max-connections 300 \
    --bcdb-serial-gate-mode 1 \
    --bcdb-result-ring-slots 256 \
    --db-conn-pool-cap 4 \
    --db-conn-pool-size 4 \
    --det-window 16 \
    --comparison-profile "$profile" \
    |& tee -a "$log" >&2

  local path
  path="$(rg -o 'Local out dir\s+:\s+.*' "$log" | tail -n 1 | sed -E 's/^.*:\s+//' | tr -d '\r')"
  if [[ -z "$path" ]]; then
    echo "ERROR: profile '$profile' failed - no Local out dir found in log" >&2
    tail -n 40 "$log" >&2 || true
    return 1
  fi

  if ! assert_profile_outputs "$profile" "$path" "$run_start_epoch" "$log"; then
    return 1
  fi
  echo "$path"
}

run_single_node_profile_min() {
  local profile="$1"
  local modes="$2"
  local log="$BASE_LOG/${profile}.single_machine.log"
  local run_start_epoch
  run_start_epoch="$(date +%s)"

  echo "=== Running profile: $profile (single-machine per-node, modes=$modes) ===" | tee "$log" >&2
  echo "Nodes (no gateway path): $SINGLE_NODE_NODES_CSV" | tee -a "$log" >&2
  echo "Threads/Runs            : $FULL_THREADS / $FULL_RUNS" | tee -a "$log" >&2
  echo "Workloads/Rates         : $WORKLOADS / $RATES" | tee -a "$log" >&2

  /usr/bin/time -p "$SINGLE_MATRIX_SCRIPT" \
    --nodes "$SINGLE_NODE_NODES_CSV" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --remote-repo-root "$REMOTE_REPO_ROOT" \
    --remote-install-dir "$REMOTE_INSTALL_DIR" \
    --modes "$modes" \
    --threads "$FULL_THREADS" \
    --runs "$FULL_RUNS" \
    --workloads "$WORKLOADS" \
    --rates "$RATES" \
    |& tee -a "$log" >&2

  local single_root
  single_root="$(grep -o 'Collected outputs:\s*.*' "$log" | tail -n 1 | sed -E 's/^.*:\s*//' | tr -d '\r')"
  if [[ -z "$single_root" || ! -d "$single_root" ]]; then
    echo "ERROR: profile '$profile' single-machine run did not produce a valid output root" >&2
    tail -n 60 "$log" >&2 || true
    return 1
  fi

  local out_dir="$BASE_LOG/${profile}_single_machine_min"
  rm -rf "$out_dir"
  mkdir -p "$out_dir"
  python3 "$SINGLE_MIN_AGG_SCRIPT" \
    --input-root "$single_root" \
    --profile "$profile" \
    --out-dir "$out_dir" \
    --min-nodes "$SINGLE_NODE_COUNT" \
    |& tee -a "$log" >&2

  if ! assert_local_profile_outputs "$profile" "$out_dir" "$run_start_epoch"; then
    return 1
  fi
  echo "$out_dir"
}

SINGLE_NODE_NODES_CSV=""
SINGLE_NODE_COUNT=0
build_unique_node_csv "$PG_HOSTS" "$PG_USERS" SINGLE_NODE_NODES_CSV SINGLE_NODE_COUNT

# --------------------------------------------------------------------------
# Profile 1: vanilla-pg (single-machine per node; no gateway, no kafka)
#   Config 1: BCDB postgres in nondet/pg mode (DB_TYPE=2, plain SQL, no BCDB
#   sequence numbers). Single-node baseline — measures raw SQL overhead.
#   mode=nondet; no kafka, no raft.
# --------------------------------------------------------------------------
echo ">>> [1/4] vanilla-pg"
if ! VANILLA_DIR="$(run_single_node_profile_min vanilla-pg nondet)"; then
  echo "ERROR: aborting overhead run at profile vanilla-pg" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Profile 2: base-no-raft-no-kafka (single-machine per node; no gateway, no kafka)
#   Config 2: BCDB deterministic mode on single node. No Raft, no Kafka.
#   Measures cost of BCDB deterministic execution vs vanilla PG.
# --------------------------------------------------------------------------
echo ">>> [2/4] base-no-raft-no-kafka (det-only)"
if ! BASE_DIR="$(run_single_node_profile_min base-no-raft-no-kafka det)"; then
  echo "ERROR: aborting overhead run at profile base-no-raft-no-kafka" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Profile 3: kafka-only-no-raft
#   Config 4: gateway broadcasts to all replicas in same order (no Raft),
#   each replica executes deterministically and publishes to Kafka.
#   Gateway waits for threshold-matching Kafka results (wait_majority=1).
#   Isolates Kafka overhead from Raft consensus cost.
# --------------------------------------------------------------------------
echo ">>> [3/4] kafka-only-no-raft"
if ! ensure_kafka_ready; then
  echo "ERROR: aborting overhead run before kafka-only-no-raft (Kafka unavailable)" >&2
  exit 1
fi
export PROFILE_WAIT_MAJORITY=1
export PROFILE_SERVER_BYPASS_RAFT=1
export PROFILE_GW_BROADCAST_ALL=1
export PROFILE_CASE_TIMEOUT_S=180
export PROFILE_GATEWAY_TIMEOUT_S=120
export ARIABC_MAX_CASE_TIMEOUT_S=180
if ! KAFKA_ONLY_DIR="$(run_one kafka-only-no-raft 0 "/tmp/kafka_2.13-3.7.0")"; then
  echo "ERROR: aborting overhead run at profile kafka-only-no-raft" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Profile 4: raft-kafka (full system)
#   Config 5: Raft + deterministic execution + Kafka. Gateway waits for
#   threshold-matching Kafka results (wait_majority=1). This is the headline
#   number — overhead = Raft consensus + execution + Kafka pub/sub.
# --------------------------------------------------------------------------
echo ">>> [4/4] raft-kafka (full system)"
if ! ensure_kafka_ready; then
  echo "ERROR: aborting overhead run before raft-kafka (Kafka unavailable)" >&2
  exit 1
fi
export PROFILE_WAIT_MAJORITY=1
export PROFILE_SERVER_BYPASS_RAFT=0
export PROFILE_GW_BROADCAST_ALL=0
export PROFILE_CASE_TIMEOUT_S=180
export PROFILE_GATEWAY_TIMEOUT_S=120
export ARIABC_MAX_CASE_TIMEOUT_S=180
if ! RK_DIR="$(run_one raft-kafka 0 "/tmp/kafka_2.13-3.7.0")"; then
  echo "ERROR: aborting overhead run at profile raft-kafka" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Analysis
# --------------------------------------------------------------------------
PLOT_OUT="$BASE_LOG/four_profile_tps_combined.png"
CSV_OUT="$BASE_LOG/overhead_comparison.csv"

python3 "$ROOT/scripts/distributed/plot_overhead_profiles_combined.py" \
  --vanilla-pg  "$VANILLA_DIR/summary.csv" \
  --base        "$BASE_DIR/summary.csv" \
  --kafka-only  "$KAFKA_ONLY_DIR/summary.csv" \
  --raft-kafka  "$RK_DIR/summary.csv" \
  --out "$PLOT_OUT"

python3 "$ROOT/scripts/distributed/compare_overhead_profiles.py" \
  --vanilla-pg  "$VANILLA_DIR/summary.csv" \
  --base        "$BASE_DIR/summary.csv" \
  --kafka-only  "$KAFKA_ONLY_DIR/summary.csv" \
  --raft-kafka  "$RK_DIR/summary.csv" \
  --out "$CSV_OUT"

echo "DONE"
echo "run_root=$BASE_LOG"
echo "vanilla_pg_dir=$VANILLA_DIR"
echo "base_dir=$BASE_DIR"
echo "kafka_only_dir=$KAFKA_ONLY_DIR"
echo "raft_kafka_dir=$RK_DIR"
echo "combined_plot=$PLOT_OUT"
echo "overhead_csv=$CSV_OUT"
