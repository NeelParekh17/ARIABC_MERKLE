#!/usr/bin/env bash
# Run only profiles 3 (kafka-only-no-raft) and 4 (raft-kafka), then combine with
# existing profile 1 and 2 results for full overhead analysis.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# --------------------------------------------------------------------------
# Reachable topology (same as run_overhead_full_profiles_reachable.sh)
# --------------------------------------------------------------------------
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

# Existing profile 1+2 result dirs (from the run that already completed).
# Override on command line if needed:  VANILLA_DIR=... BASE_DIR=... ./run_profiles_3_4_only.sh
VANILLA_DIR="${VANILLA_DIR:-/work/ARIABC/AriaBC/scripts/bench_full_results/distributed_20260405_103024}"
BASE_DIR="${BASE_DIR:-/work/ARIABC/AriaBC/scripts/bench_full_results/distributed_20260405_103857}"

if [[ ! -f "$VANILLA_DIR/summary.csv" ]]; then
  echo "ERROR: vanilla-pg summary.csv not found at: $VANILLA_DIR" >&2
  exit 1
fi
if [[ ! -f "$BASE_DIR/summary.csv" ]]; then
  echo "ERROR: base-no-raft summary.csv not found at: $BASE_DIR" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Global knobs (same as the main script)
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
BASE_LOG="$ROOT/scripts/bench_full_results/overhead_p3p4_${RUN_TS}"
mkdir -p "$BASE_LOG"

ensure_kafka_ready() {
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
    echo "ERROR: profile '$profile' out dir appears stale: $out_dir" >&2
    return 1
  fi
  if ! rg -q "Benchmark completed\. Artifacts:" "$log"; then
    echo "ERROR: profile '$profile' did not reach benchmark completion marker" >&2
    return 1
  fi
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

# --------------------------------------------------------------------------
# Profile 3: kafka-only-no-raft
# --------------------------------------------------------------------------
echo ">>> [3/4] kafka-only-no-raft"
if ! ensure_kafka_ready; then
  echo "ERROR: aborting before kafka-only-no-raft (Kafka unavailable)" >&2
  exit 1
fi
export PROFILE_WAIT_MAJORITY=1
export PROFILE_SERVER_BYPASS_RAFT=1
export PROFILE_GW_BROADCAST_ALL=1
export PROFILE_CASE_TIMEOUT_S=180
export PROFILE_GATEWAY_TIMEOUT_S=120
export ARIABC_MAX_CASE_TIMEOUT_S=180
if ! KAFKA_ONLY_DIR="$(run_one kafka-only-no-raft 0 "/tmp/kafka_2.13-3.7.0")"; then
  echo "ERROR: aborting at profile kafka-only-no-raft" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Profile 4: raft-kafka (full system)
# --------------------------------------------------------------------------
echo ">>> [4/4] raft-kafka (full system)"
if ! ensure_kafka_ready; then
  echo "ERROR: aborting before raft-kafka (Kafka unavailable)" >&2
  exit 1
fi
export PROFILE_WAIT_MAJORITY=1
export PROFILE_SERVER_BYPASS_RAFT=0
export PROFILE_GW_BROADCAST_ALL=0
export PROFILE_CASE_TIMEOUT_S=180
export PROFILE_GATEWAY_TIMEOUT_S=120
export ARIABC_MAX_CASE_TIMEOUT_S=180
if ! RK_DIR="$(run_one raft-kafka 0 "/tmp/kafka_2.13-3.7.0")"; then
  echo "ERROR: aborting at profile raft-kafka" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# Analysis (combine with profile 1+2 results)
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
