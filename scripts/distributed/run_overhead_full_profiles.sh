#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PG_HOSTS="10.129.148.248,10.129.148.246,10.129.148.247"
RAFT_HOSTS="$PG_HOSTS"
RAFT_MEMBER_HOSTS="$PG_HOSTS"
RAFT_CLIENT_HOSTS="$PG_HOSTS"
PG_USERS="neel,neel,neel"
RAFT_USERS="neel,neel,neel"
GW_HOST="10.129.148.247"
GW_USER="neel"
SSH_USER="neel"
SSH_KEY="/home/neel/.ssh/id_rsa"
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"

# Keep knobs consistent across profiles for fair overhead comparison.
BENCH_PRESET="${PROFILE_BENCH_PRESET:-canonical}"
if [[ "$BENCH_PRESET" != "canonical" && "$BENCH_PRESET" != "tuned" ]]; then
  echo "ERROR: PROFILE_BENCH_PRESET must be canonical or tuned (got '$BENCH_PRESET')" >&2
  exit 2
fi
export PROFILE_PG_CONFIG_MODE="${PROFILE_PG_CONFIG_MODE:-$BENCH_PRESET}"
export PROFILE_AUTO_DET_MODE="${PROFILE_AUTO_DET_MODE:-1}"
export PROFILE_DET_RUNTIME_MODE="${PROFILE_DET_RUNTIME_MODE:-throughput}"
export PROFILE_GW_DET_RAW_SQL="${PROFILE_GW_DET_RAW_SQL:-0}"
export PROFILE_REQUIRE_DET_PARSER="${PROFILE_REQUIRE_DET_PARSER:-1}"
export PROFILE_GW_SUBMIT_MODE="${PROFILE_GW_SUBMIT_MODE:-event}"
export PROFILE_GW_DET_SUBMIT_PIPELINE="${PROFILE_GW_DET_SUBMIT_PIPELINE:-1}"
export PROFILE_SRV_PG_EXEC_MODE="${PROFILE_SRV_PG_EXEC_MODE:-event}"
# Strict policy: no single case should run longer than one minute.
export PROFILE_CASE_TIMEOUT_S=60
export PROFILE_GATEWAY_TIMEOUT_S=45
export ARIABC_MAX_CASE_TIMEOUT_S=60
export PROFILE_ABORT_ON_INVALID_CASE=0
export PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S="${PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S:-5}"
export PROFILE_POSTCHECK_CONVERGENCE_POLL_MS="${PROFILE_POSTCHECK_CONVERGENCE_POLL_MS:-100}"
export PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS:-2}"
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
BASE_LOG="$ROOT/scripts/bench_full_results/overhead_full_${RUN_TS}"
mkdir -p "$BASE_LOG"

LOCAL_INSTALL_DIR="/work/ARIABC/install"

sync_and_build() {
  echo "=== Step 0: Sync codebase + rebuild ariabc_pg ===" >&2

  # Build ariabc_pg locally first so synced repo has fresh binaries.
  local ariabc_build="$ROOT/ariabc_pg/build"
  if [[ -d "$ariabc_build" && -f "$ariabc_build/Makefile" ]]; then
    echo "[build] rebuilding ariabc_pg (cmake --build $ariabc_build)" >&2
    local jobs
    jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)"
    cmake --build "$ariabc_build" --parallel "$jobs" >&2
    echo "[build] ariabc_pg build complete" >&2
  else
    echo "[build] WARNING: ariabc_pg build dir not found at $ariabc_build — skipping local rebuild" >&2
  fi

  local rsync_ssh="ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -p $SSH_PORT"
  local IFS_OLD="$IFS"
  IFS=',' read -r -a host_arr <<< "$PG_HOSTS"
  IFS="$IFS_OLD"

  for host in "${host_arr[@]}"; do
    local h
    h="$(echo "$host" | xargs)"
    echo "[sync] $h: rsyncing repo -> $REMOTE_REPO_ROOT" >&2
    rsync -az --delete \
      --exclude='.git' \
      --exclude='.venv' \
      --exclude='.bench_tmp' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      --exclude='scripts/bench_full_results' \
      --exclude='scripts/bench_results' \
      -e "$rsync_ssh" \
      "$ROOT/" "$SSH_USER@$h:$REMOTE_REPO_ROOT/" >&2

    if [[ -d "$LOCAL_INSTALL_DIR" ]]; then
      echo "[sync] $h: rsyncing PG install -> $REMOTE_INSTALL_DIR" >&2
      rsync -az --delete \
        -e "$rsync_ssh" \
        "$LOCAL_INSTALL_DIR/" "$SSH_USER@$h:$REMOTE_INSTALL_DIR/" >&2
    fi

    echo "[sync] $h: done" >&2
  done

  echo "=== Step 0 complete ===" >&2
}

assert_profile_outputs() {
  local profile="$1"
  local out_dir="$2"
  local run_start_epoch="$3"
  local log="$4"

  if [[ ! -d "$out_dir" ]]; then
    echo "ERROR: profile '$profile' output dir missing: $out_dir" >&2
    return 1
  fi
  if [[ ! -f "$out_dir/summary.csv" ]]; then
    echo "ERROR: profile '$profile' missing summary.csv in: $out_dir" >&2
    return 1
  fi
  if [[ ! -f "$out_dir/results.csv" ]]; then
    echo "ERROR: profile '$profile' missing results.csv in: $out_dir" >&2
    return 1
  fi

  # Require at least one data row in summary and results.
  if [[ "$(wc -l < "$out_dir/summary.csv")" -le 1 ]]; then
    echo "ERROR: profile '$profile' summary.csv has no data rows" >&2
    return 1
  fi
  if [[ "$(wc -l < "$out_dir/results.csv")" -le 1 ]]; then
    echo "ERROR: profile '$profile' results.csv has no data rows" >&2
    return 1
  fi

  # Guard against stale directory reuse.
  local dir_mtime
  dir_mtime="$(stat -c %Y "$out_dir" 2>/dev/null || echo 0)"
  if [[ "$dir_mtime" -lt "$run_start_epoch" ]]; then
    echo "ERROR: profile '$profile' out dir appears stale (mtime older than run start): $out_dir" >&2
    return 1
  fi

  if ! rg -q "Benchmark completed\. Artifacts:" "$log"; then
    echo "ERROR: profile '$profile' log does not show benchmark completion marker" >&2
    return 1
  fi
}

run_one() {
  local profile="$1"
  local no_kafka="$2"
  local kafka_home="$3"
  local log="$BASE_LOG/${profile}.log"
  local modes="det"
  local run_start_epoch
  run_start_epoch="$(date +%s)"

  if [[ "$profile" == "base-no-raft-no-kafka" ]]; then
    modes="nondet"
  fi

  echo "=== Running profile: $profile ===" | tee "$log" >&2
  export PROFILE_COMPARISON_PROFILE="$profile"
  export PROFILE_NO_KAFKA="$no_kafka"
  export PROFILE_KAFKA_HOME="$kafka_home"
  export PROFILE_MODES="$modes"
  echo "Profile modes    : $modes" | tee -a "$log" >&2

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

sync_and_build

if ! BASE_DIR="$(run_one base-no-raft-no-kafka 1 "")"; then
  echo "ERROR: aborting overhead run at profile base-no-raft-no-kafka" >&2
  exit 1
fi
if ! RAFT_DIR="$(run_one raft-no-kafka 1 "")"; then
  echo "ERROR: aborting overhead run at profile raft-no-kafka" >&2
  exit 1
fi
if ! RK_DIR="$(run_one raft-kafka 0 "/tmp/kafka_2.13-3.7.0")"; then
  echo "ERROR: aborting overhead run at profile raft-kafka" >&2
  exit 1
fi

PLOT_OUT="$BASE_LOG/three_profile_tps_combined.png"
CSV_OUT="$BASE_LOG/overhead_comparison.csv"

python3 "$ROOT/scripts/distributed/plot_three_profiles_combined.py" \
  --base "$BASE_DIR/summary.csv" \
  --raft "$RAFT_DIR/summary.csv" \
  --raft-kafka "$RK_DIR/summary.csv" \
  --out "$PLOT_OUT"

python3 "$ROOT/scripts/distributed/compare_overhead_profiles.py" \
  --base "$BASE_DIR/summary.csv" \
  --raft "$RAFT_DIR/summary.csv" \
  --raft-kafka "$RK_DIR/summary.csv" \
  --out "$CSV_OUT"

echo "DONE"
echo "base_dir=$BASE_DIR"
echo "raft_dir=$RAFT_DIR"
echo "raft_kafka_dir=$RK_DIR"
echo "combined_plot=$PLOT_OUT"
echo "overhead_csv=$CSV_OUT"
