#!/usr/bin/env bash
set -euo pipefail

# Wrapper: run infra checks -> start remote Postgres -> smoke benchmark -> full benchmark

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

PG_HOSTS=""
PG_CLIENT_HOSTS=""
RAFT_HOSTS=""
RAFT_MEMBER_HOSTS=""
RAFT_CLIENT_HOSTS=""
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

# Postgres deterministic tuning (forwarded to start_remote_postgres_cluster.sh)
BCDB_WORKER_COUNTS="${PROFILE_BCDB_WORKER_COUNTS:-4,8,4}"
SHARED_BUFFERS="${PROFILE_SHARED_BUFFERS:-512MB,2GB,512MB}"
MAX_CONNECTIONS="${PROFILE_MAX_CONNECTIONS:-300}"
BCDB_SERIAL_GATE_MODE="${PROFILE_BCDB_SERIAL_GATE_MODE:-1}"
BCDB_RESULT_RING_SLOTS="${PROFILE_BCDB_RESULT_RING_SLOTS:-256}"

# Benchmark-side tuning (forwarded to smoke/full benchmark wrappers)
DB_CONN_POOL_CAP="${PROFILE_DB_CONN_POOL_CAP:-8}"
DB_CONN_POOL_SIZE="${PROFILE_DB_CONN_POOL_SIZE:-8}"
DET_WINDOW="${PROFILE_DET_WINDOW:-16}"
KAFKA_HOME="${PROFILE_KAFKA_HOME:-}"
KAFKA_BOOTSTRAP="${PROFILE_KAFKA_BOOTSTRAP:-localhost:9092}"
NO_KAFKA="${PROFILE_NO_KAFKA:-1}"
WAIT_MAJORITY="${PROFILE_WAIT_MAJORITY:-0}"
SERVER_BYPASS_RAFT="${PROFILE_SERVER_BYPASS_RAFT:-0}"
GW_BROADCAST_ALL="${PROFILE_GW_BROADCAST_ALL:-0}"
DET_PARALLEL_WORKERS="${PROFILE_DET_PARALLEL_WORKERS:-1}"
GATEWAY_TIMEOUT_S="${PROFILE_GATEWAY_TIMEOUT_S:-60}"
CASE_TIMEOUT_S="${PROFILE_CASE_TIMEOUT_S:-60}"
PRE_CLEANUP="${PROFILE_PRE_CLEANUP:-1}"
GW_SUBMIT_MODE="${PROFILE_GW_SUBMIT_MODE:-event}"
GW_DET_SUBMIT_PIPELINE="${PROFILE_GW_DET_SUBMIT_PIPELINE:-1}"
SRV_PG_EXEC_MODE="${PROFILE_SRV_PG_EXEC_MODE:-event}"
SKIP_DET_WINDOW_SWEEP="${PROFILE_SKIP_DET_WINDOW_SWEEP:-1}"
ABORT_ON_INVALID_CASE="${PROFILE_ABORT_ON_INVALID_CASE:-1}"
COMPARISON_PROFILE="${PROFILE_COMPARISON_PROFILE:-manual}"
POSTCHECK_CONVERGENCE_TIMEOUT_S="${PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S:-5}"
POSTCHECK_CONVERGENCE_POLL_MS="${PROFILE_POSTCHECK_CONVERGENCE_POLL_MS:-100}"
POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS:-2}"

RUN_FULL=1
SKIP_SMOKE="${PROFILE_SKIP_SMOKE:-0}"

usage() {
  cat <<'EOF_HELP'
Usage:
  preflight_then_run_full.sh \
    --pg-hosts <h1,h2,h3> \
    [--pg-client-hosts <h1,h2,h3>] \
    [--raft-hosts <r1,r2,r3> | --raft-host <r>] \
    [--raft-member-hosts <m1,m2,m3>] \
    [--raft-client-hosts <c1,c2,c3>] \
    [--gateway-host <g>] \
    [--ssh-user <default_user>] \
    [--pg-users <u1,u2,u3>] [--raft-users <u1,u2,u3> | --raft-user <u>] [--gateway-user <u>] \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </work/ARIABC/AriaBC>] \
    [--remote-install-dir </work/ARIABC/install>] \
    [--bcdb-worker-counts <w1,w2,w3>] [--shared-buffers <b1,b2,b3>] \
    [--max-connections <n>] [--bcdb-serial-gate-mode <0|1>] [--bcdb-result-ring-slots <n>] \
    [--db-conn-pool-cap <n>] [--db-conn-pool-size <n>] [--det-window <n>] \
    [--kafka-home <path>] \
    [--no-kafka 0|1] [--wait-majority 0|1] [--det-parallel-workers 0|1] \
    [--gateway-timeout-s <seconds>] [--case-timeout-s <seconds>] [--pre-cleanup 0|1] \
    [--gw-submit-mode blocking|event] [--gw-det-submit-pipeline 0|1] [--srv-pg-exec-mode threaded|event] \
    [--skip-det-window-sweep 0|1] [--abort-on-invalid-case 0|1] \
    [--postcheck-convergence-timeout-s <seconds>] [--postcheck-convergence-poll-ms <ms>] \
    [--postcheck-convergence-stable-rounds <n>] \
    [--comparison-profile manual|base-no-raft-no-kafka|raft-no-kafka|raft-kafka|vanilla-pg|kafka-only-no-raft] \
    [--skip-smoke 0|1] \
    [--skip-full]

This script executes:
1) preflight_cluster_checks.sh
2) start_remote_postgres_cluster.sh
3) preflight_smoke_benchmark.sh
4) run_distributed_benchmark.sh (unless --skip-full)
EOF_HELP
}

split_csv() {
  local csv="$1"
  local -n out_ref="$2"
  out_ref=()
  IFS=',' read -r -a raw <<< "$csv"
  for x in "${raw[@]}"; do
    x="${x#${x%%[![:space:]]*}}"
    x="${x%${x##*[![:space:]]}}"
    [[ -n "$x" ]] && out_ref+=("$x")
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-hosts) PG_HOSTS="${2:-}"; shift 2 ;;
    --pg-client-hosts) PG_CLIENT_HOSTS="${2:-}"; shift 2 ;;
    --raft-hosts) RAFT_HOSTS="${2:-}"; shift 2 ;;
    --raft-member-hosts) RAFT_MEMBER_HOSTS="${2:-}"; shift 2 ;;
    --raft-client-hosts) RAFT_CLIENT_HOSTS="${2:-}"; shift 2 ;;
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
    --bcdb-worker-counts) BCDB_WORKER_COUNTS="${2:-}"; shift 2 ;;
    --shared-buffers) SHARED_BUFFERS="${2:-}"; shift 2 ;;
    --max-connections) MAX_CONNECTIONS="${2:-300}"; shift 2 ;;
    --bcdb-serial-gate-mode) BCDB_SERIAL_GATE_MODE="${2:-1}"; shift 2 ;;
    --bcdb-result-ring-slots) BCDB_RESULT_RING_SLOTS="${2:-256}"; shift 2 ;;
    --db-conn-pool-cap) DB_CONN_POOL_CAP="${2:-8}"; shift 2 ;;
    --db-conn-pool-size) DB_CONN_POOL_SIZE="${2:-8}"; shift 2 ;;
    --det-window) DET_WINDOW="${2:-16}"; shift 2 ;;
    --kafka-home) KAFKA_HOME="${2:-}"; shift 2 ;;
    --kafka-bootstrap) KAFKA_BOOTSTRAP="${2:-localhost:9092}"; shift 2 ;;
    --no-kafka) NO_KAFKA="${2:-1}"; shift 2 ;;
    --wait-majority) WAIT_MAJORITY="${2:-0}"; shift 2 ;;
    --det-parallel-workers) DET_PARALLEL_WORKERS="${2:-1}"; shift 2 ;;
    --gateway-timeout-s) GATEWAY_TIMEOUT_S="${2:-60}"; shift 2 ;;
    --case-timeout-s) CASE_TIMEOUT_S="${2:-60}"; shift 2 ;;
    --pre-cleanup) PRE_CLEANUP="${2:-1}"; shift 2 ;;
    --gw-submit-mode) GW_SUBMIT_MODE="${2:-event}"; shift 2 ;;
    --gw-det-submit-pipeline) GW_DET_SUBMIT_PIPELINE="${2:-1}"; shift 2 ;;
    --srv-pg-exec-mode) SRV_PG_EXEC_MODE="${2:-event}"; shift 2 ;;
    --skip-det-window-sweep) SKIP_DET_WINDOW_SWEEP="${2:-1}"; shift 2 ;;
    --abort-on-invalid-case) ABORT_ON_INVALID_CASE="${2:-1}"; shift 2 ;;
    --postcheck-convergence-timeout-s) POSTCHECK_CONVERGENCE_TIMEOUT_S="${2:-5}"; shift 2 ;;
    --postcheck-convergence-poll-ms) POSTCHECK_CONVERGENCE_POLL_MS="${2:-100}"; shift 2 ;;
    --postcheck-convergence-stable-rounds) POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${2:-2}"; shift 2 ;;
    --comparison-profile) COMPARISON_PROFILE="${2:-manual}"; shift 2 ;;
    --skip-smoke) SKIP_SMOKE="${2:-1}"; shift 2 ;;
    --skip-full) RUN_FULL=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

ariabc_apply_comparison_profile_defaults "$COMPARISON_PROFILE"
ariabc_normalize_benchmark_flags

if [[ -z "$PG_HOSTS" ]]; then
  usage
  echo "ERROR: --pg-hosts is required." >&2
  exit 2
fi

if [[ -z "$PG_CLIENT_HOSTS" ]]; then
  PG_CLIENT_HOSTS="$PG_HOSTS"
fi

if [[ -z "$RAFT_HOSTS" ]]; then
  if [[ -n "$RAFT_HOST" ]]; then
    RAFT_HOSTS="$RAFT_HOST,$RAFT_HOST,$RAFT_HOST"
  else
    RAFT_HOSTS="$PG_HOSTS"
  fi
fi
if [[ -z "$RAFT_MEMBER_HOSTS" ]]; then
  RAFT_MEMBER_HOSTS="$RAFT_HOSTS"
fi
if [[ -z "$RAFT_CLIENT_HOSTS" ]]; then
  RAFT_CLIENT_HOSTS="$RAFT_HOSTS"
fi

if [[ -z "$GATEWAY_HOST" ]]; then
  declare -a _tmp_raft=()
  split_csv "$RAFT_HOSTS" _tmp_raft
  if [[ "${#_tmp_raft[@]}" -gt 0 ]]; then
    GATEWAY_HOST="${_tmp_raft[0]}"
  else
    echo "ERROR: could not derive gateway host from --raft-hosts." >&2
    exit 2
  fi
fi

if [[ -z "$SSH_USER" && -z "$PG_USERS" && -z "$RAFT_USERS" && -z "$RAFT_USER" && -z "$GATEWAY_USER" ]]; then
  echo "ERROR: provide --ssh-user default or explicit --pg-users/--raft-users/--gateway-user." >&2
  exit 2
fi

base_args=(
  --pg-hosts "$PG_HOSTS"
  --raft-hosts "$RAFT_HOSTS"
  --gateway-host "$GATEWAY_HOST"
  --ssh-port "$SSH_PORT"
  --remote-repo-root "$REMOTE_REPO_ROOT"
  --remote-install-dir "$REMOTE_INSTALL_DIR"
)

check_args=(
  --pg-hosts "$PG_HOSTS"
  --pg-client-hosts "$PG_CLIENT_HOSTS"
  --raft-hosts "$RAFT_HOSTS"
  --gateway-host "$GATEWAY_HOST"
  --ssh-port "$SSH_PORT"
  --remote-repo-root "$REMOTE_REPO_ROOT"
  --remote-install-dir "$REMOTE_INSTALL_DIR"
)

bench_common_args=(
  --pg-hosts "$PG_CLIENT_HOSTS"
  --raft-hosts "$RAFT_HOSTS"
  --raft-member-hosts "$RAFT_MEMBER_HOSTS"
  --raft-client-hosts "$RAFT_CLIENT_HOSTS"
  --gateway-host "$GATEWAY_HOST"
  --ssh-port "$SSH_PORT"
  --remote-repo-root "$REMOTE_REPO_ROOT"
  --remote-install-dir "$REMOTE_INSTALL_DIR"
  --db-conn-pool-cap "$DB_CONN_POOL_CAP"
  --db-conn-pool-size "$DB_CONN_POOL_SIZE"
  --det-window "$DET_WINDOW"
  --no-kafka "$NO_KAFKA"
  --wait-majority "$WAIT_MAJORITY"
  --server-bypass-raft "$SERVER_BYPASS_RAFT"
  --gw-broadcast-all "$GW_BROADCAST_ALL"
  --det-parallel-workers "$DET_PARALLEL_WORKERS"
  --comparison-profile "$COMPARISON_PROFILE"
)

if [[ -n "$KAFKA_HOME" ]]; then
  bench_common_args+=(--kafka-home "$KAFKA_HOME")
fi
bench_common_args+=(--kafka-bootstrap "$KAFKA_BOOTSTRAP")

smoke_bench_args=("${bench_common_args[@]}")

full_bench_args=(
  "${bench_common_args[@]}"
  --gateway-timeout-s "$GATEWAY_TIMEOUT_S"
  --case-timeout-s "$CASE_TIMEOUT_S"
  --pre-cleanup "$PRE_CLEANUP"
  --gw-submit-mode "$GW_SUBMIT_MODE"
  --gw-det-submit-pipeline "$GW_DET_SUBMIT_PIPELINE"
  --srv-pg-exec-mode "$SRV_PG_EXEC_MODE"
  --skip-det-window-sweep "$SKIP_DET_WINDOW_SWEEP"
  --abort-on-invalid-case "$ABORT_ON_INVALID_CASE"
  --postcheck-convergence-timeout-s "$POSTCHECK_CONVERGENCE_TIMEOUT_S"
  --postcheck-convergence-poll-ms "$POSTCHECK_CONVERGENCE_POLL_MS"
  --postcheck-convergence-stable-rounds "$POSTCHECK_CONVERGENCE_STABLE_ROUNDS"
)

pg_start_tune_args=(
  --bcdb-worker-counts "$BCDB_WORKER_COUNTS"
  --shared-buffers "$SHARED_BUFFERS"
  --max-connections "$MAX_CONNECTIONS"
  --bcdb-serial-gate-mode "$BCDB_SERIAL_GATE_MODE"
  --bcdb-result-ring-slots "$BCDB_RESULT_RING_SLOTS"
)

if [[ -n "$SSH_USER" ]]; then
  base_args+=(--ssh-user "$SSH_USER")
  check_args+=(--ssh-user "$SSH_USER")
  smoke_bench_args+=(--ssh-user "$SSH_USER")
  full_bench_args+=(--ssh-user "$SSH_USER")
fi
if [[ -n "$PG_USERS" ]]; then
  base_args+=(--pg-users "$PG_USERS")
  check_args+=(--pg-users "$PG_USERS")
  smoke_bench_args+=(--pg-users "$PG_USERS")
  full_bench_args+=(--pg-users "$PG_USERS")
fi
if [[ -n "$RAFT_USERS" ]]; then
  base_args+=(--raft-users "$RAFT_USERS")
  check_args+=(--raft-users "$RAFT_USERS")
  smoke_bench_args+=(--raft-users "$RAFT_USERS")
  full_bench_args+=(--raft-users "$RAFT_USERS")
fi
if [[ -n "$RAFT_USER" ]]; then
  base_args+=(--raft-user "$RAFT_USER")
  check_args+=(--raft-user "$RAFT_USER")
  smoke_bench_args+=(--raft-user "$RAFT_USER")
  full_bench_args+=(--raft-user "$RAFT_USER")
fi
if [[ -n "$GATEWAY_USER" ]]; then
  base_args+=(--gateway-user "$GATEWAY_USER")
  check_args+=(--gateway-user "$GATEWAY_USER")
  smoke_bench_args+=(--gateway-user "$GATEWAY_USER")
  full_bench_args+=(--gateway-user "$GATEWAY_USER")
fi
if [[ -n "$SSH_KEY" ]]; then
  base_args+=(--ssh-key "$SSH_KEY")
  check_args+=(--ssh-key "$SSH_KEY")
  smoke_bench_args+=(--ssh-key "$SSH_KEY")
  full_bench_args+=(--ssh-key "$SSH_KEY")
fi

echo "== Step 1/4: Infrastructure preflight checks =="
"$SCRIPT_DIR/preflight_cluster_checks.sh" "${check_args[@]}" || {
  echo "ERROR: Step 1/4 failed (preflight_cluster_checks.sh)." >&2
  exit 1
}

echo
echo "== Step 2/4: Start remote Postgres nodes =="
"$SCRIPT_DIR/start_remote_postgres_cluster.sh" "${base_args[@]}" "${pg_start_tune_args[@]}" || {
  echo "ERROR: Step 2/4 failed (start_remote_postgres_cluster.sh)." >&2
  exit 1
}

echo
echo "== Step 3/4: Smoke preflight benchmark =="
if [[ "$SKIP_SMOKE" == "1" ]]; then
  echo "Smoke benchmark skipped (--skip-smoke=1)."
else
  "$SCRIPT_DIR/preflight_smoke_benchmark.sh" "${smoke_bench_args[@]}" || {
    echo "ERROR: Step 3/4 failed (preflight_smoke_benchmark.sh)." >&2
    exit 1
  }
fi

if [[ "$RUN_FULL" == "1" ]]; then
  echo
  echo "== Step 4/4: Full distributed benchmark =="
  "$SCRIPT_DIR/run_distributed_benchmark.sh" "${full_bench_args[@]}" || {
    echo "ERROR: Step 4/4 failed (run_distributed_benchmark.sh)." >&2
    exit 1
  }
else
  echo
  echo "Full benchmark skipped (--skip-full)."
fi
