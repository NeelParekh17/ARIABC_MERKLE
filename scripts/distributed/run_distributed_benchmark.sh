#!/usr/bin/env bash
set -euo pipefail

# Runs distributed benchmark from the gateway host and pulls results locally.
# Preferred topology:
# - 3 PG hosts (one instance each)
# - 3 co-located Raft server hosts (typically same as PG hosts)
# - 1 gateway host

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

PG_HOSTS=""
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

THREADS="${PROFILE_THREADS:-$ARIABC_DEFAULT_FULL_THREADS}"
RUNS="${PROFILE_RUNS:-3}"
MODES="${PROFILE_MODES:-det}"
WORKLOADS="${PROFILE_WORKLOADS:-ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
RATES="${PROFILE_RATES:-0}"
NO_KAFKA="${PROFILE_NO_KAFKA:-1}"
WAIT_MAJORITY="${PROFILE_WAIT_MAJORITY:-0}"
SERVER_BYPASS_RAFT="${PROFILE_SERVER_BYPASS_RAFT:-0}"
GW_BROADCAST_ALL="${PROFILE_GW_BROADCAST_ALL:-0}"
DET_PARALLEL_WORKERS="${PROFILE_DET_PARALLEL_WORKERS:-1}"
SKIP_DET_WINDOW_SWEEP="${PROFILE_SKIP_DET_WINDOW_SWEEP:-1}"
ABORT_ON_INVALID_CASE="${PROFILE_ABORT_ON_INVALID_CASE:-1}"
PSQL_PROBE_TIMEOUT_S="${PROFILE_PSQL_PROBE_TIMEOUT_S:-8}"
DET_PROBE_TOTAL_TIMEOUT_S="${PROFILE_DET_PROBE_TOTAL_TIMEOUT_S:-45}"
DB_CONN_POOL_CAP="${PROFILE_DB_CONN_POOL_CAP:-8}"
DB_CONN_POOL_SIZE="${PROFILE_DB_CONN_POOL_SIZE:-8}"
DET_WINDOW="${PROFILE_DET_WINDOW:-16}"
KAFKA_HOME="${PROFILE_KAFKA_HOME:-}"
KAFKA_BOOTSTRAP="${PROFILE_KAFKA_BOOTSTRAP:-localhost:9092}"
GATEWAY_TIMEOUT_S="${PROFILE_GATEWAY_TIMEOUT_S:-60}"
CASE_TIMEOUT_S="${PROFILE_CASE_TIMEOUT_S:-60}"
PRE_CLEANUP="${PROFILE_PRE_CLEANUP:-1}"
GW_SUBMIT_MODE="${PROFILE_GW_SUBMIT_MODE:-event}"
GW_DET_SUBMIT_PIPELINE="${PROFILE_GW_DET_SUBMIT_PIPELINE:-1}"
SRV_PG_EXEC_MODE="${PROFILE_SRV_PG_EXEC_MODE:-event}"
COMPARISON_PROFILE="${PROFILE_COMPARISON_PROFILE:-manual}"
POSTCHECK_CONVERGENCE_TIMEOUT_S="${PROFILE_POSTCHECK_CONVERGENCE_TIMEOUT_S:-5}"
POSTCHECK_CONVERGENCE_POLL_MS="${PROFILE_POSTCHECK_CONVERGENCE_POLL_MS:-100}"
POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${PROFILE_POSTCHECK_CONVERGENCE_STABLE_ROUNDS:-2}"

usage() {
  cat <<'EOF_HELP'
Usage:
  run_distributed_benchmark.sh \
    --pg-hosts <h1,h2,h3> \
    [--raft-hosts <r1,r2,r3> | --raft-host <r>] \
    [--raft-member-hosts <m1,m2,m3>] \
    [--raft-client-hosts <c1,c2,c3>] \
    [--gateway-host <g>] \
    [--ssh-user <default_user>] \
    [--pg-users <u1,u2,u3>] [--raft-users <u1,u2,u3> | --raft-user <u>] [--gateway-user <u>] \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </work/ARIABC/AriaBC>] \
    [--remote-install-dir </work/ARIABC/install>] \
    [--threads <csv>] [--runs <n>] [--modes <det|nondet>] \
    [--workloads <csv>] [--rates <csv>] [--no-kafka 0|1] \
    [--wait-majority 0|1] [--det-parallel-workers 0|1] \
    [--skip-det-window-sweep 0|1] \
    [--abort-on-invalid-case 0|1] \
    [--gateway-timeout-s <seconds>] \
    [--case-timeout-s <seconds>] \
    [--pre-cleanup 0|1] \
    [--gw-submit-mode blocking|event] [--gw-det-submit-pipeline 0|1] \
    [--srv-pg-exec-mode threaded|event] \
    [--db-conn-pool-cap <n>] [--db-conn-pool-size <n>] [--det-window <n>] \
    [--postcheck-convergence-timeout-s <seconds>] [--postcheck-convergence-poll-ms <ms>] \
    [--postcheck-convergence-stable-rounds <n>]
    [--kafka-home <path>]
    [--comparison-profile manual|base-no-raft-no-kafka|raft-no-kafka|raft-kafka|vanilla-pg|kafka-only-no-raft]

Behavior:
1) SSH into gateway host.
2) Runs scripts/bench_nuraft_kafka_matrix.py in distributed mode.
3) Stores outputs under remote scripts/bench_full_results/<timestamp>.
4) Pulls result folder to local scripts/bench_full_results/distributed_<timestamp>.
5) Prints key CSV and graph artifact paths.
EOF_HELP
}

validate_local_results() {
  local out_dir="$1"
  local summary_csv="$out_dir/summary.csv"
  local results_csv="$out_dir/results.csv"

  if [[ ! -f "$summary_csv" ]]; then
    echo "ERROR: summary.csv missing in $out_dir" >&2
    return 1
  fi
  if [[ ! -f "$results_csv" ]]; then
    echo "ERROR: results.csv missing in $out_dir" >&2
    return 1
  fi

  if [[ "$(wc -l < "$summary_csv")" -le 1 ]]; then
    echo "ERROR: summary.csv has no data rows in $out_dir" >&2
    return 1
  fi

  local valid_count
  valid_count="$(awk -F, '
    NR==1 { for (i=1; i<=NF; i++) if ($i=="valid_run") c=i; next }
    NR>1 && c>0 && $c=="1" { n++ }
    END { print n+0 }
  ' "$results_csv")"
  if [[ "$valid_count" -le 0 ]]; then
    echo "ERROR: results.csv has no valid_run=1 rows in $out_dir" >&2
    return 1
  fi

  echo "Results validated: valid_run=1 rows = $valid_count"
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

join_csv() {
  local -n arr_ref="$1"
  local IFS=,
  printf '%s' "${arr_ref[*]}"
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-hosts) PG_HOSTS="${2:-}"; shift 2 ;;
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
    --threads) THREADS="${2:-}"; shift 2 ;;
    --runs) RUNS="${2:-3}"; shift 2 ;;
    --modes) MODES="${2:-det}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --rates) RATES="${2:-0}"; shift 2 ;;
    --no-kafka) NO_KAFKA="${2:-1}"; shift 2 ;;
    --wait-majority) WAIT_MAJORITY="${2:-0}"; shift 2 ;;
    --server-bypass-raft) SERVER_BYPASS_RAFT="${2:-0}"; shift 2 ;;
    --gw-broadcast-all) GW_BROADCAST_ALL="${2:-0}"; shift 2 ;;
    --det-parallel-workers) DET_PARALLEL_WORKERS="${2:-1}"; shift 2 ;;
    --skip-det-window-sweep) SKIP_DET_WINDOW_SWEEP="${2:-1}"; shift 2 ;;
    --abort-on-invalid-case) ABORT_ON_INVALID_CASE="${2:-1}"; shift 2 ;;
    --gateway-timeout-s) GATEWAY_TIMEOUT_S="${2:-60}"; shift 2 ;;
    --case-timeout-s) CASE_TIMEOUT_S="${2:-60}"; shift 2 ;;
    --pre-cleanup) PRE_CLEANUP="${2:-1}"; shift 2 ;;
    --gw-submit-mode) GW_SUBMIT_MODE="${2:-event}"; shift 2 ;;
    --gw-det-submit-pipeline) GW_DET_SUBMIT_PIPELINE="${2:-1}"; shift 2 ;;
    --srv-pg-exec-mode) SRV_PG_EXEC_MODE="${2:-event}"; shift 2 ;;
    --db-conn-pool-cap) DB_CONN_POOL_CAP="${2:-8}"; shift 2 ;;
    --db-conn-pool-size) DB_CONN_POOL_SIZE="${2:-8}"; shift 2 ;;
    --det-window) DET_WINDOW="${2:-16}"; shift 2 ;;
    --postcheck-convergence-timeout-s) POSTCHECK_CONVERGENCE_TIMEOUT_S="${2:-5}"; shift 2 ;;
    --postcheck-convergence-poll-ms) POSTCHECK_CONVERGENCE_POLL_MS="${2:-100}"; shift 2 ;;
    --postcheck-convergence-stable-rounds) POSTCHECK_CONVERGENCE_STABLE_ROUNDS="${2:-2}"; shift 2 ;;
    --kafka-home) KAFKA_HOME="${2:-}"; shift 2 ;;
    --kafka-bootstrap) KAFKA_BOOTSTRAP="${2:-localhost:9092}"; shift 2 ;;
    --comparison-profile) COMPARISON_PROFILE="${2:-manual}"; shift 2 ;;
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
  read -r -p "Enter PG hosts (comma-separated, exactly 3 entries): " PG_HOSTS
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

declare -a PG_HOST_ARR=()
split_csv "$PG_HOSTS" PG_HOST_ARR
if [[ "${#PG_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --pg-hosts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a RAFT_HOST_ARR=()
split_csv "$RAFT_HOSTS" RAFT_HOST_ARR
if [[ "${#RAFT_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --raft-hosts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a RAFT_MEMBER_HOST_ARR=()
split_csv "$RAFT_MEMBER_HOSTS" RAFT_MEMBER_HOST_ARR
if [[ "${#RAFT_MEMBER_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --raft-member-hosts must contain exactly 3 entries." >&2
  exit 2
fi

declare -a RAFT_CLIENT_HOST_ARR=()
split_csv "$RAFT_CLIENT_HOSTS" RAFT_CLIENT_HOST_ARR
if [[ "${#RAFT_CLIENT_HOST_ARR[@]}" -ne 3 ]]; then
  echo "ERROR: --raft-client-hosts must contain exactly 3 entries." >&2
  exit 2
fi

if [[ -z "$GATEWAY_HOST" ]]; then
  GATEWAY_HOST="${RAFT_HOST_ARR[0]}"
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
    echo "ERROR: provide --ssh-user default or explicit --pg-users/--raft-users/--gateway-user." >&2
    exit 2
  fi
  PG_USER_ARR=("$SSH_USER" "$SSH_USER" "$SSH_USER")
fi

declare -a RAFT_USER_ARR=()
if [[ -n "$RAFT_USERS" ]]; then
  split_csv "$RAFT_USERS" RAFT_USER_ARR
  if [[ "${#RAFT_USER_ARR[@]}" -ne 3 ]]; then
    echo "ERROR: --raft-users must contain exactly 3 entries." >&2
    exit 2
  fi
else
  if [[ -z "$RAFT_USER" ]]; then
    RAFT_USER="$SSH_USER"
  fi
  for host in "${RAFT_HOST_ARR[@]}"; do
    if idx="$(index_of_host "$host" PG_HOST_ARR 2>/dev/null)"; then
      RAFT_USER_ARR+=("${PG_USER_ARR[$idx]}")
    elif [[ -n "$RAFT_USER" ]]; then
      RAFT_USER_ARR+=("$RAFT_USER")
    elif [[ -n "$SSH_USER" ]]; then
      RAFT_USER_ARR+=("$SSH_USER")
    else
      echo "ERROR: could not derive raft user for host $host" >&2
      exit 2
    fi
  done
fi

if [[ -z "$GATEWAY_USER" ]]; then
  if idx="$(index_of_host "$GATEWAY_HOST" PG_HOST_ARR 2>/dev/null)"; then
    GATEWAY_USER="${PG_USER_ARR[$idx]}"
  elif idx="$(index_of_host "$GATEWAY_HOST" RAFT_HOST_ARR 2>/dev/null)"; then
    GATEWAY_USER="${RAFT_USER_ARR[$idx]}"
  elif [[ -n "$RAFT_USER" ]]; then
    GATEWAY_USER="$RAFT_USER"
  elif [[ -n "$SSH_USER" ]]; then
    GATEWAY_USER="$SSH_USER"
  else
    echo "ERROR: missing user mapping for gateway host $GATEWAY_HOST" >&2
    exit 2
  fi
fi

declare -A HOST_USER=()
set_host_user() {
  local host="$1"
  local user="$2"
  local role="$3"
  if [[ -n "${HOST_USER[$host]:-}" && "${HOST_USER[$host]}" != "$user" ]]; then
    if [[ "$host" == 127.0.0.1 || "$host" == 127.0.0.* || "$host" == localhost ]]; then
      return 0
    fi
    echo "ERROR: conflicting users for host $host (${HOST_USER[$host]} vs $user) role=$role" >&2
    exit 2
  fi
  HOST_USER[$host]="$user"
}

for i in 0 1 2; do
  set_host_user "${PG_HOST_ARR[$i]}" "${PG_USER_ARR[$i]}" "pg$((i+1))"
  set_host_user "${RAFT_HOST_ARR[$i]}" "${RAFT_USER_ARR[$i]}" "raft$((i+1))"
done
set_host_user "$GATEWAY_HOST" "$GATEWAY_USER" "gateway"

declare -a ALL_HOSTS=("${PG_HOST_ARR[@]}" "${RAFT_HOST_ARR[@]}" "$GATEWAY_HOST")
declare -A seen=()
declare -a UNIQUE_HOSTS=()
for h in "${ALL_HOSTS[@]}"; do
  if [[ -z "${seen[$h]:-}" ]]; then
    seen[$h]=1
    UNIQUE_HOSTS+=("$h")
  fi
done

RAFT_HOSTS_CSV="$(join_csv RAFT_HOST_ARR)"
RAFT_MEMBER_HOSTS_CSV="$(join_csv RAFT_MEMBER_HOST_ARR)"
RAFT_CLIENT_HOSTS_CSV="$(join_csv RAFT_CLIENT_HOST_ARR)"
PG_HOSTS_CSV="$(join_csv PG_HOST_ARR)"
RAFT_USERS_CSV="$(join_csv RAFT_USER_ARR)"

SSH_USER_MAP_CSV=""
for host in "${UNIQUE_HOSTS[@]}"; do
  if [[ -n "$SSH_USER_MAP_CSV" ]]; then
    SSH_USER_MAP_CSV+=","
  fi
  SSH_USER_MAP_CSV+="$host=${HOST_USER[$host]}"
done

REMOTE_BENCH_SSH_USER="$GATEWAY_USER"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT")
rsync_ssh="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p $SSH_PORT"
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
fi
scp_base=(scp -o BatchMode=yes -o StrictHostKeyChecking=no -P "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  scp_base+=(-i "$SSH_KEY")
fi

# Detect whether the gateway is the local machine.
# When local: run Python directly (no SSH), skip remote rsync.
_gw_is_local() {
  local h="$1"
  [[ -z "$h" || "$h" == "localhost" || "$h" == "127.0.0.1" ]] && return 0
  local hs hf
  hs="$(hostname -s 2>/dev/null || true)"
  hf="$(hostname -f 2>/dev/null || true)"
  [[ "$h" == "$hs" || "$h" == "$hf" ]] && return 0
  local lip
  for lip in $(hostname -I 2>/dev/null); do
    [[ "$h" == "$lip" ]] && return 0
  done
  return 1
}
GATEWAY_IS_LOCAL=0
if _gw_is_local "$GATEWAY_HOST"; then
  GATEWAY_IS_LOCAL=1
fi

# Helper: run a command string on the gateway (local or remote).
gw_run() {
  local cmd="$1"
  if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
    bash -c "$cmd"
  else
    "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$cmd")"
  fi
}

sync_bench_script() {
  if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
    return 0
  fi
  rsync -a -e "$rsync_ssh" \
    "$REPO_ROOT/scripts/bench_nuraft_kafka_matrix.py" \
    "$GATEWAY_USER@$GATEWAY_HOST:$REMOTE_REPO_ROOT/scripts/bench_nuraft_kafka_matrix.py"
}

ts="$(date +%Y%m%d_%H%M%S)"
preset_label_raw="${PROFILE_RESULTS_LABEL:-${PROFILE_PG_CONFIG_MODE:-${PROFILE_BENCH_PRESET:-canonical}}}"
preset_label="$(printf '%s' "$preset_label_raw" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//')"
if [[ -z "$preset_label" ]]; then
  preset_label="canonical"
fi
if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
  # When gateway is local, use local repo root so Python writes results here directly.
  remote_out_dir="$REPO_ROOT/scripts/bench_full_results/distributed_${preset_label}_${ts}"
else
  remote_out_dir="$REMOTE_REPO_ROOT/scripts/bench_full_results/distributed_${preset_label}_${ts}"
fi
local_out_dir="$REPO_ROOT/scripts/bench_full_results/distributed_${preset_label}_${ts}"
mkdir -p "$local_out_dir"

echo "== Distributed Benchmark Run =="
echo "PG hosts         : $PG_HOSTS_CSV"
echo "Raft hosts       : $RAFT_HOSTS_CSV"
echo "Raft member hosts: $RAFT_MEMBER_HOSTS_CSV"
echo "Raft client hosts: $RAFT_CLIENT_HOSTS_CSV"
echo "Gateway host/user: $GATEWAY_HOST / $GATEWAY_USER"
echo "PG users         : ${PG_USER_ARR[*]}"
echo "Raft users       : $RAFT_USERS_CSV"
echo "Remote repo root : $REMOTE_REPO_ROOT"
echo "Result preset    : $preset_label"
echo "Remote out dir   : $remote_out_dir"
echo "Local out dir    : $local_out_dir"
echo "Probe timeouts   : psql=${PSQL_PROBE_TIMEOUT_S}s det_total=${DET_PROBE_TOTAL_TIMEOUT_S}s"
echo "Gateway timeout  : ${GATEWAY_TIMEOUT_S}s"
echo "Case timeout     : ${CASE_TIMEOUT_S}s"
echo "Pre-cleanup      : ${PRE_CLEANUP}"
echo "Gateway submit   : mode=${GW_SUBMIT_MODE} det_pipeline=${GW_DET_SUBMIT_PIPELINE}"
echo "Server pg exec   : ${SRV_PG_EXEC_MODE}"
echo "Profile knobs    : profile=${COMPARISON_PROFILE} no_kafka=${NO_KAFKA} wait_majority=${WAIT_MAJORITY} bypass_raft=${SERVER_BYPASS_RAFT} broadcast_all=${GW_BROADCAST_ALL} det_parallel_workers=${DET_PARALLEL_WORKERS}"
echo "DB pool size/cap : ${DB_CONN_POOL_SIZE}/${DB_CONN_POOL_CAP}"
echo "Det window       : ${DET_WINDOW}"
echo "Skip det sweep   : ${SKIP_DET_WINDOW_SWEEP}"
echo "Abort on invalid : ${ABORT_ON_INVALID_CASE}"
echo

echo "Syncing benchmark driver to gateway..."
sync_bench_script

# Ensure gateway-side psql can talk to all PG servers.
echo "Checking gateway psql compatibility (all PG hosts)..."
probe_all_sql_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
PSQL="$REMOTE_INSTALL_DIR/bin/psql"
export PGCONNECT_TIMEOUT=5
export PGOPTIONS='-c statement_timeout=5000'
for hp in "${PG_HOST_ARR[0]}:5438" "${PG_HOST_ARR[1]}:5439" "${PG_HOST_ARR[2]}:5440"; do
  h="\${hp%:*}"
  p="\${hp##*:}"
  timeout "${PSQL_PROBE_TIMEOUT_S}s" env -u LD_LIBRARY_PATH "\$PSQL" -X -q -h "\$h" -p "\$p" -U postgres -d postgres -At -c "select 1;" >/dev/null
done
EOF_REMOTE
)
if ! gw_run "$probe_all_sql_cmd" >/dev/null 2>&1; then
  echo "Gateway psql probe failed; attempting auto-fix from PG system psql candidates"
  fixed=0
  gw_psql_path="$(gw_run 'ls -1 /usr/lib/postgresql/*/bin/psql 2>/dev/null | sort -V | tail -n1')"
  if [[ -n "$gw_psql_path" ]]; then
    gw_run "cp '$gw_psql_path' '$REMOTE_INSTALL_DIR/bin/psql' && chmod +x '$REMOTE_INSTALL_DIR/bin/psql'"
    if gw_run "$probe_all_sql_cmd" >/dev/null 2>&1; then
      fixed=1
      echo "Gateway psql auto-fix applied successfully (source: local gateway system psql)."
    fi
  fi
  for i in 0 1 2; do
    if [[ "$fixed" == "1" ]]; then
      break
    fi
    src_host="${PG_HOST_ARR[$i]}"
    src_user="${PG_USER_ARR[$i]}"
    src_psql_path="$("${ssh_base[@]}" "$src_user@$src_host" "bash -lc 'ls -1 /usr/lib/postgresql/*/bin/psql 2>/dev/null | sort -V | tail -n1'")"
    if [[ -z "$src_psql_path" ]]; then
      continue
    fi
    tmp_psql="/tmp/ariabc_psql_fix_$$"
    "${scp_base[@]}" "$src_user@$src_host:$src_psql_path" "$tmp_psql"
    chmod +x "$tmp_psql"
    if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
      cp "$tmp_psql" "$REMOTE_INSTALL_DIR/bin/psql"
    else
      "${scp_base[@]}" "$tmp_psql" "$GATEWAY_USER@$GATEWAY_HOST:$REMOTE_INSTALL_DIR/bin/psql"
    fi
    rm -f "$tmp_psql"
    if gw_run "$probe_all_sql_cmd" >/dev/null 2>&1; then
      fixed=1
      echo "Gateway psql auto-fix applied successfully (source host: $src_host)."
      break
    fi
  done
  if [[ "$fixed" != "1" ]]; then
    echo "ERROR: Gateway psql auto-fix failed for all PG-host candidates." >&2
    exit 1
  fi
else
  echo "Gateway psql probe OK."
fi

# Deterministic runtime policy:
# - Prefer throughput parser path for performance.
# - Auto-fallback to compat/raw if parser-path is unavailable on any node.
DET_RUNTIME_MODE="${PROFILE_DET_RUNTIME_MODE:-throughput}"
GW_DET_RAW_SQL="${PROFILE_GW_DET_RAW_SQL:-0}"
REQUIRE_DET_PARSER="${PROFILE_REQUIRE_DET_PARSER:-1}"
AUTO_DET_MODE="${PROFILE_AUTO_DET_MODE:-1}"

if [[ "$AUTO_DET_MODE" == "1" ]]; then
  echo "Checking deterministic parser-path capability (all PG hosts)..."
  probe_det_parser_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
PSQL="$REMOTE_INSTALL_DIR/bin/psql"
export PGCONNECT_TIMEOUT=5
export PGOPTIONS='-c statement_timeout=5000'
probe_q="select case when exists (select 1 from pg_proc where proname = 'merkle_root_hash') and exists (select 1 from pg_proc where proname = 'merkle_verify') and exists (select 1 from pg_proc where proname = 'merkle_tree_stats') then 1 else 0 end;"
for hp in "${PG_HOST_ARR[0]}:5438" "${PG_HOST_ARR[1]}:5439" "${PG_HOST_ARR[2]}:5440"; do
  h="\${hp%:*}"
  p="\${hp##*:}"
  v="\$(timeout "${PSQL_PROBE_TIMEOUT_S}s" env -u LD_LIBRARY_PATH "\$PSQL" -X -q -h "\$h" -p "\$p" -U postgres -d postgres -At -c "\$probe_q" 2>/tmp/ariabc_det_probe_\${p}.err || true)"
  if [[ "\$v" != "1" ]]; then
    echo "det_probe_failed host=\$h port=\$p value=\${v:-<empty>} err=\$(tr '\n' ' ' </tmp/ariabc_det_probe_\${p}.err | sed 's/[[:space:]]\+/ /g')" >&2
    exit 1
  fi
done
EOF_REMOTE
)
  det_probe_ok=0
  if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
    timeout "${DET_PROBE_TOTAL_TIMEOUT_S}s" bash -c "$probe_det_parser_cmd" >/dev/null 2>&1 && det_probe_ok=1
  else
    timeout "${DET_PROBE_TOTAL_TIMEOUT_S}s" "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$probe_det_parser_cmd")" >/dev/null 2>&1 && det_probe_ok=1
  fi
  if [[ "$det_probe_ok" == "1" ]]; then
    DET_RUNTIME_MODE="throughput"
    GW_DET_RAW_SQL="0"
    REQUIRE_DET_PARSER="1"
    echo "Det parser probe OK; using throughput mode."
  else
    DET_RUNTIME_MODE="compat"
    GW_DET_RAW_SQL="1"
    REQUIRE_DET_PARSER="0"
    echo "Det parser probe failed on at least one node; falling back to compat/raw mode (lower throughput)."
  fi
fi

echo "Det mode config    : mode=$DET_RUNTIME_MODE raw_sql=$GW_DET_RAW_SQL require_parser=$REQUIRE_DET_PARSER"
echo "Bench dbPool/detW  : dbConnPoolSize=$DB_CONN_POOL_SIZE dbConnPoolCap=$DB_CONN_POOL_CAP detWindow=$DET_WINDOW"

if [[ "$PRE_CLEANUP" == "1" ]]; then
  echo "Pre-cleaning stale benchmark processes on all involved hosts..."
  for host in "${UNIQUE_HOSTS[@]}"; do
    host_user="${HOST_USER[$host]:-}"
    cleanup_cmd='pkill -9 -f "bench_nuraft_kafka_matrix.py|ariabc_pg_gateway|ariabc_pg_server" || true'
    if _gw_is_local "$host"; then
      bash -c "$cleanup_cmd" >/dev/null 2>&1 || true
    else
      "${ssh_base[@]}" "$host_user@$host" "bash -lc '$cleanup_cmd'" >/dev/null 2>&1 || true
    fi
  done
fi

no_kafka_flag=""
if [[ "$NO_KAFKA" == "1" ]]; then
  no_kafka_flag="--no-kafka"
fi

skip_det_sweep_flag=""
if [[ "$SKIP_DET_WINDOW_SWEEP" == "1" ]]; then
  skip_det_sweep_flag="--skip-det-window-sweep"
fi

ssh_key_flag=""
if [[ -n "$SSH_KEY" ]]; then
  ssh_key_flag="--ssh-key '$SSH_KEY'"
fi

# When gateway is local, use the actual local repo root for cd/out-dir.
effective_repo_root="$REMOTE_REPO_ROOT"
if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
  effective_repo_root="$REPO_ROOT"
fi

remote_cmd=$(cat <<EOF_REMOTE
set -euo pipefail
cd "$effective_repo_root"
if [[ -d .venv ]]; then
  . .venv/bin/activate
fi
export ARIABC_DET_PARALLEL_WORKERS="$DET_PARALLEL_WORKERS"
export ARIABC_SSH_USER_MAP="$SSH_USER_MAP_CSV"
python3 scripts/bench_nuraft_kafka_matrix.py \
  --distributed \
  --nodes 3 \
  --raft-hosts "$RAFT_HOSTS_CSV" \
  --raft-member-hosts "$RAFT_MEMBER_HOSTS_CSV" \
  --client-hosts "$RAFT_CLIENT_HOSTS_CSV" \
  --pg-hosts "$PG_HOSTS_CSV" \
  --gateway-host "$GATEWAY_HOST" \
  --remote-repo-root "$REMOTE_REPO_ROOT" \
  --ssh-user "$REMOTE_BENCH_SSH_USER" \
  --ssh-port "$SSH_PORT" \
  --nuraftRoot "$REMOTE_REPO_ROOT/NuRaft" \
  --installDir "$REMOTE_INSTALL_DIR" \
  --kafkaBootstrap "${KAFKA_BOOTSTRAP:-localhost:9092}" \
  --threads "$THREADS" \
  --runs "$RUNS" \
  --modes "$MODES" \
  --workloads "$WORKLOADS" \
  --rates "$RATES" \
  --wait-majority "$WAIT_MAJORITY" \
  --server-bypass-raft "$SERVER_BYPASS_RAFT" \
  --gw-broadcast-all "$GW_BROADCAST_ALL" \
  --gateway-timeout-s "$GATEWAY_TIMEOUT_S" \
  --case-timeout-s "$CASE_TIMEOUT_S" \
  --gw-submit-mode "$GW_SUBMIT_MODE" \
  --gw-det-submit-pipeline "$GW_DET_SUBMIT_PIPELINE" \
  --srv-pg-exec-mode "$SRV_PG_EXEC_MODE" \
  --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
  --dbConnPoolCap "$DB_CONN_POOL_CAP" \
  --det-window "$DET_WINDOW" \
  --postcheck-convergence-timeout-s "$POSTCHECK_CONVERGENCE_TIMEOUT_S" \
  --postcheck-convergence-poll-ms "$POSTCHECK_CONVERGENCE_POLL_MS" \
  --postcheck-convergence-stable-rounds "$POSTCHECK_CONVERGENCE_STABLE_ROUNDS" \
  --det-runtime-mode "$DET_RUNTIME_MODE" \
  --gw-det-raw-sql "$GW_DET_RAW_SQL" \
  --require-det-parser "$REQUIRE_DET_PARSER" \
  --out-dir "$remote_out_dir" \
  --comparison-profile "$COMPARISON_PROFILE" \
  ${KAFKA_HOME:+--kafkaHome "$KAFKA_HOME"} \
  $skip_det_sweep_flag \
  $ssh_key_flag \
  $no_kafka_flag
EOF_REMOTE
)

if [[ "$GATEWAY_IS_LOCAL" == "1" ]]; then
  # Gateway is local — run Python directly.
  bash -c "$remote_cmd" || {
    echo "ERROR: gateway benchmark command failed." >&2
    exit 1
  }
  # remote_out_dir == local_out_dir when gateway is local; no rsync needed.
  echo "Pulling benchmark artifacts to local machine..."
  if [[ "$remote_out_dir" != "$local_out_dir" ]]; then
    rsync -a "$remote_out_dir/" "$local_out_dir/" || {
      echo "ERROR: failed to copy local benchmark artifacts." >&2
      exit 1
    }
  else
    echo "(Artifacts already local at $local_out_dir)"
  fi
else
  "${ssh_base[@]}" "$GATEWAY_USER@$GATEWAY_HOST" "bash -lc $(printf '%q' "$remote_cmd")" || {
    echo "ERROR: gateway benchmark command failed." >&2
    exit 1
  }
  echo "Pulling benchmark artifacts to local machine..."
  rsync -a -e "$rsync_ssh" "$GATEWAY_USER@$GATEWAY_HOST:$remote_out_dir/" "$local_out_dir/" || {
    echo "ERROR: failed to pull benchmark artifacts from gateway." >&2
    exit 1
  }
fi

validate_local_results "$local_out_dir" || exit 1

echo
echo "Benchmark completed. Artifacts:"
echo "- Local results CSV : $local_out_dir/results.csv"
echo "- Local summary CSV : $local_out_dir/summary.csv"
echo "- Local profiling CSV: $local_out_dir/profiling_summary.csv"
echo "- Local graphs dir  : $local_out_dir"
