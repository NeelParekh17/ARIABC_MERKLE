#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/cluster_topology.sh"

RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
STRICT_VERIFIER="$SCRIPT_DIR/verify_safe_recovery_case.sh"

CASE_NAME="E"
NODE_ID_TARGET=""
TARGET="follower"
FAILPOINT_OVERRIDE=""
PROBE_TEMPLATE="scripts/distributed/safe_recovery_probe.sql.in"
SETUP_SQL="scripts/distributed/safe_recovery_probe_setup.sql"
BOOTSTRAP_WORKLOAD="scripts/distributed/safe_recovery_bootstrap.sql"
ARTIFACT_DIR=""
CLUSTER_ID=""
EPOCH_HEX=""
EXPECT_STATE="2"
SKIP_POST_VERIFY=1
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/test_remote_raft_recovery.sh \
    --case <A|B|C|D|E|F|G> \
    [--node-id <1|2|4> | --target leader|follower|all] \
    [--failpoint <name>] \
    [--probe-template <repo-relative path>] \
    [--setup-sql <repo-relative path>] \
    [--artifact-dir <directory>] \
    [--cluster-id <id>] \
    [--epoch <64 lowercase hex>] \
    [--expect-state <2|3>] \
    [--skip-post-verify]
EOF
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

resolve_repo_path() {
  local value="$1"
  local path="$value"
  if [[ "$path" != /* ]]; then
    path="$REPO_ROOT/$path"
  fi
  [[ -e "$path" ]] || fail "path does not exist: $value"
  path="$(readlink -f "$path")"
  [[ "$path" == "$REPO_ROOT/"* ]] || fail "path is outside the repository root: $value"
  printf '%s\n' "$path"
}

repo_relative_path() {
  local abs_path="$1"
  local rel="${abs_path#"$REPO_ROOT"/}"
  [[ "$rel" != "$abs_path" ]] || fail "path is outside repository root: $abs_path"
  printf '%s\n' "$rel"
}

case_to_failpoint() {
  case "$1" in
    A) printf '%s\n' "ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE" ;;
    B) printf '%s\n' "ARIABC_FAILPOINT_AFTER_LEDGER_CLAIM_BEFORE_USER_SQL" ;;
    C) printf '%s\n' "ARIABC_FAILPOINT_AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT" ;;
    D) printf '%s\n' "ARIABC_FAILPOINT_BEFORE_WORKER_TOPLEVEL_COMMIT" ;;
    E) printf '%s\n' "ARIABC_FAILPOINT_AFTER_WORKER_TOPLEVEL_COMMIT" ;;
    F) printf '%s\n' "ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH" ;;
    G) printf '%s\n' "ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK" ;;
    *) fail "unsupported case: $1" ;;
  esac
}

choose_node_id() {
  if [[ -n "$NODE_ID_TARGET" ]]; then
    printf '%s\n' "$NODE_ID_TARGET"
    return
  fi
  case "$TARGET" in
    leader)
      printf '%s\n' "${ARIABC_PREFERRED_LEADER_ID:-1}"
      ;;
    follower)
      if [[ "${ARIABC_PREFERRED_LEADER_ID:-1}" == "1" ]]; then
        printf '%s\n' "2"
      else
        printf '%s\n' "1"
      fi
      ;;
    all)
      printf '%s\n' "${ARIABC_PREFERRED_LEADER_ID:-1}"
      ;;
    *)
      fail "invalid target: $TARGET"
      ;;
  esac
}

latest_run_dir() {
  ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null | head -n1
}

sync_run_artifacts() {
  local src="$1"
  local dest="$2"
  rm -rf "$dest"
  mkdir -p "$dest"
  rsync -a "$src/" "$dest/"
}

remote_psql() {
  local idx="$1"
  local sql="$2"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  local quoted_sql
  quoted_sql="$(printf '%q' "$sql")"
  sshpass -p "$CLUSTER_PASSWORD" \
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
    "${user}@${ip}" \
    "export PATH='${REMOTE_INSTALL_DIR}/bin':\$PATH; export LD_LIBRARY_PATH='${REMOTE_INSTALL_DIR}/lib':\${LD_LIBRARY_PATH:-}; psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p ${DB_PORT} -d ${DB_NAME} -qAt -F '|' -c ${quoted_sql}"
}

apply_probe_setup_all_nodes() {
  local idx
  for idx in "${!NODE_IDS[@]}"; do
    sshpass -p "$CLUSTER_PASSWORD" \
      ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='${REMOTE_INSTALL_DIR}/bin':\$PATH; export LD_LIBRARY_PATH='${REMOTE_INSTALL_DIR}/lib':\${LD_LIBRARY_PATH:-}; psql -X -v ON_ERROR_STOP=1 \
        -U '${DB_USER}' \
        -h 127.0.0.1 \
        -p '${DB_PORT}' \
        -d '${DB_NAME}' \
        -f '${REMOTE_REPO_ROOT}/scripts/distributed/safe_recovery_probe_setup.sql'"
  done
}

verify_probe_setup_all_nodes() {
  local idx value
  for idx in "${!NODE_IDS[@]}"; do
    value="$(remote_psql "$idx" "SELECT n FROM public.safe_recovery_probe WHERE k = 1;" | tr -d '\r' | sed '/^$/d')"
    [[ "$value" == "0" ]] || fail "setup probe value on node ${NODE_IDS[$idx]} is '$value', expected 0"
  done
}

discover_latest_terminal_row() {
  local idx="$1"
  remote_psql "$idx" "
SELECT raft_log_index, item_ordinal
  FROM ariabc_internal.raft_apply_item
 WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
   AND state IN (2,3)
 ORDER BY raft_log_index DESC, item_ordinal DESC
 LIMIT 1;
"
}

run_4node_step() {
  local label="$1"
  local allow_failure="$2"
  shift 2

  local log_file="$ARTIFACT_DIR/${label}.runner.log"
  local rc=0
  set +e
  env \
    SKIP_BUILD="${SKIP_BUILD:-0}" \
    SKIP_POST_VERIFY=1 \
    timeout -k 45s 900s \
    "$RUNNER" "$@" 2>&1 | tee "$log_file"
  rc=${PIPESTATUS[0]}
  set -e

  local run_dir
  run_dir="$(latest_run_dir || true)"
  [[ -n "$run_dir" ]] || fail "run_4node did not create a bench_full_results/cluster4_* artifact directory"
  sync_run_artifacts "$run_dir" "$ARTIFACT_DIR/stages/$label"
  LAST_RUN_RC="$rc"
  if [[ "$allow_failure" != "1" && "$rc" -ne 0 ]]; then
    fail "run_4node step '$label' failed with exit code $rc"
  fi
}

extract_failpoint_proof() {
  local crash_dir="$ARTIFACT_DIR/stages/crash"
  local marker_file="$ARTIFACT_DIR/failpoint_marker.txt"
  local marker_line=""

  marker_line="$(grep -R -h -m1 "SAFE_FAILPOINT_TRIGGERED name=${FAILPOINT_NAME}" \
    "$crash_dir"/server_node*.log "$crash_dir"/postgres_node*.log 2>/dev/null || true)"

  [[ -n "$marker_line" ]] || fail "SAFE_FAILPOINT_TRIGGERED marker not found for ${FAILPOINT_NAME}"
  printf '%s\n' "$marker_line" > "$marker_file"

  FAILPOINT_NODE="$(sed -n 's/.*node=\([0-9]\+\).*/\1/p' <<<"$marker_line" | head -n1)"
  FAILPOINT_PID="$(sed -n 's/.*pid=\([0-9]\+\).*/\1/p' <<<"$marker_line" | head -n1)"
  FAILPOINT_LOG_INDEX="$(sed -n 's/.*log=\([0-9]\+\).*/\1/p' <<<"$marker_line" | head -n1)"
  FAILPOINT_ITEM_ORDINAL="$(sed -n 's/.*ordinal=\([0-9]\+\).*/\1/p' <<<"$marker_line" | head -n1)"

  [[ -n "$FAILPOINT_NODE" ]] || fail "could not parse failpoint node from marker"
  [[ -n "$FAILPOINT_PID" ]] || fail "could not parse failpoint pid from marker"
  if [[ "$FAILPOINT_LOG_INDEX" =~ ^[1-9][0-9]*$ && "$FAILPOINT_ITEM_ORDINAL" =~ ^[0-9]+$ ]]; then
    FAILPOINT_IDENTITY_PRESENT=1
  fi
}

assert_failpoint_targets_probe_sql() {
  local crash_dir="$ARTIFACT_DIR/stages/crash"
  local expected_sql
  local pid_matches
  local stmt_matches

  expected_sql="UPDATE public.safe_recovery_probe SET n = n + 1, token = '${RECOVERY_TOKEN}' WHERE k = 1;"

  local log_file
  local pids=()
  for log_file in "$crash_dir"/postgres_node*.log; do
    [ -f "$log_file" ] || continue
    local line
    while read -r line; do
      if [[ "$line" =~ SAFE_FAILPOINT_TRIGGERED.*log=${FAILPOINT_LOG_INDEX}\ .*ordinal=${FAILPOINT_ITEM_ORDINAL} ]]; then
        local pid=""
        if [[ "$line" =~ pid=([0-9]+) ]]; then
          pid="${BASH_REMATCH[1]}"
          pids+=("$pid")
        fi
      fi
      if [[ "$line" =~ RAFT_LEDGER_BOUNDARY.*raft_log_index=${FAILPOINT_LOG_INDEX}.*item_ordinal=${FAILPOINT_ITEM_ORDINAL} ]]; then
        local pid=""
        if [[ "$line" =~ backend_pid=([0-9]+) ]]; then
          pid="${BASH_REMATCH[1]}"
          pids+=("$pid")
        fi
      fi
    done < "$log_file"
  done

  # Unique PIDs
  if [[ ${#pids[@]} -gt 0 ]]; then
    pids=($(printf '%s\n' "${pids[@]}" | sort -u))
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    fail "could not find any SAFE_FAILPOINT_TRIGGERED or RAFT_LEDGER_BOUNDARY log matching log=${FAILPOINT_LOG_INDEX} ordinal=${FAILPOINT_ITEM_ORDINAL}"
  fi

  local found_sql=0
  local pid
  for pid in "${pids[@]}"; do
    local matches=""
    matches="$(grep -h -F "[$pid]" "$crash_dir"/postgres_node*.log | grep -i -F "$expected_sql" || true)"
    if [[ -z "$matches" ]]; then
      matches="$(grep -h -C 3 "process (PID $pid) was terminated" "$crash_dir"/postgres_node*.log | grep -i -F "$expected_sql" || true)"
    fi

    if [[ -n "$matches" ]]; then
      found_sql=1
      printf '%s\n' "$matches" >> "$ARTIFACT_DIR/target_sql_proof.txt"
    fi
  done

  [[ "$found_sql" == "1" ]] ||
    fail "failpoint target ${FAILPOINT_LOG_INDEX}|${FAILPOINT_ITEM_ORDINAL} is not the tokenized probe UPDATE (expected SQL: '$expected_sql')"

  TARGET_SQL_MATCH=1
  TARGET_SQL_TOKEN="$RECOVERY_TOKEN"
}

assert_pid_exited() {
  local node_id="$1"
  local pid="$2"
  local idx

  for idx in "${!NODE_IDS[@]}"; do
    [[ "${NODE_IDS[$idx]}" == "$node_id" ]] || continue

    if sshpass -p "$CLUSTER_PASSWORD" \
         ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
         "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
         "kill -0 '$pid' 2>/dev/null"; then
      fail "failpoint PID $pid is still alive on node $node_id"
    fi
    TARGET_PID_EXITED=1
    return 0
  done

  fail "unknown failpoint node: $node_id"
}

read_env_value() {
  local file="$1"
  local key="$2"
  if [[ -f "$file" ]]; then
    sed -n "s/^${key}=//p" "$file" | tail -n1
  fi
}

assert_cluster_healthy() {
  local run_dir="$1"
  local idx hosts_csv ports_csv leader_output leader_count meta_file mode action epoch cluster_id skip_workload

  for idx in "${!NODE_IDS[@]}"; do
    sshpass -p "$CLUSTER_PASSWORD" \
      ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='${REMOTE_INSTALL_DIR}/bin':\$PATH; export LD_LIBRARY_PATH='${REMOTE_INSTALL_DIR}/lib':\${LD_LIBRARY_PATH:-}; pg_isready -h 127.0.0.1 -p '${DB_PORT}' -d '${DB_NAME}' -U '${DB_USER}' >/dev/null" \
      || fail "pg_isready failed on node ${NODE_IDS[$idx]}"

    sshpass -p "$CLUSTER_PASSWORD" \
      ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "nc -z 127.0.0.1 '${NODE_CLIENT_PORTS[$idx]}'" \
      || fail "Raft client port ${NODE_CLIENT_PORTS[$idx]} did not respond on node ${NODE_IDS[$idx]}"
  done

  hosts_csv="$(IFS=,; printf '%s' "${NODE_IPS[*]}")"
  ports_csv="$(IFS=,; printf '%s' "${NODE_CLIENT_PORTS[*]}")"
  leader_output="$(python3 "$SCRIPT_DIR/check_leader.py" "$hosts_csv" "$ports_csv" --print-all 2>/dev/null | tr -d '\r')"
  leader_count="$(printf '%s\n' "$leader_output" | grep -c 'is_leader=1' || true)"
  [[ "$leader_count" == "1" ]] || fail "check_leader.py did not find exactly one leader: ${leader_output//$'\n'/; }"

  meta_file="$run_dir/run_meta.env"
  [[ -f "$meta_file" ]] || fail "missing recovery run metadata: $meta_file"
  mode="$(read_env_value "$meta_file" raft_storage_mode)"
  action="$(read_env_value "$meta_file" raft_storage_action)"
  epoch="$(read_env_value "$meta_file" raft_epoch_hex)"
  cluster_id="$(read_env_value "$meta_file" raft_cluster_id)"
  skip_workload="$(read_env_value "$meta_file" skip_workload)"
  [[ "$mode" == "durable" ]] || fail "recovery run_meta raft_storage_mode=$mode expected durable"
  [[ "$action" == "preserve" ]] || fail "recovery run_meta raft_storage_action=$action expected preserve"
  [[ "$epoch" == "$EPOCH_HEX" ]] || fail "recovery run_meta raft_epoch_hex=$epoch expected $EPOCH_HEX"
  [[ "$cluster_id" == "$CLUSTER_ID" ]] || fail "recovery run_meta raft_cluster_id=$cluster_id expected $CLUSTER_ID"
  [[ "$skip_workload" == "1" ]] || fail "recovery run_meta skip_workload=$skip_workload expected 1"

  POST_CRASH_CLUSTER_HEALTHY=1
  RAFT_STORAGE_PRESERVED=1
  EPOCH_REUSED=1
  RESTART_CONFIRMED=1
}

parse_run_summary() {
  local run_dir="$1"
  local summary_file="$run_dir/run_summary.env"
  local gateway_log="$run_dir/gateway_test.log"
  local gateway_completed="0"
  if [[ -f "$summary_file" ]]; then
    local permanent_failures divergence_count workload_transactions completed gateway_summary
    gateway_summary="$(sed -n 's/^gateway_completed=//p' "$summary_file" | tail -n1)"
    if [[ "$gateway_summary" == "not_applicable" ]]; then
      printf '%s\n' "not_applicable"
      return 0
    fi
    permanent_failures="$(sed -n 's/^permanent_failures=//p' "$summary_file" | tail -n1)"
    divergence_count="$(sed -n 's/^divergence_count=//p' "$summary_file" | tail -n1)"
    workload_transactions="$(sed -n 's/^workload_transactions=//p' "$summary_file" | tail -n1)"
    completed="$(sed -n 's/^client_quorum_complete_count=//p' "$summary_file" | tail -n1)"
    if [[ -z "$completed" && -f "$gateway_log" ]]; then
      completed="$(grep -E '^PROGRESS_GATEWAY_DET\b' "$gateway_log" 2>/dev/null | sed -n 's/.*completed=\([0-9]\+\).*/\1/p' | tail -n1)"
    fi
    if [[ -z "$completed" && -f "$gateway_log" ]]; then
      completed="$(grep -E '^loaded [0-9]+ queries' "$gateway_log" 2>/dev/null | sed -n 's/^loaded \([0-9]\+\) queries.*/\1/p' | tail -n1)"
    fi
    if [[ "${completed:-}" == "${workload_transactions:-missing}" &&
          "${permanent_failures:-1}" == "0" &&
          "${divergence_count:-1}" == "0" ]]; then
      gateway_completed="1"
    fi
  fi
  printf '%s\n' "$gateway_completed"
}

write_failpoint_proof() {
  local terminal_epoch="$1"
  local terminal_log_index="$2"
  local terminal_item_ordinal="$3"
  local restart_confirmed="$4"
  local proof_file="$ARTIFACT_DIR/failpoint_proof.env"
  {
    printf 'FAILPOINT_FIRED=1\n'
    printf 'TARGET_PID_EXITED=%s\n' "$TARGET_PID_EXITED"
    printf 'POST_CRASH_CLUSTER_HEALTHY=%s\n' "$POST_CRASH_CLUSTER_HEALTHY"
    printf 'RAFT_STORAGE_PRESERVED=%s\n' "$RAFT_STORAGE_PRESERVED"
    printf 'EPOCH_REUSED=%s\n' "$EPOCH_REUSED"
    printf 'RESTART_CONFIRMED=%s\n' "$restart_confirmed"
    printf 'FAILPOINT_NAME=%s\n' "$FAILPOINT_NAME"
    printf 'FAILPOINT_CASE=%s\n' "$CASE_NAME"
    printf 'FAILPOINT_NODE=%s\n' "$FAILPOINT_NODE"
    printf 'FAILPOINT_PID=%s\n' "$FAILPOINT_PID"
    printf 'RAFT_LOG_INDEX=%s\n' "$terminal_log_index"
    printf 'ITEM_ORDINAL=%s\n' "$terminal_item_ordinal"
    printf 'FAILPOINT_MARKER_EPOCH=%s\n' "$terminal_epoch"
    printf 'TARGET_SQL_MATCH=%s\n' "${TARGET_SQL_MATCH:-0}"
    printf 'TARGET_SQL_TOKEN=%s\n' "${TARGET_SQL_TOKEN:-}"
    printf 'RECOVERY_HARNESS_SHA256=%s\n' "${RECOVERY_HARNESS_SHA256:-}"
  } > "$proof_file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) CASE_NAME="${2:?missing value for --case}"; shift 2 ;;
    --node-id) NODE_ID_TARGET="${2:?missing value for --node-id}"; shift 2 ;;
    --target) TARGET="${2:?missing value for --target}"; shift 2 ;;
    --failpoint) FAILPOINT_OVERRIDE="${2:?missing value for --failpoint}"; shift 2 ;;
    --probe-template) PROBE_TEMPLATE="${2:?missing value for --probe-template}"; shift 2 ;;
    --setup-sql) SETUP_SQL="${2:?missing value for --setup-sql}"; shift 2 ;;
    --artifact-dir) ARTIFACT_DIR="${2:?missing value for --artifact-dir}"; shift 2 ;;
    --cluster-id) CLUSTER_ID="${2:?missing value for --cluster-id}"; shift 2 ;;
    --epoch) EPOCH_HEX="${2:?missing value for --epoch}"; shift 2 ;;
    --expect-state) EXPECT_STATE="${2:?missing value for --expect-state}"; shift 2 ;;
    --skip-post-verify) SKIP_POST_VERIFY=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

case "$CASE_NAME" in A|B|C|D|E|F|G) ;;
  *) fail "--case must be one of A-G" ;;
esac

case "$TARGET" in leader|follower|all) ;;
  *) fail "--target must be leader|follower|all" ;;
esac

if [[ -n "$NODE_ID_TARGET" && "$TARGET" != "follower" ]]; then
  fail "--node-id cannot be combined with --target ${TARGET}"
fi

if [[ -z "$FAILPOINT_OVERRIDE" ]]; then
  FAILPOINT_NAME="$(case_to_failpoint "$CASE_NAME")"
else
  FAILPOINT_NAME="$FAILPOINT_OVERRIDE"
fi

PROBE_TEMPLATE="$(resolve_repo_path "$PROBE_TEMPLATE")"
grep -q "@RECOVERY_TOKEN@" "$PROBE_TEMPLATE" || fail "probe template '$PROBE_TEMPLATE' does not contain @RECOVERY_TOKEN@"

SETUP_SQL="$(resolve_repo_path "$SETUP_SQL")"
BOOTSTRAP_WORKLOAD="$(resolve_repo_path "$BOOTSTRAP_WORKLOAD")"
BOOTSTRAP_WORKLOAD_ARG="$(repo_relative_path "$BOOTSTRAP_WORKLOAD")"

RECOVERY_HARNESS_SHA256="$(
  sha256sum \
    "$SCRIPT_DIR/run_4node_raft_cluster.sh" \
    "$SCRIPT_DIR/test_remote_raft_recovery.sh" \
    "$SCRIPT_DIR/run_safe_ledger_recovery_matrix.sh" \
    "$SCRIPT_DIR/verify_safe_recovery_case.sh" \
    "$SCRIPT_DIR/verify_safe_ledger_run.sh" \
    "$SCRIPT_DIR/safe_recovery_bootstrap.sql" \
    "$SCRIPT_DIR/safe_recovery_probe.sql.in" \
    "$SCRIPT_DIR/safe_recovery_probe_setup.sql" \
  | sha256sum | awk '{print $1}'
)"

if [[ -z "$ARTIFACT_DIR" ]]; then
  ARTIFACT_DIR="$REPO_ROOT/scripts/bench_full_results/recovery_case_${CASE_NAME}_$(date +%Y%m%d_%H%M%S)"
elif [[ "$ARTIFACT_DIR" != /* ]]; then
  ARTIFACT_DIR="$REPO_ROOT/$ARTIFACT_DIR"
fi

if [[ -z "$CLUSTER_ID" ]]; then
  CLUSTER_ID="safe_recovery_${CASE_NAME}_$(date +%Y%m%d_%H%M%S)_$RANDOM"
fi
if [[ -z "$EPOCH_HEX" ]]; then
  EPOCH_HEX="$(openssl rand -hex 32)"
fi

[[ "$EPOCH_HEX" =~ ^[0-9a-f]{64}$ ]] || fail "--epoch must be exactly 64 lowercase hex characters"
[[ "$EXPECT_STATE" =~ ^[23]$ ]] || fail "--expect-state must be 2 or 3"

RECOVERY_TOKEN="safe_recovery_${EPOCH_HEX}"
GENERATED_WORKLOAD="$SCRIPT_DIR/.safe_recovery_probe_${EPOCH_HEX}.sql"

sed "s/@RECOVERY_TOKEN@/${RECOVERY_TOKEN}/g" \
  "$PROBE_TEMPLATE" \
  > "$GENERATED_WORKLOAD"

trap 'rm -f "$GENERATED_WORKLOAD"' EXIT

WORKLOAD_FILE="$GENERATED_WORKLOAD"
WORKLOAD_ARG="$(repo_relative_path "$WORKLOAD_FILE")"

mkdir -p "$ARTIFACT_DIR/stages"
LOG_FILE="$ARTIFACT_DIR/harness.log"
exec > >(tee -ia "$LOG_FILE") 2>&1

log() {
  echo "[$(date +'%H:%M:%S')] $*"
}

log "case=$CASE_NAME failpoint=$FAILPOINT_NAME node=${NODE_ID_TARGET:-$TARGET} epoch=$EPOCH_HEX artifact=$ARTIFACT_DIR"

TARGET_NODE_ID="$(choose_node_id)"
FAILPOINT_LOG_INDEX=""
FAILPOINT_ITEM_ORDINAL=""
FAILPOINT_IDENTITY_PRESENT=0
FAILPOINT_MIN_RAFT_LOG_INDEX=""
TARGET_PID_EXITED=0
POST_CRASH_CLUSTER_HEALTHY=0
RAFT_STORAGE_PRESERVED=0
EPOCH_REUSED=0
RESTART_CONFIRMED=0

COMMON_ARGS=(
  --threads 1
  --pool-size 1
  --bcdb-worker-count 1
  --det-window 1
  --det-batch-size 1
  --det-pipeline-depth 1
  --det-prefixed-direct-parallel 0
  --bcdb-dt-conflict-tracking 0
  --bcdb-dt-light-snapshot 1
  --bcdb-dt-parse-barrier 0
  --preferred-leader-id "${ARIABC_PREFERRED_LEADER_ID:-1}"
  --raft-storage-mode durable
  --raft-apply-ledger-mode safe
  --raft-epoch-hex "$EPOCH_HEX"
  --raft-cluster-id "$CLUSTER_ID"
  --skip-restore
  --skip-post-verify
)

log "bootstrap run: fresh storage, workload=$(basename "$BOOTSTRAP_WORKLOAD")"
run_4node_step "bootstrap" 0 \
  --raft-storage-action fresh \
  "${COMMON_ARGS[@]}" \
  --workload "$BOOTSTRAP_WORKLOAD_ARG"

log "applying probe setup directly with psql on every replica"
apply_probe_setup_all_nodes
verify_probe_setup_all_nodes

  idx=""
  val=""
  declare -a values=()
  for idx in "${!NODE_IDS[@]}"; do
    val="$(remote_psql "$idx" "SELECT COALESCE(max(raft_log_index), 0) FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('${EPOCH_HEX}', 'hex') AND state IN (2, 3);" | tr -d '\r' | sed '/^$/d')"
    if [[ ! "$val" =~ ^[0-9]+$ ]]; then
      fail "invalid maximum log index returned from node ${NODE_IDS[$idx]}: '$val'"
    fi
    values+=("$val")
  done

  first_val="${values[0]}"
  for val in "${values[@]}"; do
    if [[ "$val" != "$first_val" ]]; then
      fail "replica log index mismatch: first node had $first_val, but another node had $val"
    fi
  done

  BASELINE_MAX_LOG_INDEX="$first_val"
  FAILPOINT_MIN_RAFT_LOG_INDEX="$((BASELINE_MAX_LOG_INDEX + 1))"

  {
    printf 'BASELINE_MAX_LOG_INDEX=%s\n' "$BASELINE_MAX_LOG_INDEX"
    printf 'FAILPOINT_MIN_RAFT_LOG_INDEX=%s\n' "$FAILPOINT_MIN_RAFT_LOG_INDEX"
    printf 'RECOVERY_TOKEN=%s\n' "$RECOVERY_TOKEN"
    printf "TARGET_SQL=UPDATE public.safe_recovery_probe SET n = n + 1, token = '%s' WHERE k = 1;\n" "$RECOVERY_TOKEN"
    printf 'RECOVERY_HARNESS_SHA256=%s\n' "$RECOVERY_HARNESS_SHA256"
  } > "$ARTIFACT_DIR/target_contract.env"

log "crash run: preserve storage, workload=$(basename "$WORKLOAD_FILE"), failpoint=$FAILPOINT_NAME node=$TARGET_NODE_ID"
CRASH_FAILPOINT_ARGS=()
if [[ -n "$FAILPOINT_MIN_RAFT_LOG_INDEX" ]]; then
  CRASH_FAILPOINT_ARGS+=(--failpoint-min-raft-log-index "$FAILPOINT_MIN_RAFT_LOG_INDEX")
fi
run_4node_step "crash" 1 \
  --raft-storage-action preserve \
  "${COMMON_ARGS[@]}" \
  --workload "$WORKLOAD_ARG" \
  --failpoint-node-id "$TARGET_NODE_ID" \
  --failpoint-env "$FAILPOINT_NAME" \
  "${CRASH_FAILPOINT_ARGS[@]}"
if [[ "${LAST_RUN_RC:-0}" == "0" ]]; then
  log "WARNING: crash run exited cleanly; expected a failpoint-induced failure"
fi

extract_failpoint_proof
assert_failpoint_targets_probe_sql
assert_pid_exited "$FAILPOINT_NODE" "$FAILPOINT_PID"

[[ "$FAILPOINT_IDENTITY_PRESENT" == "1" ]] ||
  fail "failpoint marker did not provide target log/index identity"

log "recovery run: preserve storage, skip workload and allow Raft recovery"
run_4node_step "recovery" 0 \
  --raft-storage-action preserve \
  "${COMMON_ARGS[@]}" \
  --skip-workload

sync_run_artifacts "$ARTIFACT_DIR/stages/recovery" "$ARTIFACT_DIR/final_run"
assert_cluster_healthy "$ARTIFACT_DIR/final_run"

log "strict verification"
"$STRICT_VERIFIER" \
  --artifact-dir "$ARTIFACT_DIR" \
  --epoch "$EPOCH_HEX" \
  --raft-log-index "$FAILPOINT_LOG_INDEX" \
  --item-ordinal "$FAILPOINT_ITEM_ORDINAL" \
  --expect-state "$EXPECT_STATE" \
  --expect-probe-token "$RECOVERY_TOKEN" \
  --probe-key 1

VERIFY_ENV="$ARTIFACT_DIR/recovery_verify.env"
[[ -f "$VERIFY_ENV" ]] || fail "strict verifier did not produce $VERIFY_ENV"

terminal_log_index="$(sed -n 's/^VERIFY_RAFT_LOG_INDEX=//p' "$VERIFY_ENV" | tail -n1)"
terminal_item_ordinal="$(sed -n 's/^VERIFY_ITEM_ORDINAL=//p' "$VERIFY_ENV" | tail -n1)"
terminal_state="$(sed -n 's/^VERIFY_TERMINAL_STATE=//p' "$VERIFY_ENV" | tail -n1)"
terminal_digest="$(sed -n 's/^VERIFY_TERMINAL_DIGEST=//p' "$VERIFY_ENV" | tail -n1)"

write_failpoint_proof "$EPOCH_HEX" "${terminal_log_index:-0}" "${terminal_item_ordinal:-0}" 1

gateway_completed="$(parse_run_summary "$ARTIFACT_DIR/final_run")"
printf 'GATEWAY_COMPLETED=%s\n' "$gateway_completed" >> "$ARTIFACT_DIR/failpoint_proof.env"
printf 'TERMINAL_STATE=%s\n' "${terminal_state:-}" >> "$ARTIFACT_DIR/failpoint_proof.env"
printf 'TERMINAL_DIGEST=%s\n' "${terminal_digest:-}" >> "$ARTIFACT_DIR/failpoint_proof.env"

log "recovery harness complete"
