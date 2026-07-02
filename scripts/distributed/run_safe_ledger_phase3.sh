#!/usr/bin/env bash
# Run the Phase 3 safe-ledger non-crash validation campaign.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/cluster_topology.sh"

RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
DOTFILE="$REPO_ROOT/.safe_phase3_artifact_root"

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/run_safe_ledger_phase3.sh \
    --case <success-one|success-50|route-user|route-internal|error-42P01|error-22012|error-22012-replay> \
    [--artifact-root <directory>]

  scripts/distributed/run_safe_ledger_phase3.sh \
    --case error-22012-replay \
    --replay-from <case_error_22012 artifact dir> \
    [--artifact-root <directory>]
EOF
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

# Parse args
CASE_NAME=""
ARTIFACT_ROOT=""
REPLAY_FROM=""
INVOCATION_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) CASE_NAME="${2:?missing case name}"; shift 2 ;;
    --artifact-root) ARTIFACT_ROOT="${2:?missing artifact root}"; shift 2 ;;
    --replay-from) REPLAY_FROM="${2:?missing replay source dir}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$CASE_NAME" ]] || fail "--case is required"

# Determine artifact root
if [[ -n "$ARTIFACT_ROOT" ]]; then
  if [[ "$ARTIFACT_ROOT" != /* ]]; then
    ARTIFACT_ROOT="$REPO_ROOT/$ARTIFACT_ROOT"
  fi
  mkdir -p "$ARTIFACT_ROOT"
  echo "$ARTIFACT_ROOT" > "$DOTFILE"
else
  if [[ -f "$DOTFILE" ]]; then
    ARTIFACT_ROOT="$(cat "$DOTFILE")"
  fi
  if [[ -z "$ARTIFACT_ROOT" || ! -d "$ARTIFACT_ROOT" ]]; then
    ARTIFACT_ROOT="$REPO_ROOT/scripts/bench_full_results/phase3_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$ARTIFACT_ROOT"
    echo "$ARTIFACT_ROOT" > "$DOTFILE"
  fi
fi

# Map case name to variables
case "$CASE_NAME" in
  success-one)
    CASE_LABEL="success_one"
    EXPECT_STATE="2"
    EXPECT_TARGET_COUNT=1
    EXPECT_ROUTE="deferred"
    EXPECT_RELATION="public.safe_phase3_probe"
    EXPECT_SQLSTATE=""
    ;;
  success-50)
    CASE_LABEL="success_50"
    EXPECT_STATE="2"
    EXPECT_TARGET_COUNT=50
    EXPECT_ROUTE="deferred"
    EXPECT_RELATION="public.safe_phase3_probe"
    EXPECT_SQLSTATE=""
    ;;
  route-user)
    CASE_LABEL="route_user"
    EXPECT_STATE="2"
    EXPECT_TARGET_COUNT=1
    EXPECT_ROUTE="deferred"
    EXPECT_RELATION="public.safe_phase3_probe"
    EXPECT_SQLSTATE=""
    ;;
  route-internal)
    CASE_LABEL="route_internal"
    EXPECT_STATE="2"
    EXPECT_TARGET_COUNT=3
    EXPECT_ROUTE="direct"
    EXPECT_RELATION="ariabc_internal.raft_apply_epoch"
    EXPECT_SQLSTATE=""
    ;;
  error-42P01)
    CASE_LABEL="error_42P01"
    EXPECT_STATE="3"
    EXPECT_TARGET_COUNT=1
    EXPECT_ROUTE="none"
    EXPECT_RELATION=""
    EXPECT_SQLSTATE="42P01"
    ;;
  error-22012)
    CASE_LABEL="error_22012"
    EXPECT_STATE="nonterminal-failure"
    EXPECT_TARGET_COUNT=1
    EXPECT_ROUTE="none"
    EXPECT_RELATION=""
    EXPECT_SQLSTATE="22012"
    ;;
  error-22012-replay)
    CASE_LABEL="error_22012_replay"
    EXPECT_STATE="nonterminal-failure"
    EXPECT_TARGET_COUNT=0
    EXPECT_ROUTE="none"
    EXPECT_RELATION=""
    EXPECT_SQLSTATE="22012"
    ;;
  *)
    fail "unknown case: $CASE_NAME"
    ;;
esac

REPLAY_FROM_ABS=""
if [[ -n "$REPLAY_FROM" ]]; then
  REPLAY_FROM_ABS="$(readlink -f "$REPLAY_FROM")"
  [[ -d "$REPLAY_FROM_ABS" ]] || fail "--replay-from must point to an existing directory: $REPLAY_FROM"
fi

CASE_DIR="$ARTIFACT_ROOT/case_${CASE_LABEL}"
if [[ -d "$CASE_DIR" ]] && find "$CASE_DIR" -mindepth 1 -print -quit | grep -q .; then
  CASE_DIR="$ARTIFACT_ROOT/case_${CASE_LABEL}_$(date +%Y%m%d_%H%M%S)_$RANDOM"
fi
mkdir -p "$CASE_DIR"

INVOCATION_ID="phase3_${CASE_LABEL}_$(date +%s)_$RANDOM"
export ARIABC_PHASE3_INVOCATION_ID="$INVOCATION_ID"

# Clean up temporary workloads
CLEANUP_FILES=()
cleanup() {
  if [[ ${#CLEANUP_FILES[@]} -gt 0 ]]; then
    rm -f "${CLEANUP_FILES[@]}"
  fi
}
trap cleanup EXIT

# 10. Phase 3 static build gate
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Executing Phase 3 static build gate..."
  git diff --check || fail "git diff --check failed"

  bash -n "$SCRIPT_DIR/run_safe_ledger_phase3.sh" || fail "run_safe_ledger_phase3.sh syntax check failed"
  bash -n "$SCRIPT_DIR/verify_safe_ledger_phase3.sh" || fail "verify_safe_ledger_phase3.sh syntax check failed"
  bash -n "$RUNNER" || fail "run_4node_raft_cluster.sh syntax check failed"

  make -j"$(nproc)" || fail "make failed"
  make install || fail "make install failed"

  cmake --build ariabc_pg/build -j"$(nproc)" || fail "cmake build failed"
  ctest --test-dir ariabc_pg/build --output-on-failure || fail "ctest failed"
fi

# Reset failpoints
unset ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE
unset ARIABC_FAILPOINT_AFTER_LEDGER_CLAIM_BEFORE_USER_SQL
unset ARIABC_FAILPOINT_AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT
unset ARIABC_FAILPOINT_BEFORE_WORKER_TOPLEVEL_COMMIT
unset ARIABC_FAILPOINT_AFTER_WORKER_TOPLEVEL_COMMIT
unset ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH
unset ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK
unset ARIABC_FAILPOINT_MIN_RAFT_LOG_INDEX
unset ARIABC_FAILPOINT_RAFT_LOG_INDEX
unset ARIABC_FAILPOINT_ITEM_ORDINAL
unset ARIABC_FAILPOINT_NODE_ID

resolve_repo_path() {
  local value="$1"
  local path="$value"
  if [[ "$path" != /* ]]; then
    path="$REPO_ROOT/$path"
  fi
  [[ -e "$path" ]] || fail "path does not exist: $value"
  readlink -f "$path"
}

repo_relative_path() {
  local abs_path="$1"
  local rel="${abs_path#"$REPO_ROOT"/}"
  [[ "$rel" != "$abs_path" ]] || fail "path is outside repository root: $abs_path"
  printf '%s\n' "$rel"
}

read_env_value() {
  local file="$1"
  local key="$2"
  [[ -f "$file" ]] || return 1
  sed -n "s/^${key}=//p" "$file" | tail -n1
}

find_matching_run_dir() {
  local expected_storage_action="$1"
  local expected_skip_workload="$2"
  local run_dir run_meta run_invocation run_cluster run_epoch run_action run_skip

  while IFS= read -r run_dir; do
    [[ -d "$run_dir" ]] || continue
    run_meta="$run_dir/run_meta.env"
    [[ -f "$run_meta" ]] || continue
    run_invocation="$(read_env_value "$run_meta" phase3_invocation_id || true)"
    run_cluster="$(read_env_value "$run_meta" raft_cluster_id || true)"
    [[ -z "$run_cluster" ]] && run_cluster="$(read_env_value "$run_meta" RAFT_CLUSTER_ID || true)"
    run_epoch="$(read_env_value "$run_meta" raft_epoch_hex || true)"
    [[ -z "$run_epoch" ]] && run_epoch="$(read_env_value "$run_meta" RAFT_EPOCH_HEX || true)"
    run_action="$(read_env_value "$run_meta" raft_storage_action || true)"
    [[ -z "$run_action" ]] && run_action="$(read_env_value "$run_meta" RAFT_STORAGE_ACTION || true)"
    run_skip="$(read_env_value "$run_meta" skip_workload || true)"
    [[ -z "$run_skip" ]] && run_skip="$(read_env_value "$run_meta" SKIP_WORKLOAD || true)"

    if [[ "$run_invocation" == "$INVOCATION_ID" &&
          "$run_cluster" == "$CLUSTER_ID" &&
          "$run_epoch" == "$EPOCH_HEX" &&
          "$run_action" == "$expected_storage_action" &&
          "$run_skip" == "$expected_skip_workload" ]]; then
      printf '%s\n' "$run_dir"
      return 0
    fi
  done < <(ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null || true)

  return 1
}

copy_run_artifacts() {
  local run_dir="$1"
  local step_dir="$2"
  mkdir -p "$step_dir"
  cp -a "$run_dir"/. "$step_dir"/
}

materialize_step_contract() {
  local step_dir="$1"
  if [[ -f "$CASE_DIR/run_meta.env" ]]; then
    cp -f "$CASE_DIR/run_meta.env" "$step_dir/run_meta.env"
  fi
  if [[ -f "$CASE_DIR/target_contract.env" ]]; then
    cp -f "$CASE_DIR/target_contract.env" "$step_dir/target_contract.env"
  fi
}

apply_setup_all_nodes() {
  local idx
  for idx in "${!NODE_IDS[@]}"; do
    ssh -o BatchMode=yes -o ConnectTimeout=8 "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='/home/neel/Desktop/ariabc_install/bin':\$PATH; export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib':\${LD_LIBRARY_PATH:-}; \
       psql -X -v ON_ERROR_STOP=1 -U '${DB_USER}' -h 127.0.0.1 -p '${DB_PORT}' -d '${DB_NAME}'" < "$REPO_ROOT/scripts/distributed/safe_phase3_setup.sql" \
      || fail "apply_setup failed on node ${NODE_IDS[$idx]}"
  done
}

verify_setup_all_nodes() {
  local idx value
  for idx in "${!NODE_IDS[@]}"; do
    value="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='/home/neel/Desktop/ariabc_install/bin':\$PATH; export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib':\${LD_LIBRARY_PATH:-}; \
       psql -X -q -h 127.0.0.1 -p '${DB_PORT}' -U '${DB_USER}' '${DB_NAME}' -tAc \"SELECT count(*), min(n), max(n) FROM public.safe_phase3_probe;\"" | tr -d '\r')"
    if [[ "$value" != "50|0|0" ]]; then
      fail "Setup verification failed on node ${NODE_IDS[$idx]}: expected '50|0|0', got '$value'"
    fi
  done
}

record_baseline() {
  local idx val
  declare -a values=()
  for idx in "${!NODE_IDS[@]}"; do
    val="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='/home/neel/Desktop/ariabc_install/bin':\$PATH; export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib':\${LD_LIBRARY_PATH:-}; \
       psql -X -q -h 127.0.0.1 -p '${DB_PORT}' -U '${DB_USER}' '${DB_NAME}' -tAc \"SELECT COALESCE(max(raft_log_index), 0) FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('${EPOCH_HEX}', 'hex') AND state IN (2, 3, 4);\"" | tr -d '\r')"
    if [[ ! "$val" =~ ^[0-9]+$ ]]; then
      fail "Invalid baseline maximum log index returned from node ${NODE_IDS[$idx]}: '$val'"
    fi
    values+=("$val")
  done

  local first_val="${values[0]}"
  for val in "${values[@]}"; do
    if [[ "$val" != "$first_val" ]]; then
      fail "Replica log index mismatch: node 0 had $first_val, but node $idx had $val"
    fi
  done

  echo "$first_val"
}

LAST_RUN_RC=0
run_4node_step() {
  local label="$1"
  local allow_failure="$2"
  local step_timeout="$3"
  local expected_storage_action="$4"
  local expected_skip_workload="$5"
  shift 5

  local step_dir="$CASE_DIR/$label"
  local log_file="$step_dir/phase3.log"
  local rc=0
  mkdir -p "$step_dir"
  set +e
  if [[ "$step_timeout" -gt 0 ]]; then
    timeout -k 15s "${step_timeout}s" "$RUNNER" "$@" > "$log_file" 2>&1
    rc=$?
  else
    "$RUNNER" "$@" > "$log_file" 2>&1
    rc=$?
  fi
  set -e

  local run_dir
  run_dir="$(find_matching_run_dir "$expected_storage_action" "$expected_skip_workload" || true)"
  if [[ -n "$run_dir" ]]; then
    copy_run_artifacts "$run_dir" "$step_dir"
    materialize_step_contract "$step_dir"
  else
    fail "no artifact produced by this invocation"
  fi

  LAST_RUN_RC="$rc"
  if [[ "$allow_failure" != "1" && "$rc" -ne 0 ]]; then
    fail "run_4node step '$label' failed with exit code $rc"
  fi
}

# Export safe-ledger variables and resolve the replay source before defining
# shared runner arguments that depend on epoch/cluster identifiers.
export ARIABC_SAFE_TRACE=1
export ARIABC_SAFE_POSTCOMMIT_WITNESS=1
export ARIABC_SAFE_EXTERNAL_PROBE=1

if [[ "$CASE_NAME" == "error-22012-replay" ]]; then
  [[ -n "$REPLAY_FROM_ABS" ]] || fail "--replay-from is required for error-22012-replay"
  [[ -f "$REPLAY_FROM_ABS/run_meta.env" ]] || fail "replay source is missing run_meta.env"
  [[ -f "$REPLAY_FROM_ABS/target_contract.env" ]] || fail "replay source is missing target_contract.env"
  CLUSTER_ID="$(grep -E '^(RAFT_CLUSTER_ID|raft_cluster_id)=' "$REPLAY_FROM_ABS/run_meta.env" | tail -n1 | cut -d= -f2-)"
  EPOCH_HEX="$(grep -E '^(RAFT_EPOCH_HEX|raft_epoch_hex)=' "$REPLAY_FROM_ABS/run_meta.env" | tail -n1 | cut -d= -f2-)"
  BASELINE_MAX_LOG_INDEX="$(grep -E '^(BASELINE_MAX_LOG_INDEX|baseline_max_log_index)=' "$REPLAY_FROM_ABS/target_contract.env" | tail -n1 | cut -d= -f2-)"
  [[ -n "$CLUSTER_ID" && -n "$EPOCH_HEX" && -n "$BASELINE_MAX_LOG_INDEX" ]] || fail "replay source metadata is incomplete"
else
  CLUSTER_ID="safe_phase3_${CASE_LABEL}_$(date +%Y%m%d_%H%M%S)_$RANDOM"
  EPOCH_HEX="$(openssl rand -hex 32)"
fi

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

# Run the baseline phase unless this is a replay-only proof.
RUN_TOKEN=""
TEST_EPOCH=""
CASE_WORKLOAD_FILE=""

if [[ "$CASE_NAME" != "error-22012-replay" ]]; then
  # 1. Start fresh durable Raft storage with bootstrap SELECT 1 workload
  echo "Running bootstrap step (fresh storage)..."
  run_4node_step "bootstrap_run" 0 300 fresh 0 \
    --raft-storage-action fresh \
    "${COMMON_ARGS[@]}" \
    --workload "scripts/distributed/safe_phase3_bootstrap.sql"

  # 2. Apply setup SQL directly on all replicas
  echo "Applying probe setup SQL..."
  apply_setup_all_nodes
  verify_setup_all_nodes

  # 3. Record baseline max log index
  echo "Recording baseline max log index..."
  BASELINE_MAX_LOG_INDEX="$(record_baseline)"
  echo "BASELINE_MAX_LOG_INDEX=$BASELINE_MAX_LOG_INDEX"

  case "$CASE_NAME" in
    success-one)
      RUN_TOKEN="phase3_success_one_$(openssl rand -hex 16)"
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/.safe_phase3_success_one_${RUN_TOKEN}.sql"
      sed "s/@RUN_TOKEN@/${RUN_TOKEN}/g" "$SCRIPT_DIR/safe_phase3_success_one.sql.in" > "$CASE_WORKLOAD_FILE"
      CLEANUP_FILES+=("$CASE_WORKLOAD_FILE")
      ;;
    success-50)
      RUN_TOKEN="phase3_success_50_$(openssl rand -hex 16)"
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/.safe_phase3_success_50_${RUN_TOKEN}.sql"
      sed "s/@RUN_TOKEN@/${RUN_TOKEN}/g" "$SCRIPT_DIR/safe_phase3_success_50.sql.in" > "$CASE_WORKLOAD_FILE"
      CLEANUP_FILES+=("$CASE_WORKLOAD_FILE")
      ;;
    route-user)
      RUN_TOKEN="phase3_route_user_$(openssl rand -hex 16)"
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/.safe_phase3_route_user_${RUN_TOKEN}.sql"
      sed "s/@RUN_TOKEN@/${RUN_TOKEN}/g" "$SCRIPT_DIR/safe_phase3_route_user.sql.in" > "$CASE_WORKLOAD_FILE"
      CLEANUP_FILES+=("$CASE_WORKLOAD_FILE")
      ;;
    route-internal)
      TEST_EPOCH="$(openssl rand -hex 32)"
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/.safe_phase3_route_internal_${TEST_EPOCH}.sql"
      sed "s/@TEST_EPOCH@/${TEST_EPOCH}/g" "$SCRIPT_DIR/safe_phase3_route_internal.sql.in" > "$CASE_WORKLOAD_FILE"
      CLEANUP_FILES+=("$CASE_WORKLOAD_FILE")
      ;;
    error-42P01)
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/safe_phase3_error_42P01.sql"
      ;;
    error-22012)
      CASE_WORKLOAD_FILE="$SCRIPT_DIR/safe_phase3_error_22012.sql"
      ;;
  esac
else
  echo "Preparing preserved-storage replay proof from: $REPLAY_FROM_ABS"
  CASE_WORKLOAD_FILE=""
fi

# Write metadata and contract
{
  printf 'RAFT_CLUSTER_ID=%s\n' "$CLUSTER_ID"
  printf 'RAFT_STORAGE_MODE=durable\n'
  printf 'RAFT_STORAGE_ACTION=preserve\n'
  printf 'RAFT_EPOCH_HEX=%s\n' "$EPOCH_HEX"
  printf 'phase3_invocation_id=%s\n' "$INVOCATION_ID"
  printf 'CASE_NAME=%s\n' "$CASE_NAME"
} > "$CASE_DIR/run_meta.env"

{
  printf 'BASELINE_MAX_LOG_INDEX=%s\n' "$BASELINE_MAX_LOG_INDEX"
  printf 'TARGET_MIN_LOG_INDEX=%s\n' "$((BASELINE_MAX_LOG_INDEX + 1))"
  printf 'RUN_TOKEN=%s\n' "$RUN_TOKEN"
  printf 'TEST_EPOCH=%s\n' "$TEST_EPOCH"
  printf 'TARGET_WORKLOAD=%s\n' "$CASE_WORKLOAD_FILE"
  if [[ "$CASE_NAME" == "error-22012-replay" ]]; then
    printf 'REPLAY_FROM=%s\n' "$REPLAY_FROM_ABS"
  fi
} > "$CASE_DIR/target_contract.env"

# Run target workload step
TARGET_TIMEOUT=300
if [[ "$CASE_NAME" == "error-22012" || "$CASE_NAME" == "error-22012-replay" ]]; then
  TARGET_TIMEOUT=600
fi

echo "Running target workload: $(basename "$CASE_WORKLOAD_FILE")..."
TARGET_STEP="target_run"
if [[ "$CASE_NAME" == "error-22012-replay" ]]; then
  TARGET_STEP="replay_run"
fi

if [[ "$CASE_NAME" == "error-22012-replay" ]]; then
  FORCE_BUILD=0 SKIP_BUILD=1 run_4node_step "$TARGET_STEP" 1 "$TARGET_TIMEOUT" preserve 1 \
    --raft-storage-action preserve \
    "${COMMON_ARGS[@]}" \
    --skip-workload
else
  FORCE_BUILD=0 SKIP_BUILD=1 run_4node_step "$TARGET_STEP" 1 "$TARGET_TIMEOUT" preserve 0 \
    --raft-storage-action preserve \
    "${COMMON_ARGS[@]}" \
    --workload "$(repo_relative_path "$CASE_WORKLOAD_FILE")"
fi

# Record LAST_RUN_RC in target_contract.env
printf 'LAST_RUN_RC=%s\n' "$LAST_RUN_RC" >> "$CASE_DIR/target_contract.env"
cp -f "$CASE_DIR/run_meta.env" "$CASE_DIR/$TARGET_STEP/run_meta.env"
cp -f "$CASE_DIR/target_contract.env" "$CASE_DIR/$TARGET_STEP/target_contract.env"

# Execute verifier
echo "Executing strict verifier..."
VERIFY_ARGS=(
  --artifact-dir "$CASE_DIR/$TARGET_STEP" \
  --epoch "$EPOCH_HEX" \
  --baseline-max-log "$BASELINE_MAX_LOG_INDEX" \
  --expect-target-count "$EXPECT_TARGET_COUNT" \
  --expect-state "$EXPECT_STATE"
  --target-run-rc "$LAST_RUN_RC"
)
if [[ -n "$EXPECT_SQLSTATE" ]]; then
  VERIFY_ARGS+=(--expect-sqlstate "$EXPECT_SQLSTATE")
fi
if [[ -n "$EXPECT_ROUTE" && "$EXPECT_ROUTE" != "none" ]]; then
  VERIFY_ARGS+=(--expect-route "$EXPECT_ROUTE")
fi
if [[ -n "$EXPECT_RELATION" ]]; then
  VERIFY_ARGS+=(--expect-relation "$EXPECT_RELATION")
fi
if [[ -n "$RUN_TOKEN" ]]; then
  VERIFY_ARGS+=(--expect-token-prefix "$RUN_TOKEN")
fi
if [[ "$CASE_NAME" == "error-22012" ]]; then
  :
elif [[ "$CASE_NAME" == "error-22012-replay" ]]; then
  VERIFY_ARGS+=(--replay-from "$REPLAY_FROM_ABS")
fi

set +e
"$SCRIPT_DIR/verify_safe_ledger_phase3.sh" "${VERIFY_ARGS[@]}"
VERIFY_RC=$?
set -e

# Update summaries
CSV_FILE="$ARTIFACT_ROOT/summary.csv"
if [[ ! -f "$CSV_FILE" ]]; then
  echo "case,epoch,baseline_max_log,status,notes" > "$CSV_FILE"
fi

STATUS_STR="PASS"
NOTES_STR=""

if [[ "$VERIFY_RC" -ne 0 ]]; then
  STATUS_STR="FAIL"
  NOTES_STR="Strict verifier failed"
fi

if [[ "$LAST_RUN_RC" -ne 0 ]]; then
  STATUS_STR="FAIL"
  NOTES_STR="Runner failed before verification: rc=$LAST_RUN_RC"
elif [[ "$VERIFY_RC" -eq 0 ]]; then
  STATUS_STR="PASS"
  if [[ "$CASE_NAME" == "error-22012" || "$CASE_NAME" == "error-22012-replay" ]]; then
    NOTES_STR="Bounded failure confirmed"
  fi
fi

echo "${CASE_NAME},${EPOCH_HEX},${BASELINE_MAX_LOG_INDEX},${STATUS_STR},${NOTES_STR}" >> "$CSV_FILE"

MD_FILE="$ARTIFACT_ROOT/summary.md"
if [[ ! -f "$MD_FILE" ]]; then
  {
    echo "# Phase 3 Summary"
    echo ""
    echo "| Case | Epoch | Baseline Max Log | Status | Notes |"
    echo "| --- | --- | --- | --- | --- |"
  } > "$MD_FILE"
fi
echo "| ${CASE_NAME} | \`${EPOCH_HEX:0:8}...\` | ${BASELINE_MAX_LOG_INDEX} | **${STATUS_STR}** | ${NOTES_STR} |" >> "$MD_FILE"

# Report and exit
if [[ "$STATUS_STR" == "PASS" ]]; then
  echo "PASS: Case $CASE_NAME complete"
  exit 0
else
  echo "FAIL: Case $CASE_NAME failed (${NOTES_STR})"
  exit 1
fi
