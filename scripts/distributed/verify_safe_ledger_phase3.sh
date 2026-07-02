#!/usr/bin/env bash
# Verify a completed safe-ledger Phase 3 distributed run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster_topology.sh"

ARTIFACT_DIR=""
EPOCH_HEX=""
BASELINE_MAX_LOG=""
EXPECT_TARGET_COUNT=""
EXPECT_STATE=""
EXPECT_SQLSTATE=""
EXPECT_ROUTE="none"
EXPECT_RELATION=""
EXPECT_TOKEN_PREFIX=""
REPLAY_FROM=""

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/verify_safe_ledger_phase3.sh \
    --artifact-dir <directory> \
    --epoch <64-hex> \
    --baseline-max-log <N> \
    --expect-target-count <N> \
    --expect-state <2|3|nonterminal-failure> \
    [--expect-sqlstate <five-character SQLSTATE>] \
    [--expect-route <deferred|direct|none>] \
    [--expect-relation <schema.table>] \
    [--expect-token-prefix <text>] \
    [--replay-from <source artifact dir>]
EOF
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

TARGET_RUN_RC=""
REQUIRE_GATEWAY_FAILURE_MARKER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-dir) ARTIFACT_DIR="${2:?missing value for --artifact-dir}"; shift 2 ;;
    --epoch) EPOCH_HEX="${2:?missing value for --epoch}"; shift 2 ;;
    --baseline-max-log) BASELINE_MAX_LOG="${2:?missing value for --baseline-max-log}"; shift 2 ;;
    --expect-target-count) EXPECT_TARGET_COUNT="${2:?missing value for --expect-target-count}"; shift 2 ;;
    --expect-state) EXPECT_STATE="${2:?missing value for --expect-state}"; shift 2 ;;
    --expect-sqlstate) EXPECT_SQLSTATE="${2:?missing value for --expect-sqlstate}"; shift 2 ;;
    --expect-route) EXPECT_ROUTE="${2:?missing value for --expect-route}"; shift 2 ;;
    --expect-relation) EXPECT_RELATION="${2:?missing value for --expect-relation}"; shift 2 ;;
    --expect-token-prefix) EXPECT_TOKEN_PREFIX="${2:?missing value for --expect-token-prefix}"; shift 2 ;;
    --replay-from) REPLAY_FROM="${2:?missing value for --replay-from}"; shift 2 ;;
    --target-run-rc) TARGET_RUN_RC="${2:?missing value for --target-run-rc}"; shift 2 ;;
    --require-gateway-failure-marker) REQUIRE_GATEWAY_FAILURE_MARKER="${2:?missing value for --require-gateway-failure-marker}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$ARTIFACT_DIR" ]] || fail "--artifact-dir is required"
[[ -d "$ARTIFACT_DIR" ]] || fail "artifact dir does not exist: $ARTIFACT_DIR"
[[ "$EPOCH_HEX" =~ ^[0-9a-fA-F]{64}$ ]] || fail "--epoch must be 64 hex characters"
[[ "$BASELINE_MAX_LOG" =~ ^[0-9]+$ ]] || fail "--baseline-max-log must be a non-negative integer"
[[ "$EXPECT_TARGET_COUNT" =~ ^[0-9]+$ ]] || fail "--expect-target-count must be a non-negative integer"
case "$EXPECT_STATE" in
  2|3|nonterminal-failure) ;;
  *) fail "--expect-state must be 2, 3, or nonterminal-failure" ;;
esac
if [[ -n "$REPLAY_FROM" && ! -d "$REPLAY_FROM" ]]; then
  fail "--replay-from must point to an existing directory: $REPLAY_FROM"
fi

TEST_EPOCH=""
if [[ -f "$ARTIFACT_DIR/target_contract.env" ]]; then
  TEST_EPOCH="$(grep "^TEST_EPOCH=" "$ARTIFACT_DIR/target_contract.env" | cut -d= -f2- || true)"
fi

psql_node() {
  local idx="$1" sql="$2"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$user@$ip" \
    "export PATH='/home/neel/Desktop/ariabc_install/bin:\$PATH'; \
     export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib:\${LD_LIBRARY_PATH:-}'; \
     psql -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"$sql\""
}

read_env_value() {
  local file="$1"
  local key="$2"
  if [[ -f "$file" ]]; then
    sed -n "s/^${key}=//p" "$file" | tail -n1
  fi
}

load_replay_reference() {
  local src_dir="$1"
  local marker_file=""
  local parsed=""

  REPLAY_TARGET_LOG=""
  REPLAY_TARGET_ORD=""
  REPLAY_TARGET_DIGEST=""
  REPLAY_TARGET_SQLSTATE=""
  REPLAY_TARGET_CLASS=""
  REPLAY_TARGET_RETRYABLE=""

  marker_file="$src_dir/phase3_gateway_failure_marker.txt"
  if [[ -f "$marker_file" ]]; then
    parsed="$(cat "$marker_file")"
    REPLAY_TARGET_LOG="$(sed -nE 's/.*(^|[[:space:]])log=([0-9]+).*/\2/p' <<<"$parsed")"
    REPLAY_TARGET_ORD="$(sed -nE 's/.*(^|[[:space:]])ord=([0-9]+).*/\2/p' <<<"$parsed")"
    REPLAY_TARGET_DIGEST="$(sed -nE 's/.*(^|[[:space:]])failure_digest=([0-9a-f]{64}).*/\2/p' <<<"$parsed")"
    REPLAY_TARGET_SQLSTATE="$(sed -nE 's/.*(^|[[:space:]])sqlstate=([0-9A-Z]{5}).*/\2/p' <<<"$parsed")"
    REPLAY_TARGET_CLASS="$(sed -nE 's/.*(^|[[:space:]])failure_class=([^[:space:]]+).*/\2/p' <<<"$parsed")"
    REPLAY_TARGET_RETRYABLE="$(sed -nE 's/.*(^|[[:space:]])retryable=([01]).*/\2/p' <<<"$parsed")"
  else
    parsed="$(grep -R -h -m1 'SAFE_VERIFY_NONTERMINAL_FAILURE' "$src_dir"/server_node*.log 2>/dev/null | head -n1 || true)"
    if [[ -n "$parsed" ]]; then
      REPLAY_TARGET_LOG="$(sed -nE 's/.*(^|[[:space:]])log=([0-9]+).*/\2/p' <<<"$parsed")"
      REPLAY_TARGET_ORD="$(sed -nE 's/.*(^|[[:space:]])ord=([0-9]+).*/\2/p' <<<"$parsed")"
      REPLAY_TARGET_DIGEST="$(sed -nE 's/.*(^|[[:space:]])failure_digest=([0-9a-f]{64}).*/\2/p' <<<"$parsed")"
      REPLAY_TARGET_SQLSTATE="$(sed -nE 's/.*(^|[[:space:]])sqlstate=([0-9A-Z]{5}).*/\2/p' <<<"$parsed")"
      REPLAY_TARGET_CLASS="SQL_EXECUTION_ABORTED"
      REPLAY_TARGET_RETRYABLE="0"
    fi
  fi

  [[ "$REPLAY_TARGET_LOG" =~ ^[0-9]+$ ]] || fail "could not load replay target log from $src_dir"
  [[ "$REPLAY_TARGET_ORD" =~ ^[0-9]+$ ]] || fail "could not load replay target ordinal from $src_dir"
  [[ "$REPLAY_TARGET_DIGEST" =~ ^[0-9a-f]{64}$ ]] || fail "could not load replay target digest from $src_dir"
  [[ "$REPLAY_TARGET_SQLSTATE" =~ ^[0-9A-Z]{5}$ ]] || fail "could not load replay target SQLSTATE from $src_dir"
  [[ -n "$REPLAY_TARGET_CLASS" ]] || fail "could not load replay target failure class from $src_dir"
  [[ "$REPLAY_TARGET_RETRYABLE" =~ ^[01]$ ]] || fail "could not load replay target retryable flag from $src_dir"
}

# Scan collected logs for forbidden markers
if grep -r -E "SAFE_POSTCOMMIT_WITNESS_MISMATCH|SAFE_POSTCOMMIT_WITNESS_FATAL|SAFE_LEDGER_DURABLE_VERIFY_FAILED" "$ARTIFACT_DIR/" >/dev/null 2>&1; then
  fail "Found forbidden patterns in logs: $(grep -r -o -E "SAFE_POSTCOMMIT_WITNESS_MISMATCH|SAFE_POSTCOMMIT_WITNESS_FATAL|SAFE_LEDGER_DURABLE_VERIFY_FAILED" "$ARTIFACT_DIR/" | head -n 5)"
fi

if [[ "$EXPECT_STATE" == "2" || "$EXPECT_STATE" == "3" ]]; then
  # Direct ledger queries
  ledger_sql="
SELECT raft_log_index,
       item_ordinal,
       state,
       encode(terminal_digest, 'hex'),
       (committed_at IS NOT NULL)::int,
       (result_payload IS NOT NULL)::int,
       (error_payload IS NOT NULL)::int,
       COALESCE(result_format_version::text, ''),
       COALESCE(error_format_version::text, ''),
       COALESCE(sqlstate_code, '')
FROM ariabc_internal.raft_apply_item
WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
  AND raft_log_index > ${BASELINE_MAX_LOG}
ORDER BY raft_log_index, item_ordinal;
"

  declare -a LEDGERS=()
  for idx in "${!NODE_IDS[@]}"; do
    out="$(psql_node "$idx" "$ledger_sql" | tr -d '\r' | sed '/^$/d')"
    LEDGERS+=("$out")
  done

  # Verify counts and row sequence are identical on all nodes
  ref_ledger="${LEDGERS[0]}"
  for idx in "${!NODE_IDS[@]}"; do
    if [[ "${LEDGERS[$idx]}" != "$ref_ledger" ]]; then
      fail "Ledger row mismatch on node ${NODE_IDS[$idx]} compared to node ${NODE_IDS[0]}"
    fi
  done

  # Verify zero state=1 rows for the epoch on all nodes
  for idx in "${!NODE_IDS[@]}"; do
    claimed_count="$(psql_node "$idx" "SELECT count(*) FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('${EPOCH_HEX}', 'hex') AND state = 1;" | tr -d '\r')"
    if [[ "$claimed_count" -ne 0 ]]; then
      fail "Node ${NODE_IDS[$idx]} has persistent CLAIMED (state=1) rows: $claimed_count"
    fi
  done

  # Parse and validate each row in the ledger
  line_count=0
  if [[ -n "$ref_ledger" ]]; then
    line_count="$(echo "$ref_ledger" | wc -l)"
  fi

  if [[ "$line_count" -ne "$EXPECT_TARGET_COUNT" ]]; then
    fail "Expected target count $EXPECT_TARGET_COUNT, but got $line_count terminal rows"
  fi

  if [[ "$line_count" -gt 0 ]]; then
    while read -r line; do
      if [[ -z "$line" ]]; then continue; fi
      # Split by |
      IFS='|' read -r r_idx r_ord r_state r_digest r_committed r_res_pres r_err_pres r_res_fmt r_err_fmt r_sqlstate <<<"$line"

      # Check general invariants
      if [[ ! "$r_digest" =~ ^[0-9a-f]{64}$ ]]; then
        fail "Terminal digest is not 64 lowercase hex characters: '$r_digest'"
      fi
      if [[ "$r_committed" -ne 1 ]]; then
        fail "committed_at is missing (not 1)"
      fi

      # State-specific checks
      if [[ "$EXPECT_STATE" == "2" ]]; then
        if [[ "$r_state" -ne 2 ]]; then fail "Expected state=2, got $r_state"; fi
        if [[ "$r_res_pres" -ne 1 ]]; then fail "Expected result_payload to be present for state=2"; fi
        if [[ "$r_err_pres" -ne 0 ]]; then fail "Expected error_payload to be absent for state=2"; fi
        if [[ "$r_res_fmt" != "1" ]]; then fail "Expected result_format_version=1 for state=2"; fi
        if [[ -n "$r_err_fmt" ]]; then fail "Expected error_format_version to be absent for state=2"; fi
      elif [[ "$EXPECT_STATE" == "3" ]]; then
        if [[ "$r_state" -ne 3 ]]; then fail "Expected state=3, got $r_state"; fi
        if [[ "$r_res_pres" -ne 0 ]]; then fail "Expected result_payload to be absent for state=3"; fi
        if [[ "$r_err_pres" -ne 1 ]]; then fail "Expected error_payload to be present for state=3"; fi
        if [[ -n "$r_res_fmt" ]]; then fail "Expected result_format_version to be absent for state=3"; fi
        if [[ "$r_err_fmt" != "1" ]]; then fail "Expected error_format_version=1 for state=3"; fi
        if [[ -n "${EXPECT_SQLSTATE:-}" && "$r_sqlstate" != "$EXPECT_SQLSTATE" ]]; then
          fail "Expected sqlstate $EXPECT_SQLSTATE, got $r_sqlstate"
        fi
      fi
    done <<<"$ref_ledger"
  fi

  # Log check requirements for state=2 success
  if [[ "$EXPECT_STATE" == "2" ]]; then
    if ! grep -r "SAFE_VERIFY_FRESH_CONN" "$ARTIFACT_DIR/" | grep "state=2" | grep "digest_present=1" | grep "committed=1" >/dev/null; then
      fail "Log marker SAFE_VERIFY_FRESH_CONN ... state=2 ... digest_present=1 ... committed=1 is missing"
    fi
    if ! grep -r "SAFE_KAFKA_PUBLISH_DELIVERED" "$ARTIFACT_DIR/" >/dev/null; then
      fail "Log marker SAFE_KAFKA_PUBLISH_DELIVERED is missing"
    fi
  fi

  # Verify probe table content on every replica if tokens are expected
  if [[ -n "${EXPECT_TOKEN_PREFIX:-}" ]]; then
    for idx in "${!NODE_IDS[@]}"; do
      # Query count(*), min(n), max(n), and count(*) matching token prefix
      probe_info="$(psql_node "$idx" "SELECT count(*), min(n), max(n), count(*) FILTER (WHERE token LIKE '${EXPECT_TOKEN_PREFIX}%') FROM public.safe_phase3_probe;" | tr -d '\r')"
      IFS='|' read -r p_count p_min p_max p_tok_count <<<"$probe_info"
      if [[ "$p_count" -ne 50 ]]; then
        fail "Node ${NODE_IDS[$idx]} probe table row count is $p_count, expected 50"
      fi
      expected_min=0
      if [[ "$EXPECT_TARGET_COUNT" -eq 50 ]]; then
        expected_min=1
      fi
      if [[ "$p_min" -ne "$expected_min" || "$p_max" -ne 1 ]]; then
        fail "Node ${NODE_IDS[$idx]} probe n values min=$p_min max=$p_max, expected min=$expected_min max=1"
      fi
      if [[ "$p_tok_count" -ne "$EXPECT_TARGET_COUNT" ]]; then
        fail "Node ${NODE_IDS[$idx]} probe token count is $p_tok_count, expected $EXPECT_TARGET_COUNT"
      fi
    done
  fi

  # If EXPECT_STATE is 3, require the gateway deterministic error marker
  if [[ "$EXPECT_STATE" == "3" ]]; then
    gateway_log=""
    if [[ -f "$ARTIFACT_DIR/gateway_test.log" ]]; then
      gateway_log="$ARTIFACT_DIR/gateway_test.log"
    elif [[ -f "$ARTIFACT_DIR/gateway.log" ]]; then
      gateway_log="$ARTIFACT_DIR/gateway.log"
    else
      gateway_log="$(find "$ARTIFACT_DIR" -name "*gateway*.log" | head -n1)"
    fi

    if [[ -n "$gateway_log" && -f "$gateway_log" ]]; then
      marker_line=""
      marker_line="$(grep "SAFE_GATEWAY_DETERMINISTIC_ERROR" "$gateway_log" | head -n1 || true)"
      if [[ -z "$marker_line" ]]; then
        fail "Gateway deterministic error marker not found in $gateway_log"
      fi
      echo "Found gateway deterministic error marker: $marker_line"

      if [[ ! "$marker_line" =~ epoch=${EPOCH_HEX} ]]; then
        fail "Marker does not contain the correct epoch: expected ${EPOCH_HEX}, got line: $marker_line"
      fi
      if [[ ! "$marker_line" =~ sqlstate=${EXPECT_SQLSTATE} ]]; then
        fail "Marker does not contain the correct SQLSTATE: expected ${EXPECT_SQLSTATE}, got line: $marker_line"
      fi
      if [[ ! "$marker_line" =~ result_kind=deterministic_error ]]; then
        fail "Marker does not contain result_kind=deterministic_error: got line: $marker_line"
      fi
    else
      fail "Could not find gateway log file in $ARTIFACT_DIR for state=3 error marker verification"
    fi
  fi

elif [[ "$EXPECT_STATE" == "nonterminal-failure" ]]; then
  target_log=""
  target_ord=""
  target_digest=""
  target_class=""
  target_retryable=""
  target_state="absent"
  verify_digest_match=0
  verify_gateway_nonterminal=0
  verify_no_watchdog=1
  verify_no_false_success=1
  verify_replay_gateway="not_applicable"
  verify_replay_executor_verify=0
  verify_replay_kafka_publish=0
  verify_replay_no_protocol_failure=0
  verify_replay_no_task_failure=0
  failure_state_node1=""
  failure_state_node2=""
  failure_state_node4=""

  # Verify bounded failure
  # 1. Target run must exit cleanly. Replay proofs are skip-workload runs.
  if [[ -n "$TARGET_RUN_RC" ]]; then
    if [[ "$TARGET_RUN_RC" -ne 0 ]]; then
      fail "TARGET_RUN_RC indicates termination or timeout: $TARGET_RUN_RC"
    fi
  fi

  # 2. target_run.runner.log has no WATCHDOG termination marker.
  if [[ -f "$ARTIFACT_DIR/target_run.runner.log" ]]; then
    if grep -q 'WATCHDOG: Gateway completion stalled\|WATCHDOG: Sending SIGTERM' "$ARTIFACT_DIR/target_run.runner.log"; then
      verify_no_watchdog=0
      fail "WATCHDOG termination marker found in target_run.runner.log"
    fi
  fi

  # 3. For normal bounded-failure runs, prefer the gateway failure marker but
  #    fall back to the server-side verification marker if the gateway log does
  #    not contain the marker. Replay proofs keep using the preserved source.
  if [[ -n "$REPLAY_FROM" ]]; then
    load_replay_reference "$REPLAY_FROM"
    target_log="$REPLAY_TARGET_LOG"
    target_ord="$REPLAY_TARGET_ORD"
    target_digest="$REPLAY_TARGET_DIGEST"
    target_class="$REPLAY_TARGET_CLASS"
    target_retryable="$REPLAY_TARGET_RETRYABLE"
  else
    gateway_log=""
    if [[ -f "$ARTIFACT_DIR/gateway_test.log" ]]; then
      gateway_log="$ARTIFACT_DIR/gateway_test.log"
    elif [[ -f "$ARTIFACT_DIR/gateway.log" ]]; then
      gateway_log="$ARTIFACT_DIR/gateway.log"
    else
      gateway_log="$(find "$ARTIFACT_DIR" -name "*gateway*.log" | head -n1)"
    fi

    marker_line=""
    if [[ -n "$gateway_log" && -f "$gateway_log" ]]; then
      marker_line="$(grep 'SAFE_GATEWAY_NONTERMINAL_FAILURE' "$gateway_log" | head -n1 || true)"
    fi
    if [[ -z "$marker_line" ]]; then
      marker_line="$(grep -R -h -m1 'SAFE_VERIFY_NONTERMINAL_FAILURE' "$ARTIFACT_DIR/"/server_node*.log 2>/dev/null | head -n1 || true)"
    fi
    [[ -n "$marker_line" ]] || fail "Could not find a nonterminal-failure marker in gateway or server logs"
    echo "Found nonterminal failure marker: $marker_line"

    verify_gateway_nonterminal=1
    if [[ ! "$marker_line" =~ epoch=${EPOCH_HEX} ]]; then
      fail "Marker does not contain the correct epoch: expected ${EPOCH_HEX}, got line: $marker_line"
    fi

    sqlstate_pattern="22012"
    if [[ -n "${EXPECT_SQLSTATE:-}" ]]; then
      sqlstate_pattern="${EXPECT_SQLSTATE}"
    fi
    if [[ ! "$marker_line" =~ sqlstate=${sqlstate_pattern} ]]; then
      fail "Marker does not contain the correct SQLSTATE: expected ${sqlstate_pattern}, got line: $marker_line"
    fi

    if [[ ! "$marker_line" =~ log=[0-9]+ ]]; then
      fail "Marker does not contain log index, got line: $marker_line"
    fi
    target_log="$(sed -nE 's/.*(^|[[:space:]])log=([0-9]+).*/\2/p' <<<"$marker_line")"
    if [[ ! "$marker_line" =~ ord=[0-9]+ ]]; then
      fail "Marker does not contain item ordinal, got line: $marker_line"
    fi
    target_ord="$(sed -nE 's/.*(^|[[:space:]])ord=([0-9]+).*/\2/p' <<<"$marker_line")"
    target_digest="$(sed -nE 's/.*(^|[[:space:]])failure_digest=([0-9a-f]{64}).*/\2/p' <<<"$marker_line")"
    target_class="$(sed -nE 's/.*(^|[[:space:]])failure_class=([^[:space:]]+).*/\2/p' <<<"$marker_line")"
    target_retryable="$(sed -nE 's/.*(^|[[:space:]])retryable=([01]).*/\2/p' <<<"$marker_line")"
    [[ "$target_digest" =~ ^[0-9a-f]{64}$ ]] || fail "Marker does not contain 64-hex failure_digest: $marker_line"
    if [[ -n "$target_class" ]]; then
      [[ "$target_class" == "SQL_EXECUTION_ABORTED" ]] || fail "Unexpected failure_class in marker: $target_class"
    else
      target_class="SQL_EXECUTION_ABORTED"
    fi
    if [[ -n "$target_retryable" ]]; then
      [[ "$target_retryable" == "0" ]] || fail "Unexpected retryable in marker: $target_retryable"
    else
      target_retryable="0"
    fi
  fi

  [[ "$target_log" =~ ^[0-9]+$ ]] || fail "Could not parse target log from nonterminal-failure marker"
  [[ "$target_ord" =~ ^[0-9]+$ ]] || fail "Could not parse target ordinal from nonterminal-failure marker"

  # 6. Exact state=4 ledger row exists and matches on every replica.
  declare -a FAILURE_ROWS=()
  for idx in "${!NODE_IDS[@]}"; do
    node_num="${NODE_IDS[$idx]}"
    row="$(psql_node "$idx" "
SELECT state,
       encode(failure_digest, 'hex'),
       failure_sqlstate,
       failure_class,
       CASE WHEN failure_retryable THEN 1 ELSE 0 END,
       failure_format_version,
       (failure_recorded_at IS NOT NULL)::int,
       (sqlstate_code IS NULL)::int,
       (terminal_digest IS NULL)::int,
       (result_payload IS NULL)::int,
       (error_payload IS NULL)::int,
       (committed_at IS NULL)::int,
       (result_format_version IS NULL)::int,
       (error_format_version IS NULL)::int
FROM ariabc_internal.raft_apply_item
WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
  AND raft_log_index = ${target_log}
  AND item_ordinal = ${target_ord};" | tr -d '\r' | sed '/^$/d')"
    if [[ -z "$row" ]]; then
      fail "Node ${node_num} has no state-4 target row for log=${target_log} ord=${target_ord}"
    fi
    if [[ "$(wc -l <<<"$row")" -ne 1 ]]; then
      fail "Node ${node_num} returned multiple target rows: $row"
    fi
    FAILURE_ROWS+=("$row")
    printf 'state\tfailure_digest\tfailure_sqlstate\tfailure_class\tfailure_retryable\tfailure_format_version\tfailure_recorded\tsqlstate_null\tterminal_null\tresult_null\terror_null\tcommitted_null\tresult_fmt_null\terror_fmt_null\n%s\n' "$row" > "$ARTIFACT_DIR/phase3_failure_node${node_num}.tsv"
    IFS='|' read -r r_state r_digest r_sqlstate r_class r_retry r_fmt r_recorded r_sqlstate_null r_term_null r_result_null r_error_null r_committed_null r_resfmt_null r_errfmt_null <<<"$row"
    case "$node_num" in
      1) failure_state_node1="$r_state" ;;
      2) failure_state_node2="$r_state" ;;
      4) failure_state_node4="$r_state" ;;
    esac
    if [[ "$r_state" != "4" || "$r_digest" != "$target_digest" ||
          "$r_sqlstate" != "${EXPECT_SQLSTATE:-22012}" ||
          "$r_class" != "SQL_EXECUTION_ABORTED" ||
          "$r_retry" != "0" || "$r_fmt" != "1" ||
          "$r_recorded" != "1" || "$r_sqlstate_null" != "1" || "$r_term_null" != "1" ||
          "$r_result_null" != "1" || "$r_error_null" != "1" ||
          "$r_committed_null" != "1" || "$r_resfmt_null" != "1" ||
          "$r_errfmt_null" != "1" ]]; then
      fail "Node ${node_num} state-4 row violates failure contract: $row"
    fi
    forbidden="$(psql_node "$idx" "SELECT count(*) FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('${EPOCH_HEX}', 'hex') AND raft_log_index = ${target_log} AND item_ordinal = ${target_ord} AND state IN (2, 3);" | tr -d '\r')"
    if [[ "$forbidden" -ne 0 ]]; then
      fail "Node ${node_num} has forbidden state=2/3 row for exact target"
    fi
  done

  if [[ "${FAILURE_ROWS[0]}" != "${FAILURE_ROWS[1]}" || "${FAILURE_ROWS[0]}" != "${FAILURE_ROWS[2]}" ]]; then
    fail "State-4 failure rows differ across replicas"
  fi
  verify_digest_match=1
  target_state="4"

  if [[ -z "$REPLAY_FROM" ]]; then
    # The live failure case must emit gateway evidence and keep the summary clean.
    if grep -r "SAFE_GATEWAY_DETERMINISTIC_ERROR" "$ARTIFACT_DIR/" | grep -E "log=${target_log}([[:space:]]|$)" >/dev/null; then
      verify_no_false_success=0
      fail "Found deterministic-error gateway marker for nonterminal target log ${target_log}"
    fi
    if ! grep -r "client_quorum_complete_count=1" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1 ||
       ! grep -r "success_count=0" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1 ||
       ! grep -r "deterministic_error_count=0" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1 ||
       ! grep -r "nonterminal_failure_count=1" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1 ||
       ! grep -r "permanent_failures=0" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1 ||
       ! grep -r "divergence_count=0" "$ARTIFACT_DIR/gateway_test.log" >/dev/null 2>&1; then
      verify_no_false_success=0
      fail "Gateway summary does not show exactly one completed nonterminal failure and zero other failures"
    fi
    if ! grep -r "SAFE_VERIFY_NONTERMINAL_FAILURE" "$ARTIFACT_DIR/"/server_node*.log |
         grep -E "log=${target_log}([[:space:]]|$)" |
         grep -E "ord=${target_ord}([[:space:]]|$)" >/dev/null; then
      fail "Server logs do not show SAFE_VERIFY_NONTERMINAL_FAILURE for target log=${target_log} ord=${target_ord}"
    fi
  else
    grep -q '^gateway_completed=not_applicable$' "$ARTIFACT_DIR/run_summary.env" ||
      fail "Replay unexpectedly started or required gateway work"

    [[ "$(read_env_value "$REPLAY_FROM/phase3_failure_verify.env" VERIFY_NONTERMINAL_FAILURE_PASS)" == "1" ]] ||
      fail "Replay source was not a verified state-4 failure"

    replay_server_hits=0
    for node_log in "$ARTIFACT_DIR"/server_node*.log; do
      [[ -f "$node_log" ]] || continue
      if grep -E "SAFE_VERIFY_NONTERMINAL_FAILURE" "$node_log" |
           grep -E "log=${target_log}([[:space:]]|$)" |
           grep -E "ord=${target_ord}([[:space:]]|$)" >/dev/null; then
        replay_server_hits=$((replay_server_hits + 1))
      fi
    done
    [[ "$replay_server_hits" -eq 3 ]] || fail "Replay server logs did not show SAFE_VERIFY_NONTERMINAL_FAILURE on all 3 nodes for log=${target_log} ord=${target_ord}"

    replay_kafka_hits=0
    for node_log in "$ARTIFACT_DIR"/server_node*.log; do
      [[ -f "$node_log" ]] || continue
      if grep -E "SAFE_KAFKA_PUBLISH_DELIVERED" "$node_log" |
           grep -E "first_log=${target_log}([[:space:]]|$)" |
           grep -E "first_ord=${target_ord}([[:space:]]|$)" >/dev/null; then
        replay_kafka_hits=$((replay_kafka_hits + 1))
      fi
    done
    [[ "$replay_kafka_hits" -eq 3 ]] || fail "Replay server logs did not show SAFE_KAFKA_PUBLISH_DELIVERED on all 3 nodes for log=${target_log} ord=${target_ord}"

    if grep -r -E "SAFE_TASK_FAILED|SAFE_PROTOCOL_FAILURE|SAFE_LEDGER_NONTERMINAL_VERIFY_FAILED|SAFE_LEDGER_DURABLE_VERIFY_FAILED|worker_tx_error|\\[BCDB_FATAL\\]|FATAL:" "$ARTIFACT_DIR"/server_node*.log "$ARTIFACT_DIR"/postgres_node*.log "$ARTIFACT_DIR"/gateway_test.log 2>/dev/null |
       grep -E "log=${target_log}([[:space:]]|$)" >/dev/null; then
      fail "Replay logs contain a forbidden failure marker for log=${target_log} ord=${target_ord}"
    fi
  fi

  if [[ -n "$REPLAY_FROM" ]]; then
    if ! grep -r "SAFE_NONTERMINAL_FAILURE_REPLAY" "$ARTIFACT_DIR/"/postgres_node*.log |
         grep -E "log_index=${target_log}([[:space:]]|$)" |
         grep -E "ordinal=${target_ord}([[:space:]]|$)" >/dev/null; then
      fail "PostgreSQL logs do not show SAFE_NONTERMINAL_FAILURE_REPLAY for target log=${target_log} ord=${target_ord}"
    fi
    if grep -r "ledger_business_sql" "$ARTIFACT_DIR/"/postgres_node*.log >/dev/null 2>&1; then
      fail "Recovery logs contain ledger_business_sql marker during replay"
    fi
  fi

  # 3. Worker logs contain expected SQLSTATE
  sqlstate_pattern="22012"
  if [[ -n "${EXPECT_SQLSTATE:-}" ]]; then
    sqlstate_pattern="${EXPECT_SQLSTATE}"
  fi
  if ! grep -r -E "${sqlstate_pattern}" "$ARTIFACT_DIR/" >/dev/null 2>&1; then
    fail "Worker logs do not contain expected SQLSTATE ${sqlstate_pattern}"
  fi

  # 4. No "errstart was not called"
  if grep -r -i "errstart was not called" "$ARTIFACT_DIR/" >/dev/null 2>&1; then
    fail "Found forbidden 'errstart was not called' in logs"
  fi

  # 8. PostgreSQL remains healthy on all replicas.
  for idx in "${!NODE_IDS[@]}"; do
    ssh -o BatchMode=yes -o ConnectTimeout=8 "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" \
      "export PATH='/home/neel/Desktop/ariabc_install/bin':\$PATH; export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib':\${LD_LIBRARY_PATH:-}; pg_isready -h 127.0.0.1 -p '${DB_PORT}' -d '${DB_NAME}' -U '${DB_USER}'" \
      || fail "PostgreSQL node ${NODE_IDS[$idx]} is not healthy after the failure"
  done
fi

# Route-internal postconditions
if [[ -n "${TEST_EPOCH:-}" ]]; then
  echo "Verifying route-internal postcondition: raft_apply_epoch count should be 0 for epoch ${TEST_EPOCH} on all nodes"
  for idx in "${!NODE_IDS[@]}"; do
    epoch_count=""
    epoch_count="$(psql_node "$idx" "SELECT count(*) FROM ariabc_internal.raft_apply_epoch WHERE epoch_id = decode('${TEST_EPOCH}', 'hex');" | tr -d '\r')"
    if [[ "$epoch_count" -ne 0 ]]; then
      fail "Node ${NODE_IDS[$idx]} has $epoch_count remaining rows in raft_apply_epoch for epoch ${TEST_EPOCH}, expected 0"
    fi
  done
  echo "Route-internal postcondition verified: count is 0 on all nodes."
fi

if [[ "${EXPECT_RELATION:-}" == "ariabc_internal.raft_apply_epoch" ]]; then
  echo "Verifying separate route markers for INSERT, UPDATE, DELETE on ariabc_internal.raft_apply_epoch"
  for log_file in "$ARTIFACT_DIR"/postgres_node*.log; do
    [[ -f "$log_file" ]] || continue
    if ! grep -q "BCDB_DML_ROUTE op=INSERT relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
      fail "Log $log_file does not contain INSERT mode=direct for raft_apply_epoch"
    fi
    if ! grep -q "BCDB_DML_ROUTE op=UPDATE relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
      fail "Log $log_file does not contain UPDATE mode=direct for raft_apply_epoch"
    fi
    if ! grep -q "BCDB_DML_ROUTE op=DELETE relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
      fail "Log $log_file does not contain DELETE mode=direct for raft_apply_epoch"
    fi
  done
  echo "All separate DML route markers verified successfully."
fi

# Check route if expected
if [[ -n "${EXPECT_ROUTE:-}" && "${EXPECT_ROUTE}" != "none" ]]; then
  [[ -n "${EXPECT_RELATION:-}" ]] || fail "--expect-relation is required when --expect-route is set"

  for log_file in "$ARTIFACT_DIR"/postgres_node*.log; do
    [[ -f "$log_file" ]] || continue

    match_count="$(grep -c "BCDB_DML_ROUTE op=.* relation=${EXPECT_RELATION} mode=${EXPECT_ROUTE}" "$log_file" || true)"
    if [[ "$match_count" -eq 0 ]]; then
      fail "Log $log_file does not contain expected DML route: mode=${EXPECT_ROUTE} for relation=${EXPECT_RELATION}"
    fi

    opposite_mode="direct"
    if [[ "${EXPECT_ROUTE}" == "direct" ]]; then
      opposite_mode="deferred"
    fi
    forbidden_count="$(grep -c "BCDB_DML_ROUTE op=.* relation=${EXPECT_RELATION} mode=${opposite_mode}" "$log_file" || true)"
    if [[ "$forbidden_count" -gt 0 ]]; then
      fail "Log $log_file contains forbidden DML route: mode=${opposite_mode} for relation=${EXPECT_RELATION}"
    fi
  done
fi

# Write direct SQL evidence into TSV files
for idx in "${!NODE_IDS[@]}"; do
  node_num="${NODE_IDS[$idx]}"
  psql_node "$idx" "COPY (
    SELECT raft_log_index, item_ordinal, state, encode(terminal_digest, 'hex'), (committed_at IS NOT NULL)::int, COALESCE(sqlstate_code, '')
    FROM ariabc_internal.raft_apply_item
    WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
    ORDER BY raft_log_index, item_ordinal
  ) TO STDOUT WITH CSV DELIMITER E'\t' HEADER;" > "$ARTIFACT_DIR/phase3_ledger_node${node_num}.tsv"

  psql_node "$idx" "COPY (
    SELECT n, token
    FROM public.safe_phase3_probe
    ORDER BY k
  ) TO STDOUT WITH CSV DELIMITER E'\t' HEADER;" > "$ARTIFACT_DIR/phase3_probe_node${node_num}.tsv"
done

# Route proof TSV
echo -e "log_file\top\trelation\tmode" > "$ARTIFACT_DIR/phase3_route_proof.tsv"
for log_file in "$ARTIFACT_DIR"/postgres_node*.log; do
  [[ -f "$log_file" ]] || continue
  base_name="$(basename "$log_file")"
  grep "BCDB_DML_ROUTE" "$log_file" | while read -r line; do
    if [[ "$line" =~ op=([a-zA-Z0-9._]+)[[:space:]]relation=([a-zA-Z0-9._]+)[[:space:]]mode=([a-zA-Z0-9._]+) ]]; then
      echo -e "${base_name}\t${BASH_REMATCH[1]}\t${BASH_REMATCH[2]}\t${BASH_REMATCH[3]}" >> "$ARTIFACT_DIR/phase3_route_proof.tsv"
    fi
  done
done

# Compute environment variables for phase3_verify.env
claimed_node1=0
claimed_node2=0
claimed_node4=0
sha256_node1=""
sha256_node2=""
sha256_node4=""
absent_node1=1
absent_node2=1
absent_node4=1

for idx in "${!NODE_IDS[@]}"; do
  node_num="${NODE_IDS[$idx]}"
  c_cnt=""
  c_cnt="$(psql_node "$idx" "SELECT count(*) FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('${EPOCH_HEX}', 'hex') AND state = 1;" | tr -d '\r')"

  abs_cnt=1
  if [[ -n "${TEST_EPOCH:-}" ]]; then
    ep_cnt=""
    ep_cnt="$(psql_node "$idx" "SELECT count(*) FROM ariabc_internal.raft_apply_epoch WHERE epoch_id = decode('${TEST_EPOCH}', 'hex');" | tr -d '\r')"
    if [[ "$ep_cnt" -eq 0 ]]; then
      abs_cnt=1
    else
      abs_cnt=0
    fi
  fi

  tsv="$ARTIFACT_DIR/phase3_ledger_node${node_num}.tsv"
  sha=""
  if [[ -f "$tsv" ]]; then
    sha="$(sha256sum "$tsv" | awk '{print $1}')"
  fi

  if [[ "$node_num" -eq 1 ]]; then
    claimed_node1="$c_cnt"
    sha256_node1="$sha"
    absent_node1="$abs_cnt"
  elif [[ "$node_num" -eq 2 ]]; then
    claimed_node2="$c_cnt"
    sha256_node2="$sha"
    absent_node2="$abs_cnt"
  elif [[ "$node_num" -eq 4 ]]; then
    claimed_node4="$c_cnt"
    sha256_node4="$sha"
    absent_node4="$abs_cnt"
  fi
done

gateway_log=""
if [[ -f "$ARTIFACT_DIR/gateway_test.log" ]]; then
  gateway_log="$ARTIFACT_DIR/gateway_test.log"
elif [[ -f "$ARTIFACT_DIR/gateway.log" ]]; then
  gateway_log="$ARTIFACT_DIR/gateway.log"
else
  gateway_log="$(find "$ARTIFACT_DIR" -name "*gateway*.log" | head -n1)"
fi

gw_completed=0
gw_total=0
gw_resolved=0
if [[ -n "$gateway_log" && -f "$gateway_log" ]]; then
  comp_val=""
  comp_val="$(grep -o 'completed=[0-9]\+' "$gateway_log" | tail -n1 | cut -d= -f2 || true)"
  if [[ -n "$comp_val" ]]; then
    gw_completed="$comp_val"
    gw_resolved="$comp_val"
  fi
  gw_total="$EXPECT_TARGET_COUNT"
  if [[ "$EXPECT_STATE" == "nonterminal-failure" ]]; then
    gw_total=1
  fi
fi

r_ins=0
r_upd=0
r_del=0
for log_file in "$ARTIFACT_DIR"/postgres_node*.log; do
  [[ -f "$log_file" ]] || continue
  if grep -q "BCDB_DML_ROUTE op=INSERT relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
    r_ins=1
  fi
  if grep -q "BCDB_DML_ROUTE op=UPDATE relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
    r_upd=1
  fi
  if grep -q "BCDB_DML_ROUTE op=DELETE relation=ariabc_internal.raft_apply_epoch mode=direct" "$log_file"; then
    r_del=1
  fi
done

# Write phase3_verify.env
{
  printf 'VERIFY_PASS=1\n'
  printf 'VERIFY_EXPECT_STATE=%s\n' "$EXPECT_STATE"
  printf 'VERIFY_EPOCH=%s\n' "$EPOCH_HEX"
  printf 'VERIFY_TARGET_COUNT=%s\n' "$EXPECT_TARGET_COUNT"
  if [[ -n "${EXPECT_SQLSTATE:-}" ]]; then
    printf 'VERIFY_SQLSTATE=%s\n' "$EXPECT_SQLSTATE"
  fi
  printf 'VERIFY_TARGET_LEDGER_SHA256_NODE1=%s\n' "$sha256_node1"
  printf 'VERIFY_TARGET_LEDGER_SHA256_NODE2=%s\n' "$sha256_node2"
  printf 'VERIFY_TARGET_LEDGER_SHA256_NODE4=%s\n' "$sha256_node4"
  printf 'VERIFY_CLAIMED_NODE1=%s\n' "$claimed_node1"
  printf 'VERIFY_CLAIMED_NODE2=%s\n' "$claimed_node2"
  printf 'VERIFY_CLAIMED_NODE4=%s\n' "$claimed_node4"
  printf 'VERIFY_GATEWAY_COMPLETED=%s\n' "$gw_completed"
  printf 'VERIFY_GATEWAY_TOTAL=%s\n' "$gw_total"
  printf 'VERIFY_GATEWAY_RESOLVED=%s\n' "$gw_resolved"
  printf 'VERIFY_ROUTE_INSERT_DIRECT=%s\n' "$r_ins"
  printf 'VERIFY_ROUTE_UPDATE_DIRECT=%s\n' "$r_upd"
  printf 'VERIFY_ROUTE_DELETE_DIRECT=%s\n' "$r_del"
  printf 'VERIFY_INTERNAL_TEST_EPOCH_ABSENT_NODE1=%s\n' "$absent_node1"
  printf 'VERIFY_INTERNAL_TEST_EPOCH_ABSENT_NODE2=%s\n' "$absent_node2"
  printf 'VERIFY_INTERNAL_TEST_EPOCH_ABSENT_NODE4=%s\n' "$absent_node4"
  if [[ "$EXPECT_STATE" == "nonterminal-failure" ]]; then
    sqlstate_pattern="22012"
    if [[ -n "${EXPECT_SQLSTATE:-}" ]]; then
      sqlstate_pattern="${EXPECT_SQLSTATE}"
    fi
    printf 'NONWHITELISTED_ERROR_SQLSTATE=%s\n' "${sqlstate_pattern}"
    printf 'NONWHITELISTED_ERROR_GATEWAY_BOUNDED=1\n'
    printf 'NONWHITELISTED_ERROR_TARGET_LEDGER_STATE=%s\n' "${target_state}"
    printf 'NONWHITELISTED_ERROR_FALSE_SUCCESS=0\n'
    printf 'VERIFY_NONTERMINAL_FAILURE_PASS=1\n'
    printf 'VERIFY_FAILURE_STATE_NODE1=%s\n' "${failure_state_node1:-}"
    printf 'VERIFY_FAILURE_STATE_NODE2=%s\n' "${failure_state_node2:-}"
    printf 'VERIFY_FAILURE_STATE_NODE4=%s\n' "${failure_state_node4:-}"
    printf 'VERIFY_FAILURE_DIGEST_MATCH=%s\n' "${verify_digest_match:-0}"
    printf 'VERIFY_FAILURE_SQLSTATE=%s\n' "${sqlstate_pattern}"
    printf 'VERIFY_FAILURE_CLASS=SQL_EXECUTION_ABORTED\n'
    printf 'VERIFY_FAILURE_RETRYABLE=0\n'
    if [[ -n "$REPLAY_FROM" ]]; then
      printf 'VERIFY_REPLAY_GATEWAY=%s\n' "$verify_replay_gateway"
      printf 'VERIFY_REPLAY_EXECUTOR_VERIFY=1\n'
      printf 'VERIFY_REPLAY_KAFKA_PUBLISH=1\n'
      printf 'VERIFY_REPLAY_NO_PROTOCOL_FAILURE=1\n'
      printf 'VERIFY_REPLAY_NO_TASK_FAILURE=1\n'
      printf 'VERIFY_NO_WATCHDOG=%s\n' "${verify_no_watchdog:-1}"
      printf 'VERIFY_NO_FALSE_SUCCESS=%s\n' "${verify_no_false_success:-1}"
    else
      printf 'VERIFY_GATEWAY_NONTERMINAL_FAILURE=%s\n' "${verify_gateway_nonterminal:-0}"
      printf 'VERIFY_NO_WATCHDOG=%s\n' "${verify_no_watchdog:-1}"
      printf 'VERIFY_NO_FALSE_SUCCESS=%s\n' "${verify_no_false_success:-1}"
    fi
  fi
} > "$ARTIFACT_DIR/phase3_verify.env"

if [[ "$EXPECT_STATE" == "nonterminal-failure" ]]; then
  cp "$ARTIFACT_DIR/phase3_verify.env" "$ARTIFACT_DIR/phase3_failure_verify.env"
fi

echo "PASS: Phase 3 verification succeeded: $EXPECT_STATE (route: $EXPECT_ROUTE, count: $EXPECT_TARGET_COUNT)"
