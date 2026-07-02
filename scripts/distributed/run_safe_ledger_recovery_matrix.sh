#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

REMOTE_HARNESS="$SCRIPT_DIR/test_remote_raft_recovery.sh"
STRICT_VERIFIER="$SCRIPT_DIR/verify_safe_recovery_case.sh"

CASES="A B C D E F G"
NODES="1 2 4"
REPEATS=3
PROBE_TEMPLATE="scripts/distributed/safe_recovery_probe.sql.in"
SETUP_SQL="scripts/distributed/safe_recovery_probe_setup.sql"
ARTIFACT_ROOT="scripts/bench_full_results/recovery_matrix"
EXPECT_STATE="2"

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/run_safe_ledger_recovery_matrix.sh \
    [--cases "A B C D E F G"] \
    [--nodes "1 2 4"] \
    [--repeats <N>] \
    [--probe-template <repo-relative path>] \
    [--setup-sql <repo-relative path>] \
    [--artifact-root <directory>] \
    [--expect-state <2|3>]
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

read_env_value() {
  local file="$1"
  local key="$2"
  if [[ -f "$file" ]]; then
    sed -n "s/^${key}=//p" "$file" | tail -n1
  fi
}

validate_cases() {
  local token
  for token in $1; do
    case "$token" in
      A|B|C|D|E|F|G) ;;
      *) fail "invalid case token: $token" ;;
    esac
  done
}

validate_nodes() {
  local token
  for token in $1; do
    case "$token" in
      1|2|4) ;;
      *) fail "invalid node token: $token" ;;
    esac
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cases) CASES="${2:?missing value for --cases}"; shift 2 ;;
    --nodes) NODES="${2:?missing value for --nodes}"; shift 2 ;;
    --repeats) REPEATS="${2:?missing value for --repeats}"; shift 2 ;;
    --probe-template) PROBE_TEMPLATE="${2:?missing value for --probe-template}"; shift 2 ;;
    --setup-sql) SETUP_SQL="${2:?missing value for --setup-sql}"; shift 2 ;;
    --artifact-root) ARTIFACT_ROOT="${2:?missing value for --artifact-root}"; shift 2 ;;
    --expect-state) EXPECT_STATE="${2:?missing value for --expect-state}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || fail "--repeats must be a positive integer"
[[ "$EXPECT_STATE" =~ ^[23]$ ]] || fail "--expect-state must be 2 or 3"
validate_cases "$CASES"
validate_nodes "$NODES"

PROBE_TEMPLATE_ABS="$(resolve_repo_path "$PROBE_TEMPLATE")"
SETUP_SQL_ABS="$(resolve_repo_path "$SETUP_SQL")"
if [[ "$PROBE_TEMPLATE" == /* ]]; then
  PROBE_TEMPLATE="${PROBE_TEMPLATE_ABS#"$REPO_ROOT"/}"
fi
if [[ "$SETUP_SQL" == /* ]]; then
  SETUP_SQL="${SETUP_SQL_ABS#"$REPO_ROOT"/}"
fi

if [[ "$ARTIFACT_ROOT" != /* ]]; then
  ARTIFACT_ROOT="$REPO_ROOT/$ARTIFACT_ROOT"
fi
mkdir -p "$ARTIFACT_ROOT"

echo "Running static shell validation..."
git diff --check
bash -n "$REMOTE_HARNESS"
bash -n "$SCRIPT_DIR/run_safe_ledger_recovery_matrix.sh"
bash -n "$SCRIPT_DIR/verify_safe_ledger_run.sh"
bash -n "$STRICT_VERIFIER"

echo "Running build gate..."
make -j"$(nproc)"
make install
cmake --build "$REPO_ROOT/ariabc_pg/build" -j"$(nproc)"
ctest --test-dir "$REPO_ROOT/ariabc_pg/build" --output-on-failure

declare -A baseline_values=()
baseline_ready=0
overall_fail=0
summary_csv="$ARTIFACT_ROOT/summary.csv"
summary_md="$ARTIFACT_ROOT/summary.md"
tmp_summary="$ARTIFACT_ROOT/.summary.rows"
rm -f "$summary_csv" "$summary_md" "$tmp_summary"

{
  printf 'case,node,repeat,epoch,failpoint_fired,restart_confirmed,probe_node1,probe_node2,probe_node4,claimed_node1,claimed_node2,claimed_node4,terminal_state,terminal_digest_match,gateway_completed,target_sql_match,probe_token_node1,probe_token_node2,probe_token_node4,source_fingerprint,uncommitted_diff_sha256,binary_provenance_pass,recovery_harness_sha256,result,artifact_dir\n'
} > "$summary_csv"

run_entry() {
  local case_name="$1"
  local node_id="$2"
  local repeat_idx="$3"
  local epoch_hex="$4"
  local artifact_dir="$5"
  local expected_token="safe_recovery_${epoch_hex}"

  mkdir -p "$artifact_dir"
  local rc=0
  set +e
  SKIP_BUILD="${SKIP_BUILD:-0}" \
    "$REMOTE_HARNESS" \
      --case "$case_name" \
      --node-id "$node_id" \
      --probe-template "$PROBE_TEMPLATE" \
      --setup-sql "$SETUP_SQL" \
      --artifact-dir "$artifact_dir" \
      --epoch "$epoch_hex" \
      --expect-state "$EXPECT_STATE" \
      --skip-post-verify
  rc=$?
  set -e

  if [[ "$rc" -ne 0 ]]; then
    overall_fail=1
  fi

  local failpoint_env="$artifact_dir/failpoint_proof.env"
  local verify_env="$artifact_dir/recovery_verify.env"
  local final_run_env="$artifact_dir/final_run/run_meta.env"
  local run_summary_env="$artifact_dir/final_run/run_summary.env"

  local failpoint_fired="0"
  local restart_confirmed="0"
  local probe_node1=""
  local probe_node2=""
  local probe_node4=""
  local claimed_node1=""
  local claimed_node2=""
  local claimed_node4=""
  local terminal_state=""
  local terminal_digest_match="0"
  local gateway_completed="0"
  local target_sql_match="0"
  local probe_token_node1=""
  local probe_token_node2=""
  local probe_token_node4=""
  local verify_pass="0"
  local current_recovery_harness_sha=""

  if [[ -f "$failpoint_env" ]]; then
    failpoint_fired="$(read_env_value "$failpoint_env" FAILPOINT_FIRED)"
    restart_confirmed="$(read_env_value "$failpoint_env" RESTART_CONFIRMED)"
    target_sql_match="$(read_env_value "$failpoint_env" TARGET_SQL_MATCH)"
    current_recovery_harness_sha="$(read_env_value "$failpoint_env" RECOVERY_HARNESS_SHA256)"
  fi
  if [[ -f "$verify_env" ]]; then
    verify_pass="$(read_env_value "$verify_env" VERIFY_PASS)"
    probe_node1="$(read_env_value "$verify_env" VERIFY_PROBE_VALUE_NODE1)"
    probe_node2="$(read_env_value "$verify_env" VERIFY_PROBE_VALUE_NODE2)"
    probe_node4="$(read_env_value "$verify_env" VERIFY_PROBE_VALUE_NODE4)"
    probe_token_node1="$(read_env_value "$verify_env" VERIFY_PROBE_TOKEN_NODE1)"
    probe_token_node2="$(read_env_value "$verify_env" VERIFY_PROBE_TOKEN_NODE2)"
    probe_token_node4="$(read_env_value "$verify_env" VERIFY_PROBE_TOKEN_NODE4)"
    claimed_node1="$(read_env_value "$verify_env" VERIFY_CLAIMED_NODE1)"
    claimed_node2="$(read_env_value "$verify_env" VERIFY_CLAIMED_NODE2)"
    claimed_node4="$(read_env_value "$verify_env" VERIFY_CLAIMED_NODE4)"
    terminal_state="$(read_env_value "$verify_env" VERIFY_TERMINAL_STATE)"
    terminal_digest_match="$(read_env_value "$verify_env" VERIFY_TERMINAL_ROW_MATCH)"
  fi
  if [[ -f "$run_summary_env" ]]; then
    local permanent_failures divergence_count workload_transactions completed summary_gateway
    summary_gateway="$(read_env_value "$run_summary_env" gateway_completed)"
    if [[ "$summary_gateway" == "not_applicable" ]]; then
      gateway_completed="not_applicable"
    fi
    permanent_failures="$(read_env_value "$run_summary_env" permanent_failures)"
    divergence_count="$(read_env_value "$run_summary_env" divergence_count)"
    workload_transactions="$(read_env_value "$run_summary_env" workload_transactions)"
    completed="$(read_env_value "$run_summary_env" client_quorum_complete_count)"
    if [[ "$gateway_completed" != "not_applicable" && -z "$completed" && -f "$artifact_dir/final_run/gateway_test.log" ]]; then
      completed="$(grep -E '^PROGRESS_GATEWAY_DET\b' "$artifact_dir/final_run/gateway_test.log" 2>/dev/null | sed -n 's/.*completed=\([0-9]\+\).*/\1/p' | tail -n1)"
    fi
    if [[ "$gateway_completed" != "not_applicable" && -z "$completed" && -f "$artifact_dir/final_run/gateway_test.log" ]]; then
      completed="$(grep -E '^loaded [0-9]+ queries' "$artifact_dir/final_run/gateway_test.log" 2>/dev/null | sed -n 's/^loaded \([0-9]\+\) queries.*/\1/p' | tail -n1)"
    fi
    if [[ "$gateway_completed" != "not_applicable" &&
          "${completed:-}" == "${workload_transactions:-missing}" &&
          "${permanent_failures:-1}" == "0" &&
          "${divergence_count:-1}" == "0" ]]; then
      gateway_completed="1"
    fi
  fi

  local current_git_head=""
  local current_git_dirty=""
  local current_source_fingerprint=""
  local current_diff_sha=""
  local current_gateway_sha=""
  local current_server_sha=""
  local current_postgres_sha=""
  local current_provenance_pass=""

  if [[ -f "$final_run_env" ]]; then
    current_git_head="$(read_env_value "$final_run_env" git_head)"
    current_git_dirty="$(read_env_value "$final_run_env" git_dirty)"
    current_source_fingerprint="$(read_env_value "$final_run_env" source_fingerprint)"
    current_gateway_sha="$(read_env_value "$final_run_env" ariabc_pg_gateway_sha256)"
    current_server_sha="$(read_env_value "$final_run_env" ariabc_pg_server_sha256)"
    current_postgres_sha="$(read_env_value "$final_run_env" postgres_sha256)"
    current_provenance_pass="$(read_env_value "$final_run_env" BINARY_PROVENANCE_PASS)"
  fi

  local patch_file="$artifact_dir/final_run/uncommitted_diff.patch"
  if [[ -f "$patch_file" ]]; then
    current_diff_sha="$(sha256sum "$patch_file" | awk '{print $1}')"
  fi

  local result="FAIL"
  if [[ "$rc" -eq 0 &&
        "${failpoint_fired:-0}" == "1" &&
        "${restart_confirmed:-0}" == "1" &&
        "${terminal_digest_match:-0}" == "1" &&
        "${target_sql_match:-0}" == "1" &&
        "${probe_node1:-}" == "1" &&
        "${probe_node2:-}" == "1" &&
        "${probe_node4:-}" == "1" &&
        "${claimed_node1:-}" == "0" &&
        "${claimed_node2:-}" == "0" &&
        "${claimed_node4:-}" == "0" &&
        "${verify_pass:-0}" == "1" &&
        "${terminal_state:-}" == "$EXPECT_STATE" &&
        "${probe_token_node1:-}" == "$expected_token" &&
        "${probe_token_node2:-}" == "$expected_token" &&
        "${probe_token_node4:-}" == "$expected_token" &&
        "${current_provenance_pass:-0}" == "1" &&
        ( "$case_name" != "A" ||
          "$gateway_completed" == "1" ||
          "$gateway_completed" == "not_applicable" ) ]]; then
    result="PASS"
  else
    overall_fail=1
  fi

  if [[ "$result" == "PASS" && "$baseline_ready" -eq 0 ]]; then
    baseline_values[git_head]="$current_git_head"
    baseline_values[git_dirty]="$current_git_dirty"
    baseline_values[source_fingerprint]="$current_source_fingerprint"
    baseline_values[diff_sha]="$current_diff_sha"
    baseline_values[gateway_sha]="$current_gateway_sha"
    baseline_values[server_sha]="$current_server_sha"
    baseline_values[postgres_sha]="$current_postgres_sha"
    baseline_values[provenance_pass]="$current_provenance_pass"
    baseline_values[recovery_harness_sha]="$current_recovery_harness_sha"
    baseline_ready=1
  fi

  if [[ "$baseline_ready" -eq 1 ]]; then
    [[ "$current_git_head" == "${baseline_values[git_head]}" ]] || fail "git_head changed during matrix: got '$current_git_head', expected '${baseline_values[git_head]}'"
    [[ "$current_git_dirty" == "${baseline_values[git_dirty]}" ]] || fail "git_dirty changed during matrix: got '$current_git_dirty', expected '${baseline_values[git_dirty]}'"
    [[ "$current_source_fingerprint" == "${baseline_values[source_fingerprint]}" ]] || fail "source_fingerprint changed during matrix: got '$current_source_fingerprint', expected '${baseline_values[source_fingerprint]}'"
    [[ "$current_diff_sha" == "${baseline_values[diff_sha]}" ]] || fail "uncommitted_diff.patch SHA changed during matrix: got '$current_diff_sha', expected '${baseline_values[diff_sha]}'"
    [[ "$current_gateway_sha" == "${baseline_values[gateway_sha]}" ]] || fail "gateway binary SHA changed during matrix: got '$current_gateway_sha', expected '${baseline_values[gateway_sha]}'"
    [[ "$current_server_sha" == "${baseline_values[server_sha]}" ]] || fail "server binary SHA changed during matrix: got '$current_server_sha', expected '${baseline_values[server_sha]}'"
    [[ "$current_postgres_sha" == "${baseline_values[postgres_sha]}" ]] || fail "postgres binary SHA changed during matrix: got '$current_postgres_sha', expected '${baseline_values[postgres_sha]}'"
    [[ "$current_provenance_pass" == "${baseline_values[provenance_pass]}" ]] || fail "BINARY_PROVENANCE_PASS changed during matrix: got '$current_provenance_pass', expected '${baseline_values[provenance_pass]}'"
    [[ "$current_recovery_harness_sha" == "${baseline_values[recovery_harness_sha]}" ]] || fail "recovery_harness_sha256 changed during matrix: got '$current_recovery_harness_sha', expected '${baseline_values[recovery_harness_sha]}'"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$case_name" \
    "$node_id" \
    "$repeat_idx" \
    "$epoch_hex" \
    "${failpoint_fired:-0}" \
    "${restart_confirmed:-0}" \
    "${probe_node1:-}" \
    "${probe_node2:-}" \
    "${probe_node4:-}" \
    "${claimed_node1:-}" \
    "${claimed_node2:-}" \
    "${claimed_node4:-}" \
    "${terminal_state:-}" \
    "${terminal_digest_match:-0}" \
    "${gateway_completed:-0}" \
    "${target_sql_match:-0}" \
    "${probe_token_node1:-}" \
    "${probe_token_node2:-}" \
    "${probe_token_node4:-}" \
    "${current_source_fingerprint:-}" \
    "${current_diff_sha:-}" \
    "${current_provenance_pass:-0}" \
    "${current_recovery_harness_sha:-}" \
    "$result" \
    "$artifact_dir" >> "$summary_csv"

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$case_name" \
    "$node_id" \
    "$repeat_idx" \
    "$epoch_hex" \
    "${failpoint_fired:-0}" \
    "${restart_confirmed:-0}" \
    "${probe_node1:-}" \
    "${probe_node2:-}" \
    "${probe_node4:-}" \
    "${claimed_node1:-}" \
    "${claimed_node2:-}" \
    "${claimed_node4:-}" \
    "${terminal_state:-}" \
    "${terminal_digest_match:-0}" \
    "${gateway_completed:-0}" \
    "${target_sql_match:-0}" \
    "${probe_token_node1:-}" \
    "${probe_token_node2:-}" \
    "${probe_token_node4:-}" \
    "${current_source_fingerprint:-}" \
    "${current_diff_sha:-}" \
    "${current_provenance_pass:-0}" \
    "${current_recovery_harness_sha:-}" \
    "$result" \
    "$artifact_dir" >> "$tmp_summary"
}

for case_name in $CASES; do
  for node_id in $NODES; do
    for repeat_idx in $(seq 1 "$REPEATS"); do
      epoch_hex="$(openssl rand -hex 32)"
      artifact_dir="$ARTIFACT_ROOT/case_${case_name}/node_${node_id}/repeat_${repeat_idx}"
      rm -rf "$artifact_dir"
      echo "Running case=$case_name node=$node_id repeat=$repeat_idx artifact=$artifact_dir"
      if [[ "$baseline_ready" -eq 1 ]]; then
        SKIP_BUILD=1 run_entry "$case_name" "$node_id" "$repeat_idx" "$epoch_hex" "$artifact_dir"
      else
        SKIP_BUILD=0 run_entry "$case_name" "$node_id" "$repeat_idx" "$epoch_hex" "$artifact_dir"
      fi
    done
  done
done

{
  printf '# Safe-ledger recovery matrix summary\n\n'
  printf '%s\n' '| case | node | repeat | epoch | failpoint_fired | restart_confirmed | probe_node1 | probe_node2 | probe_node4 | claimed_node1 | claimed_node2 | claimed_node4 | terminal_state | terminal_digest_match | gateway_completed | target_sql_match | probe_token_node1 | probe_token_node2 | probe_token_node4 | source_fingerprint | uncommitted_diff_sha256 | binary_provenance_pass | recovery_harness_sha256 | result | artifact_dir |'
  printf '%s\n' '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |'
  if [[ -f "$tmp_summary" ]]; then
    cat "$tmp_summary"
  fi
} > "$summary_md"

if [[ "$overall_fail" -ne 0 ]]; then
  echo "Recovery matrix completed with failures. See $summary_csv and $summary_md" >&2
  exit 1
fi

echo "Recovery matrix completed successfully"
