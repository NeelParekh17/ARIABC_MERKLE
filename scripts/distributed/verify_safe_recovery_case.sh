#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster_topology.sh"
CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"

ARTIFACT_DIR=""
EPOCH_HEX=""
RAFT_LOG_INDEX=""
ITEM_ORDINAL=""
EXPECT_STATE=""
EXPECT_PROBE_TOKEN=""
PROBE_KEY=1

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/verify_safe_recovery_case.sh \
    --artifact-dir <directory> \
    --epoch <64 lowercase hex characters> \
    --raft-log-index <positive integer> \
    --item-ordinal <non-negative integer> \
    --expect-state <2|3> \
    --expect-probe-token <token> \
    [--probe-key <integer>]
EOF
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-dir) ARTIFACT_DIR="${2:?missing value for --artifact-dir}"; shift 2 ;;
    --epoch) EPOCH_HEX="${2:?missing value for --epoch}"; shift 2 ;;
    --raft-log-index) RAFT_LOG_INDEX="${2:?missing value for --raft-log-index}"; shift 2 ;;
    --item-ordinal) ITEM_ORDINAL="${2:?missing value for --item-ordinal}"; shift 2 ;;
    --expect-state) EXPECT_STATE="${2:?missing value for --expect-state}"; shift 2 ;;
    --expect-probe-token) EXPECT_PROBE_TOKEN="${2:?missing value for --expect-probe-token}"; shift 2 ;;
    --probe-key) PROBE_KEY="${2:?missing value for --probe-key}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "$ARTIFACT_DIR" ]] || fail "--artifact-dir is required"
[[ -d "$ARTIFACT_DIR" ]] || fail "artifact dir does not exist: $ARTIFACT_DIR"
[[ "$EPOCH_HEX" =~ ^[0-9a-f]{64}$ ]] || fail "--epoch must be 64 lowercase hex characters"
[[ "$RAFT_LOG_INDEX" =~ ^[1-9][0-9]*$ ]] || fail "--raft-log-index must be a positive integer"
[[ "$ITEM_ORDINAL" =~ ^[0-9]+$ ]] || fail "--item-ordinal must be a non-negative integer"
[[ "$EXPECT_STATE" =~ ^[23]$ ]] || fail "--expect-state must be 2 or 3"
[[ -n "$EXPECT_PROBE_TOKEN" ]] || fail "--expect-probe-token is required"
[[ "$PROBE_KEY" =~ ^-?[0-9]+$ ]] || fail "--probe-key must be an integer"

mkdir -p "$ARTIFACT_DIR"

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

query_terminal_row() {
  local idx="$1"
  local sql="
SELECT state,
       encode(terminal_digest, 'hex') AS digest_hex,
       (committed_at IS NOT NULL)::int AS committed,
       (result_payload IS NOT NULL)::int AS result_present,
       (error_payload IS NOT NULL)::int AS error_present,
       COALESCE(result_format_version::text, '') AS result_format_version,
       COALESCE(error_format_version::text, '') AS error_format_version
  FROM ariabc_internal.raft_apply_item
 WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
   AND raft_log_index = ${RAFT_LOG_INDEX}
   AND item_ordinal = ${ITEM_ORDINAL};
"
  remote_psql "$idx" "$sql"
}

query_claimed_count() {
  local idx="$1"
  remote_psql "$idx" "
SELECT count(*)
  FROM ariabc_internal.raft_apply_item
 WHERE epoch_id = decode('${EPOCH_HEX}', 'hex')
   AND state = 1;
"
}

query_probe_value() {
  local idx="$1"
  remote_psql "$idx" "
SELECT n, token
  FROM public.safe_recovery_probe
 WHERE k = ${PROBE_KEY};
"
}

declare -a ROWS=()
declare -a CLAIMED_COUNTS=()
declare -a PROBE_VALUES=()
declare -a PROBE_TOKENS=()

for idx in "${!NODE_IDS[@]}"; do
  row="$(query_terminal_row "$idx" | tr -d '\r' | sed '/^$/d')"
  [[ -n "$row" ]] || fail "node ${NODE_IDS[$idx]} returned no terminal row"
  [[ "$(printf '%s\n' "$row" | wc -l | tr -d ' ')" == "1" ]] || fail "node ${NODE_IDS[$idx]} returned multiple terminal rows"
  ROWS+=("$row")

  claimed="$(query_claimed_count "$idx" | tr -d '\r' | sed '/^$/d')"
  [[ "$claimed" =~ ^[0-9]+$ ]] || fail "node ${NODE_IDS[$idx]} returned invalid claimed count: $claimed"
  CLAIMED_COUNTS+=("$claimed")

  probe_val_tok="$(query_probe_value "$idx" | tr -d '\r' | sed '/^$/d')"
  [[ "$probe_val_tok" =~ ^-?[0-9]+\|.*$ ]] || fail "node ${NODE_IDS[$idx]} returned invalid probe value/token: $probe_val_tok"
  IFS='|' read -r p_n p_token <<<"$probe_val_tok"
  PROBE_VALUES+=("$p_n")
  PROBE_TOKENS+=("$p_token")
done

reference_row="${ROWS[0]}"
for row in "${ROWS[@]}"; do
  [[ "$row" == "$reference_row" ]] || fail "terminal row differs across replicas"
done

IFS='|' read -r state digest_hex committed result_present error_present result_fmt error_fmt <<<"$reference_row"
[[ "$state" == "$EXPECT_STATE" ]] || fail "terminal state=$state expected $EXPECT_STATE"
[[ "$digest_hex" =~ ^[0-9a-f]{64}$ ]] || fail "terminal digest must be 64 lowercase hex chars"
[[ "$committed" == "1" ]] || fail "terminal row is not committed"

if [[ "$EXPECT_STATE" == "2" ]]; then
  [[ "$result_present" == "1" ]] || fail "state=2 requires result_payload"
  [[ "$error_present" == "0" ]] || fail "state=2 forbids error_payload"
  [[ "$result_fmt" == "1" ]] || fail "state=2 requires result_format_version=1"
  [[ -z "$error_fmt" ]] || fail "state=2 must not carry error_format_version"
elif [[ "$EXPECT_STATE" == "3" ]]; then
  [[ "$result_present" == "0" ]] || fail "state=3 forbids result_payload"
  [[ "$error_present" == "1" ]] || fail "state=3 requires error_payload"
  [[ -z "$result_fmt" ]] || fail "state=3 must not carry result_format_version"
  [[ "$error_fmt" == "1" ]] || fail "state=3 requires error_format_version=1"
fi

for claimed in "${CLAIMED_COUNTS[@]}"; do
  [[ "$claimed" == "0" ]] || fail "persistent CLAIMED rows remain: $claimed"
done

for probe_value in "${PROBE_VALUES[@]}"; do
  [[ "$probe_value" == "1" ]] || fail "probe value must be 1, got $probe_value"
done

for probe_token in "${PROBE_TOKENS[@]}"; do
  [[ "$probe_token" == "$EXPECT_PROBE_TOKEN" ]] || fail "probe token must be $EXPECT_PROBE_TOKEN, got $probe_token"
done

{
  printf 'node_id\tterminal_row\tclaimed_count\tprobe_value\tprobe_token\n'
  for i in "${!NODE_IDS[@]}"; do
    printf '%s\t%s\t%s\t%s\t%s\n' "${NODE_IDS[$i]}" "${ROWS[$i]}" "${CLAIMED_COUNTS[$i]}" "${PROBE_VALUES[$i]}" "${PROBE_TOKENS[$i]}"
  done
} > "$ARTIFACT_DIR/recovery_verify.tsv"

{
  printf 'VERIFY_PASS=1\n'
  printf 'VERIFY_EXPECT_STATE=%s\n' "$EXPECT_STATE"
  printf 'VERIFY_EPOCH=%s\n' "$EPOCH_HEX"
  printf 'VERIFY_RAFT_LOG_INDEX=%s\n' "$RAFT_LOG_INDEX"
  printf 'VERIFY_ITEM_ORDINAL=%s\n' "$ITEM_ORDINAL"
  printf 'VERIFY_TERMINAL_STATE=%s\n' "$state"
  printf 'VERIFY_TERMINAL_DIGEST=%s\n' "$digest_hex"
  printf 'VERIFY_TERMINAL_ROW_MATCH=1\n'
  printf 'VERIFY_PROBE_VALUE_NODE1=%s\n' "${PROBE_VALUES[0]}"
  printf 'VERIFY_PROBE_VALUE_NODE2=%s\n' "${PROBE_VALUES[1]}"
  printf 'VERIFY_PROBE_VALUE_NODE4=%s\n' "${PROBE_VALUES[2]}"
  printf 'VERIFY_PROBE_TOKEN_NODE1=%s\n' "${PROBE_TOKENS[0]}"
  printf 'VERIFY_PROBE_TOKEN_NODE2=%s\n' "${PROBE_TOKENS[1]}"
  printf 'VERIFY_PROBE_TOKEN_NODE4=%s\n' "${PROBE_TOKENS[2]}"
  printf 'VERIFY_CLAIMED_NODE1=%s\n' "${CLAIMED_COUNTS[0]}"
  printf 'VERIFY_CLAIMED_NODE2=%s\n' "${CLAIMED_COUNTS[1]}"
  printf 'VERIFY_CLAIMED_NODE4=%s\n' "${CLAIMED_COUNTS[2]}"
} > "$ARTIFACT_DIR/recovery_verify.env"

echo "PASS: recovery case verified"
