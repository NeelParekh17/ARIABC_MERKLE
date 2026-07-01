#!/usr/bin/env bash
# Verify a completed safe-ledger distributed run artifact.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster_topology.sh"

ARTIFACT_DIR=""
EPOCH_HEX=""
EXPECT_OK=""
EXPECT_ERROR=""
EXPECT_PROBE_VALUE=""

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/verify_safe_ledger_run.sh \
    --artifact-dir <path> \
    --epoch <64-hex> \
    --expect-ok <count> \
    --expect-error <count> \
    [--expect-probe-value <integer>]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-dir) ARTIFACT_DIR="${2:?missing value for --artifact-dir}"; shift 2 ;;
    --epoch) EPOCH_HEX="${2:?missing value for --epoch}"; shift 2 ;;
    --expect-ok) EXPECT_OK="${2:?missing value for --expect-ok}"; shift 2 ;;
    --expect-error) EXPECT_ERROR="${2:?missing value for --expect-error}"; shift 2 ;;
    --expect-probe-value) EXPECT_PROBE_VALUE="${2:?missing value for --expect-probe-value}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_int() {
  local name="$1" value="$2"
  [[ "$value" =~ ^-?[0-9]+$ ]] || fail "$name must be an integer, got '$value'"
}

[[ -n "$ARTIFACT_DIR" ]] || fail "--artifact-dir is required"
[[ -d "$ARTIFACT_DIR" ]] || fail "artifact dir does not exist: $ARTIFACT_DIR"
[[ "$EPOCH_HEX" =~ ^[0-9a-fA-F]{64}$ ]] || fail "--epoch must be 64 hex chars"
[[ -n "$EXPECT_OK" ]] || fail "--expect-ok is required"
[[ -n "$EXPECT_ERROR" ]] || fail "--expect-error is required"
require_int "--expect-ok" "$EXPECT_OK"
require_int "--expect-error" "$EXPECT_ERROR"
if [[ -n "$EXPECT_PROBE_VALUE" ]]; then
  require_int "--expect-probe-value" "$EXPECT_PROBE_VALUE"
fi

SUMMARY_DIR="$ARTIFACT_DIR/safe_ledger_verify"
mkdir -p "$SUMMARY_DIR"

psql_node() {
  local idx="$1" sql="$2"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$user@$ip" \
    "export LD_LIBRARY_PATH='/home/neel/Desktop/ariabc_install/lib:\${LD_LIBRARY_PATH:-}'; \
     /home/neel/Desktop/ariabc_install/bin/psql -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"$sql\""
}

latest_gateway_log() {
  local candidate
  for candidate in "$ARTIFACT_DIR/gateway.log" "$ARTIFACT_DIR/gateway_det.log" "$ARTIFACT_DIR"/gateway*.log; do
    [[ -f "$candidate" ]] && { printf '%s\n' "$candidate"; return 0; }
  done
  return 1
}

value_from_logs() {
  local key="$1" file="$2"
  grep -E "(^|[[:space:]])${key}=[0-9]+" "$file" 2>/dev/null |
    tail -1 |
    sed -E "s/.*(^|[[:space:]])${key}=([0-9]+).*/\\2/"
}

GW_LOG="$(latest_gateway_log || true)"
[[ -n "$GW_LOG" ]] || fail "no gateway log found in $ARTIFACT_DIR"

completed="$(value_from_logs completed "$GW_LOG")"
kafka_msgs="$(value_from_logs kafka_msgs "$GW_LOG")"
kafka_parse_failures="$(value_from_logs kafka_parse_failures "$GW_LOG")"
permanent_failures="$(value_from_logs permanent_failures "$GW_LOG")"
divergence_count="$(value_from_logs divergence_count "$GW_LOG")"

{
  echo "artifact_dir=$ARTIFACT_DIR"
  echo "gateway_log=$GW_LOG"
  echo "completed=${completed:-missing}"
  echo "kafka_msgs=${kafka_msgs:-missing}"
  echo "kafka_parse_failures=${kafka_parse_failures:-missing}"
  echo "permanent_failures=${permanent_failures:-missing}"
  echo "divergence_count=${divergence_count:-missing}"
} > "$SUMMARY_DIR/gateway_summary.env"

[[ "${permanent_failures:-}" == "0" ]] || fail "gateway permanent_failures=${permanent_failures:-missing}"
[[ "${divergence_count:-}" == "0" ]] || fail "gateway divergence_count=${divergence_count:-missing}"
[[ "${kafka_parse_failures:-}" == "0" ]] || fail "gateway kafka_parse_failures=${kafka_parse_failures:-missing}"

declare -a ROOTS=()
declare -a PROBE_VALUES=()

for idx in "${!NODE_IDS[@]}"; do
  node_id="${NODE_IDS[$idx]}"
  node_name="${NODE_NAMES[$idx]}"
  out="$SUMMARY_DIR/node${node_id}_${node_name}.txt"

  ledger_sql="
WITH rows AS (
  SELECT state,
         COALESCE(octet_length(terminal_digest), -1) AS digest_len
    FROM ariabc_internal.raft_apply_item
   WHERE epoch_id = decode('$EPOCH_HEX', 'hex')
)
SELECT 'claimed=' || COUNT(*) FILTER (WHERE state = 1) ||
       ' ok=' || COUNT(*) FILTER (WHERE state = 2) ||
       ' error=' || COUNT(*) FILTER (WHERE state = 3) ||
       ' bad_digest=' || COUNT(*) FILTER (WHERE state IN (2,3) AND digest_len <> 32) ||
       ' bad_state=' || COUNT(*) FILTER (WHERE state NOT IN (1,2,3))
  FROM rows;"
  ledger_line="$(psql_node "$idx" "$ledger_sql" | tr -d '\r')"

  merkle_sql="
SELECT CASE WHEN to_regclass('public.usertable_small') IS NULL THEN 'missing'
            ELSE concat(count(*), ':', merkle_root_hash('usertable_small'), ':', merkle_verify('usertable_small'))
       END
  FROM usertable_small;"
  merkle_line="$(psql_node "$idx" "$merkle_sql" 2>/dev/null | tr -d '\r' || true)"
  if [[ -z "$merkle_line" ]]; then
    merkle_line="missing"
  fi

  probe_line="not_checked"
  if [[ -n "$EXPECT_PROBE_VALUE" ]]; then
    probe_sql="
SELECT CASE WHEN to_regclass('public.recovery_probe') IS NULL THEN 'missing'
            ELSE COALESCE((SELECT v::text FROM recovery_probe WHERE k = 1), 'missing_key')
       END;"
    probe_line="$(psql_node "$idx" "$probe_sql" | tr -d '\r')"
    [[ "$probe_line" == "$EXPECT_PROBE_VALUE" ]] || fail "node $node_id recovery_probe.v=$probe_line expected $EXPECT_PROBE_VALUE"
    PROBE_VALUES+=("$probe_line")
  fi

  printf 'node_id=%s\nnode_name=%s\nledger=%s\nmerkle=%s\nprobe=%s\n' \
    "$node_id" "$node_name" "$ledger_line" "$merkle_line" "$probe_line" > "$out"

  claimed="$(sed -E 's/.*claimed=([0-9]+).*/\1/' <<<"$ledger_line")"
  ok="$(sed -E 's/.*ok=([0-9]+).*/\1/' <<<"$ledger_line")"
  error="$(sed -E 's/.*error=([0-9]+).*/\1/' <<<"$ledger_line")"
  bad_digest="$(sed -E 's/.*bad_digest=([0-9]+).*/\1/' <<<"$ledger_line")"
  bad_state="$(sed -E 's/.*bad_state=([0-9]+).*/\1/' <<<"$ledger_line")"

  [[ "$claimed" == "0" ]] || fail "node $node_id has persistent CLAIMED rows: $claimed"
  [[ "$ok" == "$EXPECT_OK" ]] || fail "node $node_id APPLIED_OK=$ok expected $EXPECT_OK"
  [[ "$error" == "$EXPECT_ERROR" ]] || fail "node $node_id APPLIED_ERROR=$error expected $EXPECT_ERROR"
  [[ "$bad_digest" == "0" ]] || fail "node $node_id has terminal digest length errors: $bad_digest"
  [[ "$bad_state" == "0" ]] || fail "node $node_id has unknown ledger states: $bad_state"

  if [[ "$merkle_line" != "missing" ]]; then
    root="$(cut -d: -f2 <<<"$merkle_line")"
    verify="$(cut -d: -f3 <<<"$merkle_line")"
    [[ "$verify" == "t" ]] || fail "node $node_id merkle_verify=$verify"
    ROOTS+=("$root")
  fi
done

if [[ "${#ROOTS[@]}" -gt 1 ]]; then
  first_root="${ROOTS[0]}"
  for root in "${ROOTS[@]}"; do
    [[ "$root" == "$first_root" ]] || fail "Merkle roots differ: ${ROOTS[*]}"
  done
fi

if [[ -n "$EXPECT_PROBE_VALUE" && "${#PROBE_VALUES[@]}" -gt 1 ]]; then
  for probe in "${PROBE_VALUES[@]}"; do
    [[ "$probe" == "$EXPECT_PROBE_VALUE" ]] || fail "probe values differ: ${PROBE_VALUES[*]}"
  done
fi

if compgen -G "$ARTIFACT_DIR/server_node*.log" >/dev/null; then
  grep -H "SAFE_RING_WRITE" "$ARTIFACT_DIR"/server_node*.log > "$SUMMARY_DIR/safe_ring_write_markers.txt" \
    || fail "SAFE_RING_WRITE marker missing from server logs"
fi

grep -H "SAFE_KAFKA_PUBLISH_DELIVERED" "$ARTIFACT_DIR"/*.log > "$SUMMARY_DIR/safe_kafka_delivered_markers.txt" \
  || fail "SAFE_KAFKA_PUBLISH_DELIVERED marker missing from artifact logs"

if compgen -G "$ARTIFACT_DIR/*" >/dev/null && grep -R "SAFE_FAILPOINT" "$ARTIFACT_DIR" >/dev/null 2>&1; then
  grep -R "SAFE_FAILPOINT_TRIGGERED" "$ARTIFACT_DIR" > "$SUMMARY_DIR/safe_failpoint_markers.txt" \
    || fail "SAFE_FAILPOINT marker found but expected SAFE_FAILPOINT_TRIGGERED marker is missing"
fi

{
  [[ -f "$ARTIFACT_DIR/run_meta.env" ]] && grep -E '^(RAFT_CLUSTER_ID|RAFT_STORAGE_MODE|RAFT_STORAGE_ACTION|RAFT_EPOCH_HEX)=' "$ARTIFACT_DIR/run_meta.env" || true
} > "$SUMMARY_DIR/raft_storage_identity.env"

echo "PASS: safe-ledger artifact verified: $ARTIFACT_DIR"
echo "summary: $SUMMARY_DIR"
