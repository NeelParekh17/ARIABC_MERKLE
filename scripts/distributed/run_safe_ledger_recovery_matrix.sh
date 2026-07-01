#!/usr/bin/env bash
# Run the safe-ledger crash-recovery matrix using PLAN.md case names.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_HARNESS="${SCRIPT_DIR}/test_remote_raft_recovery.sh"

CASE_NAME=""
NODE_ID=""
REPEAT=1
TARGET=""
EXTRA_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/run_safe_ledger_recovery_matrix.sh \
    --case <case_name> [--node-id <id>|--target leader|follower|all] [--repeat N]

Supported --case values:
  after_manifest_register_before_enqueue
  after_ledger_claim_before_user_sql
  after_ledger_finalize_before_toplevel_commit
  before_worker_toplevel_commit
  after_worker_toplevel_commit_before_result_ring
  after_result_ring_before_kafka_publish
  after_kafka_publish_before_applied_mark

Additional arguments after -- are forwarded to test_remote_raft_recovery.sh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) CASE_NAME="${2:?missing value for --case}"; shift 2 ;;
    --node-id) NODE_ID="${2:?missing value for --node-id}"; shift 2 ;;
    --repeat) REPEAT="${2:?missing value for --repeat}"; shift 2 ;;
    --target) TARGET="${2:?missing value for --target}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    --) shift; EXTRA_ARGS+=("$@"); break ;;
    *) EXTRA_ARGS+=("$1"); shift ;;
  esac
done

if [[ -z "$CASE_NAME" ]]; then
  echo "ERROR: --case is required" >&2
  usage >&2
  exit 1
fi
if [[ -n "$NODE_ID" && -n "$TARGET" ]]; then
  echo "ERROR: --node-id cannot be combined with --target ${TARGET}; use one selector" >&2
  exit 1
fi
if [[ -z "$NODE_ID" && -z "$TARGET" ]]; then
  echo "ERROR: one selector is required: --node-id <id> or --target leader|follower|all" >&2
  usage >&2
  exit 1
fi
if ! [[ "$REPEAT" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: --repeat must be a positive integer" >&2
  exit 1
fi

case_to_legacy() {
  case "$1" in
    after_manifest_register_before_enqueue) echo "A" ;;
    after_ledger_claim_before_user_sql) echo "B" ;;
    after_ledger_finalize_before_toplevel_commit) echo "C" ;;
    before_worker_toplevel_commit) echo "D" ;;
    after_worker_toplevel_commit_before_result_ring) echo "E" ;;
    after_result_ring_before_kafka_publish) echo "F" ;;
    after_kafka_publish_before_applied_mark) echo "G" ;;
    *)
      echo "ERROR: unsupported --case '$1'" >&2
      return 2
      ;;
  esac
}

legacy_case="$(case_to_legacy "$CASE_NAME")"

if [[ -n "$NODE_ID" ]]; then
  case "$NODE_ID" in
    1|2|4) ;;
    *) echo "ERROR: --node-id must be one of 1,2,4 (got ${NODE_ID})" >&2; exit 1 ;;
  esac
else
  case "$TARGET" in
    leader|follower|all) ;;
    *) echo "ERROR: --target must be leader|follower|all (got ${TARGET})" >&2; exit 1 ;;
  esac
fi

selector_args=()
selector_label=""
if [[ -n "$NODE_ID" ]]; then
  selector_args=(--node-id "$NODE_ID")
  selector_label="node_id=${NODE_ID}"
else
  selector_args=(--target "$TARGET")
  selector_label="target=${TARGET}"
fi

echo "safe-ledger recovery matrix: case=${CASE_NAME} legacy_case=${legacy_case} ${selector_label} repeat=${REPEAT}"

for ((i = 1; i <= REPEAT; i++)); do
  echo "=== recovery repetition ${i}/${REPEAT}: ${CASE_NAME} ${selector_label} ==="
  "${REMOTE_HARNESS}" --case "$legacy_case" "${selector_args[@]}" "${EXTRA_ARGS[@]}"
done
