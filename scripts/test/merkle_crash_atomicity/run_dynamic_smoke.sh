#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
RESULT_ROOT=

usage() {
    echo "Usage: $0 --result-root DIR" >&2
}

while (($#)); do
    case "$1" in
        --result-root) RESULT_ROOT=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

[[ -n "$RESULT_ROOT" ]] || { usage; exit 2; }
[[ ! -e "$RESULT_ROOT" ]] || {
    echo "Refusing to overwrite result root: $RESULT_ROOT" >&2
    exit 2
}
mkdir -p "$RESULT_ROOT"

run_one() {
    local case_name=$1
    local failpoint=$2
    local leaf="$RESULT_ROOT/${case_name}__${failpoint:-none}__postmaster_kill"
    local args=(
        --case "$case_name"
        --action postmaster_kill
        --merkle-mode dynamic
        --result-dir "$leaf"
    )

    if [[ -n "$failpoint" ]]; then
        args+=(--failpoint "$failpoint")
    fi
    if ! "$SCRIPT_DIR/run_case.sh" "${args[@]}"; then
        echo "FAIL dynamic crash case=$case_name failpoint=${failpoint:-none}; artifact=$leaf" >&2
        return 1
    fi
}

{
    echo "profile=dynamic-smoke"
    echo "merkle_mode=dynamic"
    echo "actions=postmaster_kill"
    echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$RESULT_ROOT/campaign.env"

# Cover every dynamic-specific durability boundary once with a full postmaster
# SIGKILL.  The shared ordered-ledger boundaries are included because dynamic
# transitions and the global apply watermark commit atomically.
run_one build_crash ""
run_one precommit_crash after_merkle_dynamic_delta_staged
run_one postcommit_crash after_user_transaction_commit

# Logical transaction and lifecycle guards exercise the dynamic index itself.
run_one sql_failure ""
run_one savepoint ""
run_one route_change ""
run_one guards ""

"$SCRIPT_DIR/summarize.py" "$RESULT_ROOT" >"$RESULT_ROOT/summary.json"
echo "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$RESULT_ROOT/campaign.env"
cat "$RESULT_ROOT/summary.json"
