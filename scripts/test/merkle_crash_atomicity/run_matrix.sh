#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
PROFILE=smoke
RESULT_ROOT=
ACTIONS_CSV=backend_kill,postmaster_kill

usage() {
    cat >&2 <<'EOF'
Usage: run_matrix.sh [--profile smoke|ci|durability] [--actions CSV] [--result-root DIR]

Profiles:
  smoke       1 repetition per crash failpoint/action
  ci          100 repetitions per crash failpoint/action
  durability  1000 repetitions per crash failpoint/action
EOF
}

while (($#)); do
    case "$1" in
        --profile) PROFILE=$2; shift 2 ;;
        --actions) ACTIONS_CSV=$2; shift 2 ;;
        --result-root) RESULT_ROOT=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

case "$PROFILE" in
    smoke) REPETITIONS=1 ;;
    ci) REPETITIONS=100 ;;
    durability) REPETITIONS=1000 ;;
    *) echo "Unknown profile: $PROFILE" >&2; exit 2 ;;
esac

if [[ -z "$RESULT_ROOT" ]]; then
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    RESULT_ROOT="$REPO_ROOT/scripts/bench_full_results/merkle_crash_atomicity_${PROFILE}_${stamp}"
fi
[[ ! -e "$RESULT_ROOT" ]] || {
    echo "Refusing to overwrite result root: $RESULT_ROOT" >&2
    exit 2
}
mkdir -p "$RESULT_ROOT"

IFS=',' read -r -a ACTIONS <<<"$ACTIONS_CSV"
PRECOMMIT_FAILPOINTS=(
    after_merkle_delta_staged
    after_merkle_delta_ledger_written
)
POSTCOMMIT_FAILPOINTS=(after_user_transaction_commit)
APPLIER_FAILPOINTS=(
    during_startup_catchup
    before_applier_page
    after_applier_page
    after_all_applier_pages
    before_apply_state_update
    after_apply_state_update
    after_apply_state_commit
)

run_one() {
    local case_name=$1
    local failpoint=$2
    local action=$3
    local repetition=$4
    local leaf="$RESULT_ROOT/${case_name}__${failpoint:-none}__${action}__$(printf '%04d' "$repetition")"
    local args=(--case "$case_name" --action "$action" --result-dir "$leaf")

    if [[ -n "$failpoint" ]]; then
        args+=(--failpoint "$failpoint")
    fi
    "$SCRIPT_DIR/run_case.sh" "${args[@]}"
}

{
    echo "profile=$PROFILE"
    echo "repetitions=$REPETITIONS"
    echo "actions=$ACTIONS_CSV"
    echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$RESULT_ROOT/campaign.env"

for action in "${ACTIONS[@]}"; do
    [[ "$action" == backend_kill || "$action" == postmaster_kill ]] || {
        echo "Invalid action: $action" >&2
        exit 2
    }
    for repetition in $(seq 1 "$REPETITIONS"); do
        for failpoint in "${PRECOMMIT_FAILPOINTS[@]}"; do
            run_one precommit_crash "$failpoint" "$action" "$repetition"
        done
        for failpoint in "${POSTCOMMIT_FAILPOINTS[@]}"; do
            run_one postcommit_crash "$failpoint" "$action" "$repetition"
        done
        for failpoint in "${APPLIER_FAILPOINTS[@]}"; do
        done
    done
done

# Logical transaction cases do not need a kill action and run once per
# campaign; crash cases above cover both backend and postmaster SIGKILL.
run_one sql_failure "" backend_kill 1
run_one savepoint "" backend_kill 1
run_one route_change "" backend_kill 1
run_one guards "" backend_kill 1

"$SCRIPT_DIR/summarize.py" "$RESULT_ROOT" | tee "$RESULT_ROOT/summary.json"
echo "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$RESULT_ROOT/campaign.env"
