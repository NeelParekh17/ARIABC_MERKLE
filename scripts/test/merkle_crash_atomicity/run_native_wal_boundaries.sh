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

FAILPOINTS=(
    after_native_register_before_finish
    after_native_record_wal
    before_native_root_publication
    after_native_root_wal_before_commit
)

{
    echo "profile=native-wal-boundaries"
    echo "merkle_mode=dynamic"
    echo "update_mode=synchronous_cow"
    echo "action=postmaster_kill"
    echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$RESULT_ROOT/campaign.env"

for failpoint in "${FAILPOINTS[@]}"; do
    leaf="$RESULT_ROOT/precommit_crash__${failpoint}__postmaster_kill"
    "$SCRIPT_DIR/run_case.sh" \
        --case precommit_crash \
        --failpoint "$failpoint" \
        --action postmaster_kill \
        --merkle-mode dynamic \
        --update-mode synchronous_cow \
        --result-dir "$leaf"
done

"$SCRIPT_DIR/summarize.py" "$RESULT_ROOT" | tee "$RESULT_ROOT/summary.json"
echo "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$RESULT_ROOT/campaign.env"
