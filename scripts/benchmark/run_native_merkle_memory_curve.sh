#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROWS_CSV=${ROWS_CSV:-1000000,3000000,5000000,10000000}
PARTITIONS=${PARTITIONS:-1}
MAINTENANCE_WORK_MEM=${MAINTENANCE_WORK_MEM:-16MB}
VERIFY_TIMEOUT_SECONDS=${VERIFY_TIMEOUT_SECONDS:-900}
ALLOW_VERIFY_TIMEOUT=${ALLOW_VERIFY_TIMEOUT:-0}
RESULT_ROOT=${1:-}

[[ -n "$RESULT_ROOT" ]] || {
    echo "usage: $0 RESULT_ROOT" >&2
    exit 2
}
[[ ! -e "$RESULT_ROOT" ]] || {
    echo "refusing to overwrite result directory: $RESULT_ROOT" >&2
    exit 2
}
mkdir -p "$RESULT_ROOT"
RESULT_ROOT=$(cd "$RESULT_ROOT" && pwd)

IFS=',' read -r -a ROWS <<<"$ROWS_CSV"
printf 'rows,status,elapsed_seconds,peak_backend_rss_kb,peak_backend_private_kb,peak_backend_pss_kb,verified,index_bytes\n' \
    >"$RESULT_ROOT/memory_curve.csv"

for rows in "${ROWS[@]}"; do
    [[ "$rows" =~ ^[1-9][0-9]*$ ]] || {
        echo "invalid row count: $rows" >&2
        exit 2
    }
    run_dir="$RESULT_ROOT/rows_${rows}"
    rc=0
    ROWS="$rows" PARTITIONS="$PARTITIONS" \
        MAINTENANCE_WORK_MEM="$MAINTENANCE_WORK_MEM" \
        VERIFY_TIMEOUT_SECONDS="$VERIFY_TIMEOUT_SECONDS" \
        "$SCRIPT_DIR/run_native_merkle_skew_build.sh" "$run_dir" \
        >"$RESULT_ROOT/rows_${rows}.stdout" \
        2>"$RESULT_ROOT/rows_${rows}.stderr" || rc=$?

    status=failed
    elapsed=
    rss=
    private=
    pss=
    verified=
    index_bytes=
    build_status=
    verification_status=
    if [[ -f "$run_dir/summary.env" ]]; then
        elapsed=$(awk -F= '$1 == "elapsed_seconds" { print $2 }' \
            "$run_dir/build.time" 2>/dev/null || true)
        rss=$(awk -F= '$1 == "peak_backend_rss_kb" { print $2 }' "$run_dir/summary.env")
        private=$(awk -F= '$1 == "peak_backend_private_kb" { print $2 }' "$run_dir/summary.env")
        pss=$(awk -F= '$1 == "peak_backend_pss_kb" { print $2 }' "$run_dir/summary.env")
        build_status=$(awk -F= '$1 == "build_status" { print $2 }' "$run_dir/summary.env")
        verification_status=$(awk -F= '$1 == "verification_status" { print $2 }' "$run_dir/summary.env")
        if [[ -f "$run_dir/verification.tsv" ]]; then
            verified=$(awk -F '\t' 'NR == 1 { print $2 }' "$run_dir/verification.tsv")
            index_bytes=$(awk -F '\t' 'NR == 1 { print $3 }' "$run_dir/verification.tsv")
        fi
        if [[ "$rc" -eq 0 && "$build_status" == PASS && "$verified" == t ]]; then
            status=pass
        elif [[ "$build_status" == PASS && "$verification_status" == TIMEOUT ]]; then
            status=build_pass_verify_timeout
        fi
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$rows" "$status" "$elapsed" "$rss" "$private" "$pss" \
        "$verified" "$index_bytes" >>"$RESULT_ROOT/memory_curve.csv"
done

{
    echo "rows_csv=$ROWS_CSV"
    echo "partitions=$PARTITIONS"
    echo "maintenance_work_mem=$MAINTENANCE_WORK_MEM"
    echo "verify_timeout_seconds=$VERIFY_TIMEOUT_SECONDS"
    echo "allow_verify_timeout=$ALLOW_VERIFY_TIMEOUT"
    echo "result_root=$RESULT_ROOT"
    echo "source_repo=$SCRIPT_DIR/../.."
    echo "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$RESULT_ROOT/campaign.env"

cat "$RESULT_ROOT/memory_curve.csv"
if [[ "$ALLOW_VERIFY_TIMEOUT" -eq 1 ]]; then
    accepted_status='pass|build_pass_verify_timeout'
else
    accepted_status='pass'
fi
if awk -F, -v accepted="$accepted_status" \
    'NR > 1 && $2 !~ ("^(" accepted ")$") { failed=1 }
     END { exit failed }' "$RESULT_ROOT/memory_curve.csv"; then
    exit 0
fi
echo "one or more memory-curve points failed; inspect per-row stderr" >&2
exit 1
