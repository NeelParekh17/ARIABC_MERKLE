#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
PG_BIN=${PG_BIN:-/work/ARIABC/install/bin}
DB_USER=${DB_USER:-$(id -un)}
ROWS=${ROWS:-10000000}
PARTITIONS=${PARTITIONS:-1}
MAINTENANCE_WORK_MEM=${MAINTENANCE_WORK_MEM:-16MB}
VERIFY_TIMEOUT_SECONDS=${VERIFY_TIMEOUT_SECONDS:-900}
RESULT_DIR=${1:-}

[[ -n "$RESULT_DIR" ]] || {
    echo "usage: $0 RESULT_DIR" >&2
    exit 2
}
[[ "$ROWS" =~ ^[1-9][0-9]*$ ]] || { echo "ROWS must be positive" >&2; exit 2; }
[[ "$PARTITIONS" =~ ^[1-9][0-9]*$ ]] || { echo "PARTITIONS must be positive" >&2; exit 2; }
[[ "$VERIFY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
    echo "VERIFY_TIMEOUT_SECONDS must be positive" >&2
    exit 2
}
[[ ! -e "$RESULT_DIR" ]] || {
    echo "refusing to overwrite result directory: $RESULT_DIR" >&2
    exit 2
}
mkdir -p "$RESULT_DIR"
RESULT_DIR=$(cd "$RESULT_DIR" && pwd)

WORK_DIR=$(mktemp -d /tmp/ariabc-native-skew-build-XXXXXX)
DATA_DIR=$WORK_DIR/data
SOCKET_DIR=$WORK_DIR/socket
PORT=$((55000 + RANDOM % 8000))
mkdir -p "$SOCKET_DIR"

cleanup() {
    "$PG_BIN/pg_ctl" -D "$DATA_DIR" stop -m immediate >/dev/null 2>&1 || true
    rm -rf -- "$WORK_DIR"
}
trap cleanup EXIT

"$PG_BIN/initdb" -D "$DATA_DIR" --no-sync >"$RESULT_DIR/initdb.log" 2>&1
"$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$RESULT_DIR/postgres.log" -w start \
    -o "-p $PORT -k $SOCKET_DIR -c listen_addresses='' -c fsync=on -c synchronous_commit=on -c full_page_writes=on -c autovacuum=off -c max_wal_size=4GB -c checkpoint_timeout=30min" \
    >"$RESULT_DIR/start.log" 2>&1

PSQL=("$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U "$DB_USER" -d postgres)

cat >"$RESULT_DIR/load.sql" <<'SQL'
\timing on
CREATE TABLE native_skew_build(id bigint PRIMARY KEY, payload text NOT NULL);
INSERT INTO native_skew_build
SELECT g, repeat(md5(g::text), 2) FROM generate_series(1, :rows) AS g;
CHECKPOINT;
SQL

cat >"$RESULT_DIR/build.sql" <<'SQL'
\timing on
COPY (SELECT pg_backend_pid()) TO :'backend_pid_file';
SET maintenance_work_mem = :'maintenance_work_mem';
SHOW maintenance_work_mem;
CREATE INDEX native_skew_build_merkle ON native_skew_build
USING merkle (id)
WITH (dynamic=true, partitions=:partitions, leaf_capacity=32,
      merge_threshold=8, leaf_byte_capacity=8192, max_key_bytes=256,
      update_mode='synchronous_cow');
SQL

"${PSQL[@]}" -v rows="$ROWS" -f "$RESULT_DIR/load.sql" \
    >"$RESULT_DIR/load.out" 2>"$RESULT_DIR/load.err"

printf 'elapsed_seconds,total_postgres_rss_kb,build_backend_rss_kb,build_backend_private_kb,build_backend_pss_kb\n' \
    >"$RESULT_DIR/memory.csv"
start_seconds=$(date +%s)
/usr/bin/time -f 'elapsed_seconds=%e\npsql_max_rss_kb=%M' \
    -o "$RESULT_DIR/build.time" \
    "${PSQL[@]}" -v maintenance_work_mem="$MAINTENANCE_WORK_MEM" \
    -v partitions="$PARTITIONS" -v backend_pid_file="$RESULT_DIR/backend.pid" \
    -f "$RESULT_DIR/build.sql" \
    >"$RESULT_DIR/build.out" 2>"$RESULT_DIR/build.err" &
build_pid=$!
postmaster_pid=$(head -n 1 "$DATA_DIR/postmaster.pid")
for _ in $(seq 1 500); do
    [[ -s "$RESULT_DIR/backend.pid" ]] && break
    kill -0 "$build_pid" 2>/dev/null || break
    sleep 0.01
done
[[ -s "$RESULT_DIR/backend.pid" ]] || {
    wait "$build_pid" || true
    echo "CREATE INDEX backend did not publish its PID" >&2
    exit 1
}
backend_pid=$(tr -d '[:space:]' <"$RESULT_DIR/backend.pid")
while kill -0 "$build_pid" 2>/dev/null; do
    total_rss=$(
        ps -o rss= --ppid "$postmaster_pid" | awk '
            { total += $1 }
            END { print total + 0 }')
    backend_rss=$(ps -o rss= -p "$backend_pid" 2>/dev/null | \
        awk '{ print $1 + 0 }' || true)
    backend_rss=${backend_rss:-0}
    backend_private=0
    backend_pss=0
    read -r backend_private backend_pss < <(
        awk '
            /^Pss:/ { pss = $2 }
            /^Private_Clean:/ { private += $2 }
            /^Private_Dirty:/ { private += $2 }
            /^Private_Hugetlb:/ { private += $2 }
            END { print private + 0, pss + 0 }
        ' "/proc/$backend_pid/smaps_rollup" 2>/dev/null || echo '0 0') || true
    backend_private=${backend_private:-0}
    backend_pss=${backend_pss:-0}
    printf '%s,%s,%s,%s,%s\n' "$(( $(date +%s) - start_seconds ))" \
        "$total_rss" "$backend_rss" "$backend_private" "$backend_pss" \
        >>"$RESULT_DIR/memory.csv"
    sleep 0.1
done
set +e
wait "$build_pid"
build_rc=$?
set -e

verify_rc=1
: >"$RESULT_DIR/verification.tsv"
if [[ "$build_rc" -eq 0 ]]; then
    set +e
    timeout --signal=TERM --kill-after=10s "${VERIFY_TIMEOUT_SECONDS}s" \
        "${PSQL[@]}" -At -F $'\t' -c \
        "SELECT count(*), merkle_verify('native_skew_build'), pg_relation_size('native_skew_build_merkle'), merkle_dynamic_tree_stats('native_skew_build_merkle');" \
        >"$RESULT_DIR/verification.tsv"
    verify_rc=$?
    set -e
fi

peak_total_rss=$(awk -F, 'NR > 1 && $2 > max { max=$2 } END { print max+0 }' "$RESULT_DIR/memory.csv")
peak_backend_rss=$(awk -F, 'NR > 1 && $3 > max { max=$3 } END { print max+0 }' "$RESULT_DIR/memory.csv")
peak_backend_private=$(awk -F, 'NR > 1 && $4 > max { max=$4 } END { print max+0 }' "$RESULT_DIR/memory.csv")
peak_backend_pss=$(awk -F, 'NR > 1 && $5 > max { max=$5 } END { print max+0 }' "$RESULT_DIR/memory.csv")
{
    echo "rows=$ROWS"
    echo "partitions=$PARTITIONS"
    echo "largest_partition_fraction=1.0"
    echo "maintenance_work_mem=$MAINTENANCE_WORK_MEM"
    echo "verify_timeout_seconds=$VERIFY_TIMEOUT_SECONDS"
    echo "build_status=$([[ "$build_rc" -eq 0 ]] && echo PASS || echo FAIL)"
    if [[ "$verify_rc" -eq 0 ]]; then
        echo "verification_status=PASS"
    elif [[ "$verify_rc" -eq 124 || "$verify_rc" -eq 137 ]]; then
        echo "verification_status=TIMEOUT"
    else
        echo "verification_status=FAIL"
    fi
    echo "build_backend_pid=$backend_pid"
    echo "peak_total_postgres_rss_kb=$peak_total_rss"
    echo "peak_backend_rss_kb=$peak_backend_rss"
    echo "peak_backend_private_kb=$peak_backend_private"
    echo "peak_backend_pss_kb=$peak_backend_pss"
    echo "server_autovacuum=off"
    echo "server_max_wal_size=4GB"
    echo "server_checkpoint_timeout=30min"
    echo "source_repo=$REPO_ROOT"
} >"$RESULT_DIR/summary.env"

cat "$RESULT_DIR/summary.env"
cat "$RESULT_DIR/build.time"
cat "$RESULT_DIR/verification.tsv"

if [[ "$build_rc" -ne 0 || "$verify_rc" -ne 0 ]]; then
    exit 1
fi
