#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
PG_BIN=${PG_BIN:-/work/ARIABC/install/bin}
FAILPOINT=
ACTION=postmaster_kill
CASE_NAME=
RESULT_DIR=
MERKLE_MODE=dynamic
UPDATE_MODE=synchronous_cow
KEEP_DATA=${KEEP_DATA:-0}
PORT=${PORT:-$((56000 + ($$ % 1000)))}

usage() {
    echo "Usage: $0 --case NAME [--failpoint NAME] [--action backend_kill|postmaster_kill] --merkle-mode dynamic --update-mode synchronous_cow --result-dir DIR" >&2
}

while (($#)); do
    case "$1" in
        --case) CASE_NAME=$2; shift 2 ;;
        --failpoint) FAILPOINT=$2; shift 2 ;;
        --action) ACTION=$2; shift 2 ;;
        --merkle-mode) MERKLE_MODE=$2; shift 2 ;;
        --update-mode) UPDATE_MODE=$2; shift 2 ;;
        --result-dir) RESULT_DIR=$2; shift 2 ;;
        --port) PORT=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

[[ -n "$CASE_NAME" && -n "$RESULT_DIR" ]] || { usage; exit 2; }
[[ "$ACTION" == backend_kill || "$ACTION" == postmaster_kill ]] || {
    echo "Invalid action: $ACTION" >&2
    exit 2
}
[[ "$MERKLE_MODE" == dynamic ]] || {
    echo "Only native dynamic Merkle mode is supported" >&2
    exit 2
}
[[ "$UPDATE_MODE" == synchronous_cow ]] || {
    echo "Only native synchronous_cow update mode is supported" >&2
    exit 2
}
[[ ! -e "$RESULT_DIR" ]] || {
    echo "Refusing to overwrite result directory: $RESULT_DIR" >&2
    exit 2
}

mkdir -p "$RESULT_DIR"
WORK_DIR=$(mktemp -d "/tmp/ariabc-merkle-${CASE_NAME//[^a-zA-Z0-9]/_}-XXXXXX")
DATA_DIR="$WORK_DIR/data"
SOCKET_DIR="$WORK_DIR/socket"
SERVER_LOG="$RESULT_DIR/postgres.log"
mkdir -p "$SOCKET_DIR"

PG_CTL="$PG_BIN/pg_ctl"
INITDB="$PG_BIN/initdb"
PSQL="$PG_BIN/psql"
PSQL_ARGS=(-X -v ON_ERROR_STOP=1 -h "$SOCKET_DIR" -p "$PORT" -U postgres -d postgres)
# The crash harness talks over the private Unix socket only.  Explicitly
# disable TCP so a restricted/containerized test runner cannot fail before it
# reaches the Merkle failpoint just because loopback sockets are unavailable.
SERVER_OPTS="-p $PORT -k $SOCKET_DIR -c listen_addresses='' -c fsync=on -c synchronous_commit=on -c full_page_writes=on"
SERVER_RUNNING=0
CASE_STATUS=FAIL

cleanup() {
    local rc=$?

    if [[ "$SERVER_RUNNING" -eq 1 ]]; then
        "$PG_CTL" -D "$DATA_DIR" stop -m immediate >/dev/null 2>&1 || true
    fi
    if [[ "$KEEP_DATA" != 1 ]]; then
        rm -rf -- "$WORK_DIR"
    else
        echo "work_dir=$WORK_DIR" >>"$RESULT_DIR/result.env"
    fi
    if [[ "$CASE_STATUS" != PASS && "$rc" -eq 0 ]]; then
        rc=1
    fi
    exit "$rc"
}
trap cleanup EXIT

start_server() {
    local failpoint=${1:-}
    local action=${2:-$ACTION}

    if [[ -n "$failpoint" ]]; then
        env ARIABC_MERKLE_FAILPOINT="$failpoint" \
            ARIABC_MERKLE_FAILPOINT_ACTION="$action" \
            "$PG_CTL" -D "$DATA_DIR" -l "$SERVER_LOG" \
            -o "$SERVER_OPTS" start >/dev/null
    else
        "$PG_CTL" -D "$DATA_DIR" -l "$SERVER_LOG" \
            -o "$SERVER_OPTS" start >/dev/null
    fi
    SERVER_RUNNING=1
}

stop_server() {
    if [[ "$SERVER_RUNNING" -eq 1 ]]; then
        set +e
        timeout 15 "$PG_CTL" -D "$DATA_DIR" stop -m fast >/dev/null 2>&1
        local rc=$?
        if [[ "$rc" -ne 0 ]]; then
            timeout 5 "$PG_CTL" -D "$DATA_DIR" stop -m immediate >/dev/null 2>&1
            rc=$?
        fi
        set -e
        [[ "$rc" -eq 0 ]] || return "$rc"
        SERVER_RUNNING=0
    fi
}

wait_for_crash() {
    local i

    for i in $(seq 1 200); do
        if ! "$PG_CTL" -D "$DATA_DIR" status >/dev/null 2>&1; then
            SERVER_RUNNING=0
            return 0
        fi
        sleep 0.05
    done
    return 1
}

wait_for_server_down() {
    local i

    for i in $(seq 1 200); do
        if ! "$PG_CTL" -D "$DATA_DIR" status >/dev/null 2>&1 &&
            [[ ! -e "$DATA_DIR/postmaster.pid" ]]; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

restart_without_failpoint() {
	local attempt
	local rc
	local stopped=0
	local stale_pid

	if "$PG_CTL" -D "$DATA_DIR" status >/dev/null 2>&1; then
        SERVER_RUNNING=1
		# A backend SIGKILL can make this PostgreSQL build restart the
		# postmaster asynchronously.  pg_ctl status still reports "running"
		# during that recovery window, so retry the fast stop instead of racing
		# the postmaster and treating a transient failure as a Merkle failure.
		for attempt in $(seq 1 40); do
			set +e
			# The backend kill may leave the postmaster in crash recovery; an
			# immediate stop is intentional here and avoids waiting on a recovery
			# cycle that is itself about to be restarted by this case.
			timeout 5 "$PG_CTL" -D "$DATA_DIR" stop -m immediate >/dev/null 2>&1
			rc=$?
			set -e
			if [[ "$rc" -eq 0 ]]; then
				SERVER_RUNNING=0
				stopped=1
				break
			fi
			sleep 0.05
		done
		[[ "$stopped" -eq 1 ]] || return 1
    else
        SERVER_RUNNING=0
        wait_for_crash || true
		# SIGKILL cannot run PostgreSQL's normal PID-file cleanup.  Remove only
		# this harness cluster's lock file, and only after proving its recorded
		# postmaster PID no longer exists.
		if [[ -f "$DATA_DIR/postmaster.pid" ]]; then
			stale_pid=$(head -n 1 "$DATA_DIR/postmaster.pid" 2>/dev/null || true)
			if [[ "$stale_pid" =~ ^[0-9]+$ ]] && ! kill -0 "$stale_pid" 2>/dev/null; then
				rm -f -- "$DATA_DIR/postmaster.pid"
			fi
		fi
    fi
    # A postmaster crash can leave the PID file in place while it performs
    # crash recovery.  Do not race a fresh pg_ctl start with that restart.
    wait_for_server_down || return 1
    start_server
}

expect_connection_loss() {
    local sql=$1
    local rc

	set +e
	# A SIGKILLed postmaster cannot always close an already-buffered libpq
	# socket promptly on every kernel.  Bound the client wait so the harness
	# can observe the dead PID and perform recovery instead of hanging forever.
	timeout 20 "$PSQL" "${PSQL_ARGS[@]}" -c "$sql" \
		>>"$RESULT_DIR/case.log" 2>&1
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
        echo "Expected failpoint connection loss, but SQL succeeded" >&2
        return 1
    fi
}

scalar() {
    "$PSQL" "${PSQL_ARGS[@]}" -Atqc "$1"
}

assert_scalar() {
    local sql=$1
    local expected=$2
    local actual

    actual=$(scalar "$sql")
    if [[ "$actual" != "$expected" ]]; then
        echo "Assertion failed: expected '$expected', got '$actual' for: $sql" >&2
        return 1
    fi
}

queue_bulk_delta() {
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "INSERT INTO merkle_atomicity_test
         SELECT g, 'bulk-' || g, 1 FROM generate_series(1000,1199) AS g;" \
        >>"$RESULT_DIR/case.log"
}

run_precommit_crash() {
    stop_server
    start_server "$FAILPOINT" "$ACTION"
    expect_connection_loss \
        "INSERT INTO merkle_atomicity_test VALUES (100, 'must-abort', 1);"
    if [[ "$ACTION" == postmaster_kill ]]; then
        wait_for_crash
    fi
    restart_without_failpoint
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=100" 0
}

run_postcommit_crash() {
    stop_server
    start_server "$FAILPOINT" "$ACTION"
    expect_connection_loss \
        "INSERT INTO merkle_atomicity_test VALUES (101, 'must-commit', 1);"
    if [[ "$ACTION" == postmaster_kill ]]; then
        wait_for_crash
    fi
    restart_without_failpoint
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=101" 1
}

run_applier_crash() {
    local before_position

    queue_bulk_delta
    before_position=$(scalar \
        "SELECT applied_seq FROM ariabc_internal.merkle_apply_state WHERE singleton")
    stop_server
    start_server "$FAILPOINT" "$ACTION"
    expect_connection_loss "SELECT merkle_recovery_status();"
    if [[ "$ACTION" == postmaster_kill ]]; then
        wait_for_crash
    fi
    restart_without_failpoint
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id BETWEEN 1000 AND 1199" 200

    if [[ "$FAILPOINT" == after_apply_state_commit ]]; then
        assert_scalar \
            "SELECT (merkle_recovery_status() LIKE '%\"state\":\"READY\"%')::int" 1
        assert_scalar \
            "SELECT (applied_seq > $before_position)::int FROM ariabc_internal.merkle_apply_state WHERE singleton" 1
    fi
}

run_sql_failure() {
    local before_count
    local rc

    before_count=$(scalar "SELECT count(*) FROM ariabc_internal.merkle_local_delta")
    set +e
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "BEGIN;
         INSERT INTO merkle_atomicity_test VALUES (200, 'failed', 1);
         SELECT 1 / 0;
         COMMIT;" >>"$RESULT_DIR/case.log" 2>&1
    rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || { echo "Expected SQL failure did not fail" >&2; return 1; }
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=200" 0
    assert_scalar "SELECT count(*) FROM ariabc_internal.merkle_local_delta" "$before_count"

    # A terminal safe-ledger failure is an ordered no-op and must not block a
    # later prefix.  Model one directly without any business-table mutation.
    "$PSQL" "${PSQL_ARGS[@]}" <<'SQL' >>"$RESULT_DIR/case.log"
BEGIN;
INSERT INTO ariabc_internal.raft_apply_epoch
       (epoch_id, epoch_label, protocol_version)
VALUES (decode(repeat('f', 64), 'hex'), 'crash-test-error', 1);
WITH reserved AS (
    UPDATE ariabc_internal.merkle_apply_counter
       SET next_seq = next_seq + 1
     WHERE singleton
     RETURNING next_seq
)
INSERT INTO ariabc_internal.raft_apply_entry
       (epoch_id, raft_log_index, entry_digest, expected_items,
        merkle_apply_seq_base)
SELECT decode(repeat('f', 64), 'hex'), 1,
       decode(repeat('e', 64), 'hex'), 1, next_seq
FROM reserved;
INSERT INTO ariabc_internal.raft_apply_entry_item
       (epoch_id, raft_log_index, item_ordinal, item_digest)
VALUES (decode(repeat('f', 64), 'hex'), 1, 0,
        decode(repeat('d', 64), 'hex'));
INSERT INTO ariabc_internal.raft_apply_item
       (epoch_id, raft_log_index, item_ordinal, entry_digest, item_digest,
        state, error_format_version, sqlstate_code, error_payload,
        terminal_digest, committed_at, merkle_apply_seq,
        merkle_delta_version,
        merkle_delta_blob)
VALUES (decode(repeat('f', 64), 'hex'), 1, 0,
        decode(repeat('e', 64), 'hex'), decode(repeat('d', 64), 'hex'),
        3, 1, '22012', convert_to('division by zero', 'UTF8'),
        decode(repeat('c', 64), 'hex'), clock_timestamp(),
        (SELECT merkle_apply_seq_base FROM ariabc_internal.raft_apply_entry
          WHERE epoch_id = decode(repeat('f', 64), 'hex') AND raft_log_index = 1),
        0, NULL);
COMMIT;
SQL
    "$PSQL" "${PSQL_ARGS[@]}" -c "SELECT merkle_recovery_status();" \
        >>"$RESULT_DIR/case.log"
}

run_savepoint() {
    "$PSQL" "${PSQL_ARGS[@]}" <<'SQL' >>"$RESULT_DIR/case.log"
BEGIN;
INSERT INTO merkle_atomicity_test VALUES (201, 'outer-commit', 1);
SAVEPOINT inner_delta;
INSERT INTO merkle_atomicity_test VALUES (202, 'inner-abort', 1);
ROLLBACK TO inner_delta;
COMMIT;
SQL
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=201" 1
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=202" 0
}

run_route_change() {
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "INSERT INTO merkle_atomicity_test VALUES (300, 'old-route', 1);" \
        >>"$RESULT_DIR/case.log"
    "$PSQL" "${PSQL_ARGS[@]}" -c "SELECT merkle_recovery_status();" \
        >>"$RESULT_DIR/case.log"
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "UPDATE merkle_atomicity_test
            SET id=301, payload='new-route', version=2
          WHERE id=300;" >>"$RESULT_DIR/case.log"
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=300" 0
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=301" 1
}

run_guards() {
    local rc

    # Turning maintenance off must fail closed rather than commit a heap-only
    # change that leaves the durable Merkle root silently stale.
    set +e
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "SET enable_merkle_index=off;
         INSERT INTO merkle_atomicity_test VALUES (400, 'maintenance-off', 1);" \
        >>"$RESULT_DIR/case.log" 2>&1
    rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || { echo "maintenance-off write unexpectedly succeeded" >&2; return 1; }
    assert_scalar "SELECT count(*) FROM merkle_atomicity_test WHERE id=400" 0

    # A same-transaction table change must not be followed by DDL that drops
    # the relfilenode referenced by the staged delta.
    set +e
    "$PSQL" "${PSQL_ARGS[@]}" <<'SQL' >>"$RESULT_DIR/case.log" 2>&1
BEGIN;
UPDATE merkle_atomicity_test SET payload = 'ddl-guard' WHERE id = 1;
DROP INDEX merkle_atomicity_test_idx;
COMMIT;
SQL
    rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || { echo "same-transaction DROP INDEX unexpectedly succeeded" >&2; return 1; }
    assert_scalar "SELECT to_regclass('merkle_atomicity_test_idx') IS NOT NULL" t

    # Concurrent Merkle builds and non-permanent relations are unsupported in
    # v7; reject them before they can create an index without crash metadata.
    set +e
    local concurrent_options=""
    local unlogged_options=""

    if [[ "$MERKLE_MODE" == dynamic ]]; then
		concurrent_options="WITH (partitions=2, fanout=32, dynamic=on, leaf_capacity=4, merge_threshold=2, update_mode='synchronous_cow')"
        unlogged_options="$concurrent_options"
    fi
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "CREATE INDEX CONCURRENTLY merkle_atomicity_test_idx2
         ON merkle_atomicity_test USING merkle (id) $concurrent_options;" \
        >>"$RESULT_DIR/case.log" 2>&1
    rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || { echo "CREATE INDEX CONCURRENTLY unexpectedly succeeded" >&2; return 1; }

    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "CREATE UNLOGGED TABLE merkle_atomicity_unlogged (id bigint);" \
        >>"$RESULT_DIR/case.log"
    set +e
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "CREATE INDEX merkle_atomicity_unlogged_idx
         ON merkle_atomicity_unlogged USING merkle (id) $unlogged_options;" \
        >>"$RESULT_DIR/case.log" 2>&1
    rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || { echo "UNLOGGED Merkle index unexpectedly succeeded" >&2; return 1; }
    "$PSQL" "${PSQL_ARGS[@]}" -c \
        "DROP TABLE merkle_atomicity_unlogged;" >>"$RESULT_DIR/case.log"

    if [[ "$MERKLE_MODE" == dynamic ]]; then
        # Dynamic routing is a complete-row integrity contract: partial,
        # nullable, and non-unique key shapes must fail at CREATE INDEX.
        set +e
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "CREATE INDEX merkle_atomicity_partial_idx
             ON merkle_atomicity_test USING merkle (id)
             $concurrent_options WHERE id > 0;" \
            >>"$RESULT_DIR/case.log" 2>&1
        rc=$?
        set -e
        [[ "$rc" -ne 0 ]] || { echo "partial dynamic Merkle index unexpectedly succeeded" >&2; return 1; }

        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "CREATE TABLE merkle_atomicity_no_unique (id bigint NOT NULL);" \
            >>"$RESULT_DIR/case.log"
        set +e
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "CREATE INDEX merkle_atomicity_no_unique_idx
             ON merkle_atomicity_no_unique USING merkle (id) $concurrent_options;" \
            >>"$RESULT_DIR/case.log" 2>&1
        rc=$?
        set -e
        [[ "$rc" -ne 0 ]] || { echo "dynamic Merkle index without matching unique key unexpectedly succeeded" >&2; return 1; }
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "DROP TABLE merkle_atomicity_no_unique;" >>"$RESULT_DIR/case.log"

        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "CREATE TABLE merkle_atomicity_nullable (id bigint UNIQUE);" \
            >>"$RESULT_DIR/case.log"
        set +e
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "CREATE INDEX merkle_atomicity_nullable_idx
             ON merkle_atomicity_nullable USING merkle (id) $concurrent_options;" \
            >>"$RESULT_DIR/case.log" 2>&1
        rc=$?
        set -e
        [[ "$rc" -ne 0 ]] || { echo "dynamic Merkle index on nullable key unexpectedly succeeded" >&2; return 1; }
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "DROP TABLE merkle_atomicity_nullable;" >>"$RESULT_DIR/case.log"

        # TRUNCATE must atomically replace the side-table generation with an
        # empty valid tree, and DROP INDEX must set-wise cascade the exact
        # relfilenode generation instead of leaking authoritative state.
        "$PSQL" "${PSQL_ARGS[@]}" <<SQL >>"$RESULT_DIR/case.log"
CREATE TABLE merkle_atomicity_lifecycle (
    id bigint PRIMARY KEY,
    payload text NOT NULL
);
CREATE INDEX merkle_atomicity_lifecycle_idx
ON merkle_atomicity_lifecycle USING merkle (id) $concurrent_options;
INSERT INTO merkle_atomicity_lifecycle VALUES (1, 'before-truncate');
SELECT merkle_recovery_status();
SQL
        local lifecycle_oid
        lifecycle_oid=$(scalar "SELECT 'merkle_atomicity_lifecycle_idx'::regclass::oid")
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_state WHERE index_oid=$lifecycle_oid" 1
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "TRUNCATE merkle_atomicity_lifecycle;" >>"$RESULT_DIR/case.log"
        assert_scalar \
            "SELECT item_count FROM ariabc_internal.merkle_dynamic_state WHERE index_oid=$lifecycle_oid" 0
        assert_scalar \
            "SELECT merkle_dynamic_verify('merkle_atomicity_lifecycle_idx'::regclass)" t
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "INSERT INTO merkle_atomicity_lifecycle VALUES (2, 'after-truncate');" \
            >>"$RESULT_DIR/case.log"
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "SELECT merkle_recovery_status();" >>"$RESULT_DIR/case.log"
        assert_scalar \
            "SELECT merkle_dynamic_verify('merkle_atomicity_lifecycle_idx'::regclass)" t
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "DROP INDEX merkle_atomicity_lifecycle_idx;" >>"$RESULT_DIR/case.log"
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_state WHERE index_oid=$lifecycle_oid" 0
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_node WHERE index_oid=$lifecycle_oid" 0
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_leaf_item WHERE index_oid=$lifecycle_oid" 0
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_build_stage WHERE index_oid=$lifecycle_oid" 0
        assert_scalar \
            "SELECT count(*) FROM ariabc_internal.merkle_dynamic_seen WHERE index_oid=$lifecycle_oid" 0
        "$PSQL" "${PSQL_ARGS[@]}" -c \
            "DROP TABLE merkle_atomicity_lifecycle;" >>"$RESULT_DIR/case.log"
    fi
}

run_build_crash() {
	# Crash immediately after setup's CREATE INDEX/build and committed seed
	# workload, before any checkpoint is requested.  This proves the AM's bulk
	# build emitted enough WAL to reconstruct its main fork and metapage.
	"$PG_CTL" -D "$DATA_DIR" stop -m immediate >/dev/null
	SERVER_RUNNING=0
	start_server
	assert_scalar "SELECT merkle_verify('merkle_atomicity_test'::regclass)" t
	if [[ "$MERKLE_MODE" == dynamic ]]; then
		assert_scalar "SELECT merkle_dynamic_verify('merkle_atomicity_test_idx'::regclass)" t
	fi
}

init_cluster() {
    local attempt

    for attempt in 1 2 3; do
        rm -rf -- "$DATA_DIR"
        echo "initdb_attempt=$attempt" >>"$RESULT_DIR/initdb.log"
        if "$INITDB" -D "$DATA_DIR" -U postgres -A trust --no-sync \
            >>"$RESULT_DIR/initdb.log" 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "initdb failed after 3 attempts" >&2
    return 1
}

init_cluster
start_server
"$PSQL" "${PSQL_ARGS[@]}" -f \
    "$REPO_ROOT/scripts/distributed/sql/raft_apply_ledger_schema.sql" \
    >"$RESULT_DIR/schema.log" 2>&1
setup_args=(-v merkle_mode="$MERKLE_MODE")
if [[ -n "$UPDATE_MODE" ]]; then
    setup_args+=(-v update_mode="$UPDATE_MODE")
fi
"$PSQL" "${PSQL_ARGS[@]}" "${setup_args[@]}" -f "$SCRIPT_DIR/setup.sql" \
    >"$RESULT_DIR/setup.log" 2>&1

case "$CASE_NAME" in
	build_crash) run_build_crash ;;
    precommit_crash) [[ -n "$FAILPOINT" ]] || exit 2; run_precommit_crash ;;
    postcommit_crash) [[ -n "$FAILPOINT" ]] || exit 2; run_postcommit_crash ;;
    applier_crash) echo "applier_crash is removed with the pending-log architecture" >&2; exit 2 ;;
    sql_failure) run_sql_failure ;;
    savepoint) run_savepoint ;;
    route_change) run_route_change ;;
    guards) run_guards ;;
    *) echo "Unknown case: $CASE_NAME" >&2; exit 2 ;;
esac

"$PSQL" "${PSQL_ARGS[@]}" -v merkle_mode="$MERKLE_MODE" -f "$SCRIPT_DIR/verify.sql" \
    >"$RESULT_DIR/verify.log" 2>&1

CASE_STATUS=PASS
{
    echo "status=PASS"
    echo "case=$CASE_NAME"
    echo "failpoint=${FAILPOINT:-none}"
    echo "action=$ACTION"
    echo "merkle_mode=$MERKLE_MODE"
    echo "update_mode=${UPDATE_MODE:-default}"
    echo "port=$PORT"
} >"$RESULT_DIR/result.env"

echo "PASS case=$CASE_NAME failpoint=${FAILPOINT:-none} action=$ACTION"
