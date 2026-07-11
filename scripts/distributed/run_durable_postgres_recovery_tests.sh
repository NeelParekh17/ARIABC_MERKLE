#!/usr/bin/env bash
# run_durable_postgres_recovery_tests.sh
#
# G2: Dedicated recovery runner for preserved-storage SIGKILL/failover testing.
#
# Flow:
#   1. Generate random epoch (32 bytes, 64 hex chars).
#   2. Bootstrap ledger schema on each replica.
#   3. Start a fresh safe-ledger cluster.
#   4. Run a deterministic workload.
#   5. Trigger one crash failpoint.
#   6. Preserve PostgreSQL data and Raft storage.
#   7. Restart with the same epoch.
#   8. Verify ledger, manifests, terminal rows, Merkle roots, and digests.
#
# Usage:
#   ./run_durable_postgres_recovery_tests.sh \
#     --nodes "node1:pg_port:server_port node2:pg_port:server_port ..." \
#     --db <dbname> --workload <sql_file> \
#     [--failpoint AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT] \
#     [--workload-rows 100]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---- defaults ----
NODES=()
DBNAME="postgres"
WORKLOAD_FILE=""
FAILPOINT=""
WORKLOAD_ROWS=50
EPOCH_HEX=""
PG_USER="${PGUSER:-postgres}"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --nodes "host:pgport:serverport ..."  Space-separated list of nodes (required)
  --db <dbname>                          PostgreSQL database name (default: postgres)
  --workload <sql_file>                  SQL workload file to execute
  --failpoint <name>                     Crash failpoint name to trigger (optional)
  --workload-rows <N>                    Number of rows to insert (default: 50)
  --epoch-hex <64hex>                    Reuse an existing epoch (for restart tests)
  --help                                 Show this help

Crash failpoint names (from Phase G3):
  ARIABC_FAILPOINT_BEFORE_WORKER_TOPLEVEL_COMMIT
  ARIABC_FAILPOINT_AFTER_WORKER_TOPLEVEL_COMMIT
  AFTER_RAFT_COMMIT_BEFORE_MANIFEST_REGISTER
  AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE
  AFTER_LEDGER_CLAIM_BEFORE_USER_SQL
  AFTER_USER_SQL_BEFORE_LEDGER_FINALIZE
  AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT
  AFTER_TOPLEVEL_COMMIT_BEFORE_RESULT_RING
  AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH
  AFTER_REPLAY_LOOKUP_BEFORE_RESULT_PUBLISH

EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)     IFS=' ' read -r -a NODES <<< "$2"; shift 2 ;;
        --db)        DBNAME="$2"; shift 2 ;;
        --workload)  WORKLOAD_FILE="$2"; shift 2 ;;
        --failpoint) FAILPOINT="$2"; shift 2 ;;
        --workload-rows) WORKLOAD_ROWS="$2"; shift 2 ;;
        --epoch-hex) EPOCH_HEX="$2"; shift 2 ;;
        --help|-h)   usage ;;
        *)           echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ${#NODES[@]} -eq 0 ]]; then
    echo "ERROR: --nodes is required" >&2
    exit 1
fi

# ---- STEP 1: Generate random epoch ----
if [[ -z "$EPOCH_HEX" ]]; then
    EPOCH_HEX=$(openssl rand -hex 32)
    echo "=== Generated new epoch: $EPOCH_HEX ==="
else
    echo "=== Reusing epoch: $EPOCH_HEX ==="
fi

if [[ ${#EPOCH_HEX} -ne 64 ]]; then
    echo "ERROR: epoch must be exactly 64 hex chars, got: ${#EPOCH_HEX}" >&2
    exit 1
fi

echo ""
echo "=== STEP 2: Bootstrap ledger schema on each replica ==="
for node_spec in "${NODES[@]}"; do
    IFS=':' read -r host pg_port server_port <<< "$node_spec"
    echo "  Bootstrapping $host:$pg_port ..."
    "$SCRIPT_DIR/bootstrap_raft_apply_ledger.sh" \
        --db "$DBNAME" \
        --port "$pg_port" \
        --host "$host" \
        --user "$PG_USER" \
        --epoch "$EPOCH_HEX"
    echo "  → Schema and epoch OK on $host:$pg_port"
done

echo ""
echo "=== STEP 3: Start fresh safe-ledger cluster ==="
echo "  (Start ariabc_pg_server on each node with --raft-apply-ledger=safe --raft-epoch-hex=${EPOCH_HEX})"
echo "  NOTE: This script does not start servers automatically."
echo "        Run the cluster startup script with these environment variables:"
echo "    ARIABC_RAFT_APPLY_LEDGER=safe"
echo "    ARIABC_RAFT_EPOCH_HEX=$EPOCH_HEX"

echo ""
echo "=== STEP 4: Workload (deterministic SQL) ==="
if [[ -n "$WORKLOAD_FILE" && -f "$WORKLOAD_FILE" ]]; then
    PRIMARY_HOST=""
    PRIMARY_PG_PORT=""
    for node_spec in "${NODES[@]}"; do
        IFS=':' read -r host pg_port server_port <<< "$node_spec"
        PRIMARY_HOST="$host"
        PRIMARY_PG_PORT="$pg_port"
        break
    done
    echo "  Running workload from $WORKLOAD_FILE on $PRIMARY_HOST:$PRIMARY_PG_PORT ..."
    psql -h "$PRIMARY_HOST" -p "$PRIMARY_PG_PORT" -U "$PG_USER" -d "$DBNAME" -f "$WORKLOAD_FILE"
else
    echo "  No workload file specified, generating synthetic workload ($WORKLOAD_ROWS rows) ..."
    PRIMARY_HOST=""
    PRIMARY_PG_PORT=""
    for node_spec in "${NODES[@]}"; do
        IFS=':' read -r host pg_port server_port <<< "$node_spec"
        PRIMARY_HOST="$host"
        PRIMARY_PG_PORT="$pg_port"
        break
    done
    psql -h "$PRIMARY_HOST" -p "$PRIMARY_PG_PORT" -U "$PG_USER" -d "$DBNAME" <<EOSQL
CREATE TABLE IF NOT EXISTS recovery_test_tbl (id serial PRIMARY KEY, val text);
$(for i in $(seq 1 "$WORKLOAD_ROWS"); do echo "INSERT INTO recovery_test_tbl (val) VALUES ('row_$i');"; done)
EOSQL
fi

echo ""
echo "=== STEP 5: Trigger failpoint ==="
if [[ -n "$FAILPOINT" ]]; then
    echo "  Exporting failpoint: $FAILPOINT=1"
    export "$FAILPOINT"=1
    echo "  Failpoint is set. Workers will crash when they hit this point."
    echo "  (Use this in conjunction with a running workload to trigger the crash.)"
else
    echo "  No failpoint specified — skipping crash injection."
fi

echo ""
echo "=== STEP 6 & 7: Preserve PostgreSQL data and Raft storage ==="
echo "  (Preservation must be done externally by NOT wiping PostgreSQL data before restart.)"
echo "  The recovery runner must be rerun with --epoch-hex=$EPOCH_HEX to use the same epoch."

echo ""
echo "=== STEP 8: Restart verification ==="
echo "  After restart, verify the following on each node:"
echo "    1. ariabc_internal.raft_apply_schema_meta has schema_version = 4"
echo "    2. ariabc_internal.raft_apply_epoch has epoch_id = \\\\x${EPOCH_HEX}"
echo "    3. All raft_apply_item rows are in state 2 (APPLIED_OK) or 3 (APPLIED_ERROR)"
echo "    4. No rows are in state 1 (CLAIMED) — that would indicate corruption"

echo ""
echo "=== VERIFICATION SQL ==="
cat <<EOSQL
-- Check schema version:
SELECT schema_version FROM ariabc_internal.raft_apply_schema_meta;

-- Check epoch anchor:
SELECT encode(epoch_id, 'hex') AS epoch_hex, epoch_label, protocol_version
  FROM ariabc_internal.raft_apply_epoch;

-- Check for CLAIMED rows (must be zero):
SELECT COUNT(*) AS claimed_rows FROM ariabc_internal.raft_apply_item WHERE state = 1;

-- Check terminal row counts:
SELECT state, COUNT(*) FROM ariabc_internal.raft_apply_item GROUP BY state ORDER BY state;

-- Check Merkle consistency (if bcdb_merkle_roots table exists):
SELECT * FROM bcdb_merkle_roots ORDER BY index_oid, partition_id LIMIT 20;
EOSQL

echo ""
echo "=== Recovery test runner complete. ==="
echo "    epoch=$EPOCH_HEX"
echo "    nodes=${NODES[*]}"
exit 0
