#!/usr/bin/env bash
# bootstrap_raft_apply_ledger.sh
# Bootstraps the AriaBC raft-apply-ledger schema and epoch metadata before Raft server start.
#
# Usage:
#   ./bootstrap_raft_apply_ledger.sh \
#       --db <dbname> --port <port> --epoch <64-lowercase-hex> \
#       [--host <host>] [--user <user>]
#   ./bootstrap_raft_apply_ledger.sh \
#       --db <dbname> --port <port> --schema-only [--reset-for-restore] \
#       [--host <host>] [--user <user>]
#
# Requirements:
#   - epoch must be exactly 64 lowercase hex characters (no uppercase).
#   - psql must be on PATH.
#   - Database must be reachable; script aborts on any SQL error.

set -euo pipefail

usage() {
    echo "Usage: $0 --db <dbname> --port <port> (--epoch <64-lowercase-hex-epoch> | --schema-only [--reset-for-restore]) [--host <host>] [--user <user>] [--clean]"
    exit 1
}

DBNAME=""
PORT=""
EPOCH=""
HOST="localhost"
USER=""

CLEAN="0"
SCHEMA_ONLY="0"
RESET_FOR_RESTORE="0"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)    DBNAME="$2"; shift 2 ;;
        --port)  PORT="$2";   shift 2 ;;
        --epoch) EPOCH="$2";  shift 2 ;;
        --host)  HOST="$2";   shift 2 ;;
        --user)  USER="$2";   shift 2 ;;
        --clean) CLEAN="1";   shift ;;
        --schema-only) SCHEMA_ONLY="1"; shift ;;
        --reset-for-restore) RESET_FOR_RESTORE="1"; shift ;;
        *)       usage ;;
    esac
done

if [[ -z "$DBNAME" || -z "$PORT" ]]; then
    usage
fi
if [[ "$SCHEMA_ONLY" -eq 1 && -n "$EPOCH" ]]; then
    echo "Error: --schema-only and --epoch are mutually exclusive." >&2
    exit 1
fi
if [[ "$SCHEMA_ONLY" -eq 1 && "$CLEAN" -eq 1 ]]; then
    echo "Error: --clean requires --epoch and cannot be used with --schema-only." >&2
    exit 1
fi
if [[ "$RESET_FOR_RESTORE" -eq 1 && "$SCHEMA_ONLY" -eq 0 ]]; then
    echo "Error: --reset-for-restore requires --schema-only." >&2
    exit 1
fi
if [[ "$SCHEMA_ONLY" -eq 0 && -z "$EPOCH" ]]; then
    usage
fi

# Require exactly 64 strictly lowercase hex characters.
# The server safe-mode epoch check requires lowercase; uppercase is rejected.
if [[ "$SCHEMA_ONLY" -eq 0 && ! "$EPOCH" =~ ^[0-9a-f]{64}$ ]]; then
    echo "Error: Epoch must be exactly 64 lowercase hex characters (no uppercase)." >&2
    exit 1
fi

echo "=== Bootstrapping AriaBC Apply Ledger Schema and Merkle Recovery ==="
echo "Database: $DBNAME"
echo "Port:     $PORT"
echo "Host:     $HOST"
echo "Mode:     $([[ "$SCHEMA_ONLY" -eq 1 ]] && echo schema-only || echo ledger-epoch)"
if [[ "$SCHEMA_ONLY" -eq 0 ]]; then
    echo "Epoch:    $EPOCH"
fi
echo "Clean:    $CLEAN"
echo "Reset:    $RESET_FOR_RESTORE"

# Build psql base arguments.  ON_ERROR_STOP=1 causes psql to exit non-zero on
# any SQL error so the script aborts rather than silently continuing.
PSQL_ARGS=( -d "$DBNAME" -p "$PORT" -h "$HOST" -v ON_ERROR_STOP=1 )
if [[ -n "$USER" ]]; then
    PSQL_ARGS+=( -U "$USER" )
fi

# Step 1: Apply schema.
SQL_FILE="$(dirname "$0")/sql/raft_apply_ledger_schema.sql"
if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: Schema SQL file not found at $SQL_FILE" >&2
    exit 1
fi

echo "Applying schema..."
psql "${PSQL_ARGS[@]}" -f "$SQL_FILE"

if [[ "$CLEAN" -eq 1 ]]; then
    echo "Cleaning existing rows for epoch $EPOCH..."
    psql "${PSQL_ARGS[@]}" -c "
    DO \$\$
    DECLARE
      applied bigint;
      pending bigint;
    BEGIN
      SELECT applied_seq INTO applied
        FROM ariabc_internal.merkle_apply_state WHERE singleton;
      SELECT COALESCE(max(merkle_apply_seq), 0) INTO pending
        FROM ariabc_internal.raft_apply_item
       WHERE epoch_id = decode('$EPOCH', 'hex');
      IF pending > applied THEN
        RAISE EXCEPTION '--clean cannot remove an unapplied epoch prefix (max_seq=% applied_seq=%)', pending, applied;
      END IF;
    END \$\$;
    DELETE FROM ariabc_internal.raft_apply_item WHERE epoch_id = decode('$EPOCH', 'hex');
    DELETE FROM ariabc_internal.raft_apply_entry_item WHERE epoch_id = decode('$EPOCH', 'hex');
    DELETE FROM ariabc_internal.raft_apply_entry WHERE epoch_id = decode('$EPOCH', 'hex');
    "
fi

# Step 2: Validate schema_meta has exactly one row with version = 5.
echo "Validating schema version..."
SCHEMA_CHECK=$(psql "${PSQL_ARGS[@]}" -t -A -c "
SELECT count(*), min(schema_version), max(schema_version)
FROM ariabc_internal.raft_apply_schema_meta;
")
SCHEMA_COUNT=$(echo "$SCHEMA_CHECK" | cut -d'|' -f1)
SCHEMA_MIN=$(echo "$SCHEMA_CHECK"   | cut -d'|' -f2)
SCHEMA_MAX=$(echo "$SCHEMA_CHECK"   | cut -d'|' -f3)
if [[ "$SCHEMA_COUNT" -ne 1 || "$SCHEMA_MIN" -ne 5 || "$SCHEMA_MAX" -ne 5 ]]; then
    echo "Error: schema_meta must have exactly one row with schema_version=5; got count=$SCHEMA_COUNT min=$SCHEMA_MIN max=$SCHEMA_MAX" >&2
    exit 1
fi
echo "Schema version OK (version=5)."

# A benchmark restore replaces the complete user table and Merkle index. Old
# direct/raft delta rows therefore refer to state that is intentionally being
# discarded and may contain relation OIDs from a previous table incarnation.
if [[ "$RESET_FOR_RESTORE" -eq 1 ]]; then
    echo "Resetting Merkle recovery queues before full table restore..."
    psql "${PSQL_ARGS[@]}" -c "
BEGIN;
DELETE FROM ariabc_internal.raft_apply_item;
DELETE FROM ariabc_internal.raft_apply_entry_item;
DELETE FROM ariabc_internal.raft_apply_entry;
TRUNCATE ariabc_internal.merkle_local_delta;
UPDATE ariabc_internal.merkle_apply_counter
   SET next_seq = 0,
       terminal_prefix_seq = 0
 WHERE singleton;
UPDATE ariabc_internal.merkle_apply_state
   SET applied_seq = 0,
       state = 0,
       error_text = NULL,
       updated_at = clock_timestamp()
 WHERE singleton;
COMMIT;
"
    psql "${PSQL_ARGS[@]}" -c "
DO \$\$
DECLARE
  s jsonb;
BEGIN
  s := pg_catalog.merkle_recovery_status()::jsonb;
  IF s->>'state' <> 'READY' OR (s->>'applied_seq')::bigint <> 0 OR
     (s->>'target_seq')::bigint <> 0 THEN
    RAISE EXCEPTION 'Merkle recovery reset is not READY at sequence zero: %', s;
  END IF;
END
\$\$;
"
    echo "Merkle recovery reset complete; table restore may proceed."
    exit 0
fi

# Step 3: Insert and validate the epoch anchor for safe-ledger mode.
if [[ "$SCHEMA_ONLY" -eq 0 ]]; then
    echo "Registering epoch anchor..."
    psql "${PSQL_ARGS[@]}" -c "
INSERT INTO ariabc_internal.raft_apply_epoch (epoch_id, epoch_label, protocol_version)
VALUES (decode('$EPOCH', 'hex'), 'raft-safe-recovery-epoch', 1)
ON CONFLICT (epoch_id) DO NOTHING;
"

    PROTO_VER=$(psql "${PSQL_ARGS[@]}" -t -A -c "
SELECT protocol_version
FROM ariabc_internal.raft_apply_epoch
WHERE epoch_id = decode('$EPOCH', 'hex');
")
    ROW_COUNT=$(echo "$PROTO_VER" | grep -c '^[0-9]' || true)
    if [[ "$ROW_COUNT" -ne 1 ]]; then
        echo "Error: Epoch verification failed; expected exactly 1 matching row, got $ROW_COUNT" >&2
        exit 1
    fi
    PROTO_VER_VAL=$(echo "$PROTO_VER" | head -1 | tr -d '[:space:]')
    if [[ "$PROTO_VER_VAL" -ne 1 ]]; then
        echo "Error: Epoch row has unexpected protocol_version=$PROTO_VER_VAL (expected 1)" >&2
        exit 1
    fi

    echo "Ledger schema and epoch registration complete (epoch=$EPOCH protocol_version=1)."
else
    echo "Ledger schema installation complete (schema-only mode)."
fi

# Native v8 indexes publish their committed roots in the originating
# transaction. There is no deferred Merkle queue or legacy-index rebuild step.
echo "Validating native Merkle v8 recovery state..."
psql "${PSQL_ARGS[@]}" -v ON_ERROR_STOP=1 -c "
DO \$\$
DECLARE
  s jsonb;
BEGIN
  s := pg_catalog.merkle_recovery_status()::jsonb;
  IF s->>'state' <> 'READY' THEN
    RAISE EXCEPTION 'Merkle recovery is not READY after bootstrap: %', s;
  END IF;
  IF NOT COALESCE((
    SELECT bool_and(pg_catalog.merkle_verify_index(i.indexrelid))
      FROM pg_catalog.pg_index i
      JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
      JOIN pg_catalog.pg_am am ON am.oid = c.relam
     WHERE am.amname = 'merkle'
  ), true) THEN
    RAISE EXCEPTION 'Merkle verification failed after bootstrap';
  END IF;
END
\$\$;
"

exit 0
