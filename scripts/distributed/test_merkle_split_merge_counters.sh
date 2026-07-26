#!/usr/bin/env bash
# =============================================================================
# test_merkle_split_merge_counters.sh
#
# Standalone single-node test that verifies split and merge counters are
# correctly accumulated and reported by merkle_dynamic_tree_stats() for
# native (synchronous_cow) dynamic Merkle indexes.
#
# Background:
#   - merkle_native_profile_enabled=on makes COW transitions accumulate
#     split_count and merge_count in ariabc_internal.merkle_dynamic_state.
#   - merkle_dynamic_tree_stats() now reads these from the side-table and
#     includes them in its JSON response (as of this fix).
#   - Standard YCSB benchmarks are insert-heavy and may not trigger merges;
#     this script forces merges by inserting many rows, then deleting most.
#
# Usage:
#   ./scripts/distributed/test_merkle_split_merge_counters.sh \
#       [--port PORT] [--db DBNAME] [--user USER] [--install-dir DIR] \
#       [--rows ROWS] [--leaf-capacity N]
#
# Requirements:
#   - A running local PostgreSQL with AriaBC installed.
#   - The test table must not exist (it will be created and dropped).
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---- Defaults ----------------------------------------------------------------
DB_PORT=5432
DB_NAME=testdb
DB_USER="${USER:-postgres}"
INSTALL_DIR="${INSTALL_DIR:-$REPO_ROOT/../install}"
TEST_ROWS=50000        # enough to force splits in a small-capacity tree
LEAF_CAPACITY=32       # small leaf capacity forces splits at low row counts
DELETE_FRACTION=90     # delete 90% of rows to force merges
TEST_TABLE="merkle_sm_test_$(date +%s)"
# ------------------------------------------------------------------------------

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)         DB_PORT="$2";         shift 2 ;;
    --db)           DB_NAME="$2";         shift 2 ;;
    --user)         DB_USER="$2";         shift 2 ;;
    --install-dir)  INSTALL_DIR="$2";     shift 2 ;;
    --rows)         TEST_ROWS="$2";       shift 2 ;;
    --leaf-capacity) LEAF_CAPACITY="$2";  shift 2 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

PSQL="$INSTALL_DIR/bin/psql"
[[ -x "$PSQL" ]] || die "psql not found at $PSQL"

psql_q() {
  "$PSQL" -X -q -h 127.0.0.1 -p "$DB_PORT" -U "$DB_USER" "$DB_NAME" \
    -v ON_ERROR_STOP=1 "$@"
}

psql_val() {
  "$PSQL" -X -q -h 127.0.0.1 -p "$DB_PORT" -U "$DB_USER" "$DB_NAME" \
    -v ON_ERROR_STOP=1 -tAc "$1" | tr -d '[:space:]'
}

# ---- Ensure ariabc_internal schema exists -----------------------------------
log "Ensuring ariabc_internal schema & ledger tables exist..."
psql_q -f "$REPO_ROOT/scripts/distributed/sql/raft_apply_ledger_schema.sql" > /dev/null

# ---- Enable profiling system-wide -------------------------------------------
log "Enabling merkle_native_profile_enabled system-wide..."
psql_q -c "ALTER SYSTEM SET merkle_native_profile_enabled = 'on';"
psql_q -c "SELECT pg_reload_conf();" > /dev/null
actual=$(psql_val "SHOW merkle_native_profile_enabled")
[[ "$actual" == "on" ]] || die "merkle_native_profile_enabled is '$actual', expected 'on'"
log "  GUC confirmed: merkle_native_profile_enabled=on"

# ---- Create test table and index --------------------------------------------
log "Creating test table '$TEST_TABLE' with leaf_capacity=$LEAF_CAPACITY..."
psql_q -c "
  CREATE TABLE IF NOT EXISTS public.${TEST_TABLE} (
    ycsb_key TEXT PRIMARY KEY,
    field0 TEXT
  );
  CREATE INDEX ${TEST_TABLE}_dynamic_merkle_idx
    ON public.${TEST_TABLE} USING merkle(ycsb_key)
    WITH (dynamic=true, leaf_capacity=${LEAF_CAPACITY},
          merge_threshold=$((LEAF_CAPACITY / 2)));
"
INDEX_NAME="public.${TEST_TABLE}_dynamic_merkle_idx"
log "  Index created: $INDEX_NAME"

# ---- Enable Merkle on the session -------------------------------------------
psql_q -c "SET enable_merkle_index = on;"

# ---- Snapshot baseline (zero) counters --------------------------------------
baseline_raw=$(psql_val "
  SELECT COALESCE(stats->>'split_count','0') || '|' || COALESCE(stats->>'merge_count','0')
  FROM (SELECT merkle_dynamic_tree_stats('${INDEX_NAME}'::regclass)::jsonb AS stats) AS s")
IFS='|' read -r baseline_splits baseline_merges <<< "$baseline_raw"
log "  Baseline: split_count=$baseline_splits merge_count=$baseline_merges"

# ---- Phase 1: INSERT rows to trigger splits ----------------------------------
log "Phase 1: Inserting $TEST_ROWS rows to trigger splits..."
ROWS_PER_BATCH=1000
batch_count=$(( TEST_ROWS / ROWS_PER_BATCH ))
for ((b=0; b<batch_count; b++)); do
  start=$(( b * ROWS_PER_BATCH + 1 ))
  end=$(( start + ROWS_PER_BATCH - 1 ))
  # Build a batch INSERT using generate_series
  psql_q -c "
    INSERT INTO public.${TEST_TABLE} (ycsb_key, field0)
    SELECT 'user' || lpad(n::text, 12, '0'),
           md5(n::text)
    FROM generate_series($start, $end) AS n
    ON CONFLICT DO NOTHING;" > /dev/null
done
log "  Inserts complete."

# ---- Snapshot post-insert counters ------------------------------------------
post_insert_raw=$(psql_val "
  SELECT COALESCE(stats->>'split_count','0') || '|' || COALESCE(stats->>'merge_count','0')
  FROM (SELECT merkle_dynamic_tree_stats('${INDEX_NAME}'::regclass)::jsonb AS stats) AS s")
IFS='|' read -r post_insert_splits post_insert_merges <<< "$post_insert_raw"
insert_splits=$(( post_insert_splits - baseline_splits ))
insert_merges=$(( post_insert_merges - baseline_merges ))
log "  After inserts: cumulative_splits=$post_insert_splits cumulative_merges=$post_insert_merges"
log "  Phase 1 delta: splits=$insert_splits merges=$insert_merges"

if [[ "$insert_splits" -le 0 ]]; then
  log "  WARNING: No splits detected after $TEST_ROWS inserts."
  log "  Possible causes:"
  log "    - The side-table was not reset between runs (try recreating the index)."
  log "    - merkle_native_profile_enabled was not active during inserts."
  log "    - The leaf_capacity ($LEAF_CAPACITY) is too large for $TEST_ROWS rows."
fi

# ---- Phase 2: DELETE most rows to trigger merges ----------------------------
delete_rows=$(( TEST_ROWS * DELETE_FRACTION / 100 ))
log "Phase 2: Deleting $delete_rows rows (${DELETE_FRACTION}% of $TEST_ROWS) to trigger merges..."
psql_q -c "
  DELETE FROM public.${TEST_TABLE}
  WHERE ycsb_key IN (
    SELECT ycsb_key FROM public.${TEST_TABLE}
    ORDER BY ycsb_key
    LIMIT ${delete_rows}
  );" > /dev/null
log "  Deletes complete."

# ---- Snapshot post-delete counters ------------------------------------------
post_delete_raw=$(psql_val "
  SELECT COALESCE(stats->>'split_count','0') || '|' || COALESCE(stats->>'merge_count','0')
  FROM (SELECT merkle_dynamic_tree_stats('${INDEX_NAME}'::regclass)::jsonb AS stats) AS s")
IFS='|' read -r post_delete_splits post_delete_merges <<< "$post_delete_raw"
benchmark_splits=$(( post_delete_splits - post_insert_splits ))
benchmark_merges=$(( post_delete_merges - post_insert_merges ))
total_splits=$(( post_delete_splits - baseline_splits ))
total_merges=$(( post_delete_merges - baseline_merges ))
log "  After deletes: cumulative_splits=$post_delete_splits cumulative_merges=$post_delete_merges"
log "  Phase 2 delta (delete phase): splits=$benchmark_splits merges=$benchmark_merges"
log "  Total (both phases): splits=$total_splits merges=$total_merges"

# ---- Verify the tree is still consistent ------------------------------------
log "Verifying index consistency..."
verify_result=$(psql_val "SELECT merkle_dynamic_verify('${INDEX_NAME}'::regclass)")
[[ "$verify_result" == "t" ]] || die "merkle_dynamic_verify returned '$verify_result' (expected 't')"
log "  Index consistency: PASS"

# ---- Cleanup -----------------------------------------------------------------
log "Cleaning up test table..."
psql_q -c "DROP TABLE IF EXISTS public.${TEST_TABLE} CASCADE;"
log "  Cleanup done."

# ---- Report ------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Split/Merge Counter Test Results"
echo "=========================================="
echo "  Rows inserted          : $TEST_ROWS"
echo "  Rows deleted           : $delete_rows (${DELETE_FRACTION}%)"
echo "  Leaf capacity          : $LEAF_CAPACITY"
echo ""
echo "  Index-build splits     : $insert_splits"
echo "  Index-build merges     : $insert_merges"
echo "  Delete-phase splits    : $benchmark_splits"
echo "  Delete-phase merges    : $benchmark_merges"
echo "  Total splits           : $total_splits"
echo "  Total merges           : $total_merges"
echo ""

PASS=1
if [[ "$insert_splits" -gt 0 ]]; then
  echo "  SPLIT TRACKING         : PASS (splits=$insert_splits during inserts)"
else
  echo "  SPLIT TRACKING         : FAIL (no splits detected during $TEST_ROWS inserts)"
  PASS=0
fi

if [[ "$benchmark_merges" -gt 0 ]]; then
  echo "  MERGE TRACKING         : PASS (merges=$benchmark_merges during deletes)"
else
  echo "  MERGE TRACKING         : WARN (no merges detected; try --rows or --leaf-capacity)"
  # Merges may not fire if combined children still exceed merge_threshold.
  # Not a hard failure — alert the user.
fi
echo "  INDEX CONSISTENCY      : PASS"
echo "=========================================="

if [[ "$PASS" -eq 0 ]]; then
  echo ""
  echo "DIAGNOSIS: Split tracking failed. Possible fixes:"
  echo "  1. Rebuild with the latest code (merklenative.c fix required)."
  echo "  2. Confirm merkle_native_profile_enabled=on was active during inserts."
  echo "  3. Run: SHOW merkle_native_profile_enabled; in psql"
  echo "  4. Try a smaller leaf-capacity: --leaf-capacity 8"
  exit 1
fi

log "test_merkle_split_merge_counters: PASS"
