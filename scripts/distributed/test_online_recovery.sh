#!/usr/bin/env bash
# ===========================================================================
# test_online_recovery.sh
# End-to-end validation of ProtectDB Algorithm 2 Online Recovery in AriaBC.
#
# Validates both:
#   1. Passive Mode: periodic comparestates check @ 200ms catches corruption
#      and automatically repairs damaged replica.
#   2. Active Mode: in-band per-transaction divergence hook repairs replica.
# ===========================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source topology defaults if present
if [[ -f "$SCRIPT_DIR/cluster_topology.sh" ]]; then
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/cluster_topology.sh"
fi

TEST_TABLE="${TEST_TABLE:-usertable_small}"
DB_PORT="${DB_PORT:-5438}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-}"
NODES_CSV="${NODES_CSV:-}"
MODE="${1:-both}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [test_online_recovery] $*"
}

die() {
  log "FATAL: $*" >&2
  exit 1
}

# If NODES_CSV is not provided, build it from NODE_IPS if available
if [[ -z "$NODES_CSV" && -n "${NODE_IPS:-}" ]]; then
  node_specs=()
  for i in "${!NODE_IPS[@]}"; do
    node_specs+=("${NODE_NAMES[$i]}=${NODE_IPS[$i]}:${DB_PORT}")
  done
  NODES_CSV="$(IFS=,; echo "${node_specs[*]}")"
fi

if [[ -z "$NODES_CSV" ]]; then
  NODES_CSV="node1=127.0.0.1:5432,node2=127.0.0.1:5433"
fi

log "=== Testing Online Merkle Recovery (Algorithm 2) ==="
log "Nodes: $NODES_CSV"
log "Table: $TEST_TABLE"
log "Mode:  $MODE"

# Extract first node as reference and last node as victim/damaged
IFS=',' read -ra ADDR_ARRAY <<< "$NODES_CSV"
[[ ${#ADDR_ARRAY[@]} -ge 2 ]] || die "Need at least 2 nodes to test recovery"

REF_SPEC="${ADDR_ARRAY[0]}"
DMG_SPEC="${ADDR_ARRAY[-1]}"

log "Reference node: $REF_SPEC"
log "Damaged target: $DMG_SPEC"

export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"

# ---------------------------------------------------------------------------
# Test Passive Mode
# ---------------------------------------------------------------------------
test_passive_mode() {
  log "--- [PASSIVE MODE TEST] ---"

  log "Step 1: Check baseline state consistency..."
  python3 "$SCRIPT_DIR/recovery/compare_states.py" \
    --nodes "$NODES_CSV" \
    --table "$TEST_TABLE" \
    --once \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME" || log "Warning: Initial state not matching or nodes unreachable"

  log "Step 2: Injecting corruption fault into damaged target ($DMG_SPEC)..."
  python3 "$SCRIPT_DIR/recovery/fault_injector.py" \
    --target-node "$DMG_SPEC" \
    --table "$TEST_TABLE" \
    --fault-type "update" \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Step 3: Triggering Passive Mode comparestates detection & auto-recovery..."
  python3 "$SCRIPT_DIR/recovery/compare_states.py" \
    --nodes "$NODES_CSV" \
    --table "$TEST_TABLE" \
    --interval-ms 200 \
    --once \
    --auto-recover \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Step 4: Verifying post-recovery cluster consistency..."
  python3 "$SCRIPT_DIR/recovery/compare_states.py" \
    --nodes "$NODES_CSV" \
    --table "$TEST_TABLE" \
    --once \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Passive Mode test SUCCESS: Replica corrupted and successfully healed!"
}

# ---------------------------------------------------------------------------
# Test Active Mode
# ---------------------------------------------------------------------------
test_active_mode() {
  log "--- [ACTIVE MODE TEST] ---"

  log "Step 1: Injecting corruption fault into damaged target ($DMG_SPEC)..."
  python3 "$SCRIPT_DIR/recovery/fault_injector.py" \
    --target-node "$DMG_SPEC" \
    --table "$TEST_TABLE" \
    --fault-type "update" \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Step 2: Triggering Active Mode in-band recovery hook..."
  python3 "$SCRIPT_DIR/recovery/active_recovery_hook.py" \
    --damaged-node "$DMG_SPEC" \
    --reference-node "$REF_SPEC" \
    --req-num 9999 \
    --table "$TEST_TABLE" \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Step 3: Verifying post-recovery cluster consistency..."
  python3 "$SCRIPT_DIR/recovery/compare_states.py" \
    --nodes "$NODES_CSV" \
    --table "$TEST_TABLE" \
    --once \
    --db-user "$DB_USER" \
    --db-name "$DB_NAME"

  log "Active Mode test SUCCESS: In-band divergence successfully repaired!"
}

case "$MODE" in
  passive)
    test_passive_mode
    ;;
  active)
    test_active_mode
    ;;
  both)
    test_passive_mode
    test_active_mode
    ;;
  *)
    die "Unknown mode '$MODE'. Use 'passive', 'active', or 'both'."
    ;;
esac

log "=== All Online Recovery Tests Completed Successfully ==="
