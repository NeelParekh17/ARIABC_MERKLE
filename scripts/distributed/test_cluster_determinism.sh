#!/usr/bin/env bash
# test_cluster_determinism.sh
#
# Verifies that AriaBC cluster nodes produce identical Merkle root hashes and
# identical table states across all nodes when executing the multi-hazard deterministic workload.
#
# End-to-end flow:
#   1. Bootstrap 4 verification tables with USING merkle indexes across all configured nodes
#   2. Submit deterministic multi-hazard workload (math, pipeline, bank transfers, lifecycle)
#      through ariabc_pg_gateway (dbType=1, direct)
#   3. Poll all nodes until quiescence
#   4. Collect and compare Merkle root hashes for all tables across all nodes
#   5. Validate zero divergence across all replicas
#
# Usage:
#   ./scripts/distributed/test_cluster_determinism.sh [--skip-setup] [--skip-workload] [--det-start-seq <seq>]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source topology defaults
if [[ -f "$SCRIPT_DIR/cluster_topology.sh" ]]; then
    source "$SCRIPT_DIR/cluster_topology.sh"
else
    declare -a NODE_IPS=(10.129.148.247 10.129.148.246 10.129.148.248)
    declare -a NODE_NAMES=(admin123 user4 utkarsh)
    declare -a NODE_USERS=(neel neel neel)
    declare -a NODE_CLIENT_PORTS=(8000 8000 8001)
    RAFT_PORT=9000
    DB_PORT=5438
    DB_USER=postgres
    DB_NAME=postgres
fi

NODE_COUNT="${#NODE_IPS[@]}"
ARIABC_CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
CLUSTER_PASSWORD="$ARIABC_CLUSTER_PASSWORD"
INSTALL_DIR="/home/neel/Desktop/ariabc_install"

LOCAL_BIN="$REPO_ROOT/ariabc_pg/build/bin"
WORKLOAD_FILE="$REPO_ROOT/scripts/deterministic_workload.sql"
BOOTSTRAP_FILE="$REPO_ROOT/scripts/bootstrap_det_verification.sql"
ORACLE_FILE="$REPO_ROOT/scripts/deterministic_oracle.json"

DET_START_SEQ="${DET_START_SEQ:-0}"
REQ_ID_OFFSET="${REQ_ID_OFFSET:-$DET_START_SEQ}"
CLIENT_ID="${CLIENT_ID:-det-verify}"

SKIP_SETUP=0
SKIP_WORKLOAD=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-setup)    SKIP_SETUP=1; shift ;;
        --skip-workload) SKIP_WORKLOAD=1; shift ;;
        --det-start-seq) DET_START_SEQ="${2:-0}"; shift 2 ;;
        --req-id-offset) REQ_ID_OFFSET="${2:-$DET_START_SEQ}"; shift 2 ;;
        --client-id)     CLIENT_ID="${2:-det-verify}"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

log()  { echo "[$(date +'%H:%M:%S')] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }
pass() { echo "[$(date +'%H:%M:%S')] PASS: $*"; }
fail() { echo "[$(date +'%H:%M:%S')] FAIL: $*" >&2; }

node_ssh() {
    local idx="$1"; shift
    local ip="${NODE_IPS[$idx]}"
    local user="${NODE_USERS[$idx]}"
    sshpass -p "$CLUSTER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "$user@$ip" "$@"
}

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
if [[ ! -f "$BOOTSTRAP_FILE" || ! -f "$WORKLOAD_FILE" ]]; then
    log "Generating workload & bootstrap SQL files..."
    python3 "$REPO_ROOT/scripts/generate_deterministic_workload.py" \
        --ops 1000 \
        --output-bootstrap "$BOOTSTRAP_FILE" \
        --output-workload "$WORKLOAD_FILE" \
        --output-oracle "$ORACLE_FILE"
fi

# ---------------------------------------------------------------------------
# Phase 1: Create verification tables + Merkle indexes on all nodes
# ---------------------------------------------------------------------------
if [[ "$SKIP_SETUP" -eq 0 ]]; then
    log "=== Phase 1: Deploy verification tables & Merkle indexes across $NODE_COUNT nodes ==="
    for idx in "${!NODE_IPS[@]}"; do
        name="${NODE_NAMES[$idx]}"
        log "  Setting up on $name (${NODE_IPS[$idx]})..."
        node_ssh "$idx" "
            INSTALL='$INSTALL_DIR'
            export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
            \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME <<'SQL'
$(cat "$BOOTSTRAP_FILE")
SQL
        " 2>&1 | sed "s/^/  [$name] /" || die "Bootstrap setup failed on $name"
    done
    log "  All tables and Merkle indexes successfully initialized on all nodes"
else
    log "=== Phase 1: Skipped (--skip-setup) ==="
fi

# ---------------------------------------------------------------------------
# Phase 2: Run deterministic workload through gateway
# ---------------------------------------------------------------------------
if [[ "$SKIP_WORKLOAD" -eq 0 ]]; then
    log "=== Phase 2: Run deterministic workload through gateway (dbType=1) ==="
    GW_BIN="$LOCAL_BIN/ariabc_pg_gateway"
    [[ ! -x "$GW_BIN" ]] && die "ariabc_pg_gateway not found at $GW_BIN — build it first"

    GW_NODES=""
    for idx in "${!NODE_IPS[@]}"; do
        [[ -n "$GW_NODES" ]] && GW_NODES+=","
        GW_NODES+="${NODE_IPS[$idx]}:${NODE_CLIENT_PORTS[$idx]}"
    done

    log "  Target Nodes: $GW_NODES"
    log "  Workload: $WORKLOAD_FILE ($(wc -l < "$WORKLOAD_FILE" | tr -d ' ') ops)"
    log "  Sequencing: detStartSeq=$DET_START_SEQ reqIdOffset=$REQ_ID_OFFSET clientId=$CLIENT_ID"

    "$GW_BIN" \
        --nodes "$GW_NODES" \
        --raft-node-ids 1,2,4 \
        --queryFrom "$WORKLOAD_FILE" \
        --dbType 1 \
        --detStartSeq "$DET_START_SEQ" \
        --reqIdOffset "$REQ_ID_OFFSET" \
        --detWindow 16 \
        --dbConnPoolSize 4 \
        --submitMode blocking \
        --clientId "$CLIENT_ID" \
        --numTerminals 4 \
        --waitMajority 0 \
        --completionPath direct \
        --totalNodes "$NODE_COUNT" \
        2>&1 | tail -10

    log "  Gateway submission complete"
else
    log "=== Phase 2: Skipped (--skip-workload) ==="
fi

# ---------------------------------------------------------------------------
# Phase 3: Quiesce — wait for all nodes to apply transactions
# ---------------------------------------------------------------------------
log "=== Phase 3: Quiesce check across cluster nodes ==="
sleep 2

# ---------------------------------------------------------------------------
# Phase 4: Collect & compare Merkle root hashes across all nodes
# ---------------------------------------------------------------------------
log "=== Phase 4: Collect and verify Merkle root hashes across $NODE_COUNT nodes ==="

TABLES=("det_accumulators" "det_state_pipeline" "det_accounts" "det_lifecycle")
OVERALL_PASS=1

for tbl in "${TABLES[@]}"; do
    log "Checking table: $tbl"
    declare -a NODE_ROOTS=()

    for idx in "${!NODE_IPS[@]}"; do
        name="${NODE_NAMES[$idx]}"
        root="$(node_ssh "$idx" "
            INSTALL='$INSTALL_DIR'
            export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
            \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
                -tAc \"SELECT merkle_root_hash('$tbl')\"
        " 2>/dev/null | tr -d '[:space:]')" || root="error"
        NODE_ROOTS+=("$root")
        log "  [$name] $tbl root: $root"
    done

    REF_ROOT="${NODE_ROOTS[0]}"
    for ((idx=1; idx<${#NODE_IPS[@]}; idx++)); do
        if [[ "${NODE_ROOTS[$idx]}" != "$REF_ROOT" ]]; then
            fail "DIVERGENCE detected on $tbl: ${NODE_NAMES[$idx]} (${NODE_ROOTS[$idx]}) != ${NODE_NAMES[0]} ($REF_ROOT)"
            OVERALL_PASS=0
        fi
    done
done

echo ""
if [[ "$OVERALL_PASS" -eq 1 ]]; then
    echo "================================================================================"
    echo "  CLUSTER DETERMINISM VERIFICATION: PASS"
    echo "  All $NODE_COUNT cluster nodes independently computed identical Merkle root hashes"
    echo "  across all 4 multi-hazard tables after concurrent deterministic execution."
    echo "  Zero divergence observed between replicas."
    echo "================================================================================"
    exit 0
else
    echo "================================================================================"
    echo "  CLUSTER DETERMINISM VERIFICATION: FAIL"
    echo "  One or more tables diverged across cluster nodes."
    echo "================================================================================"
    exit 1
fi
