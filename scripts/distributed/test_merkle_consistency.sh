#!/usr/bin/env bash
# test_merkle_consistency.sh
#
# Verifies that all 4 AriaBC cluster nodes produce identical Merkle root hashes
# after executing the same deterministic workload via ariabc_pg_gateway.
#
# End-to-end flow:
#   1. Create ariabc_kv_test + USING merkle index on all 4 nodes via direct psql
#   2. Verify empty Merkle roots are identical (all zeros expected)
#   3. Run merkle_test_workload.sql through gateway (dbType=1, direct, 1 terminal)
#   4. Poll all nodes until quiescence (final updated value visible on all)
#   5. Collect merkle_root_hash('ariabc_kv_test') from each node
#   6. Compare — PASS if all 4 match, FAIL otherwise
#
# Prerequisites:
#   - All 4 ariabc_pg_server processes running (use run_4node_raft_cluster.sh first)
#   - BCDB PostgreSQL running on port 5438 on each node
#   - ariabc_pg_gateway built locally at ariabc_pg/build/bin/ariabc_pg_gateway
#
# Usage:
#   ./test_merkle_consistency.sh [--skip-setup] [--skip-workload]
#   --skip-setup     Skip table/index creation (table already exists on all nodes)
#   --skip-workload  Skip gateway DML run (just collect and compare roots)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Cluster topology (must match run_4node_raft_cluster.sh)
# ---------------------------------------------------------------------------
declare -a NODE_IPS=(10.129.148.236 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)

CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-sunil1165}"

INSTALL_DIR="/home/neel/Desktop/ariabc_install"
DB_PORT=5438
DB_USER=postgres
DB_NAME=postgres

SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

LOCAL_BIN="$REPO_ROOT/ariabc_pg/build/bin"

TEST_TABLE="ariabc_kv_test"
N_EXPECTED_ROWS=50
# Value of v for k=10 after all workload UPDATEs have been applied.
# If this value is visible on a node, all 60 log entries have been applied.
QUIESCE_SENTINEL_KEY=10
QUIESCE_SENTINEL_VAL="val_010_v2"
QUIESCE_TIMEOUT=90

WORKLOAD_FILE="$REPO_ROOT/scripts/merkle_test_workload.sql"
DET_START_SEQ="${DET_START_SEQ:-1}"
REQ_ID_OFFSET="${REQ_ID_OFFSET:-$DET_START_SEQ}"
CLIENT_ID="${CLIENT_ID:-merkle-test}"

SKIP_SETUP=0
SKIP_WORKLOAD=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-setup)    SKIP_SETUP=1; shift ;;
        --skip-workload) SKIP_WORKLOAD=1; shift ;;
        --det-start-seq) DET_START_SEQ="${2:-1}"; shift 2 ;;
        --req-id-offset) REQ_ID_OFFSET="${2:-$DET_START_SEQ}"; shift 2 ;;
        --client-id)     CLIENT_ID="${2:-merkle-test}"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ "$DET_START_SEQ" -lt 1 || "$REQ_ID_OFFSET" -lt 1 ]]; then
    die "--det-start-seq and --req-id-offset must be >= 1"
fi

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

node_psql() {
    local idx="$1"; shift
    node_ssh "$idx" "
        INSTALL='$INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
        \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME $*
    "
}

# ---------------------------------------------------------------------------
# Phase 1: Create table + Merkle index on all 4 nodes via direct psql
# ---------------------------------------------------------------------------
if [[ "$SKIP_SETUP" -eq 0 ]]; then
    log "=== Phase 1: Create $TEST_TABLE + Merkle index on all 4 nodes ==="
    for idx in "${!NODE_IPS[@]}"; do
        name="${NODE_NAMES[$idx]}"
        log "  Setting up on $name..."
        node_ssh "$idx" "
            INSTALL='$INSTALL_DIR'
            export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
            \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME <<'SQL'
DROP TABLE IF EXISTS $TEST_TABLE;
CREATE TABLE $TEST_TABLE (
    k INT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE INDEX idx_merkle_kv ON $TEST_TABLE USING merkle (k);
SQL
        " 2>&1 | sed "s/^/  [$name] /" || die "Setup failed on $name"
    done
    log "  Table and Merkle index created on all nodes"
else
    log "=== Phase 1: Skipped (--skip-setup) ==="
fi

# ---------------------------------------------------------------------------
# Phase 2: Verify empty Merkle roots are identical
# ---------------------------------------------------------------------------
log "=== Phase 2: Verify empty Merkle roots (should be all-zeros on all nodes) ==="

declare -a EMPTY_ROOTS=()
for idx in "${!NODE_IPS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    root="$(node_ssh "$idx" "
        INSTALL='$INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
        \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
            -tAc \"SELECT merkle_root_hash('$TEST_TABLE')\"
    " 2>/dev/null | tr -d '[:space:]')" || die "Could not read Merkle root from $name"
    EMPTY_ROOTS+=("$root")
    log "  [$name] empty_root=$root"
done

EMPTY_PASS=1
for idx in 1 2; do
    if [[ "${EMPTY_ROOTS[$idx]}" != "${EMPTY_ROOTS[0]}" ]]; then
        log "  MISMATCH: ${NODE_NAMES[$idx]}=${EMPTY_ROOTS[$idx]} vs ${NODE_NAMES[0]}=${EMPTY_ROOTS[0]}"
        EMPTY_PASS=0
    fi
done

if [[ "$EMPTY_PASS" -eq 1 ]]; then
    log "  All empty roots match: ${EMPTY_ROOTS[0]}"
else
    die "Empty Merkle roots diverged across nodes — nodes are not in a clean identical state."
fi

# ---------------------------------------------------------------------------
# Phase 3: Run DML workload through gateway in deterministic mode
# ---------------------------------------------------------------------------
if [[ "$SKIP_WORKLOAD" -eq 0 ]]; then
    log "=== Phase 3: Run DML workload through gateway (dbType=1, completionPath=direct) ==="

    GW_BIN="$LOCAL_BIN/ariabc_pg_gateway"
    [[ ! -x "$GW_BIN" ]] && die "ariabc_pg_gateway not found at $GW_BIN — build it first"
    [[ ! -f "$WORKLOAD_FILE" ]] && die "Workload file not found: $WORKLOAD_FILE"

    GW_NODES=""
    for idx in "${!NODE_IPS[@]}"; do
        [[ -n "$GW_NODES" ]] && GW_NODES+=","
        GW_NODES+="${NODE_IPS[$idx]}:${NODE_CLIENT_PORTS[$idx]}"
    done

    log "  Nodes: $GW_NODES"
    log "  Workload: $WORKLOAD_FILE ($(wc -l < "$WORKLOAD_FILE" | tr -d ' ') lines)"
    log "  DET ids: detStartSeq=$DET_START_SEQ reqIdOffset=$REQ_ID_OFFSET clientId=$CLIENT_ID"

    "$GW_BIN" \
        --nodes "$GW_NODES" \
        --queryFrom "$WORKLOAD_FILE" \
        --dbType 1 \
        --detStartSeq "$DET_START_SEQ" \
        --reqIdOffset "$REQ_ID_OFFSET" \
        --detWindow 8 \
        --dbConnPoolSize 2 \
        --submitMode blocking \
        --clientId "$CLIENT_ID" \
        --numTerminals 1 \
        --waitMajority 0 \
        --completionPath direct \
        --totalNodes 4 \
        2>&1 | tail -8

    log "  Gateway completed submission"
else
    log "=== Phase 3: Skipped (--skip-workload) ==="
fi

# ---------------------------------------------------------------------------
# Phase 4: Quiesce — poll all nodes until sentinel value is visible
# The sentinel is UPDATE k=10 SET v='val_010_v2', the last log entry in the
# workload. Once this value is readable on a follower, all prior log entries
# (all 50 INSERTs + 10 UPDATEs) must also have been applied.
# ---------------------------------------------------------------------------
log "=== Phase 4: Quiesce — waiting for all nodes to apply all log entries ==="
log "  Sentinel: k=$QUIESCE_SENTINEL_KEY should have v='$QUIESCE_SENTINEL_VAL' (timeout ${QUIESCE_TIMEOUT}s)"

QUIESCE_START="$(date +%s)"
declare -a NODE_READY=()
for idx in "${!NODE_IPS[@]}"; do NODE_READY+=("0"); done

while true; do
    elapsed=$(( $(date +%s) - QUIESCE_START ))
    all_ready=1

    for idx in "${!NODE_IPS[@]}"; do
        [[ "${NODE_READY[$idx]}" -eq 1 ]] && continue
        name="${NODE_NAMES[$idx]}"

        val="$(node_ssh "$idx" "
            INSTALL='$INSTALL_DIR'
            export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
            \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
                -tAc \"SELECT v FROM $TEST_TABLE WHERE k=$QUIESCE_SENTINEL_KEY\"
        " 2>/dev/null | tr -d '[:space:]')" || true

        if [[ "$val" == "$QUIESCE_SENTINEL_VAL" ]]; then
            NODE_READY[$idx]=1
            log "  [$name] quiesced (${elapsed}s)"
        else
            all_ready=0
        fi
    done

    if [[ "$all_ready" -eq 1 ]]; then
        log "  All 4 nodes quiesced (${elapsed}s)"
        break
    fi

    if [[ "$elapsed" -ge "$QUIESCE_TIMEOUT" ]]; then
        log "  WARNING: quiesce timeout after ${QUIESCE_TIMEOUT}s — some nodes may be behind"
        for idx in "${!NODE_IPS[@]}"; do
            if [[ "${NODE_READY[$idx]}" -eq 0 ]]; then
                cnt="$(node_ssh "$idx" "
                    INSTALL='$INSTALL_DIR'
                    export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
                    \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
                        -tAc 'SELECT count(*) FROM $TEST_TABLE'
                " 2>/dev/null | tr -d '[:space:]')" || cnt="?"
                log "    [${NODE_NAMES[$idx]}] NOT READY — rows=$cnt"
            fi
        done
        break
    fi

    sleep 2
done

# ---------------------------------------------------------------------------
# Phase 5: Collect Merkle root hashes from all 4 nodes
# ---------------------------------------------------------------------------
log "=== Phase 5: Collect Merkle root hashes from all 4 nodes ==="

declare -a ROOTS=()
declare -a ROW_COUNTS=()

for idx in "${!NODE_IPS[@]}"; do
    name="${NODE_NAMES[$idx]}"

    cnt="$(node_ssh "$idx" "
        INSTALL='$INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
        \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
            -tAc 'SELECT count(*) FROM $TEST_TABLE'
    " 2>/dev/null | tr -d '[:space:]')" || cnt="error"
    ROW_COUNTS+=("$cnt")

    root="$(node_ssh "$idx" "
        INSTALL='$INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL/lib:\${LD_LIBRARY_PATH:-}\"
        \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
            -tAc \"SELECT merkle_root_hash('$TEST_TABLE')\"
    " 2>/dev/null | tr -d '[:space:]')" || root="error"
    ROOTS+=("$root")

    log "  [$name] rows=${cnt}  merkle_root=${root}"
done

# ---------------------------------------------------------------------------
# Phase 6: Compare and report
# ---------------------------------------------------------------------------
log ""
log "=== Phase 6: Merkle root consistency verdict ==="

REFERENCE="${ROOTS[0]}"
PASS=1
ALL_ROWS_OK=1

for idx in "${!NODE_IPS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    r="${ROOTS[$idx]}"
    cnt="${ROW_COUNTS[$idx]}"

    # Row count check
    if [[ "$cnt" != "$N_EXPECTED_ROWS" ]]; then
        log "  [$name] WARNING: expected $N_EXPECTED_ROWS rows, got $cnt"
        ALL_ROWS_OK=0
    fi

    # Merkle root comparison
    if [[ "$r" == "$REFERENCE" ]]; then
        log "  [$name] MATCH  rows=$cnt  root=$r"
    else
        log "  [$name] MISMATCH  rows=$cnt  expected=$REFERENCE  got=$r"
        PASS=0
    fi
done

echo ""
if [[ "$PASS" -eq 1 && "$ALL_ROWS_OK" -eq 1 ]]; then
    echo "======================================================"
    echo "  MERKLE CONSISTENCY TEST: PASS"
    echo "  Merkle root (all 4 nodes): $REFERENCE"
    echo "  Table: $TEST_TABLE | Rows: $N_EXPECTED_ROWS | Nodes: 4"
    echo "  All 4 nodes independently computed identical Merkle"
    echo "  root hashes after deterministic distributed execution."
    echo "======================================================"
    exit 0
else
    echo "======================================================"
    echo "  MERKLE CONSISTENCY TEST: FAIL"
    if [[ "$ALL_ROWS_OK" -ne 1 ]]; then
        echo "  Row count mismatch — some log entries did not apply."
    fi
    if [[ "$PASS" -ne 1 ]]; then
        echo "  Nodes disagree on Merkle root — deterministic"
        echo "  execution diverged or replication is incomplete."
    fi
    echo "======================================================"
    exit 1
fi
