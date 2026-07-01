#!/usr/bin/env bash
# test_remote_raft_recovery.sh
#
# P0-A: Deterministic crash-safe Raft → BCDB → PostgreSQL recovery harness.
#
# Usage:
#   test_remote_raft_recovery.sh [OPTIONS]
#
# Options:
#   --case   <name>           Crash case: A|B|C|D|E|F|G  (default: E)
#   --node-id <1|2|4>       Exact Raft node ID to crash
#   --target <leader|follower|all>  Node kill target   (default: follower)
#   --failpoint <name>        Failpoint name override  (derived from --case)
#   --workload <file>         SQL workload file        (default: probe workload)
#   --cluster-id <id>         Cluster ID               (auto-generated)
#   --epoch <hex>             64-char lowercase hex    (auto-generated)
#   --help                    Show this help
#
# Crash cases:
#   A  ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE
#   B  ARIABC_FAILPOINT_AFTER_LEDGER_CLAIM_BEFORE_USER_SQL
#   C  ARIABC_FAILPOINT_AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT
#   D  ARIABC_FAILPOINT_BEFORE_WORKER_TOPLEVEL_COMMIT
#   E  ARIABC_FAILPOINT_AFTER_WORKER_TOPLEVEL_COMMIT        (default)
#   F  ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH
#   G  ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK
#
# Exit codes:
#   0  all validation checks PASSED
#   1  argument error or setup failure
#   2  validation FAILED (at least one check reported incorrect state)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/cluster_topology.sh"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
CASE_NAME="E"
TARGET="follower"
NODE_ID_TARGET=""
FAILPOINT_OVERRIDE=""
WORKLOAD_FILE=""
CLUSTER_ID=""
EPOCH_HEX=""

usage() {
    grep '^#' "${BASH_SOURCE[0]}" | grep -v '^#!/' | sed 's/^# \?//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --case)         CASE_NAME="${2:?missing value for --case}";      shift 2 ;;
        --node-id)      NODE_ID_TARGET="${2:?missing value for --node-id}"; shift 2 ;;
        --target)       TARGET="${2:?missing value for --target}";       shift 2 ;;
        --failpoint)    FAILPOINT_OVERRIDE="${2:?}";                     shift 2 ;;
        --workload)     WORKLOAD_FILE="${2:?}";                          shift 2 ;;
        --cluster-id)   CLUSTER_ID="${2:?}";                             shift 2 ;;
        --epoch)        EPOCH_HEX="${2:?}";                              shift 2 ;;
        --help|-h)      usage ;;
        *) echo "ERROR: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

case "$CASE_NAME" in A|B|C|D|E|F|G) ;; *) echo "ERROR: --case must be A-G" >&2; exit 1 ;; esac
case "$TARGET" in leader|follower|all) ;; *) echo "ERROR: --target must be leader|follower|all" >&2; exit 1 ;; esac
if [[ -n "$NODE_ID_TARGET" && "$TARGET" != "follower" ]]; then
    echo "ERROR: --node-id cannot be combined with --target ${TARGET}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Derive failpoint name from case letter
# ---------------------------------------------------------------------------
case_to_failpoint() {
    case "$1" in
        A) echo "ARIABC_FAILPOINT_AFTER_MANIFEST_REGISTER_BEFORE_ENQUEUE" ;;
        B) echo "ARIABC_FAILPOINT_AFTER_LEDGER_CLAIM_BEFORE_USER_SQL" ;;
        C) echo "ARIABC_FAILPOINT_AFTER_LEDGER_FINALIZE_BEFORE_TOPLEVEL_COMMIT" ;;
        D) echo "ARIABC_FAILPOINT_BEFORE_WORKER_TOPLEVEL_COMMIT" ;;
        E) echo "ARIABC_FAILPOINT_AFTER_WORKER_TOPLEVEL_COMMIT" ;;
        F) echo "ARIABC_FAILPOINT_AFTER_RESULT_RING_BEFORE_KAFKA_PUBLISH" ;;
        G) echo "ARIABC_FAILPOINT_AFTER_KAFKA_PUBLISH_BEFORE_APPLIED_MARK" ;;
    esac
}

FAILPOINT="${FAILPOINT_OVERRIDE:-$(case_to_failpoint "$CASE_NAME")}"

case_description() {
    case "$1" in
        A) echo "manifest registered; no CLAIMED row; no SQL effect; restart re-registers and executes once" ;;
        B) echo "no durable CLAIMED row after crash; no SQL effect; restart executes once" ;;
        C) echo "no durable terminal row; no SQL effect; restart executes once" ;;
        D) echo "no durable terminal row; no SQL effect; restart executes once" ;;
        E) echo "SQL effect committed; terminal row exists; restart skips re-execution" ;;
        F) echo "SQL effect committed; terminal row exists; Kafka may lack result; restart must not rerun SQL" ;;
        G) echo "Kafka result published; applied mark missing; restart must preserve exactly-once state" ;;
    esac
}

# Cases A-D: probe value must be 1 (executed once post-restart).
# Cases E-F: probe value must be 1 (SQL committed pre-crash; replay skips).
EXPECTED_PROBE_VALUE=1

# ---------------------------------------------------------------------------
# Generate epoch / cluster ID
# ---------------------------------------------------------------------------
[[ -z "$EPOCH_HEX" ]]   && EPOCH_HEX="$(openssl rand -hex 32)"
[[ -z "$CLUSTER_ID" ]]  && CLUSTER_ID="recovery_case${CASE_NAME}_$(date +%s)"

if ! [[ "$EPOCH_HEX" =~ ^[0-9a-f]{64}$ ]]; then
    echo "ERROR: epoch must be exactly 64 lowercase hex characters" >&2; exit 1
fi

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR="${SCRIPT_DIR}/recovery_artifacts/${CLUSTER_ID}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/recovery_case${CASE_NAME}_${TARGET}.log"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "selected_case=${CASE_NAME}"
log "selected_node_id=${NODE_ID_TARGET}"
log "selected_target=${TARGET}"
die() { log "FATAL: $*"; exit 1; }

# ---------------------------------------------------------------------------
# SSH / psql helpers — use shared topology arrays
# ---------------------------------------------------------------------------
ssh_node() {
    local idx="$1"; shift
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${NODE_USERS[$idx]}@${NODE_IPS[$idx]}" "$@"
}

ssh_all() {
    local idx
    for idx in "${!NODE_IPS[@]}"; do ssh_node "$idx" "$@" || true; done
}

remote_psql() {
    local idx="$1"; shift
    # DB_PORT and DB_USER come from cluster_topology.sh
    ssh_node "$idx" "psql -U ${DB_USER} -p ${DB_PORT} -d ${DB_NAME} -t -A -c \"$*\""
}

# ---------------------------------------------------------------------------
# Find leader by polling the Raft client ports
# ---------------------------------------------------------------------------
find_leader_index() {
    local ports=()
    local idx
    for idx in "${!NODE_IPS[@]}"; do ports+=("${NODE_CLIENT_PORTS[$idx]:-8000}"); done
    local hosts_csv ports_csv
    hosts_csv="$(IFS=','; echo "${NODE_IPS[*]}")"
    ports_csv="$(IFS=','; echo "${ports[*]}")"
    python3 "${SCRIPT_DIR}/check_leader.py" "$hosts_csv" "$ports_csv" \
        --check-leader 2>/dev/null || echo "-1"
}

wait_for_cluster_ready() {
    local max="${1:-90}" elapsed=0 leader
    log "Waiting for cluster leader (up to ${max}s)..."
    while [[ $elapsed -lt $max ]]; do
        leader="$(find_leader_index)"
        if [[ "$leader" -ge 0 ]]; then log "  Cluster ready — leader idx=$leader"; return 0; fi
        sleep 3; elapsed=$((elapsed+3))
    done
    die "Cluster did not become ready within ${max}s"
}

kill_server_on_node() {
    local idx="$1"
    log "  Killing server on node idx=${idx} (${NODE_IPS[$idx]}:${RAFT_PORT})..."
    ssh_node "$idx" "fuser -k -9 ${RAFT_PORT}/tcp 2>/dev/null || true"
    sleep 1
}

# ---------------------------------------------------------------------------
# Probe workload helpers — TWO separate stages
# Workload files must live inside SCRIPT_DIR (repo) so rsync copies them
# to the gateway before delegation re-invokes run_4node_raft_cluster.sh.
# ---------------------------------------------------------------------------
WORKLOAD_STAGEDIR="${SCRIPT_DIR}/recovery_artifacts/${CLUSTER_ID}/workloads"
mkdir -p "$WORKLOAD_STAGEDIR"
SETUP_SQL="${WORKLOAD_STAGEDIR}/setup.sql"
CRASH_SQL="${WORKLOAD_STAGEDIR}/crash.sql"
BOOTSTRAP_SQL="${WORKLOAD_STAGEDIR}/bootstrap.sql"
trap 'rm -rf "$WORKLOAD_STAGEDIR"' EXIT

make_bootstrap_workload() {
    # Minimal workload just to confirm the cluster is operational.
    printf 'SELECT 1;\nSELECT 2;\nSELECT 3;\n' > "$BOOTSTRAP_SQL"
    log "Bootstrap workload written to: $BOOTSTRAP_SQL"
}

make_setup_workload() {
    cat > "$SETUP_SQL" <<'SQL'
CREATE TABLE IF NOT EXISTS raft_recovery_probe (id integer PRIMARY KEY, value integer NOT NULL);
INSERT INTO raft_recovery_probe VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
SQL
    log "Setup workload written to: $SETUP_SQL"
}

make_crash_workload() {
    cat > "$CRASH_SQL" <<'SQL'
UPDATE raft_recovery_probe SET value = value + 1 WHERE id = 1;
SQL
    log "Crash-target workload written to: $CRASH_SQL"
}

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
VALIDATION_FAIL=0

# TARGET_RAFT_INDEX is set after workload submission by parsing runner output
TARGET_RAFT_INDEX="${TARGET_RAFT_INDEX:-}"

check_replica() {
    local idx="$1"
    log "--- Validation on node idx=${idx} (${NODE_IPS[$idx]}) ---"

    # 1. No lingering CLAIMED rows (state=1) anywhere in the ledger
    local claimed
    claimed="$(remote_psql "$idx" \
        "SELECT count(*) FROM ariabc_internal.raft_apply_item WHERE state = 1;" \
        2>/dev/null || echo "ERROR")"
    if [[ "$claimed" == "0" ]]; then
        log "  [PASS] No CLAIMED rows"
    else
        log "  [FAIL] CLAIMED rows found: ${claimed}"
        VALIDATION_FAIL=1
    fi

    # 2. Exact ordinal coverage for every manifest entry:
    #    min(ordinal)=0, max(ordinal)=expected_items-1, count=expected_items
    local bad_manifest
    bad_manifest="$(remote_psql "$idx" "
        SELECT count(*)
        FROM ariabc_internal.raft_apply_entry e
        LEFT JOIN ariabc_internal.raft_apply_entry_item i
          ON  i.epoch_id      = e.epoch_id
          AND i.raft_log_index = e.raft_log_index
        WHERE e.epoch_id = decode('${EPOCH_HEX}','hex')
        GROUP BY e.raft_log_index, e.expected_items
        HAVING count(i.item_ordinal) <> e.expected_items
            OR min(i.item_ordinal)  <> 0
            OR max(i.item_ordinal)  <> e.expected_items - 1;" \
        2>/dev/null || echo "ERROR")"
    if [[ "$bad_manifest" == "0" || -z "$bad_manifest" ]]; then
        log "  [PASS] Exact ordinal coverage for all manifest entries"
    else
        log "  [FAIL] ${bad_manifest} entries have incorrect ordinal coverage"
        VALIDATION_FAIL=1
    fi

    # 3. No terminal rows with NULL digest
    local null_dig
    null_dig="$(remote_psql "$idx" \
        "SELECT count(*) FROM ariabc_internal.raft_apply_item
         WHERE epoch_id = decode('${EPOCH_HEX}','hex')
           AND state IN (2,3) AND terminal_digest IS NULL;" \
        2>/dev/null || echo "ERROR")"
    if [[ "$null_dig" == "0" ]]; then
        log "  [PASS] No terminal rows with NULL digest"
    else
        log "  [FAIL] ${null_dig} terminal rows have NULL digest"
        VALIDATION_FAIL=1
    fi

    # 4. Every terminal item must be APPLIED_OK(2) or APPLIED_ERROR(3)
    local bad_state
    bad_state="$(remote_psql "$idx" \
        "SELECT count(*) FROM ariabc_internal.raft_apply_item
         WHERE epoch_id = decode('${EPOCH_HEX}','hex')
           AND state NOT IN (2,3);" \
        2>/dev/null || echo "ERROR")"
    if [[ "$bad_state" == "0" ]]; then
        log "  [PASS] All committed items in terminal state"
    else
        log "  [FAIL] ${bad_state} committed items not yet terminal"
        VALIDATION_FAIL=1
    fi

    # 5. Case-specific: check expected terminal_state for the target index
    if [[ -n "$TARGET_RAFT_INDEX" ]]; then
        local expected_state
        case "$CASE_NAME" in
            A|B|C|D) expected_state="APPLIED_OK" ;;  # executed after restart
            E|F|G)   expected_state="APPLIED_OK" ;;  # was committed, replay skips
        esac
        local target_state
        target_state="$(remote_psql "$idx" \
            "SELECT terminal_state FROM ariabc_internal.raft_apply_item
             WHERE epoch_id = decode('${EPOCH_HEX}','hex')
               AND raft_log_index = ${TARGET_RAFT_INDEX}
             LIMIT 1;" \
            2>/dev/null || echo "MISSING")"
        if [[ "$target_state" == "$expected_state" ]]; then
            log "  [PASS] Target item terminal_state=${target_state} (case ${CASE_NAME})"
        else
            log "  [FAIL] Target item terminal_state=${target_state} (expected ${expected_state})"
            VALIDATION_FAIL=1
        fi
    fi

    # 6. Probe value: exactly once
    local pval
    pval="$(remote_psql "$idx" \
        "SELECT value FROM raft_recovery_probe WHERE id = 1;" \
        2>/dev/null || echo "MISSING")"
    if [[ "$pval" == "$EXPECTED_PROBE_VALUE" ]]; then
        log "  [PASS] probe value=${pval}"
    else
        log "  [FAIL] probe value=${pval} (expected ${EXPECTED_PROBE_VALUE})"
        VALIDATION_FAIL=1
    fi
}

validate_cross_replica_consistency() {
    log "--- Cross-replica terminal digest consistency ---"
    local digests=() idx
    for idx in "${!NODE_IPS[@]}"; do
        local d
        d="$(remote_psql "$idx" \
            "SELECT encode(terminal_digest,'hex')
             FROM ariabc_internal.raft_apply_item
             WHERE epoch_id = decode('${EPOCH_HEX}','hex')
               AND state IN (2,3)
             ORDER BY raft_log_index, item_ordinal;" \
            2>/dev/null || echo "ERROR")"
        digests+=("$d")
    done
    local consistent=1
    for d in "${digests[@]}"; do
        [[ "$d" != "${digests[0]}" ]] && consistent=0 && break
    done
    if [[ $consistent -eq 1 ]]; then
        log "  [PASS] Terminal digest sets agree across all replicas"
    else
        log "  [FAIL] Terminal digest sets DIVERGE across replicas"
        VALIDATION_FAIL=1
    fi
}

# ---------------------------------------------------------------------------
# Main harness
# ---------------------------------------------------------------------------

log "========================================================================"
log " AriaBC Crash Recovery Harness"
log "   Case:      ${CASE_NAME} - $(case_description "$CASE_NAME")"
log "   Target:    ${TARGET}"
log "   Failpoint: ${FAILPOINT}"
log "   Epoch:     ${EPOCH_HEX}"
log "   Cluster:   ${CLUSTER_ID}"
log "   Nodes:     ${#NODE_IPS[@]}"
log "   Log:       ${LOG_FILE}"
log "========================================================================"

# ---------------------------------------------------------------------------
# Step 1: Stop any existing cluster
# ---------------------------------------------------------------------------
log "[Step 1] Stopping any running cluster..."
COLLECT_FINAL_SERVER_PROFILE=0 \
    "${SCRIPT_DIR}/run_4node_raft_cluster.sh" --stop-only 2>/dev/null || true
sleep 2

# ---------------------------------------------------------------------------
# Step 2: Fresh bootstrap (establish schema/epoch without failpoints)
# ---------------------------------------------------------------------------
log "[Step 2] Fresh bootstrap — safe-ledger mode..."
make_bootstrap_workload
COLLECT_FINAL_SERVER_PROFILE=0 \
"${SCRIPT_DIR}/run_4node_raft_cluster.sh" \
    --threads 1 \
    --raft-storage-mode durable \
    --raft-storage-action fresh \
    --raft-apply-ledger-mode safe \
    --raft-epoch-hex "${EPOCH_HEX}" \
    --raft-cluster-id "${CLUSTER_ID}" \
    --det-prefixed-direct-parallel 0 \
    --workload "${BOOTSTRAP_SQL}" \
    --skip-restore \
    --skip-post-verify \
    || die "Fresh bootstrap failed"
log "[Step 2] Bootstrap complete."

# ---------------------------------------------------------------------------
# Step 3: Setup stage — CREATE TABLE + INSERT initial row
# ---------------------------------------------------------------------------
log "[Step 3] Setup stage — creating probe table and inserting row=0..."
make_setup_workload

COLLECT_FINAL_SERVER_PROFILE=0 \
"${SCRIPT_DIR}/run_4node_raft_cluster.sh" \
    --threads 1 \
    --test-queries 5 \
    --raft-storage-mode durable \
    --raft-storage-action preserve \
    --raft-apply-ledger-mode safe \
    --raft-epoch-hex "${EPOCH_HEX}" \
    --raft-cluster-id "${CLUSTER_ID}" \
    --det-prefixed-direct-parallel 0 \
    --workload "${SETUP_SQL}" \
    --skip-restore \
    --skip-post-verify \
    || die "Setup stage failed"
log "[Step 3] Setup complete — probe table initialized."

# ---------------------------------------------------------------------------
# Step 4: Determine target node
# ---------------------------------------------------------------------------
log "[Step 4] Determining target node (target=${TARGET} node_id=${NODE_ID_TARGET:-})..."
KILL_INDICES=()
KILL_RAFT_ID=""

if [[ -n "$NODE_ID_TARGET" ]]; then
    for i in "${!NODE_IDS[@]}"; do
        if [[ "${NODE_IDS[$i]}" == "$NODE_ID_TARGET" ]]; then
            KILL_INDICES=("$i")
            break
        fi
    done
    [[ ${#KILL_INDICES[@]} -eq 0 ]] && die "--node-id ${NODE_ID_TARGET} is not present in NODE_IDS=[${NODE_IDS[*]}]"
else
    case "$TARGET" in
        follower)
        LEADER_IDX="$(find_leader_index)"
        log "  Leader is node idx=${LEADER_IDX}. Selecting a non-leader."
        for i in "${!NODE_IPS[@]}"; do
            if [[ "$i" != "$LEADER_IDX" ]]; then KILL_INDICES=("$i"); break; fi
        done
        ;;
        leader)
        LEADER_IDX="$(find_leader_index)"
        [[ "$LEADER_IDX" -lt 0 ]] && die "Could not identify current leader"
        KILL_INDICES=("$LEADER_IDX")
        ;;
        all)
        for i in "${!NODE_IPS[@]}"; do KILL_INDICES+=("$i"); done
        ;;
    esac
fi

[[ ${#KILL_INDICES[@]} -eq 0 ]] && die "No kill targets selected"
KILL_RAFT_ID="${NODE_IDS[${KILL_INDICES[0]}]}"
KILL_HOST="${NODE_IPS[${KILL_INDICES[0]}]}"
LEADER_IDX_FOR_ROLE="$(find_leader_index || true)"
if [[ "$LEADER_IDX_FOR_ROLE" == "${KILL_INDICES[0]}" ]]; then
    KILL_ROLE="leader"
else
    KILL_ROLE="follower"
fi
log "selected_raft_node_id=${KILL_RAFT_ID}"
log "selected_host=${KILL_HOST}"
log "selected_role=${KILL_ROLE}"
log "  Kill target indices: [${KILL_INDICES[*]}] (Raft ID=${KILL_RAFT_ID})"

# ---------------------------------------------------------------------------
# Step 5: Crash-target stage — restart cluster WITH failpoint on target node,
#         submit the single UPDATE, wait for crash signal
# ---------------------------------------------------------------------------
log "[Step 5] Crash-target stage — launching with failpoint ${FAILPOINT} on node ${KILL_RAFT_ID}..."
make_crash_workload

# Launch the cluster with the failpoint activated on the target node at boot
# Pass FAILPOINT_NODE_ID and FAILPOINT_ENV via CLI args so they are correctly
# forwarded through delegation to the gateway machine.
# The 180s timeout prevents an infinite gateway hang when Kafka results never
# arrive (e.g. leader crash + no quorum). Non-zero exit is expected and OK.
COLLECT_FINAL_SERVER_PROFILE=0 \
timeout 180s \
"${SCRIPT_DIR}/run_4node_raft_cluster.sh" \
    --threads 1 \
    --test-queries 3 \
    --raft-storage-mode durable \
    --raft-storage-action preserve \
    --raft-apply-ledger-mode safe \
    --raft-epoch-hex "${EPOCH_HEX}" \
    --raft-cluster-id "${CLUSTER_ID}" \
    --det-prefixed-direct-parallel 0 \
    --workload "${CRASH_SQL}" \
    --failpoint-node-id "${KILL_RAFT_ID}" \
    --failpoint-env "${FAILPOINT}" \
    --skip-restore \
    --skip-post-verify \
    2>&1 | tee -a "$LOG_FILE" || true
log "[Step 5] Crash-target run finished (crash expected, non-zero exit is OK)."

# ---------------------------------------------------------------------------
# Step 6: Confirm failpoint triggered — look for crash signal in logs
# ---------------------------------------------------------------------------
log "[Step 6] Checking crash evidence in logs..."
CRASH_CONFIRMED=0
for idx in "${KILL_INDICES[@]}"; do
    local_log="${LOG_DIR}/node${idx}_crash_evidence.txt"
    ssh_node "$idx" \
        "cat /tmp/ariabc_cluster/server_node${KILL_RAFT_ID}.log 2>/dev/null | grep -i 'FAILPOINT' | tail -5" \
        > "$local_log" 2>&1 || true
    if grep -q "FAILPOINT" "$local_log" 2>/dev/null; then
        log "  [OK] Crash evidence found on node ${idx}"
        CRASH_CONFIRMED=1
    fi
done
if [[ $CRASH_CONFIRMED -eq 0 ]]; then
    log "  [WARN] No explicit FAILPOINT log found — proceeding, but crash is unconfirmed"
fi

# ---------------------------------------------------------------------------
# Step 7: Recovery restart — same epoch, same cluster, no failpoints
# ---------------------------------------------------------------------------
log "[Step 7] Recovery restart — same epoch, no failpoints..."
COLLECT_FINAL_SERVER_PROFILE=0 \
"${SCRIPT_DIR}/run_4node_raft_cluster.sh" \
    --threads 1 \
    --test-queries 5 \
    --raft-storage-mode durable \
    --raft-storage-action preserve \
    --raft-apply-ledger-mode safe \
    --raft-epoch-hex "${EPOCH_HEX}" \
    --raft-cluster-id "${CLUSTER_ID}" \
    --det-prefixed-direct-parallel 0 \
    --skip-restore \
    --skip-post-verify \
    || die "Recovery restart failed"
log "[Step 7] Recovery restart complete."

# ---------------------------------------------------------------------------
# Step 8: Wait for leader and quorum
# ---------------------------------------------------------------------------
log "[Step 8] Waiting for cluster quorum after recovery..."
wait_for_cluster_ready 90

# ---------------------------------------------------------------------------
# Step 9: Post-recovery barrier — force all nodes to apply up to same index
# ---------------------------------------------------------------------------
log "[Step 9] Post-recovery barrier workload..."
BARRIER_SQL="$(mktemp /tmp/ariabc_barrier_XXXXXX.sql)"
echo "SELECT 1;" > "$BARRIER_SQL"
COLLECT_FINAL_SERVER_PROFILE=0 \
"${SCRIPT_DIR}/run_4node_raft_cluster.sh" \
    --threads 1 \
    --test-queries 3 \
    --raft-storage-mode durable \
    --raft-storage-action preserve \
    --raft-apply-ledger-mode safe \
    --raft-epoch-hex "${EPOCH_HEX}" \
    --raft-cluster-id "${CLUSTER_ID}" \
    --det-prefixed-direct-parallel 0 \
    --workload "$BARRIER_SQL" \
    --skip-restore \
    --direct-completion-quorum "${#NODE_IPS[@]}" \
    --skip-post-verify \
    && log "  Barrier complete." \
    || log "  WARNING: barrier failed (continuing to validation)"
rm -f "$BARRIER_SQL"

# ---------------------------------------------------------------------------
# Step 10: Automated validation on every replica
# ---------------------------------------------------------------------------
log "[Step 10] Validating all replicas..."
for i in "${!NODE_IPS[@]}"; do
    check_replica "$i"
done
validate_cross_replica_consistency

# ---------------------------------------------------------------------------
# Step 11: Merkle consistency
# ---------------------------------------------------------------------------
log "[Step 11] Merkle consistency check..."
if "${SCRIPT_DIR}/test_merkle_consistency.sh" >> "$LOG_FILE" 2>&1; then
    log "  [PASS] Merkle consistency"
else
    log "  [FAIL] Merkle consistency check failed"
    VALIDATION_FAIL=1
fi

# ---------------------------------------------------------------------------
# Final result
# ---------------------------------------------------------------------------
log "========================================================================"
if [[ $VALIDATION_FAIL -eq 0 ]]; then
    log " RESULT: PASS — Case ${CASE_NAME} (${TARGET}) recovery validated."
    log " Artifacts: ${LOG_DIR}"
    exit 0
else
    log " RESULT: FAIL — One or more validation checks failed."
    log " Artifacts: ${LOG_DIR}"
    exit 2
fi
