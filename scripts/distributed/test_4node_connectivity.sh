#!/usr/bin/env bash
# test_4node_connectivity.sh — Fast sanity checks for the 4-node AriaBC cluster.
#
# Does NOT start anything. Assumes run_4node_raft_cluster.sh has already been run.
# Checks:
#   1. SSH reachability to all 4 nodes
#   2. BCDB postgres listening on :5438
#   3. ariabc_pg_server listening on :8000
#   4. Kafka broker on admin123:9092 (if --no-kafka not set)
#   5. Raft leader visible in server logs
#   6. 5-transaction gateway smoke test (direct mode, no Kafka)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.236 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)
ARIABC_CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
CLUSTER_PASSWORD="$ARIABC_CLUSTER_PASSWORD"

KAFKA_HOST="10.129.148.236"
KAFKA_PORT=9092
KAFKA_HOME_REMOTE="/home/neel/Desktop/kafka_2.13-3.7.0"
KAFKA_RESULT_TOPIC="ariabc_results"
RAFT_PORT=9000
DB_PORT=5438
DB_USER=postgres

REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
LOCAL_BIN="$REPO_ROOT/ariabc_pg/build/bin"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"

NO_KAFKA="${NO_KAFKA:-0}"
SKIP_GW_TEST="${SKIP_GW_TEST:-0}"
LOG_DIR="${LOG_DIR:-/tmp/ariabc_connectivity_$(date +%H%M%S)}"
mkdir -p "$LOG_DIR"

PASS=0
FAIL=0
WARN=0

log()  { echo "[$(date +'%H:%M:%S')] $*"; }
ok()   { echo "  [OK]   $*"; (( PASS++ )) || true; }
fail() { echo "  [FAIL] $*"; (( FAIL++ )) || true; }
warn() { echo "  [WARN] $*"; (( WARN++ )) || true; }

node_ssh() {
  local idx="$1"; shift
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 "$user@$ip" "$@"
}

# ---------------------------------------------------------------------------
# Check 1: SSH reachability
# ---------------------------------------------------------------------------
log "=== Check 1: SSH reachability ==="
for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  ip="${NODE_IPS[$idx]}"
  if node_ssh "$idx" "echo ok" >/dev/null 2>&1; then
    ok "SSH $name ($ip)"
  else
    fail "SSH $name ($ip) — unreachable"
  fi
done

# ---------------------------------------------------------------------------
# Check 2: BCDB postgres on :5438
# ---------------------------------------------------------------------------
log "=== Check 2: BCDB postgres on :5438 ==="
for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  ip="${NODE_IPS[$idx]}"
  if node_ssh "$idx" "ss -tlnp 2>/dev/null | grep -q ':$DB_PORT'" 2>/dev/null; then
    # Try to query version to confirm it's BCDB
    ver="$(node_ssh "$idx" "
      INSTALL='$REMOTE_REPO_ROOT/../ariabc_install'
      [[ -x '$REMOTE_REPO_ROOT/../ariabc_install/bin/psql' ]] || INSTALL='$REMOTE_REPO_ROOT/../ariabc_install'
      for d in '$REMOTE_REPO_ROOT/../ariabc_install' '/home/neel/Desktop/ariabc_install'; do
        if [[ -x \"\$d/bin/psql\" ]]; then
          INSTALL=\"\$d\"; break
        fi
      done
      \$INSTALL/bin/psql -h 127.0.0.1 -p $DB_PORT -U $DB_USER -t -c 'SELECT version();' 2>/dev/null | head -1 | tr -s ' ' || echo 'psql query failed'
    " 2>/dev/null | head -1 | tr -d '\r')" || ver="query failed"
    ok "postgres $name ($ip) — $ver"
  else
    fail "postgres $name ($ip) — port :$DB_PORT not listening"
  fi
done

# ---------------------------------------------------------------------------
# Check 3: ariabc_pg_server
# ---------------------------------------------------------------------------
log "=== Check 3: ariabc_pg_server ==="
for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  ip="${NODE_IPS[$idx]}"
  cport="${NODE_CLIENT_PORTS[$idx]}"
  if node_ssh "$idx" "ss -tlnp 2>/dev/null | grep -q ':$cport'" 2>/dev/null; then
    ok "ariabc_pg_server $name ($ip) port :$cport listening"
  else
    fail "ariabc_pg_server $name ($ip) — port :$cport NOT listening"
  fi
done

# ---------------------------------------------------------------------------
# Check 4: Kafka broker
# ---------------------------------------------------------------------------
if [[ "$NO_KAFKA" -eq 0 ]]; then
  log "=== Check 4: Kafka broker on $KAFKA_HOST:$KAFKA_PORT ==="
  KAFKA_BIN="$LOG_DIR/kafka_topics.sh"
  # Try to list topics from local machine using the remote node's kafka tools via SSH
  if node_ssh 0 "
    KAFKA_HOME='$KAFKA_HOME_REMOTE'
    \$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server ${KAFKA_HOST}:${KAFKA_PORT} --list >/dev/null 2>&1 && echo OK || echo FAIL
  " 2>/dev/null | grep -q "OK"; then
    ok "Kafka broker at $KAFKA_HOST:$KAFKA_PORT"
    # Check topic exists
    if node_ssh 0 "
      '$KAFKA_HOME_REMOTE/bin/kafka-topics.sh' --bootstrap-server ${KAFKA_HOST}:${KAFKA_PORT} --list 2>/dev/null | grep -q '$KAFKA_RESULT_TOPIC' && echo OK || echo MISSING
    " 2>/dev/null | grep -q "OK"; then
      ok "Kafka topic '$KAFKA_RESULT_TOPIC' exists"
    else
      warn "Kafka topic '$KAFKA_RESULT_TOPIC' missing — run setup first"
    fi
  else
    fail "Kafka broker at $KAFKA_HOST:$KAFKA_PORT not responding"
  fi
fi

# ---------------------------------------------------------------------------
# Check 5: Raft leader
# ---------------------------------------------------------------------------
log "=== Check 5: Raft leader election ==="
LATEST_LOG_DIR="$(ls -dt "$REPO_ROOT/scripts/bench_full_results"/cluster4_* 2>/dev/null | head -1 || echo "")"
if [[ -n "$LATEST_LOG_DIR" && -d "$LATEST_LOG_DIR" ]]; then
  LEADER_COUNT="$(grep -l -i 'leader\|became leader' "$LATEST_LOG_DIR"/server_node*.log 2>/dev/null | wc -l || echo 0)"
  if [[ "$LEADER_COUNT" -gt 0 ]]; then
    ok "Raft leader signal found in $LEADER_COUNT server log(s) in $LATEST_LOG_DIR"
  else
    warn "No Raft leader signal in logs — may still be electing"
  fi
else
  warn "No cluster log dir found — skipping leader check"
fi

# Also check live via gateway probe
GW_NODES=""
for idx in "${!NODE_IDS[@]}"; do
  [[ -n "$GW_NODES" ]] && GW_NODES+=","
  GW_NODES+="${NODE_IPS[$idx]}:${NODE_CLIENT_PORTS[$idx]}"
done

# ---------------------------------------------------------------------------
# Check 6: 5-transaction gateway smoke test
# ---------------------------------------------------------------------------
if [[ "$SKIP_GW_TEST" -eq 0 ]]; then
  log "=== Check 6: Gateway smoke test (5 transactions, direct mode) ==="
  GW_BIN="$LOCAL_BIN/ariabc_pg_gateway"
  if [[ ! -x "$GW_BIN" ]]; then
    warn "ariabc_pg_gateway not found at $GW_BIN — skipping gateway test"
  else
    SMOKE_WORKLOAD="$LOG_DIR/smoke_workload.sql"
    cat > "$SMOKE_WORKLOAD" <<'WEOF'
SELECT 1;
SELECT 2;
SELECT 3;
SELECT 4;
SELECT 5;
WEOF
    GW_SMOKE_LOG="$LOG_DIR/smoke_gateway.log"
    if timeout 30 "$GW_BIN" \
      --nodes "$GW_NODES" \
      --queryFrom "$SMOKE_WORKLOAD" \
      --dbType 2 \
      --dbConnPoolSize 1 \
      --detWindow 8 \
      --submitMode blocking \
      --clientId "smoke-gw" \
      --numTerminals 1 \
      --waitMajority 0 \
      --completionPath direct \
      --totalNodes ${#NODE_IDS[@]} \
      >"$GW_SMOKE_LOG" 2>&1; then
      ok "Gateway smoke test — 5 transactions submitted successfully"
      SUBMITTED="$(grep -o 'total_submitted=[0-9]*' "$GW_SMOKE_LOG" | tail -1 || echo '')"
      [[ -n "$SUBMITTED" ]] && log "    $SUBMITTED"
    else
      fail "Gateway smoke test — exited with error (see $GW_SMOKE_LOG)"
      tail -10 "$GW_SMOKE_LOG" || true
    fi
  fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log ""
log "=== Connectivity Test Summary ==="
log "  PASS: $PASS"
log "  WARN: $WARN"
log "  FAIL: $FAIL"
log "  Logs: $LOG_DIR"
log ""

if [[ "$FAIL" -gt 0 ]]; then
  log "RESULT: FAIL — $FAIL check(s) failed"
  exit 1
elif [[ "$WARN" -gt 0 ]]; then
  log "RESULT: PASS with warnings ($WARN warning(s))"
  exit 0
else
  log "RESULT: ALL CHECKS PASSED"
  exit 0
fi
