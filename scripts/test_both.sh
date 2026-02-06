#!/bin/bash
###############################################################################
# test_both.sh — Run the workload two ways and compare results:
#   1. Normal SQL (plain queries)
#   2. Blockchain mode (s XXXXXXXX prefix with incrementing sequence)
#
# Restores the database between runs so both tests start from the same state.
# Monitors server.log for errors/warnings after each run.
#
# Usage:  ./scripts/test_both.sh [workload_file]
###############################################################################

set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
PSQL="${BASEDIR}/src/bin/psql/psql"
export LD_LIBRARY_PATH="${BASEDIR}/src/interfaces/libpq"
PORT=5438
DB=postgres
USER=postgres
LOGFILE="${BASEDIR}/server.log"
WORKLOAD="${1:-${BASEDIR}/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt}"
RESULTS_DIR="${BASEDIR}/test_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$RESULTS_DIR"

psql_cmd() {
    "$PSQL" -p "$PORT" -d "$DB" -U "$USER" "$@"
}

restore_db() {
    echo ""
    echo "── Restoring database to clean state ──"
    if [[ -f "${BASEDIR}/scripts/restore.sql" ]]; then
        psql_cmd -f "${BASEDIR}/scripts/restore.sql" 2>&1 | tail -1
    else
        echo "  WARNING: restore.sql not found, skipping restore."
        echo "  Tests may not start from the same state!"
    fi
    echo ""
}

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          AriaBC Workload Test — Normal & Blockchain          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Workload : $WORKLOAD"
echo "  Timestamp: $TIMESTAMP"
echo ""

# ── Check server ───────────────────────────────────────────────────────────
if ! psql_cmd -c "SELECT 1;" &>/dev/null; then
    echo "ERROR: PostgreSQL server not reachable on port $PORT."
    echo "  Start the server first:  ./scripts/start_server.sh"
    exit 1
fi
echo "  Server   : running on port $PORT ✓"
echo ""

# ══════════════════════════════════════════════════════════════════════════
# Phase 1: Normal SQL test
# ══════════════════════════════════════════════════════════════════════════
restore_db

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " PHASE 1: Normal SQL (plain queries)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

NORMAL_START=$(date +%s)
bash "${BASEDIR}/scripts/test_normal.sh" "$WORKLOAD" 2>&1 | tee "${RESULTS_DIR}/normal_console_${TIMESTAMP}.log"
NORMAL_RC=${PIPESTATUS[0]}
NORMAL_END=$(date +%s)
NORMAL_DURATION=$((NORMAL_END - NORMAL_START))

echo ""
echo "  Normal test duration: ${NORMAL_DURATION}s (exit code: ${NORMAL_RC})"
echo ""

# ══════════════════════════════════════════════════════════════════════════
# Phase 2: Blockchain mode test
# ══════════════════════════════════════════════════════════════════════════
restore_db

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " PHASE 2: Blockchain mode (s XXXXXXXX prefix)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

BC_START=$(date +%s)
bash "${BASEDIR}/scripts/test_blockchain.sh" "$WORKLOAD" 0 2>&1 | tee "${RESULTS_DIR}/blockchain_console_${TIMESTAMP}.log"
BC_RC=${PIPESTATUS[0]}
BC_END=$(date +%s)
BC_DURATION=$((BC_END - BC_START))

echo ""
echo "  Blockchain test duration: ${BC_DURATION}s (exit code: ${BC_RC})"
echo ""

# ══════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════
SUMMARY="${RESULTS_DIR}/summary_${TIMESTAMP}.txt"

cat <<EOF | tee "$SUMMARY"

╔══════════════════════════════════════════════════════════════╗
║                     TEST SUMMARY                             ║
╚══════════════════════════════════════════════════════════════╝

  Workload  : $(basename "$WORKLOAD")
  Timestamp : $TIMESTAMP

  ┌─────────────────┬──────────┬──────────────┐
  │ Mode            │ Duration │ Exit Code    │
  ├─────────────────┼──────────┼──────────────┤
  │ Normal SQL      │ ${NORMAL_DURATION}s       │ ${NORMAL_RC}            │
  │ Blockchain (s)  │ ${BC_DURATION}s       │ ${BC_RC}            │
  └─────────────────┴──────────┴──────────────┘

  Result files in: $RESULTS_DIR/
EOF

# Final log check
echo "" | tee -a "$SUMMARY"
echo "=== Full Server Log Errors/Warnings ===" | tee -a "$SUMMARY"
if [[ -f "$LOGFILE" ]]; then
    LOG_ISSUES=$(grep -cE 'ERROR|WARNING|FATAL|PANIC' "$LOGFILE" 2>/dev/null || true)
    echo "  Total error/warning lines in server.log: $LOG_ISSUES" | tee -a "$SUMMARY"
else
    echo "  Server log not found." | tee -a "$SUMMARY"
fi

echo ""
echo "Done. See $RESULTS_DIR/ for detailed logs."
