#!/bin/bash
###############################################################################
# test_normal.sh — Run workload SQL file as plain (non-blockchain) queries
#                  and monitor server.log for errors/warnings.
#
# Usage:  ./scripts/test_normal.sh [workload_file]
#   Default workload: ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt
###############################################################################

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
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
RESULT_FILE="${RESULTS_DIR}/normal_${TIMESTAMP}.log"

mkdir -p "$RESULTS_DIR"

# ── Verify prerequisites ──────────────────────────────────────────────────
if [[ ! -x "$PSQL" ]]; then
    echo "ERROR: psql not found at $PSQL — build the project first."
    exit 1
fi

if [[ ! -f "$WORKLOAD" ]]; then
    echo "ERROR: workload file not found: $WORKLOAD"
    exit 1
fi

# Check server is running
if ! "$PSQL" -p "$PORT" -d "$DB" -U "$USER" -c "SELECT 1;" &>/dev/null; then
    echo "ERROR: PostgreSQL server not reachable on port $PORT."
    exit 1
fi

echo "=== Normal SQL Test ==="
echo "  Workload : $WORKLOAD"
echo "  Results  : $RESULT_FILE"
echo "  Log      : $LOGFILE"

# ── Record log position before test ────────────────────────────────────────
LOG_START_LINE=1
if [[ -f "$LOGFILE" ]]; then
    LOG_START_LINE=$(wc -l < "$LOGFILE")
    ((LOG_START_LINE++))
fi

# ── Count queries ──────────────────────────────────────────────────────────
TOTAL=$(grep -cE '^\s*(SELECT|INSERT|DELETE|UPDATE)' "$WORKLOAD" || true)
echo "  Queries  : $TOTAL"
echo ""

# ── Run workload ───────────────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting normal test..." | tee "$RESULT_FILE"

PASS=0
FAIL=0
COUNTER=0

while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue
    # Skip lines that don't look like SQL
    [[ ! "$line" =~ ^[[:space:]]*(SELECT|INSERT|DELETE|UPDATE) ]] && continue

    ((COUNTER++))

    # Show progress every 500 queries
    if (( COUNTER % 500 == 0 )); then
        echo "  Progress: ${COUNTER}/${TOTAL} queries..."
    fi

    # Execute query
    OUTPUT=$("$PSQL" -p "$PORT" -d "$DB" -U "$USER" -c "$line" 2>&1)
    EXIT_CODE=$?

    if [[ $EXIT_CODE -ne 0 ]]; then
        ((FAIL++))
        echo "[FAIL] Query #${COUNTER}: $line" >> "$RESULT_FILE"
        echo "       Output: $OUTPUT" >> "$RESULT_FILE"
    else
        ((PASS++))
        # Log INSERTs/DELETEs (mutations) for traceability
        if [[ "$line" =~ ^[[:space:]]*(INSERT|DELETE|UPDATE) ]]; then
            echo "[OK]   Query #${COUNTER}: $line" >> "$RESULT_FILE"
        fi
    fi

done < "$WORKLOAD"

echo "" | tee -a "$RESULT_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test complete." | tee -a "$RESULT_FILE"
echo "  Total : $COUNTER" | tee -a "$RESULT_FILE"
echo "  Pass  : $PASS" | tee -a "$RESULT_FILE"
echo "  Fail  : $FAIL" | tee -a "$RESULT_FILE"

# ── Check server log for errors/warnings ───────────────────────────────────
echo "" | tee -a "$RESULT_FILE"
echo "=== Server Log Analysis (new entries during test) ===" | tee -a "$RESULT_FILE"

if [[ -f "$LOGFILE" ]]; then
    LOG_ERRORS=$(tail -n +"$LOG_START_LINE" "$LOGFILE" | grep -iE 'ERROR|WARNING|FATAL|PANIC' || true)
    if [[ -n "$LOG_ERRORS" ]]; then
        echo "*** ERRORS/WARNINGS FOUND IN LOG: ***" | tee -a "$RESULT_FILE"
        echo "$LOG_ERRORS" | tee -a "$RESULT_FILE"
    else
        echo "  No errors or warnings found in server log." | tee -a "$RESULT_FILE"
    fi
else
    echo "  Server log file not found." | tee -a "$RESULT_FILE"
fi

echo ""
echo "Full results saved to: $RESULT_FILE"

# Exit non-zero if any query failed
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
