#!/bin/bash
set -e

# Configuration
ARIABC_DIR="/work/ARIABC/AriaBC"
PSQL="${ARIABC_DIR}/src/bin/psql/psql"
DB_PORT="5438"
DB_NAME="postgres"
DB_USER="postgres"
START_KEY=13000
TOTAL_OPS=${TOTAL_OPS:-10000}

# Output Files
LOG_FILE="benchmark_run.log"
RESULT_FILE=${RESULT_FILE:-"benchmark_results.txt"}

# Clear previous logs (only if using default result file, acts as a safety)
# If custom result file, we still clear it to start fresh
rm -f ${RESULT_FILE}
# We append to LOG_FILE, so maybe don't clear it every time if running multiple batches? 
# The original script cleared both. I'll keep it simple and clear the result file.
# But I won't clear LOG_FILE here if I want to keep history? 
# Actually, let's just clear the result file.
rm -f ${RESULT_FILE}

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a ${LOG_FILE}
}

log_result() {
    echo "$1" | tee -a ${RESULT_FILE}
}

log "========================================="
log "Optimized AriaBC Benchmark with Verification"
log "========================================="

# Helper to run SQL
run_sql() {
    echo "$1" | ${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -tA
}

# PRE-CLEANUP
log "Cleaning up existing data in range [${START_KEY}, $((START_KEY + TOTAL_OPS)))..."
DELETE_CMD="DELETE FROM usertable WHERE ycsb_key >= ${START_KEY};"
run_sql "${DELETE_CMD}" >> ${LOG_FILE} 2>&1

# ============================================
# PHASE 1: INSERT
# ============================================
log "Generating Insert Workload..."
python3 gen_workload.py insert --start ${START_KEY} --count ${TOTAL_OPS} > workload_insert.sql

log "Running Phase 1: Inserts..."
START=$(date +%s)
${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q -f workload_insert.sql >> ${LOG_FILE} 2>&1
END=$(date +%s)

INSERT_TIME=$((END - START))
if [ $INSERT_TIME -eq 0 ]; then INSERT_TIME=1; fi
INSERT_OPS=$((TOTAL_OPS / INSERT_TIME))
log_result "Insert Time: ${INSERT_TIME}s, Throughput: ${INSERT_OPS} ops/sec"

# VERIFY INSERT
log "Verifying Inserts..."
COUNT=$(run_sql "SELECT count(*) FROM usertable WHERE ycsb_key >= ${START_KEY} AND ycsb_key < $((START_KEY + TOTAL_OPS));")
if [ "${COUNT}" -eq "${TOTAL_OPS}" ]; then
    log_result "VERIFICATION PASSED: Found ${COUNT} records (Expected: ${TOTAL_OPS})"
else
    log_result "VERIFICATION FAILED: Found ${COUNT} records (Expected: ${TOTAL_OPS})"
fi

# ============================================
# PHASE 2: UPDATE
# ============================================
log "Generating Update Workload (Predictable)..."
python3 gen_workload.py update --start ${START_KEY} --count ${TOTAL_OPS} --predictable > workload_update.sql

log "Running Phase 2: Updates..."
START=$(date +%s)
${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q -f workload_update.sql >> ${LOG_FILE} 2>&1
END=$(date +%s)

UPDATE_TIME=$((END - START))
if [ $UPDATE_TIME -eq 0 ]; then UPDATE_TIME=1; fi
UPDATE_OPS=$((TOTAL_OPS / UPDATE_TIME))
log_result "Update Time: ${UPDATE_TIME}s, Throughput: ${UPDATE_OPS} ops/sec"

# VERIFY UPDATE
log "Verifying Updates..."
# Check for the pattern 'UPDATED-{key}' in field1
UPDATED_COUNT=$(run_sql "SELECT count(*) FROM usertable WHERE ycsb_key >= ${START_KEY} AND ycsb_key < $((START_KEY + TOTAL_OPS)) AND field1 = 'UPDATED-' || ycsb_key::text;")
if [ "${UPDATED_COUNT}" -eq "${TOTAL_OPS}" ]; then
    log_result "VERIFICATION PASSED: Found ${UPDATED_COUNT} updated records (Expected: ${TOTAL_OPS})"
else
    log_result "VERIFICATION FAILED: Found ${UPDATED_COUNT} updated records (Expected: ${TOTAL_OPS})"
fi

# ============================================
# PHASE 3: DELETE
# ============================================
log "Generating Delete Workload..."
python3 gen_workload.py delete --start ${START_KEY} --count ${TOTAL_OPS} > workload_delete.sql

log "Running Phase 3: Deletes..."
START=$(date +%s)
${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q -f workload_delete.sql >> ${LOG_FILE} 2>&1
END=$(date +%s)

DELETE_TIME=$((END - START))
if [ $DELETE_TIME -eq 0 ]; then DELETE_TIME=1; fi
DELETE_OPS=$((TOTAL_OPS / DELETE_TIME))
log_result "Delete Time: ${DELETE_TIME}s, Throughput: ${DELETE_OPS} ops/sec"

# VERIFY DELETE
log "Verifying Deletes..."
COUNT=$(run_sql "SELECT count(*) FROM usertable WHERE ycsb_key >= ${START_KEY} AND ycsb_key < $((START_KEY + TOTAL_OPS));")
if [ "${COUNT}" -eq "0" ]; then
    log_result "VERIFICATION PASSED: Found ${COUNT} records (Expected: 0)"
else
    log_result "VERIFICATION FAILED: Found ${COUNT} records (Expected: 0)"
fi

log "========================================="
log "Benchmark Completed. Results saved to ${RESULT_FILE}"
