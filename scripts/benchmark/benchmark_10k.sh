#!/bin/bash
# Benchmark script for AriaBC - 10k INSERT, UPDATE, DELETE operations
# Starting from ycsb_key=13000

set -e

ARIABC_DIR="/work/ARIABC/AriaBC"
PSQL="${ARIABC_DIR}/src/bin/psql/psql"
DB_PORT="5438"
DB_NAME="postgres"
DB_USER="postgres"

START_KEY=13000
END_KEY=22999
TOTAL_OPS=10000

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "AriaBC Benchmark Script - 10k Operations"
echo "========================================="
echo "Start Key: ${START_KEY}"
echo "End Key: ${END_KEY}"
echo "Total Operations: ${TOTAL_OPS}"
echo ""

# Function to generate random 20-character string
generate_random_string() {
    tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 20
}

# ============================================
# PHASE 1: 10k INSERTIONS
# ============================================
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}PHASE 1: 10k INSERTIONS${NC}"
echo -e "${BLUE}=========================================${NC}"

INSERT_START=$(date +%s)

for i in $(seq 0 $((TOTAL_OPS - 1))); do
    key=$((START_KEY + i))
    
    # Generate random strings for all 10 fields
    f1=$(generate_random_string)
    f2=$(generate_random_string)
    f3=$(generate_random_string)
    f4=$(generate_random_string)
    f5=$(generate_random_string)
    f6=$(generate_random_string)
    f7=$(generate_random_string)
    f8=$(generate_random_string)
    f9=$(generate_random_string)
    f10=$(generate_random_string)
    
    # Format SQL
    sql="INSERT INTO usertable (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) VALUES (${key}, '${f1}', '${f2}', '${f3}', '${f4}', '${f5}', '${f6}', '${f7}', '${f8}', '${f9}', '${f10}');"
    
    # Execute SQL
    echo "${sql}" | ${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q > /dev/null 2>&1
    
    # Progress reporting every 1000 operations
    if [ $((i % 1000)) -eq 999 ]; then
        echo -e "${YELLOW}Progress: $((i + 1))/${TOTAL_OPS} insertions completed${NC}"
    fi
done

INSERT_END=$(date +%s)
INSERT_TIME=$((INSERT_END - INSERT_START))
if [ $INSERT_TIME -eq 0 ]; then INSERT_TIME=1; fi
INSERT_THROUGHPUT=$((TOTAL_OPS / INSERT_TIME))

echo -e "${GREEN}✓ Insertions completed${NC}"
echo "  Time: ${INSERT_TIME} seconds"
echo "  Throughput: ${INSERT_THROUGHPUT} ops/sec"
echo ""

# ============================================
# PHASE 2: 10k UPDATES
# ============================================
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}PHASE 2: 10k UPDATES${NC}"
echo -e "${BLUE}=========================================${NC}"

UPDATE_START=$(date +%s)

for i in $(seq 0 $((TOTAL_OPS - 1))); do
    key=$((START_KEY + i))
    
    # Generate new random strings for all 10 fields
    f1=$(generate_random_string)
    f2=$(generate_random_string)
    f3=$(generate_random_string)
    f4=$(generate_random_string)
    f5=$(generate_random_string)
    f6=$(generate_random_string)
    f7=$(generate_random_string)
    f8=$(generate_random_string)
    f9=$(generate_random_string)
    f10=$(generate_random_string)
    
    # Format SQL
    sql="UPDATE usertable SET field1='${f1}', field2='${f2}', field3='${f3}', field4='${f4}', field5='${f5}', field6='${f6}', field7='${f7}', field8='${f8}', field9='${f9}', field10='${f10}' WHERE ycsb_key=${key};"
    
    # Execute SQL
    echo "${sql}" | ${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q > /dev/null 2>&1
    
    # Progress reporting every 1000 operations
    if [ $((i % 1000)) -eq 999 ]; then
        echo -e "${YELLOW}Progress: $((i + 1))/${TOTAL_OPS} updates completed${NC}"
    fi
done

UPDATE_END=$(date +%s)
UPDATE_TIME=$((UPDATE_END - UPDATE_START))
if [ $UPDATE_TIME -eq 0 ]; then UPDATE_TIME=1; fi
UPDATE_THROUGHPUT=$((TOTAL_OPS / UPDATE_TIME))

echo -e "${GREEN}✓ Updates completed${NC}"
echo "  Time: ${UPDATE_TIME} seconds"
echo "  Throughput: ${UPDATE_THROUGHPUT} ops/sec"
echo ""

# ============================================
# PHASE 3: 10k DELETIONS
# ============================================
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}PHASE 3: 10k DELETIONS${NC}"
echo -e "${BLUE}=========================================${NC}"

DELETE_START=$(date +%s)

for i in $(seq 0 $((TOTAL_OPS - 1))); do
    key=$((START_KEY + i))
    
    # Format SQL
    sql="DELETE FROM usertable WHERE ycsb_key=${key};"
    
    # Execute SQL
    echo "${sql}" | ${PSQL} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -q > /dev/null 2>&1
    
    # Progress reporting every 1000 operations
    if [ $((i % 1000)) -eq 999 ]; then
        echo -e "${YELLOW}Progress: $((i + 1))/${TOTAL_OPS} deletions completed${NC}"
    fi
done

DELETE_END=$(date +%s)
DELETE_TIME=$((DELETE_END - DELETE_START))
if [ $DELETE_TIME -eq 0 ]; then DELETE_TIME=1; fi
DELETE_THROUGHPUT=$((TOTAL_OPS / DELETE_TIME))

echo -e "${GREEN}✓ Deletions completed${NC}"
echo "  Time: ${DELETE_TIME} seconds"
echo "  Throughput: ${DELETE_THROUGHPUT} ops/sec"
echo ""

# ============================================
# SUMMARY
# ============================================
echo "========================================="
echo "BENCHMARK SUMMARY"
echo "========================================="
printf "%-15s | %-12s | %-15s\n" "Operation" "Time (sec)" "Throughput (ops/s)"
echo "-----------------+--------------+------------------"
printf "%-15s | %-12s | %-15s\n" "INSERT" "${INSERT_TIME}" "${INSERT_THROUGHPUT}"
printf "%-15s | %-12s | %-15s\n" "UPDATE" "${UPDATE_TIME}" "${UPDATE_THROUGHPUT}"
printf "%-15s | %-12s | %-15s\n" "DELETE" "${DELETE_TIME}" "${DELETE_THROUGHPUT}"
echo "========================================="

TOTAL_TIME=$((INSERT_TIME + UPDATE_TIME + DELETE_TIME))
TOTAL_ALL_OPS=$((TOTAL_OPS * 3))
OVERALL_THROUGHPUT=$((TOTAL_ALL_OPS / TOTAL_TIME))

echo "Total Operations: ${TOTAL_ALL_OPS}"
echo "Total Time: ${TOTAL_TIME} seconds"
echo "Overall Throughput: ${OVERALL_THROUGHPUT} ops/sec"
echo "========================================="
