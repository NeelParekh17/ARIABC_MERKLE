#!/bin/bash
# Test script for original BCDB version
# Tests INSERT, DELETE, UPDATE on usertable with sequential 8-digit transaction IDs
# Format: s XXXXXXXX <operation>

set -e

# Configuration
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGDATABASE=${PGDATABASE:-postgres}
PGUSER=${PGUSER:-postgres}
PSQL="/work/ARIABC/AriaBC/src/bin/psql/psql -h $PGHOST -p $PGPORT -d $PGDATABASE -U $PGUSER"

echo "========================================="
echo "BCDB Original Version Test Script"
echo "========================================="
echo "Starting tests at: $(date)"
echo ""

# Function to run SQL and display results
run_sql() {
    echo ">>> $1"
    $PSQL -c "$1" 2>&1
    echo ""
}

# Step 1: Initialize BCDB
echo "--- Step 1: Initialize BCDB ---"
run_sql "SELECT bcdb_init(false, 10);"

# Step 2: Reset any previous state
echo "--- Step 2: Reset BCDB state ---"
run_sql "SELECT bcdb_reset();"

# Step 3: Check existing data in usertable
echo "--- Step 3: Check existing data count ---"
run_sql "SELECT COUNT(*) as total_rows FROM usertable;"

# Step 4: Test INSERT operations with sequential 8-digit IDs
# Using ycsb_key starting from 200000 to avoid conflicts with existing data
echo "--- Step 4: Test INSERT operations ---"

# INSERT #1: s 00000000
echo "Transaction: s 00000000 INSERT"
run_sql "SELECT bcdb_block_submit('{\"bid\": 1, \"txs\": [{\"sql\": \"INSERT INTO usertable VALUES (200001, ''ins_f1_1'', ''ins_f2_1'', ''ins_f3_1'', ''ins_f4_1'', ''ins_f5_1'', ''ins_f6_1'', ''ins_f7_1'', ''ins_f8_1'', ''ins_f9_1'', ''ins_f10_1'')\", \"hash\": \"s00000000\"}]}');"

# INSERT #2: s 00000001
echo "Transaction: s 00000001 INSERT"
run_sql "SELECT bcdb_block_submit('{\"bid\": 2, \"txs\": [{\"sql\": \"INSERT INTO usertable VALUES (200002, ''ins_f1_2'', ''ins_f2_2'', ''ins_f3_2'', ''ins_f4_2'', ''ins_f5_2'', ''ins_f6_2'', ''ins_f7_2'', ''ins_f8_2'', ''ins_f9_2'', ''ins_f10_2'')\", \"hash\": \"s00000001\"}]}');"

# INSERT #3: s 00000002
echo "Transaction: s 00000002 INSERT"
run_sql "SELECT bcdb_block_submit('{\"bid\": 3, \"txs\": [{\"sql\": \"INSERT INTO usertable VALUES (200003, ''ins_f1_3'', ''ins_f2_3'', ''ins_f3_3'', ''ins_f4_3'', ''ins_f5_3'', ''ins_f6_3'', ''ins_f7_3'', ''ins_f8_3'', ''ins_f9_3'', ''ins_f10_3'')\", \"hash\": \"s00000002\"}]}');"

# Step 5: Verify INSERTs
echo "--- Step 5: Verify INSERTs ---"
run_sql "SELECT * FROM usertable WHERE ycsb_key IN (200001, 200002, 200003) ORDER BY ycsb_key;"

# Step 6: Test UPDATE operations with sequential 8-digit IDs
echo "--- Step 6: Test UPDATE operations ---"

# UPDATE #1: s 00000003
echo "Transaction: s 00000003 UPDATE"
run_sql "SELECT bcdb_block_submit('{\"bid\": 4, \"txs\": [{\"sql\": \"UPDATE usertable SET field1 = ''updated_f1_1'' WHERE ycsb_key = 200001\", \"hash\": \"s00000003\"}]}');"

# UPDATE #2: s 00000004
echo "Transaction: s 00000004 UPDATE"
run_sql "SELECT bcdb_block_submit('{\"bid\": 5, \"txs\": [{\"sql\": \"UPDATE usertable SET field1 = ''updated_f1_2'', field2 = ''updated_f2_2'' WHERE ycsb_key = 200002\", \"hash\": \"s00000004\"}]}');"

# Step 7: Verify UPDATEs
echo "--- Step 7: Verify UPDATEs ---"
run_sql "SELECT ycsb_key, field1, field2 FROM usertable WHERE ycsb_key IN (200001, 200002) ORDER BY ycsb_key;"

# Step 8: Test DELETE operations with sequential 8-digit IDs
echo "--- Step 8: Test DELETE operations ---"

# DELETE #1: s 00000005
echo "Transaction: s 00000005 DELETE"
run_sql "SELECT bcdb_block_submit('{\"bid\": 6, \"txs\": [{\"sql\": \"DELETE FROM usertable WHERE ycsb_key = 200003\", \"hash\": \"s00000005\"}]}');"

# Step 9: Verify DELETE
echo "--- Step 9: Verify DELETE ---"
run_sql "SELECT * FROM usertable WHERE ycsb_key IN (200001, 200002, 200003) ORDER BY ycsb_key;"

# Step 10: Test Merkle index operations
echo "--- Step 10: Test Merkle verification ---"
run_sql "SELECT merkle_verify('usertable');"

echo "--- Step 11: Get Merkle root hash ---"
run_sql "SELECT merkle_root_hash('usertable');"

echo "--- Step 12: Get Merkle tree stats ---"
run_sql "SELECT merkle_tree_stats('usertable');"

# Step 13: Cleanup test data
echo "--- Step 13: Cleanup test data ---"
# Clean the inserted rows
run_sql "DELETE FROM usertable WHERE ycsb_key IN (200001, 200002);"

# Step 14: Check bcdb_num_committed
echo "--- Step 14: Check number committed ---"
run_sql "SELECT bcdb_num_committed();"

echo "========================================="
echo "Test completed at: $(date)"
echo "========================================="
