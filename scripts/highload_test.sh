#!/bin/bash

# High Load Test Script for AriaBC with Verification
# This script performs INSERT, UPDATE, and DELETE operations
# Starting from ycsb_key = 100000 to avoid touching rows 1-12000
# Transaction IDs are sequential decimal numbers

PSQL="/work/ARIABC/AriaBC/src/bin/psql/psql"
PORT=5438
DB="postgres"
USER="postgres"
export LD_LIBRARY_PATH=/work/ARIABC/AriaBC/src/interfaces/libpq:$LD_LIBRARY_PATH

# Configuration
START_KEY=100000       # Starting ycsb_key for test data
NUM_ROWS=100000        # Number of rows to insert/update/delete

# Generate transaction ID (8 decimal digits)
generate_tx_id() {
    printf "%08d" $1
}

echo "========================================"
echo "AriaBC High Load Test Script"
echo "========================================"

# Create temp directory for SQL files
TEMP_DIR="/tmp/ariabc_highload_$$"
mkdir -p "$TEMP_DIR"

# Clean up any previous test data in this range
echo "Cleaning up range >= $START_KEY..."
$PSQL -p $PORT -d $DB -U $USER -c "DELETE FROM usertable WHERE ycsb_key >= $START_KEY;"

# ============================================
# PHASE 1: INSERT
# ============================================
echo ""
echo "[PHASE 1] Generating INSERT statements..."
INSERT_FILE="$TEMP_DIR/inserts.sql"
> "$INSERT_FILE"

for ((i=0; i<NUM_ROWS; i++)); do
    key=$((START_KEY + i))
    tx_id=$(generate_tx_id $i)
    f1="TestField1Row${i}ABCDE"
    f2="TestField2Row${i}ABCDE"
    # ... other fields skipped for brevity in echo but essential for DB ...
    # We use full fields to match schema
    
    echo "s $tx_id INSERT INTO usertable (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) VALUES ($key, '${f1:0:20}', '${f2:0:20}', 'Field3', 'Field4', 'Field5', 'Field6', 'Field7', 'Field8', 'Field9', 'Field10');" >> "$INSERT_FILE"
done

echo "Executing INSERTs..."
START_TIME=$(date +%s.%N)
$PSQL -p $PORT -d $DB -U $USER -f "$INSERT_FILE" > "$TEMP_DIR/insert_output.log" 2>&1
END_TIME=$(date +%s.%N)
echo "INSERT completed in $(echo "$END_TIME - $START_TIME" | bc)s"
grep "INSERT 0 1" "$TEMP_DIR/insert_output.log" | wc -l | xargs echo "Successful INSERTs:"

# VERIFICATION 1
echo "Verifying INSERTs..."
COUNT=$($PSQL -p $PORT -d $DB -U $USER -t -c "SELECT count(*) FROM usertable WHERE ycsb_key >= $START_KEY;")
echo "Row count (expected $NUM_ROWS): $COUNT"
SAMPLE_VAL=$($PSQL -p $PORT -d $DB -U $USER -t -c "SELECT field1 FROM usertable WHERE ycsb_key = $START_KEY;")
echo "Sample Row 0 Field1: $SAMPLE_VAL"
if [[ "$SAMPLE_VAL" == *"TestField1Row0ABCDE"* ]]; then
    echo "VERIFICATION PASSED: Data matches."
else
    echo "VERIFICATION FAILED: Data mismatch."
fi

# ============================================
# PHASE 2: UPDATE
# ============================================
echo ""
echo "[PHASE 2] Generating UPDATE statements..."
UPDATE_FILE="$TEMP_DIR/updates.sql"
> "$UPDATE_FILE"

for ((i=0; i<NUM_ROWS; i++)); do
    key=$((START_KEY + i))
    tx_id=$(generate_tx_id $((NUM_ROWS + i)))
    new_f1="UpdatedF1Row${i}ABCD"
    new_f2="UpdatedF2Row${i}ABCD"
    
    echo "s $tx_id UPDATE usertable SET field1 = '${new_f1:0:20}', field2 = '${new_f2:0:20}' WHERE ycsb_key = $key;" >> "$UPDATE_FILE"
done

echo "Executing UPDATEs..."
START_TIME=$(date +%s.%N)
$PSQL -p $PORT -d $DB -U $USER -f "$UPDATE_FILE" > "$TEMP_DIR/update_output.log" 2>&1
END_TIME=$(date +%s.%N)
echo "UPDATE completed in $(echo "$END_TIME - $START_TIME" | bc)s"
grep "UPDATE 1" "$TEMP_DIR/update_output.log" | wc -l | xargs echo "Successful UPDATEs:"

# VERIFICATION 2
echo "Verifying UPDATEs..."
SAMPLE_VAL=$($PSQL -p $PORT -d $DB -U $USER -t -c "SELECT field1 FROM usertable WHERE ycsb_key = $START_KEY;")
echo "Sample Row 0 Field1: $SAMPLE_VAL"
if [[ "$SAMPLE_VAL" == *"UpdatedF1Row0ABCD"* ]]; then
    echo "VERIFICATION PASSED: Data updated."
else
    echo "VERIFICATION FAILED: Data mismatch (expected 'Updated...')."
fi

# ============================================
# PHASE 3: DELETE
# ============================================
echo ""
echo "[PHASE 3] Generating DELETE statements..."
DELETE_FILE="$TEMP_DIR/deletes.sql"
> "$DELETE_FILE"

for ((i=0; i<NUM_ROWS; i++)); do
    key=$((START_KEY + i))
    tx_id=$(generate_tx_id $((NUM_ROWS * 2 + i)))
    
    echo "s $tx_id DELETE FROM usertable WHERE ycsb_key = $key;" >> "$DELETE_FILE"
done

echo "Executing DELETEs..."
START_TIME=$(date +%s.%N)
$PSQL -p $PORT -d $DB -U $USER -f "$DELETE_FILE" > "$TEMP_DIR/delete_output.log" 2>&1
END_TIME=$(date +%s.%N)
echo "DELETE completed in $(echo "$END_TIME - $START_TIME" | bc)s"
grep "DELETE 1" "$TEMP_DIR/delete_output.log" | wc -l | xargs echo "Successful DELETEs:"

# VERIFICATION 3
echo "Verifying DELETEs..."
COUNT=$($PSQL -p $PORT -d $DB -U $USER -t -c "SELECT count(*) FROM usertable WHERE ycsb_key >= $START_KEY;")
echo "Row count (expected 0): $COUNT"
if [ "$COUNT" -eq 0 ]; then
    echo "VERIFICATION PASSED: Rows deleted."
else
    echo "VERIFICATION FAILED: Rows remain."
fi

# Cleanup
# rm -rf "$TEMP_DIR"
echo ""
echo "Done!"
