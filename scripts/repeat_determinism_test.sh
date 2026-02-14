#!/bin/bash

# Configuration
DATA_FILE="ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt"
PYTHON_SCRIPT="generic-saicopg-traffic-load+logSkip-safedb+pg.py"
RUN_COUNT=5
BCPSQL="/work/ARIABC/install/bin/psql -p 5438 -U postgres -d postgres"

# Navigate to script directory
cd "$(dirname "$0")"

echo "Starting Determinism Test (1 to $RUN_COUNT iterations)"
echo "----------------------------------------------------"

for i in $(seq 1 $RUN_COUNT)
do
    echo "Run $i: Starting..."
    
    # 1. Restart server
    ./start_server.sh > /dev/null 2>&1
    
    # 2. Restore table
    $BCPSQL < restore_usertable.sql > /dev/null 2>&1
    
    # 3. Run workload
    python3 "$PYTHON_SCRIPT" postgres "$DATA_FILE" 1 4 > "output_run_$i.txt" 2>&1
    
    # 4. Collect results
    COUNT=$($BCPSQL -Atc "select count(*) from usertable;")
    ROOT_HASH=$($BCPSQL -Atc "select merkle_root_hash('usertable');")
    VERIFY=$($BCPSQL -Atc "select merkle_verify('usertable');")
    
    echo "Results for Run $i:"
    echo "  Count:       $COUNT"
    echo "  Root Hash:   $ROOT_HASH"
    echo "  Merkle Verify: $VERIFY"
    echo "----------------------------------------------------"
done

echo "Test completed."
