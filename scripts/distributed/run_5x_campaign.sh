#!/bin/bash
set -euo pipefail

# Define variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export ARIABC_CLUSTER_PASSWORD=clusterinfolab123

for i in {1..5}; do
  echo "=========================================================="
  echo "=== Campaign Run $i of 5"
  echo "=========================================================="
  
  ./run_4node_raft_cluster.sh --threads 8 --bcdb-block-profile 1 --preferred-leader-id 1 > "campaign_run_${i}.log" 2>&1 || {
      echo "Run $i FAILED! Check campaign_run_${i}.log for details."
      exit 1
  }
  
  # Assert conditions
  if ! grep -q "divergence_count : 0" "campaign_run_${i}.log"; then
      echo "Run $i FAILED (divergence_count != 0)"
      exit 1
  fi
  
  if ! grep -q "permanent_failures: 0" "campaign_run_${i}.log"; then
      echo "Run $i FAILED (permanent_failures != 0)"
      exit 1
  fi
  
  if ! grep -q "usertable_small consistency: PASS" "campaign_run_${i}.log"; then
      echo "Run $i FAILED (usertable_small consistency did not PASS)"
      exit 1
  fi

  if grep -q "watchdog_triggered" "campaign_run_${i}.log" || grep -q "\[BCDB_HANG\]" "campaign_run_${i}.log"; then
      echo "Run $i FAILED (Watchdog or hang detected)"
      exit 1
  fi
  
  echo "Run $i PASSED successfully."
  echo ""
done

echo "=========================================================="
echo "=== ALL 5 RUNS PASSED SUCCESSFULLY! ==="
echo "=========================================================="
