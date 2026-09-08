#!/usr/bin/env bash
# ==============================================================================
# Full 72-Workload YCSB Campaign Across All 4 Modes
# Matrix: 72 workloads x 5 worker counts (1, 2, 4, 8, 16) x 4 modes = 1,440 runs
# Modes: pg, bcdb_det, bcdb_merkle, cluster
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/scripts/bench_full_results/ycsb_all_72_sweep"
LOG_FILE="${OUT_DIR}/sweep.log"

mkdir -p "$OUT_DIR"

echo "=============================================================================="
echo "Starting Full 72-Workload YCSB Sweep (All 4 Modes)"
echo "Repo Root:  $REPO_ROOT"
echo "Output Dir: $OUT_DIR"
echo "Log File:   $LOG_FILE"
echo "Matrix:     72 workloads x [1, 2, 4, 8, 16] workers x [pg, bcdb_det, bcdb_merkle, cluster]"
echo "Total Runs: 1,440 benchmark runs"
echo "Estimated:  ~10-12 hours"
echo "=============================================================================="

# Ensure unbuffered python output for immediate log streaming
export PYTHONUNBUFFERED=1

python3 "$REPO_ROOT/scripts/distributed/run_all_modes_gateway_sweep.py" \
  --workloads all \
  --workers 1,2,4,8,16 \
  --modes pg,bcdb_det,bcdb_merkle,cluster \
  --run-cluster \
  --out-dir "$OUT_DIR" \
  "$@" \
  2>&1 | tee -a "$LOG_FILE"
