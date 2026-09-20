#!/usr/bin/env bash
# ==============================================================================
# Exact Replication Script for AriaBC TPC-C Benchmark Scaling Evaluation
#
# Suite: Standard TPC-C (45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel)
# Geometry: split_threshold=32, merge_threshold=8, fanout=32, partitions=200, fillfactor=80%
# Infrastructure: Client (10.129.27.111) -> Database (10.129.7.57:5438, AMD EPYC 9654)
# ==============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

TIMESTAMP=$(date +%Y%m%dT%H%M%SZ)
WORKERS_DIR="${REPO_ROOT}/Final_Results/TPCC/ranking_tpcc_workers_sweep_${TIMESTAMP}"
WAREHOUSES_DIR="${REPO_ROOT}/Final_Results/TPCC/ranking_tpcc_w5_to_100_w32_${TIMESTAMP}"

echo "================================================================================"
echo "AriaBC TPC-C Benchmark Scaling Replication Pipeline"
echo "Timestamp:      ${TIMESTAMP}"
echo "Repository:     ${REPO_ROOT}"
echo "Target Workers: ${WORKERS_DIR}"
echo "Target Whs:     ${WAREHOUSES_DIR}"
echo "================================================================================"

# ------------------------------------------------------------------------------
# OPTION 1: Full Automated Orchestration (Recommended)
# Executes Campaign 1, Campaign 2, generates publication plots & compiles report
# ------------------------------------------------------------------------------
run_full_orchestrated() {
    python3 scripts/distributed/orchestrate_tpcc_full_evaluation.py \
        --trials 3 \
        --split-threshold 32 \
        --merge-threshold 8 \
        --workers-dir "${WORKERS_DIR}" \
        --warehouses-dir "${WAREHOUSES_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 2: Campaign 1 Only - Worker Concurrency Sweep
# W=100 warehouses, workers w in {8, 16, 24, 32}, 3 modes x 3 trials = 36 runs
# ------------------------------------------------------------------------------
run_campaign1_workers() {
    python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
        --benchmark tpcc \
        --db-host 10.129.7.57 \
        --db-user protectdr \
        --db-port 5438 \
        --server-port 8000 \
        --gateway-host 10.129.27.111 \
        --gateway-user neel \
        --gateway-repo /home/neel/ARIABC/AriaBC \
        --modes pg,bcdb_det,bcdb_merkle \
        --warehouses 100 \
        --tpcc-workers 8,16,24,32 \
        --trials 3 \
        --tpcc-tx-count 20000 \
        --tpcc-seed 42 \
        --tpcc-remote-payment-pct 15.0 \
        --tpcc-remote-new-order-pct 1.0 \
        --tpcc-merkle-fanout 32 \
        --tpcc-merkle-partitions 200 \
        --tpcc-merkle-split-threshold 32 \
        --tpcc-merkle-merge-threshold 8 \
        --db-shared-buffers 32GB \
        --cold-runs \
        --out-dir "${WORKERS_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 3: Campaign 2 Only - Warehouse Partitioning Sweep
# w=32 workers, warehouses W in {5, 10, 20, 30, 50, 75, 100}, 3 modes x 3 trials = 63 runs
# ------------------------------------------------------------------------------
run_campaign2_warehouses() {
    python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
        --benchmark tpcc \
        --db-host 10.129.7.57 \
        --db-user protectdr \
        --db-port 5438 \
        --server-port 8000 \
        --gateway-host 10.129.27.111 \
        --gateway-user neel \
        --gateway-repo /home/neel/ARIABC/AriaBC \
        --modes pg,bcdb_det,bcdb_merkle \
        --warehouses 5,10,20,30,50,75,100 \
        --tpcc-workers 32 \
        --trials 3 \
        --tpcc-tx-count 20000 \
        --tpcc-seed 42 \
        --tpcc-remote-payment-pct 15.0 \
        --tpcc-remote-new-order-pct 1.0 \
        --tpcc-merkle-fanout 32 \
        --tpcc-merkle-partitions 200 \
        --tpcc-merkle-split-threshold 32 \
        --tpcc-merkle-merge-threshold 8 \
        --db-shared-buffers 32GB \
        --cold-runs \
        --out-dir "${WAREHOUSES_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 4: Recompile Plots and Report from Existing Data
# ------------------------------------------------------------------------------
recompile_report_only() {
    python3 scripts/distributed/orchestrate_tpcc_full_evaluation.py \
        --report-only \
        --workers-dir "${REPO_ROOT}/Final_Results/TPCC/ranking_tpcc_workers_sweep_20260920T021900Z" \
        --warehouses-dir "${REPO_ROOT}/Final_Results/TPCC/ranking_tpcc_w5_to_100_w32_20260920T021900Z"
}

# Default execution: print commands and usage
echo "Usage: $0 [full|workers|warehouses|report]"
echo "  full        - Run entire 99-run evaluation suite automatically"
echo "  workers     - Run Campaign 1: Worker concurrency sweep (36 runs)"
echo "  warehouses  - Run Campaign 2: Warehouse partition sweep (63 runs)"
echo "  report      - Recompile plots and report from existing data"

case "${1:-}" in
    full)
        run_full_orchestrated
        ;;
    workers)
        run_campaign1_workers
        ;;
    warehouses)
        run_campaign2_warehouses
        ;;
    report)
        recompile_report_only
        ;;
    *)
        exit 0
        ;;
esac
