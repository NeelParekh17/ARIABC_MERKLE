#!/usr/bin/env bash
# ==============================================================================
# Exact Replication Script for AriaBC YCSB Multi-Mode 4-Skew Evaluation
#
# Suite: 20 Workloads (Families A, B, C, D, F across θ in {0.00, 0.50, 0.99, 1.20})
# Concurrency: Workers w in {1, 4, 8, 16}
# Modes: 4 Modes (cluster, pg, bcdb_det, bcdb_merkle)
# Total Runs: 320 benchmark cases (80 per mode)
# Hardware Topology:
#   Gateway Client : 10.129.27.111
#   Database Node 1: 10.129.148.247 (Raft ID 1)
#   Database Node 2: 10.129.148.246 (Raft ID 2, U22)
#   Database Node 4: 10.129.148.248 (Raft ID 4)
# ==============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TARGET_DIR="${REPO_ROOT}/Final_Results/YCSB/abcdf_4modes_4skews_cold_${TIMESTAMP}"

echo "================================================================================"
echo "AriaBC YCSB Benchmark Replication Pipeline"
echo "Timestamp:      ${TIMESTAMP}"
echo "Repository:     ${REPO_ROOT}"
echo "Target Run Dir: ${TARGET_DIR}"
echo "================================================================================"

# ------------------------------------------------------------------------------
# OPTION 1: Full Automated Orchestration (All 320 Runs Across All 4 Modes)
# ------------------------------------------------------------------------------
run_full_ycsb() {
    python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
        --benchmark ycsb \
        --workloads abcdf_4skews \
        --workers 1,4,8,16 \
        --modes cluster,pg,bcdb_det,bcdb_merkle \
        --run-cluster \
        --db-shared-buffers 32MB \
        --cold-runs \
        --out-dir "${TARGET_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 2: Cluster Mode Only (80 Runs: 3-Node Raft + Kafka Majority)
# ------------------------------------------------------------------------------
run_cluster_only() {
    python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
        --benchmark ycsb \
        --workloads abcdf_4skews \
        --workers 1,4,8,16 \
        --modes cluster \
        --run-cluster \
        --db-shared-buffers 32MB \
        --cold-runs \
        --out-dir "${TARGET_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 3: Single-Node Modes (240 Runs: pg, bcdb_det, bcdb_merkle)
# ------------------------------------------------------------------------------
run_singlenode_modes() {
    python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
        --benchmark ycsb \
        --workloads abcdf_4skews \
        --workers 1,4,8,16 \
        --modes pg,bcdb_det,bcdb_merkle \
        --db-shared-buffers 32MB \
        --cold-runs \
        --out-dir "${TARGET_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 4: Regenerate Report and Plots from summary.csv
# ------------------------------------------------------------------------------
recompile_report_only() {
    local src_dir="${1:-${REPO_ROOT}/Final_Results/YCSB}"
    python3 -c "
import sys
from pathlib import Path
repo = Path('${REPO_ROOT}')
sys.path.insert(0, str(repo / 'scripts/distributed'))
from run_all_modes_gateway_sweep import (
    _generate_ycsb_detailed_graphs,
    _generate_ycsb_analysis_markdown,
)
out_dir = Path('${src_dir}')
_generate_ycsb_detailed_graphs(out_dir, out_dir / 'summary.csv')
_generate_ycsb_analysis_markdown(out_dir, out_dir / 'summary.csv')
print('Successfully regenerated plots and YCSB_DETAILED_ANALYSIS.md in', out_dir)
"
}

# Default execution: print commands and usage
echo "Usage: $0 [full|cluster|singlenode|report]"
echo "  full        - Run entire 320-run evaluation suite automatically"
echo "  cluster     - Run 4-Node Cluster mode only (80 runs)"
echo "  singlenode  - Run Standalone modes only: pg, bcdb_det, bcdb_merkle (240 runs)"
echo "  report      - Recompile plots and report from existing summary.csv"

case "${1:-}" in
    full)
        run_full_ycsb
        ;;
    cluster)
        run_cluster_only
        ;;
    singlenode)
        run_singlenode_modes
        ;;
    report)
        recompile_report_only "${2:-${REPO_ROOT}/Final_Results/YCSB}"
        ;;
    *)
        exit 0
        ;;
esac
