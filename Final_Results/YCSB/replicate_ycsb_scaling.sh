#!/usr/bin/env bash
# ==============================================================================
# Exact Replication Script for AriaBC YCSB Multi-Mode 4-Skew Evaluation
#
# Suite: 20 Workloads (Families A, B, C, D, F across θ in {0.00, 0.50, 0.99, 1.20})
# Concurrency: Workers w in {1, 4, 8, 16}
# Modes: 4 Modes (cluster, pg, bcdb_det, bcdb_merkle)
# Total configurations: 320; five trials by default (1600 measured runs)
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
TRIALS="${TRIALS:-5}"
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
        --trials "${TRIALS}" \
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
        --trials "${TRIALS}" \
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
        --trials "${TRIALS}" \
        --out-dir "${TARGET_DIR}"
}

# ------------------------------------------------------------------------------
# OPTION 4: Regenerate Report and Plots from summary.csv
# ------------------------------------------------------------------------------
recompile_report_only() {
    local src_dir="${1:-${REPO_ROOT}/Final_Results/YCSB}"
    python3 - "${REPO_ROOT}" "${src_dir}" <<'PY'
import csv, json, sys
from datetime import datetime
from pathlib import Path
repo = Path(sys.argv[1])
sys.path.insert(0, str(repo / 'scripts/distributed'))
from run_all_modes_gateway_sweep import (
    _generate_ycsb_detailed_graphs,
    _generate_ycsb_analysis_markdown,
    _compute_ycsb_median_aggregation,
    _format_wl_label,
)
source = Path(sys.argv[2])
campaign = json.loads((source / 'campaign.json').read_text())
if campaign.get('version') != 3:
    raise SystemExit('Historical campaigns cannot be relabeled with version 3 semantics; see Final_Results/CORRECTIONS.md')
results = list(csv.DictReader((source / 'summary.csv').open()))
if not results:
    raise SystemExit('No observations to report')
for row in results:
    row['server_workers'] = int(row['server_workers'])
workloads = sorted({r['workload'] for r in results})
workers = sorted({r['server_workers'] for r in results})
modes = sorted({r['mode'] for r in results})
trials = int(campaign['trials'])
aggregated = _compute_ycsb_median_aggregation(results)
out_dir = source / ('report_' + datetime.now().strftime('%Y%m%d_%H%M%S_%f'))
out_dir.mkdir()
_generate_ycsb_detailed_graphs(out_dir, results, aggregated, workloads, workers, modes, trials)
_generate_ycsb_analysis_markdown(aggregated, results, out_dir, workloads, workers, modes, trials, _format_wl_label)
print('Generated plots, ANALYSIS.md and MEASUREMENT_QUALIFICATION.md in', out_dir)
PY
}

# Default execution: print commands and usage
echo "Usage: $0 [full|cluster|singlenode|report]"
echo "  full        - Run 320 configurations, ${TRIALS} trials each"
echo "  cluster     - Run cluster mode only (80 configurations, ${TRIALS} trials each)"
echo "  singlenode  - Run pg, bcdb_det, bcdb_merkle (240 configurations, ${TRIALS} trials each)"
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
