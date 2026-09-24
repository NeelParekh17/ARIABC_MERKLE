#!/usr/bin/env python3
"""
Orchestrate the full TPC-C scaling evaluation on the 96-core AMD EPYC server (10.129.7.57).
Executes:
1. Campaign 1 (Worker Concurrency Axis): W=100, w in {8, 16, 24, 32}, 3 trials (36 runs)
2. Campaign 2 (Warehouse Partitioning Axis): w=32, W in {5, 10, 20, 30, 50, 75, 100}, 3 trials (63 runs)
3. Generates publication plots: tpcc_workers_scaling.png, tpcc_warehouses_scaling.png
4. Compiles TPCC_BENCHMARK_SCALING_DETAILED_ANALYSIS.md with multi-trial stats, cross-campaign validation, and formal invariant audit.
"""

import argparse
import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCH_RESULTS_DIR = REPO_ROOT / "scripts/bench_full_results"


def run_command(cmd, desc):
    print(f"\n{'='*80}\n[START] {desc}\nCMD: {' '.join(cmd)}\n{'='*80}\n", flush=True)
    res = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if res.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {res.returncode}: {desc}")
    print(f"\n[DONE] {desc} (exit code 0)\n", flush=True)


def load_campaign_data(campaign_dir: Path):
    median_file = campaign_dir / "summary_median.csv"
    summary_file = campaign_dir / "summary.csv"
    data = {}

    if median_file.exists():
        with open(median_file, "r") as f:
            reader = csv.DictReader(f)
            for r in reader:
                mode = r["mode"]
                wh = int(r["warehouses"])
                w = int(r["server_workers"])
                med_tps = float(r["median_tps"])
                mean_tps = float(r["mean_tps"])
                std_tps = float(r["std_tps"])
                min_tps = float(r["min_tps"])
                max_tps = float(r["max_tps"])
                wall_ms = float(r["median_wall_time_ms"])
                data[(mode, wh, w)] = {
                    "median_tps": med_tps,
                    "mean_tps": mean_tps,
                    "std_tps": std_tps,
                    "min_tps": min_tps,
                    "max_tps": max_tps,
                    "cv_pct": (std_tps / mean_tps * 100.0) if mean_tps > 0 else 0.0,
                    "wall_ms": wall_ms,
                    "merkle_pass": int(r.get("merkle_pass", 1)),
                    "divergence": int(r.get("divergence_count", 0)),
                    "failures": int(r.get("permanent_failures", 0)),
                    "trials": int(r.get("trials_count", 1)),
                }
    elif summary_file.exists():
        with open(summary_file, "r") as f:
            reader = csv.DictReader(f)
            for r in reader:
                mode = r["mode"]
                wh = int(r["warehouses"])
                w = int(r["server_workers"])
                tps = float(r["tps"])
                wall_ms = float(r["wall_time_ms"])
                data[(mode, wh, w)] = {
                    "median_tps": tps,
                    "mean_tps": tps,
                    "std_tps": 0.0,
                    "min_tps": tps,
                    "max_tps": tps,
                    "cv_pct": 0.0,
                    "wall_ms": wall_ms,
                    "merkle_pass": int(r.get("merkle_pass", 1)),
                    "divergence": int(r.get("divergence_count", 0)),
                    "failures": int(r.get("permanent_failures", 0)),
                    "trials": 1,
                }
    return data


def audit_summary_runs(campaign_dir: Path):
    summary_file = campaign_dir / "summary.csv"
    if not summary_file.exists():
        return 0, 0, 0, 0, 0
    with open(summary_file, "r") as f:
        reader = list(csv.DictReader(f))
    total_runs = len(reader)
    div_sum = sum(int(r.get("divergence_count", 0)) for r in reader)
    fail_sum = sum(int(r.get("permanent_failures", 0)) for r in reader)
    merkle_runs = [r for r in reader if r.get("mode") == "bcdb_merkle"]
    merkle_pass_count = sum(int(r.get("merkle_pass", 0)) for r in merkle_runs)
    return total_runs, len(merkle_runs), merkle_pass_count, div_sum, fail_sum


def main():
    parser = argparse.ArgumentParser(description="Full TPC-C Scaling Evaluation Orchestrator")
    parser.add_argument("--timestamp", default=None, help="Custom timestamp for output directories")
    parser.add_argument("--trials", type=int, default=5, help="Number of trials per configuration (default: 5)")
    parser.add_argument("--split-threshold", type=int, default=32, help="Merkle split threshold (default: 32)")
    parser.add_argument("--merge-threshold", type=int, default=8, help="Merkle merge threshold (default: 8)")
    parser.add_argument("--skip-campaign1", action="store_true", help="Skip Campaign 1 (workers sweep)")
    parser.add_argument("--skip-campaign2", action="store_true", help="Skip Campaign 2 (warehouses sweep)")
    parser.add_argument("--report-only", action="store_true", help="Only compile report from existing directories")
    parser.add_argument("--workers-dir", default=None, help="Explicit workers directory for report-only")
    parser.add_argument("--warehouses-dir", default=None, help="Explicit warehouses directory for report-only")
    args = parser.parse_args()

    timestamp = args.timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    workers_dir = Path(args.workers_dir) if args.workers_dir else (BENCH_RESULTS_DIR / f"ranking_tpcc_workers_sweep_{timestamp}")
    warehouses_dir = Path(args.warehouses_dir) if args.warehouses_dir else (BENCH_RESULTS_DIR / f"ranking_tpcc_w5_to_100_w32_{timestamp}")

    print(f"Starting TPC-C Full Evaluation Pipeline", flush=True)
    print(f"Timestamp:          {timestamp}", flush=True)
    print(f"Trials per Config:  {args.trials}", flush=True)
    print(f"Split Threshold:    {args.split_threshold}", flush=True)
    print(f"Merge Threshold:    {args.merge_threshold}", flush=True)
    print(f"Workers Dir:        {workers_dir}", flush=True)
    print(f"Warehouses Dir:     {warehouses_dir}", flush=True)

    if not args.report_only:
        # 1. Run Campaign 1: Worker Concurrency Axis
        if not args.skip_campaign1:
            cmd_workers = [
                sys.executable, "-u", "scripts/distributed/run_all_modes_gateway_sweep.py",
                "--benchmark", "tpcc",
                "--db-host", "10.129.7.57",
                "--db-user", "protectdr",
                "--db-port", "5438",
                "--server-port", "8000",
                "--gateway-host", "10.129.27.111",
                "--gateway-user", "neel",
                "--gateway-repo", "/home/neel/ARIABC/AriaBC",
                "--modes", "pg,bcdb_det,bcdb_merkle",
                "--warehouses", "100",
                "--tpcc-workers", "8,16,24,32",
                "--trials", str(args.trials),
                "--tpcc-tx-count", "20000",
                "--tpcc-seed", "42",
                "--tpcc-remote-payment-pct", "15.0",
                "--tpcc-remote-new-order-pct", "1.0",
                "--tpcc-merkle-fanout", "32",
                "--tpcc-merkle-partitions", "200",
                "--tpcc-merkle-split-threshold", str(args.split_threshold),
                "--tpcc-merkle-merge-threshold", str(args.merge_threshold),
                "--db-shared-buffers", "32GB",
                "--cold-runs",
                "--out-dir", str(workers_dir),
            ]
            run_command(cmd_workers, f"Campaign 1: Worker Concurrency Axis (W=100, w in {{8, 16, 24, 32}}, {args.trials} trials)")

        # 2. Run Campaign 2: Warehouse Partitioning Axis
        if not args.skip_campaign2:
            cmd_warehouses = [
                sys.executable, "-u", "scripts/distributed/run_all_modes_gateway_sweep.py",
                "--benchmark", "tpcc",
                "--db-host", "10.129.7.57",
                "--db-user", "protectdr",
                "--db-port", "5438",
                "--server-port", "8000",
                "--gateway-host", "10.129.27.111",
                "--gateway-user", "neel",
                "--gateway-repo", "/home/neel/ARIABC/AriaBC",
                "--modes", "pg,bcdb_det,bcdb_merkle",
                "--warehouses", "5,10,20,30,50,75,100",
                "--tpcc-workers", "32",
                "--trials", str(args.trials),
                "--tpcc-tx-count", "20000",
                "--tpcc-seed", "42",
                "--tpcc-remote-payment-pct", "15.0",
                "--tpcc-remote-new-order-pct", "1.0",
                "--tpcc-merkle-fanout", "32",
                "--tpcc-merkle-partitions", "200",
                "--tpcc-merkle-split-threshold", str(args.split_threshold),
                "--tpcc-merkle-merge-threshold", str(args.merge_threshold),
                "--db-shared-buffers", "32GB",
                "--cold-runs",
                "--out-dir", str(warehouses_dir),
            ]
            run_command(cmd_warehouses, f"Campaign 2: Warehouse Partitioning Axis (w=32, W in {{5, 10, 20, 30, 50, 75, 100}}, {args.trials} trials)")

    # 3. Copy and link master plots
    workers_plot_src = workers_dir / "tpcc_tps_vs_workers.png"
    warehouses_plot_src = warehouses_dir / "tpcc_tps_vs_warehouses.png"

    workers_plot_dst = BENCH_RESULTS_DIR / "tpcc_workers_scaling.png"
    warehouses_plot_dst = BENCH_RESULTS_DIR / "tpcc_warehouses_scaling.png"

    if workers_plot_src.exists():
        shutil.copyfile(workers_plot_src, workers_plot_dst)
        print(f"Copied {workers_plot_src} -> {workers_plot_dst}", flush=True)

    if warehouses_plot_src.exists():
        shutil.copyfile(warehouses_plot_src, warehouses_plot_dst)
        print(f"Copied {warehouses_plot_src} -> {warehouses_plot_dst}", flush=True)

    # 4. Compile analysis markdown
    compile_report(workers_dir, warehouses_dir, trials=args.trials, split_thresh=args.split_threshold, merge_thresh=args.merge_threshold)


def compile_report(workers_dir: Path, warehouses_dir: Path, trials: int = 3, split_thresh: int = 32, merge_thresh: int = 8):
    from benchmark_contract import write_variability_report
    lines = ["# TPC-C scaling measurements", "",
             "Reported PG is the PostgreSQL execution path in the custom AriaBC build; consult per-attempt version and hashes.",
             "Measured tables must be logged and synchronous_commit/fsync/full_page_writes enabled.",
             "Historical campaigns without this evidence remain unqualified and are not upgraded by this report.",
             "Local Merkle verification does not establish distributed agreement or serializability.",
             "Throughput ordering is not a correctness invariant. Retrying transactions can still finish without permanent failures.", ""]
    for label, directory in [("Worker sweep", workers_dir), ("Warehouse sweep", warehouses_dir)]:
        with (directory / "summary.csv").open() as f:
            rows = list(csv.DictReader(f))
        write_variability_report(directory, rows)
        lines += [f"## {label}", "", f"Campaign: {directory}", "",
                  (directory / "MEASUREMENT_QUALIFICATION.md").read_text(), ""]
    report_path = workers_dir.parent / ("TPCC_ANALYSIS_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".md")
    report_path.write_text("\n".join(lines))
    print(f"Generated {report_path}", flush=True)


if __name__ == "__main__":
    main()
