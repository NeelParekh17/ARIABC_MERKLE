#!/usr/bin/env python3
"""
benchmark_ycsb_updated_eval.py

Evaluates whether the current updated source code gets the same results or decreases TPS
compared to the baseline campaign in Final_Results/reruns/20260923T153000Z_campaign/YCSB.

Runs 20 representative workloads across:
- All 5 Workload Families: A, B, C, D, F
- All 4 Skew factors: theta = 0.00 (uniform), 0.50 (low), 0.99 (high), 1.20 (extreme)
- All 4 Worker counts: w = 1, 4, 8, 16
"""

import sys
import os
import csv
import json
import time
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts/distributed"))

from cluster_sweep_support import run_cluster_case

BASELINE_CSV = REPO_ROOT / "Final_Results/reruns/20260923T153000Z_campaign/YCSB/summary.csv"

EVAL_CONFIGS_20 = [
    # Workload A (50/50 read/update)
    {"family": "A", "skew": "0.00", "workers": 16, "file": "ycsb_workload_a_skew_0_00_20k.txt"},
    {"family": "A", "skew": "0.50", "workers": 8,  "file": "ycsb_workload_a_skew_0_50_20k.txt"},
    {"family": "A", "skew": "0.99", "workers": 4,  "file": "ycsb_workload_a_skew_0_99_20k.txt"},
    {"family": "A", "skew": "1.20", "workers": 1,  "file": "ycsb_workload_a_skew_1_20_20k.txt"},

    # Workload B (95/5 read/update)
    {"family": "B", "skew": "0.00", "workers": 1,  "file": "ycsb_workload_b_skew_0_00_20k.txt"},
    {"family": "B", "skew": "0.50", "workers": 4,  "file": "ycsb_workload_b_skew_0_50_20k.txt"},
    {"family": "B", "skew": "0.99", "workers": 8,  "file": "ycsb_workload_b_skew_0_99_20k.txt"},
    {"family": "B", "skew": "1.20", "workers": 16, "file": "ycsb_workload_b_skew_1_20_20k.txt"},

    # Workload C (100% read)
    {"family": "C", "skew": "0.00", "workers": 4,  "file": "ycsb_workload_c_skew_0_00_20k.txt"},
    {"family": "C", "skew": "0.50", "workers": 8,  "file": "ycsb_workload_c_skew_0_50_20k.txt"},
    {"family": "C", "skew": "0.99", "workers": 16, "file": "ycsb_workload_c_skew_0_99_20k.txt"},
    {"family": "C", "skew": "1.20", "workers": 1,  "file": "ycsb_workload_c_skew_1_20_20k.txt"},

    # Workload D (read latest / insert)
    {"family": "D", "skew": "0.00", "workers": 8,  "file": "ycsb_workload_d_skew_0_00_20k.txt"},
    {"family": "D", "skew": "0.50", "workers": 16, "file": "ycsb_workload_d_skew_0_50_20k.txt"},
    {"family": "D", "skew": "0.99", "workers": 1,  "file": "ycsb_workload_d_skew_0_99_20k.txt"},
    {"family": "D", "skew": "1.20", "workers": 4,  "file": "ycsb_workload_d_skew_1_20_20k.txt"},

    # Workload F (read-modify-write)
    {"family": "F", "skew": "0.00", "workers": 16, "file": "ycsb_workload_f_skew_0_00_20k.txt"},
    {"family": "F", "skew": "0.50", "workers": 4,  "file": "ycsb_workload_f_skew_0_50_20k.txt"},
    {"family": "F", "skew": "0.99", "workers": 8,  "file": "ycsb_workload_f_skew_0_99_20k.txt"},
    {"family": "F", "skew": "1.20", "workers": 1,  "file": "ycsb_workload_f_skew_1_20_20k.txt"},
]


def load_campaign_baselines(baseline_csv):
    baselines = {}
    if not baseline_csv.exists():
        print(f"Warning: Baseline CSV {baseline_csv} not found.")
        return baselines
    with open(baseline_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("mode") == "cluster":
                key = (row.get("workload"), int(row.get("server_workers")))
                baselines[key] = {
                    "tps": float(row.get("tps", 0.0)),
                    "wall_time_ms": float(row.get("wall_time_ms", 0.0)),
                    "run_id": row.get("run_id", "")
                }
    return baselines


def write_eval_reports(results, out_dir):
    csv_path = out_dir / "evaluation_summary.csv"
    fieldnames = [
        "index", "family", "skew", "workers", "workload",
        "baseline_tps", "measured_tps", "delta_tps_pct",
        "baseline_wall_ms", "measured_wall_ms",
        "merkle_pass", "divergence_count", "permanent_failures",
        "status", "run_id", "duration_s"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # Markdown Report
    report_path = out_dir / "EVALUATION_REPORT.md"
    lines = [
        "# YCSB Cluster Evaluation: Updated Source Code vs. Campaign Baseline",
        "",
        "> **Baseline**: `/work/ARIABC/AriaBC/Final_Results/reruns/20260923T153000Z_campaign/YCSB`",
        "> **Target**: Current Updated Source Code",
        f"> **Completed Runs**: {len(results)}/{len(EVAL_CONFIGS_20)}",
        "",
        "## Summary Comparison Table",
        "",
        "| # | Workload | Family | Skew | Workers | Baseline TPS | Updated TPS | Delta (%) | Baseline Wall (ms) | Updated Wall (ms) | Merkle | Status |",
        "|---|---|:---:|:---:|:---:|---:|---:|---:|---:|---:|:---:|:---:|",
    ]
    for r in results:
        delta_str = f"{r['delta_tps_pct']:+.2f}%" if r['delta_tps_pct'] is not None else "N/A"
        lines.append(
            f"| {r['index']} | `{r['workload']}` | {r['family']} | {r['skew']} | {r['workers']} | "
            f"{r['baseline_tps']:,.1f} | {r['measured_tps']:,.1f} | **{delta_str}** | "
            f"{r['baseline_wall_ms']:,.1f} | {r['measured_wall_ms']:,.1f} | "
            f"{'PASS' if r['merkle_pass'] == 1 else 'FAIL'} | {r['status']} |"
        )

    # Grouped Summary by Family
    lines.extend([
        "",
        "## Workload Family Breakdown",
        "",
        "| Family | Description | Baseline Avg TPS | Updated Avg TPS | Mean Delta (%) |",
        "|:---:|---|---:|---:|---:|",
    ])
    families = ["A", "B", "C", "D", "F"]
    fam_desc = {
        "A": "Update Heavy (50/50 R/W)",
        "B": "Read Heavy (95/5 R/W)",
        "C": "Read Only (100% R)",
        "D": "Read Latest / Inserts",
        "F": "Read-Modify-Write (CTE Updates)",
    }
    for fam in families:
        fam_runs = [r for r in results if r["family"] == fam and r["status"] == "PASS"]
        if fam_runs:
            b_avg = sum(r["baseline_tps"] for r in fam_runs) / len(fam_runs)
            u_avg = sum(r["measured_tps"] for r in fam_runs) / len(fam_runs)
            d_avg = sum(r["delta_tps_pct"] for r in fam_runs) / len(fam_runs)
            lines.append(f"| **{fam}** | {fam_desc.get(fam, '')} | {b_avg:,.1f} | {u_avg:,.1f} | **{d_avg:+.2f}%** |")

    lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Run YCSB cluster evaluation of updated source code.")
    parser.add_argument("--cold-runs", action="store_true", default=True, help="Cold cache runs (default: True)")
    parser.add_argument("--no-cold-runs", dest="cold_runs", action="store_false", help="Warm cache runs")
    parser.add_argument("--count", type=int, default=20, help="Number of workloads to run (10-20, default: 20)")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "scripts/bench_full_results/ycsb_updated_source_eval"),
                        help="Output directory")
    args = parser.parse_args()

    args.db_shared_buffers = "32MB"
    args.gateway_host = "10.129.27.111"
    args.gateway_user = "neel"
    args.gateway_repo = "/home/neel/ARIABC/AriaBC"
    args.tx_sign = os.environ.get("TX_SIGN", "blake3")
    args.recovery_mode = os.environ.get("RECOVERY_MODE", "off")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    baselines = load_campaign_baselines(BASELINE_CSV)
    selected_configs = EVAL_CONFIGS_20[:args.count]

    print("=" * 90)
    print(f"YCSB Cluster Evaluation: Updated Source Code vs. Campaign Baseline")
    print(f"Selected Workloads: {len(selected_configs)} | Cold Runs: {args.cold_runs} | Tx-Sign: {args.tx_sign}")
    print(f"Output Directory: {out_dir}")
    print("=" * 90)

    results = []
    for idx, cfg in enumerate(selected_configs, start=1):
        wl_file = cfg["file"]
        wl_rel_path = f"Final_Results/reruns/20260923T153000Z_campaign/YCSB/workloads_v5/{wl_file}"
        workers = cfg["workers"]
        base_info = baselines.get((wl_file, workers), {"tps": 0.0, "wall_time_ms": 0.0, "run_id": ""})
        base_tps = base_info["tps"]
        base_wall = base_info["wall_time_ms"]

        print(f"\n[{idx}/{len(selected_configs)}] Family {cfg['family']} | Skew {cfg['skew']} | Workers {workers} | {wl_file}")
        print(f"    Baseline TPS: {base_tps:,.1f} (Wall: {base_wall:,.1f} ms, Run: {base_info['run_id']})")

        t0 = time.time()
        restart = (idx == 1 or args.cold_runs)
        try:
            res = run_cluster_case(
                args=args,
                repo=REPO_ROOT,
                out=out_dir,
                workload=wl_rel_path,
                workers=workers,
                run_index=idx - 1,
                restart=restart
            )
            dur = time.time() - t0
            tps = float(res["tps"])
            wall_ms = float(res["wall_time_ms"])
            merkle = int(res["merkle_pass"])
            div = int(res["divergence_count"])
            perm = int(res["permanent_failures"])
            delta_pct = ((tps - base_tps) / base_tps * 100.0) if base_tps > 0 else 0.0
            status = "PASS" if (merkle == 1 and div == 0 and perm == 0) else "FAIL"

            r = {
                "index": idx,
                "family": cfg["family"],
                "skew": cfg["skew"],
                "workers": workers,
                "workload": wl_file,
                "baseline_tps": round(base_tps, 1),
                "measured_tps": round(tps, 1),
                "delta_tps_pct": round(delta_pct, 2),
                "baseline_wall_ms": round(base_wall, 1),
                "measured_wall_ms": round(wall_ms, 1),
                "merkle_pass": merkle,
                "divergence_count": div,
                "permanent_failures": perm,
                "status": status,
                "run_id": res["run_id"],
                "duration_s": round(dur, 1),
            }
            results.append(r)
            print(f"    --> Result: {status} | Measured TPS: {tps:,.1f} (Delta: {delta_pct:+.2f}%) | Merkle: {'PASS' if merkle==1 else 'FAIL'} | Div: {div} | PermFail: {perm} | Time: {dur:.1f}s")
            write_eval_reports(results, out_dir)
        except Exception as e:
            dur = time.time() - t0
            print(f"    --> FAILED with error: {e}")
            r = {
                "index": idx,
                "family": cfg["family"],
                "skew": cfg["skew"],
                "workers": workers,
                "workload": wl_file,
                "baseline_tps": round(base_tps, 1),
                "measured_tps": 0.0,
                "delta_tps_pct": None,
                "baseline_wall_ms": round(base_wall, 1),
                "measured_wall_ms": 0.0,
                "merkle_pass": 0,
                "divergence_count": -1,
                "permanent_failures": -1,
                "status": "FAIL",
                "run_id": "",
                "duration_s": round(dur, 1),
            }
            results.append(r)
            write_eval_reports(results, out_dir)

    print("\n" + "=" * 90)
    print("Evaluation Complete!")
    print(f"Summary CSV: {out_dir / 'evaluation_summary.csv'}")
    print(f"Report: {out_dir / 'EVALUATION_REPORT.md'}")
    print("=" * 90)


if __name__ == "__main__":
    main()
