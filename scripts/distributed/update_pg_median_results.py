#!/usr/bin/env python3
"""
Update abcdf_4modes_4skews_cold_20260917_192158 with 3-trial median PG numbers
and regenerate all graphs and analysis markdown.
"""
import csv
import json
import shutil
import sys
from pathlib import Path

# Add repo root and distributed scripts to path
repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))
sys.path.insert(0, str(repo_root / "scripts/distributed"))

from run_all_modes_gateway_sweep import (
    _format_wl_label,
    _generate_ycsb_detailed_graphs,
    _generate_ycsb_analysis_markdown,
    YCSB_CSV_FIELDS,
)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import math

target_dir = repo_root / "scripts/bench_full_results/abcdf_4modes_4skews_cold_20260917_192158"
pg_sweep_dir = repo_root / "scripts/bench_full_results/pg_rerun_cold_20260917_211929"

# 1. Back up target summary.csv if not already backed up
backup_file = target_dir / "summary_single_trial_backup.csv"
if not backup_file.exists():
    shutil.copyfile(target_dir / "summary.csv", backup_file)
    print(f"Backed up {target_dir / 'summary.csv'} -> {backup_file}")

# 2. Copy 3-trial raw and median files into target_dir
shutil.copyfile(pg_sweep_dir / "summary.csv", target_dir / "pg_3trials_raw.csv")
shutil.copyfile(pg_sweep_dir / "summary_median.csv", target_dir / "pg_3trials_median.csv")
print(f"Copied pg_3trials_raw.csv and pg_3trials_median.csv to {target_dir}")

# 3. Load PG medians
pg_medians = {}
with open(pg_sweep_dir / "summary_median.csv", "r") as f:
    reader = csv.DictReader(f)
    for r in reader:
        key = (r["workload"], int(r["server_workers"]))
        pg_medians[key] = {
            "median_tps": float(r["median_tps"]),
            "median_wall_time_ms": float(r["median_wall_time_ms"]),
            "trials_count": int(r["trials_count"]),
            "cv_pct": float(r["cv_pct"]),
        }
print(f"Loaded {len(pg_medians)} PG median configurations.")

# 4. Read target_dir/summary.csv and update PG rows
updated_rows = []
with open(backup_file, "r") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for r in reader:
        if r["mode"] == "pg":
            key = (r["workload"], int(r["server_workers"]))
            if key in pg_medians:
                med_info = pg_medians[key]
                r["tps"] = str(med_info["median_tps"])
                r["wall_time_ms"] = str(med_info["median_wall_time_ms"])
        updated_rows.append(r)

with open(target_dir / "summary.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(updated_rows)
print(f"Updated {target_dir / 'summary.csv'} with 3-trial median PG numbers.")

# 5. Prepare data structures for graph and markdown generation
with open(target_dir / "campaign.json", "r") as f:
    campaign = json.load(f)

workloads = list(campaign["workloads"].keys())
workers = campaign["workers"]
modes = campaign["modes"]

# Construct results objects
results = []
for r in updated_rows:
    results.append({
        "mode": r["mode"],
        "workload": r["workload"],
        "server_workers": int(r["server_workers"]),
        "bcdb_workers": int(r.get("bcdb_workers", r["server_workers"])),
        "pool_size": int(r.get("pool_size", r["server_workers"])),
        "total_queries": int(r.get("total_queries", 20000)),
        "wall_time_ms": float(r.get("wall_time_ms", 0.0)),
        "tps": float(r["tps"]),
        "merkle_pass": int(r.get("merkle_pass", 1)),
        "divergence_count": int(r.get("divergence_count", 0)),
        "permanent_failures": int(r.get("permanent_failures", 0)),
        "trial": int(r.get("trial", 1)),
    })

# 6. Load aggregated PG median records
aggregated = []
with open(pg_sweep_dir / "summary_median.csv", "r") as f:
    reader = csv.DictReader(f)
    for r in reader:
        aggregated.append({
            "mode": r["mode"],
            "workload": r["workload"],
            "server_workers": int(r["server_workers"]),
            "trials_count": int(r["trials_count"]),
            "median_tps": float(r["median_tps"]),
            "mean_tps": float(r["mean_tps"]),
            "std_tps": float(r["std_tps"]),
            "cv_pct": float(r["cv_pct"]),
            "min_tps": float(r["min_tps"]),
            "max_tps": float(r["max_tps"]),
            "median_wall_time_ms": float(r["median_wall_time_ms"]),
            "mean_wall_time_ms": float(r["mean_wall_time_ms"]),
            "merkle_pass": int(r.get("merkle_pass", 1)),
            "divergence_count": int(r.get("divergence_count", 0)),
            "permanent_failures": int(r.get("permanent_failures", 0)),
        })
print(f"Loaded {len(aggregated)} aggregated PG 3-trial records.")

# Update series label for PG to explicitly mention 3-Trial Med
import run_all_modes_gateway_sweep
run_all_modes_gateway_sweep._SERIES_CONFIG["pg"]["label"] = "Vanilla PostgreSQL (3-Trial Med)"

# 7. Generate Master Multi-Curve Comparison Plot
n_plots = len(workloads)
n_cols = min(3, n_plots) if n_plots > 1 else 1
n_rows = math.ceil(n_plots / n_cols)
fig, axes = plt.subplots(n_rows, n_cols, figsize=(6.5 * n_cols, 5.0 * n_rows), squeeze=False)

cluster_data = {}
for r in results:
    if r["mode"] == "cluster":
        cluster_data[(Path(r["workload"]).name, int(r["server_workers"]))] = float(r["tps"])

for idx, wl in enumerate(workloads):
    r_idx = idx // n_cols
    c_idx = idx % n_cols
    ax = axes[r_idx][c_idx]
    wl_key = Path(wl).name
    title = _format_wl_label(wl)
    x = workers

    def _get_vals(m):
        vals = []
        wl_base = Path(wl_key).name
        for w in x:
            w_int = int(w)
            match = next((r for r in results if r.get("mode") == m and Path(r.get("workload", "")).name == wl_base and int(r.get("server_workers", 0)) == w_int), None)
            val = float(match["tps"]) if match else 0.0
            vals.append(val)
        return vals

    y_pg = _get_vals("pg")
    y_det = _get_vals("bcdb_det")
    y_merkle = _get_vals("bcdb_merkle")
    y_cl = [cluster_data.get((Path(wl_key).name, int(w)), 0.0) for w in x]

    # PG min/max error shading if available
    wl_base = Path(wl_key).name
    pg_min = [next((a["min_tps"] for a in aggregated if a["mode"] == "pg" and Path(a["workload"]).name == wl_base and a["server_workers"] == int(w)), y_pg[i]) for i, w in enumerate(x)]
    pg_max = [next((a["max_tps"] for a in aggregated if a["mode"] == "pg" and Path(a["workload"]).name == wl_base and a["server_workers"] == int(w)), y_pg[i]) for i, w in enumerate(x)]

    ax.plot(x, y_pg, marker="o", color="#d9534f", label="pg (3-Trial Median)", linewidth=2.2, zorder=4)
    if any(pg_min[i] != pg_max[i] for i in range(len(x))):
        ax.fill_between(x, pg_min, pg_max, color="#d9534f", alpha=0.15, zorder=3)
    ax.plot(x, y_det, marker="s", color="#337ab7", label="bcdb_det", linewidth=2.0, zorder=3)
    ax.plot(x, y_merkle, marker="^", color="#f0ad4e", label="bcdb_merkle", linewidth=2.0, zorder=2)
    ax.plot(x, y_cl, marker="d", color="#5cb85c", label="cluster (4-node)", linewidth=2.0, zorder=1)

    max_y = max(max(y_pg + [0]), max(y_det + [0]), max(y_merkle + [0]), max(y_cl + [0]))
    ax.set_ylim(bottom=0, top=max(max_y * 1.18, 1000.0))
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel("Worker Concurrency", fontsize=9)
    ax.set_ylabel("Throughput (TPS)", fontsize=9)
    ax.set_xticks(x)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(loc="best", fontsize=8)

for idx in range(n_plots, n_rows * n_cols):
    fig.delaxes(axes[idx // n_cols][idx % n_cols])

plt.suptitle(
    "Throughput Scaling Across All Modes (PG with 3-Trial Median & Min-Max Bounds)\n"
    "pg, bcdb_det, bcdb_merkle vs 4-Node Cluster",
    fontsize=13,
    fontweight="bold",
)
plt.tight_layout()

master_plot = target_dir / "final_tps_all_modes_comparison.png"
plt.savefig(master_plot, dpi=180)
graphs_dir = target_dir / "graphs"
graphs_dir.mkdir(parents=True, exist_ok=True)
plt.savefig(graphs_dir / "final_tps_all_modes_comparison.png", dpi=180)
plt.close(fig)
print(f"Saved master comparison plot: {master_plot}")

# 8. Generate detailed per-workload and skew sensitivity plots
_generate_ycsb_detailed_graphs(target_dir, results, aggregated, workloads, workers, modes, 3)

# 9. Generate YCSB detailed markdown
_generate_ycsb_analysis_markdown(aggregated, results, target_dir, workloads, workers, modes, 3, _format_wl_label)

# 10. Update REPORT.md
report_lines = [
    "# YCSB campaign results",
    "",
    f"Shared buffers: **{campaign.get('shared_buffers', '32MB')}**. Recorded cases: **{len(results)}** (PG mode reflects 3-trial medians).",
    "",
    "| Mode | Accepted cases | Minimum TPS | Maximum TPS |",
    "|---|---:|---:|---:|",
]
for m in modes:
    m_tps = [r["tps"] for r in results if r["mode"] == m]
    if m_tps:
        report_lines.append(f"| {m} | {len(m_tps)} | {min(m_tps):.2f} | {max(m_tps):.2f} |")
report_lines.extend([
    "",
    "TPS spans different workloads and worker counts; it is not a matched speedup comparison.",
    "See [summary.csv](summary.csv), [pg_3trials_median.csv](pg_3trials_median.csv), and [campaign.json](campaign.json) for details.",
    "",
])
with open(target_dir / "REPORT.md", "w") as f:
    f.write("\n".join(report_lines))

print("Successfully updated all results, plots, and reports in target directory.")
