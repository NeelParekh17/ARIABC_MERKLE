#!/usr/bin/env python3
"""
Compute Median, Mean, and Standard Deviation across multiple TPC-C benchmark runs
and generate publication-quality comparison tables and plots.

Usage:
  python3 scripts/distributed/compute_tpcc_median.py \
    --inputs scripts/bench_full_results/ranking_tpcc_sweep_20260906T135706Z/summary.csv \
             scripts/bench_full_results/ranking_tpcc_sweep_20260906T151813Z/summary.csv \
    --out-dir scripts/bench_full_results/ranking_tpcc_median_aggregated
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path
import statistics
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Aggregate multiple TPC-C runs and compute median metrics.")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="One or more summary.csv files or directories containing summary.csv",
    )
    parser.add_argument(
        "--out-dir",
        default="scripts/bench_full_results/tpcc_median_results",
        help="Output directory for aggregated CSV and plot",
    )
    return parser.parse_args()


def load_all_records(input_paths):
    records = []
    for p_str in input_paths:
        p = Path(p_str)
        if p.is_dir():
            p = p / "summary.csv"
        if not p.exists():
            print(f"WARNING: Path {p} does not exist, skipping.")
            continue

        with open(p, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for r in reader:
                if not r.get("mode") or not r.get("warehouses"):
                    continue
                try:
                    records.append({
                        "benchmark": r.get("benchmark", "tpcc"),
                        "mode": r["mode"].strip(),
                        "warehouses": int(r["warehouses"]),
                        "server_workers": int(r.get("server_workers", 8)),
                        "bcdb_workers": int(r.get("bcdb_workers", 8)),
                        "tps": float(r["tps"]),
                        "wall_time_ms": float(r["wall_time_ms"]),
                        "merkle_pass": int(r.get("merkle_pass", 1)),
                        "divergence_count": int(r.get("divergence_count", 0)),
                        "permanent_failures": int(r.get("permanent_failures", 0)),
                        "source_file": str(p),
                    })
                    count += 1
                except (ValueError, TypeError) as e:
                    print(f"Skipping malformed row in {p}: {e}")
            print(f"Loaded {count} records from {p}")
    return records


def aggregate_by_config(records):
    # Group by (mode, warehouses, server_workers)
    grouped = defaultdict(list)
    for r in records:
        key = (r["mode"], r["warehouses"], r["server_workers"])
        grouped[key].append(r)

    aggregated = []
    for key, items in sorted(grouped.items()):
        mode, wh, workers = key
        tps_list = [it["tps"] for it in items]
        wall_list = [it["wall_time_ms"] for it in items]

        med_tps = statistics.median(tps_list)
        mean_tps = statistics.mean(tps_list)
        std_tps = statistics.stdev(tps_list) if len(tps_list) > 1 else 0.0
        min_tps = min(tps_list)
        max_tps = max(tps_list)

        med_wall = statistics.median(wall_list)
        mean_wall = statistics.mean(wall_list)

        merkle_pass = 1 if all(it["merkle_pass"] == 1 for it in items) else 0
        div_count = sum(it["divergence_count"] for it in items)
        fail_count = sum(it["permanent_failures"] for it in items)

        aggregated.append({
            "mode": mode,
            "warehouses": wh,
            "server_workers": workers,
            "trials_count": len(items),
            "median_tps": med_tps,
            "mean_tps": mean_tps,
            "std_tps": std_tps,
            "min_tps": min_tps,
            "max_tps": max_tps,
            "median_wall_time_ms": med_wall,
            "mean_wall_time_ms": mean_wall,
            "merkle_pass": merkle_pass,
            "divergence_count": div_count,
            "permanent_failures": fail_count,
        })
    return aggregated


def print_comparison_table(aggregated):
    modes = sorted(list(set(a["mode"] for a in aggregated)))
    warehouses = sorted(list(set(a["warehouses"] for a in aggregated)))

    print("\n" + "=" * 110)
    print("TPC-C MEDIAN COMPARISON TABLE (Aggregated Across Trials)")
    print("Fixed Workers: 8 | Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel")
    print("=" * 110)

    header = ["Warehouses"]
    for m in modes:
        header.append(f"{m.upper()} Median TPS (±std)")
    if "bcdb_merkle" in modes and "pg" in modes:
        header.append("Merkle vs PG (%)")
    print(" | ".join(f"{h:<26}" if i > 0 else f"{h:<12}" for i, h in enumerate(header)))
    print("-" * 110)

    for wh in warehouses:
        row = [f"{wh:<12}"]
        tps_map = {}
        for m in modes:
            item = next((a for a in aggregated if a["mode"] == m and a["warehouses"] == wh), None)
            if item:
                val_str = f"{item['median_tps']:.1f} (±{item['std_tps']:.1f})"
                tps_map[m] = item["median_tps"]
            else:
                val_str = "N/A"
            row.append(f"{val_str:<26}")

        if "bcdb_merkle" in modes and "pg" in modes:
            pg_val = tps_map.get("pg", 0.0)
            m_val = tps_map.get("bcdb_merkle", 0.0)
            if pg_val > 0:
                delta = (m_val - pg_val) / pg_val * 100.0
                row.append(f"{delta:>+7.2f}%")
            else:
                row.append(f"{'N/A':>8}")

        print(" | ".join(row))
    print("=" * 110 + "\n")


def plot_median_results(aggregated, out_dir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        modes = sorted(list(set(a["mode"] for a in aggregated)))
        warehouses = sorted(list(set(a["warehouses"] for a in aggregated)))

        fig, ax = plt.subplots(1, 1, figsize=(12, 7))

        mode_styles = {
            "pg": ("^:", "#6c757d", 2.2, 8, "Vanilla PostgreSQL (pg)"),
            "bcdb_det": ("d-.", "#28a745", 2.4, 8, "BCDB Deterministic (bcdb_det)"),
            "bcdb_merkle": ("s-", "#0056b3", 2.6, 9, "BCDB Merkle (bcdb_merkle)"),
        }

        for m in modes:
            style = mode_styles.get(m, ("o-", "#333333", 2.0, 7, m))
            marker_line, color, lw, ms, label = style

            x_vals = []
            y_meds = []
            y_mins = []
            y_maxs = []

            for wh in warehouses:
                item = next((a for a in aggregated if a["mode"] == m and a["warehouses"] == wh), None)
                if item:
                    x_vals.append(wh)
                    y_meds.append(item["median_tps"])
                    y_mins.append(item["min_tps"])
                    y_maxs.append(item["max_tps"])

            if x_vals:
                ax.plot(x_vals, y_meds, marker_line, color=color, linewidth=lw, markersize=ms, label=label)
                # Shaded confidence area across trials
                if any(y_mins[i] != y_maxs[i] for i in range(len(y_mins))):
                    ax.fill_between(x_vals, y_mins, y_maxs, color=color, alpha=0.15)

        # Annotate Merkle vs PG percentage
        if "bcdb_merkle" in modes and "pg" in modes:
            for wh in warehouses:
                pg_item = next((a for a in aggregated if a["mode"] == "pg" and a["warehouses"] == wh), None)
                m_item = next((a for a in aggregated if a["mode"] == "bcdb_merkle" and a["warehouses"] == wh), None)
                if pg_item and m_item and pg_item["median_tps"] > 0:
                    delta = (m_item["median_tps"] - pg_item["median_tps"]) / pg_item["median_tps"] * 100.0
                    ax.annotate(
                        f"{delta:+.1f}%",
                        xy=(wh, m_item["median_tps"]),
                        xytext=(0, 12),
                        textcoords="offset points",
                        ha="center",
                        fontsize=9,
                        fontweight="bold",
                        color="#0056b3",
                    )

        all_y = [a["max_tps"] for a in aggregated]
        max_y = max(all_y) if all_y else 1000.0
        ax.set_ylim(bottom=0, top=max_y * 1.22)
        ax.set_xlabel("Warehouse Count (Scale Factor)", fontsize=13, fontweight="bold")
        ax.set_ylabel("Median Throughput (TPS)", fontsize=13, fontweight="bold")
        ax.set_xticks(warehouses)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper left", fontsize=11, framealpha=0.9)

        plt.suptitle(
            "TPC-C Median Throughput vs Warehouse Scale (Multi-Trial Aggregated)\n"
            "Hardware: AMD EPYC 9654 (96-core, 192 threads, 251 GB RAM, 32 GB shared_buffers)\n"
            "Workers: 8 | Standard Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel",
            fontsize=12,
            fontweight="bold",
        )
        plt.tight_layout()

        plot_path = Path(out_dir) / "tpcc_tps_median_publication.png"
        plt.savefig(plot_path, dpi=300)
        print(f"Saved publication-quality plot to: {plot_path}")

    except Exception as e:
        print(f"Failed to generate plot: {e}")


def main():
    args = parse_args()
    records = load_all_records(args.inputs)
    if not records:
        print("ERROR: No valid records loaded. Exiting.")
        sys.exit(1)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    aggregated = aggregate_by_config(records)
    print_comparison_table(aggregated)

    # Save summary_median.csv
    out_csv = out_dir / "summary_median.csv"
    fields = [
        "mode", "warehouses", "server_workers", "trials_count",
        "median_tps", "mean_tps", "std_tps", "min_tps", "max_tps",
        "median_wall_time_ms", "mean_wall_time_ms", "merkle_pass",
        "divergence_count", "permanent_failures",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(aggregated)
    print(f"Saved median summary to: {out_csv}")

    plot_median_results(aggregated, out_dir)


if __name__ == "__main__":
    main()
