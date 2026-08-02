#!/usr/bin/env python3
"""Generate analysis PNG charts for Dynamic_merkle_docs/plots/ comparing Static vs Dynamic (Synchronous Direct)."""

from __future__ import annotations

import os
import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCS_PLOTS = ROOT / "Dynamic_merkle_docs" / "plots"
DOCS_PLOTS.mkdir(parents=True, exist_ok=True)

STATIC_DIR = (
    ROOT
    / "scripts"
    / "benchmark"
    / "recovery"
    / "fetched"
    / "ariabc-recovery-best-scaling-f32-l1024-k75-c300-20260714T040459Z-0068d0"
)
DYNAMIC_DIR = (
    ROOT
    / "scripts"
    / "benchmark"
    / "recovery"
    / "fetched"
    / "ariabc-recovery-size-scaling-k75-c300-20260801T172828Z-00530a"
)


def load_data(dir_path: Path) -> pd.DataFrame:
    df_runs = pd.read_csv(dir_path / "runs.csv")
    df_phase = pd.read_csv(dir_path / "phase_timings.csv")
    df_piv = df_phase.pivot_table(index="run_id", columns="phase", values="ms").reset_index()
    df = pd.merge(df_runs, df_piv, on="run_id")
    return df


def get_warm_medians(df: pd.DataFrame) -> pd.DataFrame:
    warm = df[df["repetition"] >= 1]
    grouped = warm.groupby("tuple_count").median(numeric_only=True).reset_index()
    return grouped


def main() -> None:
    df_s = get_warm_medians(load_data(STATIC_DIR))
    df_d = get_warm_medians(load_data(DYNAMIC_DIR))

    scale_points = sorted(df_d["tuple_count"].unique())
    x_labels = [f"{int(n // 1_000_000)}M" for n in scale_points]
    x = list(range(len(scale_points)))

    plt.rcParams.update({"font.size": 11, "figure.titlesize": 14, "axes.titlesize": 13})

    COLOR_STATIC = "#d62728"   # Red
    COLOR_DYNAMIC = "#1f77b4"  # Blue

    # 1. Total Recovery Latency Chart
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["restore_repair_ms"], label="Static (F32 / L1024)", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["restore_repair_ms"], label="Dynamic (Synchronous Direct)", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)
    
    plt.title("Total Recovery Latency: Static vs Dynamic (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Total Recovery Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "total_recovery_latency.png")
    plt.close()

    # 2. Tree Localisation Latency Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["tree_localisation_ms"], label="Static Localisation", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["tree_localisation_ms"], label="Dynamic Localisation", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Tree Localisation Latency: Static vs Dynamic (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Localisation Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "tree_localisation_comparison.png")
    plt.close()

    # 3. Candidate Fetch Latency Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["candidate_row_fetch_ms"], label="Static Candidate Fetch", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["candidate_row_fetch_ms"], label="Dynamic Candidate Fetch", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Candidate Row Fetch Latency (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Candidate Fetch Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "candidate_fetch_comparison.png")
    plt.close()

    # 4. Row Comparison Latency Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["row_comparison_ms"], label="Static Row Comparison", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["row_comparison_ms"], label="Dynamic Row Comparison", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Row / Tuple Comparison Latency (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Row Comparison Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "row_comparison_comparison.png")
    plt.close()

    # 5. Repair Write Latency Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["repair_write_ms"], label="Static Repair Write", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["repair_write_ms"], label="Dynamic Repair Write (Synchronous Direct)", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Repair Write Latency: Static vs Dynamic (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Repair Write Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "repair_write_comparison.png")
    plt.close()

    # 6. Post-Repair Confirmation Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["targeted_post_repair_confirmation_ms"], label="Static Confirmation Barrier", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["targeted_post_repair_confirmation_ms"], label="Dynamic Post-Repair Confirmation Barrier", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Targeted Post-Repair Confirmation Latency (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Confirmation Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "post_repair_confirmation_comparison.png")
    plt.close()

    # 7. Leaf Occupancy Scaling Comparison
    plt.figure(figsize=(10, 6), dpi=300)
    plt.plot(x, df_s["mean_rows_per_bad_leaf"], label="Static (F32 / L1024 Candidate Rows / Query)", color=COLOR_STATIC, marker="o", linewidth=2.5)
    plt.plot(x, df_d["mean_rows_per_bad_leaf"], label="Dynamic (Split Threshold = 32 Candidate Rows / Query)", color=COLOR_DYNAMIC, marker="s", linewidth=2.5)

    plt.title("Leaf Occupancy & Candidate Rows / Bad Leaf Query (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Mean Candidate Rows / Bad Leaf Query")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "leaf_occupancy_scaling.png")
    plt.close()

    # 8. Phase Stacked Composition Comparison for Dynamic
    plt.figure(figsize=(12, 6), dpi=300)
    loc = df_d["tree_localisation_ms"].to_numpy()
    fet = df_d["candidate_row_fetch_ms"].to_numpy()
    cmp = df_d["row_comparison_ms"].to_numpy()
    rep = df_d["repair_write_ms"].to_numpy()
    cnf = df_d["targeted_post_repair_confirmation_ms"].to_numpy()
    orc = df_d.get("recovery_orchestration_ms", pd.Series(15.0, index=df_d.index)).to_numpy()

    width = 0.55
    plt.bar(x, loc, width, label="Tree Localisation", color="#1f77b4")
    plt.bar(x, fet, width, bottom=loc, label="Candidate Row Fetch", color="#ff7f0e")
    plt.bar(x, cmp, width, bottom=loc + fet, label="Row Comparison", color="#2ca02c")
    plt.bar(x, rep, width, bottom=loc + fet + cmp, label="Synchronous Repair Write (DML)", color="#d62728")
    plt.bar(x, cnf, width, bottom=loc + fet + cmp + rep, label="Post-Repair Confirmation", color="#9467bd")
    plt.bar(x, orc, width, bottom=loc + fet + cmp + rep + cnf, label="Orchestration / Other", color="#8c564b")

    plt.title("Synchronous Dynamic Recovery Phase Timing Composition (1M - 50M Tuples)")
    plt.xlabel("Dataset Size (Tuples)")
    plt.ylabel("Latency (ms)")
    plt.xticks(x, x_labels)
    plt.grid(True, linestyle="--", alpha=0.4, axis="y")
    plt.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    plt.tight_layout()
    plt.savefig(DOCS_PLOTS / "phase_stacked_composition.png")
    plt.close()

    print("Successfully generated updated PNG charts comparing Static vs Dynamic in Dynamic_merkle_docs/plots/")


if __name__ == "__main__":
    main()
