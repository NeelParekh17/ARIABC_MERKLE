#!/usr/bin/env python3
"""
compare_dynamic_runs.py — Compare two dynamic recovery benchmark runs.

Reads fetched result directories for OLD and NEW dynamic runs, generates
comparative matplotlib PNG plots comparing "Dynamic Old" vs "Dynamic New",
and writes a comprehensive Markdown report.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, stdev

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]

COLOR_OLD = "#d62728"  # Red/Orange for Dynamic Old
COLOR_NEW = "#1f77b4"  # Blue for Dynamic New
PLT_PARAMS = {"font.size": 11, "figure.titlesize": 14, "axes.titlesize": 13}


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _flt(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def warm_medians(runs: list[dict], phase_rows: list[dict]) -> dict[int, dict]:
    phase_map: dict[str, dict[str, float]] = defaultdict(dict)
    for p in phase_rows:
        phase_map[p["run_id"]][p["phase"]] = _flt(p["ms"])

    groups: dict[int, list[dict]] = defaultdict(list)
    all_groups: dict[int, list[dict]] = defaultdict(list)
    for r in runs:
        tc = int(_flt(r["tuple_count"]))
        merged = dict(r)
        merged.update(phase_map.get(r["run_id"], {}))
        all_groups[tc].append(merged)
        if int(r.get("repetition", 0)) >= 1:
            groups[tc].append(merged)

    for tc, reps in all_groups.items():
        if tc not in groups:
            groups[tc] = reps

    PHASE_KEYS = {
        "loc":    ["tree_localisation_ms", "tree_localisation"],
        "fetch":  ["candidate_row_fetch_ms", "candidate_row_fetch"],
        "cmp":    ["row_comparison_ms", "row_comparison"],
        "repair": ["repair_write_ms", "repair_write"],
        "dml_wire": ["repair_dml_wire_ms", "repair_dml_wire"],
        "commit_wire": ["repair_commit_wire_ms", "repair_commit_wire"],
        "conf":   ["targeted_post_repair_confirmation_ms", "targeted_post_repair_confirmation"],
        "total":  ["restore_repair_ms"],
        "rpl":    ["mean_rows_per_bad_leaf"],
        "tree_depth_raw": ["child_hash_sql_calls"],
    }

    result = {}
    for tc, reps in groups.items():
        row = {}
        for key, fields in PHASE_KEYS.items():
            vals = []
            for rep in reps:
                for f in fields:
                    v = _flt(rep.get(f))
                    if not math.isnan(v):
                        vals.append(v)
                        break
            row[key] = median(vals) if vals else float("nan")
        result[tc] = row
    return result


def all_reps(runs: list[dict]) -> dict[int, list[tuple[int, float]]]:
    out: dict[int, list] = defaultdict(list)
    for r in runs:
        tc = int(_flt(r["tuple_count"]))
        out[tc].append((int(r.get("repetition", 0)), _flt(r.get("restore_repair_ms", "nan"))))
    return {k: sorted(v) for k, v in out.items()}


def cv_per_scale(dyn_all: dict[int, list[tuple[int, float]]]) -> dict[int, float]:
    result = {}
    for tc, reps in dyn_all.items():
        vals = [v for (_, v) in reps if not math.isnan(v)]
        if len(vals) >= 2:
            mu = sum(vals) / len(vals)
            sd = stdev(vals)
            result[tc] = (sd / mu * 100.0) if mu > 0 else float("nan")
        else:
            result[tc] = float("nan")
    return result


def depth_verification_per_scale(fetched_dir: Path) -> dict[int, dict[str, str]]:
    rows: dict[int, dict[str, str]] = {}
    dsizes = read_csv(fetched_dir / "dataset_sizes.csv") if (fetched_dir / "dataset_sizes.csv").exists() else []
    for d in dsizes:
        tc_val = _flt(d.get("tuple_count", ""))
        if math.isnan(tc_val) or tc_val <= 0:
            continue
        tc = int(tc_val)
        measured_depth = _flt(d.get("tree_depth", ""))
        measured_height = _flt(d.get("tree_height", ""))
        rows[tc] = {
            "measured_depth": str(int(measured_depth)) if not math.isnan(measured_depth) and measured_depth >= 0 else "N/A",
            "measured_height": str(int(measured_height)) if not math.isnan(measured_height) and measured_height > 0 else "N/A",
        }
    return rows


def tree_levels_per_scale(fetched_dir: Path) -> dict[int, int]:
    infos = depth_verification_per_scale(fetched_dir)
    measured = {
        tc: int(info["measured_height"])
        for tc, info in infos.items()
        if info["measured_height"] != "N/A"
    }
    return measured


def save_localisation_with_levels(
    path: Path, x, x_labels,
    old_vals: list[float], new_vals: list[float],
    tree_depth: list[int],
    old_label: str = "Dynamic Old",
    new_label: str = "Dynamic New",
    depth_label: str = "Actual Tree Height (root=1)",
):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax1 = plt.subplots(figsize=(11, 6), dpi=150)

    ax1.plot(x, old_vals, label=old_label, color=COLOR_OLD, marker="o", linewidth=2.2, linestyle="--")
    ax1.plot(x, new_vals, label=new_label, color=COLOR_NEW, marker="s", linewidth=2.5)
    for xi, val in zip(x, new_vals):
        if not math.isnan(val):
            ax1.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, 7),
                         ha="center", fontsize=8.5, fontweight="bold", color=COLOR_NEW)
    for xi, val in zip(x, old_vals):
        if not math.isnan(val):
            ax1.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, -13),
                         ha="center", fontsize=8.5, color=COLOR_OLD)
    ax1.set_xlabel("Dataset Size")
    ax1.set_ylabel("Tree Localisation Latency (ms)", color="black")
    ax1.set_xticks(x); ax1.set_xticklabels(x_labels)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.set_title(f"Tree Localisation Latency: {old_label} vs {new_label}\n(with {depth_label} — right axis)")

    if any(td > 0 for td in tree_depth):
        ax2 = ax1.twinx()
        depth_color = "#e377c2"
        ax2.step(x, tree_depth, where="mid", color=depth_color, linewidth=2.0, linestyle=":", label=depth_label)
        ax2.set_ylabel(depth_label, color=depth_color)
        ax2.tick_params(axis="y", labelcolor=depth_color)
        
        unique_depths = sorted(set(td for td in tree_depth if td > 0))
        if unique_depths:
            ymin = min(unique_depths) - 1
            ymax = max(unique_depths) + 1
            ax2.set_ylim(ymin, ymax)
            ax2.set_yticks(list(range(min(unique_depths), max(unique_depths) + 1)))

        prev = None
        for xi, depth in zip(x, tree_depth):
            if depth > 0 and depth != prev:
                ax2.annotate(
                    f"Height {depth}",
                    xy=(xi, depth),
                    xytext=(xi + 0.1, depth + 0.15),
                    fontsize=9,
                    color=depth_color,
                    fontweight="bold",
                )
                prev = depth

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, frameon=True, facecolor="white", framealpha=0.9, loc="upper left")
    else:
        ax1.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")

    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_comparison_line(path: Path, x, x_labels, old_vals, new_vals, title, ylabel, old_label="Dynamic Old", new_label="Dynamic New"):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    ax.plot(x, old_vals, label=old_label, color=COLOR_OLD, marker="o", linewidth=2.2, linestyle="--")
    ax.plot(x, new_vals, label=new_label, color=COLOR_NEW, marker="s", linewidth=2.5)
    for xi, val in zip(x, new_vals):
        if not math.isnan(val):
            ax.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=8.5, fontweight="bold", color=COLOR_NEW)
    for xi, val in zip(x, old_vals):
        if not math.isnan(val):
            ax.annotate(f"{val:.1f}", (xi, val), textcoords="offset points", xytext=(0, -13),
                        ha="center", fontsize=8.5, color=COLOR_OLD)
    ax.set_title(title)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_stacked_side_by_side(path: Path, x, x_labels, old_med: dict[int, dict], new_med: dict[int, dict], scales: list[int], old_label="Dynamic Old", new_label="Dynamic New"):
    plt.rcParams.update(PLT_PARAMS)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), dpi=150, sharey=True)
    keys   = ["loc",   "fetch",   "cmp",   "repair",  "conf"]
    labels = ["Tree Localisation", "Candidate Fetch", "Row Comparison", "Repair Write (DML)", "Post-Repair Confirmation"]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

    b1 = np.zeros(len(scales))
    b2 = np.zeros(len(scales))
    for key, lbl, col in zip(keys, labels, colors):
        v1 = np.nan_to_num(np.array([old_med.get(tc, {}).get(key, float("nan")) for tc in scales]))
        v2 = np.nan_to_num(np.array([new_med.get(tc, {}).get(key, float("nan")) for tc in scales]))
        ax1.bar(x, v1, 0.55, bottom=b1, label=lbl, color=col)
        ax2.bar(x, v2, 0.55, bottom=b2, label=lbl, color=col)
        b1 += v1
        b2 += v2

    ax1.set_title(f"{old_label} Composition")
    ax1.set_xlabel("Dataset Size"); ax1.set_ylabel("Latency (ms)")
    ax1.set_xticks(x); ax1.set_xticklabels(x_labels)
    ax1.grid(True, linestyle="--", alpha=0.4, axis="y")

    ax2.set_title(f"{new_label} Composition")
    ax2.set_xlabel("Dataset Size")
    ax2.set_xticks(x); ax2.set_xticklabels(x_labels)
    ax2.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax2.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")

    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_cv_comparison(path: Path, x, x_labels, cv_old: list[float], cv_new: list[float], old_label="Dynamic Old", new_label="Dynamic New"):
    plt.rcParams.update(PLT_PARAMS)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    w = 0.35
    x_old = [pos - w/2 for pos in x]
    x_new = [pos + w/2 for pos in x]

    clean_old = [0.0 if math.isnan(v) else v for v in cv_old]
    clean_new = [0.0 if math.isnan(v) else v for v in cv_new]

    ax.bar(x_old, clean_old, w, label=f"{old_label} CV%", color=COLOR_OLD, alpha=0.85)
    ax.bar(x_new, clean_new, w, label=f"{new_label} CV%", color=COLOR_NEW, alpha=0.85)
    ax.axhline(20, color="#ff7f0e", linewidth=1.8, linestyle="--", label="20% stability threshold")

    ax.set_title("Coefficient of Variation (CV%) Comparison per Scale")
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("CV (σ/μ × 100%)")
    ax.set_xticks(x); ax.set_xticklabels(x_labels)
    ax.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax.legend(frameon=True, facecolor="white", framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def save_dataset_composition_side_by_side(path: Path, x, x_labels, ds_old: dict[int, dict], ds_new: dict[int, dict], scales: list[int], old_label="Dynamic Old", new_label="Dynamic New"):
    plt.rcParams.update(PLT_PARAMS)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), dpi=150, sharey=True)

    def extract_cats(ds_data):
        heap_sec = []
        merkle_sec = []
        pk_sec = []
        ckpt_sec = []
        for tc in scales:
            t = ds_data.get(tc, {})
            h_tbl = _flt(t.get("healthy_table_ms", 0.0))
            d_tbl = _flt(t.get("damaged_table_ms", 0.0))
            h_idx = _flt(t.get("healthy_indexes_ms", 0.0))
            d_idx = _flt(t.get("damaged_indexes_ms", 0.0))
            pk = _flt(t.get("primary_keys_ms", 0.0))
            ckpt = _flt(t.get("analyze_checkpoint_ms", 0.0))

            h_tbl = 0.0 if math.isnan(h_tbl) else h_tbl
            d_tbl = 0.0 if math.isnan(d_tbl) else d_tbl
            h_idx = 0.0 if math.isnan(h_idx) else h_idx
            d_idx = 0.0 if math.isnan(d_idx) else d_idx
            pk = 0.0 if math.isnan(pk) else pk
            ckpt = 0.0 if math.isnan(ckpt) else ckpt

            heap_sec.append((h_tbl + d_tbl) / 1000.0)
            merkle_sec.append((h_idx + d_idx) / 1000.0)
            pk_sec.append(pk / 1000.0)
            ckpt_sec.append(ckpt / 1000.0)
        return [
            ("Heap Data Population", np.array(heap_sec), "#1f77b4"),
            ("Merkle Tree Index Build", np.array(merkle_sec), "#ff7f0e"),
            ("Primary Keys & Logging", np.array(pk_sec), "#2ca02c"),
            ("Catalog & Analyze/Checkpoint", np.array(ckpt_sec), "#9467bd"),
        ]

    cats_old = extract_cats(ds_old)
    cats_new = extract_cats(ds_new)

    b1 = np.zeros(len(scales))
    for label, vals, color in cats_old:
        if np.any(vals > 0):
            ax1.bar(x, vals, 0.55, bottom=b1, label=label, color=color)
            b1 += vals

    b2 = np.zeros(len(scales))
    for label, vals, color in cats_new:
        if np.any(vals > 0):
            ax2.bar(x, vals, 0.55, bottom=b2, label=label, color=color)
            b2 += vals

    ax1.set_title(f"{old_label} Dataset Composition")
    ax1.set_xlabel("Dataset Target Scale"); ax1.set_ylabel("Expansion Time (Seconds)")
    ax1.set_xticks(x); ax1.set_xticklabels(x_labels)
    ax1.grid(True, linestyle="--", alpha=0.4, axis="y")

    ax2.set_title(f"{new_label} Dataset Composition")
    ax2.set_xlabel("Dataset Target Scale")
    ax2.set_xticks(x); ax2.set_xticklabels(x_labels)
    ax2.grid(True, linestyle="--", alpha=0.4, axis="y")
    ax2.legend(frameon=True, facecolor="white", framealpha=0.9, loc="upper left")

    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def fmt(v: float, d: int = 2) -> str:
    return "N/A" if math.isnan(v) else f"{v:,.{d}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old-dir", required=True, help="Path to fetched old dynamic directory")
    ap.add_argument("--new-dir", required=True, help="Path to fetched new dynamic directory")
    ap.add_argument("--old-label", default="Dynamic Old (syncommit=on)", help="Label for old run")
    ap.add_argument("--new-label", default="Dynamic New (syncommit=off)", help="Label for new run")
    ap.add_argument("--output-dir", help="Target output directory for report and plots")
    args = ap.parse_args()

    old_dir = Path(args.old_dir).resolve()
    new_dir = Path(args.new_dir).resolve()

    old_id = old_dir.name
    new_id = new_dir.name
    old_label = args.old_label
    new_label = args.new_label

    if args.output_dir:
        report_dir = Path(args.output_dir).resolve()
    else:
        report_dir = ROOT / "Dynamic_merkle_docs" / "run_reports" / f"DYNAMIC_COMPARISON_{old_id}_VS_{new_id}"
    
    plots_dir = report_dir / "plots"
    report_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / f"DYNAMIC_COMPARISON_REPORT_{old_id}_VS_{new_id}.md"

    # Load data
    runs_old = read_csv(old_dir / "runs.csv")
    phases_old = read_csv(old_dir / "phase_timings.csv") if (old_dir / "phase_timings.csv").exists() else []
    old_med = warm_medians(runs_old, phases_old)
    old_all = all_reps(runs_old)
    cv_old = cv_per_scale(old_all)

    runs_new = read_csv(new_dir / "runs.csv")
    phases_new = read_csv(new_dir / "phase_timings.csv") if (new_dir / "phase_timings.csv").exists() else []
    new_med = warm_medians(runs_new, phases_new)
    new_all = all_reps(runs_new)
    cv_new = cv_per_scale(new_all)

    scales = sorted(set(old_med.keys()) | set(new_med.keys()))
    x = list(range(len(scales)))
    x_labels = [f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc) for tc in scales]

    def ov(key): return [old_med.get(tc, {}).get(key, float("nan")) for tc in scales]
    def nv(key): return [new_med.get(tc, {}).get(key, float("nan")) for tc in scales]

    # Extract tree depth per scale
    levels_map = tree_levels_per_scale(new_dir)
    if not levels_map:
        levels_map = tree_levels_per_scale(old_dir)
    tree_depth_vals = [levels_map.get(tc, 0) for tc in scales]

    # Generate plot suite
    specs = [
        ("total_recovery_latency.png",       "total",  f"Total Recovery Latency: {old_label} vs {new_label}",       "Total Recovery Latency (ms)"),
        ("candidate_fetch_comparison.png",   "fetch",  f"Candidate Row Fetch Latency: {old_label} vs {new_label}",  "Candidate Fetch Latency (ms)"),
        ("row_comparison_comparison.png",    "cmp",    f"Row Comparison Latency: {old_label} vs {new_label}",       "Row Comparison Latency (ms)"),
        ("repair_write_comparison.png",      "repair", f"Repair Write Latency: {old_label} vs {new_label}",         "Repair Write Latency (ms)"),
        ("post_repair_confirmation_comparison.png", "conf", f"Post-Repair Confirmation: {old_label} vs {new_label}", "Confirmation Latency (ms)"),
    ]

    for fname, key, title, ylabel in specs:
        save_comparison_line(plots_dir / fname, x, x_labels, ov(key), nv(key), title, ylabel, old_label=old_label, new_label=new_label)

    # Leaf Occupancy per schema
    ov_rpl_schema = [v / 2.0 if not math.isnan(v) else float("nan") for v in ov("rpl")]
    nv_rpl_schema = [v / 2.0 if not math.isnan(v) else float("nan") for v in nv("rpl")]
    save_comparison_line(
        plots_dir / "leaf_occupancy_scaling.png",
        x, x_labels, ov_rpl_schema, nv_rpl_schema,
        f"Physical Leaf Occupancy per Schema: {old_label} vs {new_label}",
        "Physical Rows / Bad Leaf (Per Schema)",
        old_label=old_label, new_label=new_label
    )

    # Custom Tree Localisation with right-axis levels
    save_localisation_with_levels(
        plots_dir / "tree_localisation_comparison.png",
        x, x_labels, ov("loc"), nv("loc"), tree_depth_vals,
        old_label=old_label, new_label=new_label,
    )

    save_stacked_side_by_side(plots_dir / "phase_stacked_composition.png", x, x_labels, old_med, new_med, scales, old_label=old_label, new_label=new_label)
    save_cv_comparison(plots_dir / "cv_per_scale.png", x, x_labels, [cv_old.get(tc, float("nan")) for tc in scales], [cv_new.get(tc, float("nan")) for tc in scales], old_label=old_label, new_label=new_label)

    # Helper for dataset timings from progress.jsonl
    def parse_progress_jsonl(fetched_dir: Path) -> dict[int, dict]:
        p = fetched_dir / 'progress.jsonl'
        if not p.exists(): return {}
        out = {}
        with open(p) as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if data.get('event') == 'dataset_build_timing' or 'timings_ms' in data:
                        tc = int(data.get('tuple_count', 0))
                        out[tc] = data.get('timings_ms', {})
                except Exception:
                    pass
        return out

    ds_old = parse_progress_jsonl(old_dir)
    ds_new = parse_progress_jsonl(new_dir)

    if ds_old or ds_new:
        ds_old_sec = [ds_old.get(tc, {}).get('dataset_total_ms', float('nan')) / 1000.0 for tc in scales]
        ds_new_sec = [ds_new.get(tc, {}).get('dataset_total_ms', float('nan')) / 1000.0 for tc in scales]
        save_comparison_line(
            plots_dir / "dataset_build_time_comparison.png",
            x, x_labels, ds_old_sec, ds_new_sec,
            f"Incremental Dataset Build Latency: {old_label} vs {new_label}",
            "Dataset Expansion Time (Seconds)",
            old_label=old_label, new_label=new_label
        )
        save_dataset_composition_side_by_side(
            plots_dir / "dataset_build_composition.png",
            x, x_labels, ds_old, ds_new, scales,
            old_label=old_label, new_label=new_label
        )

    # Build Markdown report
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    def plt_rel(fname): return f"./plots/{fname}"

    L = []
    L += [
        f"# Dynamic Recovery Comparison: {old_label} vs {new_label}",
        "",
        f"> **Generated**: {now}  ",
        f"> **{old_label}**: `{old_id}`  ",
        f"> **{new_label}**: `{new_id}`  ",
        "",
        "---",
        "",
        "## 1. Executive Summary & Key Highlights",
        "",
        f"This report provides a side-by-side comparison of **{old_label}** vs **{new_label}** recovery benchmark runs across all scales (1M to 50M tuples).",
        "",
        "---",
        "",
        "## 2. Total Recovery Latency Comparison",
        "",
        f"![Total Recovery Latency: {old_label} vs {new_label}]({plt_rel('total_recovery_latency.png')})",
        "",
        f"| Scale | {old_label} (ms) | {new_label} (ms) | Delta (ms) | Speedup / Change |",
        "|:---|---:|---:|---:|---:|",
    ]

    for tc in scales:
        o = old_med.get(tc, {}).get("total", float("nan"))
        n = new_med.get(tc, {}).get("total", float("nan"))
        if not math.isnan(o) and not math.isnan(n):
            diff = n - o
            diff_str = f"{diff:+.2f}"
            pct = ((n - o) / o * 100.0) if o > 0 else float("nan")
            pct_str = f"**{pct:+.1f}%** ⚡" if pct < 0 else f"+{pct:.1f}%"
        else:
            diff_str = "—"
            pct_str = "—"
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        L.append(f"| **{lbl}** | {fmt(o)} | {fmt(n)} | {diff_str} | {pct_str} |")

    L += [
        "",
        "---",
        "",
        "## 3. Phase-by-Phase Detailed Matrix (Warm Medians, ms)",
        "",
        "| Scale | Arch | Tree Localisation | Cand. Fetch | Row Cmp | Repair Write | Post-Repair Conf | **Total Recovery** |",
        "|:---|:---|---:|---:|---:|---:|---:|---:|",
    ]

    for tc in scales:
        o = old_med.get(tc, {})
        n = new_med.get(tc, {})
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        L.append(f"| **{lbl}** | {old_label} | {fmt(o.get('loc',float('nan')))} | {fmt(o.get('fetch',float('nan')))} | {fmt(o.get('cmp',float('nan')))} | {fmt(o.get('repair',float('nan')))} | {fmt(o.get('conf',float('nan')))} | **{fmt(o.get('total',float('nan')))} ms** |")
        L.append(f"| | {new_label} | {fmt(n.get('loc',float('nan')))} | {fmt(n.get('fetch',float('nan')))} | {fmt(n.get('cmp',float('nan')))} | {fmt(n.get('repair',float('nan')))} | {fmt(n.get('conf',float('nan')))} | **{fmt(n.get('total',float('nan')))} ms** |")

    L += [
        "",
        "---",
        "",
        "## 4. Phase Timing Composition",
        "",
        f"![Phase Timing Composition]({plt_rel('phase_stacked_composition.png')})",
        "",
        "---",
        "",
        "## 5. Sub-Phase Latency Graphs",
        "",
        "### 5.1 Tree Localisation Phase",
        f"![Tree Localisation Latency]({plt_rel('tree_localisation_comparison.png')})",
        "",
        "### 5.2 Candidate Fetch Phase",
        f"![Candidate Fetch Latency]({plt_rel('candidate_fetch_comparison.png')})",
        "",
        "### 5.3 Row Comparison Phase",
        f"![Row Comparison Latency]({plt_rel('row_comparison_comparison.png')})",
        "",
        "### 5.4 Repair Write Phase",
        f"![Repair Write Latency]({plt_rel('repair_write_comparison.png')})",
        "",
        "#### Detailed Repair Write Sub-Phase Breakdown:",
        "",
        f"| Scale | {old_label} Total `repair_write` | {old_label} `dml_wire` | {old_label} `commit_wire` | {new_label} Total `repair_write` | {new_label} `dml_wire` | {new_label} `commit_wire` | `COMMIT` Delta (ms) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for tc in scales:
        o = old_med.get(tc, {})
        n = new_med.get(tc, {})
        o_rep = o.get('repair', float('nan'))
        o_dml = o.get('dml_wire', float('nan'))
        o_com = o.get('commit_wire', float('nan'))
        n_rep = n.get('repair', float('nan'))
        n_dml = n.get('dml_wire', float('nan'))
        n_com = n.get('commit_wire', float('nan'))
        com_diff = f"{n_com - o_com:+.2f} ms" if not math.isnan(n_com) and not math.isnan(o_com) else "—"
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        L.append(f"| **{lbl}** | {fmt(o_rep)} ms | {fmt(o_dml)} ms | {fmt(o_com)} ms | {fmt(n_rep)} ms | {fmt(n_dml)} ms | {fmt(n_com)} ms | {com_diff} |")

    L += [
        "",
        "### 5.5 Post-Repair Confirmation Phase",
        f"![Post-Repair Confirmation Latency]({plt_rel('post_repair_confirmation_comparison.png')})",
        "",
        "---",
        "",
        "## 6. Leaf Occupancy & Repetition Stability",
        "",
        "### 6.1 Leaf Occupancy Comparison",
        f"![Leaf Occupancy Scaling]({plt_rel('leaf_occupancy_scaling.png')})",
        "",
        "> **Note on Leaf Occupancy**:",
        "> - **Physical Rows / Bad Leaf (Per Schema)**: The true physical row count per leaf bucket in PostgreSQL (bounded by $T_{\\text{split}} = 32$ and $T_{\\text{merge}} = 8$).",
        "> - **Combined Candidate Rows (Healthy + Damaged)**: Total rows fetched across both replicas ($2 \\times \\text{physical rows}$).",
        "",
        f"| Scale | {old_label} (Rows/Leaf per Schema) | {new_label} (Rows/Leaf per Schema) | {old_label} Cand. Rows ($K=75$) | {new_label} Cand. Rows ($K=75$) |",
        "|:---|---:|---:|---:|---:|",
    ]

    for tc in scales:
        o_comb = old_med.get(tc, {}).get("rpl", float("nan"))
        n_comb = new_med.get(tc, {}).get("rpl", float("nan"))
        o_schema = o_comb / 2.0 if not math.isnan(o_comb) else float("nan")
        n_schema = n_comb / 2.0 if not math.isnan(n_comb) else float("nan")
        o_cand = o_comb * 75.0 if not math.isnan(o_comb) else float("nan")
        n_cand = n_comb * 75.0 if not math.isnan(n_comb) else float("nan")
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        o_schema_str = f"{fmt(o_schema, 2)} rows/leaf" if not math.isnan(o_schema) else "—"
        n_schema_str = f"{fmt(n_schema, 2)} rows/leaf" if not math.isnan(n_schema) else "—"
        o_cand_str = f"{fmt(o_cand, 0)} rows" if not math.isnan(o_cand) else "—"
        n_cand_str = f"{fmt(n_cand, 0)} rows" if not math.isnan(n_cand) else "—"
        L.append(f"| **{lbl}** | {o_schema_str} | {n_schema_str} | {o_cand_str} | {n_cand_str} |")

    L += [
        "",
        "### 6.2 Variance (CV%) Comparison",
        f"![CV% Comparison]({plt_rel('cv_per_scale.png')})",
        "",
        "---",
        "",
        "## 7. Dataset Construction Latency Comparison (1M → 50M)",
        "",
        f"![Dataset Construction Latency]({plt_rel('dataset_build_time_comparison.png')})",
        "",
        f"![Dataset Construction Composition]({plt_rel('dataset_build_composition.png')})",
        "",
        "### 7.1 Step-by-Step Incremental Dataset Expansion Time",
        "",
        f"| Scale | Appended Tuples | Setup Mode | {old_label} (s) | {new_label} (s) | Delta (s) | Speedup / Change |",
        "|:---|:---|:---|---:|---:|---:|---:|",
    ]

    for tc in scales:
        o_ms = ds_old.get(tc, {}).get("dataset_total_ms", float("nan"))
        n_ms = ds_new.get(tc, {}).get("dataset_total_ms", float("nan"))
        o_s = o_ms / 1000.0 if not math.isnan(o_ms) else float("nan")
        n_s = n_ms / 1000.0 if not math.isnan(n_ms) else float("nan")
        if not math.isnan(o_s) and not math.isnan(n_s):
            diff_s = f"{n_s - o_s:+.2f} s"
            pct = ((n_s - o_s) / o_s * 100.0) if o_s > 0 else float("nan")
            pct_str = f"**{pct:+.1f}%** ⚡" if pct < 0 else f"+{pct:.1f}%"
        else:
            diff_s = "—"
            pct_str = "—"
        appended = ds_new.get(tc, {}).get("appended_tuple_count", ds_old.get(tc, {}).get("appended_tuple_count", tc))
        mode = ds_new.get(tc, {}).get("dataset_setup_mode", ds_old.get(tc, {}).get("dataset_setup_mode", "bulk-logged" if tc == scales[0] else "incremental-expansion"))
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        app_lbl = f"+{appended // 1_000_000}M" if appended < tc else f"{appended // 1_000_000}M" if appended >= 1_000_000 else f"+{appended:,}"
        o_str = f"{fmt(o_s)} s ({o_s/60:.2f} m)" if not math.isnan(o_s) else "N/A"
        n_str = f"{fmt(n_s)} s ({n_s/60:.2f} m)" if not math.isnan(n_s) else "N/A"
        L.append(f"| **{lbl}** | {app_lbl} | `{mode}` | {o_str} | {n_str} | {diff_s} | {pct_str} |")

    L += [
        "",
        "### 7.2 Cumulative Benchmark Dataset Preparation Time",
        "",
        f"| Target Scale | {old_label} Cum. Time | {new_label} Cum. Time | Cumulative Savings |",
        "|:---|---:|---:|---:|",
    ]

    cum_o = 0.0
    cum_n = 0.0
    for tc in scales:
        has_o = tc in ds_old and not math.isnan(_flt(ds_old[tc].get("dataset_total_ms")))
        has_n = tc in ds_new and not math.isnan(_flt(ds_new[tc].get("dataset_total_ms")))
        o_s = ds_old.get(tc, {}).get("dataset_total_ms", 0.0) / 1000.0 if has_o else 0.0
        n_s = ds_new.get(tc, {}).get("dataset_total_ms", 0.0) / 1000.0 if has_n else 0.0
        if has_o:
            cum_o += o_s
        if has_n:
            cum_n += n_s
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        if has_o and has_n:
            diff_cum = cum_n - cum_o
            diff_str = f"{diff_cum:+.2f} s ({diff_cum/60:+.2f} m)"
            o_cum_str = f"{cum_o:.2f} s ({cum_o/60:.2f} m)"
            n_cum_str = f"{cum_n:.2f} s ({cum_n/60:.2f} m)"
        elif has_n:
            diff_str = "—"
            o_cum_str = "N/A"
            n_cum_str = f"{cum_n:.2f} s ({cum_n/60:.2f} m)"
        else:
            diff_str = "—"
            o_cum_str = "N/A"
            n_cum_str = "N/A"
        L.append(f"| **{lbl}** | {o_cum_str} | {n_cum_str} | {diff_str} |")

    L += [
        "",
        "### 7.3 Detailed Sub-Phase Breakdown per Scale (ms)",
        "",
        f"| Scale | Arch | Healthy Heap (ms) | Damaged Heap (ms) | Healthy Merkle Index (ms) | Damaged Merkle Index (ms) | Primary Keys (ms) | Analyze / Catalog (ms) | Total Step (ms) |",
        "|:---|:---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for tc in scales:
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        to = ds_old.get(tc, {})
        tn = ds_new.get(tc, {})

        def get_row(t, arch_lbl):
            if not t:
                return f"| | {arch_lbl} | — | — | — | — | — | — | — |"
            h_tbl = _flt(t.get("healthy_table_ms", float("nan")))
            d_tbl = _flt(t.get("damaged_table_ms", float("nan")))
            h_idx = _flt(t.get("healthy_indexes_ms", float("nan")))
            d_idx = _flt(t.get("damaged_indexes_ms", float("nan")))
            pk = _flt(t.get("primary_keys_ms", float("nan")))
            ckpt = _flt(t.get("analyze_checkpoint_ms", float("nan")))
            tot = _flt(t.get("dataset_total_ms", float("nan")))
            pk_str = fmt(pk, 2) if not math.isnan(pk) else "—"
            ckpt_str = fmt(ckpt, 2) if not math.isnan(ckpt) else "—"
            return f"| | {arch_lbl} | {fmt(h_tbl, 2)} | {fmt(d_tbl, 2)} | {fmt(h_idx, 2)} | {fmt(d_idx, 2)} | {pk_str} | {ckpt_str} | **{fmt(tot, 2)} ms** |"

        L.append(f"| **{lbl}** | {old_label} | {fmt(_flt(to.get('healthy_table_ms', float('nan'))), 2)} | {fmt(_flt(to.get('damaged_table_ms', float('nan'))), 2)} | {fmt(_flt(to.get('healthy_indexes_ms', float('nan'))), 2)} | {fmt(_flt(to.get('damaged_indexes_ms', float('nan'))), 2)} | {fmt(_flt(to.get('primary_keys_ms', float('nan'))), 2) if not math.isnan(_flt(to.get('primary_keys_ms', float('nan')))) else '—'} | {fmt(_flt(to.get('analyze_checkpoint_ms', float('nan'))), 2) if not math.isnan(_flt(to.get('analyze_checkpoint_ms', float('nan')))) else '—'} | **{fmt(_flt(to.get('dataset_total_ms', float('nan'))), 2)} ms** |")
        L.append(get_row(tn, new_label))

    L += [
        "",
        "---",
        "",
        "## Provenance",
        "",
        f"- **Old Run ID**: `{old_id}`",
        f"- **New Run ID**: `{new_id}`",
        f"- **Report Path**: `{report_path}`",
    ]

    report_text = "\n".join(L)
    report_path.write_text(report_text)
    print(f"Report generated successfully at: {report_path}")


if __name__ == "__main__":
    main()
