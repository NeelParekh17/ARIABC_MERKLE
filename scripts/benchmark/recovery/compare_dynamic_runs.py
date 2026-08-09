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
        v1 = np.nan_to_num(np.array([old_med[tc].get(key, float("nan")) for tc in scales]))
        v2 = np.nan_to_num(np.array([new_med[tc].get(key, float("nan")) for tc in scales]))
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

    # Generate plot suite
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
        ("leaf_occupancy_scaling.png",       "rpl",    f"Leaf Occupancy Scaling: {old_label} vs {new_label}",       "Mean Candidate Rows / Bad Leaf"),
    ]

    for fname, key, title, ylabel in specs:
        save_comparison_line(plots_dir / fname, x, x_labels, ov(key), nv(key), title, ylabel, old_label=old_label, new_label=new_label)

    # Custom Tree Localisation with right-axis levels
    save_localisation_with_levels(
        plots_dir / "tree_localisation_comparison.png",
        x, x_labels, ov("loc"), nv("loc"), tree_depth_vals,
        old_label=old_label, new_label=new_label,
    )

    save_stacked_side_by_side(plots_dir / "phase_stacked_composition.png", x, x_labels, old_med, new_med, scales, old_label=old_label, new_label=new_label)
    save_cv_comparison(plots_dir / "cv_per_scale.png", x, x_labels, [cv_old.get(tc, float("nan")) for tc in scales], [cv_new.get(tc, float("nan")) for tc in scales], old_label=old_label, new_label=new_label)

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
        diff = n - o
        pct = ((n - o) / o * 100.0) if o > 0 else float("nan")
        pct_str = f"**{pct:+.1f}%** ⚡" if pct < 0 else f"+{pct:.1f}%"
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        L.append(f"| **{lbl}** | {fmt(o)} | {fmt(n)} | {diff:+.2f} | {pct_str} |")

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
        "### 6.2 Variance (CV%) Comparison",
        f"![CV% Comparison]({plt_rel('cv_per_scale.png')})",
        "",
        "---",
        "",
        "## 7. Dataset Construction Latency Comparison (1M → 50M)",
        "",
        f"![Dataset Construction Latency]({plt_rel('dataset_build_time_comparison.png')})",
        "",
        "### 7.1 Step-by-Step Incremental Dataset Expansion Time",
        "",
        f"| Scale | Appended Tuples | Setup Mode | {old_label} (s) | {new_label} (s) | Delta (s) | Speedup / Change |",
        "|:---|:---|:---|---:|---:|---:|---:|",
    ]

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
                        out[tc] = data['timings_ms']
                except Exception:
                    pass
        return out

    ds_old = parse_progress_jsonl(old_dir)
    ds_new = parse_progress_jsonl(new_dir)

    for tc in scales:
        o_ms = ds_old.get(tc, {}).get("dataset_total_ms", float("nan"))
        n_ms = ds_new.get(tc, {}).get("dataset_total_ms", float("nan"))
        o_s = o_ms / 1000.0 if not math.isnan(o_ms) else float("nan")
        n_s = n_ms / 1000.0 if not math.isnan(n_ms) else float("nan")
        diff_s = n_s - o_s
        pct = ((n_s - o_s) / o_s * 100.0) if o_s > 0 else float("nan")
        pct_str = f"**{pct:+.1f}%** ⚡" if pct < 0 else f"+{pct:.1f}%"
        appended = ds_new.get(tc, {}).get("appended_tuple_count", tc)
        mode = ds_new.get(tc, {}).get("dataset_setup_mode", "bulk-logged")
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        app_lbl = f"+{appended // 1_000_000}M" if appended < tc else f"{appended // 1_000_000}M"
        L.append(f"| **{lbl}** | {app_lbl} | `{mode}` | {fmt(o_s)} s ({o_s/60:.2f} m) | {fmt(n_s)} s ({n_s/60:.2f} m) | {diff_s:+.2f} s | {pct_str} |")

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
        o_s = ds_old.get(tc, {}).get("dataset_total_ms", 0.0) / 1000.0
        n_s = ds_new.get(tc, {}).get("dataset_total_ms", 0.0) / 1000.0
        cum_o += o_s
        cum_n += n_s
        lbl = f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc)
        diff_cum = cum_n - cum_o
        L.append(f"| **{lbl}** | {cum_o:.2f} s ({cum_o/60:.2f} m) | {cum_n:.2f} s ({cum_n/60:.2f} m) | {diff_cum:+.2f} s ({diff_cum/60:+.2f} m) |")

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
