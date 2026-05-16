#!/usr/bin/env python3
"""
plot_three_modes_comparison.py

Reads combined_results.csv (produced by run_three_mode_bench.sh or
run_three_mode_bench_all_nodes.sh) and generates four graphs:

  Graph 1 — TPS vs Threads — all three det modes + pg baseline
             One line per gate_mode.  All workloads overlaid if multiple.

  Graph 2 — Speedup ratio vs Threads
             leverd / paper  and  leverd_t3 / paper.
             Shows directly how much the Lever D and T3 optimisations help.

  Graph 3 — TPS bar chart at each thread count (grouped bars, all 3 modes)
             Side-by-side bars at threads = 1, 4, 8, 12, 16.

  Graph 4 — Per-node TPS comparison at peak-thread count
             One cluster of bars per node; three bars per cluster.
             Falls back to a single-node bar chart when node column is absent.

Usage:
    python3 plot_three_modes_comparison.py <combined_csv> <out_dir>
"""

import csv
import sys
from pathlib import Path
from typing import Optional

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import numpy as np
except ImportError:
    print("ERROR: matplotlib + numpy are required", file=sys.stderr)
    sys.exit(1)


# ---- constants ----
MODE_LABELS = {
    "paper":     "Paper (last_committed gate)",
    "leverd":    "Lever D (published_max + blocking finish)",
    "leverd_t3": "Lever D + T3 (published_max + CAS finish)",
    "pg_baseline": "PostgreSQL baseline",
    "pg":        "PostgreSQL baseline",
}
MODE_COLORS = {
    "paper":       "#d62728",   # red
    "leverd":      "#ff7f0e",   # orange
    "leverd_t3":   "#2ca02c",   # green
    "pg_baseline": "#1f77b4",   # blue
    "pg":          "#1f77b4",
}
MODE_MARKERS = {
    "paper": "s", "leverd": "^", "leverd_t3": "o",
    "pg_baseline": "D", "pg": "D",
}
DET_MODES = ["paper", "leverd", "leverd_t3"]
PLOT_ORDER = ["pg_baseline", "pg", "paper", "leverd", "leverd_t3"]


# ---- helpers ----
def _float(v: str) -> Optional[float]:
    try:
        return float(v.strip())
    except Exception:
        return None


def _int(v: str) -> Optional[int]:
    try:
        return int(v.strip())
    except Exception:
        return None


def _tps_from_row(row: dict) -> Optional[float]:
    """Derive TPS from a results.csv row."""
    ms = _float(row.get("workload_overall_ms", ""))
    if ms and ms > 0:
        # 20 000 transactions per workload (default YCSB run)
        # Use the actual count from the workload name if parseable; default 20 000.
        wl = row.get("workload", "")
        n_tx = 20000
        import re
        m = re.search(r"-(\d+)k-", wl)
        if m:
            n_tx = int(m.group(1)) * 1000
        else:
            m2 = re.search(r"-(\d{4,6})-", wl)
            if m2:
                n_tx = int(m2.group(1))
        return 1000.0 * n_tx / ms
    return None


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def gate_mode_of(row: dict) -> str:
    """Normalise gate_mode / mode column to one of our known keys."""
    gm = row.get("gate_mode", "").strip().lower()
    if gm in MODE_LABELS:
        return gm
    mode = row.get("mode", "").strip().lower()
    if mode in ("pg", "postgres"):
        return "pg_baseline"
    return gm or mode or "unknown"


def build_series(
    rows: list[dict],
    thread_filter: Optional[list[int]] = None,
) -> dict[str, dict[int, list[float]]]:
    """
    Returns {gate_mode: {threads: [tps, ...]}} averaging across workloads + runs.
    """
    data: dict[str, dict[int, list[float]]] = {}
    for row in rows:
        gm = gate_mode_of(row)
        th = _int(row.get("threads", ""))
        tps = _tps_from_row(row)
        if th is None or tps is None or tps <= 0:
            continue
        if thread_filter and th not in thread_filter:
            continue
        data.setdefault(gm, {}).setdefault(th, []).append(tps)
    return data


def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _save(fig, path: Path) -> Path:
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path


# ============================================================
# Graph 1 — TPS vs Threads
# ============================================================
def graph1_tps_vs_threads(rows: list[dict], out_dir: Path) -> Path:
    series = build_series(rows)
    fig, ax = plt.subplots(figsize=(10, 6))

    plotted = 0
    for gm in PLOT_ORDER:
        pts = series.get(gm)
        if not pts:
            continue
        xs = sorted(pts.keys())
        ys = [_mean(pts[x]) for x in xs]
        label = MODE_LABELS.get(gm, gm)
        color = MODE_COLORS.get(gm, "#999999")
        marker = MODE_MARKERS.get(gm, "o")
        ax.plot(xs, ys, marker=marker, linewidth=2.2, markersize=6,
                color=color, label=label)
        plotted += 1

    if plotted == 0:
        plt.close(fig)
        raise ValueError("no data for graph1")

    ax.set_xlabel("Threads", fontsize=12)
    ax.set_ylabel("TPS (mean)", fontsize=12)
    ax.set_title("TPS vs Threads — Paper / Lever D / Lever D+T3", fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    return _save(fig, out_dir / "graph1_tps_vs_threads.png")


# ============================================================
# Graph 2 — Speedup ratio vs Threads
# ============================================================
def graph2_speedup_ratio(rows: list[dict], out_dir: Path) -> Path:
    series = build_series(rows)
    paper = series.get("paper")
    if not paper:
        raise ValueError("no paper-mode data for graph2")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axhline(1.0, color="#d62728", linewidth=1.5, linestyle="--", label="paper (baseline = 1×)")

    for gm, color, marker in [
        ("leverd",    "#ff7f0e", "^"),
        ("leverd_t3", "#2ca02c", "o"),
    ]:
        pts = series.get(gm)
        if not pts:
            continue
        xs = sorted(set(pts.keys()) & set(paper.keys()))
        ys = [_mean(pts[x]) / _mean(paper[x]) for x in xs if _mean(paper[x]) > 0]
        label = f"{MODE_LABELS.get(gm, gm)} speedup vs paper"
        ax.plot(xs, ys, marker=marker, linewidth=2.2, markersize=6,
                color=color, label=label)

    ax.set_xlabel("Threads", fontsize=12)
    ax.set_ylabel("Speedup (× paper TPS)", fontsize=12)
    ax.set_title("Speedup vs Paper-style Gate", fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    return _save(fig, out_dir / "graph2_speedup_vs_paper.png")


# ============================================================
# Graph 3 — Grouped bar chart at each thread count
# ============================================================
def graph3_bar_by_threads(rows: list[dict], out_dir: Path) -> Path:
    series = build_series(rows)
    all_threads = sorted({th for pts in series.values() for th in pts.keys()})
    det_modes_present = [gm for gm in DET_MODES if gm in series]
    if not det_modes_present:
        raise ValueError("no det-mode data for graph3")

    n_threads = len(all_threads)
    n_modes = len(det_modes_present)
    width = 0.7 / n_modes
    x = np.arange(n_threads)

    fig, ax = plt.subplots(figsize=(max(10, n_threads * 1.5), 6))
    for i, gm in enumerate(det_modes_present):
        pts = series[gm]
        vals = [_mean(pts.get(th, [0])) for th in all_threads]
        offset = (i - (n_modes - 1) / 2.0) * width
        bars = ax.bar(x + offset, vals, width=width * 0.9,
                      label=MODE_LABELS.get(gm, gm),
                      color=MODE_COLORS.get(gm, "#999999"))
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                        f"{val:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_xlabel("Threads", fontsize=12)
    ax.set_ylabel("TPS (mean)", fontsize=12)
    ax.set_title("TPS per Thread Count — All Three Modes", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels([str(t) for t in all_threads])
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    return _save(fig, out_dir / "graph3_bar_by_threads.png")


# ============================================================
# Graph 4 — Per-node comparison at peak thread count
# ============================================================
def graph4_per_node(rows: list[dict], out_dir: Path) -> Path:
    # Find peak thread count (highest thread with data)
    all_threads = set()
    for row in rows:
        th = _int(row.get("threads", ""))
        if th:
            all_threads.add(th)
    if not all_threads:
        raise ValueError("no thread data for graph4")
    peak_th = max(all_threads)

    # Try to group by node
    node_col_present = any("node" in row for row in rows)
    if not node_col_present:
        # Single node: just show a TPS bar at peak thread by mode
        series = build_series(rows, thread_filter=[peak_th])
        fig, ax = plt.subplots(figsize=(8, 5))
        modes_present = [gm for gm in DET_MODES if gm in series]
        vals = [_mean(series[gm].get(peak_th, [0])) for gm in modes_present]
        colors = [MODE_COLORS.get(gm, "#999999") for gm in modes_present]
        labels = [MODE_LABELS.get(gm, gm) for gm in modes_present]
        bars = ax.bar(labels, vals, color=colors, width=0.5)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                        f"{val:.0f}", ha="center", va="bottom", fontsize=9)
        ax.set_ylabel("TPS (mean)", fontsize=12)
        ax.set_title(f"TPS Comparison at threads={peak_th}", fontsize=13)
        ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
        return _save(fig, out_dir / "graph4_per_node_peak_threads.png")

    # Multi-node grouped bar chart
    nodes_by_row: dict[str, list[dict]] = {}
    for row in rows:
        n = row.get("node", "local").strip()
        nodes_by_row.setdefault(n, []).append(row)

    nodes = sorted(nodes_by_row.keys())
    det_modes_present = DET_MODES  # try all

    n_nodes = len(nodes)
    n_modes = len(det_modes_present)
    width = 0.7 / n_modes
    x = np.arange(n_nodes)

    fig, ax = plt.subplots(figsize=(max(10, n_nodes * 2), 6))
    for i, gm in enumerate(det_modes_present):
        vals = []
        for node in nodes:
            node_rows = [r for r in nodes_by_row[node] if gate_mode_of(r) == gm]
            s = build_series(node_rows, thread_filter=[peak_th])
            tps = _mean(s.get(gm, {}).get(peak_th, [0]))
            vals.append(tps)
        offset = (i - (n_modes - 1) / 2.0) * width
        bars = ax.bar(x + offset, vals, width=width * 0.9,
                      label=MODE_LABELS.get(gm, gm),
                      color=MODE_COLORS.get(gm, "#999999"))
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                        f"{val:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_xlabel("Node", fontsize=12)
    ax.set_ylabel("TPS (mean)", fontsize=12)
    ax.set_title(f"Per-node TPS at threads={peak_th} — All Three Modes", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(nodes, rotation=25, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    return _save(fig, out_dir / "graph4_per_node_peak_threads.png")


# ============================================================
# Main
# ============================================================
def main() -> int:
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <combined_csv> <out_dir>", file=sys.stderr)
        return 2

    csv_path = Path(sys.argv[1])
    out_dir  = Path(sys.argv[2])

    if not csv_path.exists():
        print(f"ERROR: not found: {csv_path}", file=sys.stderr)
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(csv_path)
    if not rows:
        print("ERROR: empty CSV", file=sys.stderr)
        return 1

    print(f"Loaded {len(rows)} rows from {csv_path}")
    gate_modes_found = sorted({gate_mode_of(r) for r in rows})
    print(f"Gate modes: {gate_modes_found}")

    saved = []
    errors = []

    for fn, label in [
        (lambda: graph1_tps_vs_threads(rows, out_dir), "graph1"),
        (lambda: graph2_speedup_ratio(rows, out_dir),   "graph2"),
        (lambda: graph3_bar_by_threads(rows, out_dir),  "graph3"),
        (lambda: graph4_per_node(rows, out_dir),        "graph4"),
    ]:
        try:
            p = fn()
            saved.append(p)
        except Exception as e:
            errors.append(f"{label}: {e}")
            print(f"  WARN: {label} skipped — {e}", file=sys.stderr)

    print(f"\nGenerated {len(saved)} graph(s):")
    for p in saved:
        print(f"  {p}")
    if errors:
        print(f"\nSkipped ({len(errors)}):")
        for e in errors:
            print(f"  {e}")

    return 0 if saved else 1


if __name__ == "__main__":
    sys.exit(main())
