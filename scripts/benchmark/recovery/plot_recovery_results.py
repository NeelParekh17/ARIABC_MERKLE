#!/usr/bin/env python3
"""Generate simple SVG plots for recovery benchmark CSV output."""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def as_float(value: str | None) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except ValueError:
        return None


def aggregate(rows: list[dict[str, str]], x_field: str, y_field: str, series_field: str):
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        value = as_float(row.get(y_field))
        if value is not None and row.get("valid", "1") == "1":
            grouped[(row[x_field], row[series_field])].append(value)
    out = []
    for (x, series), values in grouped.items():
        values = sorted(values)
        out.append(
            {
                "x": x,
                "series": series,
                "median": median(values),
                "mean": mean(values),
                "min": min(values),
                "max": max(values),
                "p95": values[min(len(values) - 1, math.ceil(0.95 * len(values)) - 1)],
            }
        )
    return out


def svg_line_chart(path: Path, title: str, x_label: str, y_label: str, points: list[dict[str, object]]):
    width, height = 900, 520
    left, right, top, bottom = 80, 30, 45, 80
    plot_w = width - left - right
    plot_h = height - top - bottom
    colors = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#17becf"]
    xs = sorted({str(p["x"]) for p in points}, key=lambda v: float(v) if v.replace(".", "", 1).isdigit() else v)
    series = sorted({str(p["series"]) for p in points})
    ymax = max([float(p["median"]) for p in points] or [1.0])
    ymax = ymax * 1.10 if ymax > 0 else 1.0

    def x_pos(x: str) -> float:
        if len(xs) == 1:
            return left + plot_w / 2
        return left + xs.index(x) * plot_w / (len(xs) - 1)

    def y_pos(y: float) -> float:
        return top + plot_h - (y / ymax) * plot_h

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="25" text-anchor="middle" font-family="sans-serif" font-size="18">{title}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#222"/>',
        f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="#222"/>',
        f'<text x="{width/2}" y="{height-20}" text-anchor="middle" font-family="sans-serif" font-size="13">{x_label}</text>',
        f'<text x="18" y="{height/2}" text-anchor="middle" transform="rotate(-90 18 {height/2})" font-family="sans-serif" font-size="13">{y_label}</text>',
    ]
    for tick in range(6):
        val = ymax * tick / 5
        y = y_pos(val)
        elements.append(f'<line x1="{left-4}" y1="{y:.1f}" x2="{left}" y2="{y:.1f}" stroke="#222"/>')
        elements.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11">{val:.0f}</text>')
    for x in xs:
        xp = x_pos(x)
        elements.append(f'<line x1="{xp:.1f}" y1="{top+plot_h}" x2="{xp:.1f}" y2="{top+plot_h+4}" stroke="#222"/>')
        elements.append(f'<text x="{xp:.1f}" y="{top+plot_h+22}" text-anchor="middle" font-family="sans-serif" font-size="11">{x}</text>')
    by_series: dict[str, list[dict[str, object]]] = defaultdict(list)
    for p in points:
        by_series[str(p["series"])].append(p)
    for idx, name in enumerate(series):
        color = colors[idx % len(colors)]
        pts = sorted(by_series[name], key=lambda p: xs.index(str(p["x"])))
        coords = " ".join(f'{x_pos(str(p["x"])):.1f},{y_pos(float(p["median"])):.1f}' for p in pts)
        elements.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{coords}"/>')
        for p in pts:
            elements.append(f'<circle cx="{x_pos(str(p["x"])):.1f}" cy="{y_pos(float(p["median"])):.1f}" r="4" fill="{color}"/>')
        lx, ly = left + 10, top + 18 + idx * 18
        elements.append(f'<rect x="{lx}" y="{ly-10}" width="12" height="3" fill="{color}"/>')
        elements.append(f'<text x="{lx+18}" y="{ly-6}" font-family="sans-serif" font-size="12">{name}</text>')
    elements.append("</svg>")
    path.write_text("\n".join(elements) + "\n")


def svg_bar_chart(path: Path, title: str, x_label: str, y_label: str, points: list[dict[str, object]]):
    width, height = 900, 520
    left, right, top, bottom = 80, 30, 45, 80
    plot_w = width - left - right
    plot_h = height - top - bottom
    colors = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#17becf"]
    xs = sorted({str(p["x"]) for p in points}, key=lambda v: float(v) if v.replace(".", "", 1).isdigit() else v)
    series = sorted({str(p["series"]) for p in points})
    ymax = max([float(p["median"]) for p in points] or [1.0])
    ymax = ymax * 1.10 if ymax > 0 else 1.0

    def x_pos(x: str) -> float:
        return left + (xs.index(x) + 0.5) * plot_w / max(1, len(xs))

    def y_pos(y: float) -> float:
        return top + plot_h - (y / ymax) * plot_h

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="25" text-anchor="middle" font-family="sans-serif" font-size="18">{title}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#222"/>',
        f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="#222"/>',
        f'<text x="{width/2}" y="{height-20}" text-anchor="middle" font-family="sans-serif" font-size="13">{x_label}</text>',
        f'<text x="18" y="{height/2}" text-anchor="middle" transform="rotate(-90 18 {height/2})" font-family="sans-serif" font-size="13">{y_label}</text>',
    ]
    for tick in range(6):
        val = ymax * tick / 5
        y = y_pos(val)
        elements.append(f'<line x1="{left-4}" y1="{y:.1f}" x2="{left}" y2="{y:.1f}" stroke="#222"/>')
        elements.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11">{val:.0f}</text>')

    group_width = plot_w / max(1, len(xs)) * 0.7
    bar_width = group_width / max(1, len(series))

    for x in xs:
        xp = x_pos(x)
        elements.append(f'<line x1="{xp:.1f}" y1="{top+plot_h}" x2="{xp:.1f}" y2="{top+plot_h+4}" stroke="#222"/>')
        elements.append(f'<text x="{xp:.1f}" y="{top+plot_h+22}" text-anchor="middle" font-family="sans-serif" font-size="11">{x}</text>')

    by_series: dict[str, list[dict[str, object]]] = defaultdict(list)
    for p in points:
        by_series[str(p["series"])].append(p)

    for s_idx, name in enumerate(series):
        color = colors[s_idx % len(colors)]
        pts = sorted(by_series[name], key=lambda p: xs.index(str(p["x"])))
        for p in pts:
            xp = x_pos(str(p["x"])) - group_width / 2 + s_idx * bar_width
            y = y_pos(float(p["median"]))
            h = top + plot_h - y
            elements.append(f'<rect x="{xp:.1f}" y="{y:.1f}" width="{bar_width-1:.1f}" height="{h:.1f}" fill="{color}"/>')

        lx, ly = left + 10, top + 18 + s_idx * 18
        elements.append(f'<rect x="{lx}" y="{ly-10}" width="12" height="12" fill="{color}"/>')
        elements.append(f'<text x="{lx+18}" y="{ly}" font-family="sans-serif" font-size="12">{name}</text>')
    elements.append("</svg>")
    path.write_text("\n".join(elements) + "\n")


def plot_all(result_dir: Path):
    config_path = result_dir / "config.json"
    if not config_path.exists():
        raise RuntimeError("missing config.json; refusing to plot unversioned benchmark output")
    config = json.loads(config_path.read_text())
    if int(config.get("benchmark_schema_version", 0)) < 2:
        raise RuntimeError("pre-v2 benchmark results are invalid for plotting")

    rows = read_csv(result_dir / "runs.csv")
    plots = result_dir / "plots"
    plots.mkdir(exist_ok=True)

    fig12 = [r for r in rows if r.get("experiment") == "figure12"]
    svg_bar_chart(
        plots / "figure12_paper_total.svg",
        "Figure 12-style paper total",
        "tuple_count",
        "median ms",
        aggregate(fig12, "tuple_count", "paper_style_total_ms", "method"),
    )
    svg_bar_chart(
        plots / "figure12_recovery_only.svg",
        "Figure 12-style recovery-only",
        "tuple_count",
        "median ms",
        aggregate(fig12, "tuple_count", "restore_repair_ms", "method"),
    )

    fig13 = [r for r in rows if r.get("experiment") == "figure13" and r.get("method") == "merkle"]
    svg_line_chart(
        plots / "figure13_merkle_time.svg",
        "Figure 13-style Merkle recovery time",
        "bad_leaf_count",
        "median ms",
        aggregate(fig13, "bad_leaf_count", "restore_repair_ms", "partitions"),
    )
    svg_line_chart(
        plots / "figure13_candidate_rows.svg",
        "Candidate rows fetched",
        "bad_leaf_count",
        "median candidate rows",
        aggregate(fig13, "bad_leaf_count", "candidate_rows_fetched", "partitions"),
    )

    phase_rows = read_csv(result_dir / "phase_timings.csv")
    phase_points = []
    wanted = {
        "tree_localisation_ms": "localise",
        "candidate_row_fetch_ms": "fetch",
        "row_comparison_ms": "compare",
        "repair_write_ms": "write",
        "targeted_post_repair_confirmation_ms": "confirm",
        "audit_exact_table_compare_ms": "audit",
    }
    by_phase: dict[str, list[float]] = defaultdict(list)
    for row in phase_rows:
        if row.get("method") == "merkle" and row.get("phase") in wanted:
            value = as_float(row.get("ms"))
            if value is not None:
                by_phase[wanted[row["phase"]]].append(value)
    for phase, values in by_phase.items():
        phase_points.append({"x": phase, "series": "merkle", "median": median(values)})
    svg_line_chart(
        plots / "merkle_phase_breakdown.svg",
        "Merkle phase breakdown",
        "phase",
        "median ms",
        phase_points,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    plot_all(args.result_dir)
