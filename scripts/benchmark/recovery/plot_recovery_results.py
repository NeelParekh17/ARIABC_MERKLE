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

    fig12 = [
        r for r in rows
        if r.get("experiment") in ("figure12", "recovery-scaling-diagnosis", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300", "dynamic-size-scaling-k75-c300")
    ]
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

    run_map = {r["run_id"]: r for r in rows if "run_id" in r}
    phase_rows = read_csv(result_dir / "phase_timings.csv")
    dataset_rows = read_csv(result_dir / "dataset_sizes.csv")
    backend_rows = read_csv(result_dir / "merkle_backend_profile.csv")
    deep_rows = read_csv(result_dir / "deep_plan_summary.csv")

    def median_points_from_join(
        left_rows: list[dict[str, str]],
        x_field: str,
        series_field: str,
        y_field: str,
    ) -> list[dict[str, object]]:
        grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
        for row in left_rows:
            y = as_float(row.get(y_field))
            if y is None:
                continue
            grouped[(row.get(x_field, ""), row.get(series_field, ""))].append(y)
        out = []
        for (x, series), values in grouped.items():
            out.append({"x": x, "series": series, "median": median(values)})
        return out

    # Recovery breakdown by dataset size.
    breakdown = defaultdict(list)
    phase_label_map = {
        "tree_localisation_ms": "localisation",
        "candidate_row_fetch_ms": "candidate fetch",
        "row_comparison_ms": "row compare",
        "repair_write_ms": "repair write",
        "native_commit_visibility_ms": "native visibility",
        "global_merkle_queue_drain_ms": "compat queue drain",
        "post_queue_relocalisation_ms": "post-queue check",
    }
    for row in phase_rows:
        run = run_map.get(row.get("run_id"))
        if not run:
            continue
        phase = row.get("phase")
        if phase in phase_label_map:
            value = as_float(row.get("ms"))
            if value is not None:
                series = f"{run.get('profile_label', '')}:{phase_label_map[phase]}"
                breakdown[(run.get("tuple_count", ""), series)].append(value)
    breakdown_points = [
        {"x": x, "series": series, "median": median(values)}
        for (x, series), values in breakdown.items()
    ]
    if breakdown_points:
        svg_bar_chart(
            plots / "recovery_breakdown_vs_dataset_size.svg",
            "Recovery breakdown vs dataset size",
            "tuple_count",
            "median ms",
            breakdown_points,
        )

    candidate_points = aggregate(
        [
            r for r in rows
            if r.get("profile_label") in {"baseline_l16", "preprovisioned_l128"}
        ],
        "tuple_count",
        "candidate_rows_fetched",
        "profile_label",
    )
    candidate_points.extend(
        aggregate(
            [r for r in rows if r.get("method") == "merkle_dynamic"],
            "tuple_count",
            "dynamic_candidate_summary_items_fetched",
            "profile_label",
        )
    )
    if candidate_points:
        svg_line_chart(
            plots / "candidate_rows_vs_dataset_size.svg",
            "Candidate rows or dynamic summary items vs dataset size",
            "tuple_count",
            "median items",
            candidate_points,
        )

    recovery_points = aggregate(
        [r for r in rows if r.get("profile_label")],
        "tuple_count",
        "restore_repair_ms",
        "profile_label",
    )
    if recovery_points:
        svg_line_chart(
            plots / "recovery_time_vs_dataset_size_by_geometry.svg",
            "Recovery time vs dataset size by geometry",
            "tuple_count",
            "median ms",
            recovery_points,
        )

    backend_points = []
    if backend_rows:
        metric_map = {
            "root helper": ("root_hash_helper_us", "us"),
            "child helper": ("child_hash_helper_us", "us"),
            "row hash": ("row_hash_compute_ns", "ns"),
            "tree path": ("tree_path_update_ns", "ns"),
        }
        for label, (field, unit) in metric_map.items():
            fallback_field = field.replace("_ns", "_us")
            values = [
                as_float(r.get(field) if r.get(field) not in (None, "") else r.get(fallback_field))
                for r in backend_rows
            ]
            values = [v for v in values if v is not None]
            if values:
                divisor = 1_000_000.0 if unit == "ns" else 1000.0
                backend_points.append({"x": label, "series": "backend", "median": median(values) / divisor})
        if backend_points:
            svg_bar_chart(
                plots / "merkle_backend_time_breakdown.svg",
                "Merkle backend time breakdown",
                "metric",
                "median ms",
                backend_points,
            )

    lookup_points = []
    if deep_rows:
        lookup_rows = [
            r for r in deep_rows
            if r.get("kind") == "candidate" and r.get("schema") in {"healthy", "damaged"}
        ]
        if lookup_rows:
            for metric in ["shared_hit_blocks", "shared_read_blocks", "actual_rows"]:
                vals = defaultdict(list)
                for r in lookup_rows:
                    x = r.get("profile_label", "")
                    v = as_float(r.get(metric))
                    if v is not None:
                        vals[x].append(v)
                for x, series_vals in vals.items():
                    lookup_points.append({"x": x, "series": metric, "median": median(series_vals)})
    if lookup_points:
        svg_bar_chart(
            plots / "candidate_lookup_buffers_vs_geometry.svg",
            "Candidate lookup buffers vs geometry",
            "geometry",
            "median blocks / rows",
            lookup_points,
        )

    occupancy_points = []
    if dataset_rows:
        for metric in ["p50", "p95", "p99", "maximum"]:
            vals = defaultdict(list)
            for r in dataset_rows:
                v = as_float(r.get(metric))
                if v is not None:
                    vals[r.get("profile_label", "")].append(v)
            for x, series_vals in vals.items():
                occupancy_points.append({"x": x, "series": metric, "median": median(series_vals)})
    if occupancy_points:
        svg_bar_chart(
            plots / "leaf_occupancy_vs_geometry.svg",
            "Leaf occupancy vs geometry",
            "geometry",
            "median occupancy",
            occupancy_points,
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

    # ── fanout-width-sweep plots (only generated when sweep data present) ─────
    sweep_rows = [r for r in rows if r.get("experiment") == "fanout-width-sweep"]
    if sweep_rows:
        sweep_label_set = {r.get("profile_label") for r in sweep_rows}

        # Per leaf-count tier: plot restore_repair_ms and tree_localisation_ms vs fanout.
        # Each dict maps a human title suffix to the set of geometry labels in that tier.
        # 6 leaf-count tiers, each with its canonical fanout-sweep labels
        leaf_tiers = {
            "l16":   {"fanout_f2_l16",   "fanout_f4_l16",   "fanout_f16_l16"},
            "l64":   {"fanout_f2_l64",   "fanout_f4_l64",   "fanout_f8_l64", "fanout_f64_l64"},
            "l128":  {"fanout_f2_l128",  "fanout_f128_l128"},
            "l256":  {"fanout_f2_l256",  "fanout_f4_l256",  "fanout_f16_l256", "fanout_f256_l256"},
            "l512":  {"fanout_f2_l512",  "fanout_f512_l512"},
            "l1024": {"fanout_f2_l1024", "fanout_f4_l1024", "fanout_f32_l1024", "fanout_f1024_l1024"},
        }
        leaf_title = {
            "l16":   "L=16  (1,563 rows/leaf @5M)",
            "l64":   "L=64  (391 rows/leaf @5M)",
            "l128":  "L=128 (195 rows/leaf @5M)",
            "l256":  "L=256 (98 rows/leaf @5M)",
            "l512":  "L=512 (49 rows/leaf @5M)",
            "l1024": "L=1024 (24 rows/leaf @5M)",
        }

        for tier_key, tier_labels in leaf_tiers.items():
            tier_rows = [r for r in sweep_rows if r.get("profile_label") in tier_labels]
            if not tier_rows:
                continue
            title_suffix = leaf_title[tier_key]
            svg_line_chart(
                plots / f"fanout_recovery_time_{tier_key}.svg",
                f"Recovery time vs fanout — {title_suffix}",
                "fanout",
                "median restore_repair_ms",
                aggregate(tier_rows, "fanout", "restore_repair_ms", "leaves_per_partition"),
            )
            svg_line_chart(
                plots / f"fanout_localisation_time_{tier_key}.svg",
                f"Localisation time vs fanout — {title_suffix}",
                "fanout",
                "median tree_localisation_ms",
                aggregate(
                    [r for r in rows if r.get("profile_label") in tier_labels],
                    "fanout", "tree_localisation_ms", "leaves_per_partition",
                ),
            )

        # Merkle index size vs fanout across all sweep geometries
        index_points = []
        for row in dataset_rows:
            if row.get("profile_label") in sweep_label_set:
                v = as_float(row.get("merkle_index_bytes"))
                if v is not None:
                    index_points.append({
                        "x": str(row.get("fanout", "")),
                        "series": str(row.get("leaves_per_partition", "")),
                        "median": v / (1024 * 1024),
                    })
        if index_points:
            svg_line_chart(
                plots / "fanout_merkle_index_size_mib.svg",
                "Merkle index size vs fanout (MiB)",
                "fanout",
                "index size MiB",
                index_points,
            )

        # Backend profile plots: child-hash calls/nodes + tree-path nodes vs fanout
        if backend_rows:
            sweep_backend = [r for r in backend_rows if r.get("profile_label") in sweep_label_set]
            if sweep_backend:
                from collections import defaultdict as _defaultdict

                def _backend_line(metric_field: str, title: str, y_label: str, filename: str) -> None:
                    grouped: dict[tuple[str, str], list[float]] = _defaultdict(list)
                    for row in sweep_backend:
                        v = as_float(row.get(metric_field))
                        if v is not None:
                            grouped[(str(row.get("fanout", "")), str(row.get("leaves_per_partition", "")))].append(v)
                    agg = [
                        {"x": x, "series": s, "median": median(vals)}
                        for (x, s), vals in grouped.items()
                    ]
                    if agg:
                        svg_line_chart(plots / filename, title, "fanout", y_label, agg)

                _backend_line(
                    "child_hash_helper_calls",
                    "Child-hash helper calls vs fanout",
                    "median call count",
                    "fanout_child_hash_calls.svg",
                )
                _backend_line(
                    "child_hash_nodes_returned",
                    "Child-hash nodes returned vs fanout",
                    "median node count",
                    "fanout_child_hash_nodes_returned.svg",
                )
                _backend_line(
                    "tree_path_nodes_touched",
                    "Tree-path nodes touched vs fanout",
                    "median node count",
                    "fanout_tree_path_nodes_touched.svg",
                )




if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    plot_all(args.result_dir)
