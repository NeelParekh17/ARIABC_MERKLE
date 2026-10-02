#!/usr/bin/env python3
"""Paper figures for the 100M-row out-of-core YCSB results (Final_Results/OOM_100M).

Defaults to the fresh v2 campaign (pg SERIALIZABLE, det, synchronous Merkle).
--campaign previous regenerates the historical split-32/split-1024 figures.
The historical split-1024 campaign has no C cases; missing points stay absent.
--superseded optionally overlays an archived pg summary without retry jitter.
"""
import argparse
import csv
import glob
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

WORKERS = [1, 4, 8, 16]
COMBOS = [("a", "0.0"), ("a", "0.99"), ("a", "1.2"), ("b", "0.99"),
          ("c", "0.99"), ("d", "0.99"), ("f", "0.99")]
MIX = {"a": "50% read / 50% update", "b": "95% read / 5% update", "c": "100% read",
       "d": "95% read-latest / 5% insert", "f": "50% read / 50% read-modify-write"}

# Colour follows the entity in every figure (validated categorical slots 1-4).
# Aqua and yellow are below 3:1 on the light surface, so every line also has a
# distinct marker, a direct end label and a legend entry.
SERIES = {
    "pg_ser": dict(label="pg SERIALIZABLE", color="#2a78d6", marker="o"),
    "pg_rc": dict(label="pg READ COMMITTED", color="#eb6834", marker="s"),
    "det": dict(label="det", color="#1baf7a", marker="^"),
    "merkle": dict(label="det + Merkle (split 32, published)", color="#eda100", marker="D"),
    "merkle_s1024": dict(label="det + Merkle (split 1024, new)", color="#9348aa", marker="s"),
    "pg_old": dict(label="pg SERIALIZABLE, no retry jitter (superseded)", color="#8a8984",
                   marker="x", linestyle=(0, (4, 3))),
}
TEXT, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def load(pattern, mode, campaign="previous"):
    rows = {}
    for path in glob.glob(pattern):
        with open(path) as handle:
            for r in csv.DictReader(handle):
                selected = (r.get("campaign") == "v2_20261001") if campaign == "v2" else \
                           (r.get("campaign") != "v2_20261001")
                if r["mode"] == mode and selected:
                    identity = (r["workload"], str(float(r["skew"])), int(r["workers"]))
                    if identity in rows:
                        raise ValueError(f"Multiple rows for {mode} / {identity}; aggregate trials explicitly")
                    rows[identity] = r
    return rows


SUMMARY_MODE = {"pg_ser": "pg", "pg_rc": "pg_rc", "det": "bcdb_det", "merkle": "bcdb_merkle",
                "merkle_s1024": "bcdb_merkle_s1024"}


def old_retries(row):
    log = Path(row["artifact_dir"]) / "server.log"
    match = re.search(r"retry_attempts_total=(\d+)", log.read_text(errors="replace")) if log.is_file() else None
    return match.group(1) if match else ""


def style(ax, tps=True):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8.5, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xscale("log", base=2)
    ax.set_xticks(WORKERS)
    ax.set_xticklabels([str(w) for w in WORKERS])
    ax.set_xlim(0.85, 19)
    if tps:
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
            lambda v, _: f"{v / 1000:g}k" if v >= 1000 else f"{v:g}"))


def title(wl, skew):
    return f"YCSB-{wl.upper()}  θ={skew}  ({MIX[wl]})"


def plot_combo(ax, data, wl, skew, keys, labels=True, v2=False):
    peak = 0.0
    for key in keys:
        pts = [(w, float(data[key][(wl, skew, w)]["tps"])) for w in WORKERS
               if (wl, skew, w) in data[key]]
        if not pts:
            continue
        peak = max(peak, max(p[1] for p in pts))
        s = SERIES[key]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=s["color"], marker=s["marker"],
                markersize=5 if key != "pg_old" else 6, linewidth=2 if key != "pg_old" else 1.3,
                linestyle=s.get("linestyle", "-"), markeredgecolor=SURFACE, markeredgewidth=1.2,
                label=s["label"], zorder=3 if key != "pg_old" else 2, clip_on=False)
    # Autoscaling is off once limits are set, so size the axis from the data.
    ax.set_ylim(0, peak * 1.1)
    if labels:
        # Direct end labels, nudged apart so they never overlap.
        ends = sorted(((float(data[k][(wl, skew, 16)]["tps"]), k) for k in keys
                       if (wl, skew, 16) in data[k] and k != "pg_old"), reverse=True)
        top = ax.get_ylim()[1]
        placed = []
        for value, key in ends:
            y = value
            for p in placed:
                if abs(p - y) < top * 0.06:
                    y = p - top * 0.06
            placed.append(y)
            short = {"pg_ser": "pg SER", "pg_rc": "pg RC", "det": "det",
                     "merkle": "Merkle" if v2 else "Merkle split 32",
                     "merkle_s1024": "Merkle split 1024"}
            ax.annotate(short[key],
                        xy=(16, value), xytext=(17.2, y), textcoords="data", va="center",
                        fontsize=8, color=TEXT, annotation_clip=False)


def plot_merkle_io(data, out, v2=False):
    """Use matched v2 det, or historical det for the previous geometries."""
    combos = [(wl, skew) for wl, skew in COMBOS
              if v2 or any((wl, skew, w) in data["merkle_s1024"] for w in WORKERS)]
    fig, axes = plt.subplots(3, len(combos), figsize=(3.2 * len(combos), 9.5), squeeze=False)
    rows = []
    for col, (wl, skew) in enumerate(combos):
        for row in range(3):
            style(axes[row, col], tps=False)
        axes[0, col].set_title(f"YCSB-{wl.upper()}  θ={skew}", loc="left", fontsize=10)
        axes[0, col].axhline(1, color=MUTED, linewidth=1, linestyle="--")
        for key in (("pg_ser", "det", "merkle") if v2 else ("det", "merkle", "merkle_s1024")):
            s = SERIES[key]
            points = []
            for w in WORKERS:
                r = data[key].get((wl, skew, w))
                if r is None:
                    continue
                det = data["det"][(wl, skew, w)]
                ratio = float(r["tps"]) / float(det["tps"])
                read = float(r["device_read_mib"]) * 2**20 / (1000 * int(r["total_queries"]))
                write = float(r["device_write_mib"]) * 2**20 / (1000 * int(r["total_queries"]))
                points.append((w, ratio, read, write))
                rows.append([wl, skew, w, r["mode"], r["tps"], det["tps"], ratio, read, write])
            for row, metric in enumerate((1, 2, 3)):
                if row == 0 and key in ("det", "pg_ser"):
                    continue
                ax = axes[row, col]
                ax.plot([p[0] for p in points], [p[metric] for p in points],
                        color=s["color"], marker=s["marker"], linewidth=2, markersize=5,
                        label=s["label"])
        # Set limits after all series; an early set_ylim disables autoscaling.
        for row in range(3):
            ax = axes[row, col]
            ax.relim()
            ax.autoscale(enable=True, axis="y")
            ax.set_ylim(0, ax.get_ylim()[1] * 1.07)
        axes[2, col].set_xlabel("Workers", fontsize=9)
    for row, label in enumerate(("Merkle TPS / v2 det TPS" if v2 else "Merkle TPS / published det TPS", "Device read KB / statement",
                                 "Device write KB / statement")):
        axes[row, 0].set_ylabel(label, fontsize=9)
    handles, labels = axes[1, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, fontsize=10)
    caption = ("Fresh v2: same fillfactor-90 heap, split 1024 / merge 256, synchronous Merkle. " if v2 else
               "Split 1024 includes optimized code and a rebuilt/compacted baseline. ")
    fig.text(0.5, 0.01, caption + "Single trial. KB = 1000 bytes; timed device I/O excludes checkpoint and verification.",
             ha="center", fontsize=9, color=MUTED)
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    fig.savefig(out / "oom_merkle_ratios_io.png", dpi=200)
    plt.close(fig)
    with (out / "oom_merkle_ratios_io.csv").open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["workload", "skew", "workers", "mode", "tps", "v2_det_tps" if v2 else "published_det_tps",
                         "relative_to_v2_det" if v2 else "relative_to_published_det", "device_read_kb_per_statement",
                         "device_write_kb_per_statement"])
        writer.writerows(rows)


def plot_campaign_comparison(summary, current, out):
    """Explicitly distinguish datasets/configurations and denominator provenance."""
    previous = {key: load(summary, mode, "previous") for key, mode in SUMMARY_MODE.items()}
    campaigns = [("Published Sept 29: split 32, ff100", previous, "merkle", "#eda100", "D"),
                 ("Oct 1 rerun: split 1024; Sept det/pg bases", previous, "merkle_s1024", "#9348aa", "s"),
                 ("Fresh v2: split 1024, ff90, matched det/pg", current, "merkle", "#1baf7a", "^")]
    fig, axes = plt.subplots(2, len(COMBOS), figsize=(21, 6.5), squeeze=False)
    records = []
    for col, (wl, skew) in enumerate(COMBOS):
        axes[0, col].set_title(f"YCSB-{wl.upper()}  θ={skew}", loc="left", fontsize=10)
        for row, base_key in enumerate(("det", "pg_ser")):
            ax = axes[row, col]
            style(ax, tps=False)
            ax.axhline(1, color=MUTED, linewidth=1, linestyle="--")
            for label, dataset, key, color, marker in campaigns:
                points = []
                for w in WORKERS:
                    point = (wl, skew, w)
                    if point not in dataset[key]:
                        continue
                    numerator = dataset[key][point]
                    base = dataset[base_key][point]
                    ratio = float(numerator["tps"]) / float(base["tps"])
                    points.append((w, ratio))
                    records.append([label, wl, skew, w, base_key, numerator["tps"], base["tps"], ratio])
                ax.plot([p[0] for p in points], [p[1] for p in points], color=color, marker=marker,
                        linewidth=2, markersize=5, label=label)
            ax.set_ylim(0, max(1.12, ax.get_ylim()[1] * 1.05))
        axes[1, col].set_xlabel("Workers", fontsize=9)
    axes[0, 0].set_ylabel("Merkle / det TPS", fontsize=10)
    axes[1, 0].set_ylabel("Merkle / pg SERIALIZABLE TPS", fontsize=10)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, fontsize=10)
    fig.text(0.5, 0.015, "Different datasets, configurations and builds; one trial per point. "
             "Oct 1 rerun has no C measurement and uses historical det/pg denominators; ratios do not isolate an optimization.",
             ha="center", fontsize=9, color=MUTED)
    fig.tight_layout(rect=(0, 0.055, 1, 0.93))
    fig.savefig(out / "v2_vs_published_merkle_ratio.png", dpi=200)
    plt.close(fig)
    with (out / "v2_vs_published_merkle_ratio.csv").open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["configuration", "workload", "skew", "workers", "denominator_series",
                         "merkle_tps", "denominator_tps", "ratio"])
        writer.writerows(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path("Final_Results/OOM_100M")
    parser.add_argument("--summary", default=str(root / "summary.csv"))
    parser.add_argument("--superseded", default=None,
                        help="Optional summary.csv of pg without retry jitter, drawn as a dashed grey line")
    parser.add_argument("--out", default=str(root / "figures"))
    parser.add_argument("--campaign", choices=("v2", "previous"), default="v2",
                        help="Select fresh v2 (default), or preserved historical campaigns")
    parser.add_argument("--include-rc", action="store_true",
                        help="Also plot pg READ COMMITTED (omitted from the paper figures)")
    args = parser.parse_args()
    v2 = args.campaign == "v2"
    if v2 and (args.include_rc or args.superseded):
        parser.error("READ COMMITTED and superseded overlays are available only with --campaign previous")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    data = {key: load(args.summary, mode, args.campaign) for key, mode in SUMMARY_MODE.items()}
    data["pg_old"] = load(args.superseded, "pg") if args.superseded else {}
    plotted = ["pg_ser"] + (["pg_rc"] if args.include_rc else []) + ["det", "merkle"] + \
              ([] if v2 else ["merkle_s1024"])
    if v2:
        SERIES["merkle"]["label"] = "det + Merkle (v2, split 1024)"
    if not args.include_rc:
        data["pg_rc"] = {}
    for key in plotted:
        if not data[key]:
            raise SystemExit(f"No rows for {key} in {args.summary}")
        if v2 and (len(data[key]) != 28 or any(r["isolation"] != "serializable" for r in data[key].values())):
            raise SystemExit(f"Expected 28 SERIALIZABLE v2 rows for {key}")

    plt.rcParams.update({"font.family": "DejaVu Sans", "figure.facecolor": SURFACE,
                         "savefig.facecolor": SURFACE})
    contended = {("a", "0.99"), ("a", "1.2"), ("f", "0.99")} if data["pg_old"] else set()

    # 1. One figure per workload/skew.
    for wl, skew in COMBOS:
        fig, ax = plt.subplots(figsize=(8.5, 4.7))
        style(ax)
        keys = plotted + (["pg_old"] if (wl, skew) in contended else [])
        plot_combo(ax, data, wl, skew, keys, labels=True, v2=v2)
        ax.set_title(title(wl, skew), loc="left", fontsize=10.5, color=TEXT)
        ax.set_xlabel("Workers (pg connections / det worker threads)", fontsize=9, color=MUTED)
        ax.set_ylabel("Throughput (statements/s)", fontsize=9, color=MUTED)
        ax.legend(frameon=False, fontsize=8, loc="upper left", labelcolor=TEXT)
        if wl == "c" and not v2:
            ax.text(0.02, 0.52, "Split 1024: not measured", transform=ax.transAxes,
                    fontsize=8, color=MUTED)
        fig.subplots_adjust(right=0.80)
        fig.savefig(out / f"scaling_{wl}_{skew}.png", dpi=200)
        plt.close(fig)

    # 2. All combinations as small multiples, one shared legend.
    fig, axes = plt.subplots(2, 4, figsize=(15, 6.6))
    keys = plotted
    for ax, (wl, skew) in zip(axes.flat, COMBOS):
        style(ax)
        plot_combo(ax, data, wl, skew, keys, labels=False, v2=v2)
        ax.set_xlim(0.85, 17.5)
        ax.set_title(f"YCSB-{wl.upper()}  θ={skew}\n{MIX[wl]}", loc="left", fontsize=9.5, color=TEXT)
        if wl == "c" and not v2:
            ax.text(0.02, 0.52, "Split 1024: not measured", transform=ax.transAxes,
                    fontsize=8, color=MUTED)
    legend_ax = axes.flat[-1]
    legend_ax.axis("off")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    legend_ax.legend(handles, labels, frameon=False, loc="center left", fontsize=8.5, labelcolor=TEXT,
                     title="100M rows, 32 MB shared_buffers,\ncold start, 20,000 statements" +
                           ("\nFresh v2, fillfactor 90, one trial" if v2 else ""),
                     title_fontsize=8.5)
    for ax in axes[:, 0]:
        ax.set_ylabel("Throughput (statements/s)", fontsize=9, color=MUTED)
    for ax in axes[1, :3]:
        ax.set_xlabel("Workers", fontsize=9, color=MUTED)
    axes[0, 3].set_xlabel("Workers", fontsize=9, color=MUTED)
    fig.tight_layout()
    fig.savefig(out / "oom_scaling_all.png", dpi=200)
    plt.close(fig)

    # 3. Throughput relative to pg SERIALIZABLE at 1 and 16 workers.
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.9), sharey=True)
    for ax, w in zip(axes, (1, 16)):
        ax.set_facecolor(SURFACE)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.spines["left"].set_color(GRID); ax.spines["bottom"].set_color(GRID)
        ax.tick_params(colors=MUTED, labelsize=8.5, length=0)
        ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
        bar_keys = [k for k in plotted if k != "pg_ser"]
        width = 0.36 if len(bar_keys) == 2 else 0.26
        for i, key in enumerate(bar_keys):
            xs, ys = [], []
            for j, (wl, skew) in enumerate(COMBOS):
                base = float(data["pg_ser"][(wl, skew, w)]["tps"])
                if (wl, skew, w) in data[key]:
                    xs.append(j + (i - (len(bar_keys) - 1) / 2) * (width + 0.02))
                    ys.append(float(data[key][(wl, skew, w)]["tps"]) / base)
            ax.bar(xs, ys, width=width, color=SERIES[key]["color"], label=SERIES[key]["label"],
                   edgecolor=SURFACE, linewidth=1)
            if key != "pg_rc":   # label the subjects only; the CSV table has every value
                for x, y in zip(xs, ys):
                    ax.text(x, y + 0.015, f"{y:.2f}", ha="center", va="bottom", fontsize=7, color=MUTED,
                            zorder=4, bbox=dict(boxstyle="square,pad=0.08", facecolor=SURFACE, edgecolor="none"))
        ax.axhline(1.0, color=SERIES["pg_ser"]["color"], linewidth=1.3, linestyle=(0, (4, 3)),
                   label="pg SERIALIZABLE (= 1.0)", zorder=1)
        ax.set_xticks(range(len(COMBOS)))
        ax.set_xticklabels([f"{wl.upper()} θ{skew}" for wl, skew in COMBOS], fontsize=8.5, color=TEXT)
        ax.set_title(f"{w} worker{'s' if w > 1 else ''}", loc="left", fontsize=10, color=TEXT)
        ax.set_ylim(0, max(1.3, ax.get_ylim()[1] * 1.08))
    axes[0].set_ylabel("Throughput relative to pg SERIALIZABLE", fontsize=9, color=MUTED)
    handles, labels = axes[1].get_legend_handles_labels()
    order = [labels.index(l) for l in ["pg SERIALIZABLE (= 1.0)"] +
             [SERIES[k]["label"] for k in plotted if k != "pg_ser"]]
    fig.legend([handles[i] for i in order], [labels[i] for i in order], frameon=False, fontsize=8.5,
               loc="upper center", ncol=len(order), labelcolor=TEXT, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(out / "oom_relative_to_pg.png", dpi=200)
    plt.close(fig)

    # Table view of every plotted value.
    shown = [k for k in SERIES if data[k]]
    fields = ["workload", "skew", "workers"] + [SERIES[k]["label"] + " (TPS)" for k in shown] + \
             ["pg SERIALIZABLE retries"] + (["pg no-jitter retries"] if data["pg_old"] else [])
    if v2:
        fields += ["campaign", "fillfactor", "split_threshold", "Merkle/det", "Merkle/pg", "det/pg"]
        fields += [f"{k} device {direction} KB/statement" for k in plotted for direction in ("read", "write")]
    with open(out / "oom_tps_table.csv", "w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(fields)
        for wl, skew in COMBOS:
            for w in WORKERS:
                row = [wl, skew, w]
                for k in shown:
                    r = data[k].get((wl, skew, w))
                    row.append(r["tps"] if r else "")
                row.append(data["pg_ser"][(wl, skew, w)].get("retry_attempts", ""))
                if data["pg_old"]:
                    old = data["pg_old"].get((wl, skew, w))
                    row.append(old_retries(old) if old else "")
                if v2:
                    pg, det, merkle = [float(data[k][(wl, skew, w)]["tps"]) for k in plotted]
                    row.extend(["v2_20261001", 90, 1024, merkle / det, merkle / pg, det / pg])
                    row.extend(float(data[k][(wl, skew, w)][f"device_{direction}_mib"]) * 2**20 /
                               (1000 * int(data[k][(wl, skew, w)]["total_queries"]))
                               for k in plotted for direction in ("read", "write"))
                writer.writerow(row)
    plot_merkle_io(data, out, v2=v2)
    if v2:
        plot_campaign_comparison(args.summary, data, out)
    print(f"Wrote figures and oom_tps_table.csv to {out}")


if __name__ == "__main__":
    main()
