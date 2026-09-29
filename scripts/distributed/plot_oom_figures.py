#!/usr/bin/env python3
"""Paper figures for the 100M-row out-of-core YCSB results (Final_Results/OOM_100M).

Reads the consolidated summary.csv (det, det+Merkle, pg SERIALIZABLE with retry
jitter, pg READ COMMITTED) and writes figures plus a table of every plotted value.
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
    "merkle": dict(label="det + Merkle", color="#eda100", marker="D"),
    "pg_old": dict(label="pg SERIALIZABLE, no retry jitter (superseded)", color="#8a8984",
                   marker="x", linestyle=(0, (4, 3))),
}
TEXT, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def load(pattern, mode):
    rows = {}
    for path in glob.glob(pattern):
        for r in csv.DictReader(open(path)):
            if r["mode"] == mode:
                rows[(r["workload"], r["skew"], int(r["workers"]))] = r
    return rows


SUMMARY_MODE = {"pg_ser": "pg", "pg_rc": "pg_rc", "det": "bcdb_det", "merkle": "bcdb_merkle"}


def old_retries(row):
    log = Path(row["artifact_dir"]) / "server.log"
    match = re.search(r"retry_attempts_total=(\d+)", log.read_text(errors="replace")) if log.is_file() else None
    return match.group(1) if match else ""


def style(ax):
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
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
        lambda v, _: f"{v / 1000:g}k" if v >= 1000 else f"{v:g}"))


def title(wl, skew):
    return f"YCSB-{wl.upper()}  θ={skew}  ({MIX[wl]})"


def plot_combo(ax, data, wl, skew, keys, labels=True):
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
            ax.annotate(SERIES[key]["label"].replace("pg SERIALIZABLE", "pg SER")
                        .replace("pg READ COMMITTED", "pg RC"),
                        xy=(16, value), xytext=(17.2, y), textcoords="data", va="center",
                        fontsize=8, color=TEXT, annotation_clip=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path("Final_Results/OOM_100M")
    parser.add_argument("--summary", default=str(root / "summary.csv"))
    parser.add_argument("--superseded", default=None,
                        help="Optional summary.csv of pg without retry jitter, drawn as a dashed grey line")
    parser.add_argument("--out", default=str(root / "figures"))
    parser.add_argument("--include-rc", action="store_true",
                        help="Also plot pg READ COMMITTED (omitted from the paper figures)")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    data = {key: load(args.summary, mode) for key, mode in SUMMARY_MODE.items()}
    data["pg_old"] = load(args.superseded, "pg") if args.superseded else {}
    plotted = ["pg_ser"] + (["pg_rc"] if args.include_rc else []) + ["det", "merkle"]
    if not args.include_rc:
        data["pg_rc"] = {}
    for key in plotted:
        if not data[key]:
            raise SystemExit(f"No rows for {key} in {args.summary}")

    plt.rcParams.update({"font.family": "DejaVu Sans", "figure.facecolor": SURFACE,
                         "savefig.facecolor": SURFACE})
    contended = {("a", "0.99"), ("a", "1.2"), ("f", "0.99")} if data["pg_old"] else set()

    # 1. One figure per workload/skew.
    for wl, skew in COMBOS:
        fig, ax = plt.subplots(figsize=(6.4, 4.0))
        style(ax)
        keys = plotted + (["pg_old"] if (wl, skew) in contended else [])
        plot_combo(ax, data, wl, skew, keys, labels=True)
        ax.set_title(title(wl, skew), loc="left", fontsize=10.5, color=TEXT)
        ax.set_xlabel("Workers (pg connections / det worker threads)", fontsize=9, color=MUTED)
        ax.set_ylabel("Throughput (statements/s)", fontsize=9, color=MUTED)
        ax.legend(frameon=False, fontsize=8, loc="upper left", labelcolor=TEXT)
        fig.subplots_adjust(right=0.80)
        fig.savefig(out / f"scaling_{wl}_{skew}.png", dpi=200)
        plt.close(fig)

    # 2. All combinations as small multiples, one shared legend.
    fig, axes = plt.subplots(2, 4, figsize=(15, 6.6))
    keys = plotted
    for ax, (wl, skew) in zip(axes.flat, COMBOS):
        style(ax)
        plot_combo(ax, data, wl, skew, keys, labels=False)
        ax.set_xlim(0.85, 17.5)
        ax.set_title(f"YCSB-{wl.upper()}  θ={skew}\n{MIX[wl]}", loc="left", fontsize=9.5, color=TEXT)
    legend_ax = axes.flat[-1]
    legend_ax.axis("off")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    legend_ax.legend(handles, labels, frameon=False, loc="center left", fontsize=10, labelcolor=TEXT,
                     title="100M rows, 32 MB shared_buffers,\ncold start, 20,000 statements",
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
        ax.set_ylim(0, 1.3)
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
    with open(out / "oom_tps_table.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        for wl, skew in COMBOS:
            for w in WORKERS:
                row = [wl, skew, w]
                for k in shown:
                    r = data[k].get((wl, skew, w))
                    row.append(f"{float(r['tps']):.0f}" if r else "")
                row.append(data["pg_ser"][(wl, skew, w)].get("retry_attempts", ""))
                if data["pg_old"]:
                    old = data["pg_old"].get((wl, skew, w))
                    row.append(old_retries(old) if old else "")
                writer.writerow(row)
    print(f"Wrote figures and oom_tps_table.csv to {out}")


if __name__ == "__main__":
    main()
