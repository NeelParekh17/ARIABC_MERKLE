#!/usr/bin/env python3
"""Plot the audited paper-YCSB matrix without assigning TPS to rejected cases."""

import argparse
import csv
import hashlib
import html
import json
import math
from pathlib import Path


MODES = ("pg", "bcdb_det", "bcdb_merkle", "cluster")
LABELS = {"pg": "PG", "bcdb_det": "DET", "bcdb_merkle": "DET + Merkle", "cluster": "Cluster"}
COLORS = {"pg": "#2563eb", "bcdb_det": "#059669", "bcdb_merkle": "#d97706", "cluster": "#7c3aed"}
MARKERS = {"pg": "o", "bcdb_det": "s", "bcdb_merkle": "^", "cluster": "D"}
WORKERS = (1, 4, 8, 16)
SKEWS = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
NOTE = "One trial per point; 20,000 transactions per run, 10 operations per transaction."


def load_matrix(path):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    matrix = {}
    for row in rows:
        key = (row["mode"], int(row["workers"]), float(row["skew"]))
        if key in matrix:
            raise ValueError("Duplicate matrix point: " + str(key))
        if row["smoke"] != "False" or int(row["trial"]) != 1:
            raise ValueError("Plots require measured, single-trial rows")
        if int(row["transactions"]) != 20000 or int(row["internal_operations"]) != 200000:
            raise ValueError("Unexpected workload size")
        if row["status"] == "passed":
            row["plot_tps"] = float(row["tps"])
            if not math.isfinite(row["plot_tps"]) or row["plot_tps"] <= 0:
                raise ValueError("Accepted TPS must be positive and finite")
            if row["divergence_count"] != "0" or row["permanent_failures"] != "0":
                raise ValueError("Accepted case contains failures")
        elif row["status"] == "rejected":
            if row["tps"] or row["mode"] != "pg" or row["rejection_reason"] != "result_wait_timeout":
                raise ValueError("Rejected case must retain blank TPS and its actual timeout reason")
            row["plot_tps"] = math.nan
        else:
            raise ValueError("Unknown result status")
        matrix[key] = row
    expected = {(mode, workers, skew) for mode in MODES for workers in WORKERS for skew in SKEWS}
    if set(matrix) != expected:
        raise ValueError("CSV does not contain exactly the 96 requested points")
    if len({row["source_fingerprint"] for row in rows}) != 1:
        raise ValueError("Matrix mixes source identities")
    return rows, matrix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    rows, matrix = load_matrix(args.csv)
    if args.out.exists():
        parser.error("Use a fresh plot directory; existing artifacts are not overwritten")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.colors import Normalize
    from matplotlib.patches import Patch
    from matplotlib.ticker import StrMethodFormatter
    import numpy as np

    args.out.mkdir(parents=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "svg.fonttype": "none"})
    catalog = []
    pdf_path = args.out / "paper_ycsb_figures.pdf"
    maximum = max(r["plot_tps"] for r in rows if r["status"] == "passed")

    def footer(fig, extra=""):
        fig.text(.5, .045, NOTE, ha="center", color="#475569", fontsize=9)
        if extra:
            fig.text(.5, .02, extra, ha="center", color="#475569", fontsize=9)

    def style(ax):
        ax.grid(axis="y", color="#e2e8f0", linewidth=.8)
        ax.set_axisbelow(True)
        ax.set_ylim(bottom=0)
        ax.tick_params(axis="x", labelbottom=True)
        ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    def lines(ax, x, keys):
        for mode in MODES:
            ax.plot(x, [matrix[(mode, workers, skew)]["plot_tps"] for workers, skew in keys],
                    label=LABELS[mode], color=COLORS[mode], marker=MARKERS[mode],
                    markersize=6, linewidth=2)
        style(ax)

    def legend(fig, ax, y):
        handles, names = ax.get_legend_handles_labels()
        fig.legend(handles, names, loc="upper center", bbox_to_anchor=(.5, y),
                   ncol=4, frameon=False)

    def save(fig, name, title, description, pdf):
        png = args.out / (name + ".png")
        svg = args.out / (name + ".svg")
        fig.savefig(png, dpi=170, facecolor="white")
        fig.savefig(svg, facecolor="white")
        pdf.savefig(fig, facecolor="white")
        catalog.append(dict(name=name, title=title, description=description,
                            png=png.name, svg=svg.name))
        plt.close(fig)

    with PdfPages(pdf_path) as pdf:
        pdf.infodict().update(Title="Paper YCSB — audited 96-point sweep", Author="AriaBC benchmark artifacts")

        fig, ax = plt.subplots(figsize=(10, 6))
        lines(ax, WORKERS, [(w, .6) for w in WORKERS])
        ax.set_xticks(WORKERS)
        ax.set_xlabel("Executor workers")
        ax.set_ylabel("Transactions / second")
        fig.suptitle("Default skew 0.6: throughput vs. executor workers", fontsize=16, y=.96)
        legend(fig, ax, .90)
        fig.subplots_adjust(top=.80, bottom=.19, left=.10, right=.97)
        footer(fig, "Primary cluster uses saved trusted text receipts; signed check is separate.")
        save(fig, "01_default_skew_workers", "Default skew 0.6", "Four modes with the same 96 gateway clients; executor workers vary.", pdf)

        fig, axes = plt.subplots(2, 2, figsize=(12, 9), sharex=True, sharey=True)
        for ax, workers in zip(axes.flat, WORKERS):
            lines(ax, SKEWS, [(workers, skew) for skew in SKEWS])
            ax.set_title("Executor workers: " + str(workers), pad=16)
            ax.set_xticks(SKEWS)
            ax.set_xlabel("Zipf skew")
            ax.set_ylabel("Transactions / second")
            missing = [f"{s:.1f}" for s in SKEWS if matrix[("pg", workers, s)]["status"] == "rejected"]
            if missing:
                ax.text(.98, .96, "PG timeout: " + ", ".join(missing), transform=ax.transAxes,
                        ha="right", va="top", fontsize=10, color="#b91c1c")
        axes.flat[0].set_ylim(0, maximum * 1.08)
        fig.suptitle("Contention sweep: throughput vs. Zipf skew", fontsize=17, y=.98)
        legend(fig, axes.flat[0], .935)
        fig.subplots_adjust(top=.83, bottom=.14, left=.08, right=.98, hspace=.40, wspace=.20)
        footer(fig, "Missing PG points are 600-second timeouts; no TPS is assigned or interpolated.")
        save(fig, "02_skew_sweep", "Skew sweep at each worker count", "PG lines have gaps at rejected points. All four panels share the same throughput scale.", pdf)

        fig, axes = plt.subplots(2, 3, figsize=(15, 9), sharex=True, sharey=True)
        for ax, skew in zip(axes.flat, SKEWS):
            lines(ax, WORKERS, [(w, skew) for w in WORKERS])
            ax.set_title(f"Zipf skew: {skew:.1f}", pad=16)
            ax.set_xticks(WORKERS)
            ax.set_xlabel("Executor workers")
            ax.set_ylabel("Transactions / second")
            missing = [str(w) for w in WORKERS if matrix[("pg", w, skew)]["status"] == "rejected"]
            if missing:
                ax.text(.98, .96, "PG timeout: workers " + ", ".join(missing), transform=ax.transAxes,
                        ha="right", va="top", fontsize=9, color="#b91c1c")
        axes.flat[0].set_ylim(0, maximum * 1.08)
        fig.suptitle("Worker scaling across all six skews", fontsize=17, y=.98)
        legend(fig, axes.flat[0], .935)
        fig.subplots_adjust(top=.83, bottom=.14, left=.065, right=.98, hspace=.40, wspace=.22)
        footer(fig, "Missing PG points are 600-second timeouts; curves show raw observations without error bars.")
        save(fig, "03_worker_sweep", "Worker scaling at each skew", "Six panels use a common throughput scale and show every accepted point.", pdf)

        fig, axes = plt.subplots(2, 2, figsize=(13, 9))
        norm = Normalize(vmin=0, vmax=maximum)
        cmap = plt.get_cmap("viridis").copy()
        cmap.set_bad("#e5e7eb")
        for ax, mode in zip(axes.flat, MODES):
            values = np.array([[matrix[(mode, w, s)]["plot_tps"] for s in SKEWS] for w in WORKERS])
            image = ax.imshow(np.ma.masked_invalid(values), cmap=cmap, norm=norm, aspect="auto")
            ax.set_title(LABELS[mode], pad=10)
            ax.set_xticks(range(len(SKEWS)), [f"{s:.1f}" for s in SKEWS])
            ax.set_yticks(range(len(WORKERS)), [str(w) for w in WORKERS])
            ax.set_xlabel("Zipf skew")
            ax.set_ylabel("Executor workers")
            for i, w in enumerate(WORKERS):
                for j, s in enumerate(SKEWS):
                    value = values[i, j]
                    label = f"{value:,.0f}" if math.isfinite(value) else "TIMEOUT"
                    color = ("white" if norm(value) < .60 else "#111827") if math.isfinite(value) else "#991b1b"
                    ax.text(j, i, label, ha="center", va="center", fontsize=10, color=color)
        fig.suptitle("Full 96-point throughput matrix", fontsize=17, y=.98)
        fig.subplots_adjust(top=.90, bottom=.15, left=.07, right=.88, hspace=.42, wspace=.23)
        color_axis = fig.add_axes([.91, .20, .017, .65])
        fig.colorbar(image, cax=color_axis, label="Transactions / second (common scale)")
        footer(fig, "92 accepted points; four gray PG cells are timeouts, not zero-throughput measurements.")
        save(fig, "04_throughput_heatmap", "Complete throughput matrix", "Common color scale across modes. Gray TIMEOUT cells retain rejected outcomes.", pdf)

        failures = sorted((r for r in rows if r["status"] == "rejected"),
                          key=lambda r: (float(r["skew"]), int(r["workers"])))
        if failures:
            fig, axes = plt.subplots(1, 2, figsize=(12, 6))
            positions = np.arange(len(failures))
            labels = [f"Skew {float(r['skew']):.1f}\n{r['workers']} workers" for r in failures]
            total = [int(r["retry_attempts_total"]) for r in failures]
            deadlocks = [int(r["deadlocks"]) for r in failures]
            axes[0].bar(positions - .18, total, .36, color="#2563eb", label="All retry attempts")
            axes[0].bar(positions + .18, deadlocks, .36, color="#dc2626", label="Deadlock retries (40P01)")
            for x, value in zip(positions - .18, total):
                axes[0].annotate(f"{value:,}", (x, value), xytext=(0, 5), textcoords="offset points", ha="center", fontsize=9)
            for x, value in zip(positions + .18, deadlocks):
                axes[0].annotate(f"{value:,}", (x, value), xytext=(0, 5), textcoords="offset points", ha="center", fontsize=9)
            axes[0].set_title("Retries before timeout")
            axes[0].set_ylabel("Recorded retry count")
            axes[0].legend(frameon=False, fontsize=9)
            backlog = [int(r["backlog_at_stop"]) for r in failures]
            bars = axes[1].bar(positions, backlog, color="#d97706", width=.55)
            axes[1].bar_label(bars, labels=[f"{v:,}" for v in backlog], padding=5, fontsize=10)
            axes[1].set_title("Backlog when the case was stopped")
            axes[1].set_ylabel("Recorded outstanding backlog")
            for ax in axes:
                ax.set_xticks(positions, labels)
                style(ax)
                ax.set_ylim(top=ax.get_ylim()[1] * 1.15)
            fig.suptitle("Four PG result-wait timeouts", fontsize=17, y=.97)
            fig.subplots_adjust(top=.82, bottom=.22, left=.08, right=.98, wspace=.27)
            footer(fig, "All hit the 600-second result limit; exhausted retries = 0. Deadlock retries are a subset of all retries.")
            save(fig, "05_pg_timeout_diagnostics", "PG timeout diagnostics", "Actual final server retry and backlog counters; these cases have no accepted TPS.", pdf)

        fig, ax = plt.subplots(figsize=(10, 6))
        accepted = [sum(r["mode"] == m and r["status"] == "passed" for r in rows) for m in MODES]
        rejected = [sum(r["mode"] == m and r["status"] == "rejected" for r in rows) for m in MODES]
        positions = np.arange(len(MODES))
        bars = ax.bar(positions, accepted, color=[COLORS[m] for m in MODES], label="Accepted")
        ax.bar_label(bars, labels=[f"{n} passed" for n in accepted], label_type="center", color="white", fontweight="bold")
        failed = ax.bar(positions, rejected, bottom=accepted, color="#dc2626", label="Timeout / rejected")
        ax.bar_label(failed, labels=[f"{n} timeouts" if n else "" for n in rejected], label_type="center", color="white", fontsize=10)
        ax.set_xticks(positions, [LABELS[m] for m in MODES])
        ax.set_ylabel("Measured cases")
        ax.set_yticks(range(0, 29, 4))
        ax.set_ylim(0, 27)
        ax.grid(axis="y", color="#e2e8f0")
        ax.set_axisbelow(True)
        ax.legend(handles=[Patch(facecolor="#475569", label="Accepted"), Patch(facecolor="#dc2626", label="Timeout / rejected")],
                  loc="upper center", ncol=2, frameon=False)
        fig.suptitle("Campaign outcomes: all 96 points attempted", fontsize=17, y=.96)
        fig.subplots_adjust(top=.81, bottom=.18, left=.09, right=.97)
        footer(fig, "Cluster: all-three audit + table/oracle equality + marker + Merkle PASS at every point.")
        save(fig, "06_campaign_outcomes", "Campaign outcomes", "92 accepted and four rejected PG cases. Smoke and signed-receipt check are excluded.", pdf)

    source_hash = hashlib.sha256(args.csv.read_bytes()).hexdigest()
    images = "\n".join(
        '<section><h2>' + html.escape(item["title"]) + '</h2><p>' + html.escape(item["description"])
        + '</p><a href="' + item["png"] + '"><img src="' + item["png"] + '" alt="'
        + html.escape(item["title"]) + '" loading="lazy"></a><p><a href="' + item["png"]
        + '">PNG</a> · <a href="' + item["svg"] + '">SVG</a></p></section>' for item in catalog)
    page = '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
    page += '<title>Paper YCSB — benchmark figures</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:1200px;margin:30px auto;padding:0 20px;color:#172033}h1,h2{line-height:1.25}section{margin:40px 0}img{width:100%;height:auto;border:1px solid #e2e8f0;border-radius:8px}a{color:#2563eb}.note{background:#f1f5f9;padding:18px;border-radius:8px}</style>'
    page += '<h1>Paper YCSB: 96-point benchmark sweep</h1><p class="note">92 accepted cases; four PG timeouts. One trial per point; 20,000 transactions, ten operations each. Timeout cases have no TPS. Primary cluster uses trusted text receipts; the separate signed-receipt check is excluded.</p>'
    page += '<p><a href="paper_ycsb_figures.pdf">Download all figures as PDF</a> · <a href="../ALL_CASES.csv">96-case CSV</a> · <a href="../RESULTS.md">Detailed report</a></p>' + images
    page += '<p>Input CSV SHA-256: <code>' + source_hash + '</code></p></html>\n'
    (args.out / "index.html").write_text(page)
    manifest = dict(source_csv_sha256=source_hash, generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    measured_cases=len(rows), accepted_cases=sum(r["status"] == "passed" for r in rows),
                    rejected_cases=sum(r["status"] == "rejected" for r in rows), one_trial=True,
                    figures=catalog, pdf=pdf_path.name,
                    sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(args.out.iterdir()) if p.is_file()})
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(dict(figures=len(catalog), measured_cases=len(rows), out=str(args.out),
                          source_csv_sha256=source_hash), indent=2))


if __name__ == "__main__":
    main()
