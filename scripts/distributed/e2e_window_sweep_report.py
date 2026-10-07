#!/usr/bin/env python3
"""Latency/throughput trade-off of the gateway det window (outstanding txs).

Usage: e2e_window_sweep_report.py OUT_DIR WINDOW=CAMPAIGN_DIR [...]
Each campaign dir is a run_all_modes_gateway_sweep.py cluster sweep made with
CLUSTER_DET_WINDOW=WINDOW (same YCSB config otherwise).
"""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from e2e_latency_final_results import (INK, INK2, find_cases, log2_workers,  # noqa: E402
                                       style, summarize)
from e2e_latency_report import COLORS, load  # noqa: E402


def main():
    out = sys.argv[1]
    os.makedirs(os.path.join(out, "graphs"), exist_ok=True)
    rows = []
    for spec in sys.argv[2:]:
        win, _, campaign = spec.partition("=")
        for c in find_cases(campaign):
            s = summarize(c, load(c["csv"]))
            s["det_window"] = int(win)
            rows.append(s)
    # Keep only worker counts measured at more than one window.
    seen = {}
    for s in rows:
        seen.setdefault(s["workers"], set()).add(s["det_window"])
    rows = [s for s in rows if len(seen[s["workers"]]) > 1]
    rows.sort(key=lambda s: (s["workers"], -s["det_window"]))
    keys = ["workers", "det_window", "tps", "e2e_mean_ms", "e2e_p50_ms", "e2e_p99_ms", "e2e_min_ms",
            "majority_mean_ms", "majority_min_ms", "submit_to_accept_mean_ms",
            "replica_exec_queue_wait_ms", "replica_pg_exec_ms", "commit_sign_publish_ms",
            "first_result_to_majority_mean_ms", "majority_to_client_mean_ms",
            "merkle_pass", "divergence_count", "permanent_failures", "run_id"]
    with open(os.path.join(out, "window_sweep_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    for s in rows:
        print(" ".join(f"{k}={s.get(k)}" for k in keys[:15]))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    workers = sorted({s["workers"] for s in rows})
    color = {w: COLORS[i] for i, w in enumerate(workers)}

    # Latency vs window, and throughput vs window (two charts, one axis each).
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.6))
    for w in workers:
        pts = [s for s in rows if s["workers"] == w]
        wins = [s["det_window"] for s in pts]
        axes[0].plot(wins, [s["e2e_mean_ms"] for s in pts], color=color[w], linewidth=2, marker="o",
                     markersize=7, label=f"W={w} mean")
        axes[0].plot(wins, [s["e2e_p99_ms"] for s in pts], color=color[w], linewidth=1.5, marker="o",
                     markersize=6, linestyle="--", label=f"W={w} p99")
        axes[1].plot(wins, [s["tps"] for s in pts], color=color[w], linewidth=2, marker="o",
                     markersize=7, label=f"W={w}")
        axes[2].plot(wins, [s["majority_to_client_mean_ms"] for s in pts], color=color[w], linewidth=2,
                     marker="o", markersize=7, label=f"W={w}")
    titles = ["End-to-end latency (submit → client)", "Throughput",
              "Majority verified → client hand-off (mean)"]
    ylabels = ["latency (ms, log scale)", "majority-verified tx/s", "ms per transaction"]
    for ax, t, yl in zip(axes, titles, ylabels):
        log2_workers(ax, sorted({s["det_window"] for s in rows}))
        ax.set_xlabel("gateway det window (max outstanding transactions)", fontsize=9, color=INK2)
        ax.set_ylabel(yl, fontsize=9, color=INK2)
        ax.set_title(t, fontsize=11, color=INK)
        ax.legend(frameon=False, fontsize=8)
        style(ax)
    axes[0].set_yscale("log")
    axes[1].set_ylim(bottom=0)
    axes[2].set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "graphs", "window_sweep_latency_tps.png"), dpi=150)
    plt.close(fig)

    # Trade-off curve: each point is one window.
    fig, ax = plt.subplots(figsize=(7.6, 4.8))
    for w in workers:
        pts = [s for s in rows if s["workers"] == w]
        ax.plot([s["tps"] for s in pts], [s["e2e_mean_ms"] for s in pts], color=color[w], linewidth=2,
                marker="o", markersize=7, label=f"W={w}")
        for s in pts:
            ax.annotate(f"{s['det_window']}", (s["tps"], s["e2e_mean_ms"]), textcoords="offset points",
                        xytext=(6, 4), fontsize=8, color=INK2)
    ax.set_yscale("log")
    ax.set_xlabel("majority-verified throughput (tx/s)", fontsize=9, color=INK2)
    ax.set_ylabel("mean end-to-end latency (ms, log scale)", fontsize=9, color=INK2)
    ax.set_title("Latency vs throughput (labels = det window)", fontsize=11, color=INK)
    ax.legend(frameon=False, fontsize=9, title="server workers", title_fontsize=9)
    style(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "graphs", "window_sweep_tradeoff.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
