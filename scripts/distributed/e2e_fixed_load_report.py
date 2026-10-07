#!/usr/bin/env python3
"""Per-transaction latency vs worker threads: closed loop vs fixed offered load.

Usage: e2e_fixed_load_report.py OUT_DIR LABEL=CAMPAIGN_DIR [...]
Each campaign is a run_all_modes_gateway_sweep.py cluster sweep over W; the
label names its load shape (e.g. "closed loop, window 1024", "1,000 tx/s").
"""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from e2e_latency_final_results import INK, INK2, find_cases, log2_workers, style, summarize  # noqa: E402
from e2e_latency_report import COLORS, load  # noqa: E402


def main():
    out = sys.argv[1]
    os.makedirs(os.path.join(out, "graphs"), exist_ok=True)
    rows, labels = [], []
    for spec in sys.argv[2:]:
        label, _, campaign = spec.partition("=")
        labels.append(label)
        by_w = {}
        for c in find_cases(campaign):
            by_w.setdefault(c["workers"], []).append(c)
        for w, group in sorted(by_w.items()):
            group.sort(key=lambda c: c["tps"])
            c = group[len(group) // 2]
            s = summarize(c, load(c["csv"]))
            s["load"] = label
            rows.append(s)
    keys = ["load", "workers", "tps", "e2e_mean_ms", "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms",
            "submit_to_accept_mean_ms", "replica_exec_queue_wait_ms", "replica_pg_exec_ms",
            "commit_sign_publish_ms", "first_result_to_majority_mean_ms", "majority_to_client_mean_ms",
            "merkle_pass", "divergence_count", "permanent_failures", "run_id"]
    with open(os.path.join(out, "fixed_load_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    for s in rows:
        print(" | ".join(f"{k}={s.get(k)}" for k in keys[:13]))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    workers = sorted({s["workers"] for s in rows})
    color = {lab: COLORS[i] for i, lab in enumerate(labels)}
    panels = [("e2e_mean_ms", "Mean end-to-end latency (ms)"),
              ("e2e_p99_ms", "p99 end-to-end latency (ms)"),
              ("replica_exec_queue_wait_ms", "Wait in replica execution queue (ms)"),
              ("replica_pg_exec_ms", "PostgreSQL execution per tx (ms)")]
    fig, axes = plt.subplots(1, len(panels), figsize=(5 * len(panels), 4.6))
    for ax, (key, title) in zip(axes, panels):
        for lab in labels:
            pts = [s for s in rows if s["load"] == lab]
            ax.plot([s["workers"] for s in pts], [s[key] for s in pts], color=color[lab], linewidth=2,
                    marker="o", markersize=7, label=lab)
        log2_workers(ax, workers)
        ax.set_xlabel("server execution workers (W)", fontsize=9, color=INK2)
        ax.set_title(title, fontsize=10, color=INK)
        if key in ("e2e_mean_ms", "e2e_p99_ms", "replica_exec_queue_wait_ms"):
            ax.set_yscale("log")
        else:
            ax.set_ylim(bottom=0)
        style(ax)
    handles, hl = axes[0].get_legend_handles_labels()
    fig.legend(handles, hl, loc="lower center", ncol=len(labels), frameon=False, fontsize=9)
    fig.suptitle("Latency vs worker threads: closed loop vs fixed offered load", fontsize=12, color=INK)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(os.path.join(out, "graphs", "latency_vs_workers_by_load.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
