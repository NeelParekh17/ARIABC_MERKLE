#!/usr/bin/env python3
"""Build Final_Results/WORKER_THREAD_LATENCY from a cluster-mode YCSB sweep.

The sweep is run_all_modes_gateway_sweep.py --modes cluster with the exact
Final_Results/YCSB arguments, for one workload over several worker counts;
each cluster run leaves the gateway's per-tx timeline in
<artifact_dir>/tx_latency.csv.

Usage: e2e_latency_final_results.py CAMPAIGN_DIR OUT_DIR [LABEL=CAMPAIGN_DIR ...]
The optional LABEL=CAMPAIGN_DIR runs are compared against the main campaign
(comparison.csv and graphs/before_after_comparison.png).
"""
import csv
import glob
import gzip
import json
import os
import re
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from e2e_latency_report import COLORS, STAGES, load, stage_values, stats  # noqa: E402

STAGE_LABELS = {
    "submit_to_accept": "submit → Raft leader accept",
    "accept_to_first_result": "accept → first replica result",
    "first_result_to_majority": "first result → majority verified",
    "majority_to_client": "majority verified → client completion",
}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

# Finer split of each transaction's mean time. The replica exec-queue wait
# and PostgreSQL time come from the fastest replica's PROFILE_SERVER counters
# (durations on that node's own clock, averaged per tx); the replica with the
# shortest queue is the one that supplies the first result.
FINE_STAGES = [
    ("submit_to_accept_mean_ms", "gateway submit + Raft append (leader accept)"),
    ("replica_exec_queue_wait_ms", "wait in replica execution queue"),
    ("replica_pg_exec_ms", "PostgreSQL execution"),
    ("commit_sign_publish_ms", "Raft commit, result signing, Kafka → gateway"),
    ("first_result_to_majority_mean_ms", "wait for 2nd matching replica (majority)"),
    ("majority_to_client_mean_ms", "majority verified → client completion"),
]


def server_profile(artifact_dir):
    """Per-node mean exec-queue wait and PostgreSQL time per transaction."""
    nodes = {}
    for f in sorted(glob.glob(os.path.join(artifact_dir, "server_node*.log"))):
        lines = [ln for ln in open(f, errors="replace") if "PROFILE_SERVER" in ln]
        if not lines:
            continue
        kv = dict(re.findall(r"(\w+)=([-\d.e+]+)", lines[-1]))
        n = float(kv.get("exec_calls", 0) or 0)
        if n <= 0:
            continue
        name = re.search(r"server_(node\d+_\w+)\.log", os.path.basename(f)).group(1)
        nodes[name] = (float(kv["queue_delay_exec_start_ms"]) / n, float(kv["pg_query_ms"]) / n)
    return nodes


def find_cases(campaign):
    cases = []
    for path in sorted(glob.glob(os.path.join(campaign, "attempts", "cluster4_*.json"))):
        a = json.load(open(path))
        csv_path = os.path.join(a.get("artifact_dir", ""), "tx_latency.csv")
        if a.get("status") != "passed" or not os.path.isfile(csv_path):
            print(f"skip {a.get('run_id')}: status={a.get('status')}", file=sys.stderr)
            continue
        r = a["result"]
        cases.append({"workload": os.path.basename(a["workload"]), "workers": int(a["workers"]),
                      "server": server_profile(a["artifact_dir"]),
                      "run_id": a["run_id"], "csv": csv_path, "tps": r["tps"],
                      "det_window": int(a.get("det_window") or 65536),
                      "wall_time_ms": r["wall_time_ms"], "merkle_pass": r.get("merkle_pass"),
                      "divergence_count": r.get("divergence_count"),
                      "permanent_failures": r.get("permanent_failures")})
    # Trials are numbered by run order (run ids are timestamped) per worker count.
    cases.sort(key=lambda c: (c["workers"], c["run_id"]))
    for i, c in enumerate(cases):
        c["trial"] = 1 if i == 0 or cases[i - 1]["workers"] != c["workers"] else cases[i - 1]["trial"] + 1
    return cases


def summarize(case, rows):
    subs = [r["submit_ms"] for r in rows]
    out = {"workload": case["workload"], "workers": case["workers"], "det_window": case["det_window"],
           "trial": case["trial"], "tx": len(rows),
           "tps": case["tps"], "wall_time_ms": case["wall_time_ms"],
           "submit_span_ms": round(max(subs) - min(subs), 1)}
    for col, name in (("majority_ms", "majority"), ("latency_ms", "e2e")):
        st = stats(r.get(col) for r in rows)
        for k in ("mean", "p50", "p95", "p99", "max"):
            out[f"{name}_{k}_ms"] = round(st[k], 3)
        out[f"{name}_min_ms"] = round(min(r[col] for r in rows), 3)
    for name, start, end in STAGES:
        out[f"{name}_mean_ms"] = round(stats(stage_values(rows, start, end))["mean"], 3)
    out["all_results_mean_ms"] = round(stats(r.get("all_results_ms") for r in rows)["mean"], 3)
    if case["server"]:
        fastest = min(case["server"], key=lambda k: case["server"][k][0])
        qwait, pg = case["server"][fastest]
        out["fastest_replica"] = fastest
        out["replica_exec_queue_wait_ms"] = round(qwait, 3)
        out["replica_pg_exec_ms"] = round(pg, 3)
        out["commit_sign_publish_ms"] = round(
            max(0.0, out["accept_to_first_result_mean_ms"] - qwait - pg), 3)
        for node, (q, p) in sorted(case["server"].items()):
            out[f"{node}_exec_queue_wait_ms"] = round(q, 3)
            out[f"{node}_pg_exec_ms"] = round(p, 3)
    out["tx_with_all_replicas"] = sum(1 for r in rows if (r.get("replies") or 0) >= 3)
    for k in ("merkle_pass", "divergence_count", "permanent_failures", "run_id"):
        out[k] = case[k]
    return out


def style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#b9b8b2")
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def log2_workers(ax, workers):
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_locator(FixedLocator(workers))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v)}"))


def main():
    campaign, out = sys.argv[1], sys.argv[2]
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    cases = find_cases(campaign)
    if not cases:
        sys.exit(f"no passed cluster cases with tx_latency.csv under {campaign}")
    # Every trial is summarized; the median-TPS trial of each worker count is
    # the representative run for the traces and graphs.
    trials = [summarize(c, load(c["csv"])) for c in cases]
    by_w = {}
    for c, t in zip(cases, trials):
        by_w.setdefault(c["workers"], []).append((c, t))
    workers = sorted(by_w)
    rep_case, summary = {}, []
    for w in workers:
        group = sorted(by_w[w], key=lambda ct: ct[1]["tps"])
        c, t = group[len(group) // 2]
        rep_case[w] = c
        row = dict(t)
        row["trials"] = len(group)
        for k in ("tps", "e2e_mean_ms", "e2e_p50_ms", "e2e_p99_ms", "majority_to_client_mean_ms"):
            vals = sorted(g[1][k] for g in group)
            row[f"{k}_min"], row[f"{k}_median"], row[f"{k}_max"] = vals[0], vals[len(vals) // 2], vals[-1]
        summary.append(row)
    cases = [rep_case[w] for w in workers]
    data = {w: load(rep_case[w]["csv"]) for w in workers}
    color = {w: COLORS[i] for i, w in enumerate(workers)}
    workload = cases[0]["workload"]

    graphs = os.path.join(out, "graphs")
    traces = os.path.join(out, "traces")
    os.makedirs(graphs, exist_ok=True)
    os.makedirs(traces, exist_ok=True)
    for c in cases:
        dst = os.path.join(traces, f"workers={c['workers']}_tx_latency.csv.gz")
        with open(c["csv"], "rb") as src, gzip.open(dst, "wb") as gz:
            shutil.copyfileobj(src, gz)
    with open(os.path.join(out, "summary.csv"), "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        wr.writeheader()
        wr.writerows(summary)
    trials.sort(key=lambda t: (t["workers"], t["trial"]))
    with open(os.path.join(out, "trials.csv"), "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(trials[0].keys()))
        wr.writeheader()
        wr.writerows(trials)

    # 1. CDF: verified-by-majority and completed-to-client.
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), sharey=True)
    for ax, col, title in ((axes[0], "majority_ms", "Submit → verified by majority (2 of 3 replicas)"),
                           (axes[1], "latency_ms", "Submit → completed to client")):
        for w in workers:
            v = sorted(r[col] for r in data[w])
            ax.plot(v, [(i + 1) / len(v) for i in range(len(v))], color=color[w], linewidth=2,
                    label=f"W={w}")
        ax.set_title(title, fontsize=11, color=INK)
        ax.set_xlabel("latency (ms)", fontsize=9, color=INK2)
        ax.set_xlim(left=0)
        style(ax)
    axes[0].set_ylabel("fraction of transactions", fontsize=9, color=INK2)
    axes[1].legend(frameon=False, fontsize=9, title="server workers", title_fontsize=9)
    fig.suptitle(f"Per-transaction end-to-end latency — {workload}, det window {cases[0]['det_window']}",
                 fontsize=12, color=INK)
    fig.tight_layout()
    fig.savefig(os.path.join(graphs, "e2e_latency_cdf.png"), dpi=150)
    plt.close(fig)

    # 2. Percentiles vs workers.
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    for j, (k, lab) in enumerate((("mean", "mean"), ("p50", "p50"), ("p95", "p95"), ("p99", "p99"))):
        vals = [s[f"e2e_{k}_ms"] for s in summary]
        ax.plot(workers, vals, color=COLORS[j], linewidth=2, marker="o", markersize=7, label=lab)
        if k in ("p50", "p99"):
            ax.annotate(f"{lab} {vals[-1]:,.0f}", (workers[-1], vals[-1]), textcoords="offset points",
                        xytext=(8, 6 if k == "p99" else -6), va="center", fontsize=8, color=INK2)
    log2_workers(ax, workers)
    ax.set_xlim(workers[0] / 1.25, workers[-1] * 1.6)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("server execution workers (W)", fontsize=9, color=INK2)
    ax.set_ylabel("submit → completed to client (ms)", fontsize=9, color=INK2)
    ax.set_title("End-to-end latency percentiles vs worker threads", fontsize=11, color=INK)
    ax.legend(frameon=False, fontsize=9)
    style(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(graphs, "e2e_latency_percentiles_vs_workers.png"), dpi=150)
    plt.close(fig)

    # 3. Mean per-tx time by stage; right panel drops the two queueing stages
    #    so the per-transaction work is visible.
    fine = all("replica_exec_queue_wait_ms" in s for s in summary)
    stages = FINE_STAGES if fine else [(f"{n}_mean_ms", STAGE_LABELS[n]) for n, _, _ in STAGES]
    queued = {"submit_to_accept_mean_ms", "replica_exec_queue_wait_ms"}
    panels = [("All stages", stages)]
    if fine:
        panels.append(("Without queueing (submit/Raft backlog, replica exec queue)",
                       [st for st in stages if st[0] not in queued]))
    fig, axes = plt.subplots(1, len(panels), figsize=(7.2 * len(panels), 5.2), squeeze=False)
    labels = [f"W={w}" for w in workers]
    for ax, (title, sts) in zip(axes[0], panels):
        bottoms = [0.0] * len(summary)
        for key, lab in sts:
            j = [k for k, _ in stages].index(key)
            vals = [s[key] for s in summary]
            ax.bar(labels, vals, bottom=bottoms, color=COLORS[j], width=0.6, edgecolor="white",
                   linewidth=1.5, label=lab)
            bottoms = [b + v for b, v in zip(bottoms, vals)]
        for x, t in enumerate(bottoms):
            ax.text(x, t, f"{t:,.1f} ms" if t < 100 else f"{t:,.0f} ms", ha="center", va="bottom",
                    fontsize=9, color=INK2)
        ax.set_ylim(0, max(bottoms) * 1.12)
        ax.set_title(title, fontsize=11, color=INK)
        ax.set_ylabel("mean latency per transaction (ms)", fontsize=9, color=INK2)
        style(ax)
    handles, hl = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, hl, loc="lower center", ncol=3, frameon=False, fontsize=9)
    fig.suptitle("Where each transaction's end-to-end time goes", fontsize=12, color=INK)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    fig.savefig(os.path.join(graphs, "e2e_stage_breakdown.png"), dpi=150)
    plt.close(fig)

    # 4. Latency vs submit order: all 20k are admitted within the first few
    #    hundred ms, so latency is set by queue position.
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4), sharey=True)
    for ax, col, title in ((axes[0], "majority_ms", "Submit → verified by majority"),
                           (axes[1], "latency_ms", "Submit → completed to client")):
        for w in workers:
            rows = data[w]
            ax.plot([r["tx_idx"] for r in rows], [r[col] for r in rows], color=color[w],
                    linewidth=1.4, label=f"W={w}")
        ax.set_title(title, fontsize=11, color=INK)
        ax.set_xlabel("transaction index (submit order)", fontsize=9, color=INK2)
        ax.set_ylim(0, max(r[col] for rs in data.values() for r in rs) * 1.3)
        style(ax)
    axes[0].set_ylabel("latency (ms)", fontsize=9, color=INK2)
    axes[1].legend(frameon=False, fontsize=9, title="server workers", title_fontsize=9, ncol=4,
                   loc="upper center")
    fig.suptitle(f"Latency vs submit order (det window {cases[0]['det_window']})", fontsize=12, color=INK)
    fig.tight_layout()
    fig.savefig(os.path.join(graphs, "e2e_latency_vs_queue_position.png"), dpi=150)
    plt.close(fig)

    # 5. Throughput of the same runs.
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    tps = [s["tps"] for s in summary]
    ax.plot(workers, tps, color=COLORS[0], linewidth=2, marker="o", markersize=7)
    for w, t in zip(workers, tps):
        ax.annotate(f"{t:,.0f}", (w, t), textcoords="offset points", xytext=(0, 8), ha="center",
                    fontsize=8, color=INK2)
    log2_workers(ax, workers)
    ax.set_ylim(0, max(tps) * 1.15)
    ax.set_xlabel("server execution workers (W)", fontsize=9, color=INK2)
    ax.set_ylabel("majority-verified throughput (tx/s)", fontsize=9, color=INK2)
    ax.set_title("Throughput of the measured runs", fontsize=11, color=INK)
    style(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(graphs, "tps_vs_workers.png"), dpi=150)
    plt.close(fig)

    if len(sys.argv) > 3:
        compare(out, [spec.partition("=")[::2] for spec in sys.argv[3:]] + [("window 1024 + completion fix", campaign)])

    for s in summary:
        print({k: s[k] for k in ("workers", "tps", "submit_span_ms", "majority_mean_ms",
                                 "e2e_mean_ms", "e2e_p50_ms", "e2e_p99_ms")})


def compare(out, specs):
    """Per-W mean/p99 latency, hand-off and TPS for several campaigns (median-TPS trial)."""
    import matplotlib.pyplot as plt
    rows = []
    for label, campaign in specs:
        by_w = {}
        for c in find_cases(campaign):
            by_w.setdefault(c["workers"], []).append(c)
        for w, group in sorted(by_w.items()):
            group.sort(key=lambda c: c["tps"])
            c = group[len(group) // 2]
            s = summarize(c, load(c["csv"]))
            rows.append({"config": label, "workers": w, "det_window": c["det_window"], "trials": len(group),
                         "tps": s["tps"], "e2e_mean_ms": s["e2e_mean_ms"], "e2e_p50_ms": s["e2e_p50_ms"],
                         "e2e_p99_ms": s["e2e_p99_ms"], "majority_to_client_mean_ms": s["majority_to_client_mean_ms"],
                         "replica_exec_queue_wait_ms": s.get("replica_exec_queue_wait_ms"),
                         "run_id": s["run_id"]})
    with open(os.path.join(out, "comparison.csv"), "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        wr.writeheader()
        wr.writerows(rows)

    labels = [lab for lab, _ in specs]
    workers = sorted({r["workers"] for r in rows})
    get = {(r["config"], r["workers"]): r for r in rows}
    fig, axes = plt.subplots(1, 4, figsize=(19, 4.6))
    width = 0.8 / len(labels)
    metrics = [("e2e_mean_ms", "Mean end-to-end latency (ms, log)", True),
               ("e2e_p99_ms", "p99 end-to-end latency (ms, log)", True),
               ("majority_to_client_mean_ms", "Majority → client hand-off (ms)", False),
               ("tps", "Throughput (tx/s)", False)]
    for ax, (key, title, logy) in zip(axes, metrics):
        for j, lab in enumerate(labels):
            xs = [i + (j - (len(labels) - 1) / 2) * width for i in range(len(workers))]
            vals = [get[(lab, w)][key] if (lab, w) in get else 0 for w in workers]
            ax.bar(xs, vals, width=width * 0.92, color=COLORS[j], label=lab)
            if key != "tps":
                for x, v in zip(xs, vals):
                    ax.text(x, v, f"{v:,.0f}" if v >= 10 else f"{v:.1f}", ha="center", va="bottom",
                            fontsize=6.5, color=INK2)
        ax.set_xticks(range(len(workers)))
        ax.set_xticklabels([f"W={w}" for w in workers])
        if logy:
            ax.set_yscale("log")
        ax.set_title(title, fontsize=10, color=INK)
        style(ax)
    handles, hl = axes[0].get_legend_handles_labels()
    fig.legend(handles, hl, loc="lower center", ncol=len(labels), frameon=False, fontsize=9)
    fig.suptitle("Before vs after: gateway completion fix and det window 1024", fontsize=12, color=INK)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(os.path.join(out, "graphs", "before_after_comparison.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
