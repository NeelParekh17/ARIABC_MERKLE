#!/usr/bin/env python3
"""Per-transaction end-to-end latency report from gateway tx_latency.csv files.

The gateway (--txLatencyCsv) stamps every transaction on its own steady clock:
  submit          gateway sends the request to the Raft leader
  accept_ms       leader ACK observed (Raft append accepted)
  first_result_ms first replica's signed result consumed from Kafka
  majority_ms     result hash verified by a majority of replicas
  all_results_ms  last replica's result (audit, not on the client path)
  latency_ms      gateway hands the verified result back to the client
All *_ms stage columns are relative to the transaction's own submit.

Usage:
  e2e_latency_report.py --out DIR LABEL=path/to/tx_latency.csv [...]
"""
import argparse
import csv
import math
import os
import sys

STAGES = [
    # (name, start column or None for submit, end column)
    ("submit_to_accept", None, "accept_ms"),
    ("accept_to_first_result", "accept_ms", "first_result_ms"),
    ("first_result_to_majority", "first_result_ms", "majority_ms"),
    ("majority_to_client", "majority_ms", "latency_ms"),
]
POINTS = ["accept_ms", "first_result_ms", "majority_ms", "all_results_ms", "latency_ms"]
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]


def load(path):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            row = {}
            for k, v in r.items():
                if v is None or v == "":
                    row[k] = None
                    continue
                try:
                    row[k] = float(v)
                except ValueError:
                    row[k] = v
            rows.append(row)
    return rows


def pct(sorted_vals, p):
    if not sorted_vals:
        return math.nan
    i = max(0, min(len(sorted_vals) - 1, math.ceil(p / 100.0 * len(sorted_vals)) - 1))
    return sorted_vals[i]


def stats(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return {"n": 0, "mean": math.nan, "p50": math.nan, "p95": math.nan, "p99": math.nan, "max": math.nan}
    return {"n": len(v), "mean": sum(v) / len(v), "p50": pct(v, 50), "p95": pct(v, 95),
            "p99": pct(v, 99), "max": v[-1]}


def stage_values(rows, start, end):
    out = []
    for r in rows:
        e = r.get(end)
        s = 0.0 if start is None else r.get(start)
        if e is None or s is None:
            continue
        # The leader ACK is read lazily in the pipelined submit path, so it can
        # land after the first result; clamp so stages never go negative.
        out.append(max(0.0, e - s))
    return out


def summarize(label, rows):
    span_ms = 0.0
    fins = [r["finish_ms"] for r in rows if r.get("finish_ms") is not None]
    subs = [r["submit_ms"] for r in rows if r.get("submit_ms") is not None]
    if fins:
        span_ms = max(fins) - (min(subs) if subs else 0.0)
    out = {"label": label, "tx": len(rows),
           "tps": (len(rows) / (span_ms / 1000.0)) if span_ms > 0 else math.nan,
           "submit_span_ms": (max(subs) - min(subs)) if subs else math.nan}
    for col in POINTS:
        st = stats(r.get(col) for r in rows)
        for k, v in st.items():
            out[f"{col}_{k}"] = v
    for name, start, end in STAGES:
        st = stats(stage_values(rows, start, end))
        out[f"{name}_mean"] = st["mean"]
        out[f"{name}_p50"] = st["p50"]
    return out


def fmt(v):
    return "–" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:,.2f}"


def write_markdown(path, summaries):
    with open(path, "w") as f:
        f.write("| Run | Tx | TPS | Submit span (ms) | Majority-verified p50 / p95 / p99 (ms) "
                "| Client-complete p50 / p95 / p99 (ms) | Mean e2e (ms) |\n")
        f.write("|---|---:|---:|---:|---|---|---:|\n")
        for s in summaries:
            f.write(f"| {s['label']} | {s['tx']} | {fmt(s['tps'])} | {fmt(s['submit_span_ms'])} "
                    f"| {fmt(s['majority_ms_p50'])} / {fmt(s['majority_ms_p95'])} / {fmt(s['majority_ms_p99'])} "
                    f"| {fmt(s['latency_ms_p50'])} / {fmt(s['latency_ms_p95'])} / {fmt(s['latency_ms_p99'])} "
                    f"| {fmt(s['latency_ms_mean'])} |\n")
        f.write("\nMean stage breakdown (ms):\n\n")
        f.write("| Run | " + " | ".join(n for n, _, _ in STAGES) + " | total |\n")
        f.write("|---|" + "---:|" * (len(STAGES) + 1) + "\n")
        for s in summaries:
            vals = [s[f"{n}_mean"] for n, _, _ in STAGES]
            f.write(f"| {s['label']} | " + " | ".join(fmt(v) for v in vals)
                    + f" | {fmt(s['latency_ms_mean'])} |\n")


def plot(out_dir, runs, summaries):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not available; skipping plots", file=sys.stderr)
        return

    def style(ax):
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.grid(axis="y", color="#e4e3df", linewidth=0.8)
        ax.set_axisbelow(True)

    # CDF of majority-verified and client-complete latency.
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), sharey=True)
    for ax, col, title in ((axes[0], "majority_ms", "Submit → verified by majority"),
                           (axes[1], "latency_ms", "Submit → completed to client")):
        for i, (label, rows) in enumerate(runs):
            v = sorted(r[col] for r in rows if r.get(col) is not None)
            if not v:
                continue
            ys = [(k + 1) / len(v) for k in range(len(v))]
            ax.plot(v, ys, color=COLORS[i % len(COLORS)], linewidth=2, label=label)
        ax.set_title(title, fontsize=12)
        ax.set_xlabel("latency (ms)")
        style(ax)
    axes[0].set_ylabel("fraction of transactions")
    axes[1].legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "e2e_latency_cdf.png"), dpi=150)
    plt.close(fig)

    # Stacked mean stage breakdown.
    fig, ax = plt.subplots(figsize=(9, 4.8))
    labels = [s["label"] for s in summaries]
    bottoms = [0.0] * len(summaries)
    names = {"submit_to_accept": "submit → leader accept",
             "accept_to_first_result": "accept → first replica result",
             "first_result_to_majority": "first result → majority verified",
             "majority_to_client": "majority → client completion"}
    for j, (name, _, _) in enumerate(STAGES):
        vals = [0.0 if math.isnan(s[f"{name}_mean"]) else s[f"{name}_mean"] for s in summaries]
        ax.bar(labels, vals, bottom=bottoms, color=COLORS[j], width=0.6,
               edgecolor="white", linewidth=2, label=names[name])
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    for x, total in enumerate(bottoms):
        ax.text(x, total, f"{total:,.1f} ms", ha="center", va="bottom", fontsize=9, color="#52514e")
    ax.set_ylabel("mean latency per transaction (ms)")
    ax.set_title("Where each transaction's end-to-end time goes", fontsize=12)
    ax.legend(frameon=False, fontsize=9)
    style(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "e2e_latency_breakdown.png"), dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("runs", nargs="+", help="LABEL=path/to/tx_latency.csv")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    runs = []
    for spec in a.runs:
        # Labels and paths may both contain '=' (e.g. workers=16_run1/...):
        # split at the first '=' whose remainder is an existing file.
        cuts = [i for i, ch in enumerate(spec) if ch == "="]
        cut = next((i for i in cuts if os.path.isfile(spec[i + 1:])), None)
        if cut is None:
            ap.error(f"expected LABEL=path to an existing file, got {spec}")
        label, path = spec[:cut], spec[cut + 1:]
        rows = load(path)
        if rows and "majority_ms" not in rows[0]:
            print(f"warning: {path} has no timeline columns (old gateway build)", file=sys.stderr)
        runs.append((label, rows))

    summaries = [summarize(label, rows) for label, rows in runs]
    keys = list(summaries[0].keys())
    with open(os.path.join(a.out, "e2e_latency_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(summaries)
    write_markdown(os.path.join(a.out, "e2e_latency_summary.md"), summaries)
    plot(a.out, runs, summaries)
    with open(os.path.join(a.out, "e2e_latency_summary.md")) as f:
        print(f.read())


if __name__ == "__main__":
    main()
