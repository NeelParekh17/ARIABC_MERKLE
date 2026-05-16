#!/usr/bin/env python3
"""Plot TPS vs threads for 4 overhead comparison profiles.

Profiles:
  vanilla-pg          -- raw Postgres nondet (Config 1 baseline)
  base-no-raft-no-kafka -- BCDB det-only single-node (Config 2)
  kafka-only-no-raft  -- broadcast + Kafka result collection, no Raft (Config 4)
  raft-kafka          -- full system: Raft + det execution + Kafka (Config 5)
"""
import argparse
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple

METRIC_CHOICES = ("valid_gateway", "valid_wall", "valid_strict_wall")


def _as_int(v: str) -> Optional[int]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _as_float(v: str) -> Optional[float]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _read_summary(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def _metric_for_row(row: dict, metric: str) -> Optional[float]:
    if metric == "valid_gateway":
        return _as_float(row.get("mean_throughput_valid_ops_per_s", "")) or _as_float(
            row.get("mean_throughput_ops_per_s", "")
        )
    if metric == "valid_wall":
        return _as_float(row.get("mean_valid_throughput_ops_per_s_wall", "")) or _metric_for_row(
            row, "valid_gateway"
        )
    if metric == "valid_strict_wall":
        strict = _as_float(row.get("mean_valid_throughput_strict_ops_per_s_wall", ""))
        if strict is not None:
            return strict
        completion = (row.get("completion_semantics") or "").strip()
        strict_completion = (row.get("strict_completion_semantics") or "").strip()
        if strict_completion and strict_completion == completion:
            return _metric_for_row(row, "valid_wall")
        if strict_completion.startswith("single_node_") or completion.startswith("single_node_"):
            return _metric_for_row(row, "valid_wall")
        return None
    raise ValueError(f"unsupported metric: {metric}")


def _collect_workloads(rows_by_profile: Dict[str, List[dict]]) -> List[str]:
    ws = set()
    for rows in rows_by_profile.values():
        for r in rows:
            w = (r.get("workload") or "").strip()
            if w:
                ws.add(w)
    return sorted(ws)


def _series_for(rows: List[dict], workload: str, metric: str) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    for r in rows:
        if (r.get("workload") or "").strip() != workload:
            continue
        th = _as_int(r.get("threads", ""))
        tps = _metric_for_row(r, metric)
        if th is None or tps is None:
            continue
        out.append((th, tps))
    out.sort(key=lambda x: x[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Plot combined TPS curves for 4 overhead comparison profiles")
    ap.add_argument("--vanilla-pg",  required=True, help="vanilla-pg summary.csv")
    ap.add_argument("--base",        required=True, help="base-no-raft-no-kafka summary.csv")
    ap.add_argument("--kafka-only",  required=True, help="kafka-only-no-raft summary.csv")
    ap.add_argument("--raft-kafka",  required=True, help="raft-kafka summary.csv")
    ap.add_argument("--metric", choices=METRIC_CHOICES, default="valid_wall",
                    help="Which throughput metric to plot")
    ap.add_argument("--out",         required=True, help="Output PNG path")
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        raise RuntimeError(f"matplotlib not available: {e}")

    rows_by_profile = {
        "vanilla-pg":           _read_summary(Path(args.vanilla_pg).resolve()),
        "base-no-raft-no-kafka": _read_summary(Path(args.base).resolve()),
        "kafka-only-no-raft":   _read_summary(Path(args.kafka_only).resolve()),
        "raft-kafka":           _read_summary(Path(args.raft_kafka).resolve()),
    }

    workloads = _collect_workloads(rows_by_profile)
    if not workloads:
        raise RuntimeError("No workload rows found in input summaries")

    colors = {
        "vanilla-pg":            "#9467bd",   # purple
        "base-no-raft-no-kafka": "#1f77b4",   # blue
        "kafka-only-no-raft":    "#2ca02c",   # green
        "raft-kafka":            "#d62728",   # red
    }
    labels = {
        "vanilla-pg":            "vanilla-pg (Config 1 baseline)",
        "base-no-raft-no-kafka": "det-only single-node (Config 2)",
        "kafka-only-no-raft":    "kafka-only no-raft (Config 4)",
        "raft-kafka":            "raft+kafka full system (Config 5)",
    }
    ylabel = {
        "valid_gateway": "Mean Valid Gateway Ops/s",
        "valid_wall": "Mean Valid Wall Ops/s",
        "valid_strict_wall": "Mean Valid Strict Wall Ops/s",
    }[args.metric]

    fig, axes = plt.subplots(len(workloads), 1,
                              figsize=(12, 4.2 * len(workloads)),
                              dpi=130, squeeze=False)

    for i, workload in enumerate(workloads):
        ax = axes[i][0]
        plotted = False
        for profile, rows in rows_by_profile.items():
            pts = _series_for(rows, workload, args.metric)
            if not pts:
                continue
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=4.5,
                    color=colors[profile], label=labels[profile])
            plotted = True
        ax.set_title(f"{ylabel} vs Threads — {workload}")
        ax.set_xlabel("Threads")
        ax.set_ylabel(ylabel)
        ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
        if plotted:
            ax.legend(fontsize=9)

    fig.tight_layout()
    out = Path(args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
