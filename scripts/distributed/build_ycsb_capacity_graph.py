#!/usr/bin/env python3
"""Build a provenance-backed YCSB capacity graph from validated artifacts."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


SERIES_LABELS = {
    "single_node_pg": "PG",
    "single_node_det": "DET",
    "single_node_gateway_direct": "Direct gateway",
    "cluster_kafka": "Kafka cluster",
    "cluster_raft_kafka": "Raft + Kafka cluster",
}

PLOT_SERIES_LABELS = {
    "single_node_pg": "PG",
    "single_node_det": "DET",
    "cluster_kafka": "Kafka cluster",
    "cluster_raft_kafka": "Raft + Kafka cluster",
}

COLORS = {
    "single_node_pg": "#2563eb",
    "single_node_det": "#16a34a",
    "single_node_gateway_direct": "#0891b2",
    "cluster_kafka": "#f59e0b",
    "cluster_raft_kafka": "#dc2626",
}

MARKERS = {
    "single_node_pg": "o",
    "single_node_det": "s",
    "single_node_gateway_direct": "D",
    "cluster_kafka": "P",
    "cluster_raft_kafka": "^",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _as_float(value: str | None) -> float | None:
    if value is None or value.strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _as_int(value: str | None) -> int | None:
    if value is None or value.strip() == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _threads(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _best_single_rows(single_root: Path, threads: list[int]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for mode, series in (("pg", "single_node_pg"), ("det", "single_node_det")):
        for th in threads:
            candidates: list[tuple[float, str, Path, dict[str, str]]] = []
            for summary in sorted(single_root.glob("single_*/summary.csv")):
                machine = summary.parent.name.removeprefix("single_")
                for row in _read_csv(summary):
                    if (row.get("mode") or "") != mode:
                        continue
                    if _as_int(row.get("threads")) != th:
                        continue
                    if (row.get("pass_rate_merkle_verify") or "") not in {"1.000", "1", "1.0"}:
                        continue
                    if _as_float(row.get("mean_permanent_failures")) not in (0.0, None):
                        continue
                    tps = _as_float(row.get("median_throughput_tps"))
                    if tps is not None:
                        candidates.append((tps, machine, summary, row))
            if not candidates:
                continue
            tps, machine, summary, row = max(candidates, key=lambda x: x[0])
            out.append({
                "series": series,
                "thread": str(th),
                "tps": f"{tps:.6f}",
                "stat": "best_valid_single_node_median_tps",
                "runs": row.get("runs", ""),
                "valid_runs": row.get("runs", ""),
                "effective_inflight": str(th),
                "source_artifact": str(summary.parent),
                "source_machine": machine,
                "source_series": mode,
                "notes": "best valid raw single-node result across available hosts",
            })
    return out


def _summary_rows(summary: Path,
                  threads: list[int],
                  *,
                  series: list[str],
                  only_threads: set[int] | None = None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in _read_csv(summary):
        s = row.get("series") or ""
        th = _as_int(row.get("thread"))
        if s not in series or th is None or th not in threads:
            continue
        if only_threads is not None and th not in only_threads:
            continue
        if row.get("valid_runs") != row.get("runs"):
            continue
        tps = _as_float(row.get("trimmed_mean_tps")) or _as_float(row.get("median_tps"))
        if tps is None:
            continue
        out.append({
            "series": s,
            "thread": str(th),
            "tps": f"{tps:.6f}",
            "stat": "trimmed_mean_tps",
            "runs": row.get("runs", ""),
            "valid_runs": row.get("valid_runs", ""),
            "effective_inflight": row.get("effective_inflight", ""),
            "source_artifact": str(summary.parent),
            "source_machine": row.get("machine", ""),
            "source_series": s,
            "notes": row.get("experiment_mode", ""),
        })
    return out


def _write_csv(rows: list[dict[str, str]], path: Path) -> None:
    fields = [
        "series",
        "thread",
        "tps",
        "stat",
        "runs",
        "valid_runs",
        "effective_inflight",
        "source_artifact",
        "source_machine",
        "source_series",
        "notes",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def _write_overhead(rows: list[dict[str, str]], path: Path) -> None:
    by_key = {(r["series"], r["thread"]): _as_float(r.get("tps")) for r in rows}
    threads = sorted({_as_int(r["thread"]) for r in rows if _as_int(r["thread"]) is not None})
    fields = [
        "thread",
        "pg_tps",
        "det_tps",
        "direct_gateway_tps",
        "kafka_tps",
        "raft_kafka_tps",
        "det_vs_pg_pct",
        "direct_vs_det_pct",
        "kafka_vs_direct_pct",
        "raft_vs_kafka_pct",
        "raft_overhead_pct_from_tps",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for th in threads:
            key = str(th)
            pg = by_key.get(("single_node_pg", key))
            det = by_key.get(("single_node_det", key))
            direct = by_key.get(("single_node_gateway_direct", key))
            kafka = by_key.get(("cluster_kafka", key))
            raft = by_key.get(("cluster_raft_kafka", key))
            pct = lambda a, b: f"{(100.0 * (a - b) / b):.3f}" if a is not None and b else ""
            w.writerow({
                "thread": key,
                "pg_tps": f"{pg:.6f}" if pg is not None else "",
                "det_tps": f"{det:.6f}" if det is not None else "",
                "direct_gateway_tps": f"{direct:.6f}" if direct is not None else "",
                "kafka_tps": f"{kafka:.6f}" if kafka is not None else "",
                "raft_kafka_tps": f"{raft:.6f}" if raft is not None else "",
                "det_vs_pg_pct": pct(det, pg),
                "direct_vs_det_pct": pct(direct, det),
                "kafka_vs_direct_pct": pct(kafka, direct),
                "raft_vs_kafka_pct": pct(raft, kafka),
                "raft_overhead_pct_from_tps": f"{(100.0 * (kafka - raft) / kafka):.3f}" if kafka and raft is not None else "",
            })


def _plot(rows: list[dict[str, str]], out: Path, workload: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10.5, 6.0), dpi=150)
    for series, label in PLOT_SERIES_LABELS.items():
        pts = []
        for row in rows:
            if row.get("series") != series:
                continue
            th = _as_int(row.get("thread"))
            tps = _as_float(row.get("tps"))
            if th is not None and tps is not None:
                pts.append((th, tps))
        pts.sort()
        if not pts:
            continue
        ax.plot(
            [p[0] for p in pts],
            [p[1] for p in pts],
            marker=MARKERS[series],
            color=COLORS[series],
            linewidth=2.2,
            markersize=5.5,
            label=label,
        )
    ax.set_xlabel("Ordered concurrency budget (raw PG/DET use matching client-thread labels)")
    ax.set_ylabel("Valid TPS")
    ax.set_title(f"YCSB skew capacity comparison\n{workload}")
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--single-root", required=True, type=Path)
    ap.add_argument("--gateway-cluster-summary", required=True, type=Path)
    ap.add_argument("--cluster-override-summary", type=Path)
    ap.add_argument("--cluster-override-threads", default="")
    ap.add_argument("--threads", required=True)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    threads = _threads(args.threads)
    rows = _best_single_rows(args.single_root, threads)
    rows.extend(_summary_rows(
        args.gateway_cluster_summary,
        threads,
        series=["single_node_gateway_direct", "cluster_kafka", "cluster_raft_kafka"],
    ))
    if args.cluster_override_summary is not None and args.cluster_override_threads:
        override_threads = set(_threads(args.cluster_override_threads))
        rows = [
            r for r in rows
            if not (r["series"] in {"cluster_kafka", "cluster_raft_kafka"} and _as_int(r["thread"]) in override_threads)
        ]
        rows.extend(_summary_rows(
            args.cluster_override_summary,
            threads,
            series=["cluster_kafka", "cluster_raft_kafka"],
            only_threads=override_threads,
        ))
    rows.sort(key=lambda r: (int(r["thread"]), list(SERIES_LABELS).index(r["series"])))

    _write_csv(rows, args.out_dir / "capacity_summary.csv")
    _write_overhead(rows, args.out_dir / "capacity_overhead.csv")
    _plot(rows, args.out_dir / "ycsb_skew_capacity_all_systems.png", args.workload)
    print(f"Wrote {args.out_dir / 'capacity_summary.csv'}")
    print(f"Wrote {args.out_dir / 'capacity_overhead.csv'}")
    print(f"Wrote {args.out_dir / 'ycsb_skew_capacity_all_systems.png'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
