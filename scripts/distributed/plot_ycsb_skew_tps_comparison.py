#!/usr/bin/env python3
"""Combine single-node PG/DET and trusted 4-node full-system YCSB TPS results."""

from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path
from statistics import median
from typing import Any


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _mean(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


def _stddev(values: list[float]) -> float | None:
    if not values:
        return None
    mu = sum(values) / len(values)
    return math.sqrt(sum((x - mu) ** 2 for x in values) / len(values))


def _trim_one_outlier(values: list[float]) -> tuple[list[float], int | None]:
    """For 3-run groups, drop the value farthest from the median."""
    if len(values) < 3:
        return values, None
    med = median(values)
    drop_idx = max(range(len(values)), key=lambda i: abs(values[i] - med))
    return [v for i, v in enumerate(values) if i != drop_idx], drop_idx


def _fmt(value: float | None, ndigits: int = 3) -> str:
    if value is None:
        return ""
    return f"{value:.{ndigits}f}"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _parse_threads(value: str) -> set[int] | None:
    value = value.strip()
    if not value:
        return None
    out: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(int(part))
    return out


def _parse_gateway_log(path: Path) -> dict[str, Any]:
    text = path.read_text(errors="replace") if path.exists() else ""

    def first(pattern: str) -> str:
        m = re.search(pattern, text, flags=re.MULTILINE)
        return m.group(1) if m else ""

    loaded = _as_int(first(r"^loaded\s+([0-9]+)\s+queries")) or 0
    overall_ms = _as_float(first(r"^overall time taken \(millisec\)\s*=\s*([0-9.]+)"))
    submit_ms = _as_float(first(r"^\s*submit time \(ms\)\s*([0-9.]+)"))
    majority_wait_ms = _as_float(first(r"^\s*majority wait time \(ms\)\s*([0-9.]+)"))
    divergence = _as_int(first(r"^divergence_count=([0-9]+)"))
    permanent = _as_int(first(r"^permanent_failures=([0-9]+)"))
    completion_path = first(r"completion_path=([A-Za-z0-9_]+)")
    wait_majority = _as_int(first(r"waitMajority=([0-9]+)"))
    tps = (1000.0 * loaded / overall_ms) if loaded and overall_ms and overall_ms > 0 else None
    valid = (
        completion_path == "kafka_majority"
        and wait_majority == 1
        and divergence == 0
        and permanent == 0
        and tps is not None
    )
    return {
        "loaded_queries": loaded,
        "overall_ms": overall_ms,
        "submit_ms": submit_ms,
        "majority_wait_ms": majority_wait_ms,
        "divergence_count": divergence,
        "permanent_failures": permanent,
        "completion_path": completion_path,
        "wait_majority": wait_majority,
        "tps": tps,
        "valid": valid,
    }


def _load_single_rows(single_results: Path, *, workload: str, machine: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for r in _read_csv(single_results):
        mode = (r.get("mode") or "").strip()
        if mode not in {"pg", "det"}:
            continue
        if (r.get("workload") or "").strip() != workload:
            continue
        signing = _as_int(r.get("signing"))
        if mode == "det" and signing not in (0, None):
            continue
        threads = _as_int(r.get("threads"))
        run = _as_int(r.get("run"))
        overall_ms = _as_float(r.get("workload_overall_ms"))
        tps = (1000.0 * 20000.0 / overall_ms) if overall_ms and overall_ms > 0 else None
        valid = (
            tps is not None
            and (_as_int(r.get("start_server_exit")) or 0) == 0
            and (_as_int(r.get("restore_exit")) or 0) == 0
            and (_as_int(r.get("py_exit")) or 0) == 0
            and (r.get("db_merkle_verify") or "").strip().lower() == "t"
        )
        tps_s = _fmt(tps, 6)
        out.append(
            {
                "system": "single_node",
                "series": f"single_node_{mode}",
                "mode": mode,
                "machine": machine,
                "thread": str(threads or ""),
                "run": str(run or ""),
                "workload": workload,
                "tps": tps_s if valid else "",
                "raw_tps": tps_s,
                "valid_tps": tps_s if valid else "",
                "overall_ms": _fmt(overall_ms, 3),
                "valid": "1" if valid else "0",
                "completion_path": "direct_pg" if mode == "pg" else "direct_bcdb_det",
                "wait_majority": "0",
                "divergence_count": "",
                "permanent_failures": str(_as_int(r.get("permanent_failures")) or 0),
                "artifact_dir": str(single_results.parent),
                "invalid_reason": "" if valid else "single_node_runner_or_merkle_failed",
                "notes": "single-node bench_threads_matrix signing=0",
            }
        )
    return out


def _load_full_rows(manifest: Path, *, workload: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for m in _read_csv(manifest):
        artifact = Path(m.get("artifact_dir") or "")
        parsed = _parse_gateway_log(artifact / "gateway_test.log")
        exit_ok = (m.get("exit_code") or "1") == "0"
        valid = parsed["valid"] and exit_ok
        invalid_reasons: list[str] = []
        if not exit_ok:
            invalid_reasons.append(f"runner_exit_{m.get('exit_code') or 'missing'}")
        if parsed["completion_path"] != "kafka_majority":
            invalid_reasons.append("not_kafka_majority")
        if parsed["wait_majority"] != 1:
            invalid_reasons.append("wait_majority_not_1")
        if parsed["divergence_count"] not in (0, None):
            invalid_reasons.append(f"divergence_{parsed['divergence_count']}")
        if parsed["permanent_failures"] not in (0, None):
            invalid_reasons.append(f"permanent_failures_{parsed['permanent_failures']}")
        if parsed["tps"] is None:
            invalid_reasons.append("missing_gateway_tps")
        raw_tps = _fmt(parsed["tps"], 6)
        out.append(
            {
                "system": "full_system",
                "series": "full_system_kafka_raft_bcdb",
                "mode": "kafka_raft_bcdb",
                "machine": "4node_cluster",
                "thread": m.get("thread", ""),
                "run": m.get("run", ""),
                "workload": workload,
                "tps": raw_tps if valid else "",
                "raw_tps": raw_tps,
                "valid_tps": raw_tps if valid else "",
                "overall_ms": _fmt(parsed["overall_ms"], 3),
                "valid": "1" if valid else "0",
                "completion_path": str(parsed["completion_path"]),
                "wait_majority": str(parsed["wait_majority"] or ""),
                "divergence_count": str(parsed["divergence_count"] if parsed["divergence_count"] is not None else ""),
                "permanent_failures": str(parsed["permanent_failures"] if parsed["permanent_failures"] is not None else ""),
                "artifact_dir": str(artifact),
                "invalid_reason": ";".join(invalid_reasons),
                "notes": m.get("notes", ""),
            }
        )
    return out


def _write_results(rows: list[dict[str, str]], path: Path) -> None:
    fields = [
        "system",
        "series",
        "mode",
        "machine",
        "thread",
        "run",
        "workload",
        "tps",
        "raw_tps",
        "valid_tps",
        "overall_ms",
        "valid",
        "completion_path",
        "wait_majority",
        "divergence_count",
        "permanent_failures",
        "artifact_dir",
        "invalid_reason",
        "notes",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def _write_summary(rows: list[dict[str, str]], path: Path) -> list[dict[str, str]]:
    groups: dict[tuple[str, int], list[dict[str, str]]] = {}
    for r in rows:
        th = _as_int(r.get("thread"))
        if th is None:
            continue
        groups.setdefault((r.get("series", ""), th), []).append(r)

    out: list[dict[str, str]] = []
    for (series, th), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        valid_rs = [r for r in rs if (r.get("valid") or "") == "1"]
        vals = [_as_float(r.get("tps")) for r in valid_rs]
        vals = [v for v in vals if v is not None]
        trimmed, drop_idx = _trim_one_outlier(vals)
        representative = valid_rs[0] if valid_rs else (rs[0] if rs else {})
        out.append(
            {
                "series": series,
                "system": representative.get("system", ""),
                "mode": representative.get("mode", ""),
                "thread": str(th),
                "runs": str(len(rs)),
                "valid_runs": str(len(valid_rs)),
                "mean_tps": _fmt(_mean(vals), 6),
                "median_tps": _fmt(median(vals) if vals else None, 6),
                "trimmed_mean_tps": _fmt(_mean(trimmed), 6),
                "dropped_outlier_index_in_valid_values": "" if drop_idx is None else str(drop_idx),
                "min_tps": _fmt(min(vals) if vals else None, 6),
                "max_tps": _fmt(max(vals) if vals else None, 6),
                "stdev_tps": _fmt(_stddev(vals), 6),
                "mean_overall_ms": _fmt(_mean([x for x in (_as_float(r.get("overall_ms")) for r in valid_rs) if x is not None]), 3),
            }
        )

    with path.open("w", newline="") as f:
        fields = list(out[0].keys()) if out else [
            "series", "system", "mode", "thread", "runs", "valid_runs", "mean_tps",
            "median_tps", "trimmed_mean_tps", "dropped_outlier_index_in_valid_values",
            "min_tps", "max_tps", "stdev_tps", "mean_overall_ms",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in out:
            w.writerow(row)
    return out


def _write_overhead(summary: list[dict[str, str]], path: Path) -> None:
    by_key = {(r["series"], r["thread"]): r for r in summary}
    threads = sorted({_as_int(r["thread"]) for r in summary if _as_int(r["thread"]) is not None})
    rows: list[dict[str, str]] = []
    for th in threads:
        key = str(th)
        pg = _as_float(by_key.get(("single_node_pg", key), {}).get("trimmed_mean_tps"))
        det = _as_float(by_key.get(("single_node_det", key), {}).get("trimmed_mean_tps"))
        full = _as_float(by_key.get(("full_system_kafka_raft_bcdb", key), {}).get("trimmed_mean_tps"))
        row = {"thread": key}
        row["single_det_vs_pg_pct"] = _fmt((100.0 * (det - pg) / pg) if pg and det is not None else None, 3)
        row["full_vs_single_pg_pct"] = _fmt((100.0 * (full - pg) / pg) if pg and full is not None else None, 3)
        row["full_vs_single_det_pct"] = _fmt((100.0 * (full - det) / det) if det and full is not None else None, 3)
        row["single_pg_tps"] = _fmt(pg, 6)
        row["single_det_tps"] = _fmt(det, 6)
        row["full_system_tps"] = _fmt(full, 6)
        rows.append(row)
    with path.open("w", newline="") as f:
        fields = [
            "thread",
            "single_pg_tps",
            "single_det_tps",
            "full_system_tps",
            "single_det_vs_pg_pct",
            "full_vs_single_pg_pct",
            "full_vs_single_det_pct",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _plot(summary: list[dict[str, str]], out_dir: Path, workload: str, machine: str) -> Path:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise RuntimeError(f"matplotlib is required for graph generation: {exc}") from exc

    labels = {
        "single_node_pg": f"Single node PG ({machine})",
        "single_node_det": f"Single node DET unsigned ({machine})",
        "full_system_kafka_raft_bcdb": "4-node Kafka+Raft+BCDB trusted",
    }
    colors = {
        "single_node_pg": "#2563eb",
        "single_node_det": "#16a34a",
        "full_system_kafka_raft_bcdb": "#dc2626",
    }
    markers = {
        "single_node_pg": "o",
        "single_node_det": "s",
        "full_system_kafka_raft_bcdb": "^",
    }
    fig, ax = plt.subplots(figsize=(10.5, 6.0), dpi=140)
    for series in labels:
        pts = []
        for r in summary:
            if r.get("series") != series:
                continue
            th = _as_int(r.get("thread"))
            tps = _as_float(r.get("trimmed_mean_tps")) or _as_float(r.get("median_tps"))
            if th is not None and tps is not None:
                pts.append((th, tps))
        pts.sort()
        if not pts:
            continue
        ax.plot(
            [p[0] for p in pts],
            [p[1] for p in pts],
            marker=markers[series],
            linewidth=2.2,
            markersize=5.5,
            color=colors[series],
            label=labels[series],
        )
    ax.set_xlabel("Threads (single node) / ordered concurrency budget (full system)")
    ax.set_ylabel("Valid TPS (3-run mean with one outlier removed)")
    ax.set_title(f"YCSB skew full workload TPS comparison\n{workload}")
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
    ax.legend()
    fig.tight_layout()
    out = out_dir / "ycsb_skew_tps_comparison.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--single-results", required=True, type=Path)
    ap.add_argument("--full-manifest", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--machine", default="10.129.148.248")
    ap.add_argument("--threads", default="", help="Optional CSV thread filter for graph-ready output")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    thread_filter = _parse_threads(args.threads)
    rows = []
    rows.extend(_load_single_rows(args.single_results, workload=args.workload, machine=args.machine))
    rows.extend(_load_full_rows(args.full_manifest, workload=args.workload))
    if thread_filter is not None:
        rows = [r for r in rows if (_as_int(r.get("thread")) in thread_filter)]
    rows.sort(key=lambda r: (r["series"], int(r["thread"] or 0), int(r["run"] or 0)))

    _write_results(rows, args.out_dir / "results.csv")
    summary = _write_summary(rows, args.out_dir / "summary.csv")
    _write_overhead(summary, args.out_dir / "overhead.csv")
    graph = _plot(summary, args.out_dir, args.workload, args.machine)
    print(f"Wrote results:  {args.out_dir / 'results.csv'}")
    print(f"Wrote summary:  {args.out_dir / 'summary.csv'}")
    print(f"Wrote overhead: {args.out_dir / 'overhead.csv'}")
    print(f"Wrote graph:    {graph}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
