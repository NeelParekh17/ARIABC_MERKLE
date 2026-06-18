#!/usr/bin/env python3
"""Combine single-node PG/DET and trusted 4-node cluster YCSB TPS results."""

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


def _first_present(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = (row.get(name) or "").strip()
        if value != "":
            return value
    return ""


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

    def last(pattern: str) -> str:
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        return matches[-1] if matches else ""

    loaded = _as_int(first(r"^loaded\s+([0-9]+)\s+queries")) or 0
    progress_total = _as_int(last(r"^PROGRESS_GATEWAY_DET\b.*\btotal=([0-9]+)"))
    progress_sent = _as_int(last(r"^PROGRESS_GATEWAY_DET\b.*\bsent=([0-9]+)"))
    progress_accepted = _as_int(last(r"^PROGRESS_GATEWAY_DET\b.*\baccepted=([0-9]+)"))
    progress_completed = _as_int(last(r"^PROGRESS_GATEWAY_DET\b.*\bcompleted=([0-9]+)"))
    overall_ms = _as_float(first(r"^overall time taken \(millisec\)\s*=\s*([0-9.]+)"))
    submit_ms = _as_float(first(r"^\s*submit time \(ms\)\s*([0-9.]+)"))
    majority_wait_ms = _as_float(first(r"^\s*majority wait time \(ms\)\s*([0-9.]+)"))
    divergence = _as_int(first(r"^divergence_count=([0-9]+)"))
    permanent = _as_int(first(r"^permanent_failures=([0-9]+)"))
    completion_path = first(r"completion_path=([A-Za-z0-9_]+)")
    validation_mode = first(r"validation_mode=([A-Za-z0-9_]+)")
    wait_majority = _as_int(first(r"waitMajority=([0-9]+)"))
    broadcast_to_all = _as_int(first(r"broadcastToAll=([0-9]+)"))
    if broadcast_to_all is None:
        broadcast_to_all = _as_int(first(r"broadcast_to_all=([0-9]+)"))
    tps_denominator = progress_completed or progress_accepted or loaded
    tps = (1000.0 * tps_denominator / overall_ms) if tps_denominator and overall_ms and overall_ms > 0 else None
    # Apples-to-apples requires that EVERY workload statement actually
    # completed. A partially-finished run can otherwise produce a plausible TPS
    # number from progress_accepted alone and silently enter the graph.
    fully_completed = (
        loaded > 0
        and progress_completed is not None
        and progress_completed == loaded
    )
    correctly_completed = (
        divergence == 0
        and permanent == 0
        and tps is not None
        and fully_completed
    )
    return {
        "loaded_queries": loaded,
        "progress_total": progress_total,
        "progress_sent": progress_sent,
        "progress_accepted": progress_accepted,
        "progress_completed": progress_completed,
        "tps_denominator": tps_denominator,
        "overall_ms": overall_ms,
        "submit_ms": submit_ms,
        "majority_wait_ms": majority_wait_ms,
        "divergence_count": divergence,
        "permanent_failures": permanent,
        "completion_path": completion_path,
        "validation_mode": validation_mode,
        "wait_majority": wait_majority,
        "broadcast_to_all": broadcast_to_all,
        "tps": tps,
        "valid": correctly_completed,
        "fully_completed": fully_completed,
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
        executed = _as_int(r.get("statement_count"))
        workload_log = Path(r.get("workload_log") or "")
        if executed is None or executed <= 0:
            executed = _count_sql_statements_from_log(workload_log)
        if executed is None:
            executed = _infer_workload_statement_count(workload)
        tps = (1000.0 * executed / overall_ms) if executed and overall_ms and overall_ms > 0 else None
        permanent_failures = _as_int(r.get("permanent_failures")) or 0
        valid = (
            tps is not None
            and (_as_int(r.get("start_server_exit")) or 0) == 0
            and (_as_int(r.get("restore_exit")) or 0) == 0
            and (_as_int(r.get("py_exit")) or 0) == 0
            and (r.get("db_merkle_verify") or "").strip().lower() == "t"
            and permanent_failures == 0
        )
        invalid_reason = ""
        if not valid:
            if permanent_failures > 0:
                invalid_reason = f"permanent_failures_{permanent_failures}"
            else:
                invalid_reason = "single_node_runner_or_merkle_failed"
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
                "experiment_mode": "raw-single-node",
                "tps_denominator": str(executed or ""),
                "loaded_queries": str(executed or ""),
                "accepted_queries": str(executed or "") if valid else "",
                "completed_queries": str(executed or "") if valid else "",
                "effective_inflight": str(threads or ""),
                "pool_size": "",
                "bcdb_worker_count": "",
                "det_batch_size": "",
                "det_window": "",
                "completion_path": "direct_pg" if mode == "pg" else "direct_bcdb_det",
                "wait_majority": "0",
                "divergence_count": "",
                "permanent_failures": str(permanent_failures),
                "artifact_dir": str(single_results.parent),
                "invalid_reason": invalid_reason,
                "notes": ";".join(
                    part for part in [
                        "single-node bench_threads_matrix signing=0",
                        r.get("notes", ""),
                    ] if part
                ),
            }
        )
    return out


def _load_full_rows(manifest: Path, *, workload: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for m in _read_csv(manifest):
        artifact = Path(m.get("artifact_dir") or "")
        parsed = _parse_gateway_log(artifact / "gateway_test.log")
        series = (m.get("series") or "").strip()
        if not series:
            series = "cluster_raft_kafka"
        mode = (m.get("mode") or "").strip()
        if not mode:
            mode = "kafka_raft_bcdb" if series in {"cluster_raft_kafka", "full_system_kafka_raft_bcdb"} else "kafka_only_bcdb"
        ordering_mode = (m.get("ordering_mode") or "").strip()
        ordering_path = (m.get("ordering_path") or "").strip()
        if not ordering_path:
            ordering_path = "preordered_direct_broadcast" if series == "cluster_kafka" else "raft"
        server_bypass_raft = (m.get("server_bypass_raft") or "").strip()
        gateway_broadcast_to_all = (m.get("gateway_broadcast_to_all") or "").strip()
        if not gateway_broadcast_to_all and parsed["broadcast_to_all"] is not None:
            gateway_broadcast_to_all = str(parsed["broadcast_to_all"])
        exit_ok = (m.get("exit_code") or "1") == "0"
        num_terminals = _as_int(m.get("num_terminals"))
        det_pipeline_depth = _as_int(m.get("det_pipeline_depth"))
        effective_inflight = _as_int(m.get("effective_inflight"))
        if effective_inflight is None and num_terminals is not None and det_pipeline_depth is not None:
            effective_inflight = num_terminals * max(det_pipeline_depth, 1)
        notes = m.get("notes", "")
        majority_completion = (
            parsed["completion_path"] == "kafka_majority"
            and parsed["wait_majority"] == 1
        )
        async_kafka_completion = (
            parsed["completion_path"] == "direct"
            and parsed["wait_majority"] in (0, None)
            and parsed["validation_mode"] == "async_hash"
            and (
                "kafka_completion_mode=async" in notes
                or "trusted_gate=async_kafka_post_marker_merkle" in notes
            )
        )
        completion_valid = majority_completion or async_kafka_completion
        valid = parsed["valid"] and exit_ok and completion_valid
        invalid_reasons: list[str] = []
        if not exit_ok:
            invalid_reasons.append(f"runner_exit_{m.get('exit_code') or 'missing'}")
        if not completion_valid:
            if parsed["completion_path"] == "kafka_majority" and parsed["wait_majority"] != 1:
                invalid_reasons.append("kafka_majority_without_wait")
            elif parsed["completion_path"] == "direct":
                invalid_reasons.append("direct_without_async_kafka_merkle_gate")
            else:
                invalid_reasons.append(f"unsupported_completion_{parsed['completion_path'] or 'missing'}")
        if parsed["divergence_count"] not in (0, None):
            invalid_reasons.append(f"divergence_{parsed['divergence_count']}")
        if parsed["permanent_failures"] not in (0, None):
            invalid_reasons.append(f"permanent_failures_{parsed['permanent_failures']}")
        if parsed["tps"] is None:
            invalid_reasons.append("missing_gateway_tps")
        if not parsed["fully_completed"]:
            invalid_reasons.append(
                f"partial_completed_{parsed['progress_completed']}_of_{parsed['loaded_queries']}"
            )
        raw_tps = _fmt(parsed["tps"], 6)
        out.append(
            {
                "system": "cluster",
                "series": series,
                "mode": mode,
                "machine": "4node_cluster",
                "thread": m.get("thread", ""),
                "run": m.get("run", ""),
                "workload": workload,
                "tps": raw_tps if valid else "",
                "raw_tps": raw_tps,
                "valid_tps": raw_tps if valid else "",
                "overall_ms": _fmt(parsed["overall_ms"], 3),
                "valid": "1" if valid else "0",
                "experiment_mode": _first_present(m, "experiment_mode") or "pipeline-saturation",
                "tps_denominator": str(parsed["tps_denominator"] or ""),
                "loaded_queries": str(parsed["loaded_queries"] or ""),
                "accepted_queries": str(parsed["progress_accepted"] or ""),
                "completed_queries": str(parsed["progress_completed"] or ""),
                "effective_inflight": str(effective_inflight if effective_inflight is not None else ""),
                "pool_size": m.get("pool_size", ""),
                "bcdb_worker_count": m.get("bcdb_worker_count", ""),
                "det_batch_size": m.get("det_batch_size", ""),
                "det_window": m.get("det_window", ""),
                "num_terminals": m.get("num_terminals", ""),
                "det_pipeline_depth": m.get("det_pipeline_depth", ""),
                "ordering_mode": ordering_mode,
                "ordering_path": ordering_path,
                "completion_path": str(parsed["completion_path"]),
                "wait_majority": str(parsed["wait_majority"] if parsed["wait_majority"] is not None else ""),
                "server_bypass_raft": server_bypass_raft,
                "gateway_broadcast_to_all": gateway_broadcast_to_all,
                "divergence_count": str(parsed["divergence_count"] if parsed["divergence_count"] is not None else ""),
                "permanent_failures": str(parsed["permanent_failures"] if parsed["permanent_failures"] is not None else ""),
                "artifact_dir": str(artifact),
                "invalid_reason": ";".join(invalid_reasons),
                "notes": notes,
            }
        )
    return out


def _load_single_gateway_rows(manifest: Path, *, workload: str, machine: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not manifest.exists():
        return out
    for m in _read_csv(manifest):
        artifact = Path(m.get("artifact_dir") or "")
        parsed = _parse_gateway_log(artifact / "gateway_test.log")
        exit_ok = (m.get("exit_code") or "1") == "0"
        num_terminals = _as_int(m.get("num_terminals"))
        det_pipeline_depth = _as_int(m.get("det_pipeline_depth"))
        effective_inflight = _as_int(m.get("effective_inflight"))
        if effective_inflight is None and num_terminals is not None and det_pipeline_depth is not None:
            effective_inflight = num_terminals * max(det_pipeline_depth, 1)
        valid = (
            exit_ok
            and parsed["completion_path"] == "direct"
            and parsed["divergence_count"] in (0, None)
            and parsed["permanent_failures"] in (0, None)
            and parsed["tps"] is not None
            and parsed["fully_completed"]
        )
        invalid_reasons: list[str] = []
        if not exit_ok:
            invalid_reasons.append(f"runner_exit_{m.get('exit_code') or 'missing'}")
        if parsed["completion_path"] != "direct":
            invalid_reasons.append("not_direct_completion")
        if parsed["divergence_count"] not in (0, None):
            invalid_reasons.append(f"divergence_{parsed['divergence_count']}")
        if parsed["permanent_failures"] not in (0, None):
            invalid_reasons.append(f"permanent_failures_{parsed['permanent_failures']}")
        if parsed["tps"] is None:
            invalid_reasons.append("missing_gateway_tps")
        if not parsed["fully_completed"]:
            invalid_reasons.append(
                f"partial_completed_{parsed['progress_completed']}_of_{parsed['loaded_queries']}"
            )
        raw_tps = _fmt(parsed["tps"], 6)
        out.append(
            {
                "system": "single_node",
                "series": "single_node_gateway_direct",
                "mode": "gateway_direct_bcdb",
                "machine": machine,
                "thread": m.get("thread", ""),
                "run": m.get("run", ""),
                "workload": workload,
                "tps": raw_tps if valid else "",
                "raw_tps": raw_tps,
                "valid_tps": raw_tps if valid else "",
                "overall_ms": _fmt(parsed["overall_ms"], 3),
                "valid": "1" if valid else "0",
                "experiment_mode": _first_present(m, "experiment_mode") or "gateway-direct",
                "tps_denominator": str(parsed["tps_denominator"] or ""),
                "loaded_queries": str(parsed["loaded_queries"] or ""),
                "accepted_queries": str(parsed["progress_accepted"] or ""),
                "completed_queries": str(parsed["progress_completed"] or ""),
                "effective_inflight": str(effective_inflight if effective_inflight is not None else ""),
                "pool_size": m.get("pool_size", ""),
                "bcdb_worker_count": m.get("bcdb_worker_count", ""),
                "det_batch_size": m.get("det_batch_size", ""),
                "det_window": m.get("det_window", ""),
                "num_terminals": m.get("num_terminals", ""),
                "det_pipeline_depth": m.get("det_pipeline_depth", ""),
                "ordering_mode": "direct",
                "ordering_path": "single_node_gateway",
                "completion_path": str(parsed["completion_path"]),
                "wait_majority": str(parsed["wait_majority"] or "0"),
                "server_bypass_raft": "1",
                "gateway_broadcast_to_all": "0",
                "divergence_count": str(parsed["divergence_count"] if parsed["divergence_count"] is not None else ""),
                "permanent_failures": str(parsed["permanent_failures"] if parsed["permanent_failures"] is not None else ""),
                "artifact_dir": str(artifact),
                "invalid_reason": ";".join(invalid_reasons),
                "notes": m.get("notes", ""),
            }
        )
    return out


def _infer_workload_statement_count(workload: str) -> int:
    for candidate in (
        Path(workload),
        Path("scripts") / workload,
        Path(__file__).resolve().parents[1] / workload,
    ):
        count = _count_sql_statements(candidate)
        if count is not None:
            return count
    # Existing YCSB skew workload names encode their logical statement count.
    m = re.search(r"tx[_-]([0-9]+)k\b", workload)
    if m:
        return int(m.group(1)) * 1000
    return 20000


def _count_sql_statements_from_log(workload_log: Path) -> int | None:
    # bench_threads_matrix records the workload path in results.csv and the
    # workload runner logs every input statement only in verbose modes. Prefer
    # reading the source file when the path is available and local.
    if not workload_log.exists():
        return None
    text = workload_log.read_text(errors="replace")
    m = re.search(r"Query data file '([^']+)'", text)
    if m:
        p = Path(m.group(1))
        if p.exists():
            return _count_sql_statements(p)
    return None


def _count_sql_statements(path: Path) -> int | None:
    if not path.exists():
        return None
    count = 0
    for line in path.read_text(errors="replace").splitlines():
        stripped = line.strip()
        if (
            not stripped
            or stripped.startswith("--")
            or stripped.startswith("/*")
            or stripped.startswith("\\")
        ):
            continue
        count += 1
    return count


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
        "experiment_mode",
        "tps_denominator",
        "loaded_queries",
        "accepted_queries",
        "completed_queries",
        "effective_inflight",
        "pool_size",
        "bcdb_worker_count",
        "det_batch_size",
        "det_window",
        "num_terminals",
        "det_pipeline_depth",
        "ordering_mode",
        "ordering_path",
        "completion_path",
        "wait_majority",
        "server_bypass_raft",
        "gateway_broadcast_to_all",
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
                "experiment_mode": representative.get("experiment_mode", ""),
                "mean_tps_denominator": _fmt(_mean([x for x in (_as_float(r.get("tps_denominator")) for r in valid_rs) if x is not None]), 3),
                "effective_inflight": representative.get("effective_inflight", ""),
                "num_terminals": representative.get("num_terminals", ""),
                "det_pipeline_depth": representative.get("det_pipeline_depth", ""),
                "pool_size": representative.get("pool_size", ""),
                "bcdb_worker_count": representative.get("bcdb_worker_count", ""),
                "det_batch_size": representative.get("det_batch_size", ""),
                "det_window": representative.get("det_window", ""),
                "completion_path": representative.get("completion_path", ""),
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
            "series", "system", "mode", "thread", "runs", "valid_runs",
            "experiment_mode", "mean_tps_denominator", "effective_inflight",
            "num_terminals", "det_pipeline_depth", "pool_size", "bcdb_worker_count",
            "det_batch_size", "det_window", "completion_path", "mean_tps",
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
        gateway = _as_float(by_key.get(("single_node_gateway_direct", key), {}).get("trimmed_mean_tps"))
        cluster_kafka = _as_float(by_key.get(("cluster_kafka", key), {}).get("trimmed_mean_tps"))
        cluster_raft = _as_float(by_key.get(("cluster_raft_kafka", key), {}).get("trimmed_mean_tps"))
        if cluster_raft is None:
            cluster_raft = _as_float(by_key.get(("full_system_kafka_raft_bcdb", key), {}).get("trimmed_mean_tps"))
        row = {"thread": key}
        row["single_det_vs_pg_pct"] = _fmt((100.0 * (det - pg) / pg) if pg and det is not None else None, 3)
        row["single_gateway_vs_single_det_pct"] = _fmt((100.0 * (gateway - det) / det) if det and gateway is not None else None, 3)
        row["cluster_kafka_vs_single_det_pct"] = _fmt((100.0 * (cluster_kafka - det) / det) if det and cluster_kafka is not None else None, 3)
        row["cluster_raft_vs_single_det_pct"] = _fmt((100.0 * (cluster_raft - det) / det) if det and cluster_raft is not None else None, 3)
        row["cluster_kafka_vs_single_gateway_pct"] = _fmt((100.0 * (cluster_kafka - gateway) / gateway) if gateway and cluster_kafka is not None else None, 3)
        row["cluster_raft_vs_single_gateway_pct"] = _fmt((100.0 * (cluster_raft - gateway) / gateway) if gateway and cluster_raft is not None else None, 3)
        row["cluster_raft_vs_cluster_kafka_pct"] = _fmt((100.0 * (cluster_raft - cluster_kafka) / cluster_kafka) if cluster_kafka and cluster_raft is not None else None, 3)
        row["raft_overhead_pct_from_tps"] = _fmt((100.0 * (cluster_kafka - cluster_raft) / cluster_kafka) if cluster_kafka and cluster_raft is not None else None, 3)
        row["full_vs_single_pg_pct"] = _fmt((100.0 * (cluster_raft - pg) / pg) if pg and cluster_raft is not None else None, 3)
        row["full_vs_single_det_pct"] = _fmt((100.0 * (cluster_raft - det) / det) if det and cluster_raft is not None else None, 3)
        row["full_vs_single_gateway_pct"] = _fmt((100.0 * (cluster_raft - gateway) / gateway) if gateway and cluster_raft is not None else None, 3)
        row["single_pg_tps"] = _fmt(pg, 6)
        row["single_det_tps"] = _fmt(det, 6)
        row["single_gateway_direct_tps"] = _fmt(gateway, 6)
        row["cluster_kafka_tps"] = _fmt(cluster_kafka, 6)
        row["cluster_raft_kafka_tps"] = _fmt(cluster_raft, 6)
        row["full_system_tps"] = _fmt(cluster_raft, 6)
        rows.append(row)
    with path.open("w", newline="") as f:
        fields = [
            "thread",
            "single_pg_tps",
            "single_det_tps",
            "single_gateway_direct_tps",
            "cluster_kafka_tps",
            "cluster_raft_kafka_tps",
            "full_system_tps",
            "single_det_vs_pg_pct",
            "single_gateway_vs_single_det_pct",
            "cluster_kafka_vs_single_det_pct",
            "cluster_raft_vs_single_det_pct",
            "cluster_kafka_vs_single_gateway_pct",
            "cluster_raft_vs_single_gateway_pct",
            "cluster_raft_vs_cluster_kafka_pct",
            "raft_overhead_pct_from_tps",
            "full_vs_single_pg_pct",
            "full_vs_single_det_pct",
            "full_vs_single_gateway_pct",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _write_interpretation(summary: list[dict[str, str]], path: Path) -> None:
    fields = [
        "thread",
        "verdict",
        "reason",
        "single_baseline_series",
        "single_det_tps_denominator",
        "single_gateway_tps_denominator",
        "cluster_kafka_tps_denominator",
        "cluster_raft_tps_denominator",
        "single_gateway_effective_inflight",
        "cluster_kafka_effective_inflight",
        "cluster_raft_effective_inflight",
        "single_gateway_experiment_mode",
        "cluster_kafka_experiment_mode",
        "cluster_raft_experiment_mode",
    ]
    by_key = {(r["series"], r["thread"]): r for r in summary}
    threads = sorted({_as_int(r["thread"]) for r in summary if _as_int(r["thread"]) is not None})
    rows: list[dict[str, str]] = []
    for th in threads:
        key = str(th)
        single_det = by_key.get(("single_node_det", key), {})
        single_gateway = by_key.get(("single_node_gateway_direct", key), {})
        single = single_gateway or single_det
        single_baseline_series = single.get("series", "")
        kafka = by_key.get(("cluster_kafka", key), {})
        raft = by_key.get(("cluster_raft_kafka", key), {})
        if not raft:
            raft = by_key.get(("full_system_kafka_raft_bcdb", key), {})
        single_den = _as_float(single.get("mean_tps_denominator"))
        single_det_den = _as_float(single_det.get("mean_tps_denominator"))
        single_gateway_den = _as_float(single_gateway.get("mean_tps_denominator"))
        kafka_den = _as_float(kafka.get("mean_tps_denominator"))
        raft_den = _as_float(raft.get("mean_tps_denominator"))
        single_gateway_eff = _as_float(single_gateway.get("effective_inflight"))
        kafka_eff = _as_float(kafka.get("effective_inflight"))
        raft_eff = _as_float(raft.get("effective_inflight"))
        single_gateway_mode = single_gateway.get("experiment_mode", "")
        kafka_mode = kafka.get("experiment_mode", "")
        raft_mode = raft.get("experiment_mode", "")

        strict_ready = (
            raft_mode == "strict-overhead"
            and (not kafka or kafka_mode == "strict-overhead")
            and single_baseline_series == "single_node_gateway_direct"
            and single_den is not None
            and raft_den is not None
            and abs(single_den - raft_den) < 0.5
            and (kafka_den is None or abs(single_den - kafka_den) < 0.5)
            and (single_gateway_eff is None or single_gateway_eff == float(th))
            and raft_eff == float(th)
            and (kafka_eff is None or kafka_eff == float(th))
        )
        if strict_ready:
            verdict = "strict-overhead-valid"
            reason = "gateway-direct baseline, same denominator, and same effective inflight"
        else:
            verdict = "pipeline-saturation-only"
            reasons: list[str] = []
            if single_baseline_series != "single_node_gateway_direct":
                reasons.append("missing single-node gateway-direct baseline")
            if raft_mode != "strict-overhead":
                reasons.append("cluster experiment mode is not strict-overhead")
            if single_den is not None and raft_den is not None and abs(single_den - raft_den) >= 0.5:
                reasons.append("single and raft denominators differ")
            if single_gateway_eff is not None and single_gateway_eff != float(th):
                reasons.append("single gateway effective inflight differs from x-axis thread")
            if raft_eff is not None and raft_eff != float(th):
                reasons.append("raft effective inflight differs from x-axis thread")
            if kafka and kafka_mode != "strict-overhead":
                reasons.append("kafka experiment mode is not strict-overhead")
            if kafka_den is not None and single_den is not None and abs(single_den - kafka_den) >= 0.5:
                reasons.append("single and kafka denominators differ")
            if kafka_eff is not None and kafka_eff != float(th):
                reasons.append("kafka effective inflight differs from x-axis thread")
            reason = "; ".join(reasons) or "missing metadata for strict-overhead verdict"

        rows.append({
            "thread": key,
            "verdict": verdict,
            "reason": reason,
            "single_baseline_series": single_baseline_series,
            "single_det_tps_denominator": _fmt(single_det_den, 3),
            "single_gateway_tps_denominator": _fmt(single_gateway_den, 3),
            "cluster_kafka_tps_denominator": _fmt(kafka_den, 3),
            "cluster_raft_tps_denominator": _fmt(raft_den, 3),
            "single_gateway_effective_inflight": _fmt(single_gateway_eff, 3),
            "cluster_kafka_effective_inflight": _fmt(kafka_eff, 3),
            "cluster_raft_effective_inflight": _fmt(raft_eff, 3),
            "single_gateway_experiment_mode": single_gateway_mode,
            "cluster_kafka_experiment_mode": kafka_mode,
            "cluster_raft_experiment_mode": raft_mode,
        })
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _plot_series(summary: list[dict[str, str]],
                 out_dir: Path,
                 workload: str,
                 x_label: str,
                 *,
                 filename: str,
                 title: str,
                 labels: dict[str, str],
                 colors: dict[str, str],
                 markers: dict[str, str],
                 linestyles: dict[str, str] | None = None,
                 alphas: dict[str, float] | None = None) -> Path:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise RuntimeError(f"matplotlib is required for graph generation: {exc}") from exc

    linestyles = linestyles or {}
    alphas = alphas or {}
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
            linestyle=linestyles.get(series, "-"),
            alpha=alphas.get(series, 1.0),
        )
    ax.set_xlabel(x_label or "Threads")
    ax.set_ylabel("Valid TPS (trimmed mean when 3+ runs are available)")
    ax.set_title(f"{title}\n{workload}")
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
    ax.legend()
    fig.tight_layout()
    out = out_dir / filename
    fig.savefig(out)
    plt.close(fig)
    return out


def _plot(summary: list[dict[str, str]],
          out_dir: Path,
          workload: str,
          machine: str,
          x_label: str) -> list[Path]:
    pg_vs_det = _plot_series(
        summary,
        out_dir,
        workload,
        x_label,
        filename="ycsb_skew_pg_vs_det.png",
        title="YCSB skew single-node PG vs DET",
        labels={
            "single_node_pg": f"Single-node PG ({machine})",
            "single_node_det": f"Single-node DET ({machine})",
        },
        colors={
            "single_node_pg": "#2563eb",
            "single_node_det": "#16a34a",
        },
        markers={
            "single_node_pg": "o",
            "single_node_det": "s",
        },
    )
    # Headline comparison: keep gateway-direct in the CSV diagnostics, but do
    # not plot it by default. The visible story is the four user-facing systems:
    # PG, DET, Kafka cluster, and Raft+Kafka cluster.
    det_vs_cluster = _plot_series(
        summary,
        out_dir,
        workload,
        x_label,
        filename="ycsb_skew_det_vs_cluster.png",
        title="YCSB skew: single-node DET vs replicated cluster paths",
        labels={
            "single_node_det": f"Single-node DET ({machine})",
            "cluster_kafka": "Cluster + Kafka only (broadcast, no Raft)",
            "cluster_raft_kafka": "Cluster + Raft + Kafka (full system)",
            "full_system_kafka_raft_bcdb": "Cluster + Raft + Kafka (full system)",
        },
        colors={
            "single_node_det": "#16a34a",
            "cluster_kafka": "#2563eb",
            "cluster_raft_kafka": "#dc2626",
            "full_system_kafka_raft_bcdb": "#dc2626",
        },
        markers={
            "single_node_det": "s",
            "cluster_kafka": "o",
            "cluster_raft_kafka": "^",
            "full_system_kafka_raft_bcdb": "^",
        },
    )
    all_systems = _plot_series(
        summary,
        out_dir,
        workload,
        x_label,
        filename="ycsb_skew_all_systems.png",
        title="YCSB skew: PG, DET, Kafka, and Raft+Kafka",
        labels={
            "single_node_pg": f"Single-node PG ({machine})",
            "single_node_det": f"Single-node DET ({machine})",
            "cluster_kafka": "Kafka cluster",
            "cluster_raft_kafka": "Raft + Kafka cluster",
            "full_system_kafka_raft_bcdb": "Raft + Kafka cluster",
        },
        colors={
            "single_node_pg": "#2563eb",
            "single_node_det": "#16a34a",
            "cluster_kafka": "#f59e0b",
            "cluster_raft_kafka": "#dc2626",
            "full_system_kafka_raft_bcdb": "#dc2626",
        },
        markers={
            "single_node_pg": "o",
            "single_node_det": "s",
            "cluster_kafka": "P",
            "cluster_raft_kafka": "^",
            "full_system_kafka_raft_bcdb": "^",
        },
    )
    return [pg_vs_det, det_vs_cluster, all_systems]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--single-results", required=True, type=Path)
    ap.add_argument("--single-gateway-manifest", type=Path)
    ap.add_argument("--full-manifest", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--machine", default="10.129.148.248")
    ap.add_argument("--threads", default="", help="Optional CSV thread filter for graph-ready output")
    ap.add_argument("--x-label", default="Threads")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    thread_filter = _parse_threads(args.threads)
    rows = []
    rows.extend(_load_single_rows(args.single_results, workload=args.workload, machine=args.machine))
    if args.single_gateway_manifest is not None:
        rows.extend(_load_single_gateway_rows(args.single_gateway_manifest, workload=args.workload, machine=args.machine))
    rows.extend(_load_full_rows(args.full_manifest, workload=args.workload))
    if thread_filter is not None:
        rows = [r for r in rows if (_as_int(r.get("thread")) in thread_filter)]
    rows.sort(key=lambda r: (r["series"], int(r["thread"] or 0), int(r["run"] or 0)))

    _write_results(rows, args.out_dir / "results.csv")
    summary = _write_summary(rows, args.out_dir / "summary.csv")
    _write_overhead(summary, args.out_dir / "overhead.csv")
    _write_interpretation(summary, args.out_dir / "interpretation.csv")
    graphs = _plot(summary, args.out_dir, args.workload, args.machine, args.x_label)
    print(f"Wrote results:  {args.out_dir / 'results.csv'}")
    print(f"Wrote summary:  {args.out_dir / 'summary.csv'}")
    print(f"Wrote overhead: {args.out_dir / 'overhead.csv'}")
    print(f"Wrote interpretation: {args.out_dir / 'interpretation.csv'}")
    for graph in graphs:
        print(f"Wrote graph:    {graph}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
