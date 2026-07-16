#!/usr/bin/env python3
"""Summarize distributed Raft/Kafka benchmark profile artifacts.

Usage:
  scripts/distributed/summarize_raft_profile.py scripts/bench_full_results/cluster4_...
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sys
from typing import Dict, Iterable, List, Tuple


PROFILE_PREFIXES = (
    "PROFILE_GATEWAY",
    "PROFILE_SERVER",
    "PROFILE_RAFT_ORDERER",
    "PROFILE_RAFT_STORAGE",
    "PROGRESS_GATEWAY_DET",
)

CSV_FIELDS = [
    "run_id",
    "orderer_policy",
    "assigned_seq_mode",
    "tps",
    "p50",
    "p95",
    "p99",
    "client_workers",
    "server_workers",
    "bcdb_workers",
    "bcdb_init_arg_size",
    "leader_id",
    "target_entries",
    "linger_us",
    "entries_per_fsync",
    "fsync_p50",
    "fsync_p95",
    "append_entries_avg",
    "orderer_gap_wait_ms",
    "executor_queue_delay_ms",
    "pqexec_avg",
    "max_pqexec",
    "kafka_pending_max",
    "kafka_async_publisher_enabled",
    "kafka_async_publisher_queue_max",
    "gateway_submit_to_accept_ms",
    "gateway_accept_to_terminal_ms",
    "all_three_agreement_p95_ms",
    "post_marker_visibility_ms",
    "merkle_apply_and_equality_ms",
    "post_run_equality_verification_ms",
    "dynamic_structure_gate_pass",
    "dynamic_pending_crash_restart_pass",
    "dynamic_cross_partition_keys",
    "merkle_pass",
    "divergence_count",
    "permanent_failures",
]


def parse_kv_line(line: str) -> Tuple[str, Dict[str, str]]:
    parts = line.strip().split()
    if not parts:
        return "", {}
    prefix = parts[0]
    out: Dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k] = v
    return prefix, out


def as_float(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key, default))
    except (TypeError, ValueError):
        return default


def as_int(row: Dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(float(row.get(key, default)))
    except (TypeError, ValueError):
        return default


def profile_lines(root: pathlib.Path) -> Iterable[Tuple[pathlib.Path, str, Dict[str, str]]]:
    for path in sorted(root.rglob("*.log")):
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            if not line.startswith(PROFILE_PREFIXES):
                continue
            prefix, row = parse_kv_line(line)
            if prefix:
                yield path, prefix, row


def last_by_prefix(root: pathlib.Path) -> Dict[str, List[Tuple[pathlib.Path, Dict[str, str]]]]:
    rows: Dict[str, List[Tuple[pathlib.Path, Dict[str, str]]]] = {}
    for path, prefix, row in profile_lines(root):
        rows.setdefault(prefix, []).append((path, row))
    return rows


def read_env_file(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return out
    for line in text.splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def merkle_pass(root: pathlib.Path) -> int:
    summary = read_env_file(root / "run_summary.env")
    if summary.get("DYNAMIC_MERKLE_THREE_REPLICA_EQUALITY_PASS") == "1":
        return 1

    runner_log = root / "runner.log"
    if not runner_log.exists():
        return 0
    text = runner_log.read_text(errors="replace")
    return 1 if re.search(
        r"pre-marker .*PASS|post-marker .*PASS|consistency: PASS|"
        r"DYNAMIC_MERKLE_THREE_REPLICA_EQUALITY_PASS=1",
        text,
    ) else 0


def collect_csv_row(root: pathlib.Path) -> Dict[str, object]:
    rows = last_by_prefix(root)
    meta = read_env_file(root / "run_meta.env")
    summary = read_env_file(root / "run_summary.env")
    out: Dict[str, object] = {k: "" for k in CSV_FIELDS}
    out["run_id"] = root.name
    out["merkle_pass"] = merkle_pass(root)
    out["orderer_policy"] = meta.get("raft_ordering_policy", "")
    out["assigned_seq_mode"] = "1" if out["orderer_policy"] == "leader-assigned" else "0"
    out["post_marker_visibility_ms"] = summary.get("post_marker_visibility_ms", "")
    out["merkle_apply_and_equality_ms"] = summary.get("merkle_apply_and_equality_ms", "")
    out["post_run_equality_verification_ms"] = summary.get("post_run_equality_verification_ms", "")
    out["dynamic_structure_gate_pass"] = summary.get("DYNAMIC_DISTRIBUTED_STRUCTURE_GATE_PASS", "")
    out["dynamic_pending_crash_restart_pass"] = summary.get("DYNAMIC_DISTRIBUTED_PENDING_CRASH_RESTART_PASS", "")
    out["dynamic_cross_partition_keys"] = summary.get("dynamic_cross_partition_keys", "")
    out["target_entries"] = meta.get("raft_ordered_batch_target_entries", "")
    out["linger_us"] = meta.get("raft_ordered_batch_linger_us", "")
    out["bcdb_init_arg_size"] = meta.get("bcdb_init_arg_size", meta.get("bcdb_init_block_size", ""))
    leader_path: pathlib.Path | None = None

    progress = rows.get("PROGRESS_GATEWAY_DET", [])
    if progress:
        _, row = max(progress, key=lambda item: as_int(item[1], "completed"))
        out["tps"] = f"{as_float(row, 'completed_tps'):.2f}"
        out["divergence_count"] = as_int(row, "divergence_count")
        out["permanent_failures"] = as_int(row, "permanent_failures")

    gateways = rows.get("PROFILE_GATEWAY", [])
    if gateways:
        _, row = max(gateways, key=lambda item: as_int(item[1], "submit_attempts"))
        out["client_workers"] = row.get("configured_gateway_workers", "")
        if as_int(row, "request_latency_count") > 0:
            out["p50"] = f"{as_float(row, 'request_latency_p50_ms'):.3f}"
            out["p95"] = f"{as_float(row, 'request_latency_p95_ms'):.3f}"
            out["p99"] = f"{as_float(row, 'request_latency_p99_ms'):.3f}"
        out["gateway_submit_to_accept_ms"] = f"{as_float(row, 'submit_to_accept_ms'):.3f}"
        out["gateway_accept_to_terminal_ms"] = f"{as_float(row, 'accept_to_terminal_ms'):.3f}"
        out["all_three_agreement_p95_ms"] = f"{as_float(row, 'majority_to_all3_ms_p95'):.3f}"
        if out["permanent_failures"] == "":
            out["permanent_failures"] = as_int(row, "permanent_failures")

    server_rows = rows.get("PROFILE_SERVER", [])
    if server_rows:
        leader_rows = [
            (path, row) for path, row in server_rows
            if as_int(row, "append_calls") > 0 or as_int(row, "read_frames") > 100
        ]
        leader_path, row = leader_rows[-1] if leader_rows else server_rows[-1]
        out["server_workers"] = row.get("configured_server_workers", "")
        out["bcdb_workers"] = row.get("bcdb_block_size", "")
        out["bcdb_init_arg_size"] = row.get(
            "bcdb_init_arg_size_configured",
            row.get("bcdb_init_block_size_configured", out["bcdb_init_arg_size"]),
        )
        out["leader_id"] = row.get("raft_leader", "")
        out["executor_queue_delay_ms"] = f"{as_float(row, 'queue_delay_ms'):.3f}"
        out["kafka_pending_max"] = as_int(row, "kafka_delivery_pending_max")
        out["kafka_async_publisher_enabled"] = as_int(row, "kafka_async_publisher_enabled")
        out["kafka_async_publisher_queue_max"] = as_int(row, "kafka_async_publisher_queue_max")
        exec_calls = as_int(row, "exec_calls")
        pg_query_ms = as_float(row, "pg_query_ms")
        out["pqexec_avg"] = f"{((pg_query_ms / exec_calls) if exec_calls else 0.0):.6f}"
        out["max_pqexec"] = as_int(row, "max_concurrent_PQexec")

    orderer_rows = rows.get("PROFILE_RAFT_ORDERER", [])
    if orderer_rows:
        _, row = max(orderer_rows, key=lambda item: as_int(item[1], "arrivals"))
        out["orderer_policy"] = row.get("policy", out["orderer_policy"])
        out["assigned_seq_mode"] = "1" if out["orderer_policy"] == "leader-assigned" else "0"
        out["append_entries_avg"] = f"{as_float(row, 'append_vector_entries_avg'):.3f}"
        out["orderer_gap_wait_ms"] = f"{as_float(row, 'gap_wait_ms'):.3f}"
        out["target_entries"] = row.get("batch_target_entries", out["target_entries"])
        out["linger_us"] = row.get("batch_linger_us", out["linger_us"])

    storage_rows = rows.get("PROFILE_RAFT_STORAGE", [])
    if storage_rows:
        if leader_path is not None:
            leader_storage = [
                (path, row) for path, row in storage_rows
                if path.name == leader_path.name
            ]
        else:
            leader_storage = []
        _, row = (leader_storage[-1] if leader_storage
                  else max(storage_rows, key=lambda item: as_int(item[1], "append_calls")))
        out["entries_per_fsync"] = f"{as_float(row, 'entries_per_fsync_avg'):.3f}"
        out["fsync_p50"] = f"{as_float(row, 'fdatasync_p50_ms'):.3f}"
        out["fsync_p95"] = f"{as_float(row, 'fdatasync_p95_ms'):.3f}"

    return out


def summarize_root(root: pathlib.Path) -> None:
    rows = last_by_prefix(root)
    meta = read_env_file(root / "run_meta.env")
    print(f"artifact={root}")
    leader_path: pathlib.Path | None = None

    progress = rows.get("PROGRESS_GATEWAY_DET", [])
    if progress:
        _, row = max(progress, key=lambda item: as_int(item[1], "completed"))
        print(
            "  result "
            f"tps={as_float(row, 'completed_tps'):.2f} "
            f"completed={as_int(row, 'completed')} "
            f"permanent_failures={as_int(row, 'permanent_failures')} "
            f"divergence_count={as_int(row, 'divergence_count')}"
        )

    gateways = rows.get("PROFILE_GATEWAY", [])
    if gateways:
        _, row = max(gateways, key=lambda item: as_int(item[1], "submit_attempts"))
        print(
            "  gateway "
            f"mode={row.get('det_client_mode', '-')} "
            f"workers={row.get('configured_gateway_workers', '-')} "
            f"submit_attempts={as_int(row, 'submit_attempts')} "
            f"write_calls={as_int(row, 'write_calls')} "
            f"read_calls={as_int(row, 'read_calls')} "
            f"fused_waits={as_int(row, 'fused_wait_requests')} "
            f"write_ms={as_float(row, 'write_ms'):.1f} "
            f"read_ms={as_float(row, 'read_ms'):.1f} "
            f"permanent_failures={as_int(row, 'permanent_failures')}"
        )

    server_rows = rows.get("PROFILE_SERVER", [])
    if server_rows:
        leader_rows = [
            (path, row) for path, row in server_rows
            if as_int(row, "append_calls") > 0 or as_int(row, "read_frames") > 100
        ]
        path, row = leader_rows[-1] if leader_rows else server_rows[-1]
        leader_path = path
        print(
            "  leader_server "
            f"file={path.name} "
            f"append_calls={as_int(row, 'append_calls')} "
            f"append_ms={as_float(row, 'append_ms'):.1f} "
            f"max_pqexec={as_int(row, 'max_concurrent_PQexec')} "
            f"owned_pg_connections={as_int(row, 'unique_owned_pg_connections')} "
            f"bcdb_init_arg_size={row.get('bcdb_init_arg_size_configured', row.get('bcdb_init_block_size_configured', meta.get('bcdb_init_arg_size', meta.get('bcdb_init_block_size', '-'))))} "
            f"queue_depth_max={as_int(row, 'queue_depth_max')} "
            f"queue_delay_ms={as_float(row, 'queue_delay_ms'):.1f} "
            f"kafka_pending_max={as_int(row, 'kafka_delivery_pending_max')} "
            f"kafka_async_pub={as_int(row, 'kafka_async_publisher_enabled')} "
            f"kafka_async_qmax={as_int(row, 'kafka_async_publisher_queue_max')} "
            f"kafka_batch_avg={as_float(row, 'kafka_batch_records_avg'):.2f}"
        )

    orderer_rows = rows.get("PROFILE_RAFT_ORDERER", [])
    if orderer_rows:
        _, row = max(orderer_rows, key=lambda item: as_int(item[1], "arrivals"))
        print(
            "  orderer "
            f"policy={row.get('policy', meta.get('raft_ordering_policy', '-'))} "
            f"arrivals={as_int(row, 'arrivals')} "
            f"leader_assigned_items={as_int(row, 'leader_assigned_items')} "
            f"drains={as_int(row, 'ordered_drains')} "
            f"pending_max={as_int(row, 'pending_depth_max')} "
            f"gap_wait_ms={as_float(row, 'gap_wait_ms'):.1f} "
            f"target={row.get('batch_target_entries', meta.get('raft_ordered_batch_target_entries', '-'))} "
            f"linger_us={row.get('batch_linger_us', meta.get('raft_ordered_batch_linger_us', '-'))} "
            f"append_vector_calls={as_int(row, 'append_vector_calls')} "
            f"append_vector_avg={as_float(row, 'append_vector_entries_avg'):.2f} "
            f"append_vector_max={as_int(row, 'append_vector_entries_max')}"
        )

    storage_rows = rows.get("PROFILE_RAFT_STORAGE", [])
    if storage_rows:
        if leader_path is not None:
            leader_storage = [
                (path, row) for path, row in storage_rows
                if path.name == leader_path.name
            ]
        else:
            leader_storage = []
        path, row = (leader_storage[-1] if leader_storage
                     else max(storage_rows, key=lambda item: as_int(item[1], "append_calls")))
        print(
            "  storage "
            f"file={path.name} "
            f"append_calls={as_int(row, 'append_calls')} "
            f"append_batches={as_int(row, 'append_batches')} "
            f"entries_per_fsync={as_float(row, 'entries_per_fsync_avg'):.2f} "
            f"fdatasync_p50_ms={as_float(row, 'fdatasync_p50_ms'):.3f} "
            f"fdatasync_p95_ms={as_float(row, 'fdatasync_p95_ms'):.3f} "
            f"fdatasync_p99_ms={as_float(row, 'fdatasync_p99_ms'):.3f} "
            f"append_write_ms={as_float(row, 'append_write_ms'):.1f} "
            f"append_fsync_ms={as_float(row, 'append_fsync_ms'):.1f} "
            f"async_enabled={as_int(row, 'async_flush_enabled')} "
            f"async_jobs={as_int(row, 'async_flush_jobs')} "
            f"async_coalesced={as_int(row, 'async_flush_coalesced_jobs')} "
            f"async_queue_max={as_int(row, 'async_flush_queue_max')}"
        )

    summary = read_env_file(root / "run_summary.env")
    print(
        "  correctness "
        f"merkle_equality_pass={merkle_pass(root)} "
        f"barrier={summary.get('post_run_barrier', '-')} "
        f"dynamic_structure_gate={summary.get('DYNAMIC_DISTRIBUTED_STRUCTURE_GATE_PASS', '-')} "
        f"dynamic_crash_restart_gate={summary.get('DYNAMIC_DISTRIBUTED_PENDING_CRASH_RESTART_PASS', '-')}"
    )
    print()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pretty", action="store_true",
                        help="print the human-readable summary instead of CSV")
    parser.add_argument("--no-header", action="store_true",
                        help="omit the CSV header")
    parser.add_argument("artifacts", nargs="+", type=pathlib.Path)
    args = parser.parse_args()
    if args.pretty:
        for artifact in args.artifacts:
            summarize_root(artifact)
        return 0

    writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDS)
    if not args.no_header:
        writer.writeheader()
    for artifact in args.artifacts:
        writer.writerow(collect_csv_row(artifact))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
