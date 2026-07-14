"""CSV / JSON / environment reporting helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg

from .config import ROOT
from .config import DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT, DYNAMIC_PROFILE
from .metrics import Metrics


# ── progress emitter ─────────────────────────────────────────────────────────

def emit_progress(result_dir: Path, **event: object) -> None:
    event["timestamp_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    line = json.dumps(event, sort_keys=True, default=str)
    with (result_dir / "progress.jsonl").open("a") as f:
        f.write(line + "\n")
        f.flush()
    (result_dir / "progress.json").write_text(
        json.dumps(event, indent=2, sort_keys=True, default=str) + "\n"
    )
    # main() redirects normal stdout into scratch/stdout.log; use the original
    # stream so the synced remote launcher can tee live progress to the terminal.
    print(f"[progress] {line}", file=sys.__stdout__, flush=True)


# ── environment capture ──────────────────────────────────────────────────────

def write_environment(result_dir: Path, args) -> None:
    lines = [
        f"timestamp={datetime.now().isoformat()}",
        f"cwd={ROOT}",
        f"python={sys.version.replace(os.linesep, ' ')}",
        f"platform={platform.platform()}",
        f"dsn={args.dsn}",
    ]
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        lines.append(f"git_head={head}")
    except Exception as exc:
        lines.append(f"git_head_error={exc}")
    (result_dir / "environment.txt").write_text("\n".join(lines) + "\n")


def write_python_environment(result_dir: Path) -> None:
    try:
        psycopg_version = psycopg.__version__
    except Exception as exc:
        psycopg_version = f"error:{exc}"
    payload = {
        "python_executable": sys.executable,
        "python_version": sys.version.replace(os.linesep, " "),
        "platform": platform.platform(),
        "psycopg_version": psycopg_version,
        "working_directory": str(Path.cwd()),
        "benchmark_script": str(Path(sys.argv[0]).resolve()),
    }
    (result_dir / "python_environment.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )


# ── CSV helpers ───────────────────────────────────────────────────────────────

def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ── Metrics → CSV rows ────────────────────────────────────────────────────────

def metrics_to_rows(
    metrics: list[Metrics],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    run_rows: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []
    for m in metrics:
        row: dict[str, Any] = {
            "run_id": m.run_id,
            "experiment": m.experiment,
            "method": m.method,
            "corruption_mode": m.corruption_mode,
            "profile_label": m.profile_label,
            "profiling_mode": m.profiling_mode,
            "tuple_count": m.tuple_count,
            "partitions": m.partitions,
            "leaves_per_partition": m.leaves_per_partition,
            "fanout": m.fanout,
            "bad_leaf_count": m.bad_leaf_count,
            "corrupted_tuple_count": m.corrupted_tuple_count,
            "repetition": m.repetition,
            "valid": int(m.valid),
            "warning": m.warning,
            "paper_style_total_ms": f"{m.paper_style_total_ms:.3f}",
            "restore_repair_ms": f"{m.restore_repair_ms:.3f}",
            "audit_validation_ms": f"{m.audit_validation_ms:.3f}",
            "end_to_end_observed_ms": f"{m.end_to_end_observed_ms:.3f}",
            "cleanup_ms": f"{m.cleanup_ms:.3f}",
            # legacy aliases kept for downstream plot scripts
            "paper_total_ms": f"{m.paper_style_total_ms:.3f}",
            "recovery_only_ms": f"{m.restore_repair_ms:.3f}",
            "total_ms": f"{m.end_to_end_observed_ms:.3f}",
        }
        row.update(m.counters)
        run_rows.append(row)
        for phase, value in m.phase.items():
            phase_rows.append(
                {
                    "run_id": m.run_id,
                    "manifest_sha256": m.counters.get("manifest_sha256", ""),
                    "method": m.method,
                    "phase": phase,
                    "ms": f"{value:.3f}",
                }
            )
    return run_rows, phase_rows


# ── benchmark contract assertion ─────────────────────────────────────────────

def assert_benchmark_contract(profile: str, metrics: list[Metrics]) -> None:
    failures: list[str] = []
    for m in metrics:
        if not m.valid:
            failures.append(f"{m.run_id}: {m.warning or 'marked invalid'}")
        if int(m.counters.get("paper_end_before_audit_start", 0)) != 1:
            failures.append(f"{m.run_id}: paper_end_ms is not before audit_start_ms")
        if int(m.counters.get("full_audit_skipped", 0)) != 1:
            if int(m.counters.get("audit_validation_positive", 0)) != 1:
                failures.append(f"{m.run_id}: audit_validation_ms is not positive")
        if int(m.counters.get("end_to_end_covers_paper_and_audit", 0)) != 1:
            failures.append(f"{m.run_id}: end_to_end_observed_ms does not cover paper plus audit")
        if int(m.counters.get("schema_fidelity_ok", 0)) != 1:
            failures.append(f"{m.run_id}: schema fidelity failed")
        # merkle-specific
        if int(m.counters.get("partition_root_batches", -1)) != 2:
            failures.append(
                f"{m.run_id}: partition_root_batches={m.counters.get('partition_root_batches')}"
            )
        if int(m.counters.get("partition_root_batches_ok", 0)) != 1:
            failures.append(
                f"{m.run_id}: partition_root_batches_ok={m.counters.get('partition_root_batches_ok')}"
            )
        if int(m.counters.get("planner_checks_passed", 0)) != 1:
            failures.append(f"{m.run_id}: planner checks did not pass")
        if profile in ("recovery-scaling-diagnosis", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
            if m.corruption_mode != "paper-update-only":
                failures.append(f"{m.run_id}: diagnosis corruption_mode={m.corruption_mode}")
            if int(m.corrupted_tuple_count) != 300:
                failures.append(f"{m.run_id}: corrupted_tuple_count={m.corrupted_tuple_count}")
            if int(m.counters.get("total_rows_repaired", -1)) != 300:
                failures.append(f"{m.run_id}: total_rows_repaired={m.counters.get('total_rows_repaired')}")
            if int(m.counters.get("recovery_user_table_seq_scan_delta", -1)) != 0:
                failures.append(
                    f"{m.run_id}: recovery_user_table_seq_scan_delta="
                    f"{m.counters.get('recovery_user_table_seq_scan_delta')}"
                )
            if int(m.counters.get("planner_checks_passed", 0)) != 1:
                failures.append(f"{m.run_id}: planner_checks_passed={m.counters.get('planner_checks_passed')}")
            if int(m.counters.get("schema_fidelity_ok", 0)) != 1:
                failures.append(f"{m.run_id}: schema_fidelity_ok={m.counters.get('schema_fidelity_ok')}")
        if profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
            expected_bad_leaf_count = 75 if profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") else 20
            if int(m.bad_leaf_count) != expected_bad_leaf_count:
                failures.append(f"{m.run_id}: configured bad_leaf_count={m.bad_leaf_count}, expected {expected_bad_leaf_count}")
            if int(m.counters.get("bad_leaf_count", -1)) != expected_bad_leaf_count:
                failures.append(f"{m.run_id}: detected bad_leaf_count={m.counters.get('bad_leaf_count')}, expected {expected_bad_leaf_count}")
        if profile == DYNAMIC_PROFILE:
            dynamic_checks = [
                (m.method == "merkle_dynamic", f"method={m.method}"),
                (m.corruption_mode in ("paper-update-only", "update-only"),
                 f"corruption_mode={m.corruption_mode}"),
                (m.partitions == 200, f"partitions={m.partitions}"),
                (m.fanout == 32, f"logical_fanout={m.fanout}"),
                (m.bad_leaf_count == 75, f"configured_bad_range_count={m.bad_leaf_count}"),
                (m.corrupted_tuple_count == 300, f"corrupted_tuple_count={m.corrupted_tuple_count}"),
                (int(m.counters.get("total_rows_repaired", -1)) == 300,
                 f"total_rows_repaired={m.counters.get('total_rows_repaired')}"),
                (
                    int(m.counters.get(
                        "dynamic_candidate_summary_items_fetched",
                        DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT + 1,
                    )) <= DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT,
                    "dynamic_candidate_summary_items_fetched="
                    f"{m.counters.get('dynamic_candidate_summary_items_fetched')}",
                ),
                (int(m.counters.get("candidate_summary_bound_ok", 0)) == 1,
                 f"candidate_summary_bound_ok={m.counters.get('candidate_summary_bound_ok')}"),
                (int(m.counters.get("dynamic_storage_seq_scan_delta", -1)) == 0,
                 "dynamic_storage_seq_scan_delta="
                 f"{m.counters.get('dynamic_storage_seq_scan_delta')}"),
                (int(m.counters.get("dynamic_side_table_seq_scan_plans", -1)) == 0,
                 "dynamic_side_table_seq_scan_plans="
                 f"{m.counters.get('dynamic_side_table_seq_scan_plans')}"),
                (int(m.counters.get("full_damaged_heap_rows_fetched", -1)) == 0,
                 f"full_damaged_heap_rows_fetched={m.counters.get('full_damaged_heap_rows_fetched')}"),
                (int(m.counters.get("exact_healthy_heap_rows_fetched", -1)) == 300,
                 f"exact_healthy_heap_rows_fetched={m.counters.get('exact_healthy_heap_rows_fetched')}"),
                (int(m.counters.get("rows_inserted", -1)) == 0,
                 f"rows_inserted={m.counters.get('rows_inserted')}"),
                (int(m.counters.get("rows_updated", -1)) == 300,
                 f"rows_updated={m.counters.get('rows_updated')}"),
                (int(m.counters.get("rows_deleted", -1)) == 0,
                 f"rows_deleted={m.counters.get('rows_deleted')}"),
                (int(m.counters.get("remaining_bad_range_count", -1)) == 0,
                 f"remaining_bad_range_count={m.counters.get('remaining_bad_range_count')}"),
                (int(m.counters.get("healthy_minus_damaged", -1)) == 0,
                 f"healthy_minus_damaged={m.counters.get('healthy_minus_damaged')}"),
                (int(m.counters.get("damaged_minus_healthy", -1)) == 0,
                 f"damaged_minus_healthy={m.counters.get('damaged_minus_healthy')}"),
                (int(m.counters.get("roots_match", 0)) == 1,
                 f"roots_match={m.counters.get('roots_match')}"),
                (int(m.counters.get("root_counts_match", 0)) == 1,
                 f"root_counts_match={m.counters.get('root_counts_match')}"),
                (int(m.counters.get("healthy_dynamic_verify", 0)) == 1,
                 f"healthy_dynamic_verify={m.counters.get('healthy_dynamic_verify')}"),
                (int(m.counters.get("damaged_dynamic_verify", 0)) == 1,
                 f"damaged_dynamic_verify={m.counters.get('damaged_dynamic_verify')}"),
                (m.counters.get("healthy_dynamic_state") == "READY",
                 f"healthy_dynamic_state={m.counters.get('healthy_dynamic_state')}"),
                (m.counters.get("damaged_dynamic_state") == "READY",
                 f"damaged_dynamic_state={m.counters.get('damaged_dynamic_state')}"),
                (0 <= int(m.counters.get("healthy_max_leaf_occupancy", -1)) <= 32,
                 f"healthy_max_leaf_occupancy={m.counters.get('healthy_max_leaf_occupancy')}"),
                (0 <= int(m.counters.get("damaged_max_leaf_occupancy", -1)) <= 32,
                 f"damaged_max_leaf_occupancy={m.counters.get('damaged_max_leaf_occupancy')}"),
                (int(m.counters.get("recovery_user_table_seq_scan_delta", -1)) == 0,
                 f"recovery_user_table_seq_scan_delta={m.counters.get('recovery_user_table_seq_scan_delta')}"),
                (int(m.counters.get("static_lookup_index_count", -1)) == 0,
                 f"static_lookup_index_count={m.counters.get('static_lookup_index_count')}"),
                (m.counters.get("audit_mode") == "full",
                 f"audit_mode={m.counters.get('audit_mode')}"),
            ]
            for ok, detail in dynamic_checks:
                if not ok:
                    failures.append(f"{m.run_id}: dynamic acceptance failed: {detail}")
    if failures and profile in ("smoke", "preflight", "paper", "recovery-scaling-diagnosis", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300", DYNAMIC_PROFILE):
        shown = "\n".join(failures[:20])
        more = "" if len(failures) <= 20 else f"\n... {len(failures) - 20} more"
        raise RuntimeError(f"benchmark contract failed:\n{shown}{more}")
