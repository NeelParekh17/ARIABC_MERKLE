"""CSV / JSON / environment reporting helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from .config import ROOT
from .metrics import Metrics


# ── progress emitter ─────────────────────────────────────────────────────────

def format_event_table(event: dict[str, Any]) -> str:
    evt_type = str(event.get("event", "progress"))

    def build_table(headers: list[str], row: list[str]) -> str:
        widths = [max(len(h), len(r)) for h, r in zip(headers, row)]
        border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        header_line = "| " + " | ".join(f"{h:<{w}}" for h, w in zip(headers, widths)) + " |"
        data_line = "| " + " | ".join(f"{r:<{w}}" for r, w in zip(row, widths)) + " |"
        return f"{border}\n{header_line}\n{border}\n{data_line}\n{border}"

    if evt_type == "benchmark_start":
        headers = ["EVENT", "PROFILE", "EXPERIMENT", "CORRUPTION", "PROFILING", "REPS", "TOTAL RUNS", "TIME (LOCAL)"]
        row = [
            evt_type,
            str(event.get("profile", "")),
            str(event.get("experiment", "")),
            str(event.get("corruption_mode", "")),
            str(event.get("profiling_mode", "")),
            str(event.get("repetitions", "")),
            str(event.get("total_runs", "")),
            str(event.get("timestamp_local", "")),
        ]
        return build_table(headers, row)
    elif evt_type == "dataset_start":
        headers = ["EVENT", "EXPERIMENT", "LABEL", "TUPLES", "BAD LEAVES", "PROGRESS", "TIME (LOCAL)"]
        progress_str = f"{event.get('completed_runs', 0)}/{event.get('total_runs', 0)}"
        row = [
            evt_type,
            str(event.get("experiment", "")),
            str(event.get("profile_label", "")),
            f"{int(event.get('tuple_count', 0)):,}",
            str(event.get("bad_leaf_count", "")),
            progress_str,
            str(event.get("timestamp_local", "")),
        ]
        return build_table(headers, row)
    elif evt_type == "method_start":
        headers = ["EVENT", "RUN ID", "METHOD", "TUPLES", "BAD LEAVES", "REP", "PROGRESS", "TIME (LOCAL)"]
        progress_str = f"{event.get('completed_runs', 0)}/{event.get('total_runs', 0)}"
        row = [
            evt_type,
            str(event.get("run_id", "")),
            str(event.get("method", "")),
            f"{int(event.get('tuple_count', 0)):,}",
            str(event.get("bad_leaf_count", "")),
            str(event.get("repetition", "")),
            progress_str,
            str(event.get("timestamp_local", "")),
        ]
        return build_table(headers, row)
    elif evt_type == "method_complete":
        headers = ["EVENT", "RUN ID", "STATUS", "TOTAL (MS)", "RECOVERY (MS)", "AUDIT (MS)", "PROGRESS", "TIME (LOCAL)"]
        progress_str = f"{event.get('completed_runs', 0)}/{event.get('total_runs', 0)}"
        valid = bool(event.get("valid", True))
        status = "PASS" if valid else f"FAIL ({event.get('warning', 'invalid')})"
        row = [
            evt_type,
            str(event.get("run_id", "")),
            status,
            f"{float(event.get('paper_style_total_ms', 0)):.2f}",
            f"{float(event.get('method_elapsed_ms', 0)):.2f}",
            f"{float(event.get('audit_validation_ms', 0)):.2f}",
            progress_str,
            str(event.get("timestamp_local", "")),
        ]
        return build_table(headers, row)
    elif evt_type == "benchmark_complete":
        headers = ["EVENT", "COMPLETED", "TOTAL RUNS", "ELAPSED (S)", "TIME (LOCAL)"]
        row = [
            evt_type,
            str(event.get("completed_runs", "")),
            str(event.get("total_runs", "")),
            f"{float(event.get('elapsed_ms', 0))/1000.0:.2f}",
            str(event.get("timestamp_local", "")),
        ]
        return build_table(headers, row)
    else:
        keys = list(event.keys())
        max_k = max((len(k) for k in keys), default=10)
        max_v = max((len(str(event[k])) for k in keys), default=10)
        k_w = max(max_k, len("KEY"))
        v_w = max(max_v, len("VALUE"))
        border = f"+{'-' * (k_w + 2)}+{'-' * (v_w + 2)}+"
        lines = [border, f"| {'KEY':<{k_w}} | {'VALUE':<{v_w}} |", border]
        for k in keys:
            v = str(event[k])
            lines.append(f"| {k:<{k_w}} | {v:<{v_w}} |")
        lines.append(border)
        return "\n".join(lines)


def emit_progress(result_dir: Path, **event: object) -> None:
    now = datetime.now()
    event["timestamp_local"] = now.astimezone().isoformat(timespec="seconds")
    event["timestamp_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    line = json.dumps(event, sort_keys=True, default=str)
    with (result_dir / "progress.jsonl").open("a") as f:
        f.write(line + "\n")
        f.flush()
    (result_dir / "progress.json").write_text(
        json.dumps(event, indent=2, sort_keys=True, default=str) + "\n"
    )
    # main() redirects normal stdout into scratch/stdout.log; use the original
    # stream so the synced remote launcher can tee live progress to the terminal.
    print(format_event_table(event), file=sys.__stdout__, flush=True)


# ── environment capture ──────────────────────────────────────────────────────

def write_environment(result_dir: Path, args) -> None:
    lines = [
        f"timestamp_utc={datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')}",
        f"timestamp_local={datetime.now().astimezone().isoformat(timespec='seconds')}",
        f"cwd={ROOT}",
        f"python={sys.version.replace(os.linesep, ' ')}",
        f"platform={platform.platform()}",
        f"dsn={args.dsn}",
    ]
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
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
            "split_threshold": m.split_threshold,
            "merge_threshold": m.merge_threshold,
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
        if int(m.counters.get("partition_root_batches", 0)) <= 0:
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
            if m.corruption_mode not in ("paper-update-only", "mixed", "update-only", "delete-only", "insert-only"):
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
    if failures and profile in ("smoke", "preflight", "paper", "recovery-scaling-diagnosis", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
        shown = "\n".join(failures[:20])
        more = "" if len(failures) <= 20 else f"\n... {len(failures) - 20} more"
        raise RuntimeError(f"benchmark contract failed:\n{shown}{more}")
