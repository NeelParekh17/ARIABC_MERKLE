#!/usr/bin/env python3
"""Paper-style Merkle-only recovery benchmark for AriaBC (Figures 12 & 13)."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Any

from merkle_recovery.config import (
    BENCH_DIR, RESULT_ROOT as _DEFAULT_RESULT_ROOT,
    BENCHMARK_SCHEMA_VERSION, TIMING_CONTRACT_VERSION,
    BENCHMARK_SCOPE_METADATA,
    profile_config,
)
from merkle_recovery.db import connect
from merkle_recovery.dataset import (
    build_dataset, reset_damaged_from_healthy,
    leaf_occupancy, occupancy_stats, table_sizes,
    bucket_consistency_sample, ensure_helpers,
)
from merkle_recovery.manifest import (
    choose_corruption_manifest, validate_manifest_leaf_mapping, apply_corruption,
)
from merkle_recovery.localisation import detect_bad_leaves
from merkle_recovery.repair import (
    run_planner_preflight, repair_leaf,
    seq_scan_snapshot, seq_scan_delta,
    per_leaf_row_counts,
)
from merkle_recovery.verification import audit_recovery_with_scan_counters
from merkle_recovery.metrics import Metrics, add_warning, finalize_metrics
from merkle_recovery.reporting import (
    emit_progress, write_environment, write_python_environment,
    write_csv, metrics_to_rows, assert_benchmark_contract,
)

RESULT_ROOT = _DEFAULT_RESULT_ROOT


# ── timing helper ─────────────────────────────────────────────────────────────

def now_ms() -> float:
    return time.perf_counter() * 1000.0


@contextmanager
def timer(store: dict[str, float], name: str):
    start = now_ms()
    yield
    store[name] = store.get(name, 0.0) + now_ms() - start


# ── core Merkle repair run ────────────────────────────────────────────────────

def repair_merkle(
    conn,
    manifest: dict[str, Any],
    tuple_count: int,
    repetition: int,
    planner_results: dict[str, Any],
    schema_rows_out: list[dict[str, Any]],
) -> Metrics:
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    run_base = (
        f"{manifest['experiment']}-n{tuple_count}"
        f"-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
    )
    m = Metrics(
        run_id=f"{run_base}-merkle-r{repetition}",
        experiment=manifest["experiment"],
        method="merkle",
        tuple_count=tuple_count,
        bad_leaf_count=len(manifest["bad_leaves"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        corruption_mode=manifest.get("corruption_mode", "paper-update-only"),
        **cfg,
    )
    total_start = now_ms()
    paper_start = total_start
    recovery_scan_before = seq_scan_snapshot(conn)
    counters = m.counters
    counters.update(planner_results)

    with timer(m.phase, "tree_localisation_ms"):
        bad_leaves = detect_bad_leaves(conn, counters)

    if bad_leaves != sorted(int(v) for v in manifest["bad_leaves"]):
        add_warning(m, f"bad leaves mismatch expected={manifest['bad_leaves']} actual={bad_leaves}")

    rows_inserted = rows_updated = rows_deleted = 0
    candidate_rows = healthy_rows = damaged_rows = 0
    lookup_scans = 0
    per_leaf_candidates: list[int] = []

    for leaf_id in bad_leaves:
        lookup_scans += 2
        hrows, drows, ins, upd, dlt = repair_leaf(conn, leaf_id, phase=m.phase)
        healthy_rows += len(hrows)
        damaged_rows += len(drows)
        leaf_total = len(hrows) + len(drows)
        candidate_rows += leaf_total
        per_leaf_candidates.append(leaf_total)
        rows_inserted += ins
        rows_updated += upd
        rows_deleted += dlt

    with timer(m.phase, "targeted_post_repair_confirmation_ms"):
        post_repair_counters: dict[str, Any] = {}
        remaining_bad_leaves = detect_bad_leaves(conn, post_repair_counters, prefix="targeted_confirmation_")
        repaired_leaf_mismatch = False
        from merkle_recovery.repair import fetch_leaf_rows
        for leaf_id in bad_leaves:
            if fetch_leaf_rows(conn, "healthy", leaf_id) != fetch_leaf_rows(conn, "damaged", leaf_id):
                repaired_leaf_mismatch = True
                break

    recovery_end = now_ms()
    paper_end = recovery_end
    recovery_scan_after = seq_scan_snapshot(conn)
    recovery_full_heap_scans = seq_scan_delta(recovery_scan_before, recovery_scan_after)

    audit_start = now_ms()
    verified = audit_recovery_with_scan_counters(conn, counters, m.run_id, m.method)
    audit_end = now_ms()
    schema_rows_out.extend(verified["schema_fidelity_rows"])
    m.phase.update(verified["audit_phase"])

    # ── Phase 4 extended counters ──────────────────────────────────────────
    leaf_stats = per_leaf_row_counts(bad_leaves, per_leaf_candidates)
    counters.update(
        {
            "leaf_lookup_sql_calls": lookup_scans,
            "candidate_rows_fetched": candidate_rows,
            "healthy_candidate_rows": healthy_rows,
            "damaged_candidate_rows": damaged_rows,
            "total_candidate_rows": candidate_rows,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "total_rows_repaired": rows_inserted + rows_updated + rows_deleted,
            "bad_leaf_count": len(bad_leaves),
            "bad_partition_count": counters.get("bad_partition_count", 0),
            "tree_nodes_visited": counters.get("tree_nodes_visited", 0),
            **leaf_stats,
            "targeted_confirmation_root_batches": post_repair_counters.get(
                "targeted_confirmation_partition_root_batches", 0
            ),
            "targeted_confirmation_root_nodes_read": post_repair_counters.get(
                "targeted_confirmation_partition_root_nodes_read", 0
            ),
            "recovery_user_table_seq_scan_delta": recovery_full_heap_scans,
            "partition_root_batches_ok": int(counters.get("partition_root_batches") == 2),
        }
    )

    if candidate_rows >= 0.5 * tuple_count:
        add_warning(m, "candidate rows exceed sparse threshold")
    if recovery_full_heap_scans != 0:
        add_warning(m, "recovery performed heap sequential scan")
    if remaining_bad_leaves or repaired_leaf_mismatch:
        add_warning(m, "targeted post-repair confirmation failed")
    if counters.get("partition_root_batches") != 2:
        add_warning(m, "partition root detection used more than two batches")
    if not verified["ok"]:
        add_warning(m, f"verification failed {verified}")

    cleanup_end = now_ms()
    finalize_metrics(
        m,
        total_start_ms=total_start,
        paper_start_ms=paper_start,
        paper_end_ms=paper_end,
        recovery_start_ms=paper_start,
        recovery_end_ms=recovery_end,
        audit_start_ms=audit_start,
        audit_end_ms=audit_end,
        cleanup_end_ms=cleanup_end,
    )
    m.phase["merkle_total_ms"] = m.end_to_end_observed_ms
    return m


# ── single-manifest loop ──────────────────────────────────────────────────────

def run_one_manifest(
    conn,
    manifest: dict[str, Any],
    reps: int,
    planner_rows_out: list[dict[str, Any]],
    schema_rows_out: list[dict[str, Any]],
    result_dir: Path,
    progress_state: dict[str, int],
) -> list[Metrics]:
    tuple_count = int(manifest["tuple_count"])
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    metrics: list[Metrics] = []
    for rep in range(reps):
        run_base = (
            f"{manifest['experiment']}-n{tuple_count}"
            f"-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
        )
        run_id = f"{run_base}-merkle-r{rep}"
        emit_progress(
            result_dir,
            event="method_start",
            run_id=run_id,
            experiment=manifest["experiment"],
            method="merkle",
            corruption_mode=manifest.get("corruption_mode", "paper-update-only"),
            tuple_count=tuple_count,
            partitions=cfg["partitions"],
            bad_leaf_count=len(manifest["bad_leaves"]),
            repetition=rep,
            completed_runs=progress_state["completed_runs"],
            total_runs=progress_state["total_runs"],
        )
        method_start = now_ms()
        reset_damaged_from_healthy(conn, cfg)
        apply_corruption(conn, manifest)
        planner_results, planner_rows = run_planner_preflight(conn, manifest, run_id)
        planner_rows_out.extend(planner_rows)
        metric = repair_merkle(conn, manifest, tuple_count, rep, planner_results, schema_rows_out)
        metrics.append(metric)
        progress_state["completed_runs"] += 1
        emit_progress(
            result_dir,
            event="method_complete",
            run_id=metric.run_id,
            experiment=metric.experiment,
            method=metric.method,
            corruption_mode=metric.corruption_mode,
            tuple_count=metric.tuple_count,
            partitions=metric.partitions,
            bad_leaf_count=metric.bad_leaf_count,
            repetition=metric.repetition,
            valid=metric.valid,
            warning=metric.warning,
            method_elapsed_ms=round(now_ms() - method_start, 3),
            paper_style_total_ms=round(metric.paper_style_total_ms, 3),
            audit_validation_ms=round(metric.audit_validation_ms, 3),
            completed_runs=progress_state["completed_runs"],
            total_runs=progress_state["total_runs"],
        )
    return metrics


def _selected(values, selected):
    out = list(values)
    if selected is None:
        return out
    return [v for v in out if v == selected]


def _count_planned_runs(config, args) -> int:
    reps = int(config.repetitions)
    total = 0
    if args.experiment in (None, "figure12"):
        total += len(_selected(config.fig12_sizes, args.tuple_count)) * reps
    if args.experiment in (None, "figure13"):
        total += (
            len(_selected([100, 200], args.partitions))
            * len(_selected(config.fig13_sizes, args.tuple_count))
            * len(_selected(config.fig13_k, args.bad_leaf_count))
            * reps
        )
    return total


# ── main benchmark orchestrator ───────────────────────────────────────────────

def run_benchmark(args: argparse.Namespace) -> Path:
    global RESULT_ROOT
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_dir = RESULT_ROOT / ts
    result_dir.mkdir(parents=True, exist_ok=False)
    (result_dir / "plots").mkdir()

    config = profile_config(args.profile)
    if args.repetitions is not None:
        config.repetitions = args.repetitions

    cfg_dict = config.to_dict()
    cfg_dict.update(vars(args))
    cfg_dict.update(BENCHMARK_SCOPE_METADATA)
    (result_dir / "config.json").write_text(json.dumps(cfg_dict, indent=2, default=str) + "\n")
    write_environment(result_dir, args)
    write_python_environment(result_dir)

    total_runs = _count_planned_runs(config, args)
    if total_runs <= 0:
        raise RuntimeError("selected benchmark filters match no runs")
    progress_state = {"completed_runs": 0, "total_runs": total_runs}
    emit_progress(
        result_dir,
        event="benchmark_start",
        profile=args.profile,
        repetitions=config.repetitions,
        total_runs=total_runs,
        experiment=args.experiment or "all",
        corruption_mode=args.corruption_mode,
    )

    all_metrics: list[Metrics] = []
    dataset_rows: list[dict[str, Any]] = []
    bucket_summary_rows: list[dict[str, Any]] = []
    bucket_debug_rows: list[dict[str, Any]] = []
    planner_rows: list[dict[str, Any]] = []
    schema_fidelity_rows: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []

    with connect(args) as conn:
        ensure_helpers(conn)

        # ── Figure 12 ────────────────────────────────────────────────────────
        fig12_sizes = _selected(config.fig12_sizes, args.tuple_count) \
            if args.experiment in (None, "figure12") else []
        for n in fig12_sizes:
            fig12_k = args.bad_leaf_count if args.bad_leaf_count is not None else (10 if n >= 10 else 1)
            emit_progress(result_dir, event="dataset_start", experiment="figure12",
                          tuple_count=n, partitions=200, bad_leaf_count=fig12_k,
                          completed_runs=progress_state["completed_runs"],
                          total_runs=total_runs)
            build_dataset(conn, n, 200, 16, 2)
            bsum, bdebug = bucket_consistency_sample(conn, n, 200, 16, 2, args.seed)
            bucket_summary_rows.append(bsum)
            if args.artifact_mode == "debug":
                bucket_debug_rows.extend(bdebug)
            occ = leaf_occupancy(conn)
            dataset_rows.append({**table_sizes(conn), "partitions": 200,
                                  "leaves_per_partition": 16, "fanout": 2,
                                  **occupancy_stats(occ)})
            emit_progress(result_dir, event="dataset_complete", experiment="figure12",
                          tuple_count=n, partitions=200, bad_leaf_count=fig12_k,
                          completed_runs=progress_state["completed_runs"],
                          total_runs=total_runs)
            d = 300 if args.profile in ("paper", "preflight") else fig12_k
            manifest = choose_corruption_manifest(
                conn, "figure12", n, 200, 16, 2, fig12_k, d, args.seed,
                corruption_mode=args.corruption_mode,
            )
            validate_manifest_leaf_mapping(conn, manifest)
            manifests.append(manifest)
            all_metrics.extend(
                run_one_manifest(conn, manifest, config.repetitions,
                                 planner_rows, schema_fidelity_rows,
                                 result_dir, progress_state)
            )

        # ── Figure 13 ────────────────────────────────────────────────────────
        if args.experiment in (None, "figure13"):
            fig13_partitions = _selected([100, 200], args.partitions)
            fig13_sizes = _selected(config.fig13_sizes, args.tuple_count)
            fig13_k_values = _selected(config.fig13_k, args.bad_leaf_count)
        else:
            fig13_partitions = fig13_sizes = fig13_k_values = []

        for partitions in fig13_partitions:
            for n in fig13_sizes:
                emit_progress(result_dir, event="dataset_start", experiment="figure13",
                              tuple_count=n, partitions=partitions,
                              completed_runs=progress_state["completed_runs"],
                              total_runs=total_runs)
                build_dataset(conn, n, partitions, 16, 2)
                bsum, bdebug = bucket_consistency_sample(conn, n, partitions, 16, 2, args.seed)
                bucket_summary_rows.append(bsum)
                if args.artifact_mode == "debug":
                    bucket_debug_rows.extend(bdebug)
                occ = leaf_occupancy(conn)
                dataset_rows.append({**table_sizes(conn), "partitions": partitions,
                                      "leaves_per_partition": 16, "fanout": 2,
                                      **occupancy_stats(occ)})
                emit_progress(result_dir, event="dataset_complete", experiment="figure13",
                              tuple_count=n, partitions=partitions,
                              completed_runs=progress_state["completed_runs"],
                              total_runs=total_runs)
                for k in fig13_k_values:
                    d = 300 if args.profile in ("paper", "preflight") else k
                    manifest = choose_corruption_manifest(
                        conn, "figure13", n, partitions, 16, 2, k, d, args.seed,
                        corruption_mode=args.corruption_mode,
                    )
                    validate_manifest_leaf_mapping(conn, manifest)
                    manifests.append(manifest)
                    all_metrics.extend(
                        run_one_manifest(conn, manifest, config.repetitions,
                                         planner_rows, schema_fidelity_rows,
                                         result_dir, progress_state)
                    )

    # ── write artifacts ───────────────────────────────────────────────────────
    (result_dir / "corruption_manifest.json").write_text(json.dumps(manifests, indent=2) + "\n")

    write_csv(result_dir / "dataset_sizes.csv", dataset_rows, [
        "tuple_count", "partitions", "leaves_per_partition", "fanout",
        "base_table_bytes", "primary_index_bytes", "merkle_index_bytes",
        "leaf_lookup_index_bytes", "total_schema_bytes",
        "minimum", "p50", "p95", "p99", "maximum", "mean", "stddev",
    ])
    write_csv(result_dir / "bucket_consistency_summary.csv", bucket_summary_rows, [
        "tuple_count", "partitions", "leaves_per_partition", "fanout",
        "sample_count", "sample_seed", "mismatch_count", "sample_digest",
    ])
    if args.artifact_mode == "debug":
        write_csv(result_dir / "bucket_consistency.csv", bucket_debug_rows, [
            "tuple_count", "partitions", "leaves_per_partition", "fanout",
            "ycsb_key", "bucket", "leaf_id", "match",
        ])
    write_csv(result_dir / "planner_checks.csv", planner_rows, [
        "run_id", "schema", "leaf_id", "index_oid", "index_relfilenode",
        "index_definition", "plan_uses_expected_leaf_lookup_index", "plan_json_sha256",
    ])
    write_csv(result_dir / "schema_fidelity.csv", schema_fidelity_rows, [
        "run_id", "method", "check_name", "healthy_value", "damaged_value", "match",
    ])

    run_rows, phase_rows = metrics_to_rows(all_metrics)
    all_run_fields = sorted({k for r in run_rows for k in r})
    write_csv(result_dir / "runs.csv", run_rows, all_run_fields)
    write_csv(result_dir / "phase_timings.csv", phase_rows, ["run_id", "method", "phase", "ms"])
    write_csv(
        result_dir / "timing_contract.csv",
        [
            {
                "run_id": m.run_id,
                "method": m.method,
                "paper_style_total_ms": f"{m.paper_style_total_ms:.3f}",
                "restore_repair_ms": f"{m.restore_repair_ms:.3f}",
                "audit_validation_ms": f"{m.audit_validation_ms:.3f}",
                "end_to_end_observed_ms": f"{m.end_to_end_observed_ms:.3f}",
                "cleanup_ms": f"{m.cleanup_ms:.3f}",
                "paper_end_before_audit_start": m.counters.get("paper_end_before_audit_start", 0),
                "audit_validation_positive": m.counters.get("audit_validation_positive", 0),
                "end_to_end_covers_paper_and_audit": m.counters.get("end_to_end_covers_paper_and_audit", 0),
            }
            for m in all_metrics
        ],
        [
            "run_id", "method", "paper_style_total_ms", "restore_repair_ms",
            "audit_validation_ms", "end_to_end_observed_ms", "cleanup_ms",
            "paper_end_before_audit_start", "audit_validation_positive",
            "end_to_end_covers_paper_and_audit",
        ],
    )
    write_csv(
        result_dir / "verification_results.csv",
        [{"all_runs_valid": int(all(m.valid for m in all_metrics))}],
        ["all_runs_valid"],
    )

    assert_benchmark_contract(args.profile, all_metrics)

    try:
        from plot_recovery_results import plot_all
        plot_all(result_dir)
    except Exception as exc:
        print(f"[warn] plotting failed: {exc}", file=sys.__stderr__)

    emit_progress(
        result_dir,
        event="benchmark_complete",
        completed_runs=progress_state["completed_runs"],
        total_runs=total_runs,
        all_runs_valid=all(m.valid for m in all_metrics),
    )
    return result_dir


def main(argv: list[str] | None = None) -> int:
    global RESULT_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="host=127.0.0.1 port=5432 dbname=postgres user=neel")
    parser.add_argument("--profile", choices=["smoke", "preflight", "paper"], default="smoke")
    parser.add_argument("--experiment", choices=["figure12", "figure13"])
    parser.add_argument("--tuple-count", type=int, dest="tuple_count")
    parser.add_argument("--partitions", type=int)
    parser.add_argument("--bad-leaf-count", type=int, dest="bad_leaf_count")
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--seed", type=int, default=20260703)
    parser.add_argument("--result-dir", dest="result_dir")
    parser.add_argument("--scratch-dir", dest="scratch_dir")
    parser.add_argument("--artifact-mode", choices=["summary", "debug"], default="summary",
                        dest="artifact_mode")
    parser.add_argument(
        "--corruption-mode",
        choices=["paper-update-only", "update-only", "delete-only", "insert-only", "mixed"],
        default="paper-update-only",
        dest="corruption_mode",
        help="Corruption injection mode. Use paper-update-only for paper-profile runs.",
    )
    args = parser.parse_args(argv)

    if args.result_dir:
        RESULT_ROOT = Path(args.result_dir)

    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    # Use --scratch-dir as the parent of the tmp_ directory when supplied,
    # so the remote launcher's dedicated scratch volume is honoured.
    scratch_parent = Path(args.scratch_dir) if args.scratch_dir else RESULT_ROOT
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch = scratch_parent / ("tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    scratch.mkdir(parents=True, exist_ok=False)
    try:
        with (scratch / "stdout.log").open("w") as out, (scratch / "stderr.log").open("w") as err:
            with redirect_stdout(out), redirect_stderr(err):
                result_dir = run_benchmark(args)
        shutil.move(str(scratch / "stdout.log"), result_dir / "stdout.log")
        shutil.move(str(scratch / "stderr.log"), result_dir / "stderr.log")
        scratch.rmdir()
        print(result_dir)
        return 0
    except Exception:
        print(f"failed; logs in {scratch}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
