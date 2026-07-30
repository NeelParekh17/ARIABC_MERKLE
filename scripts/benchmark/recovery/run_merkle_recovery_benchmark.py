#!/usr/bin/env python3
"""Paper-style Merkle-only recovery benchmark for AriaBC (Figures 12 & 13)."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
import time
from statistics import median
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Any

from merkle_recovery.config import (
    BENCH_DIR, RESULT_ROOT as _DEFAULT_RESULT_ROOT, GEOMETRY_MATRIX_PATH,
    BENCHMARK_SCHEMA_VERSION, TIMING_CONTRACT_VERSION,
    BENCHMARK_SCOPE_METADATA,
    profile_config,
)
from merkle_recovery.db import connect, execute, scalar, show_setting
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
    fetch_leaf_rows, fetch_leaf_rows_batch, run_planner_preflight, repair_leaf,
    seq_scan_snapshot, seq_scan_delta,
    per_leaf_row_counts, FetchResult,
)
from merkle_recovery.verification import audit_recovery_with_scan_counters, schema_fidelity_checks
from merkle_recovery.metrics import Metrics, add_warning, finalize_metrics
from merkle_recovery.reporting import (
    emit_progress, write_environment, write_python_environment,
    write_csv, metrics_to_rows, assert_benchmark_contract,
)
from merkle_recovery.profiling import (
    ProfileCollector, group_profile_rows_with_denominators,
    group_profile_rows_with_fraction, validate_profile_invariants,
    record_call,
)

RESULT_ROOT = _DEFAULT_RESULT_ROOT

PROFILE_OPERATION_FIELDS = [
    "run_id", "manifest_sha256", "experiment", "tuple_count", "partitions",
    "leaves_per_partition", "fanout", "profile_label", "bad_leaf_count",
    "corrupted_tuple_count", "repetition", "stage", "operation",
    "schema", "partition", "node_in_partition", "leaf_id",
    "call_ordinal", "rows_returned", "client_wall_ms", "success",
]


# ── timing helper ─────────────────────────────────────────────────────────────

def now_ms() -> float:
    return time.perf_counter() * 1000.0


@contextmanager
def timer(store: dict[str, float], name: str):
    start = now_ms()
    yield
    store[name] = store.get(name, 0.0) + now_ms() - start


def tree_depth(leaves_per_partition: int, fanout: int) -> int:
    if leaves_per_partition <= 0 or fanout <= 1:
        return 0
    depth = 1
    nodes = 1
    while nodes < leaves_per_partition:
        nodes *= fanout
        depth += 1
    return depth


def nodes_per_partition(leaves_per_partition: int, fanout: int) -> int:
    if leaves_per_partition <= 0:
        return 0
    total = 0
    level_nodes = 1
    remaining = leaves_per_partition
    while remaining > 0:
        total += level_nodes
        if remaining == 1:
            break
        remaining = (remaining + fanout - 1) // fanout
        level_nodes *= fanout
    return total


def load_geometry_matrix() -> dict[str, dict[str, int]]:
    if not GEOMETRY_MATRIX_PATH.exists():
        return {}
    data = json.loads(GEOMETRY_MATRIX_PATH.read_text())
    return {k: {kk: int(vv) for kk, vv in v.items()} for k, v in data.items()}


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, default=str).encode()).hexdigest()


def recovery_run_id(manifest: dict[str, Any], repetition: int, profile_label: str) -> str:
    label = profile_label or str(manifest.get("geometry_label", "")) or f"l{manifest['leaves_per_partition']}"
    safe_label = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label)
    return (
        f"{manifest['experiment']}-n{manifest['tuple_count']}"
        f"-p{manifest['partitions']}"
        f"-l{manifest['leaves_per_partition']}"
        f"-f{manifest['fanout']}"
        f"-k{len(manifest['bad_leaves'])}"
        f"-c{len(manifest['corruptions'])}"
        f"-{safe_label}-merkle-r{repetition}"
    )


def validate_geometry(partitions: int, leaves_per_partition: int, fanout: int) -> None:
    if partitions <= 0:
        raise ValueError(f"invalid Merkle geometry: partitions must be > 0, got {partitions}")
    if fanout < 2:
        raise ValueError(f"invalid Merkle geometry: fanout must be >= 2, got {fanout}")
    if leaves_per_partition <= 0:
        raise ValueError(
            f"invalid Merkle geometry: leaves_per_partition must be > 0, got {leaves_per_partition}"
        )
    value = leaves_per_partition
    while value > 1 and value % fanout == 0:
        value //= fanout
    if value != 1:
        raise ValueError(
            "invalid Merkle geometry: leaves_per_partition must be an exact power "
            f"of fanout, got leaves_per_partition={leaves_per_partition}, fanout={fanout}"
        )


def _manual_geometry_label(partitions: int, leaves_per_partition: int, fanout: int) -> str:
    return f"manual-p{partitions}-l{leaves_per_partition}-f{fanout}"


def validate_backend_profile_stats(
    backend: dict[str, Any],
    cfg: dict[str, int],
    counters: dict[str, Any],
    post_repair_counters: dict[str, Any],
    *,
    rows_updated: int,
    rows_inserted: int,
    rows_deleted: int,
) -> list[str]:
    reasons: list[str] = []
    expected_root_calls = 4
    expected_root_nodes = expected_root_calls * cfg["partitions"]
    expected_child_calls = int(counters.get("child_hash_sql_calls", 0)) + int(
        post_repair_counters.get("targeted_confirmation_child_hash_sql_calls", 0)
    )
    expected_child_nodes = int(counters.get("child_hash_nodes_read", 0)) + int(
        post_repair_counters.get("targeted_confirmation_child_hash_nodes_read", 0)
    )
    checks = [
        ("root_hash_helper_calls", expected_root_calls),
        ("root_hash_nodes_returned", expected_root_nodes),
        ("child_hash_helper_calls", expected_child_calls),
        ("child_hash_nodes_returned", expected_child_nodes),
    ]
    for field, expected in checks:
        actual = int(backend.get(field, 0))
        if actual != expected:
            reasons.append(f"{field} expected {expected}, got {actual}")

    if rows_updated > 0:
        expected_row_hash_calls = rows_updated * 2
        actual_row_hash_calls = int(backend.get("row_hash_compute_calls", 0))
        if actual_row_hash_calls != expected_row_hash_calls:
            reasons.append(
                f"row_hash_compute_calls expected {expected_row_hash_calls}, "
                f"got {actual_row_hash_calls}"
            )

    if rows_updated > 0 and rows_inserted == 0 and rows_deleted == 0:
        expected_path_calls = rows_updated * 2
        expected_nodes_touched = expected_path_calls * tree_depth(
            cfg["leaves_per_partition"], cfg["fanout"]
        )
        if int(backend.get("tree_path_update_calls", 0)) != expected_path_calls:
            reasons.append(
                f"tree_path_update_calls expected {expected_path_calls}, "
                f"got {backend.get('tree_path_update_calls', 0)}"
            )
        if int(backend.get("tree_path_nodes_touched", 0)) != expected_nodes_touched:
            reasons.append(
                f"tree_path_nodes_touched expected {expected_nodes_touched}, "
                f"got {backend.get('tree_path_nodes_touched', 0)}"
            )
        if int(backend.get("tree_path_update_ns", 0)) <= 0:
            reasons.append("tree_path_update_ns is zero despite tree updates")
    return reasons


def _dataset_row(conn, tuple_count: int, geo: dict[str, int], profile_label: str = "") -> dict[str, Any]:
    occ = leaf_occupancy(conn)
    sizes = table_sizes(conn)
    leaf_count = geo["partitions"] * geo["leaves_per_partition"]
    node_count = geo.get("nodes_per_partition")
    if node_count is None:
        node_count = nodes_per_partition(geo["leaves_per_partition"], geo["fanout"])
    # tree_depth() returns the count of nodes on a root-to-leaf path (i.e. tree_levels).
    # tree_edges = tree_levels - 1.
    t_levels = tree_depth(geo["leaves_per_partition"], geo["fanout"])
    phys_per_leaf = tuple_count / max(1, leaf_count)
    row = {
        **sizes,
        "profile_label": profile_label,
        "tuple_count": tuple_count,
        "partitions": geo["partitions"],
        "leaves_per_partition": geo["leaves_per_partition"],
        "fanout": geo["fanout"],
        "total_leaf_count": leaf_count,
        # unambiguous tree-shape fields
        "tree_levels": t_levels,
        "tree_edges": t_levels - 1,
        "tree_depth": t_levels,               # kept for backward compat of downstream tools
        "nodes_per_partition": node_count,
        "total_merkle_nodes": geo["partitions"] * node_count,
        "total_logical_tree_nodes": geo["partitions"] * node_count,
        "physical_rows_per_leaf_expected": round(phys_per_leaf, 2),
        "expected_candidate_rows_per_bad_leaf": round(2 * phys_per_leaf, 2),
        **occupancy_stats(occ),
    }
    return row



def _run_deep_diagnostics(
    conn,
    manifest: dict[str, Any],
    result_dir: Path,
    *,
    run_id: str,
    tuple_count: int,
    partitions: int,
    leaves_per_partition: int,
    fanout: int,
    profile_label: str,
    repetition: int,
) -> list[dict[str, Any]]:
    from merkle_recovery.repair import lookup_explain_plan_json

    def plan_uses_index(node: Any, index_name: str) -> bool:
        if isinstance(node, dict):
            if node.get("Index Name") == index_name:
                return True
            return any(plan_uses_index(value, index_name) for value in node.values())
        if isinstance(node, list):
            return any(plan_uses_index(value, index_name) for value in node)
        return False

    jsonl_path = result_dir / "deep_plan_profiles.jsonl"
    summary_rows: list[dict[str, Any]] = []
    io_timing_available = int(str(show_setting(conn, "track_io_timing")).lower() == "on")
    diagnostic_replay_id = f"{run_id}-deep-diagnostic"
    manifest_digest = manifest_sha256(manifest)

    def capture_plans(kind: str, plan_order: int) -> None:
        with jsonl_path.open("a") as jsonl:
            bad_leaf_keys = [(bytes.fromhex(v[0]), int(v[1])) if isinstance(v, (list, tuple)) else v for v in manifest["bad_leaves"]]
            for leaf_id in sorted(bad_leaf_keys):
                for schema in ("healthy", "damaged"):
                    plan = lookup_explain_plan_json(conn, schema, leaf_id)
                    if plan is None:
                        continue
                    payload = {
                        "run_id": run_id,
                        "manifest_sha256": manifest_digest,
                        "diagnostic_replay_id": diagnostic_replay_id,
                        "diagnostic_cache_mode": "post_recovery_warm",
                        "tuple_count": tuple_count,
                        "partitions": partitions,
                        "leaves_per_partition": leaves_per_partition,
                        "fanout": fanout,
                        "profile_label": profile_label,
                        "repetition": repetition,
                        "leaf_id": leaf_id,
                        "schema": schema,
                        "kind": kind,
                        "plan_order": plan_order,
                        "io_timing_available": io_timing_available,
                        "plan": plan,
                    }
                    jsonl.write(json.dumps(payload, sort_keys=True) + "\n")
                    plan_node = plan.get("Plan", {})
                    summary_rows.append(
                        {
                            "leaf_id": leaf_id,
                            "run_id": run_id,
                            "manifest_sha256": manifest_digest,
                            "diagnostic_replay_id": diagnostic_replay_id,
                            "diagnostic_cache_mode": "post_recovery_warm",
                            "tuple_count": tuple_count,
                            "partitions": partitions,
                            "leaves_per_partition": leaves_per_partition,
                            "fanout": fanout,
                            "profile_label": profile_label,
                            "repetition": repetition,
                            "schema": schema,
                            "kind": kind,
                            "plan_order": plan_order,
                            "planning_ms": plan.get("Planning Time", 0.0),
                            "execution_ms": plan.get("Execution Time", 0.0),
                            "actual_rows": plan_node.get("Actual Rows", 0),
                            "shared_hit_blocks": plan_node.get("Shared Hit Blocks", 0),
                            "shared_read_blocks": plan_node.get("Shared Read Blocks", 0),
                            "shared_dirtied_blocks": plan_node.get("Shared Dirtied Blocks", 0),
                            "shared_written_blocks": plan_node.get("Shared Written Blocks", 0),
                            "local_hit_blocks": plan_node.get("Local Hit Blocks", 0),
                            "local_read_blocks": plan_node.get("Local Read Blocks", 0),
                            "temp_read_blocks": plan_node.get("Temp Read Blocks", 0),
                            "temp_written_blocks": plan_node.get("Temp Written Blocks", 0),
                            "wal_records": plan_node.get("WAL Records", 0),
                            "wal_bytes": plan_node.get("WAL Bytes", 0),
                            "plan_uses_expected_leaf_lookup_index": int(
                                plan_uses_index(plan, f"{schema}.usertable_merkle_covering_idx")
                            ),
                            "io_timing_available": io_timing_available,
                        }
                    )

    reset_damaged_from_healthy(conn, {
        "partitions": partitions,
        "leaves_per_partition": leaves_per_partition,
        "fanout": fanout,
    })
    apply_corruption(conn, manifest)
    capture_plans("candidate", 1)
    diagnostic_phase: dict[str, float] = {}
    for leaf_id in sorted(int(v) for v in manifest["bad_leaves"]):
        repair_leaf(conn, leaf_id, phase=diagnostic_phase, profiler=None)
    capture_plans("confirmation", 2)
    return summary_rows


# ── core Merkle repair run ────────────────────────────────────────────────────

def repair_merkle(
    conn,
    manifest: dict[str, Any],
    tuple_count: int,
    repetition: int,
    planner_results: dict[str, Any],
    schema_rows_out: list[dict[str, Any]],
    *,
    profile_label: str = "",
    profiling_mode: str = "off",
    profiler: ProfileCollector | None = None,
    benchmark_profile: str = "",
    audit_mode: str = "full",
    leaf_fetch_chunk_size: int = 64,
) -> Metrics:
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    run_id = recovery_run_id(manifest, repetition, profile_label)
    manifest_digest = manifest_sha256(manifest)
    m = Metrics(
        run_id=run_id,
        experiment=manifest["experiment"],
        method="merkle",
        tuple_count=tuple_count,
        bad_leaf_count=len(manifest["bad_leaves"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        corruption_mode=manifest.get("corruption_mode", "paper-update-only"),
        profile_label=profile_label,
        profiling_mode=profiling_mode,
        **cfg,
    )
    total_start = now_ms()
    paper_start = total_start
    recovery_scan_before = seq_scan_snapshot(conn)
    counters = m.counters
    counters.update(planner_results)
    counters["manifest_sha256"] = manifest_digest

    with timer(m.phase, "tree_localisation_ms"):
        bad_leaves = detect_bad_leaves(conn, counters, profiler=profiler)

    expected_bad_leaves = sorted(set((bytes.fromhex(v[0]), int(v[1])) if isinstance(v, (list, tuple)) else v for v in manifest["bad_leaves"]))
    if sorted(bad_leaves) != expected_bad_leaves:
        add_warning(m, f"bad leaves mismatch expected={expected_bad_leaves} actual={bad_leaves}")

    rows_inserted = rows_updated = rows_deleted = 0
    candidate_rows = healthy_rows = damaged_rows = 0
    lookup_scans = 0
    per_leaf_candidates: list[int] = []

    # Split bad_leaves into chunks to bound peak memory.
    if leaf_fetch_chunk_size <= 0:
        bad_leaf_chunks = [bad_leaves]
    else:
        bad_leaf_chunks = [bad_leaves[i:i + leaf_fetch_chunk_size] for i in range(0, len(bad_leaves), leaf_fetch_chunk_size)]

    candidate_fetch_sql_calls = 0
    candidate_fetch_batches = 0
    candidate_leaf_buckets_requested = 0

    for chunk in bad_leaf_chunks:
        if not chunk:
            continue

        with timer(m.phase, "candidate_row_fetch_ms"):
            healthy_by_leaf: FetchResult = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_batch_healthy",
                schema="healthy",
                fn=lambda: fetch_leaf_rows_batch(conn, "healthy", chunk,
                                                 chunk_size=0),
            )
            damaged_by_leaf: FetchResult = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_batch_damaged",
                schema="damaged",
                fn=lambda: fetch_leaf_rows_batch(conn, "damaged", chunk,
                                                 chunk_size=0),
            )

        candidate_fetch_sql_calls += healthy_by_leaf.sql_calls + damaged_by_leaf.sql_calls
        candidate_fetch_batches += healthy_by_leaf.batches + damaged_by_leaf.batches
        candidate_leaf_buckets_requested += healthy_by_leaf.leaf_buckets_requested + damaged_by_leaf.leaf_buckets_requested

        for leaf_id in chunk:
            hrows, drows, ins, upd, dlt = repair_leaf(
                conn,
                leaf_id,
                phase=m.phase,
                profiler=profiler,
                prefetched=(
                    healthy_by_leaf.get(leaf_id, {}),
                    damaged_by_leaf.get(leaf_id, {}),
                ),
            )
            healthy_rows += len(hrows)
            damaged_rows += len(drows)
            leaf_total = len(hrows) + len(drows)
            candidate_rows += leaf_total
            per_leaf_candidates.append(leaf_total)
            rows_inserted += ins
            rows_updated += upd
            rows_deleted += dlt

        # Discard chunk from memory
        del healthy_by_leaf
        del damaged_by_leaf

    if bad_leaves:
        lookup_scans = candidate_fetch_sql_calls

    with timer(m.phase, "targeted_post_repair_confirmation_ms"):
        # Repair DML commits a second Merkle delta batch.  Confirmation reads
        # must observe the repaired roots, so drain that batch at the durable
        # boundary before asking the backend for recovery status.
        record_call(
            profiler,
            stage="targeted_confirmation",
            operation="confirmation_merkle_apply_pending",
            fn=lambda: execute(conn, "SELECT merkle_apply_pending()"),
        )
        post_repair_counters: dict[str, Any] = {}
        remaining_bad_leaves = detect_bad_leaves(
            conn,
            post_repair_counters,
            prefix="targeted_confirmation_",
            operation_prefix="confirmation_",
            profiler=profiler,
            stage_name="targeted_confirmation",
        )
        repaired_leaf_mismatch = False
        confirmation_fetch_sql_calls = 0
        confirmation_fetch_batches = 0
        confirmation_leaf_buckets_requested = 0
        confirmation_rows_fetched = 0

        for chunk in bad_leaf_chunks:
            if not chunk:
                continue

            confirmed_healthy: FetchResult = record_call(
                profiler,
                stage="targeted_confirmation",
                operation="confirmation_leaf_fetch_batch_healthy",
                schema="healthy",
                fn=lambda: fetch_leaf_rows_batch(conn, "healthy", chunk,
                                                 chunk_size=0),
            )
            confirmed_damaged: FetchResult = record_call(
                profiler,
                stage="targeted_confirmation",
                operation="confirmation_leaf_fetch_batch_damaged",
                schema="damaged",
                fn=lambda: fetch_leaf_rows_batch(conn, "damaged", chunk,
                                                 chunk_size=0),
            )

            confirmation_fetch_sql_calls += confirmed_healthy.sql_calls + confirmed_damaged.sql_calls
            confirmation_fetch_batches += confirmed_healthy.batches + confirmed_damaged.batches
            confirmation_leaf_buckets_requested += confirmed_healthy.leaf_buckets_requested + confirmed_damaged.leaf_buckets_requested
            confirmation_rows_fetched += confirmed_healthy.rows_fetched + confirmed_damaged.rows_fetched

            for leaf_id in chunk:
                hrows = confirmed_healthy.get(leaf_id, {})
                drows = confirmed_damaged.get(leaf_id, {})
                if profiler is not None and profiler.enabled:
                    t0 = time.perf_counter_ns()
                    repaired_leaf_mismatch = repaired_leaf_mismatch or (hrows != drows)
                    profiler.record(
                        stage="targeted_confirmation",
                        operation="confirmation_compare_cpu",
                        leaf_id=leaf_id,
                        rows_returned=len(hrows) + len(drows),
                        client_wall_ns=time.perf_counter_ns() - t0,
                    )
                elif hrows != drows:
                    repaired_leaf_mismatch = True

            del confirmed_healthy
            del confirmed_damaged

    with timer(m.phase, "recovery_observability_ms"):
        if profiler is not None and profiler.enabled:
            stats = scalar(conn, "SELECT merkle_recovery_profile_stats()")
            profiler.extend_backend_profile(json.loads(stats))
            backend = profiler.backend_profile or {}
            backend_reasons = validate_backend_profile_stats(
                backend,
                cfg,
                counters,
                post_repair_counters,
                rows_updated=rows_updated,
                rows_inserted=rows_inserted,
                rows_deleted=rows_deleted,
            )
            if backend_reasons:
                add_warning(m, f"backend profile invariant failed: {backend_reasons}; {backend}")
                raise RuntimeError(
                    f"backend profile invariant failed for {m.run_id}: {backend_reasons}; {backend}"
                )

    recovery_end = now_ms()
    paper_end = recovery_end

    total_recovery_ms = recovery_end - paper_start
    accounted_ms = (
        m.phase.get("tree_localisation_ms", 0.0)
        + m.phase.get("candidate_row_fetch_ms", 0.0)
        + m.phase.get("row_comparison_ms", 0.0)
        + m.phase.get("repair_write_ms", 0.0)
        + m.phase.get("targeted_post_repair_confirmation_ms", 0.0)
        + m.phase.get("recovery_observability_ms", 0.0)
    )
    m.phase["recovery_orchestration_ms"] = max(0.0, total_recovery_ms - accounted_ms)

    recovery_scan_after = seq_scan_snapshot(conn)
    recovery_full_heap_scans = seq_scan_delta(recovery_scan_before, recovery_scan_after)

    try:
        res = execute(conn, "SELECT merkle_apply_pending()")
        print("merkle_apply_pending returned:", res)
    except Exception as e:
        print("merkle_apply_pending exception:", e)

    audit_start = now_ms()
    if audit_mode == "full":
        verified = audit_recovery_with_scan_counters(conn, counters, m.run_id, m.method)
        full_audit_skipped = 0
    else:
        schema_rows = schema_fidelity_checks(conn, m.run_id, m.method)
        schema_ok = all(int(r["match"]) == 1 for r in schema_rows)
        index_count = scalar(
            conn,
            """
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = 'damaged'
              AND indexname IN ('usertable_pkey', 'usertable_merkle_idx', 'usertable_merkle_covering_idx')
            """,
        )
        verified = {
            "healthy_minus_damaged": 0,
            "damaged_minus_healthy": 0,
            "roots_match": True,
            "healthy_merkle_verify": True,
            "damaged_merkle_verify": True,
            "damaged_required_indexes": int(index_count),
            "audit_validation_ms": 0.0,
            "audit_phase": {},
            "schema_fidelity_ok": bool(schema_ok),
            "schema_fidelity_rows": schema_rows,
            "ok": bool(schema_ok) and int(index_count) == 3,
        }
        counters.update(
            {
                "audit_user_table_seq_scan_delta": 0,
                "audit_merkle_root_hash_calls": 0,
                "audit_merkle_verify_calls": 0,
                "audit_validation_ms": 0.0,
                "schema_fidelity_ok": int(schema_ok),
            }
        )
        full_audit_skipped = 1
    audit_end = now_ms()
    schema_rows_out.extend(verified["schema_fidelity_rows"])
    m.phase.update(verified["audit_phase"])

    # ── Phase 4 extended counters ──────────────────────────────────────────
    leaf_stats = per_leaf_row_counts(bad_leaves, per_leaf_candidates)
    counters.update(
        {
            # Candidate fetch: SQL-call granularity vs. rows-returned granularity.
            "leaf_lookup_sql_calls": lookup_scans,
            "candidate_fetch_sql_calls": candidate_fetch_sql_calls,
            "candidate_fetch_batches": candidate_fetch_batches,
            "candidate_leaf_buckets_requested": candidate_leaf_buckets_requested,
            "candidate_rows_fetched": candidate_rows,
            "confirmation_fetch_sql_calls": confirmation_fetch_sql_calls,
            "confirmation_fetch_batches": confirmation_fetch_batches,
            "confirmation_leaf_buckets_requested": confirmation_leaf_buckets_requested,
            "confirmation_rows_fetched": confirmation_rows_fetched,
            # Legacy alias kept for downstream tooling.
            "candidate_rows_fetched_alias": candidate_rows,
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
            "selected_bad_leaf_row_capacity": manifest.get("selected_bad_leaf_row_capacity", 0),
            **leaf_stats,
            "targeted_confirmation_root_batches": post_repair_counters.get(
                "targeted_confirmation_partition_root_batches", 0
            ),
            "targeted_confirmation_root_nodes_read": post_repair_counters.get(
                "targeted_confirmation_partition_root_nodes_read", 0
            ),
            "recovery_user_table_seq_scan_delta": recovery_full_heap_scans,
            "partition_root_batches": counters.get("child_hash_sql_calls", 0) // 2,
            "partition_root_batches_ok": int((counters.get("child_hash_sql_calls", 0) // 2) > 0),
            "full_audit_skipped": full_audit_skipped,
            "audit_mode": audit_mode,
            "leaf_fetch_chunk_size": leaf_fetch_chunk_size,
        }
    )

    if candidate_rows >= 0.5 * tuple_count:
        add_warning(m, "candidate rows exceed sparse threshold")
    if recovery_full_heap_scans != 0:
        add_warning(m, "recovery performed heap sequential scan")

    if benchmark_profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
        expected_bad_leaf_count = 75 if benchmark_profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") else 20
        if m.bad_leaf_count != expected_bad_leaf_count:
            raise RuntimeError(f"{m.run_id}: bad_leaf_count={m.bad_leaf_count}, expected {expected_bad_leaf_count}")
        if m.corrupted_tuple_count != 300:
            raise RuntimeError(f"{m.run_id}: corrupted_tuple_count={m.corrupted_tuple_count}, expected 300")
        if len(bad_leaves) != expected_bad_leaf_count:
            raise RuntimeError(f"{m.run_id}: detected_bad_leaves={len(bad_leaves)}, expected {expected_bad_leaf_count}")
        expected_bad_leaves = sorted(int(v) for v in manifest["bad_leaves"])
        if bad_leaves != expected_bad_leaves:
            raise RuntimeError(
                f"{m.run_id}: detected leaves do not match manifest: "
                f"expected={expected_bad_leaves}, actual={bad_leaves}"
            )
        total_repaired = rows_inserted + rows_updated + rows_deleted
        if total_repaired != 300:
            raise RuntimeError(f"{m.run_id}: total_rows_repaired={total_repaired}, expected 300")
        if m.corruption_mode != "paper-update-only":
            raise RuntimeError(f"{m.run_id}: corruption_mode={m.corruption_mode}, expected paper-update-only")
        if recovery_full_heap_scans != 0:
            raise RuntimeError(f"{m.run_id}: recovery_user_table_seq_scan_delta={recovery_full_heap_scans}, expected 0")
    if remaining_bad_leaves or repaired_leaf_mismatch:
        add_warning(m, "targeted post-repair confirmation failed")
    if counters.get("partition_root_batches", 0) <= 0:
        add_warning(m, "partition root detection produced 0 batches")
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
        audit_end_ms=audit_start if audit_mode == "skip" else audit_end,
        cleanup_end_ms=cleanup_end,
        audit_skipped=(audit_mode == "skip"),
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
    profile_operation_rows_out: list[dict[str, Any]],
    backend_profile_rows_out: list[dict[str, Any]],
    deep_plan_summary_rows_out: list[dict[str, Any]],
    result_dir: Path,
    progress_state: dict[str, int],
    *,
    profile_label: str = "",
    profiling_mode: str = "off",
    benchmark_profile: str = "",
    audit_mode: str = "full",
    leaf_fetch_chunk_size: int = 64,
) -> list[Metrics]:
    tuple_count = int(manifest["tuple_count"])
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    metrics: list[Metrics] = []
    for rep in range(reps):
        run_id = recovery_run_id(manifest, rep, profile_label)
        manifest_digest = manifest_sha256(manifest)
        emit_progress(
            result_dir,
            event="method_start",
            run_id=run_id,
            manifest_sha256=manifest_digest,
            experiment=manifest["experiment"],
            profile_label=profile_label,
            profiling_mode=profiling_mode,
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
        print("[TRACE] state before apply_corruption:", execute(conn, "SELECT * FROM ariabc_internal.merkle_apply_state"))
        apply_corruption(conn, manifest)
        print("[TRACE] state after apply_corruption:", execute(conn, "SELECT * FROM ariabc_internal.merkle_apply_state"))
        execute(conn, "SELECT merkle_apply_pending()")
        print("[TRACE] state after merkle_apply_pending:", execute(conn, "SELECT * FROM ariabc_internal.merkle_apply_state"))
        planner_results, planner_rows = run_planner_preflight(conn, manifest, run_id)
        print("[TRACE] state after run_planner_preflight:", execute(conn, "SELECT * FROM ariabc_internal.merkle_apply_state"))
        planner_rows_out.extend(planner_rows)
        profiler = None
        if profiling_mode in ("light", "deep"):
            try:
                execute(conn, "SHOW merkle_recovery_profile_enabled")
                execute(conn, "SET merkle_recovery_profile_enabled = on")
                execute(conn, "SELECT merkle_recovery_profile_reset()")
                execute(conn, "SELECT merkle_recovery_profile_stats()")
            except Exception as exc:
                raise RuntimeError(
                    "Profiling backend is not installed in this PGDATA. "
                    "Use the benchmark bootstrap command to create a fresh profiling cluster."
                ) from exc
            profiler = ProfileCollector(
                run_id=run_id,
                manifest_sha256=manifest_digest,
                experiment=manifest["experiment"],
                tuple_count=tuple_count,
                split_threshold=cfg.get("split_threshold", 32),
                merge_threshold=cfg.get("merge_threshold", 8),
                fanout=cfg["fanout"],
                profile_label=profile_label,
                bad_leaf_count=len(manifest["bad_leaves"]),
                corrupted_tuple_count=len(manifest["corruptions"]),
                repetition=rep,
                enabled=True,
                deep=(profiling_mode == "deep"),
            )
        metric = repair_merkle(
            conn,
            manifest,
            tuple_count,
            rep,
            planner_results,
            schema_rows_out,
            profile_label=profile_label,
            profiling_mode=profiling_mode,
            profiler=profiler,
            benchmark_profile=benchmark_profile,
            audit_mode=audit_mode,
            leaf_fetch_chunk_size=leaf_fetch_chunk_size,
        )
        metrics.append(metric)
        if profiler is not None:
            profile_operation_rows_out.extend(profiler.rows())
            if profiler.backend_profile is not None:
                backend_profile_rows_out.append(
                    {
                        "run_id": run_id,
                        "manifest_sha256": manifest_digest,
                        "experiment": manifest["experiment"],
                        "profile_label": profile_label,
                        "profiling_mode": profiling_mode,
                        "tuple_count": tuple_count,
                        "partitions": cfg["partitions"],
                        "leaves_per_partition": cfg["leaves_per_partition"],
                        "fanout": cfg["fanout"],
                        "bad_leaf_count": len(manifest["bad_leaves"]),
                        "corrupted_tuple_count": len(manifest["corruptions"]),
                        "repetition": rep,
                        **profiler.backend_profile,
                    }
                )
            if profiling_mode == "deep" and rep == 0:
                deep_rows = _run_deep_diagnostics(
                    conn,
                    manifest,
                    result_dir,
                    run_id=run_id,
                    tuple_count=tuple_count,
                    partitions=cfg["partitions"],
                    leaves_per_partition=cfg["leaves_per_partition"],
                    fanout=cfg["fanout"],
                    profile_label=profile_label,
                    repetition=rep,
                )
                deep_plan_summary_rows_out.extend(deep_rows)
        progress_state["completed_runs"] += 1
        emit_progress(
            result_dir,
            event="method_complete",
            run_id=metric.run_id,
            manifest_sha256=manifest_digest,
            experiment=metric.experiment,
            profile_label=profile_label,
            profiling_mode=profiling_mode,
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
    if isinstance(selected, str):
        selected_list = [int(x.strip()) for x in selected.split(",") if x.strip()]
    elif isinstance(selected, list):
        selected_list = [int(x) for x in selected]
    else:
        selected_list = [int(selected)]
    matched = [v for v in out if v in selected_list]
    return matched if matched else selected_list


def _series_for_profile(args: argparse.Namespace, config) -> list[dict[str, Any]]:
    def case(
        *,
        experiment: str,
        tuple_count: int,
        partitions: int,
        leaves_per_partition: int,
        fanout: int,
        bad_leaf_count: int,
        corrupted_tuple_count: int,
        geometry_label: str,
    ) -> dict[str, Any]:
        return {
            "experiment": experiment,
            "tuple_count": tuple_count,
            "partitions": partitions,
            "leaves_per_partition": leaves_per_partition,
            "fanout": fanout,
            "bad_leaf_count": bad_leaf_count,
            "corrupted_tuple_count": corrupted_tuple_count,
            "geometry_label": geometry_label,
        }

    override_lpp = args.leaves_per_partition
    override_fanout = args.fanout
    manual_geometry = (
        args.partitions is not None
        or args.leaves_per_partition is not None
        or args.fanout is not None
    )
    geometry_matrix = load_geometry_matrix()
    if args.profile == "recovery-scaling-diagnosis":
        if manual_geometry and not args.geometry_label:
            raise ValueError("manual geometry overrides require --geometry-label")
        series: list[dict[str, Any]] = []
        campaigns = [
            ("baseline_l16", [1_000_000, 3_000_000, 5_000_000]),
            ("preprovisioned_l128", [1_000_000, 3_000_000, 5_000_000]),
            ("sensitivity_l32", [5_000_000]),
            ("sensitivity_l64", [5_000_000]),
        ]
        for label, sizes in campaigns:
            if args.geometry_label and label != args.geometry_label:
                continue
            geo = geometry_matrix.get(label)
            if not geo:
                raise RuntimeError(f"canonical geometry label '{label}' missing from recovery_geometry_matrix.json")
            if override_lpp is not None:
                geo["leaves_per_partition"] = override_lpp
            if override_fanout is not None:
                geo["fanout"] = override_fanout
            partitions = int(args.partitions or geo.get("partitions", 200))
            leaves_per_partition = int(geo.get("leaves_per_partition", 16))
            fanout = int(geo.get("fanout", 2))
            out_label = (
                _manual_geometry_label(partitions, leaves_per_partition, fanout)
                if manual_geometry else label
            )
            for n in _selected(sizes, args.tuple_count):
                series.append(
                    case(
                        experiment=args.profile,
                        tuple_count=n,
                        partitions=partitions,
                        leaves_per_partition=leaves_per_partition,
                        fanout=fanout,
                        bad_leaf_count=int(args.bad_leaf_count or 10),
                        corrupted_tuple_count=300,
                        geometry_label=out_label,
                    )
                )
        return series

    if args.profile == "fanout-width-sweep":
        # fanout-width-sweep: fixed workload, no overrides permitted.
        # All parameters are encoded in the geometry matrix labels.
        # The only allowed user selection is --geometry-label <known label>.
        _SWEEP_FIXED_PARAMS = (
            "tuple_count", "partitions", "leaves_per_partition", "fanout", "bad_leaf_count",
        )
        _rejected = [
            p for p in _SWEEP_FIXED_PARAMS
            if getattr(args, p, None) is not None
        ]
        if _rejected:
            raise ValueError(
                "fanout-width-sweep uses fixed geometry labels; "
                f"do not override: {', '.join('--' + p.replace('_', '-') for p in _rejected)}"
            )

        # Canonical 19-geometry list — 6 leaf-count tiers (L=16,64,128,256,512,1024).
        # Within each tier L is fixed (bucket density fixed); only F varies.
        # Fanouts covered: 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024.
        #
        # Tier  L    rows/leaf@5M  F values
        # ----  ---  -----------   --------
        # L=16  16   1,563         2, 4, 16
        # L=64  64     391         2, 4, 8, 64
        # L=128 128    195         2, 128
        # L=256 256     98         2, 4, 16, 256
        # L=512 512     49         2, 512
        # L=1024 1024   24         2, 4, 32, 1024
        SWEEP_CANONICAL_LABELS = [
            # L=16
            "fanout_f2_l16",
            "fanout_f4_l16",
            "fanout_f16_l16",
            # L=64
            "fanout_f2_l64",
            "fanout_f4_l64",
            "fanout_f8_l64",
            "fanout_f64_l64",
            # L=128
            "fanout_f2_l128",
            "fanout_f128_l128",
            # L=256
            "fanout_f2_l256",
            "fanout_f4_l256",
            "fanout_f16_l256",
            "fanout_f256_l256",
            # L=512
            "fanout_f2_l512",
            "fanout_f512_l512",
            # L=1024
            "fanout_f2_l1024",
            "fanout_f4_l1024",
            "fanout_f32_l1024",
            "fanout_f1024_l1024",
        ]

        # Validate --geometry-label before touching the database
        if args.geometry_label and args.geometry_label not in SWEEP_CANONICAL_LABELS:
            raise ValueError(
                f"unknown fanout-width-sweep geometry label '{args.geometry_label}'; "
                f"valid labels: {', '.join(SWEEP_CANONICAL_LABELS)}"
            )

        sweep_series: list[dict[str, Any]] = []
        for label in SWEEP_CANONICAL_LABELS:
            if args.geometry_label and label != args.geometry_label:
                continue
            geo = geometry_matrix.get(label)
            if geo is None:
                raise RuntimeError(
                    f"geometry label '{label}' not found in recovery_geometry_matrix.json"
                )
            partitions = int(geo["partitions"])
            lpp = int(geo["leaves_per_partition"])
            fanout = int(geo["fanout"])
            sweep_series.append(
                case(
                    experiment=args.profile,
                    tuple_count=5_000_000,
                    partitions=partitions,
                    leaves_per_partition=lpp,
                    fanout=fanout,
                    bad_leaf_count=20,
                    corrupted_tuple_count=300,
                    geometry_label=label,
                )
            )
        return sweep_series

    if args.profile == "size-scaling-k75-c300":
        if args.tuple_count is not None:
            raise ValueError("size-scaling-k75-c300 owns tuple counts 1M,3M,5M; do not pass --tuple-count")
        if args.partitions is not None or args.leaves_per_partition is not None or args.fanout is not None:
            raise ValueError("size-scaling-k75-c300 uses fixed canonical geometries; do not override geometry")
        if args.bad_leaf_count is not None:
            raise ValueError("size-scaling-k75-c300 uses fixed --bad-leaf-count=75")

        labels = [
            "fanout_f2_l16",
            "fanout_f2_l128",
            "fanout_f32_l1024",
        ]

        if args.geometry_label and args.geometry_label not in labels:
            raise ValueError(
                f"unknown size-scaling-k75-c300 geometry label '{args.geometry_label}'; "
                f"valid labels: {', '.join(labels)}"
            )

        series = []
        for label in labels:
            if args.geometry_label and label != args.geometry_label:
                continue
            geo = geometry_matrix.get(label)
            if geo is None:
                raise RuntimeError(f"geometry label '{label}' missing from recovery_geometry_matrix.json")

            for n in [1_000_000, 3_000_000, 5_000_000]:
                series.append(
                    case(
                        experiment=args.profile,
                        tuple_count=n,
                        partitions=int(geo["partitions"]),
                        leaves_per_partition=int(geo["leaves_per_partition"]),
                        fanout=int(geo["fanout"]),
                        bad_leaf_count=75,
                        corrupted_tuple_count=300,
                        geometry_label=label,
                    )
                )
        return series

    if args.profile == "best-scaling-f32-l1024-k75-c300":
        if (args.partitions is not None and args.partitions != 200) or \
                (args.leaves_per_partition is not None and args.leaves_per_partition != 1024) or \
                (args.fanout is not None and args.fanout != 32):
            raise ValueError(
                "best-scaling-f32-l1024-k75-c300 uses fixed geometry "
                "P=200,F=32,L=1024; supplied geometry must match exactly"
            )
        if args.bad_leaf_count is not None and args.bad_leaf_count != 75:
            raise ValueError(
                "best-scaling-f32-l1024-k75-c300 uses fixed --bad-leaf-count=75"
            )
        if args.geometry_label and args.geometry_label != "fanout_f32_l1024":
            raise ValueError(
                "best-scaling-f32-l1024-k75-c300 only supports geometry_label=fanout_f32_l1024"
            )

        label = "fanout_f32_l1024"
        geo = geometry_matrix.get(label)
        if geo is None:
            raise RuntimeError(
                "geometry label 'fanout_f32_l1024' missing from recovery_geometry_matrix.json"
            )

        sizes = [int(n) for n in config.fig12_sizes]

        # Allows cheap smoke like --tuple-count 7000000 or --tuple-count 20000000.
        selected_sizes = _selected(sizes, args.tuple_count)

        series = []
        for n in selected_sizes:
            series.append(
                case(
                    experiment=args.profile,
                    tuple_count=n,
                    partitions=int(geo["partitions"]),
                    leaves_per_partition=int(geo["leaves_per_partition"]),
                    fanout=int(geo["fanout"]),
                    bad_leaf_count=75,
                    corrupted_tuple_count=300,
                    geometry_label=label,
                )
            )
        return series



    series = []
    if args.experiment in (None, "figure12"):
        for n in _selected(config.fig12_sizes, args.tuple_count):
            partitions = int(args.partitions or 200)
            leaves_per_partition = int(override_lpp or 16)
            fanout = int(override_fanout or 2)
            series.append(
                case(
                    experiment="figure12",
                    tuple_count=n,
                    partitions=partitions,
                    leaves_per_partition=leaves_per_partition,
                    fanout=fanout,
                    bad_leaf_count=(args.bad_leaf_count if args.bad_leaf_count is not None else (10 if n >= 10 else 1)),
                    corrupted_tuple_count=300,
                    geometry_label=(
                        _manual_geometry_label(partitions, leaves_per_partition, fanout)
                        if manual_geometry else f"l{leaves_per_partition}"
                    ),
                )
            )
    if args.experiment in (None, "figure13"):
        partitions_values = _selected([100, 200], args.partitions)
        sizes = _selected(config.fig13_sizes, args.tuple_count)
        bad_counts = _selected(config.fig13_k, args.bad_leaf_count)
        for partitions in partitions_values:
            for n in sizes:
                for k in bad_counts:
                    leaves_per_partition = int(override_lpp or 16)
                    fanout = int(override_fanout or 2)
                    series.append(
                        case(
                            experiment="figure13",
                            tuple_count=n,
                            partitions=partitions,
                            leaves_per_partition=leaves_per_partition,
                            fanout=fanout,
                            bad_leaf_count=k,
                            corrupted_tuple_count=300,
                            geometry_label=(
                                _manual_geometry_label(partitions, leaves_per_partition, fanout)
                                if manual_geometry else f"p{partitions}-l{leaves_per_partition}"
                            ),
                        )
                    )
    return series


def _count_planned_runs(config, args) -> int:
    return len(_series_for_profile(args, config)) * int(config.repetitions)


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
        profile_label=args.profile_label or "",
        profiling_mode=args.profiling,
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
    profile_operation_rows: list[dict[str, Any]] = []
    backend_profile_rows: list[dict[str, Any]] = []
    deep_plan_summary_rows: list[dict[str, Any]] = []
    io_timing_setting = ""
    server_version = ""
    git_head = ""

    with connect(args) as conn:
        ensure_helpers(conn)
        if args.profiling in ("light", "deep"):
            try:
                execute(conn, "SHOW merkle_recovery_profile_enabled")
                execute(conn, "SELECT merkle_recovery_profile_reset()")
                execute(conn, "SELECT merkle_recovery_profile_stats()")
            except Exception as exc:
                raise RuntimeError(
                    "Profiling backend is not installed in this PGDATA. "
                    "Use the benchmark bootstrap command to create a fresh profiling cluster."
                ) from exc
        io_timing_setting = str(show_setting(conn, "track_io_timing"))
        server_version = str(scalar(conn, "SHOW server_version"))
        try:
            import subprocess
            git_bin = shutil.which("git") or "/usr/bin/git"
            if Path(git_bin).exists():
                git_head = subprocess.check_output(
                    [git_bin, "rev-parse", "HEAD"], cwd=BENCH_DIR.parents[2],
                    text=True, stderr=subprocess.DEVNULL
                ).strip()
            else:
                git_head = "unavailable; source_snapshot.json records synced source provenance"
        except Exception as exc:
            git_head = f"unavailable: {exc}; source_snapshot.json records synced source provenance"
        for spec in _series_for_profile(args, config):
            n = int(spec["tuple_count"])
            partitions = int(spec["partitions"])
            leaves_per_partition = int(spec["leaves_per_partition"])
            fanout = int(spec["fanout"])
            bad_leaf_count = int(spec["bad_leaf_count"])
            corrupted_tuple_count = int(spec["corrupted_tuple_count"])
            geometry_label = str(spec["geometry_label"])
            experiment = str(spec["experiment"])
            validate_geometry(partitions, leaves_per_partition, fanout)

            emit_progress(
                result_dir,
                event="dataset_start",
                experiment=experiment,
                profile_label=geometry_label,
                tuple_count=n,
                partitions=partitions,
                bad_leaf_count=bad_leaf_count,
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
            )
            build_dataset(conn, n, partitions, leaves_per_partition, fanout)
            bsum, bdebug = bucket_consistency_sample(conn, n, partitions, leaves_per_partition, fanout, args.seed)
            bucket_summary_rows.append(bsum)
            if args.artifact_mode == "debug":
                bucket_debug_rows.extend(bdebug)
            dataset_rows.append(_dataset_row(conn, n, {
                "partitions": partitions,
                "leaves_per_partition": leaves_per_partition,
                "fanout": fanout,
            }, geometry_label))
            emit_progress(
                result_dir,
                event="dataset_complete",
                experiment=experiment,
                profile_label=geometry_label,
                tuple_count=n,
                partitions=partitions,
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
            )
            d = corrupted_tuple_count if args.profile == "recovery-scaling-diagnosis" else (
                300 if args.profile in ("paper", "preflight", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") else bad_leaf_count
            )

            if args.profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                forced_key = geometry_label
                forced_map = progress_state.setdefault("forced_bad_leaves_by_geometry", {})
                forced_bad_leaves = forced_map.get(forced_key)
            else:
                forced_bad_leaves = None

            manifest = choose_corruption_manifest(
                conn,
                experiment,
                n,
                partitions,
                leaves_per_partition,
                fanout,
                bad_leaf_count,
                d,
                args.seed,
                corruption_mode=args.corruption_mode,
                forced_bad_leaves=forced_bad_leaves,
            )
            d = manifest["corrupted_tuple_count"]
            validate_manifest_leaf_mapping(conn, manifest)

            if args.profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                if forced_key not in forced_map:
                    forced_map[forced_key] = list(manifest["bad_leaves"])
                elif list(manifest["bad_leaves"]) != list(forced_map[forced_key]):
                    raise RuntimeError(
                        f"bad leaf mismatch for {geometry_label}: "
                        f"expected={forced_map[forced_key]}, actual={manifest['bad_leaves']}"
                    )

            # Capacity validation & Provenance validation
            if args.profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                selected_capacity = manifest["selected_bad_leaf_row_capacity"]
                if selected_capacity < d:
                    raise RuntimeError(
                        f"capacity check failed for {geometry_label}: "
                        f"selected {bad_leaf_count} bad leaves contain only {selected_capacity} rows "
                        f"but C={d} corruptions are required. "
                        f"Reduce C or increase L."
                    )
                emit_progress(
                    result_dir,
                    event="capacity_check_passed",
                    profile_label=geometry_label,
                    selected_bad_leaf_count=bad_leaf_count,
                    selected_bad_leaf_row_capacity=selected_capacity,
                    required_corrupted_tuple_count=d,
                    corruption_selection_sha256=manifest["corruption_selection_sha256"],
                )

                # Assert provenance
                if args.profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                    tier_key = f"tier_{geometry_label}"
                    expected_sha = progress_state.setdefault("provenance_map", {}).get(tier_key)
                    actual_sha = manifest["bad_leaf_selection_sha256"]
                    if expected_sha is None:
                        progress_state["provenance_map"][tier_key] = actual_sha
                    elif expected_sha != actual_sha:
                        raise RuntimeError(
                            f"provenance mismatch in tier {tier_key}: "
                            f"expected {expected_sha} but got {actual_sha} for {geometry_label}"
                        )
                else:
                    tier_key = f"tier_l{leaves_per_partition}"
                    expected_sha = progress_state.setdefault("provenance_map", {}).get(tier_key)
                    actual_sha = manifest["corruption_selection_sha256"]
                    if expected_sha is None:
                        progress_state["provenance_map"][tier_key] = actual_sha
                    elif expected_sha != actual_sha:
                        raise RuntimeError(
                            f"provenance mismatch in tier {tier_key}: "
                            f"expected {expected_sha} but got {actual_sha} for {geometry_label}"
                        )

                if "provenance_rows" not in progress_state:
                    progress_state["provenance_rows"] = []
                progress_state["provenance_rows"].append({
                    "profile_label": geometry_label,
                    "tuple_count": n,
                    "partitions": partitions,
                    "leaves_per_partition": leaves_per_partition,
                    "fanout": fanout,
                    "bad_leaf_count": bad_leaf_count,
                    "corrupted_tuple_count": d,
                    "required_rows_per_bad_leaf": manifest["required_rows_per_bad_leaf"],
                    "selected_bad_leaf_row_capacity": selected_capacity,
                    "selected_leaf_capacities_json": json.dumps(manifest["selected_leaf_capacities"]),
                    "corruption_selection_sha256": manifest["corruption_selection_sha256"],
                    "bad_leaf_selection_sha256": manifest["bad_leaf_selection_sha256"],
                })

            manifests.append(manifest)

            all_metrics.extend(
                run_one_manifest(
                    conn,
                    manifest,
                    config.repetitions,
                    planner_rows,
                    schema_fidelity_rows,
                    profile_operation_rows,
                    backend_profile_rows,
                    deep_plan_summary_rows,
                    result_dir,
                    progress_state,
                    profile_label=geometry_label,
                    profiling_mode=args.profiling,
                    benchmark_profile=args.profile,
                    audit_mode=args.audit_mode,
                    leaf_fetch_chunk_size=args.leaf_fetch_batch_size,
                )
            )

    # ── write artifacts ───────────────────────────────────────────────────────
    (result_dir / "corruption_manifest.json").write_text(json.dumps(manifests, indent=2) + "\n")

    if args.profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
        write_csv(
            result_dir / "fanout_provenance.csv",
            progress_state.get("provenance_rows", []),
            [
                "profile_label",
                "tuple_count",
                "partitions",
                "leaves_per_partition",
                "fanout",
                "bad_leaf_count",
                "corrupted_tuple_count",
                "required_rows_per_bad_leaf",
                "selected_bad_leaf_row_capacity",
                "selected_leaf_capacities_json",
                "corruption_selection_sha256",
                "bad_leaf_selection_sha256",
            ],
        )

    write_csv(result_dir / "dataset_sizes.csv", dataset_rows, [
        "profile_label", "tuple_count", "partitions", "leaves_per_partition", "fanout",
        "total_leaf_count",
        "tree_levels", "tree_edges", "tree_depth",
        "nodes_per_partition", "total_merkle_nodes", "total_logical_tree_nodes",
        "physical_rows_per_leaf_expected", "expected_candidate_rows_per_bad_leaf",
        "base_table_bytes", "primary_index_bytes", "merkle_index_bytes",
        "leaf_lookup_index_bytes",
        "total_schema_bytes",
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
    write_csv(result_dir / "phase_timings.csv", phase_rows, ["run_id", "manifest_sha256", "method", "phase", "ms"])
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
                "full_audit_skipped": m.counters.get("full_audit_skipped", 0),
                "audit_validation_skipped": m.counters.get("audit_validation_skipped", 0),
            }
            for m in all_metrics
        ],
        [
            "run_id", "method", "paper_style_total_ms", "restore_repair_ms",
            "audit_validation_ms", "end_to_end_observed_ms", "cleanup_ms",
            "paper_end_before_audit_start", "audit_validation_positive",
            "end_to_end_covers_paper_and_audit", "full_audit_skipped",
            "audit_validation_skipped",
        ],
    )
    write_csv(
        result_dir / "verification_results.csv",
        [{"all_runs_valid": int(all(m.valid for m in all_metrics))}],
        ["all_runs_valid"],
    )

    if profile_operation_rows:
        write_csv(
            result_dir / "profile_operations.csv",
            profile_operation_rows,
            PROFILE_OPERATION_FIELDS,
        )
        summary_rows = group_profile_rows_with_fraction(
            profile_operation_rows,
            sum(m.restore_repair_ms for m in all_metrics),
        )
        for row in summary_rows:
            row["fraction_of_campaign_restore_repair_ms"] = row.pop("fraction_restore_repair_ms")
        write_csv(
            result_dir / "profile_summary.csv",
            summary_rows,
            [
                "stage", "operation", "call_count", "row_count",
                "total_ms", "median_ms", "p95_ms", "fraction_of_campaign_restore_repair_ms",
            ],
        )
        per_run_keys = (
            "run_id", "manifest_sha256", "experiment", "tuple_count", "partitions",
            "leaves_per_partition", "fanout", "profile_label", "bad_leaf_count",
            "corrupted_tuple_count", "repetition",
        )
        per_run_denominators = {
            (
                m.run_id,
                m.counters.get("manifest_sha256", ""),
                m.experiment,
                m.tuple_count,
                m.partitions,
                m.leaves_per_partition,
                m.fanout,
                m.profile_label,
                m.bad_leaf_count,
                m.corrupted_tuple_count,
                m.repetition,
            ): m.restore_repair_ms
            for m in all_metrics
        }
        per_run_rows = group_profile_rows_with_denominators(
            profile_operation_rows,
            group_keys=per_run_keys,
            denominators=per_run_denominators,
        )
        write_csv(
            result_dir / "profile_summary_per_run.csv",
            per_run_rows,
            [
                *per_run_keys, "stage", "operation", "call_count", "row_count",
                "total_ms", "median_ms", "p95_ms", "fraction_restore_repair_ms",
            ],
        )
        by_geometry_keys = (
            "manifest_sha256", "experiment", "tuple_count", "partitions",
            "leaves_per_partition", "fanout", "profile_label", "bad_leaf_count",
            "corrupted_tuple_count",
        )
        by_geometry_denominators: dict[tuple[Any, ...], float] = {}
        for m in all_metrics:
            key = (
                m.counters.get("manifest_sha256", ""),
                m.experiment,
                m.tuple_count,
                m.partitions,
                m.leaves_per_partition,
                m.fanout,
                m.profile_label,
                m.bad_leaf_count,
                m.corrupted_tuple_count,
            )
            by_geometry_denominators[key] = by_geometry_denominators.get(key, 0.0) + m.restore_repair_ms
        by_geometry_rows = group_profile_rows_with_denominators(
            profile_operation_rows,
            denominators=by_geometry_denominators,
            group_keys=by_geometry_keys,
        )
        write_csv(
            result_dir / "profile_summary_by_geometry.csv",
            by_geometry_rows,
            [
                *by_geometry_keys, "stage", "operation", "call_count", "row_count",
                "total_ms", "median_ms", "p95_ms", "fraction_restore_repair_ms",
            ],
        )
    if backend_profile_rows:
        backend_fields = [
            "run_id", "experiment", "profile_label", "profiling_mode",
            "manifest_sha256", "tuple_count", "partitions", "leaves_per_partition", "fanout",
            "bad_leaf_count", "corrupted_tuple_count", "repetition",
        ]
        extra_fields = sorted({
            key for row in backend_profile_rows for key in row.keys()
            if key not in backend_fields
        })
        write_csv(
            result_dir / "merkle_backend_profile.csv",
            backend_profile_rows,
            backend_fields + extra_fields,
        )

    if deep_plan_summary_rows:
        write_csv(
            result_dir / "deep_plan_summary.csv",
            deep_plan_summary_rows,
            [
                "leaf_id",
                "run_id",
                "manifest_sha256",
                "diagnostic_replay_id",
                "diagnostic_cache_mode",
                "tuple_count",
                "partitions",
                "leaves_per_partition",
                "fanout",
                "profile_label",
                "repetition",
                "schema",
                "kind",
                "plan_order",
                "planning_ms",
                "execution_ms",
                "actual_rows",
                "shared_hit_blocks",
                "shared_read_blocks",
                "shared_dirtied_blocks",
                "shared_written_blocks",
                "local_hit_blocks",
                "local_read_blocks",
                "temp_read_blocks",
                "temp_written_blocks",
                "wal_records",
                "wal_bytes",
                "plan_uses_expected_leaf_lookup_index",
                "io_timing_available",
            ],
        )

    phase_names = [
        "tree_localisation_ms",
        "candidate_row_fetch_ms",
        "row_comparison_ms",
        "repair_write_ms",
        "targeted_post_repair_confirmation_ms",
        "recovery_observability_ms",
        "recovery_orchestration_ms",
        "restore_repair_ms",
    ]

    def metric_phase_value(metric: Metrics, phase_name: str) -> float:
        if phase_name == "restore_repair_ms":
            return metric.restore_repair_ms
        return float(metric.phase.get(phase_name, 0.0))

    def percentile95(values: list[float]) -> float:
        ordered = sorted(values)
        if not ordered:
            return 0.0
        return ordered[min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)]

    grouped_metrics: dict[tuple[Any, ...], list[Metrics]] = {}
    for metric in all_metrics:
        key = (
            metric.profile_label,
            metric.tuple_count,
            metric.partitions,
            metric.leaves_per_partition,
            metric.fanout,
            metric.bad_leaf_count,
            metric.corrupted_tuple_count,
        )
        grouped_metrics.setdefault(key, []).append(metric)

    phase_medians: dict[tuple[Any, ...], dict[str, float]] = {}
    report_lines = [
        "# Recovery Profiling Report",
        "",
        f"- recovery measurement boundary: `restore_repair_ms`",
        f"- audit measurement boundary: `audit_validation_ms`",
        "- cache_mode: `uncontrolled normal benchmark state`",
        f"- track_io_timing: `{io_timing_setting}`",
        f"- PostgreSQL version: `{server_version}`",
        f"- git commit: `{git_head}`",
        f"- I/O timing available: `{int(io_timing_setting.lower() == 'on')}`",
    ]
    if dataset_rows:
        report_lines.append("- exact geometry:")
        seen_geometry: set[tuple[Any, Any, Any, Any]] = set()
        for row in dataset_rows:
            key = (row["profile_label"], row["partitions"], row["leaves_per_partition"], row["fanout"])
            if key in seen_geometry:
                continue
            seen_geometry.add(key)
            report_lines.append(
                f"  - {row['profile_label']}: partitions={row['partitions']}, "
                f"leaves_per_partition={row['leaves_per_partition']}, fanout={row['fanout']}, "
                f"total_leaf_count={row['total_leaf_count']}, tree_depth={row['tree_depth']}"
            )
    report_lines.extend([
        "",
        "## Phase Medians And P95",
        "",
        "| profile_label | tuple_count | partitions | leaves_per_partition | fanout | bad_leaf_count | corrupted_tuple_count | phase | median_ms | p95_ms |",
        "|---|---:|---:|---:|---:|---:|---:|---|---:|---:|",
    ])
    for key, items in sorted(grouped_metrics.items()):
        phase_medians[key] = {}
        for phase_name in phase_names:
            vals = [metric_phase_value(m, phase_name) for m in items]
            median_ms = median(vals) if vals else 0.0
            phase_medians[key][phase_name] = median_ms
            report_lines.append(
                f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {key[5]} | {key[6]} | "
                f"{phase_name} | {median_ms:.3f} | {percentile95(vals):.3f} |"
            )

    report_lines.extend(["", "## Growth Ratios", ""])
    growth_rows: list[tuple[str, str, float]] = []
    growth_labels = sorted({str(m.profile_label) for m in all_metrics})
    for label in growth_labels:
        label_keys = [key for key in phase_medians if key[0] == label]
        low_keys = [key for key in label_keys if int(key[1]) == 1_000_000]
        high_keys = [key for key in label_keys if int(key[1]) == 5_000_000]
        if not low_keys or not high_keys:
            report_lines.append(f"- {label}: insufficient 1M/5M data for growth ratios")
            continue
        low_key = sorted(low_keys)[0]
        high_key = sorted(high_keys)[0]
        report_lines.append(f"- {label}: 5M / 1M median ratios")
        for phase_name in [
            "candidate_row_fetch_ms",
            "targeted_post_repair_confirmation_ms",
            "repair_write_ms",
            "tree_localisation_ms",
        ]:
            low = phase_medians[low_key].get(phase_name, 0.0)
            high = phase_medians[high_key].get(phase_name, 0.0)
            ratio = high / low if low > 0 else 0.0
            growth_rows.append((label, phase_name, ratio))
            report_lines.append(f"  - {phase_name}: {ratio:.3f}")
    if growth_rows:
        label, phase_name, ratio = max(growth_rows, key=lambda item: item[2])
        report_lines.append("")
        report_lines.append(
            f"- highest-growing phase: `{phase_name}` for `{label}` at ratio `{ratio:.3f}`"
        )
    (result_dir / "profiling_report.md").write_text("\n".join(report_lines) + "\n")

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
    parser.add_argument("--profile", choices=["smoke", "preflight", "paper", "recovery-scaling-diagnosis", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"], default="smoke")

    parser.add_argument("--experiment", choices=["figure12", "figure13"])
    parser.add_argument("--tuple-count", type=str, dest="tuple_count")
    parser.add_argument("--partitions", type=int)
    parser.add_argument("--bad-leaf-count", type=int, dest="bad_leaf_count")
    parser.add_argument("--leaves-per-partition", type=int, dest="leaves_per_partition")
    parser.add_argument("--fanout", type=int)
    parser.add_argument("--profile-label", dest="profile_label")
    parser.add_argument("--geometry-label", dest="geometry_label")
    parser.add_argument("--profiling", choices=["off", "light", "deep"], default="off")
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
    parser.add_argument(
        "--leaf-fetch-batch-size",
        type=int,
        default=64,
        dest="leaf_fetch_batch_size",
        help=(
            "Maximum number of leaf IDs per SQL statement during candidate fetch. "
            "Set to a positive value (default 64) to bound peak memory when K is large. "
            "Set to 0 to disable bounding and send all IDs in a single statement."
        ),
    )
    parser.add_argument(
        "--audit-mode",
        choices=["full", "skip"],
        default="full",
        dest="audit_mode",
        help="Validation mode after repair. full runs expensive full-table audit; skip keeps sparse targeted confirmation only.",
    )
    args = parser.parse_args(argv)

    if args.profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") and args.corruption_mode != "paper-update-only":
        raise ValueError(f"{args.profile} requires --corruption-mode paper-update-only")

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
