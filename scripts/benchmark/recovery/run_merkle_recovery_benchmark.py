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
    profile_config, leaf_key,
)
from merkle_recovery.db import (
    connect,
    diff_merkle_node_index_stats,
    execute,
    merkle_node_index_stats,
    scalar,
    show_setting,
)
from merkle_recovery.dataset import (
    build_dataset, reset_damaged_from_healthy,
    expand_dataset,
    leaf_occupancy, occupancy_stats, table_sizes, tree_stats,
    bucket_consistency_sample, ensure_helpers,
)
from merkle_recovery.manifest import (
    choose_corruption_manifest, validate_manifest_leaf_mapping, apply_corruption,
)
from merkle_recovery.localisation import detect_bad_leaves
from merkle_recovery.repair import (
    fetch_leaf_rows, fetch_leaf_rows_batch, run_planner_preflight, repair_leaf,
    execute_batched_deletes, execute_batched_inserts, execute_batched_updates,
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
    parse_json_plan, record_call,
)

RESULT_ROOT = _DEFAULT_RESULT_ROOT

PROFILE_OPERATION_FIELDS = [
    "run_id", "manifest_sha256", "experiment", "tuple_count", "split_threshold",
    "merge_threshold", "fanout", "profile_label", "bad_leaf_count",
    "corrupted_tuple_count", "repetition", "stage", "operation",
    "schema", "partition", "node_in_partition", "leaf_id",
    "localisation_prefix_len", "localisation_frontier_nodes",
    "localisation_batch_depth", "localisation_max_depth",
    "call_ordinal", "rows_returned", "client_wall_ms", "success",
]

# Recovery writes are synchronous and touch the same small damaged-leaf set on
# every repetition. A single warmup is not enough to settle the first WAL/
# commit path after a large index build, so keep several untimed cycles outside
# the reported sample. The normal apply_corruption() checkpoint remains the
# clean boundary immediately before each measured repetition.
RECOVERY_WARMUP_CYCLES = 3


# ── timing helper ─────────────────────────────────────────────────────────────

def now_ms() -> float:
    return time.perf_counter() * 1000.0


def assert_synchronous_merkle_state(conn) -> None:
    """Fail fast if the crash-safe Merkle apply engine is not ready."""
    raw = scalar(conn, "SELECT merkle_recovery_status()")
    if raw is None:
        return
    status = json.loads(raw)
    if status.get("state") not in (None, "READY"):
        raise RuntimeError(f"Merkle recovery state is not READY: {status}")


def chunk_list(values: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        return [values]
    return [values[i:i + size] for i in range(0, len(values), size)]


@contextmanager
def timer(store: dict[str, float], name: str):
    start = now_ms()
    yield
    store[name] = store.get(name, 0.0) + now_ms() - start


def load_geometry_matrix() -> dict[str, dict[str, int]]:
    if not GEOMETRY_MATRIX_PATH.exists():
        return {}
    data = json.loads(GEOMETRY_MATRIX_PATH.read_text())
    return {k: {kk: int(vv) for kk, vv in v.items()} for k, v in data.items()}


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, default=str).encode()).hexdigest()


def recovery_run_id(manifest: dict[str, Any], repetition: int, profile_label: str) -> str:
    label = profile_label or str(manifest.get("geometry_label", ""))
    safe_label = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label)
    return (
        f"{manifest['experiment']}-n{manifest['tuple_count']}"
        f"-f{manifest['fanout']}"
        f"-k{len(manifest['bad_leaves'])}"
        f"-c{len(manifest['corruptions'])}"
        f"-{safe_label}-merkle-r{repetition}"
    )


def validate_geometry(fanout: int = 4, split_threshold: int = 32, merge_threshold: int = 8, *args, **kwargs) -> None:
    if fanout < 2:
        raise ValueError(f"invalid Merkle geometry: fanout must be >= 2, got {fanout}")
    if split_threshold <= merge_threshold:
        raise ValueError(
            f"invalid Merkle geometry: split_threshold ({split_threshold}) must be > merge_threshold ({merge_threshold})"
        )


def _manual_geometry_label(fanout: int = 4, split_threshold: int = 32, merge_threshold: int = 8, *args, **kwargs) -> str:
    return f"manual-f{fanout}-s{split_threshold}-m{merge_threshold}"


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
    """Validate counters emitted by the current native dynamic path.

    The old static implementation exposed root-helper and tree-path counters.
    Dynamic recovery reports the batched child lookups here.  Root reads are
    ordinary catalog SQL, while full-audit row hashing is intentionally outside
    this recovery-only profile snapshot.
    """
    reasons: list[str] = []
    partition_catalog_path = bool(
        counters.get("partition_subtree_sql_calls", 0)
        or post_repair_counters.get("targeted_confirmation_partition_subtree_sql_calls", 0)
    )
    expected_child_calls = 0 if partition_catalog_path else int(counters.get("child_hash_sql_calls", 0)) + int(
        post_repair_counters.get("targeted_confirmation_child_hash_sql_calls", 0)
    )
    expected_child_nodes = 0 if partition_catalog_path else int(counters.get("child_hash_nodes_read", 0)) + int(
        post_repair_counters.get("targeted_confirmation_child_hash_nodes_read", 0)
    )
    checks = [
        ("child_hash_helper_calls", expected_child_calls),
        ("child_hash_nodes_returned", expected_child_nodes),
    ]
    for field, expected in checks:
        actual = int(backend.get(field, 0))
        if actual != expected:
            reasons.append(f"{field} expected {expected}, got {actual}")

    return reasons


def _dataset_row(conn, tuple_count: int, geo: dict[str, int], profile_label: str = "") -> dict[str, Any]:
    import time
    t_occ0 = time.perf_counter()
    occ = leaf_occupancy(conn)
    leaf_occupancy_ms = (time.perf_counter() - t_occ0) * 1000.0

    t_sz0 = time.perf_counter()
    sizes = table_sizes(conn)
    table_sizes_ms = (time.perf_counter() - t_sz0) * 1000.0

    t_ts0 = time.perf_counter()
    tstats = tree_stats(conn, int(geo.get("fanout", 4)))
    tree_stats_ms = (time.perf_counter() - t_ts0) * 1000.0

    leaf_count = len(occ)
    phys_per_leaf = tuple_count / max(1, leaf_count)
    row = {
        **sizes,
        **tstats,
        "profile_label": profile_label,
        "tuple_count": tuple_count,
        "fanout": geo.get("fanout", 4),
        "split_threshold": geo.get("split_threshold", 32),
        "merge_threshold": geo.get("merge_threshold", 8),
        "total_leaf_count": leaf_count,
        "physical_rows_per_leaf_expected": round(phys_per_leaf, 2),
        "expected_candidate_rows_per_bad_leaf": round(2 * phys_per_leaf, 2),
        "leaf_occupancy_ms": round(leaf_occupancy_ms, 3),
        "table_sizes_ms": round(table_sizes_ms, 3),
        "tree_stats_ms": round(tree_stats_ms, 3),
        **occupancy_stats(occ),
    }
    return row



def _extend_localisation_node_id(node_id: bytes, prefix_len: int, bucket: int, bits: int) -> bytes:
    """Mirror merkle_bytea_extend() for an untimed diagnostic replay."""
    required_bytes = (prefix_len + bits + 7) // 8
    result = bytearray(node_id)
    if len(result) < required_bytes:
        result.extend(b"\x00" * (required_bytes - len(result)))
    for offset in range(bits):
        bit_idx = prefix_len + offset
        byte_pos = bit_idx // 8
        bit_pos = 7 - (bit_idx % 8)
        bit = (bucket >> (bits - 1 - offset)) & 1
        if bit:
            result[byte_pos] |= 1 << bit_pos
        else:
            result[byte_pos] &= ~(1 << bit_pos)
    return bytes(result)


def _localisation_sql(explain: bool = False, partition_aware: bool = False) -> str:
    prefix = "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF, FORMAT JSON) " if explain else ""
    if partition_aware:
        return (
            f"{prefix}WITH wanted(partition_id, node_id, prefix_len) AS ("
            "SELECT * FROM unnest(%s::int4[], %s::bytea[], %s::int2[])) "
            "SELECT n.partition_id, n.node_id, n.prefix_len, n.is_leaf, n.hash "
            "FROM wanted w JOIN ariabc_internal.merkle_node n "
            "ON n.partition_id = w.partition_id AND n.node_id = w.node_id AND n.prefix_len = w.prefix_len "
            "WHERE n.index_oid = %s "
            "ORDER BY n.partition_id, n.prefix_len, n.node_id"
        )
    return (
        f"{prefix}SELECT node_id, prefix_len, is_leaf, hash "
        "FROM ariabc_internal.merkle_node "
        "WHERE index_oid = %s AND prefix_len = %s "
        "AND node_id = ANY(%s::bytea[]) ORDER BY node_id"
    )


def _plan_access_summary(plan: dict[str, Any]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if "Node Type" in value:
                nodes.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(plan)
    root = plan.get("Plan", {}) if isinstance(plan, dict) else {}
    return {
        "plan_node_types": "|".join(sorted({str(n.get("Node Type", "")) for n in nodes})),
        "plan_index_names": "|".join(sorted({str(n.get("Index Name", "")) for n in nodes if n.get("Index Name")})),
        "plan_actual_rows": root.get("Actual Rows", 0),
        "plan_rows_removed_by_filter": sum(int(n.get("Rows Removed by Filter", 0) or 0) for n in nodes),
        "shared_hit_blocks": sum(int(n.get("Shared Hit Blocks", 0) or 0) for n in nodes),
        "shared_read_blocks": sum(int(n.get("Shared Read Blocks", 0) or 0) for n in nodes),
        "shared_dirtied_blocks": sum(int(n.get("Shared Dirtied Blocks", 0) or 0) for n in nodes),
        "shared_written_blocks": sum(int(n.get("Shared Written Blocks", 0) or 0) for n in nodes),
        "planning_ms": plan.get("Planning Time", 0.0),
        "execution_ms": plan.get("Execution Time", 0.0),
        "io_read_ms": plan.get("I/O Read Time", 0.0),
        "io_write_ms": plan.get("I/O Write Time", 0.0),
    }


def _run_localisation_diagnostics(
    conn,
    profiler: ProfileCollector,
    result_dir: Path,
    *,
    run_id: str,
    tuple_count: int,
    fanout: int,
    split_threshold: int,
    merge_threshold: int,
    profile_label: str,
    repetition: int,
) -> list[dict[str, Any]]:
    """Replay the exact localization frontiers with EXPLAIN, outside timing.

    The native function performs several internal SPI queries per call.  The
    profiler records the exact frontier inputs; this function replays those
    inputs and the same child expansion in an untimed diagnostic phase, making
    the chosen index, filtered rows, and buffer activity visible in artifacts.
    """
    if not profiler.localisation_batches:
        return []
    plan_jsonl = result_dir / "localisation_plan_profiles.jsonl"
    summary_rows: list[dict[str, Any]] = []
    bits_per_split = 0
    while (1 << bits_per_split) < max(2, fanout):
        bits_per_split += 1
    replay_id = f"{run_id}-localisation-diagnostic"

    index_oids = {
        schema: scalar(conn, f"SELECT '{schema}.usertable_merkle_idx'::regclass::oid")
        for schema in ("healthy", "damaged")
    }
    with plan_jsonl.open("a") as jsonl:
        for batch in profiler.localisation_batches:
            current_nodes = list(batch["node_ids"])
            current_partition_ids = batch.get("partition_ids")
            partition_aware = current_partition_ids is not None
            max_depth = int(batch.get("max_depth", 0))
            current_prefix_len = int(batch.get("prefix_len", 0))
            current_prefix_lens = batch.get("prefix_lens")
            if current_prefix_lens is None:
                current_prefix_lens = [current_prefix_len] * len(current_nodes)
            for inner_depth in range(max_depth + 1):
                if not current_nodes:
                    break
                explain_sql = _localisation_sql(explain=True, partition_aware=partition_aware)
                plain_sql = _localisation_sql(explain=False, partition_aware=partition_aware)
                healthy_rows: list[dict[str, Any]] = []
                for schema in ("healthy", "damaged"):
                    if partition_aware:
                        plan_params = (
                            current_partition_ids,
                            current_nodes,
                            current_prefix_lens,
                            index_oids[schema],
                        )
                    else:
                        plan_params = (index_oids[schema], current_prefix_len, current_nodes)
                    plan_rows = execute(
                        conn,
                        explain_sql,
                        plan_params,
                    )
                    plan = parse_json_plan(plan_rows)
                    if not isinstance(plan, dict):
                        continue
                    if schema == "healthy":
                        healthy_rows = execute(
                            conn,
                            plain_sql,
                            plan_params,
                        )
                    payload = {
                        "run_id": run_id,
                        "manifest_sha256": profiler.manifest_sha256,
                        "diagnostic_replay_id": replay_id,
                        "diagnostic_cache_mode": "post_recovery_warm",
                        "tuple_count": tuple_count,
                        "fanout": fanout,
                        "split_threshold": split_threshold,
                        "merge_threshold": merge_threshold,
                        "profile_label": profile_label,
                        "repetition": repetition,
                        "schema": schema,
                        "source_batch_ordinal": batch["batch_ordinal"],
                        "inner_depth": inner_depth,
                        "prefix_len": current_prefix_len,
                        "input_node_count": len(current_nodes),
                        "partition_aware": int(partition_aware),
                        "index_oid": index_oids[schema],
                        "plan": plan,
                    }
                    jsonl.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
                    summary_rows.append(
                        {
                            "run_id": run_id,
                            "manifest_sha256": profiler.manifest_sha256,
                            "diagnostic_replay_id": replay_id,
                            "diagnostic_cache_mode": "post_recovery_warm",
                            "tuple_count": tuple_count,
                            "fanout": fanout,
                            "split_threshold": split_threshold,
                            "merge_threshold": merge_threshold,
                            "profile_label": profile_label,
                            "repetition": repetition,
                            "schema": schema,
                            "source_batch_ordinal": batch["batch_ordinal"],
                            "inner_depth": inner_depth,
                            "prefix_len": current_prefix_len,
                            "input_node_count": len(current_nodes),
                            "partition_aware": int(partition_aware),
                            "index_oid": index_oids[schema],
                            **_plan_access_summary(plan),
                        }
                    )
                next_nodes: list[bytes] = []
                next_partition_ids: list[int] = []
                if inner_depth < max_depth:
                    for row in healthy_rows:
                        if not bool(row["is_leaf"]):
                            for bucket in range(fanout):
                                next_nodes.append(
                                    _extend_localisation_node_id(
                                        bytes(row["node_id"]),
                                        current_prefix_len,
                                        bucket,
                                        bits_per_split,
                                    )
                                )
                                if partition_aware:
                                    next_partition_ids.append(int(row["partition_id"]))
                current_nodes = next_nodes
                if partition_aware:
                    current_partition_ids = next_partition_ids
                current_prefix_len += bits_per_split
    return summary_rows



def _run_deep_diagnostics(
    conn,
    manifest: dict[str, Any],
    result_dir: Path,
    *,
    run_id: str,
    tuple_count: int,
    fanout: int = 4,
    split_threshold: int = 32,
    merge_threshold: int = 8,
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
            bad_leaf_keys = [leaf_key(v) for v in manifest["bad_leaves"]]
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
                        "fanout": fanout,
                        "split_threshold": split_threshold,
                        "merge_threshold": merge_threshold,
                        "profile_label": profile_label,
                        "repetition": repetition,
                        "leaf_id": leaf_id,
                        "schema": schema,
                        "kind": kind,
                        "plan_order": plan_order,
                        "io_timing_available": io_timing_available,
                        "plan": plan,
                    }
                    # psycopg can expose bytea parameters as bytes in an
                    # EXPLAIN payload.  Preserve the diagnostic record rather
                    # than failing the benchmark after recovery has already
                    # completed.
                    jsonl.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
                    plan_node = plan.get("Plan", {})
                    summary_rows.append(
                        {
                            "leaf_id": leaf_id,
                            "run_id": run_id,
                            "manifest_sha256": manifest_digest,
                            "diagnostic_replay_id": diagnostic_replay_id,
                            "diagnostic_cache_mode": "post_recovery_warm",
                            "tuple_count": tuple_count,
                            "fanout": fanout,
                            "split_threshold": split_threshold,
                            "merge_threshold": merge_threshold,
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
                                plan_uses_index(plan, f"{schema}.usertable_merkle_partition_lookup_idx")
                            ),
                            "io_timing_available": io_timing_available,
                        }
                    )

    reset_damaged_from_healthy(conn, {
        "fanout": fanout,
        "split_threshold": split_threshold,
        "merge_threshold": merge_threshold,
    })
    apply_corruption(conn, manifest)
    capture_plans("candidate", 1)
    diagnostic_phase: dict[str, float] = {}
    bad_leaf_keys = [leaf_key(v) for v in manifest["bad_leaves"]]
    for leaf_id in sorted(bad_leaf_keys):
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
    localisation_index_stats_rows_out: list[dict[str, Any]] | None = None,
    *,
    profile_label: str = "",
    profiling_mode: str = "off",
    profiler: ProfileCollector | None = None,
    benchmark_profile: str = "",
    audit_mode: str = "full",
    leaf_fetch_chunk_size: int = 64,
    levels_per_batch: int = 3,
    synchronous_commit: str = "on",
) -> Metrics:
    cfg = {
        "fanout": int(manifest.get("fanout", 4)),
        "split_threshold": int(manifest.get("split_threshold", 32)),
        "merge_threshold": int(manifest.get("merge_threshold", 8)),
        "partitions": int(manifest.get("partitions", 200)),
    }
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
    localisation_stats_flush_wait_ms = 0.0
    localisation_stats_probe_ms = 0.0

    # Snapshot the catalog index counters immediately around localization.
    # The native function executes its descendant SQL through SPI, so this is
    # the server-side evidence of which merkle_node index actually did work.
    localisation_index_before: list[dict[str, Any]] = []
    localisation_index_stats_ok = False
    try:
        track_counts_enabled = str(show_setting(conn, "track_counts")).lower() == "on"
        localisation_index_stats_ok = track_counts_enabled
        if track_counts_enabled:
            stats_probe_start = now_ms()
            localisation_index_before = merkle_node_index_stats(conn)
            localisation_stats_probe_ms += now_ms() - stats_probe_start
    except Exception as exc:
        localisation_index_stats_ok = False
        counters["localisation_index_stats_error"] = str(exc)

    with timer(m.phase, "tree_localisation_ms"):
        bad_leaves = detect_bad_leaves(
            conn,
            counters,
            profiler=profiler,
            fanout=cfg["fanout"],
            partition_aware=True,
            levels_per_batch=levels_per_batch,
        )

    if localisation_index_stats_ok:
        stats_probe_start = now_ms()
        try:
            localisation_index_after = merkle_node_index_stats(conn)
            localisation_index_deltas = diff_merkle_node_index_stats(
                localisation_index_before,
                localisation_index_after,
            )
            stats_activity_fields = (
                "idx_scan_delta",
                "idx_tup_read_delta",
                "idx_tup_fetch_delta",
                "idx_blks_read_delta",
                "idx_blks_hit_delta",
            )
            # PostgreSQL 13's stats collector may not flush a short SPI query
            # before the next statement.  Retry once outside the measured
            # localization timer; never add this wait to tree_localisation_ms.
            if (
                localisation_index_deltas
                and all(bool(stat.get("track_counts_enabled")) for stat in localisation_index_deltas)
                and not any(
                    int(stat.get(field, 0) or 0)
                    for stat in localisation_index_deltas
                    for field in stats_activity_fields
                )
            ):
                flush_wait_start = now_ms()
                time.sleep(0.6)
                localisation_stats_flush_wait_ms += now_ms() - flush_wait_start
                localisation_index_after = merkle_node_index_stats(conn)
                localisation_index_deltas = diff_merkle_node_index_stats(
                    localisation_index_before,
                    localisation_index_after,
                )
            counters["localisation_index_stats_available"] = int(
                bool(localisation_index_deltas)
                and all(bool(stat.get("track_counts_enabled")) for stat in localisation_index_deltas)
            )
            counters["localisation_index_stats_nonzero"] = int(
                any(
                    int(stat.get(field, 0) or 0)
                    for stat in localisation_index_deltas
                    for field in stats_activity_fields
                )
            )
            for stat in localisation_index_deltas:
                index_name = str(stat["index_name"])
                safe_name = "".join(
                    ch if ch.isalnum() else "_" for ch in index_name
                ).strip("_")
                for field in (
                    "idx_scan_delta",
                    "idx_tup_read_delta",
                    "idx_tup_fetch_delta",
                    "idx_blks_read_delta",
                    "idx_blks_hit_delta",
                ):
                    counters[f"localisation_{safe_name}_{field}"] = stat[field]
                if localisation_index_stats_rows_out is not None:
                    localisation_index_stats_rows_out.append(
                        {
                            "run_id": run_id,
                            "manifest_sha256": manifest_digest,
                            "experiment": manifest["experiment"],
                            "profile_label": profile_label,
                            "tuple_count": tuple_count,
                            "fanout": cfg["fanout"],
                            "split_threshold": cfg["split_threshold"],
                            "merge_threshold": cfg["merge_threshold"],
                            "bad_leaf_count": len(manifest["bad_leaves"]),
                            "corrupted_tuple_count": len(manifest["corruptions"]),
                            "repetition": repetition,
                            "phase": "tree_localisation",
                            **stat,
                        }
                    )
        except Exception as exc:
            counters["localisation_index_stats_available"] = 0
            counters["localisation_index_stats_error"] = str(exc)
        finally:
            localisation_stats_probe_ms += now_ms() - stats_probe_start
    else:
        counters["localisation_index_stats_available"] = 0

    expected_bad_leaves = sorted(set(leaf_key(v) for v in manifest["bad_leaves"]))
    if sorted(bad_leaves) != expected_bad_leaves:
        add_warning(m, f"bad leaves mismatch expected={expected_bad_leaves} actual={bad_leaves}")

    rows_inserted = rows_updated = rows_deleted = 0
    candidate_rows = healthy_rows = damaged_rows = 0
    lookup_scans = 0
    per_leaf_candidates: list[int] = []

    if synchronous_commit.lower() in ("on", "off"):
        execute(conn, f"SET synchronous_commit = {synchronous_commit}")

    # Split bad_leaves into chunks to bound peak memory.
    if leaf_fetch_chunk_size <= 0:
        bad_leaf_chunks = [bad_leaves]
    else:
        bad_leaf_chunks = [bad_leaves[i:i + leaf_fetch_chunk_size] for i in range(0, len(bad_leaves), leaf_fetch_chunk_size)]

    candidate_fetch_sql_calls = 0
    candidate_fetch_batches = 0
    candidate_leaf_buckets_requested = 0

    all_inserts: list[int] = []
    all_updates: list[int] = []
    all_deletes: list[int] = []
    all_hrows: dict[int, dict[str, Any]] = {}

    for chunk in bad_leaf_chunks:
        if not chunk:
            continue

        with timer(m.phase, "candidate_row_fetch_ms"):
            healthy_by_leaf: FetchResult = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_batch_healthy",
                schema="healthy",
                fn=lambda c=chunk: fetch_leaf_rows_batch(conn, "healthy", c,
                                                         chunk_size=0, partitions=cfg["partitions"]),
            )
            damaged_by_leaf: FetchResult = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_batch_damaged",
                schema="damaged",
                fn=lambda c=chunk: fetch_leaf_rows_batch(conn, "damaged", c,
                                                         chunk_size=0, partitions=cfg["partitions"]),
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
                measure_repair_write=False,
                execute_dml=False,
            )
            healthy_rows += len(hrows)
            damaged_rows += len(drows)
            leaf_total = len(hrows) + len(drows)
            candidate_rows += leaf_total
            per_leaf_candidates.append(leaf_total)
            all_hrows.update(hrows)
            all_inserts.extend(ins)
            all_updates.extend(upd)
            all_deletes.extend(dlt)

        # Discard chunk from memory
        del healthy_by_leaf
        del damaged_by_leaf

    # Apply all accumulated repair DMLs in one single write transaction across the entire recovery phase
    if all_inserts or all_updates or all_deletes:
        with timer(m.phase, "repair_write_ms"):
            with timer(m.phase, "repair_transaction_block_ms"):
                t_tx_enter = time.perf_counter()
                with conn.transaction():
                    t_inside_tx = time.perf_counter()
                    rows_inserted += execute_batched_inserts(conn, "damaged", all_inserts, all_hrows, profiler, phase=m.phase)
                    rows_updated += execute_batched_updates(conn, "damaged", all_updates, all_hrows, profiler, phase=m.phase)
                    rows_deleted += execute_batched_deletes(conn, "damaged", all_deletes, profiler, phase=m.phase)
                    t_before_commit = time.perf_counter()
                t_after_commit = time.perf_counter()
                m.phase["repair_commit_wire_ms"] = m.phase.get("repair_commit_wire_ms", 0.0) + (t_after_commit - t_before_commit) * 1000.0
                m.phase["repair_begin_wire_ms"] = m.phase.get("repair_begin_wire_ms", 0.0) + (t_inside_tx - t_tx_enter) * 1000.0

    if bad_leaves:
        lookup_scans = candidate_fetch_sql_calls

    with timer(m.phase, "targeted_post_repair_confirmation_ms"):
        post_repair_counters: dict[str, Any] = {}
        affected_partitions = {leaf[0] for leaf in bad_leaves if len(leaf) == 3} if bad_leaves else None
        remaining_bad_leaves = detect_bad_leaves(
            conn,
            post_repair_counters,
            prefix="targeted_confirmation_",
            operation_prefix="confirmation_",
            profiler=profiler,
            stage_name="targeted_confirmation",
            fanout=cfg["fanout"],
            partition_aware=True,
            target_partitions=affected_partitions,
            levels_per_batch=levels_per_batch,
        )
        repaired_leaf_mismatch = False
        confirmation_fetch_sql_calls = 0
        confirmation_fetch_batches = 0
        confirmation_leaf_buckets_requested = 0
        confirmation_rows_fetched = 0

        confirmation_chunks = chunk_list(remaining_bad_leaves, leaf_fetch_chunk_size) if remaining_bad_leaves else []
        for chunk in confirmation_chunks:
            if not chunk:
                continue

            confirmed_healthy: FetchResult = record_call(
                profiler,
                stage="targeted_confirmation",
                operation="confirmation_leaf_fetch_batch_healthy",
                schema="healthy",
                fn=lambda c=chunk: fetch_leaf_rows_batch(conn, "healthy", c,
                                                         chunk_size=0, partitions=cfg["partitions"]),
            )
            confirmed_damaged: FetchResult = record_call(
                profiler,
                stage="targeted_confirmation",
                operation="confirmation_leaf_fetch_batch_damaged",
                schema="damaged",
                fn=lambda c=chunk: fetch_leaf_rows_batch(conn, "damaged", c,
                                                         chunk_size=0, partitions=cfg["partitions"]),
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

    # Exclude the index-statistics probes from the measured recovery boundary.
    # They are diagnostic SQL, not recovery work.  Keep both the total probe
    # cost and its asynchronous flush-wait subset as explicit fields so the
    # observability cost remains auditable.
    recovery_end = now_ms() - localisation_stats_probe_ms
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
              AND indexname IN ('usertable_pkey', 'usertable_merkle_idx', 'usertable_merkle_partition_lookup_idx')
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
            "partition_root_batches": counters.get("partition_root_batches", 0),
            "partition_root_batches_ok": int(counters.get("partition_root_batches", 0) > 0),
            "full_audit_skipped": full_audit_skipped,
            "audit_mode": audit_mode,
            "leaf_fetch_chunk_size": leaf_fetch_chunk_size,
            "localisation_stats_probe_ms": round(localisation_stats_probe_ms, 3),
            "localisation_stats_flush_wait_ms": round(localisation_stats_flush_wait_ms, 3),
        }
    )

    if candidate_rows >= 0.5 * tuple_count:
        add_warning(m, "candidate rows exceed sparse threshold")
    if recovery_full_heap_scans != 0:
        add_warning(m, "recovery performed heap sequential scan")

    if benchmark_profile in ("fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
        expected_bad_leaf_count = 75 if benchmark_profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") else 20
        if m.bad_leaf_count != expected_bad_leaf_count and m.bad_leaf_count != m.corrupted_tuple_count:
            raise RuntimeError(f"{m.run_id}: bad_leaf_count={m.bad_leaf_count}, expected {expected_bad_leaf_count} or {m.corrupted_tuple_count}")
        if m.corrupted_tuple_count != 300:
            raise RuntimeError(f"{m.run_id}: corrupted_tuple_count={m.corrupted_tuple_count}, expected 300")
        if len(bad_leaves) != expected_bad_leaf_count and len(bad_leaves) != m.corrupted_tuple_count:
            raise RuntimeError(f"{m.run_id}: detected_bad_leaves={len(bad_leaves)}, expected {expected_bad_leaf_count} or {m.corrupted_tuple_count}")
        total_repaired = rows_inserted + rows_updated + rows_deleted
        if m.corruption_mode not in ("paper-update-only", "mixed", "update-only", "delete-only", "insert-only"):
            raise RuntimeError(f"{m.run_id}: corruption_mode={m.corruption_mode}, invalid corruption mode")
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
    localisation_index_stats_rows_out: list[dict[str, Any]] | None = None,
    localisation_plan_summary_rows_out: list[dict[str, Any]] | None = None,
    *,
    profile_label: str = "",
    profiling_mode: str = "off",
    benchmark_profile: str = "",
    audit_mode: str = "full",
    leaf_fetch_chunk_size: int = 64,
    levels_per_batch: int = 3,
    synchronous_commit: str = "on",
) -> list[Metrics]:
    tuple_count = int(manifest["tuple_count"])
    cfg = {
        "fanout": int(manifest.get("fanout", 4)),
        "split_threshold": int(manifest.get("split_threshold", 32)),
        "merge_threshold": int(manifest.get("merge_threshold", 8)),
    }
    metrics: list[Metrics] = []
    # ── Untimed Warmup Cycles ───────────────────────────────────────────────
    # Prime PostgreSQL query plans, Python bytecode, shared_buffers pages, and
    # the synchronous Merkle/WAL write path before rep 0. Do not add an
    # explicit checkpoint here: it would introduce an unrelated I/O barrier
    # between warmup and measurement. The dataset build already establishes
    # the initial checkpoint boundary.
    warmup_cycles_ms = 0.0
    if reps > 0:
        import time
        t_w_start = time.perf_counter()
        for warmup_rep in range(RECOVERY_WARMUP_CYCLES):
            warmup_id = f"{recovery_run_id(manifest, 0, profile_label)}-warmup{warmup_rep}"
            apply_corruption(conn, manifest)
            planner_results, _ = run_planner_preflight(conn, manifest, warmup_id)
            repair_merkle(
                conn,
                manifest,
                tuple_count,
                -1,
                planner_results,
                [],
                localisation_index_stats_rows_out=None,
                profile_label=profile_label,
                profiling_mode="off",
                profiler=None,
                benchmark_profile="",
                audit_mode="skip",
                leaf_fetch_chunk_size=leaf_fetch_chunk_size,
                levels_per_batch=levels_per_batch,
                synchronous_commit=synchronous_commit,
            )
        warmup_cycles_ms = (time.perf_counter() - t_w_start) * 1000.0
        print(f"  [warmup] {RECOVERY_WARMUP_CYCLES} untimed recovery cycles took {warmup_cycles_ms/1000.0:.2f}s", flush=True)
    manifest["recovery_warmup_cycles_ms"] = round(warmup_cycles_ms, 3)


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
            bad_leaf_count=len(manifest["bad_leaves"]),
            repetition=rep,
            completed_runs=progress_state["completed_runs"],
            total_runs=progress_state["total_runs"],
        )
        method_start = now_ms()
        apply_corruption(conn, manifest)
        planner_results, planner_rows = run_planner_preflight(conn, manifest, run_id)
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
            localisation_index_stats_rows_out=localisation_index_stats_rows_out,
            profile_label=profile_label,
            profiling_mode=profiling_mode,
            profiler=profiler,
            benchmark_profile=benchmark_profile,
            audit_mode=audit_mode,
            leaf_fetch_chunk_size=leaf_fetch_chunk_size,
            levels_per_batch=levels_per_batch,
            synchronous_commit=synchronous_commit,
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
                        "fanout": cfg["fanout"],
                        "split_threshold": cfg.get("split_threshold", 32),
                        "merge_threshold": cfg.get("merge_threshold", 8),
                        "bad_leaf_count": len(manifest["bad_leaves"]),
                        "corrupted_tuple_count": len(manifest["corruptions"]),
                        "repetition": rep,
                        **profiler.backend_profile,
                    }
                )
            if profiling_mode == "deep" and rep == 0:
                localisation_rows = _run_localisation_diagnostics(
                    conn,
                    profiler,
                    result_dir,
                    run_id=run_id,
                    tuple_count=tuple_count,
                    fanout=cfg["fanout"],
                    split_threshold=cfg.get("split_threshold", 32),
                    merge_threshold=cfg.get("merge_threshold", 8),
                    profile_label=profile_label,
                    repetition=rep,
                )
                if localisation_plan_summary_rows_out is not None:
                    localisation_plan_summary_rows_out.extend(localisation_rows)
                deep_rows = _run_deep_diagnostics(
                    conn,
                    manifest,
                    result_dir,
                    run_id=run_id,
                    tuple_count=tuple_count,
                    fanout=cfg["fanout"],
                    split_threshold=cfg.get("split_threshold", 32),
                    merge_threshold=cfg.get("merge_threshold", 8),
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
        fanout: int,
        split_threshold: int = 32,
        merge_threshold: int = 8,
        bad_leaf_count: int,
        corrupted_tuple_count: int,
        geometry_label: str,
        **kwargs,
    ) -> dict[str, Any]:
        return {
            "experiment": experiment,
            "tuple_count": tuple_count,
            "fanout": fanout,
            "split_threshold": split_threshold,
            "merge_threshold": merge_threshold,
            "bad_leaf_count": bad_leaf_count,
            "corrupted_tuple_count": corrupted_tuple_count,
            "geometry_label": geometry_label,
        }

    override_fanout = getattr(args, "fanout", None)
    manual_geometry = (
        getattr(args, "fanout", None) is not None
        or getattr(args, "split_threshold", None) is not None
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
            geo = geometry_matrix.get(label, {})
            fanout = int(override_fanout or geo.get("fanout", 4))
            split_threshold = int(geo.get("split_threshold", 32))
            merge_threshold = int(geo.get("merge_threshold", 8))
            out_label = (
                _manual_geometry_label(fanout, split_threshold, merge_threshold)
                if manual_geometry else label
            )
            for n in _selected(sizes, args.tuple_count):
                series.append(
                    case(
                        experiment=args.profile,
                        tuple_count=n,
                        fanout=fanout,
                        split_threshold=split_threshold,
                        merge_threshold=merge_threshold,
                        bad_leaf_count=int(args.bad_leaf_count or 10),
                        corrupted_tuple_count=300,
                        geometry_label=out_label,
                    )
                )
        return series

    if args.profile == "fanout-width-sweep":
        if getattr(args, "corruption_mode", None) not in (None, "paper-update-only", "mixed"):
            raise ValueError("fanout-width-sweep requires --corruption-mode mixed or paper-update-only")
        # fanout-width-sweep: fixed workload, no overrides permitted.
        # All parameters are encoded in the geometry matrix labels.
        # The only allowed user selection is --geometry-label <known label>.
        _SWEEP_FIXED_PARAMS = (
            "tuple_count", "fanout", "bad_leaf_count",
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

        SWEEP_CANONICAL_LABELS = [
            "fanout_f2_l16",
            "fanout_f4_l16",
            "fanout_f16_l16",
            "fanout_f2_l64",
            "fanout_f4_l64",
            "fanout_f8_l64",
            "fanout_f64_l64",
            "fanout_f2_l128",
            "fanout_f128_l128",
            "fanout_f2_l256",
            "fanout_f4_l256",
            "fanout_f16_l256",
            "fanout_f256_l256",
            "fanout_f2_l512",
            "fanout_f512_l512",
            "fanout_f2_l1024",
            "fanout_f4_l1024",
            "fanout_f32_l1024",
            "fanout_f1024_l1024",
        ]

        if args.geometry_label and args.geometry_label not in SWEEP_CANONICAL_LABELS:
            raise ValueError(
                f"unknown fanout-width-sweep geometry label '{args.geometry_label}'; "
                f"valid labels: {', '.join(SWEEP_CANONICAL_LABELS)}"
            )

        sweep_series: list[dict[str, Any]] = []
        for label in SWEEP_CANONICAL_LABELS:
            if args.geometry_label and label != args.geometry_label:
                continue
            geo = geometry_matrix.get(label, {})
            fanout = int(geo.get("fanout", 4))
            split_threshold = int(geo.get("split_threshold", 32))
            merge_threshold = int(geo.get("merge_threshold", 8))
            sweep_series.append(
                case(
                    experiment=args.profile,
                    tuple_count=5_000_000,
                    fanout=fanout,
                    split_threshold=split_threshold,
                    merge_threshold=merge_threshold,
                    bad_leaf_count=20,
                    corrupted_tuple_count=300,
                    geometry_label=label,
                )
            )
        return sweep_series

    if args.profile == "size-scaling-k75-c300":
        if getattr(args, "corruption_mode", None) not in (None, "paper-update-only", "mixed"):
            raise ValueError("size-scaling-k75-c300 requires --corruption-mode mixed or paper-update-only")
        allowed_tuple_counts = [
            1_000_000,
            3_000_000,
            5_000_000,
            7_000_000,
            10_000_000,
            15_000_000,
            20_000_000,
            25_000_000,
            30_000_000,
            40_000_000,
            50_000_000,
        ]
        if args.tuple_count is not None:
            selected_sizes = _selected(allowed_tuple_counts, args.tuple_count)
            invalid_sizes = [n for n in selected_sizes if n not in allowed_tuple_counts]
            if invalid_sizes:
                raise ValueError(
                    "size-scaling-k75-c300 owns tuple counts from 1M to 50M"
                )
            tuple_counts = selected_sizes
        else:
            tuple_counts = allowed_tuple_counts
        if getattr(args, "fanout", None) is not None:
            raise ValueError("size-scaling-k75-c300 uses fixed canonical geometries; do not override geometry")
        if getattr(args, "bad_leaf_count", None) is not None:
            raise ValueError("size-scaling-k75-c300 uses fixed --bad-leaf-count=75")

        default_labels = [
            "fanout_f4_l16",
        ]

        if args.geometry_label:
            if args.geometry_label not in geometry_matrix:
                raise ValueError(
                    f"unknown size-scaling-k75-c300 geometry label '{args.geometry_label}'; "
                    f"valid labels: {', '.join(default_labels)}"
                )
            labels = [args.geometry_label]
        else:
            labels = default_labels

        series = []
        for label in labels:
            geo = geometry_matrix.get(label, {})
            fanout = int(geo.get("fanout", 4))
            split_threshold = int(geo.get("split_threshold", 32))
            merge_threshold = int(geo.get("merge_threshold", 8))

            for n in tuple_counts:
                series.append(
                    case(
                        experiment=args.profile,
                        tuple_count=n,
                        fanout=fanout,
                        split_threshold=split_threshold,
                        merge_threshold=merge_threshold,
                        bad_leaf_count=75,
                        corrupted_tuple_count=300,
                        geometry_label=label,
                    )
                )
        return series

    if args.profile == "best-scaling-f32-l1024-k75-c300":
        if getattr(args, "corruption_mode", None) not in (None, "paper-update-only", "mixed"):
            raise ValueError("best-scaling-f32-l1024-k75-c300 requires --corruption-mode mixed or paper-update-only")
        if getattr(args, "fanout", None) is not None and getattr(args, "fanout") != 32:
            raise ValueError(
                "best-scaling-f32-l1024-k75-c300 uses fixed geometry "
                "F=32; supplied geometry must match exactly"
            )
        if getattr(args, "bad_leaf_count", None) is not None and getattr(args, "bad_leaf_count") != 75:
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
                    fanout=int(geo.get("fanout", 4)),
                    split_threshold=int(geo.get("split_threshold", 32)),
                    merge_threshold=int(geo.get("merge_threshold", 8)),
                    bad_leaf_count=75,
                    corrupted_tuple_count=300,
                    geometry_label=label,
                )
            )
        return series



    series = []
    if getattr(args, "experiment", None) in (None, "figure12"):
        for n in _selected(config.fig12_sizes, getattr(args, "tuple_count", None)):
            fanout = int(override_fanout or 4)
            series.append(
                case(
                    experiment="figure12",
                    tuple_count=n,
                    fanout=fanout,
                    split_threshold=32,
                    merge_threshold=8,
                    bad_leaf_count=(args.bad_leaf_count if getattr(args, "bad_leaf_count", None) is not None else (10 if n >= 10 else 1)),
                    corrupted_tuple_count=300,
                    geometry_label=(
                        _manual_geometry_label(fanout, 32, 8)
                        if manual_geometry else "default_dynamic"
                    ),
                )
            )
    if getattr(args, "experiment", None) in (None, "figure13"):
        sizes = _selected(config.fig13_sizes, getattr(args, "tuple_count", None))
        bad_counts = _selected(config.fig13_k, getattr(args, "bad_leaf_count", None))
        for n in sizes:
            for k in bad_counts:
                fanout = int(override_fanout or 4)
                series.append(
                    case(
                        experiment="figure13",
                        tuple_count=n,
                        fanout=fanout,
                        split_threshold=32,
                        merge_threshold=8,
                        bad_leaf_count=k,
                        corrupted_tuple_count=300,
                        geometry_label=(
                            _manual_geometry_label(fanout, 32, 8)
                            if manual_geometry else "default_dynamic"
                        ),
                    )
                )
    return series


def _count_planned_runs(config, args) -> int:
    return len(_series_for_profile(args, config)) * int(config.repetitions)


# ── main benchmark orchestrator ───────────────────────────────────────────────

def run_benchmark(args: argparse.Namespace) -> Path:
    global RESULT_ROOT
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    result_dir = RESULT_ROOT / ts
    result_dir.mkdir(parents=True, exist_ok=True)
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
    localisation_index_stats_rows: list[dict[str, Any]] = []
    localisation_plan_summary_rows: list[dict[str, Any]] = []
    runtime_setting_rows: list[dict[str, Any]] = []
    io_timing_setting = ""
    server_version = ""
    git_head = ""
    incremental_dataset = args.incremental_dataset_expansion
    if incremental_dataset is None:
        incremental_dataset = args.profile in {
            "size-scaling-k75-c300",
            "best-scaling-f32-l1024-k75-c300",
        }
    dataset_state: tuple[int, int, int, int, int] | None = None

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
        for setting_name in (
            "shared_buffers",
            "effective_cache_size",
            "work_mem",
            "random_page_cost",
            "seq_page_cost",
            "enable_seqscan",
            "max_parallel_workers_per_gather",
            "track_io_timing",
            "track_counts",
            "jit",
        ):
            try:
                setting_value = show_setting(conn, setting_name)
            except Exception as exc:
                setting_value = f"unavailable: {exc}"
            runtime_setting_rows.append(
                {
                    "setting": setting_name,
                    "value": str(setting_value),
                }
            )
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
            fanout = int(spec["fanout"])
            split_threshold = int(spec.get("split_threshold", 32))
            merge_threshold = int(spec.get("merge_threshold", 8))
            bad_leaf_count = int(spec["bad_leaf_count"])
            corrupted_tuple_count = int(spec["corrupted_tuple_count"])
            geometry_label = str(spec["geometry_label"])
            experiment = str(spec["experiment"])
            validate_geometry(fanout, split_threshold, merge_threshold)

            emit_progress(
                result_dir,
                event="dataset_start",
                experiment=experiment,
                profile_label=geometry_label,
                tuple_count=n,
                bad_leaf_count=bad_leaf_count,
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
            )
            setup_mode = args.dataset_setup_mode
            if setup_mode is None:
                setup_mode = "bulk-logged"
            geometry_state = (fanout, split_threshold, merge_threshold, int(spec.get("partitions", 200)))
            if incremental_dataset and dataset_state is not None and dataset_state[1:] == geometry_state and n > dataset_state[0]:
                build_timings = expand_dataset(
                    conn,
                    dataset_state[0],
                    n,
                    fanout,
                    split_threshold,
                    merge_threshold,
                    geometry_state[3],
                )
            else:
                build_timings = build_dataset(
                    conn,
                    n,
                    fanout,
                    split_threshold,
                    merge_threshold,
                    setup_mode=setup_mode,
                    partitions=geometry_state[3],
                )
            dataset_state = (n, *geometry_state)
            emit_progress(
                result_dir,
                event="dataset_build_timing",
                profile_label=geometry_label,
                tuple_count=n,
                timings_ms=build_timings,
            )
            t_bcs0 = time.perf_counter()
            bsum, bdebug = bucket_consistency_sample(conn, n, fanout, args.seed)
            bucket_consistency_sample_ms = (time.perf_counter() - t_bcs0) * 1000.0
            bucket_summary_rows.append(bsum)
            if args.artifact_mode == "debug":
                bucket_debug_rows.extend(bdebug)

            t_dsr0 = time.perf_counter()
            ds_row = _dataset_row(conn, n, {
                "fanout": fanout,
                "split_threshold": split_threshold,
                "merge_threshold": merge_threshold,
            }, geometry_label)
            dataset_row_total_ms = (time.perf_counter() - t_dsr0) * 1000.0
            dataset_rows.append(ds_row)

            emit_progress(
                result_dir,
                event="dataset_complete",
                experiment=experiment,
                profile_label=geometry_label,
                tuple_count=n,
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
                bucket_consistency_sample_ms=round(bucket_consistency_sample_ms, 3),
                dataset_row_total_ms=round(dataset_row_total_ms, 3),
                tree_stats_ms=ds_row.get("tree_stats_ms", 0.0),
                leaf_occupancy_ms=ds_row.get("leaf_occupancy_ms", 0.0),
                table_sizes_ms=ds_row.get("table_sizes_ms", 0.0),
            )
            d = corrupted_tuple_count if args.profile == "recovery-scaling-diagnosis" else (
                300 if args.profile in ("paper", "preflight", "fanout-width-sweep", "size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300") else bad_leaf_count
            )

            if args.profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                # Progress state is JSON; use a stable string key rather than
                # a tuple, which cannot be serialized as a JSON object key.
                forced_key = f"{geometry_label}:n{n}"
                forced_map = progress_state.setdefault("forced_bad_leaves_by_geometry", {})
                forced_bad_leaves = forced_map.get(forced_key)
            else:
                forced_bad_leaves = None

            t_cm0 = time.perf_counter()
            manifest = choose_corruption_manifest(
                conn,
                experiment,
                n,
                fanout,
                bad_leaf_count,
                d,
                args.seed,
                corruption_mode=args.corruption_mode,
                forced_bad_leaves=forced_bad_leaves,
            )
            choose_corruption_manifest_ms = (time.perf_counter() - t_cm0) * 1000.0
            d = manifest["corrupted_tuple_count"]

            t_vmlm0 = time.perf_counter()
            validate_manifest_leaf_mapping(conn, manifest)
            validate_manifest_leaf_mapping_ms = (time.perf_counter() - t_vmlm0) * 1000.0

            print(
                f"[verification] tuple_count={n} "
                f"bucket_sample={bucket_consistency_sample_ms:.1f}ms "
                f"tree_stats={ds_row.get('tree_stats_ms', 0):.1f}ms "
                f"leaf_occ={ds_row.get('leaf_occupancy_ms', 0):.1f}ms "
                f"table_sizes={ds_row.get('table_sizes_ms', 0):.1f}ms "
                f"choose_manifest={choose_corruption_manifest_ms:.1f}ms "
                f"validate_mapping={validate_manifest_leaf_mapping_ms:.1f}ms",
                flush=True,
            )


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
                    choose_corruption_manifest_ms=round(choose_corruption_manifest_ms, 3),
                    validate_manifest_leaf_mapping_ms=round(validate_manifest_leaf_mapping_ms, 3),
                )


                # Assert provenance
                if args.profile in ("size-scaling-k75-c300", "best-scaling-f32-l1024-k75-c300"):
                    # Leaf IDs are a property of the built tree, and the tree
                    # changes as the dataset grows.  Do not compare different
                    # tuple-count tiers under one provenance slot.
                    tier_key = f"tier_{geometry_label}_n{n}"
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
                    tier_key = f"tier_f{fanout}"
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
                    "fanout": fanout,
                    "split_threshold": split_threshold,
                    "merge_threshold": merge_threshold,
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
                    localisation_index_stats_rows,
                    localisation_plan_summary_rows,
                    profile_label=geometry_label,
                    profiling_mode=args.profiling,
                    benchmark_profile=args.profile,
                    audit_mode=args.audit_mode,
                    leaf_fetch_chunk_size=args.leaf_fetch_batch_size,
                    levels_per_batch=getattr(args, "levels_per_batch", 3),
                    synchronous_commit=getattr(args, "synchronous_commit", "on"),
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
                "fanout",
                "split_threshold",
                "merge_threshold",
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
        "profile_label", "tuple_count", "fanout", "split_threshold", "merge_threshold",
        "total_leaf_count",
        "tree_levels", "tree_edges", "tree_depth",
        "tree_height", "max_prefix_len",
        "total_merkle_nodes", "total_logical_tree_nodes",
        "physical_rows_per_leaf_expected", "expected_candidate_rows_per_bad_leaf",
        "base_table_bytes", "primary_index_bytes", "merkle_index_bytes",
        "leaf_lookup_index_bytes",
        "total_schema_bytes",
        "minimum", "p50", "p95", "p99", "maximum", "mean", "stddev",
    ])
    write_csv(result_dir / "bucket_consistency_summary.csv", bucket_summary_rows, [
        "tuple_count", "fanout",
        "sample_count", "sample_seed", "mismatch_count", "sample_digest",
    ])
    if args.artifact_mode == "debug":
        write_csv(result_dir / "bucket_consistency.csv", bucket_debug_rows, [
            "tuple_count", "fanout",
            "ycsb_key", "bucket", "leaf_id", "match",
        ])
    write_csv(result_dir / "planner_checks.csv", planner_rows, [
        "run_id", "schema", "leaf_id", "index_oid", "index_relfilenode",
        "index_definition", "plan_uses_expected_leaf_lookup_index", "plan_json_sha256",
    ])
    write_csv(result_dir / "localisation_index_stats.csv", localisation_index_stats_rows, [
        "run_id", "manifest_sha256", "experiment", "profile_label", "tuple_count",
        "fanout", "split_threshold", "merge_threshold", "bad_leaf_count",
        "corrupted_tuple_count", "repetition", "phase", "index_name",
        "idx_scan_delta", "idx_tup_read_delta", "idx_tup_fetch_delta",
        "idx_blks_read_delta", "idx_blks_hit_delta", "relation_bytes",
        "index_bytes", "estimated_relation_rows", "track_counts_enabled",
    ])
    write_csv(result_dir / "runtime_settings.csv", runtime_setting_rows, [
        "setting", "value",
    ])
    write_csv(result_dir / "localisation_plan_summary.csv", localisation_plan_summary_rows, [
        "run_id", "manifest_sha256", "diagnostic_replay_id", "diagnostic_cache_mode",
        "tuple_count", "fanout", "split_threshold", "merge_threshold",
        "profile_label", "repetition", "schema", "source_batch_ordinal",
        "inner_depth", "prefix_len", "input_node_count", "index_oid",
        "plan_node_types", "plan_index_names", "plan_actual_rows",
        "plan_rows_removed_by_filter", "shared_hit_blocks", "shared_read_blocks",
        "shared_dirtied_blocks", "shared_written_blocks", "planning_ms",
        "execution_ms", "io_read_ms", "io_write_ms",
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
            "run_id", "manifest_sha256", "experiment", "tuple_count",
            "fanout", "split_threshold", "merge_threshold", "profile_label", "bad_leaf_count",
            "corrupted_tuple_count", "repetition",
        )
        per_run_denominators = {
            (
                m.run_id,
                m.counters.get("manifest_sha256", ""),
                m.experiment,
                m.tuple_count,
                m.fanout,
                m.split_threshold,
                m.merge_threshold,
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
            "manifest_sha256", "experiment", "tuple_count",
            "fanout", "split_threshold", "merge_threshold", "profile_label", "bad_leaf_count",
            "corrupted_tuple_count",
        )
        by_geometry_denominators: dict[tuple[Any, ...], float] = {}
        for m in all_metrics:
            key = (
                m.counters.get("manifest_sha256", ""),
                m.experiment,
                m.tuple_count,
                m.fanout,
                m.split_threshold,
                m.merge_threshold,
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
            "manifest_sha256", "tuple_count", "fanout", "split_threshold", "merge_threshold",
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
                "fanout",
                "split_threshold",
                "merge_threshold",
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
            metric.fanout,
            metric.split_threshold,
            metric.merge_threshold,
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
            key = (row["profile_label"], row["fanout"], row["split_threshold"], row["merge_threshold"])
            if key in seen_geometry:
                continue
            seen_geometry.add(key)
            report_lines.append(
                f"  - {row['profile_label']}: fanout={row['fanout']}, "
                f"split_threshold={row['split_threshold']}, merge_threshold={row['merge_threshold']}, "
                f"total_leaf_count={row['total_leaf_count']}, tree_depth={row.get('tree_depth', row.get('max_depth', 0))}"
            )
    report_lines.extend([
        "",
        "## Localization Access Observability",
        "",
        "Per-run deltas for `pg_stat_all_indexes` and `pg_statio_all_indexes` around the native `merkle_node` localization call are in `localisation_index_stats.csv`.",
        "These include index scans, index tuples read/fetched, buffer reads/hits, and relation/index sizes.",
        "The diagnostic snapshot and asynchronous stats flush are reported as `localisation_stats_probe_ms` and `localisation_stats_flush_wait_ms`; both are excluded from `restore_repair_ms`.",
        "The session planner/cache settings used by the campaign are in `runtime_settings.csv`.",
        "Deep profiling additionally replays the exact localization frontiers with `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` in `localisation_plan_summary.csv` and `localisation_plan_profiles.jsonl`.",
    ])
    report_lines.extend([
        "",
        "## Phase Medians And P95",
        "",
        "| profile_label | tuple_count | fanout | split_threshold | merge_threshold | bad_leaf_count | corrupted_tuple_count | phase | median_ms | p95_ms |",
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
    parser.add_argument("--split-threshold", type=int, dest="split_threshold")
    parser.add_argument("--merge-threshold", type=int, dest="merge_threshold")
    parser.add_argument("--bad-leaf-count", type=int, dest="bad_leaf_count")
    parser.add_argument("--fanout", type=int, default=None)
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
        default="mixed",
        dest="corruption_mode",
        help="Corruption injection mode. Defaults to mixed (1/3 update, 1/3 delete, 1/3 insert).",
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
        default="skip",
        dest="audit_mode",
        help="Validation mode after repair. full runs expensive full-table audit; skip keeps sparse targeted confirmation only.",
    )
    parser.add_argument(
        "--dataset-setup-mode",
        choices=["legacy", "bulk-logged", "bulk-unlogged"],
        default=None,
        help=(
            "Dataset setup path. Size campaigns default to bulk-unlogged "
            "(converted to logged before Merkle CREATE INDEX); other profiles "
            "default to bulk-logged."
        ),
    )
    parser.add_argument(
        "--incremental-dataset-expansion",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Append ascending size checkpoints and rebuild only derived Merkle "
            "state. Size campaigns default to enabled."
        ),
    )
    parser.add_argument(
        "--levels-per-batch",
        "--localisation-depth-step",
        type=int,
        default=1,
        dest="levels_per_batch",
        help="Number of Merkle tree levels to descend per SQL batch/round-trip during localisation (default: 1).",
    )
    parser.add_argument(
        "--partitions",
        "--num-partitions",
        type=int,
        default=None,
        dest="partitions",
        help="Number of Merkle tree partitions (default: 200).",
    )
    parser.add_argument(
        "--synchronous-commit",
        choices=["on", "off"],
        default="on",
        dest="synchronous_commit",
        help="PostgreSQL synchronous_commit setting during recovery repair (default: on).",
    )
    args = parser.parse_args(argv)

    if args.result_dir:
        RESULT_ROOT = Path(args.result_dir)

    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    # Use --scratch-dir as the parent of the tmp_ directory when supplied,
    # so the remote launcher's dedicated scratch volume is honoured.
    scratch_parent = Path(args.scratch_dir) if args.scratch_dir else RESULT_ROOT
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch = scratch_parent / ("tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f"))
    scratch.mkdir(parents=True, exist_ok=True)
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
