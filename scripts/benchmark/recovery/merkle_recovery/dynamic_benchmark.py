"""End-to-end dynamic Merkle recovery benchmark path."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from contextlib import contextmanager
from typing import Any, Mapping, Sequence

from .db import execute, scalar
from .dynamic import (
    LocalisationTrace,
    LogicalRange,
    RangeItem,
    RangeSummary,
    enforce_candidate_summary_bound,
    compare_range_items,
    localise_bad_ranges,
)
from .dynamic_db import (
    apply_set_based_repairs,
    dynamic_storage_scan_snapshot,
    dynamic_tree_stats,
    dynamic_verify,
    exact_heap_fetch_plan,
    fetch_exact_healthy_rows,
    partition_roots,
    range_items,
    range_summaries,
)
from .config import (
    DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT,
    DYNAMIC_NATIVE_LAYOUT_VERSION,
    DYNAMIC_PHYSICAL_NODE_FANOUT,
)
from .metrics import Metrics, add_warning, finalize_metrics
from .repair import seq_scan_delta, seq_scan_snapshot


_NATIVE_PROFILE_RE = re.compile(
    r"NATIVE_MERKLE_PROFILE index_oid=(\d+) splits=(\d+) merges=(\d+)"
)


def _profile_log_offset() -> int:
    path = os.environ.get("ARIABC_NATIVE_PROFILE_LOG", "")
    if not path:
        return -1
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _profile_log_counts(offset: int, index_oids: Mapping[str, int]) -> dict[str, tuple[int, int]]:
    counts = {schema: [0, 0] for schema in index_oids}
    path = os.environ.get("ARIABC_NATIVE_PROFILE_LOG", "")
    if offset < 0 or not path:
        return {schema: (0, 0) for schema in index_oids}
    oid_to_schema = {oid: schema for schema, oid in index_oids.items()}
    try:
        with open(path, "rb") as profile_log:
            profile_log.seek(offset)
            text = profile_log.read().decode(errors="replace")
    except OSError:
        return {schema: (0, 0) for schema in index_oids}
    for oid_text, splits_text, merges_text in _NATIVE_PROFILE_RE.findall(text):
        schema = oid_to_schema.get(int(oid_text))
        if schema is not None:
            counts[schema][0] += int(splits_text)
            counts[schema][1] += int(merges_text)
    return {schema: (values[0], values[1]) for schema, values in counts.items()}
from .verification import schema_fidelity_checks

def _now_ms() -> float:
    return time.perf_counter() * 1000.0


@contextmanager
def _timer(store: dict[str, float], name: str):
    start = _now_ms()
    yield
    store[name] = store.get(name, 0.0) + _now_ms() - start


def dynamic_recovery_run_id(
    manifest: Mapping[str, Any], repetition: int, profile_label: str
) -> str:
    label = profile_label or "dynamic"
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label)
    return (
        f"{manifest['experiment']}-n{manifest['tuple_count']}"
        f"-p{manifest['partitions']}-lf{manifest['fanout']}"
        f"-pf{DYNAMIC_PHYSICAL_NODE_FANOUT}"
        f"-cap{manifest['leaf_capacity']}-merge{manifest['merge_threshold']}"
        f"-bad{len(manifest['bad_ranges'])}-c{len(manifest['corruptions'])}"
        f"-{safe}-merkle-dynamic-r{repetition}"
    )


def _canonical_stats(stats: Mapping[str, Any]) -> dict[str, Any]:
    def first(*names: str, default: Any = 0) -> Any:
        for name in names:
            if name in stats:
                return stats[name]
        return default

    state_value = first("state", "readiness_state", "status", default="")
    authority = str(first("authority", default=""))
    if not state_value and authority == "native_index_pages":
        # A successful native stats traversal validates every visible root and
        # page locator; native indexes do not own a compatibility state row.
        state_value = "READY"
    if not state_value and "ready" in stats:
        state_value = "READY" if bool(stats["ready"]) else "CATCHING_UP"
    return {
        "state": str(state_value).upper(),
        "tuple_count": int(first("tuple_count", "item_count", "total_items", default=0)),
        "max_leaf_occupancy": int(
            first("max_leaf_occupancy", "max_leaf_items", "maximum_leaf_count", default=-1)
        ),
        "max_depth": int(first("max_depth", "tree_depth", "maximum_depth", default=-1)),
        "logical_fanout": int(first("logical_fanout", default=-1)),
        "physical_node_fanout": int(first("physical_node_fanout", default=-1)),
        "authority": authority,
        "update_mode": str(first("update_mode", default="")),
        "layout_version": int(first("layout_version", default=-1)),
        "split_count": int(first("split_count", "splits", "total_splits", default=0)),
        "merge_count": int(first("merge_count", "merges", "total_merges", default=0)),
        "dynamic_bytes": int(
            first(
                "dynamic_bytes", "total_dynamic_bytes", "storage_bytes", "item_bytes",
                default=0,
            )
        ),
        "raw_stats": dict(stats),
    }


def _root_tuple_count(roots: Mapping[int, RangeSummary]) -> int:
    return sum(summary.tuple_count for summary in roots.values())


def _storage_scan_delta(before: Mapping[str, int], after: Mapping[str, int]) -> int:
    return sum(
        max(0, int(after.get(relation, 0)) - int(before.get(relation, 0)))
        for relation in set(before) | set(after)
    )


def _wal_checkpoint_snapshot(conn) -> dict[str, int]:
    snapshot: dict[str, int] = {}
    for view, prefix in (("pg_stat_wal", "wal"), ("pg_stat_bgwriter", "bgwriter")):
        if scalar(conn, "SELECT to_regclass(%s)", (f"pg_catalog.{view}",)) is None:
            continue
        rows = execute(conn, f"SELECT * FROM {view}")
        if not rows:
            continue
        for name, value in dict(rows[0]).items():
            if value is None or isinstance(value, bool):
                continue
            try:
                snapshot[f"{prefix}_{name}"] = int(value)
            except (TypeError, ValueError, OverflowError):
                continue
    return snapshot


def _snapshot_delta(
    before: Mapping[str, int], after: Mapping[str, int]
) -> dict[str, int]:
    return {
        f"post_repair_{name}_delta": int(after[name]) - int(before.get(name, 0))
        for name in after
        if name in before
    }


def _roots_equal(
    healthy: Mapping[int, RangeSummary], damaged: Mapping[int, RangeSummary]
) -> bool:
    partitions = set(healthy) | set(damaged)
    for partition_id in partitions:
        h = healthy.get(partition_id)
        d = damaged.get(partition_id)
        if h is None or d is None or h.signature != d.signature:
            return False
    return True


def _physical_dynamic_storage(conn) -> dict[str, int]:
    """Measure allocated Merkle storage, including side-table indexes."""
    shared_bytes = int(
        scalar(
            conn,
            """
            SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ariabc_internal'
              AND c.relkind IN ('r', 'p')
              AND c.relname IN (
                    'merkle_dynamic_state', 'merkle_dynamic_node',
                    'merkle_dynamic_leaf_item', 'merkle_dynamic_seen'
              )
            """,
        )
    )
    return {
        "shared_side_table_total_bytes": shared_bytes,
        "healthy_index_relation_bytes": int(
            scalar(
                conn,
                "SELECT pg_total_relation_size('healthy.usertable_merkle_idx'::regclass)",
            )
        ),
        "damaged_index_relation_bytes": int(
            scalar(
                conn,
                "SELECT pg_total_relation_size('damaged.usertable_merkle_idx'::regclass)",
            )
        ),
    }


def _localise(
    conn,
    leaf_capacity: int,
    logical_fanout: int,
) -> tuple[list[LogicalRange], LocalisationTrace]:
    if logical_fanout <= 1 or logical_fanout & (logical_fanout - 1):
        raise ValueError("configured dynamic logical fanout must be a power of two")
    trace = LocalisationTrace()
    healthy = partition_roots(conn, "healthy")
    damaged = partition_roots(conn, "damaged")

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        return range_summaries(conn, schema, ranges)

    bad = localise_bad_ranges(
        healthy,
        damaged,
        fetch,
        leaf_capacity=leaf_capacity,
        # The native tree remains physically binary. Recovery consumes one
        # configured logical level at a time (log2(fanout) route bits).
        logical_fanout=logical_fanout,
        trace=trace,
    )
    return bad, trace


def _validate_fanout_contract(
    stats_by_schema: Mapping[str, Mapping[str, Any]], logical_fanout: int
) -> None:
    """Bind manifest, authoritative index metadata, and localisation geometry."""
    if (logical_fanout < 2 or logical_fanout > 32 or
            logical_fanout & (logical_fanout - 1)):
        raise RuntimeError(
            "dynamic benchmark logical fanout must be one of 2,4,8,16,32; "
            f"got {logical_fanout}"
        )
    for schema, stats in stats_by_schema.items():
        if stats.get("authority") != "native_index_pages":
            raise RuntimeError(
                f"{schema} dynamic recovery authority is "
                f"{stats.get('authority')!r}, expected 'native_index_pages'"
            )
        stored_logical = int(stats.get("logical_fanout", -1))
        stored_physical = int(stats.get("physical_node_fanout", -1))
        stored_layout = int(stats.get("layout_version", -1))
        if stored_layout != DYNAMIC_NATIVE_LAYOUT_VERSION:
            raise RuntimeError(
                f"{schema} native layout mismatch: expected "
                f"{DYNAMIC_NATIVE_LAYOUT_VERSION}, index_stats={stored_layout}"
            )
        if stored_logical != logical_fanout:
            raise RuntimeError(
                f"{schema} dynamic fanout mismatch: manifest/localisation="
                f"{logical_fanout}, index_metadata={stored_logical}"
            )
        if stored_physical != DYNAMIC_PHYSICAL_NODE_FANOUT:
            raise RuntimeError(
                f"{schema} physical node fanout mismatch: expected "
                f"{DYNAMIC_PHYSICAL_NODE_FANOUT}, index_stats={stored_physical}"
            )


def _append_trace_rows(
    rows_out: list[dict[str, Any]],
    run_id: str,
    stage: str,
    trace: LocalisationTrace,
) -> None:
    for schema, summaries in (
        ("healthy", trace.healthy_summary_rows),
        ("damaged", trace.damaged_summary_rows),
    ):
        for ordinal, summary in enumerate(summaries):
            logical_range = summary.logical_range
            rows_out.append(
                {
                    "run_id": run_id,
                    "stage": stage,
                    "schema": schema,
                    "ordinal": ordinal,
                    "partition_id": logical_range.partition_id,
                    "prefix_length": logical_range.prefix_length,
                    "prefix_value": logical_range.prefix_hex,
                    "logical_range": logical_range.label,
                    "tuple_count": summary.tuple_count,
                    "data_xor": summary.data_xor.hex(),
                }
            )


def _append_item_rows(
    rows_out: list[dict[str, Any]],
    run_id: str,
    schema: str,
    items: Sequence[RangeItem],
    repair_operations: Mapping[int, str],
) -> None:
    for item in items:
        rows_out.append(
            {
                "run_id": run_id,
                "schema": schema,
                "logical_range": item.logical_range.label,
                "partition_id": item.logical_range.partition_id,
                "prefix_length": item.logical_range.prefix_length,
                "prefix_value": item.logical_range.prefix_hex,
                "ycsb_key": item.key,
                "route_digest": item.route_digest.hex(),
                "tuple_hash": item.tuple_hash.hex(),
                "repair_operation": repair_operations.get(item.key, "none"),
            }
        )


def _audit_dynamic(
    conn,
    run_id: str,
    leaf_capacity: int,
) -> dict[str, Any]:
    phase: dict[str, float] = {}
    with _timer(phase, "audit_exact_table_compare_ms"):
        healthy_table_count = int(scalar(conn, "SELECT count(*) FROM healthy.usertable"))
        damaged_table_count = int(scalar(conn, "SELECT count(*) FROM damaged.usertable"))
        healthy_minus_damaged = int(
            scalar(
                conn,
                """
                SELECT count(*) FROM (
                    SELECT * FROM healthy.usertable
                    EXCEPT ALL
                    SELECT * FROM damaged.usertable
                ) AS diff
                """,
            )
        )
        damaged_minus_healthy = int(
            scalar(
                conn,
                """
                SELECT count(*) FROM (
                    SELECT * FROM damaged.usertable
                    EXCEPT ALL
                    SELECT * FROM healthy.usertable
                ) AS diff
                """,
            )
        )
    with _timer(phase, "audit_dynamic_roots_ms"):
        healthy_roots = partition_roots(conn, "healthy")
        damaged_roots = partition_roots(conn, "damaged")
        roots_equal = _roots_equal(healthy_roots, damaged_roots)
        healthy_count = _root_tuple_count(healthy_roots)
        damaged_count = _root_tuple_count(damaged_roots)
    with _timer(phase, "audit_dynamic_verify_ms"):
        healthy_verify = dynamic_verify(conn, "healthy")
        damaged_verify = dynamic_verify(conn, "damaged")
    with _timer(phase, "audit_dynamic_stats_ms"):
        healthy_stats = _canonical_stats(dynamic_tree_stats(conn, "healthy"))
        damaged_stats = _canonical_stats(dynamic_tree_stats(conn, "damaged"))
    with _timer(phase, "audit_schema_fidelity_ms"):
        schema_rows = schema_fidelity_checks(
            conn, run_id, "merkle_dynamic", merkle_mode="dynamic"
        )
        schema_ok = all(int(row["match"]) == 1 for row in schema_rows)
    physical_storage = _physical_dynamic_storage(conn)
    index_count = int(
        scalar(
            conn,
            """
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = 'damaged'
              AND indexname IN ('usertable_pkey', 'usertable_merkle_idx')
            """,
        )
    )
    static_lookup_index_count = int(
        scalar(
            conn,
            """
            SELECT count(*) FROM pg_indexes
            WHERE schemaname IN ('healthy', 'damaged')
              AND (
                    indexname = 'usertable_leaf_lookup_idx'
                 OR indexdef LIKE '%merkle_bucket_for_key%'
              )
            """,
        )
    )
    ok = (
        healthy_minus_damaged == 0
        and damaged_minus_healthy == 0
        and roots_equal
        and healthy_count == damaged_count
        and healthy_count == healthy_table_count
        and damaged_count == damaged_table_count
        and healthy_verify
        and damaged_verify
        and healthy_stats["state"] == "READY"
        and damaged_stats["state"] == "READY"
        and 0 <= healthy_stats["max_leaf_occupancy"] <= leaf_capacity
        and 0 <= damaged_stats["max_leaf_occupancy"] <= leaf_capacity
        and schema_ok
        and index_count == 2
        and static_lookup_index_count == 0
    )
    return {
        "healthy_minus_damaged": healthy_minus_damaged,
        "damaged_minus_healthy": damaged_minus_healthy,
        "roots_match": roots_equal,
        "healthy_root_tuple_count": healthy_count,
        "damaged_root_tuple_count": damaged_count,
        "healthy_table_tuple_count": healthy_table_count,
        "damaged_table_tuple_count": damaged_table_count,
        "root_counts_match": (
            healthy_count == damaged_count
            and healthy_count == healthy_table_count
            and damaged_count == damaged_table_count
        ),
        "healthy_dynamic_verify": healthy_verify,
        "damaged_dynamic_verify": damaged_verify,
        "healthy_stats": healthy_stats,
        "damaged_stats": damaged_stats,
        "schema_fidelity_ok": schema_ok,
        "schema_fidelity_rows": schema_rows,
        "damaged_required_indexes": index_count,
        "static_lookup_index_count": static_lookup_index_count,
        "physical_storage": physical_storage,
        "audit_phase": phase,
        "audit_validation_ms": sum(phase.values()),
        "ok": ok,
    }


def _targeted_audit_dynamic(
    conn,
    run_id: str,
    leaf_capacity: int,
) -> dict[str, Any]:
    """Cheap post-repair proof for iteration; deliberately not full acceptance."""
    phase: dict[str, float] = {}
    with _timer(phase, "audit_dynamic_roots_ms"):
        healthy_roots = partition_roots(conn, "healthy")
        damaged_roots = partition_roots(conn, "damaged")
        roots_equal = _roots_equal(healthy_roots, damaged_roots)
        healthy_count = _root_tuple_count(healthy_roots)
        damaged_count = _root_tuple_count(damaged_roots)
    with _timer(phase, "audit_dynamic_stats_ms"):
        healthy_stats = _canonical_stats(dynamic_tree_stats(conn, "healthy"))
        damaged_stats = _canonical_stats(dynamic_tree_stats(conn, "damaged"))
    with _timer(phase, "audit_schema_fidelity_ms"):
        schema_rows = schema_fidelity_checks(
            conn, run_id, "merkle_dynamic", merkle_mode="dynamic"
        )
        schema_ok = all(int(row["match"]) == 1 for row in schema_rows)
    index_count = int(
        scalar(
            conn,
            """
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = 'damaged'
              AND indexname IN ('usertable_pkey', 'usertable_merkle_idx')
            """,
        )
    )
    static_lookup_index_count = int(
        scalar(
            conn,
            """
            SELECT count(*) FROM pg_indexes
            WHERE schemaname IN ('healthy', 'damaged')
              AND (
                    indexname = 'usertable_leaf_lookup_idx'
                 OR indexdef LIKE '%merkle_bucket_for_key%'
              )
            """,
        )
    )
    physical_storage = _physical_dynamic_storage(conn)
    ok = (
        roots_equal
        and healthy_count == damaged_count
        and healthy_stats["state"] == "READY"
        and damaged_stats["state"] == "READY"
        and 0 <= healthy_stats["max_leaf_occupancy"] <= leaf_capacity
        and 0 <= damaged_stats["max_leaf_occupancy"] <= leaf_capacity
        and schema_ok
        and index_count == 2
        and static_lookup_index_count == 0
    )
    return {
        # -1 means intentionally not measured by the targeted diagnostic audit.
        "healthy_minus_damaged": -1,
        "damaged_minus_healthy": -1,
        "roots_match": roots_equal,
        "healthy_root_tuple_count": healthy_count,
        "damaged_root_tuple_count": damaged_count,
        "healthy_table_tuple_count": -1,
        "damaged_table_tuple_count": -1,
        "root_counts_match": healthy_count == damaged_count,
        "healthy_dynamic_verify": -1,
        "damaged_dynamic_verify": -1,
        "healthy_stats": healthy_stats,
        "damaged_stats": damaged_stats,
        "schema_fidelity_ok": schema_ok,
        "schema_fidelity_rows": schema_rows,
        "damaged_required_indexes": index_count,
        "static_lookup_index_count": static_lookup_index_count,
        "physical_storage": physical_storage,
        "audit_phase": phase,
        "audit_validation_ms": sum(phase.values()),
        "ok": ok,
    }


def repair_dynamic_merkle(
    conn,
    manifest: dict[str, Any],
    repetition: int,
    schema_rows_out: list[dict[str, Any]],
    range_rows_out: list[dict[str, Any]],
    item_rows_out: list[dict[str, Any]],
    heap_rows_out: list[dict[str, Any]],
    tree_stats_rows_out: list[dict[str, Any]],
    planner_rows_out: list[dict[str, Any]],
    *,
    build_stats: dict[str, dict[str, Any]],
    profile_label: str,
    audit_mode: str,
) -> Metrics:
    if audit_mode not in ("full", "skip"):
        raise ValueError(f"unsupported dynamic audit mode: {audit_mode}")
    leaf_capacity = int(manifest["leaf_capacity"])
    logical_fanout = int(manifest["fanout"])
    run_id = dynamic_recovery_run_id(manifest, repetition, profile_label)
    m = Metrics(
        run_id=run_id,
        experiment=str(manifest["experiment"]),
        method="merkle_dynamic",
        tuple_count=int(manifest["tuple_count"]),
        partitions=int(manifest["partitions"]),
        leaves_per_partition=0,
        fanout=int(manifest["fanout"]),
        bad_leaf_count=len(manifest["bad_ranges"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        corruption_mode=str(manifest.get("corruption_mode", "paper-update-only")),
        profile_label=profile_label,
    )
    m.counters["manifest_sha256"] = __import__("hashlib").sha256(
        json.dumps(manifest, sort_keys=True, default=str).encode()
    ).hexdigest()
    m.counters["merkle_mode"] = "dynamic"

    # Dataset/index state needed to establish the recovery contract is setup,
    # not recovery work.  In particular, dynamic_tree_stats() walks native
    # index metadata and naturally grows with table size.  Keep it visible as
    # setup telemetry, but start the recovery stopwatch only after it is done.
    total_start = _now_ms()
    recovery_scan_before = seq_scan_snapshot(conn)
    before_stats = {
        schema: _canonical_stats(dynamic_tree_stats(conn, schema))
        for schema in ("healthy", "damaged")
    }
    _validate_fanout_contract(before_stats, logical_fanout)
    dynamic_scan_before = dynamic_storage_scan_snapshot(conn)
    paper_start = _now_ms()
    m.phase["pre_recovery_setup_ms"] = paper_start - total_start

    with _timer(m.phase, "tree_localisation_ms"):
        bad_ranges, trace = _localise(conn, leaf_capacity, logical_fanout)
    _append_trace_rows(range_rows_out, run_id, "initial", trace)

    with _timer(m.phase, "candidate_summary_fetch_ms"):
        healthy_items = range_items(conn, "healthy", bad_ranges)
        damaged_items = range_items(conn, "damaged", bad_ranges)
    for schema, items in (("healthy", healthy_items), ("damaged", damaged_items)):
        counts: dict[LogicalRange, int] = {}
        for item in items:
            counts[item.logical_range] = counts.get(item.logical_range, 0) + 1
        oversized = {
            logical_range.label: count
            for logical_range, count in counts.items()
            if count > leaf_capacity
        }
        if oversized:
            raise RuntimeError(f"{schema} bounded dynamic range overflow: {oversized}")

    candidate_summary_limit = enforce_candidate_summary_bound(
        len(healthy_items),
        len(damaged_items),
        bad_range_count=len(manifest["bad_ranges"]),
        leaf_capacity=leaf_capacity,
    )
    summary_items = len(healthy_items) + len(damaged_items)
    if candidate_summary_limit != DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT:
        raise RuntimeError(
            "dynamic candidate-summary contract drift: "
            f"computed_limit={candidate_summary_limit}, "
            f"acceptance_limit={DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT}"
        )

    with _timer(m.phase, "summary_comparison_ms"):
        repairs = compare_range_items(healthy_items, damaged_items)
    operations = {
        **{key: "insert" for key in repairs.inserts},
        **{key: "update" for key in repairs.updates},
        **{key: "delete" for key in repairs.deletes},
    }
    _append_item_rows(item_rows_out, run_id, "healthy", healthy_items, operations)
    _append_item_rows(item_rows_out, run_id, "damaged", damaged_items, operations)

    with _timer(m.phase, "exact_heap_fetch_ms"):
        plan = exact_heap_fetch_plan(conn, repairs.healthy_heap_keys)
        plan.update({"run_id": run_id, "schema": "healthy"})
        planner_rows_out.append(plan)
        healthy_rows = fetch_exact_healthy_rows(conn, repairs.healthy_heap_keys)
    for row in healthy_rows:
        key = int(row["ycsb_key"])
        heap_rows_out.append(
            {
                "run_id": run_id,
                "schema": "healthy",
                "ycsb_key": key,
                "repair_operation": operations[key],
                "full_row_json": json.dumps(dict(row), sort_keys=True, default=str),
            }
        )

    with _timer(m.phase, "repair_write_ms"):
        repair_result = apply_set_based_repairs(conn, repairs, healthy_rows)

    # Native synchronous-COW publication is visible immediately after the
    # repair commit.  Prove that boundary before touching the compatibility
    # Native v8 recovery has no pending queue to drain.
    with _timer(m.phase, "native_commit_visibility_ms"):
        native_remaining_bad_ranges, native_confirmation_trace = _localise(
            conn, leaf_capacity, logical_fanout
        )
    _append_trace_rows(
        range_rows_out, run_id, "native_commit_visibility", native_confirmation_trace
    )
    if native_remaining_bad_ranges:
        raise RuntimeError(
            "native synchronous-COW roots were not current at commit: "
            f"remaining_ranges={len(native_remaining_bad_ranges)}"
        )
    recovery_end = _now_ms()
    paper_end = recovery_end

    recovery_scan_after = seq_scan_snapshot(conn)
    recovery_seq_scans = seq_scan_delta(recovery_scan_before, recovery_scan_after)
    dynamic_scan_after = dynamic_storage_scan_snapshot(conn)
    dynamic_storage_seq_scans = _storage_scan_delta(
        dynamic_scan_before, dynamic_scan_after
    )

    post_repair_io_before = _wal_checkpoint_snapshot(conn)
    post_repair_io_after = _wal_checkpoint_snapshot(conn)
    with _timer(m.phase, "post_commit_relocalisation_ms"):
        remaining_bad_ranges, post_queue_trace = _localise(
            conn, leaf_capacity, logical_fanout
        )
    _append_trace_rows(
        range_rows_out, run_id, "post_commit_relocalisation", post_queue_trace
    )
    if remaining_bad_ranges:
        raise RuntimeError(
            "native roots changed or remain divergent after compatibility queue drain: "
            f"remaining_ranges={len(remaining_bad_ranges)}"
        )

    audit_scan_before = seq_scan_snapshot(conn)
    audit_start = _now_ms()
    audit = (
        _audit_dynamic(conn, run_id, leaf_capacity)
        if audit_mode == "full"
        else _targeted_audit_dynamic(conn, run_id, leaf_capacity)
    )
    final_stats_by_schema = {
        schema: audit[f"{schema}_stats"] for schema in ("healthy", "damaged")
    }
    _validate_fanout_contract(final_stats_by_schema, logical_fanout)
    native_api_proofs: list[dict[str, Any]] = []
    for schema in ("healthy", "damaged"):
        stats = final_stats_by_schema[schema]
        detail = json.dumps(stats["raw_stats"], sort_keys=True, default=str)
        proof = {
            "run_id": run_id,
            "schema": schema,
            "operation": "native_dynamic_api_authority",
            "requested_key_count": len(bad_ranges),
            "ordinal": 0,
            "logical_range": "all_localised_ranges",
            "expected_index": "native_index_pages",
            "index_used": int(stats["authority"] == "native_index_pages"),
            "rows_examined": 0,
            "shared_hit_blocks": 0,
            "shared_read_blocks": 0,
            "plan_json_sha256": hashlib.sha256(detail.encode()).hexdigest(),
            "plan_json": detail,
        }
        native_api_proofs.append(proof)
        planner_rows_out.append(proof)
    audit_end = _now_ms()
    audit_scan_after = seq_scan_snapshot(conn)
    audit_seq_scans = seq_scan_delta(audit_scan_before, audit_scan_after)
    schema_rows_out.extend(audit["schema_fidelity_rows"])
    # Audit is optional and must never pollute the recovery phase report.  The
    # recovery boundary ends before this block; retain audit timings only for
    # an explicitly requested full audit.
    if audit_mode == "full":
        m.phase.update(audit["audit_phase"])

    for schema in ("healthy", "damaged"):
        tree_stats_rows_out.append(
            {
                "run_id": run_id,
                "schema": schema,
                "stage": "index_build",
                **build_stats[schema],
                "raw_stats": json.dumps(build_stats[schema]["raw_stats"], sort_keys=True, default=str),
            }
        )
        tree_stats_rows_out.append(
            {
                "run_id": run_id,
                "schema": schema,
                "stage": "recovery_execution_pre_recovery",
                **before_stats[schema],
                "split_count": 0,
                "merge_count": 0,
                "raw_stats": json.dumps(before_stats[schema]["raw_stats"], sort_keys=True, default=str),
            }
        )
        final_stats = audit[f"{schema}_stats"]
        tree_stats_rows_out.append(
            {
                "run_id": run_id,
                "schema": schema,
                "stage": "recovery_execution_post_audit",
                **final_stats,
                "split_count": final_stats["split_count"] - before_stats[schema]["split_count"],
                "merge_count": final_stats["merge_count"] - before_stats[schema]["merge_count"],
                "raw_stats": json.dumps(final_stats["raw_stats"], sort_keys=True, default=str),
            }
        )

    summary_items = len(healthy_items) + len(damaged_items)
    candidate_summary_bytes = sum(
        item.encoded_bytes for item in (*healthy_items, *damaged_items)
    )
    exact_heap_payload_bytes = sum(
        len(json.dumps(dict(row), sort_keys=True, default=str).encode("utf-8"))
        for row in healthy_rows
    )
    total_repairs = repair_result.total
    healthy_stats = audit["healthy_stats"]
    damaged_stats = audit["damaged_stats"]
    execution_split_counts = {
        schema: audit[f"{schema}_stats"]["split_count"] - before_stats[schema]["split_count"]
        for schema in ("healthy", "damaged")
    }
    execution_merge_counts = {
        schema: audit[f"{schema}_stats"]["merge_count"] - before_stats[schema]["merge_count"]
        for schema in ("healthy", "damaged")
    }
    if any(value < 0 for value in (*execution_split_counts.values(), *execution_merge_counts.values())):
        raise RuntimeError("native dynamic split/merge counters moved backwards during recovery")
    m.counters.update(
        {
            "partition_root_batches": 2,
            "partition_root_batches_ok": 1,
            "targeted_confirmation_root_batches": 2,
            "bad_partition_count": trace.bad_partitions,
            "bad_range_count": len(bad_ranges),
            "localised_bad_range_count": len(bad_ranges),
            "configured_leaf_capacity": leaf_capacity,
            "dynamic_layout_version": healthy_stats["layout_version"],
            "logical_localisation_fanout": logical_fanout,
            "physical_node_fanout": DYNAMIC_PHYSICAL_NODE_FANOUT,
            "logical_ranges_compared": trace.logical_ranges_compared,
            "range_summary_rows_read": trace.range_summary_rows,
            "localisation_levels_visited": trace.levels_visited,
            "healthy_candidate_summary_items_fetched": len(healthy_items),
            "damaged_candidate_summary_items_fetched": len(damaged_items),
            "dynamic_candidate_summary_items_fetched": summary_items,
            "dynamic_candidate_summary_item_limit": candidate_summary_limit,
            "candidate_summary_bound_ok": int(summary_items <= candidate_summary_limit),
            "candidate_summary_bytes": candidate_summary_bytes,
            "exact_healthy_heap_rows_fetched": len(healthy_rows),
            "exact_heap_payload_bytes": exact_heap_payload_bytes,
            "full_damaged_heap_rows_fetched": 0,
            "rows_inserted": repair_result.rows_inserted,
            "rows_updated": repair_result.rows_updated,
            "rows_deleted": repair_result.rows_deleted,
            "total_rows_repaired": total_repairs,
            "remaining_bad_range_count": len(remaining_bad_ranges),
            "native_remaining_bad_range_count": len(native_remaining_bad_ranges),
            "native_roots_match_after_commit": int(not native_remaining_bad_ranges),
            "native_roots_unchanged_after_commit": int(
                not remaining_bad_ranges
            ),
            "planner_checks_passed": int(
                bool(plan["index_used"])
                and all(bool(row["index_used"]) for row in native_api_proofs)
            ),
            "dynamic_native_api_check_count": len(native_api_proofs),
            "dynamic_native_api_authority_failures": sum(
                1 for row in native_api_proofs if not bool(row["index_used"])
            ),
            "dynamic_storage_seq_scan_delta": dynamic_storage_seq_scans,
            "recovery_user_table_seq_scan_delta": recovery_seq_scans,
            "audit_mode": audit_mode,
            "full_audit_skipped": int(audit_mode == "skip"),
            "audit_user_table_seq_scan_delta": audit_seq_scans,
            "audit_validation_ms": audit["audit_validation_ms"] if audit_mode == "full" else 0.0,
            "schema_fidelity_ok": int(audit["schema_fidelity_ok"]),
            "static_lookup_index_count": audit["static_lookup_index_count"],
            "healthy_minus_damaged": audit["healthy_minus_damaged"],
            "damaged_minus_healthy": audit["damaged_minus_healthy"],
            "roots_match": int(audit["roots_match"]),
            "logical_root_signatures_match": int(audit["roots_match"]),
            "root_counts_match": int(audit["root_counts_match"]),
            "healthy_root_tuple_count": audit["healthy_root_tuple_count"],
            "damaged_root_tuple_count": audit["damaged_root_tuple_count"],
            "healthy_table_tuple_count": audit["healthy_table_tuple_count"],
            "damaged_table_tuple_count": audit["damaged_table_tuple_count"],
            "healthy_dynamic_verify": int(audit["healthy_dynamic_verify"]),
            "damaged_dynamic_verify": int(audit["damaged_dynamic_verify"]),
            "healthy_dynamic_state": healthy_stats["state"],
            "damaged_dynamic_state": damaged_stats["state"],
            "healthy_max_leaf_occupancy": healthy_stats["max_leaf_occupancy"],
            "damaged_max_leaf_occupancy": damaged_stats["max_leaf_occupancy"],
            "healthy_max_depth": healthy_stats["max_depth"],
            "damaged_max_depth": damaged_stats["max_depth"],
            "healthy_split_count": execution_split_counts["healthy"],
            "damaged_split_count": execution_split_counts["damaged"],
            "healthy_merge_count": execution_merge_counts["healthy"],
            "damaged_merge_count": execution_merge_counts["damaged"],
            "index_build_healthy_split_count": build_stats["healthy"]["split_count"],
            "index_build_damaged_split_count": build_stats["damaged"]["split_count"],
            "index_build_healthy_merge_count": build_stats["healthy"]["merge_count"],
            "index_build_damaged_merge_count": build_stats["damaged"]["merge_count"],
            "recovery_execution_healthy_split_count": execution_split_counts["healthy"],
            "recovery_execution_damaged_split_count": execution_split_counts["damaged"],
            "recovery_execution_healthy_merge_count": execution_merge_counts["healthy"],
            "recovery_execution_damaged_merge_count": execution_merge_counts["damaged"],
            "healthy_dynamic_bytes": healthy_stats["dynamic_bytes"],
            "damaged_dynamic_bytes": damaged_stats["dynamic_bytes"],
            "healthy_logical_item_bytes": healthy_stats["dynamic_bytes"],
            "damaged_logical_item_bytes": damaged_stats["dynamic_bytes"],
            **audit["physical_storage"],
            **_snapshot_delta(post_repair_io_before, post_repair_io_after),
        }
    )

    acceptance_failures: list[str] = []
    checks = [
        (audit["roots_match"], "dynamic roots differ"),
        (audit["root_counts_match"], "dynamic root counts differ"),
        (not remaining_bad_ranges, "post-repair logical ranges still differ"),
        (healthy_stats["state"] == "READY", "healthy dynamic state is not READY"),
        (damaged_stats["state"] == "READY", "damaged dynamic state is not READY"),
        (
            0 <= healthy_stats["max_leaf_occupancy"] <= leaf_capacity,
            "healthy max leaf occupancy exceeds capacity",
        ),
        (
            0 <= damaged_stats["max_leaf_occupancy"] <= leaf_capacity,
            "damaged max leaf occupancy exceeds capacity",
        ),
        (total_repairs == len(manifest["corruptions"]), "repair count differs from manifest"),
        (recovery_seq_scans == 0, "timed recovery performed user-table sequential scan"),
        (
            dynamic_storage_seq_scans == 0,
            "timed recovery performed dynamic side-table sequential scan",
        ),
        (bool(plan["index_used"]), "exact healthy heap fetch did not use primary index"),
        (
            all(bool(row["index_used"]) for row in native_api_proofs),
            "native dynamic API authority proof failed",
        ),
        (audit["static_lookup_index_count"] == 0, "static bucket lookup index exists"),
        (audit["schema_fidelity_ok"], "dynamic schema fidelity failed"),
        (audit["ok"], "dynamic audit failed"),
    ]
    if audit_mode == "full":
        checks.extend(
            [
                (audit["healthy_minus_damaged"] == 0, "healthy EXCEPT damaged is nonzero"),
                (audit["damaged_minus_healthy"] == 0, "damaged EXCEPT healthy is nonzero"),
                (audit["healthy_dynamic_verify"], "healthy dynamic verify failed"),
                (audit["damaged_dynamic_verify"], "damaged dynamic verify failed"),
            ]
        )
    if manifest.get("corruption_mode") in ("paper-update-only", "update-only"):
        checks.extend(
            [
                (total_repairs == 300, "dynamic acceptance requires exactly 300 repairs"),
                (
                    summary_items <= DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT,
                    "dynamic candidate summary items exceed 4800",
                ),
                (len(healthy_rows) == len(repairs.updates), "update-only heap rows are not exact"),
            ]
        )
    for ok, reason in checks:
        if not ok:
            acceptance_failures.append(reason)
    if acceptance_failures:
        add_warning(m, "; ".join(acceptance_failures))

    cleanup_end = _now_ms()
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
    m.phase["merkle_total_ms"] = (
        m.end_to_end_observed_ms if audit_mode == "full" else m.restore_repair_ms
    )
    return m


def run_one_dynamic_manifest(
    conn,
    manifest: dict[str, Any],
    repetitions: int,
    schema_rows_out: list[dict[str, Any]],
    range_rows_out: list[dict[str, Any]],
    item_rows_out: list[dict[str, Any]],
    heap_rows_out: list[dict[str, Any]],
    tree_stats_rows_out: list[dict[str, Any]],
    planner_rows_out: list[dict[str, Any]],
    result_dir,
    progress_state: dict[str, Any],
    *,
    profile_label: str,
    audit_mode: str,
) -> list[Metrics]:
    """Corrupt, catch up, recover, and audit one dynamic dataset repeatedly."""
    from .manifest import apply_corruption
    from .reporting import emit_progress

    metrics: list[Metrics] = []
    build_stats = {
        schema: _canonical_stats(dynamic_tree_stats(conn, schema))
        for schema in ("healthy", "damaged")
    }
    index_oids = {
        schema: int(scalar(conn, f"SELECT '{schema}.usertable_merkle_idx'::regclass::oid"))
        for schema in ("healthy", "damaged")
    }
    for repetition in range(repetitions):
        run_id = dynamic_recovery_run_id(manifest, repetition, profile_label)
        emit_progress(
            result_dir,
            event="method_start",
            run_id=run_id,
            experiment=manifest["experiment"],
            profile_label=profile_label,
            method="merkle_dynamic",
            merkle_mode="dynamic",
            corruption_mode=manifest.get("corruption_mode", "paper-update-only"),
            tuple_count=manifest["tuple_count"],
            partitions=manifest["partitions"],
            bad_range_count=len(manifest["bad_ranges"]),
            repetition=repetition,
            completed_runs=progress_state["completed_runs"],
            total_runs=progress_state["total_runs"],
        )
        started = _now_ms()
        apply_corruption(conn, manifest)
        # Native synchronous-COW commits publish the damaged dynamic root
        # before the transaction becomes visible to the recovery query.
        if audit_mode == "skip":
            # Fast diagnostics use one repetition, whereas the published
            # static medians came from repeated recovery on one built dataset.
            # Warm the exact sparse read set once outside paper timing so the
            # single diagnostic sample has the same cache interpretation.
            warm_ranges, _warm_trace = _localise(
                conn,
                int(manifest["leaf_capacity"]),
                int(manifest["fanout"]),
            )
            warm_healthy = range_items(conn, "healthy", warm_ranges)
            warm_damaged = range_items(conn, "damaged", warm_ranges)
            warm_repairs = compare_range_items(warm_healthy, warm_damaged)
            fetch_exact_healthy_rows(conn, warm_repairs.healthy_heap_keys)
        profile_log_offset = _profile_log_offset()
        metric = repair_dynamic_merkle(
            conn,
            manifest,
            repetition,
            schema_rows_out,
            range_rows_out,
            item_rows_out,
            heap_rows_out,
            tree_stats_rows_out,
            planner_rows_out,
            build_stats=build_stats,
            profile_label=profile_label,
            audit_mode=audit_mode,
        )
        if profile_log_offset >= 0:
            log_counts = _profile_log_counts(profile_log_offset, index_oids)
            for schema in ("healthy", "damaged"):
                splits, merges = log_counts[schema]
                metric.counters[f"{schema}_split_count"] = splits
                metric.counters[f"{schema}_merge_count"] = merges
                metric.counters[f"recovery_execution_{schema}_split_count"] = splits
                metric.counters[f"recovery_execution_{schema}_merge_count"] = merges
                for row in tree_stats_rows_out:
                    if (row.get("run_id") == run_id and row.get("schema") == schema and
                            row.get("stage") == "recovery_execution_post_audit"):
                        row["split_count"] = splits
                        row["merge_count"] = merges
        metrics.append(metric)
        progress_state["completed_runs"] += 1
        emit_progress(
            result_dir,
            event="method_complete",
            run_id=run_id,
            experiment=metric.experiment,
            profile_label=profile_label,
            method=metric.method,
            merkle_mode="dynamic",
            tuple_count=metric.tuple_count,
            partitions=metric.partitions,
            bad_range_count=metric.counters.get("bad_range_count", 0),
            repetition=repetition,
            valid=metric.valid,
            warning=metric.warning,
            method_elapsed_ms=round(_now_ms() - started, 3),
            completed_runs=progress_state["completed_runs"],
            total_runs=progress_state["total_runs"],
        )
    return metrics
