"""SQL adapter and set-based repair operations for dynamic Merkle recovery.

All backend function names and row-shape compatibility handling live here so
the benchmark algorithm does not duplicate SQL contracts.  The canonical API
is intentionally batch-oriented:

``merkle_dynamic_get_partition_roots(index regclass)``
    ``partition_id, tuple_count, data_xor``

``merkle_dynamic_get_ranges(index regclass, requests jsonb)``
    One aggregate summary for every requested logical prefix.

``merkle_dynamic_get_leaf_frontier(index regclass)``
    The current physical leaves, used only for deterministic corruption
    selection and build-conservation checks outside timed recovery.

``merkle_dynamic_get_range_items(index regclass, requests jsonb)``
    Bounded ``key, route_digest, tuple_hash`` rows under the requested ranges.

``merkle_dynamic_tree_stats(index regclass)`` and
``merkle_dynamic_verify(index regclass)``
    Observability and structural validation.

Requests encode ``prefix_value`` as a canonical, MSB-aligned 64-character hex
string.  The backend may return bytea or the same hex form.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .config import ALL_COLUMNS, FIELDS
from .db import execute, scalar
from .dynamic import (
    LogicalRange,
    RangeItem,
    RangeSummary,
    RepairKeys,
    digest_bytes,
)


DYNAMIC_API_FUNCTIONS = (
    "merkle_dynamic_get_partition_roots",
    "merkle_dynamic_get_ranges",
    "merkle_dynamic_get_range_items",
    "merkle_dynamic_get_leaf_frontier",
    "merkle_dynamic_tree_stats",
    "merkle_dynamic_verify",
)


def ensure_dynamic_api(conn) -> None:
    missing = execute(
        conn,
        """
        SELECT wanted.name
        FROM unnest(%s::text[]) AS wanted(name)
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_proc p WHERE p.proname = wanted.name
        )
        ORDER BY wanted.name
        """,
        (list(DYNAMIC_API_FUNCTIONS),),
    )
    if missing:
        names = ", ".join(str(row["name"]) for row in missing)
        raise RuntimeError(
            f"Missing built-in dynamic Merkle SQL functions: {names}. "
            "Use a cluster initialized from the dynamic-Merkle build."
        )
    missing_relations = execute(
        conn,
        """
        SELECT wanted.name
        FROM unnest(%s::text[]) AS wanted(name)
        WHERE to_regclass('ariabc_internal.' || wanted.name) IS NULL
        ORDER BY wanted.name
        """,
        ([
            "merkle_dynamic_state",
            "merkle_dynamic_node",
            "merkle_dynamic_leaf_item",
            "merkle_dynamic_build_stage",
            "merkle_dynamic_seen",
        ],),
    )
    if missing_relations:
        names = ", ".join(str(row["name"]) for row in missing_relations)
        raise RuntimeError(
            f"Missing dynamic Merkle storage relations: {names}. Run the "
            "current raft_apply_ledger_schema.sql bootstrap before the benchmark."
        )


def _field(row: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return default


def _prefix_bytes(value: Any, prefix_length: int) -> bytes:
    """Normalize bytea/hex/bit-string prefix representations."""
    if isinstance(value, str):
        text = value.strip()
        if text and set(text) <= {"0", "1"} and len(text) == prefix_length:
            prefix_value = int(text, 2) if text else 0
            return (prefix_value << (256 - prefix_length)).to_bytes(32, "big")
    return digest_bytes(value)


def logical_range_from_row(row: Mapping[str, Any]) -> LogicalRange:
    partition_id = int(_field(row, "partition_id", "partition"))
    prefix_length = int(_field(row, "prefix_length", "prefix_len", default=0))
    prefix = _field(row, "prefix_value", "prefix", "prefix_bits", "logical_prefix")
    if prefix is None:
        if prefix_length != 0:
            raise RuntimeError("dynamic API omitted prefix_value for a non-root range")
        prefix = bytes(32)
    return LogicalRange.from_prefix_bytes(
        partition_id, prefix_length, _prefix_bytes(prefix, prefix_length)
    )


def summary_from_row(row: Mapping[str, Any]) -> RangeSummary:
    logical_range = logical_range_from_row(row)
    count = int(_field(row, "tuple_count", "item_count", "count", default=0))
    data_xor = _field(row, "data_xor", "subtree_hash", "hash")
    return RangeSummary(logical_range, count, digest_bytes(data_xor))


def _requests_json(ranges: Sequence[LogicalRange]) -> str:
    return json.dumps([logical_range.to_request() for logical_range in ranges])


def partition_roots(conn, schema: str) -> dict[int, RangeSummary]:
    rows = execute(
        conn,
        f"SELECT * FROM merkle_dynamic_get_partition_roots("
        f"'{schema}.usertable_merkle_idx'::regclass)",
    )
    result: dict[int, RangeSummary] = {}
    for row in rows:
        # Root APIs need not repeat prefix columns.
        normalized = dict(row)
        normalized.setdefault("prefix_length", 0)
        normalized.setdefault("prefix_value", bytes(32))
        summary = summary_from_row(normalized)
        if summary.logical_range.partition_id in result:
            raise RuntimeError("dynamic partition-root API returned a duplicate partition")
        result[summary.logical_range.partition_id] = summary
    return result


def range_summaries(
    conn,
    schema: str,
    ranges: Sequence[LogicalRange],
) -> dict[LogicalRange, RangeSummary]:
    if not ranges:
        return {}
    rows = execute(
        conn,
        f"SELECT * FROM merkle_dynamic_get_ranges("
        f"'{schema}.usertable_merkle_idx'::regclass, %s::jsonb)",
        (_requests_json(ranges),),
    )
    result: dict[LogicalRange, RangeSummary] = {}
    for row in rows:
        summary = summary_from_row(row)
        if summary.logical_range in result:
            raise RuntimeError(
                f"dynamic range API returned duplicate {summary.logical_range.label}"
            )
        result[summary.logical_range] = summary
    return result


def physical_leaf_summaries(conn, schema: str) -> list[RangeSummary]:
    """Return the current leaf frontier for deterministic corruption selection."""
    rows = execute(
        conn,
        f"SELECT * FROM merkle_dynamic_get_leaf_frontier("
        f"'{schema}.usertable_merkle_idx'::regclass)",
    )
    result: list[RangeSummary] = []
    for row in rows:
        is_leaf = _field(row, "is_leaf", "leaf", default=True)
        if not bool(is_leaf):
            continue
        result.append(summary_from_row(row))
    return sorted(result, key=lambda item: item.logical_range)


def range_items(
    conn,
    schema: str,
    ranges: Sequence[LogicalRange],
) -> list[RangeItem]:
    if not ranges:
        return []
    rows = execute(
        conn,
        f"SELECT * FROM merkle_dynamic_get_range_items("
        f"'{schema}.usertable_merkle_idx'::regclass, %s::jsonb)",
        (_requests_json(ranges),),
    )
    by_partition: dict[int, list[LogicalRange]] = {}
    for logical_range in ranges:
        by_partition.setdefault(logical_range.partition_id, []).append(logical_range)

    result: list[RangeItem] = []
    for row in rows:
        partition_id = int(_field(row, "partition_id", "partition"))
        route_digest = digest_bytes(_field(row, "route_digest", "route_hash"))
        candidates = [
            logical_range
            for logical_range in by_partition.get(partition_id, [])
            if logical_range.contains_digest(route_digest)
        ]
        if len(candidates) != 1:
            raise RuntimeError(
                "dynamic range-item API returned an item outside the requested "
                "non-overlapping logical ranges"
            )
        key_value = _field(row, "ycsb_key", "key_text", "canonical_key", "key")
        if key_value is None:
            raise RuntimeError(
                "dynamic range-item key_text is NULL; the YCSB recovery harness "
                "requires a canonical single-column bigint key"
            )
        key = int(key_value)
        tuple_hash = digest_bytes(_field(row, "tuple_hash", "item_hash"))
        key_data = _field(row, "key_data", "canonical_key")
        if isinstance(key_data, memoryview):
            key_data = key_data.tobytes()
        if not isinstance(key_data, (bytes, bytearray)):
            raise RuntimeError("dynamic range-item API omitted canonical key_data")
        result.append(
            RangeItem(
                candidates[0],
                key,
                route_digest,
                tuple_hash,
                encoded_bytes=len(key_data) + len(route_digest) + len(tuple_hash),
            )
        )
    return result


def dynamic_tree_stats(conn, schema: str) -> dict[str, Any]:
    rows = execute(
        conn,
        f"SELECT merkle_dynamic_tree_stats("
        f"'{schema}.usertable_merkle_idx'::regclass) AS stats",
    )
    if not rows:
        raise RuntimeError("dynamic tree stats API returned no rows")
    value = rows[0].get("stats")
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, Mapping):
        return dict(value)
    # Permit a set-returning/record implementation without weakening callers.
    if len(rows) == 1:
        return dict(rows[0])
    raise RuntimeError("dynamic tree stats API returned an unsupported shape")


def dynamic_verify(conn, schema: str) -> bool:
    return bool(
        scalar(
            conn,
            f"SELECT merkle_dynamic_verify("
            f"'{schema}.usertable_merkle_idx'::regclass)",
        )
    )


def dynamic_apply_pending(conn) -> None:
    execute(conn, "SELECT merkle_apply_pending()")


def _plan_uses_index(node: Any, expected: str) -> bool:
    if isinstance(node, Mapping):
        if node.get("Index Name") == expected:
            return True
        return any(_plan_uses_index(value, expected) for value in node.values())
    if isinstance(node, list):
        return any(_plan_uses_index(value, expected) for value in node)
    return False


def _plan_uses_any_index(node: Any, expected: Sequence[str]) -> bool:
    return any(_plan_uses_index(node, index_name) for index_name in expected)


def _plan_metric(node: Any, name: str) -> int:
    if isinstance(node, Mapping):
        if name in node:
            return int(node.get(name, 0) or 0)
        return sum(_plan_metric(value, name) for value in node.values())
    if isinstance(node, list):
        return sum(_plan_metric(value, name) for value in node)
    return 0


def _relation_rows_examined(node: Any, relation: str) -> int:
    if isinstance(node, Mapping):
        own = 0
        if node.get("Relation Name") == relation:
            loops = int(node.get("Actual Loops", 1) or 1)
            own = loops * (
                int(node.get("Actual Rows", 0) or 0)
                + int(node.get("Rows Removed by Filter", 0) or 0)
                + int(node.get("Rows Removed by Index Recheck", 0) or 0)
            )
        return own + sum(
            _relation_rows_examined(value, relation) for value in node.values()
        )
    if isinstance(node, list):
        return sum(_relation_rows_examined(value, relation) for value in node)
    return 0


def _explain_document(rows: Sequence[Mapping[str, Any]]) -> tuple[Any, str]:
    if not rows:
        raise RuntimeError("dynamic side-table EXPLAIN returned no plan")
    plan = rows[0].get("QUERY PLAN")
    if isinstance(plan, str):
        plan = json.loads(plan)
    detail = json.dumps(plan, sort_keys=True, default=str)
    return plan, detail


def _generation_identity(conn, schema: str) -> tuple[int, int, int, int]:
    rows = execute(
        conn,
        f"""
        SELECT s.index_oid::bigint AS index_oid,
               s.rnode_spc::bigint AS rnode_spc,
               s.rnode_db::bigint AS rnode_db,
               s.rnode_rel::bigint AS rnode_rel
        FROM ariabc_internal.merkle_dynamic_state AS s
        WHERE s.index_oid = '{schema}.usertable_merkle_idx'::regclass
        """,
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"{schema} dynamic Merkle generation identity is not unique"
        )
    row = rows[0]
    return tuple(
        int(row[name]) for name in ("index_oid", "rnode_spc", "rnode_db", "rnode_rel")
    )


def _range_upper_bound(logical_range: LogicalRange) -> bytes | None:
    if logical_range.prefix_length == 0:
        return None
    step = 1 << (256 - logical_range.prefix_length)
    value = int.from_bytes(logical_range.prefix_bytes, "big") + step
    if value >= 1 << 256:
        return None
    return value.to_bytes(32, "big")


def dynamic_side_table_plan_checks(
    conn,
    schema: str,
    ranges: Sequence[LogicalRange],
) -> list[dict[str, Any]]:
    """Prove every candidate range and an exact node lookup use B-tree indexes.

    These ANALYZE/BUFFERS diagnostics run after the measured recovery interval.
    They mirror the SPI predicates in the backend and fail closed on any
    sequential side-table plan.
    """
    if not ranges:
        raise RuntimeError("dynamic side-table plan checks require candidate ranges")
    generation = _generation_identity(conn, schema)
    result: list[dict[str, Any]] = []
    for ordinal, logical_range in enumerate(ranges):
        lower = logical_range.prefix_bytes
        upper = _range_upper_bound(logical_range)
        params: tuple[Any, ...] = (
            *generation,
            logical_range.partition_id,
            lower,
        )
        predicate = "AND route_digest >= %s"
        if logical_range.prefix_length == 0:
            predicate = ""
            params = (*generation, logical_range.partition_id)
        elif upper is not None:
            predicate += " AND route_digest < %s"
            params = (*params, upper)
        rows = execute(
            conn,
            """
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT key_data,route_digest,tuple_hash
            FROM ariabc_internal.merkle_dynamic_leaf_item
            WHERE index_oid=%s AND rnode_spc=%s AND rnode_db=%s AND rnode_rel=%s
              AND partition_id=%s
            """
            + predicate
            + " ORDER BY route_digest,key_data",
            params,
        )
        plan, detail = _explain_document(rows)
        expected = "merkle_dynamic_route_lookup_idx"
        ok = _plan_uses_index(plan, expected)
        row = {
            "schema": schema,
            "operation": "dynamic_candidate_range_lookup",
            "ordinal": ordinal,
            "logical_range": logical_range.label,
            "requested_key_count": 0,
            "expected_index": expected,
            "index_used": int(ok),
            "rows_examined": _relation_rows_examined(
                plan, "merkle_dynamic_leaf_item"
            ),
            "shared_hit_blocks": _plan_metric(plan, "Shared Hit Blocks"),
            "shared_read_blocks": _plan_metric(plan, "Shared Read Blocks"),
            "plan_json_sha256": hashlib.sha256(detail.encode()).hexdigest(),
            "plan_json": detail,
        }
        result.append(row)
        if not ok:
            raise RuntimeError(
                f"{schema} {logical_range.label} dynamic candidate lookup "
                f"did not use {expected}: {detail}"
            )

    logical_range = ranges[0]
    rows = execute(
        conn,
        """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT tuple_count,data_xor,is_leaf
        FROM ariabc_internal.merkle_dynamic_node
        WHERE index_oid=%s AND rnode_spc=%s AND rnode_db=%s AND rnode_rel=%s
          AND partition_id=%s AND prefix_len=%s AND prefix_bytes=%s
        """,
        (
            *generation,
            logical_range.partition_id,
            logical_range.prefix_length,
            logical_range.prefix_bytes,
        ),
    )
    plan, detail = _explain_document(rows)
    expected_indexes = (
        "merkle_dynamic_node_pkey",
        "merkle_dynamic_node_prefix_lookup_idx",
    )
    expected = "|".join(expected_indexes)
    ok = _plan_uses_any_index(plan, expected_indexes)
    result.append(
        {
            "schema": schema,
            "operation": "dynamic_exact_node_lookup",
            "ordinal": 0,
            "logical_range": logical_range.label,
            "requested_key_count": 1,
            "expected_index": expected,
            "index_used": int(ok),
            "rows_examined": _relation_rows_examined(plan, "merkle_dynamic_node"),
            "shared_hit_blocks": _plan_metric(plan, "Shared Hit Blocks"),
            "shared_read_blocks": _plan_metric(plan, "Shared Read Blocks"),
            "plan_json_sha256": hashlib.sha256(detail.encode()).hexdigest(),
            "plan_json": detail,
        }
    )
    if not ok:
        raise RuntimeError(
            f"{schema} exact dynamic node lookup did not use an approved "
            f"index ({expected}): {detail}"
        )
    return result


def dynamic_storage_scan_snapshot(conn) -> dict[str, int]:
    """Return current-backend-visible seq-scan counters for dynamic hot tables."""
    if scalar(conn, "SELECT to_regprocedure('pg_stat_force_next_flush()') IS NOT NULL"):
        execute(conn, "SELECT pg_stat_force_next_flush()")
    rows = execute(
        conn,
        """
        SELECT relname,seq_scan::bigint AS seq_scan
        FROM pg_stat_all_tables
        WHERE schemaname='ariabc_internal'
          AND relname IN ('merkle_dynamic_node','merkle_dynamic_leaf_item')
        ORDER BY relname
        """,
    )
    return {str(row["relname"]): int(row["seq_scan"]) for row in rows}


def exact_heap_fetch_plan(conn, keys: Sequence[int]) -> dict[str, Any]:
    """Capture and enforce the healthy primary-key plan used inside timing."""
    if not keys:
        return {
            "operation": "healthy_exact_heap_fetch",
            "requested_key_count": 0,
            "expected_index": "usertable_pkey",
            "index_used": 1,
            "plan_json_sha256": hashlib.sha256(b"[]").hexdigest(),
            "plan_json": "[]",
        }
    rows = execute(
        conn,
        f"EXPLAIN (FORMAT JSON) SELECT {', '.join(ALL_COLUMNS)} "
        "FROM healthy.usertable WHERE ycsb_key = ANY(%s::bigint[])",
        (list(keys),),
    )
    if not rows:
        raise RuntimeError("exact healthy-row EXPLAIN returned no plan")
    plan = rows[0].get("QUERY PLAN")
    if isinstance(plan, str):
        plan = json.loads(plan)
    detail = json.dumps(plan, sort_keys=True, default=str)
    ok = _plan_uses_index(plan, "usertable_pkey")
    if not ok:
        raise RuntimeError(
            "timed dynamic recovery healthy-row fetch does not use "
            f"healthy.usertable_pkey: {detail}"
        )
    return {
        "operation": "healthy_exact_heap_fetch",
        "requested_key_count": len(keys),
        "expected_index": "usertable_pkey",
        "index_used": int(ok),
        "plan_json_sha256": hashlib.sha256(detail.encode()).hexdigest(),
        "plan_json": detail,
    }


def fetch_exact_healthy_rows(conn, keys: Sequence[int]) -> list[dict[str, Any]]:
    unique = sorted(set(int(key) for key in keys))
    if not unique:
        return []
    rows = execute(
        conn,
        f"SELECT {', '.join(ALL_COLUMNS)} FROM healthy.usertable "
        "WHERE ycsb_key = ANY(%s::bigint[]) ORDER BY ycsb_key",
        (unique,),
    )
    if len(rows) != len(unique):
        found = {int(row["ycsb_key"]) for row in rows}
        missing = sorted(set(unique) - found)
        raise RuntimeError(f"healthy repair rows are missing keys: {missing[:10]}")
    return rows


@dataclass(frozen=True)
class SetBasedRepairResult:
    rows_inserted: int
    rows_updated: int
    rows_deleted: int

    @property
    def total(self) -> int:
        return self.rows_inserted + self.rows_updated + self.rows_deleted


def apply_set_based_repairs(
    conn,
    repairs: RepairKeys,
    healthy_rows: Sequence[Mapping[str, Any]],
) -> SetBasedRepairResult:
    """Apply at most one UPSERT and one DELETE statement for the whole repair."""
    expected_rows = set(repairs.healthy_heap_keys)
    row_map = {int(row["ycsb_key"]): row for row in healthy_rows}
    if set(row_map) != expected_rows:
        raise RuntimeError("healthy full-row payload does not match insert/update keys")

    if expected_rows:
        payload = [
            {column: row_map[key][column] for column in ALL_COLUMNS}
            for key in sorted(expected_rows)
        ]
        execute(
            conn,
            """
            INSERT INTO damaged.usertable (
                ycsb_key, field0, field1, field2, field3, field4,
                field5, field6, field7, field8, field9
            )
            SELECT ycsb_key, field0, field1, field2, field3, field4,
                   field5, field6, field7, field8, field9
            FROM jsonb_to_recordset(%s::jsonb) AS r(
                ycsb_key bigint,
                field0 text, field1 text, field2 text, field3 text, field4 text,
                field5 text, field6 text, field7 text, field8 text, field9 text
            )
            ON CONFLICT (ycsb_key) DO UPDATE SET
                field0 = EXCLUDED.field0,
                field1 = EXCLUDED.field1,
                field2 = EXCLUDED.field2,
                field3 = EXCLUDED.field3,
                field4 = EXCLUDED.field4,
                field5 = EXCLUDED.field5,
                field6 = EXCLUDED.field6,
                field7 = EXCLUDED.field7,
                field8 = EXCLUDED.field8,
                field9 = EXCLUDED.field9
            """,
            (json.dumps(payload, default=str),),
        )
    if repairs.deletes:
        execute(
            conn,
            "DELETE FROM damaged.usertable WHERE ycsb_key = ANY(%s::bigint[])",
            (list(repairs.deletes),),
        )
    return SetBasedRepairResult(
        rows_inserted=len(repairs.inserts),
        rows_updated=len(repairs.updates),
        rows_deleted=len(repairs.deletes),
    )
