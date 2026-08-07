"""Leaf-level candidate fetch and row-level repair DML."""

from __future__ import annotations

import json
import statistics
import time
from contextlib import contextmanager, nullcontext
from typing import Any

from .config import ALL_COLUMNS, FIELDS, LEAF_LOOKUP_INDEXES, leaf_key as leaf_key_fn
from .db import execute, scalar
from .profiling import ProfileCollector, record_call, parse_json_plan

LEAF_LOOKUP_PLAN_INDEXES = (
    "usertable_merkle_partition_lookup_idx",
)


# ── leaf lookup ─────────────────────────────────────────────────────────────

def _db_text(value: Any) -> Any:
    """Normalize psycopg text values before comparison or DML binding."""
    return value.decode("utf-8") if isinstance(value, bytes) else value

def bytea_lower_bound(node_id: bytes, prefix_len: int) -> bytes:
    """Canonical form: the node_id itself is the lower bound."""
    return node_id

def bytea_upper_bound(node_id: bytes, prefix_len: int) -> bytes:
    """Compute upper bound of the subtree."""
    res = bytearray(node_id)
    full_bytes = prefix_len // 8
    rem = prefix_len % 8
    if rem > 0:
        mask = 0xFF >> rem
        res[full_bytes] |= mask
        first_free = full_bytes + 1
    else:
        first_free = full_bytes
    for i in range(first_free, 8):
        res[i] = 0xFF
    return bytes(res)

def leaf_lookup_sql(schema: str, explain: bool = False, partition_aware: bool = False) -> str:
    prefix = "EXPLAIN (FORMAT JSON) " if explain else ""
    partition_predicate = ""
    if partition_aware:
        partition_predicate = (
            " AND merkle_partition_for_hash(merkle_key_hash(ycsb_key), %s) = %s"
        )
    return (
        f"{prefix}SELECT {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE merkle_key_hash(ycsb_key) BETWEEN %s AND %s"
        f"{partition_predicate}"
    )

def _leaf_lookup_params(leaf_spec: tuple, partitions: int = 200) -> tuple:
    if len(leaf_spec) == 3:
        partition_id, node_id, prefix_len = leaf_spec
        return (
            bytea_lower_bound(node_id, prefix_len),
            bytea_upper_bound(node_id, prefix_len),
            partitions,
            partition_id,
        )
    return (bytea_lower_bound(leaf_spec[0], leaf_spec[1]), bytea_upper_bound(leaf_spec[0], leaf_spec[1]))


def fetch_leaf_rows(conn, schema: str, leaf_node_id: bytes | tuple, prefix_len: int | None = None,
                    partitions: int = 200) -> dict[int, dict[str, Any]]:
    spec = leaf_key_fn(leaf_node_id) if prefix_len is None else (leaf_node_id, int(prefix_len))
    aware = len(spec) == 3
    params = _leaf_lookup_params(spec, partitions)
    rows = execute(conn, leaf_lookup_sql(schema, partition_aware=aware), params)
    return {int(r["ycsb_key"]): r for r in rows}


def leaf_lookup_batch_sql(schema: str, partition_aware: bool = True) -> str:
    # Batch lookup might be tricky with overlapping ranges if not handled, but leaves don't overlap.
    if partition_aware:
        return (
            f"SELECT {', '.join(ALL_COLUMNS)}, p.partition_id, p.node_id AS merkle_leaf_id, p.prefix_len "
            f"FROM ROWS FROM (unnest(%s::int4[]), unnest(%s::bytea[]), unnest(%s::smallint[]), unnest(%s::bytea[]), unnest(%s::bytea[])) "
            f"AS p(partition_id, node_id, prefix_len, lower_bound, upper_bound) "
            f"JOIN {schema}.usertable u ON merkle_key_hash(u.ycsb_key) BETWEEN p.lower_bound AND p.upper_bound "
            f"AND merkle_partition_for_hash(merkle_key_hash(u.ycsb_key), %s) = p.partition_id"
        )
    return (
        f"SELECT {', '.join(ALL_COLUMNS)}, p.node_id AS merkle_leaf_id, p.prefix_len "
        f"FROM ROWS FROM (unnest(%s::bytea[]), unnest(%s::smallint[]), unnest(%s::bytea[]), unnest(%s::bytea[])) AS p(node_id, prefix_len, lower_bound, upper_bound) "
        f"JOIN {schema}.usertable u ON merkle_key_hash(u.ycsb_key) BETWEEN p.lower_bound AND p.upper_bound"
    )


class FetchResult:
    __slots__ = ("by_leaf", "sql_calls", "batches", "leaf_buckets_requested", "rows_fetched")

    def __init__(self) -> None:
        self.by_leaf: dict[tuple[bytes, int], dict[int, dict]] = {}
        self.sql_calls: int = 0
        self.batches: int = 0
        self.leaf_buckets_requested: int = 0
        self.rows_fetched: int = 0

    def get(self, key: tuple[bytes, int], default=None):
        return self.by_leaf.get(key, default)

    def __contains__(self, key: tuple[bytes, int]) -> bool:
        return key in self.by_leaf

    def __len__(self) -> int:
        return self.rows_fetched


def fetch_leaf_rows_batch(
    conn, schema: str, leaf_keys: list[tuple[bytes, int]], chunk_size: int = 0
) -> FetchResult:
    result = FetchResult()
    unique_keys = sorted(set(leaf_key_fn(value) for value in leaf_keys), key=str)
    if not unique_keys:
        return result

    for k in unique_keys:
        result.by_leaf[k] = {}

    if chunk_size <= 0:
        chunks = [unique_keys]
    else:
        chunks = [unique_keys[i:i + chunk_size] for i in range(0, len(unique_keys), chunk_size)]

    for chunk in chunks:
        result.sql_calls += 1
        result.batches += 1
        result.leaf_buckets_requested += len(chunk)
        aware = all(len(k) == 3 for k in chunk)
        sql = leaf_lookup_batch_sql(schema, aware)
        node_ids = [k[1] if aware else k[0] for k in chunk]
        prefix_lens = [k[2] if aware else k[1] for k in chunk]
        lowers = [bytea_lower_bound(node_ids[i], prefix_lens[i]) for i in range(len(chunk))]
        uppers = [bytea_upper_bound(node_ids[i], prefix_lens[i]) for i in range(len(chunk))]
        if aware:
            params = ([k[0] for k in chunk], node_ids, prefix_lens, lowers, uppers, 200)
        else:
            params = (node_ids, prefix_lens, lowers, uppers)

        for row in execute(conn, sql, params):
            if aware:
                key = (int(row["partition_id"]), bytes(row["merkle_leaf_id"]), int(row["prefix_len"]))
            else:
                key = (bytes(row["merkle_leaf_id"]), int(row["prefix_len"]))
            payload = {column: _db_text(row[column]) for column in ALL_COLUMNS}
            result.by_leaf.setdefault(key, {})[int(row["ycsb_key"])] = payload
            result.rows_fetched += 1

    return result

# ── planner preflight ────────────────────────────────────────────────────────
def _plan_node_uses_index(node: Any, index_name: str) -> bool:
    if isinstance(node, dict):
        if node.get("Index Name") == index_name:
            return True
        return any(_plan_node_uses_index(value, index_name) for value in node.values())
    if isinstance(node, list):
        return any(_plan_node_uses_index(value, index_name) for value in node)
    return False


def _plan_uses_leaf_lookup_index(plan: Any) -> bool:
    return any(_plan_node_uses_index(plan, name) for name in LEAF_LOOKUP_PLAN_INDEXES)

def indexed_lookup_plan_ok(conn, schema: str, leaf_key: tuple[bytes, int]) -> tuple[bool, str]:
    leaf_key = leaf_key_normalized = leaf_key_fn(leaf_key)
    params = _leaf_lookup_params(leaf_key_normalized)
    plan_rows = execute(
        conn, leaf_lookup_sql(schema, explain=True, partition_aware=len(leaf_key_normalized) == 3), params
    )
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    ok = _plan_uses_leaf_lookup_index(plan_doc)
    return ok, json.dumps(plan_doc, default=str)


def leaf_lookup_batch_explain_sql(schema: str, partition_aware: bool = True) -> str:
    return "EXPLAIN (FORMAT JSON) " + leaf_lookup_batch_sql(schema, partition_aware)

def batch_indexed_lookup_plan_ok(conn, schema: str, leaf_keys: list[tuple[bytes, int]]) -> tuple[bool, str]:
    if not leaf_keys:
        return True, "{}"
    normalized = [leaf_key_fn(value) for value in leaf_keys]
    aware = all(len(value) == 3 for value in normalized)
    node_ids = [k[1] if aware else k[0] for k in normalized]
    prefix_lens = [k[2] if aware else k[1] for k in normalized]
    lowers = [bytea_lower_bound(node_ids[i], prefix_lens[i]) for i in range(len(normalized))]
    uppers = [bytea_upper_bound(node_ids[i], prefix_lens[i]) for i in range(len(normalized))]
    if aware:
        params = ([k[0] for k in normalized], node_ids, prefix_lens, lowers, uppers, 200)
    else:
        params = (node_ids, prefix_lens, lowers, uppers)
    plan_rows = execute(conn, leaf_lookup_batch_explain_sql(schema, aware), params)
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    ok = _plan_uses_leaf_lookup_index(plan_doc)
    return ok, json.dumps(plan_doc, default=str)


def run_planner_preflight(
    conn,
    manifest: dict[str, Any],
    run_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    # We can skip complex scaling logic for preflight in the dynamic tree,
    # just check the actual bad leaves.
    import hashlib
    unique_bad_leaves = set(tuple(x) if isinstance(x, (list, tuple)) else x for x in manifest["bad_leaves"])
    out: dict[str, Any] = {
        "planner_checked_leaf_count": len(unique_bad_leaves),
        "planner_checks_passed": 1,
        "batch_plan_actual_k_index_used": 0,
    }
    rows: list[dict[str, Any]] = []

    bad_keys = [leaf_key_fn(x) for x in unique_bad_leaves]

    for leaf_key in bad_keys:
        for schema in ("healthy", "damaged"):
            ok, detail = indexed_lookup_plan_ok(conn, schema, leaf_key)
            if not ok:
                out["planner_checks_passed"] = 0
                raise RuntimeError(f"Plan missing index usage: {detail}")

    if bad_keys:
        for schema in ("healthy", "damaged"):
            ok, detail = batch_indexed_lookup_plan_ok(conn, schema, bad_keys)
            if not ok:
                out["planner_checks_passed"] = 0
                raise RuntimeError(f"Batch plan missing index usage: {detail}")
        out["batch_plan_actual_k_index_used"] = 1

    return out, rows

def lookup_explain_plan_json(conn, schema: str, leaf_key: tuple[bytes, int]) -> dict[str, Any] | None:
    leaf_key = leaf_key_fn(leaf_key)
    params = _leaf_lookup_params(leaf_key)
    plan_rows = execute(
        conn,
        f"""
        EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF, FORMAT JSON)
        SELECT {', '.join(ALL_COLUMNS)}
        FROM {schema}.usertable
        {"WHERE merkle_key_hash(ycsb_key) BETWEEN %s AND %s"}
        {"AND merkle_partition_for_hash(merkle_key_hash(ycsb_key), %s) = %s" if len(leaf_key) == 3 else ""}
        """,
        params,
    )
    return parse_json_plan(plan_rows)


# ── seq-scan counters ────────────────────────────────────────────────────────

def seq_scan_snapshot(conn) -> dict[str, int]:
    execute(conn, "SELECT pg_stat_clear_snapshot()")
    rows = execute(
        conn,
        """
        SELECT schemaname, relname, seq_scan
        FROM pg_stat_user_tables
        WHERE schemaname IN ('healthy', 'damaged')
          AND relname = 'usertable'
        """,
    )
    return {f"{r['schemaname']}.{r['relname']}": int(r["seq_scan"]) for r in rows}


def seq_scan_delta(before: dict[str, int], after: dict[str, int]) -> int:
    keys = set(before) | set(after)
    return sum(max(0, after.get(k, 0) - before.get(k, 0)) for k in keys)


def representative_leaf_ids(total_leaves: int, count: int) -> list[int]:
    if total_leaves <= count:
        return list(range(total_leaves))
    step = total_leaves / float(count)
    return [int(round(i * step)) for i in range(count)]


# ── repair DML ───────────────────────────────────────────────────────────────

def execute_batched_deletes(
    conn,
    schema: str,
    keys: list[int],
    profiler: ProfileCollector | None = None,
    leaf_id_str: str = "",
    batch_size: int = 500,
) -> int:
    if not keys:
        return 0
    total_deleted = 0
    for i in range(0, len(keys), batch_size):
        sub_keys = keys[i:i + batch_size]
        in_sql = ", ".join(["%s"] * len(sub_keys))
        sql = f"DELETE FROM {schema}.usertable WHERE ycsb_key IN ({in_sql})"
        params = tuple(sub_keys)
        record_call(
            profiler,
            stage="repair",
            operation="delete_dml",
            leaf_id=leaf_id_str,
            schema=schema,
            fn=lambda sql=sql, params=params: execute(conn, sql, params),
        )
        total_deleted += len(sub_keys)
    return total_deleted


def execute_batched_inserts(
    conn,
    schema: str,
    keys: list[int],
    hrows: dict[int, dict[str, Any]],
    profiler: ProfileCollector | None = None,
    leaf_id_str: str = "",
    batch_size: int = 500,
) -> int:
    if not keys:
        return 0
    total_inserted = 0
    cols_sql = "ycsb_key, " + ", ".join(FIELDS)
    first_row_pattern = "(%s::bigint, " + ", ".join(["%s::text"] * len(FIELDS)) + ")"
    other_row_pattern = "(" + ", ".join(["%s"] * len(ALL_COLUMNS)) + ")"
    for i in range(0, len(keys), batch_size):
        sub_keys = keys[i:i + batch_size]
        values_sql = ", ".join([first_row_pattern] + [other_row_pattern] * (len(sub_keys) - 1))
        sql = f"INSERT INTO {schema}.usertable ({cols_sql}) VALUES {values_sql}"
        params_list = []
        for k in sub_keys:
            vals = hrows[k]
            params_list.append(k)
            params_list.extend(_db_text(vals[f]) for f in FIELDS)
        params = tuple(params_list)
        record_call(
            profiler,
            stage="repair",
            operation="insert_dml",
            leaf_id=leaf_id_str,
            schema=schema,
            fn=lambda sql=sql, params=params: execute(conn, sql, params),
        )
        total_inserted += len(sub_keys)
    return total_inserted


def execute_batched_updates(
    conn,
    schema: str,
    keys: list[int],
    hrows: dict[int, dict[str, Any]],
    profiler: ProfileCollector | None = None,
    leaf_id_str: str = "",
    batch_size: int = 500,
) -> int:
    if not keys:
        return 0
    total_updated = 0
    set_clause = ", ".join([f"{f} = v.{f}" for f in FIELDS])
    cols_sql = "ycsb_key, " + ", ".join(FIELDS)
    first_row_pattern = "(%s::bigint, " + ", ".join(["%s::text"] * len(FIELDS)) + ")"
    other_row_pattern = "(" + ", ".join(["%s"] * len(ALL_COLUMNS)) + ")"
    for i in range(0, len(keys), batch_size):
        sub_keys = keys[i:i + batch_size]
        values_sql = ", ".join([first_row_pattern] + [other_row_pattern] * (len(sub_keys) - 1))
        sql = (
            f"UPDATE {schema}.usertable AS u "
            f"SET {set_clause} "
            f"FROM (VALUES {values_sql}) AS v({cols_sql}) "
            f"WHERE u.ycsb_key = v.ycsb_key"
        )
        params_list = []
        for k in sub_keys:
            vals = hrows[k]
            params_list.append(k)
            params_list.extend(_db_text(vals[f]) for f in FIELDS)
        params = tuple(params_list)
        record_call(
            profiler,
            stage="repair",
            operation="update_dml",
            leaf_id=leaf_id_str,
            schema=schema,
            fn=lambda sql=sql, params=params: execute(conn, sql, params),
        )
        total_updated += len(sub_keys)
    return total_updated


def repair_leaf(
    conn,
    leaf_key: tuple[bytes, int],
    *,
    phase: dict[str, float],
    profiler: ProfileCollector | None = None,
    prefetched: tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]] | None = None,
    measure_repair_write: bool = True,
    execute_dml: bool = True,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], Any, Any, Any]:
    @contextmanager
    def _timer(key: str):
        t0 = time.perf_counter() * 1000.0
        yield
        phase[key] = phase.get(key, 0.0) + time.perf_counter() * 1000.0 - t0

    leaf_key = leaf_key_fn(leaf_key)
    rows_inserted = rows_updated = rows_deleted = 0
    if len(leaf_key) == 3:
        partition_id, node_id, prefix_len = leaf_key
        leaf_id_str = f"{partition_id}_{node_id.hex()}_{prefix_len}"
    else:
        node_id, prefix_len = leaf_key
        leaf_id_str = f"{node_id.hex()}_{prefix_len}"

    if prefetched is None:
        with _timer("candidate_row_fetch_ms"):
            hrows = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_healthy",
                schema="healthy",
                leaf_id=leaf_id_str,
                fn=lambda: fetch_leaf_rows(conn, "healthy", leaf_key),
            )
            drows = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_damaged",
                schema="damaged",
                leaf_id=leaf_id_str,
                fn=lambda: fetch_leaf_rows(conn, "damaged", leaf_key),
            )
    else:
        hrows, drows = prefetched

    with _timer("row_comparison_ms"):
        key_start = time.perf_counter_ns()
        hkeys = set(hrows)
        dkeys = set(drows)
        inserts = sorted(hkeys - dkeys)   
        deletes = sorted(dkeys - hkeys)   
        common_keys = sorted(hkeys & dkeys)
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage="comparison",
                operation="key_set_build_cpu",
                leaf_id=leaf_id_str,
                client_wall_ns=time.perf_counter_ns() - key_start,
                rows_returned=len(hrows) + len(drows),
            )
        payload_start = time.perf_counter_ns()
        updates = [key for key in common_keys if hrows[key] != drows[key]]
        if not execute_dml:
            return hrows, drows, inserts, updates, deletes

        rows_inserted = rows_updated = rows_deleted = 0
        write_timer = _timer("repair_write_ms") if measure_repair_write else nullcontext()
        with write_timer:
            with _timer("repair_table_dml_ms"):
                rows_inserted += execute_batched_inserts(conn, "damaged", inserts, hrows, profiler, leaf_id_str)
                rows_updated += execute_batched_updates(conn, "damaged", updates, hrows, profiler, leaf_id_str)
                rows_deleted += execute_batched_deletes(conn, "damaged", deletes, profiler, leaf_id_str)

        return hrows, drows, rows_inserted, rows_updated, rows_deleted



def per_leaf_row_counts(bad_leaves: list, per_leaf: list[int]) -> dict[str, float]:
    if not per_leaf:
        return {"mean_rows_per_bad_leaf": 0.0, "p95_rows_per_bad_leaf": 0.0}
    ordered = sorted(per_leaf)
    p95_idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return {
        "mean_rows_per_bad_leaf": statistics.mean(ordered),
        "p95_rows_per_bad_leaf": float(ordered[p95_idx]),
    }
