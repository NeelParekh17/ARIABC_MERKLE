"""Leaf-level candidate fetch and row-level repair DML."""

from __future__ import annotations

import json
import statistics
import time
from contextlib import contextmanager
from typing import Any

from .config import ALL_COLUMNS, FIELDS, LEAF_LOOKUP_INDEXES
from .db import execute, scalar
from .profiling import ProfileCollector, record_call, parse_json_plan


# ── leaf lookup ─────────────────────────────────────────────────────────────

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

def leaf_lookup_sql(schema: str, explain: bool = False) -> str:
    prefix = "EXPLAIN (FORMAT JSON) " if explain else ""
    return (
        f"{prefix}SELECT {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE merkle_key_hash(ycsb_key) BETWEEN %s AND %s"
    )

def fetch_leaf_rows(conn, schema: str, leaf_node_id: bytes, prefix_len: int) -> dict[int, dict[str, Any]]:
    lower = bytea_lower_bound(leaf_node_id, prefix_len)
    upper = bytea_upper_bound(leaf_node_id, prefix_len)
    rows = execute(conn, leaf_lookup_sql(schema), (lower, upper))
    return {int(r["ycsb_key"]): r for r in rows}


def leaf_lookup_batch_sql(schema: str) -> str:
    # Batch lookup might be tricky with overlapping ranges if not handled, but leaves don't overlap.
    return (
        f"SELECT {', '.join(ALL_COLUMNS)}, p.node_id AS merkle_leaf_id, p.prefix_len "
        f"FROM unnest(%s::bytea[], %s::smallint[], %s::bytea[], %s::bytea[]) AS p(node_id, prefix_len, lower_bound, upper_bound) "
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
    # leaf_keys are (node_id, prefix_len)
    unique_keys = sorted(set(leaf_keys))
    if not unique_keys:
        return result

    for k in unique_keys:
        result.by_leaf[k] = {}

    if chunk_size <= 0:
        chunks = [unique_keys]
    else:
        chunks = [unique_keys[i:i + chunk_size] for i in range(0, len(unique_keys), chunk_size)]

    sql = leaf_lookup_batch_sql(schema)
    for chunk in chunks:
        result.sql_calls += 1
        result.batches += 1
        result.leaf_buckets_requested += len(chunk)
        
        node_ids = [k[0] for k in chunk]
        prefix_lens = [k[1] for k in chunk]
        lowers = [bytea_lower_bound(k[0], k[1]) for k in chunk]
        uppers = [bytea_upper_bound(k[0], k[1]) for k in chunk]
        
        for row in execute(conn, sql, (node_ids, prefix_lens, lowers, uppers)):
            key = (bytes(row["merkle_leaf_id"]), int(row["prefix_len"]))
            payload = {column: row[column] for column in ALL_COLUMNS}
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

def indexed_lookup_plan_ok(conn, schema: str, leaf_key: tuple[bytes, int]) -> tuple[bool, str]:
    lower = bytea_lower_bound(leaf_key[0], leaf_key[1])
    upper = bytea_upper_bound(leaf_key[0], leaf_key[1])
    plan_rows = execute(conn, leaf_lookup_sql(schema, explain=True), (lower, upper))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = "usertable_merkle_covering_idx" # New index name per plan
    ok = _plan_node_uses_index(plan_doc, index_name)
    return ok, json.dumps(plan_doc, default=str)


def leaf_lookup_batch_explain_sql(schema: str) -> str:
    return (
        f"EXPLAIN (FORMAT JSON) SELECT {', '.join(ALL_COLUMNS)}, p.node_id AS merkle_leaf_id, p.prefix_len "
        f"FROM unnest(%s::bytea[], %s::smallint[], %s::bytea[], %s::bytea[]) AS p(node_id, prefix_len, lower_bound, upper_bound) "
        f"JOIN {schema}.usertable u ON merkle_key_hash(u.ycsb_key) BETWEEN p.lower_bound AND p.upper_bound"
    )

def batch_indexed_lookup_plan_ok(conn, schema: str, leaf_keys: list[tuple[bytes, int]]) -> tuple[bool, str]:
    if not leaf_keys:
        return True, "{}"
    node_ids = [k[0] for k in leaf_keys]
    prefix_lens = [k[1] for k in leaf_keys]
    lowers = [bytea_lower_bound(k[0], k[1]) for k in leaf_keys]
    uppers = [bytea_upper_bound(k[0], k[1]) for k in leaf_keys]
    
    plan_rows = execute(conn, leaf_lookup_batch_explain_sql(schema), (node_ids, prefix_lens, lowers, uppers))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = "usertable_merkle_covering_idx"
    ok = _plan_node_uses_index(plan_doc, index_name)
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

    bad_keys = [(bytes.fromhex(x[0]), int(x[1])) if isinstance(x, (list, tuple)) else x for x in unique_bad_leaves]

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
    lower = bytea_lower_bound(leaf_key[0], leaf_key[1])
    upper = bytea_upper_bound(leaf_key[0], leaf_key[1])
    plan_rows = execute(
        conn,
        f"""
        EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF, FORMAT JSON)
        SELECT {', '.join(ALL_COLUMNS)}
        FROM {schema}.usertable
        WHERE merkle_key_hash(ycsb_key) BETWEEN %s AND %s
        """,
        (lower, upper),
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

def repair_leaf(
    conn,
    leaf_key: tuple[bytes, int],
    *,
    phase: dict[str, float],
    profiler: ProfileCollector | None = None,
    prefetched: tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]] | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], int, int, int]:
    
    from contextlib import contextmanager
    import time

    @contextmanager
    def _timer(key: str):
        t0 = time.perf_counter() * 1000.0
        yield
        phase[key] = phase.get(key, 0.0) + time.perf_counter() * 1000.0 - t0

    rows_inserted = rows_updated = rows_deleted = 0
    leaf_id_str = f"{leaf_key[0].hex()}_{leaf_key[1]}"

    if prefetched is None:
        with _timer("candidate_row_fetch_ms"):
            hrows_raw = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_healthy",
                schema="healthy",
                leaf_id=leaf_id_str,
                fn=lambda: execute(conn, leaf_lookup_sql("healthy"), (bytea_lower_bound(*leaf_key), bytea_upper_bound(*leaf_key))),
            )
            drows_raw = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_damaged",
                schema="damaged",
                leaf_id=leaf_id_str,
                fn=lambda: execute(conn, leaf_lookup_sql("damaged"), (bytea_lower_bound(*leaf_key), bytea_upper_bound(*leaf_key))),
            )
            mat_start = time.perf_counter_ns()
            hrows = {int(r["ycsb_key"]): r for r in hrows_raw}
            drows = {int(r["ycsb_key"]): r for r in drows_raw}
            if profiler is not None and profiler.enabled:
                profiler.record(
                    stage="candidate_fetch",
                    operation="candidate_dict_materialisation_cpu",
                    schema="",
                    leaf_id=leaf_id_str,
                    client_wall_ns=time.perf_counter_ns() - mat_start,
                    rows_returned=len(hrows_raw) + len(drows_raw),
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
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage="comparison",
                operation="row_payload_comparison_cpu",
                leaf_id=leaf_id_str,
                client_wall_ns=time.perf_counter_ns() - payload_start,
                rows_returned=len(hrows) + len(drows),
            )

    with _timer("repair_write_ms"):
        with _timer("repair_table_dml_ms"):
            for key in inserts:
                vals = hrows[key]
                record_call(
                    profiler,
                    stage="repair",
                    operation="insert_dml",
                    leaf_id=leaf_id_str,
                    schema="damaged",
                    fn=lambda vals=vals: execute(
                        conn,
                        "INSERT INTO damaged.usertable VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        tuple(vals[c] for c in ALL_COLUMNS),
                    ),
                )
                rows_inserted += 1
            for key in updates:
                vals = hrows[key]
                record_call(
                    profiler,
                    stage="repair",
                    operation="update_dml",
                    leaf_id=leaf_id_str,
                    schema="damaged",
                    fn=lambda vals=vals, key=key: execute(
                        conn,
                        "UPDATE damaged.usertable SET field0=%s, field1=%s, field2=%s, field3=%s, field4=%s, "
                        "field5=%s, field6=%s, field7=%s, field8=%s, field9=%s WHERE ycsb_key=%s",
                        tuple(vals[f] for f in FIELDS) + (key,),
                    ),
                )
                rows_updated += 1
            for key in deletes:
                record_call(
                    profiler,
                    stage="repair",
                    operation="delete_dml",
                    leaf_id=leaf_id_str,
                    schema="damaged",
                    fn=lambda key=key: execute(conn, "DELETE FROM damaged.usertable WHERE ycsb_key = %s", (key,)),
                )
                rows_deleted += 1

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
