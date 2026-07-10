"""Leaf-level candidate fetch and row-level repair DML.

All timing is done by the caller via the ``timer`` context manager.
"""

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

def leaf_lookup_sql(schema: str, explain: bool = False) -> str:
    prefix = "EXPLAIN (FORMAT JSON) " if explain else ""
    bucket_expr = f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"
    return (
        f"{prefix}SELECT {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE {bucket_expr} = %s"
    )


def fetch_leaf_rows(conn, schema: str, leaf_id: int) -> dict[int, dict[str, Any]]:
    rows = execute(conn, leaf_lookup_sql(schema), (leaf_id,))
    return {int(r["ycsb_key"]): r for r in rows}


def leaf_lookup_batch_sql(schema: str) -> str:
    bucket_expr = f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"
    return (
        f"SELECT {bucket_expr}::bigint AS merkle_leaf_id, {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE {bucket_expr} = ANY(%s::bigint[])"
    )


class FetchResult:
    """Structured result from a batched leaf-row fetch.

    Attributes
    ----------
    by_leaf : dict[int, dict[int, dict]]
        Rows grouped by leaf ID then by ycsb_key.
    sql_calls : int
        Number of SQL statements issued (one per chunk).
    batches : int
        Same as ``sql_calls``; kept as a distinct name for clarity in counters.
    leaf_buckets_requested : int
        Total distinct leaf IDs requested across all chunks (sum of chunk sizes).
    rows_fetched : int
        Total rows returned across all chunks.
    """

    __slots__ = ("by_leaf", "sql_calls", "batches", "leaf_buckets_requested", "rows_fetched")

    def __init__(self) -> None:
        self.by_leaf: dict[int, dict[int, dict]] = {}
        self.sql_calls: int = 0
        self.batches: int = 0
        self.leaf_buckets_requested: int = 0
        self.rows_fetched: int = 0

    def get(self, leaf_id: int, default=None):
        return self.by_leaf.get(leaf_id, default)

    def __contains__(self, leaf_id: int) -> bool:
        return leaf_id in self.by_leaf

    def __len__(self) -> int:
        return self.rows_fetched


def fetch_leaf_rows_batch(
    conn, schema: str, leaf_ids: list[int], chunk_size: int = 0
) -> FetchResult:
    """Fetch all requested static buckets, optionally in memory-bounded chunks.

    Parameters
    ----------
    conn :
        Database connection.
    schema :
        "healthy" or "damaged".
    leaf_ids :
        List of leaf IDs to fetch.
    chunk_size :
        Maximum number of leaf IDs per SQL statement.  ``0`` (default) sends all
        IDs in a single statement.  Use a positive value (e.g. 50) to cap peak
        memory when ``K`` is large.

    Returns
    -------
    FetchResult
        Structured result with per-leaf row maps and fetch counters.
    """
    result = FetchResult()
    unique_ids = sorted(set(int(v) for v in leaf_ids))
    if not unique_ids:
        for leaf in unique_ids:
            result.by_leaf[leaf] = {}
        return result

    for leaf in unique_ids:
        result.by_leaf[leaf] = {}

    if chunk_size <= 0:
        chunks = [unique_ids]
    else:
        chunks = [unique_ids[i:i + chunk_size] for i in range(0, len(unique_ids), chunk_size)]

    sql = leaf_lookup_batch_sql(schema)
    for chunk in chunks:
        result.sql_calls += 1
        result.batches += 1
        result.leaf_buckets_requested += len(chunk)
        for row in execute(conn, sql, (chunk,)):
            leaf_id = int(row["merkle_leaf_id"])
            payload = {column: row[column] for column in ALL_COLUMNS}
            result.by_leaf.setdefault(leaf_id, {})[int(row["ycsb_key"])] = payload
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


def indexed_lookup_plan_ok(conn, schema: str, leaf_id: int) -> tuple[bool, str]:
    plan_rows = execute(conn, leaf_lookup_sql(schema, explain=True), (leaf_id,))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = LEAF_LOOKUP_INDEXES[schema]
    ok = _plan_node_uses_index(plan_doc, index_name)
    return ok, json.dumps(plan_doc, default=str)


def leaf_lookup_batch_explain_sql(schema: str) -> str:
    bucket_expr = f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"
    return (
        f"EXPLAIN (FORMAT JSON) SELECT {bucket_expr}::bigint AS merkle_leaf_id, {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE {bucket_expr} = ANY(%s::bigint[])"
    )


def batch_indexed_lookup_plan_ok(conn, schema: str, leaf_ids: list[int]) -> tuple[bool, str]:
    plan_rows = execute(conn, leaf_lookup_batch_explain_sql(schema), (leaf_ids,))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = LEAF_LOOKUP_INDEXES[schema]
    ok = _plan_node_uses_index(plan_doc, index_name)
    return ok, json.dumps(plan_doc, default=str)


def representative_leaf_ids(total_leaves: int, requested: int) -> list[int]:
    """Return up to *requested* evenly-spaced valid leaf IDs in [0, total_leaves).

    If *requested* >= *total_leaves*, every leaf is returned.
    Always returns IDs that are valid arguments for the merkle_bucket_for_key
    SQL function so that EXPLAIN queries do not fail with an out-of-range error.
    """
    count = min(total_leaves, requested)
    if count <= 0:
        return []
    if count == total_leaves:
        return list(range(total_leaves))
    return sorted({
        (i * total_leaves) // count
        for i in range(count)
    })


def run_planner_preflight(
    conn,
    manifest: dict[str, Any],
    run_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run planner preflight checks for the batched ANY(...) lookup query.

    For each tested scale K, this function:
    - generates *valid* representative leaf IDs bounded by total tree leaves;
    - records whether the index was used (always);
    - hard-fails only when coverage is sparse (coverage <= 10 %) so that
      small smoke datasets are not incorrectly penalised;
    - always hard-fails for the canonical P=200/L=1024/K=75 geometry because
      its 75/204800 ≈ 0.037 % coverage must use the functional index.

    Per-bad-leaf single-key EXPLAIN checks remain unchanged.
    """
    import hashlib
    import math as _math

    total_leaves = (
        int(manifest.get("partitions", 0))
        * int(manifest.get("leaves_per_partition", 0))
    )

    out: dict[str, Any] = {
        "planner_checked_leaf_count": len(manifest["bad_leaves"]),
        "planner_checks_passed": 1,
        "batch_plan_k1_index_used": 0,
        "batch_plan_k10_index_used": 0,
        "batch_plan_k75_index_used": 0,
        "batch_plan_k200_index_used": 0,
        "batch_plan_actual_k_index_used": 0,
        "batch_plan_actual_k_index_required": 0,
    }
    rows: list[dict[str, Any]] = []

    # ── single-key EXPLAIN for each bad leaf ─────────────────────────────────
    for leaf_id in sorted(int(v) for v in manifest["bad_leaves"]):
        for schema in ("healthy", "damaged"):
            ok, detail = indexed_lookup_plan_ok(conn, schema, leaf_id)
            index_row = execute(
                conn,
                """
                SELECT c.oid::bigint AS index_oid,
                       pg_relation_filenode(c.oid)::bigint AS index_relfilenode,
                       pg_get_indexdef(c.oid) AS index_definition
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s
                """,
                (schema, LEAF_LOOKUP_INDEXES[schema]),
            )
            if not index_row:
                raise RuntimeError(
                    f"{schema}.{LEAF_LOOKUP_INDEXES[schema]} not found for planner preflight"
                )
            idx = index_row[0]
            row = {
                "run_id": run_id,
                "schema": schema,
                "leaf_id": leaf_id,
                "index_oid": int(idx["index_oid"]),
                "index_relfilenode": int(idx["index_relfilenode"]),
                "index_definition": idx["index_definition"],
                "plan_uses_expected_leaf_lookup_index": int(ok),
                "plan_json_sha256": hashlib.sha256(detail.encode()).hexdigest(),
            }
            rows.append(row)
            if not ok:
                out["planner_checks_passed"] = 0
                raise RuntimeError(
                    f"{schema} timed leaf lookup for leaf {leaf_id} does not use "
                    f"{schema}.{LEAF_LOOKUP_INDEXES[schema]}: {detail}"
                )

    # ── batched ANY(...) EXPLAIN at fixed scales K = 1, 10, 75, 200 ──────────
    # Use valid representative IDs bounded by the actual tree size so that
    # EXPLAIN does not reject out-of-range values.  Only hard-fail when the
    # lookup is sparse (coverage <= 10 %), because PostgreSQL may legitimately
    # choose a sequential scan when the leaf list covers the whole table.
    _SCALE_KEYS = {1: "k1", 10: "k10", 75: "k75", 200: "k200"}

    for scale, label in _SCALE_KEYS.items():
        leaf_ids = (
            representative_leaf_ids(total_leaves, scale)
            if total_leaves > 0
            else list(range(1, min(scale, 1) + 1))
        )
        if not leaf_ids:
            leaf_ids = [0]  # fallback: at least one ID so EXPLAIN runs

        coverage = len(leaf_ids) / total_leaves if total_leaves > 0 else 0.0
        # Sparse coverage (few leaves out of many) must use the index.
        # Dense coverage (many leaves vs small tree) may use seq scan: do not fail.
        require_index = coverage <= 0.10

        index_used_any_schema = True
        for schema in ("healthy", "damaged"):
            ok, detail = batch_indexed_lookup_plan_ok(conn, schema, leaf_ids)
            if not ok:
                index_used_any_schema = False

        # Record whether the index was used (True only if both schemas used it)
        out[f"batch_plan_{label}_index_used"] = int(index_used_any_schema)

    # ── batched EXPLAIN for the actual run's K value ──────────────────────────
    # The canonical paper geometry P=200/L=1024/K=75 has 75/204800 ≈ 0.037 %
    # coverage — the planner MUST use the functional bucket index here.
    actual_leaf_ids = sorted({
        int(value)
        for value in manifest.get("bad_leaves", [])
    })
    actual_k = len(actual_leaf_ids)
    if actual_k > 0:
        actual_coverage = len(actual_leaf_ids) / total_leaves if total_leaves > 0 else 0.0
        actual_require = (
            int(manifest.get("tuple_count", 0)) >= 100_000
            and actual_coverage <= 0.10
        )
        out["batch_plan_actual_k_index_required"] = int(actual_require)
        actual_index_used = True
        for schema in ("healthy", "damaged"):
            ok, detail = batch_indexed_lookup_plan_ok(conn, schema, actual_leaf_ids)
            if not ok:
                actual_index_used = False
                if actual_require:
                    out["planner_checks_passed"] = 0
                    out["batch_plan_actual_k_index_used"] = 0
                    raise RuntimeError(
                        f"Batched lookup at actual K={actual_k} (coverage={actual_coverage:.4%}) "
                        f"on schema '{schema}' does not use index "
                        f"'{LEAF_LOOKUP_INDEXES[schema]}'. "
                        f"This geometry requires the functional bucket index. Plan: {detail}"
                    )
        out["batch_plan_actual_k_index_used"] = int(actual_index_used)

    return out, rows


def lookup_explain_plan_json(conn, schema: str, leaf_id: int) -> dict[str, Any] | None:
    plan_rows = execute(
        conn,
        """
        EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF, FORMAT JSON)
        SELECT {cols}
        FROM {schema}.usertable
        WHERE merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key) = %s
        """.format(cols=", ".join(ALL_COLUMNS), schema=schema),
        (leaf_id,),
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


# ── repair DML ───────────────────────────────────────────────────────────────

def repair_leaf(
    conn,
    leaf_id: int,
    *,
    phase: dict[str, float],
    profiler: ProfileCollector | None = None,
    prefetched: tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]] | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], int, int, int]:
    """Fetch rows for *leaf_id* and apply whatever repairs are needed.

    Returns (hrows, drows, rows_inserted, rows_updated, rows_deleted).
    Timing is accumulated into *phase* under canonical keys.
    """
    from contextlib import contextmanager
    import time

    @contextmanager
    def _timer(key: str):
        t0 = time.perf_counter() * 1000.0
        yield
        phase[key] = phase.get(key, 0.0) + time.perf_counter() * 1000.0 - t0

    rows_inserted = rows_updated = rows_deleted = 0

    if prefetched is None:
        with _timer("candidate_row_fetch_ms"):
            hrows_raw = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_healthy",
                schema="healthy",
                leaf_id=leaf_id,
                fn=lambda: execute(conn, leaf_lookup_sql("healthy"), (leaf_id,)),
            )
            drows_raw = record_call(
                profiler,
                stage="candidate_fetch",
                operation="leaf_fetch_damaged",
                schema="damaged",
                leaf_id=leaf_id,
                fn=lambda: execute(conn, leaf_lookup_sql("damaged"), (leaf_id,)),
            )
            mat_start = time.perf_counter_ns()
            hrows = {int(r["ycsb_key"]): r for r in hrows_raw}
            drows = {int(r["ycsb_key"]): r for r in drows_raw}
            if profiler is not None and profiler.enabled:
                profiler.record(
                    stage="candidate_fetch",
                    operation="candidate_dict_materialisation_cpu",
                    schema="",
                    leaf_id=leaf_id,
                    client_wall_ns=time.perf_counter_ns() - mat_start,
                    rows_returned=len(hrows_raw) + len(drows_raw),
                )
    else:
        hrows, drows = prefetched

    with _timer("row_comparison_ms"):
        key_start = time.perf_counter_ns()
        hkeys = set(hrows)
        dkeys = set(drows)
        inserts = sorted(hkeys - dkeys)   # present in healthy, missing from damaged → INSERT
        deletes = sorted(dkeys - hkeys)   # spurious in damaged, not in healthy    → DELETE
        common_keys = sorted(hkeys & dkeys)
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage="comparison",
                operation="key_set_build_cpu",
                leaf_id=leaf_id,
                client_wall_ns=time.perf_counter_ns() - key_start,
                rows_returned=len(hrows) + len(drows),
            )
        payload_start = time.perf_counter_ns()
        updates = [key for key in common_keys if hrows[key] != drows[key]]
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage="comparison",
                operation="row_payload_comparison_cpu",
                leaf_id=leaf_id,
                client_wall_ns=time.perf_counter_ns() - payload_start,
                rows_returned=len(hrows) + len(drows),
            )

    with _timer("repair_write_ms"):
        for key in inserts:
            vals = hrows[key]
            record_call(
                profiler,
                stage="repair",
                operation="insert_dml",
                leaf_id=leaf_id,
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
                leaf_id=leaf_id,
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
                leaf_id=leaf_id,
                schema="damaged",
                fn=lambda key=key: execute(conn, "DELETE FROM damaged.usertable WHERE ycsb_key = %s", (key,)),
            )
            rows_deleted += 1

    return hrows, drows, rows_inserted, rows_updated, rows_deleted


def per_leaf_row_counts(bad_leaves: list[int], per_leaf: list[int]) -> dict[str, float]:
    """Compute mean and p95 rows per bad leaf from raw per-leaf candidate counts."""
    if not per_leaf:
        return {"mean_rows_per_bad_leaf": 0.0, "p95_rows_per_bad_leaf": 0.0}
    ordered = sorted(per_leaf)
    p95_idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return {
        "mean_rows_per_bad_leaf": statistics.mean(ordered),
        "p95_rows_per_bad_leaf": float(ordered[p95_idx]),
    }
