"""Leaf-level candidate fetch and row-level repair DML.

All timing is done by the caller via the ``timer`` context manager.
"""

from __future__ import annotations

import json
import statistics
from typing import Any

from .config import ALL_COLUMNS, FIELDS, LEAF_LOOKUP_INDEXES
from .db import execute, scalar


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
    import hashlib

    plan_rows = execute(conn, leaf_lookup_sql(schema, explain=True), (leaf_id,))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = LEAF_LOOKUP_INDEXES[schema]
    ok = _plan_node_uses_index(plan_doc, index_name)
    return ok, json.dumps(plan_doc, default=str)


def run_planner_preflight(
    conn,
    manifest: dict[str, Any],
    run_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    import hashlib

    out: dict[str, Any] = {
        "planner_checked_leaf_count": len(manifest["bad_leaves"]),
        "planner_checks_passed": 1,
    }
    rows: list[dict[str, Any]] = []
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
    return out, rows


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

    with _timer("candidate_row_fetch_ms"):
        hrows = fetch_leaf_rows(conn, "healthy", leaf_id)
        drows = fetch_leaf_rows(conn, "damaged", leaf_id)

    with _timer("row_comparison_ms"):
        hkeys = set(hrows)
        dkeys = set(drows)
        inserts = sorted(hkeys - dkeys)   # present in healthy, missing from damaged → INSERT
        deletes = sorted(dkeys - hkeys)   # spurious in damaged, not in healthy    → DELETE
        updates = sorted(k for k in hkeys & dkeys if hrows[k] != drows[k])

    with _timer("repair_write_ms"):
        for key in inserts:
            vals = hrows[key]
            execute(
                conn,
                "INSERT INTO damaged.usertable VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                tuple(vals[c] for c in ALL_COLUMNS),
            )
            rows_inserted += 1
        for key in updates:
            vals = hrows[key]
            execute(
                conn,
                "UPDATE damaged.usertable SET field0=%s, field1=%s, field2=%s, field3=%s, field4=%s, "
                "field5=%s, field6=%s, field7=%s, field8=%s, field9=%s WHERE ycsb_key=%s",
                tuple(vals[f] for f in FIELDS) + (key,),
            )
            rows_updated += 1
        for key in deletes:
            execute(conn, "DELETE FROM damaged.usertable WHERE ycsb_key = %s", (key,))
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
