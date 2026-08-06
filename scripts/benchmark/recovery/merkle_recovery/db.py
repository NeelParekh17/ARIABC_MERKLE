"""Database connection utilities and low-level SQL helpers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


def connect(args: argparse.Namespace):
    """Open a psycopg3 connection with autocommit and dict row factory."""
    conn = psycopg.connect(args.dsn, autocommit=True, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        # Disable parallel workers: with 688K+ rows in merkle_node, the planner
        # may choose a parallel count(*) plan whose workers hang indefinitely
        # (ParallelFinish deadlock), blocking the entire benchmark run.
        cur.execute("SET max_parallel_workers_per_gather = 0")
    return conn


def execute(conn, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Execute SQL and return all rows as a list of dicts."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return []


def scalar(conn, sql: str, params: tuple[Any, ...] | None = None) -> Any:
    """Execute SQL and return the first column of the first row."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return next(iter(row.values()))


def run_file(conn, path: Path) -> None:
    """Execute a SQL file verbatim."""
    sql = path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)


def geometry(conn, schema: str = "healthy") -> dict[str, int]:
    """Return Merkle tree geometry for *schema*.usertable."""
    raw = scalar(conn, f"SELECT merkle_tree_stats('{schema}.usertable'::regclass)")
    data = json.loads(raw)
    fanout = int(data.get("fanout", 4))
    bits_per_split = max(1, (fanout - 1).bit_length())
    depth_rows = execute(
        conn,
        """
        SELECT coalesce(max(prefix_len), 0)::int AS max_prefix_len
        FROM ariabc_internal.merkle_node
        WHERE index_oid = %s::regclass
        """,
        (f"{schema}.usertable_merkle_idx",),
    )
    max_prefix_len = int(depth_rows[0]["max_prefix_len"]) if depth_rows else 0
    return {
        "fanout": fanout,
        "split_threshold": int(data.get("split_threshold", 32)),
        "merge_threshold": int(data.get("merge_threshold", 8)),
        "partitions": int(data.get("partitions", 200)),
        "total_nodes": int(data.get("total_nodes", 0)),
        "leaf_nodes": int(data.get("leaf_nodes", 0)),
        "max_prefix_len": max_prefix_len,
        "tree_depth": (max_prefix_len + bits_per_split - 1) // bits_per_split if max_prefix_len else 0,
    }


def partition_roots(conn, schema: str) -> dict[int, str]:
    """Fetch the complete partition-root vector from the native Merkle API."""
    rows = execute(
        conn,
        """
        SELECT partition, hash
        FROM merkle_get_partition_root_hashes(%s::regclass)
        ORDER BY partition
        """,
        (f"{schema}.usertable_merkle_idx",),
    )
    return {int(row["partition"]): str(row["hash"]) for row in rows}


def explain_json(conn, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Run EXPLAIN JSON and return the JSON payload rows."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return []


def show_setting(conn, name: str) -> Any:
    value = scalar(conn, f"SHOW {name}")
    return value.decode("ascii") if isinstance(value, bytes) else value


def merkle_node_index_stats(conn) -> list[dict[str, Any]]:
    """Return current cumulative stats for the localization catalog indexes.

    Localization runs inside the native SQL function, so its inner SPI query
    is not visible to the Python timing wrapper.  PostgreSQL's index and
    index-I/O statistics provide an inexpensive, server-side observation of
    the actual access path used by that query.  The caller must take a
    before/after snapshot around localization and compute the delta.
    """
    execute(conn, "SELECT pg_stat_clear_snapshot()")
    track_counts_enabled = str(show_setting(conn, "track_counts")).lower() == "on"
    return execute(
        conn,
        """
        SELECT
            s.indexrelname AS index_name,
            s.idx_scan,
            s.idx_tup_read,
            s.idx_tup_fetch,
            COALESCE(io.idx_blks_read, 0) AS idx_blks_read,
            COALESCE(io.idx_blks_hit, 0) AS idx_blks_hit,
            pg_relation_size(s.relid) AS relation_bytes,
            pg_relation_size(s.indexrelid) AS index_bytes,
            c.reltuples AS estimated_relation_rows,
            %s::boolean AS track_counts_enabled
        FROM pg_stat_all_indexes s
        JOIN pg_class c ON c.oid = s.relid
        LEFT JOIN pg_statio_all_indexes io
          ON io.schemaname = s.schemaname
         AND io.relname = s.relname
         AND io.indexrelname = s.indexrelname
        WHERE s.schemaname = 'ariabc_internal'
          AND s.relname = 'merkle_node'
        ORDER BY s.indexrelname
        """,
        (track_counts_enabled,),
    )


def diff_merkle_node_index_stats(
    before: list[dict[str, Any]], after: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return non-negative per-index deltas plus after-snapshot sizes."""
    before_by_name = {str(row["index_name"]): row for row in before}
    out: list[dict[str, Any]] = []
    counters = (
        "idx_scan",
        "idx_tup_read",
        "idx_tup_fetch",
        "idx_blks_read",
        "idx_blks_hit",
    )
    for row in after:
        name = str(row["index_name"])
        old = before_by_name.get(name, {})
        item: dict[str, Any] = {"index_name": name}
        for field in counters:
            current = int(row.get(field) or 0)
            previous = int(old.get(field) or 0)
            # Statistics can reset while a benchmark is running.  Preserve a
            # safe zero rather than reporting a negative access count.
            item[f"{field}_delta"] = max(0, current - previous)
        for field in ("relation_bytes", "index_bytes", "estimated_relation_rows", "track_counts_enabled"):
            item[field] = row.get(field, 0)
        out.append(item)
    return out
