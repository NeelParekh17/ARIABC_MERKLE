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
    return {
        "fanout": int(data.get("fanout", 4)),
        "split_threshold": int(data.get("split_threshold", 32)),
        "merge_threshold": int(data.get("merge_threshold", 8)),
        "total_nodes": int(data.get("total_nodes", 0)),
        "leaf_nodes": int(data.get("leaf_nodes", 0)),
    }


def explain_json(conn, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Run EXPLAIN JSON and return the JSON payload rows."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return []


def show_setting(conn, name: str) -> Any:
    return scalar(conn, f"SHOW {name}")
