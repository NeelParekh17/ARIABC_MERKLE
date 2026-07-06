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
    return psycopg.connect(args.dsn, autocommit=True, row_factory=dict_row)


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
        "partitions": int(data["num_partitions"]),
        "leaves_per_partition": int(data["leaves_per_partition"]),
        "fanout": int(data["fanout"]),
        "nodes_per_partition": int(data["nodes_per_partition"]),
    }
