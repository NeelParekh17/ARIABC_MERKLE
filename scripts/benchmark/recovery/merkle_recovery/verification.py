"""Post-repair verification and full audit logic."""

from __future__ import annotations

import json
from typing import Any

from .db import execute, scalar
from .localisation import detect_bad_leaves
from .repair import fetch_leaf_rows, seq_scan_delta, seq_scan_snapshot


def schema_fidelity_checks(conn, run_id: str, method: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(check_name: str, healthy_value: Any, damaged_value: Any) -> None:
        rows.append(
            {
                "run_id": run_id,
                "method": method,
                "check_name": check_name,
                "healthy_value": json.dumps(healthy_value, sort_keys=True, default=str),
                "damaged_value": json.dumps(damaged_value, sort_keys=True, default=str),
                "match": int(healthy_value == damaged_value),
            }
        )

    columns: dict[str, Any] = {}
    for schema in ("healthy", "damaged"):
        columns[schema] = execute(
            conn,
            """
            SELECT ordinal_position,
                   column_name,
                   data_type,
                   udt_name,
                   is_nullable,
                   column_default
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = 'usertable'
            ORDER BY ordinal_position
            """,
            (schema,),
        )
    add("columns", columns["healthy"], columns["damaged"])

    constraints: dict[str, Any] = {}
    for schema in ("healthy", "damaged"):
        constraints[schema] = execute(
            conn,
            """
            SELECT contype, pg_get_constraintdef(c.oid) AS def
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            JOIN pg_class r ON r.oid = c.conrelid
            WHERE n.nspname = %s
              AND r.relname = 'usertable'
            ORDER BY contype, def
            """,
            (schema,),
        )
    add("constraints", constraints["healthy"], constraints["damaged"])

    for index_name in ("usertable_pkey", "usertable_merkle_idx", "usertable_merkle_covering_idx"):
        definitions: dict[str, Any] = {}
        for schema in ("healthy", "damaged"):
            definition = scalar(
                conn,
                """
                SELECT regexp_replace(
                         regexp_replace(indexdef, ' ON (healthy|damaged)\\.', ' ON <schema>.'),
                         '(healthy|damaged)\\.', '<schema>.', 'g'
                       )
                FROM pg_indexes
                WHERE schemaname = %s
                  AND indexname = %s
                """,
                (schema, index_name),
            )
            definitions[schema] = definition
        add(f"index:{index_name}", definitions["healthy"], definitions["damaged"])

    return rows


def audit_recovery(conn, run_id: str, method: str) -> dict[str, Any]:
    """Full recovery audit: EXCEPT ALL comparison, Merkle root match, merkle_verify."""
    import time
    from contextlib import contextmanager

    phase: dict[str, float] = {}

    @contextmanager
    def _timer(key: str):
        t0 = time.perf_counter() * 1000.0
        yield
        phase[key] = phase.get(key, 0.0) + time.perf_counter() * 1000.0 - t0

    with _timer("audit_exact_table_compare_ms"):
        h_minus = scalar(
            conn,
            """
            SELECT count(*) FROM (
              SELECT * FROM healthy.usertable
              EXCEPT ALL
              SELECT * FROM damaged.usertable
            ) diff
            """,
        )
        d_minus = scalar(
            conn,
            """
            SELECT count(*) FROM (
              SELECT * FROM damaged.usertable
              EXCEPT ALL
              SELECT * FROM healthy.usertable
            ) diff
            """,
        )
    with _timer("audit_merkle_root_hash_ms"):
        roots_match = scalar(
            conn,
            "SELECT merkle_root_hash('healthy.usertable'::regclass) = merkle_root_hash('damaged.usertable'::regclass)",
        )
    with _timer("audit_merkle_verify_ms"):
        healthy_verify = scalar(conn, "SELECT merkle_verify('healthy.usertable'::regclass)")
        damaged_verify = scalar(conn, "SELECT merkle_verify('damaged.usertable'::regclass)")
    with _timer("audit_schema_fidelity_ms"):
        schema_rows = schema_fidelity_checks(conn, run_id, method)
        schema_ok = all(int(r["match"]) == 1 for r in schema_rows)

    index_count = scalar(
        conn,
        """
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'damaged'
          AND indexname IN ('usertable_pkey', 'usertable_merkle_idx', 'usertable_merkle_covering_idx')
        """,
    )
    return {
        "healthy_minus_damaged": int(h_minus),
        "damaged_minus_healthy": int(d_minus),
        "roots_match": bool(roots_match),
        "healthy_merkle_verify": bool(healthy_verify),
        "damaged_merkle_verify": bool(damaged_verify),
        "damaged_required_indexes": int(index_count),
        "audit_validation_ms": sum(phase.values()),
        "audit_phase": phase,
        "schema_fidelity_ok": bool(schema_ok),
        "schema_fidelity_rows": schema_rows,
        "ok": (
            int(h_minus) == 0
            and int(d_minus) == 0
            and bool(roots_match)
            and bool(healthy_verify)
            and bool(damaged_verify)
            and int(index_count) == 3
            and bool(schema_ok)
        ),
    }


def audit_recovery_with_scan_counters(
    conn,
    counters: dict[str, Any],
    run_id: str,
    method: str,
) -> dict[str, Any]:
    audit_scan_before = seq_scan_snapshot(conn)
    verified = audit_recovery(conn, run_id, method)
    audit_scan_after = seq_scan_snapshot(conn)
    counters.update(
        {
            "audit_user_table_seq_scan_delta": seq_scan_delta(audit_scan_before, audit_scan_after),
            "audit_merkle_root_hash_calls": 2,
            "audit_merkle_verify_calls": 2,
            "audit_validation_ms": verified["audit_validation_ms"],
            "schema_fidelity_ok": int(verified["schema_fidelity_ok"]),
        }
    )
    return verified
