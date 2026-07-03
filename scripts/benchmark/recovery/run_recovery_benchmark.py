#!/usr/bin/env python3
"""Paper-style logical corruption recovery benchmark for AriaBC Merkle indexes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[3]
BENCH_DIR = Path(__file__).resolve().parent
RESULT_ROOT = BENCH_DIR / "results"
BENCHMARK_SCHEMA_VERSION = 2
TIMING_CONTRACT_VERSION = 1
ZERO_HASH = "0" * 64
FIELDS = [f"field{i}" for i in range(10)]
ALL_COLUMNS = ["ycsb_key", *FIELDS]
LEAF_LOOKUP_INDEXES = {
    "healthy": "usertable_leaf_lookup_idx",
    "damaged": "usertable_leaf_lookup_idx",
}


@dataclass
class Metrics:
    run_id: str
    experiment: str
    method: str
    tuple_count: int
    partitions: int
    leaves_per_partition: int
    fanout: int
    bad_leaf_count: int
    corrupted_tuple_count: int
    repetition: int
    valid: bool = True
    warning: str = ""
    paper_style_total_ms: float = 0.0
    restore_repair_ms: float = 0.0
    audit_validation_ms: float = 0.0
    end_to_end_observed_ms: float = 0.0
    cleanup_ms: float = 0.0
    phase: dict[str, float] = field(default_factory=dict)
    counters: dict[str, Any] = field(default_factory=dict)


def add_warning(m: Metrics, msg: str):
    m.warning = (m.warning + "; " if m.warning else "") + msg
    m.valid = False


def finalize_metrics(
    m: Metrics,
    *,
    total_start_ms: float,
    paper_start_ms: float,
    paper_end_ms: float,
    recovery_start_ms: float,
    recovery_end_ms: float,
    audit_start_ms: float,
    audit_end_ms: float,
    cleanup_end_ms: float,
) -> None:
    m.paper_style_total_ms = paper_end_ms - paper_start_ms
    m.restore_repair_ms = recovery_end_ms - recovery_start_ms
    m.audit_validation_ms = audit_end_ms - audit_start_ms
    m.end_to_end_observed_ms = cleanup_end_ms - total_start_ms
    m.cleanup_ms = max(0.0, cleanup_end_ms - audit_end_ms)
    m.counters.update(
        {
            "paper_end_before_audit_start": int(paper_end_ms <= audit_start_ms),
            "audit_validation_positive": int(m.audit_validation_ms > 0),
            "end_to_end_covers_paper_and_audit": int(
                m.end_to_end_observed_ms + 1e-6 >= m.paper_style_total_ms + m.audit_validation_ms
            ),
        }
    )
    if paper_end_ms > audit_start_ms:
        add_warning(m, "paper timing overlaps audit")
    if m.audit_validation_ms <= 0:
        add_warning(m, "audit timing is not positive")
    if m.end_to_end_observed_ms + 1e-6 < m.paper_style_total_ms + m.audit_validation_ms:
        add_warning(m, "end-to-end timing does not cover paper plus audit")


def now_ms() -> float:
    return time.perf_counter() * 1000.0


def emit_progress(result_dir: Path, **event: object) -> None:
    event["timestamp_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    line = json.dumps(event, sort_keys=True, default=str)

    with (result_dir / "progress.jsonl").open("a") as f:
        f.write(line + "\n")
        f.flush()

    (result_dir / "progress.json").write_text(
        json.dumps(event, indent=2, sort_keys=True, default=str) + "\n"
    )

    # main() redirects normal stdout into scratch/stdout.log; use the original
    # stream so the synced remote launcher can tee live progress to the terminal.
    print(f"[progress] {line}", file=sys.__stdout__, flush=True)


@contextmanager
def timer(store: dict[str, float], name: str):
    start = now_ms()
    yield
    store[name] = store.get(name, 0.0) + now_ms() - start


def connect(args: argparse.Namespace):
    return psycopg.connect(args.dsn, autocommit=True, row_factory=dict_row)


def execute(conn, sql: str, params: tuple[Any, ...] | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return []


def scalar(conn, sql: str, params: tuple[Any, ...] | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return next(iter(row.values()))


def run_file(conn, path: Path):
    sql = path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)


def row_expr(schema: str) -> str:
    return f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"


def leaf_lookup_sql(schema: str, explain: bool = False) -> str:
    prefix = "EXPLAIN (FORMAT JSON) " if explain else ""
    return (
        f"{prefix}SELECT {', '.join(ALL_COLUMNS)} "
        f"FROM {schema}.usertable "
        f"WHERE {row_expr(schema)} = %s"
    )


def qualified(schema: str) -> str:
    return f"{schema}.usertable"


def ensure_helpers(conn):
    run_file(conn, BENCH_DIR / "sql" / "recovery_helpers.sql")


def recreate_schema(conn):
    run_file(conn, BENCH_DIR / "create_schema.sql")


def create_merkle_indexes(conn, partitions: int, leaves_per_partition: int, fanout: int):
    sql = (BENCH_DIR / "create_merkle_indexes.sql").read_text()
    sql = sql.replace(":partitions", str(partitions))
    sql = sql.replace(":leaves_per_partition", str(leaves_per_partition))
    sql = sql.replace(":fanout", str(fanout))
    with conn.cursor() as cur:
        cur.execute(sql)


def build_dataset(conn, tuple_count: int, partitions: int, leaves_per_partition: int, fanout: int):
    recreate_schema(conn)
    execute(
        conn,
        """
        INSERT INTO healthy.usertable
        SELECT gs::bigint,
               'field0-' || gs,
               'field1-' || gs,
               'field2-' || gs,
               'field3-' || gs,
               'field4-' || gs,
               'field5-' || gs,
               'field6-' || gs,
               'field7-' || gs,
               'field8-' || gs,
               'field9-' || gs
        FROM generate_series(1, %s) AS gs
        """,
        (tuple_count,),
    )
    execute(conn, "INSERT INTO damaged.usertable SELECT * FROM healthy.usertable")
    create_merkle_indexes(conn, partitions, leaves_per_partition, fanout)


def create_damaged_indexes(conn, partitions: int, leaves_per_partition: int, fanout: int):
    execute(conn, "DROP INDEX IF EXISTS damaged.usertable_leaf_lookup_idx")
    execute(conn, "DROP INDEX IF EXISTS damaged.usertable_merkle_idx")
    execute(conn, "ALTER TABLE damaged.usertable ADD PRIMARY KEY (ycsb_key)")
    execute(
        conn,
        f"""
        CREATE INDEX usertable_merkle_idx
        ON damaged.usertable USING merkle (ycsb_key)
        WITH (partitions = {partitions}, leaves_per_partition = {leaves_per_partition}, fanout = {fanout})
        """,
    )
    execute(
        conn,
        "CREATE INDEX usertable_leaf_lookup_idx ON damaged.usertable "
        "((merkle_bucket_for_key('damaged.usertable_merkle_idx'::regclass, ycsb_key)))",
    )
    execute(conn, "ANALYZE damaged.usertable")


def reset_damaged_from_healthy(conn, cfg: dict[str, int], manifest: dict[str, Any]):
    execute(conn, "DROP TABLE IF EXISTS damaged.usertable CASCADE")
    execute(conn, "CREATE TABLE damaged.usertable (LIKE healthy.usertable INCLUDING DEFAULTS)")
    execute(conn, "INSERT INTO damaged.usertable SELECT * FROM healthy.usertable")
    create_damaged_indexes(conn, cfg["partitions"], cfg["leaves_per_partition"], cfg["fanout"])
    apply_corruption(conn, manifest)


def leaf_occupancy(conn) -> list[dict[str, Any]]:
    return execute(
        conn,
        f"""
        SELECT {row_expr('healthy')}::bigint AS leaf_id, count(*)::bigint AS tuple_count
        FROM healthy.usertable
        GROUP BY 1
        ORDER BY 1
        """,
    )


def occupancy_stats(rows: list[dict[str, Any]]) -> dict[str, float]:
    counts = [int(r["tuple_count"]) for r in rows]
    if not counts:
        return {k: 0.0 for k in ["minimum", "p50", "p95", "p99", "maximum", "mean", "stddev"]}
    ordered = sorted(counts)

    def pct(p: float) -> float:
        idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * p)))
        return float(ordered[idx])

    return {
        "minimum": float(min(ordered)),
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "maximum": float(max(ordered)),
        "mean": float(statistics.mean(ordered)),
        "stddev": float(statistics.pstdev(ordered)) if len(ordered) > 1 else 0.0,
    }


def choose_corruption_manifest(
    conn,
    experiment: str,
    tuple_count: int,
    partitions: int,
    leaves_per_partition: int,
    fanout: int,
    bad_leaf_count: int,
    corrupted_tuple_count: int,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed + tuple_count * 31 + partitions * 17 + bad_leaf_count)
    occ = leaf_occupancy(conn)
    eligible = [int(r["leaf_id"]) for r in occ if int(r["tuple_count"]) > 0]
    if len(eligible) < bad_leaf_count:
        raise RuntimeError(f"only {len(eligible)} non-empty leaves available, need {bad_leaf_count}")
    leaves = sorted(rng.sample(eligible, bad_leaf_count))
    base = corrupted_tuple_count // bad_leaf_count
    rem = corrupted_tuple_count % bad_leaf_count
    entries: list[dict[str, Any]] = []
    for pos, leaf_id in enumerate(leaves):
        want = base + (1 if pos < rem else 0)
        keys = execute(
            conn,
            f"""
            SELECT ycsb_key
            FROM healthy.usertable
            WHERE {row_expr('healthy')} = %s
            ORDER BY ycsb_key
            LIMIT %s
            """,
            (leaf_id, want),
        )
        if len(keys) < want:
            raise RuntimeError(f"leaf {leaf_id} has {len(keys)} rows, need {want}")
        for row in keys:
            entries.append({"leaf_id": leaf_id, "ycsb_key": int(row["ycsb_key"])})
    return {
        "experiment": experiment,
        "tuple_count": tuple_count,
        "partitions": partitions,
        "leaves_per_partition": leaves_per_partition,
        "fanout": fanout,
        "seed": seed,
        "bad_leaves": leaves,
        "corruptions": entries,
    }


def apply_corruption(conn, manifest: dict[str, Any]):
    for entry in manifest["corruptions"]:
        execute(
            conn,
            "UPDATE damaged.usertable SET field9 = public.recovery_corrupted_value(ycsb_key, %s) WHERE ycsb_key = %s",
            (manifest["seed"], entry["ycsb_key"]),
        )


def validate_manifest_leaf_mapping(conn, manifest: dict[str, Any]):
    mismatches: list[dict[str, Any]] = []
    for entry in manifest["corruptions"]:
        actual = scalar(
            conn,
            "SELECT merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, %s)",
            (entry["ycsb_key"],),
        )
        if int(actual) != int(entry["leaf_id"]):
            mismatches.append({"ycsb_key": entry["ycsb_key"], "expected": entry["leaf_id"], "actual": int(actual)})
    if mismatches:
        raise RuntimeError(f"corruption manifest rows do not map to intended leaves: {mismatches[:5]}")


def geometry(conn, schema: str = "healthy") -> dict[str, int]:
    raw = scalar(conn, f"SELECT merkle_tree_stats('{schema}.usertable'::regclass)")
    data = json.loads(raw)
    return {
        "partitions": int(data["num_partitions"]),
        "leaves_per_partition": int(data["leaves_per_partition"]),
        "fanout": int(data["fanout"]),
        "nodes_per_partition": int(data["nodes_per_partition"]),
    }


def detect_bad_leaves(conn, counters: dict[str, Any], *, prefix: str = "") -> list[int]:
    geo = geometry(conn)
    bad: list[int] = []
    counters[f"{prefix}partition_root_batches"] = 2
    counters[f"{prefix}partition_root_nodes_read"] = geo["partitions"] * 2
    counters[f"{prefix}child_hash_sql_calls"] = 0
    counters[f"{prefix}child_hash_nodes_read"] = 0
    counters[f"{prefix}leaf_nodes_found"] = 0
    leaf_start = geo["nodes_per_partition"] - geo["leaves_per_partition"] + 1

    def compare_node(partition: int, node: int):
        if node >= leaf_start:
            leaf_id = partition * geo["leaves_per_partition"] + (node - leaf_start)
            bad.append(leaf_id)
            counters[f"{prefix}leaf_nodes_found"] += 1
            return
        healthy_children = execute(
            conn,
            "SELECT child_node_in_partition, hash FROM merkle_get_child_hashes('healthy.usertable_merkle_idx'::regclass, %s, %s)",
            (partition, node),
        )
        damaged_rows = execute(
            conn,
            "SELECT child_node_in_partition, hash FROM merkle_get_child_hashes('damaged.usertable_merkle_idx'::regclass, %s, %s)",
            (partition, node),
        )
        damaged_children = {int(r["child_node_in_partition"]): r["hash"] for r in damaged_rows}
        counters[f"{prefix}child_hash_sql_calls"] += 2
        counters[f"{prefix}child_hash_nodes_read"] += len(healthy_children) + len(damaged_rows)
        for child in healthy_children:
            child_node = int(child["child_node_in_partition"])
            if child["hash"] != damaged_children[child_node]:
                compare_node(partition, child_node)

    healthy_roots = {
        int(r["partition"]): r["hash"]
        for r in execute(conn, "SELECT * FROM merkle_get_partition_root_hashes('healthy.usertable_merkle_idx'::regclass)")
    }
    damaged_roots = {
        int(r["partition"]): r["hash"]
        for r in execute(conn, "SELECT * FROM merkle_get_partition_root_hashes('damaged.usertable_merkle_idx'::regclass)")
    }
    for partition, healthy_hash in healthy_roots.items():
        if healthy_hash != damaged_roots.get(partition):
            compare_node(partition, 1)
    return sorted(bad)


def plan_node_uses_index(node: Any, index_name: str) -> bool:
    if isinstance(node, dict):
        if node.get("Index Name") == index_name:
            return True
        return any(plan_node_uses_index(value, index_name) for value in node.values())
    if isinstance(node, list):
        return any(plan_node_uses_index(value, index_name) for value in node)
    return False


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def indexed_lookup_plan_ok(conn, schema: str, leaf_id: int) -> tuple[bool, str]:
    plan_rows = execute(conn, leaf_lookup_sql(schema, explain=True), (leaf_id,))
    if not plan_rows:
        return False, "empty EXPLAIN output"
    plan_doc = plan_rows[0].get("QUERY PLAN")
    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    index_name = LEAF_LOOKUP_INDEXES[schema]
    ok = plan_node_uses_index(plan_doc, index_name)
    return ok, json.dumps(plan_doc, default=str)


def run_planner_preflight(conn, manifest: dict[str, Any], run_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
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
                raise RuntimeError(f"{schema}.{LEAF_LOOKUP_INDEXES[schema]} not found for planner preflight")
            idx = index_row[0]
            row = {
                "run_id": run_id,
                "schema": schema,
                "leaf_id": leaf_id,
                "index_oid": int(idx["index_oid"]),
                "index_relfilenode": int(idx["index_relfilenode"]),
                "index_definition": idx["index_definition"],
                "plan_uses_expected_leaf_lookup_index": int(ok),
                "plan_json_sha256": sha256_text(detail),
            }
            rows.append(row)
            if not ok:
                out["planner_checks_passed"] = 0
                raise RuntimeError(
                    f"{schema} timed leaf lookup for leaf {leaf_id} does not use "
                    f"{schema}.{LEAF_LOOKUP_INDEXES[schema]}: {detail}"
                )
    return out, rows


def fetch_leaf_rows(conn, schema: str, leaf_id: int) -> dict[int, dict[str, Any]]:
    rows = execute(conn, leaf_lookup_sql(schema), (leaf_id,))
    return {int(r["ycsb_key"]): r for r in rows}


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


def schema_fidelity_checks(conn, run_id: str, method: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(check_name: str, healthy_value: Any, damaged_value: Any):
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

    columns = {}
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

    constraints = {}
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

    for index_name in ("usertable_pkey", "usertable_merkle_idx", "usertable_leaf_lookup_idx"):
        definitions = {}
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
    phase: dict[str, float] = {}
    with timer(phase, "audit_exact_table_compare_ms"):
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
    with timer(phase, "audit_merkle_root_hash_ms"):
        roots_match = scalar(
            conn,
            "SELECT merkle_root_hash('healthy.usertable'::regclass) = merkle_root_hash('damaged.usertable'::regclass)",
        )
    with timer(phase, "audit_merkle_verify_ms"):
        healthy_verify = scalar(conn, "SELECT merkle_verify('healthy.usertable'::regclass)")
        damaged_verify = scalar(conn, "SELECT merkle_verify('damaged.usertable'::regclass)")
    with timer(phase, "audit_schema_fidelity_ms"):
        schema_rows = schema_fidelity_checks(conn, run_id, method)
        schema_ok = all(int(r["match"]) == 1 for r in schema_rows)
    index_count = scalar(
        conn,
        """
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'damaged'
          AND indexname IN ('usertable_pkey', 'usertable_merkle_idx', 'usertable_leaf_lookup_idx')
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
        "ok": int(h_minus) == 0 and int(d_minus) == 0 and bool(roots_match) and bool(healthy_verify) and bool(damaged_verify) and int(index_count) == 3 and bool(schema_ok),
    }


def audit_recovery_with_scan_counters(conn, counters: dict[str, Any], run_id: str, method: str) -> dict[str, Any]:
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


def verify_recovery(conn) -> dict[str, Any]:
    result = audit_recovery(conn, "manual-verify", "manual")
    result.pop("audit_phase", None)
    return result


def repair_merkle(
    conn,
    manifest: dict[str, Any],
    tuple_count: int,
    repetition: int,
    planner_results: dict[str, Any],
    schema_rows_out: list[dict[str, Any]],
) -> Metrics:
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    run_base = f"{manifest['experiment']}-n{tuple_count}-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
    m = Metrics(
        run_id=f"{run_base}-merkle-r{repetition}",
        experiment=manifest["experiment"],
        method="merkle",
        tuple_count=tuple_count,
        bad_leaf_count=len(manifest["bad_leaves"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        **cfg,
    )
    total_start = now_ms()
    paper_start = total_start
    recovery_scan_before = seq_scan_snapshot(conn)
    counters = m.counters
    counters.update(planner_results)

    with timer(m.phase, "tree_localisation_ms"):
        bad_leaves = detect_bad_leaves(conn, counters)
    if bad_leaves != sorted(manifest["bad_leaves"]):
        add_warning(m, f"bad leaves mismatch expected={manifest['bad_leaves']} actual={bad_leaves}")

    rows_inserted = rows_updated = rows_deleted = 0
    candidate_rows = healthy_rows = damaged_rows = 0
    lookup_scans = 0
    for leaf_id in bad_leaves:
        with timer(m.phase, "candidate_row_fetch_ms"):
            lookup_scans += 2
            hrows = fetch_leaf_rows(conn, "healthy", leaf_id)
            drows = fetch_leaf_rows(conn, "damaged", leaf_id)
            healthy_rows += len(hrows)
            damaged_rows += len(drows)
            candidate_rows += len(hrows) + len(drows)
        with timer(m.phase, "row_comparison_ms"):
            hkeys = set(hrows)
            dkeys = set(drows)
            inserts = sorted(hkeys - dkeys)
            deletes = sorted(dkeys - hkeys)
            updates = sorted(k for k in hkeys & dkeys if hrows[k] != drows[k])
        with timer(m.phase, "repair_write_ms"):
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

    with timer(m.phase, "targeted_post_repair_confirmation_ms"):
        post_repair_counters: dict[str, Any] = {}
        remaining_bad_leaves = detect_bad_leaves(conn, post_repair_counters, prefix="targeted_confirmation_")
        repaired_leaf_mismatch = False
        for leaf_id in bad_leaves:
            if fetch_leaf_rows(conn, "healthy", leaf_id) != fetch_leaf_rows(conn, "damaged", leaf_id):
                repaired_leaf_mismatch = True
                break
    recovery_end = now_ms()
    paper_end = recovery_end
    recovery_scan_after = seq_scan_snapshot(conn)
    recovery_full_heap_scans = seq_scan_delta(recovery_scan_before, recovery_scan_after)

    audit_start = now_ms()
    verified = audit_recovery_with_scan_counters(conn, counters, m.run_id, m.method)
    audit_end = now_ms()
    schema_rows_out.extend(verified["schema_fidelity_rows"])
    m.phase.update(verified["audit_phase"])
    counters.update(
        {
            "leaf_lookup_sql_calls": lookup_scans,
            "candidate_rows_fetched": candidate_rows,
            "healthy_rows_fetched": healthy_rows,
            "damaged_rows_fetched": damaged_rows,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "targeted_confirmation_root_batches": post_repair_counters.get("targeted_confirmation_partition_root_batches", 0),
            "targeted_confirmation_root_nodes_read": post_repair_counters.get("targeted_confirmation_partition_root_nodes_read", 0),
            "recovery_user_table_seq_scan_delta": recovery_full_heap_scans,
            "partition_root_batches_ok": int(counters.get("partition_root_batches") == 2),
        }
    )
    if candidate_rows >= 0.5 * tuple_count:
        add_warning(m, "candidate rows exceed sparse threshold")
    if recovery_full_heap_scans != 0:
        add_warning(m, "recovery performed heap sequential scan")
    if remaining_bad_leaves or repaired_leaf_mismatch:
        add_warning(m, "targeted post-repair confirmation failed")
    if counters.get("partition_root_batches") != 2:
        add_warning(m, "partition root detection used more than two batches")
    if not verified["ok"]:
        add_warning(m, f"verification failed {verified}")
    cleanup_end = now_ms()
    finalize_metrics(
        m,
        total_start_ms=total_start,
        paper_start_ms=paper_start,
        paper_end_ms=paper_end,
        recovery_start_ms=paper_start,
        recovery_end_ms=recovery_end,
        audit_start_ms=audit_start,
        audit_end_ms=audit_end,
        cleanup_end_ms=cleanup_end,
    )
    m.phase["merkle_total_ms"] = m.end_to_end_observed_ms
    return m


def rebuild_indexes_for_table(conn, schema: str, cfg: dict[str, int], phase: dict[str, float], prefix: str):
    with timer(phase, f"{prefix}_primary_index_build_ms"):
        execute(conn, f"ALTER TABLE {schema}.usertable ADD PRIMARY KEY (ycsb_key)")
    with timer(phase, f"{prefix}_merkle_index_build_ms"):
        execute(
            conn,
            f"""
            CREATE INDEX usertable_merkle_idx
            ON {schema}.usertable USING merkle (ycsb_key)
            WITH (partitions = {cfg['partitions']}, leaves_per_partition = {cfg['leaves_per_partition']}, fanout = {cfg['fanout']})
            """,
        )
    with timer(phase, f"{prefix}_leaf_lookup_index_build_ms"):
        execute(
            conn,
            f"CREATE INDEX usertable_leaf_lookup_idx ON {schema}.usertable "
            f"((merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)))",
        )
    execute(conn, f"ANALYZE {schema}.usertable")


def swap_recovered(conn, recovered_schema: str, phase: dict[str, float], prefix: str):
    with timer(phase, f"{prefix}_table_swap_ms"):
        execute(conn, "DROP TABLE damaged.usertable CASCADE")
        execute(conn, f"ALTER TABLE {recovered_schema}.usertable SET SCHEMA damaged")
        execute(conn, f"DROP SCHEMA {recovered_schema}")


def repair_cta(
    conn,
    manifest: dict[str, Any],
    tuple_count: int,
    repetition: int,
    schema_rows_out: list[dict[str, Any]],
) -> Metrics:
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    run_base = f"{manifest['experiment']}-n{tuple_count}-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
    m = Metrics(
        run_id=f"{run_base}-cta-r{repetition}",
        experiment=manifest["experiment"],
        method="cta",
        tuple_count=tuple_count,
        bad_leaf_count=len(manifest["bad_leaves"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        **cfg,
    )
    total_start = now_ms()
    execute(conn, "DROP SCHEMA IF EXISTS recovery_cta_snapshot CASCADE")
    execute(conn, "DROP SCHEMA IF EXISTS recovery_cta CASCADE")
    paper_start = now_ms()
    with timer(m.phase, "cta_snapshot_create_ms"):
        execute(conn, "CREATE SCHEMA recovery_cta_snapshot")
        execute(conn, "CREATE TABLE recovery_cta_snapshot.usertable AS TABLE healthy.usertable")
    recovery_scan_before = seq_scan_snapshot(conn)
    recovery_start = now_ms()
    with timer(m.phase, "cta_full_data_copy_ms"):
        execute(conn, "DROP SCHEMA IF EXISTS recovery_cta CASCADE")
        execute(conn, "CREATE SCHEMA recovery_cta")
        execute(conn, "CREATE TABLE recovery_cta.usertable (LIKE healthy.usertable INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
        execute(conn, "INSERT INTO recovery_cta.usertable SELECT * FROM recovery_cta_snapshot.usertable")
    rebuild_indexes_for_table(conn, "recovery_cta", cfg, m.phase, "cta")
    swap_recovered(conn, "recovery_cta", m.phase, "cta")
    recovery_end = now_ms()
    paper_end = recovery_end
    recovery_scan_after = seq_scan_snapshot(conn)
    m.counters.update(
        {
            "recovery_user_table_seq_scan_delta": seq_scan_delta(recovery_scan_before, recovery_scan_after),
        }
    )
    audit_start = now_ms()
    verified = audit_recovery_with_scan_counters(conn, m.counters, m.run_id, m.method)
    audit_end = now_ms()
    schema_rows_out.extend(verified["schema_fidelity_rows"])
    m.phase.update(verified["audit_phase"])
    if not verified["ok"]:
        add_warning(m, f"verification failed {verified}")
    execute(conn, "DROP SCHEMA IF EXISTS recovery_cta_snapshot CASCADE")
    cleanup_end = now_ms()
    finalize_metrics(
        m,
        total_start_ms=total_start,
        paper_start_ms=paper_start,
        paper_end_ms=paper_end,
        recovery_start_ms=recovery_start,
        recovery_end_ms=recovery_end,
        audit_start_ms=audit_start,
        audit_end_ms=audit_end,
        cleanup_end_ms=cleanup_end,
    )
    m.phase["cta_total_ms"] = m.end_to_end_observed_ms
    return m


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def repair_disk(
    conn,
    manifest: dict[str, Any],
    tuple_count: int,
    repetition: int,
    scratch_dir: Path,
    keep_disk_snapshots: bool,
    schema_rows_out: list[dict[str, Any]],
) -> Metrics:
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    run_base = f"{manifest['experiment']}-n{tuple_count}-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
    m = Metrics(
        run_id=f"{run_base}-disk-r{repetition}",
        experiment=manifest["experiment"],
        method="disk",
        tuple_count=tuple_count,
        bad_leaf_count=len(manifest["bad_leaves"]),
        corrupted_tuple_count=len(manifest["corruptions"]),
        repetition=repetition,
        **cfg,
    )
    snapshot_dir = scratch_dir / "disk_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot = snapshot_dir / f"{m.run_id}.copybin"
    total_start = now_ms()
    paper_start = total_start
    paper_end = paper_start
    recovery_start = paper_start
    recovery_end = paper_start
    audit_start = paper_start
    audit_end = paper_start
    try:
        with timer(m.phase, "disk_snapshot_write_ms"):
            with snapshot.open("wb") as f:
                with conn.cursor() as cur:
                    with cur.copy("COPY healthy.usertable TO STDOUT WITH (FORMAT binary)") as copy:
                        for data in copy:
                            f.write(data)
        m.counters["disk_snapshot_file_bytes"] = snapshot.stat().st_size
        m.counters["disk_snapshot_sha256"] = file_sha256(snapshot)
        m.counters["disk_snapshot_retained"] = int(keep_disk_snapshots)
        recovery_scan_before = seq_scan_snapshot(conn)
        recovery_start = now_ms()
        with timer(m.phase, "disk_restore_copy_ms"):
            execute(conn, "DROP SCHEMA IF EXISTS recovery_disk CASCADE")
            execute(conn, "CREATE SCHEMA recovery_disk")
            execute(conn, "CREATE TABLE recovery_disk.usertable (LIKE healthy.usertable INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
            with snapshot.open("rb") as f:
                with conn.cursor() as cur:
                    with cur.copy("COPY recovery_disk.usertable FROM STDIN WITH (FORMAT binary)") as copy:
                        while True:
                            data = f.read(1024 * 1024)
                            if not data:
                                break
                            copy.write(data)
        rebuild_indexes_for_table(conn, "recovery_disk", cfg, m.phase, "disk")
        swap_recovered(conn, "recovery_disk", m.phase, "disk")
        recovery_end = now_ms()
        paper_end = recovery_end
        recovery_scan_after = seq_scan_snapshot(conn)
        m.counters.update(
            {
                "recovery_user_table_seq_scan_delta": seq_scan_delta(recovery_scan_before, recovery_scan_after),
            }
        )
        audit_start = now_ms()
        verified = audit_recovery_with_scan_counters(conn, m.counters, m.run_id, m.method)
        audit_end = now_ms()
        schema_rows_out.extend(verified["schema_fidelity_rows"])
        m.phase.update(verified["audit_phase"])
        if not verified["ok"]:
            add_warning(m, f"verification failed {verified}")
    finally:
        if not keep_disk_snapshots:
            snapshot.unlink(missing_ok=True)
    cleanup_end = now_ms()
    if audit_end == paper_start:
        audit_end = cleanup_end
    if recovery_end == paper_start:
        recovery_end = cleanup_end
    if paper_end == paper_start:
        paper_end = recovery_end
    if recovery_start == paper_start and recovery_end == paper_start:
        recovery_start = paper_start
    if audit_start == paper_start and audit_end == paper_start:
        audit_start = cleanup_end
    finalize_metrics(
        m,
        total_start_ms=total_start,
        paper_start_ms=paper_start,
        paper_end_ms=paper_end,
        recovery_start_ms=recovery_start,
        recovery_end_ms=recovery_end,
        audit_start_ms=audit_start,
        audit_end_ms=audit_end,
        cleanup_end_ms=cleanup_end,
    )
    m.phase["disk_total_ms"] = m.end_to_end_observed_ms
    return m

def table_sizes(conn) -> dict[str, int]:
    return {
        "tuple_count": int(scalar(conn, "SELECT count(*) FROM healthy.usertable")),
        "base_table_bytes": int(scalar(conn, "SELECT pg_relation_size('healthy.usertable'::regclass)")),
        "primary_index_bytes": int(scalar(conn, "SELECT pg_relation_size('healthy.usertable_pkey'::regclass)")),
        "merkle_index_bytes": int(scalar(conn, "SELECT pg_relation_size('healthy.usertable_merkle_idx'::regclass)")),
        "leaf_lookup_index_bytes": int(scalar(conn, "SELECT pg_relation_size('healthy.usertable_leaf_lookup_idx'::regclass)")),
        "total_schema_bytes": int(scalar(conn, "SELECT pg_total_relation_size('healthy.usertable'::regclass)")),
    }


def bucket_consistency_sample(
    conn,
    tuple_count: int,
    partitions: int,
    leaves_per_partition: int,
    fanout: int,
    seed: int,
    sample_size: int = 10_000,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = execute(
        conn,
        """
        SELECT ycsb_key,
               merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, ycsb_key)::bigint AS bucket,
               (merkle_leaf_id('healthy.usertable'::regclass, ycsb_key)).leaf_id::bigint AS leaf_id
        FROM healthy.usertable
        ORDER BY ((ycsb_key * 1103515245 + 12345) %% 2147483647)
        LIMIT %s
        """,
        (min(sample_size, tuple_count),),
    )
    out: list[dict[str, Any]] = []
    mismatch_count = 0
    for row in rows:
        match = int(row["bucket"]) == int(row["leaf_id"])
        if not match:
            mismatch_count += 1
        out.append(
            {
                "tuple_count": tuple_count,
                "partitions": partitions,
                "leaves_per_partition": leaves_per_partition,
                "fanout": fanout,
                "ycsb_key": int(row["ycsb_key"]),
                "bucket": int(row["bucket"]),
                "leaf_id": int(row["leaf_id"]),
                "match": int(match),
            }
        )
    if mismatch_count:
        raise RuntimeError(f"merkle_bucket_for_key disagreed with merkle_leaf_id for {mismatch_count} sampled keys")
    sample_digest = sha256_text(json.dumps(out, sort_keys=True))
    return (
        {
            "tuple_count": tuple_count,
            "partitions": partitions,
            "leaves_per_partition": leaves_per_partition,
            "fanout": fanout,
            "sample_count": len(out),
            "sample_seed": seed,
            "mismatch_count": mismatch_count,
            "sample_digest": sample_digest,
        },
        out,
    )


def write_environment(result_dir: Path, args: argparse.Namespace):
    lines = [
        f"timestamp={datetime.now().isoformat()}",
        f"cwd={ROOT}",
        f"python={sys.version.replace(os.linesep, ' ')}",
        f"platform={platform.platform()}",
        f"dsn={args.dsn}",
    ]
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        lines.append(f"git_head={head}")
    except Exception as exc:
        lines.append(f"git_head_error={exc}")
    (result_dir / "environment.txt").write_text("\n".join(lines) + "\n")


def write_python_environment(result_dir: Path):
    try:
        psycopg_version = psycopg.__version__
    except Exception as exc:
        psycopg_version = f"error:{exc}"
    payload = {
        "python_executable": sys.executable,
        "python_version": sys.version.replace(os.linesep, " "),
        "platform": platform.platform(),
        "psycopg_version": psycopg_version,
        "working_directory": str(Path.cwd()),
        "benchmark_script": str(Path(__file__).resolve()),
    }
    (result_dir / "python_environment.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def metrics_to_rows(metrics: list[Metrics]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    run_rows: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []
    for m in metrics:
        row = {
            "run_id": m.run_id,
            "experiment": m.experiment,
            "method": m.method,
            "tuple_count": m.tuple_count,
            "partitions": m.partitions,
            "leaves_per_partition": m.leaves_per_partition,
            "fanout": m.fanout,
            "bad_leaf_count": m.bad_leaf_count,
            "corrupted_tuple_count": m.corrupted_tuple_count,
            "repetition": m.repetition,
            "valid": int(m.valid),
            "warning": m.warning,
            "paper_style_total_ms": f"{m.paper_style_total_ms:.3f}",
            "restore_repair_ms": f"{m.restore_repair_ms:.3f}",
            "audit_validation_ms": f"{m.audit_validation_ms:.3f}",
            "end_to_end_observed_ms": f"{m.end_to_end_observed_ms:.3f}",
            "cleanup_ms": f"{m.cleanup_ms:.3f}",
            "paper_total_ms": f"{m.paper_style_total_ms:.3f}",
            "recovery_only_ms": f"{m.restore_repair_ms:.3f}",
            "total_ms": f"{m.end_to_end_observed_ms:.3f}",
        }
        row.update(m.counters)
        run_rows.append(row)
        for phase, value in m.phase.items():
            phase_rows.append({"run_id": m.run_id, "method": m.method, "phase": phase, "ms": f"{value:.3f}"})
    return run_rows, phase_rows


def profile_config(profile: str):
    if profile == "paper":
        return {
            "fig12_sizes": [1_000_000, 3_000_000, 5_000_000],
            "fig13_sizes": [3_000_000],
            "fig13_k": list(range(1, 11)),
            "repetitions": 5,
        }
    if profile == "preflight":
        return {
            "fig12_sizes": [1_000_000],
            "fig13_sizes": [1_000_000],
            "fig13_k": [1, 10],
            "repetitions": 1,
        }
    return {
        "fig12_sizes": [1000],
        "fig13_sizes": [1200],
        "fig13_k": [1, 2],
        "repetitions": 1,
    }


def assert_benchmark_contract(profile: str, metrics: list[Metrics]):
    failures: list[str] = []
    for m in metrics:
        if not m.valid:
            failures.append(f"{m.run_id}: {m.warning or 'marked invalid'}")
        if int(m.counters.get("paper_end_before_audit_start", 0)) != 1:
            failures.append(f"{m.run_id}: paper_end_ms is not before audit_start_ms")
        if int(m.counters.get("audit_validation_positive", 0)) != 1:
            failures.append(f"{m.run_id}: audit_validation_ms is not positive")
        if int(m.counters.get("end_to_end_covers_paper_and_audit", 0)) != 1:
            failures.append(f"{m.run_id}: end_to_end_observed_ms does not cover paper plus audit")
        if int(m.counters.get("schema_fidelity_ok", 0)) != 1:
            failures.append(f"{m.run_id}: schema fidelity failed")
        if m.method != "merkle":
            continue
        if int(m.counters.get("partition_root_batches", -1)) != 2:
            failures.append(f"{m.run_id}: partition_root_batches={m.counters.get('partition_root_batches')}")
        if int(m.counters.get("partition_root_batches_ok", 0)) != 1:
            failures.append(f"{m.run_id}: partition_root_batches_ok={m.counters.get('partition_root_batches_ok')}")
        if int(m.counters.get("planner_checks_passed", 0)) != 1:
            failures.append(f"{m.run_id}: planner checks did not pass")
    if failures and profile in ("smoke", "preflight", "paper"):
        shown = "\n".join(failures[:20])
        more = "" if len(failures) <= 20 else f"\n... {len(failures) - 20} more"
        raise RuntimeError(f"benchmark contract failed:\n{shown}{more}")


def method_order(rep: int) -> list[str]:
    orders = [
        ["cta", "disk", "merkle"],
        ["disk", "merkle", "cta"],
        ["merkle", "cta", "disk"],
        ["cta", "merkle", "disk"],
        ["disk", "cta", "merkle"],
    ]
    return orders[rep % len(orders)]


def selected_values(values: Iterable[int], selected: int | None) -> list[int]:
    out = list(values)
    if selected is None:
        return out
    return [v for v in out if v == selected]


def count_planned_method_runs(config: dict[str, Any], args: argparse.Namespace) -> int:
    reps = int(config["repetitions"])
    total = 0
    if args.experiment in (None, "figure12"):
        total += len(selected_values(config["fig12_sizes"], args.tuple_count)) * reps * 3
    if args.experiment in (None, "figure13"):
        total += (
            len(selected_values([100, 200], args.partitions))
            * len(selected_values(config["fig13_sizes"], args.tuple_count))
            * len(selected_values(config["fig13_k"], args.bad_leaf_count))
            * reps
            * 3
        )
    return total


def run_one_manifest(
    conn,
    manifest: dict[str, Any],
    scratch_dir: Path,
    reps: int,
    keep_disk_snapshots: bool,
    planner_rows_out: list[dict[str, Any]],
    schema_rows_out: list[dict[str, Any]],
    result_dir: Path,
    progress_state: dict[str, int],
) -> list[Metrics]:
    tuple_count = int(manifest["tuple_count"])
    cfg = {k: int(manifest[k]) for k in ["partitions", "leaves_per_partition", "fanout"]}
    metrics: list[Metrics] = []
    for rep in range(reps):
        for method in method_order(rep):
            run_base = f"{manifest['experiment']}-n{tuple_count}-p{cfg['partitions']}-k{len(manifest['bad_leaves'])}"
            run_id = f"{run_base}-{method}-r{rep}"
            emit_progress(
                result_dir,
                event="method_start",
                run_id=run_id,
                experiment=manifest["experiment"],
                method=method,
                tuple_count=tuple_count,
                partitions=cfg["partitions"],
                bad_leaf_count=len(manifest["bad_leaves"]),
                repetition=rep,
                completed_runs=progress_state["completed_runs"],
                total_runs=progress_state["total_runs"],
            )
            method_start = now_ms()
            reset_damaged_from_healthy(conn, cfg, manifest)
            if method == "cta":
                metric = repair_cta(conn, manifest, tuple_count, rep, schema_rows_out)
            elif method == "disk":
                metric = repair_disk(
                    conn,
                    manifest,
                    tuple_count,
                    rep,
                    scratch_dir,
                    keep_disk_snapshots,
                    schema_rows_out,
                )
            elif method == "merkle":
                merkle_run_id = f"{run_base}-merkle-r{rep}"
                planner_results, planner_rows = run_planner_preflight(conn, manifest, merkle_run_id)
                planner_rows_out.extend(planner_rows)
                metric = repair_merkle(conn, manifest, tuple_count, rep, planner_results, schema_rows_out)
            else:
                raise AssertionError(method)
            metrics.append(metric)
            progress_state["completed_runs"] += 1
            emit_progress(
                result_dir,
                event="method_complete",
                run_id=metric.run_id,
                experiment=metric.experiment,
                method=metric.method,
                tuple_count=metric.tuple_count,
                partitions=metric.partitions,
                bad_leaf_count=metric.bad_leaf_count,
                repetition=metric.repetition,
                valid=metric.valid,
                warning=metric.warning,
                method_elapsed_ms=round(now_ms() - method_start, 3),
                paper_style_total_ms=round(metric.paper_style_total_ms, 3),
                audit_validation_ms=round(metric.audit_validation_ms, 3),
                completed_runs=progress_state["completed_runs"],
                total_runs=progress_state["total_runs"],
            )
    return metrics


def run_benchmark(args: argparse.Namespace) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_dir = RESULT_ROOT / ts
    result_dir.mkdir(parents=True, exist_ok=False)
    (result_dir / "plots").mkdir()
    config = profile_config(args.profile)
    if args.repetitions is not None:
        config["repetitions"] = args.repetitions
    config.update(
        {
            "benchmark_schema_version": BENCHMARK_SCHEMA_VERSION,
            "timing_contract_version": TIMING_CONTRACT_VERSION,
        }
    )
    total_runs = count_planned_method_runs(config, args)
    if total_runs <= 0:
        raise RuntimeError("selected benchmark filters match no runs")
    scratch_dir = Path(args.scratch_dir) if args.scratch_dir else RESULT_ROOT / ("scratch_" + ts)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    (result_dir / "config.json").write_text(json.dumps({**vars(args), **config}, indent=2, default=str) + "\n")
    write_environment(result_dir, args)
    write_python_environment(result_dir)
    progress_state = {"completed_runs": 0, "total_runs": total_runs}
    emit_progress(
        result_dir,
        event="benchmark_start",
        profile=args.profile,
        repetitions=config["repetitions"],
        total_runs=total_runs,
        experiment=args.experiment or "all",
        tuple_count=args.tuple_count,
        partitions=args.partitions,
        bad_leaf_count=args.bad_leaf_count,
    )

    all_metrics: list[Metrics] = []
    dataset_rows: list[dict[str, Any]] = []
    bucket_summary_rows: list[dict[str, Any]] = []
    bucket_debug_rows: list[dict[str, Any]] = []
    planner_rows: list[dict[str, Any]] = []
    schema_fidelity_rows: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []

    with connect(args) as conn:
        ensure_helpers(conn)
        if args.experiment in (None, "figure12"):
            fig12_sizes = selected_values(config["fig12_sizes"], args.tuple_count)
        else:
            fig12_sizes = []
        for n in fig12_sizes:
            fig12_bad_leaf_count = args.bad_leaf_count if args.bad_leaf_count is not None else (10 if n >= 10 else 1)
            emit_progress(
                result_dir,
                event="dataset_start",
                experiment="figure12",
                tuple_count=n,
                partitions=200,
                bad_leaf_count=fig12_bad_leaf_count,
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
            )
            build_dataset(conn, n, 200, 16, 2)
            bucket_summary, bucket_debug = bucket_consistency_sample(conn, n, 200, 16, 2, args.seed)
            bucket_summary_rows.append(bucket_summary)
            if args.artifact_mode == "debug":
                bucket_debug_rows.extend(bucket_debug)
            occ = leaf_occupancy(conn)
            occ_stats = occupancy_stats(occ)
            sizes = table_sizes(conn)
            dataset_rows.append({**sizes, "partitions": 200, "leaves_per_partition": 16, "fanout": 2, **occ_stats})
            emit_progress(
                result_dir,
                event="dataset_complete",
                experiment="figure12",
                tuple_count=n,
                partitions=200,
                bad_leaf_count=fig12_bad_leaf_count,
                total_schema_bytes=sizes.get("total_schema_bytes"),
                completed_runs=progress_state["completed_runs"],
                total_runs=total_runs,
            )
            manifest = choose_corruption_manifest(conn, "figure12", n, 200, 16, 2, fig12_bad_leaf_count, fig12_bad_leaf_count, args.seed)
            validate_manifest_leaf_mapping(conn, manifest)
            apply_corruption(conn, manifest)
            manifests.append(manifest)
            all_metrics.extend(
                run_one_manifest(
                    conn,
                    manifest,
                    scratch_dir,
                    config["repetitions"],
                    args.keep_disk_snapshots,
                    planner_rows,
                    schema_fidelity_rows,
                    result_dir,
                    progress_state,
                )
            )

        if args.experiment in (None, "figure13"):
            fig13_partitions = selected_values([100, 200], args.partitions)
            fig13_sizes = selected_values(config["fig13_sizes"], args.tuple_count)
            fig13_k_values = selected_values(config["fig13_k"], args.bad_leaf_count)
        else:
            fig13_partitions = []
            fig13_sizes = []
            fig13_k_values = []
        for partitions in fig13_partitions:
            for n in fig13_sizes:
                emit_progress(
                    result_dir,
                    event="dataset_start",
                    experiment="figure13",
                    tuple_count=n,
                    partitions=partitions,
                    completed_runs=progress_state["completed_runs"],
                    total_runs=total_runs,
                )
                build_dataset(conn, n, partitions, 16, 2)
                bucket_summary, bucket_debug = bucket_consistency_sample(conn, n, partitions, 16, 2, args.seed)
                bucket_summary_rows.append(bucket_summary)
                if args.artifact_mode == "debug":
                    bucket_debug_rows.extend(bucket_debug)
                # Record occupancy and size stats once per (n, partitions) geometry,
                # before iterating k, so dataset_sizes.csv captures all Figure 13 rows.
                occ = leaf_occupancy(conn)
                occ_stats = occupancy_stats(occ)
                sizes = table_sizes(conn)
                dataset_rows.append({
                    **sizes,
                    "partitions": partitions,
                    "leaves_per_partition": 16,
                    "fanout": 2,
                    **occ_stats,
                })
                emit_progress(
                    result_dir,
                    event="dataset_complete",
                    experiment="figure13",
                    tuple_count=n,
                    partitions=partitions,
                    total_schema_bytes=sizes.get("total_schema_bytes"),
                    completed_runs=progress_state["completed_runs"],
                    total_runs=total_runs,
                )
                for k in fig13_k_values:
                    d = 300 if args.profile in ("paper", "preflight") else k
                    emit_progress(
                        result_dir,
                        event="manifest_start",
                        experiment="figure13",
                        tuple_count=n,
                        partitions=partitions,
                        bad_leaf_count=k,
                        corrupted_tuple_count=d,
                        completed_runs=progress_state["completed_runs"],
                        total_runs=total_runs,
                    )
                    manifest = choose_corruption_manifest(conn, "figure13", n, partitions, 16, 2, k, d, args.seed)
                    validate_manifest_leaf_mapping(conn, manifest)
                    apply_corruption(conn, manifest)
                    manifests.append(manifest)
                    all_metrics.extend(
                        run_one_manifest(
                            conn,
                            manifest,
                            scratch_dir,
                            config["repetitions"],
                            args.keep_disk_snapshots,
                            planner_rows,
                            schema_fidelity_rows,
                            result_dir,
                            progress_state,
                        )
                    )

    (result_dir / "corruption_manifest.json").write_text(json.dumps(manifests, indent=2) + "\n")
    write_csv(
        result_dir / "dataset_sizes.csv",
        dataset_rows,
        [
            "tuple_count",
            "partitions",
            "leaves_per_partition",
            "fanout",
            "base_table_bytes",
            "primary_index_bytes",
            "merkle_index_bytes",
            "leaf_lookup_index_bytes",
            "total_schema_bytes",
            "minimum",
            "p50",
            "p95",
            "p99",
            "maximum",
            "mean",
            "stddev",
        ],
    )
    write_csv(
        result_dir / "bucket_consistency_summary.csv",
        bucket_summary_rows,
        [
            "tuple_count",
            "partitions",
            "leaves_per_partition",
            "fanout",
            "sample_count",
            "sample_seed",
            "mismatch_count",
            "sample_digest",
        ],
    )
    if args.artifact_mode == "debug":
        write_csv(
            result_dir / "bucket_consistency.csv",
            bucket_debug_rows,
            [
                "tuple_count",
                "partitions",
                "leaves_per_partition",
                "fanout",
                "ycsb_key",
                "bucket",
                "leaf_id",
                "match",
            ],
        )
    write_csv(
        result_dir / "planner_checks.csv",
        planner_rows,
        [
            "run_id",
            "schema",
            "leaf_id",
            "index_oid",
            "index_relfilenode",
            "index_definition",
            "plan_uses_expected_leaf_lookup_index",
            "plan_json_sha256",
        ],
    )
    write_csv(
        result_dir / "schema_fidelity.csv",
        schema_fidelity_rows,
        [
            "run_id",
            "method",
            "check_name",
            "healthy_value",
            "damaged_value",
            "match",
        ],
    )
    run_rows, phase_rows = metrics_to_rows(all_metrics)
    all_run_fields = sorted({k for r in run_rows for k in r})
    write_csv(result_dir / "runs.csv", run_rows, all_run_fields)
    write_csv(result_dir / "phase_timings.csv", phase_rows, ["run_id", "method", "phase", "ms"])
    write_csv(
        result_dir / "timing_contract.csv",
        [
            {
                "run_id": m.run_id,
                "method": m.method,
                "paper_style_total_ms": f"{m.paper_style_total_ms:.3f}",
                "restore_repair_ms": f"{m.restore_repair_ms:.3f}",
                "audit_validation_ms": f"{m.audit_validation_ms:.3f}",
                "end_to_end_observed_ms": f"{m.end_to_end_observed_ms:.3f}",
                "cleanup_ms": f"{m.cleanup_ms:.3f}",
                "paper_end_before_audit_start": m.counters.get("paper_end_before_audit_start", 0),
                "audit_validation_positive": m.counters.get("audit_validation_positive", 0),
                "end_to_end_covers_paper_and_audit": m.counters.get("end_to_end_covers_paper_and_audit", 0),
            }
            for m in all_metrics
        ],
        [
            "run_id",
            "method",
            "paper_style_total_ms",
            "restore_repair_ms",
            "audit_validation_ms",
            "end_to_end_observed_ms",
            "cleanup_ms",
            "paper_end_before_audit_start",
            "audit_validation_positive",
            "end_to_end_covers_paper_and_audit",
        ],
    )
    write_csv(result_dir / "verification_results.csv", [{"all_runs_valid": int(all(m.valid for m in all_metrics))}], ["all_runs_valid"])
    assert_benchmark_contract(args.profile, all_metrics)
    leaked = list(result_dir.rglob("*.copybin"))
    if leaked:
        raise RuntimeError(f"binary disk snapshot leaked into result artifacts: {leaked[:3]}")

    from plot_recovery_results import plot_all

    plot_all(result_dir)
    emit_progress(
        result_dir,
        event="benchmark_complete",
        completed_runs=progress_state["completed_runs"],
        total_runs=total_runs,
        all_runs_valid=all(m.valid for m in all_metrics),
    )
    return result_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="host=127.0.0.1 port=5432 dbname=postgres user=neel")
    parser.add_argument("--profile", choices=["smoke", "preflight", "paper"], default="smoke")
    parser.add_argument("--experiment", choices=["figure12", "figure13"])
    parser.add_argument("--tuple-count", type=int)
    parser.add_argument("--partitions", type=int)
    parser.add_argument("--bad-leaf-count", type=int)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--seed", type=int, default=20260703)
    parser.add_argument("--result-dir")
    parser.add_argument("--scratch-dir")
    parser.add_argument("--keep-disk-snapshots", action="store_true")
    parser.add_argument("--artifact-mode", choices=["summary", "debug"], default="summary")
    args = parser.parse_args(argv)

    if args.result_dir:
        global RESULT_ROOT
        RESULT_ROOT = Path(args.result_dir)

    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    scratch = RESULT_ROOT / ("tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    scratch.mkdir(parents=True, exist_ok=False)
    try:
        with (scratch / "stdout.log").open("w") as out, (scratch / "stderr.log").open("w") as err:
            with redirect_stdout(out), redirect_stderr(err):
                result_dir = run_benchmark(args)
        shutil.move(str(scratch / "stdout.log"), result_dir / "stdout.log")
        shutil.move(str(scratch / "stderr.log"), result_dir / "stderr.log")
        scratch.rmdir()
        print(result_dir)
        return 0
    except Exception:
        print(f"failed; logs in {scratch}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
