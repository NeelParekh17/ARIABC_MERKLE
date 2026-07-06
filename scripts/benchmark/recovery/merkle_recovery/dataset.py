"""Schema setup, dataset construction, and index management."""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import Any

from .config import ALL_COLUMNS, BENCH_DIR, FIELDS
from .db import execute, run_file, scalar


# ── helpers ─────────────────────────────────────────────────────────────────

def ensure_helpers(conn) -> None:
    run_file(conn, BENCH_DIR / "sql" / "recovery_helpers.sql")

    required = [
        "merkle_bucket_for_key",
        "merkle_get_child_hashes",
        "merkle_get_partition_root_hashes",
    ]

    missing = execute(
        conn,
        """
        SELECT wanted.name
        FROM unnest(%s::text[]) AS wanted(name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_proc p
            WHERE p.proname = wanted.name
        )
        """,
        (required,),
    )

    if missing:
        names = ", ".join(row["name"] for row in missing)
        raise RuntimeError(
            f"Missing built-in Merkle SQL functions: {names}. "
            "Use a PostgreSQL cluster initialized from the current AriaBC build."
        )


def recreate_schema(conn) -> None:
    run_file(conn, BENCH_DIR / "create_schema.sql")


def create_merkle_indexes(conn, partitions: int, leaves_per_partition: int, fanout: int) -> None:
    sql = (BENCH_DIR / "create_merkle_indexes.sql").read_text()
    sql = sql.replace(":partitions", str(partitions))
    sql = sql.replace(":leaves_per_partition", str(leaves_per_partition))
    sql = sql.replace(":fanout", str(fanout))
    with conn.cursor() as cur:
        cur.execute(sql)


def create_damaged_indexes(conn, partitions: int, leaves_per_partition: int, fanout: int) -> None:
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


# ── dataset ──────────────────────────────────────────────────────────────────

def build_dataset(conn, tuple_count: int, partitions: int, leaves_per_partition: int, fanout: int) -> None:
    """Create both schemas from scratch and populate healthy.usertable."""
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


def reset_damaged_from_healthy(conn, cfg: dict[str, int]) -> None:
    """Restore damaged.usertable to a clean copy of healthy; rebuild all indexes."""
    execute(conn, "DROP TABLE IF EXISTS damaged.usertable CASCADE")
    execute(conn, "CREATE TABLE damaged.usertable (LIKE healthy.usertable INCLUDING DEFAULTS)")
    execute(conn, "INSERT INTO damaged.usertable SELECT * FROM healthy.usertable")
    create_damaged_indexes(conn, cfg["partitions"], cfg["leaves_per_partition"], cfg["fanout"])


# ── occupancy helpers ────────────────────────────────────────────────────────

def leaf_occupancy(conn) -> list[dict[str, Any]]:
    return execute(
        conn,
        f"""
        SELECT merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, ycsb_key)::bigint AS leaf_id,
               count(*)::bigint AS tuple_count
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
    import hashlib
    import json

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
        raise RuntimeError(
            f"merkle_bucket_for_key disagreed with merkle_leaf_id for {mismatch_count} sampled keys"
        )
    sample_digest = hashlib.sha256(json.dumps(out, sort_keys=True).encode()).hexdigest()
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
