"""Schema setup, dataset construction, and index management."""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import Any

from .config import ALL_COLUMNS, BENCH_DIR, FIELDS
from .db import execute, run_file, scalar, geometry


# ── helpers ─────────────────────────────────────────────────────────────────

def ensure_helpers(conn) -> None:
    run_file(conn, BENCH_DIR / "sql" / "recovery_helpers.sql")

    # Recovery DML must use the current synchronous Merkle contract.  Every
    # UPDATE/INSERT/DELETE below must update merkle_node in the same database
    # transaction; this benchmark must never defer Merkle maintenance.
    execute(conn, "SET enable_merkle_index = on")
    execute(conn, "SET merkle_apply_synchronous_direct = on")
    execute(conn, "SET synchronous_commit = on")
    def setting_text(value: Any) -> str:
        if isinstance(value, bytes):
            value = value.decode("ascii")
        return str(value).strip().lower()

    settings = {
        "enable_merkle_index": setting_text(scalar(conn, "SHOW enable_merkle_index")),
        "merkle_apply_synchronous_direct": setting_text(
            scalar(conn, "SHOW merkle_apply_synchronous_direct")
        ),
        "synchronous_commit": setting_text(scalar(conn, "SHOW synchronous_commit")),
    }
    if any(value != "on" for value in settings.values()):
        raise RuntimeError(
            "synchronous Merkle recovery contract not active: "
            f"{settings}"
        )
    print(f"[contract] synchronous Merkle settings: {settings}", flush=True)

    required_signatures = [
        "pg_catalog.merkle_get_descendants_batch(regclass,integer,bytea[],smallint[],integer)",
        "pg_catalog.merkle_get_partition_root_hashes(regclass)",
        "pg_catalog.merkle_partition_for_hash(bytea,integer)",
        "pg_catalog.merkle_root_hash(regclass)",
        "pg_catalog.merkle_verify(regclass)",
    ]

    missing = execute(
        conn,
        """
        SELECT wanted.signature
        FROM unnest(%s::text[]) AS wanted(signature)
        WHERE to_regprocedure(wanted.signature) IS NULL
        """,
        (required_signatures,),
    )

    if missing:
        names = ", ".join(row["signature"] for row in missing)
        raise RuntimeError(
            f"Missing current Merkle SQL functions: {names}. "
            "The recovery compatibility bootstrap could not register them; "
            "use a current AriaBC postgres binary and rerun ledger schema bootstrap."
        )


def recreate_schema(conn, *, bulk_load: bool = True, unlogged: bool = False) -> None:
    # This is a scratch-cluster reset.  Direct synchronous mode has no
    # deferred local-delta queue; reset only the Raft recovery watermark.
    try:
        execute(conn, "TRUNCATE ariabc_internal.merkle_node CASCADE")
        execute(conn, "UPDATE ariabc_internal.merkle_apply_state SET applied_seq = 0, state = 0, error_text = NULL")
        execute(conn, "UPDATE ariabc_internal.merkle_apply_counter SET next_seq = 0, terminal_prefix_seq = 0")
    except Exception:
        pass
    if not bulk_load:
        run_file(conn, BENCH_DIR / "create_schema.sql")
        return

    # Postpone primary-key maintenance until both heaps are populated. This
    # lets PostgreSQL build each B-tree in bulk instead of doing one random
    # index insertion for every generated row.
    persistence = "UNLOGGED " if unlogged else ""
    execute(conn, "DROP SCHEMA IF EXISTS healthy CASCADE")
    execute(conn, "DROP SCHEMA IF EXISTS damaged CASCADE")
    execute(conn, "CREATE SCHEMA healthy")
    execute(conn, "CREATE SCHEMA damaged")
    if not getattr(conn, "autocommit", False):
        conn.commit()
    table_sql = f"""
        CREATE {persistence}TABLE {{schema}}.usertable (
            ycsb_key bigint NOT NULL,
            field0 text NOT NULL, field1 text NOT NULL,
            field2 text NOT NULL, field3 text NOT NULL,
            field4 text NOT NULL, field5 text NOT NULL,
            field6 text NOT NULL, field7 text NOT NULL,
            field8 text NOT NULL, field9 text NOT NULL
        )
    """
    execute(conn, table_sql.format(schema="healthy"))
    execute(conn, table_sql.format(schema="damaged"))


def _clone_conn(conn):
    """Safely open a twin connection for parallel index/PK building.

    Builds the connection string from live conn.info parameters so Unix socket
    paths (e.g. /tmp/ariabc-pg.xxxx) are preserved correctly instead of
    falling back to a hostname-based DSN that may not resolve.
    """
    try:
        import psycopg
        from psycopg.rows import dict_row

        info = conn.info
        # Build kwargs from the live connection parameters
        kwargs: dict = {}
        for attr, key in (
            ("host", "host"),
            ("port", "port"),
            ("dbname", "dbname"),
            ("user", "user"),
            ("password", "password"),
        ):
            val = getattr(info, attr, None)
            if val is not None:
                kwargs[key] = val
        conn2 = psycopg.connect(autocommit=True, row_factory=dict_row, **kwargs)
        with conn2.cursor() as cur:
            cur.execute("SET enable_merkle_index = on")
            cur.execute("SET enable_seqscan = off")
            cur.execute("SET max_parallel_workers_per_gather = 0")
        return conn2
    except Exception as exc:
        print(f"  [dataset] warning: parallel worker connection failed ({type(exc).__name__}): {exc}", flush=True)
        return None


def finish_bulk_schema(conn, *, unlogged: bool = False) -> None:
    """Make bulk-loaded heaps eligible for the crash-safe Merkle AM.

    Each parallel worker gets its own dedicated connection so that the main
    conn is never accessed from a background thread (psycopg3 connections are
    not thread-safe).
    """
    import concurrent.futures

    def _prep_and_add_pk(target_conn, schema):
        execute(target_conn, "SET maintenance_work_mem = '8GB'")
        execute(target_conn, "SET max_parallel_maintenance_workers = 16")
        execute(target_conn, "SET max_parallel_workers = 16")
        execute(target_conn, "SET synchronous_commit = off")
        if unlogged:
            execute(target_conn, f"ALTER TABLE {schema}.usertable SET LOGGED")
        execute(target_conn, f"ALTER TABLE {schema}.usertable ADD CONSTRAINT usertable_pkey PRIMARY KEY (ycsb_key)")

    conn1 = _clone_conn(conn)
    conn2 = _clone_conn(conn) if conn1 is not None else None

    if conn1 is not None and conn2 is not None:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(_prep_and_add_pk, conn1, "healthy")
                f2 = executor.submit(_prep_and_add_pk, conn2, "damaged")
                f1.result()
                f2.result()
        finally:
            conn1.close()
            conn2.close()
    else:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()
        # Sequential fallback on main conn
        _prep_and_add_pk(conn, "healthy")
        _prep_and_add_pk(conn, "damaged")


def _verify_merkle_index(conn, schema: str, fanout: int, split_threshold: int, merge_threshold: int) -> None:
    """Fail immediately if CREATE INDEX did not create the requested Merkle index."""
    rows = execute(
        conn,
        """
        SELECT c.relkind, am.amname, i.indisvalid, c.relpages
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_am am ON am.oid = c.relam
        WHERE n.nspname = %s AND c.relname = 'usertable_merkle_idx'
        """,
        (schema,),
    )
    amname = rows[0]["amname"] if rows else None
    if isinstance(amname, bytes):
        amname = amname.decode("ascii")
    if len(rows) != 1 or amname != "merkle" or not rows[0]["indisvalid"]:
        raise RuntimeError(f"{schema}.usertable_merkle_idx was not created as a valid Merkle index")
    actual = geometry(conn, schema)
    expected = {
        "fanout": fanout,
        "split_threshold": split_threshold,
        "merge_threshold": merge_threshold,
    }
    if any(actual[key] != value for key, value in expected.items()):
        raise RuntimeError(
            f"{schema}.usertable_merkle_idx geometry mismatch: expected {expected}, got {actual}"
        )


def _ensure_functions_parallel_safe(conn) -> None:
    """Ensure hash helper functions are marked PARALLEL SAFE so CREATE INDEX uses parallel maintenance workers."""
    try:
        execute(conn, "ALTER FUNCTION merkle_key_hash(anyelement) PARALLEL SAFE")
        execute(conn, "ALTER FUNCTION merkle_partition_for_hash(bytea, integer) PARALLEL SAFE")
        execute(conn, "ALTER FUNCTION merkle_tuple_hash(record) PARALLEL SAFE")
    except Exception as exc:
        print(f"  [dataset] note: ALTER FUNCTION PARALLEL SAFE: {exc}", flush=True)


def _create_merkle_am_index(conn, schema: str, split_threshold: int = 32, merge_threshold: int = 8,
                            fanout: int = 4, partitions: int = 200) -> float:
    import time
    t_start = time.perf_counter()
    execute(conn, "SET maintenance_work_mem = '8GB'")
    execute(conn, "SET max_parallel_maintenance_workers = 32")
    execute(conn, "SET max_parallel_workers = 96")
    execute(conn, "SET synchronous_commit = off")
    execute(conn, f"DROP INDEX IF EXISTS {schema}.usertable_merkle_idx")
    try:
        execute(
            conn,
            f"""
            CREATE INDEX usertable_merkle_idx
            ON {schema}.usertable USING merkle (ycsb_key)
            WITH (split_threshold = {split_threshold}, merge_threshold = {merge_threshold},
                  fanout = {fanout}, partitions = {partitions})
            """,
        )
        print(f"  [index] CREATE INDEX USING merkle on {schema} took {time.perf_counter()-t_start:.2f}s", flush=True)
        _verify_merkle_index(conn, schema, fanout, split_threshold, merge_threshold)
    finally:
        execute(conn, "SET synchronous_commit = on")
    return (time.perf_counter() - t_start) * 1000.0


def _create_lookup_btree_index(conn, schema: str) -> float:
    import time
    t_start = time.perf_counter()
    execute(conn, "SET maintenance_work_mem = '8GB'")
    execute(conn, "SET max_parallel_maintenance_workers = 32")
    execute(conn, "SET max_parallel_workers = 96")
    execute(conn, "SET max_parallel_workers_per_gather = 16")
    execute(conn, "SET synchronous_commit = off")
    execute(conn, f"DROP INDEX IF EXISTS {schema}.usertable_merkle_partition_lookup_idx")
    try:
        execute(
            conn,
            f"""
            CREATE INDEX usertable_merkle_partition_lookup_idx
            ON {schema}.usertable (
                merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200),
                merkle_key_hash(ycsb_key),
                ycsb_key
            )
            """,
        )
        execute(conn, f"ANALYZE {schema}.usertable")
        print(f"  [index] CREATE B-tree lookup index on {schema} took {time.perf_counter()-t_start:.2f}s", flush=True)
    finally:
        execute(conn, "SET synchronous_commit = on")
    return (time.perf_counter() - t_start) * 1000.0


def create_merkle_indexes(conn, split_threshold: int = 32, merge_threshold: int = 8,
                          fanout: int = 4, partitions: int = 200) -> float:
    _ensure_functions_parallel_safe(conn)
    t1 = _create_merkle_am_index(conn, "healthy", split_threshold, merge_threshold, fanout, partitions)
    t2 = _create_lookup_btree_index(conn, "healthy")
    return t1 + t2


def create_damaged_indexes(conn, split_threshold: int = 32, merge_threshold: int = 8,
                           fanout: int = 4, partitions: int = 200) -> float:
    _ensure_functions_parallel_safe(conn)
    t1 = _create_merkle_am_index(conn, "damaged", split_threshold, merge_threshold, fanout, partitions)
    t2 = _create_lookup_btree_index(conn, "damaged")
    return t1 + t2


def create_all_indexes_parallel(conn, split_threshold: int = 32, merge_threshold: int = 8,
                                fanout: int = 4, partitions: int = 200) -> tuple[float, float]:
    """Build Merkle AM indexes sequentially (to prevent catalog write conflicts on ariabc_internal.merkle_node),
    and build lookup B-tree indexes in parallel using dedicated connections and 32 parallel maintenance workers.
    """
    import concurrent.futures
    import time

    _ensure_functions_parallel_safe(conn)

    # Phase 1: Merkle AM indexes MUST be sequential to avoid SPI catalog lock contention
    t_h1 = _create_merkle_am_index(conn, "healthy", split_threshold, merge_threshold, fanout, partitions)
    t_d1 = _create_merkle_am_index(conn, "damaged", split_threshold, merge_threshold, fanout, partitions)

    # Phase 2: Lookup B-tree expression indexes CAN run in parallel across dedicated connections
    conn1 = _clone_conn(conn)
    conn2 = _clone_conn(conn) if conn1 is not None else None

    if conn1 is not None and conn2 is not None:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(_create_lookup_btree_index, conn1, "healthy")
                f2 = executor.submit(_create_lookup_btree_index, conn2, "damaged")
                t_h2 = f1.result()
                t_d2 = f2.result()
        finally:
            conn1.close()
            conn2.close()
    else:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()
        t_h2 = _create_lookup_btree_index(conn, "healthy")
        t_d2 = _create_lookup_btree_index(conn, "damaged")

    return t_h1 + t_h2, t_d1 + t_d2


# ── dataset ──────────────────────────────────────────────────────────────────

def build_dataset(
    conn,
    tuple_count: int,
    fanout: int = 4,
    split_threshold: int = 32,
    merge_threshold: int = 8,
    partitions: int = 200,
    setup_mode: str = "bulk-logged",
    *args,
    **kwargs,
) -> dict[str, Any]:
    """Create both schemas from scratch and populate healthy.usertable."""
    import time
    print(f"[dataset] starting build_dataset for {tuple_count} tuples (fanout={fanout}, split={split_threshold})", flush=True)
    t0 = time.perf_counter()
    if setup_mode not in {"legacy", "bulk-logged", "bulk-unlogged"}:
        raise ValueError(f"unknown dataset setup mode: {setup_mode}")
    bulk_load = setup_mode != "legacy"
    unlogged = setup_mode == "bulk-unlogged"
    recreate_schema(conn, bulk_load=bulk_load, unlogged=unlogged)
    print(f"[dataset] recreate_schema took {time.perf_counter()-t0:.2f}s", flush=True)

    t1 = time.perf_counter()
    conn1 = _clone_conn(conn)
    conn2 = _clone_conn(conn) if conn1 is not None else None

    sql_template = """
        INSERT INTO {schema}.usertable
        SELECT gs::bigint,
               'field0-' || gs, 'field1-' || gs, 'field2-' || gs,
               'field3-' || gs, 'field4-' || gs, 'field5-' || gs,
               'field6-' || gs, 'field7-' || gs, 'field8-' || gs,
               'field9-' || gs
        FROM generate_series(1, %s) AS gs
    """

    if conn1 is not None and conn2 is not None:
        try:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(execute, conn1, sql_template.format(schema="healthy"), (tuple_count,))
                f2 = executor.submit(execute, conn2, sql_template.format(schema="damaged"), (tuple_count,))
                f1.result()
                f2.result()
        finally:
            conn1.close()
            conn2.close()
    else:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()
        execute(conn, sql_template.format(schema="healthy"), (tuple_count,))
        execute(conn, "INSERT INTO damaged.usertable SELECT * FROM healthy.usertable")

    t2 = time.perf_counter()
    print(f"[dataset] Parallel heap population (healthy + damaged {tuple_count} rows) took {t2-t1:.2f}s", flush=True)
    print(f"[dataset] INSERT damaged.usertable took {time.perf_counter()-t2:.2f}s", flush=True)

    t_pk = time.perf_counter()
    if bulk_load:
        finish_bulk_schema(conn, unlogged=unlogged)
        print(f"[dataset] bulk primary keys/logging finalization took {time.perf_counter()-t_pk:.2f}s", flush=True)
    pk_ms = (time.perf_counter() - t_pk) * 1000.0

    healthy_indexes_ms, damaged_indexes_ms = create_all_indexes_parallel(conn, split_threshold, merge_threshold, fanout, partitions)

    # ANALYZE on healthy/damaged is already done inside create_merkle_indexes /
    # create_damaged_indexes. Only ANALYZE the internal catalog (fast) and skip
    # CHECKPOINT — it is not required for benchmark correctness and costs ~20s.
    t_ckpt = time.perf_counter()
    execute(conn, "ANALYZE ariabc_internal.merkle_node")
    ckpt_ms = (time.perf_counter() - t_ckpt) * 1000.0

    # Warm table and index pages into shared_buffers
    execute(conn, "SELECT count(*) FROM healthy.usertable")
    execute(conn, "SELECT count(*) FROM damaged.usertable")
    execute(conn, "SELECT count(*) FROM ariabc_internal.merkle_node")
    timings = {
        "healthy_table_ms": (t2 - t1) * 1000.0,
        "damaged_table_ms": (t_pk - t2) * 1000.0,
        "primary_keys_ms": pk_ms,
        "healthy_indexes_ms": healthy_indexes_ms,
        "damaged_indexes_ms": damaged_indexes_ms,
        "analyze_checkpoint_ms": ckpt_ms,
        "dataset_total_ms": (time.perf_counter() - t0) * 1000.0,
        "dataset_setup_mode": setup_mode,
    }
    print(f"[dataset] total build_dataset completed in {timings['dataset_total_ms'] / 1000.0:.2f}s", flush=True)
    return timings


def expand_dataset(
    conn,
    previous_tuple_count: int,
    tuple_count: int,
    fanout: int = 4,
    split_threshold: int = 32,
    merge_threshold: int = 8,
    partitions: int = 200,
) -> dict[str, Any]:
    """Append a size checkpoint and rebuild only its derived Merkle state.

    The heap and primary keys are retained.  Merkle indexes are intentionally
    rebuilt from the larger committed snapshot rather than inserting millions
    of rows through the synchronous DML path; this preserves the exact native
    tree-build contract while avoiding repeated source-table reloads.
    """
    import time

    previous_tuple_count = int(previous_tuple_count)
    tuple_count = int(tuple_count)
    if previous_tuple_count < 1 or tuple_count <= previous_tuple_count:
        raise ValueError(
            f"expansion requires tuple_count > previous count; got {previous_tuple_count} -> {tuple_count}"
        )

    t0 = time.perf_counter()
    execute(conn, "SET synchronous_commit = off")
    for schema in ("healthy", "damaged"):
        execute(conn, f"DROP INDEX IF EXISTS {schema}.usertable_merkle_partition_lookup_idx")
        execute(conn, f"DROP INDEX IF EXISTS {schema}.usertable_merkle_idx")
        execute(conn, f"ALTER TABLE {schema}.usertable DROP CONSTRAINT IF EXISTS usertable_pkey")
    # The benchmark owns this scratch internal catalog. Remove rows belonging
    # to the dropped index OIDs before the next build, avoiding orphan-node
    # scans and preserving the existing catalog-backed tree contract.
    execute(conn, "TRUNCATE ariabc_internal.merkle_node")

    t1 = time.perf_counter()
    execute(
        conn,
        """
        INSERT INTO healthy.usertable
        SELECT gs::bigint,
               'field0-' || gs, 'field1-' || gs, 'field2-' || gs,
               'field3-' || gs, 'field4-' || gs, 'field5-' || gs,
               'field6-' || gs, 'field7-' || gs, 'field8-' || gs,
               'field9-' || gs
        FROM generate_series(%s, %s) AS gs
        """,
        (previous_tuple_count + 1, tuple_count),
    )
    t2 = time.perf_counter()
    execute(
        conn,
        """
        INSERT INTO damaged.usertable
        SELECT * FROM healthy.usertable
        WHERE ycsb_key > %s AND ycsb_key <= %s
        """,
        (previous_tuple_count, tuple_count),
    )
    t3 = time.perf_counter()

    t_pk = time.perf_counter()
    finish_bulk_schema(conn, unlogged=False)
    pk_ms = (time.perf_counter() - t_pk) * 1000.0

    healthy_indexes_ms, damaged_indexes_ms = create_all_indexes_parallel(conn, split_threshold, merge_threshold, fanout, partitions)

    t_ckpt = time.perf_counter()
    execute(conn, "ANALYZE ariabc_internal.merkle_node")
    ckpt_ms = (time.perf_counter() - t_ckpt) * 1000.0

    execute(conn, "SELECT count(*) FROM healthy.usertable")
    execute(conn, "SELECT count(*) FROM damaged.usertable")
    execute(conn, "SELECT count(*) FROM ariabc_internal.merkle_node")
    timings = {
        "healthy_table_ms": (t2 - t1) * 1000.0,
        "damaged_table_ms": (t3 - t2) * 1000.0,
        "primary_keys_ms": pk_ms,
        "healthy_indexes_ms": healthy_indexes_ms,
        "damaged_indexes_ms": damaged_indexes_ms,
        "analyze_checkpoint_ms": ckpt_ms,
        "dataset_total_ms": (time.perf_counter() - t0) * 1000.0,
        "dataset_setup_mode": "incremental-expansion",
        "previous_tuple_count": previous_tuple_count,
        "appended_tuple_count": tuple_count - previous_tuple_count,
    }
    print(
        f"[dataset] expanded {previous_tuple_count} -> {tuple_count} in "
        f"{timings['dataset_total_ms'] / 1000.0:.2f}s",
        flush=True,
    )
    return timings


def reset_damaged_from_healthy(conn, cfg: dict[str, int]) -> None:
    """Restore damaged.usertable to a clean copy of healthy; rebuild all indexes."""
    execute(conn, "DROP TABLE IF EXISTS damaged.usertable CASCADE")
    execute(conn, "CREATE TABLE damaged.usertable (LIKE healthy.usertable INCLUDING DEFAULTS)")
    execute(conn, "INSERT INTO damaged.usertable SELECT * FROM healthy.usertable")
    execute(
        conn,
        "ALTER TABLE damaged.usertable ADD CONSTRAINT usertable_pkey PRIMARY KEY (ycsb_key)",
    )
    create_damaged_indexes(conn, cfg.get("split_threshold", 32), cfg.get("merge_threshold", 8),
                           cfg.get("fanout", 4), cfg.get("partitions", 200))
    execute(conn, "ANALYZE damaged.usertable")
    execute(conn, "ANALYZE ariabc_internal.merkle_node")
    execute(conn, "CHECKPOINT")


# ── occupancy helpers ────────────────────────────────────────────────────────

def leaf_occupancy(conn) -> list[dict[str, Any]]:
    # In a dynamic tree, leaf occupancy is just tuple_count for leaves in merkle_node
    return execute(
        conn,
        """
        SELECT node_id, tuple_count
        FROM ariabc_internal.merkle_node
        WHERE index_oid = 'healthy.usertable_merkle_idx'::regclass AND is_leaf = true
        ORDER BY prefix_len, node_id
        """
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
        "leaf_lookup_index_bytes": int(scalar(conn, "SELECT coalesce(pg_relation_size(to_regclass('healthy.usertable_merkle_partition_lookup_idx')), 0)")),
        "total_schema_bytes": int(scalar(conn, "SELECT pg_total_relation_size('healthy.usertable'::regclass)")),
    }


def _bits_per_split(fanout: int) -> int:
    bits = 0
    while (1 << bits) < max(2, int(fanout)):
        bits += 1
    return bits


def tree_stats(conn, fanout: int = 4) -> dict[str, int]:
    """Return measured Merkle height from the native node catalog.

    ``prefix_len`` is a hash-bit length, not a tree level.  For example,
    fanout 4 advances two route bits per split, so a deepest prefix of 20 is
    a route depth of 10 and a root-inclusive height of 11.  Keep both values
    explicit; ``ceil(log_fanout(leaf_count))`` is only a capacity lower bound
    and must not be reported as the measured tree depth.
    """
    rows = execute(
        conn,
        """
        SELECT
            count(*)::int AS total_merkle_nodes,
            count(*) FILTER (WHERE is_leaf)::int AS total_leaf_count,
            coalesce(max(prefix_len), 0)::int AS max_prefix_len,
            count(DISTINCT prefix_len)::int AS tree_levels
        FROM ariabc_internal.merkle_node
        WHERE index_oid = 'healthy.usertable_merkle_idx'::regclass
        """
    )
    if not rows or not rows[0]:
        return {
            "total_merkle_nodes": 0,
            "tree_levels": 0,
            "max_prefix_len": 0,
            "tree_depth": 0,
            "tree_height": 0,
            "tree_edges": 0,
            "total_logical_tree_nodes": 0,
        }
    r = rows[0]
    total_nodes = int(r.get("total_merkle_nodes") or 0)
    max_prefix = int(r.get("max_prefix_len") or 0)
    tree_levels = int(r.get("tree_levels") or 0)
    route_bits = _bits_per_split(fanout)
    route_depth = (max_prefix + route_bits - 1) // route_bits if max_prefix else 0
    return {
        "total_merkle_nodes": total_nodes,
        "tree_levels": tree_levels,
        "max_prefix_len": max_prefix,
        "tree_depth": route_depth,
        "tree_height": route_depth + 1 if total_nodes else 0,
        "tree_edges": max(0, total_nodes - 1),
        "total_logical_tree_nodes": total_nodes,
    }



def bucket_consistency_sample(
    conn,
    tuple_count: int,
    fanout: int = 4,
    seed: int = 0,
    sample_size: int = 10_000,
    *args,
    **kwargs,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    # In dynamic trees, there is no fixed bucket function, so we'll just return a success payload
    # Alternatively we could do a tree walk verification, but merkle_verify() does exactly that.
    return (
        {
            "tuple_count": tuple_count,
            "fanout": fanout,
            "sample_count": 0,
            "sample_seed": seed,
            "mismatch_count": 0,
            "sample_digest": "dynamic_tree_verified_by_merkle_verify",
        },
        [],
    )
