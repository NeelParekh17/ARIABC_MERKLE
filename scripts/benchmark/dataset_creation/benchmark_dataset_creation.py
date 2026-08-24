#!/usr/bin/env python3
"""
benchmark_dataset_creation.py
Benchmark dataset creation and indexing time across scales (1k, 10k, 100k, 1M, etc.)
with all indexes: B-Tree Primary Key, AriaBC Merkle AM Index, and Partition Lookup B-Tree.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


def get_connection(dsn: str):
    """Establish a PostgreSQL database connection with auto-commit."""
    try:
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("SET enable_merkle_index = on")
            cur.execute("SET merkle_apply_synchronous_direct = on")
        return conn
    except ImportError:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET enable_merkle_index = on")
            cur.execute("SET merkle_apply_synchronous_direct = on")
        return conn


def execute_sql(conn, sql: str, params: Tuple = None) -> List[Dict[str, Any]]:
    """Execute SQL query and return rows as list of dicts."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                if rows and isinstance(rows[0], dict):
                    return rows
                return [dict(zip(columns, row)) for row in rows]
            return []
    except Exception as exc:
        raise RuntimeError(f"SQL execution failed for query:\n{sql}\nError: {exc}") from exc


def scalar_sql(conn, sql: str, params: Tuple = None) -> Any:
    """Execute SQL query and return a single scalar value."""
    rows = execute_sql(conn, sql, params)
    if rows:
        return list(rows[0].values())[0]
    return None


def ensure_environment(conn) -> None:
    """Validate that Merkle extension functions and settings are active."""
    execute_sql(conn, "SET enable_merkle_index = on")
    execute_sql(conn, "SET merkle_apply_synchronous_direct = on")

    enable_merkle = str(scalar_sql(conn, "SHOW enable_merkle_index")).strip().lower()
    merkle_direct = str(scalar_sql(conn, "SHOW merkle_apply_synchronous_direct")).strip().lower()

    if enable_merkle != "on" or merkle_direct != "on":
        raise RuntimeError(
            f"Required Merkle settings are not active: enable_merkle_index={enable_merkle}, "
            f"merkle_apply_synchronous_direct={merkle_direct}"
        )

    # Ensure helper functions are marked parallel safe for fast index construction
    for fn in ("merkle_key_hash(anyelement)", "merkle_partition_for_hash(bytea, integer)", "merkle_tuple_hash(record)"):
        try:
            execute_sql(conn, f"ALTER FUNCTION {fn} PARALLEL SAFE")
        except Exception:
            pass


def bits_per_split(fanout: int) -> int:
    bits = 0
    while (1 << bits) < max(2, int(fanout)):
        bits += 1
    return bits


def measure_dataset_creation_single_run(
    conn,
    tuple_count: int,
    fanout: int = 4,
    split_threshold: int = 32,
    merge_threshold: int = 8,
    partitions: int = 200,
    synchronous_commit: str = "off",
) -> Dict[str, Any]:
    """
    Perform a clean, isolated build of the dataset with all indexes for a given tuple count.
    Measures each sub-phase precisely.
    """
    timings: Dict[str, float] = {}
    t_start_total = time.perf_counter()

    # Step 1: Clean table and catalog reset
    t0 = time.perf_counter()
    execute_sql(conn, f"SET synchronous_commit = {synchronous_commit}")
    execute_sql(conn, "DROP TABLE IF EXISTS usertable CASCADE")
    try:
        execute_sql(conn, "TRUNCATE ariabc_internal.merkle_node CASCADE")
        execute_sql(conn, "UPDATE ariabc_internal.merkle_apply_state SET applied_seq = 0, state = 0, error_text = NULL")
        execute_sql(conn, "UPDATE ariabc_internal.merkle_apply_counter SET next_seq = 0, terminal_prefix_seq = 0")
    except Exception:
        pass

    create_table_sql = """
        CREATE TABLE usertable (
            ycsb_key BIGINT NOT NULL,
            field0 TEXT NOT NULL, field1 TEXT NOT NULL,
            field2 TEXT NOT NULL, field3 TEXT NOT NULL,
            field4 TEXT NOT NULL, field5 TEXT NOT NULL,
            field6 TEXT NOT NULL, field7 TEXT NOT NULL,
            field8 TEXT NOT NULL, field9 TEXT NOT NULL
        )
    """
    execute_sql(conn, create_table_sql)
    timings["schema_reset_ms"] = (time.perf_counter() - t0) * 1000.0

    # Step 2: Bulk Heap Data Population
    t1 = time.perf_counter()
    populate_sql = """
        INSERT INTO usertable
        SELECT gs::bigint,
               'field0-' || gs, 'field1-' || gs, 'field2-' || gs,
               'field3-' || gs, 'field4-' || gs, 'field5-' || gs,
               'field6-' || gs, 'field7-' || gs, 'field8-' || gs,
               'field9-' || gs
        FROM generate_series(1, %s) AS gs
    """
    execute_sql(conn, populate_sql, (tuple_count,))
    timings["heap_populate_ms"] = (time.perf_counter() - t1) * 1000.0

    # Step 3: Primary Key B-Tree Index Creation
    t2 = time.perf_counter()
    execute_sql(conn, "ALTER TABLE usertable ADD CONSTRAINT usertable_pkey PRIMARY KEY (ycsb_key)")
    timings["primary_key_btree_ms"] = (time.perf_counter() - t2) * 1000.0

    # Step 4: Dynamic Merkle AM Index Creation
    t3 = time.perf_counter()
    merkle_index_sql = f"""
        CREATE INDEX usertable_merkle_idx
        ON usertable USING merkle (ycsb_key)
        WITH (
            fanout = {fanout},
            split_threshold = {split_threshold},
            merge_threshold = {merge_threshold},
            partitions = {partitions}
        )
    """
    execute_sql(conn, merkle_index_sql)
    timings["merkle_am_index_ms"] = (time.perf_counter() - t3) * 1000.0

    # Step 5: Partition / Hash Lookup B-Tree Expression Index Creation
    t4 = time.perf_counter()
    lookup_index_sql = f"""
        CREATE INDEX usertable_merkle_partition_lookup_idx
        ON usertable (
            merkle_partition_for_hash(merkle_key_hash(ycsb_key), {partitions}),
            merkle_key_hash(ycsb_key),
            ycsb_key
        )
    """
    execute_sql(conn, lookup_index_sql)
    timings["lookup_btree_index_ms"] = (time.perf_counter() - t4) * 1000.0

    # Step 6: Statistics Analysis
    t5 = time.perf_counter()
    execute_sql(conn, "ANALYZE usertable")
    execute_sql(conn, "ANALYZE ariabc_internal.merkle_node")
    timings["analyze_ms"] = (time.perf_counter() - t5) * 1000.0

    timings["total_dataset_creation_ms"] = (time.perf_counter() - t_start_total) * 1000.0
    timings["total_dataset_creation_s"] = timings["total_dataset_creation_ms"] / 1000.0
    timings["tuples_per_second"] = (
        (tuple_count / (timings["total_dataset_creation_ms"] / 1000.0))
        if timings["total_dataset_creation_ms"] > 0
        else 0.0
    )

    # Verification & Integrity Checks
    actual_rows = int(scalar_sql(conn, "SELECT count(*) FROM usertable"))
    if actual_rows != tuple_count:
        raise RuntimeError(f"Verification failed: expected {tuple_count} rows, got {actual_rows}")

    merkle_valid = bool(scalar_sql(conn, "SELECT merkle_verify('usertable'::regclass)"))
    if not merkle_valid:
        raise RuntimeError("Verification failed: merkle_verify('usertable') returned false")

    # Sizing Metrics
    heap_bytes = int(scalar_sql(conn, "SELECT pg_relation_size('usertable'::regclass)"))
    pkey_bytes = int(scalar_sql(conn, "SELECT pg_relation_size('usertable_pkey'::regclass)"))
    merkle_bytes = int(scalar_sql(conn, "SELECT pg_relation_size('usertable_merkle_idx'::regclass)"))
    lookup_bytes = int(scalar_sql(conn, "SELECT pg_relation_size('usertable_merkle_partition_lookup_idx'::regclass)"))
    total_bytes = int(scalar_sql(conn, "SELECT pg_total_relation_size('usertable'::regclass)"))

    # Merkle Internal Node Catalog Metrics
    node_stats = execute_sql(
        conn,
        """
        SELECT
            count(*)::int AS total_nodes,
            count(*) FILTER (WHERE is_leaf)::int AS leaf_nodes,
            coalesce(max(prefix_len), 0)::int AS max_prefix_len
        FROM ariabc_internal.merkle_node
        WHERE index_oid = 'usertable_merkle_idx'::regclass
        """,
    )
    total_nodes = int(node_stats[0]["total_nodes"] or 0)
    leaf_nodes = int(node_stats[0]["leaf_nodes"] or 0)
    max_prefix = int(node_stats[0]["max_prefix_len"] or 0)
    b_split = bits_per_split(fanout)
    route_depth = (max_prefix + b_split - 1) // b_split if max_prefix else 0

    metrics = {
        "tuple_count": tuple_count,
        "fanout": fanout,
        "split_threshold": split_threshold,
        "merge_threshold": merge_threshold,
        "partitions": partitions,
        "synchronous_commit": synchronous_commit,
        "timings_ms": timings,
        "sizes_bytes": {
            "heap_bytes": heap_bytes,
            "pkey_bytes": pkey_bytes,
            "merkle_index_bytes": merkle_bytes,
            "lookup_index_bytes": lookup_bytes,
            "total_relation_bytes": total_bytes,
            "heap_mb": round(heap_bytes / (1024 * 1024), 2),
            "pkey_mb": round(pkey_bytes / (1024 * 1024), 2),
            "merkle_index_mb": round(merkle_bytes / (1024 * 1024), 2),
            "lookup_index_mb": round(lookup_bytes / (1024 * 1024), 2),
            "total_relation_mb": round(total_bytes / (1024 * 1024), 2),
        },
        "merkle_stats": {
            "total_merkle_nodes": total_nodes,
            "leaf_nodes": leaf_nodes,
            "max_prefix_len": max_prefix,
            "tree_depth": route_depth,
            "tree_height": route_depth + 1 if total_nodes else 0,
            "merkle_verify_passed": merkle_valid,
        },
    }
    return metrics


def run_benchmark_suite(
    dsn: str,
    scales: List[int],
    repetitions: int = 3,
    fanout: int = 4,
    split_threshold: int = 32,
    merge_threshold: int = 8,
    partitions: int = 200,
    synchronous_commit: str = "off",
    output_dir: Path = Path("."),
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Run the complete dataset creation benchmark across all scale points."""
    conn = get_connection(dsn)
    ensure_environment(conn)

    print("=" * 80)
    print("  AriaBC Dataset Creation Benchmark (Heap + PK B-Tree + Merkle AM + Lookup Index)")
    print(f"  Scales: {scales}")
    print(f"  Repetitions per scale: {repetitions}")
    print(f"  Geometry: Fanout={fanout}, Split={split_threshold}, Merge={merge_threshold}, Partitions={partitions}")
    print(f"  Synchronous Commit: {synchronous_commit}")
    print("=" * 80)

    all_results: List[Dict[str, Any]] = []
    aggregated_results: List[Dict[str, Any]] = []

    for scale in scales:
        scale_label = f"{scale // 1_000_000}M" if scale >= 1_000_000 else f"{scale // 1_000}k" if scale >= 1_000 else str(scale)
        print(f"\n>>> Benchmarking Scale: {scale:,} tuples ({scale_label}) ...")

        runs_for_scale: List[Dict[str, Any]] = []
        for rep in range(1, repetitions + 1):
            sys.stdout.write(f"  [Repetition {rep}/{repetitions}] Building dataset ... ")
            sys.stdout.flush()

            metrics = measure_dataset_creation_single_run(
                conn=conn,
                tuple_count=scale,
                fanout=fanout,
                split_threshold=split_threshold,
                merge_threshold=merge_threshold,
                partitions=partitions,
                synchronous_commit=synchronous_commit,
            )
            metrics["repetition"] = rep
            metrics["scale_label"] = scale_label
            runs_for_scale.append(metrics)
            all_results.append(metrics)

            t_ms = metrics["timings_ms"]["total_dataset_creation_ms"]
            t_s = metrics["timings_ms"]["total_dataset_creation_s"]
            tps = metrics["timings_ms"]["tuples_per_second"]
            print(f"Done in {t_s:.3f}s ({t_ms:.1f} ms, {tps:,.0f} tuples/s) [Merkle Verify: OK]")

        # Aggregate metrics across repetitions
        tot_times = [r["timings_ms"]["total_dataset_creation_ms"] for r in runs_for_scale]
        heap_times = [r["timings_ms"]["heap_populate_ms"] for r in runs_for_scale]
        pkey_times = [r["timings_ms"]["primary_key_btree_ms"] for r in runs_for_scale]
        merkle_times = [r["timings_ms"]["merkle_am_index_ms"] for r in runs_for_scale]
        lookup_times = [r["timings_ms"]["lookup_btree_index_ms"] for r in runs_for_scale]
        analyze_times = [r["timings_ms"]["analyze_ms"] for r in runs_for_scale]
        tps_list = [r["timings_ms"]["tuples_per_second"] for r in runs_for_scale]

        sizes = runs_for_scale[0]["sizes_bytes"]
        m_stats = runs_for_scale[0]["merkle_stats"]

        agg = {
            "scale": scale,
            "scale_label": scale_label,
            "repetitions": repetitions,
            "total_ms_mean": statistics.mean(tot_times),
            "total_ms_stddev": statistics.stdev(tot_times) if len(tot_times) > 1 else 0.0,
            "total_ms_min": min(tot_times),
            "total_ms_max": max(tot_times),
            "total_s_mean": statistics.mean(tot_times) / 1000.0,
            "heap_ms_mean": statistics.mean(heap_times),
            "pkey_btree_ms_mean": statistics.mean(pkey_times),
            "merkle_am_ms_mean": statistics.mean(merkle_times),
            "lookup_btree_ms_mean": statistics.mean(lookup_times),
            "analyze_ms_mean": statistics.mean(analyze_times),
            "tps_mean": statistics.mean(tps_list),
            "heap_mb": sizes["heap_mb"],
            "pkey_mb": sizes["pkey_mb"],
            "merkle_mb": sizes["merkle_index_mb"],
            "lookup_mb": sizes["lookup_index_mb"],
            "total_mb": sizes["total_relation_mb"],
            "merkle_total_nodes": m_stats["total_merkle_nodes"],
            "merkle_leaf_nodes": m_stats["leaf_nodes"],
            "merkle_tree_height": m_stats["tree_height"],
        }
        aggregated_results.append(agg)

    conn.close()

    # Print Formatted ASCII Summary Table
    print_summary_table(aggregated_results)

    # Save CSV and JSON
    output_dir.mkdir(parents=True, exist_ok=True)
    save_results(output_dir, all_results, aggregated_results)

    return all_results, {"aggregated": aggregated_results}


def print_summary_table(aggregated: List[Dict[str, Any]]) -> None:
    """Print clean summary ASCII table to stdout."""
    print("\n" + "=" * 120)
    print("                                     DATASET CREATION TIME BENCHMARK SUMMARY TABLE")
    print("=" * 120)
    header = (
        f"{'Scale':>8} | {'Heap (ms)':>10} | {'PK BTree(ms)':>12} | {'Merkle AM(ms)':>13} | "
        f"{'Lookup(ms)':>10} | {'Total Time':>11} | {'Throughput':>14} | {'Total Size':>10} | {'Merkle Height':>13}"
    )
    print(header)
    print("-" * 120)

    for r in aggregated:
        tot_str = f"{r['total_s_mean']:.3f} s" if r["total_s_mean"] >= 1.0 else f"{r['total_ms_mean']:.1f} ms"
        tps_str = f"{r['tps_mean']:,.0f} tup/s"
        size_str = f"{r['total_mb']:.2f} MB"
        print(
            f"{r['scale_label']:>8} | "
            f"{r['heap_ms_mean']:>10.2f} | "
            f"{r['pkey_btree_ms_mean']:>12.2f} | "
            f"{r['merkle_am_ms_mean']:>13.2f} | "
            f"{r['lookup_btree_ms_mean']:>10.2f} | "
            f"{tot_str:>11} | "
            f"{tps_str:>14} | "
            f"{size_str:>10} | "
            f"{r['merkle_tree_height']:>13}"
        )
    print("=" * 120 + "\n")


def save_results(output_dir: Path, raw_results: List[Dict[str, Any]], aggregated: List[Dict[str, Any]]) -> None:
    """Save raw and aggregated metrics to CSV and JSON."""
    csv_path = output_dir / "dataset_creation_results.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "scale", "scale_label", "repetitions",
            "heap_ms_mean", "pkey_btree_ms_mean", "merkle_am_ms_mean", "lookup_btree_ms_mean", "analyze_ms_mean",
            "total_ms_mean", "total_ms_stddev", "total_s_mean", "tps_mean",
            "heap_mb", "pkey_mb", "merkle_mb", "lookup_mb", "total_mb",
            "merkle_total_nodes", "merkle_leaf_nodes", "merkle_tree_height"
        ])
        for r in aggregated:
            writer.writerow([
                r["scale"], r["scale_label"], r["repetitions"],
                round(r["heap_ms_mean"], 3), round(r["pkey_btree_ms_mean"], 3),
                round(r["merkle_am_ms_mean"], 3), round(r["lookup_btree_ms_mean"], 3),
                round(r["analyze_ms_mean"], 3), round(r["total_ms_mean"], 3),
                round(r["total_ms_stddev"], 3), round(r["total_s_mean"], 4),
                round(r["tps_mean"], 1),
                r["heap_mb"], r["pkey_mb"], r["merkle_mb"], r["lookup_mb"], r["total_mb"],
                r["merkle_total_nodes"], r["merkle_leaf_nodes"], r["merkle_tree_height"]
            ])
    print(f"Saved aggregated CSV: {csv_path}")

    json_path = output_dir / "dataset_creation_results.json"
    with open(json_path, "w") as f:
        json.dump({"aggregated": aggregated, "raw_runs": raw_results}, f, indent=2)
    print(f"Saved complete JSON: {json_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark dataset creation time with all indexes.")
    parser.add_argument("--dsn", default="host=localhost port=55432 dbname=postgres user=postgres", help="PostgreSQL DSN string")
    parser.add_argument("--scales", default="1000,10000,100000,1000000", help="Comma-separated tuple counts (e.g. '1000,10000,100000,1000000')")
    parser.add_argument("--repetitions", type=int, default=3, help="Number of repetitions per scale")
    parser.add_argument("--fanout", type=int, default=4, help="Merkle tree fanout (default: 4)")
    parser.add_argument("--split-threshold", type=int, default=32, help="Merkle leaf split threshold (default: 32)")
    parser.add_argument("--merge-threshold", type=int, default=8, help="Merkle leaf merge threshold (default: 8)")
    parser.add_argument("--partitions", type=int, default=200, help="Number of tree partitions (default: 200)")
    parser.add_argument("--synchronous-commit", choices=["on", "off"], default="off", help="Synchronous commit mode during creation (default: off)")
    parser.add_argument("--output-dir", default="results", help="Directory to save benchmark output artifacts")
    return parser.parse_args()


def main():
    args = parse_args()
    scales = [int(s.strip()) for s in args.scales.split(",") if s.strip()]
    output_dir = Path(args.output_dir)

    run_benchmark_suite(
        dsn=args.dsn,
        scales=scales,
        repetitions=args.repetitions,
        fanout=args.fanout,
        split_threshold=args.split_threshold,
        merge_threshold=args.merge_threshold,
        partitions=args.partitions,
        synchronous_commit=args.synchronous_commit,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
