"""Merkle tree descent: detect mismatching leaves between healthy and damaged."""

from __future__ import annotations

import time
from typing import Any

from .db import execute, geometry
from .profiling import ProfileCollector, record_call


def detect_bad_leaves(
    conn,
    counters: dict[str, Any],
    *,
    prefix: str = "",
    operation_prefix: str = "",
    profiler: ProfileCollector | None = None,
    stage_name: str = "localisation",
) -> list[int]:
    """Walk the Merkle tree and return sorted leaf IDs that differ.

    Counters recorded (all prefixed with *prefix*)
    -----------------------------------------------
    partition_root_batches          always 2 (one call per schema)
    partition_root_nodes_read       partitions × 2
    child_hash_sql_calls            incremented per internal-node comparison
    child_hash_nodes_read           incremented per child row returned
    leaf_nodes_found                number of differing leaf IDs found
    bad_partition_count             number of partitions whose roots differ
    tree_nodes_visited              total internal nodes visited during descent
    """
    geo = geometry(conn)
    bad: list[int] = []
    counters[f"{prefix}partition_root_batches"] = 2
    counters[f"{prefix}partition_root_nodes_read"] = geo["partitions"] * 2
    counters[f"{prefix}child_hash_sql_calls"] = 0
    counters[f"{prefix}child_hash_nodes_read"] = 0
    counters[f"{prefix}leaf_nodes_found"] = 0
    counters[f"{prefix}bad_partition_count"] = 0
    counters[f"{prefix}tree_nodes_visited"] = 0
    leaf_start = geo["nodes_per_partition"] - geo["leaves_per_partition"] + 1

    healthy_root_rows = record_call(
        profiler,
        stage=stage_name,
        operation=f"{operation_prefix}root_hashes_healthy",
        schema="healthy",
        fn=lambda: execute(
            conn,
            "SELECT * FROM merkle_get_partition_root_hashes('healthy.usertable_merkle_idx'::regclass)",
        ),
    )
    damaged_root_rows = record_call(
        profiler,
        stage=stage_name,
        operation=f"{operation_prefix}root_hashes_damaged",
        schema="damaged",
        fn=lambda: execute(
            conn,
            "SELECT * FROM merkle_get_partition_root_hashes('damaged.usertable_merkle_idx'::regclass)",
        ),
    )
    healthy_roots = {int(r["partition"]): r["hash"] for r in healthy_root_rows}
    damaged_roots = {int(r["partition"]): r["hash"] for r in damaged_root_rows}
    bad_partitions = 0
    frontier: list[tuple[int, int]] = []
    for partition, healthy_hash in healthy_roots.items():
        root_compare_start = time.perf_counter_ns()
        damaged_hash = damaged_roots.get(partition)
        mismatch = healthy_hash != damaged_hash
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage=stage_name,
                operation=f"{operation_prefix}root_hash_compare_cpu",
                partition=partition,
                node_in_partition=1,
                client_wall_ns=time.perf_counter_ns() - root_compare_start,
                rows_returned=2,
            )
        if mismatch:
            bad_partitions += 1
            frontier.append((partition, 1))
    counters[f"{prefix}bad_partition_count"] = bad_partitions

    # Descend breadth-first.  Each level costs two SQL round trips regardless
    # of how many mismatching nodes it contains.
    while frontier:
        parents: list[tuple[int, int]] = []
        for partition, node in frontier:
            if node >= leaf_start:
                bad.append(
                    partition * geo["leaves_per_partition"] + (node - leaf_start)
                )
                counters[f"{prefix}leaf_nodes_found"] += 1
            else:
                parents.append((partition, node))
        if not parents:
            break

        counters[f"{prefix}tree_nodes_visited"] += len(parents)
        partitions = [partition for partition, _ in parents]
        nodes = [node for _, node in parents]
        batch_sql = (
            "SELECT partition, parent_node_in_partition, "
            "child_node_in_partition, hash "
            "FROM merkle_get_children_batch(%s::regclass, %s::int4[], %s::int4[])"
        )
        healthy_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}child_hashes_batch_healthy",
            schema="healthy",
            fn=lambda: execute(
                conn,
                batch_sql,
                ("healthy.usertable_merkle_idx", partitions, nodes),
            ),
        )
        damaged_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}child_hashes_batch_damaged",
            schema="damaged",
            fn=lambda: execute(
                conn,
                batch_sql,
                ("damaged.usertable_merkle_idx", partitions, nodes),
            ),
        )
        counters[f"{prefix}child_hash_sql_calls"] += 2
        counters[f"{prefix}child_hash_nodes_read"] += len(healthy_rows) + len(damaged_rows)

        damaged_children = {
            (
                int(row["partition"]),
                int(row["parent_node_in_partition"]),
                int(row["child_node_in_partition"]),
            ): row["hash"]
            for row in damaged_rows
        }
        compare_start = time.perf_counter_ns()
        next_frontier: list[tuple[int, int]] = []
        for row in healthy_rows:
            key = (
                int(row["partition"]),
                int(row["parent_node_in_partition"]),
                int(row["child_node_in_partition"]),
            )
            if row["hash"] != damaged_children.get(key):
                next_frontier.append((key[0], key[2]))
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage=stage_name,
                operation=f"{operation_prefix}child_hash_batch_compare_cpu",
                client_wall_ns=time.perf_counter_ns() - compare_start,
                rows_returned=len(healthy_rows) + len(damaged_rows),
            )
        frontier = next_frontier

    return sorted(bad)
