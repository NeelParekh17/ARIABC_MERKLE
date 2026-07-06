"""Merkle tree descent: detect mismatching leaves between healthy and damaged."""

from __future__ import annotations

from typing import Any

from .db import execute, geometry


def detect_bad_leaves(
    conn,
    counters: dict[str, Any],
    *,
    prefix: str = "",
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

    def compare_node(partition: int, node: int) -> None:
        if node >= leaf_start:
            leaf_id = partition * geo["leaves_per_partition"] + (node - leaf_start)
            bad.append(leaf_id)
            counters[f"{prefix}leaf_nodes_found"] += 1
            return
        counters[f"{prefix}tree_nodes_visited"] += 1
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
            if child["hash"] != damaged_children.get(child_node):
                compare_node(partition, child_node)

    healthy_roots = {
        int(r["partition"]): r["hash"]
        for r in execute(
            conn,
            "SELECT * FROM merkle_get_partition_root_hashes('healthy.usertable_merkle_idx'::regclass)",
        )
    }
    damaged_roots = {
        int(r["partition"]): r["hash"]
        for r in execute(
            conn,
            "SELECT * FROM merkle_get_partition_root_hashes('damaged.usertable_merkle_idx'::regclass)",
        )
    }
    bad_partitions = 0
    for partition, healthy_hash in healthy_roots.items():
        if healthy_hash != damaged_roots.get(partition):
            bad_partitions += 1
            compare_node(partition, 1)
    counters[f"{prefix}bad_partition_count"] = bad_partitions
    return sorted(bad)
