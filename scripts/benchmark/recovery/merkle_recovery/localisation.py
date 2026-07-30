"""Merkle tree descent: detect mismatching leaves between healthy and damaged."""

from __future__ import annotations

import time
from typing import Any

from .db import execute
from .profiling import ProfileCollector, record_call

def detect_bad_leaves(
    conn,
    counters: dict[str, Any],
    *,
    prefix: str = "",
    operation_prefix: str = "",
    profiler: ProfileCollector | None = None,
    stage_name: str = "localisation",
    depth: int = 3,
) -> list[tuple[bytes, int]]:
    """Walk the dynamic Merkle tree and return sorted (node_id, prefix_len) of differing leaves."""
    bad: list[tuple[bytes, int]] = []
    
    counters[f"{prefix}child_hash_sql_calls"] = 0
    counters[f"{prefix}child_hash_nodes_read"] = 0
    counters[f"{prefix}leaf_nodes_found"] = 0
    counters[f"{prefix}tree_nodes_visited"] = 0

    # Start at the canonical root
    frontier: set[tuple[bytes, int]] = {(bytes(8), 0)}
    bits_per_split = 2  # fanout = 4 -> bits_per_split = 2

    while frontier:
        parents = sorted(list(frontier))
        frontier = set()
        counters[f"{prefix}tree_nodes_visited"] += len(parents)
        
        node_ids = [n for n, p in parents]
        prefix_lens = [p for n, p in parents]
        p_len = prefix_lens[0]
        boundary_len = p_len + depth * bits_per_split
        
        batch_sql = """
            SELECT node_id, prefix_len, is_leaf, hash 
            FROM merkle_get_descendants_batch(%s::regclass, %s::bytea[], %s::smallint[], %s::int4)
        """
        
        healthy_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}descendants_batch_healthy",
            schema="healthy",
            fn=lambda: execute(
                conn,
                batch_sql,
                ("healthy.usertable_merkle_idx", node_ids, prefix_lens, depth),
            ),
        )
        damaged_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}descendants_batch_damaged",
            schema="damaged",
            fn=lambda: execute(
                conn,
                batch_sql,
                ("damaged.usertable_merkle_idx", node_ids, prefix_lens, depth),
            ),
        )
        counters[f"{prefix}child_hash_sql_calls"] += 2
        counters[f"{prefix}child_hash_nodes_read"] += len(healthy_rows) + len(damaged_rows)

        damaged_children = {
            (bytes(row["node_id"]), int(row["prefix_len"])): row["hash"]
            for row in damaged_rows
        }

        compare_start = time.perf_counter_ns()
        for row in healthy_rows:
            node_p_len = int(row["prefix_len"])
            if node_p_len <= p_len:
                continue
            key = (bytes(row["node_id"]), node_p_len)
            if row["hash"] != damaged_children.get(key):
                if row["is_leaf"]:
                    if key not in bad:
                        bad.append(key)
                        counters[f"{prefix}leaf_nodes_found"] += 1
                else:
                    # Only add to frontier if this internal node is at the batch expansion boundary
                    if node_p_len >= boundary_len:
                        frontier.add(key)
                    
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage=stage_name,
                operation=f"{operation_prefix}child_hash_batch_compare_cpu",
                client_wall_ns=time.perf_counter_ns() - compare_start,
                rows_returned=len(healthy_rows) + len(damaged_rows),
            )

    return sorted(bad)
