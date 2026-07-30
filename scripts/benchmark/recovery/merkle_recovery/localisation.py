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
) -> list[tuple[bytes, int]]:
    """Walk the dynamic Merkle tree and return sorted (node_id, prefix_len) of differing leaves."""
    bad: list[tuple[bytes, int]] = []
    
    counters[f"{prefix}child_hash_sql_calls"] = 0
    counters[f"{prefix}child_hash_nodes_read"] = 0
    counters[f"{prefix}leaf_nodes_found"] = 0
    counters[f"{prefix}tree_nodes_visited"] = 0

    # Start at the canonical root
    frontier: list[tuple[bytes, int]] = [(bytes(8), 0)]

    while frontier:
        parents = frontier
        frontier = []
        counters[f"{prefix}tree_nodes_visited"] += len(parents)
        
        node_ids = [n for n, p in parents]
        prefix_lens = [p for n, p in parents]
        
        # Use depth=4 as per Plan_review.md Section 11 & 13 for optimal per-roundtrip progress
        depth = 4
        
        batch_sql = """
            SELECT p.parent_node_id, p.parent_prefix_len, d.node_id, d.prefix_len, d.is_leaf, d.hash 
            FROM unnest(%s::bytea[], %s::smallint[]) AS p(parent_node_id, parent_prefix_len)
            CROSS JOIN LATERAL merkle_get_descendants_batch(%s::regclass, p.parent_node_id, p.parent_prefix_len, %s::int4) d
            WHERE d.prefix_len > p.parent_prefix_len
        """
        
        healthy_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}descendants_batch_healthy",
            schema="healthy",
            fn=lambda: execute(
                conn,
                batch_sql,
                (node_ids, prefix_lens, "healthy.usertable_merkle_idx", depth),
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
                (node_ids, prefix_lens, "damaged.usertable_merkle_idx", depth),
            ),
        )
        counters[f"{prefix}child_hash_sql_calls"] += 2
        counters[f"{prefix}child_hash_nodes_read"] += len(healthy_rows) + len(damaged_rows)

        damaged_children = {
            (bytes(row["node_id"]), int(row["prefix_len"])): row["hash"]
            for row in damaged_rows
        }

        # Track which nodes have their children present in this batch
        parents_in_batch = {
            (bytes(row["parent_node_id"]), int(row["parent_prefix_len"]))
            for row in healthy_rows
        }
        
        compare_start = time.perf_counter_ns()
        for row in healthy_rows:
            key = (bytes(row["node_id"]), int(row["prefix_len"]))
            if row["hash"] != damaged_children.get(key):
                if row["is_leaf"]:
                    if key not in bad:
                        bad.append(key)
                        counters[f"{prefix}leaf_nodes_found"] += 1
                else:
                    # Only add to frontier if its children were not fetched in this batch
                    if key not in parents_in_batch and key not in frontier:
                        frontier.append(key)
                    
        if profiler is not None and profiler.enabled:
            profiler.record(
                stage=stage_name,
                operation=f"{operation_prefix}child_hash_batch_compare_cpu",
                client_wall_ns=time.perf_counter_ns() - compare_start,
                rows_returned=len(healthy_rows) + len(damaged_rows),
            )

    return sorted(bad)
