"""Partition-aware Merkle tree descent for recovery localisation."""

from __future__ import annotations

import time
from typing import Any

from .db import execute, geometry, partition_roots
from .profiling import ProfileCollector, record_call


def _extend_node_id(node_id: bytes, prefix_len: int, bucket: int, bits_per_split: int) -> bytes:
    """Return the canonical child coordinate used by the native tree."""
    child = bytearray(node_id)
    for offset in range(bits_per_split):
        bit_position = prefix_len + offset
        byte_position = bit_position // 8
        bit_in_byte = 7 - (bit_position % 8)
        bit = (bucket >> (bits_per_split - offset - 1)) & 1
        child[byte_position] = (
            child[byte_position] & ~(1 << bit_in_byte)
        ) | (bit << bit_in_byte)
    return bytes(child)


def detect_bad_leaves(
    conn,
    counters: dict[str, Any],
    *,
    prefix: str = "",
    operation_prefix: str = "",
    profiler: ProfileCollector | None = None,
    stage_name: str = "localisation",
    depth: int | None = 30,
    fanout: int | None = None,
    partition_aware: bool = False,
    target_partitions: list[int] | set[int] | None = None,
    levels_per_batch: int = 1,
) -> list[tuple]:
    """Return differing leaves as ``(partition, node_id, prefix_len)``.

    ``partition_aware=False`` retains the old two-field return shape for
    callers written against pre-partition manifests.  The descent itself is
    still partition-scoped, so duplicate root rows can never cross-contaminate
    the comparison.
    """
    counters[f"{prefix}child_hash_sql_calls"] = 0
    counters[f"{prefix}child_hash_nodes_read"] = 0
    counters[f"{prefix}leaf_nodes_found"] = 0
    counters[f"{prefix}tree_nodes_visited"] = 0

    healthy_roots = record_call(
        profiler,
        stage=stage_name,
        operation=f"{operation_prefix}partition_roots_healthy",
        schema="healthy",
        fn=lambda: partition_roots(conn, "healthy"),
    )
    damaged_roots = record_call(
        profiler,
        stage=stage_name,
        operation=f"{operation_prefix}partition_roots_damaged",
        schema="damaged",
        fn=lambda: partition_roots(conn, "damaged"),
    )
    if target_partitions is not None:
        target_set = set(target_partitions)
        healthy_roots = {k: v for k, v in healthy_roots.items() if k in target_set}
        damaged_roots = {k: v for k, v in damaged_roots.items() if k in target_set}

    counters[f"{prefix}partition_root_batches"] = 1
    counters[f"{prefix}partition_root_hash_sql_calls"] = 2
    counters[f"{prefix}partition_root_nodes_read"] = len(healthy_roots) + len(damaged_roots)
    mismatched_partitions = [
        partition for partition in sorted(set(healthy_roots) | set(damaged_roots))
        if healthy_roots.get(partition) != damaged_roots.get(partition)
    ]
    counters[f"{prefix}bad_partition_count"] = len(mismatched_partitions)

    if not mismatched_partitions:
        return [] if partition_aware else []

    if fanout is None or depth is None:
        geometry_info = geometry(conn, "healthy")
    else:
        geometry_info = {}
    if fanout is None:
        fanout = int(geometry_info.get("fanout", 4))
    fanout = int(fanout)
    if depth is None:
        # MAX_PREFIX_LEN is 60 bits and the smallest supported fanout advances
        # one bit per level. This bound avoids a measured-path catalog scan;
        # the frontier naturally terminates as soon as it reaches leaves.
        depth = max(1, (60 + max(1, (fanout - 1).bit_length()) - 1) // max(1, (fanout - 1).bit_length()))
    depth = int(depth)
    bits_per_split = 0
    while (1 << bits_per_split) < fanout and bits_per_split < 8:
        bits_per_split += 1
    bits_per_split = max(1, bits_per_split)

    bad: set[tuple] = set()
    frontier: set[tuple[int, bytes, int]] = {
        (partition_id, bytes(8), 0) for partition_id in mismatched_partitions
    }
    subtree_sql = """
        WITH wanted(partition_id, node_id, prefix_len) AS (
            SELECT * FROM unnest(%s::int4[], %s::bytea[], %s::int2[])
        )
        SELECT n.partition_id, n.node_id, n.prefix_len, n.is_leaf, n.hash
        FROM wanted w
        JOIN ariabc_internal.merkle_node n
          ON n.partition_id = w.partition_id
         AND n.node_id = w.node_id
         AND n.prefix_len = w.prefix_len
        WHERE n.index_oid = %s::regclass
        ORDER BY n.partition_id, n.prefix_len, n.node_id
    """
    batch_ordinal = 0
    step_limit = max(1, int(levels_per_batch))

    while frontier:
        batch_ordinal += 1
        base_prefix_len = min(item[2] for item in frontier)
        current_parents = sorted(item for item in frontier if item[2] == base_prefix_len)
        frontier = {item for item in frontier if item[2] != base_prefix_len}

        # Expand candidate nodes up to step_limit levels below current_parents
        batch_candidates: list[tuple[int, bytes, int]] = []
        parent_map: dict[tuple[int, bytes, int], tuple[int, bytes, int]] = {}

        level_parents = current_parents
        for _step in range(1, step_limit + 1):
            next_level_candidates: list[tuple[int, bytes, int]] = []
            for parent in level_parents:
                p_id, p_node_id, p_prefix_len = parent
                route_depth = p_prefix_len // bits_per_split
                if route_depth < depth:
                    child_prefix_len = p_prefix_len + bits_per_split
                    for bucket in range(fanout):
                        child_node_id = _extend_node_id(p_node_id, p_prefix_len, bucket, bits_per_split)
                        child_key = (p_id, child_node_id, child_prefix_len)
                        if child_key not in parent_map:
                            parent_map[child_key] = parent
                            batch_candidates.append(child_key)
                            next_level_candidates.append(child_key)
            level_parents = next_level_candidates
            if not level_parents:
                break

        if not batch_candidates:
            continue

        partitions = [item[0] for item in batch_candidates]
        node_ids = [item[1] for item in batch_candidates]
        prefix_lens = [item[2] for item in batch_candidates]

        counters[f"{prefix}tree_nodes_visited"] += len(batch_candidates)

        if profiler is not None:
            profiler.record_localisation_batch(
                stage=stage_name,
                batch_ordinal=batch_ordinal,
                prefix_len=base_prefix_len,
                node_ids=node_ids,
                max_depth=depth,
                partition_ids=partitions,
                prefix_lens=prefix_lens,
            )

        def fetch(schema: str) -> list[dict[str, Any]]:
            return execute(
                conn,
                subtree_sql,
                (partitions, node_ids, prefix_lens, f"{schema}.usertable_merkle_idx"),
            )

        healthy_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}partition_nodes_healthy",
            schema="healthy",
            localisation_prefix_len=base_prefix_len,
            localisation_frontier_nodes=len(current_parents),
            localisation_batch_depth=batch_ordinal,
            localisation_max_depth=depth,
            fn=lambda: fetch("healthy"),
        )
        damaged_rows = record_call(
            profiler,
            stage=stage_name,
            operation=f"{operation_prefix}partition_nodes_damaged",
            schema="damaged",
            localisation_prefix_len=base_prefix_len,
            localisation_frontier_nodes=len(current_parents),
            localisation_batch_depth=batch_ordinal,
            localisation_max_depth=depth,
            fn=lambda: fetch("damaged"),
        )

        counters[f"{prefix}partition_subtree_sql_calls"] = counters.get(
            f"{prefix}partition_subtree_sql_calls", 0
        ) + 2
        counters[f"{prefix}partition_subtree_nodes_read"] = counters.get(
            f"{prefix}partition_subtree_nodes_read", 0
        ) + len(healthy_rows) + len(damaged_rows)
        counters[f"{prefix}child_hash_sql_calls"] = counters.get(
            f"{prefix}child_hash_sql_calls", 0
        ) + 2
        counters[f"{prefix}child_hash_nodes_read"] = counters.get(
            f"{prefix}child_hash_nodes_read", 0
        ) + len(healthy_rows) + len(damaged_rows)

        healthy_nodes = {
            (int(row["partition_id"]), bytes(row["node_id"]), int(row["prefix_len"])): row
            for row in healthy_rows
        }
        damaged_nodes = {
            (int(row["partition_id"]), bytes(row["node_id"]), int(row["prefix_len"])): row
            for row in damaged_rows
        }

        compare_start = time.perf_counter_ns()

        distinct_prefix_lens = sorted({item[2] for item in batch_candidates})
        max_batch_prefix_len = distinct_prefix_lens[-1] if distinct_prefix_lens else 0

        active_mismatched_parents = set(current_parents)

        for lvl_prefix_len in distinct_prefix_lens:
            level_keys = [k for k in batch_candidates if k[2] == lvl_prefix_len]
            active_mismatches_for_next_level: set[tuple[int, bytes, int]] = set()

            for key in level_keys:
                parent_key = parent_map.get(key)
                if parent_key not in active_mismatched_parents:
                    continue

                healthy_row = healthy_nodes.get(key)
                damaged_row = damaged_nodes.get(key)

                if (healthy_row and healthy_row["hash"]) == (damaged_row and damaged_row["hash"]):
                    continue

                leaf_row = healthy_row if healthy_row is not None and healthy_row["is_leaf"] else damaged_row
                partition_id, node_id, p_len = key
                if leaf_row is not None and leaf_row["is_leaf"]:
                    bad.add((partition_id, node_id, p_len))
                else:
                    if p_len == max_batch_prefix_len:
                        frontier.add(key)
                    else:
                        active_mismatches_for_next_level.add(key)

            active_mismatched_parents = active_mismatches_for_next_level

        if profiler is not None and profiler.enabled:
            profiler.record(
                stage=stage_name,
                operation=f"{operation_prefix}partition_nodes_compare_cpu",
                client_wall_ns=time.perf_counter_ns() - compare_start,
                rows_returned=len(healthy_rows) + len(damaged_rows),
            )
    counters[f"{prefix}leaf_nodes_found"] = len(bad)

    ordered = sorted(bad, key=lambda item: (item[0], item[2], item[1]))
    if partition_aware:
        return ordered
    # The historical API has no partition coordinate.  Preserve its old
    # set-like semantics when multiple partition roots happen to project to
    # the same legacy (node_id, prefix_len) pair.
    legacy = {(node_id, prefix_len) for _, node_id, prefix_len in ordered}
    return sorted(legacy, key=lambda item: (item[1], item[0]))
