"""Recovery-path profiling helpers.

This module keeps profiling opt-in and low overhead when disabled.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from statistics import median
from typing import Any, Iterable


LIGHT_STAGES = {
    "localisation",
    "candidate_fetch",
    "comparison",
    "repair",
    "targeted_confirmation",
}


@dataclass
class ProfileOperation:
    run_id: str
    manifest_sha256: str
    experiment: str
    tuple_count: int
    split_threshold: int
    merge_threshold: int
    fanout: int
    profile_label: str
    bad_leaf_count: int
    corrupted_tuple_count: int
    repetition: int
    stage: str
    operation: str
    schema: str = ""
    partition: str = ""
    node_in_partition: str = ""
    leaf_id: str = ""
    localisation_prefix_len: str = ""
    localisation_frontier_nodes: str = ""
    localisation_batch_depth: str = ""
    localisation_max_depth: str = ""
    call_ordinal: int = 0
    rows_returned: int = 0
    client_wall_ms: float = 0.0
    success: int = 1

    def as_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "manifest_sha256": self.manifest_sha256,
            "experiment": self.experiment,
            "tuple_count": self.tuple_count,
            "split_threshold": self.split_threshold,
            "merge_threshold": self.merge_threshold,
            "fanout": self.fanout,
            "profile_label": self.profile_label,
            "bad_leaf_count": self.bad_leaf_count,
            "corrupted_tuple_count": self.corrupted_tuple_count,
            "repetition": self.repetition,
            "stage": self.stage,
            "operation": self.operation,
            "schema": self.schema,
            "partition": self.partition,
            "node_in_partition": self.node_in_partition,
            "leaf_id": self.leaf_id,
            "localisation_prefix_len": self.localisation_prefix_len,
            "localisation_frontier_nodes": self.localisation_frontier_nodes,
            "localisation_batch_depth": self.localisation_batch_depth,
            "localisation_max_depth": self.localisation_max_depth,
            "call_ordinal": self.call_ordinal,
            "rows_returned": self.rows_returned,
            "client_wall_ms": f"{self.client_wall_ms:.6f}",
            "success": self.success,
        }


@dataclass
class ProfileCollector:
    run_id: str
    manifest_sha256: str
    experiment: str
    tuple_count: int
    split_threshold: int
    merge_threshold: int
    fanout: int
    profile_label: str
    bad_leaf_count: int
    corrupted_tuple_count: int
    repetition: int
    enabled: bool = False
    deep: bool = False
    _call_ordinal: int = 0
    operations: list[ProfileOperation] = field(default_factory=list)
    backend_profile: dict[str, Any] | None = None
    deep_plan_rows: list[dict[str, Any]] = field(default_factory=list)
    deep_plan_summary_rows: list[dict[str, Any]] = field(default_factory=list)
    localisation_batches: list[dict[str, Any]] = field(default_factory=list)
    invalid_reasons: list[str] = field(default_factory=list)

    def next_ordinal(self) -> int:
        self._call_ordinal += 1
        return self._call_ordinal

    def record(
        self,
        *,
        stage: str,
        operation: str,
        client_wall_ns: int,
        rows_returned: int = 0,
        success: bool = True,
        schema: str = "",
        partition: int | str = "",
        node_in_partition: int | str = "",
        leaf_id: int | str = "",
        localisation_prefix_len: int | str = "",
        localisation_frontier_nodes: int | str = "",
        localisation_batch_depth: int | str = "",
        localisation_max_depth: int | str = "",
    ) -> None:
        if not self.enabled:
            return
        self.operations.append(
            ProfileOperation(
                run_id=self.run_id,
                manifest_sha256=self.manifest_sha256,
                experiment=self.experiment,
                tuple_count=self.tuple_count,
                split_threshold=self.split_threshold,
                merge_threshold=self.merge_threshold,
                fanout=self.fanout,
                profile_label=self.profile_label,
                bad_leaf_count=self.bad_leaf_count,
                corrupted_tuple_count=self.corrupted_tuple_count,
                repetition=self.repetition,
                stage=stage,
                operation=operation,
                schema=str(schema),
                partition="" if partition == "" else str(partition),
                node_in_partition="" if node_in_partition == "" else str(node_in_partition),
                leaf_id="" if leaf_id == "" else str(leaf_id),
                localisation_prefix_len=(
                    "" if localisation_prefix_len == "" else str(localisation_prefix_len)
                ),
                localisation_frontier_nodes=(
                    "" if localisation_frontier_nodes == "" else str(localisation_frontier_nodes)
                ),
                localisation_batch_depth=(
                    "" if localisation_batch_depth == "" else str(localisation_batch_depth)
                ),
                localisation_max_depth=(
                    "" if localisation_max_depth == "" else str(localisation_max_depth)
                ),
                call_ordinal=self.next_ordinal(),
                rows_returned=rows_returned,
                client_wall_ms=client_wall_ns / 1_000_000.0,
                success=int(bool(success)),
            )
        )

    def extend_backend_profile(self, stats: dict[str, Any] | None) -> None:
        self.backend_profile = stats

    def add_invalid_reason(self, reason: str) -> None:
        self.invalid_reasons.append(reason)

    def record_localisation_batch(
        self,
        *,
        stage: str,
        batch_ordinal: int,
        prefix_len: int,
        node_ids: list[bytes],
        max_depth: int,
        partition_ids: list[int] | None = None,
        prefix_lens: list[int] | None = None,
    ) -> None:
        """Keep exact batch inputs for an untimed post-run EXPLAIN replay."""
        if not self.enabled or stage != "localisation":
            return
        self.localisation_batches.append(
            {
                "stage": stage,
                "batch_ordinal": batch_ordinal,
                "prefix_len": prefix_len,
                "node_ids": [bytes(node_id) for node_id in node_ids],
                "max_depth": max_depth,
                "partition_ids": None if partition_ids is None else [int(value) for value in partition_ids],
                "prefix_lens": None if prefix_lens is None else [int(value) for value in prefix_lens],
            }
        )

    def rows(self) -> list[dict[str, Any]]:
        return [op.as_row() for op in self.operations]


def now_ns() -> int:
    import time

    return time.perf_counter_ns()


@contextmanager
def timed_record(collector: ProfileCollector | None, **kwargs):
    if collector is None or not collector.enabled:
        yield None
        return
    start = now_ns()
    success = True
    try:
        yield None
    except Exception:
        success = False
        raise
    finally:
        end = now_ns()
        collector.record(client_wall_ns=end - start, success=success, **kwargs)


def record_call(
    collector: ProfileCollector | None,
    *,
    stage: str,
    operation: str,
    rows_returned: int | None = None,
    success: bool = True,
    schema: str = "",
    partition: int | str = "",
    node_in_partition: int | str = "",
    leaf_id: int | str = "",
    localisation_prefix_len: int | str = "",
    localisation_frontier_nodes: int | str = "",
    localisation_batch_depth: int | str = "",
    localisation_max_depth: int | str = "",
    fn,
):
    if collector is None or not collector.enabled:
        return fn()
    start = now_ns()
    ok = success
    result = None
    try:
        result = fn()
        return result
    except Exception:
        ok = False
        raise
    finally:
        inferred_rows = rows_returned
        if inferred_rows is None:
            try:
                inferred_rows = len(result)  # type: ignore[name-defined]
            except Exception:
                inferred_rows = 0
        collector.record(
            stage=stage,
            operation=operation,
            rows_returned=inferred_rows,
            success=ok,
            schema=schema,
            partition=partition,
            node_in_partition=node_in_partition,
            leaf_id=leaf_id,
            localisation_prefix_len=localisation_prefix_len,
            localisation_frontier_nodes=localisation_frontier_nodes,
            localisation_batch_depth=localisation_batch_depth,
            localisation_max_depth=localisation_max_depth,
            client_wall_ns=now_ns() - start,
        )


def group_profile_rows(
    rows: Iterable[dict[str, Any]],
    *,
    group_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(row.get(name, "") for name in group_keys) + (
            str(row.get("stage", "")),
            str(row.get("operation", "")),
        )
        grouped[key].append(row)

    out: list[dict[str, Any]] = []
    for key, items in sorted(grouped.items()):
        group_values = key[:len(group_keys)]
        stage = str(key[-2])
        operation = str(key[-1])
        values = [float(item.get("client_wall_ms") or 0.0) for item in items]
        ordered = sorted(values)
        rows_returned = [int(item.get("rows_returned") or 0) for item in items]
        restore_ms = sum(values)
        total_rows = sum(rows_returned)
        p95 = ordered[min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)] if ordered else 0.0
        row = {name: value for name, value in zip(group_keys, group_values)}
        row.update(
            {
                "stage": stage,
                "operation": operation,
                "call_count": len(items),
                "row_count": total_rows,
                "total_ms": f"{restore_ms:.6f}",
                "median_ms": f"{median(values):.6f}" if values else "0.000000",
                "p95_ms": f"{p95:.6f}",
                "fraction_restore_repair_ms": "0.000000",
            }
        )
        out.append(row)
    return out


def group_profile_rows_with_fraction(
    rows: Iterable[dict[str, Any]],
    restore_repair_ms: float,
    *,
    group_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    grouped = group_profile_rows(rows, group_keys=group_keys)
    if restore_repair_ms <= 0:
        return grouped
    for row in grouped:
        row["fraction_restore_repair_ms"] = f"{float(row['total_ms']) / restore_repair_ms:.6f}"
    return grouped


def group_profile_rows_with_denominators(
    rows: Iterable[dict[str, Any]],
    denominators: dict[tuple[Any, ...], float],
    *,
    group_keys: tuple[str, ...],
    fraction_field: str = "fraction_restore_repair_ms",
) -> list[dict[str, Any]]:
    grouped = group_profile_rows(rows, group_keys=group_keys)
    for row in grouped:
        key = tuple(row.get(name, "") for name in group_keys)
        denominator = float(denominators.get(key, 0.0) or 0.0)
        value = float(row.get("total_ms") or 0.0)
        row[fraction_field] = f"{(value / denominator if denominator > 0 else 0.0):.6f}"
        if fraction_field != "fraction_restore_repair_ms":
            row.pop("fraction_restore_repair_ms", None)
    return grouped


def validate_profile_invariants(
    *,
    phase: dict[str, float],
    operations: list[dict[str, Any]],
    bad_leaf_count: int,
    run_id: str,
    tolerance_ms: float = 5.0,
    leaf_fetch_chunk_size: int = 0,
) -> list[str]:
    """Validate internal consistency of collected profiling rows.

    Parameters
    ----------
    phase:
        Timing dict from the run (keys like ``candidate_row_fetch_ms``).
    operations:
        Flat list of profiling operation dicts (all rows for this run).
    bad_leaf_count:
        Number of leaves that were corrupted (K).
    run_id:
        Expected run_id; rows from other runs are rejected.
    tolerance_ms:
        Allowed absolute timing slack between phase total and profile sum.
    leaf_fetch_chunk_size:
        The chunk size used for batched leaf fetching.  ``0`` or negative means
        unbounded (one SQL call per phase per schema).  The expected batch call
        counts are derived from this value:

        Expected calls per phase = 2 * ceil(K / chunk_size)  when chunk_size > 0
        Expected calls per phase = 2                          when chunk_size <= 0 and K > 0
        Expected calls per phase = 0                          when K == 0

        Boundary behaviour (chunk_size=64):
            K=0   -> 0 calls
            K=10  -> 2 calls   (ceil(10/64)=1, *2 schemas)
            K=64  -> 2 calls   (ceil(64/64)=1, *2 schemas)
            K=65  -> 4 calls   (ceil(65/64)=2, *2 schemas)
            K=75  -> 4 calls   (ceil(75/64)=2, *2 schemas)
            K=200 -> 8 calls   (ceil(200/64)=4, *2 schemas)
        Boundary behaviour (chunk_size=0):
            K=75  -> 2 calls   (unbounded single SQL, *2 schemas)
    """
    import math as _math

    reasons: list[str] = []
    op_rows = [row for row in operations if row.get("run_id") == run_id]
    if len(op_rows) != len(operations):
        reasons.append("profile rows must belong to exactly one run_id")

    def sum_ms(stage: str, prefix: str | None = None) -> float:
        total = 0.0
        for row in op_rows:
            if row.get("stage") != stage:
                continue
            if prefix is not None and not str(row.get("operation", "")).startswith(prefix):
                continue
            total += float(row.get("client_wall_ms") or 0.0)
        return total

    localisation_ms = phase.get("tree_localisation_ms", 0.0)
    candidate_fetch_ms = phase.get("candidate_row_fetch_ms", 0.0)
    repair_write_ms = phase.get("repair_write_ms", 0.0)
    confirmation_ms = phase.get("targeted_post_repair_confirmation_ms", 0.0)

    def timing_mismatch(observed_ms: float, expected_ms: float) -> bool:
        allowed = max(tolerance_ms, 0.05 * max(expected_ms, 1.0))
        return abs(observed_ms - expected_ms) > allowed

    localisation_sum = sum_ms("localisation")
    if timing_mismatch(localisation_sum, localisation_ms):
        reasons.append(
            f"localisation profile sum {localisation_sum:.3f}ms != tree_localisation_ms {localisation_ms:.3f}ms"
        )
    candidate_fetch_sum = sum_ms("candidate_fetch")
    if timing_mismatch(candidate_fetch_sum, candidate_fetch_ms):
        reasons.append(
            f"candidate fetch profile sum {candidate_fetch_sum:.3f}ms != candidate_row_fetch_ms {candidate_fetch_ms:.3f}ms"
        )
    repair_sum = sum_ms("repair")
    if timing_mismatch(repair_sum, repair_write_ms):
        reasons.append(
            f"repair profile sum {repair_sum:.3f}ms != repair_write_ms {repair_write_ms:.3f}ms"
        )
    targeted_sum_ms = sum_ms("targeted_confirmation")
    if timing_mismatch(targeted_sum_ms, confirmation_ms):
        reasons.append(
            f"targeted confirmation profile sum {targeted_sum_ms:.3f}ms "
            f"!= targeted_post_repair_confirmation_ms {confirmation_ms:.3f}ms"
        )

    candidate_leaf_calls = sum(
        1
        for row in op_rows
        if row.get("stage") == "candidate_fetch" and row.get("operation") in {"leaf_fetch_healthy", "leaf_fetch_damaged"}
    )
    confirmation_leaf_calls = sum(
        1
        for row in op_rows
        if row.get("stage") == "targeted_confirmation"
        and row.get("operation") in {"confirmation_leaf_fetch_healthy", "confirmation_leaf_fetch_damaged"}
    )
    candidate_batch_calls = sum(
        1
        for row in op_rows
        if row.get("stage") == "candidate_fetch"
        and row.get("operation") in {"leaf_fetch_batch_healthy", "leaf_fetch_batch_damaged"}
    )
    confirmation_batch_calls = sum(
        1
        for row in op_rows
        if row.get("stage") == "targeted_confirmation"
        and row.get("operation")
        in {
            "confirmation_leaf_fetch_batch_healthy",
            "confirmation_leaf_fetch_batch_damaged",
        }
    )

    # Compute the exact expected number of batch SQL calls per phase.
    # Each phase issues one SQL call per chunk per schema (healthy + damaged = 2).
    if bad_leaf_count == 0:
        expected_batch_calls = 0
    elif leaf_fetch_chunk_size <= 0:
        # Unbounded mode: one SQL call per schema → 2 calls per phase
        expected_batch_calls = 2
    else:
        chunks_per_schema = _math.ceil(bad_leaf_count / leaf_fetch_chunk_size)
        expected_batch_calls = 2 * chunks_per_schema

    expected_leaf_calls = 2 * bad_leaf_count
    if bad_leaf_count == 0:
        candidate_ok = (candidate_batch_calls == 0 and candidate_leaf_calls == 0)
        candidate_detail = f"batch={candidate_batch_calls}, per_leaf={candidate_leaf_calls}"
        confirmation_ok = (confirmation_batch_calls == 0 and confirmation_leaf_calls == 0)
        confirmation_detail = f"batch={confirmation_batch_calls}, per_leaf={confirmation_leaf_calls}"
    else:
        if candidate_batch_calls:
            # Batch mode: require the exact expected count, not just any even number.
            candidate_ok = (candidate_batch_calls == expected_batch_calls) and candidate_leaf_calls == 0
            candidate_detail = (
                f"batch={candidate_batch_calls} (expected={expected_batch_calls}), "
                f"per_leaf={candidate_leaf_calls}"
            )
        else:
            candidate_ok = candidate_leaf_calls == expected_leaf_calls
            candidate_detail = str(candidate_leaf_calls)
        if confirmation_batch_calls:
            confirmation_ok = (
                confirmation_batch_calls == expected_batch_calls
                and confirmation_leaf_calls == 0
            )
            confirmation_detail = (
                f"batch={confirmation_batch_calls} (expected={expected_batch_calls}), "
                f"per_leaf={confirmation_leaf_calls}"
            )
        else:
            confirmation_ok = confirmation_leaf_calls == expected_leaf_calls
            confirmation_detail = str(confirmation_leaf_calls)

    if not candidate_ok:
        reasons.append(
            f"candidate leaf fetch calls {candidate_detail} do not match "
            f"expected batch calls or per-leaf={expected_leaf_calls}"
        )
    if not confirmation_ok:
        reasons.append(
            f"confirmation leaf fetch calls {confirmation_detail} do not match "
            f"expected batch calls or per-leaf={expected_leaf_calls}"
        )
    if confirmation_ms < 0:
        reasons.append("confirmation timer is invalid")
    return reasons


def parse_json_plan(explain_rows: list[dict[str, Any]]) -> Any:
    if not explain_rows:
        return None
    doc = explain_rows[0].get("QUERY PLAN")
    if isinstance(doc, str):
        doc = json.loads(doc)
    # psycopg may decode FORMAT JSON directly to the top-level EXPLAIN array,
    # while text results arrive as the JSON string handled above.
    if isinstance(doc, list):
        return doc[0] if doc else None
    return doc
