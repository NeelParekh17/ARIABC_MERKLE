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
    partitions: int
    leaves_per_partition: int
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
            "partitions": self.partitions,
            "leaves_per_partition": self.leaves_per_partition,
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
    partitions: int
    leaves_per_partition: int
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
    ) -> None:
        if not self.enabled:
            return
        self.operations.append(
            ProfileOperation(
                run_id=self.run_id,
                manifest_sha256=self.manifest_sha256,
                experiment=self.experiment,
                tuple_count=self.tuple_count,
                partitions=self.partitions,
                leaves_per_partition=self.leaves_per_partition,
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
) -> list[str]:
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

    if abs(sum_ms("localisation") - localisation_ms) > tolerance_ms:
        reasons.append(
            f"localisation profile sum {sum_ms('localisation'):.3f}ms != tree_localisation_ms {localisation_ms:.3f}ms"
        )
    if abs(sum_ms("candidate_fetch") - candidate_fetch_ms) > tolerance_ms:
        reasons.append(
            f"candidate fetch profile sum {sum_ms('candidate_fetch'):.3f}ms != candidate_row_fetch_ms {candidate_fetch_ms:.3f}ms"
        )
    if abs(sum_ms("repair") - repair_write_ms) > tolerance_ms:
        reasons.append(
            f"repair profile sum {sum_ms('repair'):.3f}ms != repair_write_ms {repair_write_ms:.3f}ms"
        )
    if abs(sum_ms("targeted_confirmation") - confirmation_ms) > tolerance_ms:
        reasons.append(
            f"targeted confirmation profile sum {sum_ms('targeted_confirmation'):.3f}ms "
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
    expected_leaf_calls = 2 * bad_leaf_count
    if candidate_leaf_calls != expected_leaf_calls:
        reasons.append(
            f"candidate leaf fetch calls {candidate_leaf_calls} != 2 * bad_leaf_count {expected_leaf_calls}"
        )
    if confirmation_leaf_calls != expected_leaf_calls:
        reasons.append(
            f"confirmation leaf fetch calls {confirmation_leaf_calls} != 2 * bad_leaf_count {expected_leaf_calls}"
        )
    if confirmation_ms < 0:
        reasons.append("confirmation timer is invalid")
    return reasons


def parse_json_plan(explain_rows: list[dict[str, Any]]) -> Any:
    if not explain_rows:
        return None
    doc = explain_rows[0].get("QUERY PLAN")
    if isinstance(doc, str):
        return json.loads(doc)[0]
    return doc
