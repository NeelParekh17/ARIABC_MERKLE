#!/usr/bin/env python3
"""Run native dynamic-Merkle recovery with healthy and damaged on different hosts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from merkle_recovery.dataset import (
    cleanup_dynamic_benchmark_generations,
    ensure_helpers,
    recreate_schema,
    truncate_dynamic_side_tables,
)
from merkle_recovery.db import execute, scalar
from merkle_recovery.dynamic import (
    LocalisationTrace,
    LogicalRange,
    compare_range_items,
    localise_bad_ranges,
)
from merkle_recovery.dynamic_db import (
    apply_set_based_repairs,
    dynamic_tree_stats,
    dynamic_verify,
    exact_heap_fetch_plan,
    fetch_exact_healthy_rows,
    partition_roots,
    range_items,
    range_summaries,
)
from merkle_recovery.manifest import (
    apply_corruption,
    choose_dynamic_corruption_manifest,
    validate_dynamic_manifest_mapping,
)


DEFAULT_SIZES = (1_000_000, 3_000_000, 5_000_000, 10_000_000)
FULL_SIZES = (
    1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000,
    15_000_000, 20_000_000, 25_000_000, 30_000_000,
    40_000_000, 50_000_000,
)
def now_ms() -> float:
    return time.perf_counter() * 1000.0


@contextmanager
def timer(phases: dict[str, float], name: str):
    started = now_ms()
    yield
    phases[name] = phases.get(name, 0.0) + now_ms() - started


def connect(dsn: str):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def parse_sizes(value: str | None, full_scale: bool) -> tuple[int, ...]:
    if full_scale and value is not None:
        raise ValueError("--full-scale and --tuple-count are mutually exclusive")
    if full_scale:
        return FULL_SIZES
    if value is None:
        return DEFAULT_SIZES
    sizes = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not sizes or any(size <= 0 for size in sizes) or len(set(sizes)) != len(sizes):
        raise ValueError("--tuple-count must contain unique positive integers")
    return sizes


def probe_connection(conn, samples: int) -> dict[str, Any]:
    endpoint = execute(
        conn,
        "SELECT inet_server_addr()::text AS server_addr, "
        "inet_server_port() AS server_port, "
        "inet_client_addr()::text AS client_addr, "
        "inet_client_port() AS client_port",
    )[0]
    values = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        scalar(conn, "SELECT 1")
        values.append((time.perf_counter_ns() - started) / 1_000_000.0)
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))]
    return {
        **endpoint,
        "sample_count": samples,
        "round_trip_median_ms": statistics.median(values),
        "round_trip_p95_ms": p95,
        "round_trip_min_ms": min(values),
        "round_trip_max_ms": max(values),
    }


def prepare_role(
    conn,
    role: str,
    tuple_count: int,
    partitions: int,
    fanout: int,
    leaf_capacity: int,
    merge_threshold: int,
) -> dict[str, Any]:
    if role not in ("healthy", "damaged"):
        raise ValueError(f"unsupported split-host role: {role}")
    cleanup_dynamic_benchmark_generations(conn)
    truncate_dynamic_side_tables(conn)
    recreate_schema(conn)
    unused = "damaged" if role == "healthy" else "healthy"
    execute(conn, f"DROP TABLE {unused}.usertable CASCADE")
    execute(
        conn,
        f"""
        INSERT INTO {role}.usertable
        SELECT gs::bigint,
               'field0-' || gs, 'field1-' || gs, 'field2-' || gs,
               'field3-' || gs, 'field4-' || gs, 'field5-' || gs,
               'field6-' || gs, 'field7-' || gs, 'field8-' || gs,
               'field9-' || gs
        FROM generate_series(1, %s) AS gs
        """,
        (tuple_count,),
    )
    execute(
        conn,
        f"""
        CREATE INDEX usertable_merkle_idx ON {role}.usertable
        USING merkle (ycsb_key) WITH (
            partitions={partitions}, fanout={fanout}, dynamic=on,
            leaf_capacity={leaf_capacity}, merge_threshold={merge_threshold}
        )
        """,
    )
    execute(conn, f"ANALYZE {role}.usertable")
    stats = dynamic_tree_stats(conn, role)
    if int(stats.get("logical_fanout", -1)) != fanout:
        raise RuntimeError(f"{role} logical fanout does not match requested fanout")
    if int(stats.get("layout_version", -1)) != 8:
        raise RuntimeError(f"{role} index is not native dynamic layout v8")
    return stats


def localise_split(
    healthy_conn,
    damaged_conn,
    leaf_capacity: int,
    fanout: int,
) -> tuple[list[LogicalRange], LocalisationTrace]:
    trace = LocalisationTrace()
    healthy_roots = partition_roots(healthy_conn, "healthy")
    damaged_roots = partition_roots(damaged_conn, "damaged")

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        if schema == "healthy":
            return range_summaries(healthy_conn, "healthy", ranges)
        if schema == "damaged":
            return range_summaries(damaged_conn, "damaged", ranges)
        raise ValueError(f"unexpected split-host schema: {schema}")

    bad = localise_bad_ranges(
        healthy_roots,
        damaged_roots,
        fetch,
        leaf_capacity=leaf_capacity,
        logical_fanout=fanout,
        trace=trace,
    )
    return bad, trace


def roots_match(healthy_conn, damaged_conn) -> tuple[bool, int, int]:
    healthy = partition_roots(healthy_conn, "healthy")
    damaged = partition_roots(damaged_conn, "damaged")
    keys = set(healthy) | set(damaged)
    equal = all(
        key in healthy and key in damaged and
        healthy[key].signature == damaged[key].signature
        for key in keys
    )
    return (
        equal,
        sum(item.tuple_count for item in healthy.values()),
        sum(item.tuple_count for item in damaged.values()),
    )


def physical_size(conn, schema: str) -> dict[str, int]:
    return {
        "table_bytes": int(scalar(conn, f"SELECT pg_relation_size('{schema}.usertable'::regclass)")),
        "primary_index_bytes": int(scalar(conn, f"SELECT pg_relation_size('{schema}.usertable_pkey'::regclass)")),
        "merkle_index_bytes": int(scalar(conn, f"SELECT pg_relation_size('{schema}.usertable_merkle_idx'::regclass)")),
        "total_schema_bytes": int(scalar(conn, f"SELECT pg_total_relation_size('{schema}.usertable'::regclass)")),
    }


def run_recovery(
    healthy_conn,
    damaged_conn,
    manifest: dict[str, Any],
    repetition: int,
    warmup: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    leaf_capacity = int(manifest["leaf_capacity"])
    fanout = int(manifest["fanout"])
    if warmup:
        warm_ranges, _ = localise_split(
            healthy_conn, damaged_conn, leaf_capacity, fanout
        )
        healthy_items = range_items(healthy_conn, "healthy", warm_ranges)
        damaged_items = range_items(damaged_conn, "damaged", warm_ranges)
        warm_repairs = compare_range_items(healthy_items, damaged_items)
        fetch_exact_healthy_rows(healthy_conn, warm_repairs.healthy_heap_keys)

    phases: dict[str, float] = {}
    recovery_started = now_ms()
    with timer(phases, "tree_localisation_ms"):
        bad_ranges, trace = localise_split(
            healthy_conn, damaged_conn, leaf_capacity, fanout
        )
    with timer(phases, "candidate_summary_fetch_ms"):
        healthy_items = range_items(healthy_conn, "healthy", bad_ranges)
        damaged_items = range_items(damaged_conn, "damaged", bad_ranges)
    with timer(phases, "summary_comparison_ms"):
        repairs = compare_range_items(healthy_items, damaged_items)
    with timer(phases, "exact_heap_fetch_ms"):
        plan = exact_heap_fetch_plan(healthy_conn, repairs.healthy_heap_keys)
        healthy_rows = fetch_exact_healthy_rows(
            healthy_conn, repairs.healthy_heap_keys
        )
    with timer(phases, "repair_write_ms"):
        repaired = apply_set_based_repairs(damaged_conn, repairs, healthy_rows)
    with timer(phases, "native_commit_visibility_ms"):
        remaining, confirmation_trace = localise_split(
            healthy_conn, damaged_conn, leaf_capacity, fanout
        )
    restore_repair_ms = now_ms() - recovery_started
    with timer(phases, "post_commit_relocalisation_ms"):
        post_remaining, post_trace = localise_split(
            healthy_conn, damaged_conn, leaf_capacity, fanout
        )

    equal, healthy_count, damaged_count = roots_match(healthy_conn, damaged_conn)
    healthy_ok = dynamic_verify(healthy_conn, "healthy")
    damaged_ok = dynamic_verify(damaged_conn, "damaged")
    valid = (
        not remaining and not post_remaining and equal and
        healthy_count == damaged_count == int(manifest["tuple_count"]) and
        healthy_ok and damaged_ok and repaired.total == len(manifest["corruptions"])
    )
    run_id = (
        f"split-host-n{manifest['tuple_count']}-p{manifest['partitions']}"
        f"-lf{fanout}-bad{len(manifest['bad_ranges'])}"
        f"-c{len(manifest['corruptions'])}-r{repetition}"
    )
    result = {
        "run_id": run_id,
        "tuple_count": int(manifest["tuple_count"]),
        "partitions": int(manifest["partitions"]),
        "logical_fanout": fanout,
        "physical_node_fanout": 2,
        "leaf_capacity": leaf_capacity,
        "merge_threshold": int(manifest["merge_threshold"]),
        "bad_range_count": len(manifest["bad_ranges"]),
        "localised_bad_range_count": len(bad_ranges),
        "corrupted_tuple_count": len(manifest["corruptions"]),
        "total_rows_repaired": repaired.total,
        "rows_inserted": repaired.rows_inserted,
        "rows_updated": repaired.rows_updated,
        "rows_deleted": repaired.rows_deleted,
        "healthy_candidate_items": len(healthy_items),
        "damaged_candidate_items": len(damaged_items),
        "candidate_summary_items": len(healthy_items) + len(damaged_items),
        "localisation_levels_visited": trace.levels_visited,
        "logical_ranges_compared": trace.logical_ranges_compared,
        "range_summary_rows_read": trace.range_summary_rows,
        "confirmation_levels_visited": confirmation_trace.levels_visited,
        "post_confirmation_levels_visited": post_trace.levels_visited,
        "restore_repair_ms": restore_repair_ms,
        "roots_match": int(equal),
        "healthy_root_tuple_count": healthy_count,
        "damaged_root_tuple_count": damaged_count,
        "healthy_dynamic_verify": int(healthy_ok),
        "damaged_dynamic_verify": int(damaged_ok),
        "remaining_bad_range_count": len(post_remaining),
        "healthy_plan_index_used": int(plan["index_used"]),
        "repetition": repetition,
        "valid": int(valid),
        "warning": "" if valid else "split-host recovery correctness gate failed",
    }
    phase_rows = [
        {"run_id": run_id, "phase": phase, "ms": round(value, 3)}
        for phase, value in phases.items()
    ]
    return result, phase_rows


def write_csv(path: Path, values: list[dict[str, Any]]) -> None:
    if not values:
        return
    fields: list[str] = []
    for value in values:
        for key in value:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(values)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--healthy-dsn", required=True)
    parser.add_argument("--damaged-dsn", required=True)
    parser.add_argument("--healthy-placement", default="user4")
    parser.add_argument("--damaged-placement", default="admin123")
    parser.add_argument("--client-placement", default="admin123")
    parser.add_argument("--tuple-count")
    parser.add_argument("--full-scale", action="store_true")
    parser.add_argument("--fanout", type=int, default=32)
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--leaf-capacity", type=int, default=32)
    parser.add_argument("--merge-threshold", type=int, default=8)
    parser.add_argument("--bad-range-count", type=int, default=75)
    parser.add_argument("--corrupted-tuple-count", type=int, default=300)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20260703)
    parser.add_argument("--network-probe-samples", type=int, default=20)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--no-warmup", action="store_true")
    args = parser.parse_args()

    if os.environ.get("ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET") != "1":
        raise RuntimeError("set ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 for dedicated databases")
    sizes = parse_sizes(args.tuple_count, args.full_scale)
    if args.fanout not in (2, 4, 8, 16, 32):
        raise ValueError("--fanout must be one of 2,4,8,16,32")
    if args.partitions <= 0 or args.leaf_capacity <= 0:
        raise ValueError("partitions and leaf capacity must be positive")
    if not 0 <= args.merge_threshold < args.leaf_capacity:
        raise ValueError("merge threshold must be in [0, leaf capacity)")
    if args.repetitions <= 0 or args.network_probe_samples <= 0:
        raise ValueError("repetitions and probe samples must be positive")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.result_dir / f"split_host_dynamic_recovery_{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    runs: list[dict[str, Any]] = []
    phases: list[dict[str, Any]] = []
    datasets: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []

    with connect(args.healthy_dsn) as healthy_conn, connect(args.damaged_dsn) as damaged_conn:
        ensure_helpers(healthy_conn, merkle_mode="dynamic")
        ensure_helpers(damaged_conn, merkle_mode="dynamic")
        healthy_probe = probe_connection(healthy_conn, args.network_probe_samples)
        damaged_probe = probe_connection(damaged_conn, args.network_probe_samples)
        if not healthy_probe.get("server_addr") or not healthy_probe.get("client_addr"):
            raise RuntimeError("healthy connection is not TCP; network overhead would not be measured")
        if healthy_probe["server_addr"] == healthy_probe["client_addr"]:
            raise RuntimeError("healthy server and recovery client resolve to the same host")
        (output / "network_probe.json").write_text(json.dumps({
            "client_placement": args.client_placement,
            "healthy_placement": args.healthy_placement,
            "damaged_placement": args.damaged_placement,
            "healthy": healthy_probe,
            "damaged": damaged_probe,
        }, indent=2, default=str) + "\n")

        for size in sizes:
            print(
                f"[split-host] preparing healthy {args.healthy_placement} "
                f"dataset rows={size}",
                flush=True,
            )
            healthy_stats = prepare_role(
                healthy_conn, "healthy", size, args.partitions, args.fanout,
                args.leaf_capacity, args.merge_threshold,
            )
            print(
                f"[split-host] preparing damaged {args.damaged_placement} "
                f"dataset rows={size}",
                flush=True,
            )
            damaged_stats = prepare_role(
                damaged_conn, "damaged", size, args.partitions, args.fanout,
                args.leaf_capacity, args.merge_threshold,
            )
            manifest = choose_dynamic_corruption_manifest(
                healthy_conn,
                "split-host-network-recovery",
                size,
                args.partitions,
                args.fanout,
                args.leaf_capacity,
                args.merge_threshold,
                args.bad_range_count,
                args.corrupted_tuple_count,
                args.seed,
                corruption_mode="paper-update-only",
            )
            validate_dynamic_manifest_mapping(healthy_conn, manifest)
            manifests.append(manifest)
            datasets.append({
                "tuple_count": size,
                "healthy_stats": json.dumps(healthy_stats, sort_keys=True),
                "damaged_stats": json.dumps(damaged_stats, sort_keys=True),
                **{f"healthy_{key}": value for key, value in physical_size(healthy_conn, "healthy").items()},
                **{f"damaged_{key}": value for key, value in physical_size(damaged_conn, "damaged").items()},
            })
            for repetition in range(args.repetitions):
                if repetition:
                    damaged_stats = prepare_role(
                        damaged_conn, "damaged", size, args.partitions,
                        args.fanout, args.leaf_capacity, args.merge_threshold,
                    )
                apply_corruption(damaged_conn, manifest)
                result, phase_rows = run_recovery(
                    healthy_conn,
                    damaged_conn,
                    manifest,
                    repetition,
                    warmup=not args.no_warmup,
                )
                runs.append(result)
                phases.extend(phase_rows)
                print(
                    f"[split-host] rows={size} repetition={repetition} "
                    f"recovery_ms={result['restore_repair_ms']:.3f} "
                    f"valid={result['valid']}",
                    flush=True,
                )

    config = {
        "client_placement": args.client_placement,
        "healthy_placement": args.healthy_placement,
        "damaged_placement": args.damaged_placement,
        "tuple_counts": sizes,
        "full_scale": args.full_scale,
        "fanout": args.fanout,
        "partitions": args.partitions,
        "physical_node_fanout": 2,
        "leaf_capacity": args.leaf_capacity,
        "merge_threshold": args.merge_threshold,
        "bad_range_count": args.bad_range_count,
        "corrupted_tuple_count": args.corrupted_tuple_count,
        "repetitions": args.repetitions,
        "seed": args.seed,
        "warmup": not args.no_warmup,
    }
    (output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    (output / "corruption_manifest.json").write_text(
        json.dumps(manifests, indent=2, default=str) + "\n"
    )
    write_csv(output / "runs.csv", runs)
    write_csv(output / "phase_timings.csv", phases)
    write_csv(output / "dataset_sizes.csv", datasets)
    all_valid = bool(runs) and all(int(row["valid"]) == 1 for row in runs)
    (output / "verification.json").write_text(json.dumps({
        "all_runs_valid": all_valid,
        "run_count": len(runs),
        "failed_run_ids": [row["run_id"] for row in runs if not int(row["valid"])],
    }, indent=2) + "\n")
    print(f"SPLIT_HOST_RECOVERY_ARTIFACT={output}")
    return 0 if all_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
