#!/usr/bin/env python3
"""Compare overhead profiles using matched workload/thread/rate rows.

This intentionally avoids the old "pick the best row from each profile"
approach, because those rows can come from different workloads or thread counts
and are not comparable. The output contains only keys that are present in all
four profiles, and reports gateway, wall-clock, and strict all-nodes-verified
throughput side by side.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Optional

PROFILE_ARGS = [
    ("vanilla-pg", "vanilla_pg"),
    ("base-no-raft-no-kafka", "base"),
    ("kafka-only-no-raft", "kafka_only"),
    ("raft-kafka", "raft_kafka"),
]


def _summary_path(p: Path) -> Path:
    if p.is_dir():
        return p / "summary.csv"
    return p


def _as_float(v: str) -> Optional[float]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _as_int(v: str) -> Optional[int]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _fmt(v: Optional[float], nd: int = 6) -> str:
    if v is None:
        return ""
    return f"{v:.{nd}f}"


def _pct_drop(base: Optional[float], other: Optional[float]) -> Optional[float]:
    if base is None or other is None or base <= 0:
        return None
    return ((base - other) / base) * 100.0


def _row_key(row: dict[str, str]) -> Optional[tuple[str, int, int]]:
    workload = (row.get("workload") or "").strip()
    threads = _as_int(row.get("threads", ""))
    rate = _as_int(row.get("rate", ""))
    if not workload or threads is None or rate is None:
        return None
    return workload, threads, rate


def _metric_gateway_valid(row: dict[str, str]) -> Optional[float]:
    return _as_float(row.get("mean_throughput_valid_ops_per_s", ""))


def _metric_wall_valid(row: dict[str, str]) -> Optional[float]:
    wall = _as_float(row.get("mean_valid_throughput_ops_per_s_wall", ""))
    if wall is not None:
        return wall
    return _metric_gateway_valid(row)


def _metric_strict_wall_valid(row: dict[str, str]) -> Optional[float]:
    strict = _as_float(row.get("mean_valid_throughput_strict_ops_per_s_wall", ""))
    if strict is not None:
        return strict
    completion = (row.get("completion_semantics") or "").strip()
    strict_completion = (row.get("strict_completion_semantics") or "").strip()
    if strict_completion and strict_completion == completion:
        return _metric_wall_valid(row)
    if strict_completion.startswith("single_node_") or completion.startswith("single_node_"):
        return _metric_wall_valid(row)
    return None


def _best_metric_row(rows: list[dict[str, str]], metric_fn) -> Optional[dict[str, str]]:
    best_row: Optional[dict[str, str]] = None
    best_val: Optional[float] = None
    for row in rows:
        val = metric_fn(row)
        if val is None:
            continue
        if best_val is None or val > best_val:
            best_val = val
            best_row = row
    return best_row


def _read_summary_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def _load_profile_rows(summary_csv: Path) -> dict[tuple[str, int, int], dict[str, str]]:
    rows = _read_summary_rows(summary_csv)
    keyed: dict[tuple[str, int, int], list[dict[str, str]]] = {}
    for row in rows:
        key = _row_key(row)
        if key is None:
            continue
        keyed.setdefault(key, []).append(row)

    chosen: dict[tuple[str, int, int], dict[str, str]] = {}
    for key, candidates in keyed.items():
        best = _best_metric_row(
            candidates,
            lambda r: _metric_strict_wall_valid(r)
            or _metric_wall_valid(r)
            or _metric_gateway_valid(r)
            or 0.0,
        )
        if best is not None:
            chosen[key] = best
    return chosen


def _row_for_key(
    key: tuple[str, int, int],
    by_profile: dict[str, dict[tuple[str, int, int], dict[str, str]]],
) -> dict[str, str]:
    workload, threads, rate = key
    row = {
        "workload": workload,
        "threads": str(threads),
        "rate": str(rate),
    }

    gateway_vals: dict[str, Optional[float]] = {}
    wall_vals: dict[str, Optional[float]] = {}
    strict_vals: dict[str, Optional[float]] = {}

    for profile_name, arg_name in PROFILE_ARGS:
        profile_row = by_profile[arg_name][key]
        prefix = profile_name.replace("-", "_")
        gateway_vals[prefix] = _metric_gateway_valid(profile_row)
        wall_vals[prefix] = _metric_wall_valid(profile_row)
        strict_vals[prefix] = _metric_strict_wall_valid(profile_row)
        row[f"{prefix}_completion_semantics"] = (profile_row.get("completion_semantics") or "").strip()
        row[f"{prefix}_strict_completion_semantics"] = (profile_row.get("strict_completion_semantics") or "").strip()
        row[f"{prefix}_valid_gateway_ops_per_s"] = _fmt(gateway_vals[prefix])
        row[f"{prefix}_valid_wall_ops_per_s"] = _fmt(wall_vals[prefix])
        row[f"{prefix}_valid_strict_wall_ops_per_s"] = _fmt(strict_vals[prefix])
        row[f"{prefix}_strict_ready_pass_rate"] = (profile_row.get("pass_rate_strict_completion_ready") or "").strip()

    row["base_vs_vanilla_gateway_overhead_pct"] = _fmt(
        _pct_drop(gateway_vals["vanilla_pg"], gateway_vals["base_no_raft_no_kafka"]), 3
    )
    row["kafka_only_vs_base_gateway_overhead_pct"] = _fmt(
        _pct_drop(gateway_vals["base_no_raft_no_kafka"], gateway_vals["kafka_only_no_raft"]), 3
    )
    row["raft_vs_kafka_only_gateway_overhead_pct"] = _fmt(
        _pct_drop(gateway_vals["kafka_only_no_raft"], gateway_vals["raft_kafka"]), 3
    )
    row["raft_vs_vanilla_gateway_overhead_pct"] = _fmt(
        _pct_drop(gateway_vals["vanilla_pg"], gateway_vals["raft_kafka"]), 3
    )

    row["base_vs_vanilla_wall_overhead_pct"] = _fmt(
        _pct_drop(wall_vals["vanilla_pg"], wall_vals["base_no_raft_no_kafka"]), 3
    )
    row["kafka_only_vs_base_wall_overhead_pct"] = _fmt(
        _pct_drop(wall_vals["base_no_raft_no_kafka"], wall_vals["kafka_only_no_raft"]), 3
    )
    row["raft_vs_kafka_only_wall_overhead_pct"] = _fmt(
        _pct_drop(wall_vals["kafka_only_no_raft"], wall_vals["raft_kafka"]), 3
    )
    row["raft_vs_vanilla_wall_overhead_pct"] = _fmt(
        _pct_drop(wall_vals["vanilla_pg"], wall_vals["raft_kafka"]), 3
    )

    row["base_vs_vanilla_strict_wall_overhead_pct"] = _fmt(
        _pct_drop(strict_vals["vanilla_pg"], strict_vals["base_no_raft_no_kafka"]), 3
    )
    row["kafka_only_vs_base_strict_wall_overhead_pct"] = _fmt(
        _pct_drop(strict_vals["base_no_raft_no_kafka"], strict_vals["kafka_only_no_raft"]), 3
    )
    row["raft_vs_kafka_only_strict_wall_overhead_pct"] = _fmt(
        _pct_drop(strict_vals["kafka_only_no_raft"], strict_vals["raft_kafka"]), 3
    )
    row["raft_vs_vanilla_strict_wall_overhead_pct"] = _fmt(
        _pct_drop(strict_vals["vanilla_pg"], strict_vals["raft_kafka"]), 3
    )
    return row


def _best_case_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    metric_fields = [
        ("raft_kafka_valid_gateway_ops_per_s", "valid_gateway"),
        ("raft_kafka_valid_wall_ops_per_s", "valid_wall"),
        ("raft_kafka_valid_strict_wall_ops_per_s", "valid_strict_wall"),
    ]
    out: list[dict[str, str]] = []
    for field, label in metric_fields:
        best_row: Optional[dict[str, str]] = None
        best_val: Optional[float] = None
        for row in rows:
            val = _as_float(row.get(field, ""))
            if val is None:
                continue
            if best_val is None or val > best_val:
                best_val = val
                best_row = row
        if best_row is None:
            continue
        chosen = dict(best_row)
        chosen["selection_metric"] = label
        out.append(chosen)
    return out


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compare overhead profiles using only matched workload/thread/rate rows.",
    )
    ap.add_argument("--vanilla-pg", required=True, help="Path to vanilla-pg run dir or summary.csv")
    ap.add_argument("--base", required=True, help="Path to base-no-raft-no-kafka run dir or summary.csv")
    ap.add_argument("--kafka-only", required=True, help="Path to kafka-only-no-raft run dir or summary.csv")
    ap.add_argument("--raft-kafka", required=True, help="Path to raft-kafka run dir or summary.csv")
    ap.add_argument("--out", default="", help="Matched-row output CSV path")
    ap.add_argument("--best-case-out", default="", help="Optional best-comparable summary CSV path")
    args = ap.parse_args()

    by_profile = {
        "vanilla_pg": _load_profile_rows(_summary_path(Path(args.vanilla_pg).resolve())),
        "base": _load_profile_rows(_summary_path(Path(args.base).resolve())),
        "kafka_only": _load_profile_rows(_summary_path(Path(args.kafka_only).resolve())),
        "raft_kafka": _load_profile_rows(_summary_path(Path(args.raft_kafka).resolve())),
    }

    common_keys = set.intersection(*(set(rows.keys()) for rows in by_profile.values()))
    common_keys = set(sorted(common_keys, key=lambda x: (x[0], x[1], x[2])))
    if not common_keys:
        raise RuntimeError("No common (workload, threads, rate) rows found across all four profiles")

    out_rows = [_row_for_key(key, by_profile) for key in sorted(common_keys, key=lambda x: (x[0], x[1], x[2]))]

    raft_summary_path = _summary_path(Path(args.raft_kafka).resolve())
    out = Path(args.out).resolve() if args.out else (raft_summary_path.parent / "overhead_comparison.csv")
    _write_csv(out, out_rows)
    print(f"Wrote {out}")

    best_rows = _best_case_rows(out_rows)
    if args.best_case_out:
        best_case_out = Path(args.best_case_out).resolve()
        _write_csv(best_case_out, best_rows)
        print(f"Wrote {best_case_out}")

    preferred = next(
        (row for row in best_rows if row.get("selection_metric") == "valid_strict_wall"),
        best_rows[0] if best_rows else out_rows[0],
    )
    print("\n=== Matched Overhead Summary ===")
    print(
        "  best comparable key        : "
        f"workload={preferred.get('workload', '')} threads={preferred.get('threads', '')} rate={preferred.get('rate', '')}"
    )
    print(
        "  valid wall ops/s           : "
        f"vanilla={preferred.get('vanilla_pg_valid_wall_ops_per_s', '')} "
        f"base={preferred.get('base_no_raft_no_kafka_valid_wall_ops_per_s', '')} "
        f"kafka_only={preferred.get('kafka_only_no_raft_valid_wall_ops_per_s', '')} "
        f"raft={preferred.get('raft_kafka_valid_wall_ops_per_s', '')}"
    )
    print(
        "  valid strict wall ops/s    : "
        f"vanilla={preferred.get('vanilla_pg_valid_strict_wall_ops_per_s', '')} "
        f"base={preferred.get('base_no_raft_no_kafka_valid_strict_wall_ops_per_s', '')} "
        f"kafka_only={preferred.get('kafka_only_no_raft_valid_strict_wall_ops_per_s', '')} "
        f"raft={preferred.get('raft_kafka_valid_strict_wall_ops_per_s', '')}"
    )
    print(
        "  raft vs vanilla overhead   : "
        f"wall={preferred.get('raft_vs_vanilla_wall_overhead_pct', '')}% "
        f"strict_wall={preferred.get('raft_vs_vanilla_strict_wall_overhead_pct', '')}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
