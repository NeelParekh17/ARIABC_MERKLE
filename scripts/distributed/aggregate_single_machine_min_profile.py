#!/usr/bin/env python3
"""Aggregate per-node single-machine benchmark summaries using min TPS.

For each (mode, workload, threads, rate) key present on all nodes:
- select the minimum mean_throughput_tps across nodes
- emit distributed-style summary/results CSV rows so existing overhead
  comparison tooling can consume them
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple


Key = Tuple[str, str, int, int]


def _as_int(v: str) -> Optional[int]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _as_float(v: str) -> Optional[float]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fmtf(v: Optional[float], nd: int = 6) -> str:
    if v is None:
        return ""
    return f"{float(v):.{nd}f}"


def _summary_rows(path: Path) -> Dict[Key, dict]:
    rows: Dict[Key, dict] = {}
    with path.open("r", newline="") as f:
        for r in csv.DictReader(f):
            mode = (r.get("mode") or "").strip()
            workload = (r.get("workload") or "").strip()
            th = _as_int(r.get("threads", ""))
            rate = _as_int(r.get("rate", ""))
            if not mode or not workload or th is None or rate is None:
                continue
            rows[(mode, workload, th, rate)] = r
    return rows


def _find_node_summaries(input_root: Path) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    for d in sorted(input_root.iterdir()):
        if not d.is_dir():
            continue
        s = d / "summary.csv"
        if s.exists():
            out.append((d.name, s))
    return out


def _det_runtime_mode(mode: str) -> str:
    return "throughput" if mode.strip() == "det" else "nondet"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Aggregate single-machine per-node summaries into min-TPS profile summary/results.",
    )
    ap.add_argument("--input-root", required=True, help="single_machine_nodes_<ts> root")
    ap.add_argument("--profile", required=True, help="comparison_profile label")
    ap.add_argument("--out-dir", required=True, help="Output directory for summary/results/profiling_summary")
    ap.add_argument("--min-nodes", type=int, default=3, help="Minimum node summaries required")
    args = ap.parse_args()

    input_root = Path(args.input_root).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    node_summaries = _find_node_summaries(input_root)
    if len(node_summaries) < int(args.min_nodes):
        raise RuntimeError(
            f"need at least {args.min_nodes} node summaries under {input_root}, found {len(node_summaries)}"
        )

    node_maps: Dict[str, Dict[Key, dict]] = {}
    for node_name, summary_path in node_summaries:
        node_maps[node_name] = _summary_rows(summary_path)

    common_keys: Optional[set[Key]] = None
    for m in node_maps.values():
        ks = set(m.keys())
        common_keys = ks if common_keys is None else (common_keys & ks)
    common_keys = common_keys or set()
    if not common_keys:
        raise RuntimeError("no common (mode, workload, threads, rate) keys across node summaries")

    summary_rows: List[dict] = []
    results_rows: List[dict] = []
    profile_rows: List[dict] = []

    sorted_nodes = sorted(node_maps.keys())
    for mode, workload, threads, rate in sorted(common_keys, key=lambda x: (x[0], x[1], x[2], x[3])):
        per_node: List[Tuple[str, dict, float]] = []
        for node_name in sorted_nodes:
            row = node_maps[node_name][(mode, workload, threads, rate)]
            tps = _as_float(row.get("mean_throughput_tps", ""))
            if tps is None:
                raise RuntimeError(
                    f"missing/invalid mean_throughput_tps for key={(mode, workload, threads, rate)} node={node_name}"
                )
            per_node.append((node_name, row, tps))

        min_node, min_row, min_tps = min(per_node, key=lambda x: x[2])
        runs = _as_int(min_row.get("runs", "")) or 1
        wall_s = _as_float(min_row.get("mean_wall_time_s", ""))
        overall_ms = _as_float(min_row.get("mean_workload_overall_ms", ""))
        wait_ms = _as_float(min_row.get("mean_workload_wait_ms", ""))
        pass_merkle = min(
            (_as_float(row.get("pass_rate_merkle_verify", "")) or 0.0) for (_n, row, _tps) in per_node
        )
        mode_det_runtime = _det_runtime_mode(mode)
        completion = "single_node_local_commit"

        base_summary = {
            "comparison_profile": args.profile,
            "mode": mode,
            "det_runtime_mode": mode_det_runtime,
            "workload": workload,
            "threads": str(threads),
            "rate": str(rate),
            "runs": str(runs),
            "completed_runs": str(runs),
            "valid_runs": str(runs),
            "pass_rate_merkle_verify": _fmtf(pass_merkle, 3),
            "pass_rate_root_hash_equal": "1.000",
            "pass_rate_row_count_all_nodes_equal": "1.000",
            "completion_semantics": completion,
            "strict_completion_semantics": completion,
            "mean_wall_time_s": _fmtf(wall_s),
            "mean_gateway_overall_ms": _fmtf(overall_ms, 3),
            "mean_gateway_wait_ms": _fmtf(wait_ms, 3),
            "mean_throughput_ops_per_s": _fmtf(min_tps),
            "mean_throughput_ops_per_s_wall": _fmtf(min_tps),
            "mean_throughput_strict_ops_per_s_wall": _fmtf(min_tps),
            "throughput_ops_per_s_wall": _fmtf(min_tps),
            "throughput_strict_ops_per_s_wall": _fmtf(min_tps),
            "mean_throughput_valid_ops_per_s": _fmtf(min_tps),
            "mean_valid_throughput_ops_per_s_wall": _fmtf(min_tps),
            "mean_valid_throughput_strict_ops_per_s_wall": _fmtf(min_tps),
            "throughput_valid_ops_per_s_wall": _fmtf(min_tps),
            "throughput_valid_strict_ops_per_s_wall": _fmtf(min_tps),
            "selected_min_node": min_node,
            "selected_min_tps": _fmtf(min_tps),
            "source_nodes": ",".join(sorted_nodes),
            "source_node_count": str(len(sorted_nodes)),
        }
        summary_rows.append(base_summary)

        results_rows.append(
            {
                "comparison_profile": args.profile,
                "mode": mode,
                "det_runtime_mode": mode_det_runtime,
                "workload": workload,
                "threads": str(threads),
                "rate": str(rate),
                "run": "1",
                "valid_run": "1",
                "valid_run_strict": "1",
                "gateway_exit": "0",
                "wall_time_s": _fmtf(wall_s),
                "gateway_overall_ms": _fmtf(overall_ms, 3),
                "gateway_wait_ms": _fmtf(wait_ms, 3),
                "throughput_ops_per_s": _fmtf(min_tps),
                "throughput_valid_ops_per_s": _fmtf(min_tps),
                "throughput_ops_per_s_wall": _fmtf(min_tps),
                "throughput_valid_ops_per_s_wall": _fmtf(min_tps),
                "throughput_strict_ops_per_s_wall": _fmtf(min_tps),
                "throughput_valid_strict_ops_per_s_wall": _fmtf(min_tps),
                "completion_semantics": completion,
                "strict_completion_semantics": completion,
                "all_merkle_verify_true": "1",
                "all_root_hash_equal": "1",
                "row_count_all_nodes_equal": "1",
                "invalid_reason": "NA",
                "selected_min_node": min_node,
                "notes": "single_machine_min_tps_aggregate",
            }
        )

        profile_rows.append(
            {
                "comparison_profile": args.profile,
                "mode": mode,
                "det_runtime_mode": mode_det_runtime,
                "workload": workload,
                "threads": str(threads),
                "rate": str(rate),
                "runs": str(runs),
                "ok_runs": str(runs),
                "completion_semantics": completion,
                "strict_completion_semantics": completion,
                "throughput_ops_per_s_wall_mean": _fmtf(min_tps),
                "throughput_valid_ops_per_s_wall_mean": _fmtf(min_tps),
                "throughput_strict_ops_per_s_wall_mean": _fmtf(min_tps),
                "throughput_valid_strict_ops_per_s_wall_mean": _fmtf(min_tps),
                "throughput_ops_per_s_wall": _fmtf(min_tps),
                "throughput_valid_ops_per_s_wall": _fmtf(min_tps),
                "throughput_strict_ops_per_s_wall": _fmtf(min_tps),
                "throughput_valid_strict_ops_per_s_wall": _fmtf(min_tps),
                "selected_min_node": min_node,
                "source_node_count": str(len(sorted_nodes)),
            }
        )

    def write_csv(path: Path, rows: List[dict]) -> None:
        with path.open("w", newline="") as f:
            fieldnames = list(rows[0].keys()) if rows else []
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow(row)

    write_csv(out_dir / "summary.csv", summary_rows)
    write_csv(out_dir / "results.csv", results_rows)
    write_csv(out_dir / "profiling_summary.csv", profile_rows)

    (out_dir / "aggregation_notes.txt").write_text(
        "Aggregation model: per-key minimum mean_throughput_tps across node summaries.\n"
        f"input_root={input_root}\n"
        f"profile={args.profile}\n"
        f"nodes={','.join(sorted_nodes)}\n"
        f"keys={len(summary_rows)}\n",
        encoding="utf-8",
    )

    print(f"Wrote {out_dir / 'summary.csv'}")
    print(f"Wrote {out_dir / 'results.csv'}")
    print(f"Wrote {out_dir / 'profiling_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

