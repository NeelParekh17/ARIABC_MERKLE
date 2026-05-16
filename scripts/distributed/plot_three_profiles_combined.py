#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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


def _read_summary(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def _collect_workloads(rows_by_profile: Dict[str, List[dict]]) -> List[str]:
    ws = set()
    for rows in rows_by_profile.values():
        for r in rows:
            w = (r.get("workload") or "").strip()
            if w:
                ws.add(w)
    return sorted(ws)


def _series_for(rows: List[dict], workload: str) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    for r in rows:
        if (r.get("workload") or "").strip() != workload:
            continue
        th = _as_int(r.get("threads", ""))
        # Prefer strict-valid mean TPS if available.
        tps = _as_float(r.get("mean_throughput_valid_ops_per_s", ""))
        if tps is None:
            tps = _as_float(r.get("mean_throughput_ops_per_s", ""))
        if th is None or tps is None:
            continue
        out.append((th, tps))
    out.sort(key=lambda x: x[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot combined TPS curves for three comparison profiles")
    ap.add_argument("--base", required=True, help="Path to base profile summary.csv")
    ap.add_argument("--raft", required=True, help="Path to raft-no-kafka summary.csv")
    ap.add_argument("--raft-kafka", required=True, help="Path to raft-kafka summary.csv")
    ap.add_argument("--out", required=True, help="Output PNG path")
    args = ap.parse_args()

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        raise RuntimeError(f"matplotlib not available: {e}")

    rows_by_profile = {
        "base-no-raft-no-kafka": _read_summary(Path(args.base).resolve()),
        "raft-no-kafka": _read_summary(Path(args.raft).resolve()),
        "raft-kafka": _read_summary(Path(args.raft_kafka).resolve()),
    }

    workloads = _collect_workloads(rows_by_profile)
    if not workloads:
        raise RuntimeError("No workload rows found in input summaries")

    colors = {
        "base-no-raft-no-kafka": "#1f77b4",
        "raft-no-kafka": "#2ca02c",
        "raft-kafka": "#d62728",
    }

    fig, axes = plt.subplots(len(workloads), 1, figsize=(12, 4.2 * len(workloads)), dpi=130, squeeze=False)

    for i, workload in enumerate(workloads):
        ax = axes[i][0]
        plotted = False
        for profile, rows in rows_by_profile.items():
            pts = _series_for(rows, workload)
            if not pts:
                continue
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=4.5, color=colors[profile], label=profile)
            plotted = True
        ax.set_title(f"TPS vs Threads - {workload}")
        ax.set_xlabel("Threads")
        ax.set_ylabel("Mean Valid TPS")
        ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
        if plotted:
            ax.legend()

    fig.tight_layout()
    out = Path(args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
