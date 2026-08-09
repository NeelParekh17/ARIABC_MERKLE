#!/usr/bin/env python3
"""
plot_dataset_build_comparison.py — Compare dataset build latency across scales for syncommit=on vs syncommit=off.
"""
from __future__ import annotations

import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def parse_progress_jsonl(fetched_dir):
    p = Path(fetched_dir) / 'progress.jsonl'
    if not p.exists():
        return {}
    out = {}
    with open(p) as f:
        for line in f:
            if not line.strip(): continue
            data = json.loads(line)
            if data.get('event') == 'dataset_build_timing' or 'timings_ms' in data:
                tc = int(data.get('tuple_count', 0))
                out[tc] = data['timings_ms']
    return out

on_dir = 'scripts/benchmark/recovery/fetched/ariabc-recovery-size-scaling-k75-c300-20260809T003526Z-000bc2'
off_dir = 'scripts/benchmark/recovery/fetched/ariabc-recovery-size-scaling-k75-c300-20260809T021108Z-007976'

on_data = parse_progress_jsonl(on_dir)
off_data = parse_progress_jsonl(off_dir)

scales = sorted(set(on_data.keys()) | set(off_data.keys()))
x_labels = [f"{tc // 1_000_000}M" if tc >= 1_000_000 else str(tc) for tc in scales]

on_sec = [on_data.get(tc, {}).get('dataset_total_ms', 0) / 1000.0 for tc in scales]
off_sec = [off_data.get(tc, {}).get('dataset_total_ms', 0) / 1000.0 for tc in scales]

plt.figure(figsize=(11, 6), dpi=150)
x = list(range(len(scales)))
plt.plot(x, on_sec, label="Dynamic New (syncommit=on)", color="#1f77b4", marker="o", linewidth=2.2, linestyle="--")
plt.plot(x, off_sec, label="Dynamic New (syncommit=off)", color="#d62728", marker="s", linewidth=2.5)

plt.title("Incremental Dataset Build Latency per Scale Step (1M → 50M)")
plt.xlabel("Dataset Target Scale")
plt.ylabel("Dataset Expansion Time (Seconds)")
plt.xticks(x, x_labels)
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend(frameon=True, facecolor="white", framealpha=0.9)

output_dir = Path("Dynamic_merkle_docs/run_reports/DYNAMIC_COMPARISON_ariabc-recovery-size-scaling-k75-c300-20260809T003526Z-000bc2_VS_ariabc-recovery-size-scaling-k75-c300-20260809T021108Z-007976/plots")
output_dir.mkdir(parents=True, exist_ok=True)
plt.tight_layout()
plt.savefig(output_dir / "dataset_build_time_comparison.png")
print(f"Saved dataset comparison plot to {output_dir / 'dataset_build_time_comparison.png'}")
