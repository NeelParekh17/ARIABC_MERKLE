#!/usr/bin/env python3
"""
Plots Merkle fanout=4 vs fanout=32 comparison across Workload A, skews 0.00 and 0.99.
Generates comprehensive comparative graphs showing:
1. TPS throughput scaling (fanout=4 vs fanout=32 vs pg vs bcdb_det)
2. Physical storage I/O and buffer page read reduction
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Paths
ARTIFACT_DIR = Path("/home/neel/.gemini/antigravity-ide/brain/5472be74-1419-461d-b847-6c93321852b6")

# Style configuration
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({
    "font.size": 11,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.titlesize": 14,
})

# Benchmark data
workers = [1, 8, 16]

# Skew 0.00
pg_tps_s0 = [952.88, 1849.11, 1984.32]
det_tps_s0 = [935.37, 1835.70, 1881.11]
f4_tps_s0 = [368.05, 773.48, 871.57]
f32_tps_s0 = [664.19, 1333.69, 1334.58]

f4_devread_s0 = [821.09, 817.04, 816.71]
f32_devread_s0 = [666.15, 665.61, 666.30]
f4_blks_s0 = [169161, 169568, 169774]
f32_blks_s0 = [122479, 122952, 123023]

# Skew 0.99
pg_tps_s099 = [1103.14, 2799.55, 4585.05]
det_tps_s099 = [1164.55, 3357.96, 4738.21]
f4_tps_s099 = [668.96, 1243.70, 1444.36]
f32_tps_s099 = [874.70, 1881.82, 2166.85]

f4_devread_s099 = [525.01, 503.56, 503.71]
f32_devread_s099 = [386.39, 383.79, 384.44]
f4_blks_s099 = [105060, 105095, 105303]
f32_blks_s099 = [70885, 70996, 71144]

# -------------------------------------------------------------
# Graph 1: Direct Merkle Fanout=4 vs Fanout=32 Comparison (2x2)
# -------------------------------------------------------------
fig, axs = plt.subplots(2, 2, figsize=(14, 10))

# Top Left: TPS at Skew 0.00
x = np.arange(len(workers))
width = 0.35
rects1 = axs[0, 0].bar(x - width/2, f4_tps_s0, width, label="Merkle Fanout=4", color="#9467bd", alpha=0.9)
rects2 = axs[0, 0].bar(x + width/2, f32_tps_s0, width, label="Merkle Fanout=32", color="#2ca02c", alpha=0.9)
axs[0, 0].set_title("Throughput at Skew θ = 0.00 (Uniform)", fontweight="bold")
axs[0, 0].set_ylabel("Throughput (TPS)")
axs[0, 0].set_xticks(x)
axs[0, 0].set_xticklabels([f"w={w}" for w in workers])
axs[0, 0].legend()
for r1, r2 in zip(rects1, rects2):
    h1, h2 = r1.get_height(), r2.get_height()
    gain = ((h2 - h1) / h1) * 100
    axs[0, 0].annotate(f"{h1:.0f}", xy=(r1.get_x() + r1.get_width() / 2, h1),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)
    axs[0, 0].annotate(f"{h2:.0f}\n(+{gain:.1f}%)", xy=(r2.get_x() + r2.get_width() / 2, h2),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold")
axs[0, 0].set_ylim(0, 1600)

# Top Right: TPS at Skew 0.99
rects3 = axs[0, 1].bar(x - width/2, f4_tps_s099, width, label="Merkle Fanout=4", color="#9467bd", alpha=0.9)
rects4 = axs[0, 1].bar(x + width/2, f32_tps_s099, width, label="Merkle Fanout=32", color="#2ca02c", alpha=0.9)
axs[0, 1].set_title("Throughput at Skew θ = 0.99 (Zipfian)", fontweight="bold")
axs[0, 1].set_ylabel("Throughput (TPS)")
axs[0, 1].set_xticks(x)
axs[0, 1].set_xticklabels([f"w={w}" for w in workers])
axs[0, 1].legend()
for r1, r2 in zip(rects3, rects4):
    h1, h2 = r1.get_height(), r2.get_height()
    gain = ((h2 - h1) / h1) * 100
    axs[0, 1].annotate(f"{h1:.0f}", xy=(r1.get_x() + r1.get_width() / 2, h1),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)
    axs[0, 1].annotate(f"{h2:.0f}\n(+{gain:.1f}%)", xy=(r2.get_x() + r2.get_width() / 2, h2),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold")
axs[0, 1].set_ylim(0, 2500)

# Bottom Left: Physical NVMe Device Read (MiB)
rects5 = axs[1, 0].bar(x - width/2, f4_devread_s0, width, label="Fanout=4 (θ=0.00)", color="#1f77b4", alpha=0.7)
rects6 = axs[1, 0].bar(x + width/2, f32_devread_s0, width, label="Fanout=32 (θ=0.00)", color="#1f77b4", hatch="//", alpha=0.9)
axs[1, 0].set_title("Physical Storage NVMe Read Volume (MiB)", fontweight="bold")
axs[1, 0].set_ylabel("Device Read (MiB)")
axs[1, 0].set_xticks(x)
axs[1, 0].set_xticklabels([f"w={w}" for w in workers])
axs[1, 0].legend()
for r1, r2 in zip(rects5, rects6):
    h1, h2 = r1.get_height(), r2.get_height()
    reduction = ((h1 - h2) / h1) * 100
    axs[1, 0].annotate(f"{h1:.0f} MB", xy=(r1.get_x() + r1.get_width() / 2, h1),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)
    axs[1, 0].annotate(f"{h2:.0f} MB\n(-{reduction:.1f}%)", xy=(r2.get_x() + r2.get_width() / 2, h2),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, color="#1f77b4", fontweight="bold")
axs[1, 0].set_ylim(0, 1000)

# Bottom Right: PostgreSQL Buffer Pages Read (blks_read)
rects7 = axs[1, 1].bar(x - width/2, [b/1000 for b in f4_blks_s0], width, label="Fanout=4 (k pages)", color="#d62728", alpha=0.7)
rects8 = axs[1, 1].bar(x + width/2, [b/1000 for b in f32_blks_s0], width, label="Fanout=32 (k pages)", color="#d62728", hatch="//", alpha=0.9)
axs[1, 1].set_title("PostgreSQL Buffer Cache Misses (k Pages Read)", fontweight="bold")
axs[1, 1].set_ylabel("Buffer Reads (Thousands of 8KB Pages)")
axs[1, 1].set_xticks(x)
axs[1, 1].set_xticklabels([f"w={w}" for w in workers])
axs[1, 1].legend()
for r1, r2 in zip(rects7, rects8):
    h1, h2 = r1.get_height(), r2.get_height()
    reduction = ((h1 - h2) / h1) * 100
    axs[1, 1].annotate(f"{h1:.1f}k", xy=(r1.get_x() + r1.get_width() / 2, h1),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)
    axs[1, 1].annotate(f"{h2:.1f}k\n(-{reduction:.1f}%)", xy=(r2.get_x() + r2.get_width() / 2, h2),
                       xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, color="#d62728", fontweight="bold")
axs[1, 1].set_ylim(0, 200)

plt.suptitle("Impact of Merkle Index Tree Fanout (Fanout=4 vs Fanout=32)\n100M Database, Workload A, 20,000 Transactions, 32MB Buffer Pool", fontsize=14, fontweight="bold", y=0.98)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])

out_path1 = ARTIFACT_DIR / "merkle_fanout_comparison.png"
plt.savefig(out_path1, dpi=200)
plt.close()
print(f"Generated: {out_path1}")

# -------------------------------------------------------------
# Graph 2: All 4 Modes Scaling Curves - Skew 0.00
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 6))
ax.plot(workers, pg_tps_s0, marker="o", linewidth=2.5, markersize=8, color="#1f77b4", label="pg (PostgreSQL Baseline)")
ax.plot(workers, det_tps_s0, marker="s", linewidth=2.5, markersize=8, color="#ff7f0e", label="bcdb_det (Deterministic Concurrency)")
ax.plot(workers, f32_tps_s0, marker="^", linewidth=2.5, markersize=8, color="#2ca02c", label="bcdb_merkle (Fanout=32)")
ax.plot(workers, f4_tps_s0, marker="d", linewidth=2.5, markersize=8, color="#9467bd", linestyle="--", label="bcdb_merkle (Fanout=4)")

for w, y in zip(workers, pg_tps_s0):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(-10, 8), ha="center", fontsize=9, color="#1f77b4", fontweight="bold")
for w, y in zip(workers, det_tps_s0):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(12, -12), ha="center", fontsize=9, color="#ff7f0e", fontweight="bold")
for w, y in zip(workers, f32_tps_s0):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(-12, 8), ha="center", fontsize=9, color="#2ca02c", fontweight="bold")
for w, y in zip(workers, f4_tps_s0):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(12, -12), ha="center", fontsize=9, color="#9467bd")

ax.set_title("Cold-Start Scalability: Workload A, Skew θ = 0.00 (Uniform)\n100M Database, 20,000 Transactions, 32MB Shared Buffers", fontweight="bold")
ax.set_xlabel("Worker Threads (Concurrency)")
ax.set_ylabel("Throughput (TPS)")
ax.set_xticks(workers)
ax.set_ylim(0, 2400)
ax.legend(loc="lower right", frameon=True)
plt.tight_layout()

out_path2 = ARTIFACT_DIR / "scaling_all_modes_skew0.png"
plt.savefig(out_path2, dpi=200)
plt.close()
print(f"Generated: {out_path2}")

# -------------------------------------------------------------
# Graph 3: All 4 Modes Scaling Curves - Skew 0.99
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 6))
ax.plot(workers, det_tps_s099, marker="s", linewidth=2.5, markersize=8, color="#ff7f0e", label="bcdb_det (Deterministic Concurrency)")
ax.plot(workers, pg_tps_s099, marker="o", linewidth=2.5, markersize=8, color="#1f77b4", label="pg (PostgreSQL Baseline)")
ax.plot(workers, f32_tps_s099, marker="^", linewidth=2.5, markersize=8, color="#2ca02c", label="bcdb_merkle (Fanout=32)")
ax.plot(workers, f4_tps_s099, marker="d", linewidth=2.5, markersize=8, color="#9467bd", linestyle="--", label="bcdb_merkle (Fanout=4)")

for w, y in zip(workers, det_tps_s099):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(-10, 8), ha="center", fontsize=9, color="#ff7f0e", fontweight="bold")
for w, y in zip(workers, pg_tps_s099):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(12, -12), ha="center", fontsize=9, color="#1f77b4", fontweight="bold")
for w, y in zip(workers, f32_tps_s099):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(-12, 8), ha="center", fontsize=9, color="#2ca02c", fontweight="bold")
for w, y in zip(workers, f4_tps_s099):
    ax.annotate(f"{y:.0f}", (w, y), textcoords="offset points", xytext=(12, -12), ha="center", fontsize=9, color="#9467bd")

ax.set_title("Cold-Start Scalability: Workload A, Skew θ = 0.99 (High Contention)\n100M Database, 20,000 Transactions, 32MB Shared Buffers", fontweight="bold")
ax.set_xlabel("Worker Threads (Concurrency)")
ax.set_ylabel("Throughput (TPS)")
ax.set_xticks(workers)
ax.set_ylim(0, 5300)
ax.legend(loc="upper left", frameon=True)
plt.tight_layout()

out_path3 = ARTIFACT_DIR / "scaling_all_modes_skew099.png"
plt.savefig(out_path3, dpi=200)
plt.close()
print(f"Generated: {out_path3}")
