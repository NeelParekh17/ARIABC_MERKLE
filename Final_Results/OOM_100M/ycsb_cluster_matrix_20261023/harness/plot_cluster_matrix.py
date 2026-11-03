#!/usr/bin/env python3
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.colors import TwoSlopeNorm
import numpy as np

# Styling configuration
plt.rcParams.update({
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'xtick.labelsize': 10.5,
    'ytick.labelsize': 10.5,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'savefig.bbox': 'tight',
    'figure.autolayout': False
})

BASE_DIR = Path("/work/ARIABC/AriaBC/Final_Results/OOM_100M/ycsb_cluster_matrix_20261023")
OUT = BASE_DIR / "figures"
OUT.mkdir(exist_ok=True)

rows = list(csv.DictReader(open(BASE_DIR / 'summary.csv')))
WL = ['a', 'b', 'c', 'd', 'f']
WL_NAME = {'a': 'Workload A', 'b': 'Workload B', 'c': 'Workload C', 'd': 'Workload D', 'f': 'Workload F'}
WL_DESC = {
    'a': 'A: 50% read / 50% update',
    'b': 'B: 95% read / 5% update',
    'c': 'C: 100% read',
    'd': 'D: 95% read-latest / 5% insert',
    'f': 'F: 50% read / 50% read-modify-write'
}
SK = [0.0, 0.5, 0.99, 1.2]
SK_STR = ['0.0', '0.5', '0.99', '1.2']

MODES = ['pg', 'det', 'merkle_btree', 'merkle_lsm', 'cluster_merkle_lsm']
MODES_WITH_MAJ = ['pg', 'det', 'merkle_btree', 'merkle_lsm', 'cluster_merkle_lsm', 'cluster_merkle_lsm_maj']

NAME = {
    'pg': 'Vanilla PG',
    'det': 'Deterministic AriaBC',
    'merkle_btree': 'Merkle + B-tree (1-Node)',
    'merkle_lsm': 'Merkle + LSM (1-Node)',
    'cluster_merkle_lsm': 'Cluster Mode (Merkle + LSM, All-3)',
    'cluster_merkle_lsm_maj': 'Cluster Mode (Majority Quorum)'
}

COLOR = {
    'pg': '#7f7f7f',
    'det': '#1f77b4',
    'merkle_btree': '#d62728',
    'merkle_lsm': '#2ca02c',
    'cluster_merkle_lsm': '#9467bd',
    'cluster_merkle_lsm_maj': '#e377c2'
}

MARK = {
    'pg': 'o',
    'det': 's',
    'merkle_btree': 'v',
    'merkle_lsm': '^',
    'cluster_merkle_lsm': 'D',
    'cluster_merkle_lsm_maj': 'X'
}

LINESTYLE = {
    'pg': '-',
    'det': '-',
    'merkle_btree': '-',
    'merkle_lsm': '-',
    'cluster_merkle_lsm': '-',
    'cluster_merkle_lsm_maj': '--'
}

V = {(r['workload'].lower(), float(r['skew']), r['label']): float(r['tps']) for r in rows}

def num(w, s, m):
    return V.get((w.lower(), float(s), m), 0.0)

def save(fig, name):
    for ext in ('png', 'pdf'):
        fig.savefig(OUT / f'{name}.{ext}', dpi=300)
    plt.close(fig)
    print(f"Generated {name}.png and {name}.pdf")

def mode_legend(fig, include_maj=False, **kw):
    modes = MODES_WITH_MAJ if include_maj else MODES
    handles = [Line2D([], [], color=COLOR[m], marker=MARK[m], ls=LINESTYLE[m],
                      lw=2.2, markersize=7, label=NAME[m]) for m in modes]
    ncol = 3 if include_maj else 5
    fig.legend(handles=handles, ncol=ncol, frameon=True, facecolor='white', framealpha=0.9, **kw)


# -----------------------------------------------------------------------------
# 1. Full 1x5 Combined Panel: Throughput vs Skew across all workloads
# -----------------------------------------------------------------------------
fig, axes = plt.subplots(1, 5, figsize=(22, 4.8))
for ax, w in zip(axes, WL):
    for m in MODES:
        vals = [num(w, s, m) / 1000.0 for s in SK]
        ax.plot(range(4), vals, marker=MARK[m], color=COLOR[m], ls=LINESTYLE[m],
                lw=2.2, markersize=6.5, zorder=3)
    # optionally plot cluster majority as a subtle dashed line
    maj_vals = [num(w, s, 'cluster_merkle_lsm_maj') / 1000.0 for s in SK]
    ax.plot(range(4), maj_vals, marker=MARK['cluster_merkle_lsm_maj'], color=COLOR['cluster_merkle_lsm_maj'],
            ls=LINESTYLE['cluster_merkle_lsm_maj'], lw=1.6, markersize=5.5, alpha=0.85, zorder=2)

    ax.set_title(WL_DESC[w], weight='bold', pad=10)
    ax.set_xticks(range(4))
    ax.set_xticklabels(SK_STR)
    ax.set_xlabel('Zipf Skew')
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.25, linestyle='--')

axes[0].set_ylabel('Throughput (K statements/sec)')
mode_legend(fig, include_maj=True, loc='upper center', bbox_to_anchor=(0.5, 1.15))
fig.suptitle('4-Node Cluster Mode vs 100M Single-Node Baselines\n'
             '(2.4M statements per run, 100M rows, SERIALIZABLE, 32MB shared_buffers, 3-level Merkle LSM)',
             y=1.24, fontsize=14, weight='bold')
save(fig, '01_tps_vs_skew_all_workloads')


# -----------------------------------------------------------------------------
# 2. Individual Workload Figures (Workloads A, B, C, D, F)
# -----------------------------------------------------------------------------
for w in WL:
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    for m in MODES:
        vals = [num(w, s, m) / 1000.0 for s in SK]
        ax.plot(range(4), vals, marker=MARK[m], color=COLOR[m], ls=LINESTYLE[m],
                lw=2.5, markersize=8, label=NAME[m], zorder=4)
        for i, val in enumerate(vals):
            # Annotate points with exact values
            y_offset = 12 if m == 'cluster_merkle_lsm' else (-15 if m == 'merkle_lsm' else 5)
            ax.annotate(f"{val:.1f}k", (i, val), textcoords="offset points",
                        xytext=(0, y_offset), ha='center', fontsize=8.5,
                        color=COLOR[m], weight='bold' if 'cluster' in m else 'normal')
            
    # Also plot majority
    maj_vals = [num(w, s, 'cluster_merkle_lsm_maj') / 1000.0 for s in SK]
    ax.plot(range(4), maj_vals, marker=MARK['cluster_merkle_lsm_maj'], color=COLOR['cluster_merkle_lsm_maj'],
            ls=LINESTYLE['cluster_merkle_lsm_maj'], lw=1.8, markersize=6.5, label=NAME['cluster_merkle_lsm_maj'], zorder=3)

    ax.set_title(f"{WL_NAME[w]} Throughput vs Zipf Skew\n({WL_DESC[w]})", weight='bold', pad=12)
    ax.set_xticks(range(4))
    ax.set_xticklabels(SK_STR)
    ax.set_xlabel('Zipfian Skew Parameter', weight='bold')
    ax.set_ylabel('Throughput (K statements / second)', weight='bold')
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(frameon=True, facecolor='white', framealpha=0.95, loc='best')
    
    # Custom callout for Workload D and Workload F
    if w == 'd':
        ax.annotate('Cluster Merkle+LSM is 5.8%–40.8% FASTER\nthan Single-Node Merkle+BTree!',
                    xy=(2, num('d', 0.99, 'cluster_merkle_lsm') / 1000.0),
                    xytext=(1.0, 24),
                    arrowprops=dict(facecolor='black', arrowstyle='->', lw=1.2),
                    fontsize=9.5, weight='bold', bbox=dict(boxstyle="round,pad=0.4", fc="#fff2cc", ec="#d6b656"))
    elif w == 'f':
        ax.annotate('Cluster All-3 Verified is +10.4% faster\nthan Single-Node Merkle+LSM at Skew 0.5',
                    xy=(1, num('f', 0.5, 'cluster_merkle_lsm') / 1000.0),
                    xytext=(0.5, 4.8),
                    arrowprops=dict(facecolor='black', arrowstyle='->', lw=1.2),
                    fontsize=9.5, weight='bold', bbox=dict(boxstyle="round,pad=0.4", fc="#d5e8d4", ec="#82b366"))

    save(fig, f"workload_{w}_tps")


# -----------------------------------------------------------------------------
# 3. Workload D Deep-Dive: LSM Advantage (Eliminating B-Tree Page Splits)
# -----------------------------------------------------------------------------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.2), gridspec_kw={'width_ratios': [1.3, 1]})

# Bar plot comparing B-Tree, Single-Node LSM, and Cluster LSM
x = np.arange(len(SK))
width = 0.25

b_tree_vals = [num('d', s, 'merkle_btree') / 1000.0 for s in SK]
lsm_vals = [num('d', s, 'merkle_lsm') / 1000.0 for s in SK]
cl_vals = [num('d', s, 'cluster_merkle_lsm') / 1000.0 for s in SK]

ax1.bar(x - width, b_tree_vals, width, label='Single-Node Merkle + B-Tree', color=COLOR['merkle_btree'], alpha=0.9)
ax1.bar(x, lsm_vals, width, label='Single-Node Merkle + LSM', color=COLOR['merkle_lsm'], alpha=0.9)
ax1.bar(x + width, cl_vals, width, label='4-Node Cluster Merkle + LSM (All-3)', color=COLOR['cluster_merkle_lsm'], alpha=0.9)

for i in range(len(SK)):
    pct = ((cl_vals[i] / b_tree_vals[i]) - 1.0) * 100.0
    ax1.text(x[i] + width, cl_vals[i] + 0.5, f"+{pct:.1f}%", ha='center', va='bottom',
             fontsize=9, weight='bold', color=COLOR['cluster_merkle_lsm'])

ax1.set_xticks(x)
ax1.set_xticklabels([f"Skew {s}" for s in SK_STR])
ax1.set_ylabel('Throughput (K statements / second)', weight='bold')
ax1.set_title('Workload D: Cluster LSM vs Single-Node B-Tree', weight='bold')
ax1.grid(True, alpha=0.25, linestyle='--', axis='y')
ax1.legend(loc='upper left', frameon=True)

# Percent speedup plot
pct_speedups = [((num('d', s, 'cluster_merkle_lsm') / num('d', s, 'merkle_btree')) - 1.0) * 100.0 for s in SK]
bars = ax2.bar(SK_STR, pct_speedups, color='#2ca02c', edgecolor='black', linewidth=0.8, width=0.55)
for bar in bars:
    yval = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 1.0, f"+{yval:.1f}%", ha='center', va='bottom', weight='bold')

ax2.set_xlabel('Zipf Skew', weight='bold')
ax2.set_ylabel('Cluster LSM Speedup over B-Tree (%)', weight='bold')
ax2.set_title('Cluster Speedup over 1-Node B-Tree', weight='bold')
ax2.set_ylim(0, 50)
ax2.grid(True, alpha=0.25, linestyle='--', axis='y')

fig.suptitle('How Merkle LSM Eliminates B-Tree Page Splits and Bottlenecks in Workload D (95% Read / 5% Insert)',
             fontsize=13, weight='bold', y=1.02)
save(fig, '07_workload_d_lsm_advantage')


# -----------------------------------------------------------------------------
# 4. Heatmap: Cluster Mode (All-3) relative to Single-Node Baselines
# -----------------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
norm = TwoSlopeNorm(vmin=0.4, vcenter=1.0, vmax=1.4)

# Heatmap 1: Cluster / Det
grid_det = [[num(w, s, 'cluster_merkle_lsm') / num(w, s, 'det') for s in SK] for w in WL]
im1 = axes[0].imshow(grid_det, cmap='RdYlGn', norm=norm, aspect='auto')
for i, w in enumerate(WL):
    for j, s in enumerate(SK):
        val = grid_det[i][j]
        color = 'white' if val < 0.65 or val > 1.25 else 'black'
        axes[0].text(j, i, f'{val:.2f}x', ha='center', va='center', fontsize=9.5, weight='bold', color=color)
axes[0].set_xticks(range(4))
axes[0].set_xticklabels(SK_STR)
axes[0].set_yticks(range(5))
axes[0].set_yticklabels([w.upper() for w in WL])
axes[0].set_xlabel('Zipf Skew', weight='bold')
axes[0].set_title('Cluster All-3 / Single-Node Det AriaBC', weight='bold')

# Heatmap 2: Cluster / Single-Node Merkle+LSM
grid_lsm = [[num(w, s, 'cluster_merkle_lsm') / num(w, s, 'merkle_lsm') for s in SK] for w in WL]
im2 = axes[1].imshow(grid_lsm, cmap='RdYlGn', norm=norm, aspect='auto')
for i, w in enumerate(WL):
    for j, s in enumerate(SK):
        val = grid_lsm[i][j]
        color = 'white' if val < 0.65 or val > 1.25 else 'black'
        axes[1].text(j, i, f'{val:.2f}x', ha='center', va='center', fontsize=9.5, weight='bold', color=color)
axes[1].set_xticks(range(4))
axes[1].set_xticklabels(SK_STR)
axes[1].set_yticks(range(5))
axes[1].set_yticklabels([w.upper() for w in WL])
axes[1].set_xlabel('Zipf Skew', weight='bold')
axes[1].set_title('Cluster All-3 / Single-Node Merkle + LSM', weight='bold')

fig.colorbar(im2, ax=axes, shrink=0.85, label='Throughput Ratio (1.0 = Parity with Single Node)')
fig.suptitle('Cluster Mode Throughput Ratio vs Single-Node Deterministic & Merkle Baselines',
             fontsize=13, weight='bold', y=1.03)
save(fig, '08_relative_overhead_heatmap')

print("All figures successfully created in figures/!")
