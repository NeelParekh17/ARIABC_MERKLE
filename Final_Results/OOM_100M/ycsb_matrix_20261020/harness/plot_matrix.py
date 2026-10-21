# Figures for the 2026-10-20 YCSB matrix from summary.csv (run from the archive dir).
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

rows = list(csv.DictReader(open('summary.csv')))
WL = ['a', 'b', 'c', 'd', 'f']
SK = [0.0, 0.5, 0.99, 1.2]
MODES = [('pg', 'pg', 'tab:gray'), ('det', 'det', 'tab:blue'),
         ('merkle_lsm', 'Merkle + LSM', 'tab:green'), ('merkle_btree', 'Merkle + B-tree', 'tab:red')]
val = {(r['workload'], float(r['skew']), r['label']): r for r in rows}

def save(fig, name):
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(f'figures/{name}.{ext}', dpi=150)

fig, axes = plt.subplots(1, 5, figsize=(20, 4), sharex=True)
for ax, w in zip(axes, WL):
    for label, name, color in MODES:
        ax.plot(range(4), [float(val[w, s, label]['tps']) / 1000 for s in SK], 'o-', label=name, color=color)
    ax.set_title(f'Workload {w.upper()}')
    ax.set_xticks(range(4), [str(s) for s in SK])
    ax.set_xlabel('Zipf skew')
    ax.set_ylim(bottom=0)
    ax.grid(alpha=.3)
axes[0].set_ylabel('TPS (thousands)')
axes[0].legend(fontsize=8)
save(fig, 'tps_vs_skew')

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for w in WL:
    gw = [float(val[w, s, 'merkle_lsm']['tps']) / float(val[w, s, 'merkle_btree']['tps']) for s in SK]
    axes[0].plot(range(4), gw, 'o-', label=w.upper())
    if w == 'd':
        full = [float(val[w, s, 'merkle_lsm']['run_to_checkpoint_stmt_s']) /
                float(val[w, s, 'merkle_btree']['run_to_checkpoint_stmt_s']) for s in SK]
        axes[0].plot(range(4), full, 's--', color='tab:red', label='D incl. drain + checkpoint')
axes[0].axhline(1, color='k', lw=.8)
axes[0].set_title('Merkle + LSM / Merkle + B-tree throughput')
axes[0].set_xticks(range(4), [str(s) for s in SK])
axes[0].set_xlabel('Zipf skew')
axes[0].legend(fontsize=8)
axes[0].grid(alpha=.3)
x = range(len(WL))
for i, (label, name, color) in enumerate(MODES):
    axes[1].bar([j + (i - 1.5) * .2 for j in x], [float(val[w, 0.99, label]['wal_full_b_per_stmt']) for w in WL],
                .2, label=name, color=color)
axes[1].set_xticks(list(x), [w.upper() for w in WL])
axes[1].set_yscale('symlog', linthresh=10)
axes[1].set_title('WAL bytes per statement incl. LSM drain (skew 0.99)')
axes[1].legend(fontsize=8)
axes[1].grid(alpha=.3, axis='y')
save(fig, 'lsm_ratio_and_wal')
