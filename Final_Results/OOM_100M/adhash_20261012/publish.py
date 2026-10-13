#!/usr/bin/env python3
"""Replace the published Merkle rows with the AdHash (6a1c66d6) re-measurement.

PG and det rows stay from the 16577072 paper campaign (they do not use the Merkle
hash). Writes the root summary.csv and figures, the derived CSVs in this directory,
and rewrites the Merkle-dependent numbers in the root README. Local only.
"""
from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

A = Path(__file__).resolve().parent
ROOT = A.parent
PAPER = ROOT / 'paper_20261010/phase2'
FIG = ROOT / 'figures'
KEY = ['workload', 'skew', 'workers', 'mode']
MODES = ['pg', 'bcdb_det', 'bcdb_merkle']
NAMES = {'pg': 'PG (SSI + jitter)', 'bcdb_det': 'det', 'bcdb_merkle': 'det + Merkle'}
SHORT = {'pg': 'PG', 'bcdb_det': 'det', 'bcdb_merkle': 'Merkle'}
COMBOS = [('a', 0.), ('a', .99), ('a', 1.2), ('b', .99), ('c', .99), ('d', .99), ('f', .99)]
COLORS = ['#0072B2', '#E69F00', '#009E73']
MARKERS = ['o', 's', '^']

paper = pd.read_csv(PAPER / 'trials.csv')
new = pd.read_csv(A / 'campaign/summary.csv')
old_v2 = pd.read_csv(PAPER / 'previous_v2_trials.csv')
assert len(paper) == 252 and len(new) == 112
merkle = new[new['mode'] == 'bcdb_merkle']
det_control = new[new['mode'] == 'bcdb_det']
assert len(merkle) == 84 and len(det_control) == 28
for d in (merkle, det_control):
    assert (d[['divergence_count', 'permanent_failures', 'retry_exhausted']] == 0).all().all()
    assert (d.validated_completed_queries == 20000).all() and d.noise_excluded.eq(False).all()
assert (merkle.merkle_verify == 'PASS').all()
assert (merkle.groupby(KEY).trial.agg(set) == {1, 2, 3}).all()
assert set(merkle.workload_sha256) == set(paper.workload_sha256)

D = pd.concat([paper[paper['mode'] != 'bcdb_merkle'], merkle], ignore_index=True)
assert len(D) == 252 and not D.duplicated(KEY + ['trial']).any()
D.to_csv(ROOT / 'summary.csv', index=False)
D.to_csv(A / 'trials.csv', index=False)

rows = []
for key, g in D.groupby(KEY, sort=True):
    t = g.tps.to_numpy()
    v = dict(zip(KEY, key))
    v.update(n=len(t), median_tps=float(np.median(t)), min_tps=float(t.min()), max_tps=float(t.max()),
             best_tps=float(t.max()), mean_tps=float(t.mean()), sample_std_tps=float(np.std(t, ddof=1)),
             cv_pct=float(np.std(t, ddof=1) / t.mean() * 100),
             range_pct_of_median=float((t.max() - t.min()) / np.median(t) * 100))
    for i in (1, 2, 3):
        v[f'trial_{i}_tps'] = float(g.loc[g.trial == i, 'tps'].iloc[0])
    for m in ('device_read_mib', 'device_write_mib'):
        v[m + '_median'] = float(g[m].median())
    v['source'] = 'adhash_20261012 (6a1c66d6)' if key[3] == 'bcdb_merkle' else 'paper_20261010 (16577072)'
    rows.append(v)
G = pd.DataFrame(rows)
G.to_csv(A / 'configuration_summary.csv', index=False)
G.sort_values('cv_pct', ascending=False).to_csv(A / 'variability.csv', index=False)

R = []
for key, g in G.groupby(KEY[:3]):
    x = g.set_index('mode')
    v = dict(zip(KEY[:3], key))
    for mode in MODES:
        v[mode + '_median_tps'] = x.loc[mode, 'median_tps']
    for label, a, b in [('det_over_pg', 'bcdb_det', 'pg'), ('merkle_over_det', 'bcdb_merkle', 'bcdb_det'),
                        ('merkle_over_pg', 'bcdb_merkle', 'pg')]:
        v[label] = x.loc[a, 'median_tps'] / x.loc[b, 'median_tps']
    for m in ('device_read_mib', 'device_write_mib'):
        b = x.loc['bcdb_det', m + '_median']
        v[m + '_merkle_over_det'] = x.loc['bcdb_merkle', m + '_median'] / b if b > 0 else np.nan
    R.append(v)
R = pd.DataFrame(R)
R.to_csv(A / 'matched_ratios.csv', index=False)

C = G.merge(old_v2[KEY + ['tps']], on=KEY, validate='one_to_one').rename(columns={'tps': 'previous_v2_tps'})
C['new_over_previous'] = C.median_tps / C.previous_v2_tps
C.to_csv(A / 'previous_v2_comparison.csv', index=False)

cmp = pd.read_csv(A / 'comparison/comparison.csv')
H = []
for label in ['det_over_pg', 'merkle_over_det', 'merkle_over_pg']:
    for stat in ('min', 'median', 'max'):
        H.append(dict(metric=label, statistic=stat, value=getattr(R[label], stat)(), n=28))
for mode, g in C.groupby('mode'):
    for stat in ('min', 'median', 'max'):
        H.append(dict(metric=mode + '_new_over_v2', statistic=stat, value=getattr(g.new_over_previous, stat)(), n=28))
for col, label in [('merkle_ratio', 'adhash_over_xor_merkle'), ('det_control_ratio', 'det_control_over_paper_det'),
                   ('drift_adjusted_merkle_ratio', 'adhash_over_xor_drift_adjusted')]:
    for stat in ('min', 'median', 'max'):
        H.append(dict(metric=label, statistic=stat, value=getattr(cmp[col], stat)(), n=28))
H = pd.DataFrame(H)
H.to_csv(A / 'headline_numbers.csv', index=False)

# Figures: same style and geometry as paper_20261010/phase2/analyze.py.
plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 8, 'axes.labelsize': 9, 'axes.titlesize': 9,
                     'legend.fontsize': 8, 'xtick.labelsize': 8, 'ytick.labelsize': 8, 'pdf.fonttype': 42,
                     'ps.fonttype': 42, 'axes.spines.top': False, 'axes.spines.right': False, 'savefig.dpi': 300})


def save(fig, name):
    for ext in ('pdf', 'png'):
        fig.savefig(FIG / (name + '.' + ext))
    plt.close(fig)


def lines(ax, w, s):
    for mode, color, marker in zip(MODES, COLORS, MARKERS):
        d = G[(G.workload == w) & (G['skew'] == s) & (G['mode'] == mode)].sort_values('workers')
        x = d.workers.to_numpy()
        ax.fill_between(x, d.min_tps / 1000, d.max_tps / 1000, color=color, alpha=.18, linewidth=0)
        ax.plot(x, d.median_tps / 1000, color=color, marker=marker, markersize=3.5, linewidth=1.3, label=NAMES[mode])
    ax.set_xticks([1, 4, 8, 16]); ax.set_xlim(.5, 16.5); ax.set_ylim(bottom=0)
    ax.set_ylabel('Throughput (kstmt/s)'); ax.set_xlabel('Workers'); ax.set_title(f'{w.upper()}, θ = {s:g}')
    ax.grid(axis='y', alpha=.25, linewidth=.5); ax.yaxis.set_major_locator(MaxNLocator(4))


for w, s in COMBOS:
    fig, ax = plt.subplots(figsize=(3.35, 2.4)); lines(ax, w, s)
    fig.legend(*ax.get_legend_handles_labels(), loc='upper center', bbox_to_anchor=(.5, .998), ncol=3,
               frameon=False, columnspacing=.65, handlelength=1.4)
    fig.subplots_adjust(left=.19, right=.97, bottom=.20, top=.76); save(fig, f'scaling_{w}_{s}')
fig, axes = plt.subplots(7, 1, figsize=(3.35, 9.0))
for ax, (w, s) in zip(axes, COMBOS):
    lines(ax, w, s); ax.set_xlabel(''); ax.set_ylabel('kstmt/s', fontsize=8); ax.set_title('')
    ax.set_title(f'{w.upper()}, θ = {s:g}', loc='left', fontsize=8); ax.tick_params(labelsize=8)
for ax in axes[:-1]:
    ax.tick_params(labelbottom=False)
axes[-1].set_xlabel('Workers')
fig.legend(*axes[0].get_legend_handles_labels(), loc='upper center', ncol=3, frameon=False, columnspacing=.65,
           handlelength=1.3)
fig.subplots_adjust(left=.17, right=.97, bottom=.045, top=.95, hspace=.90); save(fig, 'oom_scaling_all')


def heat(data, value, name, title, vmin, vmax):
    z = np.array([[data.loc[(data.workload == w) & (data['skew'] == s) & (data.workers == t), value].iloc[0]
                   for t in [1, 4, 8, 16]] for w, s in COMBOS])
    fig, ax = plt.subplots(figsize=(3.35, 2.8))
    im = ax.imshow(z, cmap='cividis', vmin=vmin, vmax=vmax, aspect='auto')
    ax.set_xticks(range(4), [1, 4, 8, 16]); ax.set_yticks(range(7), [f'{w.upper()} θ{s:g}' for w, s in COMBOS])
    ax.set_xlabel('Workers'); ax.set_title(title.replace(': ', ':\n') if len(title) > 38 else title, fontsize=8.4)
    for i in range(7):
        for j in range(4):
            ax.text(j, i, f'{z[i, j]:.2f}', ha='center', va='center', fontsize=8,
                    color='white' if (z[i, j] - vmin) / (vmax - vmin) < .5 else 'black')
    cb = fig.colorbar(im, ax=ax, fraction=.045, pad=.04); cb.ax.tick_params(labelsize=8)
    fig.subplots_adjust(left=.23, right=.90, bottom=.18, top=.87); save(fig, name)


for value, title in [('merkle_over_det', 'Merkle / det: ratio of medians'),
                     ('merkle_over_pg', 'Merkle / PG: ratio of medians')]:
    heat(R, value, value, title, .55, 1.25)
heat(C[C['mode'] == 'bcdb_merkle'], 'new_over_previous', 'previous_v2_bcdb_merkle',
     f"{NAMES['bcdb_merkle']}: new median / Oct 1 trial", .55, 1.05)
heat(cmp, 'merkle_ratio', 'adhash_over_xor_merkle',
     'Merkle: AdHash median / XOR median', .90, 1.10)

# README: rewrite every Merkle-dependent number; PG/det text is unchanged.
readme = (ROOT / 'README.md').read_text()
f2 = lambda x: f'{x:.2f}'
hv = H.set_index(['metric', 'statistic']).value
for (w, s), heading in zip(COMBOS, [f'### {w.upper()}, θ = {s:g}' for w, s in COMBOS]):
    start = readme.index(heading)
    end = readme.find('\n### ', start + 1)
    end = readme.index('\n## ', start) if end < 0 or end > readme.index('\n## ', start) else end
    block = readme[start:end]
    for _, r in G[(G.workload == w) & (G['skew'] == s) & (G['mode'] == 'bcdb_merkle')].iterrows():
        prev = C.loc[(C.workload == w) & (C['skew'] == s) & (C.workers == r.workers) & (C['mode'] == 'bcdb_merkle')].iloc[0]
        row = (f"| {r.workers} | Merkle | 3 | {f2(r.median_tps)} | {f2(r.min_tps)} | {f2(r.max_tps)} | "
               f"{f2(r.best_tps)} | {f2(r.cv_pct)} | {f2(prev.previous_v2_tps)} | {prev.new_over_previous:.3f} |")
        block, n = re.subn(rf'^\| {r.workers} \| Merkle \|.*$', row, block, flags=re.M)
        assert n == 1, (w, s, r.workers)
    readme = readme[:start] + block + readme[end:]
for _, r in R.iterrows():
    pat = rf'^(\| {r.workload.upper()} \| {r["skew"]:g} \| {r.workers} \| [0-9.]+ \| )[0-9.]+ \| [0-9.]+ \| [0-9.]+ \| [0-9.]+ \|$'
    rep = (rf'\g<1>{r.merkle_over_det:.3f} | {r.merkle_over_pg:.3f} | '
           f'{r.device_read_mib_merkle_over_det:.3f} | {r.device_write_mib_merkle_over_det:.3f} |')
    readme, n = re.subn(pat, rep, readme, flags=re.M)
    assert n == 1, r
top = G.sort_values('cv_pct', ascending=False).head(5)
spread = '\n'.join(f"| {r.workload.upper()} | {r['skew']:g} | {r.workers} | {SHORT[r['mode']]} | {f2(r.median_tps)} | "
                   f"{f2(r.min_tps)}–{f2(r.max_tps)} | {f2(r.cv_pct)} | {f2(r.range_pct_of_median)} |" for _, r in top.iterrows())
readme = re.sub(r'(\| Workload \| θ \| W \| Mode \| Median \| Min–Max \| CV % \| Range/median % \|\n\| [-| ]+\|\n)(?:\|.*\n)+',
                lambda m: m.group(1) + spread + '\n', readme)
readme = readme.replace('Complete ranking: [variability.csv](paper_20261010/phase2/variability.csv)',
                        'Complete ranking: [variability.csv](adhash_20261012/variability.csv)')
mv = C[C['mode'] == 'bcdb_merkle'].new_over_previous
readme, n = re.subn(r'^\| Merkle \| [0-9.]+ \| [0-9.]+ \| [0-9.]+ \|$',
                    f'| Merkle (AdHash) | {mv.median():.3f} | {mv.min():.3f} | {mv.max():.3f} |', readme, flags=re.M)
assert n == 1
for metric in ('merkle_over_det', 'merkle_over_pg'):
    readme = readme.replace(f'(paper_20261010/phase2/{metric}', f'(adhash_20261012/{metric}')
readme = readme.replace('[matched_ratios.csv](paper_20261010/phase2/matched_ratios.csv)',
                        '[matched_ratios.csv](adhash_20261012/matched_ratios.csv)')
readme = readme.replace('[configuration_summary.csv](paper_20261010/phase2/configuration_summary.csv)',
                        '[configuration_summary.csv](adhash_20261012/configuration_summary.csv)')
readme = readme.replace('[headline_numbers.csv](paper_20261010/phase2/headline_numbers.csv)',
                        '[headline_numbers.csv](adhash_20261012/headline_numbers.csv)')
readme = re.sub(r'the median \*\*det/PG ratio is [0-9.]+\*\*, \*\*Merkle/det is [0-9.]+\*\*, and \*\*Merkle/PG is [0-9.]+\*\*',
                f"the median **det/PG ratio is {hv['det_over_pg','median']:.3f}**, **Merkle/det is "
                f"{hv['merkle_over_det','median']:.3f}**, and **Merkle/PG is {hv['merkle_over_pg','median']:.3f}**", readme)
readme = re.sub(r'Merkle/det ranges \*\*[0-9.]+–[0-9.]+\*\*',
                f"Merkle/det ranges **{hv['merkle_over_det','min']:.3f}–{hv['merkle_over_det','max']:.3f}**", readme)
marker = '<!-- adhash_20261012 -->'
if marker not in readme:
    note = f"""{marker}
> **Merkle rows are the AdHash re-measurement (2026-10-12/13).** Since merge `6b19ff92` (commit `6a1c66d6`), Merkle nodes combine row hashes by addition mod 2^256 instead of XOR, so identical rows no longer cancel. Every Merkle number, Merkle ratio and Merkle figure on this page comes from [adhash_20261012/](adhash_20261012/README.md): 84 accepted cases (28 points × 3 trials, all `merkle_verify` PASS) on the same hosts, harness, workload hashes and settings, against an AdHash-format baseline rebuilt from the same paper heap. PG and det rows remain the 16577072 paper campaign below; they do not use the Merkle hash. The previous XOR Merkle rows were removed from this page and from [summary.csv](summary.csv); they remain in git history (commit `d63a4cad`) and in the frozen audit archive [paper_20261010/phase2/](paper_20261010/phase2/README.md). AdHash/XOR Merkle median ratio across the 28 points: **{hv['adhash_over_xor_merkle','median']:.3f}** (range {hv['adhash_over_xor_merkle','min']:.3f}–{hv['adhash_over_xor_merkle','max']:.3f}); a one-trial det control measured host drift at **{hv['det_control_over_paper_det','median']:.3f}**, so the drift-adjusted ratio is **{hv['adhash_over_xor_drift_adjusted','median']:.3f}**. The AdHash change has no measurable throughput cost.

"""
    first = readme.index('\n\n') + 2
    readme = readme[:first] + note + readme[first:]
readme = readme.replace('Every Merkle case passed post-run verification: **84 campaign + one smoke**.',
                        'Every Merkle case passed post-run verification: **84 campaign + one smoke** (and all 84 AdHash replacement cases).')
cvs = G.sort_values('cv_pct', ascending=False)
second = cvs.iloc[1]
readme = re.sub(r'\*\*No point exceeds the 10% CV diagnostic; maximum CV is [0-9.]+%\*\*[^.]*\.',
                f"**One point exceeds the 10% CV diagnostic: AdHash Merkle A θ1.2 workers 8 ({cvs.iloc[0].cv_pct:.2f}%), "
                f"whose trial 3 ran at {G.iloc[cvs.index[0]].min_tps:.2f} TPS against ~5,400 for the other two. It passed "
                f"every acceptance check and the noise rule, so it is kept; it does not move the median. Every other point "
                f"is at most {second.cv_pct:.2f}%**.", readme)
readme = readme.replace('[previous_v2_comparison.csv](paper_20261010/phase2/previous_v2_comparison.csv)',
                        '[previous_v2_comparison.csv](adhash_20261012/previous_v2_comparison.csv)')
(ROOT / 'README.md').write_text(readme)
print(H.to_string())
