# Figures for the 2026-10-20 YCSB matrix. Run from the archive directory:
#   python3 harness/plot_matrix.py
# Inputs: summary.csv, evidence/sampler_series.json.gz (1 Hz commits/WAL/LSM state per run).
import csv
import gzip
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
from matplotlib.lines import Line2D

plt.rcParams.update({'font.size': 10, 'axes.titlesize': 11, 'axes.spines.top': False,
                     'axes.spines.right': False, 'savefig.bbox': 'tight'})
OUT = Path('figures')
OUT.mkdir(exist_ok=True)
rows = list(csv.DictReader(open('summary.csv')))
series = json.load(gzip.open('evidence/sampler_series.json.gz', 'rt'))
# Per-second host telemetry from the harness: [unix_s, Dirty MiB, Writeback MiB, MemAvailable MiB, read_ios,
# read_sectors, write_ios, write_sectors, in_flight, io_pressure_full_total_us, {pg wait type: backends}].
telemetry = json.load(gzip.open('evidence/telemetry_series.json.gz', 'rt'))
WL = ['a', 'b', 'c', 'd', 'f']
WL_DESC = {'a': 'A: 50% read / 50% update', 'b': 'B: 95% read / 5% update', 'c': 'C: 100% read',
           'd': 'D: 95% read-latest / 5% insert', 'f': 'F: 50% read / 50% read-modify-write'}
SK = [0.0, 0.5, 0.99, 1.2]
MODES = ['pg', 'det', 'merkle_lsm', 'merkle_btree']
NAME = {'pg': 'pg', 'det': 'det', 'merkle_lsm': 'Merkle + LSM', 'merkle_btree': 'Merkle + B-tree'}
COLOR = {'pg': '#7f7f7f', 'det': '#1f77b4', 'merkle_lsm': '#2ca02c', 'merkle_btree': '#d62728'}
MARK = {'pg': 'o', 'det': 's', 'merkle_lsm': '^', 'merkle_btree': 'v'}
WCOLOR = dict(zip(WL, ['#9467bd', '#ff7f0e', '#17becf', '#d62728', '#8c564b']))
STMT = 2_400_000
V = {(r['workload'], float(r['skew']), r['label']): r for r in rows}


def num(w, s, m, key):
    return float(V[w, s, m][key])


def save(fig, name):
    for ext in ('png', 'pdf'):
        fig.savefig(OUT / f'{name}.{ext}', dpi=160)
    plt.close(fig)


def mode_legend(fig, **kw):
    handles = [Line2D([], [], color=COLOR[m], marker=MARK[m], label=NAME[m]) for m in MODES]
    fig.legend(handles=handles, ncol=4, frameon=False, **kw)


def rate_series(key, max_s):
    """Commits/s per second and elapsed time for the measurement phase of one run."""
    pts = [p for p in series[key] if p[1] == 'm']
    t0 = pts[0][0]
    out = []
    for a, b in zip(pts, pts[1:]):
        if b[0] - t0 > max_s:
            break
        out.append(((b[0] - t0), (b[2] - a[2]) / max(b[0] - a[0], 1e-3)))
    return out


def smooth(values, n=5):
    return [sum(values[max(0, i - n + 1):i + 1]) / len(values[max(0, i - n + 1):i + 1]) for i in range(len(values))]


# 1. Throughput per workload and skew.
fig, axes = plt.subplots(1, 5, figsize=(20, 4.2))
for ax, w in zip(axes, WL):
    for m in MODES:
        ax.plot(range(4), [num(w, s, m, 'tps') / 1000 for s in SK], marker=MARK[m], color=COLOR[m], lw=2)
    ax.set_title(WL_DESC[w])
    ax.set_xticks(range(4), [str(s) for s in SK])
    ax.set_xlabel('Zipf skew')
    ax.set_ylim(bottom=0)
    ax.grid(alpha=.3)
axes[0].set_ylabel('Throughput (K statements/s)')
mode_legend(fig, loc='upper center', bbox_to_anchor=(.5, 1.06))
fig.suptitle('Throughput, 2.4M statements per run, 16 workers, SERIALIZABLE, 100M rows, 32 MB shared_buffers',
             y=1.12)
save(fig, '01_tps_vs_skew')

# 2. Every mode relative to det (heatmaps), to show what Merkle and pg cost or gain.
fig, axes = plt.subplots(1, 3, figsize=(15, 3.6))
norm = TwoSlopeNorm(vmin=0.5, vcenter=1.0, vmax=2.0)
for ax, m in zip(axes, ['pg', 'merkle_lsm', 'merkle_btree']):
    grid = [[num(w, s, m, 'tps') / num(w, s, 'det', 'tps') for s in SK] for w in WL]
    im = ax.imshow(grid, cmap='RdYlGn', norm=norm, aspect='auto')
    for i, w in enumerate(WL):
        for j, s in enumerate(SK):
            ax.text(j, i, f'{grid[i][j]:.2f}', ha='center', va='center', fontsize=9)
    ax.set_xticks(range(4), [str(s) for s in SK])
    ax.set_yticks(range(5), [w.upper() for w in WL])
    ax.set_xlabel('Zipf skew')
    ax.set_title(f'{NAME[m]} / det')
fig.colorbar(im, ax=axes, shrink=.8, label='throughput ratio (1 = same as det)')
fig.suptitle('Throughput relative to det: pg wins on reads but loses at hot-key writes; '
             'Merkle + B-tree costs most on D', y=1.03)
save(fig, '02_relative_to_det_heatmap')

# 3. Merkle overhead over det, per lookup AM.
fig, ax = plt.subplots(figsize=(13, 4))
x = 0
ticks = []
for w in WL:
    for j, s in enumerate(SK):
        for k, m in enumerate(['merkle_lsm', 'merkle_btree']):
            ov = (1 - num(w, s, m, 'tps') / num(w, s, 'det', 'tps')) * 100
            ax.bar(x + k * .4, ov, .4, color=COLOR[m])
        ticks.append((x + .2, f'{s}'))
        x += 1.1
    ax.text(x - 2.2 + .2, 47, w.upper(), ha='center', fontsize=12, weight='bold')
    x += .8
ax.axhline(0, color='k', lw=.8)
ax.set_xticks([t for t, _ in ticks], [l for _, l in ticks], fontsize=8)
ax.set_ylim(-15, 50)
ax.set_ylabel('Throughput loss vs det (%)')
ax.set_xlabel('Zipf skew, grouped by workload')
ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=COLOR[m]) for m in ('merkle_lsm', 'merkle_btree')],
          labels=[NAME[m] for m in ('merkle_lsm', 'merkle_btree')], frameon=False, loc='lower left', ncol=2)
ax.set_title('Cost of Merkle maintenance over det. D: B-tree lookup 28-43%, LSM 2-6%. A/B/F: up to ~22% for both\n'
             '(node-tree updates, same for both lookup AMs). C: none. Negative values = single-trial noise.')
ax.grid(alpha=.3, axis='y')
save(fig, '03_merkle_overhead_vs_det')

# 4. Why the LSM only helps D: lookup-index write volume vs LSM/B-tree ratio.
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5), gridspec_kw=dict(width_ratios=[1.2, 1]))
ax = axes[0]
for w in WL:
    for s in SK:
        grown = (num(w, s, 'merkle_lsm', 'lsm_l0_max_bytes') + num(w, s, 'merkle_lsm', 'lsm_l1_max_bytes') - 2 * 8192)
        ratio = num(w, s, 'merkle_lsm', 'tps') / num(w, s, 'merkle_btree', 'tps')
        ax.scatter(max(grown, 0) / 1024, ratio, color=WCOLOR[w], s=60, zorder=3, edgecolor='k', linewidth=.3)
        if w == 'd':
            ax.annotate(f'skew {s}', (grown / 1024, ratio), textcoords='offset points',
                        xytext=(-52, -7 if s == 0.0 else (5 if s == 0.5 else 0)), fontsize=8, va='center')
ax.set_xscale('symlog', linthresh=1)
ax.legend(handles=[Line2D([], [], color=WCOLOR[w], marker='o', ls='', label=w.upper()) for w in WL],
          frameon=False, loc='upper left', title='workload')
ax.annotate('A, B, C, F: L0 empty or one page,\nratio 0.88-1.08 (noise)', (2, 1.13), fontsize=8)
ax.axhline(1, color='k', lw=.8)
ax.set_xlabel('Lookup-index growth during the run: max L0 + L1 above empty (KiB)')
ax.set_ylabel('TPS Merkle + LSM / Merkle + B-tree')
ax.set_title('Only workloads that write the lookup index can gain.\n'
             'A/B/C/F leave it empty (updates are HOT, key unchanged)')
ax.grid(alpha=.3)
ax = axes[1]
gw = [num('d', s, 'merkle_lsm', 'tps') / num('d', s, 'merkle_btree', 'tps') - 1 for s in SK]
full = [num('d', s, 'merkle_lsm', 'run_to_checkpoint_stmt_s') / num('d', s, 'merkle_btree', 'run_to_checkpoint_stmt_s') - 1
        for s in SK]
ax.bar([i - .2 for i in range(4)], [g * 100 for g in gw], .4, color=COLOR['merkle_lsm'], label='during the run (gateway TPS)')
ax.bar([i + .2 for i in range(4)], [f * 100 for f in full], .4, color='#98df8a',
       label='run start to after drain + checkpoint')
for i in range(4):
    ax.text(i - .2, gw[i] * 100 + 1, f'+{gw[i]:.0%}', ha='center', fontsize=8)
    ax.text(i + .2, full[i] * 100 + 1, f'+{full[i]:.0%}', ha='center', fontsize=8)
ax.set_xticks(range(4), [str(s) for s in SK])
ax.set_xlabel('Zipf skew')
ax.set_ylabel('LSM gain over B-tree lookup (%)')
ax.set_title('Workload D: part of the gain is deferred merge work')
ax.legend(frameon=False, fontsize=8)
ax.grid(alpha=.3, axis='y')
save(fig, '04_why_lsm_helps_only_d')

# 5. WAL per statement and the LSM's deferred WAL.
fig, axes = plt.subplots(1, 2, figsize=(15, 4.5), gridspec_kw=dict(width_ratios=[2.2, 1]))
ax = axes[0]
x = 0
centers = []
for w in WL:
    for s in SK:
        for k, m in enumerate(MODES):
            ax.bar(x + k * .2, num(w, s, m, 'wal_full_b_per_stmt'), .2, color=COLOR[m])
        x += 1
    if w == 'c':
        ax.text(x - 2.6, 3, 'read-only:\nno WAL', ha='center', fontsize=8)
    centers.append(x - 2.6)
    x += .6
ax.set_yscale('log')
ax.set_xticks(centers, [w.upper() for w in WL])
ax.set_ylabel('WAL bytes per statement (log)')
ax.set_ylim(1, 8000)
ax.set_title('WAL per statement incl. LSM drain + checkpoint (bars per workload: skew 0, 0.5, 0.99, 1.2).\n'
             'D: Merkle logs ~20x det (node-tree updates per insert); A/F: Merkle adds 7% (skew 0) to 60% (skew 1.2)')
ax.grid(alpha=.3, axis='y')
ax = axes[1]
for i, s in enumerate(SK):
    run = num('d', s, 'merkle_lsm', 'wal_window_b_per_stmt')
    tot = num('d', s, 'merkle_lsm', 'wal_full_b_per_stmt')
    ax.bar(i - .2, run, .4, color=COLOR['merkle_lsm'])
    ax.bar(i - .2, tot - run, .4, bottom=run, color='#98df8a')
    ax.bar(i + .2, num('d', s, 'merkle_btree', 'wal_full_b_per_stmt'), .4, color=COLOR['merkle_btree'])
    ax.bar(i + .2, 0, .4)
ax.set_xticks(range(4), [str(s) for s in SK])
ax.set_xlabel('Zipf skew')
ax.set_ylabel('WAL bytes per statement')
ax.set_title('Workload D: LSM writes most WAL after the run')
ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=c) for c in (COLOR['merkle_lsm'], '#98df8a', COLOR['merkle_btree'])],
          labels=['LSM during run', 'LSM drain + checkpoint', 'B-tree (all during run)'], frameon=False, fontsize=8,
          loc='upper center', ncol=2)
ax.set_ylim(0, 480)
ax.grid(alpha=.3, axis='y')
mode_legend(fig, loc='upper center', bbox_to_anchor=(.35, 1.06))
save(fig, '05_wal_per_statement')

# 6. Device I/O: what bounds throughput.
fig, axes = plt.subplots(1, 3, figsize=(17, 4.6))
for r in rows:
    w, m = r['workload'], r['label']
    kw = dict(color=WCOLOR[w], marker=MARK[m], s=45, alpha=.85, edgecolor='k', linewidth=.3)
    axes[0].scatter(float(r['device_read_mib']) * 1024 / STMT, float(r['tps']) / 1000, **kw)
    axes[1].scatter(float(r['device_write_mib']) * 1024 / STMT, float(r['tps']) / 1000, **kw)
    axes[2].scatter(float(r['read_await_ms']), float(r['tps']) / 1000, **kw)
axes[0].set_xlabel('Device read KiB per statement')
axes[1].set_xlabel('Device write KiB per statement')
axes[2].set_xlabel('Mean device read latency (ms)')
axes[0].set_title('Reads: skew sets the cache hit rate')
axes[1].set_title('Writes: A/F write 10x more than B/D')
axes[2].set_title('Latency rises with write load')
for ax in axes:
    ax.set_ylabel('Throughput (K stmt/s)')
    ax.grid(alpha=.3)
handles = [Line2D([], [], color=WCOLOR[w], marker='o', ls='', label=w.upper()) for w in WL]
handles += [Line2D([], [], color='k', marker=MARK[m], ls='', label=NAME[m], markerfacecolor='none') for m in MODES]
fig.legend(handles=handles, ncol=9, frameon=False, loc='upper center', bbox_to_anchor=(.5, 1.06))
save(fig, '06_io_vs_throughput')

# 7. Skew and the page cache: reads per statement fall as skew rises.
fig, ax = plt.subplots(figsize=(7, 4.2))
for w in WL:
    ax.plot(range(4), [num(w, s, 'det', 'device_read_mib') * 1024 / STMT for s in SK], marker='o', color=WCOLOR[w],
            label=WL_DESC[w])
ax.set_xticks(range(4), [str(s) for s in SK])
ax.set_xlabel('Zipf skew')
ax.set_ylabel('Device read KiB per statement (det)')
ax.set_title('Skew 0 and 0.5: ~1 page (8 KiB) read from disk per statement;\nskew 1.2: hot set fits in cache (28 GB heap, 32 MB shared_buffers)')
ax.legend(frameon=False, fontsize=8)
ax.grid(alpha=.3)
save(fig, '07_skew_and_cache')

# 8. pg's SSI retries at hot keys vs det, which never retries.
fig, axes = plt.subplots(1, 2, figsize=(13, 4.2))
for w in ['a', 'b', 'f']:
    pts = [(j, num(w, s, 'pg', 'retry_attempts') / STMT) for j, s in enumerate(SK)
           if num(w, s, 'pg', 'retry_attempts') > 0]
    axes[0].plot([j for j, _ in pts], [r for _, r in pts], marker='o' if w != 'a' else 'x', ms=8 if w == 'a' else 6,
                 ls='--' if w == 'a' else '-', color=WCOLOR[w], label=WL_DESC[w])
    axes[1].plot(range(4), [num(w, s, 'pg', 'tps') / num(w, s, 'det', 'tps') for s in SK], marker='o', color=WCOLOR[w],
                 label=WL_DESC[w])
axes[0].set_yscale('log')
axes[0].set_ylim(1e-4, 1)
axes[0].text(0.25, 2e-4, '0 retries at skew 0 and 0.5', ha='center', fontsize=8)
axes[0].set_ylabel('pg serialization retries per statement (log)')
axes[0].set_title('pg retries explode at hot keys (A and F overlap;\ndet and Merkle: 0 retries everywhere)')
axes[1].axhline(1, color='k', lw=.8)
axes[1].set_ylabel('TPS pg / det')
axes[1].set_title('so pg falls below det at skew 1.2 in A and F')
for ax in axes:
    ax.set_xticks(range(4), [str(s) for s in SK])
    ax.set_xlabel('Zipf skew')
    ax.grid(alpha=.3)
    ax.legend(frameon=False, fontsize=8)
save(fig, '08_pg_retries_vs_det')


# 9-10. Throughput over time within a run (1 Hz sampler, 5 s moving average).
def timeline(w, s, name, title, with_lsm):
    n = 2 if with_lsm else 1
    fig, axes = plt.subplots(n, 1, figsize=(12, 3.6 * n), sharex=True, squeeze=False)
    ax = axes[0][0]
    for m in MODES:
        key = V[w, s, m]['case_dir']
        pts = rate_series(key, float(V[w, s, m]['gateway_s']))
        ax.plot([p[0] for p in pts], [v / 1000 for v in smooth([p[1] for p in pts])], color=COLOR[m],
                label=f"{NAME[m]} ({float(V[w, s, m]['tps']):,.0f} TPS)", lw=1.4)
    ax.set_ylabel('Commits/s (K, 5 s avg)')
    ax.set_title(title)
    ax.legend(frameon=False, fontsize=8, ncol=4)
    ax.grid(alpha=.3)
    if with_lsm:
        ax = axes[1][0]
        pts = [p for p in series[V[w, s, 'merkle_lsm']['case_dir']] if p[1] == 'm' and len(p) > 4]
        t0 = pts[0][0]
        pts = [p for p in pts if p[0] - t0 <= float(V[w, s, 'merkle_lsm']['gateway_s'])]
        ts = [p[0] - t0 for p in pts]
        ax.plot(ts, [sum(p[4]) / 1024 for p in pts], color='#2ca02c', label='L0 (both slots)')
        ax.plot(ts, [sum(p[5]) / 1024 for p in pts], color='#1f4e1f', label='L1 (both slots)')
        prev = pts[0][6]
        for p, t in zip(pts, ts):
            if p[6][0] > prev[0]:
                ax.axvline(t, color='orange', lw=.8, ls='--')
            if p[6][1] > prev[1]:
                ax.axvline(t, color='purple', lw=1, ls='-')
            prev = p[6]
        ax.plot([], [], color='orange', ls='--', label='L0 to L1 merge')
        ax.set_ylabel('LSM component size (KiB)')
        ax.set_xlabel('Seconds since run start')
        ax.legend(frameon=False, fontsize=8)
        ax.grid(alpha=.3)
    else:
        ax.set_xlabel('Seconds since run start')
    save(fig, name)


timeline('d', 0.99, '09_timeline_d099',
         'Workload D, skew 0.99: LSM keeps det-like throughput through its 5 merges; the B-tree lookup\n'
         'runs near LSM speed between repeated device stalls to ~0 (see figure 13)', True)
timeline('a', 0.5, '10_timeline_a05',
         'Workload A, skew 0.5: all four modes alternate between ~6.5K and <1K commits/s on a ~40 s cycle\n'
         '(dirty-page writeback of 25-30 GB per run; no checkpoint runs inside the window)', False)

# 11. LSM component sizes for every LSM run: the lookup index is only written in D.
fig, ax = plt.subplots(figsize=(11, 3.8))
labels, l0, l1 = [], [], []
for w in WL:
    for s in SK:
        labels.append(f'{w.upper()}/{s}')
        l0.append(num(w, s, 'merkle_lsm', 'lsm_l0_max_bytes') / 1024)
        l1.append(num(w, s, 'merkle_lsm', 'lsm_l1_max_bytes') / 1024)
ax.bar([i - .2 for i in range(20)], l0, .4, color='#2ca02c', label='L0 max (KiB)')
ax.bar([i + .2 for i in range(20)], l1, .4, color='#1f4e1f', label='L1 max (KiB)')
ax.set_yscale('log')
ax.set_xticks(range(20), labels, rotation=60, fontsize=8)
ax.axhline(8, color='k', lw=.8, ls=':')
ax.text(-.6, 9.2, 'empty = 8 KiB metadata page; 16 KiB = one leaf page', ha='left', fontsize=8)
ax.set_ylabel('Max component size during run (log)')
ax.set_title('merkle_lsm component sizes per run: D grows L0 to 1 MiB and L1 to ~5 MiB; every other workload stays empty')
ax.legend(frameon=False, loc='upper left', bbox_to_anchor=(0, .9))
ax.grid(alpha=.3, axis='y')
save(fig, '11_lsm_component_sizes')

# 12. Where the campaign time goes per run: restore + settle, measured run, post-run work.
fig, ax = plt.subplots(figsize=(12, 4))
x = 0
ticks = []
for w in WL:
    for m in MODES:
        reset = sum(num(w, s, m, 'reset_s') for s in SK) / 4 / 60
        run = sum(num(w, s, m, 'gateway_s') for s in SK) / 4 / 60
        post = sum(num(w, s, m, 'run_to_checkpoint_s') - num(w, s, m, 'gateway_s') for s in SK) / 4 / 60
        ax.bar(x, reset, color='#c7c7c7')
        ax.bar(x, run, bottom=reset, color=COLOR[m])
        ax.bar(x, post, bottom=reset + run, color='#ffbb78')
        ticks.append((x, NAME[m].replace('Merkle + ', 'M+')))
        x += 1
    ax.text(x - 2.5, 24.5, w.upper(), ha='center', weight='bold', fontsize=12)
    x += .7
ax.set_xticks([t for t, _ in ticks], [l for _, l in ticks], rotation=60, fontsize=7)
ax.set_ylabel('Minutes per run (mean of 4 skews)')
ax.set_ylim(0, 26)
ax.set_title('Time per run: restore + device settle (grey), measured run (mode colour), '
             'post-run drain, logs and checkpoint (orange); excludes verify and calibration')
ax.grid(alpha=.3, axis='y')
save(fig, '12_time_per_run')


def stall_fraction(key, gw):
    pts = [p for p in series[key] if p[1] == 'm']
    t0 = pts[0][0]
    r = [(b[2] - a[2]) / (b[0] - a[0]) for a, b in zip(pts, pts[1:]) if b[0] - t0 <= gw]
    med = sorted(r)[len(r) // 2]
    return sum(x < .25 * med for x in r) / len(r)


# 13. Anatomy of a stall: D/0.99 B-tree vs LSM, commits next to device traffic and the kernel's dirty pages.
fig, axes = plt.subplots(3, 2, figsize=(15, 8.5), sharex='col')
for col, m in enumerate(['merkle_btree', 'merkle_lsm']):
    r = V['d', 0.99, m]
    gw = float(r['gateway_s'])
    pts = [p for p in series[r['case_dir']] if p[1] == 'm']
    t0 = pts[0][0]
    rate = [(b[0] - t0, (b[2] - a[2]) / (b[0] - a[0])) for a, b in zip(pts, pts[1:]) if b[0] - t0 <= gw]
    tel = [x for x in telemetry[r['case_dir']] if 0 <= x[0] - t0 <= gw]
    tt = [b[0] - t0 for b in tel[1:]]
    dt = [max(b[0] - a[0], 1e-3) for a, b in zip(tel, tel[1:])]
    rd = [(b[5] - a[5]) / 2048 / d for a, b, d in zip(tel, tel[1:], dt)]
    wr = [(b[7] - a[7]) / 2048 / d for a, b, d in zip(tel, tel[1:], dt)]
    stall = [min(100, (b[9] - a[9]) / 1e4 / d) for a, b, d in zip(tel, tel[1:], dt)]
    axes[0][col].plot([x for x, _ in rate], [y / 1000 for _, y in rate], color=COLOR[m], lw=1)
    axes[0][col].set_title(f"{NAME[m]}: {float(r['tps']):,.0f} TPS, stalled {stall_fraction(r['case_dir'], gw):.0%} of seconds")
    axes[0][col].set_ylabel('Commits/s (K)')
    axes[1][col].plot(tt, wr, color='#d62728', lw=1, label='device write MiB/s')
    axes[1][col].plot(tt, rd, color='#1f77b4', lw=1, label='device read MiB/s')
    axes[1][col].set_ylabel('MiB/s')
    axes[1][col].legend(frameon=False, fontsize=8, loc='upper left')
    axes[2][col].plot([x[0] - t0 for x in tel], [x[1] + x[2] for x in tel], color='#8c564b', lw=1,
                      label='kernel Dirty + Writeback (MiB)')
    ax2 = axes[2][col].twinx()
    ax2.plot(tt, stall, color='#ff7f0e', lw=.8, alpha=.8, label='I/O pressure: % time all tasks stalled')
    ax2.set_ylim(0, 105)
    ax2.set_ylabel('IO stall %')
    axes[2][col].set_ylabel('MiB')
    axes[2][col].set_xlabel('Seconds since run start')
    h1, l1 = axes[2][col].get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    axes[2][col].legend(h1 + h2, l1 + l2, frameon=False, fontsize=8, loc='upper left')
    for ax in axes[:, col]:
        ax.grid(alpha=.3)
fig.subplots_adjust(wspace=.3)
fig.suptitle('Workload D, skew 0.99: during the B-tree stalls the device serves almost no reads or writes while I/O pressure '
             'peaks;\nkernel dirty pages stay small (<250 MiB), so this is the device stalling, not a writeback backlog. '
             'The LSM run (sequential L0\nappends instead of random writes into the 3 GB hash-ordered index) never stalls. '
             'The SSD-internal cause is not observable here.', y=1.03)
save(fig, '13_stall_anatomy_d099')

# 14. Share of each run spent stalled (commits/s below a quarter of the run's median).
fig, axes = plt.subplots(1, 4, figsize=(17, 3.6))
for ax, m in zip(axes, MODES):
    grid = [[stall_fraction(V[w, sk, m]['case_dir'], float(V[w, sk, m]['gateway_s'])) * 100 for sk in SK] for w in WL]
    im = ax.imshow(grid, cmap='Reds', vmin=0, vmax=40, aspect='auto')
    for i in range(5):
        for j in range(4):
            ax.text(j, i, f'{grid[i][j]:.0f}', ha='center', va='center', fontsize=9,
                    color='white' if grid[i][j] > 25 else 'black')
    ax.set_xticks(range(4), [str(x) for x in SK])
    ax.set_yticks(range(5), [w.upper() for w in WL])
    ax.set_xlabel('Zipf skew')
    ax.set_title(NAME[m])
fig.colorbar(im, ax=axes, shrink=.85, label='% of run seconds stalled')
fig.suptitle('Share of run seconds stalled (commits < 25% of the run median). Write-heavy A/F stall in every mode; '
             'in D only the B-tree lookup stalls.\nSingle trial per cell; B differences between modes are not consistent '
             'across skews.', y=1.06)
save(fig, '14_stall_share_heatmap')
print('figures:', sorted(p.name for p in OUT.glob('*.png')))
