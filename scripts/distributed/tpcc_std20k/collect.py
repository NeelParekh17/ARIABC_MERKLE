#!/usr/bin/env python3
"""Summarize std20k runs: TPS (gateway completed_tps, v2 definition), NOPM, checks."""
import csv
import sys
from pathlib import Path

root = Path(sys.argv[1])
rows = []
for run in sorted(p for p in (root / 'runs').glob('*_w*_k*_t*') if '.failed_' not in p.name):
    r = dict(l.split('=', 1) for l in (run / 'result.txt').read_text().split() if '=' in l) \
        if (run / 'result.txt').exists() else {}
    mode, w, k, t = run.name.split('_')
    elapsed = float(r['elapsed_s']) if r.get('elapsed_s', 'missing') != 'missing' else None
    no = int(r['new_orders_committed']) if r.get('new_orders_committed') else None
    state = (run / 'state.hash').read_text().strip() if (run / 'state.hash').exists() else ''
    rows.append(dict(mode=mode, W=int(w[1:]), workers=int(k[1:]), trial=int(t[1:]),
                     tps=r.get('completed_tps'), nopm=round(no * 60 / elapsed, 1) if no and elapsed else None,
                     elapsed_s=elapsed, user_aborts=r.get('user_aborts'),
                     restarts=r.get('total_restarts'), pg_serialization_failures=r.get('retryable_sqlstate_40001'),
                     wal_bytes_per_tx=round(int(r['wal_bytes']) / 20000) if r.get('wal_bytes') else None,
                     accepted=r.get('accepted'), settle_s=r.get('presettle_s'), run_disk_util_pct=r.get('run_disk_util_pct'),
                     run_disk_w_await_ms=r.get('run_disk_w_await_ms'), state_hash=state, run=run.name))
with (root / 'all_runs.csv').open('w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=[k for k in rows[0] if k != 'state_hash'] + ['state_hash'])
    w.writeheader(); w.writerows(rows)
print('# Standard TPC-C, 20k transactions, v2 method\n')
print('TPS = gateway completed_tps over the whole 20k run (v2 definition). '
      'NOPM = committed NewOrders x 60 / gateway elapsed (tpmC-equivalent, unaudited, no think time).\n')
import statistics
def agg(vals):
    v = sorted(float(x) for x in vals if x not in (None, '', 'missing'))
    return (f'{statistics.median(v):,.0f} [{v[0]:,.0f}-{v[-1]:,.0f}] n={len(v)}' if v else '-'), (statistics.median(v) if v else None)
print('Cells: median [min-max] n=accepted trials. Only accepted runs are included; all runs are in all_runs.csv.\n')
for title, sel in (('Warehouse scaling (32 workers)', lambda r: r['workers'] == 32),
                   ('Worker scaling (W=100)', lambda r: r['W'] == 100)):
    key = 'W' if 'Warehouse' in title else 'workers'
    print(f'## {title}\n')
    print(f'| {key} | pg TPS | det TPS | Merkle TPS | pg NOPM | det NOPM | Merkle NOPM | Merkle/det (medians) |')
    print('|---:|---|---|---|---|---|---|---:|')
    for v in sorted({r[key] for r in rows if sel(r)}):
        g = {m: [r for r in rows if sel(r) and r[key] == v and r['mode'] == m and r['accepted'] == '1']
             for m in ('pg', 'det', 'merkle')}
        t = {m: agg([r['tps'] for r in g[m]]) for m in g}
        n = {m: agg([r['nopm'] for r in g[m]]) for m in g}
        ratio = f"{t['merkle'][1] / t['det'][1]:.3f}" if t['merkle'][1] and t['det'][1] else '-'
        print(f"| {v} | {t['pg'][0]} | {t['det'][0]} | {t['merkle'][0]} | {n['pg'][0]} | {n['det'][0]} | {n['merkle'][0]} | {ratio} |")
    print()
par = {}
for r in rows:
    if r['mode'] in ('det', 'merkle'):
        par.setdefault((r['W'], r['workers'], r['trial']), {})[r['mode']] = r['state_hash']
same = [k for k, v in par.items() if len(v) == 2 and v['det'] == v['merkle'] and v['det']]
print(f'Det/Merkle final-state parity (all 9 tables incl. timestamps): {len(same)}/{len(par)} points identical.')
