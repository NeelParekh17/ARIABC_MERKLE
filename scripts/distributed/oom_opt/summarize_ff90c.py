import csv, glob, sys, statistics as st
O = sys.argv[1]
rows = {}
for arm in ('R1_compact', 'R2_bloated', 'R3_compact', 'R4_bloated', 'R5_det'):
    for f in glob.glob(f'{O}/{arm}/run_*/summary.csv'):
        for r in csv.DictReader(open(f)):
            rows[(arm, r['workload'], r['skew'], r['workers'])] = r
v = lambda a, p, k: float(rows[(a,) + p][k]) if (a,) + p in rows else float('nan')
print('| point | compact t1 / t2 | bloated t1 / t2 | compact vs bloated (mean) | det | Merkle/det compact / bloated | read KB/stmt compact / bloated | write KB/stmt compact / bloated | verify |')
print('|---|---|---|---|---|---|---|---|---|')
for w, s in (('a', '0.0'), ('f', '0.99')):
    for n in ('1', '16'):
        p = (w, s, n)
        c = [v('R1_compact', p, 'tps'), v('R3_compact', p, 'tps')]; b = [v('R2_bloated', p, 'tps'), v('R4_bloated', p, 'tps')]
        d = v('R5_det', p, 'tps'); cm, bm = st.mean(c), st.mean(b)
        kb = lambda a, k: st.mean([v(x, p, k) for x in a]) * 1048576 / 1000 / 20000
        ver = [rows.get((a,) + p, {}).get('merkle_verify') for a in ('R1_compact', 'R2_bloated', 'R3_compact', 'R4_bloated')]
        print(f"| {w} θ{s} w{n} | {c[0]:.0f} / {c[1]:.0f} | {b[0]:.0f} / {b[1]:.0f} | {100*(cm/bm-1):+.1f}% | {d:.0f} | {cm/d:.2f} / {bm/d:.2f} | "
              f"{kb(('R1_compact','R3_compact'),'device_read_mib'):.1f} / {kb(('R2_bloated','R4_bloated'),'device_read_mib'):.1f} | "
              f"{kb(('R1_compact','R3_compact'),'device_write_mib'):.1f} / {kb(('R2_bloated','R4_bloated'),'device_write_mib'):.1f} | {ver} |")
