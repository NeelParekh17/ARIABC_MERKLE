#!/usr/bin/env python3
import csv,json
from pathlib import Path
R=Path('/home/neel/claude_ctl/lsm_lookup_20261014');O=R/'results';P=R/'evidence/published_configuration_summary.csv'
ref={(r['workload'],float(r['skew']),int(r['workers']),r['mode']):r for r in csv.DictReader(P.open())}
summaries=sorted((O/'smoke').glob('run_*/summary.csv'));assert summaries
rows=list(csv.DictReader(summaries[-1].open()));assert len(rows)==3
out=[];drift=[]
for r in rows:
 mapped={'det':'bcdb_det','merkle_btree':'bcdb_merkle'}.get(r['mode']);a=dict(mode=r['mode'],new_tps=float(r['tps']),artifact_dir=r['artifact_dir'])
 if mapped:
  p=ref['a',0.,8,mapped];a.update(published_median_tps=float(p['median_tps']),published_min_tps=float(p['min_tps']),published_max_tps=float(p['max_tps']),ratio=float(r['tps'])/float(p['median_tps']),published_source=p['source'])
  if not .9<=a['ratio']<=1.1:drift.append(a)
 out.append(a)
(O/'smoke_comparison.json').write_text(json.dumps(out,indent=2)+'\n')
lines=['# Smoke comparison','', 'A theta 0, workers 8, fillfactor 90, 20,000 statements. Single trials, separate from grid. Published AdHash Merkle and paper det controls.','', '| Mode | Fresh TPS | Published median | Min | Max | Fresh/published |','|---|---:|---:|---:|---:|---:|']
for a in out:
 if 'ratio' in a:lines.append(f"| {a['mode']} | {a['new_tps']:.2f} | {a['published_median_tps']:.2f} | {a['published_min_tps']:.2f} | {a['published_max_tps']:.2f} | {a['ratio']:.4f} |")
 else:lines.append(f"| {a['mode']} | {a['new_tps']:.2f} | no published LSM control | | | |")
(O/'SMOKE_COMPARISON.md').write_text('\n'.join(lines)+'\n')
if drift:
 (O/'SMOKE_DRIFT_REVIEW_REQUIRED.json').write_text(json.dumps(drift,indent=2)+'\n');raise SystemExit(2)
print('SMOKE_DRIFT_WITHIN_10_PERCENT')
