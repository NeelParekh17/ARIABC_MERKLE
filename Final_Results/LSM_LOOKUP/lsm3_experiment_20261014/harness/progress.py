import csv,datetime as dt,json,statistics,subprocess,time
from pathlib import Path
R=Path('/home/neel/claude_ctl/lsm_lookup_20261014');O=R/'results'
while True:
 try:
  value=dict(updated_utc=dt.datetime.now(dt.timezone.utc).isoformat(),stages={})
  for stage,n in [('smoke',3),('ff90',72)]:
   sums=sorted((O/stage).glob('run_*/summary.csv'));rows=list(csv.DictReader(sums[-1].open())) if sums else []
   starts=[min(p.stat().st_mtime for p in Path(r['artifact_dir']).iterdir() if p.is_file()) for r in rows]
   gaps=[b-a for a,b in zip(starts,starts[1:]) if b>a];spacing=statistics.mean(gaps) if gaps else None
   eta=dt.datetime.fromtimestamp(max((Path(r['artifact_dir'])/'result.json').stat().st_mtime for r in rows)+(n-len(rows))*spacing,dt.timezone.utc).isoformat() if spacing else None
   value['stages'][stage]=dict(accepted=len(rows),planned=n,case_spacing_s=spacing,eta_utc=eta,accepted_by_mode={m:sum(r['mode']==m for r in rows) for m in ['det','merkle_btree','merkle_lsm']})
  (O/'progress.json').write_text(json.dumps(value,indent=2)+'\n')
 except Exception as e:(O/'progress_error.txt').write_text(repr(e)+'\n')
 time.sleep(60)
