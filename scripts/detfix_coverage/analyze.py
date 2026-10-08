#!/usr/bin/env python3
"""Summarize final matrix, response evidence, trace counters and repetition."""
import collections,csv,json,os,re
from pathlib import Path
root=Path(os.environ.get('DETFIX_ROOT',str(Path(__file__).resolve().parent)))
runs=[];excluded=[]
for file in sorted((root/'runs').glob('*/summary.json')):
 r=json.loads(file.read_text())
 if 'final' not in r.get('variant',''):continue
 if r.get('variant')=='nodelay_final':
  excluded.append({'run':r['run'],'reason':'Incorrect harness variant label retained pg_sleep; replaced by nodelay_verified_final'})
  continue
 trace_counts=collections.Counter()
 wanted={'early_rotation_count','publish_hash_clear_count','post_publish_settles','ring_fallbacks','early_conflict_hits','turn_conflict_hits','restarts'}
 for trace in file.parent.glob('ptrace*'):
  if trace.is_file():
   with trace.open() as f:
    for row in csv.DictReader(f):
     for k,v in row.items():
      if k and (k.startswith('dt_') or k in wanted):trace_counts[k]+=int(v)
 r['counters']=dict(trace_counts)
 if r.get('integration') and (file.parent/'server.log').exists():
  receipts={int(m[1])-1:m[2] for m in re.finditer(r'^det-audit-(\d+)  1  ([^\n]*)$',(file.parent/'server.log').read_text(),re.M)}
  serial=[json.loads(x) for x in (file.parent/'serial_results.jsonl').read_text().splitlines()]
  def canonical(v):
   return v['tag']+''.join(' '+(c or '') for row in sorted(v['rows']) for c in row)
  mismatches=[v['seq'] for v in serial if receipts.get(v['seq'])!=canonical(v)]
  comparison={'completed':len(receipts),'mismatch_sequences':mismatches,'receipts':receipts,'source':'canonical executor output in server.log; request suffix minus reqIdOffset maps to SQL sequence'}
  (file.parent/'gateway_receipt_comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
  r['result_mismatches']=len(mismatches);r['completed']=len(receipts)
  if mismatches:r['status']='MISMATCH'
 runs.append(r)
groups=collections.defaultdict(list)
for r in runs:groups[(r['case'],r['variant'],r['integration'])].append(r)
rows=[]
for (case,variant,integration),rs in sorted(groups.items()):
 hashes={w:sorted({r.get('det_sha256','missing') for r in rs if r['workers']==w}) for w in (2,8)}
 counters=collections.Counter()
 for r in rs:counters.update(r.get('counters',{}))
 rows.append({'case':case,'variant':variant,'integration':integration,'runs':len(rs),'statuses':dict(collections.Counter(r['status'] for r in rs)),'requested':sum(r['requested'] for r in rs),'completed':sum(r.get('completed',0) for r in rs),'response_mismatches':sum(r.get('result_mismatches',0) for r in rs),'state_mismatches':sum(r.get('state_equal') is False for r in rs),'repeat_equal':all(len(hashes[w])==1 for w in (2,8)),'hashes':hashes,'counters':dict(counters)})
report={'runs':len(runs),'statuses':dict(collections.Counter(r['status'] for r in runs)),'groups':rows,'errors':[r for r in runs if r['status']=='ERROR'],'excluded':excluded}
(root/'analysis_final.json').write_text(json.dumps(report,indent=2)+'\n')
text=['| Family | Native W2 / W8 | Response mismatches | Repeat hashes |','|---|---|---:|---|']
for r in rows:
 if r['integration']:continue
 status='; '.join(f'{k} {v}' for k,v in r['statuses'].items())
 text.append(f"| {r['case']} ({r['variant']}) | {status} | {r['response_mismatches']} | {'identical' if r['repeat_equal'] else 'DIFFERENT'} |")
(root/'matrix_table.md').write_text('\n'.join(text)+'\n');print(json.dumps({'runs':len(runs),'statuses':report['statuses'],'errors':report['errors']}))
