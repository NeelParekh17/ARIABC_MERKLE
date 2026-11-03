#!/usr/bin/env python3
"""Run on .111: compact per-case facts for the final report (stdout JSON)."""
import csv,json,sys
from pathlib import Path
R=Path('/home/neel/claude_ctl/lsm_relation_audit_20261021/results')
BINS=['same','+1','-1','2-7','8-63','64-1023','>=1024']
out={}
for d in sorted(R.iterdir()):
    if not (d/'PASS.json').exists() or 'smoke' in d.name or 'interrupted' in d.name:continue
    p=json.loads((d/'PASS.json').read_text());t=json.loads((d/'timing.json').read_text())
    c=dict(tps=p['metrics']['tps'],gateway_s=t['gateway_s'],drain_s=t['drain_s'],checkpoint_s=t['checkpoint_s'],
        total_tps=t['total_tps'],verify=p['merkle_verify'],rel={},dist={})
    for r in csv.DictReader(open(d/'relation_io.tsv'),delimiter='\t'):
        rel=r['relation']
        if rel.startswith('public.ml_') and '_l' in rel:rel='lsm_'+rel.split('_')[-2]  # l0 / l1 slots aggregated
        k=(r['phase'],rel);a=c['rel'].setdefault('|'.join(k),dict(rd=0,wr=0,wal=0,fpi=0,refs=0,uniq=0,dirtied=0))
        for f,src in [('rd','file_read_main_blocks'),('wr','file_write_main_blocks'),('wal','wal_bytes'),('fpi','fpis'),
                      ('refs','wal_block_references'),('uniq','unique_wal_referenced_pages'),('dirtied','dirtied')]:
            a[f]+=float(r[src] or 0)
    h=json.loads((d/'offset_histograms.json').read_text())
    for r in h['rows']:
        if r['fork']!='main':continue
        rel=r['relation']
        if rel.startswith('public.ml_') and '_l' in rel:rel='lsm_'+rel.split('_')[-2]
        key='|'.join([r['phase'],r['actor'],rel,r['operation']])
        a=c['dist'].setdefault(key,[0]*7)
        for i in range(7):a[i]+=r.get('distance_%d'%i,0)
    for name in ['before','after_run','after_drain']:
        s=json.loads((d/(name+'.json')).read_text()).get('lsm')
        if s:c['lsm_'+name]=dict(merges=s.get('merges'),bytes=s.get('bytes'))
    cov=json.loads((d/'coverage.json').read_text());c['unmapped_bytes']=cov.get('unmapped_file_io_bytes');c['renamed']=cov.get('renamed_relation_inodes')
    out[d.name]=c
json.dump(out,sys.stdout)
