#!/usr/bin/env python3
"""Render markdown tables for FINAL_REPORT.md from facts.json."""
import json,sys
d=json.load(open(sys.argv[1] if len(sys.argv)>1 else 'facts.json'))
SHORT={'public.usertable':'heap','public.usertable_pkey1':'PK','public.usertable_merkle_lookup_idx':'lookup base',
 'lsm_l0':'LSM L0','lsm_l1':'LSM L1','ariabc_internal.merkle_node_usertable':'node heap',
 'ariabc_internal.merkle_node_usertable_pkey':'node pkey','ariabc_internal.merkle_node_usertable_prefix_idx':'node prefix_idx',
 'ariabc_internal.merkle_node_usertable_root_idx':'node root_idx','public.ml_16856_meta':'LSM meta','public.usertable_merkle_idx':'merkle idx'}
ORDER=list(SHORT)
PH=['foreground','drain','checkpoint','shutdown']
def f(x):
    x=float(x)
    return '%.2fM'%(x/1e6) if x>=1e6 else ('%.0fK'%(x/1e3) if x>=1e4 else '%d'%x)
def gb(x): return '%.2f GB'%(x/1e9) if x>=1e8 else '%.0f MB'%(x/1e6)
print('## TPS\n\n| case | run TPS | total TPS (incl. drain+ckpt) | drain s | ckpt s | verify | unmapped I/O |\n|---|---|---|---|---|---|---|')
for n,c in d.items():
    print('| %s | %.0f | %.0f | %.1f | %.1f | %s | %s |'%(n,c['tps'],c['total_tps'],c['drain_s'],c['checkpoint_s'],c['verify'],gb(c['unmapped_bytes'])))
for wl in ['insert','update']:
  for t in ['t1','t2']:
    print('\n## %s %s: per relation, all phases summed (blocks read / written from file, WAL, FPI, pages dirtied)\n'%(wl,t))
    print('| relation | '+' | '.join('%s rd / wr / WAL / FPI'%m for m in ['plain','btree','lsm'])+' |\n|---|---|---|---|')
    for r in ORDER:
        cells=[];any_=False
        for m in ['plain','btree','lsm']:
            c=d['%s_%s_%s'%(wl,m,t)];s=dict(rd=0,wr=0,wal=0,fpi=0)
            for p in PH:
                v=c['rel'].get(p+'|'+r)
                if v:
                    for k in s:s[k]+=v[k]
            if any(s.values()):any_=True
            cells.append('%s / %s / %s / %s'%(f(s['rd']),f(s['wr']),gb(s['wal']) if s['wal'] else '0',f(s['fpi'])))
        if any_:print('| %s | %s |'%(SHORT[r],' | '.join(cells)))
BINS=['same','+1','-1','2-7','8-63','64-1023','>=1024']
print('\n## Access pattern (bpftrace VFS offsets, main fork). far = distance >= 64 blocks from the previous I/O of the same pid on the same file\n')
print('| case | phase | actor | relation | op | n | +1 % | near(<64) % | far % |\n|---|---|---|---|---|---|---|---|---|')
for n,c in d.items():
    if not n.endswith('t1'):continue
    rows=[]
    for k,v in c['dist'].items():
        p,a,r,o=k.split('|');tot=sum(v)
        if tot<2000 or r not in SHORT:continue
        rows.append((ORDER.index(r),p,a,r,o,tot,v))
    for _,p,a,r,o,tot,v in sorted(rows):
        print('| %s | %s | %s | %s | %s | %s | %.0f | %.0f | %.0f |'%(n,p,a,SHORT[r],o,f(tot),100*v[1]/tot,100*sum(v[:5])/tot,100*sum(v[5:])/tot))
