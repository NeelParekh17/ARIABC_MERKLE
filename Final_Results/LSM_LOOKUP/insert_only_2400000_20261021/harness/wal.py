#!/usr/bin/env python3
"""Parse matching PG13 pg_waldump -b; retain raw compressed dump and summaries."""
import argparse
from collections import Counter,defaultdict
import gzip
import json
from pathlib import Path
import re
import subprocess

HEADER=re.compile(r'rmgr: (.*?)\s+len \(rec/tot\):\s*(\d+)/\s*(\d+), tx:\s*(\d+), lsn: ([0-9A-F]+/[0-9A-F]+),.*?desc: (\S+)')
BLOCK=re.compile(r'blkref #\d+: rel (\d+/\d+/\d+) fork (\S+) blk (\d+)(.*)')


def lsn(s):
    a,b=s.split('/');return (int(a,16)<<32)+int(b,16)


def bucket(d):
    a=abs(d)
    return 0 if a==0 else 1 if d==1 else 2 if d==-1 else 3 if a<8 else 4 if a<64 else 5 if a<1024 else 6


def run(args):
    out=Path(args.directory)
    markers=json.loads((out/'markers.json').read_text())
    intervals=[(name,lsn(markers[name]['lsn']),lsn(markers[end]['lsn'])) for name,end in
        [('foreground','drain'),('drain','checkpoint'),('checkpoint','shutdown'),('shutdown','end')]
        if name in markers and end in markers]
    cmd=[args.waldump,'-p',args.wal,'-s',markers['foreground']['lsn'],'-e',markers['end']['lsn'],'-b']
    (out/'waldump_command.json').write_text(json.dumps(cmd)+'\n')
    if markers['foreground']['lsn']==markers['end']['lsn']:
        (out/'wal_summary.json').write_text(json.dumps(dict(command=cmd,returncode=0,
            note='Identical insert-LSN endpoints: no WAL records in this interval',
            intervals=intervals,rmgr=[],relations=[],descriptions=[],blocks=[]),indent=2)+'\n')
        return
    stats=defaultdict(Counter);rmgr=defaultdict(Counter);seen=defaultdict(set)
    prev={};dist=defaultdict(Counter);xids=defaultdict(Counter)
    current=None

    def finish(rec):
        if rec is None:return
        phase=next((n for n,a,b in intervals if a<=lsn(rec['lsn'])<b),'outside')
        image_bytes=sum(b['fpi_bytes'] for b in rec['blocks'])
        if image_bytes!=rec['total']-rec['rec']:
            raise RuntimeError('Detailed block FPI bytes do not reconcile at '+rec['lsn'])
        rmgr[(phase,rec['rmgr'])].update(records=1,record_bytes=rec['rec'],total_bytes=rec['total'],fpi_bytes=rec['total']-rec['rec'])
        relations=sorted({b['rel'] for b in rec['blocks']}|set(rec.get('relation_hints',[])))
        for rel in relations or ['NO_BLOCK_REFERENCE']:
            s=stats[(phase,rel,rec['rmgr'])]
            s.update(records=1,record_bytes=rec['rec']/max(1,len(relations)))
            xids[(phase,rel)].update({'xid_zero' if rec['xid']==0 else 'xid_nonzero':1})
        for b in rec['blocks']:
            key=(phase,b['rel'],rec['rmgr'])
            stats[key].update(block_refs=1,fpis=b['fpi'],fpi_bytes=b['fpi_bytes'])
            if b['fork']=='main':
                key=(phase,b['rel'])
                seen[key].add(b['blk'])
                if key in prev:dist[key][bucket(b['blk']-prev[key])]+=1
                prev[key]=b['blk']

    err=(out/'waldump.stderr').open('w')
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
    with gzip.open(out/'waldump.txt.gz','wt',compresslevel=1) as raw:
        for line in proc.stdout:
            raw.write(line)
            m=HEADER.match(line)
            if m:
                finish(current)
                current=dict(rmgr=m[1].strip(),rec=int(m[2]),total=int(m[3]),xid=int(m[4]),lsn=m[5],desc=m[6],blocks=[])
                if current['rmgr']=='Storage':
                    hint=re.search(r'desc:\s+(?:CREATE|TRUNCATE)\s+(?:base/(\d+)/(\d+)|global/(\d+))',line)
                    if hint:current['relation_hints']=[f'1663/{hint[1]}/{hint[2]}' if hint[1] else f'1664/0/{hint[3]}']
                stats[('descriptions',m[1].strip(),m[6])]['records']+=1
            else:
                m=BLOCK.search(line)
                if m and current:
                    tail=m[4];fpi='(FPW' in tail
                    hole=re.search(r'length: (\d+)',tail);saved=re.search(r'compression saved: (\d+)',tail)
                    current['blocks'].append(dict(rel=m[1],fork=m[2],blk=int(m[3]),fpi=int(fpi),
                        fpi_bytes=(8192-(int(hole[1]) if hole else 0)-(int(saved[1]) if saved else 0)) if fpi else 0))
        finish(current)
    rc=proc.wait();err.close()
    data=dict(command=cmd,returncode=rc,intervals=intervals,
        rmgr=[dict(phase=k[0],rmgr=k[1],**v) for k,v in sorted(rmgr.items())],
        relations=[dict(phase=k[0],rel=k[1],rmgr=k[2],**v) for k,v in sorted(stats.items()) if k[0]!='descriptions'],
        descriptions=[dict(rmgr=k[1],description=k[2],**v) for k,v in sorted(stats.items()) if k[0]=='descriptions'],
        blocks=[dict(phase=k[0],rel=k[1],unique_wal_referenced_main_blocks=len(v),
            distance={str(b):dist[k][b] for b in range(7)},**xids[k]) for k,v in sorted(seen.items())])
    (out/'wal_summary.json').write_text(json.dumps(data,indent=2)+'\n')
    if rc: raise RuntimeError('pg_waldump failed; preserve stderr and partial output')
    # Independent matching-version totals for each interval, including rmgr type.
    for name,a,b in intervals:
        if a==b:continue
        cmd=[args.waldump,'-p',args.wal,'-s',f'{a>>32:X}/{a&0xffffffff:X}',
             '-e',f'{b>>32:X}/{b&0xffffffff:X}','--stats=record']
        with (out/f'waldump_stats_{name}.txt').open('w') as f:
            subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--directory',required=True)
    p.add_argument('--waldump',required=True);p.add_argument('--wal',required=True)
    run(p.parse_args())
