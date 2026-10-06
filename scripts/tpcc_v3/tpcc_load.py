#!/usr/bin/env python3
"""Generate seeded TPC-C v3 COPY streams, then COPY into an empty v3 schema.

--generate-only DIR and --self-test never connect to a database.
"""
import argparse
from collections import Counter
from contextlib import ExitStack
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import tempfile
import time

from generate_workload import LOAD_TS, constants, last_name, nurand

TABLES=('warehouse','district','customer','history','item','stock','oorder','new_order','order_line')
ALPHA='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
VERSION=3


def fixed(n, scale=2):
    return f'{n//10**scale}.{n%10**scale:0{scale}d}'


def generate_rows(warehouses, seed):
    """Yield (table, tuple) in bounded memory, in schema column order."""
    r=random.Random(seed)
    cload=constants(seed)['C_LOAD']['255']
    text=lambda lo,hi: ''.join(r.choices(ALPHA,k=r.randint(lo,hi)))
    digits=lambda n: ''.join(r.choices('0123456789',k=n))
    def address():
        return (text(10,20),text(10,20),text(10,20),''.join(r.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ',k=2)),digits(4)+'11111')
    def data(original):
        s=text(26,50)
        if original:
            pos=r.randint(0,len(s)-8); s=s[:pos]+'ORIGINAL'+s[pos+8:]
        return s
    originals=set(r.sample(range(1,100001),10000))
    for i in range(1,100001):
        yield 'item',(i,text(14,24),fixed(r.randint(100,10000)),data(i in originals),r.randint(1,10000))
    for w in range(1,warehouses+1):
        yield 'warehouse',(w,'300000.00',fixed(r.randint(0,2000),4),text(6,10),*address())
        originals=set(r.sample(range(1,100001),10000))
        for i in range(1,100001):
            yield 'stock',(w,i,r.randint(10,100),'0.00',0,0,data(i in originals),*(text(24,24) for _ in range(10)))
        for d in range(1,11):
            yield 'district',(w,d,'30000.00',fixed(r.randint(0,2000),4),3001,text(6,10),*address())
            bad_credit=set(r.sample(range(1,3001),300))
            for c in range(1,3001):
                name=last_name(c-1 if c<=1000 else nurand(r,255,0,999,cload))
                yield 'customer',(w,d,c,fixed(r.randint(0,5000),4),'BC' if c in bad_credit else 'GC',name,text(8,16),'50000.00','-10.00','10.00',1,0,*address(),digits(16),LOAD_TS,'OE',text(300,500))
                yield 'history',(c,d,w,d,w,LOAD_TS,'10.00',text(12,24))
            permutation=list(range(1,3001)); r.shuffle(permutation)
            for o,c in enumerate(permutation,1):
                n=r.randint(5,15); delivered=o<2101
                yield 'oorder',(w,d,o,c,r.randint(1,10) if delivered else None,n,1,LOAD_TS)
                if not delivered: yield 'new_order',(w,d,o)
                for line in range(1,n+1):
                    yield 'order_line',(w,d,o,line,r.randint(1,100000),LOAD_TS if delivered else None,'0.00' if delivered else fixed(r.randint(1,999999)),w,5,text(24,24))


def copy_line(row):
    # Generated strings use alphanumerics plus fixed timestamps: no COPY escapes needed.
    return '\t'.join('\\N' if v is None else str(v) for v in row)+'\n'


def write_cache(directory, warehouses, seed):
    directory.mkdir(parents=True,exist_ok=True)
    counts=Counter(); hashes={t:hashlib.sha256() for t in TABLES}
    with ExitStack() as stack:
        files={t:stack.enter_context(gzip.GzipFile(filename='',mode='wb',fileobj=stack.enter_context((directory/(t+'.tsv.gz')).open('wb')),compresslevel=1,mtime=0)) for t in TABLES}
        for table,row in generate_rows(warehouses,seed):
            value=copy_line(row).encode('ascii'); files[table].write(value); hashes[table].update(value); counts[table]+=1
    manifest={'format_version':VERSION,'warehouses':warehouses,'seed':seed,'load_timestamp':LOAD_TS,**constants(seed),'counts':dict(counts),'sha256_uncompressed':{t:h.hexdigest() for t,h in hashes.items()}}
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
    return manifest


def cache(root, warehouses, seed):
    root.mkdir(parents=True,exist_ok=True)
    directory=root/f'v{VERSION}-w{warehouses}-seed{seed}'
    with (root/(directory.name+'.lock')).open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if directory.exists():
            meta=json.loads((directory/'manifest.json').read_text())
            if (meta['format_version'],meta['warehouses'],meta['seed'])!=(VERSION,warehouses,seed):
                raise ValueError('cache identity mismatch')
            for t in TABLES:
                h=hashlib.sha256()
                with gzip.open(directory/(t+'.tsv.gz'),'rb') as f:
                    for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
                if h.hexdigest()!=meta['sha256_uncompressed'][t]: raise ValueError(f'cache checksum mismatch: {t}')
            return directory,meta
        with tempfile.TemporaryDirectory(prefix=directory.name+'.',dir=root) as tmp:
            stage=Path(tmp)/'ready'; meta=write_cache(stage,warehouses,seed)
            stage.rename(directory)
        return directory,meta


def load(args, directory, meta):
    # Import only for actual DB work; generation/self-tests have no dependency on psycopg.
    try:
        import psycopg2
        conn=psycopg2.connect(host=args.host,port=args.port,user=args.user,dbname=args.db)
        driver=2
    except ImportError:
        import psycopg
        conn=psycopg.connect(host=args.host,port=args.port,user=args.user,dbname=args.db)
        driver=3
    with conn:
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(f'LOCK TABLE public.{table} IN ACCESS EXCLUSIVE MODE')
                cur.execute(f'SELECT EXISTS (SELECT 1 FROM public.{table} LIMIT 1)')
                if cur.fetchone()[0]: raise ValueError(f'refusing nonempty table {table}')
                cur.execute('SELECT relpersistence FROM pg_class WHERE oid=%s::regclass',(f'public.{table}',))
                if cur.fetchone()[0]!='u': raise ValueError(f'{table} must be UNLOGGED for loading')
            for table in TABLES:
                started=time.monotonic()
                sql=f'COPY public.{table} FROM STDIN'
                with gzip.open(directory/(table+'.tsv.gz'),'rb') as f:
                    if driver==2: cur.copy_expert(sql,f,size=1024*1024)
                    else:
                        with cur.copy(sql) as cp:
                            for chunk in iter(lambda:f.read(1024*1024),b''): cp.write(chunk)
                print(f'loaded {table} rows={meta["counts"][table]} seconds={time.monotonic()-started:.2f}',flush=True)
    conn.close()


def self_test():
    with tempfile.TemporaryDirectory(prefix='tpcc-v3-load-test-') as tmp:
        directory=Path(tmp); meta=write_cache(directory,1,42)
        assert meta['counts']=={'warehouse':1,'district':10,'customer':30000,'history':30000,'item':100000,'stock':100000,'oorder':30000,'new_order':9000,'order_line':meta['counts']['order_line']}
        assert 150000<=meta['counts']['order_line']<=450000
        def rows(t):
            with gzip.open(directory/(t+'.tsv.gz'),'rt') as f:
                for line in f: yield line.rstrip('\n').split('\t')
        assert all(v[2]=='30000.00' and v[4]=='3001' for v in rows('district'))
        assert all(v[1]=='300000.00' for v in rows('warehouse'))
        bc=Counter(); names=Counter()
        for v in rows('customer'):
            w,d,c=map(int,v[:3]); bc[(w,d)]+=v[4]=='BC'
            assert v[7:12]==['50000.00','-10.00','10.00','1','0']
            assert v[18:20]==[LOAD_TS,'OE'] and 300<=len(v[20])<=500
            if c<=1000: assert v[5]==last_name(c-1)
            names[(w,d)]+=1
        assert all(n==300 for n in bc.values()) and all(n==3000 for n in names.values())
        assert sum('ORIGINAL' in v[3] for v in rows('item'))==10000
        assert sum('ORIGINAL' in v[6] for v in rows('stock'))==10000
        assert all(10<=int(v[2])<=100 and v[3:6]==['0.00','0','0'] for v in rows('stock'))
        permutations={d:set() for d in range(1,11)}; linecounts={}
        for v in rows('oorder'):
            d,o,c=int(v[1]),int(v[2]),int(v[3]); permutations[d].add(c); linecounts[(d,o)]=int(v[5])
            assert v[4]!='\\N' if o<2101 else v[4]=='\\N'
            assert v[6:]==['1',LOAD_TS]
        assert all(s==set(range(1,3001)) for s in permutations.values())
        actual=Counter()
        for v in rows('order_line'):
            d,o=int(v[1]),int(v[2]); actual[(d,o)]+=1
            assert 1<=int(v[4])<=100000 and v[7:9]==['1','5'] and len(v[9])==24
            if o<2101: assert v[5:7]==[LOAD_TS,'0.00']
            else: assert v[5]=='\\N' and 1<=round(float(v[6])*100)<=999999
        assert dict(actual)==linecounts
        assert all(2101<=int(v[2])<=3000 for v in rows('new_order'))
        assert all(v[5:7]==[LOAD_TS,'10.00'] for v in rows('history'))
        # Regenerate the entire row stream and compare hashes, without retaining rows.
        hashes={t:hashlib.sha256() for t in TABLES}
        for t,row in generate_rows(1,42): hashes[t].update(copy_line(row).encode('ascii'))
        assert {t:h.hexdigest() for t,h in hashes.items()}==meta['sha256_uncompressed']
        print(json.dumps({'self_test':'PASS','written_to_temp_dir':tmp,'counts':meta['counts'],'sha256_uncompressed':meta['sha256_uncompressed']},sort_keys=True))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--warehouses',type=int,default=5); p.add_argument('--seed',type=int,default=42)
    p.add_argument('--host',default='127.0.0.1'); p.add_argument('--port',type=int,default=5432)
    p.add_argument('--user',default='postgres'); p.add_argument('--db',default='postgres')
    p.add_argument('--cache-dir',type=Path); p.add_argument('--generate-only',type=Path)
    p.add_argument('--self-test',action='store_true'); args=p.parse_args()
    if args.self_test: self_test(); return
    if args.warehouses<1: p.error('warehouses must be positive')
    if args.generate_only:
        if args.generate_only.exists(): p.error('generate-only directory must be new')
        print(json.dumps(write_cache(args.generate_only,args.warehouses,args.seed),sort_keys=True)); return
    with ExitStack() as stack:
        root=args.cache_dir or Path(stack.enter_context(tempfile.TemporaryDirectory(prefix='tpcc-v3-load-')))
        directory,meta=cache(root,args.warehouses,args.seed)
        load(args,directory,meta)
        print(json.dumps(meta,sort_keys=True))

if __name__=='__main__': main()
