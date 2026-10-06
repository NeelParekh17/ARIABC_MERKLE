#!/usr/bin/env python3
"""Seeded clause-2 TPC-C-derived inputs. Streams SQL and records actual outcomes expected."""
import argparse
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import random
import re

SYLLABLES = ('BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING')
TS_BASE = '2025-01-01 00:00:00'
TS_STEP_US = 1000
LOAD_TS = '2024-01-01 00:00:00'


def last_name(n):
    return SYLLABLES[n // 100] + SYLLABLES[n // 10 % 10] + SYLLABLES[n % 10]


def constants(seed):
    # Separate stream: loader and run generator agree regardless of W or draw count.
    r = random.Random(f'tpcc-v3-constants:{seed}')
    load = r.randint(0, 255)
    run = r.choice([c for c in range(256) if 65 <= abs(c-load) <= 119 and abs(c-load) not in (96,112)])
    return {'C_LOAD': {'255': load}, 'C_RUN': {'255': run, '1023': r.randint(0,1023), '8191': r.randint(0,8191)}}


def nurand(r, a, x, y, c):
    return (((r.randint(0,a) | r.randint(x,y)) + c) % (y-x+1)) + x


def remote(r, home, warehouses):
    n = r.randint(1,warehouses-1)
    return n + (n >= home)


def transactions(count, warehouses, seed, stats):
    r = random.Random(seed)
    cs = constants(seed)['C_RUN']
    base = datetime.fromisoformat(TS_BASE)
    for index in range(count):
        ts = "'" + (base + timedelta(microseconds=index*TS_STEP_US)).isoformat(sep=' ', timespec='microseconds') + "'::timestamp"
        w,d = r.randint(1,warehouses),r.randint(1,10)
        draw = r.randrange(100)
        if draw < 45:
            stats['new_order'] += 1
            c = nurand(r,1023,1,3000,cs['1023'])
            items=[]
            for _ in range(r.randint(5,15)):
                i = nurand(r,8191,1,100000,cs['8191'])
                while i in items:
                    i = nurand(r,8191,1,100000,cs['8191'])
                items.append(i)
            suppliers=[]
            for _ in items:
                is_remote=warehouses>1 and r.randrange(100)==0
                suppliers.append(remote(r,w,warehouses) if is_remote else w)
                stats['remote_lines'] += int(is_remote)
                stats['order_lines'] += 1
            qty=[r.randint(1,10) for _ in items]
            abort = r.randrange(100)==0
            if abort:
                items[-1]=100001
                stats['expected_rollbacks'] += 1
            arr=lambda v:'ARRAY['+','.join(map(str,v))+']'
            yield f'SELECT public.new_order_proc_exec({w},{d},{c},{arr(items)},{arr(suppliers)},{arr(qty)},{ts});'
        elif draw < 88:
            stats['payment'] += 1
            cw,cd=w,d
            is_remote=warehouses>1 and r.randrange(100)<15
            if is_remote:
                cw,cd=remote(r,w,warehouses),r.randint(1,10)
            stats['remote_payments'] += int(is_remote)
            by_name=r.randrange(100)<60
            stats['payment_name' if by_name else 'payment_id'] += 1
            customer=("'"+last_name(nurand(r,255,0,999,cs['255']))+"'") if by_name else str(nurand(r,1023,1,3000,cs['1023']))
            cents=r.randint(100,500000)
            fn='payment_by_name_proc_exec' if by_name else 'payment_proc_exec'
            yield f'SELECT public.{fn}({customer},{cd},{cw},{w},{d},{cents//100}.{cents%100:02d},{ts});'
        elif draw < 92:
            stats['order_status'] += 1
            by_name=r.randrange(100)<60
            stats['order_status_name' if by_name else 'order_status_id'] += 1
            customer=("'"+last_name(nurand(r,255,0,999,cs['255']))+"'") if by_name else str(nurand(r,1023,1,3000,cs['1023']))
            fn='order_status_by_name_exec' if by_name else 'order_status_proc_exec'
            yield f'SELECT public.{fn}({w},{d},{customer});'
        elif draw < 96:
            stats['delivery'] += 1
            yield f'SELECT public.delivery_proc_exec({w},{r.randint(1,10)},{ts});'
        else:
            stats['stock_level'] += 1
            yield f'SELECT public.stock_level_exec({w},{d},{r.randint(10,20)});'


def self_test():
    stats=Counter()
    digest=hashlib.sha256()
    emitted=Counter()
    for line in transactions(200000,5,42,stats):
        assert line.startswith('SELECT public.') and line.endswith(');') and '\n' not in line
        digest.update(line.encode())
        if line.startswith('SELECT public.new_order_proc_exec('):
            arrays=[list(map(int,a.split(','))) for a in re.findall(r'ARRAY\[([0-9,]+)\]',line)]
            ids,suppliers,quantities=arrays
            w=int(line.split('(',1)[1].split(',',1)[0])
            assert 5<=len(ids)<=15 and len(ids)==len(set(ids))
            assert len(ids)==len(suppliers)==len(quantities) and all(1<=q<=10 for q in quantities)
            assert all(1<=i<=100000 for i in ids[:-1]) and 1<=ids[-1]<=100001
            assert all(1<=v<=5 for v in suppliers)
            emitted['expected_rollbacks']+=ids[-1]==100001
            emitted['remote_lines']+=sum(v!=w for v in suppliers)
            emitted['order_lines']+=len(ids)
        elif 'payment_' in line:
            args=line.split('(',1)[1].split(',')
            emitted['payment_name']+="'" in args[0]
            emitted['remote_payments']+=int(args[2])!=int(args[3])
            assert 1<=float(args[5])<=5000
        elif 'order_status_' in line:
            emitted['order_status_name']+='by_name' in line
    for key,value in emitted.items(): assert value==stats[key],(key,value,stats[key])
    for key,pct in [('new_order',45),('payment',43),('order_status',4),('delivery',4),('stock_level',4)]:
        assert abs(stats[key]/2000-pct)<0.5,stats
    for num,den,target,tol in [('payment_name','payment',.60,.01),('order_status_name','order_status',.60,.025),('remote_lines','order_lines',.01,.001),('remote_payments','payment',.15,.01),('expected_rollbacks','new_order',.01,.002)]:
        assert abs(stats[num]/stats[den]-target)<tol,(num,stats)
    hist={}
    for a,x,y in [(255,0,999),(1023,1,3000),(8191,1,100000)]:
        r=random.Random(99); c=constants(42)['C_RUN'][str(a)]
        bins=Counter(nurand(r,a,x,y,c) for _ in range(100000))
        assert min(bins)>=x and max(bins)<=y
        assert max(bins.values()) > 1.8*100000/(y-x+1)
        hist[str(a)]={'unique':len(bins),'max_frequency':max(bins.values()),'top5':bins.most_common(5)}
    for seed in range(1000):
        cs=constants(seed); delta=abs(cs['C_RUN']['255']-cs['C_LOAD']['255'])
        assert 65<=delta<=119 and delta not in (96,112)
    single=Counter(); list(transactions(10000,1,42,single))
    assert single['remote_lines']==single['remote_payments']==0
    again=Counter(); h2=hashlib.sha256()
    for line in transactions(200000,5,42,again): h2.update(line.encode())
    assert h2.digest()==digest.digest() and again==stats
    print(json.dumps({'self_test':'PASS','count':200000,'stats':stats,'constants':constants(42),'nurand_histograms':hist},sort_keys=True))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--count',type=int,default=20000); p.add_argument('--warehouses',type=int,default=5)
    p.add_argument('--seed',type=int,default=42); p.add_argument('-o','--output',type=Path)
    p.add_argument('--self-test',action='store_true')
    args=p.parse_args()
    if args.self_test: self_test(); return
    if args.count<1 or args.warehouses<1: p.error('count and warehouses must be positive')
    if args.output is None: p.error('-o is required')
    stats=Counter(); h=hashlib.sha256()
    with args.output.open('w') as f:
        for line in transactions(args.count,args.warehouses,args.seed,stats):
            data=(line+'\n').encode(); h.update(data); f.write(data.decode())
    meta={'format_version':3,'count':args.count,'warehouses':args.warehouses,'seed':args.seed,'counts':dict(stats),'expected_rollbacks':stats['expected_rollbacks'],**constants(args.seed),'ts_base':TS_BASE,'ts_step_us':TS_STEP_US,'sha256':h.hexdigest()}
    Path(str(args.output)+'.meta.json').write_text(json.dumps(meta,indent=2,sort_keys=True)+'\n')
    print(json.dumps(meta,sort_keys=True))

if __name__=='__main__': main()
