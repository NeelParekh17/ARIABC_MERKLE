#!/usr/bin/env python3
"""Sample pg_stat_activity wait events every 100 ms until a stop file appears (run on the DB host)."""
import argparse,collections,json,time
from pathlib import Path
import psycopg

p=argparse.ArgumentParser()
p.add_argument('--port',type=int,default=5498)
p.add_argument('--out',required=True)
p.add_argument('--stop',required=True)
a=p.parse_args()
c=psycopg.connect(host='127.0.0.1',port=a.port,user='postgres',dbname='postgres',autocommit=True,application_name='lsm_waits')
c.execute("SET default_transaction_isolation='read committed'")
counts=collections.Counter();samples=0;start=time.time()
while not Path(a.stop).exists():
    rows=c.execute("SELECT backend_type,state,coalesce(wait_event_type,'CPU'),coalesce(wait_event,'-') FROM pg_stat_activity "
                   "WHERE pid<>pg_backend_pid() AND backend_type IN ('client backend','merkle_lsm merger','walwriter','checkpointer','background writer') "
                   "AND coalesce(application_name,'') NOT IN ('lsm_relation_audit','lsm_waits')").fetchall()
    for backend,state,wtype,wevent in rows:
        if backend=='client backend' and state!='active':continue
        counts['%s|%s|%s'%(backend,wtype,wevent)]+=1
    samples+=1
    time.sleep(0.1)
Path(a.out).write_text(json.dumps(dict(samples=samples,seconds=time.time()-start,counts=dict(counts.most_common())),indent=1)+'\n')
