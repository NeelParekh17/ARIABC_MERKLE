#!/usr/bin/env python3
"""DB-host observations. Run only in this audit's disposable working cluster."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import subprocess
import time

import psycopg
from psycopg.rows import dict_row

ROOT = Path('/home/neel/claude_checks/lsm_relation_audit_20261021')
SEEDS = """WITH RECURSIVE edges(parent,child) AS (
 SELECT indrelid,indexrelid FROM pg_index UNION ALL
 SELECT oid,reltoastrelid FROM pg_class WHERE reltoastrelid<>0 UNION ALL
 SELECT refobjid,objid FROM pg_depend WHERE refclassid='pg_class'::regclass
 AND classid='pg_class'::regclass AND deptype='i'
), owned(oid) AS (
 SELECT oid FROM pg_class WHERE oid IN
 (to_regclass('public.usertable'),to_regclass('ariabc_internal.merkle_node_usertable'))
 UNION
 SELECT e.child FROM owned o JOIN edges e ON e.parent=o.oid
) """
INVENTORY = SEEDS + """SELECT c.oid,n.nspname,c.relname,c.relkind,a.amname,
 c.relfilenode,c.reltoastrelid,pg_relation_filepath(c.oid) AS filepath,c.reloptions,
 pg_relation_size(c.oid) AS bytes,pg_relation_size(c.oid,'fsm') AS fsm_bytes,
 pg_relation_size(c.oid,'vm') AS vm_bytes,
 x.indislive,x.indisready,x.indisvalid,x.indnatts,x.indnkeyatts,
 CASE WHEN c.relkind='i' THEN pg_get_indexdef(c.oid) END AS indexdef,
 (SELECT json_agg(json_build_object('name',at.attname,'type',format_type(at.atttypid,at.atttypmod)))
 FROM pg_attribute at WHERE at.attrelid=c.oid AND at.attnum>0 AND NOT at.attisdropped) AS columns,
 to_jsonb(st) AS table_stats,to_jsonb(si) AS table_io,
 to_jsonb(ix) AS index_stats,to_jsonb(ii) AS index_io
 FROM owned o JOIN pg_class c ON c.oid=o.oid
 JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_am a ON a.oid=c.relam
 LEFT JOIN pg_index x ON x.indexrelid=c.oid
 LEFT JOIN pg_stat_all_tables st ON st.relid=c.oid
 LEFT JOIN pg_statio_all_tables si ON si.relid=c.oid
 LEFT JOIN pg_stat_all_indexes ix ON ix.indexrelid=c.oid
 LEFT JOIN pg_statio_all_indexes ii ON ii.indexrelid=c.oid ORDER BY c.oid"""
ACTIVITY = "SELECT pid,backend_type,application_name,state,query,backend_xid::text FROM pg_stat_activity"


def connect(port=5498, serializable=False):
    c = psycopg.connect(host='127.0.0.1', port=port, user='postgres', dbname='postgres',
                        autocommit=True, row_factory=dict_row, application_name='lsm_relation_audit')
    c.execute("SET default_transaction_isolation='" + ('serializable' if serializable else 'read committed') + "'")
    return c


def snapshot(c):
    c.execute('SELECT pg_stat_clear_snapshot()')
    rows = c.execute(INVENTORY).fetchall()
    am = next((r['amname'] for r in rows if r['relname']=='usertable_merkle_lookup_idx'), None)
    return dict(t=time.time(), lsn=c.execute('SELECT pg_current_wal_insert_lsn()::text AS lsn').fetchone()['lsn'],
        relations=rows, activity=c.execute(ACTIVITY).fetchall(),
        lsm=c.execute("SELECT merkle_lsm_stats('usertable_merkle_lookup_idx') AS s").fetchone()['s'] if am=='merkle_lsm' else None,
        all_filenodes=c.execute("SELECT c.oid,c.relfilenode,n.nspname,c.relname,pg_relation_filepath(c.oid) AS filepath FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relfilenode<>0").fetchall())


def mark(phase):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl(15, b'audit_phase', 0, 0, 0)
    libc.getpgid(-1000-phase)


def monitor(args):
    directory = ROOT / 'results' / args.case
    c = connect(args.port)
    initial = snapshot(c)
    oids = [r['oid'] for r in initial['relations']]
    am = 'merkle_lsm' if initial['lsm'] else None
    previous = None
    files = set()
    with (directory/'sampler.jsonl').open('w') as out:
        while not (directory/'observer.stop').exists():
            started = time.monotonic()
            try:
                rels = c.execute("SELECT oid,relfilenode,relname,pg_relation_filepath(oid) AS filepath FROM pg_class WHERE oid=ANY(%s)", (oids,)).fetchall()
                r = dict(t=time.time(), lsn=c.execute('SELECT pg_current_wal_insert_lsn()::text AS lsn').fetchone()['lsn'],
                    activity=c.execute(ACTIVITY).fetchall(),
                    lsm=c.execute("SELECT merkle_lsm_stats('usertable_merkle_lookup_idx') AS s").fetchone()['s'] if am else None)
                if rels != previous:
                    r['relations'] = rels
                    previous = rels
                r['files'] = []
                for d in (Path(args.pgdata)/'base').iterdir():
                    if not d.is_dir():
                        continue
                    for f in os.scandir(d):
                        if not re.fullmatch(r'\d+(?:_(?:fsm|vm|init))?(?:\.\d+)?', f.name):
                            continue
                        try:st = f.stat(follow_symlinks=False)
                        except FileNotFoundError:continue
                        key = (st.st_dev, st.st_ino, str(Path(d)/f.name))
                        if key not in files:
                            files.add(key)
                            r['files'].append(dict(device=os.major(st.st_dev)*1048576+os.minor(st.st_dev),
                                os_device=st.st_dev,inode=st.st_ino,path=key[2]))
                mem={line.split(':')[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.split(':')[0] in ['MemAvailable','Cached','Dirty','Writeback']}
                r['meminfo_kb']=mem
                r['cpu_ticks']=[int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
                out.write(json.dumps(r, default=str)+'\n')
                out.flush()
            except Exception as e:
                out.write(json.dumps(dict(t=time.time(), error=str(e)))+'\n');out.flush()
            time.sleep(max(0, args.interval-(time.monotonic()-started)))
    c.close()


def probes(args):
    directory = ROOT/'results'/args.case
    c = connect(args.port, True)
    probe_pid=c.execute('SELECT pg_backend_pid() AS p').fetchone()['p']
    # Caller completed its timed run/checkpoint. Put a first batch into L1 and
    # then leave a short tail in L0, so old/base, L1 and L0 data are all tested.
    am=c.execute("SELECT a.amname FROM pg_class r JOIN pg_am a ON a.oid=r.relam WHERE r.oid='usertable_merkle_lookup_idx'::regclass").fetchone()['amname']
    initial=c.execute("SELECT merkle_lsm_stats('usertable_merkle_lookup_idx') AS s").fetchone()['s'] if am=='merkle_lsm' else None
    c.execute("INSERT INTO usertable SELECT i," + ','.join("repeat('P',20)" for _ in range(10)) +
              " FROM generate_series(110000001,110050000) i")
    if initial:
        deadline=time.monotonic()+60
        while True:
            state=c.execute("SELECT merkle_lsm_stats('usertable_merkle_lookup_idx') AS s").fetchone()['s']
            if state['phase']=='idle' and state['merges'][0]>initial['merges'][0]:break
            if time.monotonic()>deadline:raise RuntimeError('Diagnostic L0->L1 merge did not finish')
            time.sleep(.1)
    last_key=110050000
    for attempt in range(10):
        start=last_key+1;last_key+=1000
        c.execute("INSERT INTO usertable SELECT i," + ','.join("repeat('P',20)" for _ in range(10)) +
              f" FROM generate_series({start},{last_key}) i")
        if not initial:break
        time.sleep(.2)
        state=c.execute("SELECT merkle_lsm_stats('usertable_merkle_lookup_idx') AS s").fetchone()['s']
        if state['phase']=='idle' and state['bytes'][0][state['active'][0]]>8192:break
    else:raise RuntimeError('Cannot leave a nonempty active L0 diagnostic tail')
    if initial:
        assert sum(state['bytes'][1])>16384,state
        (directory/'unmerged_components.json').write_text(json.dumps(dict(before=initial,after=state,last_key=last_key),indent=2)+'\n')
    keys = [1,2,1000,100000000,110000001,110025000,last_key]
    result=[]
    for key in keys:
        expected=c.execute('SELECT * FROM usertable WHERE ycsb_key=%s',(key,)).fetchall()
        h=c.execute("SELECT encode(merkle_key_hash(%s::int),'hex') AS h,merkle_partition_for_hash(merkle_key_hash(%s::int),200) AS p",(key,key)).fetchone()
        statement=("SELECT * FROM usertable WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),200)="+str(h['p'])+
            " AND merkle_key_hash(ycsb_key)=decode('"+h['h']+"','hex')")
        default_plan=c.execute('EXPLAIN (FORMAT JSON) '+statement).fetchone()['QUERY PLAN']
        forced='usertable_merkle_lookup_idx' not in str(default_plan)
        if forced:c.execute('SET enable_seqscan=off; SET enable_bitmapscan=off')
        plan=c.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+statement).fetchone()['QUERY PLAN']
        got=c.execute(statement).fetchall()
        assert got==expected, (key,expected,got)
        # Exact split SQL shape from merkleapply.c, narrowed to this hash so the
        # independent primary-key oracle does not need a 100M-row scan.
        split_sql=statement.replace('SELECT * FROM usertable WHERE',
            'SELECT merkle_key_hash(ycsb_key) AS kh, merkle_tuple_hash(usertable.*) AS th FROM usertable WHERE')
        split_sql=split_sql.replace("merkle_key_hash(ycsb_key)=decode('","merkle_key_hash(ycsb_key) BETWEEN decode('")
        split_sql+=" AND decode('"+h['h']+"','hex')"
        split_plan=c.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+split_sql).fetchone()['QUERY PLAN']
        split_rows=c.execute(split_sql).fetchall()
        expected_hash=c.execute('SELECT merkle_key_hash(ycsb_key) AS kh,merkle_tuple_hash(usertable.*) AS th FROM usertable WHERE ycsb_key=%s',(key,)).fetchall()
        assert split_rows==expected_hash,(key,split_rows,expected_hash)
        result.append(dict(key=key,pid=probe_pid,plan=plan,default_plan=default_plan,forced=forced,
            rows=got,sql=statement,split_sql=split_sql,split_plan=split_plan))
        c.execute('RESET enable_seqscan; RESET enable_bitmapscan')
    c.execute('RESET enable_seqscan; RESET enable_bitmapscan')
    (directory/'scan_correctness.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
    # A multi-row range containing a known unmerged new key and old base keys.
    # The independent oracle is an explicitly forced heap scan, performed once
    # outside all benchmark/I/O timing rather than inferred from the same AM.
    h=c.execute("SELECT encode(merkle_key_hash(%s::int),'hex') AS h,merkle_partition_for_hash(merkle_key_hash(%s::int),200) AS p",(last_key,last_key)).fetchone()
    for digits in [4,3,2]:
        lo=h['h'][:digits]+'0'*(16-digits);hi=h['h'][:digits]+'f'*(16-digits)
        statement=("SELECT * FROM usertable WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),200)="+str(h['p'])+
            " AND merkle_key_hash(ycsb_key) BETWEEN decode('"+lo+"','hex') AND decode('"+hi+"','hex') ORDER BY merkle_key_hash(ycsb_key),ycsb_key")
        c.execute('SET enable_seqscan=off; SET enable_bitmapscan=off')
        index_plan=c.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+statement).fetchone()['QUERY PLAN']
        got=c.execute(statement).fetchall()
        if any(r['ycsb_key']<=100000000 for r in got) and any(r['ycsb_key']>=110000001 for r in got):break
    else:raise RuntimeError('Cannot construct a mixed base/upper range control')
    assert 'usertable_merkle_lookup_idx' in str(index_plan),index_plan
    c.execute('SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off')
    oracle_plan=c.execute('EXPLAIN (FORMAT JSON) '+statement).fetchone()['QUERY PLAN']
    expected=c.execute(statement).fetchall()
    assert got==expected,('range mismatch',got,expected)
    (directory/'range_correctness.json').write_text(json.dumps(dict(pid=probe_pid,sql=statement,
        index_plan=index_plan,oracle_plan=oracle_plan,rows=len(got),
        old_rows=sum(r['ycsb_key']<=100000000 for r in got),
        new_rows=sum(r['ycsb_key']>=110000001 for r in got),equal=True,actual=got),indent=2,default=str)+'\n')
    c.close()


def plans(args):
    directory = ROOT/'results'/args.case
    examples=json.loads((ROOT/'scripts/examples.json').read_text())
    result=[]
    for operation,statement in examples.items():
        with connect(args.port) as observer:
            before=snapshot(observer)
        with connect(args.port,True) as c:
            pid=c.execute('SELECT pg_backend_pid() AS p').fetchone()['p']
            c.execute('BEGIN ISOLATION LEVEL SERIALIZABLE')
            plan=c.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+statement).fetchone()['QUERY PLAN']
            c.execute('COMMIT')
        time.sleep(1)
        with connect(args.port) as observer:
            after=snapshot(observer)
        result.append(dict(operation=operation,sql=statement,pid=pid,before=before,after=after,plan=plan))
        walout=directory/('plan_'+operation)
        walout.mkdir(exist_ok=True)
        markers=dict(foreground={'lsn':before['lsn']})
        markers.update({name:{'lsn':after['lsn']} for name in ['drain','checkpoint','shutdown','end']})
        (walout/'markers.json').write_text(json.dumps(markers)+'\n')
        subprocess.run(['python3',str(ROOT/'scripts/wal.py'),'--directory',str(walout),
            '--waldump','/home/neel/claude_checks/ycsb_matrix_20261020/install/bin/pg_waldump',
            '--wal',args.pgdata+'/pg_wal'],check=True)
    (directory/'plans.json').write_text(json.dumps(result,indent=2,default=str)+'\n')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['snapshot','monitor','mark','probes','plans'])
    p.add_argument('--port',type=int,default=5498);p.add_argument('--case')
    p.add_argument('--pgdata');p.add_argument('--interval',type=float,default=.5)
    p.add_argument('--phase',type=int)
    args=p.parse_args()
    if args.action=='mark': mark(args.phase)
    elif args.action=='monitor': monitor(args)
    elif args.action=='probes': probes(args)
    elif args.action=='plans': plans(args)
    else:
        with connect(args.port) as c: print(json.dumps(snapshot(c),default=str))


if __name__=='__main__': main()
