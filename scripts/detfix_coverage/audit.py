#!/usr/bin/env python3
"""Remote-only serial-oracle checks; each run owns its postmaster and data."""
import argparse, ctypes as C, ctypes.util, hashlib, json, os, subprocess as S
import threading, time, traceback
from pathlib import Path

ROOT = Path(os.environ.get('DETFIX_ROOT', str(Path(__file__).resolve().parent)))
if os.uname().nodename != 'Neel':
    raise SystemExit('Remote lab host only')
INST = ROOT / 'install'

class PG:
    def __init__(self, port):
        self.lib = C.CDLL(str(INST / 'lib/libpq.so'))
        signatures = {
          'PQconnectdb':(C.c_void_p,[C.c_char_p]),'PQstatus':(C.c_int,[C.c_void_p]),
          'PQerrorMessage':(C.c_char_p,[C.c_void_p]),'PQfinish':(None,[C.c_void_p]),
          'PQbackendPID':(C.c_int,[C.c_void_p]),
          'PQexec':(C.c_void_p,[C.c_void_p,C.c_char_p]),'PQresultStatus':(C.c_int,[C.c_void_p]),
          'PQntuples':(C.c_int,[C.c_void_p]),'PQnfields':(C.c_int,[C.c_void_p]),
          'PQgetvalue':(C.c_char_p,[C.c_void_p,C.c_int,C.c_int]),
          'PQgetisnull':(C.c_int,[C.c_void_p,C.c_int,C.c_int]),
          'PQresultErrorField':(C.c_char_p,[C.c_void_p,C.c_int]),
          'PQresultErrorMessage':(C.c_char_p,[C.c_void_p]),'PQcmdStatus':(C.c_char_p,[C.c_void_p]),
          'PQclear':(None,[C.c_void_p])}
        for name,(restype,args) in signatures.items():
            f=getattr(self.lib,name);f.restype=restype;f.argtypes=args
        self.conn=self.lib.PQconnectdb(f'host=127.0.0.1 port={port} dbname=postgres user=postgres connect_timeout=5'.encode())
        if self.lib.PQstatus(self.conn)!=0: raise RuntimeError(self.lib.PQerrorMessage(self.conn).decode())
    def query(self, sql):
        r=self.lib.PQexec(self.conn,sql.encode())
        if not r: raise RuntimeError(self.lib.PQerrorMessage(self.conn).decode())
        try:
            l=self.lib;v=l.PQresultErrorField(r,ord('C'))
            return {'status':l.PQresultStatus(r),'sqlstate':v.decode() if v else None,
              'tag':l.PQcmdStatus(r).decode(),
              'rows':[[None if l.PQgetisnull(r,i,j) else l.PQgetvalue(r,i,j).decode()
                       for j in range(l.PQnfields(r))] for i in range(l.PQntuples(r))],
              'error':l.PQresultErrorMessage(r).decode()}
        finally:self.lib.PQclear(r)
    def close(self):self.lib.PQfinish(self.conn)

def native(run,workers,port):
    sql=(run/'workload.sql').read_text().splitlines()
    lock=threading.Lock();barrier=threading.Barrier(workers)
    out=(run/'det_results.jsonl').open('w',buffering=1)
    errors=[]
    def lane(k):
        try:
            p=PG(port)
            with lock:
                with (run/'backend_pids.jsonl').open('a') as f:f.write(json.dumps({'lane':k,'pid':p.lib.PQbackendPID(p.conn)})+'\n')
            barrier.wait(timeout=10)
            time.sleep(float(os.environ.get('AUDIT_START_DELAY','0')))
            for seq in range(k,len(sql),workers):
                started=time.monotonic()
                q=sql[seq]
                if (run/'signed').exists():
                    import base64
                    signature=S.check_output(['openssl','dgst','-sha256','-sign',str(ROOT/'test_private.pem')],input=q.encode())
                    q=base64.b64encode(signature).decode()+'##'+q
                value=p.query(f's {seq:08d} {q}')
                value.update(seq=seq,seconds=time.monotonic()-started)
                with lock:out.write(json.dumps(value)+'\n')
            p.close()
        except Exception:
            with lock:errors.append(traceback.format_exc())
    ts=[threading.Thread(target=lane,args=(i,)) for i in range(workers)]
    for t in ts:t.start()
    for t in ts:t.join()
    out.close()
    if errors:raise RuntimeError('\n'.join(errors))

SCHEMA = '''
CREATE TABLE data(id int PRIMARY KEY, email int NOT NULL UNIQUE, v bigint NOT NULL);
CREATE TABLE obs(id int PRIMARY KEY, v bigint);
CREATE TABLE other(id int PRIMARY KEY, v bigint);
CREATE OR REPLACE FUNCTION put_proc(i int, delay real DEFAULT 0) RETURNS void LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(delay); INSERT INTO data VALUES(i,i,i*17); END $$;
CREATE OR REPLACE FUNCTION read_proc(i int,k int,mode int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE x bigint;
BEGIN
 IF mode=0 THEN SELECT v INTO x FROM data WHERE id=k;
 ELSIF mode=1 THEN SELECT v INTO x FROM data WHERE email=k;
 ELSIF mode=2 THEN SELECT count(*) INTO x FROM data WHERE id BETWEEN k AND k+1;
 ELSIF mode=3 THEN SELECT count(*) INTO x FROM data;
 ELSIF mode=4 THEN SELECT count(*) INTO x FROM data WHERE id>=0 AND id<=k;
 END IF;
 x:=coalesce(x,-1); INSERT INTO obs VALUES(i,x); RETURN x;
END $$;
CREATE OR REPLACE FUNCTION fold_proc(i int,k int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE x bigint;
BEGIN SELECT v INTO x FROM data WHERE id=k; UPDATE data SET v=(v*31+i)%1000000007 WHERE id=k;
 INSERT INTO obs VALUES(i,x); RETURN x; END $$;
CREATE OR REPLACE FUNCTION own_proc(i int,mode int) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE x bigint;
BEGIN
 IF mode=0 THEN INSERT INTO data VALUES(i,i,i*17); SELECT v INTO x FROM data WHERE id=i;
 ELSIF mode=1 THEN UPDATE data SET v=v+1 WHERE id=0; SELECT v INTO x FROM data WHERE id=0;
 ELSIF mode=2 THEN UPDATE data SET v=v+1 WHERE id=0; UPDATE data SET v=v+1 WHERE id=0; SELECT v INTO x FROM data WHERE id=0;
 ELSIF mode=3 THEN DELETE FROM data WHERE id=0; SELECT count(*) INTO x FROM data WHERE id=0;
 END IF;
 INSERT INTO obs VALUES(i,x); RETURN x; END $$;
CREATE OR REPLACE FUNCTION move_proc(i int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN UPDATE data SET id=id+1,email=email+1 WHERE id=i; END $$;
'''

def workload(case,n=128):
    schema=SCHEMA;sql=[];extra={};env={};plan=None
    modes={'primary_empty':0,'secondary_empty':1,'range_empty':2,'aggregate_phantom':3,'range_existing':4}
    if case in modes:
        mode=modes[case]
        if case=='range_existing':schema+='INSERT INTO data SELECT g,g,g*17 FROM generate_series(0,15)g;'
        for i in range(n):
            k=1000+i*2
            sql += [f'SELECT put_proc({k},{float(os.environ.get("AUDIT_DELAY","0.002"))});',f'SELECT read_proc({i},{k},{mode});']
        plan=['SELECT v FROM data WHERE id=1000','SELECT v FROM data WHERE email=1000',
              'SELECT count(*) FROM data WHERE id BETWEEN 1000 AND 1001',
              'SELECT count(*) FROM data','SELECT count(*) FROM data WHERE id>=0 AND id<=1000'][mode]
    elif case in ('secondary_existing','secondary_indexonly','primary_indexonly'):
        schema+=f'INSERT INTO data SELECT g,g,1 FROM generate_series(0,{n-1})g;'
        mode=0 if case=='primary_indexonly' else 1
        col='id' if mode==0 else 'email'
        schema+=f'CREATE INDEX data_cover ON data({col}) INCLUDE(v);'
        if case!='secondary_existing':extra['enable_indexonlyscan']='on'
        for i in range(n):sql += [f'UPDATE data SET v={i*17} WHERE id={i};',f'SELECT read_proc({i},{i},{mode});']
        plan=f'SELECT v FROM data WHERE {col}=0'
    elif case in ('fold_hot','settle_forced','rotation','digest_overflow','digest_wrap'):
        schema+='INSERT INTO data SELECT g,g,g+1 FROM generate_series(0,15)g;'
        if case=='digest_overflow':
            schema+='INSERT INTO other SELECT g,0 FROM generate_series(0,599)g;'
            sql=[f'UPDATE other SET v=v+1 WHERE id BETWEEN {i%2*300} AND {i%2*300+299};' for i in range(128)]
        else:
            count=10000 if case=='digest_wrap' else 512
            sql=[f'SELECT fold_proc({i},{i%8});' for i in range(count)]
        if case=='settle_forced':env['BCDB_FAILPOINT_POST_PUBLISH_APPLY']='7'
        if case=='rotation':env['BCDB_DT_EARLY_ROTATION']='1';extra['bcdb_dt_hashtab_switch_threshold']='64'
    elif case in ('own_insert_read','own_update_read','own_update_twice','own_delete_read'):
        mode=['own_insert_read','own_update_read','own_update_twice','own_delete_read'].index(case)
        if mode:schema+='INSERT INTO data VALUES(0,0,1);'
        sql=[f'SELECT own_proc({1000+i},{mode});' for i in range(n if mode<3 else 1)]
    elif case in ('unique_errors','onconflict_multi','onconflict_update'):
        schema+='INSERT INTO data VALUES(0,0,1);'
        if case=='unique_errors':sql=[f'INSERT INTO data VALUES({1000+i},0,{i});' for i in range(n)]
        elif case=='onconflict_multi':sql=[f'INSERT INTO data VALUES(0,0,99),({1000+i},{1000+i},{i}) ON CONFLICT DO NOTHING;' for i in range(n)]
        else:sql=[f'INSERT INTO data VALUES(0,0,1) ON CONFLICT(id) DO UPDATE SET v=data.v+excluded.v;' for _ in range(n)]
    elif case=='key_updates':
        schema+='INSERT INTO data VALUES(0,0,1);'
        sql=[f'SELECT move_proc({i});' for i in range(n)]
    elif case=='check_errors':
        schema+='ALTER TABLE data ADD CHECK(v>=0);'
        sql=[f'INSERT INTO data VALUES({i},{i},{-1 if i%2 else i});' for i in range(n)]
    elif case=='foreign_key':
        schema+='ALTER TABLE data ADD FOREIGN KEY(email) REFERENCES other(id);'
        sql=[f'INSERT INTO data VALUES({i},{i},{i});' for i in range(16)]
    elif case=='trigger':
        schema+='''CREATE OR REPLACE FUNCTION tr_proc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO obs VALUES(NEW.id,NEW.v); RETURN NEW; END $$;
          CREATE TRIGGER tr AFTER INSERT ON data FOR EACH ROW EXECUTE FUNCTION tr_proc();'''
        sql=[f'INSERT INTO data VALUES({i},{i},{i*17});' for i in range(n)]
    elif case=='rel_digest_overflow':
        schema+='CREATE OR REPLACE FUNCTION put_many(i int) RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.002); INSERT INTO other SELECT g,1 FROM generate_series(i*600,i*600+599)g; END $$;'
        schema+='CREATE OR REPLACE FUNCTION count_many(i int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; BEGIN SELECT count(*) INTO x FROM other WHERE id BETWEEN i*600 AND i*600+599; INSERT INTO obs VALUES(i,x); RETURN x; END $$;'
        for i in range(32):sql += [f'SELECT put_many({i});',f'SELECT count_many({i});']
        plan='SELECT count(*) FROM other WHERE id BETWEEN 0 AND 599'
    elif case=='aggregate_rotation':
        extra['bcdb_dt_hashtab_switch_threshold']='64';env['BCDB_DT_EARLY_ROTATION']='1'
        for i in range(n):sql += [f'SELECT put_proc({1000+i},0.002);',f'SELECT read_proc({i},0,3);']
        plan='SELECT count(*) FROM data'
    elif case=='volatile_random_retry':
        schema+='INSERT INTO data VALUES(0,0,1);'
        schema+='CREATE OR REPLACE FUNCTION random_fold(i int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; k bigint; BEGIN SELECT v INTO x FROM data WHERE id=0; k:=floor(random()*1000000000)::bigint; UPDATE data SET v=v+1 WHERE id=0; INSERT INTO obs VALUES(i,k+x*1000000000); RETURN k; END $$;'
        sql=[f'SELECT random_fold({i});' for i in range(n)]
    elif case=='volatile_random':
        sql=[f'INSERT INTO obs VALUES({i},floor(random()*1000000000)::bigint);' for i in range(16)]
    elif case=='sequence_retry':
        schema+='''INSERT INTO data VALUES(0,0,1); DROP SEQUENCE IF EXISTS audit_seq; CREATE SEQUENCE audit_seq;
          CREATE OR REPLACE FUNCTION seq_proc() RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; k bigint;
          BEGIN SELECT v INTO x FROM data WHERE id=0; UPDATE data SET v=v+1 WHERE id=0;
          k:=nextval('audit_seq'); INSERT INTO obs VALUES(k,x); RETURN k; END $$;'''
        sql=['SELECT seq_proc();' for _ in range(256)]
    elif case in ('long_control','long_sql'):
        sql=['SELECT 1 /*'+('x'*(980 if case=='long_control' else 2048))+'*/;']
    elif case in ('seq_reads','seq_prefix','hash_reads','hash_bitmap','key_old_reader','key_old_prefix','btree_bitmap'):
        if case.startswith('seq_'):
            extra.update(enable_seqscan='on',enable_indexscan='off',enable_bitmapscan='off')
        if case=='btree_bitmap':extra.update(enable_seqscan='off',enable_bitmapscan='on',enable_indexscan='off')
        if case.startswith('hash_'):
            schema+='ALTER TABLE data DROP CONSTRAINT data_email_key; CREATE INDEX data_hash ON data USING hash(email);'
            extra.update(enable_seqscan='off',enable_bitmapscan='on' if case=='hash_bitmap' else 'off',enable_indexscan='off' if case=='hash_bitmap' else 'on')
        if case.startswith('key_old'):
            if case=='key_old_prefix':schema+='ALTER TABLE data DROP CONSTRAINT data_pkey; ALTER TABLE data ADD PRIMARY KEY(id,email);'
            schema+=f'INSERT INTO data SELECT g,g,g*17 FROM generate_series(0,{n-1})g;'
            for i in range(n):sql += [f'UPDATE data SET id=id+1000 WHERE id={i};',f'SELECT read_proc({i},{i},0);']
            plan='SELECT v FROM data WHERE id=0'
        else:
            mode=3 if case=='seq_reads' else (1 if case.startswith('hash_') else 0)
            for i in range(n):sql += [f'SELECT put_proc({1000+i},0.002);',f'SELECT read_proc({i},{1000+i},{mode});']
            plan=['SELECT v FROM data WHERE id=1000','SELECT v FROM data WHERE email=1000','','SELECT count(*) FROM data'][mode]
    elif case in ('long_2k','long_64k','signed_2k','signed_64k'):
        size=2048 if case.endswith('2k') else 65536
        sql=['SELECT 123 /*'+('x'*size)+'*/ + 456;']
        if case.startswith('signed_'):extra['bcdb_client_public_key']="'"+(ROOT/'test_public.b64').read_text().strip()+"'"
    else:raise ValueError(case)
    return schema,sql,extra,env,plan

def command(args,log,**kw):
    with log.open('w') as f:return S.run([str(x) for x in args],stdout=f,stderr=S.STDOUT,check=True,**kw)

def rows(p):
    q="SELECT 'data' t,row_to_json(d)::text r FROM data d UNION ALL SELECT 'obs',row_to_json(o)::text FROM obs o UNION ALL SELECT 'other',row_to_json(x)::text FROM other x ORDER BY 1,2"
    r=p.query(q)
    if r['status']!=2:raise RuntimeError(r)
    return r['rows']

def run_case(case,w,rep,variant='default',integration=False):
    label=f'{case}_w{w}_r{rep}_{variant}'+('_gateway' if integration else '')
    run=ROOT/'runs'/label;run.mkdir(parents=True,exist_ok=False)
    port=55491;schema,sql,gucs,env,plan=workload(case,int(os.environ.get('AUDIT_PAIRS','128')))
    if variant.startswith('nodelay'):schema=schema.replace('PERFORM pg_sleep(delay);','')
    if variant=='noev':env['BCDB_DT_EARLY_VALIDATE']='0'
    if variant=='commit':gucs['bcdb_serial_gate_source']='1'
    if variant=='nolook':env['BCDB_DT_LOOKAHEAD_WAIT']='0'
    if case.startswith('signed_'):(run/'signed').touch()
    (run/'schema.sql').write_text(schema)
    (run/'workload.sql').write_text('\n'.join(sql)+'\n')
    settings={'port':str(port),'listen_addresses':"'127.0.0.1'",'unix_socket_directories':"'"+str(ROOT)+"'",
     'shared_buffers':"'128MB'",'max_connections':'100','autovacuum':'off','fsync':'on','synchronous_commit':'on',
     'default_transaction_isolation':"'serializable'",'bcdb_worker_count':str(w),'enable_merkle_index':'off',
     'bcdb_serial_gate_mode':'1','bcdb_serial_gate_source':'0','bcdb_dt_conflict_tracking':'on',
     'bcdb_result_ring_slots':'2048','bcdb_dt_completion_only_skip_reads':'off',
     'bcdb_dt_hashtab_switch_threshold':'1500','bcdb_advance_commit_watermark':'on',
     'enable_seqscan':'off','enable_indexonlyscan':'off','max_parallel_workers_per_gather':'0','log_min_messages':'warning'}
    settings.update(gucs)
    (run/'settings.json').write_text(json.dumps({'gucs':settings,'env':env,'case':case,'workers':w,'variant':variant},indent=2))
    pgdata=run/'pgdata';started=False;server=None;p=None
    summary={'run':label,'case':case,'workers':w,'rep':rep,'variant':variant,'integration':integration,'requested':len(sql)}
    begin=time.monotonic()
    try:
        base=ROOT/'pristine_pgdata'
        if base.exists():
            command(['cp','-a','--reflink=never',base,pgdata],run/'initdb.log')
        else:
            command([INST/'bin/initdb','-D',pgdata,'-U','postgres','--no-locale','-E','UTF8'],run/'initdb.log')
        with (pgdata/'postgresql.conf').open('a') as f:f.write('\n'+'\n'.join(k+' = '+v for k,v in settings.items())+'\n')
        penv=dict(os.environ,**env,BCDB_PHASE_TRACE=str(run/'ptrace'))
        command([INST/'bin/pg_ctl','-D',pgdata,'-l',run/'postgres.log','-w','-t','30','start'],run/'start.log',env=penv)
        started=True;p=PG(port)
        r=p.query(schema)
        if r['status'] not in (1,2):raise RuntimeError(r)
        if case in ('secondary_existing','secondary_indexonly','primary_indexonly'):p.query('VACUUM ANALYZE data;')
        if plan:(run/'plan.json').write_text(json.dumps(p.query('EXPLAIN (FORMAT JSON) '+plan),indent=2))
        (run/'effective_settings.json').write_text(json.dumps(p.query("SELECT name,setting FROM pg_settings WHERE name LIKE 'bcdb_%' OR name IN ('default_transaction_isolation','fsync','synchronous_commit','enable_seqscan','enable_indexonlyscan') ORDER BY name"),indent=2))
        serial=[]
        with (run/'serial_results.jsonl').open('w') as f:
            for i,q in enumerate(sql):
                if case.startswith('volatile_random'):
                    seed=i ^ 0xBCDB13579BDF
                    if seed>=2**47:seed-=2**48
                    # Pick a float whose setseed conversion recreates all 48 bits.
                    v=seed/float(0x7FFFFFFFFFFF)
                    import math
                    while int(v*float(0x7FFFFFFFFFFF))!=seed:
                        v=math.nextafter(v, -1.0 if seed<0 else 1.0)
                    p.query(f'SELECT setseed({v!r});')
                value=p.query(q);value['seq']=i;serial.append(value);f.write(json.dumps(value)+'\n')
        ref=rows(p);(run/'serial_rows.json').write_text(json.dumps(ref,indent=2)+'\n')
        summary['serial_errors']=sum(v['status'] not in (1,2) for v in serial)
        r=p.query('DROP TABLE data,obs,other CASCADE;'+schema)
        if r['status'] not in (1,2):raise RuntimeError(r)
        if case in ('secondary_existing','secondary_indexonly','primary_indexonly'):p.query('VACUUM ANALYZE data;')
        if integration:
            senv=dict(os.environ,BCDB_DECOUPLE_WORKERS='1',BCDB_DET_QUEUE_HIGH_WM='65536',BCDB_DET_QUEUE_LOW_WM='32768',
              ARIABC_DET_BLOCK_PARALLEL='64',ARIABC_DET_BLOCK_PIPELINE='4',ARIABC_DET_BLOCK_MAX='2048',
              ARIABC_DET_ORDER_START_SEQ='0',ARIABC_DET_PREFIXED_DIRECT_PARALLEL='1')
            sf=(run/'server.log').open('w')
            server=S.Popen([str(ROOT/'bin/ariabc_pg_server'),'--id','1','--raftEndpoint','127.0.0.1:19291','--clientPort','18291',
                '--raftMembers','1=127.0.0.1:19291','--dbName','postgres','--dbHost','127.0.0.1','--dbPort',str(port),'--dbUser','postgres',
                '--dbType','1','--safedb','1','--dbConnPoolSize',str(w),'--bcdbInitBlockSize',str(w),'--pgExecMode','event','--bypassRaft','1'],
                stdout=sf,stderr=S.STDOUT,cwd=run,env=senv)
            import socket
            for _ in range(100):
                try:
                    with socket.create_connection(('127.0.0.1',18291),.1):break
                except OSError:time.sleep(.1)
            args=[ROOT/'bin/ariabc_pg_gateway','--nodes','127.0.0.1:18291','--queryFrom',run/'workload.sql','--dbType','1',
             '--detStartSeq','0','--reqIdOffset','1','--detWindow','1024','--detBatchSize','256','--dbConnPoolSize',str(w),
             '--submitMode','event','--detSubmitPipeline','1','--detPipelineDepth','1024','--detClientMode','event',
             '--detClientWorkers','8','--detClientInflight','16','--clientId','det-audit','--numTerminals','8','--connFanout','1',
             '--waitMajority','0','--completionPath','direct','--totalNodes','1']
            command(args,run/'gateway.log',env=dict(os.environ,ARIABC_WAIT_RESULT_TIMEOUT_MS='15000'),timeout=45)
        else:
            args=['python3',__file__,'--native',str(run),'--workers',str(w),'--port',str(port)]
            with (run/'client.log').open('w') as f:
                child=S.Popen(args,stdout=f,stderr=S.STDOUT)
                try:
                    try:child.wait(timeout=2)
                    except S.TimeoutExpired:
                        activity=p.query("SELECT json_build_object('pid',pid,'state',state,'wait_type',wait_event_type,'wait',wait_event,'blocking',pg_blocking_pids(pid),'query',query) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()")
                        (run/'waiting_sessions.json').write_text(json.dumps(activity,indent=2)+'\n')
                        child.wait(timeout=float(os.environ.get('AUDIT_TIMEOUT','45'))-2)
                    if child.returncode:raise S.CalledProcessError(child.returncode,args)
                finally:
                    if child.poll() is None:child.kill();child.wait()
        actual=rows(p);(run/'det_rows.json').write_text(json.dumps(actual,indent=2)+'\n')
        summary['state_equal']=actual==ref
        summary['serial_sha256']=hashlib.sha256(json.dumps(ref).encode()).hexdigest()
        summary['det_sha256']=hashlib.sha256(json.dumps(actual).encode()).hexdigest()
        summary['serial_rows']=len(ref);summary['det_rows']=len(actual)
        if not integration:
            responses=[json.loads(x) for x in (run/'det_results.jsonl').read_text().splitlines()]
            summary['completed']=len(responses)
            summary['det_error_statuses']=sum(v['status'] not in (1,2) for v in responses)
            indexed={v['seq']:v for v in responses}
            mismatches=[]
            for v in serial:
                d=indexed.get(v['seq'])
                # Error message text includes process/context details; compare SQLSTATE.
                keys=['status','sqlstate'] if v['sqlstate'] else ['status','sqlstate','tag','rows']
                if d is None or any(v[k]!=d[k] for k in keys):mismatches.append(v['seq'])
            summary['result_mismatches']=len(mismatches)
            summary['first_result_mismatches']=mismatches[:16]
        else:
            import re
            gl=(run/'gateway.log').read_text()
            summary['gateway_completion_lines']=re.findall(r'.*(?:completed=|permanent_failures=|divergence_count=).*',gl)[-10:]
        summary['status']='PASS' if summary['state_equal'] and not summary.get('result_mismatches') else 'MISMATCH'
    except Exception as e:
        summary.update(status='ERROR',error=str(e),traceback=traceback.format_exc())
        if p:
            try:(run/'det_rows.json').write_text(json.dumps(rows(p),indent=2))
            except Exception:pass
    finally:
        if p:p.close()
        if server:
            server.terminate()
            try:server.wait(timeout=3)
            except S.TimeoutExpired:server.kill();server.wait()
        if started or (pgdata/'postmaster.pid').exists():
            try:command([INST/'bin/pg_ctl','-D',pgdata,'-m','fast','-w','-t','15','stop'],run/'stop.log',timeout=20)
            except Exception:summary['stop_error']=traceback.format_exc()
        import csv
        counters={}
        for trace in run.glob('ptrace*'):
            if trace.is_file():
                with trace.open() as f:
                    for row in csv.DictReader(f):
                        for k,v in row.items():
                            if k and (k.startswith('dt_') or k in ('ring_fallbacks','post_publish_settles','early_conflict_hits','turn_conflict_hits')):
                                counters[k]=counters.get(k,0)+int(v)
        summary['counters']=counters
        summary['seconds']=time.monotonic()-begin
        (run/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        with (ROOT/'results.jsonl').open('a') as f:f.write(json.dumps(summary)+'\n')
        print(json.dumps({k:summary[k] for k in ('run','status','requested','completed','state_equal','result_mismatches','seconds') if k in summary}),flush=True)
    return summary

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--native');a.add_argument('--workers',type=int,default=8)
    a.add_argument('--port',type=int,default=55491);a.add_argument('--cases',default='primary_empty,secondary_empty,range_empty')
    a.add_argument('--reps',type=int,default=1);a.add_argument('--variant',default='default');a.add_argument('--integration',action='store_true')
    x=a.parse_args()
    if x.native:native(Path(x.native),x.workers,x.port)
    else:
        for c in x.cases.split(','):
            for r in range(1,x.reps+1):run_case(c,x.workers,r,x.variant,x.integration)
