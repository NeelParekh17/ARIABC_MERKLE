import hashlib,json,os,signal,subprocess as S,psycopg
from pathlib import Path
r=Path('/home/neel/claude_checks/lsm_lookup_20261014');o=r/'small_final'
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
c.execute("SELECT lsm3_wait_merge_completion('dl.usertable_merkle_lookup_idx')")
expected={i:('changed' if i<=500 else 'det-compaction') for i in range(1,70001) if not 501<=i<=1500}
for i in range(1001,1501):expected[i+100000]='det-compaction'
roots={}
for schema in ['db','dl']:
 actual=dict(c.execute(f'SELECT ycsb_key,v FROM {schema}.usertable').fetchall());assert actual==expected,(schema,len(actual),len(expected))
 vals=c.execute(f'SELECT merkle_tuple_hash(u.*) FROM {schema}.usertable u').fetchall();oracle=(sum(int.from_bytes(v[0],'little') for v in vals)%(1<<256)).to_bytes(32,'little').hex()
 root,verify=c.execute(f"SELECT merkle_root_hash('{schema}.usertable'),merkle_verify('{schema}.usertable')").fetchone();assert root==oracle and verify
 roots[schema]=dict(rows=len(actual),root=root,oracle=oracle,verify=verify)
 c.execute(f'ANALYZE {schema}.usertable')
assert roots['db']==roots['dl']
c.execute('SET enable_seqscan=off');c.execute('SET enable_bitmapscan=off');c.execute('SET enable_indexonlyscan=off')
parts=[];total=0
for part in range(200):
 ans={};plans={}
 for schema in ['db','dl']:
  stmt=f"SELECT ycsb_key,v FROM {schema}.usertable WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),200)={part} AND merkle_key_hash(ycsb_key) BETWEEN decode('0000000000000000','hex') AND decode('ffffffffffffffff','hex') ORDER BY merkle_key_hash(ycsb_key),ycsb_key"
  ans[schema]=c.execute(stmt).fetchall()
  if part<2:plans[schema]=c.execute('EXPLAIN(COSTS OFF) '+stmt).fetchall();assert 'usertable_merkle_lookup_idx' in str(plans[schema])
 assert ans['db']==ans['dl'],part;total+=len(ans['dl']);parts.append(dict(partition=part,rows=len(ans['dl']),sha256=hashlib.sha256(json.dumps(ans['dl']).encode()).hexdigest(),plans=plans))
assert total==len(expected)
merges,size=c.execute("SELECT lsm3_get_merge_count('dl.usertable_merkle_lookup_idx'),lsm3_top_index_size('dl.usertable_merkle_lookup_idx')").fetchone();assert merges>=1
(o/'final_audit.json').write_text(json.dumps(dict(pass_check=True,roots=roots,range_rows=total,ranges=parts,merge_count=merges,active_top_bytes=size),indent=2)+'\n')
pid=int((o/'det_server.pid').read_text());assert str(r/'cluster/ariabc_pg/build/bin/ariabc_pg_server') in Path(f'/proc/{pid}/cmdline').read_text();os.kill(pid,signal.SIGTERM);c.close()
S.run([str(r/'install/bin/pg_ctl'),'-D','/home/neel/ariabc_data/lsm_lookup_20261014/small_final','-m','fast','-w','stop'],check=True)
(o/'status.txt').write_text('LATEST_BINARY_SQL_DET_COMPACTION_MUTATIONS_RANGES_PASS\n')
