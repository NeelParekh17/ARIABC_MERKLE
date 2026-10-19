import os
# Run on the isolated lab host only; see tests/README.md.
import collections,json,time
from pathlib import Path
import psycopg
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')));O=R/'evidence'
c=psycopg.connect(f'host={R}/socket port=5494 user=postgres client_encoding=UTF8 dbname=postgres',autocommit=True)
def q(s,p=None):return c.execute(s,p)
def rec(label,**v):
 print(label,json.dumps(v,default=str),flush=True)
 with (O/'merkle_validation.jsonl').open('a') as f:f.write(json.dumps(dict(label=label,**v),default=str)+'\n')
def audit(label):
 roots=[]
 for schema in ['mb','m2','m3','m4']:
  table=schema+'.usertable'
  rows=q(f'SELECT merkle_tuple_hash(t.*) FROM {table} t').fetchall()
  expected=(sum(int.from_bytes(r[0],'little') for r in rows)%(1<<256)).to_bytes(32,'little').hex()
  root,verify,stats=q('SELECT merkle_root_hash(%s::regclass),merkle_verify(%s::regclass),merkle_tree_stats(%s::regclass)',(table,)*3).fetchone()
  assert root==expected and verify,(table,label,root,expected,verify)
  roots.append(root);rec(label,schema=schema,root=root,rows=len(rows),stats=stats)
 assert len(set(roots))==1
 # Split retrieval and repair COPY shapes: include unmerged upper-level rows.
 q('SET enable_seqscan=off; SET enable_hashjoin=off; SET enable_mergejoin=off')
 for part in range(4):
  for lo,hi in [('0000000000000000','ffffffffffffffff'),('4000000000000000','bfffffffffffffff')]:
   statement="SELECT ycsb_key,v FROM {s}.usertable WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),4)=%s AND merkle_key_hash(ycsb_key) BETWEEN decode(%s,'hex') AND decode(%s,'hex') ORDER BY merkle_key_hash(ycsb_key),ycsb_key"
   expected=q(statement.format(s='mb'),(part,lo,hi)).fetchall()
   for schema in ['m2','m3','m4']:
    got=q(statement.format(s=schema),(part,lo,hi)).fetchall()
    if got!=expected:
     rec('MISMATCH',schema=schema,part=part,lo=lo,hi=hi,missing=list((collections.Counter(expected)-collections.Counter(got)).elements()),extra=list((collections.Counter(got)-collections.Counter(expected)).elements()),got=got,expected=expected,plan=q('EXPLAIN (COSTS OFF) '+statement.format(s=schema),(part,lo,hi)).fetchall())
    assert got==expected
    plan=q('EXPLAIN (COSTS OFF) '+statement.format(s=schema),(part,lo,hi)).fetchall();assert 'usertable_merkle_lookup_idx' in str(plan),plan
 cte="WITH b(p,lo,hi) AS (SELECT u.p,u.lo,merkle_node_upper_bound(u.lo,u.l) FROM unnest(ARRAY[0,1]::int4[],ARRAY[decode('0000000000000000','hex'),decode('8000000000000000','hex')]::bytea[],ARRAY[1,1]::int4[]) u(p,lo,l)) "
 stmt="SELECT DISTINCT ON (t.ycsb_key) t.* FROM b JOIN {s}.usertable t ON merkle_partition_for_hash(merkle_key_hash(t.ycsb_key),4)=b.p AND merkle_key_hash(t.ycsb_key) BETWEEN b.lo AND b.hi ORDER BY t.ycsb_key"
 copies=[]
 for schema in ['mb','m2','m3','m4']:
  plan=q('EXPLAIN (COSTS OFF) '+cte+stmt.format(s=schema)).fetchall();assert 'usertable_merkle_lookup_idx' in str(plan),plan
  with c.cursor().copy('COPY ('+cte+stmt.format(s=schema)+') TO STDOUT') as cp:copies.append(b''.join(bytes(row) for row in cp))
  rec(label+'_repair_plan',schema=schema,plan=plan)
 assert len(set(copies))==1
 q('RESET enable_seqscan; RESET enable_hashjoin; RESET enable_mergejoin')
for schema,am,level in [('mb','btree',3),('m2','merkle_lsm',2),('m3','merkle_lsm',3),('m4','merkle_lsm',4)]:
 q(f'DROP SCHEMA IF EXISTS {schema} CASCADE');q(f'CREATE SCHEMA {schema}')
 q(f'CREATE TABLE {schema}.usertable(ycsb_key bigint PRIMARY KEY,v text) WITH (fillfactor=70)')
 q(f'CREATE INDEX usertable_merkle_idx ON {schema}.usertable USING merkle(ycsb_key) WITH(partitions=4,fanout=4,split_threshold=8,merge_threshold=2)')
 q('SELECT merkle_create_lookup_index(%s::regclass,%s,%s,16,2)',(schema+'.usertable',am,level))
for n in range(6):
 for schema in ['mb','m2','m3','m4']:
  q(f"INSERT INTO {schema}.usertable SELECT i,repeat('x',80) FROM generate_series({n*128+1},{(n+1)*128}) i")
 audit('insert_'+str(n))
for operation in ["UPDATE {s}.usertable SET v=repeat('z',90) WHERE ycsb_key%3=0",'UPDATE {s}.usertable SET ycsb_key=ycsb_key+10000 WHERE ycsb_key%7=0','DELETE FROM {s}.usertable WHERE ycsb_key%4<>0']:
 for schema in ['mb','m2','m3','m4']:q(operation.format(s=schema))
 audit(operation.split()[0])
for schema in ['m2','m3','m4']:q('SELECT merkle_lsm_flush(%s::regclass)',(schema+'.usertable_merkle_lookup_idx',))
audit('drained')
# Leading-key-routed helper variant.
q('DROP TABLE IF EXISTS routed');q('CREATE TABLE routed(a int,b int,v text)')
q('CREATE INDEX routed_merkle_idx ON routed USING merkle(a,b) WITH(partitions=4,partition_key_columns=1,subpartitions=2,fanout=4,split_threshold=8,merge_threshold=2)')
q("SELECT merkle_create_lookup_index('routed')")
q("INSERT INTO routed SELECT i%2,i,'r' FROM generate_series(1,400)i")
assert q("SELECT merkle_verify('routed')").fetchone()[0]
rec('PASS')
