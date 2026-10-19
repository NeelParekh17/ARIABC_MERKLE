"""Forward composite ordering, NULL/duplicate keys, bitmap and AM promises."""
import collections,json,os
from pathlib import Path
import psycopg
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')))
c=psycopg.connect(f'host={R}/socket port=5494 user=postgres dbname=postgres client_encoding=UTF8',autocommit=True)
def q(s,p=None):return c.execute(s,p)
q('DROP TABLE IF EXISTS capabilities');q('CREATE TABLE capabilities(a int,b text,tag text)')
q('CREATE INDEX cap_idx ON capabilities USING merkle_lsm(a DESC NULLS LAST,b ASC NULLS FIRST) INCLUDE(tag) WITH(levels=4,top_index_size=16,size_ratio=2)')
q('CREATE INDEX cap_tag ON capabilities USING merkle_lsm(tag) WITH(top_index_size=16,size_ratio=2)')
q("INSERT INTO capabilities SELECT CASE WHEN i%5=0 THEN NULL ELSE i%17 END,CASE WHEN i%7=0 THEN NULL ELSE 'b'||i%11 END,'row-'||i FROM generate_series(1,10000)i")
results={}
for stage in ['upper','drained']:
 if stage=='drained':
  q("SELECT merkle_lsm_flush('cap_idx')");q("SELECT merkle_lsm_flush('cap_tag')")
 q('SET enable_seqscan=on; SET enable_indexscan=off; SET enable_indexonlyscan=off; SET enable_bitmapscan=off')
 truth=collections.Counter(q('SELECT a,b,ctid::text,tag FROM capabilities').fetchall())
 ordered=q('SELECT a,b FROM capabilities ORDER BY a DESC NULLS LAST,b ASC NULLS FIRST').fetchall()
 for only in [False,True]:
  q('SET enable_seqscan=off; SET enable_indexscan=on; SET enable_indexonlyscan='+('on' if only else 'off'))
  if only:
   stmt='SELECT a,b FROM capabilities ORDER BY a DESC NULLS LAST,b ASC NULLS FIRST'
   assert q(stmt).fetchall()==ordered
   plan=q('EXPLAIN (COSTS OFF) '+stmt).fetchall();assert 'Index Only Scan' in str(plan),plan
  else:
   assert collections.Counter(q('SELECT a,b,ctid::text,tag FROM capabilities').fetchall())==truth
 for condition in ['a IS NULL','a=3 AND b IS NULL','a IN (1,2,7)','a BETWEEN 3 AND 9']:
  q('SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off')
  expected=collections.Counter(q('SELECT a,b,ctid::text,tag FROM capabilities WHERE '+condition).fetchall())
  for bitmap in [False,True]:
   q('SET enable_seqscan=off; SET enable_indexscan='+('off' if bitmap else 'on')+'; SET enable_bitmapscan='+('on' if bitmap else 'off'))
   assert collections.Counter(q('SELECT a,b,ctid::text,tag FROM capabilities WHERE '+condition).fetchall())==expected
 q('SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexscan=on')
 plan=q('EXPLAIN (COSTS OFF) SELECT a,b FROM capabilities ORDER BY a ASC NULLS FIRST,b DESC NULLS LAST').fetchall()
 assert 'Sort' in str(plan),plan
 results[stage]=dict(rows=sum(truth.values()),backward_plan=plan)
try:
 q('CREATE UNIQUE INDEX no_unique ON capabilities USING merkle_lsm(tag)')
 raise AssertionError('uniqueness incorrectly advertised')
except psycopg.errors.FeatureNotSupported:pass
q('DROP TABLE capabilities')
(R/'evidence/capabilities.json').write_text(json.dumps(dict(pass_check=True,results=results),indent=2)+'\n')
print('CAPABILITIES_PASS',flush=True)
