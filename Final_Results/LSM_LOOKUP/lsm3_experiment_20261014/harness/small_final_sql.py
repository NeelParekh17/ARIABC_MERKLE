#!/usr/bin/env python3
"""Run on .247 only; compare LSM scans to heap scans and B-tree roots."""
import json, os, time
from pathlib import Path
import psycopg
ROOT=Path('/home/neel/claude_checks/lsm_lookup_20261014')
OUT=ROOT/'small_final';MOD=1<<256
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
def q(s,p=None): return c.execute(s,p)
def record(name,value):
 with (OUT/'checks.jsonl').open('a') as f:f.write(json.dumps(dict(test=name,result=value),default=str)+'\n')
 print(name,json.dumps(value,default=str),flush=True)
def audit(schema):
 table=schema+'.usertable'
 rows=q(f'SELECT merkle_tuple_hash(u.*) FROM {table} u').fetchall()
 expected=sum(int.from_bytes(r[0],'little') for r in rows)%MOD
 actual=q('SELECT merkle_root_hash(%s::regclass),merkle_verify(%s::regclass),merkle_tree_stats(%s::regclass)',(table,table,table)).fetchone()
 assert actual[0]==expected.to_bytes(32,'little').hex(),(schema,actual,expected)
 assert actual[1] is True
 return dict(rows=len(rows),root=actual[0],verify=actual[1],geometry=actual[2])
def pair(label):
 b,l=audit('b'),audit('l');assert b['root']==l['root'] and b['rows']==l['rows'];record(label,dict(btree=b,lsm=l))
def ranges(label):
 q('SET enable_seqscan=off');q('SET enable_bitmapscan=off');q('SET enable_indexonlyscan=off')
 for part in range(4):
  for lo,hi in [('0000000000000000','ffffffffffffffff'),('4000000000000000','bfffffffffffffff')]:
   template="SELECT ycsb_key,v FROM {schema}.usertable WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),4)=%s AND merkle_key_hash(ycsb_key) BETWEEN decode(%s,'hex') AND decode(%s,'hex') ORDER BY merkle_key_hash(ycsb_key),ycsb_key"
   bs=q(template.format(schema='b'),(part,lo,hi)).fetchall();ls=q(template.format(schema='l'),(part,lo,hi)).fetchall();assert bs==ls
   plan=q('EXPLAIN (COSTS OFF) '+template.format(schema='l'),(part,lo,hi)).fetchall()
   assert 'usertable_merkle_lookup_idx' in str(plan),plan
   record(label+'_range',dict(part=part,lo=lo,hi=hi,rows=len(ls),plan=plan))
 q('SET enable_seqscan=on');q('SET enable_bitmapscan=on');q('SET enable_indexonlyscan=on')
for schema,am in [('b','btree'),('l','lsm3')]:
 q(f'CREATE SCHEMA {schema}');q(f'CREATE TABLE {schema}.usertable(ycsb_key bigint PRIMARY KEY,v text) WITH (fillfactor=100)')
 q(f"INSERT INTO {schema}.usertable SELECT i,repeat('x',80) FROM generate_series(1,512) i")
 q(f'CREATE INDEX usertable_merkle_lookup_idx ON {schema}.usertable USING {am}(merkle_partition_for_hash(merkle_key_hash(ycsb_key),4),merkle_key_hash(ycsb_key),ycsb_key)')
 q(f'CREATE INDEX usertable_merkle_idx ON {schema}.usertable USING merkle(ycsb_key) WITH (partitions=4,fanout=4,split_threshold=8,merge_threshold=2)')
pair('initial_split');ranges('base_only')
for op in ["INSERT INTO {s}.usertable SELECT i,repeat('y',80) FROM generate_series(513,768) i", "UPDATE {s}.usertable SET v=repeat('z',90) WHERE ycsb_key%3=0", "DELETE FROM {s}.usertable WHERE ycsb_key%4<>0"]:
 for schema in ['b','l']:q(op.format(s=schema))
 pair(op.split()[0]);ranges(op.split()[0])
record('before_merge',q("SELECT lsm3_get_merge_count('l.usertable_merkle_lookup_idx'),lsm3_top_index_size('l.usertable_merkle_lookup_idx')").fetchone())
q("SELECT lsm3_start_merge('l.usertable_merkle_lookup_idx')")
q("SELECT lsm3_wait_merge_completion('l.usertable_merkle_lookup_idx')")
pair('after_explicit_merge');ranges('after_explicit_merge')
q('CREATE TABLE stress(k bigint, v text) WITH (fillfactor=100)')
q('CREATE INDEX stress_lsm ON stress USING lsm3(k) WITH (top_index_size=1024)')
q("INSERT INTO stress SELECT i,repeat('s',20) FROM generate_series(1,150000) i")
q("SELECT lsm3_wait_merge_completion('stress_lsm')")
q('SET enable_seqscan=off');result=q('SELECT count(*),sum(k) FROM stress WHERE k BETWEEN 1 AND 150000').fetchone();assert result==(150000,11250075000)
q('SET enable_seqscan=on');record('automatic_compaction',dict(result=result,merges=q("SELECT lsm3_get_merge_count('stress_lsm'),lsm3_top_index_size('stress_lsm')").fetchone()))
(OUT/'sql.status').write_text('PASS\n')
