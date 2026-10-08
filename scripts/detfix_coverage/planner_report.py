#!/usr/bin/env python3
"""Remote-only plan coverage and reference YCSB schema extraction."""
import hashlib, json, os, re, subprocess
from pathlib import Path
if os.uname().nodename != 'Neel':raise SystemExit('Remote lab host only')
root=Path(os.environ['DETFIX_ROOT'])
raw=(root/'ycsb_restore_reference.sql').read_text()
schema=re.search(r'CREATE TABLE public\.usertable_small \(.*?\n\);',raw,re.S).group()
copy=re.search(r'COPY public\.usertable_small .*?\n\\\.',raw,re.S).group()
pk=re.search(r'ALTER TABLE ONLY public\.usertable_small\s+ADD CONSTRAINT .*?PRIMARY KEY \(ycsb_key\);',raw,re.S).group()
(root/'ycsb_schema_data.sql').write_text(schema+'\n'+copy+'\n'+pk+'\nANALYZE public.usertable_small;\n')
import psycopg
conn=psycopg.connect(host='127.0.0.1',port=55491,user='postgres',dbname='postgres');conn.autocommit=True
with (root/'ycsb_schema_load.log').open('w') as f:
 subprocess.run([str(root/'install/bin/psql'),'-X','-v','ON_ERROR_STOP=1','-h','127.0.0.1','-p','55491','-U','postgres','-f',str(root/'ycsb_schema_data.sql')],stdout=f,stderr=subprocess.STDOUT,check=True)
def walk(n):
 yield n
 for c in n.get('Plans',[]):yield from walk(c)
log=(root/'planner_postgres.log').read_text();dec=json.JSONDecoder();plans={}
for match in re.finditer(r'plan:\n',log):
 try:v,_=dec.raw_decode(log[match.end():].lstrip())
 except json.JSONDecodeError:continue
 q=re.sub(r'\s+',' ',v['Query Text']);plans.setdefault(q,[]).append(v['Plan'])
keys={'warehouse':'w_id','district':'d_w_id','customer':'c_w_id','stock':'s_w_id','oorder':'o_w_id','new_order':'no_w_id','order_line':'ol_w_id','history':'h_c_id','item':'i_id'}
scans=[]
for q,variants in plans.items():
 seen=set()
 for plan in variants:
  for n in walk(plan):
   kind=n['Node Type'];rel=n.get('Relation Name')
   if rel not in keys or 'Scan' not in kind:continue
   signature=json.dumps(n,sort_keys=True)
   if signature in seen:continue
   seen.add(signature)
   cond=n.get('Index Cond','');has_bound=bool(re.search(r'\b'+keys[rel]+r'\s*=',cond))
   scans.append({'query':q,'relation':rel,'type':kind,'index':n.get('Index Name'),'condition':cond or n.get('Filter',''),'relation_fallback':kind=='Seq Scan' or ('Index' in kind and not has_bound)})
# Every suite statement shape, retaining the actual SQL used for EXPLAIN.
shapes={};files=list((root/'src/scripts/ycsb_suite').glob('*.txt'));total=0
for file in files:
 for q in file.read_text().splitlines():
  if not q.strip():continue
  total+=1
  normalized=re.sub(r"'(?:[^']|'')*'",'?',q)
  normalized=re.sub(r'\b\d+\b','?',normalized)
  shapes.setdefault(normalized,q)
yplans=[]
with conn.cursor() as cur:
 for shape,q in shapes.items():
  cur.execute('EXPLAIN (FORMAT JSON) '+q)
  p=cur.fetchone()[0][0]['Plan'];yplans.append({'shape':shape,'query':q,'plan':p})
report={'tpcc_unique_logged_queries':len(plans),'tpcc_query_variants':{q:len(v) for q,v in plans.items()},'tpcc_scans':scans,'tpcc_relation_fallback_candidates':[s for s in scans if s['relation_fallback']], 'ycsb_source':'scripts/distributed/run_all_modes_gateway_sweep.py invokes scripts/restore_usertable_small.sql; copied read-only script into owned root','ycsb_restore_sha256':hashlib.sha256(raw.encode()).hexdigest(),'ycsb_suite_files':len(files),'ycsb_statement_count':total,'ycsb_shapes':yplans}
(root/'planner_report.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'tpcc_queries':len(plans),'tpcc_fallbacks':[(s['relation'],s['type'],s['condition']) for s in scans if s['relation_fallback']],'ycsb_files':len(files),'ycsb_statements':total,'ycsb_shapes':len(shapes)}))
conn.close()
