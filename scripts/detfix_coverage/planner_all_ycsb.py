#!/usr/bin/env python3
"""EXPLAIN every literal YCSB suite statement; stream compact evidence."""
import gzip,hashlib,json,os,time
from pathlib import Path
import psycopg
if os.uname().nodename!='Neel':raise SystemExit('Remote lab host only')
root=Path(os.environ['DETFIX_ROOT']);conn=psycopg.connect(host='127.0.0.1',port=55491,user='postgres',dbname='postgres',autocommit=True)
counts={};fallbacks=[];variants={};started=time.monotonic()
def walk(n):
 yield n
 for child in n.get('Plans',[]):yield from walk(child)
with conn.cursor() as cur,gzip.open(root/'ycsb_all_explains.jsonl.gz','wt',compresslevel=1) as out:
 for file in sorted((root/'src/scripts/ycsb_suite').glob('*.txt')):
  count=0
  for line,q in enumerate(file.read_text().splitlines(),1):
   if not q.strip():continue
   cur.execute('EXPLAIN (FORMAT JSON) '+q);plan=cur.fetchone()[0][0]['Plan']
   scans=[{k:n[k] for k in ('Node Type','Relation Name','Index Name','Index Cond','Filter') if k in n} for n in walk(plan) if 'Scan' in n['Node Type']]
   bad=any(n['Node Type']=='Seq Scan' or ('Index' in n['Node Type'] and not n.get('Index Cond','').lower().startswith('(ycsb_key =')) for n in scans)
   entry={'file':file.name,'line':line,'sql_sha256':hashlib.sha256(q.encode()).hexdigest(),'scans':scans,'fallback':bad}
   out.write(json.dumps(entry,separators=(',',':'))+'\n');count+=1
   if bad:fallbacks.append(entry)
   signature=tuple((n['Node Type'],n.get('Relation Name'),n.get('Index Name')) for n in scans)
   variants[str(signature)]=variants.get(str(signature),0)+1
  counts[file.name]=count
  print(json.dumps({'file':file.name,'count':count,'total':sum(counts.values())}),flush=True)
result={'explained':sum(counts.values()),'files':counts,'scan_variants':variants,'fallbacks':fallbacks,'seconds':time.monotonic()-started}
(root/'ycsb_all_explains_summary.json').write_text(json.dumps(result,indent=2)+'\n');conn.close()
