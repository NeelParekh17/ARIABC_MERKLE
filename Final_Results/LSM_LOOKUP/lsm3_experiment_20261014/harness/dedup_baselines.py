#!/usr/bin/env python3
"""Deduplicate byte-identical files in OUR STOPPED IMMUTABLE baselines only.

Never link a working copy or a published source. All subsequent PG starts must
use independent regular files. This changes storage allocation, not file bytes.
"""
import hashlib,json,os,stat,time
from pathlib import Path
D=Path('/home/neel/ariabc_data/lsm_lookup_20261014');R=Path('/home/neel/claude_checks/lsm_lookup_20261014');O=R/'dedup';O.mkdir(exist_ok=True)
bases=sorted(p for p in D.iterdir() if p.is_dir() and p.name.startswith('base_ff'))
for b in bases:
 assert not b.is_symlink() and not (b/'postmaster.pid').exists()
 assert (D/('.ariabc_golden_manifest_'+b.name+'.json')).exists()
groups={};records=[]
for b in bases:
 for p in sorted((b/'base').glob('*/*')):
  if not p.is_file() or p.is_symlink() or p.stat().st_size<1024*1024:continue
  st=p.stat();key=(st.st_size,stat.S_IMODE(st.st_mode),st.st_uid,st.st_gid)
  groups.setdefault(key,[]).append(p)
for key,paths in groups.items():
 if len(paths)<2:continue
 same={};seen_bases={}
 for p in paths:
  st=p.stat();h=hashlib.sha256()
  with p.open('rb') as f:
   while data:=f.read(8*1024*1024):h.update(data)
  digest=h.hexdigest();base=p.relative_to(D).parts[0]
  if base in seen_bases.get(digest,set()):continue
  seen_bases.setdefault(digest,set()).add(base)
  if digest not in same:same[digest]=p;continue
  src=same[digest];a=src.stat()
  if (a.st_dev,a.st_ino)==(st.st_dev,st.st_ino):continue
  # Keep the former directory entry until an atomic replace installs the link.
  tmp=O/('link_'+str(os.getpid())+'_'+str(len(records)));assert not tmp.exists()
  os.link(src,tmp);os.replace(tmp,p)
  records.append(dict(source=str(src),target=str(p),sha256=digest,bytes=st.st_size,source_mtime_ns=a.st_mtime_ns,target_previous_mtime_ns=st.st_mtime_ns))
  (O/'progress.json').write_text(json.dumps(dict(files=len(records),saved_apparent_bytes=sum(x['bytes'] for x in records)))+'\n')
(O/('links_'+str(time.time_ns())+'.json')).write_text(json.dumps(records,indent=2)+'\n')
print(json.dumps(dict(files=len(records),saved_bytes=sum(x['bytes'] for x in records))))

import subprocess
for b in bases:
 manifest=json.loads((D/('.ariabc_golden_manifest_'+b.name+'.json')).read_text())
 assert int(subprocess.check_output(['du','-sb',str(b)],text=True).split()[0])==manifest['total_bytes'], (b,'apparent cluster size changed')
 assert hashlib.sha256((b/'global/pg_control').read_bytes()).hexdigest()==manifest['pg_control_sha256']
