import hashlib,json,subprocess,tarfile
from pathlib import Path
base=Path(__file__).resolve().parent;pr=base/'evidence/provenance';audit=json.load(open(base/'audit_raw.json'));out={'curated_files':0,'curated_bytes':0,'errors':[]}
for p in (base/'evidence/raw').rglob('*'):
 if not p.is_file():continue
 remote='/home/neel/claude_checks/paper_20261010_oom/results/'+str(p.relative_to(base/'evidence/raw'));v=audit['raw_hashes'][remote]
 if hashlib.sha256(p.read_bytes()).hexdigest()!=v['sha256']:out['errors'].append(str(p))
 out['curated_files']+=1;out['curated_bytes']+=p.stat().st_size
out['archive_sha256']=hashlib.sha256((pr/'source.tar.gz').read_bytes()).hexdigest();expect=json.load(open(pr/'source_archive_scope.json'))['source_archive_sha256']
if out['archive_sha256']!=expect:out['errors'].append('source archive hash')
manifest=dict((name,sha) for sha,name in (l.split(maxsplit=1) for l in (pr/'engine_SHA256SUMS').read_text().splitlines()))
source_checks=0;git_checks=0;seen_manifest=set()
# Compare every archived build/source file to both its SHA256 and frozen git blob.
proc=subprocess.Popen(['git','cat-file','--batch'],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
with tarfile.open(pr/'source.tar.gz','r:gz') as t:
 for member in t:
  if not member.isfile():continue
  name=member.name.removeprefix('./');data=t.extractfile(member).read()
  if name in manifest:
   seen_manifest.add(name)
   if hashlib.sha256(data).hexdigest()!=manifest[name]:out['errors'].append('manifest '+name)
  source_checks+=1
  proc.stdin.write(('16577072:'+name+'\n').encode());proc.stdin.flush();header=proc.stdout.readline().decode().split()
  if len(header)!=3:out['errors'].append('git missing '+name);continue
  n=int(header[2]);gitdata=proc.stdout.read(n);proc.stdout.read(1)
  if gitdata!=data:out['errors'].append('git contents '+name)
  git_checks+=1
proc.stdin.close();proc.wait()
if seen_manifest!=set(manifest):out['errors'].append('missing manifest entries')
out.update(source_files_checked=source_checks,frozen_git_files_checked=git_checks,manifest_files=len(manifest))
(base/'source_and_fetch_checks.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:v for k,v in out.items() if k!='errors'},indent=2));print('errors:',len(out['errors']))
