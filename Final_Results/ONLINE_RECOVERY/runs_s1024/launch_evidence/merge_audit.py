import pathlib,json,sys
from remote import B,log
folder=B/'nested_commands'
seenfile=B/'nested_commands_merged.json'
seen=set(json.loads(seenfile.read_text())) if seenfile.exists() else set()
hostmap={}
for f in folder.glob('*.jsonl'):
    for line in f.read_text().splitlines():
        d=json.loads(line)
        if d['event']=='start':
            hostmap[d['id']]=next((a for a in d.get('argv',[]) if '@10.' in a),'see argv')
for f in folder.glob('*.jsonl'):
    for line in f.read_text().splitlines():
        d=json.loads(line); key=d['id']+d['event']
        if key in seen: continue
        seen.add(key);d['host']=hostmap.get(d['id'],'see start');d['audit_origin']='10.129.27.111'
        log(d)
seenfile.write_text(json.dumps(sorted(seen)))
print('Merged events',len(seen))
