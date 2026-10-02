#!/usr/bin/python3
import datetime, json, pathlib, subprocess, sys, uuid
B=pathlib.Path('/home/neel/Desktop/recovery_s1024_20261001T060514ZJ')
uid=uuid.uuid4().hex
p=B/'command_audit'/(uid+'.jsonl')
args=sys.argv[1:]
stdin=sys.stdin.buffer.read() if args[-2:]==['bash','-s'] or args[-1:]==['bash -s'] else None
def log(**d):
    with p.open('a') as f:
        f.write(json.dumps(dict(id=uid,time=datetime.datetime.now(datetime.timezone.utc).isoformat(),**d))+'\n')
log(event='start',argv=args,command=' '.join(args),stdin=stdin.decode(errors='replace') if stdin is not None else None)
text=' '.join(args)+'\n'+(stdin.decode(errors='replace') if stdin is not None else '')
if 'fuser -k' in text or 'pkill ' in text:
    log(event='end',exit_code=125,reason='forbidden process cleanup');sys.exit(125)
r=subprocess.run(['/usr/bin/ssh','-o','BatchMode=yes',*args],input=stdin)
log(event='end',exit_code=r.returncode)
sys.exit(r.returncode)
