#!/usr/bin/env python3
"""Stop only recorded observers whose command and output prove ownership."""
import json,os,signal,sys,time
from pathlib import Path
role=sys.argv[1]
R=Path('/home/neel/claude_checks/lsm_lookup_20261014' if role=='db' else '/home/neel/claude_ctl/lsm_lookup_20261014')
O=R/('evidence' if role=='db' else 'results');records=[]
for kind in (['host_iostat'] if role=='db' else ['host_iostat','progress']):
 pidfile=O/(kind+'.pid');pid=int(pidfile.read_text());proc=Path('/proc')/str(pid)
 if not proc.exists():records.append(dict(kind=kind,pid=pid,status='already stopped'));continue
 assert proc.stat().st_uid==os.getuid()
 cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode().strip();out=os.readlink(proc/'fd/1')
 if kind=='host_iostat':assert Path(cmd.split()[0]).name=='iostat' and out==str(O/'host_iostat.txt'),(cmd,out)
 else:assert cmd=='python3 -u '+str(R/'progress.py'),cmd
 os.kill(pid,signal.SIGTERM);records.append(dict(kind=kind,pid=pid,status='SIGTERM sent',command=cmd,stdout=out))
(O/'observers_stopped.json').write_text(json.dumps(records,indent=2)+'\n');print(json.dumps(records))
