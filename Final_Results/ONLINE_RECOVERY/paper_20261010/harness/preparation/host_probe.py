#!/usr/bin/env python3
import json,os,subprocess,time,socket
from pathlib import Path
B=Path('/home/neel/ariabc_data/paper_20261010_recov')
mem={k:int(v.strip().split()[0]) for k,v in (line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())}
vm={k:int(v) for k,v in (line.split() for line in Path('/proc/vmstat').read_text().splitlines())}
r={'epoch_ms':int(time.time()*1000),'host':socket.gethostname(),'mem_available_kb':mem['MemAvailable'],
   'mem_free_kb':mem['MemFree'],'swap_used_kb':mem['SwapTotal']-mem['SwapFree'],
   'pswpin':vm['pswpin'],'pswpout':vm['pswpout'],'pgmajfault':vm['pgmajfault'],
   'page_bytes':os.sysconf('SC_PAGE_SIZE'),'load':list(os.getloadavg()),'cpus':os.cpu_count(),
   'clocksource':Path('/sys/devices/system/clocksource/clocksource0/current_clocksource').read_text().strip(),
   'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'committed_txid':None,
   'cpu_stat':Path('/proc/stat').read_text().splitlines()[0],
   'diskstats':Path('/proc/diskstats').read_text()}
if 'myubuntu' not in r['host'] and (B/'install/bin/psql').exists():
    env=dict(os.environ,LD_LIBRARY_PATH=str(B/'install/lib'))
    try:
        p=subprocess.run([str(B/'install/bin/psql'),'-X','-q','-h','127.0.0.1','-p','5448','-U','postgres','postgres','-At',
                          '-c','SELECT bcdb_last_committed_txid();'],capture_output=True,text=True,timeout=2,env=env)
        if p.returncode==0:r['committed_txid']=int(p.stdout.strip())
    except Exception as e:r['probe_error']=type(e).__name__
print(json.dumps(r),flush=True)
