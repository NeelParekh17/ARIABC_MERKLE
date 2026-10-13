#!/usr/bin/env python3
""".247 only. Stop only F15's controller/PG if the disk reserve is violated."""
import json, os, shutil, subprocess as S, time
from pathlib import Path
OWN=Path('/home/neel/claude_checks/f15_adhash_20261012')
PAPER=Path('/home/neel/ariabc_data/paper_20261010_oom')
OUT=OWN/'oom_baseline'
os.environ['TMPDIR']=str(OWN/'scratch')
os.environ['LD_LIBRARY_PATH']=str(OWN/'install/lib')+':/home/neel/Desktop/rdkafka_local/lib'
minimum=shutil.disk_usage(PAPER).free
with (OUT/'campaign_space.jsonl').open('a',buffering=1) as log:
    while True:
        if (OUT/'GRID_DONE.flag').exists():break
        free=shutil.disk_usage(PAPER).free;minimum=min(minimum,free)
        log.write(json.dumps(dict(time=time.time(),free_bytes=free,min_free_bytes=minimum))+'\n')
        if free<5*1024**3:
            (OUT/'SPACE_RESERVE_VIOLATION.json').write_text(json.dumps(dict(time=time.time(),free_bytes=free,threshold_bytes=5*1024**3),indent=2))
            S.run(['ssh','neel@10.129.27.111','tmux -S "$HOME/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock" send-keys -t f15oom C-c'])
            for name in ['pgdata_merkle','pgdata_plain']:
                data=PAPER/name;pidfile=data/'postmaster.pid'
                if pidfile.exists() and pidfile.read_text().splitlines()[3]=='5478':
                    S.run([str(OWN/'install/bin/pg_ctl'),'-D',str(data),'-m','fast','-w','-t','120','stop'])
            break
        time.sleep(5)
