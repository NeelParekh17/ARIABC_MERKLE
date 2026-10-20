#!/usr/bin/env python3
import concurrent.futures,json,signal,subprocess,sys,time
from pathlib import Path
B=Path('/home/neel/ariabc_data/fanout_sweep_20261014')
out=Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
hosts=['10.129.148.247','10.129.148.246','10.129.148.248']
running=True
def stop(*_):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
def sample(host):
    cmd=['python3',str(B/'host_probe.py')] if host.endswith('.247') else ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','neel@'+host,'python3 '+str(B/'host_probe.py')]
    try:
        p=subprocess.run(cmd,capture_output=True,text=True,timeout=8)
        r=json.loads(p.stdout) if p.returncode==0 else {'epoch_ms':int(time.time()*1000),'error':p.stderr[-500:]}
    except Exception as e:r={'epoch_ms':int(time.time()*1000),'error':str(e)}
    r['ip']=host
    return r
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    with (out/'samples.jsonl').open('a',buffering=1) as f:
        while running:
            samples=list(pool.map(sample,hosts))
            commits={r['ip']:r.get('committed_txid') for r in samples}
            good=[v for v in commits.values() if isinstance(v,int)]
            high=max(good) if good else None
            for r in samples:
                r['lag_txids']=high-r['committed_txid'] if high is not None and isinstance(r.get('committed_txid'),int) else None
                f.write(json.dumps(r)+'\n')
            for _ in range(20):
                if not running:break
                time.sleep(.1)
