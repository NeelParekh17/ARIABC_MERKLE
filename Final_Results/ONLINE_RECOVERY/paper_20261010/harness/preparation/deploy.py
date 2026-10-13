#!/usr/bin/env python3
from pathlib import Path
import concurrent.futures,subprocess
B=Path('/home/neel/ariabc_data/paper_20261010_recov')
hosts=['10.129.148.247','10.129.148.246','10.129.148.248']
def call(args,**kw):return subprocess.run(args,check=True,**kw)
for name in ['stop_server.sh','host_sample.sh','host_probe.py','health_watch.py','run_case.sh','collect_run.py','controller.py']:
    (B/name).write_bytes((B/'preparation'/name).read_bytes())
def deploy(host):
    with (B/'provenance'/('deploy_'+host+'.log')).open('w') as f:
        call(['ssh','neel@'+host,'mkdir -p '+str(B/'repo')+' '+str(B/'install')+' '+str(B/'rdkafka')+' '+str(B/'u22_build')+' '+str(B/'provenance')],stdout=f,stderr=f)
        if not host.endswith('.247'):
            call(['rsync','-az','--exclude=.bench_tmp','--exclude=scripts/bench_full_results',str(B/'repo')+'/',f'neel@{host}:{B}/repo/'],stdout=f,stderr=f)
            for name in ['install','rdkafka','u22_build']:
                call(['rsync','-az',str(B/name)+'/',f'neel@{host}:{B}/{name}/'],stdout=f,stderr=f)
        for name in ['stop_server.sh','host_sample.sh','host_probe.py','geometry.sql']:
            call(['rsync','-a',str(B/name),f'neel@{host}:{B}/'],stdout=f,stderr=f)
        call(['rsync','-a',str(B/'provenance/BINARIES.sha256'),f'neel@{host}:{B}/provenance/'],stdout=f,stderr=f)
        call(['ssh','neel@'+host,'bash -s'],input=(B/'preparation/init_node.sh').read_text(),text=True,stdout=f,stderr=f)
    return host
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for host in pool.map(deploy,hosts):print('initialized',host,flush=True)
with (B/'provenance/controller_runtime.log').open('w') as f:
    call(['bash','-c',f'export LD_LIBRARY_PATH={B}/rdkafka/lib:{B}/install/lib; sha256sum -c {B}/provenance/BINARIES.sha256; ldd {B}/repo/ariabc_pg/build/bin/ariabc_pg_gateway; git -C {B}/repo rev-parse HEAD; git -C {B}/repo status --porcelain; python3 {B}/repo/scripts/distributed/source_fingerprint.py --repo {B}/repo --ring-capacity 2048'],stdout=f,stderr=f)
(B/'DEPLOY_READY').write_text('all-three isolated PGDATA initialized; hashes and runtime verified\n')
