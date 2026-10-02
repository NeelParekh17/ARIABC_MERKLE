"""Task J remote execution; all direct commands pass through the audit logger."""
import datetime,json,pathlib,shlex,subprocess,sys,time
import remote
B=remote.B; BASE=remote.C['base']; TAG=remote.C['tag']
HOSTS=['10.129.148.247','10.129.148.246','10.129.148.248','10.129.27.111']
FP=(B/'frozen_fingerprint.txt').read_text().strip()
def status(message):
    stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
    (B/'execution_status.txt').write_text(stamp+' '+message+'\n')
    with (B/'execution_timeline.log').open('a') as f:f.write(stamp+' '+message+'\n')
    print(stamp,message,flush=True)
def command(host,cmd,label):
    if remote.run(host,cmd,label):raise RuntimeError(label+' failed')
def transfer(host,src,dst,label,extra=()):
    args=['rsync','-az','-e','ssh -o BatchMode=yes -o ConnectTimeout=12',*extra,src,'neel@'+host+':'+dst]
    if remote.run(host,shlex.join(args),label,argv=args):raise RuntimeError(label+' failed')
def readscript(name):return (B/name).read_text().replace('@RBASE@',BASE).replace('@RTAG@',TAG)
try:
    status('WAITING_FOR_CHAIN_DONE')
    gate=pathlib.Path('/work/ARIABC/AriaBC/.bench_tmp/oom_ff90_20261001/status.txt')
    while 'CHAIN_DONE' not in gate.read_text().splitlines():
        time.sleep(60);time.sleep(60)
    status('OOM_GATE_OPEN; deploying node1')
    if not (B/'oom_gate_open.txt').exists():
        (B/'oom_gate_open.txt').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n'+gate.read_text())
    command(HOSTS[0],readscript('canonical_inventory.sh'),'canonical_before_'+HOSTS[0])
    command(HOSTS[0],f"set -e; test ! -e '{BASE}'; mkdir -p '{BASE}/repo' '{BASE}/install' '{BASE}/rdkafka' '{BASE}/repo/ariabc_pg/build/bin' '{BASE}/artifacts/u24'",'create_admin123')
    deploy=[(str(B/'frozen_repo')+'/',BASE+'/repo/',('--exclude=/scripts/bench_results*/','--exclude=/scripts/benchmark/','--exclude=/scripts/rust_workload/target/','--exclude=/Dynamic_merkle_docs/')),
            (str(B/'hosts/10.129.27.111/install')+'/',BASE+'/install/',()),
            (str(B/'hosts/10.129.27.111/rdkafka')+'/',BASE+'/rdkafka/',()),
            (str(B/'final_u24/artifacts/bin')+'/',BASE+'/repo/ariabc_pg/build/bin/',()),
            (str(B/'final_u24/postgres.manifest'),BASE+'/install/bin/postgres.manifest',()),
            (str(B/'final_u24/artifacts/build.env'),BASE+'/artifacts/u24/build.env',()),
            (str(B/'stop_server.sh'),BASE+'/stop_server.sh',()),
            (str(B/'geometry.sql'),BASE+'/geometry.sql',())]
    for i,(src,dst,extra) in enumerate(deploy):transfer(HOSTS[0],src,dst,'deploy_admin123_'+str(i),extra)
    transfer('10.129.27.111',str(B/'oom_gate_open.txt'),BASE+'/oom_gate_open.txt','publish_gate_evidence')
    command(HOSTS[0],readscript('init.sh'),'init_admin123')
    command(HOSTS[0],readscript('initial_audit.sh'),'initial_audit_admin123')
    for host in HOSTS:
        cmd=f"set -e; export LD_LIBRARY_PATH='{BASE}/rdkafka/lib:{BASE}/install/lib'; fp=$(python3 '{BASE}/repo/scripts/distributed/source_fingerprint.py' --repo '{BASE}/repo' --ring-capacity 2048); test \"$fp\" = '{FP}'; echo source_fingerprint=$fp; for bin in '{BASE}/install/bin/postgres' '{BASE}/repo/ariabc_pg/build/bin/ariabc_pg_gateway' '{BASE}/repo/ariabc_pg/build/bin/ariabc_pg_server'; do sha256sum \"$bin\"; ldd \"$bin\"; if ldd \"$bin\" | grep -q 'not found'; then exit 2; fi; done"
        command(host,cmd,'final_provenance_'+host)
    status('ALL_HOST_PROVENANCE_AND_INITIAL_AUDITS_PASS')
    cases=[(32,1024,256,n) for n in ['A','B','C','M_mixed','L_mix_prio']]+[(4,32,8,n) for n in ['A','C','M_mixed']]
    for f,s,m,name in cases:
        rid=f'cluster4_s1024_{TAG}_f{f}s{s}_{name}_r1'
        status('RUNNING '+rid)
        cmd=f"bash '{BASE}/run_case.sh' {f} {s} {m} {name} < /dev/null"
        rc=remote.run('10.129.27.111',cmd,'run_'+rid)
        result=subprocess.run([sys.executable,str(B/'collect_case.py'),rid],cwd=B.parent.parent)
        if result.returncode:raise RuntimeError('artifact collection failed '+rid)
        if rc:raise RuntimeError('run/collection returned '+str(rc)+' for '+rid)
        if subprocess.run([sys.executable,str(B/'validate_runs.py')],cwd=B.parent.parent).returncode:
            raise RuntimeError('acceptance failed '+rid)
        status('PASS '+rid)
    for host in HOSTS:
        command(host,readscript('canonical_inventory.sh'),'canonical_after_'+host)
    status('EIGHT_CASES_COMPLETED_AND_ACCEPTED')
except Exception as e:
    status('STOPPED_ON_FAILURE '+str(e))
    raise
