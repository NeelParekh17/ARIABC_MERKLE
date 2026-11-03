#!/usr/bin/env python3
"""Audit controller. Execute on .111, detached; never import/run on workstation.

Reuse the independently checked matrix harness, without editing that campaign.
Changes are confined to this wrapper: new output paths, explicit custom workload,
extra observations, and separation of run/drain/checkpoint intervals.
"""
import copy
from datetime import datetime,timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import subprocess
import sys
import time
import traceback
import uuid

ROOT=Path('/home/neel/claude_ctl/lsm_relation_audit_20261021')
DBROOT='/home/neel/claude_checks/lsm_relation_audit_20261021'
MATRIX=Path('/home/neel/claude_ctl/ycsb_matrix_20261020')
DBMATRIX='/home/neel/claude_checks/ycsb_matrix_20261020'
POOL='/home/neel/ariabc_data/lsm_lookup_20261014'
SOURCE=MATRIX/'src'
DBHOST='10.129.148.247'
INSTALL=os.environ.get('AUDIT_INSTALL',DBMATRIX+'/install')
N=int(os.environ.get('AUDIT_STATEMENTS','2400000'))
PORT=5498
SERVER_PORT=8098


def write(path,value):
    Path(path).write_text(json.dumps(value,indent=2,default=str)+'\n')


def log(message):
    s=datetime.now(timezone.utc).isoformat()+' '+message
    print(s,flush=True)
    with (ROOT/'status.txt').open('a') as f:f.write(s+'\n')


def remote(command,timeout=60,check=True):
    r=subprocess.run(['ssh','-o','ControlMaster=no','-o','BatchMode=yes','-o','ConnectTimeout=15',
        'neel@'+DBHOST,'bash -s'],input='set -euo pipefail\n'+command,
        capture_output=True,text=True,timeout=timeout)
    if check and r.returncode:raise RuntimeError(f'remote rc={r.returncode}: {r.stdout}\n{r.stderr}')
    return r


def gate():
    while True:
        status=(MATRIX/'results/matrix/status.txt').read_text()
        running=subprocess.run(['pgrep','-af','^bash /home/neel/claude_ctl/ycsb_matrix_20261020/run_matrix.sh'],
            capture_output=True,text=True).stdout.strip()
        (ROOT/'evidence/matrix_status_observed.txt').write_text(status+'\nPROCESS\n'+running+'\n')
        if 'CAMPAIGN_END f_btree' in status and not running:
            pg=remote('ps -C postgres -o pid=,args= || true').stdout.strip()
            (ROOT/'evidence/postgres_at_gate.txt').write_text(pg+'\n')
            if not pg:
                log('GATE_OPEN: f_btree ended; run_matrix exited; no postgres on DB host')
                return
            log('WAIT_FOR_DB_IDLE '+pg[:200])
        else:
            log('WAIT_FOR_MATRIX '+status.strip().splitlines()[-1])
        time.sleep(600)


def load_harness():
    sys.dont_write_bytecode=True
    reference_paths=[SOURCE/'scripts/distributed'/name for name in
        ['run_oom_100m_benchmark.py','benchmark_validation.py','benchmark_contract.py','device_settle_probe.py','large_zipf.py']]
    reference_paths+=[SOURCE/'scripts/generate_ycsb_workloads.py',SOURCE/'scripts/distributed/sql/raft_apply_ledger_schema.sql']
    write(ROOT/'evidence/reference_harness_sha256.json',
        {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in reference_paths})
    sys.path.insert(0,str(SOURCE/'scripts/distributed'))
    sys.path.insert(0,str(SOURCE/'scripts'))
    spec=importlib.util.spec_from_file_location('audit_reference_harness',SOURCE/'scripts/distributed/run_oom_100m_benchmark.py')
    h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
    base_remote=h.run_remote
    translations={POOL+'/postgres.log':DBROOT+'/postgres.log',
        POOL+'/server.log':DBROOT+'/server.log',POOL+'/server.err.log':DBROOT+'/server.err.log',
        POOL+'/server.pid':DBROOT+'/server.pid',
        'cd '+POOL+'\n':'cd '+DBROOT+'\n',
        POOL+'/device_settle_probe.py':DBROOT+'/scripts/device_settle_probe.py',
        POOL+'/.device_probe_write':DBROOT+'/scratch/device_probe_write'}

    def run_remote(host,user,cmd,**kw):
        for old,new in translations.items():cmd=cmd.replace(old,new)
        scratch=(DBROOT if host==DBHOST else str(ROOT))+'/scratch'
        cmd='export TMPDIR='+scratch+'\n'+cmd
        secret=getattr(h,'_SUDO_PASSWORD',None)
        safe=cmd.replace(secret,'<REDACTED>') if secret else cmd
        with (ROOT/'commands.jsonl').open('a') as f:
            f.write(json.dumps(dict(t=time.time(),host=host,command=safe,options=kw))+'\n')
        # All potentially long remote jobs are detached. SSH only creates the
        # job, polls its small status file, or retrieves its completed output.
        # Never write a sudo password to a job file.
        if kw.get('timeout',600)>120 and 'sudo -' not in cmd:
            jobroot=DBROOT if host==DBHOST else str(ROOT)
            job=jobroot+'/jobs/'+uuid.uuid4().hex
            script=("#!/bin/bash\nset -euo pipefail\nexport LC_ALL=C\n"
                f"date +%s.%N > {job}.begin\n"
                f"trap 'rc=$?; date +%s.%N > {job}.end; printf \"%s\\n\" \"$rc\" > {job}.status.tmp; mv {job}.status.tmp {job}.status' EXIT\n"+cmd+'\n')
            launch=f"mkdir -p {jobroot}/jobs\ncat > {job}.sh <<'AUDIT_JOB_EOF'\n{script}\nAUDIT_JOB_EOF\n(setsid nohup bash {job}.sh > {job}.out 2> {job}.err < /dev/null & echo $! > {job}.pid)"
            base_remote(host,user,launch,timeout=30)
            started=time.monotonic()
            while True:
                status=base_remote(host,user,f"if [ -f {job}.status ]; then cat {job}.status; else echo RUNNING; fi",timeout=30).stdout.strip()
                if status!='RUNNING':break
                if time.monotonic()-started>kw.get('timeout',600):
                    base_remote(host,user,f"kill -TERM -- -$(cat {job}.pid)",timeout=30,check=False)
                    raise TimeoutError('Detached remote job exceeded timeout: '+job)
                # Each poll is a new SSH login (journald/logind/wtmp writes on the
                # DB host). Polling every 3 s kept the device-settle probe from
                # ever seeing <=2 MiB windows, so back off for long jobs.
                waited=time.monotonic()-started
                time.sleep(3 if waited<30 else 30)
            stdout=base_remote(host,user,f'cat {job}.out',timeout=60).stdout
            stderr=base_remote(host,user,f'cat {job}.err',timeout=60).stdout
            timings=base_remote(host,user,f'cat {job}.begin {job}.end',timeout=30).stdout.split()
            h._last_remote_execution_seconds=float(timings[1])-float(timings[0])
            result=subprocess.CompletedProcess(['detached',job],int(status),stdout,stderr)
            if kw.get('check',True) and result.returncode:
                raise RuntimeError(f'Detached job failed: {job} rc={status}\n{stdout}\n{stderr}')
            return result
        return base_remote(host,user,cmd,**kw)

    h.run_remote=run_remote
    base_sql=h.sql
    def sql(args,statement,timeout=600):
        return base_sql(args,statement,timeout=60 if timeout==600 else timeout)
    h.sql=sql
    # The generic reference cleanup also inspects POOL/pgdata. Restrict cleanup
    # to the two explicitly authorized work copies and our port/socket marker.
    def stop_postgres(args):
        command=h.db_shell(args)+f'''
for d in {POOL}/pgdata_plain {POOL}/pgdata_merkle; do
 if [ -f "$d/postmaster.pid" ] && {args.install_dir}/bin/pg_ctl -D "$d" status >/dev/null 2>&1; then
  test "$(sed -n '4p' "$d/postmaster.pid")" = {PORT}
  grep -F "unix_socket_directories = '{DBROOT}/socket'" "$d/postgresql.auto.conf" >/dev/null
  {args.install_dir}/bin/pg_ctl -D "$d" -w -t 120 stop -m fast
 fi
done
'''
        return h.run_remote(DBHOST,'neel',command,timeout=150,retry_on_drop=True)
    h.stop_postgres=stop_postgres
    # The reference counter reads a whole 1GB SQL file. Streaming preserves its
    # skip rules without allocating several copies alongside the active gateway.
    def count_queries(path):
        with Path(path).open() as f:
            return sum(bool(s.strip()) and not s.lstrip().startswith(('--','/*','\\')) for s in f)
    h.count_workload_queries=count_queries
    return h


def generate():
    from generate_ycsb_workloads import generate_sql_select,generate_sql_update,generate_sql_insert,generate_sql_rmw
    from large_zipf import LargeZipfGenerator
    out={}
    for kind in ['insert','update']:
        path=ROOT/'workloads'/f'{kind}_{N}_seed42.sql'
        if not path.exists():
            rng=random.Random(42);zipf=LargeZipfGenerator(100000000,.99,rng)
            with path.open('x') as f:
                for i in range(N):
                    key=100000001+i if kind=='insert' else zipf.next_index()
                    sql=(generate_sql_insert if kind=='insert' else generate_sql_update)('usertable','ycsb_key',key,10,rng)
                    f.write(sql+'\n')
        digest=hashlib.file_digest(path.open('rb'),'sha256').hexdigest()
        gateway_path=path.parent/('oom_'+digest+'.sql')
        if not gateway_path.exists():os.link(path,gateway_path)
        out[kind]=dict(path=str(path),sha256=digest,statements=N,
            distribution='monotonic new keys' if kind=='insert' else 'finite Zipf theta .99, seed 42')
    examples=dict(read=generate_sql_select('usertable','ycsb_key',1),
        update=generate_sql_update('usertable','ycsb_key',1,10,random.Random(42)),
        insert=generate_sql_insert('usertable','ycsb_key',110100001,10,random.Random(42)),
        read_modify_write=generate_sql_rmw('usertable','ycsb_key',1,10,random.Random(43)))
    write(ROOT/'scripts/examples.json',examples)
    write(ROOT/'evidence/workloads.json',out)
    return out


def make_args(h):
    return h.parse_args(['--remote-host',DBHOST,'--remote-user','neel','--remote-dir',POOL,
        '--install-dir',INSTALL,'--cluster-dir',DBMATRIX+'/cluster',
        '--gateway-host','10.129.27.111','--gateway-user','neel','--gateway-repo',str(SOURCE),
        '--db-port',str(PORT),'--server-port',str(SERVER_PORT),'--base-dir-name','base_ff90',
        '--db-rows','100000000','--shared-buffers','32MB','--txs',str(N),'--seed','42',
        '--modes','bcdb_merkle','--workers','16','--combos','all_update:0.99','--trials','2',
        '--merkle-lookup-am','btree','--merkle-lookup-levels','3','--merkle-lookup-size','1024',
        '--merkle-lookup-size-ratio','16','--pg-retry-jitter','on','--pg-exec-mode','event',
        '--verify-mode','fast','--reset-mode','delta','--delta-content-check','sampled',
        '--gateway-timeout','7200','--reset-timeout','7200','--verify-timeout','7200',
        '--settle-factor','1.25','--settle-min-s','20','--settle-max-s','1800',
        '--calibrate-min-wait-s','300','--usertable-fillfactor','90','--skip-gen',
        '--require-existing-baselines','--socket-dir',DBROOT+'/socket',
        '--gateway-workload-dir',str(ROOT/'workloads')])


def prepare(h,args):
    assert Path('/sys/devices/system/clocksource/clocksource0/current_clocksource').read_text().strip()=='tsc'
    launch=h.run_remote(DBHOST,'neel',f"mkdir -p {DBROOT}/scripts {DBROOT}/scratch {DBROOT}/evidence {DBROOT}/results {DBROOT}/socket\n"
        f"test \"$(df -B1 --output=avail {DBROOT} | tail -1)\" -ge 30000000000\n"
        f"df -B1 --output=avail {DBROOT}\n"
        "test \"$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)\" = tsc\n"
        f"for port in {PORT} {SERVER_PORT} 9000; do if fuser $port/tcp >/dev/null 2>&1; then echo OCCUPIED_PORT=$port; exit 1; fi; done\n"
        f"sha256sum -c {DBMATRIX}/evidence/pg_binaries.sha256\n"
        f"sha256sum {DBMATRIX}/cluster/ariabc_pg/build/bin/ariabc_pg_server\n"
        'bpftrace --version\ncommand -v strace\ncommand -v gdb\n',timeout=60)
    (ROOT/'evidence/launch_preflight.txt').write_text(launch.stdout+launch.stderr)
    for name in ['pg_binaries.sha256','source_commit.txt','engine_source.sha256']:
        s=remote('cat '+DBMATRIX+'/evidence/'+name).stdout
        (ROOT/'evidence'/('db_'+name)).write_text(s)
    current=remote('sha256sum -c '+DBMATRIX+'/evidence/pg_binaries.sha256').stdout
    (ROOT/'evidence/binary_verify.txt').write_text(current)
    assert (ROOT/'evidence/db_source_commit.txt').read_text().strip()=='5f2a078eadb8817246f5d500dee2822f826d9c1e'
    for name in ['cpp_binaries.sha256','engine_source.sha256','source_commit.txt','harness.patch']:
        shutil.copyfile(MATRIX/'evidence'/name,ROOT/'evidence'/('controller_'+name))
    subprocess.run(['sha256sum','-c',str(MATRIX/'evidence/cpp_binaries.sha256')],check=True,
        stdout=(ROOT/'evidence/cpp_binary_verify.txt').open('w'))
    expected_server=next(line.split()[0] for line in (MATRIX/'evidence/cpp_binaries.sha256').read_text().splitlines() if line.split()[-1].endswith('/ariabc_pg_server'))
    actual_server=h.run_remote(DBHOST,'neel',f'sha256sum {DBMATRIX}/cluster/ariabc_pg/build/bin/ariabc_pg_server',timeout=60).stdout.split()[0]
    write(ROOT/'evidence/db_server_sha256.json',dict(expected=expected_server,actual=actual_server))
    if actual_server!=expected_server:raise RuntimeError('Remote C++ server digest differs from checked matrix binary')
    postgres_sha=next(line.split()[0] for line in (ROOT/'evidence/db_pg_binaries.sha256').read_text().splitlines() if line.split()[-1].endswith('/bin/postgres'))
    gateway_sha=next(line.split()[0] for line in (MATRIX/'evidence/cpp_binaries.sha256').read_text().splitlines() if line.split()[-1].endswith('/ariabc_pg_gateway'))
    args._executables=[dict(host=DBHOST,path=INSTALL+'/bin/postgres',sha256=postgres_sha),
        dict(host=DBHOST,path=DBMATRIX+'/cluster/ariabc_pg/build/bin/ariabc_pg_server',sha256=actual_server),
        dict(host='10.129.27.111',path=str(SOURCE/'ariabc_pg/build/bin/ariabc_pg_gateway'),sha256=gateway_sha)]
    write(ROOT/'evidence/executable_provenance.json',args._executables)
    # Verify compiled engine input against the recorded source manifest, and
    # retain any mismatch as a hard failure rather than blessing a stale build.
    source_check=remote('cd '+DBMATRIX+'/src\nsha256sum -c '+DBMATRIX+'/evidence/engine_source.sha256',timeout=60,check=False)
    (ROOT/'evidence/engine_source_verify.txt').write_text(source_check.stdout+source_check.stderr)
    if source_check.returncode:raise RuntimeError('engine source manifest mismatch')
    for name in ['observer.py','wal.py','io.bt','baseline_identity.py','waits.py']:
        subprocess.run(['scp','-o','ControlMaster=no',str(ROOT/'scripts'/name),'neel@'+DBHOST+':'+DBROOT+'/scripts/'+name],check=True)
    h.ensure_cache_drop_access(args)
    password_probe=h.run_remote(DBHOST,'neel',h.sudo_command('test -r /sys/kernel/btf/vmlinux; test -w /proc/sys/vm/drop_caches'),check=False)
    write(ROOT/'evidence/sudo_probe.json',dict(returncode=password_probe.returncode,stdout=password_probe.stdout,stderr=password_probe.stderr))
    if password_probe.returncode:raise RuntimeError('No sudo access for tracing/cache drop')
    # The install has an ELF symbol table but no DWARF. Struct offsets come
    # from offsetof() compiled against this install's own server headers (same
    # configure/pg_config.h), global addresses from the symbol table.
    program=f"""#include "postgres.h"
#include "storage/buf_internals.h"
#include "utils/rel.h"
#include "executor/instrument.h"
#include <stddef.h>
#undef printf
#include <stdio.h>
int main(void){{
printf("RELID=%zu\\n",offsetof(RelationData,rd_id));
printf("RELFILENODE=%zu\\n",offsetof(RelationData,rd_node.relNode));
printf("BD_SIZE=%zu\\n",sizeof(BufferDescPadded));
printf("BD_REL=%zu\\n",offsetof(BufferDesc,tag.rnode.relNode));
printf("BD_DB=%zu\\n",offsetof(BufferDesc,tag.rnode.dbNode));
printf("BD_FORK=%zu\\n",offsetof(BufferDesc,tag.forkNum));
printf("USAGEOFFSET=%zu\\n",offsetof(BufferUsage,shared_blks_dirtied));
return 0;}}
"""
    command=(f"cat > {DBROOT}/scratch/offsets.c <<'AUDIT_C_EOF'\n{program}AUDIT_C_EOF\n"
        f"gcc -O2 -D_GNU_SOURCE -I$({INSTALL}/bin/pg_config --includedir-server) -o {DBROOT}/scratch/offsets {DBROOT}/scratch/offsets.c\n"
        f"{DBROOT}/scratch/offsets\n"
        f"nm {INSTALL}/bin/postgres | while read a t n; do case $n in BufferDescriptors) echo BUFVAR=$((16#$a));; pgBufferUsage) echo USAGEVAR=$((16#$a));; esac; done\n")
    data=h.run_remote(DBHOST,'neel',command).stdout
    (ROOT/'evidence/relation_offsets.txt').write_text(data)
    offsets=dict(re.findall(r'([A-Z_]+)=(\d+)',data))
    if len(offsets)!=9:raise RuntimeError('Cannot resolve buffer/relation offsets from actual binary')
    write(ROOT/'evidence/offsets.json',offsets)
    elf=h.run_remote(DBHOST,'neel',f'readelf -h {INSTALL}/bin/postgres',timeout=60).stdout
    (ROOT/'evidence/postgres_elf.txt').write_text(elf)
    # Exact OID -> new relfilenode observations prevent a polling gap from
    # losing the identity of a component replaced quickly during a merge.
    addition=f'''\nuprobe:{INSTALL}/bin/postgres:RelationSetNewRelfilenode
/comm == "postgres"/ {{ @relation_pointer[tid] = arg0; }}
uretprobe:{INSTALL}/bin/postgres:RelationSetNewRelfilenode
/@relation_pointer[tid]/ {{
  $p = @relation_pointer[tid];
  @replacements[*(uint32 *)uptr($p+{offsets['RELID']}),*(uint32 *)uptr($p+{offsets['RELFILENODE']})] = count();
  delete(@relation_pointer[tid]);
}}
uprobe:{INSTALL}/bin/postgres:MarkBufferDirty
/comm == "postgres" && (int32)arg0 > 0/
{{
  $b = *(uint64 *)uptr(AUDIT_BUFVAR);
  @d_buf[tid] = $b + ((int32)arg0-1)*{offsets['BD_SIZE']};
  @d_start[tid] = *(uint64 *)uptr(AUDIT_USAGEVAR+{offsets['USAGEOFFSET']});
  @d_phase[tid] = @phase;
  @d_hint[tid] = 0;
}}
uprobe:{INSTALL}/bin/postgres:MarkBufferDirtyHint
/comm == "postgres" && (int32)arg0 > 0/
{{
  $b = *(uint64 *)uptr(AUDIT_BUFVAR);
  @d_buf[tid] = $b + ((int32)arg0-1)*{offsets['BD_SIZE']};
  @d_start[tid] = *(uint64 *)uptr(AUDIT_USAGEVAR+{offsets['USAGEOFFSET']});
  @d_phase[tid] = @phase;
  @d_hint[tid] = 1;
}}
uretprobe:{INSTALL}/bin/postgres:MarkBufferDirty,uretprobe:{INSTALL}/bin/postgres:MarkBufferDirtyHint
/@d_buf[tid]/
{{
  $b = @d_buf[tid]; $d = *(uint32 *)uptr($b+{offsets['BD_DB']});
  $r = *(uint32 *)uptr($b+{offsets['BD_REL']}); $f = *(uint32 *)uptr($b+{offsets['BD_FORK']});
  $p = @d_phase[tid]; $h = @d_hint[tid];
  $delta = *(uint64 *)uptr(AUDIT_USAGEVAR+{offsets['USAGEOFFSET']})-@d_start[tid];
  @dirty_calls[pid,$d,$r,$p,$f,$h] = count();
  if ($delta > 0) {{ @dirtied[pid,$d,$r,$p,$f,$h] = sum($delta); }}
  delete(@d_buf[tid]); delete(@d_start[tid]); delete(@d_phase[tid]); delete(@d_hint[tid]);
}}
'''
    trace=(ROOT/'scripts/io.bt').read_text()+addition
    h.run_remote(DBHOST,'neel',f"cat > {DBROOT}/scripts/io_effective.bt <<'AUDIT_BPF_EOF'\n{trace}\nAUDIT_BPF_EOF")
    calls='BEGIN { printf("AUDIT_TRACE_READY\\n"); }\n'
    for function,binary,arg in [('btinsert',INSTALL+'/bin/postgres',0),('_bt_doinsert',INSTALL+'/bin/postgres',0),
        ('btbeginscan',INSTALL+'/bin/postgres',0),
        ('index_beginscan',INSTALL+'/bin/postgres',1),('ml_insert',INSTALL+'/lib/postgresql/merkle_lsm.so',0),
        ('ml_beginscan',INSTALL+'/lib/postgresql/merkle_lsm.so',0)]:
        calls+=f'uprobe:{binary}:{function} /comm == "postgres"/ {{ @calls_{function}[pid,*(uint32 *)uptr(arg{arg}+{offsets["RELID"]})] = count(); }}\n'
    (ROOT/'scripts/calls.bt').write_text(calls)
    h.run_remote(DBHOST,'neel',f"cat > {DBROOT}/scripts/calls.bt <<'AUDIT_BPF_EOF'\n{calls}\nAUDIT_BPF_EOF")
    if not (ROOT/'evidence/baseline_data_identity.json').exists():
        h.run_remote(DBHOST,'neel',f'python3 {DBROOT}/scripts/baseline_identity.py',timeout=7200)
    identity=h.run_remote(DBHOST,'neel',f'cat {DBROOT}/evidence/baseline_data_identity.json',timeout=60).stdout
    (ROOT/'evidence/baseline_data_identity.json').write_text(identity)
    h.upload_probe(args)
    reuse=os.environ.get('AUDIT_CALIBRATION')
    if reuse:
        # Calibration on .247 needs 6 consecutive 10 s windows with <=2 MiB
        # device writes; since ~09:00 UTC a non-audit writer emits ~13 MiB every
        # 30-50 s. Reuse this audit's own earlier calibration of the same device
        # (same day); per-case settle still requires 3 idle windows.
        args._probe_thresholds=json.loads(Path(reuse).read_text())['thresholds']
        write(ROOT/'evidence/calibration_reused.json',dict(source=reuse,thresholds=args._probe_thresholds))
    else:
        args._probe_thresholds=h.calibrate_device(args,ROOT/'evidence')


def baseline_controls(h,label):
    command='\n'.join(f"sha256sum {POOL}/{name}/PG_VERSION {POOL}/{name}/global/pg_control\nstat -c '%n %s %Y' {POOL}/{name}/global/pg_control" for name in ['base_ff90_plain','base_ff90','base_ff90_mlsm'])
    data=h.run_remote(DBHOST,'neel',command).stdout
    (ROOT/'evidence'/f'baseline_controls_{label}.txt').write_text(data)
    return data


def obs(h,action,case,extra='',timeout=60):
    return h.run_remote(DBHOST,'neel',f"TMPDIR={DBROOT}/scratch python3 {DBROOT}/scripts/observer.py {action} --port {PORT} --case {case} {extra}",timeout=timeout).stdout


def snap(h,case,name):
    value=json.loads(obs(h,'snapshot',case))
    write(ROOT/'results'/case/(name+'.json'),value)
    return value


def marker(h,case,name,phase,markers,args):
    obs(h,'mark',case,f'--phase {phase}')
    markers[name]=dict(t=time.time(),monotonic=time.monotonic(),
        lsn=h.sql(args,'SELECT pg_current_wal_insert_lsn();'))
    write(ROOT/'results'/case/'markers.json',markers)


def start_trace(h,case,source='io_effective.bt',tag='io'):
    directory=DBROOT+'/results/'+case
    if source=='io_effective.bt':
        offsets=json.loads((ROOT/'evidence/offsets.json').read_text())
        pie='DYN' in (ROOT/'evidence/postgres_elf.txt').read_text()
        pgdata=POOL+('/pgdata_plain' if '_plain_' in case else '/pgdata_merkle')
        script=("from pathlib import Path; p=Path("+repr(pgdata+"/postmaster.pid")+
            ").read_text().splitlines()[0]; lines=Path('/proc/'+p+'/maps').read_text().splitlines(); "
            "print(next(int(s.split('-')[0],16) for s in lines if s.endswith("+repr(INSTALL+'/bin/postgres')+
            ") and s.split()[2]=='00000000'))")
        base=int(h.run_remote(DBHOST,'neel','python3 -c '+shlex.quote(script),timeout=30).stdout) if pie else 0
        # Reuse the effective source generated from actual DWARF offsets.
        effective=h.run_remote(DBHOST,'neel',f'cat {DBROOT}/scripts/io_effective.bt',timeout=30).stdout
        effective=effective.replace('AUDIT_BUFVAR',str(base+int(offsets['BUFVAR']))).replace('AUDIT_USAGEVAR',str(base+int(offsets['USAGEVAR'])))
        source='io_'+case+'.bt'
        h.run_remote(DBHOST,'neel',f"cat > {DBROOT}/scripts/{source} <<'AUDIT_BPF_EOF'\n{effective}\nAUDIT_BPF_EOF",timeout=30)
    command=(f"setsid nohup env TMPDIR={DBROOT}/scratch BPFTRACE_MAX_MAP_KEYS=65536 "
        f"bpftrace -q {DBROOT}/scripts/{source} > {directory}/{tag}.trace 2> {directory}/{tag}.stderr < /dev/null &\n"
        f"echo $! > {directory}/{tag}.pid")
    h.run_remote(DBHOST,'neel',h.sudo_command(command))
    for _ in range(60):
        r=h.run_remote(DBHOST,'neel',f"grep -q AUDIT_TRACE_READY {directory}/{tag}.trace && echo READY || true").stdout
        if 'READY' in r:return
        time.sleep(1)
    raise RuntimeError('BPF trace did not become ready; inspect '+directory+'/'+tag+'.stderr')


def stop_trace(h,case,tag='io'):
    directory=DBROOT+'/results/'+case
    command=f'''pid=$(cat {directory}/{tag}.pid)
test "$(basename "$(readlink /proc/$pid/exe)")" = bpftrace
tr '\\0' ' ' < /proc/$pid/cmdline | grep -F '{DBROOT}/scripts/' >/dev/null
kill -INT "$pid"
'''
    h.run_remote(DBHOST,'neel',h.sudo_command(command))
    for _ in range(60):
        r=h.run_remote(DBHOST,'neel',f"pid=$(cat {directory}/{tag}.pid); [ ! -d /proc/$pid ] || [ \"$(awk '{{print $3}}' /proc/$pid/stat)\" = Z ] && echo STOPPED || true").stdout
        if 'STOPPED' in r:return
        time.sleep(1)
    raise RuntimeError('trace did not exit')


def transfer(h,case):
    directory=ROOT/'results'/case
    # Raw compressed WAL stays on DB host; copy compact summaries and trace maps.
    subprocess.run(['rsync','-a','-e','ssh -o ControlMaster=no',
        '--exclude=waldump.txt.gz','neel@'+DBHOST+':'+DBROOT+'/results/'+case+'/',str(directory)+'/'],check=True)


def case_run(h,template,workload,kind,trial,workloads,diagnostic=False):
    case=f'{workload}_{kind}_t{trial}'+os.environ.get('AUDIT_CASE_SUFFIX','')
    directory=ROOT/'results'/case
    if (directory/'PASS.json').exists():log('SKIP_ALREADY_COMPLETE '+case);return
    if directory.exists() and any(directory.iterdir()):
        suffix='_interrupted_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        directory.rename(directory.with_name(case+suffix))
        h.run_remote(DBHOST,'neel',f"if [ -d {DBROOT}/results/{case} ]; then mv {DBROOT}/results/{case} {DBROOT}/results/{case}{suffix}; fi",timeout=30)
    directory.mkdir(exist_ok=True)
    args=copy.deepcopy(template)
    mode='bcdb_det' if kind=='plain' else 'bcdb_merkle'
    args.base_dir_name='base_ff90_mlsm' if kind=='lsm' else 'base_ff90'
    args.merkle_lookup_am='merkle_lsm' if kind=='lsm' else 'btree'
    args._golden_manifest=json.loads(h.run_remote(DBHOST,'neel','cat '+h.manifest_path_for(args,args.base_dir_name)).stdout)
    args._plain_manifest=json.loads(h.run_remote(DBHOST,'neel','cat '+h.manifest_path_for(args,'base_ff90_plain')).stdout)
    log('CASE_START '+case)
    foreign=h.run_remote(DBHOST,'neel','ps -C postgres -o pid=,args= || true',timeout=30).stdout.strip()
    if foreign:raise RuntimeError('DB host is occupied before reset; refusing to touch any PostgreSQL: '+foreign)
    h.run_remote(DBHOST,'neel',f"test \"$(df -B1 --output=avail {POOL} | tail -1)\" -ge 30000000000\nmkdir -p {DBROOT}/results/{case}")
    traced=False;observing=False
    try:
        setup=h.reset_remote_pgdata(args,mode,16)
        write(directory/'setup.json',setup)
        # Keep every WAL segment of this case until wal.py has parsed it: the
        # explicit CHECKPOINT and the shutdown checkpoint would otherwise recycle
        # the foreground range. This 13devel build has wal_keep_segments (SIGHUP), 2560 x 16MB = 40GB.
        h.sql(args,"ALTER SYSTEM SET wal_keep_segments=2560;")
        h.sql(args,'SELECT pg_reload_conf();')
        time.sleep(1)
        (directory/'wal_keep_segments.txt').write_text(h.sql(args,'SHOW wal_keep_segments;')+'\n')
        # Optional merkle_lsm geometry for this case, applied to the restored working copy only.
        # The merger reads level sizes from the meta page written at build time, so the
        # reloptions only take effect after REINDEX; CHECKPOINT so the run starts clean.
        (directory/'install.txt').write_text(h.run_remote(DBHOST,'neel',f'sha256sum {INSTALL}/bin/postgres {INSTALL}/lib/postgresql/merkle_lsm.so',timeout=60).stdout+
            (h.sql(args,"SELECT coalesce(current_setting('merkle_lsm.bulk_merge',true),'unset');") if kind=='lsm' else '')+'\n')
        relopts=os.environ.get('AUDIT_LSM_RELOPTS')
        if kind=='lsm' and relopts:
            t=time.monotonic()
            h.sql(args,f"ALTER INDEX usertable_merkle_lookup_idx SET ({relopts});",timeout=600)
            h.sql(args,"SET maintenance_work_mem='1GB'; REINDEX INDEX usertable_merkle_lookup_idx;",timeout=7200)
            h.sql(args,'CHECKPOINT;',timeout=7200)
            (directory/'lsm_relopts.txt').write_text(
                h.sql(args,"SELECT reloptions FROM pg_class WHERE relname='usertable_merkle_lookup_idx';")+'\n'+
                h.sql(args,"SELECT merkle_lsm_config('usertable_merkle_lookup_idx');")+'\n'+
                'reindex_and_checkpoint_s=%.1f\n'%(time.monotonic()-t))
        # levels can only change through DROP + CREATE: "levels,top_index_size_kib,size_ratio".
        recreate=os.environ.get('AUDIT_LSM_RECREATE')
        if kind=='lsm' and recreate:
            t=time.monotonic()
            levels,top,ratio=(int(x) for x in recreate.split(','))
            h.sql(args,"SET maintenance_work_mem='1GB'; DROP INDEX usertable_merkle_lookup_idx; "
                f"SELECT merkle_create_lookup_index('usertable','merkle_lsm',{levels},{top},{ratio});",timeout=7200)
            h.sql(args,'CHECKPOINT;',timeout=7200)
            (directory/'lsm_relopts.txt').write_text(
                h.sql(args,"SELECT merkle_lsm_config('usertable_merkle_lookup_idx');")+'\n'+
                'recreate_and_checkpoint_s=%.1f\n'%(time.monotonic()-t))
        # Ablation: Merkle without any lookup index (no split runs in these workloads;
        # merkle_verify scans the heap). Working copy only.
        if kind!='plain' and os.environ.get('AUDIT_DROP_LOOKUP')=='1':
            h.sql(args,'DROP INDEX usertable_merkle_lookup_idx;',timeout=600)
            h.sql(args,'CHECKPOINT;',timeout=7200)
            (directory/'lookup_dropped.txt').write_text(
                h.sql(args,"SELECT count(*) FROM pg_class WHERE relname='usertable_merkle_lookup_idx';")+'\n')
        before=snap(h,case,'before')
        start_trace(h,case);traced=True
        command=f"setsid nohup env TMPDIR={DBROOT}/scratch python3 -u {DBROOT}/scripts/observer.py monitor --port {PORT} --case {case} --pgdata {h.pgdata(args)} > {DBROOT}/results/{case}/observer.log 2>&1 < /dev/null &\necho $! > {DBROOT}/results/{case}/observer.pid"
        h.run_remote(DBHOST,'neel','rm -f '+DBROOT+'/results/'+case+'/observer.stop\n'+command)
        observing=True
        server=h.start_remote_ariabc_server(args,mode,16)
        write(directory/'server_command.json',server)
        time.sleep(1)
        pg_before=h.get_pg_io_stats(args);device_before=h.get_remote_nvme_stats(args)
        markers={};marker(h,case,'foreground',1,markers,args)
        profile=os.environ.get('AUDIT_PROFILE')=='1'
        if profile:
            # Wait events every 100 ms for the whole transaction window, plus a
            # system-wide 60 s CPU profile starting 120 s into the run.
            d=f'{DBROOT}/results/{case}'
            h.run_remote(DBHOST,'neel',f"rm -f {d}/waits.stop\nsetsid nohup env TMPDIR={DBROOT}/scratch python3 {DBROOT}/scripts/waits.py "
                f"--port {PORT} --out {d}/waits.json --stop {d}/waits.stop > {d}/waits.log 2>&1 < /dev/null &")
            h.run_remote(DBHOST,'neel',h.sudo_command(f"setsid nohup sh -c 'sleep 120; perf record -a -F 499 -o {d}/perf.data -- sleep 60; "
                f"chown neel:neel {d}/perf.data' > {d}/perf.log 2>&1 < /dev/null &"))
        metrics=h.run_local_gateway_benchmark(args,Path(workloads[workload]['path']),mode,16,directory/'gateway.log')
        if profile:
            h.run_remote(DBHOST,'neel',f"touch {d}/waits.stop; for i in $(seq 60); do test -s {d}/waits.json && break; sleep 1; done")
        marker(h,case,'drain',2,markers,args)
        after_run=snap(h,case,'after_run')
        device_after=h.get_remote_nvme_stats(args)
        h.stop_server(args)
        t=time.monotonic()
        if kind=='lsm':h.sql(args,"SELECT merkle_lsm_flush('usertable_merkle_lookup_idx');",timeout=7200)
        drain_rpc=time.monotonic()-t
        drain=h._last_remote_execution_seconds if kind=='lsm' else 0
        marker(h,case,'checkpoint',3,markers,args)
        after_drain=snap(h,case,'after_drain')
        t=time.monotonic();h.sql(args,'CHECKPOINT;',timeout=7200);checkpoint_rpc=time.monotonic()-t
        checkpoint=h._last_remote_execution_seconds
        marker(h,case,'shutdown',4,markers,args)
        device_checkpoint=h.get_remote_nvme_stats(args)
        snap(h,case,'after_checkpoint')
        h.run_remote(DBHOST,'neel','touch '+DBROOT+'/results/'+case+'/observer.stop')
        time.sleep(1);observing=False
        h.stop_postgres(args)
        # No SQL after stop: the last inserted checkpoint WAL is identified by
        # pg_controldata's checkpoint location plus waldump; the marker below is
        # taken after restart, outside all file-I/O phases.
        stop_trace(h,case);traced=False
        h.start_postgres(args)
        marker(h,case,'end',0,markers,args)
        after=snap(h,case,'after_restart')
        if kind=='lsm':
            # Inserts only: every heap row must have exactly one lookup entry after the drain (untraced).
            entries=h.sql(args,"SELECT sum(entries) FROM merkle_lsm_components('usertable_merkle_lookup_idx');",timeout=7200)
            rows=h.sql(args,'SELECT count(*) FROM usertable;',timeout=7200)
            (directory/'lsm_entry_check.txt').write_text(f'lookup_entries={entries}\nheap_rows={rows}\nequal={entries==rows}\n')
        pg_after=h.get_pg_io_stats(args)
        write(directory/'io.json',dict(pg_before=pg_before,pg_after=pg_after,
            device_before=device_before,device_after=device_after,device_checkpoint=device_checkpoint))
        # Archive/analyse WAL before any reset can recycle it. This processing
        # runs on the DB host after the benchmark, never on the workstation.
        h.run_remote(DBHOST,'neel',f"cat > {DBROOT}/results/{case}/markers.json <<'AUDIT_JSON_EOF'\n"+
            json.dumps(markers)+'\nAUDIT_JSON_EOF')
        command=f"TMPDIR={DBROOT}/scratch python3 {DBROOT}/scripts/wal.py --directory {DBROOT}/results/{case} --waldump {INSTALL}/bin/pg_waldump --wal {h.pgdata(args)}/pg_wal"
        h.run_remote(DBHOST,'neel',command,timeout=7200)
        total_cost=metrics['wall_time_ms']/1000+drain+checkpoint
        write(directory/'timing.json',dict(gateway_s=metrics['wall_time_ms']/1000,drain_s=drain,
            checkpoint_s=checkpoint,drain_rpc_s=drain_rpc,checkpoint_rpc_s=checkpoint_rpc,
            run_drain_checkpoint_s=total_cost,total_tps=N/total_cost,
            continuous_bracket_s=markers['shutdown']['monotonic']-markers['foreground']['monotonic'],
            continuous_bracket_tps=N/(markers['shutdown']['monotonic']-markers['foreground']['monotonic']),
            markers=markers))
        h.collect_logs(args,directory)
        validation=h.validate_ycsb_results((directory/'server.log').read_text(),N,mode=mode)
        counts=h.sql(args,"SELECT count(*),min(ycsb_key),max(ycsb_key) FROM usertable;",timeout=7200)
        expected=f'{100000000+N}|1|{100000000+N}' if workload=='insert' else '100000000|1|100000000'
        if counts!=expected:raise RuntimeError('Wrong final row counts: '+counts+' expected '+expected)
        write(directory/'row_counts.json',dict(actual=counts,expected=expected))
        verify='N/A'
        if kind!='plain':
            verify=h.sql(args,"SELECT merkle_verify('usertable');",timeout=7200)
            if verify!='t':raise RuntimeError('Merkle verification failed: '+verify)
            root=h.sql(args,"SELECT merkle_root_hash('usertable');")
            (directory/'merkle_root.txt').write_text(root+'\n')
        (directory/'merkle_verify.txt').write_text(verify+'\n')
        if kind=='lsm' and workload=='insert':
            a=before['lsm']['merges'];b=after_run['lsm']['merges'];z=after_drain['lsm']['merges']
            if b[0]-a[0]<3 or z[1]-a[1]<1:
                raise RuntimeError('Sustained-merge requirement not met: '+str((a,b,z)))
        if diagnostic:
            start_trace(h,case,'calls.bt','calls')
            try:
                if kind!='plain':obs(h,'probes',case,timeout=7200)
                obs(h,'plans',case,'--pgdata '+h.pgdata(args),timeout=7200)
            finally:stop_trace(h,case,'calls')
            if kind!='plain':
                verify=h.sql(args,"SELECT merkle_verify('usertable');",timeout=7200)
                (directory/'post_diagnostic_verify.txt').write_text(verify+'\n')
                if verify!='t':raise RuntimeError('Post-diagnostic verify failed')
        h.stop_postgres(args)
        transfer(h,case)
        # Any BPF lost-event/map error invalidates file-offset evidence; preserve
        # it explicitly instead of silently treating absent records as zero.
        stderr=(directory/'io.stderr').read_text()
        if re.search(r'ERROR|lost|map.*(full|fail)',stderr,re.I):
            raise RuntimeError('BPF trace reported an error: '+stderr[:1000])
        subprocess.run(['python3',str(ROOT/'scripts/summarize.py'),'--case',case],check=True)
        write(directory/'PASS.json',dict(case=case,workload=workload,kind=kind,trial=trial,
            metrics=metrics,validation=validation,counts=counts,merkle_verify=verify,
            workload_sha256=workloads[workload]['sha256']))
        log('CASE_PASS '+case+f' TPS={metrics["tps"]:.2f} total_TPS={N/total_cost:.2f}')
    except Exception:
        (directory/'FAILED.txt').write_text(traceback.format_exc())
        raise
    finally:
        if observing:h.run_remote(DBHOST,'neel','touch '+DBROOT+'/results/'+case+'/observer.stop',check=False)
        if traced:
            try:stop_trace(h,case)
            except Exception:log('TRACE_CLEANUP_FAILED '+case)
        h.stop_server(args);h.stop_postgres(args)


def main():
    for name in ['scripts','scratch','evidence','workloads','results']:(ROOT/name).mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'controller.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    os.environ['TMPDIR']=str(ROOT/'scratch');os.environ['PYTHONDONTWRITEBYTECODE']='1'
    gate()
    h=load_harness();args=make_args(h)
    prepare(h,args)
    original=baseline_controls(h,'before')
    workloads=generate()
    subprocess.run(['scp','-o','ControlMaster=no',str(ROOT/'scripts/examples.json'),'neel@'+DBHOST+':'+DBROOT+'/scripts/examples.json'],check=True)
    only=os.environ.get('AUDIT_ONLY')
    if only:
        # Smoke: workload:kind:trial:diagnostic, comma separated.
        for item in only.split(','):
            workload,kind,trial,diag=item.split(':')
            case_run(h,args,workload,kind,int(trial),workloads,diagnostic=diag=='1')
        log('AUDIT_SMOKE_COMPLETE');return
    for trial,order in [(1,['plain','btree','lsm']),(2,['lsm','btree','plain'])]:
        for workload in ['insert','update']:
            for kind in order:
                case_run(h,args,workload,kind,trial,workloads,diagnostic=(trial==2 and workload=='update'))
    final=baseline_controls(h,'after')
    if final!=original:raise RuntimeError('Immutable baseline controls changed')
    log('AUDIT_COMPLETE')


if __name__=='__main__':
    try:main()
    except BaseException:
        log('AUDIT_FAILED '+traceback.format_exc())
        raise
