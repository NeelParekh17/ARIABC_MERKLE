import subprocess,sys
from execute_helpers import B,BASE,TAG,status,command,readscript
import remote
cases=[(32,1024,256,n,3 if n=='A' else 1) for n in ['A','B','C','M_mixed','L_mix_prio']]+[(4,32,8,n,1) for n in ['A','C','M_mixed']]
try:
    for f,s,m,name,attempt in cases:
        rid=f'cluster4_s1024_{TAG}_f{f}s{s}_{name}_r{attempt}'
        status('RUNNING '+rid)
        rc=remote.run('10.129.27.111',f"bash '{BASE}/run_case.sh' {f} {s} {m} {name} {attempt} < /dev/null",'run_'+rid)
        if subprocess.run([sys.executable,str(B/'collect_case.py'),rid],cwd=B.parent.parent).returncode:raise RuntimeError('artifact collection '+rid)
        if rc:raise RuntimeError('run/collection rc='+str(rc)+' '+rid)
        if subprocess.run([sys.executable,str(B/'validate_runs.py')],cwd=B.parent.parent).returncode:raise RuntimeError('acceptance '+rid)
        status('PASS '+rid)
    for host in ['10.129.148.247','10.129.148.246','10.129.148.248','10.129.27.111']:
        command(host,readscript('canonical_inventory.sh'),'canonical_after_'+host)
    status('EIGHT_CASES_COMPLETED_AND_ACCEPTED')
except Exception as e:
    status('STOPPED_ON_FAILURE '+str(e));raise
