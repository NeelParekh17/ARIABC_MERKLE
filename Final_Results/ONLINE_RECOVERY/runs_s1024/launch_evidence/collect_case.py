import pathlib,shlex,sys
import remote
B=remote.B
name=sys.argv[1]
assert name.startswith('cluster4_s1024_'+remote.C['tag']+'_')
out=B/'runs'/name
out.mkdir(parents=True,exist_ok=False)
for label,source,dest in [
    ('run',remote.C['base']+'/repo/scripts/bench_full_results/'+name+'/',str(out)+'/'),
    ('audit',remote.C['base']+'/command_audit/',str(B/'nested_commands')+'/')]:
    args=['rsync','-az','-e','ssh -o BatchMode=yes -o ConnectTimeout=12','neel@10.129.27.111:'+source,dest]
    if remote.run('10.129.27.111',shlex.join(args),'collect_'+name+'_'+label,argv=args):raise SystemExit(1)
import merge_audit
