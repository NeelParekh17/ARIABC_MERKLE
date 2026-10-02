import pathlib, difflib, re
B=pathlib.Path('/home/neel/Desktop/recovery_s1024_20261001T060514ZJ')
P=B/'repo/scripts/distributed/recovery_s1024/cluster_runner_20261001T060514ZJ.sh'
old=P.read_text();new=old
new=new.replace('/tmp/recovery_s1024_20261001T060514ZJ_u22_build',str(B/'u22_build'))
new=new.replace('/tmp/recovery_s1024_20261001T060514ZJ',str(B/'logs'))
for v in ('server_node','postgres_node'):
    new=new.replace('/tmp/'+v,str(B/'logs'/v))
new=new.replace('/home/neel/ARIABC/AriaBC',str(B/'repo')).replace('/home/neel/ARIABC/install',str(B/'install'))
new=new.replace('/work/ARIABC/install',str(B/'install'))
new=new.replace('/home/neel/ariabc_raft_data',str(B/'raft'))
lines=[]; kills=0
for line in new.splitlines(True):
    if 'fuser -k' in line and not line.lstrip().startswith('#'):
        line=re.match(r'\s*',line)[0]+"bash '"+str(B/'stop_server.sh')+"'\n";kills+=1
    lines.append(line)
assert kills==7,kills
new=''.join(lines)
start=new.index('    hard_stop_benchmark_postgres() {')
end=new.index('    # -----------------------------------------------------------------------',start)
new=new[:start]+'''    hard_stop_benchmark_postgres() {
      # pg_ctl handles a stale pid file; refuse arbitrary process cleanup.
      return 0
    }
'''+new[end:]
anchor="\t    nohup '$srv_bin'"
assert new.count(anchor)==1
new=new.replace(anchor,"\t    mkdir -p '"+str(B/'pids')+"'\n\t    cd '"+str(B)+"'\n"+anchor)
anchor=r'    echo \"started pid=\$!\"'
assert new.count(anchor)==1
new=new.replace(anchor,"    echo \\$! > '"+str(B/'pids/ariabc_server.pid')+"'\n"+anchor)
assert all('fuser -k' not in l and 'pkill ' not in l for l in new.splitlines() if not l.lstrip().startswith('#'))
P.write_text(new)
(B/'guardrail_adaptation.diff').write_text(''.join(difflib.unified_diff(old.splitlines(True),new.splitlines(True))))
print('adapted PID cleanup; RBASE logs; RBASE server cwd')
