#!/usr/bin/env python3
"""Generate campaign-only copies of the frozen harness and SQL injector."""
import difflib
from pathlib import Path
import re
import subprocess

B = Path('/home/neel/ariabc_data/paper_20261010_recov')
repo = B / 'repo'
tag = 'paper_20261010_recov'
original = repo / 'scripts/distributed/recovery_s1024/prepare_runner.py'
generator = original.read_text().replace('base = "/home/neel/Desktop/recovery_s1024_" + a.tag', 'base = ' + repr(str(B)))
copy = B / 'prepare_runner_campaign.py'
copy.write_text(generator)
subprocess.run(['python3', str(copy), '--repo', str(repo), '--tag', tag], check=True)
p = repo / ('scripts/distributed/recovery_s1024/cluster_runner_' + tag + '.sh')
old = p.read_text()
new = old
# Separate colocated Kafka ports preserve canonical/shared brokers.
new = new.replace(':9092', ':9192').replace(':9093', ':9193')
anchor='        if [[ "$progress_made" -eq 0 ]]; then\n'
assert new.count(anchor)==1
progress='''        # An online repair can keep majority completions constant while
        # the damaged replica replays. Count its committed watermark progress.
        for idx in "${!NODE_IDS[@]}"; do
          val="${current_nodes_committed[$idx]}"
          if [[ "$val" =~ ^[0-9]+$ ]] && [[ "$val" -gt "${last_nodes_committed[$idx]}" ]]; then
            progress_made=1
          fi
        done
'''
new=new.replace(anchor,progress+anchor)
new=new.replace('WATCHDOG: Completed progress advanced', 'WATCHDOG: Gateway or replica progress; completed')
anchor='  wait "$GW_PID" || GW_RC=$?\n'
assert new.count(anchor)==1
new=new.replace(anchor,anchor+'  printf "gateway_exit_code=%s\\n" "$GW_RC" > "$LOG_DIR/gateway_exit.env"\n')
for a, b in [('/home/neel/ARIABC/AriaBC', str(repo)), ('/home/neel/ARIABC/install', str(B/'install')),
             ('/work/ARIABC/install', str(B/'install')), ('/home/neel/ariabc_raft_data', str(B/'raft'))]:
    new = new.replace(a,b)
# Pin persistent tool/data directories even in diagnostics; no /tmp artifacts.
new = new.replace('REMOTE_DATA_ROOT="${REMOTE_DATA_ROOT:-/home/neel/ariabc_data}"',
                  'REMOTE_DATA_ROOT="' + str(B) + '"')
lines=[]
kills=0
for line in new.splitlines(True):
    if 'fuser -k' in line and not line.lstrip().startswith('#'):
        line=re.match(r'\s*',line)[0]+"bash '"+str(B/'stop_server.sh')+"'\n"
        kills+=1
    lines.append(line)
assert kills == 7, kills
new=''.join(lines)
start=new.index('    hard_stop_benchmark_postgres() {')
end=new.index('    # -----------------------------------------------------------------------',start)
new=new[:start]+'''    hard_stop_benchmark_postgres() {
      # Existing pg_ctl is scoped to our PGDATA; no user-wide cleanup.
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
p.write_text(new)
(B/'guardrail_adaptation.diff').write_text(''.join(difflib.unified_diff(old.splitlines(True),new.splitlines(True))))
(B/'generator_adaptation.diff').write_text(''.join(difflib.unified_diff(original.read_text().splitlines(True),generator.splitlines(True))))
subprocess.run(['bash','-n',str(p)],check=True)
injector=p.parent/'serializable_fault_injector.py'
before=injector.read_text()
assert before.count('time.sleep(0.2 * attempt)')==1
after=before.replace('time.sleep(0.2 * attempt)', 'time.sleep(random.uniform(0.0, min(2.0, 0.2 * (2 ** (attempt - 1)))))')
injector.write_text(after)
(B/'injector_jitter.diff').write_text(''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True))))
print('campaign prepared', p)
