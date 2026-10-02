set -e
B='@RBASE@'
python3 - "$B" <<'PY'
import pathlib,sys,difflib
b=pathlib.Path(sys.argv[1]);p=b/'repo/scripts/distributed/recovery_s1024/cluster_runner_20261001T060514ZJ.sh';old=p.read_text();s=old
for name,file in [('WATCHDOG_PID','watchdog.pid'),('FAULT_INJECT_PID','fault_injector.pid')]:
 anchor=name+'=$!';assert s.count(anchor)==1;s=s.replace(anchor,anchor+'\n  echo "$'+name+'" > "$LOG_DIR/'+file+'"')
(b/'pid_recording.diff').write_text(''.join(difflib.unified_diff(old.splitlines(True),s.splitlines(True))))
p.write_text(s)
PY
bash -n "$B/repo/scripts/distributed/recovery_s1024/cluster_runner_20261001T060514ZJ.sh"
