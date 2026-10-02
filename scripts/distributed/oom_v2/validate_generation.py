#!/usr/bin/env python3
"""On the remote controller, syntax-check generated commands without executing them."""
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_oom_100m_benchmark as runner

args = runner.parse_args(sys.argv[1:])
commands = []


def capture(host, user, command, **kwargs):
    commands.append(command)
    subprocess.run(['bash', '-n'], input=command, text=True, check=True)
    if 'stream_100m.py\n' in command:
        helper = command.split("<< 'EOF'", 1)[1].split('\n', 1)[1].rsplit('\nEOF', 1)[0]
        compile(helper, 'generated_stream_100m.py', 'exec')
    return SimpleNamespace(stdout='', stderr='', returncode=0)


runner.run_remote = capture
runner.generate_remote_100m_database(args)
setup = next(cmd for cmd in commands if '[1/6]' in cmd)
index = next(cmd for cmd in commands if '[4/6]' in cmd)
assert 'WITH (fillfactor = 90)' in setup
assert 'shared_buffers = 512MB' in setup
assert 'maintenance_work_mem = 1GB' in setup
assert 'max_parallel_maintenance_workers = 2' in setup
assert "default_transaction_isolation = 'serializable'" in setup
assert 'merkle_apply_synchronous_direct = on' in setup
assert 'split_threshold = 1024, merge_threshold = 256' in index
assert 'available < 12288 * 1024' in index
print('REMOTE_GENERATED_COMMAND_SYNTAX_PASS; no database or generation commands executed')
