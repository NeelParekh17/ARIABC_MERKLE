#!/usr/bin/env python3
"""Deploy only fresh campaign paths; never overwrite canonical instances."""
import argparse
import os
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    a = p.parse_args()
    b = a.base.resolve()
    os.environ['PATH'] = str(b / 'tools') + ':' + os.environ['PATH']
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('deploy from .247')
    if (b / 'build_status.txt').read_text().strip() != 'BUILD_READY':
        raise RuntimeError('build incomplete')
    for h in ('10.129.148.247', '10.129.148.246', '10.129.148.248'):
        with (b / 'provenance' / ('deploy_' + h + '.log')).open('x') as log:
            def run(args, **kw):
                subprocess.run(args, check=True, stdout=log, stderr=subprocess.STDOUT, **kw)
            if not h.endswith('.247'):
                run(['ssh', 'neel@' + h, f'test ! -e {b}; mkdir -p {b}/repo {b}/provenance {b}/pids {b}/logs {b}/scratch'])
                for name in ('repo', 'install', 'rdkafka', 'u22_build'):
                    run(['rsync', '-az', '--exclude=.bench_tmp', '--exclude=scripts/bench_full_results',
                         str(b / name) + '/', f'neel@{h}:{b}/{name}/'])
                for name in ('stop_server.sh', 'host_sample.sh', 'host_probe.py', 'geometry.sql'):
                    run(['rsync', '-a', str(b / name), f'neel@{h}:{b}/'])
                for name in ('BINARIES.sha256', 'git_head.txt', 'workspace_status.txt'):
                    run(['rsync', '-a', str(b / 'provenance' / name), f'neel@{h}:{b}/provenance/'])
            run(['ssh', 'neel@' + h, 'bash -s'], input=(b / 'init_node.sh').read_text(), text=True)
        print('DEPLOY_READY', h, flush=True)
    (b / 'DEPLOY_READY').write_text('three isolated instances initialized; binary hashes verified\n')


if __name__ == '__main__':
    main()
