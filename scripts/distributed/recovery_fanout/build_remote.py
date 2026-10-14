#!/usr/bin/env python3
"""Build the staged snapshot in the existing Ubuntu 22 image, only on .247."""
import argparse
import json
from pathlib import Path
import subprocess
import pexpect


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--attempt', type=int, default=1)
    a = p.parse_args()
    base = a.base.resolve()
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('build only on .247')
    password = subprocess.check_output([str(base / 'tools/askpass')], text=True).strip()
    image = 'ariabc-recov-paper-20261010:u22'

    def sudo(args, logfile):
        child = pexpect.spawn('sudo', ['-S', '-p', 'FANOUT_SUDO_PASSWORD:', *args],
                              encoding='utf-8', timeout=None, echo=False)
        # Record received output only; passwords are never logged.
        with (base / (logfile + f'.attempt{a.attempt}')).open('x') as f:
            child.logfile_read = f
            while True:
                which = child.expect(['FANOUT_SUDO_PASSWORD:', pexpect.EOF])
                if which == 1:
                    break
                child.sendline(password)
        child.close()
        if child.exitstatus != 0:
            raise RuntimeError(f'build command failed: {logfile}, exit={child.exitstatus}, signal={child.signalstatus}')

    (base / 'build_status.txt').write_text('STARTED\n')
    sudo(['docker', 'image', 'inspect', image], 'container_image.json')
    sudo(['docker', 'run', '--rm', '--network', 'host', '--user', '1003:1003',
          '-v', f'{base}:{base}', '-w', str(base / 'repo'), '-e', f'TMPDIR={base}/scratch',
          image, 'bash', str(base / 'build_inside.sh')], 'build_container.log')
    (base / 'build_status.txt').write_text('BUILD_READY\n')
    print('BUILD_READY', flush=True)


if __name__ == '__main__':
    main()
