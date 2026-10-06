#!/usr/bin/env python3
"""Hash source changes, contract inputs and actual executables for each attempt."""
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    src = Path(os.environ['SRC'])
    run = Path(sys.argv[1])
    binaries = [Path(os.environ['INST'])/'bin'/n for n in
                ('postgres', 'psql', 'pg_waldump', 'initdb', 'pg_ctl')]
    binaries += [Path(os.environ['BINDIR'])/n for n in ('ariabc_pg_server', 'ariabc_pg_gateway')]
    if (src/'.git').exists():
        diff = subprocess.run(['git', '-C', str(src), 'diff', '--binary', 'HEAD'], capture_output=True, check=True).stdout
        head = subprocess.check_output(['git', '-C', str(src), 'rev-parse', 'HEAD'], text=True).strip()
        status = subprocess.check_output(['git', '-C', str(src), 'status', '--porcelain'], text=True)
    else:
        # Staged snapshot without .git: the integrator records HEAD and the working diff beside it.
        staged = src.parent
        head = (staged/'tpcc-v3-A-head.txt').read_text().strip()
        diff = (staged/'tpcc-v3-A-working.diff').read_bytes()
        status = 'staged snapshot (no .git); diff from ' + str(staged/'tpcc-v3-A-working.diff')
    (run/'source.diff').write_bytes(diff)
    files = sorted((src/'scripts'/'tpcc_v3').glob('*')) + sorted((src/'scripts'/'distributed'/'tpcc_v3').glob('*'))
    files += [src/'ariabc_pg'/'src'/n for n in ('ariabc_pg_gateway.cxx', 'pg_executor.cxx')]
    out = dict(git_head=head, status=status, diff_sha256=hashlib.sha256(diff).hexdigest(),
               binaries={str(p): digest(p) for p in binaries},
               inputs={str(p.relative_to(src)): digest(p) for p in files if p.is_file()},
               note='Hashes identify runtime files; require integrator build log/source parity before publication.')
    (run/'provenance.json').write_text(json.dumps(out, indent=2)+'\n')


if __name__ == '__main__':
    main()
