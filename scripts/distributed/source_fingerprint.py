#!/usr/bin/env python3
"""Portable identity of build inputs, excluding generated PostgreSQL outputs.

The gateway builds out of tree after receiving an in-tree checkout. Generated
headers, and absolute include aliases repaired during that build, cannot identify
its source. Their generators/templates are included instead. Executable hashes
are verified separately by the cluster runner.
"""
import argparse
import hashlib
import os
from pathlib import Path


GENERATED_NAMES = {
    'schemapg.h', 'errcodes.h', 'fmgroids.h', 'fmgrprotos.h',
    'lwlocknames.h', 'lwlocknames.c', 'probes.h', 'plerrcodes.h',
    'pg_config.h', 'pg_config_ext.h', 'pg_config_os.h',
    'ecpg_config.h', 'pg_config_paths.h', 'objfiles.txt',
}
GENERATED_ALIASES = {'src/include/parser/gram.h'}
SUFFIXES = {'.c', '.cpp', '.cxx', '.h', '.hpp', '.hxx', '.l', '.y',
            '.pl', '.in', '.mk', '.cmake', '.txt'}
BUILD_SCRIPTS = (
    'configure', 'configure.in', 'aclocal.m4', 'GNUmakefile.in',
    'scripts/distributed/ensure_custom_install_from_repo.sh',
    'scripts/distributed/run_4node_raft_cluster.sh',
    'scripts/distributed/source_fingerprint.py',
)


def inputs(root):
    root = Path(root)
    paths = set()
    for base in ('src', 'ariabc_pg', 'NuRaft', 'config'):
        for directory, dirs, files in os.walk(root / base):
            dirs[:] = [d for d in dirs if d not in ('.git', '.bench_tmp', 'CMakeFiles')
                       and not d.startswith(('build', 'cmake-build'))]
            for name in files:
                path = Path(directory) / name
                relative = path.relative_to(root).as_posix()
                if name in GENERATED_NAMES or name.endswith('_d.h') or relative in GENERATED_ALIASES:
                    continue
                if path.suffix in SUFFIXES or name in ('Makefile', 'GNUmakefile'):
                    paths.add(relative)
    paths.update(p for p in BUILD_SCRIPTS if (root / p).is_file())
    return sorted(paths)


def fingerprint(root, capacity, listing=False):
    digest = hashlib.sha256()
    digest.update(('source-inputs-v2\nRESULT_RING_CAPACITY=' + str(capacity) + '\n').encode())
    for relative in inputs(root):
        # Missing real inputs are errors; silently dropping a dangling source
        # link would turn an incomplete sync into an accepted source identity.
        value = hashlib.sha256((Path(root) / relative).read_bytes()).hexdigest()
        line = value + '  ' + relative + '\n'
        digest.update(line.encode())
        if listing:
            print(line, end='')
    return digest.hexdigest()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--ring-capacity', type=int, required=True)
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    print(fingerprint(args.repo, args.ring_capacity, args.list))
