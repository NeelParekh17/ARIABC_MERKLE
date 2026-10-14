#!/usr/bin/env python3
"""Hash a transportable evidence set; bulk traces remain in the raw campaign."""
import argparse
import hashlib
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    a = p.parse_args()
    raw = a.base / 'repo/scripts/bench_full_results'
    count = 0
    for out in sorted(raw.glob('cluster4_fanout_*')):
        if not (out / 'acceptance.json').exists():
            continue
        lines = []
        for path in sorted(out.rglob('*')):
            if not path.is_file() or path.name == 'CURATED.sha256':
                continue
            if path.name.startswith(('tx_latency', 'bcdb_phase_trace_node')):
                continue
            digest = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
            lines.append(digest + '  ' + str(path.relative_to(out)))
        manifest = '\n'.join(lines) + '\n'
        target = out / 'CURATED.sha256'
        if target.exists() and target.read_text() != manifest:
            raise RuntimeError('completed evidence changed: ' + str(out))
        if not target.exists():
            target.write_text(manifest)
        count += 1
    print(f'{count} run evidence manifests preserved')


if __name__ == '__main__':
    main()
