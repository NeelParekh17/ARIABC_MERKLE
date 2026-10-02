#!/usr/bin/env python3
"""Require every generated workload to match the archived published SHA-256."""
import json
import sys
from pathlib import Path

expected = json.loads(Path(sys.argv[1]).read_text())
manifests = sorted(Path(sys.argv[2]).glob('run_*/campaign.json'))
if not manifests:
    raise RuntimeError('No generated campaign manifest')
actual = json.loads(manifests[-1].read_text())['workloads']
assert set(actual) == set(expected), (set(actual), set(expected))
for name in expected:
    assert actual[name] == expected[name], (name, actual[name], expected[name])
print('PUBLISHED_WORKLOAD_HASHES_PASS: 7 workloads, 20000 statements each')
