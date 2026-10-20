#!/usr/bin/env python3
"""Validate the visual document and its evidence snapshot on the lab host.

Checks documentation artifacts and selected source facts, not database safety.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from PIL import Image


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    doc = root / 'doc/LSM_LOOKUP_ARCHITECTURE.md'
    text = doc.read_text()
    detailed = root / 'doc/LSM_LOOKUP_ARCHITECTURE_DETAILED.md'
    out = root / 'doc/lsm_lookup_diagrams/visual_revision_20261015'
    refs = dict(re.findall(r'^\[([^\]]+)\]:\s+(\S+)$', text, re.M))
    assert all(line == line.rstrip() for line in text.splitlines())
    assert text.count('```') % 2 == 0
    for target in list(refs.values()) + re.findall(r'\]\(([^)]+)\)', text):
        if target.startswith(('http:', 'https:', '#')):
            continue
        assert (doc.parent / target.split('#')[0]).exists(), target
    uses = set(re.findall(r'\]\[([^\]]+)\]', text))
    assert uses == set(refs), (uses - set(refs), set(refs) - uses)
    images = re.findall(r'!\[([^\]]+)\]\(([^)]+)\)', text)
    assert len(images) == len({target for _, target in images}) == 25
    captions = re.findall(r'^\*Figure (\d+)\.', text, re.M)
    assert list(map(int, captions)) == list(range(1, 26))
    image_hashes = {}
    for alt, target in images:
        assert alt.strip()
        path = doc.parent / target
        with Image.open(path) as im:
            im.verify()
        with Image.open(path) as im:
            assert im.width >= 2000 and im.height >= 700, (target, im.size)
        image_hashes[target] = sha256(path)
    base = json.loads((doc.parent / refs['baseline']).read_text())
    assert base['heap_bytes'] == 28248276992
    assert base['relpages'] == 3448276
    assert base['merkle_stats']['total_nodes'] == 211400
    reserve = 8192 * (100 - 90) // 100
    per_page = (8192 - 24 - reserve) // 252
    assert reserve == 819 and per_page == 29
    assert ((100000000 + per_page - 1) // per_page) * 8192 == base['heap_bytes']
    node_sizes = [x['bytes'] for x in base['sizes'] if x['nspname'] == 'ariabc_internal']
    assert sum(node_sizes) == 38436864
    lsm = (doc.parent / refs['lsmc']).read_text()
    header = (doc.parent / refs['lsmh']).read_text()
    util = (doc.parent / refs['util']).read_text()
    storage = (doc.parent / refs['storage']).read_text()
    assert 'LSM3_CHECK_TOP_INDEX_SIZE_PERIOD (64*1024)' in header
    assert 'insert_number % LSM3_CHECK_TOP_INDEX_SIZE_PERIOD' in lsm
    assert 'log_newpage(&index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM' in lsm
    assert 'Lsm3InsideCopy && Lsm3ReleasedLocks' in lsm
    assert 'PG_RETURN_BYTEA_P(result)' in util
    assert 'XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE' in storage
    cases = json.loads((doc.parent / refs['dsnapshot']).read_text())['cases']
    assert len(cases) == 4
    btree = [c for c in cases if 'btree' in c['case']]
    lsm_cases = [c for c in cases if 'lsm' in c['case']]
    gain = (sum(c['tps'] for c in lsm_cases) / sum(c['tps'] for c in btree) - 1) * 100
    assert round(gain, 2) == 8.74
    for case in cases:
        assert case['inserts'] == 1024
        assert case['validated_requests'] == 20000
        assert case['divergence_count'] == case['permanent_failures'] == 0
        assert case['merkle_verify'] == 'PASS'
    assert all(c['merges_requested_after'] == 0 for c in lsm_cases)
    result = dict(status='PASS', scope='Documentation and saved evidence consistency only',
                  figures=25, new_figures=17, words_before=len(detailed.read_text().split()),
                  words_after=len(text.split()), diagram_sha256=image_hashes,
                  document_sha256=sha256(doc), preserved_reference_sha256=sha256(detailed),
                  source_sha256={name: sha256(doc.parent / target) for name, target in refs.items()})
    result['text_reduction_percent'] = round((1-result['words_after']/result['words_before'])*100, 1)
    (out / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if not key.endswith('sha256')}, indent=2))


if __name__ == '__main__':
    main()
