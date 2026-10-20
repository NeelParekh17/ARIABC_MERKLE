#!/usr/bin/env python3
"""Check guide artifacts and source-snapshot freshness, not engine semantics.

Run on the lab host against the copied repository snapshot:
    python3 doc/merkle_implementation_diagrams/validate.py --root <snapshot>
Source conclusions are manually recorded in source_review.json. Matching hashes
only establishes that those inspected bytes have not changed.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re

from PIL import Image


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    root = args.root.resolve()
    doc = root / 'doc/MERKLE_IMPLEMENTATION_VISUAL_GUIDE.md'
    assets = doc.parent / 'merkle_implementation_diagrams'
    text = doc.read_text()
    manifest = json.loads((assets / 'figures.json').read_text())
    expected = {entry['name']: entry for entry in manifest}
    review = json.loads((assets / 'source_review.json').read_text())
    images = re.findall(r'!\[([^\]]*)\]\(([^)]+)\)', text)
    assert len(images) == len(expected) == 59
    assert len({target for _, target in images}) == len(images)
    assert '```mermaid' not in text and '```text' not in text
    image_hashes = {}
    for alt, target in images:
        path = doc.parent / target
        entry = expected[path.stem]
        assert alt == entry['title'], (target, 'alt/title mismatch')
        with Image.open(path) as im:
            im.verify()
        with Image.open(path) as im:
            assert im.format == 'PNG' and im.mode == 'RGB'
            assert im.size == (entry['width'], entry['height'])
            assert im.width >= 2000 and im.height >= 700
        image_hashes[path.name] = digest(path)
    assert set(expected) == {Path(target).stem for _, target in images}
    captions = re.findall(r'^\*Figure (\d+)\. (.*?)\*$', text, re.M)
    assert [int(n) for n, _ in captions] == list(range(1, len(images) + 1))
    assert [title for _, title in captions] == [alt + '.' for alt, _ in images]

    refs = dict(re.findall(r'^\[(S\d+)\]:\s+(\S+)$', text, re.M))
    uses = set(re.findall(r'\]\[(S\d+)\]', text))
    assert uses == set(refs)
    assert set(re.findall(r'^\| (S\d+) \|', text, re.M)) == set(refs)
    for ref, value in refs.items():
        target, anchor = value.split('#')
        path = (doc.parent / target).resolve()
        assert path.is_file(), ref
        assert re.fullmatch(r'L[1-9]\d*', anchor), ref
        requested = int(anchor[1:])
        with path.open() as source:
            for n, _ in enumerate(source, 1):
                if n == requested:
                    break
            else:
                raise AssertionError((ref, 'line outside file'))
    for target in re.findall(r'(?<!!)\[[^\]]*\]\(([^)]+)\)', text):
        if not target.startswith('#') and '://' not in target:
            assert (doc.parent / target.split('#')[0]).exists(), target
    slugs = set()
    for line in text.splitlines():
        assert line == line.rstrip(), 'Trailing whitespace'
        if line.startswith('#'):
            title = re.sub(r'^#+\s+', '', line).lower()
            slugs.add(re.sub(r'[^\w\- ]', '', title).replace(' ', '-'))
    for anchor in re.findall(r'\]\(#([^)]+)\)', text):
        assert anchor in slugs, anchor
    assert len(re.findall(r'^```', text, re.M)) == 4

    assert digest(doc) == review['document_sha256'], 'Source review refers to a different guide'
    assert digest(assets / 'render.py') == review['renderer_sha256']
    for relative, recorded in review['files'].items():
        assert digest(root / relative) == recorded['sha256'], ('Source changed', relative)
    for finding in review['findings']:
        assert set(finding['source_refs']).issubset(refs)

    report = {
        'kind': 'artifact_and_snapshot_validation', 'status': 'PASS',
        'checked_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'embedded_png_figures': len(images), 'source_references': len(refs),
        'image_integrity_dimensions_titles_captions': 'PASS',
        'links_anchors_reference_table': 'PASS',
        'source_snapshot_hashes': 'MATCH_MANUALLY_REVIEWED_BYTES',
        'source_files_hashed': len(review['files']),
        'document_sha256': digest(doc), 'renderer_sha256': digest(assets / 'render.py'),
        'source_review_sha256': digest(assets / 'source_review.json'),
        'validator_sha256': digest(Path(__file__)), 'image_sha256': image_hashes,
        'limits': 'PASS checks artifact integrity and snapshot freshness. It does not prove mechanism claims, build correctness, database behavior, or concurrent/crash schedules.',
        'manual_source_review': 'source_review.json',
        'visual_review': 'All figure contact sheets and changed/new figures reviewed; generator checks label fit, bounds, and connector crossings.',
        'runtime_validation': review['runtime_validation'],
    }
    (assets / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'image_sha256'}, indent=2))


if __name__ == '__main__':
    main()
