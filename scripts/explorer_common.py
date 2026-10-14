#!/usr/bin/env python3
"""Shared review gate and hashing helpers for the two explorer builders.

The pages model reviewed source behaviour.  Updating REVIEWED_HEAD is an
intentional act: read the diff since the previous pin, update the models and
excerpts, then change the pin.  A checkout at another commit, or a cited source
file with uncommitted edits, is refused instead of being silently re-labelled.
"""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
REVIEWED_HEAD = '9cf6c70a39e9bf848c93c2c24bdb55181f4b3356'
REVIEWED_DATE = '2026-10-13'
PREVIOUS_REVIEW = {'head': 'c165637f21cc25411af302a56f83717fd340c303', 'date': '2026-10-09'}


def sha256_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path):
    return sha256_bytes(Path(path).read_bytes())


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def add_snapshot_argument(parser):
    parser.add_argument('--snapshot', metavar='SOURCE_SNAPSHOT.json',
                        help='offline source copy without .git: JSON manifest whose "head" names the '
                             'commit the files were copied from; it must equal REVIEWED_HEAD')


def review_identity(paths, snapshot=None):
    """Return (head, provenance) after the review gate.

    Default: the Git checkout must be at REVIEWED_HEAD with the cited files clean.
    --snapshot: no Git metadata is consulted; the manifest's declared head must equal
    REVIEWED_HEAD.  The declared identity is recorded together with the manifest hash
    and the per-file content hashes emitted by the caller, so a copied tree can be
    compared byte-for-byte with its origin.  Nothing derives a "current HEAD" here.
    """
    if snapshot is None:
        return require_reviewed_head(paths), {'mode': 'git', 'root': str(ROOT)}
    manifest_path = Path(snapshot).resolve()
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    declared = manifest.get('head', '')
    if declared != REVIEWED_HEAD:
        raise SystemExit(f'Snapshot declares head {declared[:8] or "<missing>"}; reviewed head is '
                         f'{REVIEWED_HEAD[:8]}. Review before rebuilding.')
    missing = [p for p in sorted(set(paths)) if not (ROOT / p).is_file()]
    if missing:
        raise SystemExit('Snapshot is missing cited files: ' + ', '.join(missing))
    return declared, {'mode': 'snapshot', 'declared_head': declared,
                      'manifest_sha256': sha256_bytes(raw),
                      'origin_root': manifest.get('source_root'),
                      'snapshot_root': str(ROOT),
                      'note': 'Identity declared by the snapshot manifest; file hashes below are the evidence.'}


def require_reviewed_head(paths):
    """Refuse to build unless HEAD is the reviewed commit and cited files are clean."""
    head = git('rev-parse', 'HEAD')
    if head != REVIEWED_HEAD:
        raise SystemExit(
            f'Source HEAD {head[:8]} differs from reviewed {REVIEWED_HEAD[:8]}. '
            'Review the source diff, update the explorer models, then update '
            'REVIEWED_HEAD in scripts/explorer_common.py.')
    dirty = git('status', '--porcelain', '--', *sorted(set(paths)))
    if dirty:
        raise SystemExit('Cited source files differ from HEAD; refusing to record their hashes:\n' + dirty)
    return head


def replace_block(html, start_marker, end_marker, payload):
    """Replace the text between two unique markers (markers are kept)."""
    if html.count(start_marker) != 1 or html.count(end_marker) != 1:
        raise SystemExit(f'Markers must be unique: {start_marker!r} / {end_marker!r}')
    a = html.index(start_marker) + len(start_marker)
    b = html.index(end_marker)
    if b < a:
        raise SystemExit(f'Marker order is wrong: {start_marker!r}')
    return html[:a] + payload + html[b:]


def embed_json(data):
    """JSON safe inside <script type="application/json">."""
    return json.dumps(data, separators=(',', ':'), sort_keys=True).replace('<', '\\u003c')
