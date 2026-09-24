"""Exercise the actual shell helpers without starting a cluster."""
import hashlib
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[1] / 'run_4node_raft_cluster.sh'
sys.path.insert(0, str(RUNNER.parent))
from source_fingerprint import fingerprint, GENERATED_NAMES


def shell_helper(name):
    return re.search(r'^' + name + r'\(\) \{\n.*?^\}', RUNNER.read_text(),
                     re.M | re.S).group(0)


class ClusterProvenanceTests(unittest.TestCase):
    def test_generated_aliases_do_not_change_source_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'src/include/utils').mkdir(parents=True)
            (root / 'src/include/storage').mkdir()
            (root / 'ariabc_pg').mkdir()
            source = root / 'src/input.c'
            source.write_text('int value = 1;\n')
            source_id = lambda: fingerprint(root, 2048)
            original = source_id()
            for relative in ('utils/fmgrprotos.h', 'utils/fmgroids.h', 'utils/errcodes.h',
                             'utils/probes.h', 'storage/lwlocknames.h'):
                (root / 'src/include' / relative).write_text('generated alias\n')
            # Reproduce the build cleanup and the next sync restoring outputs.
            for name in GENERATED_NAMES:
                (root / 'src' / name).write_text('host-specific generated output\n')
            (root / 'src/include/parser').mkdir()
            alias = root / 'src/include/parser/gram.h'
            alias.symlink_to('/absent/origin/src/backend/parser/gram.h')
            self.assertEqual(original, source_id())
            alias.unlink()
            alias.write_text('repaired include alias\n')
            self.assertEqual(original, source_id())
            source.write_text('int value = 2;\n')
            self.assertNotEqual(original, source_id())

    def test_headers_and_generators_are_real_build_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'src').mkdir()
            (root / 'NuRaft').mkdir()
            original = fingerprint(root, 2048)
            for relative in ('src/parser.y', 'src/generator.pl', 'NuRaft/raft.hxx'):
                path = root / relative
                path.write_text('build input\n')
                updated = fingerprint(root, 2048)
                self.assertNotEqual(original, updated)
                original = updated
            self.assertNotEqual(original, fingerprint(root, 4096))
            (root / 'src/missing.h').symlink_to('/absent/real-input.h')
            with self.assertRaises(FileNotFoundError):
                fingerprint(root, 2048)

    def test_manifest_rejects_changed_binary_or_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / 'server'
            binary.write_text('build one')
            binary.chmod(0o755)
            manifest = binary.with_suffix('.manifest')
            manifest.write_text('binary_sha256=' + hashlib.sha256(binary.read_bytes()).hexdigest() +
                                '\nsource_fingerprint=source-one\n')
            command = shell_helper('verify_build_manifest') + '\nverify_build_manifest "$1" "$2"'
            check = lambda source: subprocess.run(['bash', '-c', command, 'test', str(binary), source]).returncode
            self.assertEqual(check('source-one'), 0)
            self.assertNotEqual(check('source-two'), 0)
            binary.write_text('build two')
            self.assertNotEqual(check('source-one'), 0)

    def test_delegated_session_propagates_failure(self):
        # A session leader forces setsid to fork. --wait must retain child status.
        self.assertIn('setsid --wait ./scripts/distributed/run_4node_raft_cluster.sh', RUNNER.read_text())
        result = subprocess.run(['setsid', '--wait', 'bash', '-c', 'exit 7'], start_new_session=True)
        self.assertEqual(result.returncode, 7)
