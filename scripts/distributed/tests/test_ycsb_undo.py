"""Parser contracts and opt-in real PostgreSQL restoration tests."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ycsb_undo as undo


class ParserTests(unittest.TestCase):
    def test_repeated_keys_and_all_operations(self):
        insert = "INSERT INTO usertable (" + ', '.join(undo.COLUMNS) + ") VALUES (9," + ','.join(["'v'"]*10) + ');'
        keys, counts = undo.parse_workload("UPDATE usertable SET field1='it''s' WHERE ycsb_key=1;\n"
            "UPDATE usertable SET field2=NULL WHERE ycsb_key=1;\n"
            "DELETE FROM usertable WHERE ycsb_key=2;\n" + insert + '\n'
            "SELECT * FROM usertable WHERE ycsb_key >= 1 ORDER BY ycsb_key LIMIT 9;")
        self.assertEqual(keys, [1, 2, 9])
        self.assertEqual(counts, dict(update=2, delete=1, insert=1, select=1))

    def test_reject_unknown_write_footprint(self):
        for statement in ["UPDATE usertable SET field1='x';", "TRUNCATE usertable;",
                          "UPDATE usertable SET ycsb_key=2 WHERE ycsb_key=1;",
                          "DELETE FROM usertable WHERE ycsb_key=1 OR 1=1;",
                          "SELECT evil() FROM usertable;",
                          "UPDATE usertable SET field1=some_function() WHERE ycsb_key=1;",
                          "SELECT * FROM usertable WHERE ycsb_key=1; DELETE FROM usertable;"]:
            with self.subTest(statement=statement), self.assertRaises(ValueError):
                undo.parse_workload(statement)

    def test_real_100m_files(self):
        root = Path(__file__).resolve().parents[2] / 'bench_full_results/oom_100m_sweep/run_20260915_012215_3cb02623/workloads'
        for filename, count in [('ycsb_a_skew_0.0_20000.sql', 9983), ('ycsb_a_skew_0.99_20000.sql', 6900)]:
            path = root / filename
            if not path.exists():
                self.skipTest('Historical artifacts unavailable')
            keys, counts = undo.parse_workload(path.read_text())
            self.assertEqual(len(keys), count)
            self.assertEqual(sum(counts.values()), 20000)


@unittest.skipUnless(os.getenv('YCSB_UNDO_TEST_PORT'), 'Needs a dedicated disposable PostgreSQL instance')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db = undo.Psql(os.environ.get('YCSB_UNDO_TEST_PSQL', 'psql'),
                            port=int(os.environ['YCSB_UNDO_TEST_PORT']))
        fields = ','.join(f'{name} text' for name in undo.COLUMNS[1:])
        self.db.run(f'DROP TABLE IF EXISTS usertable CASCADE; CREATE TABLE usertable (ycsb_key integer PRIMARY KEY,{fields});'
                    "INSERT INTO usertable(ycsb_key,field1,field2,field3) VALUES (1,'original',NULL,''),"
                    "(2,E'line\\n tab\\t slash\\\\ quote''','b','c'),(3,'untouched','d','e');")

    def test_partial_full_and_repeated_restore(self):
        insert = 'INSERT INTO usertable (' + ','.join(undo.COLUMNS) + ") VALUES (9," + ','.join(["'new'"]*10) + ');'
        statements = ["UPDATE usertable SET field1='changed' WHERE ycsb_key=1;",
                      "UPDATE usertable SET field2='again' WHERE ycsb_key=1;",
                      "DELETE FROM usertable WHERE ycsb_key=2;", insert]
        baseline = undo.full_hash(self.db)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            workload = path / 'workload.sql'
            workload.write_text('\n'.join(statements)+'\n')
            undo.prepare(self.db, workload, path / 'undo')
            restore = (path / 'undo/restore.sql').read_text()
            for subset in (statements[:1], statements, []):
                self.db.run('\n'.join(subset))
                self.db.run(restore)
                self.assertTrue(undo.verify(self.db, path / 'undo')['verified'])
                self.assertEqual(undo.full_hash(self.db)['sha256'], baseline['sha256'])
            # A targeted check deliberately cannot certify an unrelated row.
            self.db.run("UPDATE usertable SET field1='corrupt' WHERE ycsb_key=3;")
            if 'merkle_root_hash_index' in restore:
                with self.assertRaises(ValueError):
                    undo.verify(self.db, path / 'undo')
            else:
                self.assertTrue(undo.verify(self.db, path / 'undo')['verified'])
            self.assertNotEqual(undo.full_hash(self.db)['sha256'], baseline['sha256'])

    def test_target_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            workload = path / 'workload.sql'
            workload.write_text("UPDATE usertable SET field1='new' WHERE ycsb_key=1;\n")
            undo.prepare(self.db, workload, path / 'undo')
            self.db.run("UPDATE usertable SET field1='bad' WHERE ycsb_key=1;")
            with self.assertRaises(ValueError):
                undo.verify(self.db, path / 'undo')

    def test_read_only_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            workload = path / 'workload.sql'
            workload.write_text('SELECT * FROM usertable WHERE ycsb_key=1;\n')
            baseline = undo.full_hash(self.db)
            result = undo.prepare(self.db, workload, path / 'undo')
            self.assertEqual(result['before_rows'], 0)
            self.db.run((path / 'undo/restore.sql').read_text())
            self.assertTrue(undo.verify(self.db, path / 'undo')['verified'])
            self.assertEqual(undo.full_hash(self.db)['sha256'], baseline['sha256'])

    def test_merkle_restoration(self):
        schema = Path(__file__).resolve().parents[1] / 'sql/raft_apply_ledger_schema.sql'
        self.db.run(schema.read_text())
        self.db.run('CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key) '
                    'WITH (partitions=2, fanout=4, split_threshold=32, merge_threshold=8);')
        baseline_root = self.db.run("SELECT merkle_root_hash('usertable');")
        self.test_partial_full_and_repeated_restore()
        # That test deliberately corrupts the untouched key at the end; undo it
        # before checking the Merkle root of the original three-row baseline.
        self.db.run("UPDATE usertable SET field1='untouched' WHERE ycsb_key=3;")
        self.assertEqual(self.db.run("SELECT merkle_root_hash('usertable');"), baseline_root)
        self.assertEqual(self.db.run("SELECT merkle_verify('usertable');").strip(), b't')


if __name__ == '__main__':
    unittest.main()
