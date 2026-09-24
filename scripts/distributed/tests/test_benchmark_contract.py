import collections
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from benchmark_contract import balanced_cases, require_merkle, validate_ycsb_results, variability, materialize_workloads
from benchmark_cache import evict
from generate_ycsb_workloads import generate_workload_statements


class ExperimentContractTests(unittest.TestCase):
    def test_each_mode_occupies_each_position_and_no_cases_lost(self):
        cases = list(balanced_cases([('a', 8), ('b', 16)], ['pg', 'det', 'merkle'], 6))
        self.assertEqual(len(cases), 36)
        self.assertEqual(len(set(cases)), 36)
        for config in [('a', 8), ('b', 16)]:
            selected = [c for c in cases if c[:2] == config]
            for position in range(3):
                self.assertEqual(collections.Counter(c[2] for c in selected[position::3]),
                                 {'pg': 2, 'det': 2, 'merkle': 2})

    def test_false_or_partial_merkle_does_not_pass(self):
        require_merkle('9|t\n', 9)
        for text in ['8|t', 'notice: t', '9|f', '9|t\nERROR']:
            with self.assertRaises(RuntimeError):
                require_merkle(text, 9)

    def test_successful_sql_with_no_row_is_rejected(self):
        good = 'single-gateway-direct-1 1 SELECT 1 x\nsingle-gateway-direct-2 2 INSERT 0 1\n'
        self.assertEqual(validate_ycsb_results(good, 2)['empty_results'], 0)
        self.assertEqual(validate_ycsb_results(good.replace('SELECT 1', 'SELECT 0'), 2, mode='pg')['empty_results'], 1)
        for bad in [good.replace('SELECT 1', 'SELECT 0'), good.splitlines()[0], good+good]:
            with self.assertRaises(RuntimeError):
                validate_ycsb_results(bad, 2)

    def test_read_latest_does_not_depend_on_uncommitted_generated_key(self):
        lines = generate_workload_statements('d', .99, 3000, num_keys=20)
        inserted_keys = set()
        read_new_keys = 0
        for line in lines:
            if line.startswith('INSERT'):
                # Extract key from INSERT INTO table (col, ...) VALUES (key, ...)
                key = int(line.split('VALUES (', 1)[1].split(',', 1)[0])
                inserted_keys.add(key)
            elif line.startswith('SELECT'):
                self.assertNotIn('ORDER BY', line)
                self.assertNotIn('LIMIT', line)
                key = int(line.split('WHERE YCSB_KEY=', 1)[1].rstrip(';'))
                if key > 20:
                    read_new_keys += 1
                    self.assertIn(key, inserted_keys)
        self.assertGreater(read_new_keys, 0)

    def test_joined_executor_records_keep_strict_row_counts(self):
        merged = ('single-gateway-direct-1 1 UPDATE 1'
                  'single-gateway-direct-2 1 SELECT 1 value\n')
        self.assertEqual(validate_ycsb_results(merged, 2)['validated_row_results'], 2)
        joined_insert = ('[ORDERER] next_seq='
                         'single-gateway-direct-1 1 INSERT 0 110240 not in pending\n')
        with self.assertRaises(RuntimeError):
            validate_ycsb_results(joined_insert, 1)
        with self.assertRaises(RuntimeError):
            validate_ycsb_results('single-gateway-direct-1 1 INSERT 0 0\n', 1)
        for bad in (merged.replace('UPDATE 1', 'UPDATE 0'), merged + merged,
                    merged.replace('direct-2', 'direct-3')):
            with self.assertRaises(RuntimeError):
                validate_ycsb_results(bad, 2)

    def test_f_is_one_atomic_request(self):
        lines = generate_workload_statements('f', .99, 1000)
        self.assertEqual(len(lines), 1000)
        self.assertTrue(any(line.startswith('WITH ycsb_read AS MATERIALIZED') for line in lines))
        self.assertFalse(any(line.startswith('UPDATE ') for line in lines))

    def test_single_trial_and_noisy_series_not_qualified(self):
        self.assertEqual(variability([100])['status'], 'insufficient_repeats')
        self.assertEqual(variability([100, 200, 100, 200, 100])['status'], 'high_variability')
        self.assertEqual(variability([100]*5)['status'], 'repeatability_check_passed')

    def test_cache_helper_refuses_running_database_and_reports_residency(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'PG_VERSION').write_text('13')
            (root/'pg_tblspc').mkdir()
            (root/'postmaster.pid').write_text('123')
            with self.assertRaisesRegex(RuntimeError, 'Stop PostgreSQL'):
                evict(root)
            (root/'postmaster.pid').unlink()
            (root/'heap').write_bytes(b'x'*1024*1024)
            result = evict(root)
            self.assertEqual(result['files'], 2)
            self.assertLessEqual(result['resident_pages'], 8)

if __name__ == '__main__':
    unittest.main()
