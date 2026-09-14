"""Local contract tests; never contact or modify benchmark servers."""
import collections
import contextlib
import io
import json
import math
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPTS / "distributed"))
import run_oom_100m_benchmark as oom
from benchmark_validation import parse_gateway_result
from large_zipf import LargeZipfGenerator
from generate_ycsb_workloads import ZipfGenerator, generate_workload_statements


GOOD = '''loaded 20000 queries
PROGRESS_GATEWAY_DET total=20000 permanent_failures=0 divergence_count=0
completed_tps=12000.0
 overall time taken (millisec) = 2000
 overall wall time including drains (millisec) = 2100
duplicate_key_errors=0
divergence_count=0
permanent_failures=0
client_quorum_complete_count=20000 success_count=20000 deterministic_error_count=0 nonterminal_failure_count=0 permanent_failures=0
'''


class GatewayValidationTests(unittest.TestCase):
    def test_direct_paths_use_their_own_completion_evidence(self):
        direct = GOOD.replace('success_count=20000', 'success_count=0').replace(
            'client_quorum_complete_count=20000', 'client_quorum_complete_count=0')
        direct += ('PROFILE_GATEWAY completion_path=direct submit_mode=event '
                   'submit_attempts=20000 read_calls=5314 not_accepted=0\n')
        self.assertEqual(parse_gateway_result(direct, 20000, mode='pg')['validated_completed_queries'], 20000)
        with self.assertRaises(ValueError):
            parse_gateway_result(direct.replace('submit_attempts=20000', 'submit_attempts=20001'),
                                 20000, mode='pg')
        direct += ('PROGRESS_GATEWAY_DET total=20000 sent=20000 accepted=20000 completed=20000 '
                   'pipeline_outstanding=0 majority_inflight=0 pending_accept=0 final=1\n')
        self.assertEqual(parse_gateway_result(direct, 20000, mode='bcdb_det')['validated_completed_queries'], 20000)
        with self.assertRaises(ValueError):
            parse_gateway_result(direct.replace('completed=20000', 'completed=19999'),
                                 20000, mode='bcdb_merkle')

    def test_complete(self):
        result = parse_gateway_result(GOOD, 20000)
        self.assertEqual(result['tps'], 10000)
        self.assertEqual(result['wall_including_drains_ms'], 2100)

    def test_late_failure_overrides_clean_progress(self):
        with self.assertRaises(ValueError):
            parse_gateway_result(GOOD + 'permanent_failures=1\n', 20000)
        with self.assertRaises(ValueError):
            parse_gateway_result(GOOD + 'divergence_count=1\n', 20000)

    def test_reject_missing_partial_error_and_bad_exit(self):
        for output in (GOOD.replace('success_count=20000', 'success_count=19999'),
                       GOOD.replace('deterministic_error_count=0', 'deterministic_error_count=1'),
                       GOOD.replace('duplicate_key_errors=0', 'duplicate_key_errors=1'),
                       GOOD.replace('permanent_failures=0', ''),
                       GOOD.replace('loaded 20000', 'loaded 19999'),
                       GOOD.split(' overall time')[0]):
            with self.subTest(output=output), self.assertRaises(ValueError):
                parse_gateway_result(output, 20000)
        with self.assertRaises(ValueError):
            parse_gateway_result(GOOD, 20000, 1)


class ZipfTests(unittest.TestCase):
    def test_suite_distribution_and_rng_match(self):
        for skew in (0, .5, .99, 1, 1.2, 2):
            expected = ZipfGenerator(12000, skew, random.Random(141))
            actual = LargeZipfGenerator(12000, skew, random.Random(141))
            self.assertEqual([expected.next_index() for _ in range(3000)],
                             [actual.next_index() for _ in range(3000)])

    def test_tail_against_direct_sum(self):
        for skew in (.2, .99, 1, 1.2, 2):
            gen = LargeZipfGenerator(100000, skew, random.Random(1))
            exact = math.fsum(k ** -skew for k in range(1, 100001))
            self.assertAlmostEqual(gen.total / exact, 1, places=12)

    def test_100m_distribution(self):
        gen = LargeZipfGenerator(100000000, .99, random.Random(42))
        samples = [gen.next_index() for _ in range(40000)]
        self.assertLessEqual(len(gen.prefix), 16384)
        self.assertGreater(max(samples), 90000000)
        self.assertTrue(all(1 <= key <= 100000000 for key in samples))
        self.assertAlmostEqual(samples.count(1) / len(samples), 1 / gen.total, delta=.004)
        self.assertAlmostEqual(sum(k <= 12000 for k in samples) / len(samples),
                               gen.harmonic(12000) / gen.total, delta=.008)

    def test_sql_matches_existing_a_suite(self):
        for skew, suffix in ((0, '0_00'), (.99, '0_99')):
            expected = (SCRIPTS / 'ycsb_suite' / f'ycsb_workload_a_skew_{suffix}_20k.txt').read_text()
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'workload.sql'
                oom.generate_100m_ycsb_workload(path, 'a', skew, db_size=12000)
                self.assertEqual(path.read_text().replace('usertable ', 'usertable_small '), expected)

    def test_f_preserves_paired_statements(self):
        expected = generate_workload_statements('f', .99, num_tx=300, num_keys=12000,
                                                table_name='usertable', seed=141)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'workload.sql'
            oom.generate_100m_ycsb_workload(path, 'f', .99, txs=300, db_size=12000)
            self.assertEqual(path.read_text().splitlines(), expected)


class RunnerTests(unittest.TestCase):
    def test_requested_cli_and_legacy_comma_form(self):
        args = oom.parse_args(['--workloads', 'a', '--skews', '0.0', '0.99', '--workers', '1', '8', '16',
                               '--txs', '20000', '--remote-host', '10.129.148.247',
                               '--remote-dir', '/tmp/ariabc_oom_100m', '--output-dir', '/tmp/results'])
        self.assertEqual(args.workers, [1, 8, 16])
        self.assertEqual(args.shared_buffers, '32MB')
        self.assertEqual(oom.parse_args(['--workers', '1,8,16']).workers, args.workers)

    def test_invalid_config_fails_before_remote_actions(self):
        for argv in (['--reset-mode', 'inplace'], ['--skews', 'nan'], ['--skews', '-.1'],
                     ['--workers', '0'], ['--db-rows', '0'], ['--workloads', 'typo'],
                     ['--remote-dir', '/tmp/../'], ['--shared-buffers', '32MB;echo bad']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                oom.parse_args(argv)

    def test_dry_run_preserves_existing_results_and_has_no_remote_calls(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(oom, 'run_remote') as remote:
            root = Path(directory)
            (root / 'summary.csv').write_text('old results\n')
            with contextlib.redirect_stdout(io.StringIO()):
                oom.main(['--output-dir', directory, '--dry-run', '--txs', '10', '--skews', '0',
                          '--workers', '1', '--modes', 'pg'])
            remote.assert_not_called()
            self.assertEqual((root / 'summary.csv').read_text(), 'old results\n')
            manifests = list(root.glob('run_*/campaign.json'))
            self.assertEqual(len(manifests), 1)
            self.assertEqual(json.loads(manifests[0].read_text())['version'], 2)

    def test_remote_scripts_are_valid_bash_and_stop_on_error(self):
        captured = []
        def fake_remote(host, user, command, **kwargs):
            captured.append(command)
            return subprocess.CompletedProcess([], 0, 'Database cluster state: shut down\n', '')
        args = oom.parse_args([])
        with mock.patch.object(oom, 'run_remote', side_effect=fake_remote):
            self.assertTrue(oom.check_remote_db_exists(args))
            oom.stop_server(args)
            oom.stop_postgres(args)
            oom.start_postgres(args)
            oom.start_remote_ariabc_server(args, 'pg', 1)
            oom.start_remote_ariabc_server(args, 'bcdb_merkle', 8)
            oom.generate_remote_100m_database(args)
        for command in captured:
            checked = subprocess.run(['bash', '-n'], input='set -euo pipefail\n' + command,
                                     text=True, capture_output=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertFalse(any('fuser -k' in command for command in captured))
        self.assertFalse(any("-delete" in command for command in captured))

    def test_device_errors_and_counter_reset_not_reported_as_zero(self):
        with self.assertRaises(RuntimeError):
            oom.delta({'read': 100}, {'read': 99}, 'read')
        with mock.patch.object(oom, 'run_remote', return_value=subprocess.CompletedProcess([], 0, '', '')):
            with self.assertRaises(RuntimeError):
                oom.get_remote_nvme_stats(oom.parse_args([]))

    def test_reset_validates_before_cache_drop_and_clears_auto_conf(self):
        args = oom.parse_args([])
        commands = []
        def fake_remote(host, user, command, **kwargs):
            commands.append(command)
            return subprocess.CompletedProcess([], 0, '', '')
        settings = dict(shared_buffers='4096', block_size='8192', bcdb_worker_count='1',
                        enable_merkle_index='off', fsync='on', full_page_writes='on',
                        synchronous_commit='on', track_counts='on', track_io_timing='on')
        responses = ['', '100000000|1|100000000', '[{"indexdef":"USING btree"}]',
                     json.dumps(settings), '{"heap_bytes":25600000000}']
        with mock.patch.object(oom, 'run_remote', side_effect=fake_remote), \
             mock.patch.object(oom, 'prepare_ledger_schema'), \
             mock.patch.object(oom, 'sql', side_effect=responses):
            result = oom.reset_remote_pgdata(args, 'pg', 1)
        self.assertEqual(result['settings']['shared_buffers'], '4096')
        script = '\n'.join(commands)
        self.assertIn('postgresql.auto.conf', script)
        self.assertIn('cp -a --reflink=never', script)
        self.assertIn('drop_caches', script)
        self.assertNotIn('fuser -k', script)
        for command in commands:
            check = subprocess.run(['bash', '-n'], input=command, text=True, capture_output=True)
            self.assertEqual(check.returncode, 0, check.stderr)


if __name__ == '__main__':
    unittest.main()
