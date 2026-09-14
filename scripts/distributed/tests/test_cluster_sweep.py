import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cluster_sweep_support import accept_cluster_artifact, campaign_contract
from summarize_raft_profile import collect_csv_row


class ClusterEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, name, text):
        (self.root / name).write_text(text)

    def test_audit_failures_override_early_majority_progress(self):
        self.write("gateway_test.log",
                   "PROGRESS_GATEWAY_DET completed=20000 completed_tps=999 permanent_failures=0 divergence_count=0\n"
                   "PROGRESS_GATEWAY_DET completed=20000 completed_tps=440 permanent_failures=17000 divergence_count=0 final=1\n"
                   "divergence_count=0\n"
                   "PROFILE_GATEWAY submit_attempts=79 permanent_failures=17451\n")
        # The marker and echoed workload must not override workload counters.
        self.write("runner.log", "post-marker verification PASS\n"
                   "PROFILE_GATEWAY submit_attempts=1 permanent_failures=0\n")
        row = collect_csv_row(self.root)
        self.assertEqual(row["permanent_failures"], 17451)
        self.assertEqual(row["tps"], "")
        self.assertEqual(row["merkle_pass"], 0)

    def test_missing_evidence_stays_unknown(self):
        row = collect_csv_row(self.root)
        self.assertEqual(row["permanent_failures"], "")
        self.assertEqual(row["divergence_count"], "")
        self.assertEqual(row["tps"], "")

    def test_pre_marker_is_not_final_verification(self):
        self.write("runner.log", "pre-marker verification PASS\n")
        self.assertEqual(collect_csv_row(self.root)["merkle_pass"], 0)

    def test_accept_requires_all_workload_audits(self):
        self.write("runner.log", "usertable_small consistency: PASS\n")
        self.write("gateway_test.log", "divergence_count=0\nPROFILE_GATEWAY permanent_failures=0\n")
        self.write("run_summary.env", "all3_audit_valid=yes\nasync_all3_verified_count=19999\n"
                   "tps_majority_visible=2000\npermanent_failures=0\ndivergence_count=0\n")
        with self.assertRaisesRegex(RuntimeError, "count does not match"):
            accept_cluster_artifact(self.root, 20000)
        self.write("run_summary.env", (self.root / "run_summary.env").read_text().replace("19999", "20000"))
        self.assertEqual(accept_cluster_artifact(self.root, 20000)["tps"], "2000.00")

    def test_resume_rejects_changed_buffers_and_legacy_results(self):
        self.write("workload.sql", "SELECT 1;\n")
        args = SimpleNamespace(db_shared_buffers="32MB")
        campaign_contract(self.root, self.root, args, ["workload.sql"], [1], ["cluster"])
        args.db_shared_buffers = "64MB"
        with self.assertRaisesRegex(RuntimeError, "settings changed"):
            campaign_contract(self.root, self.root, args, ["workload.sql"], [1], ["cluster"])
        (self.root / "campaign.json").unlink()
        self.write("summary.csv", "legacy\n")
        with self.assertRaisesRegex(RuntimeError, "legacy"):
            campaign_contract(self.root, self.root, args, ["workload.sql"], [1], ["cluster"])


if __name__ == "__main__":
    unittest.main()
