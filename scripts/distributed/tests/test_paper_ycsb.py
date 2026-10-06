"""Validate the paper workload contract. Execute on the lab host only."""

import hashlib
import json
from pathlib import Path
import random
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import generate_paper_ycsb as paper


class PaperYcsbTests(unittest.TestCase):
    def test_atomic_gateway_lines_and_operation_mix(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "input"
            manifest = paper.generate_package(output, [0.6], count=2000)
            workload = manifest["workloads"][0]
            lines = [line for line in (output / workload["file"]).read_text().splitlines()
                     if line and not line.startswith("--")]
            self.assertEqual(len(lines), 2000)
            counts = set()
            reads_total = 0
            pattern = (r'SELECT "paper_ycsb"\.read_modify_write\('
                       r'ARRAY\[(.*?)\]::text\[\],ARRAY\[(.*?)\]::text\[\]\);')
            for line in lines:
                match = re.fullmatch(pattern, line)
                self.assertIsNotNone(match)
                reads, updates = [re.findall(r"'user(\d+)'", group)
                                  for group in match.groups()]
                self.assertEqual(len(reads) + len(updates), 10)
                for keys in (reads, updates):
                    self.assertEqual(len(keys), len(set(keys)))
                    self.assertTrue(all(0 <= int(key) < 10000 for key in keys))
                counts.add(len(reads))
                reads_total += len(reads)
            self.assertGreater(len(counts), 5)  # Detect an incorrect fixed five/five mix.
            self.assertTrue(0.48 < reads_total / 20000 < 0.52)
            self.assertEqual(reads_total, workload["read_operations"])
            self.assertEqual(workload["internal_operations"], 20000)

    def test_load_population_and_field_width(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "input"
            paper.generate_package(output, [0.0], count=1)
            lines = (output / "data.sql").read_text().splitlines()
            self.assertEqual(lines[-1], "\\.")
            rows = [line.split("\t") for line in lines[1:-1]]
            self.assertEqual(len(rows), 10000)
            self.assertEqual([row[0] for row in rows], ["user%d" % i for i in range(10000)])
            self.assertTrue(all(len(row) == 11 for row in rows))
            self.assertTrue(all(re.fullmatch(r"[A-Za-z0-9]{10}", value)
                                for row in rows for value in row[1:]))

    def test_all_skews_reproducibility_and_manifest_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = [Path(directory) / name for name in ("first", "second")]
            paper.generate_package(first, paper.PAPER_SKEWS, count=25, seed=123)
            paper.generate_package(second, paper.PAPER_SKEWS, count=25, seed=123)
            self.assertEqual(sorted(p.name for p in first.iterdir()),
                             sorted(p.name for p in second.iterdir()))
            for path in first.iterdir():
                self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
            manifest = json.loads((first / "manifest.json").read_text())
            self.assertEqual(len(manifest["workloads"]), 6)
            for name, digest in manifest["sha256"].items():
                self.assertEqual(hashlib.sha256((first / name).read_bytes()).hexdigest(), digest)

    def test_zipf_and_uniform_selection(self):
        shares = []
        for skew in (0.0, 1.0):
            sampler = paper.ZipfSampler(skew, random.Random(123))
            keys = [sampler.next_key() for _ in range(20000)]
            self.assertTrue(all(0 <= key < 10000 for key in keys))
            shares.append(sum(key < 100 for key in keys) / len(keys))
        self.assertTrue(0.007 < shares[0] < 0.013)
        self.assertTrue(0.50 < shares[1] < 0.56)

    def test_reject_overwrite_and_invalid_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "input"
            paper.generate_package(output, [0.6], count=1)
            before = (output / "manifest.json").read_bytes()
            with self.assertRaises(FileExistsError):
                paper.generate_package(output, [0.6], count=2)
            self.assertEqual((output / "manifest.json").read_bytes(), before)
            for parameters in (dict(skews=[0.6], count=0), dict(skews=[0.6], seed=-1),
                               dict(skews=[0.6], schema='x"; DROP SCHEMA public; --'),
                               dict(skews=[0.6, 0.6]), dict(skews=[1.2]), dict(skews=[])):
                bad = Path(directory) / "bad"
                with self.assertRaises(ValueError):
                    paper.generate_package(bad, **parameters)
                self.assertFalse(bad.exists())


if __name__ == "__main__":
    unittest.main()
