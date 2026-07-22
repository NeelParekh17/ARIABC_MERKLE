import unittest
from app import (MerkleDatabase, child_prefix_hex, is_power_of,
                 normalize_workload_statement, prefix_bits, split_sql,
                 truncate_prefix_hex)

class ParserTests(unittest.TestCase):
    def test_semicolon_in_string(self):
        self.assertEqual(split_sql("UPDATE usertable_small SET field1='a;b' WHERE ycsb_key=1; SELECT 1;"), ["UPDATE usertable_small SET field1='a;b' WHERE ycsb_key=1", "SELECT 1"])

    def test_scopes_table(self):
        self.assertIn("merkle_viz.data", normalize_workload_statement("DELETE FROM public.usertable_small WHERE ycsb_key=1"))

    def test_rejects_ddl(self):
        with self.assertRaises(ValueError):
            normalize_workload_statement("DROP TABLE usertable_small")

    def test_prefix(self):
        self.assertEqual(prefix_bits("00" * 32, 0), "root")
        self.assertEqual(truncate_prefix_hex("ff" * 32, 1), "80" + "00" * 31)
        self.assertEqual(truncate_prefix_hex("ff" * 32, 9), "ff80" + "00" * 30)
        self.assertEqual(prefix_bits(child_prefix_hex("00" * 32, 0, 31), 5), "11111")
        self.assertEqual(prefix_bits(child_prefix_hex("a0" + "00" * 31, 5, 3), 10), "1010000011")

    def test_native_config_contract(self):
        self.assertTrue(is_power_of(32, 32))
        self.assertTrue(is_power_of(1024, 32))
        self.assertFalse(is_power_of(64, 32))
        valid = MerkleDatabase.validate_config({})
        self.assertEqual(valid["fanout"], 32)
        for bad in (
            {"partitions": 10001}, {"leaves_per_partition": 64},
            {"leaf_capacity": 1025}, {"leaf_byte_capacity": 1023},
            {"max_key_bytes": 2001},
            {"leaf_byte_capacity": 1024, "max_key_bytes": 1025},
        ):
            with self.assertRaises(ValueError):
                MerkleDatabase.validate_config(bad)

    def test_statement_key(self):
        self.assertEqual(MerkleDatabase.statement_key("SELECT * FROM merkle_viz.data WHERE ycsb_key=42"), 42)
        self.assertEqual(MerkleDatabase.statement_key("INSERT INTO merkle_viz.data VALUES (12001,'x')"), 12001)

    def test_frontier_transition_counters(self):
        parent = (3, 1, "00")
        left = (3, 2, "00")
        right = (3, 2, "40")
        self.assertEqual(MerkleDatabase._leaf_transition_delta({parent}, {left, right}), (1, 0))
        self.assertEqual(MerkleDatabase._leaf_transition_delta({left, right}, {parent}), (0, 1))

if __name__ == "__main__":
    unittest.main()
