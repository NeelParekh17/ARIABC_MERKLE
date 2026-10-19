#!/usr/bin/env python3
"""
Official YCSB Workload Generator for AriaBC / PostgreSQL.

Generates official YCSB workloads (A, B, C, D, E, F) with customizable Zipfian skew
(theta), operation mixes, transaction counts, and target table configurations.

Standard YCSB Workloads:
  - Workload A (Update Heavy): 50% Read, 50% Update
  - Workload B (Read Mostly):  95% Read, 5% Update
  - Workload C (Read Only):    100% Read
  - Workload D (Read Latest):  95% Read, 5% Insert (newest keys read most)
  - Workload E (Short Scans):  95% Scan (range), 5% Insert
  - Workload F (Read-Mod-Write): 50% Read, 50% RMW (Update)

Skew Values (theta):
  - 0.00: Uniform (no skew)
  - 0.20: Low skew
  - 0.50: Medium skew
  - 0.70: Moderate skew
  - 0.80: High skew
  - 0.90: Very high skew
  - 0.99: Standard YCSB default high Zipfian skew
  - 1.20: Extreme hotspot skew
"""

import argparse
import bisect
import math
import os
import random
import string
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


class ZipfGenerator:
    """Exact inverse-CDF Zipfian generator over 1..n items."""

    def __init__(self, n: int, theta: float, rng: random.Random):
        self.n = max(1, n)
        self.theta = float(theta)
        self.rng = rng

        if self.theta <= 1e-6:
            self.uniform = True
            self.dist_map = None
        else:
            self.uniform = False
            # Compute harmonic numbers / cumulative sum: sum_{i=1}^n 1 / i^theta
            weights = [1.0 / math.pow(float(i), self.theta) for i in range(1, self.n + 1)]
            cum_sum = []
            running = 0.0
            for w in weights:
                running += w
                cum_sum.append(running)
            total = running
            self.dist_map = [c / total for c in cum_sum]
            self.dist_map[-1] = 1.0

    def next_index(self) -> int:
        """Returns 1-based index in [1, n]."""
        if self.uniform:
            return self.rng.randint(1, self.n)
        r = self.rng.random()
        idx = bisect.bisect_left(self.dist_map, r)
        return min(idx + 1, self.n)


def generate_random_field(length: int = 20, rng: random.Random = None) -> str:
    """Generates a random 20-character alphanumeric string matching YCSB fields."""
    if rng is None:
        rng = random
    chars = string.ascii_letters + string.digits
    return "".join(rng.choice(chars) for _ in range(length))


def generate_sql_select(table: str, key_col: str, key_val: int) -> str:
    return f"SELECT * FROM {table} WHERE {key_col}={key_val};"


def generate_sql_update(table: str, key_col: str, key_val: int, num_fields: int = 10, rng: random.Random = None) -> str:
    set_clauses = [f"FIELD{i}='{generate_random_field(20, rng)}'" for i in range(1, num_fields + 1)]
    return f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {key_col}={key_val};"


def generate_sql_rmw(table: str, key_col: str, key_val: int, num_fields: int, rng: random.Random) -> str:
    """One SQL request and transaction: materialize the read, then update its row."""
    update = generate_sql_update(table, key_col, key_val, num_fields, rng)
    assignments = update.split(' SET ', 1)[1].split(' WHERE ', 1)[0]
    return (f"WITH ycsb_read AS MATERIALIZED (SELECT * FROM {table} WHERE {key_col}={key_val}) "
            f"UPDATE {table} SET {assignments} FROM ycsb_read "
            f"WHERE {table}.{key_col}=ycsb_read.{key_col} RETURNING {table}.*;")


def generate_sql_insert(table: str, key_col: str, key_val: int, num_fields: int = 10, rng: random.Random = None) -> str:
    col_names = [key_col] + [f"FIELD{i}" for i in range(1, num_fields + 1)]
    field_values = [str(key_val)] + [f"'{generate_random_field(20, rng)}'" for _ in range(num_fields)]
    return f"INSERT INTO {table} ({', '.join(col_names)}) VALUES ({', '.join(field_values)});"


def generate_sql_delete(table: str, key_col: str, key_val: int) -> str:
    return f"DELETE FROM {table} WHERE {key_col}={key_val};"


def generate_sql_scan(table: str, key_col: str, key_val: int, max_scan: int = 100, rng: random.Random = None) -> str:
    if rng is None:
        rng = random
    scan_len = rng.randint(1, max_scan)
    return f"SELECT * FROM {table} WHERE {key_col} >= {key_val} ORDER BY {key_col} LIMIT {scan_len};"


WORKLOAD_MIXES: Dict[str, Dict[str, float]] = {
    # Workload A: 50% Read, 50% Update
    "A": {"read": 0.50, "update": 0.50, "insert": 0.00, "delete": 0.00, "scan": 0.00},
    # Workload B: 95% Read, 5% Update
    "B": {"read": 0.95, "update": 0.05, "insert": 0.00, "delete": 0.00, "scan": 0.00},
    # Workload C: 100% Read
    "C": {"read": 1.00, "update": 0.00, "insert": 0.00, "delete": 0.00, "scan": 0.00},
    # Workload D: 95% Read, 5% Insert (Read latest)
    "D": {"read": 0.95, "update": 0.00, "insert": 0.05, "delete": 0.00, "scan": 0.00},
    # Workload E: 95% Scan, 5% Insert (Short ranges)
    "E": {"read": 0.00, "update": 0.00, "insert": 0.05, "delete": 0.00, "scan": 0.95},
    # Workload F: 50% Read, 50% Read-Modify-Write
    "F": {"read": 0.50, "update": 0.50, "insert": 0.00, "delete": 0.00, "scan": 0.00},
    # Workload Balanced (AriaBC custom standard mixed): 60% Read, 20% Update, 10% Insert, 10% Delete
    "MIXED": {"read": 0.60, "update": 0.20, "insert": 0.10, "delete": 0.10, "scan": 0.00},
    # Workload DML_HEAVY (Write-Heavy): 10% Read, 50% Update, 20% Insert, 20% Delete (90% DML)
    "DML_HEAVY": {"read": 0.10, "update": 0.50, "insert": 0.20, "delete": 0.20, "scan": 0.00},
    # Workload DELETE_HEAVY (Tombstone/Churn-Heavy): 10% Read, 20% Update, 20% Insert, 50% Delete (90% DML)
    "DELETE_HEAVY": {"read": 0.10, "update": 0.20, "insert": 0.20, "delete": 0.50, "scan": 0.00},
    # Workload PURE_DML (Write-Only Mutation): 0% Read, 50% Update, 25% Insert, 25% Delete (100% DML)
    "PURE_DML": {"read": 0.00, "update": 0.50, "insert": 0.25, "delete": 0.25, "scan": 0.00},
    # Workload BALANCED_DML (Multi-DML Balanced): 20% Read, 40% Update, 20% Insert, 20% Delete (80% DML)
    "BALANCED_DML": {"read": 0.20, "update": 0.40, "insert": 0.20, "delete": 0.20, "scan": 0.00},
    # Pure single-operation workloads:
    # 100% Updates: Pure write-in-place state mutation on pre-loaded rows
    "ALL_UPDATE": {"read": 0.00, "update": 1.00, "insert": 0.00, "delete": 0.00, "scan": 0.00},
    # 100% Inserts: Pure append with clustered leaf-page skew
    "ALL_INSERT": {"read": 0.00, "update": 0.00, "insert": 1.00, "delete": 0.00, "scan": 0.00},
    # 100% Deletes: Pure table shrinking and tombstone creation
    "ALL_DELETE": {"read": 0.00, "update": 0.00, "insert": 0.00, "delete": 1.00, "scan": 0.00},
}


class PartitionedKeyManager:
    """Maintains partitioned master keys (for updates/reads) and transient keys (for inserts/deletes).

    To ensure Zipfian skew is preserved without key dilution or primary key collisions:
      - Master keys 1..num_master are permanent (never deleted).
      - Updates and reads target 1..num_master using the requested Zipfian distribution.
      - Cold initial keys (num_master+1 .. num_keys) and newly inserted transient keys
        are used for DELETE operations so deletes always target existing records.
      - INSERT operations either recycle deleted keys or allocate fresh keys > num_keys,
        guaranteeing zero duplicate key collisions.
    """
    def __init__(self, num_keys: int, num_master: int, rng: random.Random):
        self.num_keys = num_keys
        self.num_master = num_master
        self.rng = rng

        # Initial cold deletable keys from the pre-loaded table
        self.cold_deletable = list(range(num_master + 1, num_keys + 1))
        self.rng.shuffle(self.cold_deletable)

        # Live transient keys that have been inserted during this stream
        self.live_transient = []

        # Tombstones available for recycling by INSERT
        self.deleted_keys = []
        self.next_insert_key = num_keys + 1

    def get_read_or_update_key(self, candidate: int) -> int:
        """Returns a valid master key. Candidate comes from ZipfGenerator(num_master, theta)."""
        return min(max(1, candidate), self.num_master)

    def insert(self, recycle_prob: float = 0.75) -> int:
        """Inserts a key guaranteed to NOT exist currently in the database."""
        if self.deleted_keys and self.rng.random() < recycle_prob:
            key = self.deleted_keys.pop()
        else:
            key = self.next_insert_key
            self.next_insert_key += 1

        self.live_transient.append(key)
        return key

    def delete(self) -> int:
        """Deletes a key guaranteed to exist in the database, without touching master keys."""
        if self.live_transient and (self.rng.random() < 0.60 or not self.cold_deletable):
            idx = self.rng.randint(0, len(self.live_transient) - 1)
            key = self.live_transient[idx]
            last = self.live_transient.pop()
            if idx < len(self.live_transient):
                self.live_transient[idx] = last
        elif self.cold_deletable:
            key = self.cold_deletable.pop()
        else:
            key = self.rng.randint(self.num_master + 1, self.next_insert_key)

        self.deleted_keys.append(key)
        return key


def generate_workload_statements(
    workload_type: str,
    skew: float,
    num_tx: int = 20000,
    num_keys: int = 12000,
    table_name: str = "usertable_small",
    key_column: str = "YCSB_KEY",
    num_fields: int = 10,
    seed: int = 42,
    custom_mix: Dict[str, float] = None,
    zipf_factory=ZipfGenerator,
) -> List[str]:
    """Generates a list of SQL queries for the given YCSB workload configuration."""
    rng = random.Random(seed)

    wl_upper = workload_type.upper()

    # Specialized pure single-operation workloads
    if wl_upper == "ALL_UPDATE":
        zipf_gen = zipf_factory(num_keys, skew, rng)
        statements = []
        for _ in range(num_tx):
            candidate = zipf_gen.next_index()
            statements.append(generate_sql_update(table_name, key_column, candidate, num_fields, rng))
        return statements

    if wl_upper == "ALL_INSERT":
        # Clustered B-tree leaf page skew:
        # Partition key space into buckets of size B = 32 keys (matching an 8KB leaf page)
        # ZipfGenerator selects which bucket/page to insert into.
        # Within that bucket, a unique sequential key is allocated.
        num_buckets = max(10, num_tx // 32)
        bucket_zipf = zipf_factory(num_buckets, skew, rng)
        bucket_offsets = defaultdict(int)
        base_insert_key = num_keys + 1
        statements = []
        for _ in range(num_tx):
            b = bucket_zipf.next_index()
            offset = bucket_offsets[b]
            bucket_offsets[b] += 1
            key = base_insert_key + (b - 1) * (num_tx * 2) + offset
            statements.append(generate_sql_insert(table_name, key_column, key, num_fields, rng))
        return statements

    if wl_upper == "ALL_DELETE":
        zipf_gen = zipf_factory(num_keys, skew, rng)
        statements = []
        for _ in range(num_tx):
            candidate = zipf_gen.next_index()
            statements.append(generate_sql_delete(table_name, key_column, candidate))
        return statements

    is_dml_workload = wl_upper in ("BALANCED_DML", "DELETE_HEAVY", "DML_HEAVY", "PURE_DML")

    if is_dml_workload:
        if wl_upper == "DELETE_HEAVY":
            num_master = min(4000, num_keys // 3)
        else:
            num_master = min(8000, int(num_keys * 0.67))
    else:
        num_master = num_keys

    zipf_gen = zipf_factory(num_master, skew, rng)
    km = PartitionedKeyManager(num_keys=num_keys, num_master=num_master, rng=rng)

    mix = custom_mix or WORKLOAD_MIXES.get(wl_upper, WORKLOAD_MIXES["A"])

    r_prob = mix.get("read", 0.0)
    u_prob = r_prob + mix.get("update", 0.0)
    i_prob = u_prob + mix.get("insert", 0.0)
    d_prob = i_prob + mix.get("delete", 0.0)
    s_prob = d_prob + mix.get("scan", 0.0)

    # For Workload D (read latest), track pending inserts to acknowledge them after
    # a safe commit horizon (1500 operations, or 1/4 of total if short test).
    # Reads target the latest acknowledged keys using the Zipfian distribution,
    # ensuring recently inserted records are read heavily while preventing reads
    # of in-flight or uncommitted keys across concurrent/pipelined client workers.
    d_commit_horizon = min(1500, max(1, num_tx // 4))
    d_pending_inserts: List[Tuple[int, int]] = []
    d_acknowledged_max = num_keys
    statements = []

    while len(statements) < num_tx:
        current_idx = len(statements)
        if wl_upper == "D":
            while d_pending_inserts and (current_idx - d_pending_inserts[0][0]) >= d_commit_horizon:
                d_acknowledged_max = max(d_acknowledged_max, d_pending_inserts.pop(0)[1])

        coin = rng.random()

        if coin < r_prob:
            # Read
            if wl_upper == "D":
                offset = zipf_gen.next_index() - 1
                key = max(1, d_acknowledged_max - offset)
            else:
                candidate = zipf_gen.next_index()
                key = km.get_read_or_update_key(candidate)
            statements.append(generate_sql_select(table_name, key_column, key))

        elif coin < u_prob:
            # Update (or RMW in Workload F)
            candidate = zipf_gen.next_index()
            key = km.get_read_or_update_key(candidate)
            if wl_upper == "F":
                statements.append(generate_sql_rmw(table_name, key_column, key, num_fields, rng))
            else:
                statements.append(generate_sql_update(table_name, key_column, key, num_fields, rng))

        elif coin < i_prob:
            # Insert: guaranteed unique, monotonic new key for D, recycled for others if deleted keys exist
            if wl_upper == "D":
                key = km.insert(recycle_prob=0.0)
                d_pending_inserts.append((current_idx, key))
            else:
                key = km.insert(recycle_prob=0.75)
            statements.append(generate_sql_insert(table_name, key_column, key, num_fields, rng))

        elif coin < d_prob:
            # Delete: guaranteed to delete an existing row without wiping out master keys
            key = km.delete()
            statements.append(generate_sql_delete(table_name, key_column, key))

        else:
            # Scan
            candidate = zipf_gen.next_index()
            key = km.get_read_or_update_key(candidate)
            statements.append(generate_sql_scan(table_name, key_column, key, max_scan=50, rng=rng))

    return statements


def batch_generate_standard_suite(output_dir: Path, tx_count: int = 20000, num_keys: int = 12000, seed: int = 42):
    """Batch generates the standard official YCSB matrix across skews and workload types."""
    output_dir.mkdir(parents=True, exist_ok=True)
    skews = [0.0, 0.2, 0.5, 0.7, 0.8, 0.9, 0.99, 1.2]
    workloads = ["A", "B", "C", "D", "F"]

    generated_files = []
    for wl in workloads:
        for skew in skews:
            skew_str = f"{skew:.2f}".replace(".", "_")
            fname = f"ycsb_workload_{wl.lower()}_skew_{skew_str}_{tx_count // 1000}k.txt"
            target_path = output_dir / fname
            stmts = generate_workload_statements(
                workload_type=wl,
                skew=skew,
                num_tx=tx_count,
                num_keys=num_keys,
                seed=seed + int(skew * 100),
            )
            with open(target_path, "w", encoding="utf-8") as f:
                for s in stmts:
                    f.write(s + "\n")
            generated_files.append((fname, wl, skew, len(stmts)))
            print(f"Generated {fname} ({wl}, skew={skew}, {len(stmts)} txs)")

    return generated_files


def batch_generate_dml_suite(output_dir: Path, tx_count: int = 20000, num_keys: int = 12000, seed: int = 42):
    """Batch generates the DML-heavy and DELETE-enabled YCSB workload matrix across skews."""
    output_dir.mkdir(parents=True, exist_ok=True)
    skews = [0.0, 0.2, 0.5, 0.7, 0.8, 0.9, 0.99, 1.2]
    workloads = ["DML_HEAVY", "DELETE_HEAVY", "PURE_DML", "BALANCED_DML"]

    generated_files = []
    for wl in workloads:
        for skew in skews:
            skew_str = f"{skew:.2f}".replace(".", "_")
            fname = f"ycsb_workload_{wl.lower()}_skew_{skew_str}_{tx_count // 1000}k.txt"
            target_path = output_dir / fname
            stmts = generate_workload_statements(
                workload_type=wl,
                skew=skew,
                num_tx=tx_count,
                num_keys=num_keys,
                seed=seed + int(skew * 100),
            )
            with open(target_path, "w", encoding="utf-8") as f:
                for s in stmts:
                    f.write(s + "\n")
            generated_files.append((fname, wl, skew, len(stmts)))
            print(f"Generated {fname} ({wl}, skew={skew}, {len(stmts)} txs)")

    return generated_files


def batch_generate_pure_suite(output_dir: Path, tx_count: int = 20000, num_keys: int = 12000, seed: int = 42):
    """Batch generates the pure single-operation workloads (ALL_UPDATE, ALL_INSERT, ALL_DELETE) across skews."""
    output_dir.mkdir(parents=True, exist_ok=True)
    skews = [0.0, 0.2, 0.5, 0.7, 0.8, 0.9, 0.99, 1.2]
    workloads = ["ALL_UPDATE", "ALL_INSERT", "ALL_DELETE"]

    generated_files = []
    for wl in workloads:
        for skew in skews:
            skew_str = f"{skew:.2f}".replace(".", "_")
            fname = f"ycsb_workload_{wl.lower()}_skew_{skew_str}_{tx_count // 1000}k.txt"
            target_path = output_dir / fname
            stmts = generate_workload_statements(
                workload_type=wl,
                skew=skew,
                num_tx=tx_count,
                num_keys=num_keys,
                seed=seed + int(skew * 100),
            )
            with open(target_path, "w", encoding="utf-8") as f:
                for s in stmts:
                    f.write(s + "\n")
            generated_files.append((fname, wl, skew, len(stmts)))
            print(f"Generated {fname} ({wl}, skew={skew}, {len(stmts)} txs)")

    return generated_files


def main():
    parser = argparse.ArgumentParser(description="Generate official YCSB workloads with varying skews.")
    parser.add_argument(
        "--workload",
        "-w",
        type=str,
        default="A",
        choices=[
            "A", "B", "C", "D", "E", "F", "MIXED",
            "DML_HEAVY", "DELETE_HEAVY", "PURE_DML", "BALANCED_DML",
            "ALL_UPDATE", "ALL_INSERT", "ALL_DELETE",
            "PURE_ALL", "DML_ALL", "ALL"
        ],
        help="YCSB workload type (A..F, DML workloads, ALL_UPDATE, ALL_INSERT, ALL_DELETE, PURE_ALL, DML_ALL, or ALL)",
    )
    parser.add_argument(
        "--skew",
        "-s",
        type=float,
        default=0.99,
        help="Zipfian skew parameter theta (e.g. 0.0 for uniform, 0.2, 0.5, 0.8, 0.99, 1.2)",
    )
    parser.add_argument(
        "--tx-count",
        "-n",
        type=int,
        default=20000,
        help="Number of transactions to generate (default: 20000)",
    )
    parser.add_argument(
        "--num-keys",
        "-k",
        type=int,
        default=12000,
        help="Number of initial keys in the table (default: 12000 matching restore_usertable_small.sql)",
    )
    parser.add_argument(
        "--table",
        "-t",
        type=str,
        default="usertable_small",
        help="Target database table name (default: usertable_small)",
    )
    parser.add_argument(
        "--key-col",
        type=str,
        default="YCSB_KEY",
        help="Primary key column name (default: YCSB_KEY)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="",
        help="Output file path (default: auto-named in scripts/)",
    )
    parser.add_argument(
        "--batch-dir",
        type=str,
        default="scripts/ycsb_suite",
        help="Directory for batch generation when --workload ALL is chosen",
    )

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent

    if args.workload == "PURE_ALL":
        target_dir = repo_root / args.batch_dir
        print(f"Batch generating Pure Single-Operation YCSB suite (ALL_UPDATE, ALL_INSERT, ALL_DELETE) into {target_dir}...")
        batch_generate_pure_suite(target_dir, args.tx_count, args.num_keys, args.seed)
        print(f"Done! Pure Single-Operation Suite generated in {target_dir}")
        return

    if args.workload == "DML_ALL":
        target_dir = repo_root / args.batch_dir
        print(f"Batch generating DML & DELETE YCSB workload suite into {target_dir}...")
        batch_generate_dml_suite(target_dir, args.tx_count, args.num_keys, args.seed)
        print(f"Done! DML Suite generated in {target_dir}")
        return

    if args.workload == "ALL":
        target_dir = repo_root / args.batch_dir
        print(f"Batch generating complete YCSB workload suite into {target_dir}...")
        batch_generate_standard_suite(target_dir, args.tx_count, args.num_keys, args.seed)
        batch_generate_dml_suite(target_dir, args.tx_count, args.num_keys, args.seed)
        batch_generate_pure_suite(target_dir, args.tx_count, args.num_keys, args.seed)
        print(f"Done! All suites generated in {target_dir}")
        return

    stmts = generate_workload_statements(
        workload_type=args.workload,
        skew=args.skew,
        num_tx=args.tx_count,
        num_keys=args.num_keys,
        table_name=args.table,
        key_column=args.key_col,
        seed=args.seed,
    )

    if args.output:
        out_path = Path(args.output)
    else:
        skew_str = f"{args.skew:.2f}".replace(".", "_")
        out_name = f"ycsb_workload_{args.workload.lower()}_skew_{skew_str}_{args.tx_count // 1000}k.txt"
        out_path = repo_root / "scripts" / out_name

    with open(out_path, "w", encoding="utf-8") as f:
        for s in stmts:
            f.write(s + "\n")

    print(f"Successfully generated {len(stmts)} transactions for YCSB Workload {args.workload.upper()} (skew={args.skew}) -> {out_path}")


if __name__ == "__main__":
    main()
