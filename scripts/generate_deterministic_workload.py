#!/usr/bin/env python3
"""
generate_deterministic_workload.py — Deterministic Multi-Hazard Workload & Oracle Generator

Generates a mathematically deterministic test workload that contains:
1. Non-commutative arithmetic updates on shared accumulators (multiplication, addition, modulus).
2. Strict sequential step-state pipeline (chained RAW dependencies where reordering breaks the chain).
3. Double-entry bank account transfers with conservation law (sum(balance) == const).
4. Key lifecycle churn (INSERT -> UPDATE -> DELETE -> RE-INSERT on reused keys).

Also runs an internal reference simulator to produce an exact ground-truth oracle JSON.
"""

import argparse
import hashlib
import json
import os
import random
import sys
from typing import Any, Dict, List, Tuple


def build_workload(
    num_ops: int = 1000,
    num_accumulators: int = 5,
    num_accounts: int = 10,
    initial_balance: int = 10000,
    seed: int = 42,
) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Builds the bootstrap SQL statements, workload statements, and computes oracle state.
    """
    rng = random.Random(seed)

    # -------------------------------------------------------------------------
    # 1. Bootstrap statements
    # -------------------------------------------------------------------------
    bootstrap_sql = [
        "SET client_min_messages = warning;",
        "DROP TABLE IF EXISTS det_accumulators CASCADE;",
        """CREATE TABLE det_accumulators (
            id INT PRIMARY KEY,
            val BIGINT NOT NULL,
            ops_count INT NOT NULL DEFAULT 0
        );""",
        "CREATE INDEX idx_merkle_det_accumulators ON det_accumulators USING merkle (id);",

        "DROP TABLE IF EXISTS det_state_pipeline CASCADE;",
        """CREATE TABLE det_state_pipeline (
            id INT PRIMARY KEY,
            current_step INT NOT NULL,
            step_hash TEXT NOT NULL
        );""",
        "CREATE INDEX idx_merkle_det_state_pipeline ON det_state_pipeline USING merkle (id);",

        "DROP TABLE IF EXISTS det_accounts CASCADE;",
        """CREATE TABLE det_accounts (
            account_id INT PRIMARY KEY,
            balance BIGINT NOT NULL
        );""",
        "CREATE INDEX idx_merkle_det_accounts ON det_accounts USING merkle (account_id);",

        "DROP TABLE IF EXISTS det_lifecycle CASCADE;",
        """CREATE TABLE det_lifecycle (
            key_id INT PRIMARY KEY,
            version INT NOT NULL,
            payload TEXT NOT NULL
        );""",
        "CREATE INDEX idx_merkle_det_lifecycle ON det_lifecycle USING merkle (key_id);",
    ]

    # Seed data
    acc_seed_values = {}
    for i in range(1, num_accumulators + 1):
        v = i * 100
        acc_seed_values[i] = v
        bootstrap_sql.append(f"INSERT INTO det_accumulators (id, val, ops_count) VALUES ({i}, {v}, 0);")

    pipeline_seed = {1: (0, "init_0")}
    bootstrap_sql.append("INSERT INTO det_state_pipeline (id, current_step, step_hash) VALUES (1, 0, 'init_0');")

    account_seed_values = {}
    for i in range(1, num_accounts + 1):
        account_seed_values[i] = initial_balance
        bootstrap_sql.append(f"INSERT INTO det_accounts (account_id, balance) VALUES ({i}, {initial_balance});")

    # -------------------------------------------------------------------------
    # 2. Simulator State (Oracle)
    # -------------------------------------------------------------------------
    sim_accumulators = {i: {"val": acc_seed_values[i], "ops_count": 0} for i in range(1, num_accumulators + 1)}
    sim_pipeline = {1: {"current_step": 0, "step_hash": "init_0"}}
    sim_accounts = {i: initial_balance for i in range(1, num_accounts + 1)}
    sim_lifecycle: Dict[int, Dict[str, Any]] = {}

    workload_statements = []

    # Prime modulus for arithmetic
    PRIME_MOD = 1000003

    # Lifecycle state machine per key: State in [0: none, 1: v1, 2: v2, 3: deleted, 4: v3, 5: v4]
    lifecycle_keys = [1001, 1002, 1003, 1004, 1005]
    lifecycle_state = {k: 0 for k in lifecycle_keys}

    # Generate operations
    # Mix distribution:
    # 35% Accumulators (non-commutative math)
    # 25% Accounts (conservation transfers)
    # 20% State pipeline (strict sequential dependency)
    # 20% Lifecycle (insert / update / delete / re-insert)

    for op_idx in range(num_ops):
        r = rng.random()

        if r < 0.35:
            # Pattern 1: Non-commutative arithmetic
            # Pick a hot accumulator with bias toward key 1
            if rng.random() < 0.6:
                target_id = 1
            else:
                target_id = rng.randint(1, num_accumulators)

            math_op = rng.choice(["add", "mult", "sub", "mult_prime"])
            cur_val = sim_accumulators[target_id]["val"]

            if math_op == "add":
                inc = rng.randint(1, 99)
                sql = f"UPDATE det_accumulators SET val = (val + {inc}) % {PRIME_MOD}, ops_count = ops_count + 1 WHERE id = {target_id};"
                sim_accumulators[target_id]["val"] = (cur_val + inc) % PRIME_MOD
            elif math_op == "mult":
                factor = rng.choice([3, 5, 7])
                sql = f"UPDATE det_accumulators SET val = (val * {factor}) % {PRIME_MOD}, ops_count = ops_count + 1 WHERE id = {target_id};"
                sim_accumulators[target_id]["val"] = (cur_val * factor) % PRIME_MOD
            elif math_op == "sub":
                dec = rng.randint(1, 49)
                sql = f"UPDATE det_accumulators SET val = (val - {dec} + {PRIME_MOD}) % {PRIME_MOD}, ops_count = ops_count + 1 WHERE id = {target_id};"
                sim_accumulators[target_id]["val"] = (cur_val - dec + PRIME_MOD) % PRIME_MOD
            else:
                factor = 11
                sql = f"UPDATE det_accumulators SET val = (val * {factor} + 13) % {PRIME_MOD}, ops_count = ops_count + 1 WHERE id = {target_id};"
                sim_accumulators[target_id]["val"] = (cur_val * factor + 13) % PRIME_MOD

            sim_accumulators[target_id]["ops_count"] += 1
            workload_statements.append(sql)

        elif r < 0.60:
            # Pattern 2: Double-entry bank account transfers
            # Pick two distinct accounts with Zipfian skew toward 1, 2, 3
            def pick_account():
                if rng.random() < 0.5:
                    return rng.choice([1, 2, 3])
                return rng.randint(1, num_accounts)

            src = pick_account()
            dst = pick_account()
            while dst == src:
                dst = pick_account()

            amount = rng.randint(10, 200)

            # Atomic single-statement transfer
            sql = (
                f"UPDATE det_accounts SET balance = CASE "
                f"WHEN account_id = {src} THEN balance - {amount} "
                f"WHEN account_id = {dst} THEN balance + {amount} "
                f"ELSE balance END "
                f"WHERE account_id IN ({src}, {dst});"
            )
            sim_accounts[src] -= amount
            sim_accounts[dst] += amount
            workload_statements.append(sql)

        elif r < 0.80:
            # Pattern 3: State machine pipeline (Strict RAW chain)
            pipe_id = 1
            cur_step = sim_pipeline[pipe_id]["current_step"]
            next_step = cur_step + 1
            prev_hash = sim_pipeline[pipe_id]["step_hash"]
            # Compute new hash via md5(prev_hash || ':' || next_step)
            hasher = hashlib.md5()
            hasher.update(f"{prev_hash}:{next_step}".encode("utf-8"))
            new_hash = hasher.hexdigest()

            sql = (
                f"UPDATE det_state_pipeline SET current_step = {next_step}, "
                f"step_hash = md5(step_hash || ':{next_step}') "
                f"WHERE id = {pipe_id} AND current_step = {cur_step};"
            )
            sim_pipeline[pipe_id]["current_step"] = next_step
            sim_pipeline[pipe_id]["step_hash"] = new_hash
            workload_statements.append(sql)

        else:
            # Pattern 4: Key lifecycle churn (INSERT -> UPDATE -> DELETE -> RE-INSERT)
            k = rng.choice(lifecycle_keys)
            st = lifecycle_state[k]

            if st == 0:
                # INSERT v1
                sql = f"INSERT INTO det_lifecycle (key_id, version, payload) VALUES ({k}, 1, 'v1_seed_{op_idx}');"
                sim_lifecycle[k] = {"key_id": k, "version": 1, "payload": f"v1_seed_{op_idx}"}
                lifecycle_state[k] = 1
            elif st == 1:
                # UPDATE to v2
                sql = f"UPDATE det_lifecycle SET version = 2, payload = 'v2_updated_{op_idx}' WHERE key_id = {k};"
                sim_lifecycle[k] = {"key_id": k, "version": 2, "payload": f"v2_updated_{op_idx}"}
                lifecycle_state[k] = 2
            elif st == 2:
                # DELETE
                sql = f"DELETE FROM det_lifecycle WHERE key_id = {k};"
                del sim_lifecycle[k]
                lifecycle_state[k] = 3
            elif st == 3:
                # RE-INSERT as v3
                sql = f"INSERT INTO det_lifecycle (key_id, version, payload) VALUES ({k}, 3, 'v3_reborn_{op_idx}');"
                sim_lifecycle[k] = {"key_id": k, "version": 3, "payload": f"v3_reborn_{op_idx}"}
                lifecycle_state[k] = 4
            else:
                # UPDATE to v4
                sql = f"UPDATE det_lifecycle SET version = 4, payload = 'v4_final_{op_idx}' WHERE key_id = {k};"
                sim_lifecycle[k] = {"key_id": k, "version": 4, "payload": f"v4_final_{op_idx}"}
                lifecycle_state[k] = 2  # loop back to state 2 for delete

            workload_statements.append(sql)

    # -------------------------------------------------------------------------
    # 3. Assemble Oracle State Summary
    # -------------------------------------------------------------------------
    total_balance_expected = num_accounts * initial_balance
    total_balance_actual = sum(sim_accounts.values())
    assert total_balance_actual == total_balance_expected, (
        f"Conservation invariant violation: {total_balance_actual} != {total_balance_expected}"
    )

    oracle = {
        "metadata": {
            "num_ops": len(workload_statements),
            "num_accumulators": num_accumulators,
            "num_accounts": num_accounts,
            "initial_balance": initial_balance,
            "total_balance_expected": total_balance_expected,
            "seed": seed,
        },
        "det_accumulators": [
            {"id": i, "val": sim_accumulators[i]["val"], "ops_count": sim_accumulators[i]["ops_count"]}
            for i in sorted(sim_accumulators.keys())
        ],
        "det_state_pipeline": [
            {"id": 1, "current_step": sim_pipeline[1]["current_step"], "step_hash": sim_pipeline[1]["step_hash"]}
        ],
        "det_accounts": [
            {"account_id": i, "balance": sim_accounts[i]}
            for i in sorted(sim_accounts.keys())
        ],
        "det_lifecycle": [
            {"key_id": k, "version": sim_lifecycle[k]["version"], "payload": sim_lifecycle[k]["payload"]}
            for k in sorted(sim_lifecycle.keys())
        ],
    }

    return bootstrap_sql, workload_statements, oracle


def main():
    parser = argparse.ArgumentParser(description="Generate deterministic verification workload and oracle.")
    parser.add_argument("--ops", type=int, default=1000, help="Number of workload DML operations.")
    parser.add_argument("--accumulators", type=int, default=5, help="Number of accumulator records.")
    parser.add_argument("--accounts", type=int, default=10, help="Number of bank accounts.")
    parser.add_argument("--initial-balance", type=int, default=10000, help="Initial balance per account.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable workload generation.")
    parser.add_argument("--output-workload", default="scripts/deterministic_workload.sql", help="Raw SQL workload file.")
    parser.add_argument("--output-det-workload", default="scripts/deterministic_workload_s_prefixed.sql", help="s-prefixed SQL file.")
    parser.add_argument("--output-bootstrap", default="scripts/bootstrap_det_verification.sql", help="Bootstrap SQL file.")
    parser.add_argument("--output-oracle", default="scripts/deterministic_oracle.json", help="Ground truth JSON file.")
    args = parser.parse_args()

    bootstrap_sql, workload, oracle = build_workload(
        num_ops=args.ops,
        num_accumulators=args.accumulators,
        num_accounts=args.accounts,
        initial_balance=args.initial_balance,
        seed=args.seed,
    )

    # Write bootstrap SQL
    with open(args.output_bootstrap, "w") as f:
        for stmt in bootstrap_sql:
            f.write(stmt.strip() + "\n")
    print(f"Generated bootstrap SQL: {args.output_bootstrap} ({len(bootstrap_sql)} statements)")

    # Write raw workload SQL
    with open(args.output_workload, "w") as f:
        for stmt in workload:
            f.write(stmt.strip() + "\n")
    print(f"Generated raw workload SQL: {args.output_workload} ({len(workload)} operations)")

    # Write s-prefixed workload SQL (dbType=1 format: 's 00000000 <sql>')
    with open(args.output_det_workload, "w") as f:
        for seq, stmt in enumerate(workload):
            f.write(f"s {seq:08d} {stmt.strip()}\n")
    print(f"Generated s-prefixed workload SQL: {args.output_det_workload} (seq 0 to {len(workload)-1})")

    # Write oracle JSON
    with open(args.output_oracle, "w") as f:
        json.dump(oracle, f, indent=2)
    print(f"Generated oracle JSON: {args.output_oracle}")
    print(f"Oracle summary: {len(oracle['det_accumulators'])} accumulators, pipeline step {oracle['det_state_pipeline'][0]['current_step']}, {len(oracle['det_accounts'])} accounts, {len(oracle['det_lifecycle'])} lifecycle rows.")


if __name__ == "__main__":
    main()
