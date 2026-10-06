#!/usr/bin/env python3
"""Generate the transactional YCSB workload from DOI 10.1145/3588952.

Each gateway input line calls one PL/pgSQL function containing ten operations.
The paper fixes 10,000 keys and independently chooses reads/updates with equal
probability. Row layout, duplicate handling and seeded SQL updates follow the
author's public AriaBC implementation; the original experimental trace is not
published. Run generation and validation on the lab host as required by AGENTS.md.
"""

import argparse
import bisect
import collections
import hashlib
import json
import math
from pathlib import Path
import random
import re
import sys


KEYS = 10000
OPERATIONS = 10
FIELDS = 10
FIELD_LENGTH = 10
PAPER_SKEWS = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
DOI = "10.1145/3588952"
AUTHOR_COMMIT = "f5e16ccb6898a572b768d9bfb5de4fb384dab714"
ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


class ZipfSampler:
    """Inverse CDF over zero-based ranks, with weight (rank + 1) ** -theta."""

    def __init__(self, theta, rng):
        if not math.isfinite(theta) or not 0 <= theta <= 1:
            raise ValueError("skew must be finite and between 0 and 1")
        cumulative = [0.0]
        for rank in range(1, KEYS + 1):
            cumulative.append(cumulative[-1] + rank ** -theta)
        total = cumulative[-1]
        self.cdf = [value / total for value in cumulative]
        self.cdf[-1] = 1.0
        self.rng = rng

    def next_key(self):
        return bisect.bisect_right(self.cdf, self.rng.random()) - 1


def transactions(skew, count, seed):
    """Yield read/update key lists; preserve the author's within-list uniqueness.

    The same key may be read and updated in one transaction. All reads execute
    before all updates, as in the author's read_modify_write function. Separate
    random streams keep the 50/50 operation choices independent of key retries.
    """
    if count < 1 or seed < 0:
        raise ValueError("transactions must be positive and seed nonnegative")
    sampler = ZipfSampler(skew, random.Random(seed))
    operation_rng = random.Random(seed + 1)
    for _ in range(count):
        reads, updates = [], []
        for _ in range(OPERATIONS):
            destination = reads if operation_rng.random() < 0.5 else updates
            key = sampler.next_key()
            while key in destination:
                key = sampler.next_key()
            destination.append(key)
        yield reads, updates


def sql_array(keys):
    return "ARRAY[" + ",".join("'user%d'" % key for key in keys) + "]::text[]"


def transaction_sql(schema, reads, updates):
    return ('SELECT "%s".read_modify_write(%s,%s);'
            % (schema, sql_array(reads), sql_array(updates)))


def setup_sql(schema):
    relation = '"%s".usertable' % schema
    fields = ",\n    ".join("field%d text NOT NULL" % i for i in range(FIELDS))
    assignments = ",".join("field%d = $%d" % (i, i + 1) for i in range(FIELDS))
    random_values = ",".join('"%s".random_string(10)' % schema for _ in range(FIELDS))
    return r'''\set ON_ERROR_STOP on
-- Fresh, isolated paper workload schema; fail if it already exists.
BEGIN;
CREATE SCHEMA "%(schema)s";
CREATE TABLE %(relation)s (
    ycsb_key varchar(255) PRIMARY KEY,
    %(fields)s
);
CREATE INDEX usertable_key_hash ON %(relation)s USING hash (ycsb_key);

CREATE FUNCTION "%(schema)s".random_string(length integer) RETURNS text AS
$paper_ycsb$
DECLARE
    chars text[] := '{0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z}';
    result text := '';
BEGIN
    FOR i IN 1..length LOOP
        result := result || chars[1 + random() * (array_length(chars, 1) - 1)];
    END LOOP;
    RETURN result;
END;
$paper_ycsb$ LANGUAGE plpgsql;

CREATE FUNCTION "%(schema)s".read_modify_write(read_user_id text[], update_user_id text[])
RETURNS void AS
$paper_ycsb$
DECLARE
    sql_read CONSTANT text := 'SELECT * FROM %(relation)s WHERE ycsb_key = $1';
    sql_update CONSTANT text := 'UPDATE %(relation)s SET %(assignments)s WHERE ycsb_key = $11';
    user_id text;
    affected_rows bigint;
BEGIN
    IF read_user_id IS NULL OR update_user_id IS NULL
       OR coalesce(array_length(read_user_id, 1), 0)
          + coalesce(array_length(update_user_id, 1), 0) <> 10
       OR array_position(read_user_id, NULL) IS NOT NULL
       OR array_position(update_user_id, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'paper YCSB requires exactly ten non-null operation keys';
    END IF;
    -- Author's per-transaction seed makes SQL-generated updates repeatable on replicas.
    IF coalesce(array_length(read_user_id, 1), 0) > 0 THEN
        PERFORM setseed(substring(read_user_id[1], 5, 10)::float / 100000);
    ELSE
        PERFORM setseed(0);
    END IF;
    FOREACH user_id IN ARRAY read_user_id LOOP
        EXECUTE sql_read USING user_id;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN
            RAISE EXCEPTION 'paper YCSB read must find one row for key %%', user_id;
        END IF;
    END LOOP;
    FOREACH user_id IN ARRAY update_user_id LOOP
        EXECUTE sql_update USING %(random_values)s, user_id;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN
            RAISE EXCEPTION 'paper YCSB update must affect one row for key %%', user_id;
        END IF;
    END LOOP;
    RETURN;
END;
$paper_ycsb$ LANGUAGE plpgsql;

\ir data.sql
COMMIT;
ANALYZE %(relation)s;
''' % dict(schema=schema, relation=relation, fields=fields,
           assignments=assignments, random_values=random_values)


def write_data(path, schema, seed):
    rng = random.Random(seed + 2)
    columns = ",".join(["ycsb_key"] + ["field%d" % i for i in range(FIELDS)])
    with path.open("x") as stream:
        stream.write('COPY "%s".usertable (%s) FROM stdin;\n' % (schema, columns))
        for key in range(KEYS):
            values = ["user%d" % key]
            values.extend("".join(rng.choice(ALPHABET) for _ in range(FIELD_LENGTH))
                          for _ in range(FIELDS))
            stream.write("\t".join(values) + "\n")
        stream.write("\\.\n")


def package_readme(schema, count, seed):
    return '''# Paper YCSB input package

Paper: https://doi.org/10.1145/3588952 (Section 5, Figures 8, 10 and 12).
Author source: https://github.com/zllai/AriaBC/tree/%s/src/benchmark/ycsb

- 10,000 keys, user0 through user9999; ten SQL operations per transaction.
- Each operation independently chooses read/update with probability 0.5.
- Ten text fields of ten characters; updates replace all ten fields.
- Zipf rank weights are 1 / rank^skew. Reads precede updates.
- Keys are unique within each read list and each update list; overlap is allowed.
- Schema: %s. Transactions per file: %d. Generator seed: %d.

Load setup.sql with psql -X -v ON_ERROR_STOP=1 -f setup.sql on EACH replica,
in an isolated test database. The schema must not already exist. Keep data.sql
beside setup.sql. Both database operations and checks belong on the lab host.

Use a ycsb_paper_skew_*.txt file as the gateway's --queryFrom argument.
Each non-comment line is ONE complete transaction. Do not split its internal
operations across gateway requests. The procedure has no explicit COMMIT;
PostgreSQL or the deterministic executor owns the transaction boundary.

All replicas must load the same data/procedure and use the same PostgreSQL
binary. SQL values use the author's setseed/random_string method, including
the all-update transaction's seed of zero. Missing rows fail the transaction.
The procedure returns void, as in the author source. Successful result hashes
alone do not establish equal table contents; compare full state on replicas.

The file contains %d transactions and %d internal read/update operations.
Gateway total_queries and TPS count transactions/procedure calls. The internal
operation count is ten times the transaction count. Exclude setup from timing.

The original experimental seed, transaction count, warm-up and run duration
are not published. Our selected count/seed are recorded in manifest.json.
Python's seeded random stream is reproducible but is not the author's NumPy
trace. Ten-byte fields and SQL update generation come from the public source,
not an explicit field-width specification in the paper. Shape and one-row
guards are added to fail on invalid inputs; this is not a bit-for-bit artifact.

Do not use the legacy A-F sweep's default restore/verification: it restores
public.usertable_small and checks that relation rather than this package's
isolated %s.usertable. Load and verify the paper relation explicitly.
Merkle indexing and executor concurrency are separate codebase settings.
The paper tunes concurrency by block size (HarmonyBC 25, AriaBC 50, RBC 10
for the reported YCSB optima); this package does not change those settings.
''' % (AUTHOR_COMMIT, schema, count, seed, count, count * OPERATIONS, schema)


def generate_package(output, skews, count=20000, seed=42, schema="paper_ycsb"):
    if count < 1 or seed < 0:
        raise ValueError("transactions must be positive and seed nonnegative")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
        raise ValueError("schema must be a lowercase SQL identifier of at most 48 characters")
    skews = tuple(float(skew) for skew in skews)
    if not skews or any(skew not in PAPER_SKEWS for skew in skews):
        raise ValueError("use the paper's skews: 0, 0.2, 0.4, 0.6, 0.8, 1.0")
    if len(set(skews)) != len(skews):
        raise ValueError("skews must not repeat")
    output = Path(output)
    # Never replace an earlier trace, package, or benchmark artifact.
    output.mkdir(parents=True, exist_ok=False)
    with (output / "setup.sql").open("x") as stream:
        stream.write(setup_sql(schema))
    write_data(output / "data.sql", schema, seed)
    workloads = []
    for skew in skews:
        name = "ycsb_paper_skew_" + ("%.2f" % skew).replace(".", "_") + ".txt"
        histogram = collections.Counter()
        reads_total, hot_accesses = 0, 0
        with (output / name).open("x") as stream:
            stream.write("-- DOI %s: %d keys, 10 operations/transaction, 50/50 random mix\n"
                         % (DOI, KEYS))
            stream.write("-- skew=%s; transactions=%d; seed=%d; schema=%s\n"
                         % (skew, count, seed, schema))
            for reads, updates in transactions(skew, count, seed):
                stream.write(transaction_sql(schema, reads, updates) + "\n")
                histogram[len(reads)] += 1
                reads_total += len(reads)
                hot_accesses += sum(key < KEYS // 100 for key in reads + updates)
        workloads.append(dict(file=name, skew=skew, transactions=count,
                              internal_operations=count * OPERATIONS,
                              read_operations=reads_total,
                              update_operations=count * OPERATIONS - reads_total,
                              read_count_histogram={str(i): histogram[i] for i in range(11)},
                              top_one_percent_accesses=hot_accesses))
    with (output / "README.md").open("x") as stream:
        stream.write(package_readme(schema, count, seed))
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(output.iterdir())}
    manifest = dict(format_version=1, paper_doi=DOI,
                    author_repository="https://github.com/zllai/AriaBC",
                    author_commit=AUTHOR_COMMIT,
                    generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    schema=schema, table="usertable", key_type="varchar(255)",
                    key_prefix="user", key_first=0, key_last=KEYS - 1,
                    records=KEYS, fields=FIELDS, field_length=FIELD_LENGTH,
                    operations_per_transaction=OPERATIONS, read_probability=0.5,
                    generator_seed=seed, transactions_per_file=count,
                    paper_default_skew=0.6, workloads=workloads, sha256=hashes,
                    provenance={
                        "paper": ["10000 keys", "10 operations per transaction",
                                  "independent 50/50 SELECT/UPDATE", "default skew 0.6",
                                  "contention sweep 0,0.2,0.4,0.6,0.8,1.0"],
                        "author_source": ["text user keys", "10 fields of 10 characters",
                                          "Zipf CDF", "within-list unique keys",
                                          "reads before updates", "all-field seeded SQL updates"],
                        "chosen": ["transaction count", "generator seed",
                                   "Python random stream", "isolated schema",
                                   "transaction-shape and one-row guards"],
                    })
    with (output / "manifest.json").open("x") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True,
                        help="new output directory; existing directories are rejected")
    parser.add_argument("--transactions", type=int, default=20000,
                        help="transactions per skew (chosen default: 20000; paper unspecified)")
    parser.add_argument("--seed", type=int, default=42,
                        help="reproducible trace seed (paper seed unspecified)")
    parser.add_argument("--schema", default="paper_ycsb")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--skews", type=float, nargs="+", default=None,
                       help="paper skew values; default: 0.6")
    group.add_argument("--all-skews", action="store_true",
                       help="generate all six contention-sweep skews")
    args = parser.parse_args(argv)
    skews = PAPER_SKEWS if args.all_skews else args.skews or (0.6,)
    try:
        manifest = generate_package(args.out, skews, args.transactions, args.seed, args.schema)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print("Created %s: %d workload(s), %d transactions/file, %d operations/file"
          % (args.out, len(manifest["workloads"]), args.transactions,
             args.transactions * OPERATIONS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
