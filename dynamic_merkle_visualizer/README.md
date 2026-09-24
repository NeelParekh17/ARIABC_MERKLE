# Live PostgreSQL Merkle inspector

This application reads the **current AriaBC PostgreSQL Merkle index**. It does
not calculate substitute node hashes or keep a second tree model. Node IDs,
partition roots, global roots, row hashes, verification results, row routing,
and row writes come from the running database and its native Merkle functions.
The diagram connects stored prefixes that exist in the database; it does not
invent placeholder nodes.

## Start a safe working demo

From the repository root:

```bash
./dynamic_merkle_visualizer/start_postgres.sh
```

The launcher uses `/work/ARIABC/install/bin` by default (or set
`MERKLE_VIZ_PG_BIN` to another directory containing `initdb`, `pg_ctl`, and
`psql`). It creates a **new, unique disposable PostgreSQL cluster** under
`.bench_tmp/merkle-viz-*`, bootstraps the current Merkle functions, builds a
real format-10 Merkle index using this custom server, checks its native root,
and opens the UI at `http://127.0.0.1:8787`. The database listens on its private
Unix socket; it does not open a TCP port. Its data and logs remain in the
printed directory after Ctrl+C so the run can be inspected.

Writes in the demo go through ordinary PostgreSQL `INSERT`, `UPDATE`, and
`DELETE`. The native hooks update the heap and Merkle node relation inside the
same transaction. The UI also offers a rollback switch to demonstrate that
aborted writes leave the committed root unchanged. Stop the app with Ctrl+C;
the launcher stops only the cluster it created.

Options include `--port 8790`, `--rows 1000` (1 to 100,000), and
`--pg-bin /path/to/custom/bin`. `--db-only` initializes the isolated database
and keeps it running for a separately launched inspector. Running a second demo
uses another fresh directory and socket.

## Connect the UI to an existing database

The application binds only to loopback. Start a running AriaBC PostgreSQL
server built from this repository, then provide a libpq connection string:

```bash
MERKLE_VIZ_CONNINFO='host=/path/to/socket port=5432 dbname=mydb user=postgres' \
  ./.venv/bin/python3 dynamic_merkle_visualizer/app.py
```

Set `MERKLE_VIZ_TABLE` and `MERKLE_VIZ_INDEX` to schema-qualified relation
names to narrow discovery, for example `public.usertable` and
`public.usertable_merkle_idx`. The inspector verifies that the selected index
is valid and ready, its table is permanent and logged, and its native metadata
uses index format 10, routing format 4, and row-hash format 1. It exits with a
clear error for unsupported formats or missing node storage. Run the current
`raft_apply_ledger_schema.sql` bootstrap when the database needs compatibility
SQL wrappers, using a database administrator and the intended database.

The inspector never runs bootstrap or changes a database's schema by itself.
Reads briefly take a PostgreSQL `SHARE` lock on the inspected table before
reading roots, metadata, nodes, or rows. This gives a stable view against normal
DML while the request runs. PostgreSQL's `lock_timeout` and `statement_timeout`
bound waits and scans; increase them in `db.py` only if your table size requires
it. Each request opens and closes its own database connection.

## Browsing and writes

- Select a discovered Merkle index and partition. Nodes show the stored route
  prefix, row count, type, and hash returned by that index's dedicated
  `ariabc_internal.merkle_node_<index_oid>` relation.
- Select a node to browse its heap rows when the index uses a single plain key
  with typmod `-1`. Routing, partition selection, and prefix bounds then use
  `merkle_key_hash`, `merkle_partition_for_hash`, and
  `merkle_node_upper_bound` in PostgreSQL. For multicolumn, expression,
  partial, or typmod-specific keys, all native nodes remain visible and rows
  can still be browsed by table page; the UI does not pretend the single-key
  helper is an equivalent route implementation.
- “Verify heap against roots” calls `merkle_verify_index()` on PostgreSQL. It
  scans the visible heap and compares its hash aggregate with stored partition
  roots. It does not verify every internal node or provide an inclusion proof.
  XOR roots are aggregate comparisons, not cryptographic membership proofs.
- Row values display as text so `bigint`, `numeric`, timestamps, and bytea are
  not rounded through JavaScript numbers. Edits accept JSON string values (or
  `null`); PostgreSQL converts them to the column's real type.
- Edits require an explicit opt-in and a superuser connection. On a trusted
  local database, set `MERKLE_VIZ_ALLOW_WRITES=1`. This is powerful: any write
  changes that database's real data. It is disabled by default. The API has no
  arbitrary SQL endpoint. UPDATE and DELETE use the selected row's `ctid`,
  `xmin`, and native full-row hash; if that row changed since it was displayed,
  the write fails and asks you to refresh.
- Committed writes set Merkle maintenance and synchronous commit on for that
  transaction. The UI then reads a new committed snapshot. Other database
  writers can still commit between those two requests.

This browser is an operational inspector for PostgreSQL's current on-disk
logical tree, not a simulation of historic copy-on-write pages, `pageinspect`
buffers, transaction internals, or a global lock on all database sessions.

## Checks

The live integration suite launches an isolated cluster from the custom AriaBC
binaries. It checks exact equality between every displayed native node and its
stored SQL row, partition roots, native verification, committed DML, rollback,
and stale-edit rejection:

```bash
MERKLE_VIZ_TEST_LIVE=1 ./.venv/bin/python3 -m unittest \
  dynamic_merkle_visualizer.tests.test_live -v
node --check dynamic_merkle_visualizer/static/app.js
```

Without `MERKLE_VIZ_TEST_LIVE=1`, the test module runs only local input-validation
checks. Dependencies are in `requirements.txt`; psycopg 3 is also provided by
the repository virtual environment used for the recovery tools.
