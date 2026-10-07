# Transactional YCSB from the HarmonyBC paper

`generate_paper_ycsb.py` creates workload inputs for **When Private Blockchain
Meets Deterministic Database**, DOI [10.1145/3588952](https://doi.org/10.1145/3588952),
Section 5, Figures 8, 10 and 12. It keeps ten operations inside one SQL transaction,
unlike the existing A-F generator's separate operation requests.

## Workload contract

| Setting | Value | Evidence |
|---|---|---|
| Keys | 10,000 | Paper |
| Operations per transaction | 10 | Paper |
| Mix | Independent 50% SELECT / 50% UPDATE per operation | Paper |
| Default skew | 0.6 | Paper |
| Skew sweep | 0, 0.2, 0.4, 0.6, 0.8, 1.0 | Paper |
| Key selection | Zipf weights `1 / rank^skew` | Author source |
| Keys and fields | `user0` through `user9999`; `field0` through `field9`, ten characters each | Author source |
| Update width | All ten fields | Author source |
| SQL order | All reads, then all updates | Author source |
| Duplicate handling | Unique within the read list and within the update list; overlap allowed | Author source |
| Transaction count / generator seed | 20,000 / 42 by default; configurable | Chosen; paper unspecified |

Author code: [YCSB generator and SQL](https://github.com/zllai/AriaBC/tree/f5e16ccb6898a572b768d9bfb5de4fb384dab714/src/benchmark/ycsb).
We retain its per-transaction `setseed` and SQL-generated update strings, rather
than replacing that database work with precomputed update literals. Initial data
and request traces use Python's seeded random generator, so they are reproducible
but are not bit-for-bit copies of an unpublished experimental trace. Additional
shape and one-row guards reject invalid inputs. The separate hotspot variant in
Figure 14 is not part of this default/contention workload.

## Generate on the lab machine

Sync the generator to a separate checkout/directory on `neel@10.129.148.247`, then
run these commands there. Use a new output directory for every generated package.

```bash
python3 scripts/distributed/generate_paper_ycsb.py \
  --out .bench_tmp/paper_ycsb_default \
  --transactions 20000 --seed 42

python3 scripts/distributed/generate_paper_ycsb.py \
  --out .bench_tmp/paper_ycsb_contention \
  --all-skews --transactions 20000 --seed 42
```

The default package has `ycsb_paper_skew_0_60.txt`; the contention package has all
six skews. Each also contains `setup.sql`, `data.sql`, `README.md`, and a manifest
with actual operation counts, parameter provenance, generator hash and file hashes.
There are no dependencies beyond Python's standard library. Existing output
directories are rejected instead of overwritten.

## Load and submit

Use an isolated PostgreSQL instance/database on the lab machine and load the same
package on **every replica**. Never use the canonical
`~/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata` for these checks.

For **DET and DET+Merkle**, explicitly configure these settings in the isolated
instance's `postgresql.conf` before starting it:

```conf
bcdb_dt_conflict_tracking = on
bcdb_dt_completion_only_skip_reads = off
```

Conflict tracking is a postmaster setting: restart PostgreSQL after changing it.
A fresh initdb instance inherits `bcdb_dt_conflict_tracking=off` from the core
GUC default. That is unsafe for parallel conflicting transactions: optimistic
execution needs write-set publication, conflict checks and whole-transaction
retries. The standard distributed cluster launcher explicitly enables tracking.
Do not assume a manually created instance inherits the launcher's configuration.

Run `paper_ycsb_det_preflight.sql` on each replica after the restart and before
submitting a DET workload. It rejects disabled tracking or skipping SELECT bodies.
The procedure itself is a SELECT that contains updates, so skipping its body
would skip the workload. PG-mode runs do not need this DET preflight.

```bash
/path/to/install/bin/psql -X -v ON_ERROR_STOP=1 \
  -h /path/to/isolated/socket -p 55483 -U neel -d paper_ycsb_test \
  -f scripts/distributed/paper_ycsb_det_preflight.sql
```

```bash
/path/to/install/bin/psql -X -v ON_ERROR_STOP=1 \
  -h /path/to/isolated/socket -p 55483 -U neel -d paper_ycsb_test \
  -f .bench_tmp/paper_ycsb_default/setup.sql
```

The setup creates the separate `paper_ycsb` schema and fails if it already exists.
`--schema` selects another lowercase schema name. Keep `data.sql` beside the setup
file: psql's `\ir` resolves it relative to `setup.sql`.

Pass `ycsb_paper_skew_0_60.txt` as the existing gateway's **`--queryFrom`** input.
One non-comment line calls `paper_ycsb.read_modify_write(...)` and performs exactly
ten point operations atomically. The executor owns COMMIT/rollback. Load schema
and data before starting the timed workload, and keep the same PostgreSQL binaries
on replicas because the procedure uses the author's seeded `random()` method.

Gateway `total_queries` and TPS count **transactions**: 20,000 input calls contain
200,000 internal operations. Each read must find one row and each update must
affect one row; a missing key raises an error and rolls back the entire call.
The function returns `void`, matching the author source. Gateway success/result
hashes therefore need a separate comparison of actual table contents on replicas.

Executor workers, blocks and Merkle indexes are independent execution settings.
The paper tunes block size and reports YCSB optima of 25 for HarmonyBC, 50 for
AriaBC, and 10 for RBC. Do not interpret those as our gateway's client-thread count.
For Merkle runs, create and verify an index on **`paper_ycsb.usertable`** explicitly.

The existing `run_all_modes_gateway_sweep.py` A-F default restore/verification is
specific to `public.usertable_small`. It cannot establish this package's population
or Merkle correctness; use a harness that loads and verifies the paper relation.

## Checks

Run on the lab machine after syncing the source:

```bash
python3 -m unittest discover -s scripts/distributed/tests -p test_paper_ycsb.py -v
```

Database/gateway checks must additionally establish that the generated calls work
with the selected PG/deterministic mode. Replica validation must inspect full
table state or the paper table's Merkle index. The paper does not publish the
original run duration, warm-up, seed or precise peak-throughput aggregation rule,
so this input package does not claim to reproduce its published TPS.

## Ranking validation, 2026-10-03

At the user's request, checks ran on `protectdr@10.129.7.57` while the lab host
was unreachable. The existing ranking PostgreSQL/server binaries were used;
the gateway was built separately in the isolated validation directory. This
does not validate an installed build of the developer workstation's current tree.

- Five generator tests passed, including reproducibility, Zipf selection, record
  population/width, operation mix, and refusing to overwrite an existing package.
- All six skews executed with 100 transactions each. PG, deterministic, and
  deterministic plus Merkle each completed 600 gateway requests with one executor
  worker, zero reported divergence/permanent failures, and a successful post-marker.
  Ordered full-table dumps matched the serial PG oracle; Merkle verification passed.
- Rare all-read/all-update calls and rollback after an earlier update followed by
  a missing key passed.
- A four-worker deterministic run completed all 600 requests, but three final rows
  differed from the serial oracle. Investigation established that this initial
  fresh instance had inherited `bcdb_dt_conflict_tracking=off`. Three repeat runs
  under that configuration differed in 5, 5 and 4 rows, with three different
  full-table hashes. This was actual nondeterminism in that configuration, hidden
  by the procedure's void result and successful completion counters.
- Changing only conflict tracking to `on` made two four-worker repeat runs match
  the serial PG oracle exactly. A separate corrected four-worker DET and
  DET+Merkle smoke also completed 600 requests per mode with matching full tables,
  successful post-markers and Merkle verification. The preflight rejects the
  original unsafe configuration. These checks used the same ranking binaries.
- A minimal two-transaction test also found an integer-only moved-row fallback
  in `bcdb_lookup_current_tid_from_slot()`. With full predecessor commit gating
  but conflict tracking off, an optimistic update to a moved `varchar(255)` key
  was silently dropped; an integer-key control succeeded. Enabling tracking
  made the varchar test succeed by retrying the stale optimistic transaction.
  This fallback still needs general equality-operator support for text keys;
  the preflight prevents running this workload with conflict tracking disabled.

Generated 20,000-transaction files and both attempts' logs, full-state dumps and
binary hashes are under `.bench_tmp/paper_ycsb_20261003_ranking_KOt21S/`. Load
`inputs/setup.sql` and submit `inputs/ycsb_paper_skew_0_60.txt` for the default skew.
The other five files provide the paper's contention sweep. The validation runs
used isolated port 55483 and stopped their test processes afterward.

The follow-up diagnosis, setting comparisons, repeat-state dumps and corrected
four-worker checks are preserved in
`.bench_tmp/paper_ycsb_determinism_20261003_6eXkna/`. These are single-node
reproducibility checks, not a certification of every workload or a multi-node run.

## Gateway and cluster campaign

`run_paper_ycsb.py` runs on the `.111` gateway and manages separate instances on
`.247`, `.246`, and `.248`. Upload it and `benchmark_validation.py`,
`benchmark_cache.py`, and `paper_ycsb_det_preflight.sql` into a fresh staging
directory with the same path on all four hosts. Generate the input package on
`.247` and copy it to the gateway. Existing output/campaign directories are
rejected. The runner uses the installed executables only after checking their
SHA-256 build manifests and live source fingerprints on every host.

Example, executed on `neel@10.129.27.111`:

```bash
python3 -u /tmp/ariabc_paper_stage_NEW/run_paper_ycsb.py \
  --inputs /tmp/ariabc_paper_stage_NEW/inputs \
  --staging /tmp/ariabc_paper_stage_NEW \
  --out /home/neel/ARIABC/AriaBC/.bench_tmp/paper_ycsb_cluster_NEW \
  --run-id NEW --workers 1,4,8,16 \
  --modes cluster,pg,bcdb_det,bcdb_merkle --trials 1 --keep-going
```

The default campaign first runs a six-skew, 600-transaction cluster smoke, then
96 measured cases: six paper skews times four executor counts times four modes.
Each case restores the same 10,000 rows into a fresh isolated PostgreSQL instance.
The controller constructs actual serial PG reference states on `.247` for the
input traces. DET modes must match their serial reference; cluster replicas must
match each other. Cluster equality with the input-order oracle is also recorded,
because the saved leader-assigned Raft policy establishes order at admission.
Parallel PG may commit in a different valid serial order.

The execution settings mirror the saved `Final_Results/YCSB` commands and attempt
logs: 32MB buffers, SERIALIZABLE, synchronous commit/fsync/full-page writes on,
96 gateway clients and DET client workers, inflight 16, window 65536, batch 256,
pipeline depth 1024, connection fanout 1, and executor/pool/block-init counts
1/4/8/16. Cold preparation uses the existing stopped-PG fadvise/mincore helper.
Merkle modes use the saved YCSB geometry: 200 partitions, fanout 4, split 32,
merge 8, with synchronous maintenance and the auxiliary lookup index. The
fanout-32 settings in the OOM/TPC-C experiments are separate configurations.

Cluster mode uses durable Raft, leader-assigned order, asynchronous durable
flush, and colocated Kafka 3.7.0 brokers with the launcher's 1GB initial/maximum
heap. Its topic has three partitions with
one assigned to each broker, matching the saved `--replica-assignment 1,2,3`
command. Client completion waits for a majority; acceptance additionally requires
all three result records for every transaction, zero audit failures and zero
outstanding requests. Transaction BLAKE3 authentication stays enabled. As in the
saved launcher, compact T1 result receipts use
`ARIABC_TRUSTED_RESULT_SIG_FASTPATH=1`: this is the trusted-lab result-signature
shortcut, not cryptographic verification of each replica's receipt.

A separate marker table preserves the paper's 10,000-row population and fields.
The marker is submitted through the gateway after workload completion, read back
on every replica, and followed by ordered full-table SHA-256 comparisons and
Merkle verification. Gateway success for the void procedure alone is insufficient.
Each attempt keeps gateway commands/logs, final server profiles, effective settings,
cold-cache evidence, initial/final table dumps, and Merkle results. TPS counts
transactions; multiply by ten for the rate of internal operations.

Ports are isolated: PostgreSQL 55493, server 18693, Raft 19693, Kafka 19092,
controller 19093. Owned data remains under `/home/neel/ariabc_data/paper_cluster_RUN_ID`.
Only PIDs recorded by this campaign are stopped. Canonical database instances,
Kafka topics, installed source, and binaries are retained.

`--skews 0.6` selects the paper's default skew, and `--modes cluster --workers 16`
selects a single cluster point. `--oracle-from PREVIOUS_OUTPUT` reuses completed
serial reference artifacts only after checking the installed source identity,
trace SHA-256 and actual reference dump SHA-256. `--no-smoke` skips the introductory
smoke when that same build/input contract has already been validated.

`--resume-from PREVIOUS_OUTPUT` copies accepted cases into a fresh output only
after checking workload/build identity, gateway counters, actual table dump
hashes, and final server profiles. Failed attempts remain in their original
directories. The result-wait limit is ten minutes, the maximum supported by the
installed server; the saved A-F runs used three minutes. This guard gives the
ten-operation transactions time to finish under PG contention and does not
change execution, isolation, or durability. The outer case timeout is one hour.
`--keep-going` preserves a rejected case and continues the other measured points
after successful cleanup. Rejections are recorded separately from accepted TPS
rows, and the overall campaign still exits nonzero. Cleanup failures and
interruptions always stop the campaign.
When resuming, `--resume-rejected-pg` can additionally retain a PG point that
actually reached the supported ten-minute result timeout. It requires
`--keep-going`, verifies the build/trace and final gateway/server evidence,
and keeps the point rejected. Setup and invalid-protocol errors are rerun.

For an additional signed-receipt check, use `--result-receipts signed`. This
selects binary Kafka receipts and disables the trusted-result shortcut. It is
a separate validation setting, and its TPS must be labeled separately from
the saved Final_Results text-receipt configuration.

### Completed cluster validation, 2026-10-03

The fresh 96-point campaign attempted every six-skew, four-worker-count,
four-mode combination. All 24 cluster, 24 DET, and 24 DET+Merkle points passed;
PG passed 20 points and reached the ten-minute result timeout at skew 1.0 with
4/8/16 workers and skew 0.8 with 16 workers. Those four points remain rejected,
with no accepted TPS. The overall campaign exited 1. The six-skew cluster smoke
and a separate 20,000-transaction signed binary receipt check passed.

Independent artifact validation checked all 96 outcomes, 140 accepted-case
server profiles, actual full-table dumps, post-markers, effective settings and
Merkle evidence. All cluster replicas matched each other and their serial PG
reference in all 24 cases; standalone DET modes matched their references too.
Conflict tracking was ON throughout. All owned test processes stopped and all
isolated ports were closed afterward.

These are one-trial results from the verified installed September 29 build,
not a build of the dirty local C/C++ tree. The primary campaign used the saved
trusted text receipt setting and 1GB Kafka heap on every broker. Earlier attempts
are preserved separately and excluded from primary throughput results.

The report, all 96 outcomes, reusable input archive, and complete downloaded
evidence are under
`.bench_tmp/paper_ycsb_cluster_20261003_dFaUWmHp/`; see `RESULTS.md` and
`ALL_CASES.csv`. The gateway retains the primary campaign in
`/home/neel/ARIABC/AriaBC/.bench_tmp/paper_ycsb_cluster_20261003_dFaUWmHp_v8`.

### Result graphs

The results folder's `plots/index.html` provides six figures: default-skew
worker throughput, skew sensitivity, worker scaling at every skew, the full
96-point heatmap, PG timeout diagnostics, and accepted/rejected counts. PNG
and SVG exports are available alongside a six-page `paper_ycsb_figures.pdf`.
`RESULTS.md` embeds the main charts. Rejected cases retain blank TPS, appear
as gaps or gray TIMEOUT cells, and are never plotted as measured zero TPS.

`plot_paper_ycsb.py` regenerates the figures from `ALL_CASES.csv`, using
Matplotlib and NumPy. It rejects incomplete or duplicate matrix points,
mixed source identities and overwriting an existing output directory. This
is artifact generation from saved results; it does not run a workload.

```bash
python3 scripts/distributed/plot_paper_ycsb.py \
  --csv .bench_tmp/paper_ycsb_cluster_20261003_dFaUWmHp/ALL_CASES.csv \
  --out .bench_tmp/paper_ycsb_cluster_20261003_dFaUWmHp/plots_NEW
```
