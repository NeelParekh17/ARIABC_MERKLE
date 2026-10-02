# Ranking TPC-C v2 campaign

Run only as protectdr on ranking (10.129.7.57). The detached master builds
`~/claude_checks/src_v2` into `~/claude_checks/install_v2`, using the reference
configure flags, then runs a W=100 / 32-worker / 2,000-transaction C5 smoke test.
It stops on any build, run or evidence failure.

The headline matrix uses W=100, 32 workers, 20,000 transactions, seed 42,
three trials in trial-outer C1..C5 order:

| Config | Execution | Split / merge | Table fillfactor |
|---|---|---|---|
| C1 | det | unused | reference default |
| C2 | synchronous Merkle | 32 / 8 | reference default |
| C3 | synchronous Merkle | 1024 / 256 | reference default |
| C4 | det | unused | 90 |
| C5 | synchronous Merkle | 1024 / 256 | 90 |

The same job appends two trials of C1/C3/C5 at 16 and 64 workers. All measurements
use SERIALIZABLE, synchronous commit, fsync and full-page writes. Server/gateway
flags, restore, checkpoint and prewarm follow `merkle_probe.sh`. Workloads are
copied/generated inside the new output root; published files are read-only inputs.

Fillfactor is set on warehouse, district, customer, stock, oorder, order_line,
new_order and history before SET LOGGED. District is already LOGGED in the
reference restore, so only fillfactor runs first make it UNLOGGED to force its
rewrite. Item keeps its reference storage. Each run records restored sizes,
reloptions, Merkle geometry, settings, uptime/top CPU consumers, phase traces,
WAL bytes, pg_waldump stats and final per-table HOT counters. Workers stop and
PostgreSQL restarts after measurement to expose final PG13 cumulative stats.

Acceptance requires every expected transaction's verified terminal result,
no outstanding requests, divergence or failures, 9:true Merkle verification,
correct settings/geometry/storage, complete traces and WAL evidence. Every full
run (including different fillfactors/workers) must match the first C1 state.hash.
The published hash projects eight logical tables and excludes wall-clock
new-order/payment timestamps; it is distinct from Merkle's canonical row hash.
Smoke uses its own 2,000-transaction workload and cannot match the 20,000 run.

The harness only kills its recorded server PID and stops its own PGDATA; occupied
ports are rejected. Accepted, stopped PGDATA is removed to cap disk use (about
25–35 GB live per run). Failed PGDATA is preserved. Logs, hash, settings, traces,
WAL statistics and acceptance JSON remain. Master commands are traced in remote
commands.log; local SSH/rsync invocations are recorded under
`.bench_tmp/tpcc_v2_launch/commands.log`.

Launch a fresh root populated with `provenance/local_source.txt`, the git diff,
HEAD, status and `ariabc_pg.diff`:

```bash
# On ranking; launch root must be fresh, and published ports must be unused.
tmux new-session -d -s tpccv2 \
  'nohup bash ~/claude_checks/src_v2/scripts/distributed/tpcc_v2/master.sh ~/claude_checks/tpcc_v2_DATE </dev/null > ~/claude_checks/tpcc_v2_DATE/master.log 2>&1 & wait "$!"'
cat ~/claude_checks/tpcc_v2_DATE/status.txt
tail -f ~/claude_checks/tpcc_v2_DATE/master.log
```

The master writes summary.md and all_runs.csv at completion. A stopped/failed
campaign can be summarized from accepted artifacts without restarting anything:

```bash
python3 ~/claude_checks/src_v2/scripts/distributed/tpcc_v2/summary.py \
  summarize ~/claude_checks/tpcc_v2_DATE
```

Summary reports best/median/min TPS, paired Merkle/det ratios at matching
fillfactor, WAL bytes/tx, per-table non-HOT fractions and phase residuals. Paired
C5/C1 also quantifies combined changes but uses different fillfactors. The appended
sweep has no C4 control, so C5/C4 is available only at the headline point.

Trace tx_id labels can be reused: worker.c emits tx->tx_id after delete_tx(tx).
The summarizer retains all timing rows, reports reused-label counts, requires
the expected row count and uses gateway protocol v2 for verified completion.
`RESUME_AFTER_SMOKE=1` can continue only a stopped smoke-acceptance stage with no
headline run started; it checks source/binary identity and revalidates preserved
smoke evidence before starting C1, retaining the original failed acceptance log.

Existing `bcdb_ptrace_delta_us` casts a negative nanosecond delta to uint64
before division by 1000. The summary corrects top-level phase values per row
modulo floor(2^64/1000), retaining raw totals/traces and correction counts. The
reported phase residual is estimated, with at most 1 us uncertainty per corrected
interval; gateway TPS is unaffected. No core tracer code is edited by this task.
