# All Machines Detailed Benchmark Report

Generated on: 2026-04-30  
Workspace: `/work/ARIABC/AriaBC`

This report is the operational guide for the current lab benchmark setup.  It
documents the active machines, the two main distributed benchmark scripts, the
exact trust criteria for throughput numbers, and the latest verified result
family.  Older 8192-slot and multi-lane experiments were useful debugging
history, but they are **not** the current benchmark procedure in this checkout.

## Active Benchmark Nodes

| Node | IP | OS | Role |
|---|---|---|---|
| ASUS Laptop | local | Ubuntu 24.04.4 LTS | Controller, local build source, optional single-node participant |
| neel-MS-7C96 | 10.129.148.236 | Ubuntu 24.04.3 LTS | Raft node 1, Kafka host |
| kartik-MS-7C96 | 10.129.27.54 | Ubuntu 22.04.2 LTS | Raft node 2, on-host rebuild |
| anant-side | 10.129.148.179 | Ubuntu 22.04.5 LTS | Raft node 3, on-host rebuild |
| utkarsh-MS-7C96 | 10.129.148.248 | Ubuntu 24.04.3 LTS | Raft node 4 |

Desktop-based remote paths are the active defaults:

| Purpose | Path |
|---|---|
| Synced source | `~/Desktop/ariabc_cluster` |
| Synced PostgreSQL install | `~/Desktop/ariabc_install` |
| Ubuntu 22.04 `ariabc_pg` build output | `~/Desktop/ariabc_pg_build_u22/bin` |
| Local controller install | `/work/ARIABC/install` |
| Local controller source | `/work/ARIABC/AriaBC` |

## Current Headline Result

The current trusted 4-node full-system path is:

- `scripts/distributed/run_4node_raft_cluster.sh`
- `completionPath=kafka_majority`
- `waitMajority=1`
- 4 Raft nodes
- 4 PostgreSQL/BCDB replicas
- Kafka result topic on `10.129.148.236:9092`
- deterministic YCSB workload:
  `scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt`
- post-run row-count and Merkle-root verification on all four replicas

Latest recovered run family:

| Run | Command shape | Gateway time | TPS | Correctness |
|---|---|---:|---:|---|
| `cluster4_20260430_004257` | full sync/build, no block profile | 3.788 s | 5,415 | PASS |
| `cluster4_20260430_004708` | reused fresh artifacts, `--bcdb-block-profile 1` | 3.801 s | 5,396 | PASS |
| `cluster4_20260430_015443` | full self-healing sync/build, `--bcdb-block-profile 1` | 3.810 s | 5,383 | PASS |
| `cluster4_20260430_051459` | atomic Raft-batch enqueue, pool/batch 256 | 3.771 s | 5,439 | PASS |
| `cluster4_20260430_052145` | block-level result wait, pool/batch 256 | 3.832 s | 5,353 | PASS |
| `cluster4_20260430_052833` | block-level result wait, pool/batch 512 | 6.155 s | 3,332 | PASS |
| `cluster4_20260430_053151` | block-level result wait, pool/batch 128 | 4.353 s | 4,712 | PASS |

The run is trusted only when all of these are true:

- gateway reports `completion_path=kafka_majority` and `waitMajority=1`
- `divergence_count=0`
- `permanent_failures=0`
- all four replicas agree on pre-marker row count and Merkle root
- the marker transaction becomes visible on all four replicas
- all four replicas agree on post-marker row count and Merkle root
- `merkle_verify('usertable_small')` returns `t` on every replica
- the standalone Merkle check passes when enabled

The current recovered profile intentionally uses `RESULT_RING_CAPACITY=1024`
inside the distributed runner.  The checked-in header may show a different
compile-time default such as `2048`, but `run_4node_raft_cluster.sh` rewrites
`BCDB_RESULT_RING_CAPACITY` before rebuilding the local/U24-consumed install and
the U22 on-host installs.  The script then requires PostgreSQL to report
`bcdb_result_ring_slots=1024` before it will trust the run.

## Why The Recovery Was Needed

The bad low-TPS reruns were not a clean code bottleneck measurement.  The
cluster had mixed build state:

- Ubuntu 24.04 nodes were consuming stale local PostgreSQL / `ariabc_pg` builds.
- Ubuntu 22.04 nodes had fresher on-host rebuilds.
- Different result-ring capacities and binaries made the cluster look like it
  had a deterministic-path regression.

The runner was updated to fail closed:

- rebuild the local canonical PostgreSQL install before sync
- rebuild local `ariabc_pg_gateway` and `ariabc_pg_server`
- sync source and install to every node
- rebuild Ubuntu 22.04 nodes on host
- refuse Kafka-majority runs if `rdkafka_local` is missing
- verify PostgreSQL GUC readbacks before measuring

## Script 1: `run_4node_raft_cluster.sh`

Purpose: run one correctness-checked 4-node Raft + BCDB + Kafka-majority
benchmark from the controller.

Typical trusted command:

```bash
env POLL_COUNT=120000 ./scripts/distributed/run_4node_raft_cluster.sh \
  --skip-rdkafka-setup \
  --test-queries 20512 \
  --pool-size 256 \
  --det-batch-size 256 \
  --det-window 4096 \
  --num-terminals 1 \
  --bcdb-block-profile 1
```

### Default Topology

| Raft ID | Host | Client Port | Raft Port | Build source |
|---:|---|---:|---:|---|
| 1 | `10.129.148.236` | 8000 | 9000 | synced local U24 build |
| 2 | `10.129.27.54` | 8000 | 9000 | U22 on-host build |
| 3 | `10.129.148.179` | 8000 | 9000 | U22 on-host build |
| 4 | `10.129.148.248` | 8001 | 9000 | synced local U24 build |

Kafka runs on node 1 at `10.129.148.236:9092`, topic `ariabc_results`.

### Main Runtime Knobs

| Option / env | Default | Meaning |
|---|---:|---|
| `--pool-size` / `DB_CONN_POOL_SIZE` | 256 | Gateway `dbConnPoolSize` and PostgreSQL `bcdb_worker_count`; these must match. |
| `--det-window` | 4096 | Gateway deterministic in-flight window. |
| `--det-batch-size` | 256 | Gateway deterministic Raft batch size. |
| `--num-terminals` | 1 | Gateway file-mode terminal count. |
| `--submit-mode` | `event` | Gateway submission mode. |
| `--pg-exec-mode` | `event` | Server-side `pg_executor` mode. |
| `--no-kafka` | off | Switches to direct completion; this is useful for debugging but not the headline path. |
| `--skip-restore` | off | Skips restore; use only when intentionally reusing existing DB state. |
| `--skip-post-verify` | off | Skips marker + Merkle verification; output is not a trusted distributed number. |
| `--bcdb-block-profile` | 0 | Exports `BCDB_BLOCK_PROFILE` for PostgreSQL-side block-submit profiling. |
| `--bcdb-phase-trace` | 0 | Exports `BCDB_PHASE_TRACE` prefix for per-worker CSV phase traces. |
| `--bcdb-poll-max-us` | 8 | Exports `BCDB_POLL_MAX_US` during PostgreSQL verification/start. |
| `RESULT_RING_CAPACITY` | 1024 | Compile-time capacity injected by the runner before rebuild. |

When `--bcdb-block-profile 1` is set, the runner now collects PostgreSQL
`server.log` into `postgres_node*.log`, so `PROFILE_BCDB_BLOCK` lines are
preserved beside the gateway/server artifacts.

The script currently exports `ARIABC_DET_BLOCK_PARALLEL` and
`ARIABC_DET_BLOCK_PIPELINE` to server processes, but the checked-out
`ariabc_pg/src` code does not read those environment variables.  Do not treat
those exports as active multi-lane BCDB execution unless the source is changed
and re-verified.

### Phase-by-Phase Procedure

1. **Pre-cleanup**
   - Kills stale benchmark, PostgreSQL, gateway, server, and related processes
     unless `--skip-cleanup` is used.

2. **librdkafka setup**
   - Runs `ensure_rdkafka.sh` locally and remotely unless
     `--skip-rdkafka-setup` is used.
   - Kafka-majority mode requires real `librdkafka`; stubs are allowed only for
     `--no-kafka`.

3. **Canonical local rebuild**
   - Rewrites `src/include/bcdb/globals.h` to the runner's
     `RESULT_RING_CAPACITY`.
   - Rebuilds and installs PostgreSQL into `/work/ARIABC/install`.
   - Configures and rebuilds `ariabc_pg_gateway` and `ariabc_pg_server` against
     that install and `~/Desktop/rdkafka_local`.

4. **Sync**
   - Rsyncs the full working tree to `~/Desktop/ariabc_cluster`.
   - Rsyncs the local install to Ubuntu 24.04 nodes.
   - Pushes the workload and restore SQL to the remote repo.

5. **Ubuntu 22.04 on-host rebuild**
   - Rewrites the same result-ring capacity on U22 nodes.
   - Rebuilds custom PostgreSQL into `~/Desktop/ariabc_install`.
   - Builds `ariabc_pg` into `~/Desktop/ariabc_pg_build_u22/bin`.

6. **Kafka**
   - Starts or validates KRaft Kafka on node 1.
   - Creates `ariabc_results` with 4 partitions when needed.

7. **PostgreSQL verification**
   - Starts/restarts each PostgreSQL instance on port `5438`.
   - Requires:
     - `bcdb_worker_count = --pool-size`
     - `bcdb_serial_gate_mode = 0`
     - `bcdb_result_ring_slots = RESULT_RING_CAPACITY`
     - `max_connections >= max(256, 3 * pool_size + 64)`
   - Uses `ALTER SYSTEM` and restart when readbacks do not match.

8. **Role and restore**
   - Ensures role `neel` exists, because BCDB worker bootstrap can use the OS
     service account.
   - Runs `scripts/restore_usertable_small.sql` on all replicas unless skipped.
   - The restore resets BCDB state, recreates `usertable_small`, and recreates
     the Merkle index.

9. **Server start**
   - Starts one `ariabc_pg_server` per node.
   - Uses Raft members `id=host:9000`.
   - Starts servers with `--dbType 1`, `--safedb 1`, configured
     `--dbConnPoolSize`, `--pgExecMode`, and Kafka arguments.

10. **Raft readiness and BCDB preflight**
    - Waits for all client ports.
    - Runs a direct-completion `SELECT 1` probe with high deterministic IDs.
    - Requires every server log to show `bcdb_init enabled`.

11. **Workload**
    - Runs local `ariabc_pg_gateway` against all four server client ports.
    - Uses deterministic mode (`--dbType 1`), configured `--detStartSeq`,
      `--reqIdOffset`, `--detWindow`, `--detBatchSize`, and Kafka-majority
      completion unless `--no-kafka` is selected.

12. **Throughput extraction**
    - Counts non-empty, non-comment SQL lines in the workload.
    - Prefers gateway-reported `overall time taken (millisec)` for TPS.
    - Falls back to shell wall time only if gateway timing is missing.

13. **Correctness gate**
    - Reads `divergence_count` and `permanent_failures` from the gateway log.
    - Fails immediately if either is non-zero.

14. **Post-run Merkle verification**
    - Samples pre-marker row count and Merkle root on all replicas.
    - Checks expected row/root for the known YCSB workload when configured.
    - Inserts a marker row through the same gateway/Raft path.
    - Waits until the marker is visible on every node.
    - Samples post-marker row count, Merkle root, and `merkle_verify`.
    - Fails if any replica differs or any `merkle_verify` is not `t`.

15. **Profile collection**
    - Sends SIGTERM to servers so `PROFILE_SERVER` lines flush.
    - Collects `server_node*.log`, `nuraft_node*.log`, gateway log, marker log,
      and generated phase/profile artifacts into
      `scripts/bench_full_results/cluster4_<timestamp>/`.

### How To Read A Result Folder

For a trusted 4-node run, inspect:

| Artifact | What to check |
|---|---|
| `gateway_test.log` | `completion_path`, `waitMajority`, overall ms, `divergence_count`, `permanent_failures`, `PROFILE_GATEWAY`. |
| `post_verify_marker_gateway.log` | Marker transaction completed through the same completion path. |
| `server_node*.log` | `bcdb_init enabled`, `PROFILE_SERVER`, `PROFILE_BCDB_BLOCK`, BCDB hang/debug lines. |
| `nuraft_node*.log` | Raft startup and leader evidence. |
| Script stdout/log | GUC readbacks and pre/post Merkle roots. |

Do not headline a run that skipped post-verify, used `--no-kafka`, had non-zero
divergence/failures, or mixed PostgreSQL/`ariabc_pg` build state.

## Script 2: `run_parallel_ycsb_all_nodes.sh`

Purpose: run independent single-node YCSB matrices on the controller and all
remote lab machines in parallel.  This is not the same benchmark family as the
4-node Raft/Kafka-majority path.  It measures per-machine PostgreSQL/BCDB
throughput and signing overhead.

Typical command:

```bash
bash scripts/distributed/run_parallel_ycsb_all_nodes.sh
```

Useful targeted command:

```bash
bash scripts/distributed/run_parallel_ycsb_all_nodes.sh \
  --modes pg,det \
  --threads 1,2,4,8,16 \
  --runs 3 \
  --signing-modes 0,1
```

### Default Matrix

| Parameter | Default |
|---|---|
| Nodes | local ASUS plus four remote nodes |
| Modes | `pg,det` |
| Threads | from `benchmark_defaults.sh` (`ARIABC_DEFAULT_FULL_THREADS`) |
| Runs | 3 |
| Signing modes | `0,1` for deterministic runs |
| Signature enforcement | `bcdb_enforce_signatures=1` |
| Signing key | `scripts/bench_signing_privkey.pem` |
| PostgreSQL port | 5438 |

Default benchmark profiles:

1. `pg`: plain PostgreSQL baseline.
2. `det`, `sign=0`: deterministic unsigned workload while enforcement is on.
3. `det`, `sign=1`: deterministic signed workload.

### Phase-by-Phase Procedure

1. **Inventory and output directories**
   - Builds a node list from the local repo plus `NEEL_NODES`.
   - Creates a timestamped local result root under `scripts/bench_results/`.

2. **Stale process cleanup**
   - Kills old benchmark wrappers, `bench_threads_matrix.py`, PostgreSQL, and
     related run leftovers on every selected node.

3. **Local `ariabc_pg` rebuild**
   - Rebuilds `ariabc_pg/build` locally when a build directory exists.  This is
     mainly to keep C++ helper binaries fresh; the matrix itself is driven by
     `bench_threads_matrix.py`.

4. **Parallel sync**
   - Rsyncs the repo to `~/Desktop/ariabc_cluster`.
   - Rsyncs the install tree to `~/Desktop/ariabc_install`.
   - Stages OpenSSL headers and signing key material needed by remote builds.
   - Runs sync jobs in background and fails the whole launch if any sync fails.

5. **Launch benchmarks**
   - Local node runs directly in the controller shell.
   - Remote nodes run via `nohup bash -lc ...` over SSH.
   - Every node calls `ensure_custom_install_from_repo.sh` and
     `ensure_single_node_postgres.sh`.
   - Each node runs:

```bash
python -u bench_threads_matrix.py \
  --modes "$MODES" \
  --threads "$THREADS" \
  --runs "$RUNS" \
  --db postgres \
  --user postgres \
  --port 5438 \
  --out-dir <node-output-dir> \
  [--workloads ...] [--rates ...] [--signing-modes ...] \
  [--signing-privkey ...] [--enforce-signatures ...]
```

6. **Monitor**
   - Polls every `--poll-interval` seconds.
   - Tails each node's benchmark log.
   - Warns after `--hang-timeout` seconds with no log-tail change.
   - Live-rsyncs partial remote CSVs back to the controller during the run.

7. **Collect and graph**
   - Requires `results.csv` and `summary.csv` for a node to be marked done.
   - Generates per-node PNG graphs through `bench_threads_matrix.py` helpers
     when not already present.
   - Prints a final node/status/result-directory table.

### Output Files

Each node result directory normally contains:

| File | Meaning |
|---|---|
| `results.csv` | Raw run rows for workload/mode/thread/rate/signing combinations. |
| `summary.csv` | Aggregated statistics by benchmark key. |
| `det_overhead.csv` | DET-vs-PG comparison when matching keys exist. |
| `*.png` | Generated throughput graphs. |
| node benchmark log | Setup and run trace for that node. |

### How To Compare The Two Script Families

| Question | Use |
|---|---|
| "What is the full distributed replicated system TPS?" | `run_4node_raft_cluster.sh` |
| "Do all four replicas end in the same database state?" | `run_4node_raft_cluster.sh` |
| "What is a single machine's PG vs DET overhead?" | `run_parallel_ycsb_all_nodes.sh` |
| "How expensive is signed DET vs unsigned DET?" | `run_parallel_ycsb_all_nodes.sh` |
| "Can I use the single-node 9k-class result as the 4-node result?" | No. They are different benchmark families. |

## Current Bottleneck Interpretation

The recovered 5.4k-class full-system path is not blocked by raw machine
reachability, Kafka startup, or basic Raft connectivity.  The atomic
Raft-batch enqueue fix proved the server now executes real BCDB blocks:
`cluster4_20260430_051459` reported `det_block_batches=83`,
`det_block_avg=247`, and `det_block_max=256` on all four nodes.  That removed
the earlier size-1-batch suspicion, but did not move the full-system path to
10k TPS.

The remaining measured bottleneck is block completion itself.  With the
block-level result wait in `cluster4_20260430_052145`, per-slot fallback waits
fell to near zero (`slot_wait_p95_us` normally `0..1`), while
`wait_block_ms` still dominated each 256-tx block.  Pool/batch 512 made this
worse (`cluster4_20260430_052833`, 3,332 TPS), and pool/batch 128 was also
slower (`cluster4_20260430_053151`, 4,712 TPS).  The best trusted point from
this pass remains the 256/256 shape at roughly 5.4k TPS.

Any proposed optimization must preserve the trust gate above; high apparent
TPS with divergence or skipped Merkle verification is invalid.

## Operational Rules

- Verify runtime mode from artifacts, not from command intent.
- Rebuild local U24-consumed artifacts before syncing if shared PostgreSQL
  headers or `ariabc_pg` code changed.
- Keep Ubuntu 22.04 nodes on-host rebuilt when C++/PostgreSQL ABI details
  matter.
- Treat `completionPath=direct` as a debug or lower-trust latency baseline
  unless the run also passes the full post-run state check.
- Treat `completionPath=kafka_majority`, `waitMajority=1`, zero divergence,
  zero permanent failures, and matching Merkle roots as the bar for a headline
  distributed number.
- Keep `run_parallel_ycsb_all_nodes.sh` results separate from 4-node
  Raft/Kafka-majority results.
