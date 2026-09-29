# Commands for every Final_Results benchmark

Run these from `/work/ARIABC/AriaBC` in Bash, one benchmark at a time.
The commands below match the saved experiment dimensions and trial counts.
They run the **current corrected code**: they cannot recreate the old SQL semantics,
durability settings, timing boundaries, binary versions or exact measured TPS.
See [CORRECTIONS.md](CORRECTIONS.md) for those differences. No old result is overwritten.

## 0. Set output paths and trial counts

Run this block once in the terminal where you will run the commands below:

```bash
set -euo pipefail
cd /work/ARIABC/AriaBC

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)_$$"
RESULT_ROOT="$PWD/Final_Results/reruns/$RUN_STAMP"
mkdir -p "$RESULT_ROOT"

# Match the number of trials in the archived Final_Results.
YCSB_TRIALS=1
OOM_TRIALS=1
TPCC_TRIALS=3
RECOVERY_REPETITIONS=10

# Keep the normal 96-client configuration; cluster bootstrap verifies builds.
unset SKIP_SYNC SKIP_BUILD FORCE_BUILD DET_CLIENT_WORKERS
```

For the repeated performance campaign recommended by the audit, run this additional
block **before** running the benchmarks; otherwise the historical counts above apply:

```bash
YCSB_TRIALS=5
OOM_TRIALS=5
TPCC_TRIALS=5
```

| Saved result | Configuration | Historical measured runs |
|---|---|---:|
| `YCSB/abcdf_4modes_4skews_cold_20260920_132045` (also copied to `YCSB/summary.csv`) | A/B/C/D/F; skews 0, .5, .99, 1.2; workers 1/4/8/16; four modes; 20K requests; 32MB buffers | 320 |
| `OOM_100M` | 100M rows; A θ 0/.99/1.2 + B, C, D, F θ .99; workers 1/4/8/16; pg SERIALIZABLE, pg READ COMMITTED, det, det + Merkle; 20K statements; 32MB buffers; 1 trial | 108 |
| `TPCC/workers_w100` | 100 warehouses; workers 8/16/24/32/48/64; pg, det, Merkle (hash % 200 and warehouse routing); 20K transactions; 32GB buffers; 3 trials | 72 |
| `TPCC/warehouses_w32` | warehouses 5/10/20/30/50/75/100; 32 workers; pg, det, Merkle (hash % 200 and warehouse routing); 20K transactions; 32GB buffers; 3 trials | 84 |
| `Recovery/ariabc-recovery-size-scaling-k75-c300-20260920T110603Z-007325` | 11 sizes from 1M to 50M; fanout 32; K=75; C=300; 10 repetitions | 110 |

Prerequisites: SSH access to the named hosts, the custom PostgreSQL/gateway/server
installation and restore inputs on the benchmark nodes, and the existing immutable
100M `pgdata_base_fanout32` baseline. OOM cache clearing requires the configured sudo
access; do not put a password in this file. The cluster path synchronizes/builds its
nodes on its first case. TPC-C runs entirely on `protectdr@10.129.7.57` (ranking): the DB,
server, gateway and driver all run on that host, from the isolated build in `~/claude_checks`.

## 1. YCSB — all families, skews, workers and modes

Gateway: `neel@10.129.27.111`. Standalone DB: `neel@10.129.148.247`.
Cluster DB replicas: `.247`, `.246`, `.248` on `10.129.148.*`.
The `abcdf_4skews` selector is exactly the 20 saved family/skew combinations.

```bash
python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark ycsb \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-host 10.129.148.247 \
  --db-user neel \
  --db-port 5438 \
  --server-port 8000 \
  --workloads abcdf_4skews \
  --workers 1,4,8,16 \
  --modes cluster,pg,bcdb_det,bcdb_merkle \
  --run-cluster \
  --db-shared-buffers 32MB \
  --cold-runs \
  --order-seed 42 \
  --trials "$YCSB_TRIALS" \
  --out-dir "$RESULT_ROOT/YCSB"
```

Output: `$RESULT_ROOT/YCSB/summary.csv`, per-attempt evidence, plots and qualification
report. Cluster raw logs are additionally stored in `scripts/bench_full_results/cluster4_*`.
Version 5 YCSB D/F workload semantics are deliberate corrections; these
rows must not be appended to the historical CSV. Cluster TPS measures client
majority-visible throughput (matching the historical baseline and Merkle comparison),
while the all-three follower audit drain metrics are tracked in attempt metadata. D
reads recently completed inserts via point selects under the latest distribution,
preserving true read-latest behavior while ensuring reads only target committed records.

## 2. OOM 100M — pg, det and det + Merkle

Results: `Final_Results/OOM_100M/` (README, `summary.csv`, `figures/`, `runs/`).

Prerequisites on `neel@10.129.148.247`:

- The install in `~/Desktop/ariabc_install` and the server in `~/Desktop/ariabc_cluster`,
  both built from the commit being measured. The server must include pg retry jitter:
  it prints `retry_jitter=` in its stats, and the runner checks it.
  - `.247` has no C++ compiler, so ariabc_pg is built on `.111` against `.247`'s install.
- `pgdata_base_fanout32_tblnamed`: the golden copy with the node table renamed to
  `merkle_node_usertable` and `merkle_verify` PASS. The runner derives the `_plain`
  baseline (no Merkle indexes) the first time it runs.

Three campaigns produce the published results. Run them one at a time.

```bash
COMBOS="a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 f:0.99"
R="python3 -u scripts/distributed/run_oom_100m_benchmark.py --workers 1 4 8 16 --trials 1 --skip-gen"

# det and det + Merkle -> runs/det_merkle
$R --modes bcdb_det bcdb_merkle --combos $COMBOS --out-dir Final_Results/OOM_100M/runs_new/det_merkle

# pg SERIALIZABLE, exponential backoff with full jitter -> runs/pg_serializable
$R --modes pg --pg-retry-jitter on --combos $COMBOS --out-dir Final_Results/OOM_100M/runs_new/pg_serializable

# pg READ COMMITTED; F is excluded (not serializable at READ COMMITTED) -> runs/pg_read_committed
$R --modes pg_rc --combos a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 --out-dir Final_Results/OOM_100M/runs_new/pg_read_committed
```

Runner defaults:

- **Reset** (`--reset-mode delta`): byte-identical rsync restore from the stopped
  baseline for each variant, with a size/mtime check every case and a `diff -rq` byte
  comparison on the first restore of each working copy and every 10th restore.
- **SSD settle:** before each cold start, wait until QD1/QD8 reads and O_DSYNC writes are
  within 1.25× of the idle calibration.

After the runs, rebuild `summary.csv` from the runs' summaries and regenerate the figures
with `python3 scripts/distributed/plot_oom_figures.py`.

## 3. TPC-C — worker scaling at 100 warehouses

The whole pipeline runs on ranking (`protectdr@10.129.7.57`, EPYC 9654), and no other host is
involved. It uses the isolated build in `~/claude_checks`: sources in `src/`, PostgreSQL
installed to `install/`, and `ariabc_pg` built in `src/ariabc_pg/build`. The run scripts are
kept in `Final_Results/TPCC/scripts/`. Each run follows these steps:

1. Start from a fresh `initdb` with the canonical `single_node_pgdata` settings
   (SERIALIZABLE, `enable_seqscan=off`, 32GB shared buffers and the harness BCDB settings).
2. Restore exactly as `run_all_modes_gateway_sweep.py` does.
3. Run `CHECKPOINT`, then prewarm.
4. Drive the workload with the sweep harness's server and gateway flags.

```bash
ssh protectdr@10.129.7.57
cd ~/claude_checks
# 6 worker counts x 4 configurations x 3 trials = 72 runs; log in workers_sweep.log
./workers_sweep.sh
~/claude_checks/venv/bin/python chart_out/workers_plot.py
```

To run a single case, use
`merkle_run.sh <label> <warehouses> <tx> <workers> <partitions> <partition_key_columns> <subpartitions> <pg|det|merkle>`.
The configurations used are:
- **pg:** `200 0 1 pg`
- **det:** `200 0 1 det`
- **Merkle, hash % 200:** `200 0 1 merkle`
- **Merkle, warehouse routing:** `16384 1 16 merkle`

## 4. TPC-C — warehouse scaling at 32 workers

```bash
ssh protectdr@10.129.7.57
cd ~/claude_checks
# 7 warehouse counts x 4 configurations x 3 trials = 84 runs; log in chart_sweep.log
./chart_sweep.sh
~/claude_checks/venv/bin/python chart_out/plot.py
```

`Final_Results/TPCC/README.md` gives the method, results, correctness evidence and
caveats. The harness path (`run_all_modes_gateway_sweep.py --benchmark tpcc`) now also
accepts `--tpcc-merkle-partition-key-columns` and `--tpcc-merkle-subpartitions`.

## 5. Recovery — 1M through 50M, K=75, C=300, fanout 32

The profile selects exactly 1M, 3M, 5M, 7M, 10M, 15M, 20M, 25M, 30M, 40M and
50M rows; split 32, merge 8, K=75 and C=300. Ten repetitions produce 110 runs.
CPU affinity is 176–183 on the EPYC host; it does not lock CPU frequency.

> **`synchronous_commit off` for recovery benchmarks (default)**
> The metric under study is Merkle recovery latency, not PostgreSQL WAL durability.
> Enabling `synchronous_commit on` adds variable fsync wait time to every write
> inside each recovery run, inflating and noisifying the numbers without testing
> anything we care about here. If the remote host crashes mid-run the benchmark
> is simply replayed from the beginning — recovery runs are short, stateless, and
> fully deterministic given the same seed, so there is nothing to lose.

```bash
mkdir -p "$RESULT_ROOT/Recovery"

# WAL fsync is excluded because recovery latency is the metric; replay if a crash occurs.
RECOVERY_LOG_TEE_ACTIVE=1 \
bash scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh \
  --host ranking \
  --ssh-user protectdr \
  --remote-root /home/protectdr/merkle_recovery_runs \
  --remote-python /usr/bin/python3 \
  --profile size-scaling-k75-c300 \
  --build-profile release \
  --fanout 32 \
  --geometry-label fanout_f32_l16 \
  --partitions 200 \
  --levels-per-batch 1 \
  --leaf-fetch-batch-size 64 \
  --corruption-mode mixed \
  --repetitions "$RECOVERY_REPETITIONS" \
  --profiling off \
  --track-counts on \
  --artifact-mode summary \
  --audit-mode full \
  --synchronous-commit off \
  --cpu-affinity 176-183 \
  --warmup-cycles 6 \
  2>&1 | tee "$RESULT_ROOT/Recovery/runner.log"

# The runner's last stdout line is the fetched artifact directory.
RECOVERY_FETCHED="$(tail -n 1 "$RESULT_ROOT/Recovery/runner.log")"
test -d "$RECOVERY_FETCHED"
cp -a --reflink=never "$RECOVERY_FETCHED" "$RESULT_ROOT/Recovery/"
```

`RECOVERY_LOG_TEE_ACTIVE=1` routes this command's log to the new directory through
the explicit `tee`, avoiding the runner's default overwrite of repository `out.txt`.
The original fetched copy remains under `scripts/benchmark/recovery/fetched/`.
The seed remains the runner's `20260703` default, matching the saved config.

The archived recovery config used `audit_mode=skip` and `synchronous_commit=off`.
`audit_mode` is corrected to `full`; `synchronous_commit` is kept `off` (default
for all future recovery runs — see note above). The current runner randomizes
series order. The saved config leaves warmup unspecified; six cycles and affinity
176–183 come from the replication wrapper. Do not call the new run an exact
reproduction of the old audit/cache conditions.

## 6. Distributed Online Replica Recovery — 3-node cluster, YCSB 160k, in-flight fault injection

Evaluates ProtectDB Algorithm 2 running inside the replicated Raft-Kafka cluster
(`admin123` = 1, `user4` = 2, `utkarsh` = 4) under 160,000 transactions and 96 client lanes.
Client consensus path: `kafka_majority` with `majority_async_all3` validation.
Online recovery repairs corrupted tuples on a damaged replica using an MVCC snapshot
from a healthy peer, sparse Merkle tree descent and local Raft log replay without pausing
client transactions on the healthy quorum.

```bash
mkdir -p "$RESULT_ROOT/ONLINE_RECOVERY"

# Replication script executes all 7 canonical scenarios:
# 1. Baseline (Recovery OFF)
# 2. Overhead (Recovery BOTH, No Fault)
# 3. Follower Fault (Update 100 tuples @ 5s)
# 4. Follower Fault (Passive Merkle compare detection)
# 5. Follower Fault (Active vote divergence detection)
# 6. Follower Mixed Fault (Update + Delete + Insert)
# 7. Leader Mixed Fault (Prioritized Reference Selection)
bash Final_Results/ONLINE_RECOVERY/replicate_distributed_recovery.sh \
  2>&1 | tee "$RESULT_ROOT/ONLINE_RECOVERY/runner.log"
```

Individual runs can also be invoked directly:

```bash
R=scripts/distributed/recovery/run_recovery_cluster_test.sh

# A: baseline, recovery off (8,850 TPS)
$R --recovery-mode off

# B: recovery on, no fault (8,829 TPS, -0.2% overhead)
$R --recovery-mode both --skip-build

# C: follower update fault (8,693 TPS, -1.8%, 0 empty buckets)
$R --recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 \
   --inject-fault-delay-sec 5 --skip-build

# L_mix: leader mixed fault with prioritized reference selection (8,674 TPS, 0 empty buckets)
$R --recovery-mode both --inject-fault-node admin123 --inject-fault-count 100 \
   --inject-fault-delay-sec 5 --inject-fault-type mixed --skip-build
```

Verification criteria:
1. `TPS_majority_visible` within 1% to 6% of baseline.
2. `empty_buckets=0` inside the recovery window (`min_bucket_tps_inside > 0`).
3. Zero permanent failures (`permanent_failures=0`).
4. `All-3 audit valid` with 100% client quorum complete count (160,000 / 160,000).
5. Phase 8 Merkle root match across all 3 nodes (`usertable_small consistency: PASS`).

## What to retain

Keep the entire new output directories, including manifests, individual trial
rows, failed attempts, terminal logs, effective settings, cache and telemetry
evidence. Do not copy only plots or merge new rows into the archived summaries.
The commands were checked against the saved manifests/CSVs and current CLI flags;
creating this file did not launch any benchmark.
