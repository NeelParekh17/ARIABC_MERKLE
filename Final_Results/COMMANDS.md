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
TPCC_TRIALS=3 # September historical sweeps; v2 uses recorded unequal attempt counts.
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
| `OOM_100M`, fresh v2 PRIMARY | Fresh 100M rows, fillfactor 90; A θ 0/.99/1.2 + B/C/D/F θ .99; workers 1/4/8/16; pg SERIALIZABLE+jitter, det, synchronous Merkle 200/32/1024/256; 20K statements; 32MB buffers; 1 trial | 84 (220 consolidated) |
| `OOM_100M`, published 2026-09-29 | 100M rows; A θ 0/.99/1.2 + B, C, D, F θ .99; workers 1/4/8/16; pg SERIALIZABLE, pg READ COMMITTED, det, det + Merkle; 20K statements; 32MB buffers; 1 trial | 108 |
| `OOM_100M`, 2026-10-01 addition | Same 100M inputs; A θ0/.99/1.2 + B/D/F θ.99; Merkle split 1024 / merge 256, workers 1/4/8/16; canonical det controls at A/F θ.99, workers 1/16; SERIALIZABLE only | 28 (136 total) |
| `TPCC/v2_20261002`, PRIMARY scaling | All modes FF90, SERIALIZABLE; pg+jitter, det, synchronous Merkle 16384/1/16, fanout 32, split 1024/merge 256; 20K tx; initial pass + stall reruns, reverse second pass + stall reruns, extras | 89 accepted attempts / 36 distinct points |
| `TPCC/v2_20261002/headline_ab` | W100/32 workers; C1..C5 geometry/fillfactor A/B; SERIALIZABLE; 20K tx; C1 stall trial excluded from means | 11 accepted / 10 included |
| `TPCC/workers_w100`, previous | 100 warehouses; workers 8/16/24/32/48/64; pg, det, Merkle (hash % 200 and warehouse routing); 20K transactions; 32GB buffers; 3 initial trials | 72 + 9 extras |
| `TPCC/warehouses_w32`, previous | warehouses 5/10/20/30/50/75/100; 32 workers; pg, det, Merkle (hash % 200 and warehouse routing); 20K transactions; 32GB buffers; 3 trials | 84 |
| `Recovery/ariabc-recovery-size-scaling-k75-c300-20260920T110603Z-007325` | 11 sizes from 1M to 50M; fanout 32; K=75; C=300; 10 repetitions | 110 |

Prerequisites: SSH access to the named hosts, the custom PostgreSQL/gateway/server
installation and restore inputs on the benchmark nodes. Earlier OOM pgdata was deleted
at the user's request; historical campaigns cannot be repeated from their original
physical baselines. Fresh v2 uses `pgdata_base_v2_ff90` and its derived plain baseline.
OOM cache clearing requires the configured sudo
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

## 2. OOM 100M — fresh v2 primary result

Completed output: `.111:/home/neel/claude_ctl/results/oom_v2_20261001/`.
Curated evidence: `OOM_100M/runs/v2_20261001/`. All 84 cases accepted;
28 post-workload Merkle PASS; no divergence/permanent failures/exhausted retries.
The fresh fillfactor-90 dataset and v2 binaries replace the primary OOM graphs.
Previous values and figures remain archived; v2 includes C for all three modes.

The exact completed controller invocation was:

```bash
# Historical invocation on neel@10.129.27.111; campaign is already complete.
cd /home/neel/claude_ctl/AriaBC_v2
bash scripts/distributed/oom_v2/campaign_v2.sh /home/neel/claude_ctl/results/oom_v2_20261001
```

`campaign_v2.sh` runs the PG build on .247, builds matching C++ binaries on .111,
generates data, verifies the baseline, performs preflight/hash checks, runs one
balanced 84-case invocation, then verifies and writes the campaign summary.
The final settings are in `scripts/distributed/oom_v2/arguments.sh`; the literal
generation and timed-case commands expand the following shared argument array:

```bash
# Commands executed on .111 against the isolated DB/server on .247.
V2_REPO=/home/neel/claude_ctl/AriaBC_v2
V2_OUT=/home/neel/claude_ctl/results/oom_v2_20261001
V2_REMOTE=/tmp/ariabc_oom_v2_20261001
V2_INSTALL=/home/neel/claude_opt/install_v2
V2_CLUSTER=/home/neel/claude_opt/cluster_v2
V2_COMMON=(--remote-host 10.129.148.247 --remote-user neel
  --remote-dir "$V2_REMOTE" --install-dir "$V2_INSTALL" --cluster-dir "$V2_CLUSTER"
  --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo "$V2_REPO"
  --db-port 5458 --server-port 8058 --base-dir-name pgdata_base_v2_ff90
  --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
  --trials 1 --modes pg bcdb_det bcdb_merkle --workers 1 4 8 16
  --combos a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 f:0.99
  --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
  --reset-mode delta --delta-content-check sampled
  --gateway-timeout 1800 --reset-timeout 7200 --verify-timeout 7200
  --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
  --usertable-fillfactor 90 --gen-shared-buffers 512MB
  --gen-maintenance-work-mem 1GB --gen-parallel-maintenance-workers 2
  --gen-min-mem-available-mb 4608 --generation-timeout 21600)
cd "$V2_REPO"
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --gen-only --out-dir "$V2_OUT/generation"
python3 -u scripts/distributed/oom_v2/verify_baseline.py "$V2_OUT/baseline_evidence" "${V2_COMMON[@]}"
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --skip-gen --preflight-only --out-dir "$V2_OUT/preflight"
python3 scripts/distributed/oom_v2/check_workloads.py "$V2_OUT/provenance/published_workloads.json" "$V2_OUT/generation"
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --skip-gen --auto-resume --out-dir "$V2_OUT/campaign"
python3 scripts/distributed/oom_v2/summarize.py "$V2_OUT"
```

This is a record of executed commands, not an instruction to overwrite or regenerate
the completed dataset. Use the immutable final source archive and matching executable
hashes in `OOM_100M/README.md` for reproduction; git HEAD alone omits uncommitted changes.
The final successful snapshot SHA-256 is
`b376a4586ee96f263db61190f9dcdd69a330f2478cf3181b8e8135dc368cf171`.
Bulky archives remain on .111; published `remote_bulk_sha256.txt` lists their hashes.
The launch report's earlier snapshot hash is an intermediate failed attempt.
The measured runner hash and successful build manifest match the frozen source.
The controller's memory-guard and rsync corrections were made after source freeze;
supplied final copies are archived in `OOM_100M/runs/v2_20261001/controller_scripts/`.
Use those orchestration settings with the frozen PG/C++/runner source rather than
the older controller scripts embedded in the tarball. Their README records that
these script copies came from the supplied workspace, not a new remote fetch.

Preparation history: failed builds produced no data. The first load was aborted
because its 12288MB MemAvailable guard could not pass with this fork's shared-memory
allocation; the guard was lowered to 4608MB and the partial load deleted. The completed
generation is a single clean run. Stale same-size source/build state on .247 was moved
aside; snapshot synchronization now uses `rsync -a --checksum`. These attempts are
kept under the v2 archive with failed build logs under `failed_preparation_no_data/`.

PG is SERIALIZABLE only; serialization failures are retried client-side. No new
READ COMMITTED measurements exist. Merkle maintenance stays synchronous inside the
user transaction. All modes use the fresh fillfactor-90 heap. One trial per point
does not establish stable rankings or isolate changes from the older datasets.

Publication and regeneration are local artifact analysis only. The one-time
publication commands already run were:

```bash
python3 scripts/distributed/oom_v2/publish_v2.py --source .bench_tmp/oom_v2_20261001
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/plot_oom_figures.py --campaign v2
python3 scripts/distributed/oom_v2/publish_readme.py
```

Repeatable orchestrator checks (no benchmark rerun required):

```bash
python3 scripts/distributed/oom_v2/publish_v2.py --source Final_Results/OOM_100M/runs/v2_20261001 --validate-only
(cd Final_Results/OOM_100M/runs/v2_20261001 && sha256sum -c ARCHIVE_SHA256SUMS)
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/plot_oom_figures.py --campaign v2
python3 scripts/distributed/oom_v2/publish_readme.py
# Preserve the archived historical PNGs; regenerate into a separate directory.
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/plot_oom_figures.py \
  --campaign previous --out $HOME/ariabc_data/figures/oom_previous
```

## 2 (previous). OOM 100M — pg, det and det + Merkle

Results: `Final_Results/OOM_100M/` (README, `summary.csv`, `figures/`, `runs/`).

Prerequisites on `neel@10.129.148.247`:

- The install in `~/Desktop/ariabc_install` and the server in `~/Desktop/ariabc_cluster`,
  both built from the commit being measured. The server must include pg retry jitter:
  it prints `retry_jitter=` in its stats, and the runner checks it.
  - `.247` has no C++ compiler, so ariabc_pg is built on `.111` against `.247`'s install.
- `pgdata_base_fanout32_tblnamed`: the golden copy with the node table renamed to
  `merkle_node_usertable` and `merkle_verify` PASS. The runner derives the `_plain`
  baseline (no Merkle indexes) the first time it runs.

The following records the September det/Merkle and pg SERIALIZABLE command dimensions.
Use section 2a for the October split-1024 addition. A fresh build with changed geometry
defaults cannot recreate split-32 observations; keep the published baseline and its
compatible binary. The third historical campaign used `pg_rc` for A/B/C/D and is
retained only as archival evidence; fresh benchmark commands use SERIALIZABLE.

```bash
COMBOS="a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 f:0.99"
R="python3 -u scripts/distributed/run_oom_100m_benchmark.py --workers 1 4 8 16 --trials 1 --skip-gen"

# det and det + Merkle -> runs/det_merkle
$R --modes bcdb_det bcdb_merkle --combos $COMBOS --out-dir Final_Results/OOM_100M/runs_new/det_merkle

# pg SERIALIZABLE, exponential backoff with full jitter -> runs/pg_serializable
$R --modes pg --pg-retry-jitter on --combos $COMBOS --out-dir Final_Results/OOM_100M/runs_new/pg_serializable

```

Runner defaults:

- **Reset** (`--reset-mode delta`): byte-identical rsync restore from the stopped
  baseline for each variant, with a size/mtime check every case and a `diff -rq` byte
  comparison on the first restore of each working copy and every 10th restore.
- **SSD settle:** before each cold start, wait until QD1/QD8 reads and O_DSYNC writes are
  within 1.25× of the idle calibration.

These commands retain the historical dimensions. Their original pgdata is no longer
available. Regenerate the archived observations from existing CSVs with
`python3 scripts/distributed/plot_oom_figures.py --campaign previous --out $HOME/ariabc_data/figures/oom_previous`.

## 2a. OOM 100M — split 1024 + optimized code (2026-10-01)

Published archives: `OOM_100M/runs/merkle_s1024` (24 cases) and
`OOM_100M/runs/det_control_20261001` (four cases). The literal executed shell,
controller logs and successful exit status are preserved in
`OOM_100M/campaign_s1024_20261001/`; the copied raw runs retain their original paths
and mode labels. Consolidated labels are `bcdb_merkle_s1024` and `det_control`.

The measured optimized PostgreSQL identity is **HEAD 19562ae + uncommitted
changes**, reported by the campaign handoff. Executable hashes are recorded in
`OOM_100M/README.md` and each run's `preflight.txt` and per-case setup/result.
These commands select the existing installs; they do not build or sync them.
The exact measured source patch was not archived, so a newly built current tree
cannot be assumed byte-identical to the measured optimized executable.

Prerequisites on `.247`: the existing stopped `pgdata_base_f32s1024` baseline,
rebuilt with `install_opt` using `WITH (partitions=200, fanout=32)`, split 1024 /
merge 256; 211,400 nodes, 204,800 leaves; node-table `VACUUM FULL` and verification
`t`. Do not rebuild the prepared campaign baseline. The controls use the canonical
install and the existing `pgdata_base_fanout32_tblnamed_plain`. The complete
preparation account and unavailable script/log paths are in
`OOM_100M/campaign_s1024_20261001/BASELINE_PROVENANCE.md`.

The orchestrator runs this from the controller checkout, with PostgreSQL/server
on `.247` and gateway on `.111`. Run sequentially; keep both baselines and installs
unchanged. This preserves the executed dimensions while using a fresh output root.
`OOM_TRIALS=1` matches the measured campaign; five repeats are recommended for a
new performance claim. Isolation is SERIALIZABLE throughout, with synchronous
Merkle maintenance and client-executor serialization retries.

```bash
OOM_S1024_OUT="$RESULT_ROOT/OOM_100M_s1024"
test ! -e "$OOM_S1024_OUT"
mkdir -p "$OOM_S1024_OUT"

python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --workers 1 16 --trials "$OOM_TRIALS" --skip-gen \
  --install-dir /home/neel/Desktop/ariabc_install \
  --base-dir-name pgdata_base_fanout32_tblnamed \
  --modes bcdb_det --combos a:0.99 f:0.99 \
  --out-dir "$OOM_S1024_OUT/det_control" \
  > "$OOM_S1024_OUT/det_control.log" 2>&1

python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --workers 1 4 8 16 --trials "$OOM_TRIALS" --skip-gen \
  --install-dir /home/neel/claude_opt/install_opt \
  --base-dir-name pgdata_base_f32s1024 \
  --modes bcdb_merkle --combos a:0.0 a:0.99 a:1.2 b:0.99 d:0.99 f:0.99 \
  --out-dir "$OOM_S1024_OUT/merkle" \
  > "$OOM_S1024_OUT/merkle.log" 2>&1

python3 scripts/distributed/oom_s1024/compare.py \
  --new "$OOM_S1024_OUT/merkle" \
  --control "$OOM_S1024_OUT/det_control" \
  --old-summary Final_Results/OOM_100M/runs/v2_20261001/publication_input/summary_before_v2.csv \
  --out-dir "$OOM_S1024_OUT/comparison"
```

Recompute the published comparison from the relocated archives (local Python
analysis only, no DB or benchmark). The fresh directory preserves the archived
comparison; artifact paths in the recomputed CSV point at the published runs.

```bash
OOM_COMPARE_PARENT="$(mkdir -p $HOME/ariabc_data/figures && mktemp -d $HOME/ariabc_data/figures/oom-comparison.XXXXXX)"
python3 scripts/distributed/oom_s1024/compare.py \
  --new Final_Results/OOM_100M/runs/merkle_s1024 \
  --control Final_Results/OOM_100M/runs/det_control_20261001 \
  --old-summary Final_Results/OOM_100M/runs/v2_20261001/publication_input/summary_before_v2.csv \
  --out-dir "$OOM_COMPARE_PARENT/comparison"
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/plot_oom_figures.py --campaign previous --out $HOME/ariabc_data/figures/oom_previous
(cd Final_Results/OOM_100M/runs/merkle_s1024 && sha256sum -c ARCHIVE_SHA256SUMS)
(cd Final_Results/OOM_100M/runs/det_control_20261001 && sha256sum -c ARCHIVE_SHA256SUMS)
```

The orchestrator should fetch the original baseline preparation files, which were
not available under `.bench_tmp` during publication. Fetch into a fresh directory;
inspect and archive them before making a claim about exact rebuild/compaction
steps. This reads remote files and does not rerun preparation.

```bash
OOM_PREP_FETCH="$(mktemp -d /tmp/ariabc-oom-preparation.XXXXXX)"
scp neel@10.129.148.247:/tmp/ariabc_oom_100m/rebuild_s1024.sh \
    neel@10.129.148.247:/tmp/ariabc_oom_100m/rebuild_s1024.log \
    neel@10.129.148.247:/tmp/ariabc_oom_100m/compact_s1024.sh \
    "$OOM_PREP_FETCH/"
```

The separate code-only microbenchmark's reported 2–4% gain comes from the archived
campaign handoff, not paired raw observations present here. Retrieve its original
observations/provenance before quoting it as an independently verified attribution.
No fresh C case exists; default figures leave that new series absent.

## 3. TPC-C — v2 primary publication (2026-10-02)

The primary warehouse/worker PNGs now use **all-new pg/det/Merkle FF90
SERIALIZABLE measurements**, best of every accepted attempt with min–max bands.
Merkle uses synchronous warehouse routing 16384/1/16, fanout 32, split 1024 /
merge 256. Old split-32 warehouse-routing and hash-%-200 observations appear
only as labeled reference curves on the right panels. September plots and
README are preserved in `TPCC/previous/`; their source CSVs/scripts are unchanged.

Completed ranking sources (all DB/server/gateway/driver work ran there):

- `protectdr@10.129.7.57:~/claude_checks/tpcc_sweep_v2_20261002_130000/`
- `protectdr@10.129.7.57:~/claude_checks/tpcc_v2_20261002_051210/`

The completed sweep has 89 accepted attempts for 36 distinct mode/W/worker
points. The shared W100/32 point appears in both tables without another run.
Initial pass and automatic stall reruns were followed by a reverse-order second
pass and reruns, then two extras each for det W20/32, Merkle W100/16 and det
W100/64. Exact attempt counts and observations are in [TPC-C README](TPCC/README.md).
The source `summary.md` retained its original first-pass wording, so use the
archived status history and per-run accepted records for the final method.

Literal executed commands are in
[`sweeps/commands.log`](TPCC/v2_20261002/sweeps/commands.log) and
[`headline_ab/commands.log`](TPCC/v2_20261002/headline_ab/commands.log).
Frozen sweep scripts, headline launch/continuation harnesses, working-tree
diffs and binary hashes are retained with the evidence. Measured PostgreSQL
was `install_v2`; the retry-jitter server SHA starts `cc02e6df`. A fresh build of
today's working tree is not automatically byte-identical to this frozen build.

Headline means include two attempts/configuration: C1 trial 2 (887.78 TPS) is
excluded as a ranking stall, with its evidence retained. C4/C5 set FF90 on the
eight mutable tables; immutable `item` keeps default fillfactor. Sweeps set
FF90 on all nine. Headline WAL uses decimal kB/tx. These configuration details
prevent silently treating headline and sweep observations as identical controls.

Orchestrator commands from the repository root, **saved-file analysis only**:

```bash
python3 scripts/distributed/tpcc_v2/publish_v2.py --validate-only
(cd Final_Results/TPCC/v2_20261002/sweeps && sha256sum -c SHA256SUMS)
(cd Final_Results/TPCC/v2_20261002/headline_ab && sha256sum -c SHA256SUMS)
(cd Final_Results/TPCC/previous && sha256sum -c SHA256SUMS)
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/tpcc_v2/publish_v2.py
```

The publisher independently recomputes best/median/min and A/B means, audits
100 measured attempts' terminal completion, settings, durability, relation
options, WAL and eight-table state projections, verifies fetched checksums,
and regenerates three figures, derived CSVs/audit JSON and the README. It
starts no databases and invokes no builds/benchmarks. Ranking's stalls remain
unexplained; suspected NUMA placement is not a confirmed cause.

Regenerate the previous style/observations into a fresh directory:

```bash
TPCC_PREVIOUS_OUT="$(mkdir -p $HOME/ariabc_data/figures && mktemp -d $HOME/ariabc_data/figures/tpcc-previous.XXXXXX)"
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 Final_Results/TPCC/scripts/plot_warehouses.py \
  --csv Final_Results/TPCC/warehouses_w32/all_runs.csv \
  --summary "$TPCC_PREVIOUS_OUT/warehouses_summary.csv" \
  --out "$TPCC_PREVIOUS_OUT/tpcc_warehouses_scaling.png"
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 Final_Results/TPCC/scripts/plot_workers.py \
  --csv Final_Results/TPCC/workers_w100/all_runs.csv \
  --summary "$TPCC_PREVIOUS_OUT/workers_summary.csv" \
  --out "$TPCC_PREVIOUS_OUT/tpcc_workers_scaling.png"
```

`publish_fetch.py` implements BatchMode SSH/rsync of a bounded evidence allowlist,
with hashes computed on ranking before copying. It refuses to overwrite the
already-fetched immutable snapshot. No pgdata, ptrace or bulk server logs are
needed for publication; no benchmarks were run during this publication task.

## 3 (previous). TPC-C — worker scaling at 100 warehouses

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

## 4 (previous). TPC-C — warehouse scaling at 32 workers

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


## 7. OOM optimization experiments — 2026-10-01

Published evidence: `Final_Results/OOM_100M/optimizations_20261001/`.
These are 24 unique measured cases, all SERIALIZABLE, durable, synchronous
Merkle split 1024 / merge 256, and one trial per point. Eight WAL-off cases
also serve as fillfactor-100 controls; the 32 publication rows are not
independent trials. Exact historical controller scripts and status are in
`wal_compression/campaign.sh` and `fillfactor90/chain.sh` under that archive.
The fillfactor chain contains historical remote directory cleanup: retain it
as evidence and use the fresh-output commands below for another measurement.

Publication regeneration (data analysis only; no SSH or workload):

```bash
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/oom_opt/plot_opt.py
(cd Final_Results/OOM_100M/optimizations_20261001 && sha256sum -c ARCHIVE_SHA256SUMS)
```

For another matched campaign, the orchestrator runs the following on its
controller checkout (historically `/work/ARIABC/AriaBC` on `10.129.27.111`).
PostgreSQL and the server execute on lab host `10.129.148.247`; no database,
build or benchmark is to run on the developer workstation. Retain the three
stopped baselines named below and freeze the install. This reproduces the
exploratory one-trial dimensions and counterbalanced WAL mode order, using
fresh output directories. Repeat the entire sequence in fresh roots to qualify
an apparent winner; these commands do not themselves establish repeatability.

```bash
set -euo pipefail
cd /work/ARIABC/AriaBC
OOM_OPT_REPEAT_ROOT=$(mktemp -d .bench_tmp/oom_opt_repeat_20261001_XXXXXX)
OOM_OPT_COMMON=(
  --remote-host 10.129.148.247 --remote-user neel
  --remote-dir /tmp/ariabc_oom_100m
  --install-dir /home/neel/claude_opt/install_opt
  --workers 1 16 --combos a:0.0 f:0.99 --trials 1
  --txs 20000 --seed 42 --skip-gen --reset-mode delta --verify-mode fast
)
OOM_OPT_WRAPPER=(python3 -u scripts/distributed/oom_opt/run_wal_compression.py)
OOM_OPT_DET=(--base-dir-name pgdata_base_fanout32_tblnamed --modes bcdb_det)
OOM_OPT_MERKLE=(--base-dir-name pgdata_base_f32s1024 --modes bcdb_merkle)

"${OOM_OPT_WRAPPER[@]}" --wal-compression off "${OOM_OPT_COMMON[@]}" \
  "${OOM_OPT_DET[@]}" --out-dir "$OOM_OPT_REPEAT_ROOT/det_off" \
  2>&1 | tee "$OOM_OPT_REPEAT_ROOT/det_off.log"
"${OOM_OPT_WRAPPER[@]}" --wal-compression on "${OOM_OPT_COMMON[@]}" \
  "${OOM_OPT_DET[@]}" --out-dir "$OOM_OPT_REPEAT_ROOT/det_on" \
  2>&1 | tee "$OOM_OPT_REPEAT_ROOT/det_on.log"
"${OOM_OPT_WRAPPER[@]}" --wal-compression on "${OOM_OPT_COMMON[@]}" \
  "${OOM_OPT_MERKLE[@]}" --out-dir "$OOM_OPT_REPEAT_ROOT/merkle_on" \
  2>&1 | tee "$OOM_OPT_REPEAT_ROOT/merkle_on.log"
"${OOM_OPT_WRAPPER[@]}" --wal-compression off "${OOM_OPT_COMMON[@]}" \
  "${OOM_OPT_MERKLE[@]}" --out-dir "$OOM_OPT_REPEAT_ROOT/merkle_off" \
  2>&1 | tee "$OOM_OPT_REPEAT_ROOT/merkle_off.log"

# Reuse the retained, stopped fillfactor-90 baseline from 2026-10-01.
"${OOM_OPT_WRAPPER[@]}" --wal-compression off "${OOM_OPT_COMMON[@]}" \
  --base-dir-name pgdata_base_f32s1024_ff90 --modes bcdb_det bcdb_merkle \
  --out-dir "$OOM_OPT_REPEAT_ROOT/ff90_off" \
  2>&1 | tee "$OOM_OPT_REPEAT_ROOT/ff90_off.log"
```

The original fillfactor preparation was executed by the orchestrator on `.247`
using the following guarded script; it requires a new evidence directory,
110 GiB free, available memory, and a missing destination baseline. A retained
`pgdata_base_f32s1024_ff90` is reused above, so do not rerun preparation over it.
This records the original procedure, not a request to delete the old baseline.

```bash
# ON lab host 10.129.148.247, user neel; invoked by the orchestrator only.
OOM_OPT_PREP_ROOT=/tmp/ariabc_oom_100m/ff90_prep_$(date -u +%Y%m%dT%H%M%SZ)
bash /tmp/ariabc_oom_100m/prepare_fillfactor90_inroot.sh "$OOM_OPT_PREP_ROOT"
```

Keep preparation roots-before/after, verification, sizes, geometry and raw
logs with the new campaign; the publication retains the fetched preparation
stdout, while detailed historical remote preparation files were not fetched.
Every measured case must retain terminal result evidence for all 20,000
statements, zero divergence/permanent failures/retry exhaustion, SERIALIZABLE
and durability settings, matching binary/workload identities, cold-reset
records and Merkle PASS. Record per-user-table `n_tup_upd` and
`n_tup_hot_upd` before/after if measuring HOT; no such snapshots exist in this
publication. The historical ff90 node table contains dead tuples from the
pre-fix rebuild path. A later compact rebuild changes the physical baseline;
use matched node preparation for both fillfactors and label it a new campaign.
