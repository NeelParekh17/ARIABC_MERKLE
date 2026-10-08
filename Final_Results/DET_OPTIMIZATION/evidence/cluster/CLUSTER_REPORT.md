# Merged det-mode replicated cluster validation — 2026-10-08

**Overall: PARTIAL / STOPPED ON STARTUP FAILURE.** All 12 YCSB-A attempts and both YCSB-D attempts passed the replicated correctness checks. Startup for the standalone Merkle test failed on all three replicas before Raft server startup. The standalone test and online-recovery A/C cases were not run. No source fixes or database repairs were attempted.

Local starting HEAD: `deead85514be7e53ff846c308d07cff2459f5bf6` (includes merge `19e8acb2`). Builds and database/benchmark execution occurred on the approved controller/replica path; none ran on the workstation.

## Commands and artifacts

Runner `--help` was read before execution; every requested flag exists. The YCSB-A command matches `Final_Results/WORKER_THREAD_LATENCY/run_final.sh` at window 1024. First attempt had `SKIP_SYNC=0`, `SKIP_BUILD=0`; later A attempts and both D attempts reused the build. Client lanes remained 96; executor workers, PostgreSQL connections, BCDB workers and pool size followed W. Cold cache evidence exists for all three replicas in every measured attempt.

```bash
cd /work/ARIABC/AriaBC
mkdir scripts/bench_full_results/detopt_cluster_ycsb_20261008
CLUSTER_DET_WINDOW=1024 python3 -u scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo /home/neel/ARIABC/AriaBC --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt --workers 1,4,8,16 --modes cluster --run-cluster --db-shared-buffers 32MB --cold-runs --order-seed 42 --trials 3 --out-dir /work/ARIABC/AriaBC/scripts/bench_full_results/detopt_cluster_ycsb_20261008 > scripts/bench_full_results/detopt_cluster_ycsb_20261008/runner.log 2>&1
mkdir scripts/bench_full_results/detopt_cluster_ycsbd_20261008
SKIP_SYNC=1 SKIP_BUILD=1 CLUSTER_DET_WINDOW=1024 python3 -u scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo /home/neel/ARIABC/AriaBC --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 --workloads scripts/ycsb_suite/ycsb_workload_d_skew_0_99_20k.txt --workers 16 --modes cluster --run-cluster --db-shared-buffers 32MB --cold-runs --order-seed 42 --trials 2 --out-dir /work/ARIABC/AriaBC/scripts/bench_full_results/detopt_cluster_ycsbd_20261008 > scripts/bench_full_results/detopt_cluster_ycsbd_20261008/runner.log 2>&1
```

Both sweep processes exited 0. Campaign output directories:

- `/work/ARIABC/AriaBC/scripts/bench_full_results/detopt_cluster_ycsb_20261008`
- `/work/ARIABC/AriaBC/scripts/bench_full_results/detopt_cluster_ycsbd_20261008`
- Each attempt JSON records its exact delegated command, workload SHA256, exit code and artifact directory under `scripts/bench_full_results/<run_id>/`. Attempt logs are in each campaign’s `attempts/` directory.
- Full independent audit: `full_run_audit.json`; machine-readable results: `cluster_results.csv`; initial health/help/source snapshot: `preflight_evidence.txt`; baseline snapshot: `baseline_evidence.json`.

## 1. YCSB-A: theta 0, 20k, window 1024

Baseline medians come from the published `Final_Results/WORKER_THREAD_LATENCY/summary.csv` (exact unrounded values). TPS is client majority-visible workload completion; all-three drain completion is independently required for acceptance.

| W | Trial 1 TPS | Trial 2 TPS | Trial 3 TPS | Median TPS | Published TPS | Delta | Divergence / permanent failures, every trial | Merkle admin123 / user4 / utkarsh, every trial |
|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1 | 2597.74 | 2630.54 | 2630.54 | 2630.54 | 2652.52 | -0.83% | 0 / 0 | PASS / PASS / PASS |
| 4 | 5133.47 | 5125.58 | 5129.52 | 5129.52 | 5177.32 | -0.92% | 0 / 0 | PASS / PASS / PASS |
| 8 | 8467.40 | 8517.89 | 8528.78 | 8517.89 | 8605.85 | -1.02% | 0 / 0 | PASS / PASS / PASS |
| 16 | 13689.25 | 13633.27 | 13623.98 | 13633.27 | 14094.43 | -3.27% | 0 / 0 | PASS / PASS / PASS |

These matched campaign medians show a small throughput decrease at all four worker counts, largest at W=16. They do not establish a general speedup or isolate which merged optimization causes the change.

## 2. YCSB-D: theta 0.99, W=16, 20k

Chosen file: `scripts/ycsb_suite/ycsb_workload_d_skew_0_99_20k.txt`, identified by listing the suite. Original file contains 19,072 SELECTs and 928 INSERTs. The harness materializes its version-5 workload under each campaign’s `workloads_v5/`; exact workload hashes and paths are retained in campaign and attempt metadata.

| W | Trial 1 TPS | Trial 2 TPS | Median TPS | Published TPS | Delta | Divergence / permanent failures, every trial | Merkle admin123 / user4 / utkarsh, every trial |
|---:|---:|---:|---:|---:|---:|---|---|
| 16 | 23557.13 | 24509.80 | 24033.47 | 24213.08 | -0.74% | 0 / 0 | PASS / PASS / PASS |

Published D reference: `Final_Results/YCSB/summary.csv`, run `cluster4_20260924_061840_f026ce9b`, one trial, W=16, 32MB. This is a historical single-trial comparison, with a different code fingerprint; it is weaker performance evidence than the matched three-trial A baseline.

## Post-marker evidence for every measured attempt

Every row below has marker visibility on all three nodes, matching row counts, roots and data MD5, `merkle_verify=t` on all three, and `usertable_small consistency: PASS`. All-three workload audit is 20,000 / 20,000 and valid in every row. The artifact directory is `scripts/bench_full_results/<run_id>/`; the exact node lines are in `runner.log` and in `full_run_audit.json`.

| Section | W | Trial | Run ID | Rows per node | Post-marker root on all 3 nodes | Merkle admin123 / user4 / utkarsh |
|---|---:|---:|---|---:|---|---|
| YCSB-A | 8 | 1 | `cluster4_20261008_105236_869f8b8a` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 4 | 1 | `cluster4_20261008_105921_2e2dfa81` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 16 | 1 | `cluster4_20261008_110013_3b1dab09` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 1 | 1 | `cluster4_20261008_110105_fa30c38d` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 16 | 2 | `cluster4_20261008_110203_07b189d8` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 8 | 2 | `cluster4_20261008_110252_5b0dd01e` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 1 | 2 | `cluster4_20261008_110346_936bf59a` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 4 | 2 | `cluster4_20261008_110443_0491ebae` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 4 | 3 | `cluster4_20261008_110540_85f2b411` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 16 | 3 | `cluster4_20261008_110632_f057ea57` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 8 | 3 | `cluster4_20261008_110724_e9f7ea47` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-A | 1 | 3 | `cluster4_20261008_110814_d9398d8b` | 12001 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | PASS / PASS / PASS |
| YCSB-D | 16 | 1 | `cluster4_20261008_111009_e7e76afa` | 13025 | `07723811d1b83698557249077cf1fb33ada16441250640fa047b7a66ff36b5af` | PASS / PASS / PASS |
| YCSB-D | 16 | 2 | `cluster4_20261008_111109_802095d6` | 13025 | `07723811d1b83698557249077cf1fb33ada16441250640fa047b7a66ff36b5af` | PASS / PASS / PASS |

## Code and binary provenance

New portable source fingerprint (controller and every replica, every measured attempt): `179fbd4a251edc48756ff40a3e27b371b6ce4c404619ffa77d7ace13143a23a8`.
Oct 6 window-1024 campaign fingerprint: `b9ad5497ed0c526092700d0d1f7f1458eb99fb1410256b470e47708695eb07c5`. The fingerprints differ.
All measured attempts have `BINARY_PROVENANCE_PASS=1`, `build_manifests_valid=1`, and identical expected/live portable source fingerprints on all replicas. The originating commit is recorded by delegation; replica git HEAD is `unknown` because `.git` is excluded from sync, so acceptance uses source and binary manifests.

| Node | PostgreSQL SHA256 | Server SHA256 | Gateway SHA256 |
|---|---|---|---|
| admin123 | `79b10ef590d00503445f217963c8834645d36d9fc82efdfb7ee18a9125c83c98` | `eb2c5e4d40c4b4b0adab244085e32dad5ce3560486db4b148dccbe145755eb2d` | `3a135a707c4ef5c76f079f9ead2106847c5b4745cc15e5246cdb91bde1f078f0` |
| user4 | `9123177d818e9480534dc3a8049ec897f3172b33d3f6a6ce35c280d70bf4878f` | `0009ad8a5012b7128125d01f8d5c1254a2ba9e30a32ff5094ee6dab013f2b233` | `b39b3adfeff2cf420c9ccc546caca8eb4bae07fd2e2a7be241a9ef50acee8a5c` |
| utkarsh | `79b10ef590d00503445f217963c8834645d36d9fc82efdfb7ee18a9125c83c98` | `eb2c5e4d40c4b4b0adab244085e32dad5ce3560486db4b148dccbe145755eb2d` | `3a135a707c4ef5c76f079f9ead2106847c5b4745cc15e5246cdb91bde1f078f0` |

On each replica, SSH `grep -n bcdb_dt_post_publish_settle_enabled ~/Desktop/ariabc_cluster/src/backend/bcdb/worker.c` found the function and call sites at lines 498, 970, 3172 and 3305. All three `worker.c` hashes are `41867b7e82508727f843505c7babbd7a2f7cb031ec9e5fbc4e6e7afea1f2a286`. Commands and complete output: `replica_source_provenance.txt`. No det optimization environment overrides were inherited; default-ON merged behavior was used.

## 3. Standalone Merkle test: prerequisite startup FAIL; test NOT RUN

`scripts/distributed/test_merkle_consistency.sh` was read before execution. It hardcodes DB port 5438, drops/recreates only `ariabc_kv_test`, inserts 50 rows, applies updates and compares roots on all three nodes. The YCSB sweep deliberately stops servers and PostgreSQL at completion, so a startup-only step was needed before running the test. `--skip-restore` was chosen to preserve the last YCSB-D table contents.

Exact startup command (exit 1):

```bash
ssh -o BatchMode=yes neel@10.129.27.111 'mkdir -p /home/neel/ariabc_data/detopt_cluster_20261008/scratch; cd /home/neel/ARIABC/AriaBC; export PATH="$HOME/bin:$HOME/.local/bin:$PATH"; export TMPDIR=/home/neel/ariabc_data/detopt_cluster_20261008/scratch; SKIP_SYNC=1 SKIP_BUILD=1 SKIP_RDKAFKA_SETUP=1 CLUSTER_RUN_ID=cluster4_detopt_merkle_startup_20261008 ./scripts/distributed/run_4node_raft_cluster.sh --skip-sync --skip-build --skip-rdkafka-setup --skip-restore --skip-workload --ordering-mode raft-kafka --kafka-completion-mode majority_async_all3 --db-port 5438 --db-shared-buffers 32MB --server-exec-workers 16 --server-pg-connections 16 --pool-size 16 --bcdb-workers 16 --bcdb-init-block-size 16 --bcdb-decouple-workers 1 --raft-apply-ledger-mode off --enable-merkle-index 1' > .bench_tmp/detopt_cluster_20261008/merkle_startup_valid.log 2>&1
```

Failure artifacts: `/work/ARIABC/AriaBC/scripts/bench_full_results/cluster4_detopt_merkle_startup_20261008/` and `merkle_startup_valid.log` in this report directory. Temporary files for this startup were redirected to `/home/neel/ariabc_data/detopt_cluster_20261008/scratch`.

| Node | Startup/bootstrap | Standalone Merkle | TPS / median / delta | Divergence / permanent failures |
|---|---|---|---|---|
| admin123 | FAIL | NOT RUN | N/A | unknown / unknown |
| user4 | FAIL | NOT RUN | N/A | unknown / unknown |
| utkarsh | FAIL | NOT RUN | N/A | unknown / unknown |

At 11:13:47 IST all nodes failed in Phase 3.1, before Phase 4 Raft server startup:

```text
ERROR: relation "ariabc_internal.merkle_node_ariabc_kv_test" does not exist
QUERY: SELECT hash FROM ariabc_internal.merkle_node_ariabc_kv_test WHERE prefix_len = 0
CONTEXT: SELECT bool_and(pg_catalog.merkle_verify_index(i.indexrelid)) ... WHERE am.amname = 'merkle'
ERROR: Phase 3.1 setup/restore failed on one or more nodes
```

Read-only catalog evidence confirms an existing empty `ariabc_kv_test` with index `idx_merkle_kv` on all three nodes. `ariabc_internal` contains the usertable Merkle node relation but no `merkle_node_ariabc_kv_test`. The installed `pg_catalog.merkle_verify_index(regclass)` definition and index metadata are saved in `failure_catalog_<node>.txt`; SQL is in `failure_catalog_queries.sql`. The failing bootstrap scans all Merkle indexes, so the pre-existing test index blocks startup without restore. This is a schema/bootstrap prerequisite failure; no standalone workload executed, and it is not evidence of det execution divergence. No table/index/function repair was attempted.

Planned command, not executed due to failed prerequisite:

```bash
ssh -o BatchMode=yes neel@10.129.27.111 'cd /home/neel/ARIABC/AriaBC; PATH="$HOME/bin:$PATH" TMPDIR=/home/neel/ariabc_data/detopt_cluster_20261008/scratch bash scripts/distributed/test_merkle_consistency.sh'
```

## 4. Online recovery: NOT RUN

Section 3 did not pass. Per the requested stop-on-failure rule, neither recovery case was launched. Published values are references, not new measurements.

| Case | Recovery mode / fault | Published TPS | New TPS_majority_visible / median / delta | empty_buckets | permanent_failures | All-3 audit | Phase 8 / per-node Merkle |
|---|---|---:|---|---|---|---|---|
| A | off | 8850 | NOT RUN | unknown | unknown | NOT RUN | NOT RUN |
| C | both; utkarsh; count 100; delay 5 s | 8693 | NOT RUN | unknown | unknown | NOT RUN | NOT RUN |

Canonical planned commands from section 6, not executed:

```bash
R=scripts/distributed/recovery/run_recovery_cluster_test.sh
$R --recovery-mode off
$R --recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --skip-build
```

## Abnormalities, restarts and cleanup

- First sync printed `cannot delete non-empty directory` warnings for generated `scripts/rust_workload/target/` directories. Sync continued and source/binary provenance subsequently passed; warnings are retained in the first attempt log.
- Controller/U22 builds printed compiler warnings about ignored `write()` return values; both requested C++ targets and PostgreSQL installs completed.
- Initial startup invocation used `CLUSTER_RUN_ID=detopt_merkle_startup_20261008` and was rejected immediately with exit 2 (`Invalid CLUSTER_RUN_ID`) before processes or run artifacts were created. The corrected invocation used the required `cluster4_` prefix; both invocation logs are preserved. No source was changed to correct that argument.
- All 42 measured per-node PostgreSQL logs were explicitly grepped for `BCDB_HANG`, `BCDB_INVARIANT_POST_PUBLISH_APPLY` and `PANIC:`: zero matches. Commands, file lists and grep exit statuses are in `postgres_warning_scan.json`. Current live replica PostgreSQL logs were also grepped and copied after startup failure: zero such warnings; bootstrap SQL errors are present.
- No benchmark hang, watchdog abort, unexpected PostgreSQL crash or emergency restart was observed in the 14 accepted attempts. Cold-run PostgreSQL starts/restarts, cache eviction, and final SIGTERM of servers to flush profiles are expected harness operations, followed by clean sweep teardown.
- Failed Merkle startup started and restarted PostgreSQL on all three nodes as part of normal readiness setup, then stopped at bootstrap. Those instances were cleanly stopped after evidence capture; `cleanup_<node>.txt` verifies each is stopped and ports 5438/9000/8001 are absent. The unrelated utkarsh service on port 8000 was preserved.
- user4 preflight: total 15 GiB, free 8 GiB, available 6 GiB, swap 46 GiB with 1 GiB used (`free -g`). After failed startup its available memory rounded to 0 GiB, with about 6 GiB shared; cleanup released the benchmark instance. Detailed snapshots are in preflight and failure diagnostics.
- No repository source files were edited by this agent. `Final_Results/` was only read. Concurrent external edits appeared in `ARCHITECTURE_EXPLORER.html`, `Final_Results/COMMANDS.md`, and `Final_Results/DET_OPTIMIZATION/`; they were preserved. The post-startup live source fingerprints still matched the built identity.
- The explicitly approved YCSB runner internally uses unqualified `mktemp` on the controller and does not delegate TMPDIR; its SSH environment had TMPDIR unset. Thus internal transient runner files may have used the default temporary directory despite the task’s no-/tmp instruction. No benchmark/database artifacts were manually placed there. The standalone startup used the persistent TMPDIR above; no source or shell configuration was changed to alter the approved runner.

CSV convention: `merkle_pass=1` marks measured attempts with complete post-marker validation. The failed startup uses `0` to indicate unpassed validation, not a measured root mismatch; unknown metrics and unexecuted cases are blank. See section names for attempted versus skipped entries.
