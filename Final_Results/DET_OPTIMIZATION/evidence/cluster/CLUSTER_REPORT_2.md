# Cluster follow-up: matched det A/B, standalone Merkle, online recovery

Date: 2026-10-08. Source commit: `3bde2fdc` on `main` (only the PostgreSQL environment passthrough). Lab execution used gateway `10.129.27.111` and replicas `.247/.246/.248`; no build, database or benchmark ran on the workstation.

## Outcome

**A/B: PASS (18/18). Standalone Merkle: PASS (3/3). Recovery A: PASS, 8824.18 TPS, zero empty buckets. Recovery C: FAIL (exit 143), successful live repair followed by an unfinished all-three audit; final TPS/empty buckets/Phase 8 are unavailable.** The lookahead-off median improves matched default by 1.93% at W=16 and 0.26% at W=8. All requested cases were attempted and their evidence retained.

## A. Passthrough and matched interleaved YCSB-A

`run_4node_raft_cluster.sh` previously forwarded a fixed environment list to the gateway and exported a fixed set of values in its replica startup shell. It had no generic postmaster environment passthrough. The committed change validates each whitespace-separated token against `^BCDB_[A-Z0-9_]+=.*$`, quotes it with Bash `%q`, forwards `BCDB_PG_EXTRA_ENV` through delegation, prefixes all six start/restart commands (including the cold-cache start), and records `bcdb_pg_extra_env` in `run_meta.env`. Empty input preserves the existing commands. `run_all_modes_gateway_sweep.py` calls `run_cluster_case`, whose `os.environ.copy()` preserves the variable, so neither Python source file needed a committed change.

Commit: `scripts: opt-in BCDB_PG_EXTRA_ENV passthrough to replica postmasters`, with the requested co-author trailer. Patch: `2_passthrough.patch`; commit detail: `2_commit_detail.txt`. `bash -n` passed on `.247` (`2_bash_n.txt`). Remote validation accepted the requested values, rejected malformed/non-BCDB names with exit 2, and preserved shell metacharacters as literal values (`2_env_validation.txt`). `graphify update .` completed (`2_graphify_update.log`); its existing SQL-dependency/parser warnings are retained.

### Configurations and source interpretation

| Config | `BCDB_PG_EXTRA_ENV` | Effective behavior |
|---|---|---|
| default | unset | All merged switches use default-on behavior |
| nolookahead | `BCDB_GATE_LOOKAHEAD=0` | Lookahead disabled; early validation and tag dedup remain enabled |
| noev | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` | Pre-optimization validation/tag protocol; lookahead is also disabled by its early-validation prerequisite |

`src/backend/bcdb/shm_transaction.c:186` returns `early_validate_enabled && lookahead_enabled`. In `worker.c:1587`, the waiting transaction with `tx_id <= published_max + 2` spins while lookahead is enabled; the disabled branch uses bounded spin/yield before sleeping. Thus **nolookahead vs default** isolates the switch, while **delta vs noev** compares against the requested older protocol. `BCDB_DT_POST_PUBLISH_SETTLE` was never overridden and stayed default-on. These runs assess throughput and correctness; they do not directly measure CPU time stolen by waiting backends.

### Command and order

The exact flags match section 1 of the previous report: YCSB-A theta 0, 20k transactions, det window 1024, 96 client lanes, 32MB PostgreSQL buffers, verified cold runs, and executor/connection/pool/BCDB workers all equal W. Each config/W/trial gets a separate output directory and a sweep with `--trials 1`; the outer driver assigns the requested trial number.

```bash
# Run on gateway .111, with source first synced from the workstation.
export TMPDIR=$HOME/ariabc_data/detopt_cluster_20261008/scratch
export BYPASS_DELEGATION=1 CLUSTER_STOP_POSTGRES_ON_EXIT=1 CLUSTER_DET_WINDOW=1024
# BCDB_PG_EXTRA_ENV is unset/default, nolookahead, or noev as shown above.
# First case: SKIP_SYNC=0 SKIP_BUILD=0; every later case: both =1.
python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark ycsb --gateway-host 10.129.27.111 --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-host 10.129.148.247 --db-user neel --db-port 5438 --server-port 8000 \
  --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt \
  --workers "$W" --modes cluster --run-cluster --db-shared-buffers 32MB \
  --cold-runs --order-seed 42 --trials 1 \
  --out-dir "$OUT/2_ab_t${TRIAL}_w${W}_${CONFIG}"
```

Order for each W within a trial: t1 default → nolookahead → noev; t2 nolookahead → noev → default; t3 noev → default → nolookahead. W=16 then W=8 within each trial. This is a rotating, interleaved matrix, not three grouped config campaigns. Full commands/environment/exit statuses are in each `2_ab_*/2_command.json` and `attempts/*.json`; driver: `2_ab_driver.py`. Initial sync/build logs are retained under the first run.

### Medians and deltas

| W | Config | Trial 1 TPS | Trial 2 TPS | Trial 3 TPS | Median TPS | Delta vs noev |
|---:|---|---:|---:|---:|---:|---:|
| 16 | default | 13522.65 | 13333.33 | 13559.32 | 13522.65 | -3.92% |
| 16 | nolookahead | 13783.60 | 13661.20 | 13802.62 | 13783.60 | -2.07% |
| 16 | noev | 14074.60 | 14104.37 | 13995.80 | 14074.60 | +0.00% |
| 8 | default | 8481.76 | 8496.18 | 8442.38 | 8481.76 | -1.65% |
| 8 | nolookahead | 8554.32 | 8485.36 | 8503.40 | 8503.40 | -1.40% |
| 8 | noev | 8631.85 | 8624.41 | 8572.65 | 8624.41 | +0.00% |

Disabling lookahead changes the median by **+1.93% at W=16** and **+0.26% at W=8** relative to matched default. Three short trials describe this campaign; they do not establish a universal ranking or prove the CPU-stealing mechanism.

### Every measured attempt

All rows below retain the full gateway terminal profile, all-three audit, post-marker readback, PostgreSQL logs, cache evidence, and binary/source provenance. The root/data digest and three independent `merkle_verify=t` checks are in `2_full_run_audit.json`.

| W | Trial | Config | TPS | Divergence | Permanent failures | Merkle admin123 / user4 / utkarsh | Run ID (under `2_runs/`) |
|---:|---:|---|---:|---:|---:|---|---|
| 16 | 1 | default | 13522.65 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_112447_3486d873` |
| 16 | 1 | nolookahead | 13783.60 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113142_0965d69d` |
| 16 | 1 | noev | 14074.60 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113248_7e4f1a97` |
| 8 | 1 | default | 8481.76 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113353_56697f7e` |
| 8 | 1 | nolookahead | 8554.32 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113457_4c72dafc` |
| 8 | 1 | noev | 8631.85 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113605_72475c84` |
| 16 | 2 | nolookahead | 13661.20 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113714_bc97673f` |
| 16 | 2 | noev | 14104.37 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113821_e1878c54` |
| 16 | 2 | default | 13333.33 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_113929_a3d13d70` |
| 8 | 2 | nolookahead | 8485.36 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114036_db47d1f4` |
| 8 | 2 | noev | 8624.41 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114141_930740df` |
| 8 | 2 | default | 8496.18 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114251_91f390cc` |
| 16 | 3 | noev | 13995.80 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114357_efcd406e` |
| 16 | 3 | default | 13559.32 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114502_966929c8` |
| 16 | 3 | nolookahead | 13802.62 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114609_2f9439b7` |
| 8 | 3 | noev | 8572.65 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114717_5663eb77` |
| 8 | 3 | default | 8442.38 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114826_cf2ca0b3` |
| 8 | 3 | nolookahead | 8503.40 | 0 | 0 | PASS / PASS / PASS | `cluster4_20261008_114934_eaaf7d67` |

### Live postmaster evidence

Every case was sampled read-only via `/proc/<pid>/environ` on all replicas. PID changes retain evidence for startup/restart and the final cold-cache postmaster. The sampling cadence and commands were identical for all cases (three concurrent SSH queries roughly every 2 seconds, continuing through the run); this small instrumentation cost is shared across the matched configs. Full records: `2_ab_*/2_postmaster_environ.jsonl`.

```bash
pid=$(head -n1 ~/Desktop/ariabc_cluster/.bench_tmp/2_detopt_pgdata/postmaster.pid)
tr '\0' '\n' < /proc/$pid/environ | grep -E '^BCDB_(GATE_LOOKAHEAD|DT_EARLY_VALIDATE|DT_TAG_DEDUP|DT_POST_PUBLISH_SETTLE)='
```

| Config | Node | Example postmaster evidence |
|---|---|---|
| default | admin123 | `POSTMASTER_PID=490682; no optimization overrides` |
| default | user4 | `POSTMASTER_PID=1037456; no optimization overrides` |
| default | utkarsh | `POSTMASTER_PID=380355; no optimization overrides` |
| nolookahead | admin123 | `POSTMASTER_PID=495279; BCDB_GATE_LOOKAHEAD=0` |
| nolookahead | user4 | `POSTMASTER_PID=1040670; BCDB_GATE_LOOKAHEAD=0` |
| nolookahead | utkarsh | `POSTMASTER_PID=384067; BCDB_GATE_LOOKAHEAD=0` |
| noev | admin123 | `POSTMASTER_PID=499878; BCDB_DT_TAG_DEDUP=0; BCDB_DT_EARLY_VALIDATE=0` |
| noev | user4 | `POSTMASTER_PID=1043741; BCDB_DT_TAG_DEDUP=0; BCDB_DT_EARLY_VALIDATE=0` |
| noev | utkarsh | `POSTMASTER_PID=387711; BCDB_DT_TAG_DEDUP=0; BCDB_DT_EARLY_VALIDATE=0` |

### Provenance and unchanged controls

Matched portable source fingerprint(s): `8de79d55eb32ff09bfae6d6befa29ba39437e851f65c6f903a3dd3b760c40576`. Runtime harness path/TMPDIR changes are included in that fingerprint and synchronized to all replicas; they are preserved in `2_original_scripts/`, `2_runtime_changes.json`, and run snapshots. Native det code is the committed merged code. All accepted attempts require matching expected/live source fingerprints and valid build manifests on each replica.

## B. Standalone Merkle consistency test

To honor the repository rule protecting the canonical data directory, each replica used a fresh physical copy (`cp -a --reflink=never`) of the stopped canonical PostgreSQL instance at `.bench_tmp/2_detopt_pgdata`, on port 5438. The canonical `.bench_tmp/single_node_pgdata` was only read for the copy and checksums. The catalog cleanup therefore removes the stale fixture from the **isolated copies**, not from the offline canonical databases. Before/after/control-file hashes are retained under `2_isolation_*.txt` and `2_canonical_unchanged_*.txt`.

All three isolated copies initially had `ariabc_kv_test` and `idx_merkle_kv`, with the node relation absent. The only SQL repair was:

```sql
DROP TABLE IF EXISTS ariabc_kv_test CASCADE;
DROP TABLE IF EXISTS ariabc_internal.merkle_node_ariabc_kv_test;
```

Before/after catalog queries are in `2_repair.sql`; results in `2_catalog_repair_<node>.txt` show both table/index present before and no matching catalog objects after. No orphan node relation existed.

The first startup-only attempt (`2_merkle_startup_command.json`, `2_merkle_startup.log`) used `--skip-restore` and failed on another pre-existing stale index: `ariabc_internal.merkle_node_warehouse` was missing on all three replicas. This attempt is preserved in the CSV as `merkle_startup_failed`. No TPC-C table/index was dropped. After recovery A/C, the startup-only retry used the normal YCSB restore and `--skip-sync --skip-build --skip-rdkafka-setup --skip-workload`, default det settings, raft-kafka/majority_async_all3, port 5438, 32MB and W=16. It passed. Exact command/result ID: `2_merkle_startup_retry_command.json`; console: `2_merkle_startup_retry.log`. Then, on gateway .111:

```bash
export TMPDIR=$HOME/ariabc_data/detopt_cluster_20261008/scratch
bash scripts/distributed/test_merkle_consistency.sh
```

`2_standalone_merkle.log` retains the script verdict. Independent SQL readbacks verify row count, root, native `merkle_verify_index`, and the final `k=10` sentinel.

| Node | Verdict | Rows | Root | Native verify | Sentinel |
|---|---|---:|---|---|---|
| admin123 | PASS | 50 | c531ccbb513259116dea3321af59140d8ebc7e013305acc8c1b65c27aa2cf3b9 | t | val_010_v2 |
| user4 | PASS | 50 | c531ccbb513259116dea3321af59140d8ebc7e013305acc8c1b65c27aa2cf3b9 | t | val_010_v2 |
| utkarsh | PASS | 50 | c531ccbb513259116dea3321af59140d8ebc7e013305acc8c1b65c27aa2cf3b9 | t | val_010_v2 |

The test fixture was dropped afterward on each isolated instance (`2_merkle_fixture_cleanup_<node>.txt`); server/PostgreSQL logs and orderly teardown are retained as `2_merkle_*` and `2_cleanup_after_merkle_retry_*`.

## C. Online recovery A and C

The wrapper help was read first (`2_recovery_help.txt`). Both cases used its canonical YCSB-A theta-0 160k workload, W=8, 96 lanes, window 1024, 32MB, raft-kafka and majority_async_all3, default merged det environment, with no A/B overrides. `SKIP_SYNC=1 SKIP_BUILD=1` reused the matched build. These are one run per case, not three-trial medians.

```bash
R=scripts/distributed/recovery/run_recovery_cluster_test.sh
$R --recovery-mode off --skip-build
$R --recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 \
   --inject-fault-delay-sec 5 --skip-build
```

| Case | TPS majority-visible | Published reference | Delta | Empty 100ms steady buckets | Permanent failures | All-3 audit / quorum | Phase 8 admin123 / user4 / utkarsh |
|---|---:|---:|---:|---:|---:|---|---|
| A | 8824.18 | 8,850 | -0.29% | 0 | 0 | yes; 160000/160000; quorum 160000/160000 | PASS / PASS / PASS |
| C | UNKNOWN (abort) | 8,693 | UNKNOWN | UNKNOWN (no latency CSV) | 0 last observed; terminal UNKNOWN | Not finalized; 160000 client completions last observed | NOT RUN / NOT RUN / NOT RUN |

Case A result directory: `2_runs/cluster4_2_recovery_A_20261008_115302_d7cd8841/`. Exact invocation/exit: `2_recovery_A_command.json`; console: `2_recovery_A.log`.

`[11:53:58]   TPS_TIMELINE completions=160000 duration_ms=18101 bucket_ms=100 mean_tps=8839.2 steady_min_tps=7140.0 steady_max_tps=9910.0 empty_buckets=0 max_completion_gap_ms=24.93 max_gap_at_ms=9658`

Recovery counts: triggered=0, succeeded=0, failed=0.


Case C result directory: `2_runs/cluster4_2_recovery_C_20261008_115402_c30f719c/`. Exact invocation/exit: `2_recovery_C_command.json`; console: `2_recovery_C.log`.


Recovery events (gateway, avoiding duplicate wrapper copies):

```
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=177 B=45055 detect_to_quarantine_ms=0 cut_ms=103 recover_call_ms=66217 repair_ms=54764 drain_ms=139 catchup_ms=11282 live=1 mismatched_partitions=75 differing_leaves=85 rows_deleted=0 rows_upserted=91 full_copies=0 replay_from=178 replay_target=626 total_ms=66339 digest=usertable_small:9d05b53e3e7638baa835844e544ce32eb2761c7ebaa9c65e56484b74f7b060e0
```


**Case C: FAIL, runner exit 143.** Injection changed exactly 100 tuples and the root (`fault_injection.log` and `fault_timeline.env`, injection rc=0). The gateway detected node 4 through result divergence. The live repair event passed, repairing 85 differing leaves with 91 upserts, no full copy, and total_ms=66339 (repair_ms=54764, catchup_ms=11282). Healthy-majority client completions reached 160000 between the 20s and 25s progress samples. The final snapshot still had permanent_failures=0 and raw divergence_count=1, but neither run_summary.env nor tx_latency.csv nor a terminal gateway profile was produced. The all-three audit never finalized and Phase 8 was not entered. Reported progress completed_tps after 25s includes subsequent waiting and is not the requested majority-visible workload TPS; it is deliberately excluded from the CSV throughput field.

The follower initially stopped at det_seq=45055 during repair, then resumed to 159999. By watchdog cycle 8 all replicas showed bcdb_last_committed_txid=159999; nevertheless the gateway remained alive through the 100s progress sample. The unchanged client completion counter reached watchdog cycle 12, which collected pg_stat_activity/locks/gate diagnostics and terminated the gateway. These observations establish an unfinished audit after repair; they do not isolate its internal cause. Live evidence: 2_recovery_C_live_gateway.txt, 2_recovery_C_live_utkarsh.txt, 2_recovery_C_live_admin123.txt; complete watchdog diagnostics are in the result directory.

Observed recovery events: 1 live PASS event; terminal aggregate counters remain UNKNOWN because the gateway was terminated before its final profile.
Recovery counts: triggered=UNKNOWN, succeeded=UNKNOWN, failed=UNKNOWN.

### Phase 8 roots for recovery

| Case | Node | Rows | Root | Data MD5 | Native verify |
|---|---|---:|---|---|---|
| A | admin123 | 12001 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` | t |
| A | user4 | 12001 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` | t |
| A | utkarsh | 12001 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` | t |
| C | admin123 | NOT RUN | UNKNOWN | UNKNOWN | NOT RUN |
| C | user4 | NOT RUN | UNKNOWN | UNKNOWN | NOT RUN |
| C | utkarsh | NOT RUN | UNKNOWN | UNKNOWN | NOT RUN |

## Evidence, abnormalities and final state

Every captured replica PostgreSQL log was explicitly grepped for `BCDB_HANG`, `BCDB_INVARIANT_POST_PUBLISH_APPLY`, and `PANIC`: 81 logs scanned, 0 logs with matches. File list, commands' exit codes and exact hits: `2_postgres_warning_scan.json`. Independent counters, checks, post-marker roots, profiles, environment samples and manifests: `2_full_run_audit.json`; any acceptance failures: `2_audit_errors.json`.

The initial builds completed with OpenSSL SHA256_Final deprecation and ignored write-return-value warnings (first run build logs; full PostgreSQL build logs in 2_build_pg_gateway_full.log and 2_build_pg_user4_full.log). The failed startup and C watchdog abort are retained above. An early reporting-helper regex was corrected before the final audit; the earlier extractor output is clearly named 2_ab_only_full_audit_pre_regex_fix.json, while 2_full_run_audit.json and 2_ab_only_full_audit.json contain the corrected checks. No native code was changed to make a failed attempt pass.

**No-/tmp constraint exception:** TMPDIR-backed scratch files were directed to ~/ariabc_data/detopt_cluster_20261008/scratch, including gateway and replica SSH work. The copied PostgreSQL configuration inherited its compiled /tmp Unix-socket default during the catalog repair and A/B runs; that socket path was missed during setup. The transient socket/lock files therefore violated the literal no-/tmp requirement. Before standalone/recovery, only the isolated copy was changed to use the persistent scratch directory for unix_socket_directories (2_persistent_socket_*.txt; standalone SHOW readbacks confirm the path). The canonical configuration remained unchanged. No benchmark database or result artifacts were placed in /tmp.

All sources were edited locally before sync; only the passthrough change was committed. Runtime copies adjust the data directory, run/log roots, persistent scratch environment, excludes for `Final_Results/` and graph artifacts, redundant gateway self-scp, and process diagnostic matching. They do not change workload, protocol switches, durability/quorum paths or det code. Full runtime differences are preserved for reproduction. No `Final_Results/` file was modified by this task; pre-existing user changes were preserved.

All benchmark PostgreSQL instances and ariabc_pg_server processes are cleanly stopped. Final stop/port evidence: 2_cleanup_after_merkle_retry_<node>.txt and 2_canonical_unchanged_<node>.txt. Each canonical postgresql.conf, postgresql.auto.conf and global/pg_control SHA256 equals its pre-copy hash, and no canonical postmaster.pid exists. The unrelated utkarsh port-8000 service and pre-existing Kafka brokers were preserved. Stopped isolated databases are retained at ~/Desktop/ariabc_cluster/.bench_tmp/2_detopt_pgdata. Gateway source scripts were restored to their committed/original versions after preserving exact operative copies in 2_runtime_scripts/ (2_runtime_restore.json). A future normal run should sync/build to refresh the wrapper fingerprint, since the operative path variant was included in the experiment build identity.

Local artifact root: `/work/ARIABC/AriaBC/.bench_tmp/detopt_cluster_20261008/`. The complete new run artifacts are under `2_runs/`; per-case sweeps under `2_ab_*/`. Gateway originals use the same relative paths under `/home/neel/ARIABC/AriaBC/`. `cluster_results_2.csv` keeps the previous columns plus `config`, with append-only rows for the 18 A/B attempts, failed startup, standalone test and recovery A/C. Blank metrics in failed-attempt rows mean no valid terminal measurement, not zero; merkle_pass=0 for those rows means validation was not passed or not run, not a measured root mismatch.
