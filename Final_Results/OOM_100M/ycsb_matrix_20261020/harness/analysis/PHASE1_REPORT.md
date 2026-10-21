# Phase 1 report — YCSB 100M matrix

**Status: COMPLETE_PHASE1 — matrix left running.** As of **2026-10-20 12:07 UTC**, all **16/16 D cases** are accepted and **16/80 main matrix cases** are complete. A separate B-tree anchor confirmation also passed. D review was released and **`c_plain` started at 12:07:06 UTC** in the existing `ycsb_matrix` session. No main matrix row was replaced. The original B-tree anchor deviation is retained below.

**Build and validation.** Source commit is `5f2a078eadb8817246f5d500dee2822f826d9c1e`. Both hosts received `git archive HEAD` from the isolated clone. SHA-256 checks of tracked regular engine, extension, C++ and NuRaft source files passed on both hosts against `engine_source.sha256`. No engine files were edited, and no commit or push was made.

PostgreSQL was built on `.247` with the reference `--enable-depend --without-readline CFLAGS=-O2 CPPFLAGS=-D_GNU_SOURCE`, then installed with `merkle_lsm` and `pgcrypto`. C++ Release server/gateway builds ran on `.111` against a copy of that PostgreSQL installation; the server was copied back to `.247`. The extension's `make installcheck` passed **1/1 SQL regression test**, against a disposable cluster on port 5494 with 32 MB shared buffers. That cluster stopped cleanly. All **35 existing OOM harness tests** passed on `.111`; separate guard checks confirmed that missing baseline inputs/manifests fail before mutation, and a second controller was rejected by the lock.

The first build driver reached successful compilation/installation but failed its final hash command because it expected `lib/merkle_lsm.so`. The library is under `lib/postgresql/`; the script now asks `pg_config --pkglibdir`. Hashes were recomputed successfully. The original failure log/status remain in `.247/evidence/build_driver_attempt1.log` and `pg_build_attempt1.status` under the new root.

| Binary | SHA-256 |
|---|---|
| `.247 install/bin/postgres` | `56cbb4d4e36a1aae5929810afb4c3abc4d8c2175d2a6e1d50c1f62ce84347a88` |
| `.247 cluster/ariabc_pg/build/bin/ariabc_pg_server` and `.111 src/ariabc_pg/build/bin/ariabc_pg_server` | `142bdca5f71fac3c599ec52fced42317f722c4c8f973cd3f97f4748622903352` |
| `.111 src/ariabc_pg/build/bin/ariabc_pg_gateway` | `489658d1e281ecb7e8ed4dd3348d7a34a55707ca20214f8abe7be91487f5349a` |
| `.247 install/lib/postgresql/merkle_lsm.so` | `93ecf7aab6080a7e94c27f03f7b9553d764d887363513f1558a712f2fca41a79` |
| `.247 install/lib/libpq.so` | `d609437caddd8613feafe7191a00982fa2a1e2d60e3a3c655c0d1829bbcffd3b` |

**Changes and reasons.** All changes are in scripts; `src/`, `contrib/`, and `.codex` were left untouched. Local paths below are relative to `repo/`. New campaign scripts live in `scripts/distributed/ycsb_matrix_20261020/` and are copied to the remote campaign roots.

| File | Reason |
|---|---|
| `scripts/distributed/run_oom_100m_benchmark.py` | Add `ControlMaster=no` to SSH/SCP; fail-closed existing-baseline guard; explicit socket and gateway workload directories; per-case WAL/commit boundary snapshots and sampler phase markers; measured campaign setup times. |
| `scripts/distributed/benchmark_contract.py` | Add `ControlMaster=no` to telemetry SSH. |
| `scripts/distributed/benchmark_telemetry.py` | Connect telemetry over explicit loopback TCP instead of the default Unix socket in `/tmp`. |
| `…/build_pg.sh` | Reproduce the PostgreSQL/extension build in the new `.247` root and record hashes. |
| `…/build_cpp.sh` | Build server/gateway on `.111` against the new installation, record hashes, copy server to `.247`. |
| `…/regression.sh` | Run isolated extension installcheck, retaining its logs and stopping its own cluster. |
| `…/lsm_sampler.py` | Singleton 1 Hz sampler with reconnection, mode-aware SQL, commits, WAL LSN, postmaster identity, LSM state and case/phase labels. |
| `…/run_matrix.sh` | Reference flags, three smoke campaigns, 15 matrix campaigns, lock, gates, timestamped status, failure continuation, and D review pause. |
| `…/check_results.py` | Audit exact case coverage, SQL count/isolation/buffers, workload hashes, failures, verification, drains and WAL samples; retain measurement and through-checkpoint WAL totals. |
| `…/inspect_anchor.py` | Read-only comparison of the B-tree anchor deviation with archived reference settings, I/O and current memory telemetry. |
| `…/repeat_anchor.py` | One separate diagnostic repeat, permitted only while D review is paused; inherit original campaign settings and retain the official matrix row. |

The top-level harness diff is saved in `agents/harness.patch` and `.111/evidence/harness.patch`. Graphify was updated after code edits. Builds, tests and database work ran only on the lab hosts.

**Controller and launch.** Workload order is **d, c, b, a, f**. Each workload runs plain, LSM, then B-tree campaigns covering its four skews: **15 invocations / 80 main cases**, each with 1 trial, 2,400,000 statements, 16 workers, SERIALIZABLE, seed 42, shared buffers 32 MB, full-jitter pg retry and gateway timeout 3600. Reset, verification, calibration, settle, generation-memory and skip-generation flags match `run_d.sh`. Failed campaigns are logged and the next campaign runs; incomplete cases remain visible for later rerun. Auto-resume preserves accepted results and checks source/executable provenance.

The plain campaign passes `--base-dir-name base_ff90`: the harness appends `_plain`, so pg/det actually restore from **base_ff90_plain**. Passing `base_ff90_plain` directly would request the wrong double-suffixed directory. LSM restores **base_ff90_mlsm**, with levels 3 / size 1024 KiB / ratio 16; B-tree restores **base_ff90**. The new guard prevents baseline creation or rebuilding. The four baseline directories' file metadata and control hashes matched before/after smoke **and after the entire D group plus diagnostic**. No old `pgdata_lsm` or `base_ff90_lsm` data was removed.

Per-workload grouping was retained because fixed overhead is substantial:

| Kind | Smoke preflight s | Smoke calibration s | Smoke total before cases s | Full D total before cases s |
|---|---:|---:|---:|---:|
| Plain | 1.060 | 300.283 | 303.472 | 411.351 |
| LSM | 1.092 | 300.298 | 302.872 | 411.385 |
| B-tree | 1.083 | 300.312 | 302.830 | 415.236 |

Full-D setup also includes generating its four 2.4M-statement workload files. Splitting into 60 campaigns would add **45 × 300 s = 3 h 45 min** of minimum calibration alone, plus additional baseline checks and first-copy content checks. Preflight is cheap; calibration is not.

The matrix launched at **2026-10-20 09:09:59 UTC** in tmux **`ycsb_matrix`** on `.111`. Immediately before launch, `.247` had **no PostgreSQL processes**, both clocksources were `tsc`, and free space was **118,854,291,456 bytes on .247 / 119,708,577,792 bytes on .111**. The earlier foreign port-5448 instance had disappeared independently; this worker never stopped or queried it. `cluster_full` and `stall_catcher` were left untouched. Raw launch gates are retained on both hosts.

**Sampler and smoke.** The sampler dynamically selects LSM stats only when the lookup AM is `merkle_lsm`, avoiding function-planning errors on other baselines. Its unmeasured monitoring connection uses READ COMMITTED. All measured clients remain SERIALIZABLE. Live non-LSM proof includes commits **285 → 302**, LSN **5/4F301978 → 5/4F341F10**, with null LSM stats. LSM live rows contain merge counters, epoch, component/base sizes and merger state. Full examples are retained in the audit JSON and sampler JSONL. Connection-refused records during deliberate server shutdowns are retained; **measurement-phase sampler errors are zero** for every accepted smoke and D case.

| Smoke case, D skew .99 | Statements | TPS | Gateway wall s | LSM drain s | WAL B/stmt: window / through checkpoint | WAL samples | Verify |
|---|---:|---:|---:|---:|---:|---:|---|
| pg | 20,000 | 15,661.71 | 1.277 | — | 19.74 / 19.75 | 6 | N/A |
| det | 20,000 | 12,315.27 | 1.624 | — | 20.15 / 20.16 | 5 | N/A |
| Merkle + LSM | 20,000 | 11,086.47 | 1.804 | 1.234 | 427.35 / 814.95 | 5 | PASS |
| Merkle + B-tree | 20,000 | 10,309.28 | 1.940 | — | 805.97 / 805.98 | 6 | PASS |

All four smoke cases passed, with zero retry attempts, exhausted retries, permanent failures or divergence. Their workload SHA-256 was identical: `9c025251db4a79632681b1a6274fd67924245a97d6d200314b84b4336900c34a`. Smoke is a setup check, not an anchor-length throughput comparison.

**First workload group: all 16 D cases accepted.** Every case completed 2,400,000 statements with **retry attempts = 0, exhausted retries = 0, permanent failures = 0, divergence = 0**. Both Merkle kinds verified `t` at every skew. LSM drains and WAL snapshots exist for every relevant case. Each row has 118–351 valid measurement-phase sampler rows; `matrix_audit.json` reports PASS with no errors and exact 16-case coverage.

TPS and wall use the gateway's timed denominator. Device counters bracket telemetry startup and the gateway process; the first WAL value additionally extends through server stop/log validation and collector settling, before the manual drain. Sampler rows labelled `measurement` use that broader WAL bracket. The second WAL value runs through the post-workload drain and checkpoint, before verification. Device counts include other filesystem traffic. Main-table device I/O excludes the manual LSM drain; deferred LSM I/O is shown separately below. All four LSM cases recorded merge counters reaching **[6, 2]** through drain; the raw state records and `lsm_d_merge_counts.json` are retained.

| Skew | Mode | TPS | Wall s | Drain s | Read MiB | Write MiB | WAL B/stmt: window / full | Retries / exhausted | Verify |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | pg | 13,747.20 | 174.581 | — | 20,558.36 | 1,467.81 | 19.05 / 19.05 | 0 / 0 | N/A |
| 0 | det | 11,720.35 | 204.772 | — | 20,919.63 | 1,516.70 | 19.45 / 19.45 | 0 / 0 | N/A |
| 0 | Merkle + LSM | 11,432.76 | 209.923 | 31.044 | 20,803.47 | 2,281.45 | 61.73 / 384.39 | 0 / 0 | PASS |
| 0 | Merkle + B-tree | 8,464.59 | 283.534 | — | 22,346.16 | 3,990.43 | 375.34 / 375.34 | 0 / 0 | PASS |
| .5 | pg | 14,589.31 | 164.504 | — | 18,991.09 | 1,499.49 | 19.00 / 19.00 | 0 / 0 | N/A |
| .5 | det | 12,316.03 | 194.868 | — | 19,020.91 | 1,683.77 | 19.40 / 19.40 | 0 / 0 | N/A |
| .5 | Merkle + LSM | 11,897.56 | 201.722 | 9.455 | 19,221.32 | 2,408.81 | 61.61 / 383.64 | 0 / 0 | PASS |
| .5 | Merkle + B-tree | 8,728.92 | 274.948 | — | 20,496.52 | 4,057.80 | 374.60 / 374.60 | 0 / 0 | PASS |
| .99 | pg | 30,914.69 | 77.633 | — | 5,601.95 | 1,111.61 | 18.91 / 18.91 | 0 / 0 | N/A |
| .99 | det | 18,530.10 | 129.519 | — | 5,602.40 | 1,527.20 | 19.31 / 19.31 | 0 / 0 | N/A |
| .99 | Merkle + LSM | 17,751.48 | 135.200 | 8.554 | 5,639.89 | 2,148.81 | 61.31 / 382.03 | 0 / 0 | PASS |
| .99 | Merkle + B-tree | 11,731.29 | 204.581 | — | 6,565.69 | 3,669.28 | 373.04 / 373.04 | 0 / 0 | PASS |
| 1.2 | pg | 46,659.93 | 51.436 | — | 1,231.13 | 1,049.81 | 18.98 / 18.98 | 0 / 0 | N/A |
| 1.2 | det | 23,640.90 | 101.519 | — | 1,236.84 | 1,434.96 | 19.38 / 19.38 | 0 / 0 | N/A |
| 1.2 | Merkle + LSM | 22,299.86 | 107.624 | 8.651 | 1,261.82 | 2,023.23 | 61.54 / 383.25 | 0 / 0 | PASS |
| 1.2 | Merkle + B-tree | 13,423.12 | 178.796 | — | 2,124.60 | 3,569.38 | 374.22 / 374.22 | 0 / 0 | PASS |

The LSM's deferred work is material. At skew .99 its window-only WAL is 61.31 B/statement, but through-checkpoint WAL is **382.03 B/statement**, comparable in scope to the reference's approximately 381 B/statement. Window-only numbers must not be presented as its full WAL cost.

| LSM skew | Additional read MiB after window through checkpoint | Additional write MiB | Total read MiB through checkpoint | Total write MiB through checkpoint |
|---:|---:|---:|---:|---:|
| 0 | 3,279.00 | 1,664.13 | 24,082.46 | 3,945.58 |
| .5 | 2,730.03 | 1,642.65 | 21,951.35 | 4,051.46 |
| .99 | 2,762.23 | 1,614.47 | 8,402.12 | 3,763.28 |
| 1.2 | 2,756.36 | 1,614.25 | 4,018.17 | 3,637.48 |

For each D combo, all four modes have the same workload SHA-256:

| Skew | SHA-256 |
|---:|---|
| 0 | `e67005b7426cdb825b3f2c858edc337ec0069f1a61eecff87896c44899929a26` |
| .5 | `e5323f203fc4dc09fc04bb39ee016f1382b09875753ffd1d3be6dd397b11ac07` |
| .99 | `ae78761fed624edacea9e66ebf356df619b41179948b25007f314fe661d69415` |
| 1.2 | `f6ab0c589c4e297b85fc82c844e3f34aa0073f4f07923dee115f32dc2e2e72b7` |

**Anchor comparison and investigation.** The original one-trial matrix results are retained:

| Mode, D .99 | Anchor TPS | Main matrix TPS | Difference |
|---|---:|---:|---:|
| pg | 30,676 | 30,914.69 | +0.78% |
| det | 18,436 | 18,530.10 | +0.51% |
| Merkle + LSM | 17,576 | 17,751.48 | +1.00% |
| Merkle + B-tree | 13,091 | 11,731.29 | **−10.39%** |

The B-tree result passed correctness but lies just outside the nominal 10% anchor band. Its SQL hash matches all three reference trials. Effective PostgreSQL settings match those trials except for the required socket directory. Its measured read await was **0.538 ms**, versus **0.459 ms** around the reference median, despite similar settled QD1 probes (~102–103 µs). Read/write volumes were broadly similar. Minimum available memory was **6,742 MiB**, swap-out and OOM-kill deltas were zero, and swap-in was only 10 pages. The reference B-tree trials already ranged from **12,355.72 to 13,160.71 TPS**. These observations support I/O variation as an explanation; they do not establish its cause or exclude every build effect.

A separate confirmation campaign completed in `results/anchor_check/`, using the same binaries, baseline and case flags, with only combos/output narrowed to D .99 and a separate directory. Executable hashes and SQL hash were checked against the original result.

| B-tree D .99 observation | TPS | Difference from anchor | Gateway wall s | Read await ms | Read / write MiB | Verify |
|---|---:|---:|---:|---:|---:|---|
| Official main matrix row, retained | 11,731.29 | −10.39% | 204.581 | 0.53846 | 6,565.69 / 3,669.28 | PASS |
| Separate diagnostic confirmation | 13,176.10 | **+0.65%** | 182.148 | 0.46165 | 6,847.24 / 3,608.79 | PASS |

The confirmation completed all 2.4M statements with zero retries, exhausted retries, permanent failures or divergence, and had **251 valid WAL samples / zero measurement-phase sampler errors**. Its WAL was 373.04 B/statement. Device read latency returned close to the reference and throughput recovered with the same build. This demonstrates run-to-run variation; the precise cause of the earlier I/O stalls is not proven. Both observations remain available. No settings were relaxed and the official one-trial matrix data was not substituted. Review approval was recorded at **12:06:57 UTC**; the controller resumed at **12:07:06 UTC**. Before release, `.247` again had no PostgreSQL processes and all four baseline metadata/control checks passed.

**Measured time and projection.** D ran from **09:09:59 to 11:45:36 UTC**, taking **9,337 s (2 h 35 m 37 s)**, excluding the subsequent review/diagnostic pause.

| Campaign | Accepted cases | End-to-end s | Mean per case, including campaign overhead |
|---|---:|---:|---:|
| Plain | 8 | 3,331 | 416.4 s |
| LSM | 4 | 2,905 | 726.3 s |
| B-tree | 4 | 3,101 | 775.3 s |
| D total | 16 | 9,337 | 583.6 s |

Gateway measurement totals **2,695.16 s**, averaging **168.45 s/case** (range **51.44–283.53 s**). Recorded resets total **1,899.75 s**, averaging **118.73 s/case**. End-to-end time additionally includes workload generation, calibration, transfers, validation, drains, verification, checkpoint/restarts and report generation.

From C's actual start at **12:07:06 UTC**, four D-equivalent remaining groups project **37,348 s (10 h 22 m 28 s)**, giving a provisional finish of **2026-10-20 22:29:34 UTC / 2026-10-21 03:59:34 IST**. The D review/diagnostic pause lasted **21 m 30 s** and is already included in this clock estimate. This is a D-based extrapolation; the unmeasured C/B/A/F workload costs can change it, especially write-heavy cases. There were **64 main cases remaining at release**; the extra diagnostic is complete and separate from those 80 cases. No repeatability claim is made from one official trial per case.

**Paths and control.** On DB host `neel@10.129.148.247`, the new root is `/home/neel/claude_checks/ycsb_matrix_20261020`: `src/`, `install/`, `cluster/`, `socket/`, `scratch/`, `regression_data/`, `lsm_sampler.py`, and `evidence/`. The persistent raw sampler is `evidence/wal_sampler.jsonl`. Existing inputs and working copies remain under `/home/neel/ariabc_data/lsm_lookup_20261014`; its live `server.log`/`postgres.log` are copied into each case's artifacts.

On controller/gateway `neel@10.129.27.111`, the new root is `/home/neel/claude_ctl/ycsb_matrix_20261020`: source `src/`, C++ build `src/ariabc_pg/build/`, copied client install `pg_install/`, workload staging `scratch/workloads/`, controller `run_matrix.sh`, and results:

- `results/smoke/{d_plain,d_lsm,d_btree}/run_*/`
- `results/matrix/d_plain/run_20261020_143959_37a62875/`
- `results/matrix/d_lsm/run_20261020_153530_0577f49b/`
- `results/matrix/d_btree/run_20261020_162355_78081032/`
- `results/matrix/status.txt`, `results/matrix_driver.log`
- `results/anchor_check/run_20261020_171625_41c0133c/`, `results/anchor_check_driver.log`
- `.111 evidence/{source_commit.txt,engine_source.sha256,cpp_binaries.sha256,harness_tests.log,guard_tests.log,READY,smoke_audit.json,matrix_audit.json,anchor_investigation.json,anchor_confirmation.json,lsm_d_merge_counts.json,d_group.approved}`.
- `.247 evidence/{pg_build.log,pg_install.log,pg_binaries.sha256,installcheck.log,regression.status,baseline_metadata_before.txt,baseline_metadata_after_phase1.txt,baseline_controls_after_phase1.log,launch_gate.txt,d_review_gate.txt,wal_sampler.jsonl}`.

Small evidence copies are local under `agents/phase1_evidence/`; full authoritative run artifacts remain on the hosts. All SSH/SCP/rsync transports specify `ControlMaster=no`. New builds, logs, sockets, scratch and sampler artifacts use the authorized roots, with existing working-data files in the explicitly reused data root.

Exact matrix controller launch command, for a clean/no-controller state (auto-resume is enabled):

```bash
ssh -o ControlMaster=no neel@10.129.27.111 'tmux new-session -d -s ycsb_matrix "bash /home/neel/claude_ctl/ycsb_matrix_20261020/run_matrix.sh matrix >> /home/neel/claude_ctl/ycsb_matrix_20261020/results/matrix_driver.log 2>&1"'
```

The existing matrix is alive and continuing with workload C; the command above is for a future clean restart and appends to its driver log. The controller lock protects against duplicates. Phase 2 analysis/archive has not been performed.
