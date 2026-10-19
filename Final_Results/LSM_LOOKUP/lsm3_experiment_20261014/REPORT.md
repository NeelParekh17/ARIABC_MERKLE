# Final ff90 LSM lookup report

At fillfactor 90, lsm3 does not consistently move Merkle TPS closer to det: four points improve and four worsen, with -2.61% to +2.23% LSM/B-tree changes. Almost every update is HOT, so there is almost no lookup-index insertion work to remove. Compaction behaviour was not studied by user choice.

The controller finished72/72 at 2026-10-14 21:25:06Z (2026-10-15 02:55:06 IST). Independent audit:75 cases including smoke, 1.5M requests, 50/50 Merkle PASS. Nothing was committed or pushed.

## Full ff90 results

TPS = median [min–max], three trials; rounded here, full precision in CSV. Ratios divide matched mode medians. Gap closed=(LSM-Btree)/(det-Btree).

| θ | Workers | det TPS | B-tree TPS | LSM TPS | B-tree/det | LSM/det | Gap closed |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1,402 [1,385–1,474] | 1,321 [1,310–1,376] | 1,309 [1,302–1,383] | 0.9427 | 0.9339 | -15.4% |
| 0 | 4 | 2,777 [2,740–2,824] | 2,586 [2,568–2,680] | 2,644 [2,641–2,719] | 0.9311 | 0.9519 | 30.2% |
| 0 | 8 | 3,723 [3,717–3,865] | 3,563 [3,523–3,731] | 3,580 [3,576–3,750] | 0.9569 | 0.9615 | 10.7% |
| 0 | 16 | 4,908 [4,891–5,085] | 4,918 [4,619–4,922] | 4,867 [4,466–4,943] | 1.0020 | 0.9917 | 520.7%† |
| 0.99 | 1 | 2,025 [1,934–2,026] | 1,845 [1,772–1,855] | 1,797 [1,747–1,859] | 0.9113 | 0.8875 | -26.8% |
| 0.99 | 4 | 3,557 [3,527–3,700] | 3,314 [3,309–3,330] | 3,317 [3,294–3,326] | 0.9316 | 0.9325 | 1.4% |
| 0.99 | 8 | 5,024 [5,013–5,206] | 4,593 [4,587–4,757] | 4,615 [4,611–4,795] | 0.9143 | 0.9186 | 4.9% |
| 0.99 | 16 | 7,158 [6,847–7,184] | 6,291 [6,196–6,519] | 6,215 [6,053–6,553] | 0.8789 | 0.8682 | -8.8% |

† No positive baseline gap at θ0/w16: B-tree already exceeds det by9.65 TPS. Its raw520.7% formula is not an improvement claim.

## Exact HOT and lookup activity

Counts sum all three trials. No workload-A usertable INSERT statements occurred; lookup inserts are inferred non-HOT updates. There are zero merges in every individual LSM run.

| θ | Workers | det HOT/updates | B-tree HOT/updates | LSM HOT/updates | Lookup inserts B/LSM | LSM merges requested/completed |
|---|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 29,949/29,949 | 29,949/29,949 | 29,949/29,949 | 0/0 | 0/0 |
| 0 | 4 | 29,949/29,949 | 29,949/29,949 | 29,949/29,949 | 0/0 | 0/0 |
| 0 | 8 | 29,949/29,949 | 29,949/29,949 | 29,949/29,949 | 0/0 | 0/0 |
| 0 | 16 | 29,949/29,949 | 29,949/29,949 | 29,949/29,949 | 0/0 | 0/0 |
| 0.99 | 1 | 30,024/30,024 | 30,024/30,024 | 30,024/30,024 | 0/0 | 0/0 |
| 0.99 | 4 | 30,012/30,024 | 30,013/30,024 | 30,013/30,024 | 11/11 | 0/0 |
| 0.99 | 8 | 30,007/30,024 | 30,008/30,024 | 30,009/30,024 | 16/15 | 0/0 |
| 0.99 | 16 | 29,986/30,024 | 29,981/30,024 | 29,979/30,024 | 43/45 | 0/0 |

Both LSM tops end at 8 KiB for θ0 and θ0.99/w1; for θ0.99/w4,w8,w16 they end at 16 KiB/8 KiB in every trial. The1 MiB threshold is not approached. See hot_lookup_by_point.csv and results.csv for per-trial values.

## Choice and correctness

Postgres Pro lsm3 at 3bac10420e7738ddbd78e0eea7e3967afdd35aac, three B-trees (base plus alternating tops). Fork 13devel compatibility and four upstream defect fixes are documented in LSM_CHOICE.md/PATCH_NOTES.md and uncommitted patches. No PG engine algorithm change. All modes preload the same final extension.

Small split/merge/range tests and an independent additive root oracle passed;150K scalar inserts triggered two automatic merges. The actual det server accepted143K paired INSERT/update/delete/key-change commands with strict row tags, one automatic composite merge, matching expected rows and independent roots.100M split and repair-transfer query plans use the identical expression index in both modes and return the same range fingerprint. The final DROP-only fix has focused missing/existing DROP coverage. See CORRECTNESS.md for exact evidence and boundaries.

## Baseline and OOM method

All three normalized own baselines contain 100M rows at fillfactor 90. Heap28,248,276,992 B; primary 2,246,197,248 B. Lookup B-tree3,154,264,064 B; LSM base same plus8,192 B per empty top. Node heap 24,739,840 B; node indexes 6,676,480 B+7,004,160 B+16,384 B; Merkle metapage8,192 B. Geometry200/32/1024/256, format 11, equal verified baseline roots. Data was copied from read-only published sources, then rebuilt with AdHash binaries; normalized visibility matches all modes. See BASELINES.md.

RAM about 15 GiB, shared_buffers 32 MiB, cold cache drop, rsync --inplace delta reset with settle probes, 20K statements, 96 terminals/client workers, SERIALIZABLE. Both-host1Hz telemetry, native PG counters and decoded WAL/FPI evidence are retained. Per-case device read KiB/statement ranges across the grid: det7.50–16.69, B-tree9.06–18.24, LSM9.05–18.25 (matched median ranges). Final free space remains above40 GiB. No ff100 baseline was built; dedup_baselines.py was never run.

## Smoke versus publication

Aθ0/w8: det3703.02 TPS versus published median 3881.23 (-4.59%); B-tree3747.42 versus AdHash3723.70 (+0.64%); LSM3527.96, with no historical LSM reference. Both controls pass±10%. Smoke is excluded from grid medians. See evidence/SMOKE_COMPARISON.md.

## Interpretation

Replacing the lookup B-tree with lsm3 did not consistently bring Merkle TPS closer to det at ff90. Four points have higher LSM medians and four have lower medians; LSM/B-tree changes range from -2.61% to +2.23%. Their observed trial min–max ranges overlap at every point. This is a three-trial comparison, not a formal equivalence test or a causal estimate of an index-write benefit.

All θ0 updates were HOT: every mode/point had 29,949 HOT updates out of 29,949 across its three trials, and neither Merkle lookup received inferred inserts. At θ0.99, lookup inserts summed across three trials were B-tree/LSM: w1=0/0, w4=11/11, w8=16/15, w16=43/45. Across all 24 grid cases per Merkle mode, that is only 70 B-tree and 71 LSM lookup insertions out of 239,892 updates. The proposed random-leaf write bottleneck is almost absent in this ff90 window. Lookup inserts are inferred from usertable inserts plus non-HOT updates; they are not direct AM callback counts.

At θ0/w8, median lookup buffer reads/hits are B-tree 1/7 and LSM 0/0. Merkle node heap/index buffer reads remain 8,443/3,845 versus 8,448/3,853, with about 81.6K/86.2K node heap/index hits in either mode. WAL is 5,035.018 versus 5,035.138 bytes/statement, and FPIs are 0.64645 versus 0.64650 per statement. det uses 3,848.530 bytes and 0.49805 FPIs per statement. Device read/write KiB per statement are det 16.487/7.167, B-tree 18.235/8.444, LSM 18.232/8.434. The remaining Merkle traffic is essentially unchanged by replacing the nearly idle lookup index.

At θ0.99/w16, median lookup reads/hits fall from 40/31 to 5/18, but node heap/index reads are 5,608/2,103 versus 5,609/2,102. WAL falls only from 3,226.639 to 3,222.380 bytes/statement (0.13%), FPIs from 0.39995 to 0.39940, and device reads from 9.0714 to 9.0526 KiB/statement (0.21%). LSM median TPS is nevertheless 1.21% lower. At θ0/w4, the arithmetic gap closed is 30.2%, despite zero inferred lookup inserts and no reduction in WAL or device reads. These counters do not support attributing the small, mixed TPS differences to avoided random lookup-index writes.

No LSM grid run requested or completed a merge: 0 in every one of the 24 runs, also 0 in smoke. For θ0 and θ0.99/w1, both tops end at 8,192 bytes in every trial. For θ0.99/w4,w8,w16, top0 ends at 16,384 bytes and top1 at 8,192 bytes in every trial. The nominal 1 MiB threshold and the 65,536-insert check period were never exercised by this short ff90 workload. Compaction behaviour was not studied by user choice; ff100 and sustained runs were cancelled. The small correctness tests exercised automatic merges, which is a separate claim from sustained 100M performance.

At θ0/w16, B-tree is already 9.65 TPS faster than det. The raw formula yields 520.7% because its denominator is negative and tiny; there is no positive det–B-tree gap to close. This value must not be interpreted as an improvement. The full-precision raw formula is retained in ratios.csv.

## Acceptance, variability and bounds

The final dataset is exactly smoke 3 plus ff90 72: 75 accepted attempts, 1,500,000 validated request/row results, gateway exits 0, divergence 0, permanent failures 0, and 50/50 full Merkle verifications. No final-build grid failure, noise exclusion or noise rerun occurred. The three earlier failed smoke campaigns remain under results/failed_attempts, including one completed B-tree case on the previous module that is excluded from the final same-build dataset.

Across the 24 grid configurations, TPS CV ranges from 0.33% to 5.38%. No severe-stall rule fired: maximum sampler gap is below 1.001s, database full-memory PSI is 0%, and database full-I/O PSI averages span 15.59–39.17% across accepted cases. Normal synchronous-I/O waiting remains visible. The rule excludes sampler gaps>=10s, full-I/O PSI>=95% or full-memory PSI>=50% over>=10s, and telemetry errors uniformly across modes; shorter fluctuations remain in all trials.

The heap is 28.25 GB, larger than the approximately15 GiB RAM host; shared_buffers is 32 MiB and each case drops OS caches after restore/validation/device settling. This is database-larger-than-memory cold-start evidence. It does not show that the short workload's working set exceeds RAM. PostgreSQL buffer reads are shared-buffer misses and may hit the OS cache; they are not device reads. Device counters include other traffic and sampler/launch time around the gateway interval; checkpoint/drain/restore costs are retained separately.

All modes use the same PostgreSQL/server/gateway/module hashes and harness source contract, SERIALIZABLE, 96 terminals/96 client workers, 200 partitions/fanout 32/split 1024/merge256, and identical published workloads. All modes preload lsm3. The equally vacuumed/analysed baselines have matching all-visible heap state; that normalization differs from the historical published controls and is disclosed. Smoke met the±10% drift gate. Small tests do not establish arbitrary concurrent SQL, extension crash safety or end-to-end replica repair.

## Evidence and reproduction

- Analysis: analyze.py → results.csv, attempts.csv, medians.csv, ratios.csv, hot_lookup_by_point.csv, RESULTS.md, tps_and_ratios.{png,pdf}.
- Audit: audit.py → final_audit.json, AUDIT.md; analysis_status.json confirms smoke 3/ff90 72 complete.
- Correctness and builds: CORRECTNESS.md; small_final_evidence/, small_final_client/, small_evidence/repair_copy.json; pg_evidence/, cpp_evidence/; build_pg.sh/build_cpp.sh.
- Baselines: baseline_evidence/baselines_normalized/, final_baseline_identities.json, BASELINES.md.
- Patches: harness.patch, harness_added_files.patch, lsm3_compatibility.patch, lsm3_full.patch; hashes in evidence/source_revision.json. All remain uncommitted in the clone.
- Local complete case evidence: agents/lsm/results/{smoke,ff90,failed_attempts}/ under /work/ARIABC/AriaBC/.bench_tmp/lsm_lookup_20261014/.
- Authoritative remote results: .111 /home/neel/claude_ctl/lsm_lookup_20261014/results/{smoke,ff90,failed_attempts}/.
- .247 build/test evidence: /home/neel/claude_checks/lsm_lookup_20261014/; baselines/working data: /home/neel/ariabc_data/lsm_lookup_20261014/.
- Status/history/resume details: MONITOR.md, STATE.md; results/status.txt, status_ff100.txt, supervision.jsonl and progress.json. The healthy controller was not restarted after either resumption. The workstation reboot did not stop the detached campaign.

Normalized baselines passed at 13:15:18Z and initial smoke started13:16:01Z before the usage-limit interruption. Three setup fixes and preserved smoke failures preceded the final same-build smoke at 14:59Z. The final grid ran15:36:33–21:25:06Z. ff100 and sustained compaction were cancelled at the user's request; no result for them is implied.
