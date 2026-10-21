# Phase 1b: slowdown diagnosis and completed-case audit

Artifact audit at **2026-10-20 19:04:06 UTC**; sampler cutoff **2026-10-20 19:04:03 UTC**. Main matrix: **52/80 accepted cases**, D/C/B 16 each, A 4. F has not started. The matrix remains running in tmux `ycsb_matrix` on .111. All times below are UTC; add 05:30 for IST. Mode names: PG, deterministic plain (DET), Merkle+LSM (LSM), Merkle+B-tree (BTREE).

## Verdict

**A is substantially more expensive because it updates existing random/hot rows in 50% of statements. Its added WAL, dirty-page eviction/writeback, restore volume, and hotspot serialization retries explain the measured slowdown. The device is under more pressure during A; the evidence does not establish permanent SSD degradation or QLC SLC-cache exhaustion.** No active disk test, SQL query, restart, benchmark rerun, large-file copy from .247, controller edit, or harness edit was performed for this diagnosis. Only existing artifacts/log tails, saved telemetry/sampler records, small baseline control hashes, and read-only process/device/memory counters were inspected. A small streaming sampler reader ran once per snapshot at nice 19; full telemetry analysis ran on .111, not the 15-GB DB host.

A/0.5 versus B/0.5 holds reads almost constant but multiplies device writes by 7.12–7.23 and WAL by 8.10. A DET takes 865.770 s versus 240.482 s (3.60×); PG takes 698.130 s versus 234.640 s (2.98×). Read await increases from 0.710→1.654 ms (DET) and 1.240→2.040 ms (PG), while write await rises 1.204→4.221 and 2.047→4.370 ms. This is mixed random-read/write pressure, not a measurement of the drive’s sequential bandwidth ceiling. A’s PG reset after the DET/0.5 case writes 20,397.7 MiB and takes 685.993 s, so looking only at campaign wall exaggerates the TPS change.

The hotspot case has a different bottleneck: A/1.2/PG performs 938,038 SQLSTATE 40001 retries (0.391 attempts per successful statement), versus 23,079 for B/1.2/PG. Requested backoff is 4,707.422 aggregate worker-seconds; this overlaps across workers and must not be added to elapsed wall. A/1.2/PG is 4,339.7 TPS / 553.038 s, versus B’s 38,507.2 TPS / 62.326 s. DET avoids these retries and reaches 5,916.6 TPS / 405.637 s. Low-skew A cases have zero retries yet remain slow, independently demonstrating the write/I/O cost. All completed A cases so far are plain; no observed A slowdown can yet be attributed to Merkle maintenance.

## Same-mode comparisons

| Case | B TPS → A TPS | Run-time ratio A/B | Read MiB B → A | Write MiB B → A | WAL B/stmt B → A | Read await ms B → A | Write await ms B → A |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A/1.2/DET | 21907.8 → 5916.6 | 3.70× | 1292 → 1360 | 1737 → 6426 | 50.6 → 368.0 | 0.309 → 0.665 | 0.330 → 0.569 |
| A/1.2/PG | 38507.2 → 4339.7 | 8.87× | 1289 → 1538 | 1429 → 5802 | 50.4 → 379.5 | 0.483 → 0.995 | 0.431 → 1.116 |
| A/0.5/DET | 9980.0 → 2772.1 | 3.60× | 19814 → 20504 | 3629 → 25835 | 370.4 → 3000.7 | 0.710 → 1.654 | 1.204 → 4.221 |
| A/0.5/PG | 10228.4 → 3437.8 | 2.98× | 19840 → 20333 | 3514 → 25418 | 370.0 → 2996.7 | 1.240 → 2.040 | 2.047 → 4.370 |

## Disk, memory, and overnight trend

The settled probe medians do **not** show a persistent upward step across the night. The following are medians across accepted cases (A is only four plain cases, so workload-wide values are not matched comparisons). These are the harness’s already-recorded probes; no probe was invoked by this diagnostic.

| Workload | Cases | Settle QD1/QD8/write µs | Settle wait median (range), s | Read await median (range), ms | Write await median (range), ms | Min available RAM, MiB | Max dirty, MiB | Swap-in/out pages |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| D | 16 | 103.0/241.9/389.6 | 60 (60–100) | 0.555 (0.311–0.890) | 0.525 (0.303–1.913) | 6728 | 242 | 29/0 |
| C | 16 | 102.9/242.3/390.0 | 60 (50–70) | 0.332 (0.166–0.536) | 2.830 (2.428–3.303) | 6753 | 300 | 11/0 |
| B | 16 | 103.4/243.8/389.5 | 60 (40–150) | 0.746 (0.306–1.240) | 1.142 (0.325–2.445) | 6491 | 215 | 59/0 |
| A | 4 | 117.8/243.9/389.9 | 85 (40–100) | 1.325 (0.665–2.040) | 2.668 (0.569–4.370) | 6557 | 643 | 54/0 |

A’s QD1 median is modestly higher, but QD8 remains ~244 µs and synchronous-write median ~390 µs, comparable to earlier groups. All 52 settle checks accepted. Saved campaign calibration idle medians:

| Campaign | Start | Idle QD1 µs | Idle QD8 µs | Idle write µs |
| --- | --- | --- | --- | --- |
| d_plain | 09:09:59 | 127.3 | 246.3 | 390.6 |
| d_lsm | 10:05:30 | 114.6 | 243.5 | 391.5 |
| d_btree | 10:53:55 | 126.7 | 245.7 | 390.2 |
| c_plain | 12:07:06 | 117.2 | 244.1 | 390.7 |
| c_lsm | 12:57:47 | 112.7 | 246.3 | 389.1 |
| c_btree | 13:38:32 | 135.2 | 245.2 | 390.4 |
| b_plain | 14:18:39 | 127.0 | 248.3 | 390.4 |
| b_lsm | 15:32:22 | 112.0 | 245.8 | 391.0 |
| b_btree | 16:26:48 | 126.7 | 245.9 | 389.0 |
| a_plain | 17:26:30 | 123.6 | 247.2 | 389.9 |

Later matched observations contradict a monotonic, irreversible device slowdown: B/0/BTREE, at 17:13, reads at 0.790 ms and 8,519 TPS, better than B/0/LSM at 16:13 (0.912 ms, 7,520 TPS). B/1.2/BTREE at 17:03 has 0.306 ms read await versus 0.437 ms in the earlier LSM case. C/0.99 LSM and BTREE, 41 minutes apart, deliver 27,217 and 27,211 TPS with 0.210/0.209 ms read await. Different AMs are not perfect controlled repeats, but there is no one-way worsening in these adjacent comparable observations.

Available memory never falls below 6,491 MiB in the saved measurement telemetry. Across all 52 windows: zero swap-out pages, zero OOM kills, and only 153 swap-in pages (~0.60 MiB at 4-KiB pages). Roughly 440 MiB of already allocated swap in the live host snapshot is not evidence of active swapping. Cold OS-cache drops are recorded for each case; 32MB shared_buffers does not bound Linux page cache or the engine’s shared memory. A/0.5 DET/PG dirty peaks are 520/643 MiB, compared with B/0.5’s 164/196 MiB; their memory full-pressure time remains ~0.030%, while I/O full-pressure time is 37.43%/49.13%. This indicates I/O waiting, not a RAM/swap collapse. No requested checkpoint is observed inside the run via the saved checkpoints_req deltas (all zero). Timed checkpoint counters are not in these snapshots, so this is not a universal checkpoint absence claim. A’s explicit post-run checkpoints take 0.31–9.36 s, not hundreds of seconds.

QLC cache exhaustion, garbage collection, firmware thermal throttling, and media aging cannot be separately identified from these counters. Mixed-I/O await and cyclic throughput could include drive-internal effects, but the workload and incoming restore bytes also change. There is no saved matched sustained-write control or drive-internal cache/temperature history that isolates them. If needed, propose a matched repeated read/write experiment with thermal/SMART history **after the matrix**, on an approved disposable target. Do not run it between cases: restore/settle is active then.

## Within-run A progress

Sampler commit rates below use complete 60-second bins from the measurement marker, which starts shortly before gateway timing. The final partial active bin and post-gateway log/collector tail are excluded when comparing early and late bins. These are PostgreSQL commit-counter rates, including a few monitoring commits per second, not a replacement for validated gateway TPS. They have coarse alignment and publication lag; WAL rates and device counters corroborate the changes.

| Case | Full-minute commit/s sequence | First 3 → last 3 means | Change |
| --- | --- | --- | --- |
| A/1.2/DET | 7408, 6168, 4651, 4540, 6874, 7116 | 6076 → 6177 | +1.7% |
| A/1.2/PG | 5555, 4690, 3276, 5572, 3696, 3195, 5150, 4182, 3247 | 4507 → 4193 | -7.0% |
| A/0.5/DET | 5279, 1233, 4067, 1493, 3883, 2080, 3288, 1761, 3293, 2085, 3224, 1810, 3100, 2188 | 3526 → 2366 | -32.9% |
| A/0.5/PG | 3991, 3424, 3350, 2902, 3996, 3029, 3051, 4545, 3633, 2833, 3660 | 3588 → 3375 | -5.9% |

DET/0.5 alternates between fast and slow intervals, rather than steadily falling every minute: its second minute has 4.28 ms read / 5.16 ms write await and 1,233 commits/s, later recovering to ~3,100–4,067 commits/s. Its slow write-await minutes reach 8.67 ms; dirty pages peak ~520 MiB. PG/0.5 fluctuates 2,833–4,545 commits/s in full bins; its first-three versus last-three mean changes only about −6%. PG/1.2 also recovers after slow intervals. Thus there is real in-run variability and some DET/0.5 drift, but no sustained terminal throughput collapse common to A’s runs. WAL bytes/s may also decline because repeated writes can need fewer first-touch full-page images (inference; WAL record types were not decoded); WAL rate alone is not a transaction-rate proxy.

**Live A/0.99/PG**: setup marker 18:50:47, measurement marker 19:02:01 (674 s setup-to-measurement). At 19:04:03 UTC it remains in measurement: 977,966 cumulative PostgreSQL commits and a bounded server tail containing request ID 989182 (of 2,400,000; approximate, not a validation counter). From 19:03:09 to 19:04:03, sampler commits advance at 8,190/s. Device interval: 21.1/27.9 MiB/s read/write, 1.214/2.497 ms await, 2501/3019 IOPS; 300.7 MiB dirty, 0.0 MiB writeback, 6573 MiB available. Existing full-minute commit-rate bins: 8620, 7397. This is a short current view, not a final TPS. No measurement sampler errors are present.

The gateway is captured by the harness with subprocess capture_output; gateway.log is written when the process returns. Therefore there is no durable live gateway-progress file during execution. Reading its pipe would consume output and disturb validation, so this diagnostic uses the existing sampler and a bounded server-log tail instead. Completed gateway logs prove terminal counts/TPS. A request ID in a live server tail is an approximate progress indication, not a validated completion count.

## Campaign time versus measured run time

Reset includes stopping/restoring the working copy, checks, the recorded settle wait, cache drop, validation/startup, and server start. It is not pure rsync time. Fixed setup includes workload generation, baseline/preflight checks and device calibration. Post-run time includes log copy/validation, collector settling, drain/checkpoint, PostgreSQL restart, Merkle verification, and cleanup. Per-case cycle boundaries come from sampler setup transitions and campaign END timestamps (~1 s resolution). Phase markers do not isolate every substep; unknown residuals are explicitly retained rather than called disk time.

| Group | Cases | Campaign elapsed min | Fixed setup min | Gateway min | Reset min | Settle within reset min | Post-run min | Drain within post s | Checkpoint within post s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| D | 16 | 155.62 | 20.63 | 44.92 | 31.66 | 17.83 | 58.33 | 57.70 | 14.90 |
| C | 16 | 131.55 | 20.67 | 27.33 | 25.48 | 16.17 | 58.02 | 1.34 | 3.95 |
| B | 16 | 187.83 | 20.76 | 53.01 | 55.86 | 20.50 | 58.15 | 2.85 | 10.51 |
| A | 4 | 84.27 | 10.14 | 42.04 | 24.12 | 5.17 | 7.96 | 0.00 | 16.33 |

D/C/B campaign durations are the controller’s actual END elapsed_s totals; the fixed+case decomposition differs by only a few seconds due to marker granularity/dispatch. A is a completed-case prefix through the next setup marker, not the full in-progress campaign. Average completed A cycle is 1,112 s versus B’s 626 s, but its mean gateway duration is 631 s versus B’s 199 s. A’s first four cases include different skews/modes from a full group; matched comparisons above are the meaningful performance ratios. A fixed setup is 608.4 s (300.3 s calibration), versus B plain’s 414.6 s (300.3 s calibration); the remaining generation/preparation work also grows with longer update SQL.

## Updated A and F projection

| Milestone | Central UTC | Central IST | Planning range UTC |
| --- | --- | --- | --- |
| A plain complete | 2026-10-20 20:22:52 UTC | 2026-10-21 01:52 | 2026-10-20 20:00–21:00 |
| All A complete | 2026-10-21 00:23:46 UTC | 2026-10-21 05:53 | 2026-10-20 23:00 to 2026-10-21 03:00 |
| F / full matrix complete | 2026-10-21 08:23:38 UTC | 2026-10-21 13:53 | 2026-10-21 05:00–13:00 |

These are scenario estimates, not statistical intervals or a guarantee. Model: A/DET is 3.65× B gateway time (observed 3.60–3.70×); A/PG uses 2.98× at uniform and the geometric mean of 2.98×/8.87× at skew .99. Four remaining plain resets budget 2,300 s, following the 20-GiB incoming delta (~700 s), then ~450/450/700 s; plain post-run uses the observed 119.5 s per case. Each future A Merkle campaign budgets 2,200 s reset (~800/450/700/250 s, including first content check), 608 s fixed setup, B’s measured post-run total, and B’s same-AM gateway total ×3.65. This yields about 6.95 h for all A. F has the same 50% update mix, but its source uses one materialized SELECT CTE + UPDATE RETURNING per RMW statement, so budget 1.15× A (~8.00 h); the wide range covers unmeasured RMW contention/CPU and future restore/drive behavior. No A Merkle or F case has yet calibrated this extrapolation. Projection assumes no failure/rerun, no new controller hold, and gateway <3,600 s per case; exceeded timeouts or a new sustained device slowdown would move it later. The old D-only ETA is obsolete.

## Acceptance audit

**PASS for the completed snapshot: 52 cases × 2,400,000 = 124,800,000 validated statements.** Every accepted result agrees on SERIALIZABLE, 16 workers, one trial, 32MB shared_buffers; setup SHOW values independently show transaction_isolation/default_transaction_isolation=serializable and shared_buffers=4096 8-KiB blocks. Gateway direct-terminal success, signatures signed/verified, and validated request counts are 2,400,000 in every case; signature mismatches, exhausted retries, permanent failures, deterministic/nonterminal errors and divergence are zero. No FAILED.txt or cleanup_errors.txt exists in a main matrix case. No accepted configuration key is duplicated. These are the single-node direct-completion semantics used by the task, not a multi-replica consistency proof.

| Group | Cases | Validated statements/case | Config + failures | Merkle verify t | Retry attempts | Valid WAL samples | Measurement sampler errors | WAL brackets |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| D plain | 8 | all 2,400,000 | all PASS | N/A | 0 | 1638 | 0 | present; consistent |
| D lsm | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 925 | 0 | present; consistent |
| D btree | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 1217 | 0 | present; consistent |
| C plain | 8 | all 2,400,000 | all PASS | N/A | 0 | 1303 | 0 | present; consistent |
| C lsm | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 710 | 0 | present; consistent |
| C btree | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 713 | 0 | present; consistent |
| B plain | 8 | all 2,400,000 | all PASS | N/A | 24391 | 1946 | 0 | present; consistent |
| B lsm | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 1165 | 0 | present; consistent |
| B btree | 4 | all 2,400,000 | all PASS | 4/4 | 0 | 1133 | 0 | present; consistent |
| A plain | 4 | all 2,400,000 | all PASS | N/A | 938038 | 2739 | 0 | present; consistent |

All 24 completed Merkle cases have saved merkle_verify.txt exactly `t`, result PASS, recovery state READY and lag_items=0. All 12 LSM cases have drain elapsed time, non-null sampler merge state, and final saved state idle/flush=false. Zero merges in read-only C or in low-skew B is recorded behavior, not a missing sample. Merge values are per-index lifetime counters bounded by PostgreSQL restarts; the table below reports each case’s maximum observed counters, not sums across restarts.

There are 13,489 valid measurement WAL/commit samples (111–908 per case), zero measurement-phase sampler errors, and 0 malformed JSONL lines. Connection-refused samples in setup/verify are expected while PostgreSQL is stopped, including the live restore; they are not counted as measurement errors. Both WAL snapshots and the post-checkpoint LSN exist for every completed case, and the measurement before/after postmaster_start matches. C correctly has zero WAL bytes despite valid samples. Device writes during C include process/result logging and other partition traffic; device counters are not PostgreSQL-only.

### Workload SHA parity and baseline integrity

| Workload | Skew | Completed modes | SHA256 | Parity |
| --- | --- | --- | --- | --- |
| D | 0.0 | 4 | e67005b7426cdb825b3f2c858edc337ec0069f1a61eecff87896c44899929a26 | 4/4 identical |
| D | 0.5 | 4 | e5323f203fc4dc09fc04bb39ee016f1382b09875753ffd1d3be6dd397b11ac07 | 4/4 identical |
| D | 0.99 | 4 | ae78761fed624edacea9e66ebf356df619b41179948b25007f314fe661d69415 | 4/4 identical |
| D | 1.2 | 4 | f6ab0c589c4e297b85fc82c844e3f34aa0073f4f07923dee115f32dc2e2e72b7 | 4/4 identical |
| C | 0.0 | 4 | 98aba6ae82ce9dfacac646e4a986950c95a6d85bb398f25f383da7ea36cd3c21 | 4/4 identical |
| C | 0.5 | 4 | 5ddfae9fbdfc23f34a8c36a0b9405af6639be68add8e959ec61fec11ac68385a | 4/4 identical |
| C | 0.99 | 4 | 9275307087756a2ef05fff33fe514d0d10be6b79931df9497e229f253d57fbe5 | 4/4 identical |
| C | 1.2 | 4 | a8576c0534769cda89b47cd70bcf49c06f471e16efc86230c8d123d35ae4ce1c | 4/4 identical |
| B | 0.0 | 4 | e9f9e53298d7404b74373f31b67c4e076e80d9ba2fea226faa040c62e1ce8f69 | 4/4 identical |
| B | 0.5 | 4 | 59ffee8a8e2b1d3946dc1659df94f6e4136e30b24a20f2fdd13edb6019a37225 | 4/4 identical |
| B | 0.99 | 4 | 5a5fd70496f8f427d525e667351918e799ef8decb390d6ac1ad43d4d9a94e56b | 4/4 identical |
| B | 1.2 | 4 | c31206610a7a00eeebdb780832a69dedd8f11a90c0d3ccc595225b29a8132cee | 4/4 identical |
| A | 0.5 | 2 | 4893899a85bbe56d13eca97bcde5614a669f0e45392dde81b2f2090612506bba | completed modes identical; other modes pending |
| A | 1.2 | 2 | 509d85b8788e509813492a1b6171fbaecdf1ea394ce96ad4c45c7d6dcddb2e85 | completed modes identical; other modes pending |

Each row’s SHA also matches its generated campaign workload manifest. D/C/B have complete 4-mode parity for all 12 combos. A has only two completed combos/two modes each at this cutoff, so four-mode A parity is pending, not proved. No A pending configuration or F is silently treated as accepted.

Per-case baseline manifests are identical within each baseline family, name the expected base, and retain 100,000,000 initial keys (1..100,000,000), the same physical heap/index geometry and baseline control hashes. All saved setup settle checks passed; harness checks the restored working copy’s names/sizes/mtimes/modes and PG_VERSION/control hashes every reset, with sampled full byte comparisons on the first restore per campaign. Nine completed campaign-start restores have CONTENT_CHECK=identical (plus A’s first restore); these checks are already saved, not rerun here. A current tiny-file read at 18:55:36 UTC found all four base global/pg_control SHA256s equal to the prelaunch record, including the unused old LSM base:

| Baseline | pg_control SHA256 |
| --- | --- |
| base_ff90_plain | aeaea1f0290772b3e154987af0383980735937a6136df3976e38df664e02a063 |
| base_ff90 | 4be7ab2ceeffd9a0b32d0656f8c9511732814075a1146b168c3721bb22af7797 |
| base_ff90_mlsm | 2237ba3ffc70264d6ebcb20734d5b07baeab9dc80ed5faba9f6311d980469161 |
| base_ff90_lsm (unused) | 14db14fd4dc3d1a46ea04395a1e6c74fb6ad79b68b293d812b5dcebdc2f21821 |

All four PG_VERSION hashes equal `1a252402972f6057fa53cc172b52b9ffca698e18311facd0f3b06ecaaef79e17`. This supports preserved baseline provenance and reset acceptance; it is **not a new full-content hash of all baseline files**. A fresh full scan during measurements would violate the no-extra-load instruction, so none was run. Saved executable provenance is identical across all 52 results: PostgreSQL `56cbb4d4…`, server `142bdca5…`, gateway `489658d1…`; full hashes are in audit.json/PHASE1_REPORT.md. No new binary scan was run on .247.

### Problems and qualifications requiring attention

| Finding | Evidence / impact | Action |
| --- | --- | --- |
| A hotspot retry cost | A/1.2/PG 938,038 serialization retries; B/1.2/PG 23,079; B/.99/PG 1,312. All others 0; all exhausted/permanent/divergence 0. | Keep these accepted runs; report retry/backoff cost. Do not change retry policy mid-matrix. |
| A low-skew write/backpressure and restore overhead | A/.5 write volume ~25 GiB; read/write await up to 2.040/4.370 ms; next restore ~20 GiB / 686 s. DET has cyclic minute rates and some late drift. | Retain timings separately; no disk test or cache-reset change during this matrix. |
| D/.99/BTREE anchor outlier | Main result 11,731 TPS, −10.3866% from 13,091 anchor; read await .53846 ms. Prior Phase-1 separate diagnostic repeat 13,176 TPS, +.6501%, .46165 ms, same SQL/build and PASS. | Keep original matrix result; label variability. Repeat was done before C, not during this diagnostic; never replace/cherry-pick it. |
| B/0/LSM vs BTREE variability | 7,520 vs 8,519 TPS (BTREE +13.3%); .912 vs .790 ms read await, same combo hash. Different AMs and one trial prevent isolating causality. | Flag in comparison; no rerun now. |
| Long first restore B/BTREE | 528.104 s, 8,148.7 MiB restore, including campaign first full byte check and 150-s settle. | Setup outlier, not a 528-s TPS penalty; retain incoming-copy provenance. |
| Deferred D/LSM WAL omitted by window-only view | D/LSM window ~61–62 B/stmt but through drain/checkpoint ~382–384 B/stmt; 8.55–31.04-s drain, [6,2] merges. | Use full WAL bracket for write-amplification comparisons; window and full both tabulated below. |
| No repeated-trial or media-cause identification | One trial/config; no matched A Merkle/F observation, sustained disk control, temperature/cache history. | Keep ETA and causal attribution bounded; propose any active media experiment only after matrix. |
| Partial matrix audit | D/C/B complete; A partial and F pending. Live snapshots can advance after report cutoff. | Continue the unchanged controller; audit later results separately. |

No completed case acceptance/correctness failure was found. The checker does not establish that every application result equals an external serial oracle; it checks the saved harness completion/validation evidence, plus Merkle verification. No controller or harness fix is justified during the run. For a later campaign, add non-consuming timestamped gateway progress and explicit restore/verify/cleanup durations; preserve comparable settings and record device temperature/health without altering this campaign.

## Time-ordered per-case performance

Rows are ordered by first sampler setup marker, not lexical skew order. Read/write values are MiB; await is ms per completed device I/O; IOPS and MiB/s divide device deltas by device_window_ms (gateway plus short telemetry overhead), not by campaign wall. Partition-wide counters include logging and unrelated traffic. The gateway duration alone is the TPS denominator.

| UTC setup | Case | TPS | Gateway s | Read/write MiB | Read/write await ms | Read/write IOPS | Read/write MiB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 09:16:52 | D/1.2/DET | 23640.9 | 101.519 | 1236.8/1435.0 | 0.311/0.303 | 1422/2119 | 11.8/13.7 |
| 09:23:42 | D/1.2/PG | 46659.9 | 51.436 | 1231.1/1049.8 | 0.484/0.387 | 2729/2467 | 22.7/19.3 |
| 09:28:07 | D/0.5/DET | 12316.0 | 194.868 | 19020.9/1683.8 | 0.547/0.482 | 11381/1240 | 96.2/8.5 |
| 09:34:58 | D/0.5/PG | 14589.3 | 164.504 | 18991.1/1499.5 | 0.890/0.675 | 13435/1267 | 113.4/9.0 |
| 09:41:18 | D/0.99/PG | 30914.7 | 77.633 | 5601.9/1111.6 | 0.759/0.561 | 8222/1700 | 69.5/13.8 |
| 09:46:12 | D/0.99/DET | 18530.1 | 129.519 | 5602.4/1527.2 | 0.369/0.349 | 4984/1699 | 42.2/11.5 |
| 09:51:57 | D/0/PG | 13747.2 | 174.581 | 20558.4/1467.8 | 0.878/0.669 | 13960/1177 | 115.8/8.3 |
| 09:58:27 | D/0/DET | 11720.4 | 204.772 | 20919.6/1516.7 | 0.571/0.491 | 12115/1118 | 100.4/7.3 |
| 10:12:22 | D/0.99/LSM | 17751.5 | 135.200 | 5639.9/2148.8 | 0.371/0.386 | 4805/2026 | 40.7/15.5 |
| 10:22:37 | D/0.5/LSM | 11897.6 | 201.722 | 19221.3/2408.8 | 0.539/0.511 | 11148/1508 | 94.0/11.8 |
| 10:33:08 | D/1.2/LSM | 22299.9 | 107.624 | 1261.8/2023.2 | 0.312/0.335 | 1371/2432 | 11.4/18.3 |
| 10:42:33 | D/0/LSM | 11432.8 | 209.923 | 20803.5/2281.4 | 0.563/0.540 | 11744/1394 | 97.5/10.7 |
| 11:00:51 | D/0.99/BTREE | 11731.3 | 204.581 | 6565.7/3669.3 | 0.538/1.913 | 3742/1866 | 31.5/17.6 |
| 11:12:33 | D/0.5/BTREE | 8728.9 | 274.948 | 20496.5/4057.8 | 0.653/1.512 | 8773/1541 | 73.8/14.6 |
| 11:23:55 | D/1.2/BTREE | 13423.1 | 178.796 | 2124.6/3569.4 | 0.583/1.204 | 1413/2049 | 11.7/19.7 |
| 11:33:52 | D/0/BTREE | 8464.6 | 283.534 | 22346.2/3990.4 | 0.666/1.472 | 9400/1463 | 77.8/13.9 |
| 12:14:40 | C/1.2/DET | 35305.5 | 67.978 | 1292.0/414.3 | 0.167/2.838 | 2211/68 | 18.3/5.9 |
| 12:20:55 | C/1.2/PG | 55050.9 | 43.596 | 1292.5/508.2 | 0.191/3.082 | 3378/113 | 27.9/11.0 |
| 12:25:13 | C/0.5/DET | 17402.8 | 137.909 | 19801.5/562.7 | 0.353/2.773 | 16651/50 | 140.4/4.0 |
| 12:31:08 | C/0.5/PG | 22237.5 | 107.926 | 19973.3/549.0 | 0.531/3.007 | 21399/59 | 180.5/5.0 |
| 12:36:34 | C/0.99/PG | 46737.2 | 51.351 | 5812.8/548.8 | 0.310/3.303 | 12705/105 | 107.6/10.2 |
| 12:41:02 | C/0.99/DET | 27457.4 | 87.408 | 5812.2/578.5 | 0.210/3.001 | 7619/71 | 64.5/6.4 |
| 12:46:06 | C/0/PG | 20674.7 | 116.084 | 21532.3/593.9 | 0.536/3.221 | 21875/59 | 181.4/5.0 |
| 12:51:41 | C/0/DET | 16370.9 | 146.602 | 21866.0/527.7 | 0.377/2.428 | 17686/47 | 146.6/3.5 |
| 13:04:21 | C/0.99/LSM | 27217.4 | 88.179 | 5826.6/576.2 | 0.210/2.824 | 7560/71 | 64.1/6.3 |
| 13:14:24 | C/0.5/LSM | 17326.5 | 138.516 | 19774.7/559.6 | 0.354/2.798 | 16610/50 | 140.2/4.0 |
| 13:22:49 | C/1.2/LSM | 35560.8 | 67.490 | 1292.3/419.7 | 0.167/2.836 | 2231/69 | 18.4/6.0 |
| 13:30:00 | C/0/LSM | 16537.0 | 145.129 | 21479.0/532.3 | 0.374/2.737 | 17532/47 | 145.4/3.6 |
| 13:45:06 | C/0.99/BTREE | 27210.9 | 88.200 | 5810.0/572.8 | 0.209/2.862 | 7548/71 | 63.9/6.3 |
| 13:54:42 | C/0.5/BTREE | 17127.4 | 140.126 | 20330.5/558.2 | 0.357/2.677 | 16690/50 | 142.5/3.9 |
| 14:02:58 | C/1.2/BTREE | 35558.2 | 67.495 | 1293.1/415.7 | 0.166/2.541 | 2232/69 | 18.5/5.9 |
| 14:10:08 | C/0/BTREE | 16428.9 | 146.084 | 21640.7/527.9 | 0.375/2.634 | 17547/47 | 145.6/3.6 |
| 14:25:35 | B/1.2/DET | 21907.8 | 109.550 | 1291.6/1736.6 | 0.309/0.330 | 1386/2247 | 11.5/15.4 |
| 14:32:26 | B/1.2/PG | 38507.2 | 62.326 | 1289.1/1428.7 | 0.483/0.431 | 2386/2540 | 19.8/21.9 |
| 14:38:12 | B/0.5/DET | 9980.0 | 240.482 | 19814.2/3629.4 | 0.710/1.204 | 9634/1632 | 81.4/14.9 |
| 14:47:02 | B/0.5/PG | 10228.4 | 234.640 | 19840.5/3514.3 | 1.240/2.047 | 9887/1547 | 83.6/14.8 |
| 14:57:21 | B/0.99/PG | 29361.4 | 81.740 | 5805.3/1941.1 | 0.783/0.673 | 8134/2380 | 68.7/23.0 |
| 15:04:11 | B/0.99/DET | 14919.2 | 160.867 | 5828.1/2544.8 | 0.499/1.080 | 4203/1958 | 35.6/15.6 |
| 15:11:55 | B/0/PG | 9792.0 | 245.099 | 21560.3/3545.3 | 1.209/1.983 | 10474/1506 | 87.0/14.3 |
| 15:21:05 | B/0/DET | 8670.1 | 276.813 | 22051.4/3629.5 | 0.814/1.577 | 9513/1413 | 78.9/13.0 |
| 15:39:18 | B/0.99/LSM | 15987.0 | 150.122 | 5846.7/2801.6 | 0.430/0.617 | 4516/2149 | 38.2/18.3 |
| 15:50:34 | B/0.5/LSM | 7786.5 | 308.225 | 20952.3/4182.4 | 0.886/2.070 | 7888/1423 | 67.3/13.4 |
| 16:03:25 | B/1.2/LSM | 19431.3 | 123.512 | 1332.3/2068.0 | 0.437/0.356 | 1260/2244 | 10.6/16.4 |
| 16:13:56 | B/0/LSM | 7519.6 | 319.166 | 22288.8/4344.7 | 0.912/2.445 | 8344/1413 | 69.2/13.5 |
| 16:33:45 | B/0.99/BTREE | 14789.7 | 162.275 | 5847.9/2760.5 | 0.473/0.774 | 4187/1994 | 35.4/16.7 |
| 16:50:05 | B/0.5/BTREE | 7782.1 | 308.402 | 20754.6/4225.1 | 0.879/2.168 | 7812/1425 | 66.7/13.6 |
| 17:03:06 | B/1.2/BTREE | 20774.4 | 115.527 | 1332.5/2060.9 | 0.306/0.325 | 1346/2400 | 11.3/17.4 |
| 17:13:58 | B/0/BTREE | 8519.0 | 281.722 | 22042.9/4216.8 | 0.790/1.691 | 9342/1565 | 77.5/14.8 |
| 17:36:39 | A/1.2/DET | 5916.6 | 405.637 | 1359.6/6426.5 | 0.665/0.569 | 382/2857 | 3.3/15.6 |
| 17:50:51 | A/1.2/PG | 4339.7 | 553.038 | 1538.1/5801.7 | 0.995/1.116 | 285/1589 | 2.8/10.4 |
| 18:06:34 | A/0.5/DET | 2772.1 | 865.770 | 20504.4/25834.8 | 1.654/4.221 | 2787/2443 | 23.5/29.7 |
| 18:25:58 | A/0.5/PG | 3437.8 | 698.130 | 20332.7/25418.0 | 2.040/4.370 | 3419/2832 | 28.9/36.1 |

## Time-ordered reset, settle and overhead

Reset excludes neither settle nor validation/startup. Reset−settle is the remaining setup bundle, not an exact restore timer. Post is full cycle−reset−gateway; CP/drain are components of post. Verify+cleanup is marker-based and includes restart/verification/cleanup; cleanup alone uses result.json mtime to the next setup/campaign END. Exact substep timers not saved by the harness cannot be reconstructed more finely.

| UTC | Case | Reset s | Reset−settle s | Restore MiB | Settle s | QD1/QD8/write µs | Post s | Verify+cleanup s | Cleanup s | Checkpoint s | Cycle s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 09:16:52 | D/1.2/DET | 165.5 | 105.5 | 11.0 | 60 | 102.2/240.2/390.0 | 143.7 | 68.0 | 56.9 | 0.25 | 410.7 |
| 09:23:42 | D/1.2/PG | 75.6 | 15.6 | 52.0 | 60 | 104.5/241.0/387.9 | 138.1 | 67.0 | 56.0 | 0.24 | 265.1 |
| 09:28:07 | D/0.5/DET | 75.5 | 15.5 | 51.3 | 60 | 102.9/240.4/389.2 | 139.7 | 68.0 | 56.1 | 0.45 | 410.1 |
| 09:34:58 | D/0.5/PG | 76.7 | 16.7 | 52.1 | 60 | 103.1/242.8/386.9 | 138.9 | 68.0 | 56.3 | 0.35 | 380.1 |
| 09:41:18 | D/0.99/PG | 76.9 | 16.9 | 51.0 | 60 | 140.9/243.1/388.1 | 139.6 | 69.0 | 56.9 | 0.24 | 294.1 |
| 09:46:12 | D/0.99/DET | 76.4 | 16.4 | 51.0 | 60 | 105.4/242.6/389.0 | 139.1 | 68.0 | 56.5 | 0.25 | 345.1 |
| 09:51:57 | D/0/PG | 76.4 | 16.4 | 51.7 | 60 | 103.1/241.1/387.7 | 139.1 | 68.0 | 56.9 | 0.25 | 390.1 |
| 09:58:27 | D/0/DET | 76.8 | 16.8 | 51.2 | 60 | 103.4/240.9/388.1 | 141.0 | 66.5 | 55.4 | 0.24 | 422.6 |
| 10:12:22 | D/0.99/LSM | 181.3 | 121.3 | 3061.8 | 60 | 97.9/243.3/390.8 | 298.6 | 217.0 | 57.4 | 0.54 | 615.1 |
| 10:22:37 | D/0.5/LSM | 130.0 | 60.0 | 3096.8 | 70 | 98.2/243.8/388.3 | 298.5 | 215.0 | 56.0 | 0.74 | 630.2 |
| 10:33:08 | D/1.2/LSM | 162.4 | 62.4 | 3104.9 | 100 | 124.5/243.9/391.7 | 295.1 | 215.0 | 57.2 | 0.48 | 565.1 |
| 10:42:33 | D/0/LSM | 144.8 | 74.8 | 3103.8 | 70 | 101.8/242.3/390.2 | 327.1 | 216.1 | 56.1 | 9.64 | 681.8 |
| 11:00:51 | D/0.99/BTREE | 195.6 | 135.6 | 3950.2 | 60 | 101.6/240.4/390.7 | 302.6 | 219.0 | 57.3 | 0.30 | 702.7 |
| 11:12:33 | D/0.5/BTREE | 121.5 | 51.5 | 3075.2 | 70 | 123.9/246.0/391.2 | 284.7 | 215.0 | 56.4 | 0.31 | 681.2 |
| 11:23:55 | D/1.2/BTREE | 132.3 | 52.3 | 3083.0 | 80 | 101.6/240.7/391.0 | 286.0 | 214.0 | 56.3 | 0.31 | 597.1 |
| 11:33:52 | D/0/BTREE | 132.0 | 52.0 | 3081.7 | 80 | 101.8/241.5/390.6 | 288.3 | 214.5 | 55.8 | 0.31 | 703.8 |
| 12:14:40 | C/1.2/DET | 166.6 | 106.6 | 58.2 | 60 | 132.1/242.9/388.7 | 140.5 | 69.0 | 57.6 | 0.24 | 375.1 |
| 12:20:55 | C/1.2/PG | 74.4 | 14.4 | 3.9 | 60 | 103.0/238.5/390.1 | 140.0 | 69.0 | 57.6 | 0.24 | 258.1 |
| 12:25:13 | C/0.5/DET | 74.3 | 14.3 | 4.6 | 60 | 103.4/244.4/390.1 | 142.8 | 70.0 | 58.6 | 0.24 | 355.1 |
| 12:31:08 | C/0.5/PG | 75.5 | 15.5 | 4.5 | 60 | 101.4/242.8/390.1 | 142.6 | 70.0 | 58.4 | 0.25 | 326.1 |
| 12:36:34 | C/0.99/PG | 75.2 | 15.2 | 4.3 | 60 | 100.5/242.2/389.3 | 141.5 | 70.0 | 58.2 | 0.24 | 268.1 |
| 12:41:02 | C/0.99/DET | 75.1 | 15.1 | 4.2 | 60 | 103.1/243.1/391.3 | 141.5 | 70.0 | 58.5 | 0.24 | 304.1 |
| 12:46:06 | C/0/PG | 75.0 | 15.0 | 4.3 | 60 | 103.1/240.2/387.9 | 144.0 | 71.0 | 58.4 | 0.36 | 335.1 |
| 12:51:41 | C/0/DET | 75.5 | 15.5 | 4.1 | 60 | 103.4/240.4/390.0 | 143.0 | 70.1 | 58.4 | 0.24 | 365.1 |
| 13:04:21 | C/0.99/LSM | 204.1 | 134.1 | 3924.6 | 70 | 99.0/241.2/390.7 | 310.8 | 239.0 | 57.6 | 0.25 | 603.1 |
| 13:14:24 | C/0.5/LSM | 75.7 | 15.7 | 4.2 | 60 | 99.2/242.3/390.5 | 290.9 | 218.0 | 59.1 | 0.24 | 505.1 |
| 13:22:49 | C/1.2/LSM | 75.7 | 15.7 | 4.2 | 60 | 102.6/244.7/391.9 | 287.9 | 217.0 | 57.3 | 0.24 | 431.1 |
| 13:30:00 | C/0/LSM | 75.1 | 15.1 | 4.2 | 60 | 98.4/244.9/388.3 | 291.3 | 218.5 | 58.3 | 0.24 | 511.5 |
| 13:45:06 | C/0.99/BTREE | 189.9 | 119.9 | 3022.8 | 70 | 101.3/242.3/390.7 | 298.1 | 227.0 | 57.9 | 0.24 | 576.1 |
| 13:54:42 | C/0.5/BTREE | 66.0 | 16.0 | 4.1 | 50 | 102.8/242.5/388.7 | 290.0 | 218.0 | 58.9 | 0.24 | 496.1 |
| 14:02:58 | C/1.2/BTREE | 75.4 | 15.4 | 4.2 | 60 | 104.1/241.5/388.3 | 286.3 | 215.0 | 57.1 | 0.24 | 429.1 |
| 14:10:08 | C/0/BTREE | 75.1 | 15.1 | 4.4 | 60 | 103.5/240.8/388.5 | 289.8 | 216.9 | 58.0 | 0.23 | 510.9 |
| 14:25:35 | B/1.2/DET | 164.5 | 104.5 | 10.0 | 60 | 102.4/242.4/388.5 | 137.0 | 67.0 | 55.4 | 0.25 | 411.1 |
| 14:32:26 | B/1.2/PG | 146.6 | 86.6 | 415.6 | 60 | 125.7/245.3/389.4 | 137.2 | 67.0 | 55.6 | 0.26 | 346.1 |
| 14:38:12 | B/0.5/DET | 146.7 | 86.7 | 416.6 | 60 | 103.2/243.7/388.0 | 142.9 | 68.0 | 56.6 | 0.32 | 530.1 |
| 14:47:02 | B/0.5/PG | 246.1 | 136.1 | 4205.0 | 110 | 100.5/241.2/389.4 | 138.5 | 68.0 | 56.5 | 1.12 | 619.2 |
| 14:57:21 | B/0.99/PG | 191.3 | 131.3 | 4202.9 | 60 | 135.8/246.1/388.5 | 137.1 | 67.0 | 55.7 | 0.27 | 410.1 |
| 15:04:11 | B/0.99/DET | 165.4 | 105.4 | 1622.5 | 60 | 101.7/241.5/388.9 | 136.9 | 67.0 | 55.8 | 0.28 | 463.1 |
| 15:11:55 | B/0/PG | 167.6 | 107.6 | 1623.1 | 60 | 102.4/240.5/390.0 | 138.2 | 68.0 | 56.1 | 0.83 | 550.9 |
| 15:21:05 | B/0/DET | 256.6 | 136.6 | 4386.3 | 120 | 103.2/242.2/390.6 | 142.7 | 67.6 | 55.4 | 1.45 | 676.1 |
| 15:39:18 | B/0.99/LSM | 231.0 | 121.0 | 3023.5 | 110 | 131.9/244.6/390.4 | 295.1 | 223.0 | 56.7 | 1.43 | 676.2 |
| 15:50:34 | B/0.5/LSM | 164.6 | 104.6 | 1915.6 | 60 | 103.5/244.7/391.5 | 297.5 | 223.0 | 59.8 | 0.31 | 770.3 |
| 16:03:25 | B/1.2/LSM | 219.1 | 149.1 | 4860.6 | 70 | 97.6/244.3/387.6 | 288.7 | 216.0 | 55.2 | 0.27 | 631.3 |
| 16:13:56 | B/0/LSM | 159.4 | 99.4 | 568.9 | 60 | 102.6/245.0/389.7 | 293.0 | 222.9 | 56.1 | 0.33 | 771.6 |
| 16:33:45 | B/0.99/BTREE | 528.1 | 378.1 | 8148.7 | 150 | 105.4/243.3/390.5 | 289.9 | 220.0 | 55.8 | 0.28 | 980.2 |
| 16:50:05 | B/0.5/BTREE | 155.0 | 115.0 | 1915.5 | 40 | 124.6/243.9/387.5 | 317.8 | 244.1 | 60.3 | 1.85 | 781.2 |
| 17:03:06 | B/1.2/BTREE | 246.0 | 156.0 | 4860.1 | 90 | 106.2/244.8/390.7 | 290.6 | 219.0 | 54.7 | 0.26 | 652.2 |
| 17:13:58 | B/0/BTREE | 163.8 | 103.8 | 567.8 | 60 | 103.8/240.5/392.2 | 305.7 | 233.1 | 56.8 | 1.01 | 751.2 |
| 17:36:39 | A/1.2/DET | 352.4 | 262.4 | 4400.1 | 90 | 109.3/244.4/389.5 | 94.2 | 44.0 | 34.9 | 0.32 | 852.2 |
| 17:50:51 | A/1.2/PG | 213.8 | 113.8 | 2506.6 | 100 | 105.1/242.0/387.5 | 175.9 | 85.0 | 75.3 | 0.31 | 942.7 |
| 18:06:34 | A/0.5/DET | 195.3 | 115.3 | 2531.0 | 80 | 126.3/243.4/390.4 | 102.9 | 45.0 | 35.7 | 6.34 | 1164.0 |
| 18:25:58 | A/0.5/PG | 686.0 | 646.0 | 20397.7 | 40 | 144.5/250.5/390.3 | 104.9 | 45.0 | 35.5 | 9.36 | 1489.0 |

## Per-case WAL, retry and Merkle/drain audit

All rows below additionally pass the shared configuration/count/failure checks described above. WAL full is LSN(after explicit checkpoint)−LSN(before measurement), including deferred LSM drain. It does not include subsequent verification/restart WAL. Measurement sampler phase extends through server stop, copied-log validation and collector settling, and therefore lasts longer than gateway timing; its sample count must not be used as the TPS denominator.

| Case | WAL window/full B per stmt | Retry attempts | Exhausted/permanent/divergence | Merkle verify | LSM drain s | LSM max merges | WAL samples/errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D/1.2/DET | 19.382/19.382 | 0 | 0/0/0 | N/A | — | — | 172/0 |
| D/1.2/PG | 18.981/18.981 | 0 | 0/0/0 | N/A | — | — | 118/0 |
| D/0.5/DET | 19.404/19.404 | 0 | 0/0/0 | N/A | — | — | 262/0 |
| D/0.5/PG | 19.002/19.002 | 0 | 0/0/0 | N/A | — | — | 230/0 |
| D/0.99/PG | 18.907/18.907 | 0 | 0/0/0 | N/A | — | — | 144/0 |
| D/0.99/DET | 19.307/19.307 | 0 | 0/0/0 | N/A | — | — | 196/0 |
| D/0/PG | 19.046/19.046 | 0 | 0/0/0 | N/A | — | — | 241/0 |
| D/0/DET | 19.449/19.449 | 0 | 0/0/0 | N/A | — | — | 275/0 |
| D/0.99/LSM | 61.308/382.028 | 0 | 0/0/0 | PASS | 8.554 | [6, 2] | 204/0 |
| D/0.5/LSM | 61.607/383.635 | 0 | 0/0/0 | PASS | 9.455 | [6, 2] | 271/0 |
| D/1.2/LSM | 61.538/383.246 | 0 | 0/0/0 | PASS | 8.651 | [6, 2] | 174/0 |
| D/0/LSM | 61.733/384.392 | 0 | 0/0/0 | PASS | 31.044 | [6, 2] | 276/0 |
| D/0.99/BTREE | 373.039/373.039 | 0 | 0/0/0 | PASS | — | — | 279/0 |
| D/0.5/BTREE | 374.604/374.604 | 0 | 0/0/0 | PASS | — | — | 341/0 |
| D/1.2/BTREE | 374.223/374.223 | 0 | 0/0/0 | PASS | — | — | 246/0 |
| D/0/BTREE | 375.340/375.340 | 0 | 0/0/0 | PASS | — | — | 351/0 |
| C/1.2/DET | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 135/0 |
| C/1.2/PG | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 111/0 |
| C/0.5/DET | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 207/0 |
| C/0.5/PG | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 176/0 |
| C/0.99/PG | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 118/0 |
| C/0.99/DET | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 155/0 |
| C/0/PG | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 186/0 |
| C/0/DET | 0.000/0.000 | 0 | 0/0/0 | N/A | — | — | 215/0 |
| C/0.99/LSM | 0.000/0.000 | 0 | 0/0/0 | PASS | 0.335 | [0, 0] | 156/0 |
| C/0.5/LSM | 0.000/0.000 | 0 | 0/0/0 | PASS | 0.335 | [0, 0] | 207/0 |
| C/1.2/LSM | 0.000/0.000 | 0 | 0/0/0 | PASS | 0.337 | [0, 0] | 134/0 |
| C/0/LSM | 0.000/0.000 | 0 | 0/0/0 | PASS | 0.337 | [0, 0] | 213/0 |
| C/0.99/BTREE | 0.000/0.000 | 0 | 0/0/0 | PASS | — | — | 155/0 |
| C/0.5/BTREE | 0.000/0.000 | 0 | 0/0/0 | PASS | — | — | 209/0 |
| C/1.2/BTREE | 0.000/0.000 | 0 | 0/0/0 | PASS | — | — | 135/0 |
| C/0/BTREE | 0.000/0.000 | 0 | 0/0/0 | PASS | — | — | 214/0 |
| B/1.2/DET | 50.616/50.616 | 0 | 0/0/0 | N/A | — | — | 176/0 |
| B/1.2/PG | 50.433/50.433 | 23079 | 0/0/0 | N/A | — | — | 128/0 |
| B/0.5/DET | 370.422/370.422 | 0 | 0/0/0 | N/A | — | — | 311/0 |
| B/0.5/PG | 370.022/370.022 | 0 | 0/0/0 | N/A | — | — | 300/0 |
| B/0.99/PG | 156.131/156.131 | 1312 | 0/0/0 | N/A | — | — | 148/0 |
| B/0.99/DET | 156.472/156.472 | 0 | 0/0/0 | N/A | — | — | 227/0 |
| B/0/PG | 380.770/380.770 | 0 | 0/0/0 | N/A | — | — | 310/0 |
| B/0/DET | 381.172/381.172 | 0 | 0/0/0 | N/A | — | — | 346/0 |
| B/0.99/LSM | 187.402/187.429 | 0 | 0/0/0 | PASS | 1.047 | [1, 1] | 216/0 |
| B/0.5/LSM | 401.215/401.215 | 0 | 0/0/0 | PASS | 0.441 | [0, 0] | 376/0 |
| B/1.2/LSM | 81.234/81.288 | 0 | 0/0/0 | PASS | 1.036 | [1, 1] | 190/0 |
| B/0/LSM | 412.058/412.058 | 0 | 0/0/0 | PASS | 0.330 | [0, 0] | 383/0 |
| B/0.99/BTREE | 187.379/187.379 | 0 | 0/0/0 | PASS | — | — | 227/0 |
| B/0.5/BTREE | 401.215/401.216 | 0 | 0/0/0 | PASS | — | — | 375/0 |
| B/1.2/BTREE | 81.270/81.270 | 0 | 0/0/0 | PASS | — | — | 183/0 |
| B/0/BTREE | 412.058/412.058 | 0 | 0/0/0 | PASS | — | — | 348/0 |
| A/1.2/DET | 367.985/367.985 | 0 | 0/0/0 | N/A | — | — | 451/0 |
| A/1.2/PG | 379.484/379.484 | 938038 | 0/0/0 | N/A | — | — | 638/0 |
| A/0.5/DET | 3000.704/3000.704 | 0 | 0/0/0 | N/A | — | — | 908/0 |
| A/0.5/PG | 2996.691/2996.691 | 0 | 0/0/0 | N/A | — | — | 742/0 |

## Reproduction and evidence

Authoritative remote evidence on .111: `/home/neel/claude_ctl/ycsb_matrix_20261020/evidence/phase1b/{audit.json,sampler_summary.json,projection.json,live.json,analysis.log}` and this report; original cases remain under `results/matrix/{d,c,b,a}_*/run_*/*/`. Source check/extension: `scripts/distributed/ycsb_matrix_20261020/diagnose_matrix.py`; renderer: `render_diagnostic.py`. Existing check_results.py, live run_matrix.sh, and live harness were left unchanged. The extension checks all completed workloads rather than only D and adds effective settings, gateway counts/signatures, planned workload SHA, baseline/binary drift, post-checkpoint WAL, merge state and telemetry/time decomposition. The read-only analysis was exercised against the real saved artifacts; audit errors=[] and no failure/cleanup artifacts.

Diagnostic scripts were copied only to new filenames in the authorized roots. A reproducible read-only refresh, if requested, is: run `nice -n 19 python3 ~/claude_checks/ycsb_matrix_20261020/diagnose_matrix.py sampler` on .247 and capture its compact stdout as .111 `evidence/phase1b/sampler_summary.json`; run `nice -n 19 python3 ~/claude_ctl/ycsb_matrix_20261020/diagnose_matrix.py artifacts` on .111. Rendering reads that fixed audit. Do not invoke run_matrix.sh, repeat_anchor.py, any disk probe, or any DB restart for this diagnostic. Small evidence copies are also beside this local report in `phase1b_evidence/`.
