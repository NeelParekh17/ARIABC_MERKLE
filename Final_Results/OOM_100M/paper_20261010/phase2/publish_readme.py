#!/usr/bin/env python3
"""Write tables and audit narrative from checked CSVs, without remote access."""
from pathlib import Path
import json
import pandas as pd
P=Path(__file__).resolve().parent;ROOT=P.parents[1]
G=pd.read_csv(P/'configuration_summary.csv');R=pd.read_csv(P/'matched_ratios.csv');C=pd.read_csv(P/'previous_v2_comparison.csv');T=pd.read_csv(P/'telemetry_summary.csv');AC=pd.read_csv(P/'audit_counts.csv');E=pd.read_csv(P/'log_messages.csv');H=pd.read_csv(P/'headline_numbers.csv');F=pd.read_csv(P/'setup_and_audit_facts.csv').set_index('metric').value
COMBOS=[('a',0.),('a',.99),('a',1.2),('b',.99),('c',.99),('d',.99),('f',.99)];NAME={'pg':'PG','bcdb_det':'det','bcdb_merkle':'Merkle'}
ref='paper_20261010/phase2/'
def h(k,s):return float(H.loc[(H.metric==k)&(H.statistic==s),'value'].iloc[0])
def table(headers,rows):return '| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+''.join('| '+' | '.join(map(str,row))+' |\n' for row in rows)
def link(name):return f'[{name}]({ref}{name})'
def number(k):return float(F[k])
binary=pd.read_csv(P/'binary_hashes.csv') if (P/'binary_hashes.csv').exists() else None
main=f'''# OOM 100M: three-trial paper re-measurement

The completed campaign contains **252 accepted cases across 84 configuration points**, plus **three accepted smoke cases**. All **5,100,000 terminal results** passed the raw completion and row-result audit; divergence, permanent failures and exhausted retries are zero. Every Merkle case passed post-run verification: **84 campaign + one smoke**. No attempt was rejected or excluded, and the independent audit found no incorrectly accepted case under the recorded contract. Counts are in {link('audit_counts.csv')}; per-case checks are in {link('audit_cases.csv')}.

Primary values are medians of three trials. Across the 28 matched workload/skew/worker points, the median **det/PG ratio is {h('det_over_pg','median'):.3f}**, **Merkle/det is {h('merkle_over_det','median'):.3f}**, and **Merkle/PG is {h('merkle_over_pg','median'):.3f}**. These are medians of matched ratios, not aggregate throughput. Merkle/det ranges **{h('merkle_over_det','min'):.3f}–{h('merkle_over_det','max'):.3f}**. Source: {link('headline_numbers.csv')} and {link('matched_ratios.csv')}.

## Setup and frozen provenance

'''
setup=[['Engine commit','`16577072ecf888fbbbb5d80d0e90fc340c8ca342`'],['Hosts','DB/server .247 (`10.129.148.247`); controller/gateway .111 (`10.129.27.111`)'],['Isolation','SERIALIZABLE in all modes; PG retries with full jitter'],['Data','Fresh COPY of 100,000,000 rows; fillfactor 90; full keyspace scan before measurement'],['Common heap',f"{int(number('heap_bytes')):,} bytes ({number('heap_gib'):.2f} GiB), {int(number('heap_pages')):,} pages"],['Stopped baselines',f"Merkle {int(number('merkle_baseline_bytes')):,} bytes; plain {int(number('plain_baseline_bytes')):,} bytes"],['Merkle','Synchronous maintenance; 200 partitions, fanout 32, split 1024, merge 256'],['Initial tree',f"{int(number('baseline_merkle_total_nodes')):,} nodes; {int(number('baseline_merkle_leaf_nodes')):,} leaves; node heap {int(number('node_storage_heap_bytes')):,} bytes, total {int(number('node_storage_total_bytes')):,} bytes"],['Timed settings','32MB shared_buffers; fsync/full_page_writes/synchronous_commit on; wal_compression off; autovacuum off'],['Inputs','Seed 42; 20,000 SQL statements; A θ0/.99/1.2 + B/C/D/F θ.99'],['Workers and modes','Workers 1/4/8/16; PG, det, det + synchronous Merkle; three trials each'],['Execution','Standalone, Raft bypassed; direct protocol v2; 96 terminals and 96 det client workers; publication gate source 0'],['Ports','DB 5468; client 8068; Raft endpoint 9068'],['Generation','512MB shared buffers; 1GB maintenance_work_mem; two maintenance workers; memory guard 4608MB'],['Environment','DB host AMD Ryzen 7 5700G, approximately 15GB RAM; DB kernel 7.0.0-38-generic, gateway 7.0.0-34-generic; .111 clocksource tsc'],['Completion',f"{F['campaign_start_utc']} to {F['campaign_end_utc']} ({number('campaign_elapsed_hours'):.2f} hours for full campaign)"]]
main+=table(['Item','Recorded value'],setup)+f'''
Setup values: {link('setup_and_audit_facts.csv')}, saved [arguments]({ref}evidence/campaign/campaign.json), [baseline verification]({ref}evidence/baseline_evidence/verification.json), and [runtime environment]({ref}evidence/provenance/runtime_environment.json). The plain baseline is derived from the same freshly generated heap by dropping the integrity indexes. Older OOM data was not reused.

The isolated PostgreSQL build used `--enable-depend`; matching C++ Release binaries were built on .111. The immutable source archive SHA-256 is `{F['source_archive_sha256']}`. The audit compared all **{int(number('source_files_checked')):,} archived files** with frozen git blobs and checked the **{int(number('manifest_files_checked')):,}-file engine/build manifest**. Harness-only overlays and the restored ignored workload generator are recorded as patches; engine/server/gateway source has no changes. See {link('source_and_fetch_checks.json')} and [patch notes]({ref}evidence/provenance/PATCH_NOTES.md).

'''
if binary is not None:main+=table(['Binary','SHA-256'],[[r['binary'],f"`{r['sha256']}`"] for _,r in binary.iterrows()])
main+=f'''
All per-case executable manifests and the initial preflight hashes agree with build provenance; current read-only checks of all three binaries and recorded PG/C++ dependencies also match. Source: {link('binary_hashes.csv')}, [audit]({ref}audit_raw.json), and [dependency checks]({ref}evidence/247_binary_recheck.json).

## Method and timing

1. Generate from scratch with the frozen binaries, build the Merkle index, full-scan count/bounds and verify fillfactor, geometry, storage and `merkle_verify`. Preserve stopped plain and Merkle baselines and the golden archive.
2. Restore the stopped baseline to the mode's working copy using `rsync --inplace --no-whole-file`. Fast catalog/size checks occur every case. Full byte checks occur on the first restore and every tenth restore per baseline variant; the audit found **27 campaign and two smoke** content checks. Physical reset is used throughout.
3. Calibrate idle random-read QD1/QD8 and O_DSYNC write latency, then settle after each restore. Require three consecutive idle probes within `max(1.25 × idle latency, idle + 30µs)` for reads and `max(1.25 × idle latency, idle + 50µs)` for writes, at most 2MiB unrelated writes per probe. Minimum wait is 20s and maximum 1800s. Every case's final three probes passed the recorded rounded thresholds. Calibration ran for at least 300s.
4. Drop OS caches after reset validation and settling; start PostgreSQL cold. Execute a randomized set of workload/worker blocks with rotating mode order. Each mode occupies each block position once across the three trials. The complete actual sequence matches [planned_case_order.csv]({ref}evidence/provenance/planned_case_order.csv).
5. Validate every terminal completion and operation row result. Measure a separate checkpoint, restart to persist PG statistics, and run full post-workload Merkle verification for every Merkle case.

TPS is **20,000 / gateway workload wall seconds**. Restore, settle, startup, checkpoint and verification are outside this interval. Device counters cover a wider interval and include unrelated device traffic; they are not exact gateway-only I/O. PostgreSQL block reads can hit the OS cache. The actual timed workload files, including each gateway-side copy, match all seven previous published workload hashes ({link('workload_hashes.csv')}). These are the repository's custom SQL YCSB variants, not an upstream YCSB client.

## Independent audit, noise and limitations

'''
main+=table(['Dataset','Accepted','Rejected','Excluded','Terminal results','Merkle PASS'],[[r.dataset,r.accepted,r.rejected,r.noise_excluded,f'{r.terminal_results:,}',r.merkle_pass] for _,r in AC.iterrows()])
main+=f'''
A second parser independent of the harness also checked every terminal ID/tag, the SQL operation mix and every emitted gateway error counter; its [results]({ref}independent_terminal_check.json) pass all cases. The audit re-parsed all gateway and server row-result logs, checked exact request IDs/counts, return codes, saved settings and mode commands, recomputed workload hashes and TPS, re-evaluated telemetry, checked settling, and extracted det counters again. All terminal row results were present; there were no empty results. `all_attempts.csv` matches the accepted rows, with no extra attempts or failure/noise/cleanup markers. Raw case files remain remotely; {link('REMOTE_RAW_SHA256SUMS')} identifies **{int(number('raw_case_files')):,} files**. Locally fetched case evidence contains **{int(number('curated_case_files')):,} files / {int(number('curated_case_bytes')):,} bytes**, each verified against those hashes. No pgdata or bulk server traces were fetched. See {link('AUDIT.md')}.

The same predeclared exclusion rule applies to every mode: invalid/error telemetry; sampler gap ≥10s; full I/O PSI ≥95% or full memory PSI ≥50% over a window ≥10s. No case met it. Max observed sampler gaps were **{number('db_247_max_sampler_gap_s'):.6f}s on .247** and **{number('gateway_111_max_sampler_gap_s'):.6f}s on .111**; boot IDs were stable during all measurements and every .111 sample used tsc. Maximum qualifying full-I/O-pressure fraction on .247 was **{number('db_247_max_io_full_fraction'):.3f}**. Background load remains in the measurements; .111 was shared with another user's PostgreSQL at preparation. **225 DB telemetry windows and 227 gateway windows were shorter than 10s**, so many cases cannot support the ten-second pressure test. Workload wall times range **{int(number('workload_wall_min_ms'))}–{int(number('workload_wall_max_ms'))}ms**. A passing rule is not proof of a noise-free host. Source: {link('telemetry_summary.csv')}.

`dt_opf`, `dt_opf_stale_error` and `dt_rel_read_tags` were **not emitted in any campaign or smoke log**. All remain null/unavailable, not zero. Saved profiling had detailed phase tracing disabled, as in v2. No claim of zero OPF/stale/tag activity follows from absent counters.

Campaign PG logs contain **{int(number('retry_attempts_campaign')):,} serialization errors**, exactly matching successful retry attempts. The audit also found readiness-probe `FATAL: role "neel" does not exist` messages and expected administrative connection termination during controlled shutdowns. No other PostgreSQL ERROR/FATAL/PANIC category appeared; the full counts are retained in {link('log_messages.csv')}. Readiness probes omit a DB username; these messages do not correspond to failed workload terminals. No accepted case failed the recorded acceptance contract.

Three trials satisfy this campaign's requested design, but the harness's separate repeatability qualification asks for five; its saved [qualification report]({ref}evidence/campaign/MEASUREMENT_QUALIFICATION.md) labels all points `insufficient_repeats`. CV is sample standard deviation / mean × 100, with no observations discarded. **No point exceeds the 10% CV diagnostic; maximum CV is {number('cv_max_pct'):.2f}%**. Bands are observed min–max, not confidence intervals. Throughput orderings and Merkle internal-consistency PASS do not prove general SQL serializability, cross-mode row equality or recovery correctness. This is the modified PostgreSQL fork with det/Merkle disabled in PG mode, not upstream PostgreSQL; the dataset exceeds RAM but 32MB buffers do not cap Linux page cache or other shared memory. No replicated-cluster or recovery-fault test ran in this campaign, and “OOM” does not assert an OOM-killer event.

Preparation had an initial build hash-path failure and a host reboot during the first load. The incomplete campaign-owned load was discarded and regenerated before measurements; no timed case used that partial data. The reboot cause is unestablished. Storage authorization, budget and preparation evidence remain in the [phase-one archive](paper_20261010/evidence/) and [provenance]({ref}evidence/provenance/).

## All configuration summaries and previous-v2 comparison

Values below are statements/s, rounded only for display. **Best equals max**; each point has n=3. Previous-v2 TPS is its **single Oct 1 trial (n=1)**. The comparison input is {link('previous_v2_trials.csv')}, copied specifically from `runs/v2_20261001/summary.csv`; the old root summary mixed multiple historical campaigns and is not used. Full precision and I/O/retry comparisons: {link('configuration_summary.csv')}, {link('previous_v2_comparison.csv')}.

'''
for w,s in COMBOS:
 main+=f'### {w.upper()}, θ = {s:g}\n\n![Median and min–max throughput, {w.upper()} θ{s:g}](figures/scaling_{w}_{s}.png)\n\n[One-column PDF](figures/scaling_{w}_{s}.pdf). Lines show medians; bands show observed min–max.\n\n'
 g=C[(C.workload==w)&(C['skew']==s)].copy();g['mode_order']=g['mode'].map({'pg':0,'bcdb_det':1,'bcdb_merkle':2});g=g.sort_values(['workers','mode_order'])
 main+=table(['W','Mode','n','Median','Min','Max','Best','CV %','Oct 1 TPS','New/Oct 1'],[[int(r.workers),NAME[r['mode']],int(r.n),f'{r.median_tps:.2f}',f'{r.min_tps:.2f}',f'{r.max_tps:.2f}',f'{r.best_tps:.2f}',f'{r.cv_pct:.2f}',f'{r.previous_v2_tps:.2f}',f'{r.new_over_previous:.3f}'] for _,r in g.iterrows()])+'\n'
main+='## Matched ratios from medians\n\nEach row uses three-trial medians for the same workload/skew/worker point. Merkle means det + synchronous Merkle. Read/write ratios use the respective median device MiB; tiny C writes are background traffic and their ratios have little meaning.\n\n'
main+=table(['Workload','θ','W','det/PG','Merkle/det','Merkle/PG','Read MiB M/det','Write MiB M/det'],[[r.workload.upper(),f"{r['skew']:g}",int(r.workers),f'{r.det_over_pg:.3f}',f'{r.merkle_over_det:.3f}',f'{r.merkle_over_pg:.3f}',f'{r.device_read_mib_merkle_over_det:.3f}',f'{r.device_write_mib_merkle_over_det:.3f}' if pd.notna(r.device_write_mib_merkle_over_det) else 'undefined (det=0)'] for _,r in R.iterrows()])
main+='\n![Merkle/det matched ratios](figures/merkle_over_det.png)\n\n[PDF](figures/merkle_over_det.pdf). Heatmap cells are ratios of medians; no ratio confidence intervals are implied.\n\n## Trial spread\n\nLargest five sample CVs; all points remain included. Complete ranking: '+link('variability.csv')+'.\n\n'
main+=table(['Workload','θ','W','Mode','Median','Min–Max','CV %','Range/median %'],[[r.workload.upper(),f"{r['skew']:g}",int(r.workers),NAME[r['mode']],f'{r.median_tps:.2f}',f'{r.min_tps:.2f}–{r.max_tps:.2f}',f'{r.cv_pct:.2f}',f'{r.range_pct_of_median:.2f}'] for _,r in G.sort_values('cv_pct',ascending=False).head(5).iterrows()])
main+='\n## What changed relative to Oct 1 v2\n\nThe old v2 run used a different frozen working-tree snapshot (git HEAD `19562ae`, with recorded uncommitted build changes), one trial per point, and its own fresh fillfactor-90 heap. Both campaigns use the same seven SQL hashes and Merkle geometry. The new study rebuilds from commit `16577072`, generates a fresh heap, uses three rotated trials and records a uniform severe-stall rule and both-host telemetry. The DB host reboot and kernel change occurred before the new measurement campaign. This is a historical comparison, not a controlled experiment isolating one patch.\n\n'
main+=table(['Mode','Median new/old across 28 points','Minimum','Maximum'],[[NAME[m],f"{h(m+'_new_over_v2','median'):.3f}",f"{h(m+'_new_over_v2','min'):.3f}",f"{h(m+'_new_over_v2','max'):.3f}"] for m in ['pg','bcdb_det','bcdb_merkle']])
x=C[(C.workload=='a')&(C['skew']==1.2)&(C.workers==16)&(C['mode']=='pg')].iloc[0]
main+=f'''
The largest change is **PG A θ1.2 at workers 16: {x.previous_v2_tps:,.2f} → {x.median_tps:,.2f} TPS ({x.change_pct:.2f}%)**. New median read await is **{x.device_read_await_ms_median:.3f}ms**, versus **{x.previous_device_read_await_ms:.3f}ms** previously; new median retries are **{int(x.retry_attempts_median):,}**, versus **{int(x.previous_retry_attempts):,}** previously. Thus these counters do not support explaining the decrease as more read latency or more retries. Host/background load, execution differences, fresh physical state and historical single-trial variability were not separately controlled; the cause is unresolved. Requested retry backoff is a cumulative concurrent counter, not a wall-time subtraction.

Read-only publication did not start or stop any database, server, benchmark, or observer. Online recovery uses these hosts separately and was not disturbed.

## Evidence, figures and regeneration

- [Accepted trials](summary.csv), {link('configuration_summary.csv')}, {link('matched_ratios.csv')}, {link('previous_v2_comparison.csv')}, {link('audit_counts.csv')}, {link('audit_cases.csv')}, {link('telemetry_summary.csv')} and {link('log_messages.csv')}.
- [All evidence and analysis](paper_20261010/phase2/README.md); [checksums](paper_20261010/phase2/SHA256SUMS); [analysis source](paper_20261010/phase2/analyze.py); [README generator](paper_20261010/phase2/publish_readme.py).
- [Figures](figures/): every figure has PDF + PNG. Individual throughput panels are 3.35-inch wide, use colorblind-safe colors plus different markers, and show median/min–max. Additional ratio and historical-comparison heatmaps use the same CSV inputs. The combined `oom_scaling_all` is a tall reference panel; individual PDFs are the preferred paper panels.
- Exact remote raw roots: `.111:/home/neel/claude_checks/paper_20261010_oom/results/campaign/run_20261011_021231_3e7625c2/` and `results/smoke/run_20261011_013308_3b13566e/`. Baselines/builds remain under `.247:/home/neel/ariabc_data/paper_20261010_oom/`. Full source/golden archives remain remote; their hashes are published.
- Earlier primary root README, entire figure tree and combined summary were **moved intact** to [previous/root_before_phase2_20261012/](previous/root_before_phase2_20261012/). The [Oct 1 v2 publication](previous/v2_20261001_publication/README.md), all old raw data, optimization campaigns, and phase-one launch snapshots remain available. No observations were deleted.
- [COMMANDS.md section 2](../COMMANDS.md#2-oom-100m--three-trial-paper-publication-2026-10-12) records the completed launch and local-only regeneration. No commit was made.
'''
(ROOT/'README.md').write_text(main)
audit=f'''# Independent read-only audit

Audit time: {json.load(open(P/'audit_raw.json'))['time_utc']}. All campaign/smoke raw gateway and server logs were read remotely through stdin-only Python. Helpers set `PYTHONDONTWRITEBYTECODE=1`; all output was redirected locally. No remote files, DB state, observer or benchmark process was created or modified.

'''+table(['Dataset','Accepted','Rejected','Excluded','Terminal results','Merkle PASS','Content checks'],[[r.dataset,r.accepted,r.rejected,r.noise_excluded,r.terminal_results,r.merkle_pass,r.content_checks] for _,r in AC.iterrows()])+'''
Acceptance checks: exact planned order and unique full grid; terminal protocol version and all-request successes; IDs 1–20000 exactly once with valid SQL row counts; gateway exit and nonterminal/error/divergence counters; retry totals; Merkle `t` and geometry; SERIALIZABLE/durability/32MB settings; actual PG/DET command and worker configuration; frozen build/hash manifests and actual gateway workload copies. Summary TPS was recalculated from raw elapsed milliseconds and every saved row compared to JSON. Every Merkle case had post-run `t`.

No accepted case should have been rejected by these checks. There were no empty SQL results, exhausted retries, failure markers, noise exclusions or extra completed attempts. A second parser independent of benchmark validators checked complete request IDs, INSERT/SELECT/UPDATE terminal tags, the canonical SQL operation mix (F's CTE ends in UPDATE), and every reported gateway error counter. All 255 cases pass; independent_terminal_check.json preserves per-case command counts. Detailed det counters were re-extracted from server/stdout, server/stderr and PostgreSQL logs; none of the three labels was emitted. Null is preserved in CSV and JSON.

Noise was independently recomputed from every telemetry record: JSON errors, gaps, and rolling PSI windows. Both hosts kept stable boot IDs and the gateway clocksource stayed tsc. Every settle history's last three entries was checked against the actual rounded thresholds and idle-write bound. Readiness and administrative fatal messages were classified rather than falsely reporting error-free logs. All PG serialization errors match successful retries. Category counts:

'''+table(['Dataset','Message','Count'],[[r.dataset,r.message,int(r['count'])] for _,r in E.iterrows()])+'''
Limits: row-count/terminal and Merkle-consistency checks are not an independent serial oracle or cross-mode final-state equality proof. Most timed cases are too short for a ten-second pressure test. Host/device background traffic is retained; no dedicated-host claim. Five-repeat qualification is not met by this intentionally three-trial campaign. Failure-free completion and low CV do not supply confidence intervals or general correctness guarantees.

The source archive matches frozen git file-by-file, and engine manifest entries match its archived contents. Read-only current executable/dependency checks match launch provenance. Every fetched case file was checked against remotely computed SHA-256. Bulk server traces and pgdata remain remote. `REMOTE_RAW_SHA256SUMS` is a remote-path manifest; `SHA256SUMS` checks local curated publication files.

Reproduce publication with `analyze.py` and `publish_readme.py` locally. Do not invoke the remote audit helpers as local scripts: they expect .111 paths and must be streamed to a read-only Python interpreter only if re-fetching is authorized. `verify_source_and_fetch.py` is the one-time agent-directory source-archive verification script and expects the agent's full source tarball, which is omitted from the curated publication.
'''
(P/'AUDIT.md').write_text(audit)
(P/'README.md').write_text('''# Completed OOM paper evidence

Campaign 252/252 accepted, smoke 3/3 accepted; independent raw audit passes all 255. Primary tables/figures are ../../README.md and ../../figures/. The parent paper_20261010 directory retains phase-one snapshots; they are not final campaign summaries.

- trials.csv: full 252 accepted rows; smoke_trials.csv: separate three smoke rows.
- configuration_summary.csv: all 84 points, median/min/max/best/n/sample CV and trial values; no exclusions.
- matched_ratios.csv: 28 matched ratios of medians, plus device-I/O ratios.
- previous_v2_trials.csv: exactly 84 Oct 1 v2 rows; previous_v2_comparison.csv: matched new medians versus old single observations.
- audit_counts.csv, audit_cases.csv, telemetry_summary.csv, log_messages.csv: audit summaries and honest limitations; AUDIT.md describes checks.
- audit_raw.json and audit_supplement.json: output of read-only inspection of all raw remote logs/settings.
- source_and_fetch_checks.json: file-by-file frozen archive/git proof and remote-to-local case checksum validation.
- evidence/: fetched small per-case result/setup/counter/noise/I/O/checkpoint files and gateway logs, saved provenance, calibration, status and baseline verification. No pgdata, bulk server traces, or full source archive.
- REMOTE_RAW_SHA256SUMS / remote_raw_inventory.csv: all raw case file hashes on .111; raw files remain there.
- SHA256SUMS: local publication evidence checksum manifest. ../../PUBLICATION_SHA256SUMS checks new root README/summary/figures and Phase 2 files.

Local regeneration (no network, database or benchmark):

```bash
MPLCONFIGDIR=/tmp/oom_phase2_matplotlib python3 Final_Results/OOM_100M/paper_20261010/phase2/analyze.py
python3 Final_Results/OOM_100M/paper_20261010/phase2/publish_readme.py
(cd Final_Results/OOM_100M/paper_20261010/phase2 && sha256sum -c SHA256SUMS)
```

Regeneration changes artifact contents only if inputs/code/dependencies differ. Review differences before updating checksum manifests. CSVs preserve full numeric precision; README/figure displays round. CV uses sample standard deviation (ddof=1), and bands show observed extrema. Analysis-library versions and validation are recorded in analysis_environment.json and publication_checks.json.
''')
print('Wrote primary README, all 84 configuration rows, all 28 ratios and independent audit.')
