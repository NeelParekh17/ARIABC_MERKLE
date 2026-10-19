#!/usr/bin/env python3
"""Assemble the final narrative from independently audited ff90 evidence."""
import csv,json,statistics as st
from pathlib import Path
A=Path(__file__).resolve().parent
read=lambda n:list(csv.DictReader((A/n).open()))
audit=json.loads((A/'final_audit.json').read_text());assert audit['complete'] and audit['accepted']==75
rows=read('results.csv');med=read('medians.csv');ratios=[r for r in read('ratios.csv') if r['stage']=='ff90'];activity=read('hot_lookup_by_point.csv')
assert len(ratios)==8 and all(int(r['min_trials'])==3 for r in ratios)
mm={(float(r['skew']),int(r['workers']),r['mode']):r for r in med if r['stage']=='ff90'}
table=['| θ | Workers | det TPS | B-tree TPS | LSM TPS | B-tree/det | LSM/det | Gap closed |','|---|---:|---:|---:|---:|---:|---:|---:|']
for r in ratios:
 t,w=float(r['skew']),int(r['workers']);cells=[]
 for mode in ['det','merkle_btree','merkle_lsm']:
  m=mm[t,w,mode];cells.append(f"{float(m['median_tps']):,.0f} [{float(m['min_tps']):,.0f}–{float(m['max_tps']):,.0f}]")
 mark='†' if r['no_det_advantage']=='True' else ''
 table.append(f"| {t:g} | {w} | {' | '.join(cells)} | {float(r['btree_det_ratio']):.4f} | {float(r['lsm_det_ratio']):.4f} | {float(r['gap_closed']):.1%}{mark} |")
table='\n'.join(table)
(A/'headline_table.md').write_text(table+'\n')
hot=['| θ | Workers | det HOT/updates | B-tree HOT/updates | LSM HOT/updates | Lookup inserts B/LSM | LSM merges requested/completed |','|---|---:|---:|---:|---:|---:|---:|']
for r in activity:
 count=lambda m:f"{int(r[m+'_hot_updates_sum']):,}/{int(r[m+'_updates_sum']):,}"
 hot.append(f"| {float(r['skew']):g} | {r['workers']} | {count('det')} | {count('merkle_btree')} | {count('merkle_lsm')} | {r['merkle_btree_inferred_lookup_inserts_sum']}/{r['merkle_lsm_inferred_lookup_inserts_sum']} | {r['merkle_lsm_merges_requested_sum']}/{r['merkle_lsm_merges_completed_sum']} |")
hot='\n'.join(hot);(A/'activity_table.md').write_text(hot+'\n')
explanation='''## Interpretation

Replacing the lookup B-tree with lsm3 did not consistently bring Merkle TPS closer to det at ff90. Four points have higher LSM medians and four have lower medians; LSM/B-tree changes range from -2.61% to +2.23%. Their observed trial min–max ranges overlap at every point. This is a three-trial comparison, not a formal equivalence test or a causal estimate of an index-write benefit.

All θ0 updates were HOT: every mode/point had 29,949 HOT updates out of 29,949 across its three trials, and neither Merkle lookup received inferred inserts. At θ0.99, lookup inserts summed across three trials were B-tree/LSM: w1=0/0, w4=11/11, w8=16/15, w16=43/45. Across all 24 grid cases per Merkle mode, that is only 70 B-tree and 71 LSM lookup insertions out of 239,892 updates. The proposed random-leaf write bottleneck is almost absent in this ff90 window. Lookup inserts are inferred from usertable inserts plus non-HOT updates; they are not direct AM callback counts.

At θ0/w8, median lookup buffer reads/hits are B-tree 1/7 and LSM 0/0. Merkle node heap/index buffer reads remain 8,443/3,845 versus 8,448/3,853, with about 81.6K/86.2K node heap/index hits in either mode. WAL is 5,035.018 versus 5,035.138 bytes/statement, and FPIs are 0.64645 versus 0.64650 per statement. det uses 3,848.530 bytes and 0.49805 FPIs per statement. Device read/write KiB per statement are det 16.487/7.167, B-tree 18.235/8.444, LSM 18.232/8.434. The remaining Merkle traffic is essentially unchanged by replacing the nearly idle lookup index.

At θ0.99/w16, median lookup reads/hits fall from 40/31 to 5/18, but node heap/index reads are 5,608/2,103 versus 5,609/2,102. WAL falls only from 3,226.639 to 3,222.380 bytes/statement (0.13%), FPIs from 0.39995 to 0.39940, and device reads from 9.0714 to 9.0526 KiB/statement (0.21%). LSM median TPS is nevertheless 1.21% lower. At θ0/w4, the arithmetic gap closed is 30.2%, despite zero inferred lookup inserts and no reduction in WAL or device reads. These counters do not support attributing the small, mixed TPS differences to avoided random lookup-index writes.

No LSM grid run requested or completed a merge: 0 in every one of the 24 runs, also 0 in smoke. For θ0 and θ0.99/w1, both tops end at 8,192 bytes in every trial. For θ0.99/w4,w8,w16, top0 ends at 16,384 bytes and top1 at 8,192 bytes in every trial. The nominal 1MiB threshold and the 65,536-insert check period were never exercised by this short ff90 workload. Compaction behaviour was not studied by user choice; ff100 and sustained runs were cancelled. The small correctness tests exercised automatic merges, which is a separate claim from sustained100M performance.

At θ0/w16, B-tree is already 9.65 TPS faster than det. The raw formula yields 520.7% because its denominator is negative and tiny; there is no positive det–B-tree gap to close. This value must not be interpreted as an improvement. The full-precision raw formula is retained in ratios.csv.

## Acceptance, variability and bounds

The final dataset is exactly smoke3 plus ff90 72: 75 accepted attempts, 1,500,000 validated request/row results, gateway exits0, divergence0, permanent failures0, and 50/50 full Merkle verifications. No final-build grid failure, noise exclusion or noise rerun occurred. The three earlier failed smoke campaigns remain under results/failed_attempts, including one completed B-tree case on the previous module that is excluded from the final same-build dataset.

Across the 24 grid configurations, TPS CV ranges from 0.33% to 5.38%. No severe-stall rule fired: maximum sampler gap is below 1.001s, database full-memory PSI is0%, and database full-I/O PSI averages span 15.59–39.17% across accepted cases. Normal synchronous-I/O waiting remains visible. The rule excludes sampler gaps>=10s, full-I/O PSI>=95% or full-memory PSI>=50% over>=10s, and telemetry errors uniformly across modes; shorter fluctuations remain in all trials.

The heap is28.25GB, larger than the approximately15GiB RAM host; shared_buffers is32MiB and each case drops OS caches after restore/validation/device settling. This is database-larger-than-memory cold-start evidence. It does not show that the short workload's working set exceeds RAM. PostgreSQL buffer reads are shared-buffer misses and may hit the OS cache; they are not device reads. Device counters include other traffic and sampler/launch time around the gateway interval; checkpoint/drain/restore costs are retained separately.

All modes use the same PostgreSQL/server/gateway/module hashes and harness source contract, SERIALIZABLE,96 terminals/96 client workers,200 partitions/fanout32/split1024/merge256, and identical published workloads. All modes preload lsm3. The equally vacuumed/analysed baselines have matching all-visible heap state; that normalization differs from the historical published controls and is disclosed. Smoke met the±10% drift gate. Small tests do not establish arbitrary concurrent SQL, extension crash safety or end-to-end replica repair.
'''
# Keep the statistical output from analyze.py intact, then append a bounded interpretation.
p=A/'RESULTS.md';base=p.read_text().split('\n## Interpretation\n')[0];p.write_text(base+'\n'+explanation)
(A/'AUDIT.md').write_text('''# Independent final audit

PASS: exact authorized dataset smoke3 + ff90 72 =75 cases and1,500,000 terminal/row results. Stage counts, unique configurations/trials and all expected points checked. Each mode has24 grid cases and1 smoke case. Merkle verifications:50/50 PASS. SELECT results:750,375; UPDATE results:749,625. Gateway exits0; divergence, permanent failures, nonterminal failures and deterministic errors all0. Accepted cases have no FAILED, cleanup error or NOISE_EXCLUDED marker.

The audit independently re-parses every server row result and request ID against the published-hash workload files, re-parses gateway success/error totals, and checks geometry,100M keyspace, SERIALIZABLE,32MiB shared buffers,96 terminals/client workers, synchronous Merkle, cold cache drop, physical reset evidence, actual lookup AM, split/repair plans and equal range fingerprints. All cases share the same binary/module/source contracts. LSM requested and completed merge counts agree in every run:0. No ff100 or sustained case is required or included.

Evidence: final_audit.json, audit.py, analysis_status.json, results/{smoke,ff90}/run_*/ and results/failed_attempts/. Local independent audit after FF90_GRID_DONE_72_CASES and detached .111 audit both pass. Source baseline and own normalized baseline control/size identities remain unchanged: baseline_evidence/final_baseline_identities.json (metadata/control/size proof, not a full content rehash).

No final-grid failure or noise exclusion occurred. Three earlier setup smoke campaigns were preserved, with three failed cases and one old-module completed B-tree case; these four attempts are excluded from the final uniform-build dataset. All final75 attempts are retained in attempts.csv. Raw per-host telemetry, PG/server/gateway logs, WAL statistics, checkpoints, resets, device counters and free-before-start logs remain in each case directory.
''')
report='''# Final ff90 LSM lookup report

At fillfactor90, lsm3 does not consistently move Merkle TPS closer to det: four points improve and four worsen, with -2.61% to +2.23% LSM/B-tree changes. Almost every update is HOT, so there is almost no lookup-index insertion work to remove. Compaction behaviour was not studied by user choice.

The controller finished72/72 at2026-10-14 21:25:06Z (2026-10-15 02:55:06 IST). Independent audit:75 cases including smoke,1.5M requests,50/50 Merkle PASS. Nothing was committed or pushed.

## Full ff90 results

TPS = median [min–max], three trials; rounded here, full precision in CSV. Ratios divide matched mode medians. Gap closed=(LSM-Btree)/(det-Btree).

'''+table+'''

† No positive baseline gap at θ0/w16: B-tree already exceeds det by9.65 TPS. Its raw520.7% formula is not an improvement claim.

## Exact HOT and lookup activity

Counts sum all three trials. No workload-A usertable INSERT statements occurred; lookup inserts are inferred non-HOT updates. There are zero merges in every individual LSM run.

'''+hot+'''

Both LSM tops end at8KiB for θ0 and θ0.99/w1; for θ0.99/w4,w8,w16 they end at16KiB/8KiB in every trial. The1MiB threshold is not approached. See hot_lookup_by_point.csv and results.csv for per-trial values.

## Choice and correctness

Postgres Pro lsm3 at3bac10420e7738ddbd78e0eea7e3967afdd35aac, three B-trees (base plus alternating tops). Fork13devel compatibility and four upstream defect fixes are documented in LSM_CHOICE.md/PATCH_NOTES.md and uncommitted patches. No PG engine algorithm change. All modes preload the same final extension.

Small split/merge/range tests and an independent additive root oracle passed;150K scalar inserts triggered two automatic merges. The actual det server accepted143K paired INSERT/update/delete/key-change commands with strict row tags, one automatic composite merge, matching expected rows and independent roots.100M split and repair-transfer query plans use the identical expression index in both modes and return the same range fingerprint. The final DROP-only fix has focused missing/existing DROP coverage. See CORRECTNESS.md for exact evidence and boundaries.

## Baseline and OOM method

All three normalized own baselines contain100M rows at fillfactor90. Heap28,248,276,992B; primary2,246,197,248B. Lookup B-tree3,154,264,064B; LSM base same plus8,192B per empty top. Node heap24,739,840B; node indexes6,676,480B+7,004,160B+16,384B; Merkle metapage8,192B. Geometry200/32/1024/256, format11, equal verified baseline roots. Data was copied from read-only published sources, then rebuilt with AdHash binaries; normalized visibility matches all modes. See BASELINES.md.

RAM about15GiB, shared_buffers32MiB, cold cache drop, rsync --inplace delta reset with settle probes,20K statements,96 terminals/client workers, SERIALIZABLE. Both-host1Hz telemetry, native PG counters and decoded WAL/FPI evidence are retained. Per-case device read KiB/statement ranges across the grid: det7.50–16.69, B-tree9.06–18.24, LSM9.05–18.25 (matched median ranges). Final free space remains above40GiB. No ff100 baseline was built; dedup_baselines.py was never run.

## Smoke versus publication

Aθ0/w8: det3703.02 TPS versus published median3881.23 (-4.59%); B-tree3747.42 versus AdHash3723.70 (+0.64%); LSM3527.96, with no historical LSM reference. Both controls pass±10%. Smoke is excluded from grid medians. See evidence/SMOKE_COMPARISON.md.

'''+explanation+'''
## Evidence and reproduction

- Analysis: analyze.py → results.csv, attempts.csv, medians.csv, ratios.csv, hot_lookup_by_point.csv, RESULTS.md, tps_and_ratios.{png,pdf}.
- Audit: audit.py → final_audit.json, AUDIT.md; analysis_status.json confirms smoke3/ff90 72 complete.
- Correctness and builds: CORRECTNESS.md; small_final_evidence/, small_final_client/, small_evidence/repair_copy.json; pg_evidence/, cpp_evidence/; build_pg.sh/build_cpp.sh.
- Baselines: baseline_evidence/baselines_normalized/, final_baseline_identities.json, BASELINES.md.
- Patches: harness.patch, harness_added_files.patch, lsm3_compatibility.patch, lsm3_full.patch; hashes in evidence/source_revision.json. All remain uncommitted in the clone.
- Local complete case evidence: agents/lsm/results/{smoke,ff90,failed_attempts}/ under /work/ARIABC/AriaBC/.bench_tmp/lsm_lookup_20261014/.
- Authoritative remote results: .111 /home/neel/claude_ctl/lsm_lookup_20261014/results/{smoke,ff90,failed_attempts}/.
- .247 build/test evidence: /home/neel/claude_checks/lsm_lookup_20261014/; baselines/working data: /home/neel/ariabc_data/lsm_lookup_20261014/.
- Status/history/resume details: MONITOR.md, STATE.md; results/status.txt, status_ff100.txt, supervision.jsonl and progress.json. The healthy controller was not restarted after either resumption. The workstation reboot did not stop the detached campaign.

Normalized baselines passed at13:15:18Z and initial smoke started13:16:01Z before the usage-limit interruption. Three setup fixes and preserved smoke failures preceded the final same-build smoke at14:59Z. The final grid ran15:36:33–21:25:06Z. ff100 and sustained compaction were cancelled at the user's request; no result for them is implied.
'''
(A/'REPORT.md').write_text(report)
# Readable prose spacing; leave identifiers, paths and SHA values intact.
import re
prefixes=['At','All','at','all','about','after','before','both','contains','contain','completed','divergence','failures','exits','each','exactly','fanout','fillfactor','format','geometry','gateway','heap','index','indexes','is','median','merges','nodes','partitions','passed','PASS','point','ports','primary','published','requests','root','rows','shared_buffers','smoke','split','submitted','sustained','terminals','threshold','trials','updates','version','versus','workers','with']
pattern=r'\b('+ '|'.join(sorted(prefixes,key=len,reverse=True))+r')(?=\d)'
for name in ['REPORT.md','RESULTS.md','AUDIT.md']:
 p=A/name;s=p.read_text();s=re.sub(pattern,r'\1 ',s);s=re.sub(r'(?<=[A-Za-z]),(?=\d)',', ',s);s=re.sub(r'(?<=\d)(KiB|MiB|GiB|GB|MB|B)\b',r' \1',s);s=s.replace('at3bac','at 3bac').replace('Fork13devel','Fork 13devel').replace('det–B-tree','det–B-tree');p.write_text(s)
