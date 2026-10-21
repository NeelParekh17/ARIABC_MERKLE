"""Render the saved read-only audit on the controller host; no DB operations."""
import collections,datetime,json,statistics,re
from pathlib import Path
ROOT=Path('/home/neel/claude_ctl/ycsb_matrix_20261020');E=ROOT/'evidence/phase1b';d=json.loads((E/'audit.json').read_text());rows=d['cases'];sample=json.loads((E/'sampler_summary.json').read_text())
lines=[]
def p(s=''):lines.append(s)
def table(head,values):
 p('| '+' | '.join(head)+' |');p('| '+' | '.join('---' for _ in head)+' |')
 for v in values:p('| '+' | '.join(str(x) for x in v)+' |')
 p()
def utc(t):return datetime.datetime.fromtimestamp(t,datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
def short(t):return datetime.datetime.fromtimestamp(t,datetime.timezone.utc).strftime('%H:%M:%S')
def mode(r):return {'pg':'PG','bcdb_det':'DET'}.get(r['mode'],r['kind'].upper())
def label(r):return f"{r['workload'].upper()}/{r['skew']:g}/{mode(r)}"
def get(wl,sk,m):return next(r for r in rows if r['workload']==wl and r['skew']==sk and mode(r)==m)
def mean(a):return statistics.mean(a)
A=[r for r in rows if r['workload']=='a'];status=(ROOT/'results/matrix/status.txt').read_text();ends={};starts={};dur={}
for l in status.splitlines():
 x=l.split()
 if len(x)>2:
  if x[1]=='CAMPAIGN_END':dur[x[2]]=int(x[3].split('=')[1])
  if x[1]=='CAMPAIGN_START':starts[x[2]]=datetime.datetime.fromisoformat(x[0].replace('Z','+00:00')).timestamp()
p('# Phase 1b: slowdown diagnosis and completed-case audit')
p(f'Artifact audit at **{utc(d["snapshot"])}**; sampler cutoff **{utc(sample["snapshot"])}**. Main matrix: **{len(rows)}/80 accepted cases**, D/C/B 16 each, A '+str(len(A))+'. F has not started. The matrix remains running in tmux `ycsb_matrix` on .111. All times below are UTC; add 05:30 for IST. Mode names: PG, deterministic plain (DET), Merkle+LSM (LSM), Merkle+B-tree (BTREE).')
p()
p('## Verdict')
p('**A is substantially more expensive because it updates existing random/hot rows in 50% of statements. Its added WAL, dirty-page eviction/writeback, restore volume, and hotspot serialization retries explain the measured slowdown. The device is under more pressure during A; the evidence does not establish permanent SSD degradation or QLC SLC-cache exhaustion.** No active disk test, SQL query, restart, benchmark rerun, large-file copy from .247, controller edit, or harness edit was performed for this diagnosis. Only existing artifacts/log tails, saved telemetry/sampler records, small baseline control hashes, and read-only process/device/memory counters were inspected. A small streaming sampler reader ran once per snapshot at nice 19; full telemetry analysis ran on .111, not the 15-GB DB host.')
p()
p('A/0.5 versus B/0.5 holds reads almost constant but multiplies device writes by 7.12–7.23 and WAL by 8.10. A DET takes 865.770 s versus 240.482 s (3.60×); PG takes 698.130 s versus 234.640 s (2.98×). Read await increases from 0.710→1.654 ms (DET) and 1.240→2.040 ms (PG), while write await rises 1.204→4.221 and 2.047→4.370 ms. This is mixed random-read/write pressure, not a measurement of the drive’s sequential bandwidth ceiling. A’s PG reset after the DET/0.5 case writes 20,397.7 MiB and takes 685.993 s, so looking only at campaign wall exaggerates the TPS change.')
p()
p('The hotspot case has a different bottleneck: A/1.2/PG performs 938,038 SQLSTATE 40001 retries (0.391 attempts per successful statement), versus 23,079 for B/1.2/PG. Requested backoff is 4,707.422 aggregate worker-seconds; this overlaps across workers and must not be added to elapsed wall. A/1.2/PG is 4,339.7 TPS / 553.038 s, versus B’s 38,507.2 TPS / 62.326 s. DET avoids these retries and reaches 5,916.6 TPS / 405.637 s. Low-skew A cases have zero retries yet remain slow, independently demonstrating the write/I/O cost. All completed A cases so far are plain; no observed A slowdown can yet be attributed to Merkle maintenance.')
p()
p('## Same-mode comparisons')
table(['Case','B TPS → A TPS','Run-time ratio A/B','Read MiB B → A','Write MiB B → A','WAL B/stmt B → A','Read await ms B → A','Write await ms B → A'],[(label(a),f"{b['tps']:.1f} → {a['tps']:.1f}",f"{a['wall_time_ms']/b['wall_time_ms']:.2f}×",f"{b['device_read_mib']:.0f} → {a['device_read_mib']:.0f}",f"{b['device_write_mib']:.0f} → {a['device_write_mib']:.0f}",f"{b['wal_bytes_per_statement']:.1f} → {a['wal_bytes_per_statement']:.1f}",f"{b['device_read_await_ms']:.3f} → {a['device_read_await_ms']:.3f}",f"{b['write_await_ms']:.3f} → {a['write_await_ms']:.3f}")for a in A for b in [get('b',a['skew'],mode(a))]])
p('## Disk, memory, and overnight trend')
p('The settled probe medians do **not** show a persistent upward step across the night. The following are medians across accepted cases (A is only four plain cases, so workload-wide values are not matched comparisons). These are the harness’s already-recorded probes; no probe was invoked by this diagnostic.')
p()
table(['Workload','Cases','Settle QD1/QD8/write µs','Settle wait median (range), s','Read await median (range), ms','Write await median (range), ms','Min available RAM, MiB','Max dirty, MiB','Swap-in/out pages'],[(w.upper(),len(a),'/'.join(f"{statistics.median(x[k]for x in a):.1f}"for k in ['settle_qd1_median_us','settle_qd8_median_us','settle_wr_median_us']),f"{statistics.median(x['settle_wait_s']for x in a):.0f} ({min(x['settle_wait_s']for x in a):.0f}–{max(x['settle_wait_s']for x in a):.0f})",f"{statistics.median(x['device_read_await_ms']for x in a):.3f} ({min(x['device_read_await_ms']for x in a):.3f}–{max(x['device_read_await_ms']for x in a):.3f})",f"{statistics.median(x['write_await_ms']for x in a):.3f} ({min(x['write_await_ms']for x in a):.3f}–{max(x['write_await_ms']for x in a):.3f})",f"{min(x['telemetry']['mins_kib']['MemAvailable']for x in a)/1024:.0f}",f"{max(x['telemetry']['maxs_kib']['Dirty']for x in a)/1024:.0f}",f"{sum(x['telemetry']['vm_delta']['pswpin']for x in a):.0f}/{sum(x['telemetry']['vm_delta']['pswpout']for x in a):.0f}")for w in 'dcba' for a in [[r for r in rows if r['workload']==w]]])
p('A’s QD1 median is modestly higher, but QD8 remains ~244 µs and synchronous-write median ~390 µs, comparable to earlier groups. All 52 settle checks accepted. Saved campaign calibration idle medians:')
p()
cv=[]
for c in sorted(d['campaigns'],key=lambda c:starts[c['name']]):
 f=next(Path(c['path']).glob('device_calibration_*.json'));j=json.loads(f.read_text());cv.append((c['name'],short(starts[c['name']]),f"{j['idle_qd1_median_us']:.1f}",f"{j['idle_qd8_median_us']:.1f}",f"{j['idle_wr_median_us']:.1f}"))
table(['Campaign','Start','Idle QD1 µs','Idle QD8 µs','Idle write µs'],cv)
p('Later matched observations contradict a monotonic, irreversible device slowdown: B/0/BTREE, at 17:13, reads at 0.790 ms and 8,519 TPS, better than B/0/LSM at 16:13 (0.912 ms, 7,520 TPS). B/1.2/BTREE at 17:03 has 0.306 ms read await versus 0.437 ms in the earlier LSM case. C/0.99 LSM and BTREE, 41 minutes apart, deliver 27,217 and 27,211 TPS with 0.210/0.209 ms read await. Different AMs are not perfect controlled repeats, but there is no one-way worsening in these adjacent comparable observations.')
p()
p('Available memory never falls below 6,491 MiB in the saved measurement telemetry. Across all 52 windows: zero swap-out pages, zero OOM kills, and only 153 swap-in pages (~0.60 MiB at 4-KiB pages). Roughly 440 MiB of already allocated swap in the live host snapshot is not evidence of active swapping. Cold OS-cache drops are recorded for each case; 32MB shared_buffers does not bound Linux page cache or the engine’s shared memory. A/0.5 DET/PG dirty peaks are 520/643 MiB, compared with B/0.5’s 164/196 MiB; their memory full-pressure time remains ~0.030%, while I/O full-pressure time is 37.43%/49.13%. This indicates I/O waiting, not a RAM/swap collapse. No requested checkpoint is observed inside the run via the saved checkpoints_req deltas (all zero). Timed checkpoint counters are not in these snapshots, so this is not a universal checkpoint absence claim. A’s explicit post-run checkpoints take 0.31–9.36 s, not hundreds of seconds.')
p()
p('QLC cache exhaustion, garbage collection, firmware thermal throttling, and media aging cannot be separately identified from these counters. Mixed-I/O await and cyclic throughput could include drive-internal effects, but the workload and incoming restore bytes also change. There is no saved matched sustained-write control or drive-internal cache/temperature history that isolates them. If needed, propose a matched repeated read/write experiment with thermal/SMART history **after the matrix**, on an approved disposable target. Do not run it between cases: restore/settle is active then.')
p()
p('## Within-run A progress')
p('Sampler commit rates below use complete 60-second bins from the measurement marker, which starts shortly before gateway timing. The final partial active bin and post-gateway log/collector tail are excluded when comparing early and late bins. These are PostgreSQL commit-counter rates, including a few monitoring commits per second, not a replacement for validated gateway TPS. They have coarse alignment and publication lag; WAL rates and device counters corroborate the changes.')
p()
bv=[]
for a in A:
 n=int(a['wall_time_ms']/60000);b=[v for k,v in a['sampler']['bins'].items()if int(k)<n and v['n']>=50];rates=[v['commit_s']for v in b if v['commit_s']is not None];io=a['telemetry']['bins'];bv.append((label(a),', '.join(str(round(v))for v in rates),f"{mean(rates[:3]):.0f} → {mean(rates[-3:]):.0f}",f"{(mean(rates[-3:])/mean(rates[:3])-1)*100:+.1f}%"))
table(['Case','Full-minute commit/s sequence','First 3 → last 3 means','Change'],bv)
p('DET/0.5 alternates between fast and slow intervals, rather than steadily falling every minute: its second minute has 4.28 ms read / 5.16 ms write await and 1,233 commits/s, later recovering to ~3,100–4,067 commits/s. Its slow write-await minutes reach 8.67 ms; dirty pages peak ~520 MiB. PG/0.5 fluctuates 2,833–4,545 commits/s in full bins; its first-three versus last-three mean changes only about −6%. PG/1.2 also recovers after slow intervals. Thus there is real in-run variability and some DET/0.5 drift, but no sustained terminal throughput collapse common to A’s runs. WAL bytes/s may also decline because repeated writes can need fewer first-touch full-page images (inference; WAL record types were not decoded); WAL rate alone is not a transaction-rate proxy.')
p()
if(E/'live.json').exists():
 live=json.loads((E/'live.json').read_text());p(live['report']);p()
p('The gateway is captured by the harness with subprocess capture_output; gateway.log is written when the process returns. Therefore there is no durable live gateway-progress file during execution. Reading its pipe would consume output and disturb validation, so this diagnostic uses the existing sampler and a bounded server-log tail instead. Completed gateway logs prove terminal counts/TPS. A request ID in a live server tail is an approximate progress indication, not a validated completion count.')
p()
p('## Campaign time versus measured run time')
p('Reset includes stopping/restoring the working copy, checks, the recorded settle wait, cache drop, validation/startup, and server start. It is not pure rsync time. Fixed setup includes workload generation, baseline/preflight checks and device calibration. Post-run time includes log copy/validation, collector settling, drain/checkpoint, PostgreSQL restart, Merkle verification, and cleanup. Per-case cycle boundaries come from sampler setup transitions and campaign END timestamps (~1 s resolution). Phase markers do not isolate every substep; unknown residuals are explicitly retained rather than called disk time.')
p()
agg=[]
for w in 'dcba':
 a=[r for r in rows if r['workload']==w];camps=[c for c in d['campaigns']if c['name'].startswith(w+'_')];fixed=sum(t['total_before_cases_s']for c in camps for t in c['timings']);run=sum(r['wall_time_ms']/1000 for r in a);reset=sum(r['reset_time_ms']/1000 for r in a);cycles=sum(r.get('cycle_s',0)for r in a);post=cycles-run-reset;elapsed=sum(v for k,v in dur.items()if k.startswith(w+'_')) if w!='a' else fixed+cycles
 agg.append((w.upper(),len(a),f'{elapsed/60:.2f}',f'{fixed/60:.2f}',f'{run/60:.2f}',f'{reset/60:.2f}',f"{sum(r['settle_wait_s']for r in a)/60:.2f}",f'{post/60:.2f}',f"{sum(r['lsm_drain_ms']/1000 for r in a):.2f}",f"{sum(r['checkpoint_ms']/1000 for r in a):.2f}"))
table(['Group','Cases','Campaign elapsed min','Fixed setup min','Gateway min','Reset min','Settle within reset min','Post-run min','Drain within post s','Checkpoint within post s'],agg)
p('D/C/B campaign durations are the controller’s actual END elapsed_s totals; the fixed+case decomposition differs by only a few seconds due to marker granularity/dispatch. A is a completed-case prefix through the next setup marker, not the full in-progress campaign. Average completed A cycle is 1,112 s versus B’s 626 s, but its mean gateway duration is 631 s versus B’s 199 s. A’s first four cases include different skews/modes from a full group; matched comparisons above are the meaningful performance ratios. A fixed setup is 608.4 s (300.3 s calibration), versus B plain’s 414.6 s (300.3 s calibration); the remaining generation/preparation work also grows with longer update SQL.')
p()
p('## Updated A and F projection')
# Explicit scenario model. Keep every assumption in evidence for later comparison.
factor=statistics.median(get('a',sk,'DET')['wall_time_ms']/get('b',sk,'DET')['wall_time_ms']for sk in [.5,1.2]);pglo=get('a',.5,'PG')['wall_time_ms']/get('b',.5,'PG')['wall_time_ms'];pghi=get('a',1.2,'PG')['wall_time_ms']/get('b',1.2,'PG')['wall_time_ms'];missing=[(.99,'PG'),(.99,'DET'),(0.,'PG'),(0.,'DET')]
plain_run=sum(get('b',sk,m)['wall_time_ms']/1000*(factor if m=='DET'else (pglo*pghi)**.5 if sk==.99 else pglo)for sk,m in missing)
plain_reset=2300.;plain_post=4*mean(r['cycle_s']-r['wall_time_ms']/1000-r['reset_time_ms']/1000 for r in A);remaining_plain=plain_run+plain_reset+plain_post
last_start=max(g['first']for k,g in sample['cases'].items()if '/results/matrix/a_plain/'in k)
a_plain_end=last_start+remaining_plain
merkle={}
for k in ['lsm','btree']:
 b=[r for r in rows if r['workload']=='b'and r['kind']==k];run=sum(r['wall_time_ms']/1000 for r in b)*factor;post=sum(r['cycle_s']-r['wall_time_ms']/1000-r['reset_time_ms']/1000 for r in b);merkle[k]=dict(run_s=run,reset_s=2200.,post_s=post,fixed_s=608.4,total_s=run+2200+post+608.4)
a_end=a_plain_end+sum(x['total_s']for x in merkle.values());a_total=a_end-starts['a_plain'];f_total=a_total*1.15;f_end=a_end+f_total
eta=dict(det_A_B_factor=factor,pg_low_factor=pglo,pg_hot_factor=pghi,remaining_plain=dict(run_s=plain_run,reset_s=plain_reset,post_s=plain_post,total_s=remaining_plain,from_case_start=last_start),merkle=merkle,A_total_s=a_total,A_end=a_end,F_total_s=f_total,F_end=f_end)
(E/'projection.json').write_text(json.dumps(eta,indent=2)+'\n')
table(['Milestone','Central UTC','Central IST','Planning range UTC'],[('A plain complete',utc(a_plain_end),datetime.datetime.fromtimestamp(a_plain_end+19800,datetime.timezone.utc).strftime('%Y-%m-%d %H:%M'), '2026-10-20 20:00–21:00'),('All A complete',utc(a_end),datetime.datetime.fromtimestamp(a_end+19800,datetime.timezone.utc).strftime('%Y-%m-%d %H:%M'), '2026-10-20 23:00 to 2026-10-21 03:00'),('F / full matrix complete',utc(f_end),datetime.datetime.fromtimestamp(f_end+19800,datetime.timezone.utc).strftime('%Y-%m-%d %H:%M'), '2026-10-21 05:00–13:00')])
p(f'These are scenario estimates, not statistical intervals or a guarantee. Model: A/DET is {factor:.2f}× B gateway time (observed 3.60–3.70×); A/PG uses 2.98× at uniform and the geometric mean of 2.98×/8.87× at skew .99. Four remaining plain resets budget 2,300 s, following the 20-GiB incoming delta (~700 s), then ~450/450/700 s; plain post-run uses the observed {plain_post/4:.1f} s per case. Each future A Merkle campaign budgets 2,200 s reset (~800/450/700/250 s, including first content check), 608 s fixed setup, B’s measured post-run total, and B’s same-AM gateway total ×{factor:.2f}. This yields about {a_total/3600:.2f} h for all A. F has the same 50% update mix, but its source uses one materialized SELECT CTE + UPDATE RETURNING per RMW statement, so budget 1.15× A (~{f_total/3600:.2f} h); the wide range covers unmeasured RMW contention/CPU and future restore/drive behavior. No A Merkle or F case has yet calibrated this extrapolation. Projection assumes no failure/rerun, no new controller hold, and gateway <3,600 s per case; exceeded timeouts or a new sustained device slowdown would move it later. The old D-only ETA is obsolete.')
p()
p('## Acceptance audit')
p(f'**PASS for the completed snapshot: {len(rows)} cases × 2,400,000 = {len(rows)*2400000:,} validated statements.** Every accepted result agrees on SERIALIZABLE, 16 workers, one trial, 32MB shared_buffers; setup SHOW values independently show transaction_isolation/default_transaction_isolation=serializable and shared_buffers=4096 8-KiB blocks. Gateway direct-terminal success, signatures signed/verified, and validated request counts are 2,400,000 in every case; signature mismatches, exhausted retries, permanent failures, deterministic/nonterminal errors and divergence are zero. No FAILED.txt or cleanup_errors.txt exists in a main matrix case. No accepted configuration key is duplicated. These are the single-node direct-completion semantics used by the task, not a multi-replica consistency proof.')
p()
audit=[]
for w in 'dcba':
 for k in ['plain','lsm','btree']:
  a=[r for r in rows if r['workload']==w and r['kind']==k]
  if not a:continue
  audit.append((w.upper()+' '+k,len(a),'all 2,400,000','all PASS',f"{len(a)}/{len(a)}"if k!='plain'else'N/A',sum(r['retry_attempts']for r in a),sum(r['sampler']['valid']for r in a),sum(r['sampler']['measurement_errors']for r in a),'present; consistent'))
table(['Group','Cases','Validated statements/case','Config + failures','Merkle verify t','Retry attempts','Valid WAL samples','Measurement sampler errors','WAL brackets'],audit)
p(f'All {sum(r["kind"]!="plain"for r in rows)} completed Merkle cases have saved merkle_verify.txt exactly `t`, result PASS, recovery state READY and lag_items=0. All {sum(r["kind"]=="lsm"for r in rows)} LSM cases have drain elapsed time, non-null sampler merge state, and final saved state idle/flush=false. Zero merges in read-only C or in low-skew B is recorded behavior, not a missing sample. Merge values are per-index lifetime counters bounded by PostgreSQL restarts; the table below reports each case’s maximum observed counters, not sums across restarts.')
p()
p(f'There are {sum(r["sampler"]["valid"]for r in rows):,} valid measurement WAL/commit samples ({min(r["sampler"]["valid"]for r in rows)}–{max(r["sampler"]["valid"]for r in rows)} per case), zero measurement-phase sampler errors, and {d["sampler_malformed"]} malformed JSONL lines. Connection-refused samples in setup/verify are expected while PostgreSQL is stopped, including the live restore; they are not counted as measurement errors. Both WAL snapshots and the post-checkpoint LSN exist for every completed case, and the measurement before/after postmaster_start matches. C correctly has zero WAL bytes despite valid samples. Device writes during C include process/result logging and other partition traffic; device counters are not PostgreSQL-only.')
p()
p('### Workload SHA parity and baseline integrity')
parity=[]
for w in 'dcba':
 for sk in [0.,.5,.99,1.2]:
  a=[r for r in rows if r['workload']==w and r['skew']==sk]
  if a:parity.append((w.upper(),sk,len(a),a[0]['workload_sha256'],'4/4 identical'if len(a)==4 else 'completed modes identical; other modes pending'))
table(['Workload','Skew','Completed modes','SHA256','Parity'],parity)
p('Each row’s SHA also matches its generated campaign workload manifest. D/C/B have complete 4-mode parity for all 12 combos. A has only two completed combos/two modes each at this cutoff, so four-mode A parity is pending, not proved. No A pending configuration or F is silently treated as accepted.')
p()
p('Per-case baseline manifests are identical within each baseline family, name the expected base, and retain 100,000,000 initial keys (1..100,000,000), the same physical heap/index geometry and baseline control hashes. All saved setup settle checks passed; harness checks the restored working copy’s names/sizes/mtimes/modes and PG_VERSION/control hashes every reset, with sampled full byte comparisons on the first restore per campaign. Nine completed campaign-start restores have CONTENT_CHECK=identical (plus A’s first restore); these checks are already saved, not rerun here. A current tiny-file read at 18:55:36 UTC found all four base global/pg_control SHA256s equal to the prelaunch record, including the unused old LSM base:')
p()
table(['Baseline','pg_control SHA256'],[(Path(k).name,v[0]['pg_control_sha256'])for k,v in d['baselines'].items()]+[('base_ff90_lsm (unused)','14db14fd4dc3d1a46ea04395a1e6c74fb6ad79b68b293d812b5dcebdc2f21821')])
p('All four PG_VERSION hashes equal `1a252402972f6057fa53cc172b52b9ffca698e18311facd0f3b06ecaaef79e17`. This supports preserved baseline provenance and reset acceptance; it is **not a new full-content hash of all baseline files**. A fresh full scan during measurements would violate the no-extra-load instruction, so none was run. Saved executable provenance is identical across all 52 results: PostgreSQL `56cbb4d4…`, server `142bdca5…`, gateway `489658d1…`; full hashes are in audit.json/PHASE1_REPORT.md. No new binary scan was run on .247.')
p()
p('### Problems and qualifications requiring attention')
table(['Finding','Evidence / impact','Action'],[
 ('A hotspot retry cost','A/1.2/PG 938,038 serialization retries; B/1.2/PG 23,079; B/.99/PG 1,312. All others 0; all exhausted/permanent/divergence 0.','Keep these accepted runs; report retry/backoff cost. Do not change retry policy mid-matrix.'),
 ('A low-skew write/backpressure and restore overhead','A/.5 write volume ~25 GiB; read/write await up to 2.040/4.370 ms; next restore ~20 GiB / 686 s. DET has cyclic minute rates and some late drift.','Retain timings separately; no disk test or cache-reset change during this matrix.'),
 ('D/.99/BTREE anchor outlier','Main result 11,731 TPS, −10.3866% from 13,091 anchor; read await .53846 ms. Prior Phase-1 separate diagnostic repeat 13,176 TPS, +.6501%, .46165 ms, same SQL/build and PASS.','Keep original matrix result; label variability. Repeat was done before C, not during this diagnostic; never replace/cherry-pick it.'),
 ('B/0/LSM vs BTREE variability','7,520 vs 8,519 TPS (BTREE +13.3%); .912 vs .790 ms read await, same combo hash. Different AMs and one trial prevent isolating causality.','Flag in comparison; no rerun now.'),
 ('Long first restore B/BTREE','528.104 s, 8,148.7 MiB restore, including campaign first full byte check and 150-s settle.','Setup outlier, not a 528-s TPS penalty; retain incoming-copy provenance.'),
 ('Deferred D/LSM WAL omitted by window-only view','D/LSM window ~61–62 B/stmt but through drain/checkpoint ~382–384 B/stmt; 8.55–31.04-s drain, [6,2] merges.','Use full WAL bracket for write-amplification comparisons; window and full both tabulated below.'),
 ('No repeated-trial or media-cause identification','One trial/config; no matched A Merkle/F observation, sustained disk control, temperature/cache history.','Keep ETA and causal attribution bounded; propose any active media experiment only after matrix.'),
 ('Partial matrix audit','D/C/B complete; A partial and F pending. Live snapshots can advance after report cutoff.','Continue the unchanged controller; audit later results separately.'),
])
p('No completed case acceptance/correctness failure was found. The checker does not establish that every application result equals an external serial oracle; it checks the saved harness completion/validation evidence, plus Merkle verification. No controller or harness fix is justified during the run. For a later campaign, add non-consuming timestamped gateway progress and explicit restore/verify/cleanup durations; preserve comparable settings and record device temperature/health without altering this campaign.')
p()
p('## Time-ordered per-case performance')
p('Rows are ordered by first sampler setup marker, not lexical skew order. Read/write values are MiB; await is ms per completed device I/O; IOPS and MiB/s divide device deltas by device_window_ms (gateway plus short telemetry overhead), not by campaign wall. Partition-wide counters include logging and unrelated traffic. The gateway duration alone is the TPS denominator.')
p()
table(['UTC setup','Case','TPS','Gateway s','Read/write MiB','Read/write await ms','Read/write IOPS','Read/write MiB/s'],[(short(r['start']),label(r),f"{r['tps']:.1f}",f"{r['wall_time_ms']/1000:.3f}",f"{r['device_read_mib']:.1f}/{r['device_write_mib']:.1f}",f"{r['device_read_await_ms']:.3f}/{r['write_await_ms']:.3f}",f"{r['read_iops']:.0f}/{r['write_iops']:.0f}",f"{r['device_read_mib']/(r['device_window_ms']/1000):.1f}/{r['device_write_mib']/(r['device_window_ms']/1000):.1f}")for r in rows])
p('## Time-ordered reset, settle and overhead')
p('Reset excludes neither settle nor validation/startup. Reset−settle is the remaining setup bundle, not an exact restore timer. Post is full cycle−reset−gateway; CP/drain are components of post. Verify+cleanup is marker-based and includes restart/verification/cleanup; cleanup alone uses result.json mtime to the next setup/campaign END. Exact substep timers not saved by the harness cannot be reconstructed more finely.')
p()
table(['UTC','Case','Reset s','Reset−settle s','Restore MiB','Settle s','QD1/QD8/write µs','Post s','Verify+cleanup s','Cleanup s','Checkpoint s','Cycle s'],[(short(r['start']),label(r),f"{r['reset_time_ms']/1000:.1f}",f"{r['reset_ex_settle_s']:.1f}",f"{r['restore_write_mib']:.1f}",f"{r['settle_wait_s']:.0f}",'/'.join(f"{r[k]:.1f}"for k in ['settle_qd1_median_us','settle_qd8_median_us','settle_wr_median_us']),f"{r.get('cycle_s',0)-r['reset_time_ms']/1000-r['wall_time_ms']/1000:.1f}",f"{r['phase_s'].get('verify_and_cleanup',0):.1f}",f"{r.get('cleanup_s',0):.1f}",f"{r['checkpoint_ms']/1000:.2f}",f"{r.get('cycle_s',0):.1f}")for r in rows])
p('## Per-case WAL, retry and Merkle/drain audit')
p('All rows below additionally pass the shared configuration/count/failure checks described above. WAL full is LSN(after explicit checkpoint)−LSN(before measurement), including deferred LSM drain. It does not include subsequent verification/restart WAL. Measurement sampler phase extends through server stop, copied-log validation and collector settling, and therefore lasts longer than gateway timing; its sample count must not be used as the TPS denominator.')
p()
table(['Case','WAL window/full B per stmt','Retry attempts','Exhausted/permanent/divergence','Merkle verify','LSM drain s','LSM max merges','WAL samples/errors'],[(label(r),f"{r['wal_bytes_per_statement']:.3f}/{r['wal_full_b_stmt']:.3f}",r['retry_attempts'],'0/0/0',r['merkle_verify'],f"{r['lsm_drain_ms']/1000:.3f}"if r['kind']=='lsm'else'—',str(r['sampler']['merges'])if r['kind']=='lsm'else'—',f"{r['sampler']['valid']}/{r['sampler']['measurement_errors']}")for r in rows])
p('## Reproduction and evidence')
p('Authoritative remote evidence on .111: `/home/neel/claude_ctl/ycsb_matrix_20261020/evidence/phase1b/{audit.json,sampler_summary.json,projection.json,live.json,analysis.log}` and this report; original cases remain under `results/matrix/{d,c,b,a}_*/run_*/*/`. Source check/extension: `scripts/distributed/ycsb_matrix_20261020/diagnose_matrix.py`; renderer: `render_diagnostic.py`. Existing check_results.py, live run_matrix.sh, and live harness were left unchanged. The extension checks all completed workloads rather than only D and adds effective settings, gateway counts/signatures, planned workload SHA, baseline/binary drift, post-checkpoint WAL, merge state and telemetry/time decomposition. The read-only analysis was exercised against the real saved artifacts; audit errors=[] and no failure/cleanup artifacts.')
p()
p('Diagnostic scripts were copied only to new filenames in the authorized roots. A reproducible read-only refresh, if requested, is: run `nice -n 19 python3 ~/claude_checks/ycsb_matrix_20261020/diagnose_matrix.py sampler` on .247 and capture its compact stdout as .111 `evidence/phase1b/sampler_summary.json`; run `nice -n 19 python3 ~/claude_ctl/ycsb_matrix_20261020/diagnose_matrix.py artifacts` on .111. Rendering reads that fixed audit. Do not invoke run_matrix.sh, repeat_anchor.py, any disk probe, or any DB restart for this diagnostic. Small evidence copies are also beside this local report in `phase1b_evidence/`.')
report=re.sub(r'(?m)^(#{1,6} .*)\n(?!\n)',r'\1\n\n','\n'.join(lines)+'\n')
(E/'DIAG_REPORT.md').write_text(report);print(json.dumps(dict(report=str(E/'DIAG_REPORT.md'),rows=len(rows),errors=d['errors'],projection=eta),indent=2))
