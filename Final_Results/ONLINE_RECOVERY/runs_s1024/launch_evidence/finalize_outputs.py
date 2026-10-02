import pathlib,csv,json,shutil,hashlib,collections
repo=pathlib.Path('/work/ARIABC/AriaBC')
b=repo/'.bench_tmp/recovery_s1024_20261001T060514ZJ'
tag='20261001T060514ZJ'
rbase='/home/neel/Desktop/recovery_s1024_'+tag
rows=[r for r in csv.DictReader((b/'comparison.csv').open()) if r['dataset']=='new']
order={'A':0,'B':1,'C':2,'M_mixed':3,'L_mix_prio':4}
rows.sort(key=lambda r:(0 if r['group']=='f32s1024' else 1,order[r['scenario']]))
a=json.loads((b/'execution_annotations.json').read_text()); annotations={x['run']:x for x in a}
measured='''| Geometry | Scenario | TPS | Signed delta vs A | Cut ms | Repair ms | Catchup ms | Total ms | Nodes / leaves | Phase-8 |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n'''
work='''| Geometry | Scenario | Bad partitions | Differing leaves | Candidate rows | Upserted | Deleted | Full copies | Empty 100ms buckets | Max gap ms | Donor | Injector retries |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n'''
for r in rows:
 g='F32/S1024/M256' if r['group']=='f32s1024' else 'F4/S32/M8'
 nodes='200 / 200' if r['group']=='f32s1024' else '1000 / 800'
 measured+=f"| {g} | {r['scenario']} | {float(r['tps_majority_visible']):.2f} | {float(r['overhead_vs_baseline_pct']):+.2f}% | {r['cut_ms']} | {r['repair_ms']} | {r['catchup_ms']} | {r['total_recovery_ms']} | {nodes} | PASS |\n"
 x=annotations[r['run_id']]; donor=','.join(x['refs']) or '—'; retry=max(0,x['injection_attempts']-1)
 work+=f"| {g} | {r['scenario']} | {r['mismatched_partitions']} | {r['differing_leaves']} | {r['candidate_rows']} | {r['rows_upserted']} | {r['rows_deleted']} | {r['full_copies']} | {r['empty_100ms_buckets']} | {float(r['max_completion_gap_ms']):.2f} | {donor} | {retry} |\n"
intro='''Task I prepared the source analysis and conditional models below. Task J
completed the eight requested remote trials on 2026-10-01. **Measured new
F32/S1024/M256 baseline: 8,609.56 TPS; same-current-binary F4/S32/M8 control:
8,848.09 TPS.** New C / M_mixed / L_mix_prio repair took **60 / 93 / 95 ms**.
All eight final all-three audits passed. These are single trials, with no
consistent measured throughput gain. The conditional estimates below are
retained as pre-rerun models; the measured results at the end supersede them.
'''
p=repo/'Final_Results/ONLINE_RECOVERY/analysis_s1024.md'; text=p.read_text(); start=text.index('Task I, 2026-10-01.'); end=text.index('\nThe existing fault injector',start); text=text[:start]+intro+text[end:]
text=text.replace('there\nare no matched cluster profiles to calibrate f. Keep measured cells pending.','the\npre-rerun model had no matched cluster profiles to calibrate f. The completed\nmeasurements below did not establish this modeled gain.')
start=text.index('## Rerun design, constraints and pending measurements'); end=text.index('Correctness limits in the current code deserve explicit validation:',start)
text=text[:start]+'''## Completed rerun: scope and correctness limits

Task J followed the user override to the Task I runbook: native U24 builds on
10.129.27.111 and native U22 builds on 10.129.148.246, rather than builds on
.247 or container builds. No local compilation, database or benchmark ran.
The frozen current working tree, ABI-specific installations, libraries,
PGDATA, Raft directories and generated runners all live within the same
fresh remote base, `/home/neel/Desktop/recovery_s1024_20261001T060514ZJ`.
PG used 5448, Raft 9018 and client listeners 8018/8019. Existing Kafka brokers
were used at the published cluster addresses (.247/.246/.248:9092); only
experiment topics ending `20261001T060514ZJ` were created. No broker was
restarted or reformatted. Heavy work on .247 began only after the local OOM
status contained `CHAIN_DONE`: gate observed 07:00:34 UTC, deployment began
07:00:49 UTC. Other-host preparation proceeded while waiting.

One timed trial per requested scenario/geometry ran sequentially, with
160,000 YCSB transactions, 96 lanes, eight workers, Kafka majority_async_all3,
32MB shared_buffers, and the published workload/ordering/pipeline flags.
The generated runner and exact command are retained in every run directory.
Workload and generated fault injector transactions used SERIALIZABLE;
whole-transaction serialization retries were retained. All three final GUC
readbacks show SERIALIZABLE default isolation, synchronous Merkle maintenance,
fsync=on, synchronous_commit=on and full_page_writes=on. Canonical row hashing
and recovery source were not changed. F32/S32/M8 was not requested and was
not run; control B and L_mix_prio were not requested either.

'''+text[end:]
text=text.replace('No isolation or recovery source was edited in Task I.','No isolation or recovery source was edited in Task I or J.')
text=text.replace('Compact node storage rebuild is unvalidated in the shared tree. Establish\n  full hash/root/heap consistency remotely before interpreting faster TPS.','Compact node storage rebuild received remote initial restore/REINDEX and\n  final heap/hash/root checks in these runs. This is bounded evidence for\n  this 12k-row workload, rather than exhaustive split/merge/recovery safety.')
start=text.index('| Geometry | Scenario | Repetitions |'); text=text[:start]+'''## Measured results (Task J)

Signed delta follows the published convention: negative means lower TPS.
Every row is one timed trial, not a multi-trial median. A_r1 and A_r2 stopped
before any database startup/workload due to preparation errors (missing
output parent, then incompatible skip-cleanup/fresh Raft storage). Both are
preserved; A_r3 was the first measured new baseline. This was not selection
among baseline samples.

'''+measured+'''
Fault-work and availability counters:

'''+work+'''
The [comparison CSV](runs_s1024/comparison.csv) includes all twelve published
rows and all eight new rows, signed baseline overhead, published-to-new TPS
changes, and recovery/audit evidence. Published candidate-row and full-copy
fields missing from summary.csv remain UNKNOWN in that CSV; corresponding
candidate rows used below were re-extracted from the archived repair logs.
The [comparison figure](graphs/s1024_comparison.png) shows TPS, cut, repair
and catchup against the published results.

### What these measurements support

- New baseline TPS was 2.72% below published A and 2.70% below the current-code
  F4 control. New C and M_mixed TPS were 1.51% and 3.55% above their current-code
  controls, respectively. B and fault scenarios exceeding the new A sample
  do not establish negative recovery overhead. Single trials in fixed order
  cannot separate geometry effects from resource variation or noise.
- Measured native geometry matches the model: new has 200 root/leaves, no
  internal nodes, about 60 rows/leaf and 106,496 node-storage bytes; control
  has 1,000 nodes / 800 leaves / 200 internal nodes, about 15 rows/leaf and
  270,336 bytes. At the final 12,001 rows, new occupancy is 42–80 rows/leaf;
  control is 4–27. Larger leaves expanded candidate transfer: C 1,835 vs
  control 654 (2.81x), mixed 3,723 vs control 1,129 (3.30x). This confirms
  reduced tree size together with greater repair transfer work.
- Published C / M / L repair was 59 / 88 / 115 ms; new was 60 / 93 / 95 ms.
  New leader repair touched 45 upserts versus published 167, and 65 mismatched
  partitions versus 132. Its lower repair latency cannot be assigned to
  geometry alone. Random selected fault keys and injection timing differ.
- All new/control follower repairs selected donor node 2; published follower
  C/M selected donor node 1. Prioritized leader repair selected donor node 4.
  The donor difference further limits published-to-new phase comparisons.
- Control mixed injection committed on attempt nine after eight SERIALIZABLE
  failures, about 7.41 seconds after its first scheduled 5s attempt. Its cut
  was at L=546 / B=139,519 versus new mixed L=272 / B=69,375. Actual replay
  was 80 entries (547..626), 444 ms, versus 354 entries (273..626), 8,186 ms
  for new mixed. Control's 445 ms catchup is therefore a different recovery
  window after a late fault; it is not evidence of faster geometry replay.
  Control C and new L each needed two injector retries; new C/M succeeded
  immediately. Every successful fault case returned LIVE with one successful
  recovery event and no full copy.
- New C had one empty 100ms completion bucket and a 212.30 ms maximum gap,
  against published 0 / 91.89 ms and control 0 / 139.62 ms. Control mixed
  had one empty bucket / 139.20 ms gap. These stalls remain in the results.
- User4 had 6,935–7,186 MiB available RAM at the before/after samples, with
  pre-existing swap use of 1,783–1,785 MiB. System-wide swap-in deltas per
  new A/B/C/M/L were 430/94/214/294/81 pages; control A/C/M were 12/110/7.
  Swap-out was zero except three pages during control C. These samples span
  setup/run/cleanup and cannot attribute all swap activity to this workload.
  No experiment OOM or severe new swapping was observed, but shared-host
  memory activity is a remaining performance confound.

### Acceptance and preservation evidence

All eight runs completed 160,000 client transactions with divergence_count=0,
permanent_failures=0, no missing completions/timeouts, provenance PASS and
Phase-8 all-three Merkle/root/heap PASS. Async verified plus recovery-attributed
completions account for the 160,000 transactions. Final state on every node
was 12,001 rows, root
`80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5`,
heap digest `9a15b0794b52e087376bc7190d48cc58`, verify=t, matching the
published final state. Fresh initial restore/REINDEX audits gave 12,000 rows,
root `0df84e84229328afb8f70564f63a5ed45fd1cd78287b1c97ff35be39baa8080f`
and heap digest `99091c724b1ff4908f315b65614f378b`, matching published initial
state. Each fault case has an actual nonzero trigger, incremental repair,
catchup and return-to-LIVE trace. These successful cases do not validate all
failure paths listed above.

Frozen source fingerprint on all four hosts:
`60cae0f48ccd4cb4797c671757e6af640c0512ec827598fca234a338c6390490`.
Git status, full working-tree diff and diff SHA, source manifests, ABI-specific
binary manifests, native build logs and ldd checks are retained. No missing
runtime libraries were found. A staging exclusion initially omitted
src/benchmark/requirements.txt; the exact frozen file was restored before
runs and source/binary manifests revalidated without a fingerprint override.

The generated adapter removed shared-port kills, scoped server shutdown to
recorded executable-validated PIDs, enabled isolated Raft cleanup, and placed
server/gateway logs within RBASE. Original runners and shared source edits
were preserved. At completion all experiment servers/PG instances were
stopped and ports 5448/9018/8018/8019 were clear on all four hosts. Canonical
before/after hash/PID/listener inventories were identical on all hosts;
SHA-256 verification found zero changes among 582 pre-existing files under
published runs, summary.csv, graphs and Report.md. Only the new comparison
figure was added under graphs.

Accepted run directories are in [runs_s1024](runs_s1024/), with separate
failed_setup archives and provenance. Full local artifacts and timestamped
remote-command log remain at
`/work/ARIABC/AriaBC/.bench_tmp/recovery_s1024_20261001T060514ZJ/`.
The remote RBASE and ten suffixed Kafka topics remain for retention. Exact
execution and retirement commands are in
[Task J report](../../.bench_tmp/codex_tasks/report_J.md).
'''
p.write_text(text)
# Exclusive copy of every accepted run; never merge with an existing directory.
out=repo/'Final_Results/ONLINE_RECOVERY/runs_s1024'; out.mkdir(exist_ok=True)
for d in sorted((b/'runs').iterdir()):
 if d.is_dir(): shutil.copytree(d,out/d.name)
failed=out/'failed_setup'; failed.mkdir()
for d in sorted((b/'failed_runs').iterdir()):
 if d.is_dir(): shutil.copytree(d,failed/d.name)
prov=out/'provenance'; prov.mkdir()
for name in ['commands.log','context.json','git_head.txt','git_status.txt','uncommitted_diff.patch','diff.sha256','frozen_fingerprint.txt','frozen_source_manifest.txt','local_fingerprint_after_builds.txt','ring_capacity.txt','oom_gate.log','oom_gate_open.txt','preservation_check.json','published_protected_hashes.json','user4_memory.json','execution_annotations.json','build.sh','run_case.sh','stop_server.sh','adapt_runner.py','geometry.sql','audit_ssh.py']:
 shutil.copy2(b/name,prov/name)
for name in ['comparison.csv','comparison.md','acceptance.json']:
 shutil.copy2(b/name,out/name)
# Detailed report, concise enough to use as the orchestration handoff.
report=f'''# Task J — executed online-recovery rerun

Completed all eight requested single-trial cases remotely on 2026-10-01;
1,280,000 timed YCSB transactions total. Every run has all-three final
Merkle/root/heap PASS, divergence_count=0, permanent_failures=0, complete
client accounting, provenance PASS and full_copies=0. Every fault has an
actual successful incremental recovery event ending LIVE. No core source
was edited by Task J. Single trials show no consistent throughput advantage.

## Results

'''+measured+'''
Published A/B/C/M/L TPS: 8850.05 / 8829.05 / 8692.82 / 8686.21 / 8673.50.
Published C/M/L cut/repair/catchup/total ms:
2741/59/7970/10824; 2759/88/8094/10997; 2777/115/7413/10363.
New baseline is 2.72% below published and 2.70% below current-code control.
The positive signed deltas for new B/faults reflect unquantified trial/order
variation, not demonstrated negative recovery overhead.

'''+work+f'''
## Findings and limits

- Current new geometry has 200 nodes/leaves vs control 1000 nodes/800 leaves.
  Candidate transfer grew 2.81x for C and 3.30x for M vs current-code control.
- Control M had eight whole-transaction SERIALIZABLE injector retries:
  successful corruption was ~7.41s later than the first 5s attempt. It replayed
  80 entries/444ms vs new M 354 entries/8186ms. Its short catchup is a changed
  recovery window. Control C and new L needed two retries each.
- New L repaired fewer upserts than published (45 vs 167). All follower
  donors were node 2 vs published node 1; leader donor was node 4. Random
  fault keys, retry timing, donors, code/isolation differences and fixed
  single-trial order limit causal comparisons.
- New C had one empty100ms bucket and max gap212.30ms; control M one/139.20ms.
  User4 available RAM6935–7186MiB, existing swap1783–1785MiB; total system-wide
  swap-in1242pages and swap-out3pages across setup/run/cleanup samples. No
  experiment OOM was observed; host memory activity remains a confound.
- Two setup attempts stopped before PG startup/workload (missing output parent,
  skip-cleanup incompatible with fresh Raft storage). Preserved in failed_setup;
  new A_r3 is the first measured A, rather than a chosen fastest repeat.
- Initial remote restore/REINDEX and final full heap/hash/root audits passed,
  including compact rebuild. This bounded success does not prove all failure
  paths or other workloads safe.

## Artifacts and changes

- Final_Results/ONLINE_RECOVERY/analysis_s1024.md:3 — measured outcome and
  final measured tables/interpretation; Task I conditional models retained.
- Final_Results/ONLINE_RECOVERY/graphs/s1024_comparison.png — TPS/cut/repair/
  catchup figure, visually checked, with delayed-control-M annotation.
- Final_Results/ONLINE_RECOVERY/runs_s1024/ — eight accepted run directories,
  two failed_setup directories, comparison.csv/md, acceptance.json, provenance.
- .bench_tmp/recovery_s1024_{tag}/run_case.sh:30 — exact common flags,
  case-specific injections, topic creation, per-node health/audit/cleanup.
- .bench_tmp/recovery_s1024_{tag}/stop_server.sh:1 — stop only recorded PID
  after verifying its executable is this experiment's server.
- .bench_tmp/recovery_s1024_{tag}/adapt_runner.py:1 — generated-only path
  and shutdown adaptations; no edits to original published runner.
- .bench_tmp/recovery_s1024_{tag}/geometry.sql:1 — native table geometry
  readback; actual table is ariabc_internal.merkle_node_usertable_small
  (src/backend/access/merkle/merkleutil.c:1565), rather than an OID suffix.
- Full local artifacts: {b}
- All remote hosts share RBASE: {rbase}
- Frozen source fingerprint: 60cae0f48ccd4cb4797c671757e6af640c0512ec827598fca234a338c6390490.
  Git status/diff SHA, native U24/U22 binaries/manifests and ldd evidence retained.

## Guardrails and process cleanup

No builds/tests/DBs/benchmarks ran locally. U24 built natively on .111; U22 on
.246. No builds on .247. Heavy .247 deployment began07:00:49UTC after CHAIN_DONE
observed07:00:34UTC. All workload/injector transactions SERIALIZABLE with
whole-transaction retries; all three GUC audits show synchronous Merkle and
fsync/synchronous_commit/full_page_writes=on. Canonical hash/recovery source
unchanged. PG5448, Raft9018, clients8018/8019, own PGDATA/Raft/log paths.
Existing Kafka brokers retained; only ten RTAG topics created.

All own PG/server processes stopped per case using recorded PID/PGDATA.
Final health checks found no experiment pid files or listeners on any host.
Canonical before/after inventories are identical on all four hosts, and
582 existing published artifact SHA256 values remain unchanged. Command log
records host, command, start/end timestamp and exit code for all690 remote
commands (including nested runner SSH/rsync); every start has an end.
No shared-port/process-pattern kills or canonical changes were used.

## Exact execution and analysis commands

The frozen source/build dependencies and generated adapters are already in
RBASE; these are the commands that were executed, not a request to rerun.
Fresh run IDs/topics are required before any future repetitions.

```bash
RBASE={rbase}
ssh -o BatchMode=yes neel@10.129.27.111 "bash '$RBASE/build.sh' '$RBASE' u24"
ssh -o BatchMode=yes neel@10.129.148.246 "bash '$RBASE/build.sh' '$RBASE' u22"
# On gateway .111, sequentially (the retained wrapper logs nested commands):
bash "$RBASE/run_case.sh" 32 1024 256 A 3
bash "$RBASE/run_case.sh" 32 1024 256 B 1
bash "$RBASE/run_case.sh" 32 1024 256 C 1
bash "$RBASE/run_case.sh" 32 1024 256 M_mixed 1
bash "$RBASE/run_case.sh" 32 1024 256 L_mix_prio 1
bash "$RBASE/run_case.sh" 4 32 8 A 1
bash "$RBASE/run_case.sh" 4 32 8 C 1
bash "$RBASE/run_case.sh" 4 32 8 M_mixed 1
# Local data analysis only, from /work/ARIABC/AriaBC; outputs must be fresh:
python3 scripts/distributed/recovery_s1024/compare_recovery.py \\
  .bench_tmp/recovery_s1024_{tag}/runs \\
  --csv .bench_tmp/recovery_s1024_{tag}/comparison.csv \\
  --markdown .bench_tmp/recovery_s1024_{tag}/comparison.md --strict
python3 .bench_tmp/recovery_s1024_{tag}/plot_comparison.py
```

## Retirement instructions (not executed)

RBASE is left intact on all four hosts, along with ten experiment topics.
After retaining desired remote logs, delete only the explicit topics below
using the existing .247 broker CLI, then remove only this exact RBASE.
No canonical cleanup or process kill is needed. If reusing the experiment
before retirement, stop its recorded PIDs again and recheck its ports first.

```bash
# Run this topic loop on .247; do not stop/reformat Kafka.
RTAG={tag}
if ! command -v java >/dev/null; then
  export JAVA_HOME=/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64
  export PATH="$JAVA_HOME/bin:$PATH"
fi
for own_case in f32s1024_A_r1 f32s1024_A_r2 f32s1024_A_r3 \\
  f32s1024_B_r1 f32s1024_C_r1 f32s1024_M_mixed_r1 \\
  f32s1024_L_mix_prio_r1 f4s32_A_r1 f4s32_C_r1 f4s32_M_mixed_r1; do
  /home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh \\
    --bootstrap-server localhost:9092 --delete --topic "recovery_${{own_case}}_${{RTAG}}"
done
# From the workstation, after retention and confirming the experiment is down:
for own_host in 10.129.148.247 10.129.148.246 10.129.148.248 10.129.27.111; do
  ssh -o BatchMode=yes "neel@$own_host" \\
    "rm -rf -- '{rbase}'"
done
```

No additional orchestrator action is needed to complete this rerun. Replicated
trials with controlled injection timing/donors are needed before a performance
claim; they are outside the eight requested trials.
'''
(repo/'.bench_tmp/codex_tasks/report_J.md').write_text(report)
# Verify every copied file byte-for-byte, and the original protected set.
copy_mismatches=[]; copied=0
for srcroot,dstroot in [(b/'runs',out),(b/'failed_runs',failed)]:
 for src in srcroot.rglob('*'):
  if src.is_file():
   dst=dstroot/src.relative_to(srcroot); copied+=1
   if hashlib.sha256(src.read_bytes()).digest()!=hashlib.sha256(dst.read_bytes()).digest(): copy_mismatches.append(str(dst))
protected=json.loads((b/'published_protected_hashes.json').read_text())
changed=[name for name,sha in protected.items() if hashlib.sha256((repo/name).read_bytes()).hexdigest()!=sha]
check={'accepted_runs':len(rows),'copied_files':copied,'copy_mismatches':copy_mismatches,'protected_original_files':len(protected),'protected_mismatches':changed}
(b/'export_check.json').write_text(json.dumps(check,indent=2))
shutil.copy2(b/'export_check.json',prov/'export_check.json')
assert len(rows)==8 and not copy_mismatches and not changed,check
print(json.dumps(check))
