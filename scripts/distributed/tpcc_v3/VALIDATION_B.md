# Agent B remote validation plan (integrator executes)

No database, server, gateway, build or SSH was run by Agent B in this sandbox.
These commands are for Claude/the integrator after Agent A and B changes are merged.
Execute all runtime/build commands on ranking as protectdr. Do not use any other
host. Keep all outputs under new `~/claude_checks/v3/B_*` paths. The canonical
Desktop database and existing published inputs stay untouched.

## 1. Stage the merged source, then enter ranking

Run this transfer from the integrator's workstation, not from Agent B's sandbox.
It includes `.git` so source provenance works; the runtime scripts also hash the
new untracked contract files. This is a fresh destination, never an overwrite.

```bash
set -euo pipefail
B_STAMP=$(date -u +%Y%m%dT%H%M%SZ)
printf '%s\n' "$B_STAMP" > /tmp/tpcc_v3_B_stamp
ssh protectdr@10.129.7.57 "test ! -e \"\$HOME/claude_checks/v3/B_src_$B_STAMP\" && mkdir -p \"\$HOME/claude_checks/v3/B_src_$B_STAMP\""
rsync -a --exclude=Final_Results/ --exclude=scripts/bench_full_results/ \
  --exclude=.bench_tmp/ --exclude=graphify-out/ --exclude=ariabc_pg/build/ \
  --exclude='__pycache__/' --exclude='*.zip' \
  /work/ARIABC/AriaBC/ protectdr@10.129.7.57:claude_checks/v3/B_src_$B_STAMP/
printf 'Ranking stamp: %s\n' "$B_STAMP"
ssh protectdr@10.129.7.57
```

In the ranking shell, paste the printed stamp in the first command. Agent A's
readiness must identify the PostgreSQL install matching its final changes. If
Agent A changed PG C code, its new PG install must also contain pg_prewarm; never
substitute install_v2 for that case. Agent A's Python loader requires psycopg2
or psycopg in the ranking Python environment.

```bash
set -euo pipefail
export B_STAMP=PASTE_PRINTED_STAMP
export V=$HOME/claude_checks/v3
export SRC=$V/B_src_$B_STAMP
export HERE=$SRC/scripts/distributed/tpcc_v3
# Integrator creates/validates A_READY from Agent A's report before this step.
test -f "$V/A_READY"
cat "$V/A_READY"
source "$V/A_READY"
export INST
: "${INST:?A_READY must set the validated PG install}"
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
export LC_ALL=C TZ=UTC
export CPUSET=0-47
free -g
lscpu
python3 - <<'PY'
import os
assert set(range(48)) <= set(os.sched_getaffinity(0)), 'Choose one allowed fixed CPUSET and record it for ALL attempts'
try:
    import psycopg2
except ImportError:
    import psycopg
PY
# No database/server is started by these checks.
for script in "$HERE"/*.sh; do bash -n "$script"; done
python3 -m py_compile "$HERE"/*.py
python3 "$HERE/selftest_summary.py"
git -C "$SRC" diff --check
```

Expected: synthetic self-test prints `PASS: ...`; bash/py_compile/diff checks exit
zero. Confirm `lscpu` shows one NUMA node and memory is sufficient (the harness
requires MemAvailable >= twice the requested shared_buffers). If CPUs 0–47 are
unavailable, select another fixed allowed 48-CPU set and set CPUSET before every
following command. Do not alter another user's affinity/processes.

## 2. Build the merged gateway/server into a new directory

```bash
export BUILD=$V/B_build_$B_STAMP
test ! -e "$BUILD"
mkdir "$BUILD"
cmake -S "$SRC/ariabc_pg" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release \
  -DLIBPQ_INCLUDE_DIR="$INST/include" -DPOSTGRES_INCLUDE_DIR="$INST/include" \
  -DLIBPQ_LIBRARY="$INST/lib/libpq.so" \
  -DRDKAFKA_INCLUDE_DIR="$HOME/Desktop/rdkafka_local/include" \
  -DRDKAFKA_LIBRARY="$HOME/Desktop/rdkafka_local/lib/librdkafka.so" \
  >"$V/B_configure_$B_STAMP.log" 2>&1
cmake --build "$BUILD" --target ariabc_pg_gateway ariabc_pg_server -j8 \
  >"$V/B_build_$B_STAMP.log" 2>&1
export BINDIR=$BUILD/bin
sha256sum "$INST/bin/postgres" "$BINDIR/ariabc_pg_server" "$BINDIR/ariabc_pg_gateway" \
  >"$V/B_BINARIES_$B_STAMP.txt"
cat "$V/B_BINARIES_$B_STAMP.txt"
"$BINDIR/ariabc_pg_gateway" --help >"$V/B_gateway_help_$B_STAMP.txt"
python3 - "$V/B_gateway_help_$B_STAMP.txt" <<'PY'
from pathlib import Path
import sys
assert '--progressIntervalMs' in Path(sys.argv[1]).read_text()
PY
```

Expected: both targets built, gateway help lists `--progressIntervalMs`, hashes
saved. Keep configure/build logs with campaign provenance. Run the integrator's
required PG build/checks separately if Agent A changed PostgreSQL source; those
must also occur on ranking in new A-owned directories.

## 3. Short end-to-end smoke, every mode, isolated B ports

This smoke also estimates rates at the final W/workers. A reduced submission
window makes a small N usable without filling 65536 outstanding requests before
warmup finishes. Its measurements are not final campaign results.

```bash
export RUNROOT=$V/B_smoke_$B_STAMP
export PORT=55459 CLIENT_PORT=18120 RAFT_PORT=19120
export SMOKE=1 SHARED_BUFFERS=8GB WARMUP_S=2 MIN_WINDOW_S=5
export CHECKPOINT_TIMEOUT=30s MAX_WAL_SIZE=64GB
export DET_WINDOW=1024 DET_PIPELINE_DEPTH=16
export SEED=42 FILLFACTOR=90 SPLIT=1024 MERGE=256 P=16384 PKC=1 SUB=16
export MODES='pg det merkle' TRIALS=1 GATEWAY_TIMEOUT_S=1800
free -g
set +e
bash "$HERE/campaign.sh" 5:32:40000 >"$V/B_smoke_$B_STAMP.log" 2>&1
B_SMOKE_RC=$?
set -e
printf 'smoke shell rc=%s\n' "$B_SMOKE_RC"
cat "$RUNROOT/summary.md"
```

Expected: fresh pg/det/merkle attempt dirs, gateway rc zero, explicit abort counts
matching the generator metadata, `consistency_ok=t` and conditions 1–4 true,
settings matching autovacuum on/SERIALIZABLE/fsync/etc, nine `table|t` Merkle rows,
complete initial/final hashes including item and timestamps, and state parity PASS.
Smoke shell rc zero relaxes only 300 s and checkpoint presence; strict acceptance
JSON still rejects a <300 s window. A checkpoint may occur in this smoke; its
presence and UTC timestamp must be reported accurately. Other failures are real.

Run this independent cross-check against actual rows and count deltas:

```bash
python3 - "$RUNROOT" <<'PY'
import csv,json,math,sys
from pathlib import Path
root=Path(sys.argv[1])
for run in sorted(p.parent for p in root.glob('*/config.json')):
    a=json.loads((run/'acceptance.json').read_text())
    cfg=json.loads((run/'config.json').read_text())
    w=json.loads((run/'window.json').read_text())
    rows=list(csv.DictReader((run/'samples.csv').open()))
    start,end=w['start'],w['end']
    assert len(rows)>=7
    assert start['elapsed_s']>=cfg['warmup_s']
    assert end['sent']<end['total'], 'drain must be excluded'
    assert any(float(r['sent'])>=float(r['total']) for r in rows if r['sent'])
    duration=end['wall_epoch']-start['wall_epoch']
    completed=end['completed']-start['completed']
    aborted=end['user_aborts']-start['user_aborts']
    outcomes=completed if w['completed_includes_aborts'] else completed+aborted
    committed=outcomes-aborted
    neworders=end['district_next']-start['district_next']
    assert math.isclose(w['duration_s'],duration)
    assert math.isclose(w['tps'],outcomes/duration)
    assert math.isclose(w['nopm'],60*neworders/duration)
    assert w['committed_delta']==committed
    wal=a['wal']
    assert wal['record_bytes']+wal['fpi_bytes']==wal['combined_bytes']
    assert math.isclose(wal['record_bytes_per_committed_tx'],wal['record_bytes']/committed)
    assert math.isclose(wal['fpi_bytes_per_committed_tx'],wal['fpi_bytes']/committed)
    def lsn(s):
        hi,lo=s.split('/'); return int(hi,16)*2**32+int(lo,16)
    assert wal['combined_bytes']<=lsn(end['wal_lsn'])-lsn(start['wal_lsn'])
    log=(run/'postgres_workload.log').read_text()
    for e in a['checkpoints_in_window']:
        assert start['wall_epoch']<=e['wall_epoch']<=end['wall_epoch']
        assert e['line'] in log
    assert a['acceptance']['smoke']
    mandatory_fail=set(a['acceptance']['failed_checks'])-{'window_300s','time_driven_checkpoint'}
    assert not mandatory_fail, (run,mandatory_fail)
    assert all(float(r['progress_age_s'])<=2.5 for r in rows if r['progress_age_s'])
    print(run.name, 'window', duration, 'outcomes',outcomes,'committed',committed,
          'NOPM',w['nopm'],'WAL record/FPI',wal['record_bytes'],wal['fpi_bytes'],
          'checkpoints',len(a['checkpoints_in_window']),'noise',a['noise_flags'])
parity=json.loads((root/'state_parity.json').read_text())
assert len(parity)==1 and parity[0]['same_workload_and_state']
assert len(list(csv.DictReader((root/'results.csv').open())))==3
assert (root/'summary.md').exists()
print('PASS: independent sampled-window/count/NOPM/WAL/checkpoint/acceptance/summary checks')
PY
```

Keep `samples.csv`, `gateway.log`, `walstats.txt`, `window.json`,
`postgres_workload.log`, `settings.csv`, `relations.csv`, `indexes.csv`,
`consistency.txt`, `merkle_verify.txt`, initial/final hashes, `acceptance.json`,
`provenance.json`, `results.csv`, `summary.md` and build logs. Do not overwrite
failed smoke attempts. If 40000 transactions cannot produce the 5 s smoke window,
create another fresh B_smoke root and increase N; return that failure and its
logs to Agent B as well. Do not relax counter/consistency/WAL/settings checks.

## 4. Recommend N from matched smoke rates

```bash
export B_SMOKE_ROOT=$RUNROOT
mapfile -t B_RATES < <(python3 - "$B_SMOKE_ROOT" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1]); found={}
for f in root.glob('*/acceptance.json'):
    r=json.loads(f.read_text()); found[r['mode']]=r['window']['tps']
assert set(found)=={'pg','det','merkle'}
for mode in ('pg','det','merkle'): print(f'{mode}={found[mode]}')
PY
)
python3 "$HERE/recommend_n.py" --rates "${B_RATES[@]}" \
  --warmup 60 --window 300 --drain 60 --inflight 65536 --margin 1.25 \
  >"$V/B_recommended_N_$B_STAMP.json"
export B_FINAL_N=$(python3 - "$V/B_recommended_N_$B_STAMP.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['count'])
PY
)
cat "$V/B_recommended_N_$B_STAMP.json"
```

The final larger submission pipeline can change rates relative to smoke. N is
only a conservative estimate. A final measured window below 300 s is a rejection,
not publishable evidence. Re-estimate using those rates in a fresh campaign root
if necessary; keep every prior attempt. A checkpoint absence also rejects a run.

## 5. Final campaign: W=5, workers=32, one trial per mode

```bash
export RUNROOT=$V/B_final_$B_STAMP
export PORT=55439 CLIENT_PORT=18100 RAFT_PORT=19100
export SMOKE=0 SHARED_BUFFERS=32GB WARMUP_S=60 MIN_WINDOW_S=300
export CHECKPOINT_TIMEOUT=5min MAX_WAL_SIZE=64GB
export DET_WINDOW=65536 DET_PIPELINE_DEPTH=1024
export CPUSET=0-47
export SEED=42 FILLFACTOR=90 SPLIT=1024 MERGE=256 P=16384 PKC=1 SUB=16
export MODES='pg det merkle' TRIALS=1 GATEWAY_TIMEOUT_S=7200
free -g
set +e
bash "$HERE/campaign.sh" "5:32:$B_FINAL_N" >"$V/B_final_$B_STAMP.log" 2>&1
B_FINAL_RC=$?
set -e
printf 'final campaign rc=%s\n' "$B_FINAL_RC"
cat "$RUNROOT/summary.md"
cat "$RUNROOT/campaign_acceptance.json"
python3 - "$RUNROOT" <<'PY'
import csv,json,sys
from pathlib import Path
root=Path(sys.argv[1])
assert json.loads((root/'campaign_acceptance.json').read_text())['accepted']
rows=list(csv.DictReader((root/'results.csv').open()))
assert len(rows)==3 and {r['mode'] for r in rows}=={'pg','det','merkle'}
for f in root.glob('*/acceptance.json'):
    r=json.loads(f.read_text())
    assert r['acceptance']['accepted'] and not r['acceptance']['smoke'], r['acceptance']
    assert r['window']['duration_s']>=300
    assert any(c['time_driven'] for c in r['checkpoints_in_window'])
    assert r['settings']['actual']['autovacuum']=='on'
    print(f.parent.name,'TPS',r['window']['tps'],'NOPM',r['window']['nopm'],
          r['nopm_label'],'noise',r['noise_flags'])
print('PASS: complete final campaign with strict acceptance and matched det/Merkle states')
PY
```

Expected: shell rc zero, exactly one attempt per mode, each strict acceptance
true, sampled window >=300 s, >=one time-driven checkpoint inside each window,
explicit zero permanent failures/divergence, all consistency checks true,
Merkle verification true, complete N/expected abort accounting, record/FPI bytes
per committed transaction, every trial/noise flag listed, qualified NOPM label,
and identical initial/workload/final hashes for det and Merkle. Inspect the raw
checkpoint lines and redo the independent arithmetic from step 3 against final
windows (replace its smoke assertion with `assert not a['acceptance']['smoke']`
and permit no failed checks). One trial cannot quantify variability: report this
limitation even when acceptance passes. Send Agent B failed check names and their
attempt evidence paths/logs for fixes; do not silently retry or drop results.
