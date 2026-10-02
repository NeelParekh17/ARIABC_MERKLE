TPC-C sweep v2 runs entirely on ranking (`protectdr@10.129.7.57`). No rebuild.

Use the headline run's `install_v2` and jitter server/gateway, verified against
`tpcc_v2_20261002_051210/provenance/binaries.sha256`. The campaign directory owns
all new scripts, workloads, logs and PGDATA. Existing source/install/run trees are
read-only inputs. Ports: 55439, 18100, 19100. Lock: `~/claude_checks/tpcc_v2.lock`.

Configuration: FF90 on all nine tables, SERIALIZABLE with pg client retries,
durability on, synchronous Merkle at 16384/1/16, fanout 32, split 1024/merge 256.
The continuation harness is preserved under `provenance/continuation_original.sh`;
its campaign diff adds the campaign path, item FF90 and pg block-size flag.
The pg server uses dbType=0, safedb=0, pool=workers, BCDB workers=1, max retries=100.

The literal requested matrix has 36 unique runs, not 38:
`(7 warehouse points + 6 worker points - 1 shared point) * 3 modes`.
Points alternate between sweeps; at each point run pg, det, Merkle in that order.
The shared W=100/32 run appears in both sweep CSVs without a duplicate execution.

Stage commands on ranking (ROOT must already contain scripts and provenance):

```bash
tmux new-session -d -s tpccsmoke "nohup bash $ROOT/scripts/sweep_v2.sh $ROOT smoke >$ROOT/smoke.log 2>&1 </dev/null"
# Wait for smoke.ok before launching the matrix.
tmux new-session -d -s tpccsweep "nohup bash $ROOT/scripts/sweep_v2.sh $ROOT matrix >$ROOT/campaign.log 2>&1 </dev/null"
```

Both smoke cases use W=5, workers=32, 2000 transactions. Matrix cases use 20000.
Each run logs uptime/top CPU consumers, settings, relation options, WAL/tx,
final HOT statistics, gateway terminal-result evidence and the published state
projection. Det/Merkle compare at the same W, including across worker counts.
The projection is the published eight-table count/64-bit-hash-sum check, not a
cryptographic hash of every column; timestamp fields are excluded. Pg may differ.

An isolated operational/completion failure is logged in failures.jsonl, retains
PGDATA, and gets one end-of-matrix retry. Integrity, FATAL/PANIC, state, setting,
binary, occupied-port or cleanup failures stop immediately. Accepted stopped
PGDATA alone is removed to bound disk use; evidence remains. Failed retries are
recorded; a point without any accepted attempt prevents COMPLETE.

After initial failures are retried, flag TPS below 0.65 times the lower immediate
neighbour (endpoints use one neighbour). Evaluate each sweep/mode separately.
Rerun flags once, deduplicating the shared point; any failure retry consumes that
point's retry budget. Retain both accepted values, report best, mark rerun.

CSV column prefixes match the published warehouses_w32/workers_w100 files.
Extra columns include rerun/reason, WAL bytes per transaction, restarts and HOT
fractions for warehouse/district/customer/stock. All table HOT fractions are in
accepted.json. Root all_runs.csv and summary.csv combine the sweeps. Failed
attempts remain in failures.jsonl and their run logs. Summaries update after
each accepted run and at completion/failure. Recreate them with:

```bash
python3 "$ROOT/scripts/sweep_summary.py" summarize "$ROOT"
cat "$ROOT/status.txt"
tail -30 "$ROOT/status_history.txt"
tail -30 "$ROOT/campaign.log"
```

The single trial and conditional reruns cannot remove all shared-host noise.
Final graphs/publication should use accepted CSVs only after COMPLETE; this task
prepares and launches the campaign, then exits without waiting for the matrix.
