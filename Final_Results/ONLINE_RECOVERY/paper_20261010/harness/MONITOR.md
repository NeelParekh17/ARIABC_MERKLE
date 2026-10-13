# RECOV-1 monitoring and resume

Controller host: `neel@10.129.27.111`.
Campaign root: `/home/neel/ariabc_data/paper_20261010_recov` (`B` below).
Frozen engine: `16577072ecf888fbbbb5d80d0e90fc340c8ca342`.

Inspect the detached matrix without attaching:

```bash
ssh neel@10.129.27.111 'B=/home/neel/ariabc_data/paper_20261010_recov; cat "$B/results/status.json"; tail -8 "$B/results/controller_matrix.log"; tmux list-sessions'
```

Matrix session: `recov_paper_20261010`. Smoke sessions have a `_smoke` suffix.
Status JSON gives phase, state, current unique run ID, accepted/rejected/noise
counts, scenario, repetition and attempt. `status.txt` preserves transitions.
`matrix_plan.json` specifies 27 accepted targets: nine modes, three rotated
blocks. A, C, B, M_passive, M_active, M_mixed, L, L_mix and C_deep rotate by
three positions per block. Smoke trials are separate from matrix statistics.

After a stopped controller, review the failed attempt and confirm campaign
ports 5448/9018/8018/8019 are free on all replicas. Do not stop processes by
port or broad name. `stop_server.sh` only signals the recorded campaign PID
when its executable matches the campaign server. PostgreSQL cleanup uses
only the campaign PGDATA. Never restart canonical services.

Resume the matrix after resolving a documented failure:

```bash
ssh neel@10.129.27.111 'B=/home/neel/ariabc_data/paper_20261010_recov; tmux new-session -d -s recov_paper_20261010 "cd $B; exec python3 controller.py --phase matrix --resume >> results/controller_matrix.log 2>&1"'
```

The controller holds an exclusive lock. Resume skips accepted scenario/rep
pairs, increments attempt IDs, and preserves incomplete directories. Each
invocation permits three additional attempts per target, then stops for
review; failed cleanup also stops immediately. It never silently accepts a
failed run or reduces the planned repetitions.

Raw evidence: `$B/repo/scripts/bench_full_results/<run_id>/`. Each directory
contains gateway and replica logs, configurations, workload/provenance hashes,
three final readbacks, geometry/durability, fault timeline, completion gaps,
health samples, and `acceptance.json`. Bulk phase traces stay on `.111`.
`$B/results/matrix_attempts.jsonl`, `matrix_all_attempts.csv`,
`matrix_summary.csv`, and `matrix_aggregate.json` retain attempts and derive
accepted-only median/min/best/n and signed TPS delta against accepted A.
Event metrics include cut/repair/catchup/total, differing partitions/leaves,
fetched/deleted/upserted rows and full-table copies. Trace counters are
recorded when available, including nonzero values.

Acceptance requires 160000/160000 quorum completions, successful all-three
result audit, zero terminal divergence/permanent failures/timeouts/missing
votes, gateway/runner/collection exit zero, identical final table counts,
roots, data digests and passing `merkle_verify()` on all three nodes. Fault
runs additionally require successful SERIALIZABLE injection and successful
live recovery on the requested target; passive requires `merkle_compare`.
Geometry must be F32/S1024/M256/P200 and SERIALIZABLE, with durability on.

Health sampling records free/available memory, swap IO, load, CPU/disk counters,
boot identity, clocksource and committed watermark lag. `.111` must use `tsc`.
The fixed noise rule excludes an otherwise-valid trial if any host incurs
more than 64 MiB cumulative swap input plus output during the gateway/all-three
result-drain interval. Such attempts remain raw and trigger extra trials.
Completion gaps themselves are recovery measurements, not noise exclusions.
`.246` is memory constrained and its sampled lag/memory must be reviewed.

Campaign Kafka: existing Kafka 3.7.0 software, isolated configuration and data,
512 MiB heap per broker, ports 9192 and leader controller 9193. If a host
reboots, restart only this campaign broker with its existing cluster identity
and data; do not rerun the formatting setup script. Check binaries against
`$B/provenance/BINARIES.sha256` and preserve the reboot evidence before resume.

Curated fetch (preserves logs/configs/health, omits bulk transaction traces):

```bash
ssh neel@10.129.27.111 'python3 /home/neel/ariabc_data/paper_20261010_recov/preparation/curate_manifest.py'
rsync -a --exclude='tx_latency*' --exclude='bcdb_phase_trace_node*' neel@10.129.27.111:/home/neel/ariabc_data/paper_20261010_recov/repo/scripts/bench_full_results/cluster4_paper_20261010_recov_RUN/ Final_Results/ONLINE_RECOVERY/paper_20261010/runs/cluster4_paper_20261010_recov_RUN/
```

Verify `sha256sum -c CURATED.sha256` inside each fetched completed run. Fetch
`results/`, current harness patches and provenance separately. Preparation
failures and smoke failures remain on `.111` and must not be paper trials.
The old unprioritized slow-reference measurements are archived history; the
frozen current code sorts reference candidates by their committed watermark
and exposes no harness switch reproducing the old path.

Phase 1 stops after detached launch and initial acceptance checks. Phase 2
must verify the completed three-block matrix, rerun excluded/failed points,
and produce final publication figures and analysis. Smoke TPS is not a
three-repetition estimate.
