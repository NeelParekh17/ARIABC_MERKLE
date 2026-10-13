FINAL REPORT

Step 2 is launched. Engine commit remains **6a1c66d67b25d6e23f4393c65b59ffdf0e197492**,
branch f15/adhash, clean worktree, no push. Step 1 was not changed or repeated.

Baseline takeover used exactly the approved paper scratch directories;
optional pgdata_plain is used for the det control. Before baseline mutation,
pgdata passed the exact full byte content check against pgdata_base_v2_ff90,
plus matching catalog, sizes, bounds, ff90, index definitions and geometry.
REINDEX only usertable_merkle_idx with the accepted binary, full verification,
CHECKPOINT and clean shutdown completed. Format 11, 100,000,000 rows,
merkle_verify=t, unchanged 28,248,276,992-byte heap and 2,246,197,248-byte primary
index, exactly 211,400 nodes / 204,800 leaves. REINDEX wall time 115.04s; minimum
free disk during monitored REINDEX was 36,163,850,240 bytes. The transient 3GB
exception was not used. The two published baseline control hashes remain
unchanged after launch; neither published baseline nor golden/install/cluster/
source/evidence was modified.

Smoke: A theta .99, worker 1, 20,000 statements, 96 terminals, SERIALIZABLE,
32MB shared buffers, cold start, delta restore and paper settle probe.

| Mode | New TPS | Published median / min / max | New / median |
|---|---:|---|---:|
| Merkle | 1790.83 | 1780.31 / 1776.36 / 1865.67 | 1.00591 |
| det | 1951.03 | 2040.82 / 2038.53 / 2055.29 | 0.95600 |

Merkle smoke is +0.59% versus the published median; det is -4.40%. Diagnostic
smoke drift-adjusted ratio is approximately 1.0522. Both accepted all 20,000
terminals, gateway exit 0, divergence/permanent failures 0, clean noise
qualification, and Merkle verify PASS. Independent raw terminal/SQL-tag audit
passes all 40,000 smoke terminals. A single smoke trial is not a full-grid
median or a causal host-drift estimate.

Grid: 84 Merkle cases (3 trials x 28 points) and 28 det controls (1 per point).
Seeded shuffled configuration blocks and rotated mode order; det controls are
distributed over all three trial blocks. All seven workloads match published
hashes. No change to terminal parsing, cold-reset/settle method, post-run
verification, or paper noise rules. Maintenance memory is capped at 256MB to
satisfy the binding limit, versus the paper's declared 2GB; maintenance is
outside the timed workload. All other measured settings and binaries remain
as recorded. Every PG start records free -g and checks the 5 GiB reserve.

Launch snapshot 2026-10-12T16:25:43.648010+00:00: **3/112 accepted** (3 Merkle / 0 det), 0 noise exclusions. Provisional ETA **2026-10-13 06:21 IST**, from mean observed case spacing 279.69s. Early mode mix and restore/verification phases can change this estimate.

The first three grid cases are accepted and independently audited (60,000
terminals): C/.99/w16 17,167.38 TPS; B/.99/w8 9,770.40 TPS;
A/1.2/w8 5,437.74 TPS. All have Merkle PASS, no failures or exclusions. The next
case is running. The full campaign remains active; we did not wait for it to
finish. Full three-trial medians and matched det-adjusted point conclusions
remain pending.

Detached controller: .111 private tmux session f15oom, socket
/home/neel/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock. The runner uses
--auto-resume with source/binary and saved acceptance rechecks. Status/ETA,
iostat/PSI/clocksource telemetry, preserved noise retries and a disk reserve
watcher remain active. Source contract setup correction for the telemetry
file was made before any gateway measurement and original snapshots retained.

Raw paths:
- .111 /home/neel/claude_ctl/f15_adhash_20261012/results/campaign/run_20261012_213147_24c8a32a/
- .111 /home/neel/claude_ctl/f15_adhash_20261012/results/smoke/run_20261012_211725_e5b80c6e/
- .111 /home/neel/claude_ctl/f15_adhash_20261012/results/{status.txt,progress.json,provenance,comparison}/
- .247 /home/neel/claude_checks/f15_adhash_20261012/oom_baseline/
- .247 own operational logs/probes: /home/neel/ariabc_data/f15_adhash_20261012/oom/

Local evidence /work/ARIABC/AriaBC/.bench_tmp/f15_adhash_20261012/agents/f15/:
SCRATCH_TAKEOVER.md (before/after/restoration), MONITOR.md (status, ETA, resume,
analysis), baseline_evidence/, oom_provenance/, oom_smoke/, oom_grid_launch/,
SMOKE_COMPARISON.md, OOM_PATCH_NOTES.md and oom_harness.patch.

compare.py now binds accepted binary provenance, prefers canonical accepted
summary attempts and excludes ambiguous duplicates. It runs on partial results
and writes comparison.csv, COMPARISON.md and rejection.csv; the detached
progress observer updates these on .111. Remote tests passed partial/complete
ratios and nine rejection cases plus duplicate exclusion. Missing trials/control
ratios remain blank, never zero. Curated partial outputs are also saved locally.
