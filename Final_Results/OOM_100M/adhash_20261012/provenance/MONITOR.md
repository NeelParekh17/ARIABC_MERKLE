# F15 AdHash OOM campaign — active

Engine commit **6a1c66d67b25d6e23f4393c65b59ffdf0e197492**; Step 1 is accepted.
The storage blocker is resolved by the explicit 2026-10-12 scratch takeover.
All builds/binaries remain the accepted F15 builds. Nothing was pushed.

## Baseline and smoke

`.247:/home/neel/ariabc_data/paper_20261010_oom/pgdata` is the stopped AdHash
baseline. It was byte-identical to read-only pgdata_base_v2_ff90 before mutation;
fast catalog checks matched, only the Merkle index was REINDEXed, then the full
count/verification passed, CHECKPOINT and clean shutdown completed. Format 11,
100,000,000 rows, heap 28,248,276,992 bytes, 211,400 nodes / 204,800 leaves:
all count/size/geometry values match the published initial tree. Baseline evidence
is `baseline_evidence/`, raw .247 `~/claude_checks/f15_adhash_20261012/oom_baseline/`.

A theta .99 / worker 1, fresh 20K-statement smoke trials:

| Mode | New TPS | Published median | Published min | Published max | New / median |
|---|---:|---:|---:|---:|---:|
| Merkle | 1790.83 | 1780.31 | 1776.36 | 1865.67 | 1.00591 |
| det | 1951.03 | 2040.82 | 2038.53 | 2055.29 | 0.95600 |

Both have gateway exit 0, 20,000 successful terminals, divergence 0, permanent
failures 0; Merkle verify PASS, clean noise qualification. Independent raw
terminal/tag audit passes 40,000 smoke terminals. Smoke is separate from the grid.
The diagnostic smoke drift-adjusted Merkle ratio is approximately 1.0522;
one trial does not establish a grid median or isolate host effects from other
engine/host differences versus published commit 16577072.

## Campaign and status

Full grid began **2026-10-12T16:01:47Z**. It has 84 Merkle cases (28 points x
three trials) plus 28 det controls (one per point). Seeded shuffled blocks and
rotated mode order are retained; det controls are distributed over all three
trial blocks, recorded as trial 1. The saved order has 112 unique cases.
All seven workload hashes match paper inputs.

- Controller tmux session: **f15oom**.
- Private socket: `.111:/home/neel/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock`.
- Status: `.111:/home/neel/claude_ctl/f15_adhash_20261012/results/status.txt`.
- Live count/ETA: that directory's `progress.json` (refreshed every 45s).
- Full raw grid: `results/campaign/run_20261012_213147_24c8a32a/`.
- Raw smoke: `results/smoke/run_20261012_211725_e5b80c6e/`.
- Controller log: `results/campaign.log`; retained attempts: run's all_attempts.csv.
- Baseline/control/manifests/probes/server logs: .247 own
  `/home/neel/ariabc_data/f15_adhash_20261012/oom/`.

```bash
ssh neel@10.129.27.111 'cat ~/claude_ctl/f15_adhash_20261012/results/progress.json; tail -8 ~/claude_ctl/f15_adhash_20261012/results/status.txt; tail -8 ~/claude_ctl/f15_adhash_20261012/results/campaign.log'
ssh neel@10.129.27.111 'tmux -S "$HOME/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock" list-sessions'
ssh neel@10.129.148.247 'df -B1 "$HOME"; free -g; tail -1 ~/claude_checks/f15_adhash_20261012/oom_baseline/campaign_space.jsonl'
```

ETA is provisional from completed-case spacing and appears in progress.json
once two accepted cases exist. Early mode mix and sampled full content checks
can change it. The final launch report preserves a timestamped count/ETA snapshot.
Do not wait for the grid to finish to use this monitor.

## Method, acceptance and observers

SERIALIZABLE, 100M ff90, 200 partitions, fanout 32, thresholds 1024/256, 32MB
shared buffers, 20K statements, 96 terminals, window 1024, cold cache drop after
physical validation, paper delta restore and calibrated settle probes. Same
terminal, divergence, permanent failure, gateway exit and Merkle verification
acceptance. Same severe-noise exclusions: >=10s sampler gap, >=95% full IO PSI
or >=50% full memory PSI over >=10s, telemetry error. Retain excluded attempts
and rerun as extras; three severe-stall attempts stop for investigation.

Maintenance memory is capped at 256MB to obey the binding memory limit, versus
the paper's declared 2GB. No maintenance operation runs in the timed workload;
shared buffers, work_mem, durability, gateway and execution settings are unchanged.
See OOM_PATCH_NOTES.md and oom_harness.patch for every harness adaptation.

Working paths: approved `pgdata_merkle` from AdHash `pgdata`; optional approved
`pgdata_plain` from read-only `pgdata_base_v2_ff90_plain`. No generation, new full
copies, or published baseline rebuild occurs. `SCRATCH_TAKEOVER.md` records
ownership and exact restoration to paper state.

Both hosts record iostat at 5s; timed cases stream paper host/PG/PSI telemetry.
A disk observer checks 5 GiB free and stops only F15 processes on violation.
The progress observer performs read-only analysis. It stops owned iostat
observers and signals the space observer to exit when GRID_DONE is recorded.
These processes remain detached along with the controller during the grid.

## Auto-resume

The controller passes --auto-resume. Source hashes, binary hashes and qualified
terminal/noise/Merkle evidence are rechecked; accepted CSV cases are skipped,
interrupted attempts are preserved under new attempt names. Never change the
frozen measured harness/binaries or restore the baseline to XOR format mid-grid.

If the controller has exited, check status/error/lock ownership first. A hard
SSH/process death can leave only our remote benchmark lock persistent; verify
no F15 controller/server/postmaster is active before removing this own stale
lock. Never remove the published paper campaign's lock or restart other PG.
Start the same controller in a new detached private-socket session; use a new
log filename to preserve prior output:

```bash
ssh neel@10.129.27.111 'tmux -S "$HOME/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock" new-session -d -s f15oom "bash $HOME/claude_ctl/f15_adhash_20261012/oom_controller.sh > $HOME/claude_ctl/f15_adhash_20261012/results/controller_resume.log 2>&1"'
```

Do not invoke this while f15oom is already running. The controller lock rejects
a second owner. For multiple resumptions choose a fresh log suffix.

## Partial/full comparison and independent audit

Analysis runs on .111, separate from measurements. The progress observer already
updates results/comparison/{comparison.csv,COMPARISON.md,rejection.csv}. Missing
trials remain blank. Canonical accepted summary attempts take precedence over
retained extras; ambiguous duplicates are excluded. The comparison verifies
accepted-build binary hashes, settings, workload hash, terminal coverage,
noise qualification and format-11 geometry.

```bash
ssh neel@10.129.27.111 'python3 ~/claude_ctl/f15_adhash_20261012/compare.py --results ~/claude_ctl/f15_adhash_20261012/results/campaign --published ~/claude_ctl/f15_adhash_20261012/results/provenance/published_configuration_summary.csv --published-trials ~/claude_ctl/f15_adhash_20261012/results/provenance/published_trials.csv --out ~/claude_ctl/f15_adhash_20261012/results/comparison'
ssh neel@10.129.27.111 'python3 ~/claude_ctl/f15_adhash_20261012/audit_oom.py --root ~/claude_ctl/f15_adhash_20261012/results/campaign --out ~/claude_ctl/f15_adhash_20261012/results/provenance/grid_independent_audit.json'
rsync -a neel@10.129.27.111:claude_ctl/f15_adhash_20261012/results/comparison/ /work/ARIABC/AriaBC/.bench_tmp/f15_adhash_20261012/agents/f15/
```

Reports contain per-point new Merkle median/min/max versus published values,
new/published ratio, det-control ratio, drift-adjusted ratio and overall median
ratios. Drift adjustment is diagnostic, not a causal attribution. Original
baseline, smoke, campaign, source/acceptance logs remain on lab hosts; curated
snapshots and reports are under this agent directory.
