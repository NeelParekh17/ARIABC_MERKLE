# OOM 100M Merkle re-measurement with AdHash (F15)

Merkle nodes now combine row hashes by **addition mod 2^256** instead of XOR (commit `6a1c66d6`, merged into main as `6b19ff92`). Under XOR, identical rows in a table without a unique key cancel in pairs: `{r,r}` hashed like `{}`. Deletes add `−h`, and a same-leaf update adds `new − old`. The index format moved to `MERKLE_VERSION` 11, and old XOR indexes are refused until REINDEXed. This campaign re-measures every OOM_100M Merkle point with that build. Its 84 Merkle trials replace the XOR Merkle rows in the [primary OOM README](../README.md), [summary.csv](../summary.csv) and figures.

## Result

| Metric (28 points) | Median | Min | Max |
| --- | --- | --- | --- |
| AdHash / XOR Merkle median TPS | 1.003 | 0.963 | 1.048 |
| det control / paper det (host drift) | 0.984 | 0.943 | 1.049 |
| Drift-adjusted AdHash / XOR | 1.017 | 0.972 | 1.096 |

AdHash has no measurable throughput cost; 21 of the 28 points are within ±1% of the XOR medians. Per-point values are in [comparison/COMPARISON.md](comparison/COMPARISON.md) and [comparison.csv](comparison/comparison.csv), with the heatmap [figures/adhash_over_xor_merkle.png](../figures/adhash_over_xor_merkle.png). The det control is a single trial per point, so the drift adjustment is diagnostic only.

There is one outlier. A θ1.2 workers 8 trial 3 ran at 1,908.76 TPS, against 5,402 and 5,438 for the other two trials. It passed every acceptance check and the noise rule, so it is kept. It does not move that point's median (5,402 vs XOR 5,397).

## Campaign

- **Cases:** 112 accepted, 0 rejected or excluded: Merkle 28 points × 3 trials, plus det 28 × 1 as a host-drift control.
- **Acceptance per case:** 20,000 terminal results, zero divergence and permanent failures, and `merkle_verify` PASS on every Merkle case.
- **Smoke test:** 2 cases (A θ.99 w1): Merkle 1,790.83 vs 1,780.31 published, det 1,951.03 vs 2,040.82. See [smoke/](smoke/).
- **Time window:** 2026-10-12 16:01Z to 2026-10-13 01:33Z.
- **Setup, unchanged from the paper campaign:**
  - hosts .247 and .111;
  - harness, plus the overlay in [provenance/oom_harness.patch](provenance/oom_harness.patch);
  - the seven workload hashes, 96 terminals, 32MB shared_buffers;
  - cold start, delta restore and settle probe;
  - SERIALIZABLE isolation.
- **Ports:** DB 5478, server 8078, Raft 9078.
- **Engine build:** `6a1c66d6`. Binary hashes are in [provenance/binary_sha256.json](provenance/binary_sha256.json).
- **Baseline:**
  - The paper scratch copy `pgdata` was first checked to be byte-identical to `pgdata_base_v2_ff90`.
  - Only `usertable_merkle_idx` was REINDEXed in format 11, which took 115s.
  - Checks after the rebuild: 100,000,000 rows, heap size unchanged, `merkle_verify=t`, and 211,400 nodes / 204,800 leaves, the same shape as published.
  - Evidence: [baseline_evidence/](baseline_evidence/). The scratch takeover and restore steps are in [provenance/SCRATCH_TAKEOVER.md](provenance/SCRATCH_TAKEOVER.md).
- **Correctness of the hash change:** [correctness/CORRECTNESS.md](correctness/CORRECTNESS.md).
  - The old build gives false equality for `{}` vs `{r,r}` and for `{r}` vs `{r,r,r}`; the new build does not.
  - Targeted duplicate, split/merge, COPY, savepoint and rollback cases all pass, and an independent Python recomputation matches every stored node.
  - det-mode duplicates: 9/9 pass.
  - 66-case regression: 63 PASS plus the same 3 known exceptions.
- **Raw case directories** (about 519MB) stay on `.111:/home/neel/claude_ctl/f15_adhash_20261012/results/campaign/run_20261012_213147_24c8a32a/`. The small summaries are copied here, in [campaign/](campaign/).

## Files

- `campaign/summary.csv`: all 112 accepted rows. `all_attempts.csv`, `planned_case_order.csv`, `campaign.json`, `MEASUREMENT_QUALIFICATION.md`.
- `trials.csv`: the combined 252-row table used for the primary README (paper PG/det + AdHash Merkle). It is identical to `../summary.csv`.
- `configuration_summary.csv`, `variability.csv`, `matched_ratios.csv`, `previous_v2_comparison.csv`, `headline_numbers.csv`: derived from `trials.csv`.
- `comparison/`: AdHash vs published XOR Merkle per point (`compare.py` output).
- `publish.py`: regenerates everything above, the root `summary.csv`, the Merkle figures and the Merkle numbers in the root README. It runs locally and makes no remote calls:

```bash
MPLCONFIGDIR=$PWD/.mplcache python3 Final_Results/OOM_100M/adhash_20261012/publish.py
```

The XOR Merkle rows that this campaign replaces remain in git history (commit `d63a4cad`) and in the frozen audit archive [../paper_20261010/phase2/](../paper_20261010/phase2/README.md). That archive's own `analyze.py`/`publish_readme.py` regenerate the XOR-era README; run `publish.py` after them.
