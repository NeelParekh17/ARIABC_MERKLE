# Curation note (2026-09-29)

This directory is the 2026-09-23 YCSB campaign (`campaign.json`, `command.txt`), with its
pg rows replaced.

- **Replaced:** the 80 original pg cases (pg retry backoff without jitter; see
  `../CORRECTIONS.md`).
- **Replaced with:** 240 pg cases, 3 trials × the same 80 points, measured on 2026-09-29
  with retry jitter.
  - Contract: `campaign_pg_rerun_20260929.json`
  - Runner log: `runner_pg_rerun_20260929.log`
  - Per-case logs: under `attempts/`
- **Regenerated from the merged `summary.csv`:** `REPORT.md`, `summary_median.csv`,
  `variability.json`, `MEASUREMENT_QUALIFICATION.md`, `final_tps_all_modes_comparison.png`
  and `graphs/`. Each point is plotted at its median.
- **Kept from the original campaign:** `ANALYSIS.md`'s interpretation text, with a pg-rerun
  section added.

The superseded pg logs and the original generated files are archived outside Final_Results
in `.bench_tmp/ycsb_superseded_20260929/`. The verification runs of the other modes are in
`.bench_tmp/ycsb_verify_20260929/`.
