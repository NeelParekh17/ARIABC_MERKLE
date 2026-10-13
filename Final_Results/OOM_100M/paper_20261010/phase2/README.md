# Completed OOM paper evidence

Campaign 252/252 accepted, smoke 3/3 accepted; independent raw audit passes all 255. Primary tables/figures are ../../README.md and ../../figures/. The parent paper_20261010 directory retains phase-one snapshots; they are not final campaign summaries.

- trials.csv: full 252 accepted rows; smoke_trials.csv: separate three smoke rows.
- configuration_summary.csv: all 84 points, median/min/max/best/n/sample CV and trial values; no exclusions.
- matched_ratios.csv: 28 matched ratios of medians, plus device-I/O ratios.
- previous_v2_trials.csv: exactly 84 Oct 1 v2 rows; previous_v2_comparison.csv: matched new medians versus old single observations.
- audit_counts.csv, audit_cases.csv, telemetry_summary.csv, log_messages.csv: audit summaries and honest limitations; AUDIT.md describes checks.
- audit_raw.json and audit_supplement.json: output of read-only inspection of all raw remote logs/settings.
- source_and_fetch_checks.json: file-by-file frozen archive/git proof and remote-to-local case checksum validation.
- evidence/: fetched small per-case result/setup/counter/noise/I/O/checkpoint files and gateway logs, saved provenance, calibration, status and baseline verification. No pgdata, bulk server traces, or full source archive.
- REMOTE_RAW_SHA256SUMS / remote_raw_inventory.csv: all raw case file hashes on .111; raw files remain there.
- SHA256SUMS: local publication evidence checksum manifest. ../../PUBLICATION_SHA256SUMS checks new root README/summary/figures and Phase 2 files.

Local regeneration (no network, database or benchmark):

```bash
MPLCONFIGDIR=/tmp/oom_phase2_matplotlib python3 Final_Results/OOM_100M/paper_20261010/phase2/analyze.py
python3 Final_Results/OOM_100M/paper_20261010/phase2/publish_readme.py
(cd Final_Results/OOM_100M/paper_20261010/phase2 && sha256sum -c SHA256SUMS)
```

Regeneration changes artifact contents only if inputs/code/dependencies differ. Review differences before updating checksum manifests. CSVs preserve full numeric precision; README/figure displays round. CV uses sample standard deviation (ddof=1), and bands show observed extrema. Analysis-library versions and validation are recorded in analysis_environment.json and publication_checks.json.
