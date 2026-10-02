# OOM optimization experiments, 2026-10-01

See the [publication section](../README.md#optimization-experiments-2026-10-01)
for methods, both tables, conclusions and caveats. The [figure](optimization_experiments.png)
contains TPS, Merkle/det ratios and device KB/statement for both experiments.
`summary.csv` contains 32 labeled rows from 24 unique measured cases;
`comparison.csv` contains 16 baseline/treatment comparisons. Fillfactor-100
controls are duplicates of the WAL-off observations, not additional trials.

[CURATION.md](CURATION.md) documents complete raw evidence preservation and
[archive_mapping.json](archive_mapping.json) maps original directories.

From this directory, verify every copied raw/provenance file with:

```bash
sha256sum -c ARCHIVE_SHA256SUMS
```

From the repository root, regenerate derived files with:

```bash
MPLCONFIGDIR=/tmp/ariabc-oom-opt-matplotlib python3 scripts/distributed/oom_opt/plot_opt.py
```
