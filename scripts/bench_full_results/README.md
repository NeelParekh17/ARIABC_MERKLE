# Benchmark results backup

The results previously stored in this folder are backed up on the ranking server:

- Server: `protectdr@10.129.7.57`
- Path: `/backup/protectdr/AriaBC/scripts/bench_full_results/`

All 286,725 files (about 59 GB) were checksum-verified before the local copy
was removed on 2026-10-03 to free disk space.

To retrieve the archived results from the repository root:

```bash
rsync -az protectdr@10.129.7.57:/backup/protectdr/AriaBC/scripts/bench_full_results/ scripts/bench_full_results/
```
