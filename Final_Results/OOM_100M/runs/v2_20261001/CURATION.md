# Fresh OOM v2 archive

Original: `neel@10.129.27.111:/home/neel/claude_ctl/results/oom_v2_20261001/`. Local input: `.bench_tmp/oom_v2_20261001`.

Every per-case file is retained unchanged: results, setup, terminal logs/profiles, I/O, checkpoint, cache policy, telemetry, and Merkle verification. Campaign, generation, baseline verification, preflight, build and source provenance are retained. Identical SQL copies are relative links to one archived copy; SOURCE_TO_ARCHIVE.csv records each input hash and path mapping. Raw CSV/JSON paths remain the original remote paths.

Failed build-attempt logs are under `failed_preparation_no_data/`; they produced no data. Generation `run_20261001_193009_25578d88` is the aborted load; `run_20261001_193334_df20ccfe` is the single completed clean generation. The partial remote load was deleted before restarting. The MemAvailable guard changed from 12288 to 4608 MB because this fork pins about 6.3 GB shared memory even with 512MB buffers (orchestrator account, not a new measurement here). Stale same-size source/build state on .247 was moved aside; snapshot rsync now uses --checksum.

No source/build tarballs or database files were copied. `remote_bulk_sha256.txt` records the omitted archives and their SHA-256; originals remain at `/home/neel/claude_ctl/results/oom_v2_20261001/` on .111. Missing failed_*_provenance directories were already omitted from the local input. No build, database, benchmark, or remote command runs during publication.

`controller_scripts/` preserves the supplied final controller configuration; its README distinguishes post-freeze orchestration corrections from frozen built source.

`publication_input/` preserves the pre-v2 consolidated summary and README. `ARCHIVE_SHA256SUMS` covers every archived file (including linked SQL) except itself.
