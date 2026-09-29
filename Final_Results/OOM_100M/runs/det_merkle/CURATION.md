# Curation note (2026-09-29)

This campaign ran pg, det and Merkle. Its pg cases used the pg retry policy without jitter,
which a later check showed to be defective at 16 workers (see Final_Results/CORRECTIONS.md).
Those 28 pg cases were removed from this directory and summary.csv, so only det and Merkle
remain here; `campaign.json` still lists the original arguments. The pg results are in
`../pg_serializable` (SERIALIZABLE with retry jitter) and `../pg_read_committed`. The
removed cases are archived outside Final_Results in `.bench_tmp/oom_superseded_20260929/`.
