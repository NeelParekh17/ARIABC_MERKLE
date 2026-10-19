# Baseline and OOM evidence

Verified, stopped baselines only. Sources in paper_20261010_oom are read/copy-only. All working files are in this campaign's own data directory.

| Baseline | Rows | Heap bytes | Primary bytes | Lookup base bytes | Top bytes | Merkle format | Verify |
|---|---:|---:|---:|---:|---:|---:|---|
| base_ff90 | 100000000 | 28248276992 | 2246197248 | 3154264064 | 0 | 11 | t |
| base_ff90_lsm | 100000000 | 28248276992 | 2246197248 | 3154264064 | 8192 + 8192 | 11 | t |
| base_ff90_plain | 100000000 | 28248276992 | 2246197248 | 0 | 0 | none | N/A |

The verified Merkle baseline has 211,400 nodes, 204,800 leaves, partitions=200, fanout=32, split=1024, merge=256. Node heap: 24,739,840 bytes; node indexes: 6,676,480 + 7,004,160 + 16,384 bytes. The plain baseline retains the published unused node relations, with no active Merkle index/lookup index.

DB host physical RAM is about 15 GiB; the 28.25GB heap alone exceeds it. Merkle table plus indexes is 33,656,586,240 bytes (LSM adds 16,384 bytes of initially empty tops), about 31.34 GiB. Timed shared_buffers is 32MiB, independently checked as 4096 8KiB blocks. Per-case cold OS-cache drop follows restore validation and the device-settle probe; Linux page cache is not capped by shared_buffers. This is a cold, database-larger-than-memory experiment, not an OOM-killer test or a claim that the 20K-statement working set exceeds RAM.

All three baselines were normalized with matching VACUUM/ANALYZE visibility-map state: relpages=relallvisible=3,448,276, heap/primary sizes preserved, roots equal. Normalization PASS at 13:15:18Z. Evidence: baseline_evidence/baselines_normalized/{base_ff90,base_ff90_lsm,base_ff90_plain}.json and *_visibility.json. Initial build manifests are retained alongside normalized manifests.

Both Merkle baselines have root 136ddf6edd207060c2352dc97c8c3977fd8146adcc3ad6b5fa2cd465c52bec28. B-tree baseline was copied from published format-10 data and the Merkle index rebuilt using the isolated AdHash format-11 binary; LSM was copied from this verified baseline before changing only the lookup index. The logical data and geometry match. The unused node relations retained by plain are measured in its manifest.

ff100 baselines and sustained runs were cancelled by the user. Only these three ff90 baselines are required. The complete ff90 measurements are in results.csv and RESULTS.md. Median device read KiB/statement ranges across the eight points: det 7.496–16.691, B-tree Merkle 9.056–18.236, LSM Merkle 9.050–18.250. All θ0 updates were HOT; θ0.99 was also nearly entirely HOT. Native shared-buffer counters and device reads are reported separately.

Final read-only metadata/control/size identity checks passed for both published sources and all three own normalized baselines: final_baseline_identities.json. No published cluster was started or written by this campaign. The ff100 output directory is empty; only the three ff90 baselines exist. No baseline deduplication ran. Final sampled .247 free space: 107,056,836,608 bytes (99.71 GiB); the harness requires a 40 GiB reserve before each PG start.
