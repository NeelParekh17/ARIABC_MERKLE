# Approved paper scratch takeover — F15, 2026-10-12

Authorization: the user explicitly approved modifying only the paper OOM
`pgdata` and `pgdata_merkle` scratch directories, plus `pgdata_plain` if needed
for the det control. Published baselines, golden archive, paper install,
cluster, source and published evidence remain read only. The det control uses
the optional plain working copy to retain the paper's per-variant reset method.

All paths below are on .247 under `/home/neel/ariabc_data/paper_20261010_oom/`.

| Directory | Before takeover | F15 role now |
|---|---|---|
| `pgdata` | Stopped byte-identical copy of `pgdata_base_v2_ff90`: system ID 7695110858662784567, checkpoint 5/3F3BEE70, control SHA c8b6ad9e7cdbbea4fc9603066f733bbfab0b8b2a71eb355b05b397b2ccea73f3, 1,146 files / 35,866,524,242 apparent bytes; format 10 | Stopped, immutable AdHash baseline, format 11; checkpoint 5/41EBAA30, control SHA fb6bbe818e284813e2bb375dbf6290988567d0f1b688fa1f287f07642bbb3969, 1,144 files / 35,832,970,910 apparent bytes |
| `pgdata_merkle` | Stopped paper Merkle working scratch; enable_merkle_index=on, workers=4, checkpoint 5/3F407D60, control SHA 2d2be33fce6c3ae73daa239053128401a3c1ef4c33bdc0cee7ca62a62272be2c, 1,146 files / 35,866,543,231 apparent bytes | F15 Merkle working copy; delta-restored from `pgdata` before each case. It contains the latest trial's changes after that case, never serves as a baseline. |
| `pgdata_plain` (optional approval used) | Stopped paper plain working scratch; Merkle off, workers=4, checkpoint 5/3F453740, control SHA d69e859aeef4235a2657b6bd195d32bd65b4242c33f803c4ee5d891815963a84, 1,143 files / 32,712,287,190 apparent bytes | F15 det working copy; delta-restored from read-only `pgdata_base_v2_ff90_plain` before each det case. |

Before the first baseline mutation, the paper harness's exact
`diff -rq --no-dereference` content check passed against `pgdata_base_v2_ff90`.
The initial copy was already byte-identical, so no initial restore was needed.
A second full content check and rsync metadata dry-run passed. Fast catalog
checks using our copied old-format binary matched the published manifest:
100M persisted root count, key bounds 1..100M, ff90, heap 28,248,276,992 bytes,
primary index 2,246,197,248 bytes, relpages 3,448,276, and identical three index
definitions. Old geometry was 211,400 nodes / 204,800 leaves.

Only `usertable_merkle_idx` was REINDEXed with the accepted AdHash binary.
No heap or primary/lookup index rebuild was performed. We changed the scratch
configuration to our port 5478, SERIALIZABLE, 32MB shared buffers and 256MB
maintenance memory. REINDEX wall time was 115.04s (engine profiler 111.32s).
Full post-rebuild count is exactly 100,000,000; heap/primary sizes and relpages
are unchanged. `merkle_verify=t`; format 11 geometry remains exactly
211,400 nodes / 204,800 leaves, 200 partitions, fanout 32, thresholds 1024/256.
CHECKPOINT and clean shutdown completed before benchmark use.

REINDEX resource monitor: 23 samples, minimum free disk 36,163,850,240 bytes,
so the transient 3GB exception was not needed. Benchmark starts/restores check
at least 5 GiB free, and `free -g` is recorded before every PG start. All
installed binaries, observer logs, probes and controller state are in F15's
own home directories. tmux uses an explicit socket under F15 scratch; no /tmp.

Evidence: local `baseline_evidence/`; raw
`.247:/home/neel/claude_checks/f15_adhash_20261012/oom_baseline/`, including
before_identity.json, before_equivalent.json, both content checks,
before_catalog_verification.json, reindex_elapsed.json, resource_monitor.jsonl,
verification.json, merkle_verify.txt, adhash_manifest.json and status.txt.
The published baseline's stopped control identity was rechecked after the
build and is unchanged.

## Restore to paper scratch state

First stop the F15 controller and let its owned server/PG cleanup finish.
Do not run restoration while the campaign is active. The controller's
auto-resume evidence is retained separately from these scratch directories.
From .111, a controlled interrupt uses the private tmux socket:

```bash
tmux -S ~/claude_ctl/f15_adhash_20261012/scratch/oom.tmux.sock send-keys -t f15oom C-c
```

Then verify no postmaster.pid remains in these three scratch directories.
Restore on .247 using the paper's delta rsync method; sources are read only:

```bash
paper=/home/neel/ariabc_data/paper_20261010_oom
test ! -f "$paper/pgdata/postmaster.pid"
test ! -f "$paper/pgdata_merkle/postmaster.pid"
test ! -f "$paper/pgdata_plain/postmaster.pid"
rsync -a --inplace --no-whole-file --delete --stats "$paper/pgdata_base_v2_ff90/" "$paper/pgdata/"
rsync -a --inplace --no-whole-file --delete --stats "$paper/pgdata_base_v2_ff90/" "$paper/pgdata_merkle/"
rsync -a --inplace --no-whole-file --delete --stats "$paper/pgdata_base_v2_ff90_plain/" "$paper/pgdata_plain/"
diff -rq --no-dereference "$paper/pgdata_base_v2_ff90" "$paper/pgdata"
diff -rq --no-dereference "$paper/pgdata_base_v2_ff90" "$paper/pgdata_merkle"
diff -rq --no-dereference "$paper/pgdata_base_v2_ff90_plain" "$paper/pgdata_plain"
```

This restores format-10 paper data and original configuration/control bytes;
it does not reinstall binaries or touch the published baselines. AdHash
Merkle cases must not be resumed after restoring these scratch paths to XOR
format; their immutable-baseline identity check will reject the change.
