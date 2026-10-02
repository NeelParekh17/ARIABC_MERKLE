# Baseline rebuild and compaction provenance

The [archived Task G handoff](CAMPAIGN_HANDOFF.md) records this preparation on
`neel@10.129.148.247`. The scripts/log below were not present under `.bench_tmp`
at publication, so their literal contents were not copied or reconstructed.

- `/tmp/ariabc_oom_100m/rebuild_s1024.sh`: copy the stopped
  `pgdata_base_fanout32_tblnamed` into the separate `pgdata_base_f32s1024`,
  drop/rebuild `usertable_merkle_idx` with `install_opt` and
  `WITH (partitions=200, fanout=32)`; verify the rebuilt tree.
- `/tmp/ariabc_oom_100m/rebuild_s1024.log`: rebuild, geometry and verification log.
- `/tmp/ariabc_oom_100m/compact_s1024.sh`: `VACUUM FULL` the internal node table
  after the old build path left dropped-tree dead tuples; verify and stop cleanly.

Handoff storage figures: node heap approximately 780 MB before compaction,
24 MB after, primary-key index 6.5 MB; baseline verification `t`.
Per-case `setup.json` independently captures 211,400 nodes, 204,800 leaves,
split 1024, merge 256, fanout 32, partitions 200 and the baseline manifest.
Every case's full verification is saved as `merkle_verify.txt` = `t`.
The separate reported 2–4% code-only microbenchmark has no paired raw evidence
in this campaign archive; fetch its original observations if available.

The optimized source identity is handoff-reported HEAD 19562ae plus uncommitted
changes. Campaign setup/preflight hashes identify the actual executables; they
do not identify a complete source patch or prove a later working tree is identical.
Any subsequent compact-on-rebuild implementation needs its own validation;
the campaign's manual VACUUM FULL is not validation of that implementation.

The orchestrator can append the original preparation evidence without rerunning
or modifying either stopped baseline. See COMMANDS.md section 2a for fetch commands.
