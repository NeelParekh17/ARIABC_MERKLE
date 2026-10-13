# Storage unblock and initial measured budget

The user reports explicit authorization to delete .247 ~/ariabc_data/oom_100m_20261006 and ~/ariabc_data/legacy_tmp_20261007, and that Claude performed both deletions. OOM-1 verified both paths absent on 2026-10-10T14:39:34Z. Original block is preserved in BLOCKED_resolved_20261010.md. No further old-data deletion is authorized or needed here.

Measured free bytes: 231,662,723,072 (215.753 GiB), same root/home filesystem. Use fresh generation; no old heap or Merkle index survives the authorized deletion.

Sizing evidence is the pre-deletion allocated-byte inventory, not a live new-baseline measurement. Conservative planning follows the existing v2 layout:

| Object | Expected allocated bytes | Copies |
|---|---:|---:|
| Merkle-sized generation pgdata | 35,866,824,704 | 1 |
| Stopped Merkle baseline | 35,866,824,704 | 1 |
| Stopped plain baseline | 32,712,556,544 | 1 |
| Merkle working copy | 35,866,824,704 | 1 |
| Plain working copy | 32,712,556,544 | 1 |
| Golden compressed archive | 5,375,303,680 | 1 |

Data total = 178,400,890,880 bytes = 166.149 GiB. Add 20 GiB WAL/growth reserve (existing runner reserve), 8 GiB temporary-index/archive/slack allowance, and 4 GiB PG source/build/install/server/log allowance: total 212,760,629,248 bytes = 198.149 GiB. Round the startup generation guard up to **214,748,364,800 bytes (200 GiB)**. Current free space exceeds it by 16,914,358,272 bytes (15.753 GiB). Generation builds its archive sequentially after copying its Merkle baseline; full-check/verification temporary copies occur before the two permanent working copies, so they do not add a sixth simultaneous data copy. The later per-case first-copy guard still requires baseline bytes plus 20 GiB free.

This is a measured-size estimate from the previous same row-count/fillfactor/geometry layout, not a guarantee about the new build. After fresh baseline verification, recompute remaining working-copy needs from the new actual baseline sizes and free space before smoke/campaign. Stop if that recheck fails. Preserve failures and partial new generation; never reclaim unrelated data.

After the first attempt was interrupted by a host reboot and its own incomplete pgdata was discarded, free bytes were remeasured at 230,735,118,336. The 200 GiB startup guard still fits. New full generation will run on kernel 7.0.0-38-generic; no interrupted data are reused.

Fresh verified-size gate passed at 2026-10-10T20:03:03Z. Merkle working copy requires 35,866,524,242 bytes; plain requires 32,712,269,772 bytes. With the unchanged 20 GiB WAL/growth and 8 GiB temporary reserve, remaining requirement is 98,643,565,086 bytes versus 120,801,132,544 free. Margin is 22,157,567,458 bytes (20.636 GiB). Golden archive is 5,359,018,724 bytes and is already present. Baseline manifests confirm both variants use the same 28,248,276,992-byte heap. Evidence: evidence/space_recheck.json and evidence/baseline_evidence/*.json. No additional data deletion was needed.
