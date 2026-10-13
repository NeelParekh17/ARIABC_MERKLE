FINAL REPORT

AdHash is implemented and correctness validation is complete. The requested
OOM performance campaign is blocked by storage and has not been launched.

Source changes: 13 files, 166 insertions and 69 deletions. Engine addition
carries across all 32 little-endian bytes, dropping only the final carry.
DELETE negates once in staging; the deferred same-route UPDATE explicitly
adds new minus old. Existing coalescing, subtransaction merge, leaf/ancestor
application, split/merge, bulk build, verification and global-root callers
therefore all add. The C++ and Python client combiners are additive and use
version-checked root helpers. Exact files/call sites and completeness reasoning
are in `commit_summary.txt`, `combine_sites.txt`, and `COMBINE_SITES.md`.

Index format 11 refuses old XOR trees before root reads or DML. Route format 4
and row-hash format 1 are unchanged. Migration rebuilds four real version-10
indexes and verifies their contents. Its inherited SPI lifetime and stale
command-ID defects were reproduced and fixed. The reserved delta version is 2;
there is no active encoder/decoder using it in this checkout. Operators should
quiesce writers, apply old work using old binaries, install the new binaries,
then run `merkle_rebuild_legacy_indexes()` or REINDEX each Merkle index and
verify. See the clone's README_MERKLE.md upgrade section.

Correctness results:

- 4921f6a5 reproduces both false equalities, empty/pair and single/triple, with
  local verification true. The new build distinguishes both.
- Final-binary SQL campaign: 291 arithmetic vectors and all duplicate/PK,
  transaction/savepoint, COPY, UPDATE/key move, split/merge, delete-to-zero and
  bulk REINDEX cases pass. Python independently checks every node's LE sum
  and count, including all leaves and partition roots.
- Final server on .247 / gateway on .111: 9 successful terminals, 4 audited
  stages, divergence 0, permanent failures 0, verification and node oracle PASS.
- Both 66-case w8 matrices: 63 PASS, exactly the same exceptions
  volatile_random / cx_empty_request / cx_multi_statement; no new failures.
- Merkle probes: same error lists as the audit baseline; F15 changes equal-root
  true to false, while verification remains true.
- Small/canonical Gap2 normal phases: zero leaf/coverage mismatches and crashes.
  Intentional sibling-swap controls produce exactly two oracle mismatches.
- Real C++/Python client digest checks and synthetic partial comparison-script
  checks pass. These are correctness checks, not TPS measurements.

Build provenance: PG on .247 with --enable-depend, C++ on .111 against our PG
install. All 4,541 PG and 1,827 C++/NuRaft source blobs match the commit artifact.
Final postgres SHA-256 is
`f5af4b193a309f19015ed7fc2aa247abe7a5e53aeedc189647563d549aa6adea`;
server `dddda624c2bd96ab08212f8e0913bf4fb4abe3f895ca557e44ba9b2b4a573271`;
gateway `6b589940c483a0a1a5075c4b22e8fdcf3764c3a0e7f49d46649f8aa33105bb98`.
All test instances are stopped. No canonical install, other postmaster, main
repo working tree or published OOM data was modified.

Git: commit **6a1c66d67b25d6e23f4393c65b59ffdf0e197492** is now verified
on branch `f15/adhash` in the requested clone, with a clean worktree, parent
4921f6a5 and plain message without trailers. Direct Git writes initially failed
because `.git` was read-only; the preserved bundle was subsequently imported
outside this sandbox (reflog: reset to refs/bundles/f15). The bundle remains
verified and archived. Nothing was pushed.

OOM baseline/smoke/grid: not run. The published baseline occupies
35,866,845,184 allocated bytes. Final .247 free space is 36,193,153,024 bytes.
New baseline + separate working copy + 5 GiB reserve need at least
77,102,399,488 bytes, a 40,909,246,464-byte shortfall before WAL/rebuild growth.
Even removing all F15 artifacts would not make two copies fit. `BLOCKED.md`
records exact paths and inventories. Grid status: **0/112**, ETA unavailable.
No TPS equivalence or host-drift conclusion is claimed.

`compare.py` works on partial paper-harness results and writes comparison.csv,
COMPARISON.md and rejection.csv. Current actual output has all 28 published
points and blank new TPS/ratio fields; no synthetic fixture is included.
`MONITOR.md` records status commands, storage prerequisites and analysis.

Evidence directory:
`/work/ARIABC/AriaBC/.bench_tmp/f15_adhash_20261012/agents/f15/`.
Detailed commands and every test result: CORRECTNESS.md. Curated evidence:
evidence_pg/, evidence_cpp/, regression_final/, regression_initial/,
gap2_final_evidence/, det_correctness_final/, probes_old.log/probes_final.log.
Raw PG/build/tests:
`.247:/home/neel/claude_checks/f15_adhash_20261012/`.
Small data:
`.247:/home/neel/ariabc_data/f15_adhash_20261012/`.
Raw C++/gateway:
`.111:/home/neel/claude_ctl/f15_adhash_20261012/`.
