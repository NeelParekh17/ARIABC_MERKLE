# Correctness evidence

All PostgreSQL instances, SQL tests and server workloads ran on .247 in ~/claude_checks/lsm_lookup_20261014; C++ builds and gateways ran on .111 in ~/claude_ctl/lsm_lookup_20261014. Own ports 5488/8088/9088, SERIALIZABLE, PostgreSQL 13devel at a7fc519dfdd0713247922abff3bc3c80b752e27e built with --enable-depend. No engine algorithm was changed. The final shared lsm3 module is SHA-256 3d5d9913d45a54e164c4fd42540885ab0e95a062ba608c76bfcca0a71b55e8ad.

## Small split/merge and independent root oracle

small_final_sql.py created paired B-tree/LSM tables with 4 partitions, fanout 4, split 8, merge 2. Initial 512 rows formed 192 nodes / 145 leaves; inserts reached 768 rows, followed by updates and deletion down to 192 rows. Every stage passed merkle_verify and an independent Python sum of SQL row hashes modulo 2^256. Both variants had equal roots. Eight exact partition/hash-range comparisons per stage passed, including rows still in the active top before compaction and after an explicit merge; EXPLAIN used the identical expression lookup index.

A 150,000-row scalar stress index completed two automatic merges and returned exact count 150,000 and sum 11,250,075,000 through an index scan. Evidence: small_final_evidence/{checks.jsonl,sql.log,postgres.log} and related retained small_evidence files.

## Actual det server/gateway and concurrent compaction

small_final_controller.py submitted 140,000 paired INSERT commands to the actual det server, then small_final_mutations.py submitted 3,000 value updates, deletes and key changes. Geometry 200 partitions/fanout32/split1024/merge 256,16 workers, 96 terminals and 96 client workers. One automatic composite-index merge completed during the insert stream. All 143,000 gateway terminals AND strict row-result tags passed with stdout/stderr separated, divergence 0, permanent failures 0, gateway exits 0.

Independent Python expected-data dictionaries matched every final row: 69,500 per variant. Both roots matched the independent additive oracle c264c3e43c9d5e7b967b8d0831d57cba4c7a8e6224a49cf1e025afe2aab76e9c, and merkle_verify=t. Exact row lists matched in all200 partition/hash ranges across the base and residual top. Evidence: small_final_evidence/final_audit.json, small_final_client/validation.json, both gateway logs and the scripts with matching names.

The earlier small_compaction attempt combined stdout/stderr, allowing ORDERER diagnostics to corrupt printed row tags. Its gateway/root checks passed but strict row-tag audit did not. It is preserved and superseded by the separated-stream 143K test, and is not an accepted benchmark row. The full 143K suite predates the final DROP IF EXISTS-only utility-hook fix; its DML/scan/merge code and index format are unchanged. Focused final-library missing/existing DROP checks passed (pg_evidence/dropfix_test.{sql,log}); all final campaign cases use the final module hash.

## Split retrieval and replica-repair transfer queries

small_repair.py reproduced replica_repair.cxx's int4-array bounds CTE, partition/hash join, DISTINCT ON primary key and ordered COPY shape. B-tree and LSM used usertable_merkle_lookup_idx and exported identical 42 rows / 3639 bytes, including top-resident rows. Evidence: small_evidence/repair_copy.json. Tiny-table plans disabled seqscan to establish index eligibility.

At 100M rows, both modes use an Index Scan for the four-parameter Merkle split retrieval shape, and a Bitmap Index Scan beneath the repair CTE/Unique/Sort plan. Neither query needed a source change. The recorded bound partition 0, hash 0000000000000000..003fffffffffffff returns the same fingerprint 499|190474|99802643|c44439e7a492a8948306d1d3bdd8819b. Every accepted setup retains both plans and the fingerprint in setup.json; audit.py independently compares them. This exercises the transfer query shape, not a complete multi-replica repair campaign.

## 100M baseline and post-workload checks

All three own ff90 baselines contain 100,000,000 rows, keys 1..100,000,000, equal heap/primary sizes and fillfactor 90. Both Merkle baselines have format 11 and geometry 200/32/1024/256, 211,400 nodes / 204,800 leaves, merkle_verify=t and root 136ddf6edd207060c2352dc97c8c3977fd8146adcc3ad6b5fa2cd465c52bec28. Normalization reverified controls and preserved sizes/data/root; PASS 13:15:18Z. See BASELINES.md and baseline_evidence/baselines_normalized/.

Every accepted Merkle workload requires a fresh full merkle_verify=t after I/O snapshots. Final case/request/verification counts are in AUDIT.md and final_audit.json. The independent audit also checks every workload statement against its server row result, all terminal IDs and gateway counters, published workload hashes, geometry, query plans, cold reset, settings and identical binary/source contracts.

## Boundaries and preserved failures

Build/runtime failures are retained: missing installed fork header, pre-release 13 API mismatch, raw expression parse transform, upstream size-helper OID bug, and the three smoke setup failures documented in PATCH_NOTES.md. The single-user batch-GDB attempt only established the preload requirement; it did not produce a successful expression-failure stack trace. That fix is supported by the fork's transformIndexStmt/DefineIndex path and passing expression-index tests.

These tests establish the exercised streams and scan shapes. They do not prove arbitrary concurrent SQL, extension crash safety or end-to-end replica repair. Sustained compaction performance was not studied by user choice. The small automatic-merge tests establish correctness through compaction, not sustained 100M performance.
