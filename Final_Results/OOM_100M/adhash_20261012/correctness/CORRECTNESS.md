# F15 correctness evidence

Engine commit artifact: `6a1c66d67b25d6e23f4393c65b59ffdf0e197492`, parent `4921f6a5a3ebf7a50c77e2b730f87c62e419c7e6`. The initial protected Git write failed; a later read-only check verified the bundle imported into the clone, branch f15/adhash at this commit with a clean worktree. See `BLOCKED.md` and `git_final_state.txt`. No push.

## Builds and provenance

- PG: .247 `/home/neel/claude_checks/f15_adhash_20261012/{src,install}`. `bash build_pg.sh`; `--enable-depend --without-readline CFLAGS=-O2 CPPFLAGS=-D_GNU_SOURCE`, `make -j8`, install and pgcrypto install. Final BUILD_OK.
- C++: .111 `/home/neel/claude_ctl/f15_adhash_20261012/{src,build,pg_install}`. `bash build_cpp.sh`; Release CMake, explicit include/libpq paths copied from our .247 install, `-j4`, server and gateway targets. Final BUILD_OK.
- Every source blob under `src` matched the commit artifact on .247: 4,541 files, zero mismatches. On .111, all 1,827 `ariabc_pg`/NuRaft blobs matched. `verify_source.py` and manifests are retained.
- Old comparator: copied the audit-merge install into our own `old_install`. Its shipped revision is 4921f6a5; all 4,541 source blobs in that audit install source matched 4921f6a5, and the copied postgres binary SHA equals the original. No original source/install was edited.
- Final PG SHA-256: `f5af4b193a309f19015ed7fc2aa247abe7a5e53aeedc189647563d549aa6adea`.
- Old PG SHA-256: `24ae5496749bdca4661f15d5a17655686a5e6e847d4cd6e4e33a0171294be711`.
- Server SHA-256: `dddda624c2bd96ab08212f8e0913bf4fb4abe3f895ca557e44ba9b2b4a573271`.
- Gateway SHA-256: `6b589940c483a0a1a5075c4b22e8fdcf3764c3a0e7f49d46649f8aa33105bb98`.
- `evidence_pg/pg_binaries.sha256`, `evidence_cpp/cpp_binaries.sha256` include the remaining executables/libpq and standalone client digest. See `pg_source_verification.json`, `old_pg_source_verification.json`, and `cpp_source_verification.json`.
- The first PG sync omitted `src/bin/scripts` due to an overbroad rsync exclude. It was copied, and the build reran. The first CMake configure lacked `src/common/blake3.c`; that directory was synced and configure reran. These were preparation errors, not engine failures. Failed logs are retained.

## Targeted SQL and independent node oracle

All instances are ours, .247, SERIALIZABLE, shared_buffers 128MB and maintenance_work_mem 128MB. `free -g` is recorded before every start. Socket/data paths use our home directories; no lab /tmp. The final complete targeted rerun uses the final PG SHA above and `final_targeted`. Earlier core checks and all failed migration attempts are preserved separately.

Commands (on .247, with TMPDIR and LD_LIBRARY_PATH set to our scratch/install paths):

```bash
python3 ~/claude_checks/f15_adhash_20261012/audit/targeted.py old
python3 ~/claude_checks/f15_adhash_20261012/audit/targeted.py migration
F15_NEW_DATA=final_targeted python3 ~/claude_checks/f15_adhash_20261012/audit/targeted.py new --leave-running
```

On 4921f6a5, empty vs pair and single vs triple both returned equal roots; merkle_verify was true for the duplicate tables. The two new inequalities are both true. The two identical inserts are committed in one SERIALIZABLE transaction. The oracle takes canonical `merkle_key_hash` and `merkle_tuple_hash` inputs from SQL, independently routes rows by partition/prefix and adds 256-bit LE integers in Python. Every stored node, including leaves, internal nodes and partition roots, is checked for both sum and count; its global sum is compared with the root API. It does not independently reimplement BLAKE3 or canonical row serialization.

Final targeted results (`evidence_pg/targeted_final.log`):

| Test | Result |
|---|---|
| carry_wrap_and_random_vectors | PASS; 291 |
| new_duplicate_inequalities | PASS; [True, True] |
| duplicate_empty | PASS; rows=0, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| duplicate_pair | PASS; rows=2, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| duplicate_one | PASS; rows=1, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| duplicate_three | PASS; rows=3, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| update_one_duplicate | PASS; rows=2, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| split_d | PASS; rows=512, nodes=188, leaves=142, depth=8, node mismatches=0, verify=True |
| copy_d | PASS; rows=576, nodes=208, leaves=157, depth=8, node mismatches=0, verify=True |
| savepoint_abort_and_subcommit_d | PASS; rows=577, nodes=208, leaves=157, depth=8, node mismatches=0, verify=True |
| transaction_rollback_d | PASS; rows=577, nodes=208, leaves=157, depth=8, node mismatches=0, verify=True |
| same_route_update_d | PASS; rows=577, nodes=208, leaves=157, depth=8, node mismatches=0, verify=True |
| route_move_d | PASS; rows=577, nodes=232, leaves=175, depth=8, node mismatches=0, verify=True |
| merge_d | PASS; rows=14, nodes=12, leaves=10, depth=2, node mismatches=0, verify=True |
| delete_all_d | PASS; rows=0, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| bulk_reindex_d | PASS; rows=256, nodes=96, leaves=73, depth=6, node mismatches=0, verify=True |
| split_p | PASS; rows=512, nodes=184, leaves=139, depth=6, node mismatches=0, verify=True |
| copy_p | PASS; rows=544, nodes=208, leaves=157, depth=6, node mismatches=0, verify=True |
| savepoint_abort_and_subcommit_p | PASS; rows=545, nodes=208, leaves=157, depth=6, node mismatches=0, verify=True |
| transaction_rollback_p | PASS; rows=545, nodes=208, leaves=157, depth=6, node mismatches=0, verify=True |
| same_route_update_p | PASS; rows=545, nodes=208, leaves=157, depth=6, node mismatches=0, verify=True |
| route_move_p | PASS; rows=545, nodes=232, leaves=175, depth=6, node mismatches=0, verify=True |
| merge_p | PASS; rows=7, nodes=8, leaves=7, depth=2, node mismatches=0, verify=True |
| delete_all_p | PASS; rows=0, nodes=4, leaves=4, depth=0, node mismatches=0, verify=True |
| bulk_reindex_p | PASS; rows=128, nodes=40, leaves=31, depth=4, node mismatches=0, verify=True |

The `d` table has no PK/unique key and contains identical full rows. `p` has a PK. The cases include split pressure, COPY with duplicate rows, savepoint abort, RELEASE/subxact commit, whole-transaction rollback, payload updates, key moves, merge pressure, deletion to empty, and REINDEX bulk builds. After deleting all rows, every remaining node has exactly zero hash and count. Arithmetic includes 291 cases: carry across each byte boundary, final wrap, and randomized pairs.

## Deferred BCDB through the real server

Both initial and final-binary runs passed 9/9 terminal statements, with four independently audited stages. Final gateway artifacts are in `det_correctness_final/`; the server log and terminal checks are in `evidence_pg/`. SQL stages insert two duplicates in one statement, update exactly one by ctid, change its route, delete its twin, and finally clear the table. A PK table is exercised in parallel. `merkle_verify` and the complete node oracle pass after every stage. ctid is used only to select one duplicate, never in the row hash.

```bash
# .247, PG already running on 5478:
F15_NEW_DATA=final_targeted python3 ~/claude_checks/f15_adhash_20261012/audit/det_server.py start
# .111, detached gateway controller:
F15_DET_OUTPUT=det_correctness_final nohup python3 ~/claude_ctl/f15_adhash_20261012/det_controller.py > ~/claude_ctl/f15_adhash_20261012/det_controller_final.log 2>&1 < /dev/null &
# .247 cleanup:
F15_NEW_DATA=final_targeted python3 ~/claude_checks/f15_adhash_20261012/audit/det_server.py stop
```

Server: id 1, DB 5478, client 8078, raft 9078, 8 connections/workers, dbType 1, safedb 1, event execution, bypassRaft 1. This is a real one-server BCDB/gateway check, not a multi-replica/crash-recovery claim. Gateway runs on .111 with tsc; all PG/server processes run on .247. Gateway stage logs record return/success/divergence/permanent-failure evidence.

## Migration and client combiners

Old empty-root reads, duplicate verification, and INSERT all refuse version 10 with SQLSTATE XX002 and a REINDEX hint. `merkle_rebuild_legacy_indexes()` rebuilds all four old indexes, returns 4, and all independently recomputed trees pass. Root inequalities are correct after rebuild. Row-hash format 1 and route format 4 remain unchanged.

Two inherited migration-helper bugs were exposed and fixed: the OID list allocated in SPI storage became invalid after SPI_finish (debugger caught index_open(7)); SPI_palloc fixes its lifetime. The read-only verification query then reused the caller's stale command ID and saw no rebuilt roots; pushing the transaction snapshot with a fresh command ID makes its own REINDEX rows visible while preserving SERIALIZABLE. Failed logs, backtrace, migration.gdb and bootstrap SQL are retained.

The reserved durable delta version is 2. This checkout has no active durable-delta encoder/decoder, so no legacy decoder rejection is claimed. The enforced durable-tree barrier is index version 11 plus native helper metadata checks. See `README_MERKLE.md` and `COMBINE_SITES.md` for the operator procedure and complete maintenance inventory.

`client_checks.py` passed four real Python root-combiner checks and a C++ compute_merkle_digest comparison against Python/SQL roots. The C++ test program is compiled on .111 and executed on .247; its source and SHA are retained. Root helper calls now validate format in both clients.

## 66-case audit regression

Final-build run: `bash ~/claude_checks/f15_adhash_20261012/regression_final.sh`, detached. The full exact case list and commands are in that script. Audit script copies use our install/ports; the final run uses port 5482 and a pristine initdb copy, a path already supported by the original audit harness. It compares native BCDB outputs and state against the serial SQL oracle at workers=8.

**66 cases: 63 PASS, the same 3 expected MISMATCH exceptions; no new failures.** Raw summaries, query streams, schema/settings, logs and row hashes are retained in `regression_final/` and remotely in `audit_final/`. The first independent matrix is also retained in `audit/`.

| Case | Status | Result mismatches | State equal |
|---|---|---:|---|
| check_errors | PASS | 0 | True |
| delete_reread | PASS | 0 | True |
| delivery_mini | PASS | 0 | True |
| digest_overflow | PASS | 0 | True |
| digest_wrap | PASS | 0 | True |
| expression_index_read | PASS | 0 | True |
| fold_hot | PASS | 0 | True |
| for_update_hot | PASS | 0 | True |
| foreign_key | PASS | 0 | True |
| foreign_key_deferred | PASS | 0 | True |
| indexed_nochange | PASS | 0 | True |
| indexed_update_read | PASS | 0 | True |
| key_updates | PASS | 0 | True |
| long_control | PASS | 0 | True |
| long_sql | PASS | 0 | True |
| onconflict_multi | PASS | 0 | True |
| onconflict_update | PASS | 0 | True |
| opf_swallowed | PASS | 0 | True |
| own_bitmap | PASS | 0 | True |
| own_delete_read | PASS | 0 | True |
| own_indexonly | PASS | 0 | True |
| own_indexonly_delete | PASS | 0 | True |
| own_insert_read | PASS | 0 | True |
| own_seqscan | PASS | 0 | True |
| own_update_read | PASS | 0 | True |
| own_update_twice | PASS | 0 | True |
| partial_index_enter | PASS | 0 | True |
| primary_empty | PASS | 0 | True |
| primary_indexonly | PASS | 0 | True |
| range_empty | PASS | 0 | True |
| range_existing | PASS | 0 | True |
| rotation | PASS | 0 | True |
| secondary_empty | PASS | 0 | True |
| secondary_existing | PASS | 0 | True |
| secondary_indexonly | PASS | 0 | True |
| sequence_error | PASS | 0 | True |
| sequence_retry | PASS | 0 | True |
| settle_forced | PASS | 0 | True |
| setval_retry | PASS | 0 | True |
| trigger | PASS | 0 | True |
| trigger_error | PASS | 0 | True |
| trigger_internal_code | PASS | 0 | True |
| trigger_own_read | PASS | 0 | True |
| trigger_statement | PASS | 0 | True |
| unique_errors | PASS | 0 | True |
| update_delete | PASS | 0 | True |
| volatile_random | MISMATCH | 0 | False |
| ycsb_point | PASS | 0 | True |
| stale_check_error | PASS | 0 | True |
| stale_div_error | PASS | 0 | True |
| stale_notnull_error | PASS | 0 | True |
| stale_raise_error | PASS | 0 | True |
| stale_mixed_error | PASS | 0 | True |
| cx_update_unique | PASS | 0 | True |
| cx_update_unique_pk | PASS | 0 | True |
| cx_caught_abort_insert | PASS | 0 | True |
| cx_caught_abort_update | PASS | 0 | True |
| cx_caught_abort_delete | PASS | 0 | True |
| cx_caught_unique | PASS | 0 | True |
| cx_delete_returning_cte | PASS | 0 | True |
| cx_delete_returning_rows | PASS | 0 | True |
| cx_delete_returning_into | PASS | 0 | True |
| cx_partial_unique | PASS | 0 | True |
| cx_exclusion | PASS | 0 | True |
| cx_empty_request | MISMATCH | 1 | True |
| cx_multi_statement | MISMATCH | 1 | False |

## Merkle probes and Gap2

`MR=.../audit/merkle_final bash .../audit/merkle_probes.sh` ran against the final install, port 5479, SERIALIZABLE. All SQL error lists exactly match the audit-merge original. Eleven probe-section outputs are byte-identical; the intended F15 section changes `same_root=t` to `same_root=f`, with verify_d still t. The remaining log-only difference is its timestamp. The harness contains existing negative/unsupported-DDL and malformed SQL probes; their errors are preserved, not called new regressions. See `probes_{old,final}.log` and `evidence_pg/probe_comparison.json`.

`python3 .../audit_gap2/gap2_final.py` ran detached, port 5483, final install. This is a copy of codexfix_20261009/audit/gap2_userdml.py; originals remain untouched. Changes are own paths/port, bounded memory, SERIALIZABLE for plain sessions, LE addition in the independent leaf oracle, and whole-transaction retries for serialization/deadlock failures during COPY.

- Small geometry: 8 partitions, fanout 4, 16/4 thresholds, 20,000 initial rows. All 5 normal phases pass; final 23,149 rows, 2,483 leaves, 3,308 nodes, depth 10.
- Canonical geometry: 200 partitions, fanout 32, 1024/256 thresholds, 260,000 initial rows. All 5 normal phases pass; final 261,400 rows, 6,400 leaves, 6,600 nodes, depth 5.
- Per geometry: 8 plain workers x 1,500 mixed transaction attempts, one 200-row COPY batch per worker, mass delete/refill, 3,000 native BCDB requests. Native request errors: 0 in both configurations; crashes: 0. Plain transactions include intentional aborts and allowed SQL/serialization failures, so this is not an all-transactions-succeeded benchmark.
- Deliberate sibling leaf hash/count swaps: exactly 2 oracle mismatches in each configuration, while root/structural merkle_verify remains true. This is an injected control, not a normal user-DML failure. It demonstrates why the independent leaf oracle is retained.
- Exact per-phase counts, operation/error/retry counts and controls: `gap2_final_evidence/gap2_final/{small,canonical}/result.json` and `summary.json`.

## OOM and bounded conclusions

No 100M AdHash baseline or working copy was created, and no smoke/grid TPS was measured. Storage fails even the conservative two-copy-plus-5-GiB minimum; see BLOCKED.md. No published data or canonical installs were changed. TPS equivalence to the paper cannot be determined from correctness tests. `comparison.csv` and `COMPARISON.md` contain all 28 reference points with new values blank.

`compare.py` ran on .247 for an empty/partial campaign. `test_compare.py` passed synthetic partial/complete median-min-max, det ratio and drift adjustment checks plus 9 rejection tests. Fixtures are isolated from campaign inputs and are not TPS samples.

AdHash repairs systematic duplicate pair cancellation. Matching checksums do not establish an adversarial cryptographic commitment. These checks cover the requested SQL/deferred maintenance and geometry scenarios, not every SQL program, crash schedule or multi-replica recovery scenario.
