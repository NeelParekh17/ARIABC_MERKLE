# AdHash combine and subtraction inventory

The historical C/SQL symbol `merkle_hash_xor` / `merkle_hash_xor_sql` and
`xor_delta` field names are retained. Their operation is now unsigned
little-endian addition modulo 2^256, with carry across every byte. The exact
post-patch call-site listing is `combine_sites.txt`.

| File / functions | Contribution or operation |
|---|---|
| `merkleutil.c`: `merkle_hash_xor`, `merkle_hash_xor_sql` | One engine addition implementation; SQL wrapper delegates to it. `merkle_hash_negate` computes in-place two's complement across all 32 bytes. |
| `merkledelta.c`: `merkle_stage_delta_event` | INSERT adds positive row hash. DELETE copies and negates the supplied positive old hash exactly once, then adds it. SAME_LEAF UPDATE consumes an already signed delta. Count deltas remain independent. |
| `merkledelta.c`: `merkle_delta_merge_one`, subxact commit, synchronous frame aggregation, partition notice aggregation | Add already signed deltas. Abort discards frames; no inverse replay or second negation. |
| `nodeModifyTable.c`: `CaptureMerkleDeletePlan` / `ApplyMerkleDeletePlan`, `ExecDeleteMerkleIndexes` | Supply positive old-row hash to DELETE staging. SQL UPDATE uses this removal followed by one INSERT contribution; HOT/non-HOT suppression is unchanged. |
| `nodeModifyTable.c`: `ExecInsertMerkleIndexes`; `merkleinsert.c`: `merkleInsert` | Supply positive new-row hashes. COPY and normal index insertion use the same path. |
| `shm_transaction.c`: `apply_optim_update`, same route | Negate `oldHash` locally, add `newHash`, stage SAME_LEAF UPDATE: new minus old. This is the second and only other negation call site. |
| `shm_transaction.c`: `apply_optim_update`, changed route | Stage positive old hash as DELETE and new hash as INSERT. Staging performs the single removal negation. |
| `shm_transaction.c`: deferred TID deletion and `apply_deferred_delete_by_key` | Positive old hash is staged as DELETE only after a successful heap removal. These do not negate locally. |
| `merkleapply.c`: coalesced leaf/ancestor nodes, `merkle_direct_update_node` | Add signed deltas to old stored hashes. Count-zero canonicalization remains zero. No event-type-based second negation. |
| `merkleapply.c`: SPI leaf/ancestor update plans | Existing SQL wrapper calls now add the same signed contributions. |
| `merkleapply.c`: `merkle_do_split_in_memory`, `do_merge_check` | Recompute bucket sums from positive row hashes or child sums. Splits/merges preserve the total sum and count. |
| `merklebuild.c`: `merkle_build_tree_pass1`, bulk partition root construction | Cached BLAKE3 hashers still finalize the unchanged canonical row hash; all row/bucket/child/root aggregation calls use the common addition implementation. |
| `merkleverify.c`: heap sum, root sum, children checks, global root | All common-combiner calls add. Shared root/verification entry now checks the metapage version first. |
| `ariabc_pg/src/replica_repair.cxx`: `compute_merkle_digest` | Fixed 32-byte accumulator with full-byte carry. Rejects malformed root lengths. `read_partition_roots` now calls the native version-checked helper. |
| `scripts/distributed/recovery/remote_db.py` | Python `int.from_bytes(..., 'little')`, addition modulo 2^256, 32-byte LE serialization. Root and partition reads use version-checked native helpers. |
| Copied `gap2_userdml.py` / `gap2_final.py` | Independent leaf accumulator changed from XOR to 256-bit LE addition. Original lab files were not edited. |

No other executable Merkle row/root byte-XOR recomputation was found in the
tracked engine/server/recovery scripts. Unrelated hash-table/key/authentication
mixers retain their XOR operations. Search evidence is the call-site inventory
and the source diff. All removal call sites feed DELETE staging, except the
explicit same-route deferred update, which supplies new-minus-old directly.

Index format is **11**, routing format **4**, canonical row-hash format **1**.
Reserved delta format is **2**; the current synchronous checkout has no active
encoder/decoder using that constant. The metapage version check prevents old
XOR index use. Migration fixes preserve its OID list outside SPI's freed
context and verify using the current command ID. Operator procedure and
quiescence requirements are in the clone's `README_MERKLE.md` upgrade section.
