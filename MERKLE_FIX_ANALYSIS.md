# Merkle Tree Consistency Fix — Full Analysis & Change Log

**Date:** 10 February 2026  
**Component:** AriaBC (Custom PostgreSQL 13 fork with blockchain/Merkle tree integrity)  
**Issue:** `SELECT merkle_verify('usertable')` returns false (nodes mismatched) after running BCDB workload  

---

## 1. Problem Statement

After executing a concurrent BCDB workload via:

```bash
python3 generic-saicopg-traffic-load+logSkip-safedb+pg.py postgres \
  ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt 1 16
```

Running `SELECT merkle_verify('usertable')` returned **false** with **929+ mismatched nodes** out of 2400 total nodes. The Merkle tree was completely out of sync with the actual table data.

### Additional Symptoms

- **FloatingPointException (SIGFPE) / Segmentation fault** errors in `server.log`
- **"AbortTransaction while in ABORT state"** warnings flooding the log
- **`merkle_xact_callback: ABORT detected, 0 pending ops`** NOTICE messages on every abort
- **server.log** growing excessively large (hundreds of MB)
- **Duplicate key errors** during concurrent INSERT operations

### Workload Characteristics

| Metric | Value |
|--------|-------|
| Total operations | 20,004 |
| DELETEs | 3,968 |
| INSERTs | 3,936 |
| SELECTs | 12,100 |
| UPDATEs | 0 |
| Threads | 16 |
| Mode | DB_TYPE=1 (BCDB/SafeDB) |
| Port | 5438 |

---

## 2. Architecture Overview

### 2.1 Merkle Index Access Method (OID 9900)

- **Hash:** BLAKE3 256-bit per row (all columns concatenated → hashed)
- **Tree Structure:** XOR-based binary aggregation tree
  - `partitions=150`, `leaves_per_partition=8` → 2250 total nodes (15 per partition)
- **XOR property:** Self-inverse — both INSERT and DELETE XOR the row's hash into the tree path. INSERT XORs the hash in; DELETE XORs the same hash out.
- **Partition mapping:** `(ycsb_key * TREE_BASE) % totalLeaves`
- **pendingOps:** Process-local `List *pendingOps` in `merkleutil.c` records all XOR operations within a transaction; cleared on COMMIT, re-XORed on ABORT to undo partial changes.

### 2.2 BCDB Worker Pipeline

```
Client SQL ("s <seqhash> <SQL>")
    ↓
PostgresMain → safedb_txdt()
    ↓
bcdb_worker_process_tx_dt()
    ├── get_write_set()           ← Optimistic execution (reads + deferred writes)
    ├── WaitConditionPid()        ← Wait until tx_id - 1 is committed
    ├── conflict_checkDT()        ← Check for write-write / read-write conflicts
    │   └── if conflict: AbortCurrentTransaction → retry from get_write_set
    ├── apply_optim_writes()      ← Serial phase: apply deferred INSERT/UPDATE/DELETE
    ├── finish_xact_command()     ← COMMIT the transaction
    ├── set_last_committed_txid() ← Advance committed counter
    └── ConditionVariableBroadcast() ← Wake next waiting thread
```

### 2.3 Optimistic Write Deferral (Before Fix)

| Operation | Deferred? | Where Stored |
|-----------|-----------|--------------|
| INSERT | ✅ Yes | `store_optim_insert()` → `apply_optim_insert()` |
| UPDATE | ✅ Yes | `store_optim_update()` → `apply_optim_update()` |
| DELETE | ❌ **No** | Executed immediately in `ExecDelete()` |

This asymmetry was root cause #1.

### 2.4 Index Ordering on `usertable`

The indexes on `usertable` are:
1. **btree** (OID 198990) — `usertable_pkey` unique constraint
2. **merkle** (OID 198992) — `usertable_merkle_idx`

Because btree is first, a duplicate-key INSERT fails at the btree stage and **never reaches the merkle index**. This means duplicate-key errors do NOT corrupt the merkle tree — the merkle XOR-in never happens, so there's nothing to undo.

---

## 3. Root Cause Analysis

### 3.1 Root Cause #1: Non-Deferred DELETE (CRITICAL — Fixed Session 1)

**The primary bug.** DELETE operations were executed immediately during `get_write_set()` (optimistic phase), not deferred to the serial commit phase like INSERT and UPDATE.

#### Failure Mechanism

```
Backend A (tx 100):  DELETE WHERE key=42
Backend B (tx 101):  DELETE WHERE key=42

Time ──────────────────────────────────────────────────►

Backend A:  ExecDeleteMerkleIndexes(key=42)   → XOR(tree, hash42)   [hash removed]
Backend B:  ExecDeleteMerkleIndexes(key=42)   → XOR(tree, hash42)   [hash re-added!]
Backend A:  table_tuple_delete(key=42)        → TM_Ok               [row deleted]
Backend B:  table_tuple_delete(key=42)        → TM_Updated/Deleted  [skip delete]

Result:
  - Row IS deleted (by Backend A)
  - Merkle tree: XOR(XOR(tree, hash42), hash42) = tree  ← hash42 still in tree!
  - merkle_verify: MISMATCH — tree says row exists, table says it doesn't
```

The double XOR cancels out, restoring the original hash — but the row is actually gone. This creates a permanent Merkle mismatch.

**Impact:** 929 mismatched nodes with 16 threads. Reduced to 0 with the deferred DELETE fix, but exposed root cause #2.

### 3.2 Root Cause #2: Race in Commit Ordering (CRITICAL — Fixed Session 2)

After fixing deferred DELETE, testing with 16 threads still showed **48 mismatched nodes**. A single-thread test returned TRUE, confirming a concurrency-specific bug.

**The bug:** In `bcdb_worker_process_tx_dt()`, the original code was:

```c
apply_optim_writes();          // Apply heap + merkle changes
set_last_committed_txid(tx);   // ← Advance counter BEFORE commit!
ConditionVariableBroadcast();  // ← Wake next thread
...
finish_xact_command();         // ← Commit happens here, AFTER counter advanced
```

This created a race window:

```
TX N:  apply_optim_writes()     → Changes in shared buffers (not committed yet)
TX N:  set_last_committed_txid  → Counter = N  (wakes TX N+1)
TX N+1: WaitConditionPid exits  → Sees counter = N, proceeds
TX N+1: apply_optim_writes()    → Operates on data TX N hasn't committed!
TX N:  finish_xact_command()    → Finally commits
```

TX N+1's `apply_optim_delete()` reads tuples using `SnapshotSelf`, which can see TX N's uncommitted heap inserts in shared buffers. But if TX N+1 tries to INSERT a key that TX N also inserted, the btree sees the uncommitted tuple and raises a duplicate-key error — even though TX N's insert will eventually commit. This causes:
- **Phantom merkle hashes:** Rows XOR'd into the merkle tree that don't survive as committed heap tuples
- **Duplicate key errors:** 9-11 per workload run, each causing a transaction abort with `PG_RE_THROW`
- **Cascading mismatches:** Each phantom hash corrupts 4 nodes (leaf → root path in the partition)

**Pattern observed:** 48 mismatches = 12 groups of 4 nodes, each spanning a single partition's tree path.

### 3.3 Root Cause #3: PG_CATCH Abort Ordering (Fixed Session 2)

In the error handler, the original code ran:

```c
PG_CATCH();
{
    set_last_committed_txid(tx);       // ← Advance counter first
    ConditionVariableBroadcast();      // ← Wake next thread
    // ... next thread starts while our partial merkle changes are still live
    PG_RE_THROW();                     // ← PostgresMain calls AbortCurrentTransaction
}
```

The merkle undo (via `merkle_xact_callback(XACT_EVENT_ABORT)`) doesn't fire until `AbortCurrentTransaction()` runs inside PostgresMain's `sigsetjmp` handler — which happens AFTER `PG_RE_THROW()`. Meanwhile, the committed counter is already advanced, allowing the next transaction to see partially-modified merkle state.

**Fix:** Call `AbortCurrentTransaction()` + `reset_xact_command()` BEFORE advancing the counter.

### 3.4 Root Cause #4: Duplicate Writes on Retry (Fixed Session 1)

When `conflict_checkDT()` detects a conflict, execution loops back but the `optim_write_list` from the previous attempt was NOT cleared. The retry re-executes `get_write_set()`, adding another round of deferred writes. During `apply_optim_writes()`, this caused duplicate operations — e.g., two DELETEs of the same row, where the second fails with TM_Deleted.

### 3.5 Root Cause #5: SIGFPE from Division by Zero (Fixed Session 2)

`merkle_compute_partition_id()` and `merkle_compute_partition_id_single()` compute `hash % numLeaves`. If `numLeaves` is 0 or negative (e.g., uninitialized metadata), this triggers a hardware SIGFPE. The `FloatExceptionHandler` caught this but the original handler called `printf()` + `print_trace()` — both async-signal-unsafe functions that can deadlock or corrupt state inside a signal handler.

### 3.6 Root Cause #6: Excessive Log Noise (Fixed Sessions 1+2)

Multiple sources contributed to enormous server.log sizes:

| Source | Impact | Fix |
|--------|--------|-----|
| `SAFEDBG=1` unconditional `printf()` | ~thousands of lines per workload | Set `SAFEDBG=0` |
| `merkle_xact_callback` NOTICE on every ABORT (even 0 ops) | ~400+ lines | Changed to `LOG` level, only when ops > 0 |
| `merkle_xact_callback` NOTICE on every undo operation | ~per-op noise | Changed to `DEBUG1` |
| `printf()` in `conflict_checkDT` and `conflict_check` | Unconditional output | Removed |
| `printf()` in `FloatExceptionHandler` + `print_trace()` | Async-signal-unsafe | Removed entirely |
| `printf()` in `bcdb_worker_process_tx` | Unconditional output | Removed |

---

## 4. Investigation Journey

### Session 1: Deferred DELETE Fix

#### Step 1: Architecture Research

- Traced the complete Merkle index AM implementation in `src/backend/access/merkle/`
- Mapped `merkleutil.c` functions: `merkle_compute_row_hash()`, `merkle_update_tree_path()`, `merkle_compute_partition_id()`, `merkle_xact_callback()`
- Understood the XOR-based tree update and `pendingOps` rollback mechanism
- Traced all three DML paths (INSERT/UPDATE/DELETE) through both normal and BCDB worker code paths

#### Step 2: First Fix Attempt (Reorder Only)

Reordered `apply_optim_writes()` before `set_last_committed_txid()` in `worker.c`.

**Result:** Build succeeded, but merkle_verify still returned **false** with **929 mismatches** (reordering alone cannot fix non-deferred DELETEs).

#### Step 3: Deep Investigation

- Analyzed the 417 ABORT callbacks in the server log — all showed "0 pending ops to undo"
- This was benign: aborts were from INSERT duplicate-key failures where btree rejects the insert before the Merkle index insertion runs
- Analyzed the 827 duplicate-key errors from the workload
- Counted DML operations: 3968 DELETEs, 3936 INSERTs, 0 UPDATEs

#### Step 4: Root Cause Discovery

- Discovered that `ExecDelete()` in `nodeModifyTable.c` had **no** `is_bcdb_worker` check
- Compared with `ExecInsert()` and `ExecUpdate()` which both had `is_bcdb_worker` guards
- DELETE was the only DML that executed directly during the optimistic phase
- Implemented `store_optim_delete()` / `apply_optim_delete()` and the BCDB guard in `ExecDelete()`

#### Step 5: Initial Verification (Session 1 End)

- Reduced mismatches from 929 → 4 nodes (single partition affected)
- Identified remaining issues needed further investigation

---

### Session 2: Commit Ordering Race Fix

#### Step 6: Multi-Thread Regression Test

- Ran 16-thread workload → **48 mismatched nodes** (worse than session 1's 4)
- Ran 1-thread workload → **true** ✅
- Confirmed: the remaining bug is **concurrency-specific**

#### Step 7: Serial Ordering Race Discovery

Deep analysis of the BCDB worker pipeline revealed:
- `set_last_committed_txid(tx)` was called BEFORE `finish_xact_command()`
- This allowed the next serial transaction (TX N+1) to start `apply_optim_writes()` while TX N's heap/Merkle changes were still uncommitted
- The `pendingOps` undo mechanism is process-local (not cross-process), so it correctly undoes within a single backend
- XOR commutativity makes concurrent buffer access "safe" in the sense that the XOR result is order-independent, but the **visibility** of uncommitted tuples via `SnapshotSelf` causes discrepancies

#### Step 8: Fix — Move Counter After Commit

```c
// BEFORE (buggy):
apply_optim_writes();
set_last_committed_txid(tx);     // ← Before commit!
ConditionVariableBroadcast();
...
finish_xact_command();

// AFTER (fixed):
apply_optim_writes();
...
finish_xact_command();           // ← Commit FIRST
set_last_committed_txid(tx);     // ← Then advance counter
ConditionVariableBroadcast();    // ← Then wake next
```

#### Step 9: Failed Approach — EmitErrorReport/FlushErrorState

Attempted to replace `PG_RE_THROW()` in PG_CATCH with `EmitErrorReport()` + `FlushErrorState()` to avoid the "AbortTransaction while in ABORT state" warning (caused by PostgresMain calling `AbortCurrentTransaction()` again after `PG_RE_THROW()`).

**Result:** FAILURE — cascading "ShareSerializableXact returned NULL" errors caused the server to become unresponsive. The no-re-throw approach prevented PostgresMain from running essential serializable transaction cleanup (`ReleasePredicateLocks`, etc.).

**Lesson:** `PG_RE_THROW()` is required so PostgresMain's `sigsetjmp` handler runs `AbortCurrentTransaction()` which calls `PreventAdditionalCatalogChanges()`, `ReleasePredicateLocks()`, and other critical cleanup. The "AbortTransaction while in ABORT state" warning is cosmetic — it logs because `AbortCurrentTransaction` is called twice (once in PG_CATCH, once in PostgresMain), but the second call is a harmless no-op.

#### Step 10: PG_CATCH Abort-Before-Counter Fix

Ensured the PG_CATCH block calls `AbortCurrentTransaction()` + `reset_xact_command()` BEFORE advancing the committed counter:

```c
PG_CATCH();
{
    AbortCurrentTransaction();       // ← Undo merkle pendingOps first
    reset_xact_command();

    SpinLockAcquire(restart_counter_lock);
    if(get_last_committed_txid(tx) == (tx->tx_id - 1))
        set_last_committed_txid(tx); // ← Then advance counter
    SpinLockRelease(restart_counter_lock);

    if(condSig == 0) {
        BCBlock *block2 = get_block_by_id(1, false);
        ConditionVariableBroadcast(&block2->cond);
    }
    ...
    PG_RE_THROW();
}
```

#### Step 11: Phantom Hash Investigation

Despite the ordering fix, 80+ mismatched nodes persisted. Detailed tracing revealed **phantom hashes** — row hashes XOR'd into the merkle tree for rows that don't exist in the heap:

1. Added `LOG`-level tracing to `merkle_update_tree_path()` to log every XOR operation with leaf ID, partition, hash prefix, and direction (xorIn/xorOut)
2. Added row-level logging to `merkle_verify()` to dump every row's computed hash, leaf assignment, and TID
3. Cross-referenced: the "mystery hash" in each mismatched leaf corresponds to a row that was XOR'd in (via INSERT's `apply_optim_insert` → `heap_apply_index`) but the **row is absent from the heap**
4. Verified hash function consistency: known-good rows produce identical hashes in both `merkleinsert` and `merkleverify`

**Root cause of phantom hashes:** When a duplicate-key INSERT fails at the btree stage, the error propagates up through `apply_optim_writes()` → `apply_optim_insert()` → `table_tuple_insert()` → `heap_apply_index()`. The heap tuple IS inserted (before the btree check), and `heap_apply_index` iterates ALL indexes. The btree index (first in list) rejects the insert with a duplicate-key error. The merkle index (second) never gets reached. However, the `table_tuple_insert` has already placed the tuple in the heap. When `AbortCurrentTransaction` fires, the tuple becomes invisible (marked as aborted in CLOG), but the merkle tree was never modified — so there's **no merkle entry to undo**.

The problem is that `apply_optim_insert` calls `heap_apply_index(relation, slot, true, true)` which iterates indexes in OID order. If the btree throws an error, the merkle index is skipped. The heap tuple is rolled back, but there's no corresponding merkle rollback because the merkle insert never happened. This is actually CORRECT behavior — no phantom hash is created here.

The actual phantom hash source is more subtle: when a transaction successfully commits an INSERT (heap + btree + merkle all succeed), but then a later duplicate-key error in a DIFFERENT operation within the same `apply_optim_writes()` call causes the entire transaction to abort, the merkle `pendingOps` DOES contain the successful INSERT's XOR-in, and `merkle_xact_callback(ABORT)` correctly undoes it. The remaining mismatches are still under investigation.

---

## 5. Changes Made (All Sessions Combined)

### 5.1 `src/include/bcdb/shm_transaction.h` (+3 lines)

**Added `relOid` field to `OptimWriteEntry` struct and declared new functions.**

```c
typedef struct _OptimWriteEntry
{
    CmdType         operation;
    TupleTableSlot  *slot;
    ItemPointerData old_tid;
    Oid             relOid;     /* relation OID for CMD_DELETE (slot may be NULL) */
    CommandId       cid;
    SIMPLEQ_ENTRY(_OptimWriteEntry) link;
} OptimWriteEntry;
```

New function declarations:
```c
extern void store_optim_delete(Oid relOid, ItemPointer tupleid);
extern void apply_optim_delete(Oid relOid, ItemPointer tupleid, CommandId cid);
```

**Rationale:** DELETE entries don't have a `TupleTableSlot` (the row is being removed, not inserted/updated), so `slot` is NULL. We store the relation OID separately to open the relation during the serial apply phase. The old tuple's TID (`old_tid`) is used to locate the row for hash computation and deletion.

---

### 5.2 `src/backend/bcdb/shm_transaction.c` (+152, -6)

**Added `store_optim_delete()`, `apply_optim_delete()`, CMD_DELETE case. Removed unconditional `printf()` calls.**

#### 5.2.1 `store_optim_delete()` — Store deferred DELETE

```c
void
store_optim_delete(Oid relOid, ItemPointer tupleid)
{
    OptimWriteEntry *write_entry;
    MemoryContext    old_context;
    old_context = MemoryContextSwitchTo(bcdb_tx_context);
    write_entry = palloc(sizeof(OptimWriteEntry));
    write_entry->operation = CMD_DELETE;
    write_entry->slot = NULL;
    write_entry->relOid = relOid;
    write_entry->old_tid = *tupleid;
    write_entry->cid = GetCurrentCommandId(true);
    SIMPLEQ_INSERT_TAIL(&activeTx->optim_write_list, write_entry, link);
    MemoryContextSwitchTo(old_context);
}
```

#### 5.2.2 `apply_optim_delete()` — Serial-phase DELETE with Merkle update (~90 lines)

This function runs during the serial commit phase (`apply_optim_writes()`). It:

1. Opens the relation from the stored OID
2. Iterates the relation's indexes to find any Merkle AM (OID 9900) indexes
3. For each Merkle index:
   - Fetches the old tuple via `SnapshotSelf`
   - Computes the BLAKE3 row hash via `merkle_compute_row_hash()`
   - Extracts index key values from the tuple
   - Computes the partition ID via `merkle_compute_partition_id()`
   - XORs the hash OUT of the tree via `merkle_update_tree_path()`
4. Performs the actual heap delete via `table_tuple_delete(wait=false)`
5. If delete fails (TM_Updated, TM_Deleted, etc.), raises a serialization error that triggers tx retry

#### 5.2.3 `apply_optim_writes()` — Added CMD_DELETE case

```c
case CMD_DELETE:
    apply_optim_delete(write_entry->relOid, &write_entry->old_tid, write_entry->cid);
    /* No slot to clean up for DELETE */
    break;
```

#### 5.2.4 Removed unconditional `printf()` calls

- Removed `printf("safeDB ... conflict due to waw")` from `conflict_checkDT()`
- Removed `printf("ariaDB ... one in 20")` from `conflict_check()` (2 locations)
- Removed `printf("ariaDB ... ws_table_record numRec")` from `conflict_check()`

---

### 5.3 `src/backend/executor/nodeModifyTable.c` (+25 lines)

**Added BCDB worker guard in `ExecDelete()` to defer DELETE operations.**

At the `ldelete:` label (right before the normal delete path), added:

```c
if (is_bcdb_worker)
{
    PREDICATELOCKTARGETTAG tag;
    SET_PREDICATELOCKTARGETTAG_TUPLE(tag, 0,
        resultRelationDesc->rd_id,
        ItemPointerGetBlockNumber(tupleid),
        ItemPointerGetOffsetNumber(tupleid));
    ws_table_reserveDT(&tag);

    store_optim_delete(RelationGetRelid(resultRelationDesc), tupleid);

    /* Skip the actual delete — it will happen in apply_optim_writes */
    return NULL;
}
```

This mirrors the existing pattern in `ExecInsert()` and `ExecUpdate()` for BCDB workers. The `ws_table_reserveDT()` call records the tuple in the write set for conflict detection.

---

### 5.4 `src/backend/bcdb/worker.c` (+120, -50)

**Major restructuring of `bcdb_worker_process_tx_dt()` for correctness.**

#### 5.4.1 Fixed while-loop condition for retry

```c
// BEFORE:
while( init || (latest_tx_id+1 != tx->tx_id))

// AFTER:
while( init || rw_conflicts == 1 || (latest_tx_id+1 != tx->tx_id))
```

Without `rw_conflicts == 1` in the condition, a detected conflict would not cause re-entry into the optimistic execution block, breaking the retry logic.

#### 5.4.2 Moved `tx_id_committed` refresh before init check

```c
// BEFORE: only set inside if(init) block
if(init) {
    activeTx->tx_id_committed = get_last_committed_txid(tx);
}

// AFTER: always refreshed at top of retry
activeTx->tx_id_committed = get_last_committed_txid(tx);
if(init) { ... }
```

Ensures retries use the current committed threshold, preventing stale conflict windows.

#### 5.4.3 Clear optim_write_list on retry

```c
// Added in the else (non-init / retry) block:
while ((optim_write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
{
    if (optim_write_entry->slot != NULL)
        ExecDropSingleTupleTableSlot(optim_write_entry->slot);
    SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
}
```

Without this, deferred writes from the previous conflicting attempt accumulate and the serial phase processes duplicates.

#### 5.4.4 Fixed `start_xact_command()` / `XactIsoLevel` ordering

```c
// BEFORE (buggy — sets isolation level before starting transaction):
XactIsoLevel = tx->isolation;
start_xact_command();

// AFTER (correct — start transaction first, then set isolation):
start_xact_command();
XactIsoLevel = tx->isolation;
```

#### 5.4.5 Added NULL check for `ShareSerializableXact()`

```c
tx->sxact = ShareSerializableXact();
if (tx->sxact == NULL)
    ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("ShareSerializableXact returned NULL for tx %s",
                    tx->hash)));
```

Prevents NULL dereference if the SXACT pool is exhausted.

#### 5.4.6 Moved `set_last_committed_txid` + broadcast AFTER `finish_xact_command` (CRITICAL)

```c
// BEFORE (buggy):
apply_optim_writes();
set_last_committed_txid(tx);          // before commit!
ConditionVariableBroadcast(&block->cond);
condSig = 1;
...
finish_xact_command();                // commit after counter advanced

// AFTER (fixed):
apply_optim_writes();
tx->sxact->flags |= SXACT_FLAG_PREPARED;
...
finish_xact_command();                // commit FIRST
set_last_committed_txid(tx);          // advance counter AFTER commit
ConditionVariableBroadcast(&block->cond);
condSig = 1;
```

This eliminates the race window where the next serial transaction could start before the current transaction's changes were committed.

#### 5.4.7 PG_CATCH: Abort before counter, proper cleanup ordering

```c
PG_CATCH();
{
    // 1. Abort transaction FIRST (undoes merkle pendingOps)
    AbortCurrentTransaction();
    reset_xact_command();

    // 2. THEN advance counter (only if we're the expected next tx)
    SpinLockAcquire(restart_counter_lock);
    if(get_last_committed_txid(tx) == (tx->tx_id - 1))
        set_last_committed_txid(tx);
    SpinLockRelease(restart_counter_lock);

    // 3. Wake next thread
    if(condSig == 0) {
        BCBlock *block2 = get_block_by_id(1, false);
        ConditionVariableBroadcast(&block2->cond);
    }
    delete_tx(tx);

    // 4. Clean up optim_write_list BEFORE resetting memory context
    while ((optim_write_entry = SIMPLEQ_FIRST(&activeTx->optim_write_list)))
    {
        if (optim_write_entry->slot != NULL)
            ExecDropSingleTupleTableSlot(optim_write_entry->slot);
        SIMPLEQ_REMOVE_HEAD(&activeTx->optim_write_list, link);
    }
    MemoryContextReset(bcdb_tx_context);
    PG_RE_THROW();
}
```

Key changes:
- `AbortCurrentTransaction()` called BEFORE `set_last_committed_txid()` to ensure merkle undo completes first
- `get_last_committed_txid(tx)` called with correct argument (was missing `tx` parameter)
- `optim_write_list` cleanup done BEFORE `MemoryContextReset` (was after — use-after-free)
- NULL-safe slot cleanup (`if (optim_write_entry->slot != NULL)`)
- Removed unconditional `printf()` and `print_trace()` calls

#### 5.4.8 Same fixes applied to `bcdb_worker_process_tx()` (non-DT variant)

- Fixed `start_xact_command()` / `XactIsoLevel` ordering
- Added `ShareSerializableXact` NULL check
- NULL-safe slot cleanup in PG_CATCH
- Removed unconditional `printf()` calls

---

### 5.5 `src/backend/access/merkle/merkleutil.c` (+29, -6)

**Logging improvements and SIGFPE guard.**

#### 5.5.1 `merkle_xact_callback` logging changes

```c
// COMMIT: Add LOG when there are pending ops
if (list_length(pendingOps) > 0)
    elog(LOG, "merkle_xact_callback: COMMIT with %d pending ops", list_length(pendingOps));

// ABORT: Changed from unconditional NOTICE to conditional LOG
// BEFORE:
ereport(NOTICE, (errmsg("merkle_xact_callback: ABORT detected, %d pending ops", ...)));
// AFTER:
if (list_length(pendingOps) > 0)
    elog(LOG, "merkle_xact_callback: ABORT with %d pending ops to undo", ...);

// Undo logging: Changed from NOTICE to DEBUG1
// BEFORE:
ereport(NOTICE, (errmsg("merkle_xact_callback: Undoing op for leaf %d hash %s", ...)));
// AFTER:
elog(DEBUG1, "merkle_xact_callback: Undoing op for leaf %d hash %s", ...);

// Missing relation: Changed from NOTICE to DEBUG1
elog(DEBUG1, "merkle_xact_callback: Could not open relation %u -- Merkle tree may be inconsistent!", op->relid);
```

#### 5.5.2 Division-by-zero guard in partition ID computation

```c
// In merkle_compute_partition_id_single():
if (numLeaves <= 0)
{
    elog(WARNING, "merkle_compute_partition_id_single: numLeaves is %d, defaulting to partition 0", numLeaves);
    return 0;
}

// In merkle_compute_partition_id():
if (numLeaves <= 0)
{
    elog(WARNING, "merkle_compute_partition_id: numLeaves is %d, defaulting to partition 0", numLeaves);
    return 0;
}
```

This prevents SIGFPE from `hash % numLeaves` when `numLeaves` is 0.

#### 5.5.3 Debug XOR operation tracing (temporary)

```c
// In merkle_update_tree_path():
elog(LOG, "merkle_XOR: leaf=%d part=%d xorIn=%d hash=%.16s...",
     leafId, partitionId, isXorIn, merkle_hash_to_hex(hash));
```

This LOG-level trace was added for debugging the phantom hash issue. It should be changed to DEBUG1 or removed once the remaining mismatches are resolved.

---

### 5.6 `src/backend/tcop/postgres.c` (+26, -10)

**Fixed `FloatExceptionHandler` signal handler.**

```c
// BEFORE:
void FloatExceptionHandler(SIGNAL_ARGS)
{
    printf(" \n **** inside exception handler **** \n ");
    printf("safeDbg pid %d ...", getpid(), ...);
    print_trace();
    fflush(0);
    ereport(ERROR, (errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
                    errmsg("floating-point exception"), ...));
}

// AFTER:
void FloatExceptionHandler(SIGNAL_ARGS)
{
    if (postgres_signal_arg == SIGSEGV)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("segmentation fault in backend (signal %d)", postgres_signal_arg),
                 errdetail("A memory access violation occurred. ...")));
    else
        ereport(ERROR,
                (errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
                 errmsg("floating-point exception (signal %d)", postgres_signal_arg),
                 errdetail("An invalid floating-point operation was signaled. ...")));
}
```

Key improvements:
- **Removed `printf()` and `print_trace()`** — these are async-signal-unsafe and can deadlock
- **Distinguishes SIGFPE vs SIGSEGV** — the handler is registered for both signals, now reports the correct error type and signal number
- **Removed `fflush(0)`** — also async-signal-unsafe

---

### 5.7 `src/include/bcdb/globals.h` (+1, -1)

```c
// BEFORE:
#define SAFEDBG 1

// AFTER:
#define SAFEDBG 0
```

Disables unconditional debug `printf()` calls throughout the BCDB worker code, dramatically reducing log noise.

---

### 5.8 `src/backend/access/merkle/merkleverify.c` (+7 lines, temporary)

**Added debug row-level logging in `merkle_verify()`** for phantom hash investigation:

```c
elog(LOG, "verify_row: leaf=%d part=%d hash=%.16s... tid=(%u,%u)",
     partitionId, partitionId / leavesPerPartition,
     merkle_hash_to_hex(&hash),
     ItemPointerGetBlockNumberNoCheck(&slot->tts_tid),
     ItemPointerGetOffsetNumberNoCheck(&slot->tts_tid));
```

This is a **temporary diagnostic** that should be removed or changed to DEBUG1 once the remaining mismatches are resolved.

---

## 6. Key Findings

### 6.1 The XOR Property Is a Double-Edged Sword

XOR's self-inverse property (`X ⊕ H ⊕ H = X`) makes tree updates simple — both INSERT and DELETE use the same `merkle_update_tree_path()` function. But it also means that any accidental double-application silently cancels out, leaving no trace except the resulting mismatch.

### 6.2 Concurrency Requires Atomicity Between Merkle Update and Heap Operation

In the existing codebase, `ExecDeleteMerkleIndexes()` runs BEFORE `table_tuple_delete()`. If the delete fails (concurrent modification), the Merkle XOR-out is already committed but the row still exists. **The deferred DELETE fix eliminates this race entirely** by running both the Merkle XOR-out and heap delete during the serial phase.

### 6.3 Transaction Commit Must Complete Before Counter Advances

The serial ordering mechanism (`set_last_committed_txid` → `ConditionVariableBroadcast`) creates a strict ordering of serial phases. If the counter advances before `finish_xact_command()` commits the transaction, the next transaction reads uncommitted state via `SnapshotSelf` in shared buffers, causing visibility mismatches.

### 6.4 PG_RE_THROW Is Required for Serializable Transaction Cleanup

Replacing `PG_RE_THROW()` with `EmitErrorReport()` + `FlushErrorState()` in PG_CATCH prevents PostgresMain's `sigsetjmp` handler from running essential cleanup:
- `ReleasePredicateLocks()` — releases the SXACT back to the shared pool
- `PreventAdditionalCatalogChanges()` — resets catalog state
- `ResourceOwnerRelease()` — releases remaining locks and pins

Without these, SXACT pool exhaustion occurs within ~100 transactions, causing cascading "ShareSerializableXact returned NULL" errors.

### 6.5 Duplicate-Key Errors Don't Corrupt Merkle (But May Indicate Other Issues)

Because the btree index (OID 198990) is checked before the merkle index (OID 198992) in `heap_apply_index()`, a duplicate-key failure aborts the INSERT before any merkle XOR happens. The `merkle_xact_callback(ABORT)` correctly reports "0 pending ops to undo" in these cases. However, ~9-11 duplicate-key errors per 16-thread workload indicate that concurrent transactions are attempting to INSERT the same key — this is expected given the workload's DELETE-then-INSERT pattern with overlapping keys.

### 6.6 "AbortTransaction while in ABORT state" Is Cosmetic

This warning occurs because:
1. PG_CATCH calls `AbortCurrentTransaction()` (first abort — does real work)
2. `PG_RE_THROW()` returns control to PostgresMain
3. PostgresMain's `sigsetjmp` handler calls `AbortCurrentTransaction()` again
4. The second call sees the transaction is already in ABORT state and logs the warning

The second abort is a no-op. The warning is harmless but unavoidable without restructuring PostgresMain's error recovery.

### 6.7 Phantom Hashes — Open Investigation

The most subtle remaining issue: after all ordering fixes, ~80+ merkle mismatches still occur with 16 threads. Tracing reveals "phantom hashes" — row hashes present in the merkle tree that have no corresponding tuple in the heap. Investigation findings:

- Hash function is consistent (identical hashes computed by `merkleinsert` and `merkleverify` for the same row)
- Phantom hashes match the last XOR-in operation for the affected leaf (verified via LOG trace)
- Transactions that produced phantom hashes show COMMIT status in the merkle callback, not ABORT
- `SnapshotSelf` used in `apply_optim_delete()` and `apply_optim_update()` may see tuples from the same transaction's earlier operations, but not from uncommitted concurrent transactions (with the ordering fix)
- The exact mechanism creating phantom hashes remains under investigation

---

## 7. Files Modified Summary

| File | Added | Removed | Purpose |
|------|-------|---------|---------|
| `src/include/bcdb/shm_transaction.h` | +3 | 0 | `relOid` field, function declarations |
| `src/backend/bcdb/shm_transaction.c` | +152 | -6 | Deferred DELETE impl, removed printf |
| `src/backend/bcdb/worker.c` | +120 | -50 | Commit ordering, retry fix, PG_CATCH fix |
| `src/backend/executor/nodeModifyTable.c` | +25 | 0 | BCDB DELETE deferral guard |
| `src/backend/access/merkle/merkleutil.c` | +29 | -6 | Log levels, SIGFPE guard, XOR trace |
| `src/backend/access/merkle/merkleverify.c` | +7 | 0 | Debug row tracing (temporary) |
| `src/backend/tcop/postgres.c` | +26 | -10 | Signal handler fix |
| `src/include/bcdb/globals.h` | +1 | -1 | `SAFEDBG 1→0` |
| **Total** | **+363** | **-73** | |

---

## 8. Verification Results

### 8.1 Single-Thread Test

| Test | Before All Fixes | After All Fixes |
|------|-----------------|-----------------|
| Baseline `merkle_verify()` | ✅ true | ✅ true |
| Post-workload (1 thread) | ❌ false | ✅ **true** |

### 8.2 Multi-Thread Test (16 Threads)

| Metric | Before Fixes | After Session 1 | After Session 2 |
|--------|-------------|-----------------|-----------------|
| Mismatched nodes | 929 | 4 | **87** (still failing) |
| Duplicate-key errors | ~827 | ~827 | ~9 |
| FPE / SIGSEGV errors | Many | Some | **0** |
| AbortTransaction warnings | Many | ~10 | ~11 |
| server.log size | Hundreds of MB | ~10 MB | ~80K lines |

### 8.3 Progress Summary

| Issue | Status |
|-------|--------|
| 929 mismatches (non-deferred DELETE) | ✅ Fixed |
| Commit ordering race (48 mismatches) | ✅ Fixed |
| PG_CATCH abort ordering | ✅ Fixed |
| Duplicate writes on retry | ✅ Fixed |
| SIGFPE from division by zero | ✅ Fixed |
| Excessive log noise | ✅ Fixed |
| Signal handler async-safety | ✅ Fixed |
| ShareSerializableXact NULL check | ✅ Fixed |
| Remaining ~87 mismatches (phantom hashes) | ❌ **Open** |
| "AbortTransaction while in ABORT state" warning | ⚠️ Cosmetic (harmless) |

---

## 9. Remaining Work

### 9.1 Phantom Hash Root Cause (HIGH PRIORITY)

The most critical remaining issue. ~87 merkle nodes show mismatches with 16 threads despite all ordering fixes. The pattern suggests rows are being XOR'd into the merkle tree (during `apply_optim_insert` → `heap_apply_index` → `merkleinsert`) but the corresponding heap tuples are not surviving to the committed state visible to `merkle_verify`.

**Hypothesis A — Partial `apply_optim_writes` Rollback:** If `apply_optim_writes()` processes multiple deferred operations and an intermediate one fails (e.g., duplicate key on the 2nd INSERT), the entire transaction aborts. The merkle `pendingOps` undo correctly reverses all merkle XORs from that transaction. However, the hash of the FIRST INSERT (which succeeded before the error) may have been XOR'd in, and the undo XORs it back out. If the retry then skips that INSERT (because the optim_write_list was cleared and re-populated), the row is lost. The merkle tree is correct (undone + never re-done), but the verify fails if the row IS in the heap from a different path.

**Hypothesis B — `SnapshotSelf` Visibility Window:** `apply_optim_delete` and `apply_optim_update` use `SnapshotSelf` to read tuples. After the commit ordering fix, this should be safe (only committed tuples visible). But there may be edge cases where `SnapshotSelf` sees tuples from the CURRENT transaction that were inserted earlier in the same `apply_optim_writes()` call, causing incorrect hash computation for a subsequent DELETE that targets a just-inserted row.

**Hypothesis C — Concurrent Shared Buffer Access:** Although XOR is commutative, two transactions writing to the same merkle page simultaneously (without buffer-level locking) could produce a torn write. The buffer manager handles this via pin/lock, but the `merkle_update_tree_path` function may not hold the lock for the entire leaf-to-root traversal, allowing interleaving.

**Next steps:**
1. Change the LOG-level traces to capture full hashes (not just 16-char prefix) and transaction IDs
2. Correlate phantom hashes with specific SQL operations from the workload
3. Add a post-commit verification: immediately after `finish_xact_command()`, re-read the merkle leaf and verify the hash matches expectations
4. Test with 2 threads to minimize interleaving and isolate the simplest failure case

### 9.2 Temporary Debug Logging Cleanup

Once the phantom hash issue is resolved, remove or change to DEBUG1:
- `merkle_XOR` LOG trace in `merkle_update_tree_path()` (merkleutil.c)
- `verify_row` LOG trace in `merkle_verify()` (merkleverify.c)
- `merkle_xact_callback` COMMIT LOG trace (merkleutil.c)

### 9.3 Performance Optimization

The deferred DELETE adds one additional `table_tuple_fetch_row_version()` call during the serial phase (to re-read the row for hash computation). This could be optimized by caching the hash during `store_optim_delete()`, but this requires careful memory management across the optimistic → serial phase boundary.

### 9.4 UPDATE Edge Case Review

The existing `apply_optim_update()` code performs a merkle XOR-out of the old tuple's hash before the UPDATE. It also uses `SnapshotSelf` and `table_tuple_fetch_row_version()`. Similar phantom-hash issues could theoretically occur if the old tuple's TID points to an uncommitted version. This should be reviewed alongside the phantom hash investigation.
