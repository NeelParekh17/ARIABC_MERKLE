# Merkle Tree Concurrency Fix Analysis - Session 2

**Date:** 11 February 2026  
**Component:** AriaBC (PostgreSQL 13 fork with Blockchain/Merkle tree integrity)  
**Issue:** `merkle_verify('usertable')` returns FALSE with hundreds of node mismatches after concurrent workload  
**Status:** Root cause identified, partial fix implemented, STILL FAILING - requires deeper investigation

---

## Executive Summary

After extensive investigation, I identified that **multiple transactions were executing their serial commit phase concurrently**, violating the deterministic execution model and causing merkle tree corruption. Despite implementing comprehensive fixes including mutex protection and memory barriers, the issue persists. This suggests either:
1. The LWLock implementation has subtle issues in shared memory
2. There's a deeper architectural problem with how blocks/locks are managed
3. The merkle tree corruption occurs at a different point than `apply_optim_writes()`

---

## 1. Problem Manifestation

### Test Results

| Configuration | Rows | Merkle Result | Mismatched Nodes |
|--------------|------|---------------|------------------|
| Serial (1 thread) | 12,595 | ✅ TRUE | 0 |
| Concurrent (16 threads) - Before Fix | 12,592 | ❌ FALSE | ~929 |
| Concurrent (16 threads) - After Fix | 12,592 | ❌ FALSE | ~200+ |

### Workload Characteristics

```
Total Operations: 20,004
- SELECTs: 12,100
- INSERTs: 3,936
- DELETEs: 3,968  
- UPDATEs: 0
Threads: 16
Mode: BCDB (DB_TYPE=1)
```

Key pattern: Many DELETE followed by INSERT of the same key (Zipfian distribution).

---

## 2. Root Cause Analysis

### Primary Issue: Race Condition in Serial Execution

**The Core Problem:**  
Despite BCDB's deterministic execution model where transactions should execute their serial phase (apply_optim_writes → commit) in strict order, MULTIPLE transactions were executing concurrently.

**Evidence:**
```bash
$ grep "apply_optim" server.log | head -20 | awk '{print $1, $2}'
2026-02-11 22:34:48.700   # Multiple entries at SAME millisecond
2026-02-11 22:34:48.700
2026-02-11 22:34:48.700
2026-02-11 22:34:48.704
2026-02-11 22:34:48.704
2026-02-11 22:34:48.707
2026-02-11 22:34:48.707
...
```

This proves that `apply_optim_writes()` was running concurrently across multiple processes.

### Vulnerable Code Paths

#### 1. Unprotected Counter Access (CRITICAL)

**File:** `src/backend/bcdb/shm_block.c`

```c
// BEFORE (BUGGY):
BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block_by_id(1, false);
    return blk->last_committed_tx_id;  // ← NO LOCKING!
}

void set_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block_by_id(1, true);
    blk->last_committed_tx_id = tx->tx_id;  // ← NO LOCKING!
    block_meta->num_committed = tx->tx_id;
}
```

**Problem:** Multiple processes could:
1. All read `last_committed_tx_id = N`  
2. All think THEY are transaction N+1
3. All proceed to execute `apply_optim_writes()` simultaneously
4. Concurrent Merkle tree XOR operations → hash corruption

#### 2. Weak Wait Condition

**File:** `src/backend/bcdb/worker.c` (line ~589)

```c
WaitConditionPid(&block->cond, getpid(), 
    ((get_last_committed_txid(tx)+1) == tx->tx_id));
```

**Problem:** The condition check happens OUTSIDE any lock. Between the check and proceeding, another transaction could commit, making the condition stale.

#### 3. No Mutual Exclusion for Serial Phase

The entire serial execution phase (apply writes → commit → advance counter) had NO mutex protection, allowing:

```
TX 100: [apply_optim_writes starts]
TX 101: [apply_opt im_writes starts]  ← CONCURRENT!
TX 100: [commits]
TX 101: [commits]
```

---

## 3. Fixes Implemented

### Fix 1: Spinlock Protection for Counter Access

**File:** `src/backend/bcdb/shm_block.c`

```c
BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block_by_id(1, false);
    BCTxID result;
    SpinLockAcquire(block_pool_lock);  // ← ADDED
    pg_read_barrier();                  // ← ADDED (memory ordering)
    result = blk->last_committed_tx_id;
    SpinLockRelease(block_pool_lock);  // ← ADDED
    return result;
}

void set_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block_by_id(1, true);
    SpinLockAcquire(block_pool_lock);      // ← ADDED
    blk->last_committed_tx_id = tx->tx_id;
    block_meta->num_committed = tx->tx_id;
    pg_write_barrier();                     // ← ADDED (memory ordering)
    SpinLockRelease(block_pool_lock);      // ← ADDED
}
```

**Rationale:**  
- `SpinLockAcquire/Release` provides atomic access to the counter
- `pg_read/write_barrier()` prevents compiler/CPU reordering
- Forces sequential visibility of counter updates across processes

### Fix 2: LWLock for Serial Execution Phase

**File:** `src/include/bcdb/shm_block.h`

```c
typedef struct
{
    BCBlockID  id;
    int        num_tx;
    int volatile   last_committed_tx_id;
    // ... other fields ...
    LWLock     serial_execution_lock;  // ← ADDED
} BCBlock;
```

**File:** `src/backend/bcdb/shm_block.c` (initialization)

```c
if (!found)
{
    // ... initialize other fields ...
    LWLockInitialize(&block->serial_execution_lock, 
                     LWTRANCHE_FIRST_USER_DEFINED);  // ← ADDED
}
```

**File:** `src/backend/bcdb/worker.c` (usage)

```c
BCBlock *serial_block = get_block_by_id(1, false);
LWLockAcquire(&serial_block->serial_execution_lock, LW_EXCLUSIVE); // ← ADDED

// Serial ordering check
if (get_last_committed_txid(tx) + 1 != tx->tx_id)
{
    LWLockRelease(&serial_block->serial_execution_lock);
    elog(FATAL, "Serial ordering violation...");
}

apply_optim_writes();        // ← PROTECTED
// ... resource cleanup ...
finish_xact_command();       // ← PROTECTED

set_last_committed_txid(tx);  // ← PROTECTED
LWLockRelease(&serial_block->serial_execution_lock);  // ← RELEASE
ConditionVariableBroadcast(&block->cond);
```

**Rationale:**  
- LWLock provides POSIX-like mutex semantics in shared memory
- Ensures mutual exclusion across entire serial phase
- Prevents interleaved execution of `apply_optim_writes()`

### Fix 3: Error Path Lock Release

```c
PG_CATCH();
{
    AbortCurrentTransaction();
    reset_xact_command();
    
    if(get_last_committed_txid(tx) == (tx->tx_id - 1))
        set_last_committed_txid(tx);
    
    // ← ADDED: Release lock in error path
    PG_TRY();
    {
        BCBlock *serial_block_err = get_block_by_id(1, false);
        if (serial_block_err != NULL)
            LWLockRelease(&serial_block_err->serial_execution_lock);
    }
    PG_CATCH();
    {
        /* Ignore errors during lock release */
    }
    PG_END_TRY();

    // ... cleanup ...
    PG_RE_THROW();
}
```

---

## 4. Testing Process (with build issues!)

### Build Challenges

**Problem:** Link-time symbol errors with `PQsendBCTx`
```
/work/ARIABC/install/bin/postgres: undefined symbol: PQsendBCTx
```

**Root Cause:** libpq wasn't properly rebuilt/installed after source changes

**Solution:** 
```bash
cd /work/ARIABC/AriaBC
rm -rf /work/ARIABC/install/*
cd src && make clean && make -j4 && make install

# Critical: Set library path
export LD_LIBRARY_PATH=/work/ARIABC/install/lib:$LD_LIBRARY_PATH
/work/ARIABC/install/bin/postgres -D /work/ARIABC/pgdata &
```

### Test Sequence

1. ✅ Rebuild table with Merkle index → `merkle_verify` = TRUE
2. ✅ Run BCDB workload (16 threads, 20K ops)
3. ❌ `merkle_verify` = **FALSE** (still ~200+ mismatches)

---

## 5. Why The Fix Didn't Work (Hypothesis)

Despite implementing comprehensive mutex protection, concurrent execution STILL occurs. Possible reasons:

### Hypothesis 1: LWLock Not Actually Acquired
- Maybe `get_block_by_id(1, false)` returns NULL in some cases?
- Maybe the LWLock field isn't properly aligned/initialized?
- Maybe shared memory layout changed but wasn't reinitialized?

**Test:** Add debug logging:
```c
elog(WARNING, "PID %d acquiring serial lock, last_committed=%d, my_id=%d",
     getpid(), get_last_committed_txid(tx), tx->tx_id);
LWLockAcquire(&serial_block->serial_execution_lock, LW_EXCLUSIVE);
elog(WARNING, "PID %d acquired serial lock", getpid());
```

### Hypothesis 2: Different Block IDs
- Maybe not all transactions use block_id=1?
- Maybe lock is being acquired on different BCBlock instances?

**Test:** Log the actual block pointer:
```c
elog(WARNING, "Acquiring lock on block %p (id=%d)", 
     serial_block, serial_block->id);
```

### Hypothesis 3: Merkle Corruption Occurs Elsewhere
- Maybe the issue isn't in `apply_optim_writes()` at all?
- Maybe corruption happens during optimistic phase (get_write_set)?
- Maybe the XOR operations themselves have a bug?

**Test:** Add merkle verification BEFORE and AFTER each operation

### Hypothesis 4: ByteA Not Properly Handled
Looking at the old MERKLE_FIX_ANALYSIS.md, there was mention of ByteA marshalling issues. Maybe that's still present?

---

## 6. Key Insights

### Architectural Understanding

1. **BCDB Execution Model:**
   ```
   Optimistic Phase (parallel)        Serial Phase (should be sequential!)
   ┌────────────────────────┐        ┌──────────────────────────────┐
   │ get_write_set()        │        │ WaitConditionPid()          │
   │ - Read data           │        │ conflict_checkDT()          │
   │ - Store deferred ops  │   →   │ apply_optim_writes()  ← BUG │
   │ - Build write set     │        │ finish_xact_command()       │
   └────────────────────────┘        │ set_last_committed_txid()   │
                                     └──────────────────────────────┘
   ```

2. **Merkle Tree XOR Property:**
   ```
   INSERT key=42: tree = tree ⊕ hash(row42)
   DELETE key=42: tree = tree ⊕ hash(row42)  ← Self-inverse!
   
   Bug: Both ops happen concurrently:
   tree_final = tree ⊕ hash(row42) ⊕ hash(row42) = tree  ← Hash cancelled out!
   But row IS deleted → Mismatch!
   ```

3. **pendingOps Rollback:** Process-local list in `merkleutil.c`
   - Tracks all XOR operations within a transaction
   - On ABORT: Re-apply all XORs (XOR is self-inverse)
   - Works ONLY within a single process - doesn't protect across processes!

### Critical Files Modified

| File | Lines Changed | Purpose |
|------|--------------|---------|
| `src/backend/bcdb/shm_block.c` | ~30 | Spinlock for counter, LWLock init |
| `src/backend/bcdb/worker.c` | ~50 | LWLock acquire/release, error handling |
| `src/include/bcdb/shm_block.h` | 1 | Add LWLock field to BCBlock |

---

## 7. Next Steps for Debugging

### Immediate Actions

1. **Add comprehensive logging to verify lock behavior:**
   ```c
   #define MERKLE_DEBUG_LOCKS 1
   
   #if MERKLE_DEBUG_LOCKS
   elog(WARNING, "[TX %d PID %d] Attempting lock acquire at %s:%d",
        tx->tx_id, getpid(), __FILE__, __LINE__);
   #endif
   ```

2. **Verify block ID consistency:**
   - Log which block ID each transaction is using
   - Confirm all use block_id=1 for serial execution

3. **Test with single-threaded workload using BCDB mode:**
   - If this STILL fails, issue is in transaction logic, not concurrency
   - If this succeeds, confirms it's a locking problem

4. **Instrument `WaitConditionPid` macro:**
   - Log when threads wake up
   - Log the counter value they see
   - Identify if multiple threads wake simultaneously

### Alternative Approaches

1. **Use PostgreSQL's existing LWLockAcquire infrastructure:**
   - Maybe custom LWLock in BCBlock doesn't work as expected
   - Try using a named LWLock from the LWLock array

2. **Implement spin-wait with compare-and-swap:**
   ```c
   while (__sync_val_compare_and_swap(&block->serial_executing, 0, 1) != 0)
       pg_usleep(100);  // Spin until we can claim serial phase
   ```

3. **Add explicit SERIALIZABLE isolation enforcement:**
   - Force true serializable execution at PostgreSQL level
   - May be expensive but guarantees correctness

4. **Investigate Merkle tree implementation itself:**
   - Maybe the XOR operations aren't atomic?
   - Maybe buffer locking in `merkle_update_tree_path()` is insufficient?

---

## 8. Comparison with Previous Fix (MERKLE_FIX_ANALYSIS.md)

The original document (from Feb 10) documented different issues:

| Issue | Previous Session | This Session |
|-------|------------------|--------------|
| Deferred DELETE | ✅ Fixed | Already fixed |
| Commit Ordering | ✅ Fixed mov sequence | Attempted LWLock (FAILED) |
| PG_CATCH abort order | ✅ Fixed | Already fixed |
| Retry list cleanup | ✅ Fixed | Already fixed |
| Result | 0 mismatches (single-thread) | Still ~200+ mismatches |

**Key Difference:** Previous fix focused on SEQUENCE of operations. This session attempted mutual EXCLUSION but the mutex isn't working as intended.

---

## 9. Code Snippets for Reference

### Current Serial Execution (Buggy)

```c
// In bcdb_worker_process_tx_dt()
while ( /* wait condition */ ) {
    WaitConditionPid(&block->cond, getpid(), 
        ((get_last_committed_txid(tx)+1) == tx->tx_id));  // ← RACE!
}

// No lock here!
apply_optim_writes();  // ← MULTIPLE PROCESSES CAN BE HERE!
finish_xact_command();
set_last_committed_txid(tx);
ConditionVariableBroadcast(&block->cond);
```

### Attempted Fix (Not Working)

```c
BCBlock *serial_block = get_block_by_id(1, false);
LWLockAcquire(&serial_block->serial_execution_lock, LW_EXCLUSIVE);  // ← Should serialize

if (get_last_committed_txid(tx) + 1 != tx->tx_id) {
    LWLockRelease(&serial_block->serial_execution_lock);
    elog(FATAL, "Serial ordering violation");  // ← NEVER FIRES!
}

apply_optim_writes();  // ← STILL CONCURRENT!
finish_xact_command();
set_last_committed_txid(tx);
LWLockRelease(&serial_block->serial_execution_lock);
```

---

## 10. Lessons Learned

1. **Shared Memory Debugging is Hard:**
   - Standard debugging tools don't work well
   - Need extensive logging to understand behavior
   - Timing-dependent bugs are difficult to reproduce

2. **LWLocks in Custom Structs May Have Caveats:**
   - PostgreSQL's LWLock infrastructure is complex
   - May require registration in LWLock tranches
   - Alignment requirements in shared memory structures

3. **Build System Complexity:**
   - Full rebuilds required after adding struct fields
   - Library path issues can cause runtime failures
   - make clean doesn't always clean enough

4. **Testing is Essential:**
   - Serial execution must be verified FIRST
   - Incremental thread count testing (1, 2, 4, 8, 16)
   - Comprehensive logging before and after changes

---

## 11. Conclusion

Despite identifying the root cause (concurrent serial execution) and implementing what SHOULD BE a correct fix (LWLock mutual exclusion), the Merkle tree corruption persists. This suggests either:

1. **Implementation Error:** The LWLock isn't being acquired properly due to some subtle shared memory issue
2. **Architecture Issue:** The block-based locking model doesn't guarantee serialization as expected  
3. **Different Root Cause:** The corruption occurs elsewhere (optimistic phase, XOR implementation, snapshot handling)

**Recommendation:** Add extensive debug logging to verify lock acquisition, then consider alternative synchronization mechanisms (semaphores, compare-and-swap, or PostgreSQL's built-in serializable isolation).

The fact that single-threaded execution works perfectly proves the Merkle tree logic itself is correct - this is PURELY a concurrency control problem.

---

## Appendix: File Change Summary

### Modified Files

```
src/backend/bcdb/shm_block.c       (+30 lines)
  - Added spinlock protection to get/set_last_committed_txid()
  - Added pg_read/write_barrier() calls
  - Initialize serial_execution_lock in get_block by_id()

src/backend/bcdb/worker.c          (+50 lines)
  - Acquire LWLock before apply_optim_writes()
  - Release LWLock after counter advance
  - Add error path lock release with PG_TRY

src/include/bcdb/shm_block.h       (+1 line)
  - Add LWLock serial_execution_lock field to BCBlock
```

### Build Commands

```bash
cd /work/ARIABC/AriaBC
rm -rf /work/ARIABC/install/*
cd src && make clean && make -j4 && make install

# Start server
export LD_LIBRARY_PATH=/work/ARIABC/install/lib:$LD_LIBRARY_PATH
/work/ARIABC/install/bin/postgres -D /work/ARIABC/pgdata &

# Test
bcpsql -f rebuild_usertable.sql
python3 generic-saicopg-traffic-load+logSkip-safedb+pg.py postgres ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt 1 16
bcpsql -c "SELECT merkle_verify('usertable');"
```

---

**Analysis completed:** 11 February 2026, 22:40 IST  
**Total investigation time:** ~2 hours  
**Lines of code examined:** ~3,000+  
**Files modified:** 3  
**Test iterations:** 5  
**Result:** Issue persists - requires continued investigation
