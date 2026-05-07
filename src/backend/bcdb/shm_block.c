#include "bcdb/shm_block.h"
#include "postgres.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include <sys/queue.h>

/*
 * Silence ad-hoc stdout debug prints in the deterministic shared-memory block
 * machinery to avoid corrupting frontend protocol responses.
 */
#undef printf
#define printf(...) ((void) 0)

/*
 * Shared-memory block pool and associated metadata.
 *
 * block_pool      - Hash table (keyed by BCBlockID) living in shared memory.
 *                   Each entry is a BCBlock that groups a set of transactions
 *                   that are committed together in the same blockchain block.
 *
 * block_pool_lock - Single spinlock that serialises all structural mutations
 *                   of the hash table (HASH_ENTER / HASH_REMOVE) as well as
 *                   the per-block num_tx counter and the global
 *                   last_committed_tx_id / num_committed counters.
 *
 * block_meta      - Small singleton struct in shared memory holding global
 *                   bookkeeping: the current [global_bmin, global_bmax]
 *                   window, commit/abort counts, condition variables for
 *                   bmin advancement, and an optional debug log.
 */
HTAB          *block_pool;
slock_t       *block_pool_lock;
BlockMeta     *block_meta;
static BCBlock *block1_cache = NULL;

static void
bcdb_reset_block_entry(BCBlock *block, BCBlockID id)
{
    block->id = id;
    block->num_tx = 0;
    block->num_ready = 0;
    block->num_finished = 0;
    block->last_committed_tx_id = -1;
    block->published_max_tx_id = -1;
    ConditionVariableInit(&block->cond);
    ConditionVariableInit(&block->condRecovery);
    ConditionVariableInit(&block->condCommit);
    block->num_tx_sub = 0;
    block->num_tx_qd = 0;
    block->blksize = (id == 1) ? bcdb_worker_count : 0;
    block->snapTid = 0;
    for (int i = 0; i < MAX_TX_PER_BLOCK; i++)
    {
        block->txs[i] = NULL;
        ConditionVariableInit(&block->done_conds[i]);
    }
    for (int i = 0; i < BCDB_RESULT_RING_CAPACITY; i++)
    {
        memset(&block->result[i], 0, sizeof(block->result[i]));
        block->result_committed_txid[i] = -1;
        block->result_commit_xid[i] = InvalidTransactionId;
        block->result_consumed_txid[i] = -1;
    }
    for (int i = 0; i < MAX_TX_PER_BLOCK; i++)
        block->published_ready_txid[i] = -1;
}

static inline BCBlock *
get_block1_cached(bool create)
{
    BCBlock *blk = block1_cache;
    if (blk != NULL)
        return blk;
    blk = get_block_by_id(1, create);
    if (blk != NULL)
        block1_cache = blk;
    return blk;
}

/*
 * block_pool_size
 *
 * Returns the total amount of shared memory required for the block
 * subsystem.  Called during the shmem sizing pass (before the postmaster
 * forks any workers) so that ShmemInitStruct / ShmemInitHash can be
 * satisfied without needing to enlarge the segment afterwards.
 *
 * The three components are:
 *   - One BlockMeta singleton.
 *   - One spinlock (slock_t) guarding the hash table and counters.
 *   - The hash table itself, sized for MAX_NUM_BLOCKS entries.
 */
Size
block_pool_size()
{
    Size ret = sizeof(BlockMeta);
    ret = add_size(ret, sizeof(slock_t));
    ret = add_size(ret, hash_estimate_size(MAX_NUM_BLOCKS, sizeof(BCBlock)));
    return ret;
}

/*
 * create_block_pool
 *
 * Initialises all shared-memory structures for the block subsystem.
 * Must be called exactly once, from the postmaster during shared-memory
 * initialisation (PG_INIT / _PG_init path), before any worker or backend
 * touches these structures.
 *
 * Initialisation order matters:
 *   1. BlockMeta singleton   -- global bmin/bmax window & statistics.
 *   2. Condition variables   -- one per bmin bucket (NUM_BMIN_COND slots);
 *                               workers wait here for global_bmin to advance.
 *   3. block_pool_lock       -- guards hash-table mutations and counters.
 *   4. block_pool hash table -- fixed-size, keyed by BCBlockID (uint32).
 *
 * NOTE: set_blksz(1) is intentionally left commented out here.  Calling
 * it at this point caused an immediate SIGSEGV because the hash table
 * entry for block 1 does not yet exist (ShmemInitHash is not done).
 */
void
create_block_pool(void)
{
    /* need to keep params in memory */
    HASHCTL info;
    bool    found;
	block_meta = ShmemInitStruct("BCDB_BLOCK_META", sizeof(BlockMeta), &found);
    block_meta->global_bmin = 1;
    block_meta->global_bmax = 0;
    block_meta->debug_seq = 0;
    block_meta->num_committed = 0;
    block_meta->num_aborted = 0;
    block_meta->previous_report_commit = 0;
    block_meta->previous_report_ts = 0;
#ifdef LOG_STATUS
    block_meta->log[0] = '\0';
    block_meta->log_counter = 0;
#endif
    for (int i = 0; i < NUM_BMIN_COND; i++)
        ConditionVariableInit(&block_meta->conds[i]);

    block_pool_lock = ShmemInitStruct("block_pool_lock", sizeof(slock_t), &found);
    if (!found)
        SpinLockInit(block_pool_lock);

    MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(BCBlockID);
	info.entrysize = sizeof(BCBlock);
	info.hash = uint32_hash;
    block_pool = ShmemInitHash("bcdb_block_pool",
                   MAX_NUM_BLOCKS,
                   MAX_NUM_BLOCKS,
                   &info, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
}

/*
 * reset the block entry for block 1, which holds global counters and state for the current block.
 */
void
bcdb_reset_block_pool_state(void)
{
    BCBlockID id = 1;
    BCBlock  *block;
    bool      found;

    block1_cache = NULL;
    SpinLockAcquire(block_pool_lock);
    block = hash_search(block_pool, &id, HASH_ENTER, &found);
    bcdb_reset_block_entry(block, id);
    SpinLockRelease(block_pool_lock);
    block1_cache = block;
}

/*
 * set_last_committed_txid
 *
 * Records the most-recently-committed transaction ID both on the sentinel
 * BCBlock (id=1) and in block_meta->num_committed.
 *
 * Uses block_pool_lock + pg_write_barrier() to ensure that concurrent
 * readers (get_last_committed_txid) in other processes never observe a
 * torn or stale value — important on weakly-ordered architectures.
 *
 * NOTE: The older non-atomic variant set_last_committed_id() has been
 * removed; its only call site in worker.c was already commented out.
 */
void set_last_committed_txid( BCDBShmXact *tx)
{
    //BCBlock* blk = get_block_by_id( tx->block_id_committed, false);
    BCBlock* blk = get_block1_cached(true);
    /*
     * Deterministic workers update/read this field on every commit gate check.
     * Avoid block_pool_lock contention by using lock-free atomic publish.
     */
    __atomic_store_n(&blk->last_committed_tx_id, tx->tx_id, __ATOMIC_RELEASE);
    __atomic_store_n(&block_meta->num_committed, tx->tx_id, __ATOMIC_RELEASE);
    if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
        ConditionVariableBroadcast(&blk->condCommit);
#if SAFEDBG2
    printf("safeDbg %s : %s: %d  blk %x txid= %d\n",
              __FILE__, __FUNCTION__, __LINE__, blk, block_meta->num_committed);
#endif
}

/*
 * advance_last_committed_txid
 *
 * T3: non-blocking replacement for the old
 * bcdb_wait_for_prev_committed + set_last_committed_txid pair.
 *
 * Each tx tries a single CAS: advance last_committed_tx_id from
 * (my_tx_id - 1) to my_tx_id.  If the predecessor has not committed yet,
 * the CAS fails and the function returns immediately — the predecessor will
 * carry the scan through this tx's slot when it finishes.
 *
 * After a successful CAS, the function eagerly scans forward through any
 * consecutive successor slots that have already set result_committed_txid,
 * so the fastest-finishing thread amortises the watermark advance for all.
 */
BCBlock *
bcdb_get_block1(void)
{
    return get_block1_cached(false);
}

void
advance_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock *blk   = get_block1_cached(true);
    int      slots = bcdb_get_runtime_result_ring_slots();
    BCTxID   my_id = tx->tx_id;
    BCTxID   prev  = my_id - 1;
    BCTxID   cur;

    if (slots < 1)
        slots = 1;

    if (!__sync_bool_compare_and_swap(&blk->last_committed_tx_id, prev, my_id))
        return;

    __atomic_store_n(&block_meta->num_committed, my_id, __ATOMIC_RELEASE);
    if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
        ConditionVariableBroadcast(&blk->condCommit);

    cur = my_id;
    for (int step = 0; step < slots; step++)
    {
        BCTxID next_id   = cur + 1;
        int    next_slot = (int)(next_id % (BCTxID)slots);
        BCTxID published;

        if (next_slot < 0)
            next_slot += slots;

        published = __atomic_load_n(&blk->result_committed_txid[next_slot],
                                    __ATOMIC_ACQUIRE);
        if (published != next_id)
            break;
        if (!__sync_bool_compare_and_swap(&blk->last_committed_tx_id, cur, next_id))
            break;
        __atomic_store_n(&block_meta->num_committed, next_id, __ATOMIC_RELEASE);
        cur = next_id;
    }
}

/*
 * set_blksz / get_blksz
 *
 * Accessors for the "block size" (number of transactions per blockchain
 * block) stored on the sentinel BCBlock (id=1).  Used by the worker to
 * decide when a block is full and ready for ordering/commit.
 *
 * NOTE: These access the sentinel block without holding block_pool_lock.
 * They are currently called only from single-threaded paths (worker init
 * and configuration reload) — if that changes a lock should be added.
 */
void set_blksz(int num)
{
    BCBlock* blk = get_block1_cached(true);
#if SAFEDBG2
    printf("ariaMyDbg %s : %s: %d bid 1, blk %x\n",
              __FILE__, __FUNCTION__, __LINE__, blk);
#endif
    blk->blksize = num;
}

BCTxID get_blksz()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->blksize;
}

/*
 * bcdb_get_result_ring_slots / bcdb_get_runtime_result_ring_slots
 *
 * Accessors for the number of slots in the block result ring buffer,
 * which is currently fixed at compile time (BCDB_RESULT_RING_CAPACITY)
 * but may be made configurable in the future.
 *
 * The runtime accessor enforces a minimum size of 2x the worker count
 * to ensure that all workers can publish without blocking on a predecessor
 * (see advance_last_committed_txid).  It is called by the worker during
 * init and must be safe to call before the sentinel block entry is created,
 * so it falls back to the compile-time default if block 1 is not yet available.
 */
int
bcdb_get_result_ring_slots(void)
{
    int slots = bcdb_result_ring_slots;

    if (slots < 2)
        slots = 2;
    if (slots > BCDB_RESULT_RING_CAPACITY)
        slots = BCDB_RESULT_RING_CAPACITY;
    return slots;
}

int
bcdb_get_runtime_result_ring_slots(void)
{
    int workers = get_blksz();
    int min_slots;
    int slots = bcdb_get_result_ring_slots();

    if (workers <= 0)
        workers = bcdb_worker_count;
    if (workers <= 0)
        workers = 1;

    min_slots = 2 * workers;
    if (slots < min_slots)
        slots = min_slots;
    if (slots < 2)
        slots = 2;
    if (slots > BCDB_RESULT_RING_CAPACITY)
        slots = BCDB_RESULT_RING_CAPACITY;
    return slots;
}

/*
 * set_num_tx_sub / get_num_tx_sub
 *
 * Accessors for num_tx_sub: the count of transactions that have been
 * "submitted" (handed off to the ordering layer) for the current block
 * on the sentinel BCBlock (id=1).
 */
void set_num_tx_sub(int num)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return;
    blk->num_tx_sub = num;
}

BCTxID get_num_tx_sub()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->num_tx_sub;
}

/*
 * set_num_txqd / get_num_txqd
 *
 * Accessors for num_tx_qd: the count of transactions currently queued
 * (waiting to be submitted) for the current sentinel block.  Used by
 * the worker to track back-pressure in the pipeline.
 */
void set_num_txqd(int num)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return;
    blk->num_tx_qd = num;
}

BCTxID get_num_txqd()
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return 0;
    return blk->num_tx_qd;
}

/*
 * get_last_committed_txid
 *
 * Returns the most-recently-committed transaction ID from the sentinel
 * BCBlock (id=1).  Mirrors set_last_committed_txid: acquires
 * block_pool_lock and issues pg_read_barrier() to prevent the compiler
 * or CPU from reordering the load before the lock acquire, ensuring we
 * always see the latest value written by any process.
 */
BCTxID get_last_committed_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return -1;
    (void) tx;
    return (BCTxID) __atomic_load_n(&blk->last_committed_tx_id, __ATOMIC_ACQUIRE);
}

/*
 * Lever D publish-phase gate accessors.
 *
 * published_max_tx_id is advanced atomically immediately after
 * publish_ws_tableDT in worker.c (Lever D v2). The serial gate waits on
 * published_max instead of last_committed so apply/finish can run in
 * parallel across backends.
 *
 * In CONDVAR mode, wake only the immediate successor's per-slot condition
 * variable.  The successor still rechecks published_max_tx_id before entering
 * conflict_checkDT(), so this changes wakeup latency, not deterministic order.
 */
static inline int
bcdb_published_ready_slot_for_txid(BCTxID tx_id)
{
    int idx = tx_id % MAX_TX_PER_BLOCK;

    if (idx < 0)
        idx += MAX_TX_PER_BLOCK;
    return idx;
}

static inline void
bcdb_signal_serial_successor(BCBlock *blk, BCTxID published_txid)
{
    if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
    {
        int wake_slot = (int) ((published_txid + 1) % MAX_TX_PER_BLOCK);

        if (wake_slot < 0)
            wake_slot += MAX_TX_PER_BLOCK;
        ConditionVariableSignal(&blk->done_conds[wake_slot]);
    }
}

static void
bcdb_advance_published_ready_prefix(BCBlock *blk)
{
    BCTxID current;
    int steps = 0;

    Assert(blk != NULL);

    while (steps++ < MAX_TX_PER_BLOCK)
    {
        BCTxID next_id;
        int next_slot;
        BCTxID ready;

        current = (BCTxID) __atomic_load_n(&blk->published_max_tx_id,
                                           __ATOMIC_ACQUIRE);
        next_id = current + 1;
        next_slot = bcdb_published_ready_slot_for_txid(next_id);
        ready = (BCTxID) __atomic_load_n(&blk->published_ready_txid[next_slot],
                                         __ATOMIC_ACQUIRE);
        if (ready != next_id)
            break;

        if (__atomic_compare_exchange_n(&blk->published_max_tx_id,
                                        &current,
                                        next_id,
                                        false,
                                        __ATOMIC_RELEASE,
                                        __ATOMIC_ACQUIRE))
        {
            bcdb_signal_serial_successor(blk, next_id);
        }
    }
}

void
mark_published_ready_txid(BCDBShmXact *tx)
{
    BCBlock *blk = get_block1_cached(true);
    int slot;

    Assert(tx != NULL);
    Assert(blk != NULL);

    slot = bcdb_published_ready_slot_for_txid(tx->tx_id);
    __atomic_store_n(&blk->published_ready_txid[slot],
                     tx->tx_id,
                     __ATOMIC_RELEASE);
    bcdb_advance_published_ready_prefix(blk);
}

void
set_published_max_txid(BCDBShmXact *tx)
{
    mark_published_ready_txid(tx);
}

BCTxID get_published_max_txid(BCDBShmXact *tx)
{
    BCBlock* blk = get_block1_cached(false);
    if (blk == NULL)
        blk = get_block1_cached(true);
    if (blk == NULL)
        return -1;
    (void) tx;
    return (BCTxID) __atomic_load_n(&blk->published_max_tx_id, __ATOMIC_ACQUIRE);
}

/*
 * get_block_by_id
 *
 * Looks up — and optionally creates — a BCBlock entry in the shared-memory
 * hash table.
 *
 * Parameters:
 *   id                   - The blockchain block ID to look up.
 *   create_if_not_found  - When true, a new entry is inserted if none exists
 *                         (HASH_ENTER); when false, returns NULL if not found
 *                         (HASH_FIND).
 *
 * Both paths hold block_pool_lock for the duration of the hash_search call
 * so that concurrent create/find operations across multiple backends are
 * serialised and the hash table is never observed in a partially-initialised
 * state.
 *
 * New entries are zero-initialised here: num_tx=0, all ConditionVariables
 * prepared, result buffers cleared.  last_committed_tx_id starts at -1 to
 * indicate "no transaction committed yet in this block".
 *
 * bcdb_worker_init() is called unconditionally to ensure the calling process
 * has attached to any per-process worker state required before touching
 * shared structures.
 */
BCBlock*
get_block_by_id(BCBlockID id, bool create_if_not_found)
{
    BCBlock *block;
    bool found;

    Assert(block_pool != NULL);
    bcdb_worker_init();
    if (create_if_not_found)
    {
        SpinLockAcquire(block_pool_lock);
        block = hash_search(block_pool, &id, HASH_ENTER, &found);
        if (!found)
        {
            printf("\n \t ** safeDbg pid= %d new blk %s : %s: %d bid %d blk %x\n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__, id, block);
            bcdb_reset_block_entry(block, id);
        }
        SpinLockRelease(block_pool_lock);
    }
    else
    {
        SpinLockAcquire(block_pool_lock);
        block = hash_search(block_pool, &id, HASH_FIND, &found);
        SpinLockRelease(block_pool_lock);
    }
    return block;
}

/*
 * delete_block
 *
 * Removes the given BCBlock from the shared-memory hash table.
 * Safe to call with a NULL pointer (no-op).
 *
 * The caller must ensure that no other process holds a pointer to this
 * block and will dereference it after the removal — there is no reference
 * counting; the hash entry is freed immediately.
 */
void
delete_block(BCBlock *block)
{
    if (block == NULL)
        return;
    DEBUGNOCHECK("[ZL] deleting block %d", block->id);
    SpinLockAcquire(block_pool_lock);
    hash_search(block_pool, &block->id, HASH_REMOVE, NULL);
    SpinLockRelease(block_pool_lock);
}

/*
 * block_add_tx
 *
 * Appends a transaction pointer to the block's txs[] array and bumps
 * num_tx.  Holds block_pool_lock for the entire operation because:
 *
 *   1. num_tx++ is a non-atomic read-modify-write; two backends racing
 *      here would corrupt the counter and potentially write to the same
 *      slot, losing one transaction silently.
 *   2. The txs[] array write must be visible to any reader of num_tx
 *      before the lock is released (sequentially consistent ordering).
 *
 * Preconditions (asserted):
 *   - tx has not already been attached to a block
 *     (block_id_committed == BCDBInvalidBid or BCDBMaxBid).
 *   - The block is not already full (num_tx < MAX_TX_PER_BLOCK).
 */
void
block_add_tx(BCBlock* block, BCDBShmXact* tx)
{
    Assert(tx->block_id_committed == BCDBInvalidBid || tx->block_id_committed == BCDBMaxBid);
    SpinLockAcquire(block_pool_lock);
    Assert(block->num_tx < MAX_TX_PER_BLOCK);
    block->txs[block->num_tx++] = tx;
    SpinLockRelease(block_pool_lock);
}
