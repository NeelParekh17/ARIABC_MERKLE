#ifndef BCDB_SHM_BLOCK_H
#define BCDB_SHM_BLOCK_H

#include "postgres.h"
#include "storage/lwlock.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/bcdb_dsa.h"
#include "lib/dshash.h"
#include "sys/queue.h"
#include "bcdb/globals.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "c.h"


/* unfortunately, the name Block conflicts with another component in postgres, so use ugly BCBlock for now */
typedef struct
{
    BCBlockID  id;
    int        num_tx;
    int volatile   last_committed_tx_id pg_attribute_aligned(PG_CACHE_LINE_SIZE);
    int volatile       num_ready;
    int volatile       num_finished;
    BCDBShmXact*       txs[MAX_TX_PER_BLOCK];
    ConditionVariable  cond;
    ConditionVariable  condRecovery;
    ConditionVariable  condCommit;
    int num_tx_sub;
    int num_tx_qd;
    int blksize;
    int snapTid;
    ConditionVariable  done_conds[MAX_TX_PER_BLOCK];
    char result[MAX_TX_PER_BLOCK][1024];
    /*
     * Lever D: publish-phase ordering counter.
     *
     * Gate now waits on (published_max_tx_id + 1 == my_tx_id) instead of
    * last_committed_tx_id. In Lever D v2 it is advanced immediately after
    * publish_ws_tableDT so apply/finish can overlap across worker backends.
     *
     * Placed AFTER result[] on purpose — result[] is the largest array in
     * the struct, so adding a field after it cannot shift the offset of
     * any earlier ConditionVariable's internal mutex byte. This is the
     * same CV-layout hazard that caused the shm_block.h wave 2.9 PANIC.
     */
    int volatile      published_max_tx_id;
    /*
     * T3 (2026-04-19): per-slot commit marker for decoupled middleware readback.
     *
     * result_committed_txid[slot] is set to tx->tx_id by the worker
     * immediately after writing block->result[slot] (release store).
     * Middleware and advance_last_committed_txid() check this slot-specific
     * value instead of the contiguous last_committed_tx_id watermark, so
     * each tx's result is readable as soon as that tx publishes — without
     * blocking on any predecessor.
     *
     * advance_last_committed_txid() uses a lock-free CAS prefix-scan to
     * advance last_committed_tx_id without bcdb_wait_for_prev_committed,
     * removing the O(N) serial bottleneck from the finish path.
     *
     * Initialized to -1.  Must remain LAST in BCBlock: inserting before
     * ConditionVariable fields causes CV-mutex byte displacement (wave-2.9
     * PANIC class of bug).
     */
    int32 volatile    result_committed_txid[MAX_TX_PER_BLOCK];
    /*
     * T3-v2 (2026-04-19): per-slot PostgreSQL commit XID for snapshot-based
     * conflict skip.
     *
     * Set to the committing backend's TransactionId (tx->xid) immediately
     * BEFORE result_committed_txid, so the release-acquire pair guarantees
     * visibility: when a reader observes result_committed_txid[slot]==tx_id,
     * result_commit_xid[slot] is already visible.
     *
     * conflict_checkDT (via table_checkDT) uses this to skip candidates that
     * committed before our GetTransactionSnapshot() xmin — those txs' writes
     * are already visible in our snapshot, so no write-set conflict is possible.
     *
     * Initialized to InvalidTransactionId (0). Must remain LAST in BCBlock.
     */
    TransactionId volatile result_commit_xid[MAX_TX_PER_BLOCK];
} BCBlock;

typedef struct
{
    /* bid < global_bmin: block committed */
    /* bid < global_bmin - CLEANING_DELAY_BLOCKS: block cleaned*/
    BCBlockID volatile global_bmin;
    BCBlockID volatile global_bmax;
    ConditionVariable  conds[NUM_BMIN_COND];
    ConditionVariable  token_cond;
    uint32 debug_seq;
    int32 volatile     num_committed;
    int32 volatile     num_aborted;
    uint64 volatile    previous_report_ts;
    int32 volatile    previous_report_commit; 
#ifdef LOG_STATUS
    char log[1024 * 1024 * 10];
    int  log_counter;
#endif
} BlockMeta;

/* todo: change it to HTAB */
extern HTAB          *block_pool;
extern slock_t       *block_pool_lock;
extern BlockMeta     *block_meta;

extern Size     block_pool_size(void);
extern void     create_block_pool(void);
extern int      get_commited(int id);
extern void     set_commited(int id,  BCDBShmXact* tx);
extern BCBlock* get_block_by_id(BCBlockID id, bool create_if_not_found);
extern void     delete_block(BCBlock *block);
extern void     block_add_tx(BCBlock* block, BCDBShmXact* tx);

extern void     set_last_committed_txid(BCDBShmXact *tx);
extern BCTxID   get_last_committed_txid(BCDBShmXact *tx);
extern void     advance_last_committed_txid(BCDBShmXact *tx);
extern BCBlock *bcdb_get_block1(void);

extern void     set_published_max_txid(BCDBShmXact *tx);
extern BCTxID   get_published_max_txid(BCDBShmXact *tx);

extern void     set_blksz(int num);
extern BCTxID   get_blksz(void);

extern void     set_num_tx_sub(int num);
extern BCTxID   get_num_tx_sub(void);

extern void     set_num_txqd(int num);
extern BCTxID   get_num_txqd(void);
extern int      bcdb_get_result_ring_slots(void);
extern int      bcdb_get_runtime_result_ring_slots(void);
/* delete_block_by_id, print_block_status, and set_last_committed_id
 * have been removed — they had no callers in the codebase. */



#endif
