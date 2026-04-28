//
// Created by Chris Liu on 6/5/2020.
//

#include "bcdb/shm_transaction.h"
#include "bcdb/worker.h"
#include "bcdb/middleware.h"
#include "bcdb/shm_block.h"
#include "bcdb/worker_controller.h"
#include "libpq/libpq.h"
#include "libpq-fe.h"
#include "storage/condition_variable.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "bcdb/globals.h"
#include "lib/stringinfo.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

/*
 * Silence ad-hoc stdout debug prints in deterministic middleware.  Raw
 * printf() output can leak into frontend sessions.
 */
#undef printf
#define printf(...) ((void) 0)

MemoryContext bcdb_middleware_context;
int32         tx_num = 0;
int32         blocksize = 0;
int32         numTxBurst = 0;
int32         burstTime = 0;
uint64        start_time;
static int  tx_id_counter = 0; // not bcdb

static BCDBShmXact *parse_tx(const char* json);
static void bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block);
static BCBlock *parse_block_object(cJSON *parsed);
static BCBlock *parse_block_with_txs(const char *json);
static void append_hex_encoded(StringInfo out, const char *input);
static int32 bcdb_select_worker_count(int32 requested);
static inline int bcdb_result_slot_for_txid(BCTxID tx_id);
static inline void bcdb_wait_until_committed(BCTxID target_tx_id);
static inline void bcdb_wait_until_slot_ready(BCTxID target_tx_id);

static int32
bcdb_select_worker_count(int32 requested)
{
    int32 workers = requested;

    if (workers <= 0)
        workers = bcdb_worker_count;
    if (workers <= 0)
        workers = BCDB_DEFAULT_WORKER_COUNT;
    if (workers <= 0)
        workers = 1;
    return workers;
}

/*
 * Use one runtime slot mapping everywhere in DT path to avoid stale/mismatched
 * result reads when blksize and result ring size diverge.
 */
static inline int
bcdb_result_slot_for_txid(BCTxID tx_id)
{
    int slots = bcdb_get_runtime_result_ring_slots();
    int idx;

    if (slots <= 0)
        slots = 1;
    idx = tx_id % slots;
    if (idx < 0)
        idx += slots;
    return idx;
}

/*
 * DT completion wait that does not depend on fragile CV broadcast contracts.
 * We poll commit progression with interruptible adaptive backoff.
 *
 * HANG DEBUG: logs every 5 s if last_committed_tx_id does not advance.
 * This fires unconditionally (no env-var gate) so hangs are always visible
 * in server.log without any pre-configuration.
 */
static inline void
bcdb_wait_until_committed(BCTxID target_tx_id)
{
    int spins = 0;
    int poll_us = 0;
    uint64 wait_start_us = bcdb_get_time();
    uint64 next_warn_us  = wait_start_us + 5000000; /* 5 s */

    for (;;)
    {
        BCTxID committed = get_last_committed_txid(NULL);
        if (committed >= target_tx_id)
            break;

        CHECK_FOR_INTERRUPTS();

        /* Always-on hang watchdog: fire every 5 s so a stuck loop is visible */
        {
            uint64 now_us = bcdb_get_time();
            if (now_us >= next_warn_us)
            {
                ereport(LOG,
                        (errmsg("[BCDB_HANG] committed_wait_stuck pid=%d target_txid=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
                                (int) getpid(), (int) target_tx_id,
                                (int) committed,
                                (unsigned long) (now_us - wait_start_us),
                                poll_us, spins)));
                next_warn_us = now_us + 5000000;
            }
        }

        if (spins < 128)
        {
            spins++;
            pg_spin_delay();
        }
        else
        {
            if (poll_us == 0)
                poll_us = 1;
            else if (poll_us < 64)
                poll_us *= 2;
            pg_usleep((long) poll_us);
        }
    }
}

/*
 * T3: per-slot middleware wait.
 * Waits for result_committed_txid[slot] == target_tx_id rather than for
 * the contiguous last_committed_tx_id watermark.  Safe only for polling a
 * single target tx; does NOT imply earlier slots are set (slots are written
 * at Step 10, before bcdb_wait_for_prev_committed serialises at Step 11).
 *
 * HANG DEBUG: logs every 5 s if the slot value does not become target_tx_id.
 * Fires unconditionally — if a worker crashes or misses writing the slot,
 * this hang is immediately visible in server.log without any env-var setup.
 * Key fields: slot index, current slot value vs expected, and elapsed time.
 */
static inline void
bcdb_wait_until_slot_ready(BCTxID target_tx_id)
{
    BCBlock *blk     = get_block_by_id(1, false);
    int      slot    = bcdb_result_slot_for_txid(target_tx_id);
    int      spins   = 0;
    int      poll_us = 0;
    uint64   wait_start_us = bcdb_get_time();
    uint64   next_warn_us  = wait_start_us + 5000000; /* 5 s */

    Assert(blk != NULL);
    for (;;)
    {
        BCTxID published = __atomic_load_n(&blk->result_committed_txid[slot],
                                           __ATOMIC_ACQUIRE);
        if (published == target_tx_id)
            break;

        CHECK_FOR_INTERRUPTS();

        /* Always-on hang watchdog: fire every 5 s so a stuck loop is visible */
        {
            uint64 now_us = bcdb_get_time();
            if (now_us >= next_warn_us)
            {
                BCTxID last_committed = get_last_committed_txid(NULL);
                ereport(LOG,
                        (errmsg("[BCDB_HANG] slot_ready_stuck pid=%d target_txid=%d slot=%d slot_value=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
                                (int) getpid(), (int) target_tx_id, slot,
                                (int) published, (int) last_committed,
                                (unsigned long) (now_us - wait_start_us),
                                poll_us, spins)));
                next_warn_us = now_us + 5000000;
            }
        }

        if (spins < 128)
        {
            spins++;
            pg_spin_delay();
        }
        else
        {
            if (poll_us == 0)
                poll_us = 1;
            else if (poll_us < 64)
                poll_us *= 2;
            pg_usleep((long) poll_us);
        }
    }
}

void
bcdb_middleware_init(bool is_oep_mode, int32 block_size)
{
    MemoryContext    old_context;
    BCBlock *block;
    //int32 nWorkers = block_size;
    //nWorkers = 5;

    /* Aria does not have oep mode */
    is_bcdb_master = true;
    blocksize = bcdb_select_worker_count(block_size);
    bcdb_worker_count = blocksize;
    bcdb_middleware_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "middleware memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_middleware_context);
    block = get_block_by_id(1, true);
        if (bcdb_get_result_ring_slots() < 2 * blocksize)
        ereport(WARNING,
            (errmsg("bcdb_result_ring_slots=%d is lower than 2 * bcdb_worker_count=%d; runtime will clamp slots",
                bcdb_get_result_ring_slots(), 2 * blocksize)));
    if (block->blksize > 0 && block->blksize != blocksize)
        ereport(ERROR,
                (errmsg("bcdb_worker_count mismatch: existing=%d requested=%d; restart required",
                        block->blksize, blocksize)));
    set_blksz(blocksize);
    if (idle_workers.num == 0)
        idle_worker_list_init(blocksize);
    else if (idle_workers.num != blocksize)
        ereport(ERROR,
                (errmsg("BCDB workers already initialized with %d workers; requested %d",
                        idle_workers.num, blocksize)));
    MemoryContextSwitchTo(old_context);
#if SAFEDBG2
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif

    start_time = bcdb_get_time();
}

void
bcdb_middleware_init2(bool is_oep_mode, int32 block_size, int32 numTx, int32 timeSlot)
{
    MemoryContext    old_context;
    BCBlock *block;

    is_bcdb_master = true;
    blocksize = bcdb_select_worker_count(block_size);
    bcdb_worker_count = blocksize;
    numTxBurst = numTx;
    burstTime = timeSlot;
    bcdb_middleware_context = 
        AllocSetContextCreate(TopMemoryContext, 
                              "middleware memory context", 
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_middleware_context);
    block = get_block_by_id(1, true);
        if (bcdb_get_result_ring_slots() < 2 * blocksize)
        ereport(WARNING,
            (errmsg("bcdb_result_ring_slots=%d is lower than 2 * bcdb_worker_count=%d; runtime will clamp slots",
                bcdb_get_result_ring_slots(), 2 * blocksize)));
    if (block->blksize > 0 && block->blksize != blocksize)
        ereport(ERROR,
                (errmsg("bcdb_worker_count mismatch: existing=%d requested=%d; restart required",
                        block->blksize, blocksize)));
    set_blksz(blocksize);
    if (idle_workers.num == 0)
        idle_worker_list_init(blocksize);
    else if (idle_workers.num != blocksize)
        ereport(ERROR,
                (errmsg("BCDB workers already initialized with %d workers; requested %d",
                        idle_workers.num, blocksize)));
    MemoryContextSwitchTo(old_context);
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif

    start_time = bcdb_get_time();
}

BCDBShmXact *
parse_tx(const char* json)
{
    cJSON   *parsed   = NULL;
    cJSON   *sql      = NULL;
    cJSON   *hash     = NULL;
    cJSON   *create_time = NULL;
    BCDBShmXact   *tx;
    int     isolation;
    bool    pred_lock = false;

    parsed = cJSON_Parse(json);
    if (!parsed)
        goto error;

    sql = cJSON_GetObjectItemCaseSensitive(parsed, "sql");
    if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
        goto error;

    hash = cJSON_GetObjectItemCaseSensitive(parsed, "hash");
    if (!cJSON_IsString(hash))
        goto error;
    
    isolation = XACT_SERIALIZABLE;
    pred_lock = true;

    tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);

#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
    create_time = cJSON_GetObjectItemCaseSensitive(parsed, "create_ts");

    if (cJSON_IsString(create_time))
    {
        char *endpt;
        tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
    }

    if (tx == NULL)
    {
        ereport(ERROR,
            (errmsg("[ZL] cannot create transaction in shared memory")));
        return NULL;
    }
    cJSON_Delete(parsed);
    return tx;

error:
    ereport(ERROR,
        (errmsg("[ZL] Cannot parse transaction: %s", json)));
    /* no need to do clean here, because memory context will do that for us */
    return NULL;
}

static BCBlock *
parse_block_object(cJSON *parsed)
{
    cJSON *tx_list;
    cJSON *block_id;
    cJSON *tx_json;
    BCBlock *block;
    int j = 0;
    int tx_base = 0;
    int tx_local_idx = 0;
    BCBlock *sentinel = NULL;
    //static int  tx_id_counter = 0; // not bcdb
    //int  tx_id_counter = 0; // not bcdb

	// printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
	//printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    if (!parsed)
        goto error;
    
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    block_id = cJSON_GetObjectItemCaseSensitive(parsed, "bid");
    if (!cJSON_IsNumber(block_id))
        goto error;
    
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    tx_list = cJSON_GetObjectItemCaseSensitive(parsed, "txs");
    if (!cJSON_IsArray(tx_list))
        goto error;

	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    block = get_block_by_id(block_id->valueint, true);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d blksz %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , get_blksz(), getpid());
#endif
    block->num_tx = cJSON_GetArraySize(tx_list);
    if (block->num_tx < 0 || block->num_tx > MAX_TX_PER_BLOCK)
        goto error;
    sentinel = get_block_by_id(1, true);
    Assert(sentinel != NULL);
    tx_base = __sync_fetch_and_add(&sentinel->num_tx_sub, block->num_tx);
    cJSON_ArrayForEach(tx_json, tx_list)
    {
        cJSON   *sql      = NULL;
        cJSON   *hash     = NULL;
        cJSON   *create_time = NULL;
        BCDBShmXact   *tx;
        int     isolation;
        bool    pred_lock = false;

        sql = cJSON_GetObjectItemCaseSensitive(tx_json, "sql");
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
        if (!cJSON_IsString(sql) || (sql->valuestring == NULL))
            goto error;

	if(j < 5) {
		//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
		cJSON_Print(sql);
	}
        hash = cJSON_GetObjectItemCaseSensitive(tx_json, "hash");
        if (!cJSON_IsString(hash))
            goto error;
	if(j < 5) {
		//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
		cJSON_Print(hash);
		j++; 
	}
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());

        isolation = XACT_SERIALIZABLE;
        pred_lock = true;

        tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);

	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
        create_time = cJSON_GetObjectItemCaseSensitive(tx_json, "create_ts");

        if (cJSON_IsString(create_time))
        {
            char *endpt;
            tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
        }
        
        tx->tx_id = tx_base + tx_local_idx;
        tx->block_id_committed = block->id;
        block->txs[tx_local_idx] = tx;
        tx_local_idx += 1;
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d txid %d bid %d hash %s \n", __FILE__, __FUNCTION__, __LINE__ , tx->tx_id, block->id, hash->valuestring);
#endif
    }
    //if(blocksize != 0) set_blksz(blocksize);
    //block->blksize = blocksize;
	//printf("ariaMyDbg %s : %s: %d blksz %d \n", __FILE__, __FUNCTION__, __LINE__ , get_blksz());
    return block;

error:
    print_trace();
    ereport(FATAL,
        (errmsg("[ZL] cannot create block in shared memory")));
    return NULL;
}

BCBlock *
parse_block_with_txs(const char *json)
{
    cJSON *parsed;
    BCBlock *block;

    parsed = cJSON_Parse(json);
    if (!parsed)
        goto error;

    block = parse_block_object(parsed);
    cJSON_Delete(parsed);
    return block;

error:
    print_trace();
    ereport(FATAL,
        (errmsg("[ZL] cannot create block in shared memory")));
    return NULL;
}

int 
bcdb_middleware_submit_tx(const char* tx_string)
{
    BCDBShmXact *tx;
    tx = parse_tx(tx_string);
    tx_queue_insert(tx, tx_num++);
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
    return 0;
}

char *
bcdb_middleware_submit_block(const char* block_json)
{
    BCBlock     *block;
    struct timeval tv1;
    tv1.tv_sec = 0; tv1.tv_usec = 0;
    int last_tx_id = -1;
    //struct timeval tv1 ;
    //tv1.tv_sec = 0; tv1.tv_usec = 0;
    // static int tmp = 0;
    ++block_meta->global_bmax;
    block = parse_block_with_txs(block_json);
/*
if(tmp < 2) {
tmp++;
print_trace();
} else { return NULL; }
*/
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d pid %d txnum %d blk-numtx %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), tx_num, block->num_tx);
#endif
    for (int i= 0; i < block->num_tx; i++)
    {
      BCDBShmXact *tx = block->txs[i];
      tx_queue_insert(tx, tx->tx_id);
      last_tx_id = tx->tx_id;
    }

        block = get_block_by_id(1, false);
        Assert(block != NULL);
        gettimeofday(&tv1, NULL);
        if (last_tx_id >= 0)
            bcdb_wait_until_committed((BCTxID) last_tx_id);
/*
*/
#if SAFEDBG
			gettimeofday(&tv1, NULL);
			printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
	        printf("blkmid read result at %d= %s\n", last_tx_id, block->result[bcdb_result_slot_for_txid(last_tx_id)]);
	        printf("\n\t *** safeDB completed txid %d pid %d %s : %s: %d *** \n\n",
	               last_tx_id, getpid(), __FILE__, __FUNCTION__, __LINE__ );
	        printf("\n\t *** safeDB txid %d pid %d result %s file %s : %s: %d *** \n\n", 
	               last_tx_id, getpid(), &block->result[bcdb_result_slot_for_txid(last_tx_id)],__FILE__, __FUNCTION__, __LINE__ );
#endif
//ereport(INFO, (errmsg(&block->result[tx_num2-1])));
// TODO -- another way to convey results...
// wait-to-finish() ?? or 

//safeOut();
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
    if (last_tx_id < 0)
        return block->result[0];
    return block->result[bcdb_result_slot_for_txid((BCTxID) last_tx_id)];
}

char *
bcdb_middleware_submit_block_results(const char* block_json)
{
    cJSON       *parsed;
    cJSON       *blocks_json;
    BCBlock    **blocks = NULL;
    int          num_blocks = 0;
    int          block_idx = 0;
    BCBlock     *block = NULL;
    StringInfoData out;

    parsed = cJSON_Parse(block_json);
    if (!parsed)
    {
        print_trace();
        ereport(FATAL,
            (errmsg("[ZL] cannot parse block-submit-results payload")));
    }

    blocks_json = cJSON_GetObjectItemCaseSensitive(parsed, "blocks");
    if (cJSON_IsArray(blocks_json))
    {
        cJSON *block_json_item;

        num_blocks = cJSON_GetArraySize(blocks_json);
        if (num_blocks <= 0)
        {
            cJSON_Delete(parsed);
            initStringInfo(&out);
            return out.data;
        }
        blocks = (BCBlock **) palloc0(sizeof(BCBlock *) * num_blocks);

        cJSON_ArrayForEach(block_json_item, blocks_json)
        {
            ++block_meta->global_bmax;
            blocks[block_idx] = parse_block_object(block_json_item);
            Assert(blocks[block_idx] != NULL);
            block_idx++;
        }
    }
    else
    {
        ++block_meta->global_bmax;
        block = parse_block_object(parsed);
        Assert(block != NULL);
        num_blocks = 1;
        blocks = (BCBlock **) palloc0(sizeof(BCBlock *));
        blocks[0] = block;
    }

    for (int b = 0; b < num_blocks; ++b)
    {
        block = blocks[b];
        for (int i = 0; i < block->num_tx; ++i)
        {
            BCDBShmXact *tx = block->txs[i];
            tx_queue_insert(tx, tx->tx_id);
        }
    }

    initStringInfo(&out);
    for (int b = 0; b < num_blocks; ++b)
    {
        block = blocks[b];
        for (int i = 0; i < block->num_tx; ++i)
        {
            BCDBShmXact *tx = block->txs[i];
            const int mem_txid = bcdb_result_slot_for_txid(tx->tx_id);

            bcdb_wait_until_slot_ready((BCTxID) tx->tx_id);
            appendStringInfoString(&out, tx->hash);
            appendStringInfoChar(&out, '\t');
            append_hex_encoded(&out, block->result[mem_txid]);
            appendStringInfoChar(&out, '\n');
        }
    }

    pfree(blocks);
    cJSON_Delete(parsed);
    return out.data;
}

void
bcdb_middleware_submit_block2(const char* block_json)
{
    BCBlock     *block;
    struct timeval tv1 ;
    tv1.tv_sec = 0; tv1.tv_usec = 0;

    ++block_meta->global_bmax;
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
    block = parse_block_with_txs(block_json);
    for (int i=0; i < block->num_tx; i++)
    {
      tx_queue_insert(block->txs[i], block->txs[i]->tx_id);
		  if( (i % numTxBurst == 0)&&(i > 0)) {
			gettimeofday(&tv1, NULL);
#if SAFEDBG
			printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
			printf("\t ariaMyDbg %s : %s: %d pid %d  sleeping %dms next burstSz %d from tx %d\n\n", __FILE__, __FUNCTION__, __LINE__ , getpid() ,burstTime, numTxBurst, i );
#endif
			usleep(burstTime);
		  }
    }
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
}

void 
bcdb_wait_tx_finish(char *tx_hash)
{
    BCDBShmXact *tx;
    tx = get_tx_by_hash(tx_hash);
    ConditionVariablePrepareToSleep(&tx->cond);
    while(tx->status != TX_COMMITED && tx->status != TX_ABORTED)
        ConditionVariableSleep(&tx->cond, WAIT_EVENT_TX_FINISH);
    ConditionVariableCancelSleep();
}

void
bcdb_middleware_wait_all_to_finish()
{
    WaitGlobalBmin(block_meta->global_bmax + 1);
    ereport(LOG, (errmsg("[ZL] total throughput: %.3f", (double)block_meta->num_committed * 1e6 / (bcdb_get_time() - start_time))));
}

void 
bcdb_middleware_set_txs_committed_block(char * tx_hash, int32 block_id)
{
    BCDBShmXact *tx;
    BCBlock     *block;
    tx = get_tx_by_hash(tx_hash);
    block = get_block_by_id(block_id, true);
    bcdb_middleware_attach_tx_to_block(tx, block);
}

void
bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block)
{
    block_add_tx(block, tx);
    tx->block_id_committed = block->id;
}

void
block_cleaning(BCBlockID current_block_id)
{
    BCBlock *block_to_clean;
    uint64 cur_report_ts = bcdb_get_time();
    int32  cur_num_committed = block_meta->num_committed;
    float abort_rate = (float)block_meta->num_aborted / (block_meta->num_aborted + block_meta->num_committed);
#if SAFEDBG
    printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
#endif

    if (current_block_id > CLEANING_DELAY_BLOCKS)
    {
        BCBlockID clean_block_id = current_block_id - CLEANING_DELAY_BLOCKS;

        block_to_clean = (clean_block_id == 1)
            ? NULL
            : get_block_by_id(clean_block_id, false);
        if (block_to_clean != NULL)
        {
            for (int i=0; i < block_to_clean->num_tx; i++)
            {
#ifdef LOG_STATUS
                block_meta->log_counter += sprintf(block_meta->log + block_meta->log_counter, "%s %d\n", block_to_clean->txs[i]->hash, block_to_clean->txs[i]->status);
                if (block_meta->log_counter > 1024 * 1024 * 10)
                    ereport(FATAL, (errmsg("[ZL] log overflow")));
#endif
                delete_tx(block_to_clean->txs[i]);
            }
        }
        delete_block(block_to_clean);
    }

    if (cur_report_ts - block_meta->previous_report_ts > 1e6 * REPORT_INTERVAL)
    {
        if (block_meta->previous_report_ts != 0)
        {
            ereport(LOG, (errmsg("[ZL] throughput: %.3f", (cur_num_committed - block_meta->previous_report_commit) * 1e6 / (cur_report_ts - block_meta->previous_report_ts))));
            ereport(LOG, (errmsg("[ZL] abort rate: %.3f", abort_rate)));
        }
        block_meta->previous_report_ts = cur_report_ts;
        block_meta->previous_report_commit = cur_num_committed;
    }
}

void
block_cleaning_dt(BCBlockID current_block_id)
{
    BCBlock *block_to_clean;
    BCBlockID clean_block_id;

    if (current_block_id <= CLEANING_DELAY_BLOCKS)
        return;

    /*
     * The DT path stores committed results on the sentinel block and deletes
     * each BCDBShmXact in bcdb_worker_process_tx_dt().  Only reclaim the
     * per-block header here; block->txs[] may point at already-removed tx-pool
     * entries by the time the block ages out.
     */
    clean_block_id = current_block_id - CLEANING_DELAY_BLOCKS;
    if (clean_block_id == 1)
        return;

    block_to_clean = get_block_by_id(clean_block_id, false);
    delete_block(block_to_clean);
}

void
allow_all_block_txs_to_commit(BCBlock *block)
{
    return;
}
/*
*/

void
bcdb_middleware_conflict_check(BCBlock *block)
{
    /* we assume no one is touching the conflict graph here */
    return;
}


void bcdb_middleware_allow_txs_exec_write_set_and_commit(BCBlock *block) {

//    bcdb_middleware_allow_execute_write_set(block);

    allow_all_block_txs_to_commit(block);
}

void bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id(int32 id){
    BCBlock *block;
    
    block = get_block_by_id(id, false);
    Assert(block != NULL);
    bcdb_middleware_allow_txs_exec_write_set_and_commit(block);
}

bool bcdb_is_tx_commited(char * tx_hash){
    BCDBShmXact* target_tx = get_tx_by_hash(tx_hash);

    if(target_tx->status == TX_COMMITED){
        return true;
    }else{
        return false;
    }
}

void 
bcdb_clear_block_txs_store()
{
#if SAFEDBG
    printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
#endif
    shm_hash_clear(block_pool, MAX_NUM_BLOCKS);
    bcdb_reset_block_pool_state();
    clear_tx_pool();
    tx_num = 0;
    block_meta->global_bmin = 1;
    block_meta->global_bmax = 0;
    block_meta->debug_seq += 1;
    block_meta->num_committed = 0;
    block_meta->num_aborted = 0;
    block_meta->previous_report_commit = 0;
    block_meta->previous_report_ts = 0;
    start_time = bcdb_get_time();
    set_num_tx_sub(0);
    set_num_txqd(0);
    while(!LIST_EMPTY(&idle_workers.list))
    {
        WorkerController *worker = LIST_FIRST(&idle_workers.list);
        worker_finish(worker);
        LIST_REMOVE(worker, link);
        pfree(worker);
    }
    idle_workers.num = 0;
}

static void
append_hex_encoded(StringInfo out, const char *input)
{
    static const char kHex[] = "0123456789abcdef";
    const unsigned char *p = (const unsigned char *) input;

    if (input == NULL)
        return;

    while (*p)
    {
        appendStringInfoChar(out, kHex[(*p >> 4) & 0x0F]);
        appendStringInfoChar(out, kHex[*p & 0x0F]);
        ++p;
    }
}

/*
void bcdb_middleware_new_block_handler(BCBlock* block){
*/

/*
// assume dummy file contains jsons per line
Transaction* parsing_dummy_block_file(const char* file_path){
*/

/*
//dummy function called by frontend
void bcdb_middleware_dummy_block(const char* file_path, uint32 block_id){
*/

/*
void bcdb_middleware_dummy_submit_tx(const char* file_path){
*/

//Return false if 1)no tx with that hash or 2) tx is not finish execution
