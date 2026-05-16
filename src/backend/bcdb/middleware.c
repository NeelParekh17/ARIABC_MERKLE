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
static BCBlock *parse_block_with_txs(const char *json);
static void append_hex_encoded(StringInfo out, const char *input);
static int32 bcdb_select_worker_count(int32 requested);
static inline int bcdb_result_slot_for_txid(BCTxID tx_id);
static inline uint64 bcdb_wait_until_committed(BCTxID target_tx_id);
static inline uint64 bcdb_wait_until_slot_ready(BCTxID target_tx_id);
static inline uint64 bcdb_wait_until_block_slots_ready(BCBlock *block);
static bool bcdb_block_profile_enabled(void);
static bool bcdb_block_return_actual_results_enabled(void);
static bool bcdb_block_wait_watermark_enabled(void);
static bool bcdb_decouple_workers_enabled(void);
static int bcdb_block_enqueue_yield_every(void);
static int bcdb_uint64_cmp(const void *a, const void *b);

static bool
bcdb_block_profile_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_PROFILE");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

static bool
bcdb_block_return_actual_results_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_RETURN_ACTUAL_RESULTS");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

static bool
bcdb_block_wait_watermark_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_BLOCK_WAIT_WATERMARK");

		/*
		 * Safe for the current queued block path: workers publish
		 * result_committed_txid before advancing last_committed_tx_id, so a
		 * contiguous watermark at the block's last tx implies every block-local
		 * result slot is readable.  It was not faster on the strict 4-node
		 * YCSB run, so keep it opt-in for A/B testing.
		 */
		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

static int
bcdb_block_enqueue_yield_every(void)
{
	static int cached = -1;

	if (cached < 0)
	{
		const char *v = getenv("BCDB_BLOCK_ENQUEUE_YIELD_EVERY");
		int parsed = 0;

		if (v != NULL && *v != '\0')
		{
			parsed = atoi(v);
			if (parsed < 0)
				parsed = 0;
			if (parsed > 256)
				parsed = 256;
		}
		cached = parsed;
	}
	return cached;
}

static bool
bcdb_decouple_workers_enabled(void)
{
	static int enabled = -1;

	if (enabled < 0)
	{
		const char *v = getenv("BCDB_DECOUPLE_WORKERS");

		enabled = (v != NULL && v[0] != '\0' &&
				   strcmp(v, "0") != 0 &&
				   strcmp(v, "false") != 0 &&
				   strcmp(v, "FALSE") != 0 &&
				   strcmp(v, "no") != 0 &&
				   strcmp(v, "NO") != 0);
	}
	return enabled != 0;
}

static int
bcdb_uint64_cmp(const void *a, const void *b)
{
	uint64 av = *(const uint64 *) a;
	uint64 bv = *(const uint64 *) b;

	if (av < bv)
		return -1;
	if (av > bv)
		return 1;
	return 0;
}

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
static inline uint64
bcdb_wait_until_committed(BCTxID target_tx_id)
{
	BCBlock *blk = get_block_by_id(1, false);
	int spins = 0;
	int poll_us = 0;
	uint64 wait_start_us = bcdb_get_time();
	uint64 next_warn_us  = wait_start_us + 5000000; /* 5 s */

	Assert(blk != NULL);
	for (;;)
	{
		BCTxID committed = get_last_committed_txid(NULL);
		if (committed >= target_tx_id)
			return bcdb_get_time() - wait_start_us;

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
			if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
			{
				ConditionVariablePrepareToSleep(&blk->condCommit);
				if (get_last_committed_txid(NULL) < target_tx_id)
					ConditionVariableSleep(&blk->condCommit, WAIT_EVENT_BLOCK_COMMIT);
				ConditionVariableCancelSleep();
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
static inline uint64
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
			return bcdb_get_time() - wait_start_us;

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

/*
 * Wait once for every result slot in this block to be fully published.
 *
 * The old block-result path walked slots in request order and performed a
 * separate adaptive wait for each missing slot.  Under a 256-tx block that can
 * turn one true readiness delay into many short poll sleeps.  This keeps the
 * exact same correctness condition (every tx's result_committed_txid must match
 * before read) but pays the wait loop once per block.
 */
static inline uint64
bcdb_wait_until_block_slots_ready(BCBlock *block)
{
	BCBlock *result_block = get_block_by_id(1, false);
	int    spins = 0;
	int    poll_us = 0;
	uint64 wait_start_us = bcdb_get_time();
	uint64 next_warn_us = wait_start_us + 5000000; /* 5 s */

	Assert(block != NULL);
	Assert(result_block != NULL);
	for (;;)
	{
		bool all_ready = true;
		BCTxID first_missing_txid = -1;
		BCTxID first_missing_value = -1;
		int first_missing_slot = -1;

		for (int i = 0; i < block->num_tx; ++i)
		{
			BCDBShmXact *tx = block->txs[i];
			const int slot = bcdb_result_slot_for_txid(tx->tx_id);
			BCTxID published;

			published = __atomic_load_n(&result_block->result_committed_txid[slot],
										__ATOMIC_ACQUIRE);
			if (published != tx->tx_id)
			{
				all_ready = false;
				first_missing_txid = tx->tx_id;
				first_missing_value = published;
				first_missing_slot = slot;
				break;
			}
		}
		if (all_ready)
			return bcdb_get_time() - wait_start_us;

		CHECK_FOR_INTERRUPTS();

		{
			uint64 now_us = bcdb_get_time();
			if (now_us >= next_warn_us)
			{
				BCTxID last_committed = get_last_committed_txid(NULL);
				ereport(LOG,
						(errmsg("[BCDB_HANG] block_slots_ready_stuck pid=%d block_id=%d first_missing_txid=%d slot=%d slot_value=%d last_committed=%d waited_us=%lu poll_us=%d spins=%d",
								(int) getpid(), (int) block->id,
								(int) first_missing_txid, first_missing_slot,
								(int) first_missing_value, (int) last_committed,
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
 * Initialize the middleware-facing BCDB runtime for a backend.
 *
 * The SQL argument is still named block_size for historical reasons, but in
 * this implementation it selects the worker/queue count.  The sentinel block
 * (block id 1) owns the runtime result ring and worker-count metadata; it must
 * already match any earlier initialization in this backend lifetime.
 *
 * The memory context is intentionally reused across repeated bcdb_init() calls
 * so restore scripts and benchmark loops do not allocate one long-lived context
 * per invocation.
 */
void
bcdb_middleware_init(bool is_oep_mode, int32 block_size)
{
	MemoryContext    old_context;
	BCBlock *block;
	int32 worker_queues;
	//int32 nWorkers = block_size;
	//nWorkers = 5;

	/* Aria does not have oep mode */
	is_bcdb_master = true;
	if (bcdb_decouple_workers_enabled())
		worker_queues = bcdb_select_worker_count(0);
	else
	{
		worker_queues = bcdb_select_worker_count(block_size);
		bcdb_worker_count = worker_queues;
	}
	blocksize = worker_queues;
	if (bcdb_middleware_context == NULL)
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

/*
 * Initialize BCDB with the same worker/queue semantics as bcdb_middleware_init
 * plus the legacy burst controls used by bcdb_middleware_submit_block2().
 *
 * numTx and timeSlot do not change deterministic correctness; they only govern
 * how submit_block2 pauses between enqueue bursts.  The normal distributed
 * YCSB path uses bcdb_block_submit_results() instead.
 */
void
bcdb_middleware_init2(bool is_oep_mode, int32 block_size, int32 numTx, int32 timeSlot)
{
	MemoryContext    old_context;
	BCBlock *block;
	int32 worker_queues;

	is_bcdb_master = true;
	if (bcdb_decouple_workers_enabled())
		worker_queues = bcdb_select_worker_count(0);
	else
	{
		worker_queues = bcdb_select_worker_count(block_size);
		bcdb_worker_count = worker_queues;
	}
	blocksize = worker_queues;
	numTxBurst = numTx;
	burstTime = timeSlot;
	if (bcdb_middleware_context == NULL)
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

/*
 * Parse one SQL transaction JSON object and allocate its shared tx entry.
 *
 * Expected shape:
 *   {"hash": "...", "sql": "...", "create_ts": "optional integer"}
 *
 * The tx id is not assigned here.  Single-tx submission assigns it atomically
 * in bcdb_middleware_submit_tx(); block submission assigns a contiguous range
 * in parse_block_with_txs() so a whole deterministic block has stable ids
 * before workers see any member of it.
 */
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
	/*
	 * Match the direct deterministic wire path ("s <seq> <sql>"):
	 * PostgreSQL's predicate-lock hook still records BCDB read-set tags before
	 * returning when pred_lock=false, but it skips heavyweight SSI predicate
	 * lock acquisition.  The queued block path relies on BCDB's own
	 * conflict_checkDT(), not PostgreSQL SSI conflict resolution.
	 */
	pred_lock = false;

	tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);
	if (tx == NULL)
	{
		ereport(ERROR,
			(errmsg("[ZL] cannot create transaction in shared memory")));
		return NULL;
	}

#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	create_time = cJSON_GetObjectItemCaseSensitive(parsed, "create_ts");

	if (cJSON_IsString(create_time))
	{
		char *endpt;
		tx->create_time = strtoll(create_time->valuestring, &endpt, 10);
	}

	cJSON_Delete(parsed);
	return tx;

error:
	ereport(ERROR,
		(errmsg("[ZL] Cannot parse transaction: %s", json)));
	/* no need to do clean here, because memory context will do that for us */
	return NULL;
}

/*
 * Parse a block-submit JSON payload into a shared BCBlock and BCDBShmXact set.
 *
 * The active distributed path builds JSON with full tx objects under "txs".
 * A contiguous tx-id range is reserved from the sentinel block before the loop,
 * then each tx receives tx_base + local_index.  That makes each block internally
 * ordered even when multiple frontend backends submit blocks concurrently.
 */
BCBlock *
parse_block_with_txs(const char *json)
{
	cJSON *parsed;
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
	parsed = cJSON_Parse(json);
	if (!parsed)
		goto error;

	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
	block_id = cJSON_GetObjectItemCaseSensitive(parsed, "bid");

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
		/*
		 * Keep block-submit semantics aligned with direct DET execution.
		 * pred_lock=false preserves BCDB read-set capture while avoiding
		 * additional PostgreSQL SSI predicate locks on every YCSB read.
		 */
		pred_lock = false;

		tx = create_tx(hash->valuestring, sql->valuestring, BCDBInvalidTid, BCDBInvalidBid, isolation, pred_lock);
		if (tx == NULL)
			goto error;

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

/*
 * SQL helper behind bcdb_tx_submit().
 *
 * This is the older one-transaction-at-a-time API: parse a tx, assign a unique
 * queue partition/tx id with an atomic counter, enqueue it, and return that id
 * to the caller.  Returning the id is important because callers otherwise see a
 * meaningless constant success value.
 */
int
bcdb_middleware_submit_tx(const char* tx_string)
{
	BCDBShmXact *tx;
	int32       tx_id;

	tx = parse_tx(tx_string);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB transaction")));

	tx_id = __sync_fetch_and_add(&tx_num, 1);
	tx->tx_id = tx_id;
	tx_queue_insert(tx, tx_id);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	return tx_id;
}

/*
 * Legacy block-submit API behind bcdb_block_submit().
 *
 * This path enqueues every tx in the parsed block and returns the result for
 * the highest tx id in that block.  The public SQL wrapper currently ignores
 * that return value; the distributed benchmark path uses
 * bcdb_middleware_submit_block_results() below when it needs per-tx completion
 * records.  Keep this function defensive anyway because older tests and tools
 * still call bcdb_block_submit().
 */
char *
bcdb_middleware_submit_block(const char* block_json)
{
	BCBlock     *submitted_block;
	BCBlock     *result_block;
	struct timeval tv1;
	int max_tx_id = -1;

	tv1.tv_sec = 0; tv1.tv_usec = 0;
	//struct timeval tv1 ;
	//tv1.tv_sec = 0; tv1.tv_usec = 0;
	// static int tmp = 0;
	submitted_block = parse_block_with_txs(block_json);
	if (submitted_block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	__sync_add_and_fetch(&block_meta->global_bmax, 1);
/*
if(tmp < 2) {
tmp++;
print_trace();
} else { return NULL; }
*/
#if SAFEDBG
		printf("ariaMyDbg %s : %s: %d pid %d txnum %d blk-numtx %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid(), tx_num, submitted_block->num_tx);
#endif
	for (int i= 0; i < submitted_block->num_tx; i++)
	{
	  BCDBShmXact *tx = submitted_block->txs[i];
	  tx_queue_insert(tx, tx->tx_id);
	  if (tx->tx_id > max_tx_id)
		  max_tx_id = tx->tx_id;
	}

		result_block = get_block_by_id(1, false);
		if (result_block == NULL)
			ereport(ERROR,
					(errmsg("BCDB result block is not initialized")));
		gettimeofday(&tv1, NULL);
		if (max_tx_id >= 0)
			bcdb_wait_until_committed((BCTxID) max_tx_id);
/*
*/
#if SAFEDBG
			gettimeofday(&tv1, NULL);
			printf("\n\n\t time= %ld.%ld  getpid %d\n", tv1.tv_sec, tv1.tv_usec, getpid());
			printf("blkmid read result at %d= %s\n", max_tx_id, result_block->result[bcdb_result_slot_for_txid(max_tx_id)]);
			printf("\n\t *** safeDB completed txid %d pid %d %s : %s: %d *** \n\n",
				   max_tx_id, getpid(), __FILE__, __FUNCTION__, __LINE__ );
			printf("\n\t *** safeDB txid %d pid %d result %s file %s : %s: %d *** \n\n",
				   max_tx_id, getpid(), &result_block->result[bcdb_result_slot_for_txid(max_tx_id)],__FILE__, __FUNCTION__, __LINE__ );
#endif
//ereport(INFO, (errmsg(&block->result[tx_num2-1])));
// TODO -- another way to convey results...
// wait-to-finish() ?? or

//safeOut();
	//printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
	if (max_tx_id < 0)
		return "";
	{
		const int slot = bcdb_result_slot_for_txid((BCTxID) max_tx_id);
		BCTxID published;

		published = __atomic_load_n(&result_block->result_committed_txid[slot],
									 __ATOMIC_ACQUIRE);
		if (published != max_tx_id)
			ereport(ERROR,
					(errmsg("BCDB result slot mismatch for txid %d: slot %d contains txid %d",
							max_tx_id, slot, (int) published)));
		return result_block->result[slot];
	}
}

/*
 * Active deterministic block-submit API used by ariabc_pg.
 *
 * The caller submits one JSON block, workers execute the contained txs through
 * the BCDB queues, and the function returns a newline-delimited completion
 * payload keyed by tx hash.  The hot correctness rule is slot ownership:
 * result_committed_txid[slot] must equal the exact tx id before the result slot
 * is read or marked consumed, because the ring slot can be reused by later txs.
 */
char *
bcdb_middleware_submit_block_results(const char* block_json)
{
	BCBlock     *block;
	BCBlock     *result_block;
	StringInfoData out;
	bool        profile = bcdb_block_profile_enabled();
	uint64      t_start_us = 0;
	uint64      t_parse_us = 0;
	uint64      t_enqueue_us = 0;
	uint64      t_wait_us = 0;
	uint64      t_format_us = 0;
	uint64      block_wait_us = 0;
	uint64     *slot_wait_us = NULL;
	uint64      slot_wait_sum_us = 0;
	uint64      slot_wait_p50_us = 0;
	uint64      slot_wait_p95_us = 0;
	uint64      slot_wait_max_us = 0;

	if (profile)
		t_start_us = bcdb_get_time();
	block = parse_block_with_txs(block_json);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	__sync_add_and_fetch(&block_meta->global_bmax, 1);
	result_block = get_block_by_id(1, false);
	if (result_block == NULL)
		ereport(ERROR,
				(errmsg("BCDB result block is not initialized")));
	if (profile)
		t_parse_us = bcdb_get_time();

	if (profile && block->num_tx > 0)
		slot_wait_us = (uint64 *) palloc0(sizeof(uint64) * block->num_tx);

	for (int i = 0; i < block->num_tx; ++i)
	{
		BCDBShmXact *tx = block->txs[i];
		tx_queue_insert(tx, tx->tx_id);
		if (bcdb_block_enqueue_yield_every() > 0 &&
			((i + 1) % bcdb_block_enqueue_yield_every()) == 0)
			pg_usleep(1);
	}
	if (profile)
		t_enqueue_us = bcdb_get_time();

	if (block->num_tx > 0)
	{
		BCDBShmXact *last_tx = block->txs[block->num_tx - 1];

		/*
		 * Prefer the contiguous committed watermark.  It advances only after
		 * result_committed_txid has been published for every predecessor, so it
		 * avoids repeatedly scanning a 512/1024-tx block while preserving the
		 * per-slot correctness check below.
		 */
		if (bcdb_block_wait_watermark_enabled())
			block_wait_us = bcdb_wait_until_committed((BCTxID) last_tx->tx_id);
		else
			block_wait_us = bcdb_wait_until_block_slots_ready(block);
	}

	initStringInfo(&out);
	for (int i = 0; i < block->num_tx; ++i)
	{
		BCDBShmXact *tx = block->txs[i];
		const int mem_txid = bcdb_result_slot_for_txid(tx->tx_id);
		BCTxID published;
		uint64 wait_us = 0;

		published = __atomic_load_n(&result_block->result_committed_txid[mem_txid],
									 __ATOMIC_ACQUIRE);
		if (published != tx->tx_id)
		{
			/* Defensive fallback; the block-local wait above should cover this. */
			wait_us = bcdb_wait_until_slot_ready((BCTxID) tx->tx_id);
		}
		if (profile)
		{
			slot_wait_sum_us += wait_us;
			if (wait_us > slot_wait_max_us)
				slot_wait_max_us = wait_us;
			if (slot_wait_us != NULL)
				slot_wait_us[i] = wait_us;
		}
		appendStringInfoString(&out, tx->hash);
		appendStringInfoChar(&out, '\t');
		/*
		 * Default to completion-only result payloads for deterministic block
		 * submit.  The queued path applies txs in deterministic order for
		 * final state, but read-row payloads can reflect replica-local worker
		 * timing.  Throughput/consistency runs vote on completion and then use
		 * the post-run Merkle gate for state correctness.  Actual payloads
		 * remain available for diagnostics via BCDB_BLOCK_RETURN_ACTUAL_RESULTS=1.
		 */
		if (bcdb_block_return_actual_results_enabled())
			append_hex_encoded(&out, result_block->result[mem_txid]);
		appendStringInfoChar(&out, '\n');

		/* Mark slot consumed so the next writer can reuse it safely. */
		__atomic_store_n(&result_block->result_consumed_txid[mem_txid],
						 (int32) tx->tx_id, __ATOMIC_RELEASE);
	}
	if (profile)
	{
		uint64 t_done_us;
		uint64 total_us;

		t_wait_us = block_wait_us + slot_wait_sum_us;
		if (slot_wait_us != NULL && block->num_tx > 0)
		{
			int p50_idx = block->num_tx / 2;
			int p95_idx = (block->num_tx * 95) / 100;

			if (p95_idx >= block->num_tx)
				p95_idx = block->num_tx - 1;
			qsort(slot_wait_us, block->num_tx, sizeof(uint64), bcdb_uint64_cmp);
			slot_wait_p50_us = slot_wait_us[p50_idx];
			slot_wait_p95_us = slot_wait_us[p95_idx];
		}
		t_done_us = bcdb_get_time();
		t_format_us = (t_done_us > t_enqueue_us + t_wait_us)
			? (t_done_us - t_enqueue_us - t_wait_us)
			: 0;
		total_us = t_done_us - t_start_us;

		ereport(LOG,
				(errmsg("PROFILE_BCDB_BLOCK pid=%d block_txs=%d total_ms=%.3f parse_ms=%.3f enqueue_ms=%.3f wait_block_ms=%.3f wait_slot_ms=%.3f format_ms=%.3f slot_wait_avg_us=%.3f slot_wait_p50_us=%lu slot_wait_p95_us=%lu slot_wait_max_us=%lu",
						(int) getpid(),
						block->num_tx,
						total_us / 1000.0,
						(t_parse_us - t_start_us) / 1000.0,
						(t_enqueue_us - t_parse_us) / 1000.0,
						block_wait_us / 1000.0,
						t_wait_us / 1000.0,
						t_format_us / 1000.0,
						block->num_tx > 0
							? ((double) slot_wait_sum_us / (double) block->num_tx)
							: 0.0,
						(unsigned long) slot_wait_p50_us,
						(unsigned long) slot_wait_p95_us,
						(unsigned long) slot_wait_max_us)));
		if (slot_wait_us != NULL)
			pfree(slot_wait_us);
	}

	return out.data;
}

/*
 * Legacy burst-submit variant.
 *
 * This is kept for older experiments that intentionally sleep after every
 * numTxBurst enqueues.  It shares the same parse-before-counter-update rule as
 * the other block-submit APIs but does not wait for or return results.
 */
void
bcdb_middleware_submit_block2(const char* block_json)
{
	BCBlock     *block;
	struct timeval tv1 ;
	tv1.tv_sec = 0; tv1.tv_usec = 0;

#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d pid %d \n", __FILE__, __FUNCTION__, __LINE__ , getpid());
#endif
	block = parse_block_with_txs(block_json);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("failed to parse BCDB block JSON")));
	__sync_add_and_fetch(&block_meta->global_bmax, 1);
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

/*
 * Wait for a legacy hash-addressed transaction to reach a terminal state.
 *
 * Missing hashes are reported as SQL errors instead of crashing on a NULL tx.
 * The wait itself remains open-ended because this compatibility API has no
 * caller-supplied deadline, so it emits a LOG line every five seconds with the
 * tx id and current status.  That makes worker crashes or lost condition
 * variable wakeups visible in server.log while preserving old SQL semantics.
 */
void
bcdb_wait_tx_finish(char *tx_hash)
{
	BCDBShmXact *tx;
	uint64 wait_start_us;
	uint64 next_warn_us;

	tx = get_tx_by_hash(tx_hash);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));
	wait_start_us = bcdb_get_time();
	next_warn_us = wait_start_us + 5000000; /* 5 s */
	ConditionVariablePrepareToSleep(&tx->cond);
	while(tx->status != TX_COMMITED && tx->status != TX_ABORTED)
	{
		uint64 now_us;

		ConditionVariableTimedSleep(&tx->cond, 1000L, WAIT_EVENT_TX_FINISH);
		CHECK_FOR_INTERRUPTS();
		now_us = bcdb_get_time();
		if (now_us >= next_warn_us)
		{
			ereport(LOG,
					(errmsg("[BCDB_HANG] tx_finish_wait_stuck pid=%d tx_hash=%s tx_id=%d status=%d waited_us=%lu",
							(int) getpid(), tx_hash, (int) tx->tx_id,
							(int) tx->status,
							(unsigned long) (now_us - wait_start_us))));
			next_warn_us = now_us + 5000000;
		}
	}
	ConditionVariableCancelSleep();
}

void
bcdb_middleware_wait_all_to_finish()
{
	WaitGlobalBmin(block_meta->global_bmax + 1);
	ereport(LOG, (errmsg("[ZL] total throughput: %.3f", (double)block_meta->num_committed * 1e6 / (bcdb_get_time() - start_time))));
}

/*
 * Attach a previously submitted single transaction to a block by hash.
 *
 * This supports the older two-step workflow:
 *   1. bcdb_tx_submit(tx_json)
 *   2. bcdb_add_tx_with_block_id(hash, block_id)
 *
 * A tx can be attached once.  Re-attaching to the same block is treated as
 * idempotent; moving it to a different block is rejected because workers and
 * conflict metadata may already reference the original block membership.
 */
void
bcdb_middleware_set_txs_committed_block(char * tx_hash, int32 block_id)
{
	BCDBShmXact *tx;
	BCBlock     *block;
	tx = get_tx_by_hash(tx_hash);
	if (tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));
	if (tx->block_id_committed == block_id)
		return;
	if (tx->block_id_committed != BCDBInvalidBid &&
		tx->block_id_committed != BCDBMaxBid)
		ereport(ERROR,
				(errmsg("BCDB transaction %s already belongs to block %d",
						tx_hash, tx->block_id_committed)));
	block = get_block_by_id(block_id, true);
	bcdb_middleware_attach_tx_to_block(tx, block);
}

/*
 * Add tx to block and publish the block id on the tx object.
 *
 * block_add_tx() owns the lock protecting block->txs[] and block->num_tx.  The
 * caller is responsible for validating that the tx exists and has not already
 * been attached to a different block.
 */
void
bcdb_middleware_attach_tx_to_block(BCDBShmXact *tx, BCBlock *block)
{
	block_add_tx(block, tx);
	tx->block_id_committed = block->id;
}

/*
 * Reclaim old non-DT block headers and transaction entries.
 *
 * This function is not a read-only status check: it can delete txs and blocks
 * once the global block window has advanced beyond CLEANING_DELAY_BLOCKS.
 * Deterministic execution uses block_cleaning_dt(), which avoids deleting txs
 * that the DT worker path already reclaimed.
 */
void
block_cleaning(BCBlockID current_block_id)
{
	BCBlock *block_to_clean;
	uint64 cur_report_ts = bcdb_get_time();
	int32  cur_num_committed = block_meta->num_committed;
	int32  total_finished = block_meta->num_aborted + block_meta->num_committed;
	float abort_rate = (total_finished > 0)
		? (float) block_meta->num_aborted / total_finished
		: 0.0f;
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

/*
 * DT-safe block-header cleanup.
 *
 * The deterministic worker path writes final results to the sentinel block and
 * removes tx-pool entries itself.  This cleanup therefore deletes only aged
 * per-block headers and intentionally skips block id 1, the runtime sentinel.
 */
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

/*
 * Historical commit-release hook.
 *
 * Current BCDB workers do not wait on a per-block "commit allowed" flag, so
 * there is no meaningful state to flip here.  Leave this as an explicit no-op
 * rather than inventing a flag that no worker observes.
 */
void
allow_all_block_txs_to_commit(BCBlock *block)
{
	return;
}
/*
*/

/*
 * Historical conflict-check hook for the old block API.
 *
 * Active deterministic execution performs conflict_checkDT() inside worker.c
 * as each queued tx is processed.  This compatibility hook therefore has no
 * work to do and must not be mistaken for the active conflict checker.
 */
void
bcdb_middleware_conflict_check(BCBlock *block)
{
	/* we assume no one is touching the conflict graph here */
	return;
}


/*
 * Compatibility wrapper for the old "allow execute/write/commit" SQL flow.
 *
 * The only remaining callable behavior is the explicit no-op commit-release
 * hook above; active queued deterministic execution does not use this path.
 */
void bcdb_middleware_allow_txs_exec_write_set_and_commit(BCBlock *block) {

//    bcdb_middleware_allow_execute_write_set(block);

	allow_all_block_txs_to_commit(block);
}

/*
 * Look up a block id for the compatibility commit-release wrapper.
 *
 * A missing block is a caller error and is reported with ereport(ERROR) instead
 * of relying on Assert(), which may be compiled out in production builds.
 */
void bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id(int32 id){
	BCBlock *block;

	block = get_block_by_id(id, false);
	if (block == NULL)
		ereport(ERROR,
				(errmsg("BCDB block %d not found", id)));
	bcdb_middleware_allow_txs_exec_write_set_and_commit(block);
}

/*
 * Return true only when the named legacy tx is committed.
 *
 * Invalid hashes are errors.  Returning false for "not found" would make a
 * missing transaction indistinguishable from a real aborted/uncommitted tx.
 */
bool bcdb_is_tx_commited(char * tx_hash){
	BCDBShmXact* target_tx = get_tx_by_hash(tx_hash);

	if (target_tx == NULL)
		ereport(ERROR,
				(errmsg("BCDB transaction not found: %s", tx_hash)));

	if(target_tx->status == TX_COMMITED){
		return true;
	}else{
		return false;
	}
}

/*
 * Reset in-memory BCDB metadata for restore/benchmark setup.
 *
 * This clears block and tx shared-memory pools, resets counters and the
 * sentinel result ring, and closes idle worker controllers.  It does not reset
 * SQL tables, Merkle indexes, Kafka/Raft state, or already-active workers; the
 * distributed runners call it only as part of controlled restore phases after
 * stopping/restarting PostgreSQL backends.
 */
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
