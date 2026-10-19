/*-------------------------------------------------------------------------
 * merge.c -- persistent copy/commit/replace protocol and database workers.
 * Copyright (c) 2026, AriaBC contributors.
 * Merge design derived from lsm3, Copyright (c) 2020 Postgres Professional.
 * Distributed under the PostgreSQL License; see LICENSE.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid_d.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/syscache.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "storage/predicate.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "merkle_lsm.h"

void ml_register_workers(void);
PGDLLEXPORT void merkle_lsm_launcher(Datum arg);
PGDLLEXPORT void merkle_lsm_merger(Datum arg);

static volatile sig_atomic_t ml_shutdown;
static volatile sig_atomic_t ml_reload;
static int ml_debug_point;
static int ml_debug_level;
static int ml_debug_delay;
static bool ml_debug_error;
static bool ml_error_fired;
static bool ml_session_locked;
static LockRelId ml_session_relid;

static void
ml_release_session_lock(int code, Datum arg)
{
	if (ml_session_locked)
	{
		UnlockRelationIdForSession(&ml_session_relid, AccessShareLock);
		ml_session_locked = false;
	}
}

static void
ml_hold_base(Relation base)
{
	ml_session_relid = base->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&ml_session_relid, AccessShareLock);
	ml_session_locked = true;
}

static void
ml_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ml_shutdown = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
ml_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ml_reload = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
ml_wait(long ms)
{
	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 ms, PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
	if (ml_reload)
	{
		ml_reload = false;
		ProcessConfigFile(PGC_SIGHUP);
		if (!ml_debug_error)
			ml_error_fired = false;
	}
}

/* Test-only SIGHUP GUCs, disabled by default. A delay gives the controller a
 * deterministic window in which to terminate the worker or postmaster. */
static void
ml_inject(int point, int level)
{
	if (ml_debug_point != point || ml_debug_level != level)
		return;
	elog(LOG, "merkle_lsm injection point %d level %d", point, level);
	if (ml_debug_error && !ml_error_fired)
	{
		ml_error_fired = true;
		elog(ERROR, "merkle_lsm injected merge error");
	}
	if (ml_debug_delay > 0)
		ml_wait(ml_debug_delay);
}

typedef struct MlWorkerOwner
{
	Oid			database;
	pid_t		pid;
} MlWorkerOwner;

static HTAB *ml_worker_owners;
static LWLock *ml_worker_lock;
static shmem_startup_hook_type ml_previous_startup;

static void
ml_shmem_startup(void)
{
	HASHCTL		ctl;

	if (ml_previous_startup)
		ml_previous_startup();
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(MlWorkerOwner);
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ml_worker_owners = ShmemInitHash("merkle_lsm database workers", 16, 1024,
								   &ctl, HASH_ELEM | HASH_BLOBS);
	ml_worker_lock = &(GetNamedLWLockTranche("merkle_lsm"))->lock;
	LWLockRelease(AddinShmemInitLock);
}

static bool
ml_claim_worker(void)
{
	MlWorkerOwner *entry;
	bool		found;
	bool		claimed;

	LWLockAcquire(ml_worker_lock, LW_EXCLUSIVE);
	entry = hash_search(ml_worker_owners, &MyDatabaseId, HASH_ENTER, &found);
	claimed = !found || entry->pid == 0;
	if (claimed)
		entry->pid = MyProcPid;
	LWLockRelease(ml_worker_lock);
	return claimed;
}

static void
ml_release_worker(int code, Datum arg)
{
	MlWorkerOwner *entry;

	LWLockAcquire(ml_worker_lock, LW_EXCLUSIVE);
	entry = hash_search(ml_worker_owners, &MyDatabaseId, HASH_FIND, NULL);
	if (entry && entry->pid == MyProcPid)
		hash_search(ml_worker_owners, &MyDatabaseId, HASH_REMOVE, NULL);
	LWLockRelease(ml_worker_lock);
}

/* Exact (all key attributes, heap TID) search in a heapkeyspace B-tree.
 * Destination has a single merger writer; no check/insert race is possible. */
static bool
ml_contains(Relation destination, IndexTuple tuple)
{
	BTScanInsert key = _bt_mkscankey(destination, tuple);
	BTStack		stack;
	Buffer		buffer = InvalidBuffer;
	bool		found = false;

	stack = _bt_search(destination, key, &buffer, BT_READ, NULL);
	if (BufferIsValid(buffer))
	{
		Page		page = BufferGetPage(buffer);
		BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		int			low = P_FIRSTDATAKEY(opaque);
		int			high = PageGetMaxOffsetNumber(page) + 1;

		while (low < high)
		{
			int			mid = low + (high - low) / 2;

			if (_bt_compare(destination, key, page, mid) > 0)
				low = mid + 1;
			else
				high = mid;
		}
		found = low <= PageGetMaxOffsetNumber(page) &&
			_bt_compare(destination, key, page, low) == 0;
		_bt_relbuf(destination, buffer);
	}
	_bt_freestack(stack);
	pfree(key);
	return found;
}

static bool
ml_nonempty(Relation relation)
{
	IndexScanDesc scan = ml_btbegin(relation, 0, 0);
	bool		result;

	scan->xs_snapshot = SnapshotAny;
	btrescan(scan, NULL, 0, NULL, 0);
	result = btgettuple(scan, ForwardScanDirection);
	ml_btend(scan);
	return result;
}

static void
ml_copy(Relation heap, Relation base, const MlMeta *data)
{
	int			level = data->merge_level;
	Oid			sourceid = data->component[level][data->merge_slot];
	Oid			destid = level + 1 == data->levels - 1 ? data->base :
		data->component[level + 1][data->active[level + 1]];
	Relation	source = index_open(sourceid, AccessShareLock);
	Relation	destination = index_open(destid, RowExclusiveLock);
	IndexScanDesc scan = ml_btbegin(source, 0, 0);
	uint64		count = 0;

	scan->xs_snapshot = SnapshotAny;
	scan->xs_want_itup = true;
	btrescan(scan, NULL, 0, NULL, 0);
	while (btgettuple(scan, ForwardScanDirection))
	{
		CHECK_FOR_INTERRUPTS();
		if (!ml_contains(destination, scan->xs_itup))
			_bt_doinsert(destination, scan->xs_itup, UNIQUE_CHECK_NO, heap);
		if (++count == 32)
			ml_inject(1, level);
	}
	ml_btend(scan);
	index_close(destination, RowExclusiveLock);
	index_close(source, AccessShareLock);
	elog(DEBUG1, "merkle_lsm copied " UINT64_FORMAT " entries from %u", count, sourceid);
}

/* Return true when there was work. Every phase runs in its own transaction.
 * Metadata transitions themselves are physical WAL operations, independent
 * of a backend snapshot or transaction commit (like B-tree page splits). */
bool
ml_merge_step(Oid baseoid)
{
	Relation	base;
	Relation	heap;
	Relation	meta;
	MlMeta		data;
	bool		work = false;
	int			level;

	/* DROP may race discovery; taking the base lock precedes opening it. */
	if (!ConditionalLockRelationOid(baseoid, AccessShareLock))
		return false;
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(baseoid)))
		return false;
	base = index_open(baseoid, NoLock);
	/* Never queue a lock ahead of VACUUM or DDL. In particular this prevents
	 * inversion with ALTER TABLE / REINDEX which lock heap before indexes. */
	if (!ConditionalLockRelationOid(base->rd_index->indrelid, ShareUpdateExclusiveLock))
	{
		index_close(base, NoLock);
		return false;
	}
	heap = table_open(base->rd_index->indrelid, NoLock);
	meta = ml_open_meta(base, AccessShareLock, false);
	ml_read_meta(meta, &data);
	pgstat_report_activity(STATE_RUNNING,
		data.phase == MLSM_COPY ? "merkle_lsm: copying" :
		(data.phase == MLSM_COPIED ? "merkle_lsm: replacing" : "merkle_lsm: checking sizes"));
	if (data.phase == MLSM_IDLE)
	{
		/* Drain the largest overflowing level first, leaving room below L0. */
		int64		target[MLSM_MAX_LEVELS - 1];

		target[0] = (int64) data.top_size * 1024;
		for (level = 1; level < data.levels - 1; level++)
			target[level] = Min(target[level - 1] * data.size_ratio, (int64) MaxBlockNumber * BLCKSZ);
		for (level = data.levels - 2; level >= 0; level--)
		{
			Relation	active = index_open(data.component[level][data.active[level]], AccessShareLock);
			bool		ready = data.flush ? ml_nonempty(active) :
				(int64) RelationGetNumberOfBlocks(active) * BLCKSZ >= target[level];

			index_close(active, AccessShareLock);
			if (ready)
			{
				/* Writers hold this buffer SHARE only for their btinsert call.
				 * No copying or source lock is acquired under this lock. */
				data.merge_level = level;
				data.merge_slot = data.active[level];
				data.active[level] ^= 1;
				data.phase = MLSM_COPY;
				data.epoch++;
				data.started_us = GetCurrentTimestamp();
				ml_write_meta(meta, &data);
				work = true;
				break;
			}
		}
		if (!work && data.flush)
		{
			Buffer		buffer = ReadBuffer(meta, 0);
			GenericXLogState *state;
			Page		page;

			/* No merger can add upper-level entries concurrently. Inserts
			 * after the inspection belong to the next flush frontier. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(meta);
			page = GenericXLogRegisterBuffer(state, buffer, 0);
			{
				Relation active = index_open(data.component[0][data.active[0]], AccessShareLock);

				/* Close the inspection/request race: inserts cannot cross
				 * this final empty check while metadata is EXCLUSIVE. */
				if (!ml_nonempty(active))
					((MlMeta *) PageGetContents(page))->flush = false;
				index_close(active, AccessShareLock);
			}
			GenericXLogFinish(state);
			UnlockReleaseBuffer(buffer);
		}
	}
	else if (data.phase == MLSM_COPY)
	{
		ml_hold_base(base);
		ml_copy(heap, base, &data);
		/* Copy WAL must commit BEFORE the durable copied marker. */
		index_close(meta, AccessShareLock);
		table_close(heap, NoLock);
		index_close(base, NoLock);
		PopActiveSnapshot();
		CommitTransactionCommand();
		ml_inject(2, data.merge_level);
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		base = index_open(baseoid, AccessShareLock);
		meta = ml_open_meta(base, AccessShareLock, false);
		data.phase = MLSM_COPIED;
		ml_write_meta(meta, &data);
		index_close(meta, AccessShareLock);
		index_close(base, AccessShareLock);
		ml_release_session_lock(0, (Datum) 0);
		return true;
	}
	else if (data.phase == MLSM_COPIED)
	{
		Oid			sourceid = data.component[data.merge_level][data.merge_slot];
		Relation	source;

		if (ConditionalLockRelationOid(sourceid, AccessExclusiveLock))
		{
			ml_hold_base(base);
			source = index_open(sourceid, NoLock);
			TransferPredicateLocksToHeapRelation(source);
			/* Transactional relfilenode replacement, never in-place truncate.
			 * An ERROR/kill restores the old source. A committed copy exists
			 * already; after restart repeating the replacement is harmless. */
			RelationSetNewRelfilenode(source, source->rd_rel->relpersistence);
			ml_empty_btree(source);
			ml_inject(3, data.merge_level);
			index_close(source, NoLock);
			index_close(meta, AccessShareLock);
			table_close(heap, NoLock);
			index_close(base, NoLock);
			PopActiveSnapshot();
			CommitTransactionCommand();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			base = index_open(baseoid, AccessShareLock);
			meta = ml_open_meta(base, AccessShareLock, false);
			data.phase = MLSM_IDLE;
			data.merges[data.merge_level]++;
			data.last_us = GetCurrentTimestamp() - data.started_us;
			ml_write_meta(meta, &data);
			index_close(meta, AccessShareLock);
			index_close(base, AccessShareLock);
			ml_release_session_lock(0, (Datum) 0);
			return true;
		}
	}
	index_close(meta, AccessShareLock);
	table_close(heap, NoLock);
	index_close(base, NoLock);
	return work;
}

static List *
ml_oids(const char *query)
{
	List	   *oids = NIL;
	uint64		i;

	SPI_connect();
	if (SPI_execute(query, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "merkle_lsm worker discovery failed");
	for (i = 0; i < SPI_processed; i++)
	{
		bool		null;
		Oid			oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &null));
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

		oids = lappend_oid(oids, oid);
		MemoryContextSwitchTo(old);
	}
	SPI_finish();
	return oids;
}

void
merkle_lsm_merger(Datum arg)
{
	int			failures = 0;

	pqsignal(SIGTERM, ml_sigterm);
	pqsignal(SIGHUP, ml_sighup);
	BackgroundWorkerUnblockSignals();
	SetConfigOption("default_transaction_isolation", "read committed", PGC_USERSET, PGC_S_SESSION);
	BackgroundWorkerInitializeConnectionByOid(DatumGetObjectId(arg), BOOTSTRAP_SUPERUSERID, 0);
	before_shmem_exit(ml_release_session_lock, (Datum) 0);
	if (!ml_claim_worker())
		proc_exit(0);
	before_shmem_exit(ml_release_worker, (Datum) 0);
	while (!ml_shutdown)
	{
		MemoryContext old = CurrentMemoryContext;
		MemoryContext iteration = AllocSetContextCreate(TopMemoryContext, "merkle_lsm iteration", ALLOCSET_DEFAULT_SIZES);

		PG_TRY();
		{
			List	   *oids;
			Oid		   *array;
			int			count;
			int			i = 0;
			ListCell   *cell;

			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			oids = ml_oids("SELECT c.oid FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle_lsm' AND c.relkind='i'");
			count = list_length(oids);
			array = MemoryContextAlloc(iteration, Max(count, 1) * sizeof(Oid));
			foreach(cell, oids)
				array[i++] = lfirst_oid(cell);
			PopActiveSnapshot();
			CommitTransactionCommand();
			for (i = 0; i < count && !ml_shutdown; i++)
			{
				StartTransactionCommand();
				PushActiveSnapshot(GetTransactionSnapshot());
				ml_merge_step(array[i]);
				PopActiveSnapshot();
				CommitTransactionCommand();
			}
			pfree(array);
			if (count == 0)
				proc_exit(0);
			failures = 0;
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(old);
			EmitErrorReport();
			FlushErrorState();
			AbortCurrentTransaction();
			ml_release_session_lock(0, (Datum) 0);
			failures = Min(failures + 1, 8);
			elog(LOG, "merkle_lsm merger retry %d; durable merge state retained", failures);
		}
		PG_END_TRY();
		MemoryContextDelete(iteration);
		if (failures >= 8)
		{
			elog(LOG, "merkle_lsm retry budget exhausted; exiting for launcher restart");
			proc_exit(1);
		}
		pgstat_report_activity(STATE_IDLE, NULL);
		ml_wait(failures ? Min(30000, 100 << failures) : 100);
	}
	proc_exit(0);
}

typedef struct MlWorker
{
	Oid			database;
	BackgroundWorkerHandle *handle;
} MlWorker;

void
merkle_lsm_launcher(Datum arg)
{
	List	   *workers = NIL;

	pqsignal(SIGTERM, ml_sigterm);
	pqsignal(SIGHUP, ml_sighup);
	BackgroundWorkerUnblockSignals();
	SetConfigOption("default_transaction_isolation", "read committed", PGC_USERSET, PGC_S_SESSION);
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	while (!ml_shutdown)
	{
		List	   *databases;
		ListCell   *cell;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		databases = ml_oids("SELECT oid FROM pg_database WHERE datallowconn AND NOT datistemplate");
		foreach(cell, databases)
		{
			Oid			database = lfirst_oid(cell);
			MlWorker   *entry = NULL;
			ListCell   *wc;
			pid_t		pid;

			foreach(wc, workers)
				if (((MlWorker *) lfirst(wc))->database == database)
					entry = lfirst(wc);
			if (entry && GetBackgroundWorkerPid(entry->handle, &pid) != BGWH_STOPPED)
				continue;
			if (!entry)
			{
				MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

				entry = palloc0(sizeof(*entry));
				entry->database = database;
				workers = lappend(workers, entry);
				MemoryContextSwitchTo(old);
			}
			if (entry->handle)
			{
				pfree(entry->handle);
				entry->handle = NULL;
			}
			{
				BackgroundWorker worker;
				MemoryContext old;

				memset(&worker, 0, sizeof(worker));
				snprintf(worker.bgw_name, BGW_MAXLEN, "merkle_lsm merger %u", database);
				strcpy(worker.bgw_type, "merkle_lsm merger");
				strcpy(worker.bgw_library_name, "merkle_lsm");
				strcpy(worker.bgw_function_name, "merkle_lsm_merger");
				worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
				worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
				worker.bgw_restart_time = BGW_NEVER_RESTART;
				worker.bgw_main_arg = ObjectIdGetDatum(database);
				old = MemoryContextSwitchTo(TopMemoryContext);
				if (!RegisterDynamicBackgroundWorker(&worker, &entry->handle))
					elog(LOG, "merkle_lsm: no worker slot for database %u; retrying", database);
				MemoryContextSwitchTo(old);
			}
		}
		PopActiveSnapshot();
		CommitTransactionCommand();
		ml_wait(2000);
	}
	proc_exit(0);
}

void
ml_register_workers(void)
{
	BackgroundWorker worker;

	RequestAddinShmemSpace(hash_estimate_size(1024, sizeof(MlWorkerOwner)));
	RequestNamedLWLockTranche("merkle_lsm", 1);
	ml_previous_startup = shmem_startup_hook;
	shmem_startup_hook = ml_shmem_startup;
	DefineCustomIntVariable("merkle_lsm.debug_point", "Injection point: 0 off, 1 partial copy, 2 committed copy, 3 replacement before commit", NULL,
							&ml_debug_point, 0, 0, 3, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("merkle_lsm.debug_level", "Injection source level", NULL,
							&ml_debug_level, 0, 0, 2, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("merkle_lsm.debug_delay_ms", "Injection pause in milliseconds", NULL,
							&ml_debug_delay, 0, 0, 60000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomBoolVariable("merkle_lsm.debug_error", "Inject one ERROR per worker at selected point", NULL,
							 &ml_debug_error, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
	memset(&worker, 0, sizeof(worker));
	strcpy(worker.bgw_name, "merkle_lsm launcher");
	strcpy(worker.bgw_type, "merkle_lsm launcher");
	strcpy(worker.bgw_library_name, "merkle_lsm");
	strcpy(worker.bgw_function_name, "merkle_lsm_launcher");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;
	RegisterBackgroundWorker(&worker);
}
