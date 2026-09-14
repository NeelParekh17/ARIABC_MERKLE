/*-------------------------------------------------------------------------
 *
 * merkledelta.c
 *    Transaction-local Merkle delta staging and synchronous application.
 *
 * Transactions aggregate Merkle XOR deltas by index/leaf.  Direct/plain
 * transactions can materialize those deltas in PRE_COMMIT; Raft-ledger
 * transactions serialize them into the ledger and use the ordered applier.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/merkle.h"
#include "access/xact.h"
#include "bcdb/globals.h"
#include "bcdb/shm_transaction.h"
#include "catalog/index.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

typedef struct MerkleSubxactFrame
{
	SubTransactionId subxid;
	HTAB	   *entries;
	struct MerkleSubxactFrame *next;
} MerkleSubxactFrame;

static MerkleSubxactFrame *merkle_delta_frames = NULL;
static bool merkle_delta_callbacks_registered = false;
static bool merkle_staged_delta_persisted = false;
static uint64 merkle_delta_generation = 0;

static void merkle_delta_xact_callback(XactEvent event, void *arg);
static void merkle_delta_subxact_callback(SubXactEvent event,
										  SubTransactionId mySubid,
										  SubTransactionId parentSubid,
										  void *arg);

void
merkle_crash_failpoint(const char *name)
{
	const char *configured = getenv("ARIABC_MERKLE_FAILPOINT");
	const char *action = getenv("ARIABC_MERKLE_FAILPOINT_ACTION");

	if (configured == NULL || strcmp(configured, name) != 0)
		return;

	elog(LOG, "MERKLE_FAILPOINT_REACHED name=%s pid=%d", name, MyProcPid);
	if (action != NULL && strcmp(action, "backend_kill") == 0)
		(void) kill(MyProcPid, SIGKILL);
	else if (action != NULL && strcmp(action, "postmaster_kill") == 0)
	{
		(void) kill(PostmasterPid, SIGKILL);
		(void) kill(MyProcPid, SIGKILL);
	}
}

static HTAB *
merkle_delta_create_map(MemoryContext context)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MerkleDeltaKey);
	ctl.entrysize = sizeof(MerkleDeltaEntry);
	ctl.hcxt = context;

	return hash_create("Merkle transaction deltas", 32, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static MerkleSubxactFrame *
merkle_delta_find_frame(SubTransactionId subxid)
{
	MerkleSubxactFrame *frame;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		if (frame->subxid == subxid)
			return frame;
	return NULL;
}

static MerkleSubxactFrame *
merkle_delta_get_frame(SubTransactionId subxid, bool create)
{
	MerkleSubxactFrame *frame = merkle_delta_find_frame(subxid);
	MemoryContext old_context;

	if (frame != NULL || !create)
		return frame;

	old_context = MemoryContextSwitchTo(TopTransactionContext);
	frame = palloc0(sizeof(*frame));
	frame->subxid = subxid;
	frame->entries = merkle_delta_create_map(TopTransactionContext);
	frame->next = merkle_delta_frames;
	merkle_delta_frames = frame;
	MemoryContextSwitchTo(old_context);

	return frame;
}

static void
merkle_delta_merge_one(HTAB *target, const MerkleDeltaEntry *source)
{
	MerkleDeltaEntry *entry;
	bool		found;

	entry = hash_search(target, &source->key, HASH_ENTER, &found);
	if (!found)
	{
		entry->key = source->key;
		merkle_hash_zero(&entry->xor_delta);
	}
	merkle_hash_xor(&entry->xor_delta, &source->xor_delta);
	if (merkle_hash_is_zero(&entry->xor_delta))
		(void) hash_search(target, &source->key, HASH_REMOVE, NULL);
}

static void
merkle_delta_unlink_frame(MerkleSubxactFrame *frame)
{
	MerkleSubxactFrame **link;

	for (link = &merkle_delta_frames; *link != NULL; link = &(*link)->next)
	{
		if (*link == frame)
		{
			*link = frame->next;
			hash_destroy(frame->entries);
			pfree(frame);
			return;
		}
	}
}

static void
merkle_delta_reset(void)
{
	while (merkle_delta_frames != NULL)
		merkle_delta_unlink_frame(merkle_delta_frames);
	merkle_staged_delta_persisted = false;
	merkle_delta_generation = 0;
}

static void
merkle_delta_register_callbacks(void)
{
	if (merkle_delta_callbacks_registered)
		return;

	RegisterXactCallback(merkle_delta_xact_callback, NULL);
	RegisterSubXactCallback(merkle_delta_subxact_callback, NULL);
	merkle_delta_callbacks_registered = true;
}

void
merkle_stage_delta_event(Relation indexRel, MerkleDeltaEventType event_type,
						 const uint8 *old_key_hash, const uint8 *new_key_hash,
						 const MerkleHash *hash)
{
	MerkleSubxactFrame *frame;
	MerkleDeltaKey key;
	MerkleDeltaEntry *entry;
	bool		found;

	if (indexRel == NULL || hash == NULL || merkle_hash_is_zero(hash))
		return;
	if (merkle_staged_delta_persisted)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("Merkle data changes are not allowed after durable delta finalization"),
				 errhint("Terminalize the transaction only after all table changes are complete.")));
	merkle_delta_generation++;

	merkle_delta_register_callbacks();
	frame = merkle_delta_get_frame(GetCurrentSubTransactionId(), true);

	MemSet(&key, 0, sizeof(key));
	key.index_oid = RelationGetRelid(indexRel);
	key.index_rnode = indexRel->rd_node;
	key.event_type = (uint8) event_type;
	if (old_key_hash != NULL)
		memcpy(key.old_key_hash, old_key_hash, 8);
	if (new_key_hash != NULL)
		memcpy(key.new_key_hash, new_key_hash, 8);

	entry = hash_search(frame->entries, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->key = key;
		merkle_hash_zero(&entry->xor_delta);
	}
	merkle_hash_xor(&entry->xor_delta, hash);
	if (merkle_hash_is_zero(&entry->xor_delta))
		(void) hash_search(frame->entries, &key, HASH_REMOVE, NULL);

	merkle_crash_failpoint("after_merkle_delta_staged");
}



bool
merkle_has_staged_delta(void)
{
	MerkleSubxactFrame *frame;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		if (hash_get_num_entries(frame->entries) > 0)
			return true;
	return false;
}

static uint64
merkle_staged_entry_count(void)
{
	MerkleSubxactFrame *frame;
	uint64 count = 0;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		count += (uint64) hash_get_num_entries(frame->entries);
	return count;
}

void
merkle_mark_staged_delta_persisted(void)
{
	merkle_staged_delta_persisted = true;
}

typedef struct MerklePartNoticeKey
{
	Oid index_oid;
	int partition;
} MerklePartNoticeKey;

typedef struct MerklePartNoticeEntry
{
	MerklePartNoticeKey key;
	MerkleHash xor_delta;
} MerklePartNoticeEntry;

static void
merkle_emit_staged_deltas_notice(void)
{
	HTAB	   *combined;
	MerkleSubxactFrame *frame;
	HASH_SEQ_STATUS seq;
	MerkleDeltaEntry *entry;
	MerklePartNoticeEntry *pentry;
	StringInfoData out;
	bool first = true;
	MemoryContext old_context;
	HTAB	   *part_map;
	HASHCTL		ctl;

	if (!merkle_update_detection || !merkle_has_staged_delta())
		return;

	old_context = MemoryContextSwitchTo(CurrentMemoryContext);

	combined = merkle_delta_create_map(CurrentMemoryContext);
	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		hash_seq_init(&seq, frame->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(combined, entry);
	}

	if (hash_get_num_entries(combined) == 0)
	{
		hash_destroy(combined);
		MemoryContextSwitchTo(old_context);
		return;
	}

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MerklePartNoticeKey);
	ctl.entrysize = sizeof(MerklePartNoticeEntry);
	ctl.hcxt = CurrentMemoryContext;
	part_map = hash_create("MerklePartNoticeMap", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	hash_seq_init(&seq, combined);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		MerklePartNoticeKey pkey;
		bool found;

		if (merkle_hash_is_zero(&entry->xor_delta))
			continue;

		if (!OidIsValid(entry->key.index_oid))
			continue;

		PG_TRY();
		{
			MemSet(&pkey, 0, sizeof(pkey));
			pkey.index_oid = entry->key.index_oid;
			pkey.partition = 0;

			pentry = hash_search(part_map, &pkey, HASH_ENTER, &found);
			if (!found)
			{
				pentry->key = pkey;
				merkle_hash_zero(&pentry->xor_delta);
			}
			merkle_hash_xor(&pentry->xor_delta, &entry->xor_delta);
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
	}

	hash_destroy(combined);

	initStringInfo(&out);

	hash_seq_init(&seq, part_map);
	while ((pentry = hash_seq_search(&seq)) != NULL)
	{
		char *hex;

		if (merkle_hash_is_zero(&pentry->xor_delta))
			continue;

		hex = merkle_hash_to_hex(&pentry->xor_delta);
		if (!first)
			appendStringInfoString(&out, " ");
		appendStringInfo(&out, "(%d, %s)", pentry->key.partition, hex);
		first = false;
		pfree(hex);
	}

	hash_destroy(part_map);
	MemoryContextSwitchTo(old_context);

	if (!first)
		ereport(NOTICE,
				(errmsg("BCDB_MERKLE_ROOTS: %s", out.data)));
	if (out.data)
		pfree(out.data);
}

/*
 * Apply the current transaction's staged deltas while the caller's current
 * transaction/subtransaction is still open.  DET worker apply uses this entry
 * point so heap DML and Merkle-node DML share the same rollback boundary.
 * The PRE_COMMIT callback below remains as a fallback for ordinary SQL paths
 * that do not pass through the BCDB worker apply stage.
 */
void
merkle_apply_staged_deltas_synchronously(void)
{
	HTAB *combined;
	MerkleSubxactFrame *frame;
	HASH_SEQ_STATUS seq;
	MerkleDeltaEntry *entry;
	bool saved_is_bcdb_worker;

	if (!merkle_has_staged_delta() || merkle_staged_delta_persisted)
		return;

	combined = merkle_delta_create_map(CurrentMemoryContext);
	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		hash_seq_init(&seq, frame->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(combined, entry);
	}

	merkle_crash_failpoint("before_merkle_sync_apply");
	/*
	 * DET workers mark their business DML with is_bcdb_worker so the heap
	 * change can be deferred into the optimistic write set.  The synchronous
	 * applier is already in the post-apply phase and its SPI statements are
	 * the real Merkle-node mutations; routing those statements back through
	 * BCDB's deferred DML path would leave the tree update unapplied at commit.
	 */
	saved_is_bcdb_worker = is_bcdb_worker;
	is_bcdb_worker = false;
	PG_TRY();
	{
		merkle_apply_staged_synchronous_safe(combined);
	}
	PG_CATCH();
	{
		is_bcdb_worker = saved_is_bcdb_worker;
		hash_destroy(combined);
		PG_RE_THROW();
	}
	PG_END_TRY();
	is_bcdb_worker = saved_is_bcdb_worker;
	merkle_crash_failpoint("after_merkle_sync_apply");
	hash_destroy(combined);
	merkle_staged_delta_persisted = true;
}

static void
merkle_delta_xact_callback(XactEvent event, void *arg)
{
	(void) arg;

	if (event == XACT_EVENT_PRE_PREPARE && merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("prepared transactions are not supported after Merkle index updates"),
				 errdetail("Crash-safe Merkle deltas must be committed by their originating backend.")));

	if (event == XACT_EVENT_PRE_COMMIT)
	{
		if (merkle_has_staged_delta())
		{
			merkle_emit_staged_deltas_notice();
			/*
			 * Synchronous direct apply: apply all staged Merkle deltas
			 * directly in-place before committing the transaction.
			 */
			if (!merkle_staged_delta_persisted)
				merkle_apply_staged_deltas_synchronously();
		}
		return;
	}

	if (event == XACT_EVENT_PARALLEL_PRE_COMMIT && merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel workers cannot persist Merkle deltas")));

	if (event == XACT_EVENT_COMMIT && merkle_staged_delta_persisted)
		merkle_crash_failpoint("after_user_transaction_commit");

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PARALLEL_ABORT || event == XACT_EVENT_PREPARE)
		merkle_delta_reset();
}

static void
merkle_delta_subxact_callback(SubXactEvent event,
								  SubTransactionId mySubid,
								  SubTransactionId parentSubid,
								  void *arg)
{
	MerkleSubxactFrame *child;

	(void) arg;
	child = merkle_delta_find_frame(mySubid);
	if (child == NULL)
		return;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		MerkleSubxactFrame *parent = merkle_delta_get_frame(parentSubid, true);
		HASH_SEQ_STATUS seq;
		MerkleDeltaEntry *entry;

		hash_seq_init(&seq, child->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(parent->entries, entry);
		merkle_delta_unlink_frame(child);
	}
	else if (event == SUBXACT_EVENT_ABORT_SUB)
		merkle_delta_unlink_frame(child);
}
