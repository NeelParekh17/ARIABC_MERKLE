/*-------------------------------------------------------------------------
 *
 * merkledelta.c
 *    Transaction-local Merkle delta staging and durable local enqueue.
 *
 * Physical Merkle pages are deliberately not modified by user transactions.
 * Each transaction aggregates XOR deltas by index/leaf and persists one
 * compact binary blob.  The ordered applier in merkleapply.c is the only
 * normal-runtime v7 page mutator.
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
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct MerkleDeltaKey
{
	Oid			index_oid;
	RelFileNode index_rnode;
	int32		leaf_id;
} MerkleDeltaKey;

typedef struct MerkleDeltaEntry
{
	MerkleDeltaKey key;
	MerkleHash	xor_delta;
} MerkleDeltaEntry;

typedef struct MerkleSubxactFrame
{
	SubTransactionId subxid;
	HTAB	   *entries;
	struct MerkleSubxactFrame *next;
} MerkleSubxactFrame;

static MerkleSubxactFrame *merkle_delta_frames = NULL;
static bool merkle_delta_callbacks_registered = false;
static bool merkle_staged_delta_persisted = false;
static uint64 merkle_serialized_entry_count = 0;
static uint64 merkle_delta_generation = 0;
static uint64 merkle_serialized_generation = 0;

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
	merkle_serialized_entry_count = 0;
	merkle_delta_generation = 0;
	merkle_serialized_generation = 0;
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
merkle_stage_delta(Relation indexRel, int leafId, const MerkleHash *hash)
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
	key.leaf_id = leafId;

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

static int
merkle_delta_entry_cmp(const void *left, const void *right)
{
	const MerkleDeltaEntry *a = (const MerkleDeltaEntry *) left;
	const MerkleDeltaEntry *b = (const MerkleDeltaEntry *) right;

	if (a->key.index_oid != b->key.index_oid)
		return a->key.index_oid < b->key.index_oid ? -1 : 1;
	if (a->key.index_rnode.spcNode != b->key.index_rnode.spcNode)
		return a->key.index_rnode.spcNode < b->key.index_rnode.spcNode ? -1 : 1;
	if (a->key.index_rnode.dbNode != b->key.index_rnode.dbNode)
		return a->key.index_rnode.dbNode < b->key.index_rnode.dbNode ? -1 : 1;
	if (a->key.index_rnode.relNode != b->key.index_rnode.relNode)
		return a->key.index_rnode.relNode < b->key.index_rnode.relNode ? -1 : 1;
	if (a->key.leaf_id != b->key.leaf_id)
		return a->key.leaf_id < b->key.leaf_id ? -1 : 1;
	return 0;
}

static void
merkle_delta_put_u32(char *dst, uint32 value)
{
	value = pg_hton32(value);
	memcpy(dst, &value, sizeof(value));
}

static void
merkle_delta_put_u64(char *dst, uint64 value)
{
	value = pg_hton64(value);
	memcpy(dst, &value, sizeof(value));
}

bytea *
merkle_serialize_staged_delta(uint64 raft_log_index, uint32 item_ordinal)
{
	MemoryContext old_context;
	HTAB	   *combined;
	MerkleSubxactFrame *frame;
	HASH_SEQ_STATUS seq;
	MerkleDeltaEntry *entry;
	MerkleDeltaEntry *sorted;
	long		count;
	long		i = 0;
	Size		payload_bytes;
	Size		total_bytes;
	bytea	   *result;
	char	   *header;
	char	   *payload;
	char		crc_header[MERKLE_DELTA_HEADER_BYTES];
	pg_crc32c	crc;

	if (!merkle_has_staged_delta())
		return NULL;

	old_context = MemoryContextSwitchTo(CurrentMemoryContext);
	combined = merkle_delta_create_map(CurrentMemoryContext);
	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		hash_seq_init(&seq, frame->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(combined, entry);
	}

	count = hash_get_num_entries(combined);
	if (count <= 0)
	{
		hash_destroy(combined);
		MemoryContextSwitchTo(old_context);
		return NULL;
	}

	sorted = palloc(sizeof(*sorted) * count);
	hash_seq_init(&seq, combined);
	while ((entry = hash_seq_search(&seq)) != NULL)
		sorted[i++] = *entry;
	Assert(i == count);
	qsort(sorted, count, sizeof(*sorted), merkle_delta_entry_cmp);

	payload_bytes = (Size) count * MERKLE_DELTA_ENTRY_BYTES;
	total_bytes = VARHDRSZ + MERKLE_DELTA_HEADER_BYTES + payload_bytes;
	if (!AllocSizeIsValid(total_bytes) || payload_bytes > PG_UINT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Merkle delta batch is too large")));

	result = palloc0(total_bytes);
	SET_VARSIZE(result, total_bytes);
	header = VARDATA(result);
	payload = header + MERKLE_DELTA_HEADER_BYTES;

	for (i = 0; i < count; i++)
	{
		char *dst = payload + (i * MERKLE_DELTA_ENTRY_BYTES);

		merkle_delta_put_u32(dst + 0, sorted[i].key.index_oid);
		merkle_delta_put_u32(dst + 4, sorted[i].key.index_rnode.spcNode);
		merkle_delta_put_u32(dst + 8, sorted[i].key.index_rnode.dbNode);
		merkle_delta_put_u32(dst + 12, sorted[i].key.index_rnode.relNode);
		merkle_delta_put_u32(dst + 16, (uint32) sorted[i].key.leaf_id);
		merkle_delta_put_u32(dst + 20, MERKLE_VERSION);
		memcpy(dst + 24, sorted[i].xor_delta.data, MERKLE_HASH_BYTES);
	}

	merkle_delta_put_u32(header + 0, MERKLE_DELTA_MAGIC);
	merkle_delta_put_u32(header + 4, MERKLE_DELTA_VERSION);
	merkle_delta_put_u32(header + 8, raft_log_index != 0 ? 1 : 0);
	merkle_delta_put_u32(header + 12, (uint32) count);
	merkle_delta_put_u32(header + 16, (uint32) payload_bytes);
	merkle_delta_put_u64(header + 24, raft_log_index);
	merkle_delta_put_u32(header + 32, item_ordinal);
	merkle_delta_put_u32(header + 36, 0);
	memcpy(crc_header, header, MERKLE_DELTA_HEADER_BYTES);
	memset(crc_header + 20, 0, sizeof(uint32));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, crc_header, sizeof(crc_header));
	COMP_CRC32C(crc, payload, payload_bytes);
	FIN_CRC32C(crc);
	merkle_delta_put_u32(header + 20, (uint32) crc);
	merkle_serialized_entry_count = (uint64) count;
	merkle_serialized_generation = merkle_delta_generation;

	pfree(sorted);
	hash_destroy(combined);
	MemoryContextSwitchTo(old_context);
	return result;
}

void
merkle_mark_staged_delta_persisted(void)
{
	if (!merkle_has_staged_delta())
		return;
	if (merkle_serialized_entry_count == 0 ||
		merkle_serialized_generation != merkle_delta_generation)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Merkle delta changed after terminal serialization"),
				 errdetail("serialized_generation=%llu current_generation=%llu staged_entries=%llu",
						   (unsigned long long) merkle_serialized_generation,
						   (unsigned long long) merkle_delta_generation,
						   (unsigned long long) merkle_staged_entry_count())));
	merkle_staged_delta_persisted = true;
}

static void
merkle_persist_local_delta_impl(void)
{
	bytea	   *blob;
	int			spi_rc;
	bool		isnull;
	Datum		seq_datum;
	uint64		apply_seq;
	bool		deterministic_bcdb_seq;
	Oid			argtypes[3] = {INT8OID, INT4OID, BYTEAOID};
	Datum		values[3];
	char		nulls[3] = {' ', ' ', ' '};
	bool		pushed_snapshot = false;

	if (!merkle_has_staged_delta())
		return;

	if (is_bcdb_worker && activeTx != NULL && activeTx->raft_ledger_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("safe-ledger transaction reached PRE_COMMIT with an unpersisted Merkle delta"),
				 errdetail("raft_log_index=%llu item_ordinal=%u",
						   (unsigned long long) activeTx->raft_log_index,
						   (unsigned) activeTx->raft_item_ordinal)));
	deterministic_bcdb_seq = enable_merkle_index && is_bcdb_worker &&
		activeTx != NULL && !activeTx->raft_ledger_enabled &&
		activeTx->tx_id != BCDBInvalidTid;

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle local delta SPI_connect failed: %d", spi_rc);

	if (deterministic_bcdb_seq)
	{
		/* Direct BCDB transactions already have a replica-agreed contiguous
		 * sequence.  Reuse it so concurrent SERIALIZABLE workers insert distinct
		 * queue keys instead of conflicting on the singleton allocator row. */
		if (activeTx->tx_id < 0 || activeTx->tx_id == PG_INT32_MAX)
			elog(ERROR, "BCDB transaction id cannot be represented as a Merkle sequence");
		apply_seq = (uint64) activeTx->tx_id + 1;
	}
	else
	{
		spi_rc = SPI_execute(
			"UPDATE ariabc_internal.merkle_apply_counter"
			"   SET next_seq = next_seq + 1"
			" WHERE singleton"
			" RETURNING next_seq",
			false, 1);
		if (spi_rc != SPI_OK_UPDATE_RETURNING || SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("Merkle crash-safety state is not initialized"),
					 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));

		seq_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								  1, &isnull);
		if (isnull || DatumGetInt64(seq_datum) <= 0)
			elog(ERROR, "Merkle apply counter returned an invalid sequence");
		apply_seq = (uint64) DatumGetInt64(seq_datum);
	}

	blob = merkle_serialize_staged_delta(0, 0);
	if (blob == NULL)
		elog(ERROR, "Merkle staged delta disappeared during PRE_COMMIT");

	values[0] = Int64GetDatum((int64) apply_seq);
	values[1] = Int32GetDatum(MERKLE_DELTA_VERSION);
	values[2] = PointerGetDatum(blob);
	spi_rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_local_delta"
		"       (apply_seq, delta_version, delta_blob)"
		" VALUES ($1, $2, $3)",
		3, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_INSERT || SPI_processed != 1)
		elog(ERROR, "failed to persist local Merkle delta at sequence %llu",
			 (unsigned long long) apply_seq);

	/*
	 * P0.2: Advance terminal_prefix_seq in the same transaction as the
	 * delta insert.  A local delta row is terminal by definition (it is
	 * committed by this transaction), so as soon as next_seq moves to
	 * apply_seq, the prefix can advance to include it.
	 */
	if (!deterministic_bcdb_seq)
		(void) merkle_advance_terminal_prefix_spi();

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle local delta SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();

	merkle_staged_delta_persisted = true;
	merkle_crash_failpoint("after_merkle_delta_ledger_written");
	merkle_crash_failpoint("after_merkle_delta_queue_written");
}

/* The durable queue is intentionally not writable by PUBLIC.  DML against a
 * Merkle-indexed table still has to work for ordinary table owners, so run
 * this backend-internal SPI mutation under the bootstrap superuser context
 * and restore the caller identity on every path. */
static void
merkle_persist_local_delta(void)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		merkle_persist_local_delta_impl();
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
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
		if (merkle_has_staged_delta() && !merkle_staged_delta_persisted)
			merkle_persist_local_delta();
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
