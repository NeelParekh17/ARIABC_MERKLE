/*-------------------------------------------------------------------------
 *
 * merkleapply.c
 *    Ordered, idempotent, Generic-WAL-backed Merkle delta application.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/merkle.h"
#include "access/xact.h"
#include "bcdb/shm_block.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "catalog/pg_authid_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/acl.h"

PG_FUNCTION_INFO_V1(merkle_recovery_status);
PG_FUNCTION_INFO_V1(merkle_apply_pending_sql);
PG_FUNCTION_INFO_V1(merkle_apply_until_sql);
PG_FUNCTION_INFO_V1(merkle_rebuild_legacy_indexes);

typedef struct MerkleLeafEvent
{
	uint64		seq;
	Oid			index_oid;
	RelFileNode index_rnode;
	int32		leaf_id;
	MerkleHash	delta;
} MerkleLeafEvent;

typedef struct MerkleNodeEvent
{
	uint64		seq;
	Oid			index_oid;
	RelFileNode index_rnode;
	BlockNumber blkno;
	int32		index_in_page;
	MerkleHash	delta;
} MerkleNodeEvent;

typedef struct MerkleEventArray
{
	MerkleLeafEvent *leaf;
	int			nleaf;
	int			leaf_capacity;
	MerkleDynamicTransition *dynamic;
	int			ndynamic;
	int			dynamic_capacity;
	MerkleNodeEvent *node;
	int			nnode;
	int			node_capacity;
} MerkleEventArray;

#define MERKLE_DELTA_V2_KIND_STATIC        1U
#define MERKLE_DELTA_V2_KIND_DYNAMIC       2U
#define MERKLE_DELTA_V2_FLAG_HAS_OLD       (1U << 0)
#define MERKLE_DELTA_V2_FLAG_HAS_NEW       (1U << 1)

static bool merkle_state_relations_exist(void);
static void merkle_parse_delta_blob(bytea *blob, uint64 seq,
									uint64 expected_log_index,
									uint32 expected_item_ordinal,
									bool is_raft,
									MerkleEventArray *events);
static void merkle_apply_leaf_events(MerkleEventArray *events,
									 uint64 batch_end);
static void merkle_apply_xact_callback(XactEvent event, void *arg);
static uint64 merkle_apply_until_impl(uint64 required_seq);

static bool merkle_apply_callback_registered = false;
static bool merkle_apply_state_advanced = false;

static void
merkle_apply_xact_callback(XactEvent event, void *arg)
{
	(void) arg;

	if (event == XACT_EVENT_COMMIT && merkle_apply_state_advanced)
		merkle_crash_failpoint("after_apply_state_commit");

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PARALLEL_ABORT || event == XACT_EVENT_PREPARE)
		merkle_apply_state_advanced = false;
}

static uint32
merkle_get_u32(const char *src)
{
	uint32 value;

	memcpy(&value, src, sizeof(value));
	return pg_ntoh32(value);
}

static uint64
merkle_get_u64(const char *src)
{
	uint64 value;

	memcpy(&value, src, sizeof(value));
	return pg_ntoh64(value);
}

static bool
merkle_bytes_are_zero(const char *src, Size length)
{
	Size i;

	for (i = 0; i < length; i++)
		if ((unsigned char) src[i] != 0)
			return false;
	return true;
}

static void
merkle_append_leaf_event(MerkleEventArray *events,
						  const MerkleLeafEvent *event)
{
	if (events->nleaf >= events->leaf_capacity)
	{
		events->leaf_capacity = events->leaf_capacity == 0 ? 64 :
			events->leaf_capacity * 2;
		events->leaf = events->leaf == NULL ?
			palloc(sizeof(*events->leaf) * events->leaf_capacity) :
			repalloc(events->leaf, sizeof(*events->leaf) * events->leaf_capacity);
	}
	events->leaf[events->nleaf++] = *event;
}

static void
merkle_append_dynamic_event(MerkleEventArray *events,
							const MerkleDynamicTransition *event)
{
	if (events->ndynamic >= events->dynamic_capacity)
	{
		events->dynamic_capacity = events->dynamic_capacity == 0 ? 64 :
			events->dynamic_capacity * 2;
		events->dynamic = events->dynamic == NULL ?
			palloc(sizeof(*events->dynamic) * events->dynamic_capacity) :
			repalloc(events->dynamic,
					   sizeof(*events->dynamic) * events->dynamic_capacity);
	}
	events->dynamic[events->ndynamic++] = *event;
}

static void
merkle_append_node_event(MerkleEventArray *events,
						  const MerkleNodeEvent *event)
{
	if (events->nnode >= events->node_capacity)
	{
		events->node_capacity = events->node_capacity == 0 ? 256 :
			events->node_capacity * 2;
		events->node = events->node == NULL ?
			palloc(sizeof(*events->node) * events->node_capacity) :
			repalloc(events->node, sizeof(*events->node) * events->node_capacity);
	}
	events->node[events->nnode++] = *event;
}

static bool
merkle_state_relations_exist(void)
{
	Oid namespace_oid = get_namespace_oid("ariabc_internal", true);

	if (!OidIsValid(namespace_oid))
		return false;
	return OidIsValid(get_relname_relid("merkle_apply_state", namespace_oid)) &&
		OidIsValid(get_relname_relid("merkle_apply_counter", namespace_oid)) &&
		OidIsValid(get_relname_relid("merkle_local_delta", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_entry", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_entry_item", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_item", namespace_oid));
}

static void
merkle_mark_recovery_state_impl(MerkleRecoveryState state, const char *reason)
{
	Oid argtypes[2] = {INT2OID, TEXTOID};
	Datum values[2];
	char nulls[2] = {' ', ' '};
	int spi_rc;

	if (!merkle_state_relations_exist())
		return;
	values[0] = Int16GetDatum((int16) state);
	values[1] = CStringGetTextDatum(reason ? reason : "");
	if (SPI_connect() != SPI_OK_CONNECT)
		return;
	spi_rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_apply_state"
		"   SET state = $1, error_text = NULLIF($2, ''),"
		"       updated_at = clock_timestamp()"
		" WHERE singleton",
		2, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_UPDATE || SPI_processed != 1)
	{
		(void) SPI_finish();
		return;
	}
	(void) SPI_finish();
}

void
merkle_mark_recovery_state(MerkleRecoveryState state, const char *reason)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		merkle_mark_recovery_state_impl(state, reason);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
}

/*
 * merkle_advance_terminal_prefix_spi() - P0.2: advance terminal_prefix_seq.
 *
 * Must be called inside an SPI session with a writable transaction.
 *
 * Algorithm:
 *   1. Lock the counter row.
 *   2. From terminal_prefix_seq + 1, repeatedly probe both
 *      raft_apply_item (states 2, 3, 4 — all terminal states) and
 *      merkle_local_delta until the first gap.
 *   3. Persist the new prefix and return it.
 *
 * The caller is responsible for ensuring this runs in the same transaction
 * as the delta/state update that makes the position terminal.
 */
uint64
merkle_advance_terminal_prefix_spi(void)
{
	int		spi_rc;
	Datum	datum;
	bool	isnull;
	uint64	current_prefix;
	uint64	new_prefix;
	bool	advanced = false;

	/* Lock the singleton counter row exclusively to serialize prefix updates. */
	spi_rc = SPI_execute(
		"SELECT terminal_prefix_seq FROM ariabc_internal.merkle_apply_counter"
		" WHERE singleton FOR UPDATE",
		false, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "merkle_advance_terminal_prefix: cannot lock counter row");
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull)
		elog(ERROR, "merkle_advance_terminal_prefix: terminal_prefix_seq is NULL");
	current_prefix = (uint64) DatumGetInt64(datum);
	new_prefix = current_prefix;

	/*
	 * Advance in bounded, ordered batches.  The old implementation issued one
	 * SPI query per sequence while holding the singleton row lock, producing a
	 * severe latency spike after a large gap closed.
	 */
	for (;;)
	{
		uint64	next_pos = new_prefix + 1;
		Datum	arg = Int64GetDatum((int64) next_pos);
		Oid		arg_type = INT8OID;
		uint64	batch_start = new_prefix;
		uint64	i;

		/*
		 * A position is terminal if it appears as a finalized Raft item
		 * (state IN (2,3,4) — committed-ok, committed-error,
		 * nonterminal-failure) or as a local delta row (committed by
		 * definition because committed_at is NOT NULL by constraint).
		 */
		spi_rc = SPI_execute_with_args(
			"SELECT seq FROM ("
			"  SELECT merkle_apply_seq AS seq"
			"    FROM ariabc_internal.raft_apply_item"
			"   WHERE merkle_apply_seq >= $1 AND state IN (2, 3, 4)"
			"  UNION"
			"  SELECT apply_seq AS seq"
			"    FROM ariabc_internal.merkle_local_delta"
			"   WHERE apply_seq >= $1"
			") terminal ORDER BY seq LIMIT 1024",
			1, &arg_type, &arg, NULL, true, 1024);
		if (spi_rc != SPI_OK_SELECT)
			elog(ERROR, "merkle_advance_terminal_prefix: terminal batch probe failed");
		for (i = 0; i < SPI_processed; i++)
		{
			uint64 seq;

			datum = SPI_getbinval(SPI_tuptable->vals[i],
								  SPI_tuptable->tupdesc, 1, &isnull);
			if (isnull)
				elog(ERROR, "merkle_advance_terminal_prefix: terminal sequence is NULL");
			seq = (uint64) DatumGetInt64(datum);
			if (seq != new_prefix + 1)
				break;
			new_prefix = seq;
			advanced = true;
		}
		if (new_prefix == batch_start || SPI_processed < 1024)
			break;
	}

	if (advanced)
	{
		Datum	arg = Int64GetDatum((int64) new_prefix);
		Oid	arg_type = INT8OID;

		spi_rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_apply_counter"
			"   SET terminal_prefix_seq = $1"
			" WHERE singleton",
			1, &arg_type, &arg, NULL, false, 1);
		if (spi_rc != SPI_OK_UPDATE)
			elog(ERROR, "merkle_advance_terminal_prefix: UPDATE failed");
	}

	return new_prefix;
}

static bool
merkle_index_page_is_v7(Oid index_oid)
{
	Relation index_rel;
	Buffer buf = InvalidBuffer;
	Page page;
	MerkleMetaPageData *meta;
	BlockNumber nblocks;
	int nodes_per_page = 0;
	int num_tree_pages = 0;
	int total_nodes = 0;
	int page_no;
	bool valid = false;

	index_rel = index_open(index_oid, AccessShareLock);
	if (index_rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
	{
		index_close(index_rel, AccessShareLock);
		return false;
	}
	nblocks = RelationGetNumberOfBlocks(index_rel);
	if (nblocks > MERKLE_METAPAGE_BLKNO)
	{
		buf = ReadBuffer(index_rel, MERKLE_METAPAGE_BLKNO);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (PageIsVerified(page, MERKLE_METAPAGE_BLKNO))
		{
			meta = MerklePageGetMeta(page);
			nodes_per_page = meta->nodesPerPage;
			num_tree_pages = meta->numTreePages;
			total_nodes = meta->totalNodes;
			valid = meta->version == MERKLE_VERSION &&
				meta->routeFormatVersion == MERKLE_ROUTE_FORMAT_VERSION &&
				meta->rowHashFormatVersion == MERKLE_ROW_HASH_FORMAT_VERSION &&
				meta->nodesPerPage > 0 &&
				meta->nodesPerPage <= MERKLE_MAX_NODES_PER_PAGE &&
				meta->numTreePages > 0 &&
				nblocks >= (BlockNumber) (MERKLE_TREE_START_BLKNO +
										 meta->numTreePages);
		}
		UnlockReleaseBuffer(buf);

		/* The metapage version alone is not enough: v7's page watermark and
		 * Generic-WAL-visible node array must be present on every tree page. */
		if (valid)
		{
			for (page_no = 0; page_no < num_tree_pages; page_no++)
			{
				int nodes_this_page = Min(nodes_per_page,
											 total_nodes - page_no * nodes_per_page);

				if (nodes_this_page <= 0)
				{
					valid = false;
					break;
				}
				buf = ReadBuffer(index_rel,
								MERKLE_TREE_START_BLKNO + page_no);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);
				if (!PageIsVerified(page,
									 MERKLE_TREE_START_BLKNO + page_no) ||
					PageGetSpecialSize(page) != MERKLE_PAGE_SPECIAL_SIZE ||
					MerklePageGetOpaque(page)->magic != MERKLE_PAGE_OPAQUE_MAGIC ||
					MerklePageGetOpaque(page)->version != MERKLE_PAGE_OPAQUE_VERSION ||
					((PageHeader) page)->pd_lower <
					(char *) PageGetContents(page) - (char *) page +
					nodes_this_page * (int) sizeof(MerkleNode))
					valid = false;
				UnlockReleaseBuffer(buf);
				if (!valid)
					break;
			}
		}
	}
	index_close(index_rel, AccessShareLock);
	return valid;
}

Datum
merkle_rebuild_legacy_indexes(PG_FUNCTION_ARGS)
{
	Oid *index_oids = NULL;
	int index_count = 0;
	int legacy_count = 0;
	int i;
	int spi_rc;
	Oid argtypes[1] = {OIDOID};
	Datum values[1] = {ObjectIdGetDatum(MERKLE_AM_OID)};
	char nulls[1] = {' '};
	bool pushed_snapshot = false;
	MerkleRecoveryStatusData status;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("merkle_rebuild_legacy_indexes() requires superuser")));
	if (!merkle_state_relations_exist())
		PG_RETURN_INT64(0);

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "Merkle legacy-index scan SPI_connect failed");
	spi_rc = SPI_execute_with_args(
		"SELECT oid FROM pg_catalog.pg_class"
		" WHERE relam = $1 AND relkind IN ('i', 'I')"
		" ORDER BY oid",
		1, argtypes, values, nulls, true, 0);
	if (spi_rc != SPI_OK_SELECT)
		elog(ERROR, "Merkle legacy-index scan failed: %d", spi_rc);
	index_count = (int) SPI_processed;
	if (index_count > 0)
	{
		index_oids = palloc(sizeof(Oid) * index_count);
		for (i = 0; i < index_count; i++)
		{
			bool isnull;
			index_oids[i] = DatumGetObjectId(SPI_getbinval(
				SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull || !OidIsValid(index_oids[i]))
				elog(ERROR, "invalid Merkle index OID in catalog scan");
		}
	}
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle legacy-index scan SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();

	for (i = 0; i < index_count; i++)
		if (!merkle_index_page_is_v7(index_oids[i]))
			legacy_count++;

	/* Normal v7 startup lag is replayed by the applier, not by migration. */
	if (legacy_count == 0)
	{
		if (index_oids != NULL)
			pfree(index_oids);
		PG_RETURN_INT64(0);
	}

	merkle_get_recovery_status(&status);
	if (status.applied_seq != status.target_seq)
	{
		merkle_mark_recovery_state(
			MERKLE_STATE_REBUILD_REQUIRED,
			"legacy Merkle format requires rebuild after committed deltas are applied");
		/* P0.5: guard pfree against NULL when index_count==0 */
		if (index_oids != NULL)
			pfree(index_oids);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("legacy Merkle indexes cannot be rebuilt while recovery is behind"),
				 errdetail("applied_seq=%llu target_seq=%llu",
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq)));
	}

	/* Allow merkleBuild() to use the already-applied heap snapshot. */
	merkle_mark_recovery_state(MERKLE_STATE_READY, NULL);
	for (i = 0; i < index_count; i++)
	{
		if (merkle_index_page_is_v7(index_oids[i]))
			continue;
		reindex_index(index_oids[i], true, RELPERSISTENCE_PERMANENT, 0);
		if (!merkle_index_page_is_v7(index_oids[i]))
		{
			Oid failed_index = index_oids[i];

			merkle_mark_recovery_state(MERKLE_STATE_REBUILD_REQUIRED,
									   "Merkle index rebuild did not produce v7 metadata");
			pfree(index_oids);
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle index %u failed v7 rebuild validation", failed_index)));
		}
	}
	/* Metadata validation is necessary but not sufficient: audit every
	 * rebuilt/current Merkle tree against its heap before declaring startup
	 * READY.  Use the index-specific API so all indexes are verified.
	 * This runs only during explicit migration/startup, never on the
	 * synchronous DML path. */
	{
		bool verify_ok;
		bool verify_null;

		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushed_snapshot = true;
		}
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "Merkle rebuild verification SPI_connect failed");
		/*
		 * P0.6 fix: use merkle_verify_index(i.indexrelid) so every Merkle
		 * index on each table is verified individually, not just the first.
		 */
		spi_rc = SPI_execute(
			"SELECT COALESCE(bool_and(pg_catalog.merkle_verify_index(i.indexrelid)), true)"
			"  FROM pg_catalog.pg_index i"
			"  JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid"
			"  JOIN pg_catalog.pg_am am ON am.oid = c.relam"
			" WHERE am.amname = 'merkle'",
			true, 1);
		if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
			elog(ERROR, "Merkle rebuild verification query failed");
		verify_ok = DatumGetBool(SPI_getbinval(
			SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &verify_null));
		if (verify_null || !verify_ok)
		{
			(void) SPI_finish();
			if (pushed_snapshot)
				PopActiveSnapshot();
			merkle_mark_recovery_state(MERKLE_STATE_INVALID,
									   "Merkle verification failed after legacy-index rebuild");
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle verification failed after legacy-index rebuild")));
		}
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "Merkle rebuild verification SPI_finish failed");
		if (pushed_snapshot)
			PopActiveSnapshot();
	}
	/* P0.5: guard pfree against NULL when index_count==0 */
	if (index_oids != NULL)
		pfree(index_oids);
	merkle_mark_recovery_state(MERKLE_STATE_READY, NULL);
	PG_RETURN_INT64(legacy_count);
}

static void
merkle_parse_delta_blob(bytea *blob, uint64 seq, uint64 expected_log_index,
						uint32 expected_item_ordinal, bool is_raft,
						MerkleEventArray *events)
{
	const char *header;
	const char *payload;
	int			blob_len;
	uint32		magic;
	uint32		version;
	uint32		entry_count;
	uint32		payload_len;
	uint32		stored_crc;
	uint64		raft_log_index;
	uint32		item_ordinal;
	uint32		flags;
	uint32		i;
	pg_crc32c	crc;
	char		crc_header[MERKLE_DELTA_HEADER_BYTES];

	if (blob == NULL)
		elog(ERROR, "Merkle delta sequence %llu has a NULL blob",
			 (unsigned long long) seq);

	blob = DatumGetByteaPP(PointerGetDatum(blob));
	blob_len = VARSIZE_ANY_EXHDR(blob);
	if (blob_len < MERKLE_DELTA_HEADER_BYTES)
		elog(ERROR, "Merkle delta sequence %llu is truncated",
			 (unsigned long long) seq);

	header = VARDATA_ANY(blob);
	magic = merkle_get_u32(header + 0);
	version = merkle_get_u32(header + 4);
	entry_count = merkle_get_u32(header + 12);
	payload_len = merkle_get_u32(header + 16);
	stored_crc = merkle_get_u32(header + 20);
	raft_log_index = merkle_get_u64(header + 24);
	item_ordinal = merkle_get_u32(header + 32);
	flags = merkle_get_u32(header + 8);

	if (magic != MERKLE_DELTA_MAGIC ||
		(version != MERKLE_DELTA_LEGACY_VERSION &&
		 version != MERKLE_DELTA_VERSION))
		elog(ERROR,
			 "Merkle delta sequence %llu has unsupported magic/version 0x%08x/%u",
			 (unsigned long long) seq, magic, version);
	if ((uint64) MERKLE_DELTA_HEADER_BYTES + payload_len != (uint64) blob_len ||
		(version == MERKLE_DELTA_LEGACY_VERSION &&
		 (uint64) entry_count * MERKLE_DELTA_ENTRY_BYTES != payload_len))
		elog(ERROR,
			 "Merkle delta sequence %llu has invalid length/count metadata",
			 (unsigned long long) seq);
	if (entry_count == 0)
		elog(ERROR, "Merkle delta sequence %llu stores an empty batch",
			 (unsigned long long) seq);

	if (merkle_get_u32(header + 36) != 0 || (flags & ~1U) != 0)
		elog(ERROR, "Merkle delta sequence %llu has unsupported header flags",
			 (unsigned long long) seq);
	if (is_raft)
	{
		if (raft_log_index != expected_log_index ||
			item_ordinal != expected_item_ordinal || flags != 1)
			elog(ERROR,
				 "Merkle delta sequence %llu is bound to the wrong Raft item",
				 (unsigned long long) seq);
	}
	else if (raft_log_index != 0 || item_ordinal != 0 || flags != 0)
		elog(ERROR, "local Merkle delta sequence %llu has a Raft identity",
			 (unsigned long long) seq);

	memcpy(crc_header, header, MERKLE_DELTA_HEADER_BYTES);
	memset(crc_header + 20, 0, sizeof(uint32));
	payload = header + MERKLE_DELTA_HEADER_BYTES;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, crc_header, sizeof(crc_header));
	COMP_CRC32C(crc, payload, payload_len);
	FIN_CRC32C(crc);
	if ((uint32) crc != stored_crc)
		elog(ERROR,
			 "Merkle delta sequence %llu failed CRC32C validation",
			 (unsigned long long) seq);

	if (version == MERKLE_DELTA_LEGACY_VERSION)
	{
		Oid previous_oid = InvalidOid;
		RelFileNode previous_rnode;
		int32 previous_leaf = 0;
		bool have_previous_key = false;

		for (i = 0; i < entry_count; i++)
		{
			const char *src = payload + ((Size) i * MERKLE_DELTA_ENTRY_BYTES);
			MerkleLeafEvent event;
			uint32 format_version;

			MemSet(&event, 0, sizeof(event));
			event.seq = seq;
			event.index_oid = (Oid) merkle_get_u32(src + 0);
			event.index_rnode.spcNode = (Oid) merkle_get_u32(src + 4);
			event.index_rnode.dbNode = (Oid) merkle_get_u32(src + 8);
			event.index_rnode.relNode = (Oid) merkle_get_u32(src + 12);
			event.leaf_id = (int32) merkle_get_u32(src + 16);
			format_version = merkle_get_u32(src + 20);
			memcpy(event.delta.data, src + 24, MERKLE_HASH_BYTES);

			if (!OidIsValid(event.index_oid) || format_version != MERKLE_VERSION)
				elog(ERROR,
					 "Merkle delta sequence %llu references invalid index %u or format %u",
					 (unsigned long long) seq, event.index_oid, format_version);
			if (have_previous_key)
			{
				bool out_of_order = false;

				if (event.index_oid < previous_oid)
					out_of_order = true;
				else if (event.index_oid == previous_oid &&
						 event.index_rnode.spcNode < previous_rnode.spcNode)
					out_of_order = true;
				else if (event.index_oid == previous_oid &&
						 event.index_rnode.spcNode == previous_rnode.spcNode &&
						 event.index_rnode.dbNode < previous_rnode.dbNode)
					out_of_order = true;
				else if (event.index_oid == previous_oid &&
						 event.index_rnode.spcNode == previous_rnode.spcNode &&
						 event.index_rnode.dbNode == previous_rnode.dbNode &&
						 event.index_rnode.relNode < previous_rnode.relNode)
					out_of_order = true;
				else if (event.index_oid == previous_oid &&
						 event.index_rnode.spcNode == previous_rnode.spcNode &&
						 event.index_rnode.dbNode == previous_rnode.dbNode &&
						 event.index_rnode.relNode == previous_rnode.relNode &&
						 event.leaf_id <= previous_leaf)
					out_of_order = true;

				if (out_of_order)
					ereport(ERROR,
							(errmsg("Merkle delta sequence %llu has non-canonical or duplicate entries",
									(unsigned long long) seq)));
			}
			previous_oid = event.index_oid;
			previous_rnode = event.index_rnode;
			previous_leaf = event.leaf_id;
			have_previous_key = true;
			if (!merkle_hash_is_zero(&event.delta))
				merkle_append_leaf_event(events, &event);
		}
	}
	else
	{
		Size offset = 0;
		Oid previous_oid = InvalidOid;
		RelFileNode previous_rnode;
		uint32 previous_kind = 0;
		uint32 previous_target = 0;
		const char *previous_route = NULL;
		const char *previous_key = NULL;
		uint32 previous_key_len = 0;
		bool have_previous_key = false;

		MemSet(&previous_rnode, 0, sizeof(previous_rnode));

		for (i = 0; i < entry_count; i++)
		{
			const char *src;
			uint32 entry_bytes;
			uint32 kind;
			Oid index_oid;
			RelFileNode index_rnode;
			uint32 format_version;
			uint32 target;
			uint32 entry_flags;
			uint32 key_len;
			const char *route;
			const char *key;
			bool out_of_order = false;

			if (payload_len - offset < MERKLE_DELTA_V2_ENTRY_FIXED_BYTES)
				elog(ERROR, "Merkle delta sequence %llu has a truncated v2 entry",
					 (unsigned long long) seq);
			src = payload + offset;
			entry_bytes = merkle_get_u32(src + 0);
			kind = merkle_get_u32(src + 4);
			index_oid = (Oid) merkle_get_u32(src + 8);
			index_rnode.spcNode = (Oid) merkle_get_u32(src + 12);
			index_rnode.dbNode = (Oid) merkle_get_u32(src + 16);
			index_rnode.relNode = (Oid) merkle_get_u32(src + 20);
			format_version = merkle_get_u32(src + 24);
			target = merkle_get_u32(src + 28);
			entry_flags = merkle_get_u32(src + 32);
			key_len = merkle_get_u32(src + 36);
			route = src + 40;
			key = src + MERKLE_DELTA_V2_ENTRY_FIXED_BYTES;

			if ((uint64) entry_bytes !=
				(uint64) MERKLE_DELTA_V2_ENTRY_FIXED_BYTES + key_len ||
				entry_bytes > payload_len - offset)
				elog(ERROR, "Merkle delta sequence %llu has invalid v2 entry length",
					 (unsigned long long) seq);
			if (!OidIsValid(index_oid) || format_version != MERKLE_VERSION ||
				(kind != MERKLE_DELTA_V2_KIND_STATIC &&
				 kind != MERKLE_DELTA_V2_KIND_DYNAMIC))
				elog(ERROR,
					 "Merkle delta sequence %llu references invalid v2 index, format, or kind",
					 (unsigned long long) seq);
			if (target > PG_INT32_MAX ||
				(kind == MERKLE_DELTA_V2_KIND_STATIC &&
				 (key_len != 0 || entry_flags != 0 ||
				  !merkle_bytes_are_zero(src + 40, 96))) ||
				(kind == MERKLE_DELTA_V2_KIND_DYNAMIC &&
				 (key_len == 0 || key_len > MERKLE_DYNAMIC_MAX_KEY_BYTES ||
				  entry_flags == 0 ||
				  (entry_flags & ~(MERKLE_DELTA_V2_FLAG_HAS_OLD |
								 MERKLE_DELTA_V2_FLAG_HAS_NEW)) != 0 ||
				  (!(entry_flags & MERKLE_DELTA_V2_FLAG_HAS_OLD) &&
				   !merkle_bytes_are_zero(src + 72, MERKLE_HASH_BYTES)) ||
				  (!(entry_flags & MERKLE_DELTA_V2_FLAG_HAS_NEW) &&
				   !merkle_bytes_are_zero(src + 104, MERKLE_HASH_BYTES)) ||
				  !merkle_bytes_are_zero(src + 136,
								MERKLE_HASH_BYTES))))
				elog(ERROR, "Merkle delta sequence %llu violates the v2 entry contract",
					 (unsigned long long) seq);

			if (have_previous_key)
			{
				if (index_oid < previous_oid)
					out_of_order = true;
				else if (index_oid == previous_oid &&
						 index_rnode.spcNode < previous_rnode.spcNode)
					out_of_order = true;
				else if (index_oid == previous_oid &&
						 index_rnode.spcNode == previous_rnode.spcNode &&
						 index_rnode.dbNode < previous_rnode.dbNode)
					out_of_order = true;
				else if (index_oid == previous_oid &&
						 index_rnode.spcNode == previous_rnode.spcNode &&
						 index_rnode.dbNode == previous_rnode.dbNode &&
						 index_rnode.relNode < previous_rnode.relNode)
					out_of_order = true;
				else if (index_oid == previous_oid &&
						 RelFileNodeEquals(index_rnode, previous_rnode))
				{
					if (kind < previous_kind)
						out_of_order = true;
					else if (kind == previous_kind)
					{
						if (target < previous_target)
							out_of_order = true;
						else if (target == previous_target)
						{
							int cmp = kind == MERKLE_DELTA_V2_KIND_STATIC ? 0 :
								memcmp(route, previous_route, MERKLE_HASH_BYTES);

							if (cmp < 0)
								out_of_order = true;
							else if (cmp == 0)
							{
								uint32 common = Min(key_len, previous_key_len);

								cmp = common == 0 ? 0 :
									memcmp(key, previous_key, common);
								if (cmp < 0 || (cmp == 0 && key_len <= previous_key_len))
									out_of_order = true;
							}
						}
					}
				}
				if (out_of_order)
					ereport(ERROR,
							(errmsg("Merkle delta sequence %llu has non-canonical or duplicate v2 entries",
									(unsigned long long) seq)));
			}

			if (kind == MERKLE_DELTA_V2_KIND_STATIC)
			{
				MerkleLeafEvent event;

				MemSet(&event, 0, sizeof(event));
				event.seq = seq;
				event.index_oid = index_oid;
				event.index_rnode = index_rnode;
				event.leaf_id = (int32) target;
				memcpy(event.delta.data, src + 136, MERKLE_HASH_BYTES);
				if (!merkle_hash_is_zero(&event.delta))
					merkle_append_leaf_event(events, &event);
			}
			else
			{
				MerkleDynamicTransition event;
				bytea *key_data;

				MemSet(&event, 0, sizeof(event));
				event.seq = seq;
				event.index_oid = index_oid;
				event.index_rnode = index_rnode;
				event.partition_id = (int32) target;
				memcpy(event.route_digest, route, MERKLE_HASH_BYTES);
				event.has_old =
					(entry_flags & MERKLE_DELTA_V2_FLAG_HAS_OLD) != 0;
				event.has_new =
					(entry_flags & MERKLE_DELTA_V2_FLAG_HAS_NEW) != 0;
				if (event.has_old)
					memcpy(event.old_hash.data, src + 72, MERKLE_HASH_BYTES);
				if (event.has_new)
					memcpy(event.new_hash.data, src + 104, MERKLE_HASH_BYTES);
				if (!AllocSizeIsValid((Size) VARHDRSZ + key_len))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("dynamic Merkle key in delta is too large")));
				key_data = palloc(VARHDRSZ + key_len);
				SET_VARSIZE(key_data, VARHDRSZ + key_len);
				memcpy(VARDATA(key_data), key, key_len);
				event.key_data = key_data;
				merkle_append_dynamic_event(events, &event);
			}

			previous_oid = index_oid;
			previous_rnode = index_rnode;
			previous_kind = kind;
			previous_target = target;
			previous_route = route;
			previous_key = key;
			previous_key_len = key_len;
			have_previous_key = true;
			offset += entry_bytes;
		}
		if (offset != payload_len)
			elog(ERROR, "Merkle delta sequence %llu has trailing v2 payload bytes",
				 (unsigned long long) seq);
	}
}

static int
merkle_leaf_event_cmp(const void *left, const void *right)
{
	const MerkleLeafEvent *a = (const MerkleLeafEvent *) left;
	const MerkleLeafEvent *b = (const MerkleLeafEvent *) right;

	if (a->index_oid != b->index_oid)
		return a->index_oid < b->index_oid ? -1 : 1;
	if (a->index_rnode.spcNode != b->index_rnode.spcNode)
		return a->index_rnode.spcNode < b->index_rnode.spcNode ? -1 : 1;
	if (a->index_rnode.dbNode != b->index_rnode.dbNode)
		return a->index_rnode.dbNode < b->index_rnode.dbNode ? -1 : 1;
	if (a->index_rnode.relNode != b->index_rnode.relNode)
		return a->index_rnode.relNode < b->index_rnode.relNode ? -1 : 1;
	if (a->seq != b->seq)
		return a->seq < b->seq ? -1 : 1;
	if (a->leaf_id != b->leaf_id)
		return a->leaf_id < b->leaf_id ? -1 : 1;
	return 0;
}

static int
merkle_node_event_cmp(const void *left, const void *right)
{
	const MerkleNodeEvent *a = (const MerkleNodeEvent *) left;
	const MerkleNodeEvent *b = (const MerkleNodeEvent *) right;

	if (a->index_oid != b->index_oid)
		return a->index_oid < b->index_oid ? -1 : 1;
	if (a->index_rnode.relNode != b->index_rnode.relNode)
		return a->index_rnode.relNode < b->index_rnode.relNode ? -1 : 1;
	if (a->blkno != b->blkno)
		return a->blkno < b->blkno ? -1 : 1;
	if (a->seq != b->seq)
		return a->seq < b->seq ? -1 : 1;
	if (a->index_in_page != b->index_in_page)
		return a->index_in_page < b->index_in_page ? -1 : 1;
	return 0;
}

static int
merkle_dynamic_event_cmp(const void *left, const void *right)
{
	const MerkleDynamicTransition *a =
		(const MerkleDynamicTransition *) left;
	const MerkleDynamicTransition *b =
		(const MerkleDynamicTransition *) right;
	int cmp;
	Size a_len;
	Size b_len;

	if (a->seq != b->seq)
		return a->seq < b->seq ? -1 : 1;
	if (a->index_oid != b->index_oid)
		return a->index_oid < b->index_oid ? -1 : 1;
	if (a->index_rnode.spcNode != b->index_rnode.spcNode)
		return a->index_rnode.spcNode < b->index_rnode.spcNode ? -1 : 1;
	if (a->index_rnode.dbNode != b->index_rnode.dbNode)
		return a->index_rnode.dbNode < b->index_rnode.dbNode ? -1 : 1;
	if (a->index_rnode.relNode != b->index_rnode.relNode)
		return a->index_rnode.relNode < b->index_rnode.relNode ? -1 : 1;
	if (a->partition_id != b->partition_id)
		return a->partition_id < b->partition_id ? -1 : 1;
	cmp = memcmp(a->route_digest, b->route_digest, MERKLE_HASH_BYTES);
	if (cmp != 0)
		return cmp;
	a_len = VARSIZE_ANY_EXHDR(a->key_data);
	b_len = VARSIZE_ANY_EXHDR(b->key_data);
	cmp = memcmp(VARDATA_ANY(a->key_data), VARDATA_ANY(b->key_data),
				 Min(a_len, b_len));
	if (cmp != 0)
		return cmp;
	if (a_len != b_len)
		return a_len < b_len ? -1 : 1;
	return 0;
}

static void
merkle_apply_dynamic_events(MerkleEventArray *events)
{
	int i;

	if (events->ndynamic == 0)
		return;
	qsort(events->dynamic, events->ndynamic, sizeof(*events->dynamic),
		  merkle_dynamic_event_cmp);
	for (i = 0; i < events->ndynamic; i++)
	{
		merkle_crash_failpoint("before_dynamic_transition");
		merkle_dynamic_apply_transition(&events->dynamic[i]);
		merkle_crash_failpoint("after_dynamic_transition");
	}
}

static void
merkle_expand_leaf_events(MerkleEventArray *events)
{
	int i = 0;

	if (events->nleaf == 0)
		return;
	qsort(events->leaf, events->nleaf, sizeof(*events->leaf),
		  merkle_leaf_event_cmp);

	while (i < events->nleaf)
	{
		int			group_end = i + 1;
		Relation	indexRel;
		MerkleGeometry geometry;
		int			nodes_per_page;
		int			num_tree_pages;

		while (group_end < events->nleaf &&
			   events->leaf[group_end].index_oid == events->leaf[i].index_oid &&
			   RelFileNodeEquals(events->leaf[group_end].index_rnode,
							 events->leaf[i].index_rnode))
			group_end++;

		indexRel = index_open(events->leaf[i].index_oid, RowExclusiveLock);
		if (indexRel->rd_rel->relam != MERKLE_AM_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle delta references non-Merkle relation %u",
							events->leaf[i].index_oid)));
		if (!RelFileNodeEquals(indexRel->rd_node, events->leaf[i].index_rnode))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle delta relation identity changed for index %u; catch up before REINDEX/DROP",
							events->leaf[i].index_oid)));

		merkle_geometry_from_index(indexRel, &geometry);
		merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
						 &nodes_per_page, &num_tree_pages, NULL);

		for (; i < group_end; i++)
		{
			int partition;
			int node_in_partition;

			if (events->leaf[i].leaf_id < 0 ||
				events->leaf[i].leaf_id >= geometry.total_leaves)
				elog(ERROR,
					 "Merkle delta sequence %llu has out-of-range leaf %d for index %u",
					 (unsigned long long) events->leaf[i].seq,
					 events->leaf[i].leaf_id, events->leaf[i].index_oid);

			partition = events->leaf[i].leaf_id / geometry.leaves_per_partition;
			node_in_partition = merkle_geometry_leaf_node(&geometry,
												 events->leaf[i].leaf_id);
			while (node_in_partition > 0)
			{
				int actual_index = merkle_geometry_global_node(&geometry,
													 partition,
													 node_in_partition);
				int page_number = actual_index / nodes_per_page;
				MerkleNodeEvent node_event;

				if (actual_index < 0 || actual_index >= geometry.total_nodes ||
					page_number < 0 || page_number >= num_tree_pages)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("Merkle geometry expansion failed for index %u",
								 events->leaf[i].index_oid)));

				MemSet(&node_event, 0, sizeof(node_event));
				node_event.seq = events->leaf[i].seq;
				node_event.index_oid = events->leaf[i].index_oid;
				node_event.index_rnode = events->leaf[i].index_rnode;
				node_event.blkno = MERKLE_TREE_START_BLKNO + page_number;
				node_event.index_in_page = actual_index % nodes_per_page;
				node_event.delta = events->leaf[i].delta;
				merkle_append_node_event(events, &node_event);

				node_in_partition = merkle_geometry_parent_node(&geometry,
														 node_in_partition);
			}
		}

		/* Keep the DDL-conflicting lock until transaction end. */
		index_close(indexRel, NoLock);
	}
}

static void
merkle_apply_leaf_events(MerkleEventArray *events, uint64 batch_end)
{
	int i = 0;
	Oid current_oid = InvalidOid;
	Relation indexRel = NULL;
	int nodes_per_page = 0;

	merkle_expand_leaf_events(events);
	if (events->nnode == 0)
		return;
	qsort(events->node, events->nnode, sizeof(*events->node),
		  merkle_node_event_cmp);

	while (i < events->nnode)
	{
		int group_end = i + 1;
		Buffer buffer;
		Page page;
		MerklePageOpaqueData *opaque;
		uint64 page_position;

		while (group_end < events->nnode &&
			   events->node[group_end].index_oid == events->node[i].index_oid &&
			   RelFileNodeEquals(events->node[group_end].index_rnode,
							 events->node[i].index_rnode) &&
			   events->node[group_end].blkno == events->node[i].blkno)
			group_end++;

		if (current_oid != events->node[i].index_oid)
		{
			if (indexRel != NULL)
				index_close(indexRel, NoLock);
			current_oid = events->node[i].index_oid;
			indexRel = index_open(current_oid, NoLock);
			if (!RelFileNodeEquals(indexRel->rd_node,
							   events->node[i].index_rnode))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("Merkle relation identity changed during apply for index %u",
								current_oid)));
			merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
							 &nodes_per_page, NULL, NULL);
		}

		merkle_crash_failpoint("before_applier_page");
		buffer = ReadBuffer(indexRel, events->node[i].blkno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (PageGetSpecialSize(page) != MERKLE_PAGE_SPECIAL_SIZE)
		{
			UnlockReleaseBuffer(buffer);
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle index %u page %u has no v7 crash-recovery opaque area",
							current_oid, events->node[i].blkno)));
		}
		opaque = MerklePageGetOpaque(page);
		if (opaque->magic != MERKLE_PAGE_OPAQUE_MAGIC ||
			opaque->version != MERKLE_PAGE_OPAQUE_VERSION)
		{
			UnlockReleaseBuffer(buffer);
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle index %u page %u has invalid v7 opaque metadata",
							current_oid, events->node[i].blkno)));
		}
		if (((PageHeader) page)->pd_lower <
			(char *) PageGetContents(page) - (char *) page +
			nodes_per_page * sizeof(MerkleNode))
		{
			UnlockReleaseBuffer(buffer);
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle index %u page %u does not mark its node array as WAL-visible",
							current_oid, events->node[i].blkno)));
		}
		page_position = opaque->last_applied_seq;

		if (page_position < batch_end)
		{
			MerkleHash *deltas = palloc0(sizeof(*deltas) * nodes_per_page);
			bool *touched = palloc0(sizeof(*touched) * nodes_per_page);
			GenericXLogState *state;
			Page target_page;
			MerkleNode *target_nodes;
			MerklePageOpaqueData *target_opaque;
			int j;

			for (j = i; j < group_end; j++)
			{
				int index_in_page = events->node[j].index_in_page;

				if (index_in_page < 0 || index_in_page >= nodes_per_page)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("Merkle page event has invalid node slot %d",
									 index_in_page)));
				if (events->node[j].seq <= page_position)
					continue;
				merkle_hash_xor(&deltas[index_in_page], &events->node[j].delta);
				touched[index_in_page] = true;
			}

			state = GenericXLogStart(indexRel);
			target_page = GenericXLogRegisterBuffer(state, buffer, 0);
			target_nodes = (MerkleNode *) PageGetContents(target_page);
			target_opaque = MerklePageGetOpaque(target_page);
			for (j = 0; j < nodes_per_page; j++)
				if (touched[j] && !merkle_hash_is_zero(&deltas[j]))
					merkle_hash_xor(&target_nodes[j].hash, &deltas[j]);
			target_opaque->last_applied_seq = batch_end;
			(void) GenericXLogFinish(state);

			pfree(touched);
			pfree(deltas);
		}

		UnlockReleaseBuffer(buffer);
		merkle_crash_failpoint("after_applier_page");
		i = group_end;
	}

	if (indexRel != NULL)
		index_close(indexRel, NoLock);
}

static void
merkle_free_events(MerkleEventArray *events)
{
	int i;

	if (events->leaf != NULL)
		pfree(events->leaf);
	if (events->dynamic != NULL)
	{
		for (i = 0; i < events->ndynamic; i++)
			if (events->dynamic[i].key_data != NULL)
				pfree(events->dynamic[i].key_data);
		pfree(events->dynamic);
	}
	if (events->node != NULL)
		pfree(events->node);
	MemSet(events, 0, sizeof(*events));
}

uint64
merkle_raft_apply_target(const uint8 *epoch_id, uint64 raft_log_index,
						 uint32 item_ordinal)
{
	Oid argtypes[3] = {BYTEAOID, INT8OID, INT4OID};
	Datum values[3];
	char nulls[3] = {' ', ' ', ' '};
	bytea *epoch;
	bool isnull;
	Datum target_datum;
	uint64 target;
	int spi_rc;

	if (epoch_id == NULL || raft_log_index == 0)
		return 0;
	if (!merkle_state_relations_exist())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("Merkle crash-safety state is not initialized")));

	epoch = palloc(VARHDRSZ + 32);
	SET_VARSIZE(epoch, VARHDRSZ + 32);
	memcpy(VARDATA(epoch), epoch_id, 32);
	values[0] = PointerGetDatum(epoch);
	values[1] = Int64GetDatum((int64) raft_log_index);
	values[2] = Int32GetDatum((int32) item_ordinal);

	PushActiveSnapshot(GetLatestSnapshot());
	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle target SPI_connect failed: %d", spi_rc);
	spi_rc = SPI_execute_with_args(
		"SELECT merkle_apply_seq_base + $3::bigint"
		"  FROM ariabc_internal.raft_apply_entry"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND $3 >= 0"
		"   AND $3 < expected_items",
		3, argtypes, values, nulls, true, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR,
			 "cannot resolve Merkle apply sequence for raft log=%llu ordinal=%u",
			 (unsigned long long) raft_log_index, (unsigned) item_ordinal);
	target_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								 1, &isnull);
	if (isnull)
		elog(ERROR, "resolved Merkle Raft target is NULL");
	target = (uint64) DatumGetInt64(target_datum);
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle target SPI_finish failed");
	PopActiveSnapshot();
	pfree(epoch);
	return target;
}

static uint64
merkle_apply_until_impl(uint64 required_seq)
{
	static const char *source_sql =
		"SELECT apply_seq, source_state, delta_version, delta_blob,"
		"       raft_log_index, item_ordinal, is_raft"
		"  FROM ("
		"    SELECT a.merkle_apply_seq AS apply_seq, a.state AS source_state,"
		"           a.merkle_delta_version AS delta_version,"
		"           a.merkle_delta_blob AS delta_blob,"
		"           a.raft_log_index AS raft_log_index,"
		"           a.item_ordinal AS item_ordinal, true AS is_raft"
		"      FROM ariabc_internal.raft_apply_item a"
		"     WHERE a.merkle_apply_seq > $1"
		"       AND a.merkle_apply_seq <= $2"
		"    UNION ALL"
		"    SELECT l.apply_seq, 2::smallint, l.delta_version, l.delta_blob,"
		"           0::bigint, 0::integer, false"
		"      FROM ariabc_internal.merkle_local_delta l"
		"     WHERE l.apply_seq > $1"
		"       AND l.apply_seq <= $2"
		"  ) sources"
		" ORDER BY apply_seq"
		" LIMIT $3";
	bool pushed_snapshot = false;
	int spi_rc;
	uint64 applied_seq;
	int16 stored_state;
	bool isnull;
	bool made_progress = false;

	merkle_crash_failpoint("during_startup_catchup");

	/* The caller must not silently operate without the durable queue tables. */
	if (!merkle_state_relations_exist())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("Merkle crash-safety state is not initialized"),
				 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));

	PushActiveSnapshot(GetLatestSnapshot());
	pushed_snapshot = true;
	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle applier SPI_connect failed: %d", spi_rc);

	spi_rc = SPI_execute(
		"SELECT applied_seq, state"
		"  FROM ariabc_internal.merkle_apply_state"
		" WHERE singleton"
		" FOR UPDATE",
		false, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "Merkle apply-state singleton is missing");
	applied_seq = (uint64) DatumGetInt64(
		SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		elog(ERROR, "Merkle apply-state applied_seq is NULL");
	stored_state = DatumGetInt16(
		SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
	if (isnull || stored_state == MERKLE_STATE_INVALID ||
		stored_state == MERKLE_STATE_REBUILD_REQUIRED)
		elog(ERROR, "Merkle apply-state is not recoverable (state=%d)",
				 (int) stored_state);

	for (;;)
	{
		Oid argtypes[3] = {INT8OID, INT8OID, INT4OID};
		Datum values[3];
		char nulls[3] = {' ', ' ', ' '};
		uint64 expected_seq = applied_seq + 1;
		uint64 batch_end = applied_seq;
		uint64 batch_bytes = 0;
		uint64 batch_page_budget = 0;
		instr_time batch_start;
		MerkleEventArray events;
		uint64 row;
		int64 upper_bound = required_seq > (uint64) PG_INT64_MAX ?
			PG_INT64_MAX : (int64) required_seq;

		if (required_seq != PG_UINT64_MAX && applied_seq >= required_seq)
			break;
		values[0] = Int64GetDatum((int64) applied_seq);
		values[1] = Int64GetDatum(upper_bound);
		values[2] = Int32GetDatum(merkle_apply_batch_items);
		MemSet(&events, 0, sizeof(events));
		spi_rc = SPI_execute_with_args(source_sql, 3, argtypes, values, nulls,
									   true, merkle_apply_batch_items);
		if (spi_rc != SPI_OK_SELECT)
			elog(ERROR, "Merkle applier source query failed: %d", spi_rc);
		if (SPI_processed == 0)
			break;
		/* The time budget bounds batch parsing/application work.  Charging the
		 * source query against a 1ms default made every query consume its own
		 * budget and reduced large catch-up runs to one row per SPI round trip. */
		INSTR_TIME_SET_CURRENT(batch_start);

		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple tuple;
			TupleDesc tupdesc;
			Datum seq_d;
			Datum state_d;
			Datum version_d;
			Datum blob_d;
			Datum log_d;
			Datum ordinal_d;
			Datum raft_d;
			bool seq_null;
			bool state_null;
			bool version_null;
			bool blob_null;
			bool log_null;
			bool ordinal_null;
			bool raft_null;
			uint64 source_seq;
			int16 source_state;
			int delta_version;
			uint64 expected_log_index;
			uint32 expected_item_ordinal;
			bool is_raft;
			Size blob_bytes = 0;
			uint32 delta_entry_count = 0;

			if (batch_end != applied_seq)
			{
				instr_time now;
				instr_time elapsed;

				INSTR_TIME_SET_CURRENT(now);
				elapsed = now;
				INSTR_TIME_SUBTRACT(elapsed, batch_start);
				if (INSTR_TIME_GET_MICROSEC(elapsed) >=
					(uint64) merkle_apply_batch_time_ms * 1000)
					break;
			}
			tuple = SPI_tuptable->vals[row];
			tupdesc = SPI_tuptable->tupdesc;

			seq_d = SPI_getbinval(tuple, tupdesc, 1, &seq_null);
			state_d = SPI_getbinval(tuple, tupdesc, 2, &state_null);
			version_d = SPI_getbinval(tuple, tupdesc, 3, &version_null);
			blob_d = SPI_getbinval(tuple, tupdesc, 4, &blob_null);
			log_d = SPI_getbinval(tuple, tupdesc, 5, &log_null);
			ordinal_d = SPI_getbinval(tuple, tupdesc, 6, &ordinal_null);
			raft_d = SPI_getbinval(tuple, tupdesc, 7, &raft_null);
			if (seq_null || state_null || version_null || log_null ||
				ordinal_null || raft_null)
				elog(ERROR, "Merkle apply source contains NULL ordering metadata");

			source_seq = (uint64) DatumGetInt64(seq_d);
			source_state = DatumGetInt16(state_d);
			delta_version = DatumGetInt32(version_d);
			expected_log_index = (uint64) DatumGetInt64(log_d);
			expected_item_ordinal = (uint32) DatumGetInt32(ordinal_d);
			is_raft = DatumGetBool(raft_d);

			/* A claimed item or an unmaterialized range is normally a prefix gap.
			 * Direct deterministic local deltas are the one exception: apply_seq is
			 * txid+1, so committed read-only txids deliberately have no queue row.
			 * Cross such a gap only when BCDB's contiguous committed watermark proves
			 * every missing txid terminal.  Raft items never use this exception. */
			if (source_seq < expected_seq)
				elog(ERROR, "Merkle apply source regressed from %llu to %llu",
					 (unsigned long long) expected_seq,
					 (unsigned long long) source_seq);
			if (source_seq > expected_seq)
			{
				BCTxID committed_txid = get_last_committed_txid(NULL);

				if (is_raft || committed_txid < 0 ||
					source_seq - 1 > (uint64) committed_txid)
					break;
				expected_seq = source_seq;
			}
			if (source_state != 2 && source_state != 3 && source_state != 4)
				break;

			if (delta_version == 0)
			{
				if (!blob_null)
					elog(ERROR,
						 "Merkle no-op sequence %llu unexpectedly has a blob",
						 (unsigned long long) source_seq);
			}
			else if (delta_version == MERKLE_DELTA_LEGACY_VERSION ||
					 delta_version == MERKLE_DELTA_VERSION)
			{
				if (blob_null)
					elog(ERROR, "Merkle delta sequence %llu has no blob",
						 (unsigned long long) source_seq);
				blob_bytes = VARSIZE_ANY_EXHDR(DatumGetByteaPP(blob_d));
				if (blob_bytes >= MERKLE_DELTA_HEADER_BYTES)
				{
					uint32 blob_version = merkle_get_u32(
						VARDATA_ANY(DatumGetByteaPP(blob_d)) + 4);

					if ((int) blob_version != delta_version)
						elog(ERROR,
							 "Merkle delta sequence %llu row/blob version mismatch (%d/%u)",
							 (unsigned long long) source_seq,
							 delta_version, blob_version);
					delta_entry_count = merkle_get_u32(
						VARDATA_ANY(DatumGetByteaPP(blob_d)) + 12);
				}
				/* A leaf touches multiple ancestors; use the entry count as a
				 * conservative page budget so one transaction cannot grow without
				 * bound.  It avoids a second geometry traversal in the hot path. */
				if (batch_end != applied_seq &&
					batch_page_budget + delta_entry_count >
					(uint64) merkle_apply_batch_pages)
					break;
				if (batch_end != applied_seq &&
					batch_bytes + blob_bytes > (uint64) merkle_apply_batch_bytes)
					break;
				merkle_parse_delta_blob(DatumGetByteaPP(blob_d), source_seq,
									expected_log_index, expected_item_ordinal,
									is_raft, &events);
				batch_bytes += blob_bytes;
				batch_page_budget += delta_entry_count;
			}
			else
				elog(ERROR, "unsupported Merkle delta version %d at sequence %llu",
					 delta_version, (unsigned long long) source_seq);

			batch_end = source_seq;
			expected_seq++;
		}

		if (batch_end == applied_seq)
		{
			merkle_free_events(&events);
			break;
		}

		merkle_apply_dynamic_events(&events);
		merkle_apply_leaf_events(&events, batch_end);
		merkle_free_events(&events);
		applied_seq = batch_end;
		made_progress = true;
		merkle_crash_failpoint("after_all_applier_pages");
	}

	if (made_progress)
	{
		Oid argtypes[1] = {INT8OID};
		Datum values[1] = {Int64GetDatum((int64) applied_seq)};
		char nulls[1] = {' '};

		merkle_crash_failpoint("before_apply_state_update");
		spi_rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_apply_state"
			"   SET applied_seq = $1, state = 0, error_text = NULL,"
			"       updated_at = clock_timestamp()"
			" WHERE singleton",
			1, argtypes, values, nulls, false, 1);
		if (spi_rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "failed to advance Merkle durable apply state");
		/*
		 * The applied batch itself proves every sequence through applied_seq is
		 * terminal.  Persist that fact before deleting local queue evidence.
		 */
		spi_rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_apply_counter"
			"   SET next_seq = GREATEST(next_seq, $1),"
			"       terminal_prefix_seq = GREATEST(terminal_prefix_seq, $1)"
			" WHERE singleton",
			1, argtypes, values, nulls, false, 0);
		if (spi_rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "failed to advance Merkle terminal prefix to applied sequence");
		/*
		 * P0.2: Advance terminal_prefix_seq in the same transaction as the
		 * applied_seq watermark so the two are always consistent on disk.
		 * This covers every delta we just applied (both Raft and local).
		 */
		(void) merkle_advance_terminal_prefix_spi();
		/*
		 * Once page WAL and applied_seq commit together, replay blobs at or
		 * below the watermark are redundant.  Retain terminal digests/results
		 * but release the potentially large Merkle payload.
		 */
		spi_rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.raft_apply_item"
			"   SET merkle_delta_version = 0, merkle_delta_blob = NULL"
			" WHERE merkle_apply_seq <= $1 AND merkle_delta_blob IS NOT NULL",
			1, argtypes, values, nulls, false, 0);
		if (spi_rc != SPI_OK_UPDATE)
			elog(ERROR, "failed to garbage-collect applied Raft Merkle deltas");
		spi_rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_local_delta WHERE apply_seq <= $1",
			1, argtypes, values, nulls, false, 0);
		if (spi_rc != SPI_OK_DELETE)
			elog(ERROR, "failed to garbage-collect local Merkle deltas");
		merkle_crash_failpoint("after_apply_state_update");
		/* Register the commit callback after the internal subtransaction is
		 * released; callers may invoke the applier from middleware's nested
		 * subtransaction, while the failpoint must observe the top-level commit. */
		merkle_apply_state_advanced = true;
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle applier SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();

	return applied_seq;
}

static uint64
merkle_apply_until_internal_impl(uint64 required_seq)
{
	MerkleRecoveryStatusData status;
	uint64 applied_seq;
	MemoryContext old_context;

	if (!merkle_state_relations_exist())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("Merkle crash-safety state is not initialized"),
				 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));

	merkle_get_recovery_status(&status);
	applied_seq = status.applied_seq;
	old_context = CurrentMemoryContext;

	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		applied_seq = merkle_apply_until_impl(required_seq);
		ReleaseCurrentSubTransaction();
		if (merkle_apply_state_advanced && !merkle_apply_callback_registered)
		{
			RegisterXactCallback(merkle_apply_xact_callback, NULL);
			merkle_apply_callback_registered = true;
		}
	}
	PG_CATCH();
	{
		ErrorData *edata;
		MerkleRecoveryState failure_state;
		char *reason;

		/* PG_CATCH executes in ErrorContext.  CopyErrorData asserts that the
		 * destination is a different, long-lived context; old_context is the
		 * caller context captured before opening the internal subtransaction. */
		MemoryContextSwitchTo(old_context);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();

		/*
		 * P1.3: classify errors correctly.
		 *
		 * Transient errors (query cancel, lock not available, deadlock,
		 * serialisation failure, OOM) must NOT permanently invalidate a
		 * healthy database.  Re-throw them so the caller can decide whether
		 * to retry; recovery state is not changed.
		 *
		 * Data/index corruption produces REBUILD_REQUIRED.
		 * Everything else produces INVALID.
		 */
		switch (edata->sqlerrcode)
		{
			case ERRCODE_QUERY_CANCELED:
			case ERRCODE_LOCK_NOT_AVAILABLE:
			case ERRCODE_T_R_DEADLOCK_DETECTED:
			case ERRCODE_T_R_SERIALIZATION_FAILURE:
			case ERRCODE_OUT_OF_MEMORY:
				/* Retryable – re-throw without touching recovery state. */
				ReThrowError(edata);
				break;
			case ERRCODE_INDEX_CORRUPTED:
			case ERRCODE_DATA_CORRUPTED:
				failure_state = MERKLE_STATE_REBUILD_REQUIRED;
				break;
			default:
				failure_state = MERKLE_STATE_INVALID;
				break;
		}
		if (edata->detail != NULL && edata->detail[0] != '\0')
			reason = psprintf("Merkle applier failed: %s; detail: %s",
						edata->message ? edata->message : "unknown error",
						edata->detail);
		else
			reason = psprintf("Merkle applier failed: %s",
							edata->message ? edata->message : "unknown error");
		merkle_mark_recovery_state(failure_state, reason);
		pfree(reason);
		FreeErrorData(edata);
		return applied_seq;
	}
	PG_END_TRY();

	return applied_seq;
}

uint64
merkle_apply_until_internal(uint64 required_seq)
{
	Oid saved_userid;
	int saved_sec_context;
	uint64 applied_seq;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		applied_seq = merkle_apply_until_internal_impl(required_seq);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
	return applied_seq;
}

uint64
merkle_apply_pending_internal(void)
{
	return merkle_apply_until_internal(PG_UINT64_MAX);
}

void
merkle_get_recovery_status(MerkleRecoveryStatusData *status)
{
	bool pushed_snapshot = false;
	int spi_rc;
	bool isnull;
	Datum datum;

	MemSet(status, 0, sizeof(*status));
	/*
	 * P0.2: do NOT default to READY when the schema is absent.  An absent
	 * schema with at least one Merkle index is INVALID; without any index
	 * it is unmanaged but acceptable (managed=false, state left as 0).
	 */
	status->state = MERKLE_STATE_INVALID;
	if (!merkle_state_relations_exist())
	{
		status->managed = false;
		status->state = MERKLE_STATE_INVALID;	/* fail closed */
		return;
	}
	status->managed = true;
	status->state = MERKLE_STATE_READY;	/* may be overwritten below */

	PushActiveSnapshot(GetLatestSnapshot());
	pushed_snapshot = true;
	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle status SPI_connect failed: %d", spi_rc);

	spi_rc = SPI_execute(
		"SELECT s.applied_seq, s.state, COALESCE(s.error_text, ''),"
		"       c.terminal_prefix_seq,"
		"       GREATEST(c.terminal_prefix_seq,"
		"         COALESCE((SELECT max(merkle_apply_seq)"
		"                     FROM ariabc_internal.raft_apply_item"
		"                    WHERE state IN (2, 3, 4)), 0),"
		"         COALESCE((SELECT max(apply_seq)"
		"                     FROM ariabc_internal.merkle_local_delta), 0))"
		"  FROM ariabc_internal.merkle_apply_state s"
		"  JOIN ariabc_internal.merkle_apply_counter c ON c.singleton"
		" WHERE s.singleton",
		true, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "Merkle apply-state singleton is missing");
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle applied sequence is NULL");
	status->applied_seq = (uint64) DatumGetInt64(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2,
						&isnull);
	if (!isnull)
		status->state = (MerkleRecoveryState) DatumGetInt16(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3,
						&isnull);
	if (!isnull)
		strlcpy(status->error_text, TextDatumGetCString(datum),
				sizeof(status->error_text));
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle terminal_prefix_seq is NULL");
	status->terminal_prefix_seq = (uint64) DatumGetInt64(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle highest terminal sequence is NULL");
	status->highest_terminal_seq = (uint64) DatumGetInt64(datum);
	status->target_seq = status->highest_terminal_seq;

	if (status->state != MERKLE_STATE_INVALID &&
		status->state != MERKLE_STATE_REBUILD_REQUIRED)
	{
		if (status->highest_terminal_seq <= status->applied_seq)
			status->state = MERKLE_STATE_READY;
		else if (status->terminal_prefix_seq > status->applied_seq)
			status->state = MERKLE_STATE_CATCHING_UP;
		else
			status->state = MERKLE_STATE_BLOCKED_ON_GAP;
	}
	if (status->target_seq > status->applied_seq)
		status->blocked_seq = status->applied_seq + 1;

	/*
	 * P0.2 hard invariant: applied_seq must never exceed target_seq.
	 * If it does, the terminal prefix was not advanced when it should have
	 * been — treat this as INVALID to prevent stale roots appearing READY.
	 */
	if (status->applied_seq > status->terminal_prefix_seq)
		status->state = MERKLE_STATE_INVALID;

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle status SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();
}

void
merkle_require_fresh(void)
{
	MerkleRecoveryStatusData status;

	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle root cannot be read after uncommitted table changes"),
				 errdetail("The current transaction has staged Merkle deltas that are not yet durable."),
				 errhint("Commit the transaction, then read or apply the Merkle root in a new transaction.")));

	merkle_get_recovery_status(&status);
	/* Avoid entering an internal apply subtransaction on the overwhelmingly
	 * common READY read path.  Besides eliminating needless overhead, this is
	 * required when the function is evaluated inside CTAS/materialized SRFs:
	 * their destination relation is already owned by the caller's resource
	 * owner and must not be crossed by an unnecessary subtransaction.
	 *
	 * WAIT observes an independently advancing applier; APPLY explicitly
	 * permits this backend to help it.
	 */
	if (status.state != MERKLE_STATE_READY &&
		merkle_read_lag_policy == MERKLE_READ_LAG_WAIT)
	{
		int retries;

		/* WAIT never mutates pages in the reader's query/resource owner. */
		for (retries = 0; retries < 1000; retries++)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
			merkle_get_recovery_status(&status);
			if (status.state == MERKLE_STATE_READY ||
				status.state == MERKLE_STATE_INVALID ||
				status.state == MERKLE_STATE_REBUILD_REQUIRED ||
				status.state == MERKLE_STATE_BLOCKED_ON_GAP)
				break;
		}
	}
	else if (status.state != MERKLE_STATE_READY &&
			 merkle_read_lag_policy == MERKLE_READ_LAG_APPLY)
	{
		(void) merkle_apply_pending_internal();
		merkle_get_recovery_status(&status);
	}
	/* P0.2: unmanaged state (no schema) must fail closed, not silently pass. */
	if (!status.managed)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle crash-safety state is not initialized"),
				 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));
	if (status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle index is not synchronized with committed database state"),
				 errdetail("state=%d applied_seq=%llu target_seq=%llu blocked_seq=%llu%s%s",
						   (int) status.state,
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq,
						   (unsigned long long) status.blocked_seq,
						   status.error_text[0] ? " error=" : "",
						   status.error_text[0] ? status.error_text : ""),
				 errhint("Run SELECT merkle_apply_pending() or set merkle_read_lag_policy=wait.")));
}

Datum
merkle_apply_pending_sql(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("merkle_apply_pending() requires superuser")));
	PG_RETURN_INT64((int64) merkle_apply_pending_internal());
}

Datum
merkle_apply_until_sql(PG_FUNCTION_ARGS)
{
	int64 required_seq = PG_GETARG_INT64(0);

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("merkle_apply_until() requires superuser")));
	if (required_seq < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Merkle required apply sequence must be non-negative")));
	PG_RETURN_INT64((int64) merkle_apply_until_internal((uint64) required_seq));
}

Datum
merkle_recovery_status(PG_FUNCTION_ARGS)
{
	MerkleRecoveryStatusData status;
	StringInfoData out;
	const char *state_name;

	merkle_get_recovery_status(&status);
	switch (status.state)
	{
		case MERKLE_STATE_READY:
			state_name = "READY";
			break;
		case MERKLE_STATE_CATCHING_UP:
			state_name = "CATCHING_UP";
			break;
		case MERKLE_STATE_BLOCKED_ON_GAP:
			state_name = "BLOCKED_ON_GAP";
			break;
		case MERKLE_STATE_REBUILD_REQUIRED:
			state_name = "REBUILD_REQUIRED";
			break;
		default:
			state_name = "INVALID";
			break;
	}

	initStringInfo(&out);
	appendStringInfo(&out,
		"{\"state\":\"%s\",\"managed\":%s,\"applied_seq\":%llu,"
		"\"target_seq\":%llu,\"terminal_prefix_seq\":%llu,"
		"\"highest_terminal_seq\":%llu,\"blocked_seq\":%llu,\"error\":",
		state_name, status.managed ? "true" : "false",
		(unsigned long long) status.applied_seq,
		(unsigned long long) status.target_seq,
		(unsigned long long) status.terminal_prefix_seq,
		(unsigned long long) status.highest_terminal_seq,
		(unsigned long long) status.blocked_seq);
	if (status.error_text[0] != '\0')
		escape_json(&out, status.error_text);
	else
		appendStringInfoString(&out, "null");
	appendStringInfoChar(&out, '}');

	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
