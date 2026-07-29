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
#include "access/table.h"
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
	uint8		event_type;
	uint8		old_key_hash[8];
	uint8		new_key_hash[8];
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
	MerkleNodeEvent *node;
	int			nnode;
	int			node_capacity;
} MerkleEventArray;

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
		elog(ERROR, "Merkle delta sequence %llu has a NULL v1 blob",
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

	if (magic != MERKLE_DELTA_MAGIC || version != MERKLE_DELTA_VERSION)
		elog(ERROR,
			 "Merkle delta sequence %llu has unsupported magic/version 0x%08x/%u",
			 (unsigned long long) seq, magic, version);
	if ((uint64) entry_count * MERKLE_DELTA_ENTRY_BYTES != payload_len ||
		(uint64) MERKLE_DELTA_HEADER_BYTES + payload_len != (uint64) blob_len)
		elog(ERROR,
			 "Merkle delta sequence %llu has invalid length/count metadata",
			 (unsigned long long) seq);
	if (entry_count == 0)
		elog(ERROR, "Merkle delta sequence %llu stores an empty v1 batch",
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
		event.event_type = (uint8) src[16];
		memcpy(event.old_key_hash, src + 17, 8);
		memcpy(event.new_key_hash, src + 25, 8);
		format_version = merkle_get_u32(src + 33);
		memcpy(event.delta.data, src + 40, MERKLE_HASH_BYTES);

		if (!OidIsValid(event.index_oid) || (format_version != MERKLE_VERSION && format_version != 7))
			elog(ERROR,
				 "Merkle delta sequence %llu references invalid index %u or format %u",
				 (unsigned long long) seq, event.index_oid, format_version);

		if (!merkle_hash_is_zero(&event.delta))
			merkle_append_leaf_event(events, &event);
	}
}

static char *
get_index_key_expr_str(Oid index_oid)
{
	int spi_rc;
	Oid argtypes[1] = {OIDOID};
	Datum values[1] = {ObjectIdGetDatum(index_oid)};
	char *expr_str = NULL;

	spi_rc = SPI_execute_with_args(
		"SELECT pg_catalog.pg_get_indexdef($1, 1, true)",
		1, argtypes, values, NULL, true, 1);

	if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
			expr_str = TextDatumGetCString(d);
	}

	if (expr_str == NULL || strlen(expr_str) == 0)
		expr_str = pstrdup("id");

	if (strstr(expr_str, "merkle_key_hash") == NULL)
	{
		char *buf = palloc(strlen(expr_str) + 30);
		sprintf(buf, "merkle_key_hash(%s)", expr_str);
		return buf;
	}

	return expr_str;
}

static void
propagate_hash_to_ancestors(Oid index_oid, const uint8 *leaf_node_id, int leaf_prefix_len, const MerkleHash *tuple_hash_delta)
{
	uint8 curr_node_id[8];
	int curr_prefix_len = leaf_prefix_len;

	memcpy(curr_node_id, leaf_node_id, 8);

	while (curr_prefix_len > 0)
	{
		uint8 parent_node_id[8];
		int parent_prefix_len = merkle_parent_of(parent_node_id, curr_node_id, curr_prefix_len, BITS_PER_SPLIT);
		int spi_rc;
		Oid sel_argtypes[3] = {OIDOID, BYTEAOID, INT2OID};
		Datum sel_values[3];
		bytea *parent_bytea = (bytea *) palloc(VARHDRSZ + 8);
		SET_VARSIZE(parent_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(parent_bytea), parent_node_id, 8);

		sel_values[0] = ObjectIdGetDatum(index_oid);
		sel_values[1] = PointerGetDatum(parent_bytea);
		sel_values[2] = Int16GetDatum((int16) parent_prefix_len);

		spi_rc = SPI_execute_with_args(
			"SELECT hash FROM ariabc_internal.merkle_node"
			" WHERE index_oid = $1 AND node_id = $2 AND prefix_len = $3",
			3, sel_argtypes, sel_values, NULL, true, 1);

		if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
		{
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			HeapTuple tuple = SPI_tuptable->vals[0];
			bool isnull;
			Datum hash_datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			bytea *hash_bytea = DatumGetByteaPP(hash_datum);
			MerkleHash parent_hash;
			MerkleHash new_parent_hash;

			memcpy(parent_hash.data, VARDATA_ANY(hash_bytea), MERKLE_HASH_BYTES);
			memcpy(&new_parent_hash, &parent_hash, sizeof(MerkleHash));
			merkle_hash_xor(&new_parent_hash, tuple_hash_delta);

			{
				Oid upd_argtypes[4] = {BYTEAOID, OIDOID, BYTEAOID, INT2OID};
				Datum upd_values[4];
				bytea *new_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
				SET_VARSIZE(new_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
				memcpy(VARDATA(new_hash_bytea), new_parent_hash.data, MERKLE_HASH_BYTES);

				upd_values[0] = PointerGetDatum(new_hash_bytea);
				upd_values[1] = ObjectIdGetDatum(index_oid);
				upd_values[2] = PointerGetDatum(parent_bytea);
				upd_values[3] = Int16GetDatum((int16) parent_prefix_len);

				SPI_execute_with_args(
					"UPDATE ariabc_internal.merkle_node SET hash = $1"
					" WHERE index_oid = $2 AND node_id = $3 AND prefix_len = $4",
					4, upd_argtypes, upd_values, NULL, false, 1);

				pfree(new_hash_bytea);
			}
		}

		pfree(parent_bytea);
		memcpy(curr_node_id, parent_node_id, 8);
		curr_prefix_len = parent_prefix_len;
	}
}

typedef struct MerkleTupleHashEntry
{
	uint8		key_hash[8];
	MerkleHash	tuple_hash;
} MerkleTupleHashEntry;

static void
do_split_in_memory(Oid index_oid, const uint8 *node_id, int prefix_len,
				   MerkleTupleHashEntry *entries, int num_entries,
				   int fanout, int bits_per_split)
{
	int			i;
	int		   *bucket_counts;
	MerkleHash *bucket_hashes;

	if (num_entries <= 0)
		return;

	bucket_counts = (int *) palloc0(fanout * sizeof(int));
	bucket_hashes = (MerkleHash *) palloc0(fanout * sizeof(MerkleHash));

	for (i = 0; i < num_entries; i++)
	{
		uint8 b = merkle_next_bits(entries[i].key_hash, prefix_len, bits_per_split);
		if (b < fanout)
		{
			bucket_counts[b]++;
			merkle_hash_xor(&bucket_hashes[b], &entries[i].tuple_hash);
		}
	}

	for (i = 0; i < fanout; i++)
	{
		if (bucket_counts[i] > 0)
		{
			uint8		child_node_id[8];
			int			child_prefix_len = prefix_len + bits_per_split;
			bytea	   *child_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			bytea	   *child_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
			Oid			ins_argtypes[5] = {OIDOID, BYTEAOID, INT2OID, INT8OID, BYTEAOID};
			Datum		ins_values[5];

			merkle_bytea_extend(child_node_id, node_id, prefix_len, (uint8) i, bits_per_split);
			SET_VARSIZE(child_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(child_id_bytea), child_node_id, 8);

			SET_VARSIZE(child_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
			memcpy(VARDATA(child_hash_bytea), bucket_hashes[i].data, MERKLE_HASH_BYTES);

			ins_values[0] = ObjectIdGetDatum(index_oid);
			ins_values[1] = PointerGetDatum(child_id_bytea);
			ins_values[2] = Int16GetDatum((int16) child_prefix_len);
			ins_values[3] = Int64GetDatum((int64) bucket_counts[i]);
			ins_values[4] = PointerGetDatum(child_hash_bytea);

			SPI_execute_with_args(
				"INSERT INTO ariabc_internal.merkle_node"
				" (index_oid, node_id, prefix_len, is_leaf, tuple_count, hash)"
				" VALUES ($1, $2, $3, true, $4, $5)"
				" ON CONFLICT (index_oid, node_id, prefix_len) DO UPDATE"
				"   SET is_leaf = true, tuple_count = EXCLUDED.tuple_count, hash = EXCLUDED.hash",
				5, ins_argtypes, ins_values, NULL, false, 1);

			pfree(child_id_bytea);
			pfree(child_hash_bytea);

			if (bucket_counts[i] > SPLIT_THRESHOLD && child_prefix_len < MAX_PREFIX_LEN)
			{
				MerkleTupleHashEntry *child_entries;
				int			child_idx = 0;
				int			j;

				child_entries = (MerkleTupleHashEntry *) palloc(bucket_counts[i] * sizeof(MerkleTupleHashEntry));
				for (j = 0; j < num_entries; j++)
				{
					uint8 b = merkle_next_bits(entries[j].key_hash, prefix_len, bits_per_split);
					if (b == i)
					{
						child_entries[child_idx++] = entries[j];
					}
				}

				do_split_in_memory(index_oid, child_node_id, child_prefix_len,
								   child_entries, bucket_counts[i],
								   fanout, bits_per_split);
				pfree(child_entries);
			}
		}
	}

	{
		bytea	   *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
		Oid			upd_argtypes[3] = {OIDOID, BYTEAOID, INT2OID};
		Datum		upd_values[3];

		SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(node_id_bytea), node_id, 8);
		upd_values[0] = ObjectIdGetDatum(index_oid);
		upd_values[1] = PointerGetDatum(node_id_bytea);
		upd_values[2] = Int16GetDatum((int16) prefix_len);

		SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_node"
			"   SET is_leaf = false"
			" WHERE index_oid = $1 AND node_id = $2 AND prefix_len = $3",
			3, upd_argtypes, upd_values, NULL, false, 1);

		pfree(node_id_bytea);
	}

	pfree(bucket_counts);
	pfree(bucket_hashes);
}

void
do_split(Oid index_oid, const uint8 *node_id, int prefix_len)
{
	uint8		lower[8];
	uint8		upper[8];
	Relation	index_rel;
	Relation	heap_rel;
	Oid			heap_oid;
	char	   *heap_name;
	char	   *key_expr;
	int			spi_rc;
	StringInfoData buf;
	MerkleOptions *opts;
	int			fanout;
	int			bits_per_split;

	memcpy(lower, node_id, 8);
	merkle_bytea_upper_bound(upper, node_id, prefix_len);

	index_rel = index_open(index_oid, AccessShareLock);
	opts = merkle_get_options(index_rel);
	fanout = (opts != NULL) ? opts->fanout : DYNAMIC_MERKLE_FANOUT;
	bits_per_split = merkle_bits_per_split_for_fanout(fanout);
	heap_oid = index_rel->rd_index->indrelid;
	index_close(index_rel, AccessShareLock);

	heap_rel = table_open(heap_oid, AccessShareLock);
	heap_name = quote_qualified_identifier(
		get_namespace_name(RelationGetNamespace(heap_rel)),
		RelationGetRelationName(heap_rel));
	table_close(heap_rel, AccessShareLock);

	key_expr = get_index_key_expr_str(index_oid);

	initStringInfo(&buf);
	appendStringInfo(&buf,
		"SELECT %s AS kh, merkle_tuple_hash(%s.*) AS th"
		"  FROM %s"
		" WHERE %s BETWEEN $1 AND $2",
		key_expr, heap_name, heap_name, key_expr);

	{
		Oid			argtypes[2] = {BYTEAOID, BYTEAOID};
		Datum		values[2];
		bytea	   *lower_bytea = (bytea *) palloc(VARHDRSZ + 8);
		bytea	   *upper_bytea = (bytea *) palloc(VARHDRSZ + 8);

		SET_VARSIZE(lower_bytea, VARHDRSZ + 8);
		SET_VARSIZE(upper_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(lower_bytea), lower, 8);
		memcpy(VARDATA(upper_bytea), upper, 8);
		values[0] = PointerGetDatum(lower_bytea);
		values[1] = PointerGetDatum(upper_bytea);

		spi_rc = SPI_execute_with_args(buf.data, 2, argtypes, values, NULL, true, 0);

		if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
		{
			int						i;
			MerkleTupleHashEntry   *entries = (MerkleTupleHashEntry *) palloc(SPI_processed * sizeof(MerkleTupleHashEntry));

			for (i = 0; i < SPI_processed; i++)
			{
				HeapTuple	tup = SPI_tuptable->vals[i];
				TupleDesc	td = SPI_tuptable->tupdesc;
				bool		isnull;
				Datum		kh_d = SPI_getbinval(tup, td, 1, &isnull);
				Datum		th_d = SPI_getbinval(tup, td, 2, &isnull);
				bytea	   *kh_b = DatumGetByteaPP(kh_d);
				bytea	   *th_b = DatumGetByteaPP(th_d);

				memcpy(entries[i].key_hash, VARDATA_ANY(kh_b), 8);
				memcpy(entries[i].tuple_hash.data, VARDATA_ANY(th_b), MERKLE_HASH_BYTES);
			}

			do_split_in_memory(index_oid, node_id, prefix_len, entries, SPI_processed, fanout, bits_per_split);
			pfree(entries);
		}

		pfree(lower_bytea);
		pfree(upper_bytea);
	}

	pfree(buf.data);
}

static void
do_merge_check(Oid index_oid, const uint8 *node_id, int prefix_len)
{
	uint8 parent_node_id[8];
	int parent_prefix_len;
	uint8 lower[8];
	uint8 upper[8];
	int spi_rc;

	if (prefix_len <= 0)
		return;

	parent_prefix_len = merkle_parent_of(parent_node_id, node_id, prefix_len, BITS_PER_SPLIT);
	memcpy(lower, parent_node_id, 8);
	merkle_bytea_upper_bound(upper, parent_node_id, parent_prefix_len);

	{
		Oid argtypes[4] = {OIDOID, INT2OID, BYTEAOID, BYTEAOID};
		Datum values[4];
		bytea *lower_bytea = (bytea *) palloc(VARHDRSZ + 8);
		bytea *upper_bytea = (bytea *) palloc(VARHDRSZ + 8);
		SET_VARSIZE(lower_bytea, VARHDRSZ + 8);
		SET_VARSIZE(upper_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(lower_bytea), lower, 8);
		memcpy(VARDATA(upper_bytea), upper, 8);

		values[0] = ObjectIdGetDatum(index_oid);
		values[1] = Int16GetDatum((int16) prefix_len);
		values[2] = PointerGetDatum(lower_bytea);
		values[3] = PointerGetDatum(upper_bytea);

		spi_rc = SPI_execute_with_args(
			"SELECT count(*), bool_and(is_leaf), sum(tuple_count)"
			"  FROM ariabc_internal.merkle_node"
			" WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4"
			" GROUP BY index_oid",
			4, argtypes, values, NULL, true, 1);

		if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
		{
			TupleDesc td = SPI_tuptable->tupdesc;
			HeapTuple tup = SPI_tuptable->vals[0];
			bool isnull;
			bool all_leaves = DatumGetBool(SPI_getbinval(tup, td, 2, &isnull));
			int64 total_count = DatumGetInt64(SPI_getbinval(tup, td, 3, &isnull));

			if (all_leaves && total_count < MERKLE_MERGE_THRESHOLD)
			{
				int i;
				MerkleHash merged_hash;
				merkle_hash_zero(&merged_hash);

				spi_rc = SPI_execute_with_args(
					"SELECT hash FROM ariabc_internal.merkle_node"
					" WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4",
					4, argtypes, values, NULL, true, 0);

				if (spi_rc == SPI_OK_SELECT)
				{
					for (i = 0; i < SPI_processed; i++)
					{
						HeapTuple c_tup = SPI_tuptable->vals[i];
						TupleDesc c_td = SPI_tuptable->tupdesc;
						Datum h_d = SPI_getbinval(c_tup, c_td, 1, &isnull);
						bytea *h_b = DatumGetByteaPP(h_d);
						MerkleHash ch;
						memcpy(ch.data, VARDATA_ANY(h_b), MERKLE_HASH_BYTES);
						merkle_hash_xor(&merged_hash, &ch);
					}
				}

				SPI_execute_with_args(
					"DELETE FROM ariabc_internal.merkle_node"
					" WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4",
					4, argtypes, values, NULL, false, 0);

				{
					bytea *parent_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
					bytea *merged_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
					Oid upd_argtypes[5] = {INT8OID, BYTEAOID, OIDOID, BYTEAOID, INT2OID};
					Datum upd_values[5];

					SET_VARSIZE(parent_id_bytea, VARHDRSZ + 8);
					memcpy(VARDATA(parent_id_bytea), parent_node_id, 8);
					SET_VARSIZE(merged_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
					memcpy(VARDATA(merged_hash_bytea), merged_hash.data, MERKLE_HASH_BYTES);

					upd_values[0] = Int64GetDatum(total_count);
					upd_values[1] = PointerGetDatum(merged_hash_bytea);
					upd_values[2] = ObjectIdGetDatum(index_oid);
					upd_values[3] = PointerGetDatum(parent_id_bytea);
					upd_values[4] = Int16GetDatum((int16) parent_prefix_len);

					SPI_execute_with_args(
						"UPDATE ariabc_internal.merkle_node"
						"   SET is_leaf = true, tuple_count = $1, hash = $2"
						" WHERE index_oid = $3 AND node_id = $4 AND prefix_len = $5",
						5, upd_argtypes, upd_values, NULL, false, 1);

					pfree(parent_id_bytea);
					pfree(merged_hash_bytea);
				}
			}
		}

		pfree(lower_bytea);
		pfree(upper_bytea);
	}
}

static void
apply_leaf_event(Oid index_oid, const uint8 key_hash[8], const MerkleHash *tuple_hash_delta, int64 count_delta)
{
	uint8 node_id[8];
	int prefix_len = 0;

	memset(node_id, 0, 8);

	for (;;)
	{
		int spi_rc;
		Oid argtypes[3] = {OIDOID, BYTEAOID, INT2OID};
		Datum values[3];
		bytea *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
		SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(node_id_bytea), node_id, 8);

		values[0] = ObjectIdGetDatum(index_oid);
		values[1] = PointerGetDatum(node_id_bytea);
		values[2] = Int16GetDatum((int16) prefix_len);

		spi_rc = SPI_execute_with_args(
			"SELECT is_leaf, tuple_count, hash"
			"  FROM ariabc_internal.merkle_node"
			" WHERE index_oid = $1 AND node_id = $2 AND prefix_len = $3",
			3, argtypes, values, NULL, true, 1);

		if (spi_rc != SPI_OK_SELECT)
			elog(ERROR, "apply_leaf_event SPI_execute failed for index %u", index_oid);

		if (SPI_processed == 0)
		{
			if (prefix_len == 0)
			{
				Oid ins_argtypes[4] = {OIDOID, BYTEAOID, INT2OID, BYTEAOID};
				Datum ins_values[4];
				bytea *zero_hash_bytea = (bytea *) palloc0(VARHDRSZ + MERKLE_HASH_BYTES);
				SET_VARSIZE(zero_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);

				ins_values[0] = ObjectIdGetDatum(index_oid);
				ins_values[1] = PointerGetDatum(node_id_bytea);
				ins_values[2] = Int16GetDatum(0);
				ins_values[3] = PointerGetDatum(zero_hash_bytea);

				SPI_execute_with_args(
					"INSERT INTO ariabc_internal.merkle_node"
					" (index_oid, node_id, prefix_len, is_leaf, tuple_count, hash)"
					" VALUES ($1, $2, $3, true, 0, $4)"
					" ON CONFLICT (index_oid, node_id, prefix_len) DO NOTHING",
					4, ins_argtypes, ins_values, NULL, false, 1);

				pfree(zero_hash_bytea);
				pfree(node_id_bytea);
				continue;
			}
			else
			{
				pfree(node_id_bytea);
				elog(ERROR, "apply_leaf_event node (%u, len=%d) not found", index_oid, prefix_len);
			}
		}

		{
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			HeapTuple tuple = SPI_tuptable->vals[0];
			bool isnull;
			bool is_leaf = DatumGetBool(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			int64 current_count = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			Datum hash_datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			bytea *hash_bytea = DatumGetByteaPP(hash_datum);
			MerkleHash current_hash;
			MerkleHash new_hash;
			int64 new_count;

			memcpy(current_hash.data, VARDATA_ANY(hash_bytea), MERKLE_HASH_BYTES);

			if (is_leaf)
			{
				memcpy(&new_hash, &current_hash, sizeof(MerkleHash));
				merkle_hash_xor(&new_hash, tuple_hash_delta);
				new_count = current_count + count_delta;
				if (new_count < 0)
					new_count = 0;

				{
					Oid upd_argtypes[5] = {BYTEAOID, INT8OID, OIDOID, BYTEAOID, INT2OID};
					Datum upd_values[5];
					bytea *new_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
					SET_VARSIZE(new_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
					memcpy(VARDATA(new_hash_bytea), new_hash.data, MERKLE_HASH_BYTES);

					upd_values[0] = PointerGetDatum(new_hash_bytea);
					upd_values[1] = Int64GetDatum(new_count);
					upd_values[2] = ObjectIdGetDatum(index_oid);
					upd_values[3] = PointerGetDatum(node_id_bytea);
					upd_values[4] = Int16GetDatum((int16) prefix_len);

					SPI_execute_with_args(
						"UPDATE ariabc_internal.merkle_node"
						"   SET hash = $1, tuple_count = $2"
						" WHERE index_oid = $3 AND node_id = $4 AND prefix_len = $5",
						5, upd_argtypes, upd_values, NULL, false, 1);

					pfree(new_hash_bytea);
				}

				propagate_hash_to_ancestors(index_oid, node_id, prefix_len, tuple_hash_delta);

				if (new_count > SPLIT_THRESHOLD && prefix_len < MAX_PREFIX_LEN)
				{
					do_split(index_oid, node_id, prefix_len);
				}
				else if (new_count < MERKLE_MERGE_THRESHOLD && prefix_len > 0)
				{
					do_merge_check(index_oid, node_id, prefix_len);
				}

				pfree(node_id_bytea);
				return;
			}
			else
			{
				uint8 bits = merkle_next_bits(key_hash, prefix_len, BITS_PER_SPLIT);
				uint8 next_node_id[8];
				merkle_bytea_extend(next_node_id, node_id, prefix_len, bits, BITS_PER_SPLIT);
				memcpy(node_id, next_node_id, 8);
				prefix_len += BITS_PER_SPLIT;
				pfree(node_id_bytea);
			}
		}
	}
}

static void
apply_static_leaf_event(Oid index_oid, const uint8 key_hash[8], const MerkleHash *delta)
{
	int leafId = (int)key_hash[0] | ((int)key_hash[1] << 8) | ((int)key_hash[2] << 16) | ((int)key_hash[3] << 24);
	Relation indexRel = index_open(index_oid, RowExclusiveLock);
	int numPartitions, nodesPerPartition, totalNodes, nodesPerPage, numTreePages;
	MerkleGeometry geometry;
	int partition;
	int node_in_partition;

	merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition,
					 &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);
	merkle_geometry_from_index(indexRel, &geometry);

	if (leafId < 0 || leafId >= geometry.total_leaves)
	{
		index_close(indexRel, RowExclusiveLock);
		elog(ERROR, "apply_static_leaf_event: leafId %d out of bounds", leafId);
	}

	partition = leafId / geometry.leaves_per_partition;
	node_in_partition = merkle_geometry_leaf_node(&geometry, leafId);

	while (node_in_partition > 0)
	{
		int actual_index = merkle_geometry_global_node(&geometry, partition, node_in_partition);
		int page_number = actual_index / nodesPerPage;
		int index_in_page = actual_index % nodesPerPage;
		Buffer buffer;
		Page target_page;
		MerkleNode *target_nodes;
		GenericXLogState *state;

		buffer = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + page_number);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		
		state = GenericXLogStart(indexRel);
		target_page = GenericXLogRegisterBuffer(state, buffer, 0);
		target_nodes = (MerkleNode *) PageGetContents(target_page);
		
		merkle_hash_xor(&target_nodes[index_in_page].hash, delta);
		
		(void) GenericXLogFinish(state);
		UnlockReleaseBuffer(buffer);

		node_in_partition = merkle_geometry_parent_node(&geometry, node_in_partition);
	}

	index_close(indexRel, RowExclusiveLock);
}

static void
merkle_apply_leaf_events(MerkleEventArray *events, uint64 batch_end)
{
	int i;
	(void) batch_end;

	for (i = 0; i < events->nleaf; i++)
	{
		MerkleLeafEvent *e = &events->leaf[i];
		Relation indexRel = index_open(e->index_oid, AccessShareLock);
		bool is_dynamic = merkle_is_dynamic_index(indexRel);
		index_close(indexRel, AccessShareLock);

		if (e->event_type == MERKLE_DELTA_INSERT)
		{
			if (is_dynamic) apply_leaf_event(e->index_oid, e->new_key_hash, &e->delta, 1);
			else apply_static_leaf_event(e->index_oid, e->new_key_hash, &e->delta);
		}
		else if (e->event_type == MERKLE_DELTA_DELETE)
		{
			if (is_dynamic) apply_leaf_event(e->index_oid, e->old_key_hash, &e->delta, -1);
			else apply_static_leaf_event(e->index_oid, e->old_key_hash, &e->delta);
		}
		else if (e->event_type == MERKLE_DELTA_UPDATE_SAME_LEAF)
		{
			if (is_dynamic) apply_leaf_event(e->index_oid, e->old_key_hash, &e->delta, 0);
			else apply_static_leaf_event(e->index_oid, e->old_key_hash, &e->delta);
		}
		else if (e->event_type == MERKLE_DELTA_UPDATE_KEY_CHANGED)
		{
			if (is_dynamic) {
				apply_leaf_event(e->index_oid, e->old_key_hash, &e->delta, -1);
				apply_leaf_event(e->index_oid, e->new_key_hash, &e->delta, 1);
			} else {
				apply_static_leaf_event(e->index_oid, e->old_key_hash, &e->delta);
				apply_static_leaf_event(e->index_oid, e->new_key_hash, &e->delta);
			}
		}
	}
}

static void
merkle_free_events(MerkleEventArray *events)
{
	if (events->leaf != NULL)
		pfree(events->leaf);
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
			else if (delta_version == MERKLE_DELTA_VERSION)
			{
				if (blob_null)
					elog(ERROR, "Merkle delta sequence %llu has no blob",
						 (unsigned long long) source_seq);
				blob_bytes = VARSIZE_ANY_EXHDR(DatumGetByteaPP(blob_d));
				if (blob_bytes >= MERKLE_DELTA_HEADER_BYTES)
					delta_entry_count = merkle_get_u32(
						VARDATA_ANY(DatumGetByteaPP(blob_d)) + 12);
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
