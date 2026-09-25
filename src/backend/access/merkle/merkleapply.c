/*-------------------------------------------------------------------------
 *
 * merkleapply.c
 *    Ordered, idempotent, Generic-WAL-backed Merkle delta application.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/heapam_xlog.h"
#include "access/merkle.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "executor/executor.h"
#include "bcdb/shm_block.h"
#include "utils/fmgroids.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "catalog/pg_authid_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
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
PG_FUNCTION_INFO_V1(merkle_apply_until_sql);
PG_FUNCTION_INFO_V1(merkle_rebuild_legacy_indexes);

static void merkle_route_cache_clear_index(Oid index_oid);
static void merkle_route_cache_clear_partition(Oid index_oid, int partition_id);
static void merkle_sync_prepare_plans(Oid index_oid);
static void propagate_hash_to_ancestors_atomic(Oid index_oid, int partition_id,
											   const uint8 *leaf_node_id,
											   int leaf_prefix_len,
											   const MerkleHash *tuple_hash_delta,
											   int64 count_delta,
											   int bits_per_split);
static bool merkle_state_relations_exist(void);

static bool
merkle_state_relations_exist(void)
{
	Oid namespace_oid = get_namespace_oid("ariabc_internal", true);

	if (!OidIsValid(namespace_oid))
		return false;
	return OidIsValid(get_relname_relid("merkle_apply_state", namespace_oid)) &&
		OidIsValid(get_relname_relid("merkle_apply_counter", namespace_oid)) &&
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
		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);
		(void) SPI_finish();
		return;
	}
	if (SPI_tuptable != NULL)
		SPI_freetuptable(SPI_tuptable);
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



static bool
merkle_index_page_is_v7(Oid index_oid)
{
	Relation index_rel;
	Buffer buf = InvalidBuffer;
	Page page;
	MerkleMetaPageData *meta;
	BlockNumber nblocks;
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
			valid = meta->version == MERKLE_VERSION &&
				meta->routeFormatVersion == MERKLE_ROUTE_FORMAT_VERSION &&
				meta->rowHashFormatVersion == MERKLE_ROW_HASH_FORMAT_VERSION;
		}
		UnlockReleaseBuffer(buf);
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
		SPI_freetuptable(SPI_tuptable);
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
		SPI_freetuptable(SPI_tuptable);
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



typedef struct MerklePlanCacheEntry
{
	Oid			index_oid;
	Oid			catalog_relid;
	Oid			pkey_idx_oid;
	char	   *key_expr_str;
	SPIPlanPtr	sync_route_plan;
	SPIPlanPtr	sync_leaf_update_plan;
	SPIPlanPtr	sync_ancestor_update_plan;
	SPIPlanPtr	sync_check_count_plan;
	SPIPlanPtr	split_update_nonleaf;
	SPIPlanPtr	split_insert_child;
} MerklePlanCacheEntry;

#define MERKLE_PLAN_CACHE_SIZE 64
static MerklePlanCacheEntry g_plan_cache[MERKLE_PLAN_CACHE_SIZE];

static MerklePlanCacheEntry *
merkle_get_plans(Oid index_oid)
{
	int i;
	int empty_slot = -1;
	for (i = 0; i < MERKLE_PLAN_CACHE_SIZE; i++)
	{
		if (g_plan_cache[i].index_oid == index_oid)
			return &g_plan_cache[i];
		if (empty_slot < 0 && g_plan_cache[i].index_oid == InvalidOid)
			empty_slot = i;
	}

	if (empty_slot < 0)
		empty_slot = index_oid % MERKLE_PLAN_CACHE_SIZE;

	g_plan_cache[empty_slot].index_oid = index_oid;
	g_plan_cache[empty_slot].catalog_relid = InvalidOid;
	g_plan_cache[empty_slot].pkey_idx_oid = InvalidOid;
	g_plan_cache[empty_slot].key_expr_str = NULL;
	g_plan_cache[empty_slot].sync_route_plan = NULL;
	g_plan_cache[empty_slot].sync_leaf_update_plan = NULL;
	g_plan_cache[empty_slot].sync_ancestor_update_plan = NULL;
	g_plan_cache[empty_slot].sync_check_count_plan = NULL;
	g_plan_cache[empty_slot].split_update_nonleaf = NULL;
	g_plan_cache[empty_slot].split_insert_child = NULL;

	return &g_plan_cache[empty_slot];
}

static char *
get_index_key_expr_str(Oid index_oid)
{
	MerklePlanCacheEntry *plans = merkle_get_plans(index_oid);
	int spi_rc;
	Oid argtypes[1] = {OIDOID};
	Datum values[1] = {ObjectIdGetDatum(index_oid)};
	char *expr_str = NULL;
	int16 natts = 1;

	if (plans->key_expr_str != NULL)
		return pstrdup(plans->key_expr_str);

	spi_rc = SPI_execute_with_args(
		"SELECT (SELECT indnatts FROM pg_catalog.pg_index WHERE indexrelid = $1) AS natts, "
		"       string_agg(pg_catalog.pg_get_indexdef($1, attnum, true), ', ' ORDER BY attnum) AS cols "
		"  FROM generate_series(1, (SELECT indnatts FROM pg_catalog.pg_index WHERE indexrelid = $1)) AS attnum "
		" GROUP BY 1",
		1, argtypes, values, NULL, true, 1);

	if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		Datum d_natts = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
			natts = DatumGetInt16(d_natts);
		Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
		if (!isnull)
			expr_str = TextDatumGetCString(d);
		SPI_freetuptable(SPI_tuptable);
	}

	if (expr_str == NULL || strlen(expr_str) == 0)
		elog(ERROR, "could not determine index key expression for index %u", index_oid);

	if (strstr(expr_str, "merkle_key_hash") == NULL)
	{
		if (natts > 1)
			expr_str = psprintf("merkle_key_hash(ROW(%s))", expr_str);
		else
			expr_str = psprintf("merkle_key_hash(%s)", expr_str);
	}

	plans->key_expr_str = strdup(expr_str);

	return expr_str;
}



void
merkle_do_split_in_memory(Oid index_oid, int partition_id, const uint8 *node_id, int prefix_len,
				   MerkleTupleHashEntry *entries, int num_entries,
				   int fanout, int bits_per_split, int split_threshold)
{
	int			i;
	int		   *bucket_counts;
	MerkleHash *bucket_hashes;
	MerklePlanCacheEntry *plans;

	if (num_entries <= 0)
		return;

	plans = merkle_get_plans(index_oid);

	/* Prepare SPI plans once for high-frequency split operations */
	if (plans->split_update_nonleaf == NULL || !SPI_plan_is_valid(plans->split_update_nonleaf))
	{
		char tablename[64];
		char *upd_sql;
		Oid upd_argtypes[5] = {INT2OID, BYTEAOID, INT2OID, INT4OID, BYTEAOID};
		merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
		upd_sql = psprintf(
			"UPDATE ariabc_internal.%s"
			"   SET is_leaf = false, tuple_count = $4, hash = $5"
			" WHERE partition_id = $1 AND node_id = $2 AND prefix_len = $3",
			tablename);
		SPIPlanPtr plan = SPI_prepare(upd_sql, 5, upd_argtypes);
		pfree(upd_sql);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed for split_update_nonleaf for index %u", index_oid);
		SPI_keepplan(plan);
		plans->split_update_nonleaf = plan;
	}

	if (plans->split_insert_child == NULL || !SPI_plan_is_valid(plans->split_insert_child))
	{
		char tablename[64];
		char *ins_sql;
		Oid ins_argtypes[5] = {INT2OID, BYTEAOID, INT2OID, INT4OID, BYTEAOID};
		merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
		ins_sql = psprintf(
			"INSERT INTO ariabc_internal.%s"
			" (partition_id, node_id, prefix_len, is_leaf, tuple_count, hash)"
			" VALUES ($1, $2, $3, true, $4, $5)"
			" ON CONFLICT (partition_id, node_id, prefix_len) DO UPDATE"
			"   SET is_leaf = true, tuple_count = EXCLUDED.tuple_count, hash = EXCLUDED.hash",
			tablename);
		SPIPlanPtr plan = SPI_prepare(ins_sql, 5, ins_argtypes);
		pfree(ins_sql);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed for split_insert_child for index %u", index_oid);
		SPI_keepplan(plan);
		plans->split_insert_child = plan;
	}

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
	/* Group entries by bucket so recursive calls receive the exact subset of tuples */
	{
		MerkleTupleHashEntry *partitioned_entries = (MerkleTupleHashEntry *) palloc((size_t) num_entries * sizeof(MerkleTupleHashEntry));
		int *bucket_offsets = (int *) palloc0(fanout * sizeof(int));
		int *current_offsets = (int *) palloc(fanout * sizeof(int));
		int running_offset = 0;

		for (i = 0; i < fanout; i++)
		{
			bucket_offsets[i] = running_offset;
			current_offsets[i] = running_offset;
			running_offset += bucket_counts[i];
		}

		for (i = 0; i < num_entries; i++)
		{
			uint8 b = merkle_next_bits(entries[i].key_hash, prefix_len, bits_per_split);
			if (b < fanout)
			{
				partitioned_entries[current_offsets[b]++] = entries[i];
			}
		}

		for (i = 0; i < fanout; i++)
		{
			uint8		child_node_id[8];
			int			child_prefix_len = prefix_len + bits_per_split;
			bytea	   *child_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			bytea	   *child_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
			Datum		ins_values[5];

			merkle_bytea_extend(child_node_id, node_id, prefix_len, (uint8) i, bits_per_split);
			SET_VARSIZE(child_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(child_id_bytea), child_node_id, 8);

			SET_VARSIZE(child_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
			memcpy(VARDATA(child_hash_bytea), bucket_hashes[i].data, MERKLE_HASH_BYTES);

			ins_values[0] = Int16GetDatum((int16) partition_id);
			ins_values[1] = PointerGetDatum(child_id_bytea);
			ins_values[2] = Int16GetDatum((int16) child_prefix_len);
			ins_values[3] = Int32GetDatum((int32) bucket_counts[i]);
			ins_values[4] = PointerGetDatum(child_hash_bytea);

			SPI_execute_plan(plans->split_insert_child, ins_values, NULL, false, 1);
			if (SPI_tuptable != NULL)
				SPI_freetuptable(SPI_tuptable);

			pfree(child_id_bytea);
			pfree(child_hash_bytea);

			if (bucket_counts[i] > split_threshold && child_prefix_len < MAX_PREFIX_LEN)
			{
				merkle_do_split_in_memory(index_oid, partition_id, child_node_id, child_prefix_len,
								   &partitioned_entries[bucket_offsets[i]], bucket_counts[i],
								   fanout, bits_per_split, split_threshold);
			}
		}

		pfree(partitioned_entries);
		pfree(bucket_offsets);
		pfree(current_offsets);
	}

	{
		MerkleHash	total_split_hash;
		int64		total_split_count = 0;
		bytea	   *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
		bytea	   *hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		Datum		upd_values[5];

		merkle_hash_zero(&total_split_hash);
		for (i = 0; i < fanout; i++)
		{
			merkle_hash_xor(&total_split_hash, &bucket_hashes[i]);
			total_split_count += bucket_counts[i];
		}

		SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(node_id_bytea), node_id, 8);

		SET_VARSIZE(hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
		memcpy(VARDATA(hash_bytea), total_split_hash.data, MERKLE_HASH_BYTES);

		upd_values[0] = Int16GetDatum((int16) partition_id);
		upd_values[1] = PointerGetDatum(node_id_bytea);
		upd_values[2] = Int16GetDatum((int16) prefix_len);
		upd_values[3] = Int32GetDatum((int32) total_split_count);
		upd_values[4] = PointerGetDatum(hash_bytea);

		SPI_execute_plan(plans->split_update_nonleaf, upd_values, NULL, false, 1);
		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);

		pfree(node_id_bytea);
		pfree(hash_bytea);

		/* Splits re-partition existing entries without changing total ancestor count or XOR hash */
	}

	pfree(bucket_counts);
	pfree(bucket_hashes);
}

void
do_split(Oid index_oid, int partition_id, const uint8 *node_id, int prefix_len, int64 target_count)
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
	int			fanout;
	int			bits_per_split;
	int			split_threshold;
	int			num_partitions;

	memcpy(lower, node_id, 8);
	merkle_bytea_upper_bound(upper, node_id, prefix_len);

	index_rel = index_open(index_oid, AccessShareLock);
	fanout = DYNAMIC_MERKLE_FANOUT;
	split_threshold = SPLIT_THRESHOLD;
	merkle_read_meta(index_rel, &fanout, &split_threshold, NULL, &num_partitions);
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
	/*
	 * Streamlined split row retrieval: scan directly using the covering
	 * B-tree index (usertable_small_merkle_lookup_idx) avoiding table-wide
	 * sequential scans, CTE materialization, and sorting overhead.
	 */
	appendStringInfo(&buf,
		"SELECT %s AS kh, merkle_tuple_hash(u.*) AS th"
		"  FROM %s u"
		" WHERE merkle_partition_for_hash(%s, $3) = $4"
		"   AND %s BETWEEN $1 AND $2",
		key_expr, heap_name, key_expr, key_expr);

	{
		Oid			argtypes[4] = {BYTEAOID, BYTEAOID, INT4OID, INT2OID};
		Datum		values[4];
		bytea	   *lower_bytea = (bytea *) palloc(VARHDRSZ + 8);
		bytea	   *upper_bytea = (bytea *) palloc(VARHDRSZ + 8);
		uint64		scan_rows;

		SET_VARSIZE(lower_bytea, VARHDRSZ + 8);
		SET_VARSIZE(upper_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(lower_bytea), lower, 8);
		memcpy(VARDATA(upper_bytea), upper, 8);
		values[0] = PointerGetDatum(lower_bytea);
		values[1] = PointerGetDatum(upper_bytea);
		values[2] = Int32GetDatum(num_partitions);
		values[3] = Int16GetDatum((int16) partition_id);

		PushActiveSnapshot(GetLatestSnapshot());
		spi_rc = SPI_execute_with_args(buf.data, 4, argtypes, values, NULL, true, 0);
		scan_rows = SPI_processed;
		PopActiveSnapshot();

		if (scan_rows == 0)
		{
			if (SPI_tuptable != NULL)
				SPI_freetuptable(SPI_tuptable);
			pfree(lower_bytea);
			pfree(upper_bytea);
			pfree(buf.data);
			return;
		}

		if (target_count > 0 && scan_rows != (uint64) target_count)
		{
			elog(DEBUG1,
				 "Merkle split row count differs from node tuple_count (index %u prefix %d: tuple_count %lld, heap scan %llu)",
				 index_oid, prefix_len, (long long) target_count, (unsigned long long) scan_rows);
		}

		if (spi_rc == SPI_OK_SELECT && scan_rows > 0)
		{
			int						i;
			int						num_entries;
			MerkleTupleHashEntry *entries;

			if (scan_rows > PG_INT32_MAX)
				elog(ERROR, "too many rows returned while splitting Merkle node (%llu)",
					 (unsigned long long) scan_rows);
			num_entries = (int) scan_rows;
			entries = (MerkleTupleHashEntry *) palloc((size_t) num_entries * sizeof(MerkleTupleHashEntry));

			/* Copy all data out of the SPI tuptable before freeing it. */
			for (i = 0; i < num_entries; i++)
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
			SPI_freetuptable(SPI_tuptable);

			merkle_do_split_in_memory(index_oid, partition_id, node_id, prefix_len, entries, num_entries, fanout, bits_per_split, split_threshold);
			merkle_route_cache_clear_partition(index_oid, partition_id);
			CommandCounterIncrement();

			pfree(entries);
		}
		else if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);

		pfree(lower_bytea);
		pfree(upper_bytea);
	}

	pfree(buf.data);
}

static void
do_merge_check(Oid index_oid, int partition_id, const uint8 *node_id, int prefix_len, int merge_thresh)
{
	uint8 parent_node_id[8];
	int parent_prefix_len;
	Relation index_rel;
	int fanout, bits_per_split;

	if (prefix_len <= 0)
		return;

	index_rel = index_open(index_oid, AccessShareLock);
	fanout = DYNAMIC_MERKLE_FANOUT;
	merkle_read_meta(index_rel, &fanout, NULL, NULL, NULL);
	bits_per_split = merkle_bits_per_split_for_fanout(fanout);
	index_close(index_rel, AccessShareLock);

	parent_prefix_len = merkle_parent_of(parent_node_id, node_id, prefix_len, bits_per_split);

	{
		uint8 lower[8];
		uint8 upper[8];
		bytea *lower_bytea = (bytea *) palloc(VARHDRSZ + 8);
		bytea *upper_bytea = (bytea *) palloc(VARHDRSZ + 8);
		Oid argtypes[4] = {INT2OID, INT2OID, BYTEAOID, BYTEAOID};
		Datum values[4];
		char *sql;
		int spi_rc;

		memcpy(lower, parent_node_id, 8);
		merkle_bytea_upper_bound(upper, parent_node_id, parent_prefix_len);

		SET_VARSIZE(lower_bytea, VARHDRSZ + 8);
		SET_VARSIZE(upper_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(lower_bytea), lower, 8);
		memcpy(VARDATA(upper_bytea), upper, 8);

		values[0] = Int16GetDatum((int16) partition_id);
		values[1] = Int16GetDatum((int16) prefix_len);
		values[2] = PointerGetDatum(lower_bytea);
		values[3] = PointerGetDatum(upper_bytea);

		PushActiveSnapshot(GetLatestSnapshot());
		{
			char tablename[64];
			merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
			sql = psprintf(
				"SELECT count(*), bool_and(is_leaf), sum(tuple_count)::bigint"
				"  FROM ariabc_internal.%s"
				" WHERE partition_id = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4",
				tablename);
		}
		spi_rc = SPI_execute_with_args(sql, 4, argtypes, values, NULL, true, 1);
		pfree(sql);
		PopActiveSnapshot();

		if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
		{
			TupleDesc td = SPI_tuptable->tupdesc;
			HeapTuple tup = SPI_tuptable->vals[0];
			bool isnull;
			Datum datum;
			int64 total_children;
			bool all_leaves;
			int64 total_count;

			datum = SPI_getbinval(tup, td, 1, &isnull);
			total_children = isnull ? 0 : DatumGetInt64(datum);
			datum = SPI_getbinval(tup, td, 2, &isnull);
			all_leaves = !isnull && DatumGetBool(datum);
			datum = SPI_getbinval(tup, td, 3, &isnull);
			total_count = isnull ? 0 : DatumGetInt64(datum);

			if (SPI_tuptable != NULL)
				SPI_freetuptable(SPI_tuptable);

			if (total_children > 0 && all_leaves && total_count <= merge_thresh)
			{
				int i;
				MerkleHash merged_hash;
				bool can_merge;
				merkle_hash_zero(&merged_hash);

				/*
				 * Lock the siblings without waiting.  A sibling locked by another
				 * transaction belongs to a leaf update that will next wait for
				 * the ancestors we already hold, so blocking here would deadlock.
				 * Merging is optional: skip it and let a later delete retry.
				 * The locked rows are the latest versions, so recompute the
				 * merge inputs from them rather than from the unlocked read.
				 */
				PushActiveSnapshot(GetLatestSnapshot());
				{
					char tablename[64];
					merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
					sql = psprintf(
						"SELECT hash, tuple_count, is_leaf FROM ariabc_internal.%s"
						" WHERE partition_id = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4"
						" FOR UPDATE SKIP LOCKED",
						tablename);
				}
				spi_rc = SPI_execute_with_args(sql, 4, argtypes, values, NULL, false, 0);
				pfree(sql);
				PopActiveSnapshot();

				can_merge = (spi_rc == SPI_OK_SELECT && SPI_processed == (uint64) total_children);
				total_count = 0;
				for (i = 0; can_merge && i < SPI_processed; i++)
				{
					HeapTuple c_tup = SPI_tuptable->vals[i];
					TupleDesc c_td = SPI_tuptable->tupdesc;
					Datum h_d = SPI_getbinval(c_tup, c_td, 1, &isnull);
					bytea *h_b = DatumGetByteaPP(h_d);
					MerkleHash ch;

					if (!DatumGetBool(SPI_getbinval(c_tup, c_td, 3, &isnull)))
						can_merge = false;
					total_count += DatumGetInt32(SPI_getbinval(c_tup, c_td, 2, &isnull));
					memcpy(ch.data, VARDATA_ANY(h_b), MERKLE_HASH_BYTES);
					merkle_hash_xor(&merged_hash, &ch);
				}
				if (total_count > merge_thresh)
					can_merge = false;

				if (SPI_tuptable != NULL)
					SPI_freetuptable(SPI_tuptable);

				if (!can_merge)
				{
					pfree(lower_bytea);
					pfree(upper_bytea);
					return;
				}

				if (total_count == 0)
					merkle_hash_zero(&merged_hash);

				CommandCounterIncrement();
				PushActiveSnapshot(GetLatestSnapshot());
				{
					char tablename[64];
					merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
					sql = psprintf(
						"DELETE FROM ariabc_internal.%s"
						" WHERE partition_id = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4",
						tablename);
				}
				SPI_execute_with_args(sql, 4, argtypes, values, NULL, false, 0);
				pfree(sql);
				PopActiveSnapshot();

				{
					bytea *parent_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
					bytea *merged_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
					Oid upd_argtypes[5] = {INT4OID, BYTEAOID, INT2OID, BYTEAOID, INT2OID};
					Datum upd_values[5];

					SET_VARSIZE(parent_id_bytea, VARHDRSZ + 8);
					memcpy(VARDATA(parent_id_bytea), parent_node_id, 8);
					SET_VARSIZE(merged_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
					memcpy(VARDATA(merged_hash_bytea), merged_hash.data, MERKLE_HASH_BYTES);

					upd_values[0] = Int32GetDatum((int32) total_count);
					upd_values[1] = PointerGetDatum(merged_hash_bytea);
					upd_values[2] = Int16GetDatum((int16) partition_id);
					upd_values[3] = PointerGetDatum(parent_id_bytea);
					upd_values[4] = Int16GetDatum((int16) parent_prefix_len);

					CommandCounterIncrement();
					PushActiveSnapshot(GetLatestSnapshot());
					{
						char tablename[64];
						merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
						sql = psprintf(
							"UPDATE ariabc_internal.%s"
							"   SET is_leaf = true, tuple_count = $1, hash = $2"
							" WHERE partition_id = $3 AND node_id = $4 AND prefix_len = $5",
							tablename);
					}
					SPI_execute_with_args(sql, 5, upd_argtypes, upd_values, NULL, false, 1);
					pfree(sql);
					PopActiveSnapshot();
					if (SPI_tuptable != NULL)
						SPI_freetuptable(SPI_tuptable);

					pfree(parent_id_bytea);
					pfree(merged_hash_bytea);
					merkle_route_cache_clear_partition(index_oid, partition_id);
					CommandCounterIncrement();
				}

				if (parent_prefix_len > 0)
				{
					do_merge_check(index_oid, partition_id, parent_node_id, parent_prefix_len, merge_thresh);
				}
			}
		}

		pfree(lower_bytea);
		pfree(upper_bytea);
	}
}

typedef struct {
	Oid index_oid;
	int partition_id;
	uint8 node_id[8];
	int prefix_len;
	bool is_split;
	int split_thresh;
	int merge_thresh;
} PendingSplitMerge;

#define MAX_PENDING_SPLIT_MERGE 1024
static PendingSplitMerge pending_sm[MAX_PENDING_SPLIT_MERGE];
static int num_pending_sm = 0;

uint64
merkle_apply_until_internal(uint64 required_seq)
{
	/* Synchronous direct apply mode is always active; tree is current */
	return required_seq;
}

void
merkle_get_recovery_status(MerkleRecoveryStatusData *status)
{
	MemSet(status, 0, sizeof(*status));
	status->managed = true;
	status->state = MERKLE_STATE_READY;
}

void
merkle_require_fresh(void)
{
	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle root cannot be read after uncommitted table changes"),
				 errdetail("The current transaction has staged Merkle deltas that are not yet durable."),
				 errhint("Commit the transaction, then read or apply the Merkle root in a new transaction.")));
}

Datum
merkle_apply_until_sql(PG_FUNCTION_ARGS)
{
	int64 required_seq = PG_GETARG_INT64(0);
	PG_RETURN_INT64(required_seq);
}

Datum
merkle_recovery_status(PG_FUNCTION_ARGS)
{
	const char *json_status = "{\"state\":\"READY\",\"managed\":true,\"applied_seq\":0,"
							  "\"target_seq\":0,\"terminal_prefix_seq\":0,"
							  "\"highest_terminal_seq\":0,\"blocked_seq\":0,\"error\":null}";
	PG_RETURN_TEXT_P(cstring_to_text(json_status));
}

/*-------------------------------------------------------------------------
 * Synchronous Per-Transaction Merkle Apply Engine
 *-------------------------------------------------------------------------
 */

typedef struct MerkleSortedEntry {
	MerkleDeltaEntry *entry;
	int partition_id;
	const uint8 *routing_key;
} MerkleSortedEntry;

static inline const uint8 *
merkle_entry_routing_key(const MerkleDeltaEntry *entry)
{
	if (entry->key.event_type == MERKLE_DELTA_INSERT)
		return entry->key.new_key_hash;
	return entry->key.old_key_hash;
}

static int
merkle_sorted_entry_cmp(const void *a, const void *b)
{
	const MerkleSortedEntry *s1 = (const MerkleSortedEntry *) a;
	const MerkleSortedEntry *s2 = (const MerkleSortedEntry *) b;
	int cmp;

	/* 1. Global index order */
	if (s1->entry->key.index_oid != s2->entry->key.index_oid)
		return (s1->entry->key.index_oid < s2->entry->key.index_oid) ? -1 : 1;

	/* 2. Strict Partition ID order (guarantees deadlock-free catalog row locking) */
	if (s1->partition_id != s2->partition_id)
		return (s1->partition_id < s2->partition_id) ? -1 : 1;

	/* 3. Routing key order within partition */
	cmp = memcmp(s1->routing_key, s2->routing_key, 8);
	if (cmp != 0)
		return cmp;

	/* 4. Event type */
	if (s1->entry->key.event_type != s2->entry->key.event_type)
		return (s1->entry->key.event_type < s2->entry->key.event_type) ? -1 : 1;

	return 0;
}


/*
 * Most benchmark workloads repeatedly touch a small hot set of keys.  A
 * route remains valid until the cached leaf is split or merged; the leaf
 * UPDATE below is guarded by is_leaf=true, so a topology change turns into a
 * clean cache miss/re-route rather than allowing a stale path to be used.
 * Include the physical index identity so DROP/CREATE or REINDEX cannot reuse
 * a route from an older tree with the same catalog OID.
 */
#define MERKLE_ROUTE_CACHE_SLOTS 65536
typedef struct MerkleRouteCacheEntry
{
	bool valid;
	Oid index_oid;
	RelFileNode index_rnode;
	int partition_id;
	uint8 leaf_node_id[8];
	int leaf_prefix_len;
} MerkleRouteCacheEntry;

static MerkleRouteCacheEntry merkle_route_cache[MERKLE_ROUTE_CACHE_SLOTS];

static inline bool
merkle_key_matches_prefix(const uint8 *key, const uint8 *node_id, int prefix_len)
{
	int full_bytes = prefix_len / 8;
	int rem = prefix_len % 8;

	if (full_bytes > 0 && memcmp(key, node_id, full_bytes) != 0)
		return false;

	if (rem > 0)
	{
		uint8 mask = (uint8) (0xFF << (8 - rem));
		if ((key[full_bytes] & mask) != (node_id[full_bytes] & mask))
			return false;
	}

	return true;
}

static inline uint32
merkle_route_cache_hash(Oid index_oid, int partition_id, const uint8 *routing_key)
{
	uint32 pfx = ((uint32) routing_key[0] << 8) | (uint32) routing_key[1];
	uint32 hash = (index_oid * 2654435761U) ^ ((uint32) partition_id * 40503U) ^ pfx;
	return hash & (MERKLE_ROUTE_CACHE_SLOTS - 1);
}

static bool
merkle_route_cache_lookup(Oid index_oid, const RelFileNode *index_rnode,
						  int partition_id, const uint8 *routing_key,
						  uint8 *leaf_node_id, int *leaf_prefix_len)
{
	uint32 idx = merkle_route_cache_hash(index_oid, partition_id, routing_key);
	MerkleRouteCacheEntry *entry = &merkle_route_cache[idx];

	if (!entry->valid || entry->index_oid != index_oid ||
		entry->partition_id != partition_id ||
		!RelFileNodeEquals(entry->index_rnode, *index_rnode))
		return false;

	if (!merkle_key_matches_prefix(routing_key, entry->leaf_node_id, entry->leaf_prefix_len))
		return false;

	memcpy(leaf_node_id, entry->leaf_node_id, 8);
	*leaf_prefix_len = entry->leaf_prefix_len;
	return true;
}

static void
merkle_route_cache_store(Oid index_oid, const RelFileNode *index_rnode,
						 int partition_id, const uint8 *routing_key,
						 const uint8 *leaf_node_id, int leaf_prefix_len)
{
	uint32 idx = merkle_route_cache_hash(index_oid, partition_id, routing_key);
	MerkleRouteCacheEntry *entry = &merkle_route_cache[idx];

	entry->valid = true;
	entry->index_oid = index_oid;
	entry->index_rnode = *index_rnode;
	entry->partition_id = partition_id;
	memcpy(entry->leaf_node_id, leaf_node_id, 8);
	entry->leaf_prefix_len = leaf_prefix_len;
}

static void
merkle_route_cache_invalidate(Oid index_oid, int partition_id, const uint8 *routing_key)
{
	if (routing_key == NULL)
	{
		merkle_route_cache_clear_partition(index_oid, partition_id);
		return;
	}

	uint32 idx = merkle_route_cache_hash(index_oid, partition_id, routing_key);
	MerkleRouteCacheEntry *entry = &merkle_route_cache[idx];

	if (entry->valid && entry->index_oid == index_oid &&
		entry->partition_id == partition_id)
		entry->valid = false;
}

/* Cached column attribute numbers for ariabc_internal.merkle_node */
static int g_cat_att_is_leaf = -1;
static TupleDesc g_cached_cat_tupdesc = NULL;

/* Cached index relation metadata to avoid reading block 0 repeatedly */
static Oid g_cached_meta_index_oid = InvalidOid;
static RelFileNode g_cached_meta_rnode;
static int g_cached_fanout = 0;
static int g_cached_split_thresh = 0;
static int g_cached_merge_thresh = 0;
static int g_cached_bits_per_split = 0;

static inline void
merkle_init_cat_col_offsets(TupleDesc tupdesc)
{
	int i;
	if (g_cat_att_is_leaf >= 0 && g_cached_cat_tupdesc == tupdesc)
		return;

	g_cached_cat_tupdesc = tupdesc;
	g_cat_att_is_leaf = -1;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped)
			continue;
		if (strcmp(NameStr(attr->attname), "is_leaf") == 0)
		{
			g_cat_att_is_leaf = i + 1;
			break;
		}
	}
}

static void
merkle_route_cache_clear_partition(Oid index_oid, int partition_id)
{
	int i;
	for (i = 0; i < MERKLE_ROUTE_CACHE_SLOTS; i++)
	{
		MerkleRouteCacheEntry *entry = &merkle_route_cache[i];

		if (entry->valid && entry->index_oid == index_oid &&
			entry->partition_id == partition_id)
			entry->valid = false;
	}
}

static void
merkle_route_cache_clear_index(Oid index_oid)
{
	int i;
	for (i = 0; i < MERKLE_ROUTE_CACHE_SLOTS; i++)
	{
		MerkleRouteCacheEntry *entry = &merkle_route_cache[i];

		if (entry->valid && entry->index_oid == index_oid)
			entry->valid = false;
	}
	if (g_cached_meta_index_oid == index_oid)
		g_cached_meta_index_oid = InvalidOid;
}

static void
merkle_sync_prepare_plans(Oid index_oid)
{
	MerklePlanCacheEntry *plans = merkle_get_plans(index_oid);
	Oid route_argtypes[3] = {INT2OID, BYTEAOID, INT2OID};
	Oid leaf_argtypes[5] = {BYTEAOID, INT4OID, INT2OID, BYTEAOID, INT2OID};
	Oid ancestor_argtypes[5] = {BYTEAOID, INT4OID, INT2OID, BYTEAOID, INT2OID};
	char tablename[64];
	char *sql;
	SPIPlanPtr plan;

	merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));

	if (plans->sync_route_plan == NULL ||
		!SPI_plan_is_valid(plans->sync_route_plan))
	{
		sql = psprintf(
			"SELECT is_leaf"
			"  FROM ariabc_internal.%s"
			" WHERE partition_id = $1 AND node_id = $2 AND prefix_len = $3",
			tablename);
		plan = SPI_prepare(sql, 3, route_argtypes);
		pfree(sql);
		if (plan == NULL || SPI_keepplan(plan) != 0)
			elog(ERROR, "SPI_prepare failed for synchronous Merkle route plan for index %u", index_oid);
		plans->sync_route_plan = plan;
	}

	if (plans->sync_leaf_update_plan == NULL ||
		!SPI_plan_is_valid(plans->sync_leaf_update_plan))
	{
		sql = psprintf(
			"UPDATE ariabc_internal.%s"
			"   SET hash = CASE WHEN tuple_count + $2 = 0 THEN '\\x0000000000000000000000000000000000000000000000000000000000000000'::bytea ELSE pg_catalog.merkle_hash_xor_sql(hash, $1) END,"
			"       tuple_count = tuple_count + $2"
			" WHERE partition_id = $3 AND node_id = $4 AND prefix_len = $5"
			"   AND is_leaf = true"
			"   AND tuple_count + $2 >= 0"
			" RETURNING tuple_count",
			tablename);
		plan = SPI_prepare(sql, 5, leaf_argtypes);
		pfree(sql);
		if (plan == NULL || SPI_keepplan(plan) != 0)
			elog(ERROR, "SPI_prepare failed for synchronous Merkle leaf plan for index %u", index_oid);
		plans->sync_leaf_update_plan = plan;
	}

	if (plans->sync_ancestor_update_plan == NULL ||
		!SPI_plan_is_valid(plans->sync_ancestor_update_plan))
	{
		sql = psprintf(
			"UPDATE ariabc_internal.%s"
			"   SET hash = CASE WHEN GREATEST(tuple_count + $2, 0) = 0 THEN '\\x0000000000000000000000000000000000000000000000000000000000000000'::bytea ELSE pg_catalog.merkle_hash_xor_sql(hash, $1) END,"
			"       tuple_count = GREATEST(tuple_count + $2, 0)"
			" WHERE partition_id = $3 AND node_id = $4 AND prefix_len = $5",
			tablename);
		plan = SPI_prepare(sql, 5, ancestor_argtypes);
		pfree(sql);
		if (plan == NULL || SPI_keepplan(plan) != 0)
			elog(ERROR, "SPI_prepare failed for synchronous Merkle ancestor plan for index %u", index_oid);
		plans->sync_ancestor_update_plan = plan;
	}

	if (plans->sync_check_count_plan == NULL ||
		!SPI_plan_is_valid(plans->sync_check_count_plan))
	{
		sql = psprintf(
			"SELECT tuple_count"
			"  FROM ariabc_internal.%s"
			" WHERE partition_id = $1 AND node_id = $2 AND prefix_len = $3 AND is_leaf = true",
			tablename);
		plan = SPI_prepare(sql, 3, route_argtypes);
		pfree(sql);
		if (plan == NULL || SPI_keepplan(plan) != 0)
			elog(ERROR, "SPI_prepare failed for synchronous Merkle check count plan for index %u", index_oid);
		plans->sync_check_count_plan = plan;
	}
}

static void
propagate_hash_to_ancestors_atomic(Oid index_oid, int partition_id,
								   const uint8 *leaf_node_id,
								   int leaf_prefix_len,
								   const MerkleHash *tuple_hash_delta,
								   int64 count_delta,
								   int bits_per_split)
{
	uint8 curr_node_id[8];
	int curr_prefix_len = leaf_prefix_len;
	MerklePlanCacheEntry *plans;

	memcpy(curr_node_id, leaf_node_id, 8);

	merkle_sync_prepare_plans(index_oid);
	plans = merkle_get_plans(index_oid);

	while (curr_prefix_len > 0)
	{
		uint8 parent_node_id[8];
		int parent_prefix_len = merkle_parent_of(parent_node_id, curr_node_id, curr_prefix_len, bits_per_split);
		Datum upd_values[5];
		bytea *delta_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		bytea *parent_bytea = (bytea *) palloc(VARHDRSZ + 8);
		int spi_rc;

		SET_VARSIZE(delta_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
		memcpy(VARDATA(delta_bytea), tuple_hash_delta->data, MERKLE_HASH_BYTES);

		SET_VARSIZE(parent_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(parent_bytea), parent_node_id, 8);

		upd_values[0] = PointerGetDatum(delta_bytea);
		upd_values[1] = Int32GetDatum((int32) count_delta);
		upd_values[2] = Int16GetDatum((int16) partition_id);
		upd_values[3] = PointerGetDatum(parent_bytea);
		upd_values[4] = Int16GetDatum((int16) parent_prefix_len);

		PushActiveSnapshot(GetLatestSnapshot());
		spi_rc = SPI_execute_plan(plans->sync_ancestor_update_plan,
								 upd_values, NULL, false, 1);
		PopActiveSnapshot();

		pfree(delta_bytea);
		pfree(parent_bytea);

		if (spi_rc != SPI_OK_UPDATE && spi_rc != SPI_OK_UPDATE_RETURNING)
			elog(ERROR, "propagate_hash_to_ancestors_atomic SPI update failed (rc=%d) for index %u", spi_rc, index_oid);

		if (SPI_processed == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle parent node disappeared while applying index %u",
							index_oid),
					 errdetail("parent prefix length=%d", parent_prefix_len)));

		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);

		memcpy(curr_node_id, parent_node_id, 8);
		curr_prefix_len = parent_prefix_len;
	}
}

static int
merkle_atomic_update_leaf(Oid index_oid, int partition_id,
						  const uint8 *leaf_node_id, int leaf_prefix_len,
						  const MerkleHash *tuple_hash_delta, int64 count_delta, int64 *new_count_out)
{
	Datum upd_values[5];
	bytea *delta_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
	bytea *node_bytea = (bytea *) palloc(VARHDRSZ + 8);
	int spi_rc;
	MerklePlanCacheEntry *plans;

	merkle_sync_prepare_plans(index_oid);
	plans = merkle_get_plans(index_oid);

	SET_VARSIZE(delta_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
	memcpy(VARDATA(delta_bytea), tuple_hash_delta->data, MERKLE_HASH_BYTES);

	SET_VARSIZE(node_bytea, VARHDRSZ + 8);
	memcpy(VARDATA(node_bytea), leaf_node_id, 8);

	upd_values[0] = PointerGetDatum(delta_bytea);
	upd_values[1] = Int32GetDatum((int32) count_delta);
	upd_values[2] = Int16GetDatum((int16) partition_id);
	upd_values[3] = PointerGetDatum(node_bytea);
	upd_values[4] = Int16GetDatum((int16) leaf_prefix_len);

	PushActiveSnapshot(GetLatestSnapshot());
	spi_rc = SPI_execute_plan(plans->sync_leaf_update_plan,
							 upd_values, NULL, false, 1);
	PopActiveSnapshot();

	pfree(delta_bytea);
	pfree(node_bytea);

	if (spi_rc == SPI_OK_UPDATE_RETURNING && SPI_processed == 1)
	{
		bool isnull;
		Datum count_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (new_count_out)
			*new_count_out = DatumGetInt32(count_datum);
		SPI_freetuptable(SPI_tuptable);
		return 1;
	}

	return 0;
}

static bool
merkle_node_is_leaf(Oid index_oid, int partition_id, const uint8 *node_id, int prefix_len)
{
	Datum values[3];
	bytea *node_bytea = (bytea *) palloc(VARHDRSZ + 8);
	int spi_rc;
	bool is_leaf = false;
	MerklePlanCacheEntry *plans;

	merkle_sync_prepare_plans(index_oid);
	plans = merkle_get_plans(index_oid);

	SET_VARSIZE(node_bytea, VARHDRSZ + 8);
	memcpy(VARDATA(node_bytea), node_id, 8);

	values[0] = Int16GetDatum((int16) partition_id);
	values[1] = PointerGetDatum(node_bytea);
	values[2] = Int16GetDatum((int16) prefix_len);

	PushActiveSnapshot(GetLatestSnapshot());
	spi_rc = SPI_execute_plan(plans->sync_route_plan,
								 values, NULL, false, 1);
	PopActiveSnapshot();

	if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		is_leaf = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
		SPI_freetuptable(SPI_tuptable);
	}

	pfree(node_bytea);
	return is_leaf;
}

#define MERKLE_ROUTE_MAX_RESTARTS 16

static int
merkle_resolve_route_leaf(Relation catalog_rel, Relation pkey_idx_rel, TupleTableSlot *slot,
						  Oid index_oid, int partition_id, const uint8 *routing_key,
						  uint8 *leaf_node_id, int *bits_per_split_out,
						  int *split_threshold_out, int *merge_threshold_out)
{
	uint8 node_id[8];
	int prefix_len = 0;
	int fanout;
	int split_threshold;
	int merge_threshold;
	int bits_per_split;
	RelFileNode index_rnode;
	int traversal_restarts = 0;

	/* Use cached index metadata to eliminate repetitive block 0 reads */
	if (g_cached_meta_index_oid != index_oid)
	{
		Relation index_rel = index_open(index_oid, AccessShareLock);
		merkle_read_meta(index_rel, &g_cached_fanout, &g_cached_split_thresh, &g_cached_merge_thresh, NULL);
		g_cached_meta_rnode = index_rel->rd_node;
		g_cached_bits_per_split = merkle_bits_per_split_for_fanout(g_cached_fanout);
		g_cached_meta_index_oid = index_oid;
		index_close(index_rel, AccessShareLock);
	}
	fanout = g_cached_fanout;
	(void) fanout;
	split_threshold = g_cached_split_thresh;
	merge_threshold = g_cached_merge_thresh;
	bits_per_split = g_cached_bits_per_split;
	index_rnode = g_cached_meta_rnode;

	if (bits_per_split_out)
		*bits_per_split_out = bits_per_split;
	if (split_threshold_out)
		*split_threshold_out = split_threshold;
	if (merge_threshold_out)
		*merge_threshold_out = merge_threshold;

	/* 1. Check PrefixRouteCache (expected >98% hit rate) */
	if (merkle_route_cache_lookup(index_oid, &index_rnode, partition_id, routing_key,
								  leaf_node_id, &prefix_len))
		return prefix_len;

restart_traversal:
	memset(node_id, 0, 8);
	prefix_len = 0;

	/* 2. Cache miss: traverse trie using direct B-tree scan without SPI */
	for (;;)
	{
		bool is_leaf = false;
		bool found_node = false;

		if (catalog_rel != NULL && pkey_idx_rel != NULL && slot != NULL)
		{
			ScanKeyData skey[3];
			bytea *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			IndexScanDesc iscan;

			SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(node_id_bytea), node_id, 8);

			ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum((int16) partition_id));
			ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_BYTEAEQ, PointerGetDatum(node_id_bytea));
			ScanKeyInit(&skey[2], 3, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum((int16) prefix_len));

			iscan = index_beginscan(catalog_rel, pkey_idx_rel, GetLatestSnapshot(), 3, 0);
			index_rescan(iscan, skey, 3, NULL, 0);
			ExecClearTuple(slot);
			found_node = index_getnext_slot(iscan, ForwardScanDirection, slot);
			if (found_node)
			{
				bool isnull;
				merkle_init_cat_col_offsets(slot->tts_tupleDescriptor);
				if (g_cat_att_is_leaf > 0)
				{
					Datum d = slot_getattr(slot, g_cat_att_is_leaf, &isnull);
					is_leaf = (!isnull && DatumGetBool(d));
				}
			}
			index_endscan(iscan);
			pfree(node_id_bytea);
		}
		else
		{
			/* Fallback to SPI if catalog_rel / pkey_idx_rel not provided */
			Datum values[3];
			bytea *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			int spi_rc;
			MerklePlanCacheEntry *plans;

			merkle_sync_prepare_plans(index_oid);
			plans = merkle_get_plans(index_oid);

			SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(node_id_bytea), node_id, 8);

			values[0] = Int16GetDatum((int16) partition_id);
			values[1] = PointerGetDatum(node_id_bytea);
			values[2] = Int16GetDatum((int16) prefix_len);

			PushActiveSnapshot(GetLatestSnapshot());
			spi_rc = SPI_execute_plan(plans->sync_route_plan, values, NULL, false, 1);
			PopActiveSnapshot();

			if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool isnull;
				found_node = true;
				is_leaf = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
				SPI_freetuptable(SPI_tuptable);
			}
			pfree(node_id_bytea);
		}

		if (!found_node)
		{
			if (prefix_len == 0)
			{
				Oid ins_argtypes[4] = {INT2OID, BYTEAOID, INT2OID, BYTEAOID};
				Datum ins_values[4];
				bytea *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
				bytea *zero_hash_bytea = (bytea *) palloc0(VARHDRSZ + MERKLE_HASH_BYTES);
				char *ins_sql;

				SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
				memcpy(VARDATA(node_id_bytea), node_id, 8);
				SET_VARSIZE(zero_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);

				ins_values[0] = Int16GetDatum((int16) partition_id);
				ins_values[1] = PointerGetDatum(node_id_bytea);
				ins_values[2] = Int16GetDatum(0);
				ins_values[3] = PointerGetDatum(zero_hash_bytea);

				{
					char tablename[64];
					merkle_get_node_tablename(index_oid, tablename, sizeof(tablename));
					ins_sql = psprintf(
						"INSERT INTO ariabc_internal.%s"
						" (partition_id, node_id, prefix_len, is_leaf, tuple_count, hash)"
						" VALUES ($1, $2, $3, true, 0, $4)"
						" ON CONFLICT (partition_id, node_id, prefix_len) DO NOTHING",
						tablename);
				}

				SPI_execute_with_args(ins_sql, 4, ins_argtypes, ins_values, NULL, false, 1);
				pfree(ins_sql);

				if (SPI_tuptable != NULL)
					SPI_freetuptable(SPI_tuptable);

				pfree(zero_hash_bytea);
				pfree(node_id_bytea);
				memcpy(leaf_node_id, node_id, 8);
				merkle_route_cache_store(index_oid, &index_rnode, partition_id, routing_key,
										 leaf_node_id, 0);
				return 0;
			}

			/*
			 * Each level is read with a fresh snapshot, so a merge that
			 * committed after we read this node's parent as internal has
			 * deleted it.  Retry from the root, which sees the merged leaf.
			 */
			if (++traversal_restarts <= MERKLE_ROUTE_MAX_RESTARTS)
				goto restart_traversal;
			elog(ERROR, "merkle_resolve_route_leaf node (index=%u, len=%d) not found", index_oid, prefix_len);
		}

		if (is_leaf)
		{
			memcpy(leaf_node_id, node_id, 8);
			merkle_route_cache_store(index_oid, &index_rnode, partition_id, routing_key,
									 leaf_node_id, prefix_len);
			return prefix_len;
		}
		else
		{
			uint8 bits = merkle_next_bits(routing_key, prefix_len, bits_per_split);
			uint8 next_node_id[8];
			merkle_bytea_extend(next_node_id, node_id, prefix_len, bits, bits_per_split);
			memcpy(node_id, next_node_id, 8);
			prefix_len += bits_per_split;
		}
	}
}

static int64
merkle_compute_advisory_lock_key(Oid index_oid, const uint8 *node_id, int prefix_len)
{
	uint64 h = (uint64) index_oid;
	int i;

	for (i = 0; i < 8; i++)
		h = (h * 31) + node_id[i];
	h = (h * 31) + (uint64) prefix_len;

	return (int64) h;
}

static int
merkle_pending_sm_cmp(const void *a, const void *b)
{
	const PendingSplitMerge *p1 = (const PendingSplitMerge *) a;
	const PendingSplitMerge *p2 = (const PendingSplitMerge *) b;
	int64 k1 = merkle_compute_advisory_lock_key(p1->index_oid, p1->node_id, p1->prefix_len);
	int64 k2 = merkle_compute_advisory_lock_key(p2->index_oid, p2->node_id, p2->prefix_len);

	if (k1 != k2)
		return (k1 < k2) ? -1 : 1;
	return 0;
}

static void
merkle_check_split_merge_guarded(Oid index_oid, int partition_id, const uint8 *node_id, int prefix_len,
								 int64 current_count, int split_thresh,
								 int merge_thresh, bool is_split)
{
	/* Same-leaf updates preserve tuple_count and can never cross a geometry
	 * threshold.  The caller only reaches this helper for count-changing
	 * events; keeping that invariant out of the hot update path avoids an
	 * advisory-lock probe for every UPDATE statement. */

	if (is_split)
	{
		if (current_count > split_thresh && prefix_len < MAX_PREFIX_LEN)
		{
			int64 lock_key = merkle_compute_advisory_lock_key(index_oid, node_id, prefix_len);
			DirectFunctionCall1(pg_advisory_xact_lock_int8, Int64GetDatum(lock_key));

			if (merkle_node_is_leaf(index_oid, partition_id, node_id, prefix_len))
			{
				do_split(index_oid, partition_id, node_id, prefix_len, current_count);
			}
		}
	}
	else
	{
		if (current_count <= merge_thresh && prefix_len > 0)
		{
			int64 lock_key = merkle_compute_advisory_lock_key(index_oid, node_id, prefix_len);
			DirectFunctionCall1(pg_advisory_xact_lock_int8, Int64GetDatum(lock_key));

			if (merkle_node_is_leaf(index_oid, partition_id, node_id, prefix_len))
			{
				do_merge_check(index_oid, partition_id, node_id, prefix_len, merge_thresh);
			}
		}
	}
}

static void
merkle_apply_single_coalesced_entry(Relation catalog_rel, Relation pkey_idx_rel,
									TupleTableSlot *slot,
									const MerkleDeltaEntry *entry,
									int partition_id,
									const uint8 *routing_key,
									int max_retries)
{
	Oid index_oid = entry->key.index_oid;
	int64 count_delta = 0;
	int attempt;
	bool applied = false;

	if (entry->key.event_type == MERKLE_DELTA_INSERT)
	{
		count_delta = 1;
	}
	else if (entry->key.event_type == MERKLE_DELTA_DELETE)
	{
		count_delta = -1;
	}
	else if (entry->key.event_type == MERKLE_DELTA_UPDATE_SAME_LEAF)
	{
		count_delta = 0;
	}
	else
	{
		elog(ERROR, "unrecognized Merkle delta event type: %u", entry->key.event_type);
	}

	for (attempt = 0; attempt < max_retries; attempt++)
	{
		uint8 leaf_node_id[8];
		int leaf_prefix_len;
		int bits_per_split;
		int split_thresh;
		int merge_thresh;
		int rows_updated;
		int64 new_count = 0;

		leaf_prefix_len = merkle_resolve_route_leaf(catalog_rel, pkey_idx_rel, slot,
										   index_oid, partition_id, routing_key,
										   leaf_node_id, &bits_per_split,
										   &split_thresh, &merge_thresh);
		rows_updated = merkle_atomic_update_leaf(index_oid, partition_id, leaf_node_id, leaf_prefix_len,
												 &entry->xor_delta, count_delta, &new_count);
		if (rows_updated == 1)
		{
			propagate_hash_to_ancestors_atomic(index_oid, partition_id, leaf_node_id, leaf_prefix_len,
											   &entry->xor_delta, count_delta, bits_per_split);
			/* Make the complete in-transaction node update visible to the
			 * split/merge guard with one CCI instead of one per ancestor. */
			CommandCounterIncrement();
			if (count_delta != 0)
			{
				bool needs_sm = false;
				bool is_split = false;

				if (count_delta > 0 && new_count > split_thresh && leaf_prefix_len < MAX_PREFIX_LEN)
				{
					needs_sm = true;
					is_split = true;
				}
				else if (count_delta < 0 && new_count <= merge_thresh && leaf_prefix_len > 0)
				{
					needs_sm = true;
					is_split = false;
				}

				if (needs_sm)
				{
					bool found = false;
					int k;
					for (k = 0; k < num_pending_sm; k++)
					{
						if (pending_sm[k].index_oid == index_oid &&
							pending_sm[k].partition_id == partition_id &&
							pending_sm[k].prefix_len == leaf_prefix_len &&
							memcmp(pending_sm[k].node_id, leaf_node_id, 8) == 0)
						{
							found = true;
							break;
						}
					}
					if (!found && num_pending_sm < MAX_PENDING_SPLIT_MERGE)
					{
						pending_sm[num_pending_sm].index_oid = index_oid;
						pending_sm[num_pending_sm].partition_id = partition_id;
						memcpy(pending_sm[num_pending_sm].node_id, leaf_node_id, 8);
						pending_sm[num_pending_sm].prefix_len = leaf_prefix_len;
						pending_sm[num_pending_sm].is_split = is_split;
						pending_sm[num_pending_sm].split_thresh = split_thresh;
						pending_sm[num_pending_sm].merge_thresh = merge_thresh;
						num_pending_sm++;
					}
				}
			}
			applied = true;
			break;
		}

		merkle_route_cache_invalidate(index_oid, partition_id, routing_key);
		if (!merkle_node_is_leaf(index_oid, partition_id, leaf_node_id, leaf_prefix_len))
		{
			/* Node split occurred during route resolution; retry route lookup */
			continue;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("Merkle index update failed: count delta %lld would make tuple_count negative for index %u",
							(long long) count_delta, index_oid)));
		}
	}

	if (!applied)
		elog(ERROR, "merkle_apply_single_coalesced_entry failed after %d retries for index %u", max_retries, index_oid);
}

typedef struct MerkleCoalescedNode
{
	int16		partition_id;
	int16		prefix_len;
	uint8		node_id[8];
	bool		is_leaf;
	bool		stale;			/* leaf was split/merged away before we locked it */
	MerkleHash	xor_delta;
	int64		count_delta;
	int			split_thresh;
	int			merge_thresh;
} MerkleCoalescedNode;

/* Current leaf route of one staged delta entry */
typedef struct MerkleEntryRoute
{
	uint8		node_id[8];
	int			prefix_len;
	int64		count_delta;
	bool		pending;		/* leaf delta not yet applied */
} MerkleEntryRoute;

#define MERKLE_LEAF_MAX_REROUTES 16

static int
merkle_coalesced_node_cmp(const void *a, const void *b)
{
	const MerkleCoalescedNode *n1 = (const MerkleCoalescedNode *) a;
	const MerkleCoalescedNode *n2 = (const MerkleCoalescedNode *) b;

	/* 1. Partition ID ascending */
	if (n1->partition_id != n2->partition_id)
		return (n1->partition_id < n2->partition_id) ? -1 : 1;

	/* 2. prefix_len descending: leaves first, then parents, then root.
	 *    Strict monotonic bottom-up order ensures all concurrent workers
	 *    lock nodes in the exact same sequence, preventing deadlocks. */
	if (n1->prefix_len != n2->prefix_len)
		return (n1->prefix_len > n2->prefix_len) ? -1 : 1;

	/* 3. node_id ascending */
	return memcmp(n1->node_id, n2->node_id, 8);
}

static int
merkle_coalesced_node_find_or_add(MerkleCoalescedNode **nodes_ptr, int *count_ptr, int *capacity_ptr,
								  int partition_id, int prefix_len, const uint8 *node_id,
								  bool is_leaf, int split_thresh, int merge_thresh)
{
	MerkleCoalescedNode *nodes = *nodes_ptr;
	int i;
	for (i = 0; i < *count_ptr; i++)
	{
		if (nodes[i].partition_id == partition_id &&
			nodes[i].prefix_len == prefix_len &&
			memcmp(nodes[i].node_id, node_id, 8) == 0)
		{
			if (is_leaf)
				nodes[i].is_leaf = true;
			return i;
		}
	}
	/* Add new */
	if (*count_ptr >= *capacity_ptr)
	{
		*capacity_ptr = (*capacity_ptr == 0) ? 64 : (*capacity_ptr * 2);
		*nodes_ptr = (*nodes_ptr == NULL) ? (MerkleCoalescedNode *) palloc(*capacity_ptr * sizeof(MerkleCoalescedNode))
										  : (MerkleCoalescedNode *) repalloc(*nodes_ptr, *capacity_ptr * sizeof(MerkleCoalescedNode));
		nodes = *nodes_ptr;
	}
	i = (*count_ptr)++;
	nodes[i].partition_id = (int16) partition_id;
	nodes[i].prefix_len = (int16) prefix_len;
	memcpy(nodes[i].node_id, node_id, 8);
	nodes[i].is_leaf = is_leaf;
	nodes[i].stale = false;
	merkle_hash_zero(&nodes[i].xor_delta);
	nodes[i].count_delta = 0;
	nodes[i].split_thresh = split_thresh;
	nodes[i].merge_thresh = merge_thresh;
	return i;
}

/*
 * merkle_direct_update_node
 *
 * Directly updates a leaf or ancestor Merkle node in the dedicated catalog table
 * using low-level B-tree index lookup and table_tuple_update() in C, completely
 * bypassing SPI, SQL query evaluation, and ProcArrayLock snapshot thrashing.
 *
 * If concurrent transactions updated the tuple while we waited, it traverses the
 * ctid chain (handling TM_Updated) and applies the commutative delta to the newest
 * tuple version, exactly mirroring EvalPlanQual in pure C.
 *
 * The lookup uses the latest snapshot, matching merkle_resolve_route_leaf():
 * under BCDB the serial gate is released before apply, so a predecessor may
 * commit a split after this transaction's snapshot was taken.  The route
 * then names a child node that the transaction snapshot cannot see.
 *
 * For a leaf, the route was resolved without holding a lock, so a concurrent
 * split (node is now internal) or merge (node deleted) can invalidate it.
 * Such a leaf is reported as MERKLE_NODE_UPDATE_STALE_LEAF without being
 * modified, so the caller can re-route its deltas.  Once the update succeeds,
 * the row lock prevents any other split/merge of this leaf until commit.
 */
typedef enum MerkleNodeUpdateResult
{
	MERKLE_NODE_UPDATE_OK,
	MERKLE_NODE_UPDATE_MISSING,
	MERKLE_NODE_UPDATE_STALE_LEAF
} MerkleNodeUpdateResult;

static MerkleNodeUpdateResult
merkle_direct_update_node(Relation catalog_rel, Relation pkey_idx_rel,
						  TupleTableSlot *scan_slot, TupleTableSlot *update_slot,
						  EState *estate,
						  int partition_id, const uint8 *node_id, int prefix_len,
						  bool is_leaf, const MerkleHash *xor_delta,
						  int64 count_delta, int64 *new_count_out)
{
	ScanKeyData skey[3];
	bytea *node_id_bytea;
	IndexScanDesc iscan;
	bool found = false;
	ItemPointerData cur_tid;
	TM_Result result;
	TM_FailureData tmfd;
	LockTupleMode lockmode = LockTupleExclusive;
	bool update_indexes = false;
	int retries = 0;
	TupleDesc td;
	Datum repl_values[6];
	bool repl_nulls[6];
	bool repl_do[6];
	bytea *new_hash_bytea;
	HeapTuple oldTup;
	HeapTuple newTup;
	bool isnull;
	Datum d_count;
	Datum d_hash;
	bytea *old_hash_bytea;
	int32 old_count;
	int64 calc_count;
	int32 new_count;
	MerkleHash old_hash;
	MerkleHash new_hash;

	td = RelationGetDescr(catalog_rel);

	node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
	SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
	memcpy(VARDATA(node_id_bytea), node_id, 8);

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum((int16) partition_id));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_BYTEAEQ, PointerGetDatum(node_id_bytea));
	ScanKeyInit(&skey[2], 3, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum((int16) prefix_len));

	iscan = index_beginscan(catalog_rel, pkey_idx_rel, GetLatestSnapshot(), 3, 0);
	index_rescan(iscan, skey, 3, NULL, 0);
	ExecClearTuple(scan_slot);
	found = index_getnext_slot(iscan, ForwardScanDirection, scan_slot);
	index_endscan(iscan);
	pfree(node_id_bytea);

	if (!found)
		return is_leaf ? MERKLE_NODE_UPDATE_STALE_LEAF : MERKLE_NODE_UPDATE_MISSING;

	cur_tid = scan_slot->tts_tid;

	for (;;)
	{
		if (is_leaf)
		{
			bool		leaf_now = false;

			merkle_init_cat_col_offsets(td);
			if (g_cat_att_is_leaf > 0)
			{
				Datum		d_leaf = slot_getattr(scan_slot, g_cat_att_is_leaf, &isnull);

				leaf_now = !isnull && DatumGetBool(d_leaf);
			}
			if (!leaf_now)
				return MERKLE_NODE_UPDATE_STALE_LEAF;
		}

		d_count = slot_getattr(scan_slot, 1, &isnull);
		old_count = isnull ? 0 : DatumGetInt32(d_count);

		d_hash = slot_getattr(scan_slot, 6, &isnull);
		if (!isnull)
		{
			old_hash_bytea = DatumGetByteaPP(d_hash);
			if (VARSIZE_ANY_EXHDR(old_hash_bytea) == MERKLE_HASH_BYTES)
				memcpy(old_hash.data, VARDATA_ANY(old_hash_bytea), MERKLE_HASH_BYTES);
			else
				merkle_hash_zero(&old_hash);
		}
		else
		{
			merkle_hash_zero(&old_hash);
		}

		calc_count = (int64) old_count + count_delta;
		new_count = (calc_count < 0) ? 0 : (int32) calc_count;

		if (new_count == 0)
		{
			merkle_hash_zero(&new_hash);
		}
		else
		{
			new_hash = old_hash;
			merkle_hash_xor(&new_hash, xor_delta);
		}

		memset(repl_values, 0, sizeof(repl_values));
		memset(repl_nulls, 0, sizeof(repl_nulls));
		memset(repl_do, 0, sizeof(repl_do));

		repl_values[0] = Int32GetDatum(new_count);
		repl_do[0] = true;

		new_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		SET_VARSIZE(new_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
		memcpy(VARDATA(new_hash_bytea), new_hash.data, MERKLE_HASH_BYTES);
		repl_values[5] = PointerGetDatum(new_hash_bytea);
		repl_do[5] = true;

		oldTup = ExecFetchSlotHeapTuple(scan_slot, false, NULL);
		newTup = heap_modify_tuple(oldTup, td, repl_values, repl_nulls, repl_do);
		pfree(new_hash_bytea);

		ExecClearTuple(update_slot);
		ExecStoreHeapTuple(newTup, update_slot, true);

		lockmode = LockTupleExclusive;
		memset(&tmfd, 0, sizeof(tmfd));
		result = table_tuple_update(catalog_rel, &cur_tid, update_slot,
									GetCurrentCommandId(true),
									GetActiveSnapshot(),
									InvalidSnapshot,
									true /* wait */,
									&tmfd, &lockmode,
									&update_indexes);

		if (result == TM_Ok)
		{
			if (unlikely(update_indexes))
			{
				/* Non-HOT update: update indexes pointing to the new heap tuple */
				if (estate != NULL)
				{
					ExecInsertIndexTuples(update_slot, estate, false, NULL, NIL);
					ResetPerTupleExprContext(estate);
				}
			}

			if (new_count_out)
				*new_count_out = new_count;
			return MERKLE_NODE_UPDATE_OK;
		}
		else if (result == TM_Deleted)
		{
			/* A concurrent merge removed this node while we waited on it. */
			return is_leaf ? MERKLE_NODE_UPDATE_STALE_LEAF : MERKLE_NODE_UPDATE_MISSING;
		}
		else if (result == TM_Updated)
		{
			retries++;
			if (retries > 256)
				elog(ERROR, "merkle_direct_update_node: max update retries exceeded for partition %d prefix %d",
					 partition_id, prefix_len);

			cur_tid = tmfd.ctid;
			ExecClearTuple(scan_slot);
			if (!table_tuple_fetch_row_version(catalog_rel, &cur_tid, SnapshotAny, scan_slot))
				elog(ERROR, "merkle_direct_update_node: failed to fetch updated tuple version at ctid (%u, %u)",
					 ItemPointerGetBlockNumber(&cur_tid), ItemPointerGetOffsetNumber(&cur_tid));
			continue;
		}
		else
		{
			elog(ERROR, "merkle_direct_update_node table_tuple_update failed with status: %u", result);
		}
	}
}

/*
 * Apply one coalesced node delta, through the direct catalog path when its
 * relations are open and through SPI otherwise.  new_count_out receives the
 * leaf's resulting tuple_count.
 */
static MerkleNodeUpdateResult
merkle_apply_coalesced_node(Relation catalog_rel, Relation pkey_idx_rel,
							TupleTableSlot *slot, TupleTableSlot *update_slot,
							EState *estate, MerklePlanCacheEntry *plans,
							Oid index_oid, const MerkleCoalescedNode *node,
							int64 *new_count_out)
{
	if (catalog_rel != NULL && pkey_idx_rel != NULL && slot != NULL && update_slot != NULL)
		return merkle_direct_update_node(catalog_rel, pkey_idx_rel,
										 slot, update_slot, estate,
										 node->partition_id, node->node_id,
										 node->prefix_len, node->is_leaf,
										 &node->xor_delta, node->count_delta,
										 new_count_out);

	/* Fallback to SPI if direct table relations are unavailable */
	if (node->is_leaf)
	{
		/* The leaf plan only matches rows that are still leaves. */
		int rows = merkle_atomic_update_leaf(index_oid, node->partition_id,
											 node->node_id, node->prefix_len,
											 &node->xor_delta, node->count_delta,
											 new_count_out);

		return (rows == 1) ? MERKLE_NODE_UPDATE_OK : MERKLE_NODE_UPDATE_STALE_LEAF;
	}
	else
	{
		Datum upd_values[5];
		bytea *delta_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		bytea *parent_bytea = (bytea *) palloc(VARHDRSZ + 8);
		int spi_rc;

		SET_VARSIZE(delta_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
		memcpy(VARDATA(delta_bytea), node->xor_delta.data, MERKLE_HASH_BYTES);

		SET_VARSIZE(parent_bytea, VARHDRSZ + 8);
		memcpy(VARDATA(parent_bytea), node->node_id, 8);

		upd_values[0] = PointerGetDatum(delta_bytea);
		upd_values[1] = Int32GetDatum((int32) node->count_delta);
		upd_values[2] = Int16GetDatum((int16) node->partition_id);
		upd_values[3] = PointerGetDatum(parent_bytea);
		upd_values[4] = Int16GetDatum((int16) node->prefix_len);

		PushActiveSnapshot(GetLatestSnapshot());
		spi_rc = SPI_execute_plan(plans->sync_ancestor_update_plan,
								  upd_values, NULL, false, 1);
		PopActiveSnapshot();

		pfree(delta_bytea);
		pfree(parent_bytea);

		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);
		return (spi_rc == SPI_OK_UPDATE || spi_rc == SPI_OK_UPDATE_RETURNING) ?
			MERKLE_NODE_UPDATE_OK : MERKLE_NODE_UPDATE_MISSING;
	}
}

/* Queue a split/merge check for a leaf whose count crossed a threshold. */
static void
merkle_queue_split_merge(Oid index_oid, const MerkleCoalescedNode *node, int64 new_count)
{
	bool is_split;
	int k;

	if (node->count_delta > 0 && new_count > node->split_thresh && node->prefix_len < MAX_PREFIX_LEN)
		is_split = true;
	else if (node->count_delta < 0 && new_count <= node->merge_thresh && node->prefix_len > 0)
		is_split = false;
	else
		return;

	/* A re-routed delta can land on a leaf already updated this transaction. */
	for (k = 0; k < num_pending_sm; k++)
	{
		if (pending_sm[k].index_oid == index_oid &&
			pending_sm[k].partition_id == node->partition_id &&
			pending_sm[k].prefix_len == node->prefix_len &&
			memcmp(pending_sm[k].node_id, node->node_id, 8) == 0)
			return;
	}

	if (num_pending_sm < MAX_PENDING_SPLIT_MERGE)
	{
		pending_sm[num_pending_sm].index_oid = index_oid;
		pending_sm[num_pending_sm].partition_id = node->partition_id;
		memcpy(pending_sm[num_pending_sm].node_id, node->node_id, 8);
		pending_sm[num_pending_sm].prefix_len = node->prefix_len;
		pending_sm[num_pending_sm].is_split = is_split;
		pending_sm[num_pending_sm].split_thresh = node->split_thresh;
		pending_sm[num_pending_sm].merge_thresh = node->merge_thresh;
		num_pending_sm++;
	}
}

static void
merkle_apply_staged_synchronous_impl(HTAB *combined_delta_map)
{
	HASH_SEQ_STATUS seq;
	MerkleDeltaEntry *entry;
	MerkleSortedEntry *sorted_entries;
	long num_entries;
	long i;
	Relation catalog_rel = NULL;
	Relation pkey_idx_rel = NULL;
	TupleTableSlot *slot = NULL;
	TupleTableSlot *update_slot = NULL;
	ResultRelInfo *rri = NULL;
	EState *estate = NULL;

	num_entries = hash_get_num_entries(combined_delta_map);
	if (num_entries == 0)
		return;

	sorted_entries = (MerkleSortedEntry *) palloc(num_entries * sizeof(MerkleSortedEntry));
	hash_seq_init(&seq, combined_delta_map);
	i = 0;
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		sorted_entries[i].entry = entry;
		sorted_entries[i].routing_key = merkle_entry_routing_key(entry);
		sorted_entries[i].partition_id = merkle_partition_for_routing_key(entry->key.index_oid, sorted_entries[i].routing_key);
		i++;
	}

	qsort(sorted_entries, num_entries, sizeof(MerkleSortedEntry), merkle_sorted_entry_cmp);

	num_pending_sm = 0;

	/* Open dedicated catalog relation and primary key index per index_oid */
	for (i = 0; i < num_entries; )
	{
		Oid curr_index_oid = sorted_entries[i].entry->key.index_oid;
		long start_idx = i;
		long end_idx;
		MerklePlanCacheEntry *plans = merkle_get_plans(curr_index_oid);
		MerkleCoalescedNode *coalesced_nodes = NULL;
		int num_coalesced = 0;
		int cap_coalesced = 0;

		while (i < num_entries && sorted_entries[i].entry->key.index_oid == curr_index_oid)
			i++;
		end_idx = i;

		catalog_rel = NULL;
		pkey_idx_rel = NULL;
		slot = NULL;
		update_slot = NULL;
		rri = NULL;
		estate = NULL;

		if (!OidIsValid(plans->catalog_relid))
		{
			merkle_ensure_node_table(curr_index_oid);
			plans->catalog_relid = merkle_get_node_table_relid(curr_index_oid);
			if (OidIsValid(plans->catalog_relid))
			{
				Relation tmp_rel = table_open(plans->catalog_relid, AccessShareLock);
				plans->pkey_idx_oid = RelationGetPrimaryKeyIndex(tmp_rel);
				table_close(tmp_rel, AccessShareLock);
			}
			if (!OidIsValid(plans->pkey_idx_oid))
			{
				char tablename[64];
				char pkey_idx_name[64];
				RangeVar *idx_rv;
				merkle_get_node_tablename(curr_index_oid, tablename, sizeof(tablename));
				snprintf(pkey_idx_name, sizeof(pkey_idx_name), "%s_pkey", tablename);
				idx_rv = makeRangeVar("ariabc_internal", pkey_idx_name, -1);
				plans->pkey_idx_oid = RangeVarGetRelid(idx_rv, NoLock, false);
			}
		}

		if (OidIsValid(plans->catalog_relid))
		{
			catalog_rel = table_open(plans->catalog_relid, RowExclusiveLock);
			if (OidIsValid(plans->pkey_idx_oid))
			{
				pkey_idx_rel = index_open(plans->pkey_idx_oid, RowExclusiveLock);
				slot = table_slot_create(catalog_rel, NULL);
				update_slot = MakeSingleTupleTableSlot(RelationGetDescr(catalog_rel), &TTSOpsHeapTuple);
				update_slot->tts_tableOid = RelationGetRelid(catalog_rel);

				estate = CreateExecutorState();
				rri = makeNode(ResultRelInfo);
				InitResultRelInfo(rri, catalog_rel, 1, NULL, 0);
				ExecOpenIndices(rri, false);
				estate->es_result_relations = rri;
				estate->es_num_result_relations = 1;
				estate->es_result_relation_info = rri;
			}
		}

		merkle_sync_prepare_plans(curr_index_oid);

		PG_TRY();
		{
			long n = end_idx - start_idx;
			long j;
			int bits_per_split = 0;
			int split_thresh = 0;
			int merge_thresh = 0;
			int round;
			MerkleEntryRoute *routes = (MerkleEntryRoute *) palloc(n * sizeof(MerkleEntryRoute));

			/* Phase 1: route every delta to the leaf that currently covers it. */
			for (j = 0; j < n; j++)
			{
				const MerkleDeltaEntry *delta_entry = sorted_entries[start_idx + j].entry;
				MerkleEntryRoute *route = &routes[j];

				if (delta_entry->key.event_type == MERKLE_DELTA_INSERT)
					route->count_delta = 1;
				else if (delta_entry->key.event_type == MERKLE_DELTA_DELETE)
					route->count_delta = -1;
				else if (delta_entry->key.event_type == MERKLE_DELTA_UPDATE_SAME_LEAF)
					route->count_delta = 0;
				else
					elog(ERROR, "unrecognized Merkle delta event type: %u", delta_entry->key.event_type);

				route->prefix_len = merkle_resolve_route_leaf(catalog_rel, pkey_idx_rel, slot,
															 curr_index_oid, sorted_entries[start_idx + j].partition_id,
															 sorted_entries[start_idx + j].routing_key,
															 route->node_id, &bits_per_split,
															 &split_thresh, &merge_thresh);
				route->pending = true;
			}

			/*
			 * Phase 2: update the leaves first, in canonical order.  Routes were
			 * resolved without locks, so a concurrent split/merge may have
			 * turned a leaf into an internal node or deleted it.  Such a leaf is
			 * left untouched; its deltas are re-routed against the committed
			 * tree and applied in the next round.  A successfully updated leaf
			 * stays locked (and therefore stays a leaf) until commit, which also
			 * pins its ancestors for phase 3.
			 */
			for (round = 0;; round++)
			{
				int num_stale = 0;

				num_coalesced = 0;
				for (j = 0; j < n; j++)
				{
					const MerkleDeltaEntry *delta_entry = sorted_entries[start_idx + j].entry;
					int leaf_idx;

					if (!routes[j].pending)
						continue;
					leaf_idx = merkle_coalesced_node_find_or_add(&coalesced_nodes, &num_coalesced, &cap_coalesced,
																sorted_entries[start_idx + j].partition_id,
																routes[j].prefix_len, routes[j].node_id,
																true, split_thresh, merge_thresh);
					merkle_hash_xor(&coalesced_nodes[leaf_idx].xor_delta, &delta_entry->xor_delta);
					coalesced_nodes[leaf_idx].count_delta += routes[j].count_delta;
				}

				if (num_coalesced > 1)
					qsort(coalesced_nodes, num_coalesced, sizeof(MerkleCoalescedNode), merkle_coalesced_node_cmp);

				for (long k = 0; k < num_coalesced; k++)
				{
					MerkleCoalescedNode *node = &coalesced_nodes[k];
					int64 new_count = 0;

					/* Net-zero deltas also cancel on every ancestor of this leaf. */
					if (node->count_delta == 0 && merkle_hash_is_zero(&node->xor_delta))
						continue;

					if (merkle_apply_coalesced_node(catalog_rel, pkey_idx_rel, slot, update_slot, estate,
													plans, curr_index_oid, node, &new_count) != MERKLE_NODE_UPDATE_OK)
					{
						node->stale = true;
						num_stale++;
						merkle_route_cache_invalidate(curr_index_oid, node->partition_id, NULL);
						continue;
					}

					if (node->count_delta != 0)
						merkle_queue_split_merge(curr_index_oid, node, new_count);
				}

				if (num_stale == 0)
					break;

				if (round >= MERKLE_LEAF_MAX_REROUTES)
					elog(ERROR, "Merkle leaf routing for index %u did not settle after %d re-routes",
						 curr_index_oid, round);

				/* Make this round's leaf updates visible to the re-route scans. */
				CommandCounterIncrement();

				for (j = 0; j < n; j++)
				{
					int leaf_idx;

					if (!routes[j].pending)
						continue;
					leaf_idx = merkle_coalesced_node_find_or_add(&coalesced_nodes, &num_coalesced, &cap_coalesced,
																sorted_entries[start_idx + j].partition_id,
																routes[j].prefix_len, routes[j].node_id,
																true, split_thresh, merge_thresh);
					if (!coalesced_nodes[leaf_idx].stale)
					{
						routes[j].pending = false;
						continue;
					}
					routes[j].prefix_len = merkle_resolve_route_leaf(catalog_rel, pkey_idx_rel, slot,
																	curr_index_oid, sorted_entries[start_idx + j].partition_id,
																	sorted_entries[start_idx + j].routing_key,
																	routes[j].node_id, &bits_per_split,
																	&split_thresh, &merge_thresh);
				}
			}

			/* Phase 3: propagate every delta to the ancestors of its final leaf. */
			num_coalesced = 0;
			for (j = 0; j < n; j++)
			{
				const MerkleDeltaEntry *delta_entry = sorted_entries[start_idx + j].entry;
				int partition_id = sorted_entries[start_idx + j].partition_id;
				uint8 curr_node_id[8];
				int curr_prefix_len = routes[j].prefix_len;

				memcpy(curr_node_id, routes[j].node_id, 8);
				while (curr_prefix_len > 0)
				{
					uint8 parent_node_id[8];
					int parent_prefix_len = merkle_parent_of(parent_node_id, curr_node_id, curr_prefix_len, bits_per_split);
					int parent_idx = merkle_coalesced_node_find_or_add(&coalesced_nodes, &num_coalesced, &cap_coalesced,
																	  partition_id, parent_prefix_len, parent_node_id,
																	  false, split_thresh, merge_thresh);
					merkle_hash_xor(&coalesced_nodes[parent_idx].xor_delta, &delta_entry->xor_delta);
					coalesced_nodes[parent_idx].count_delta += routes[j].count_delta;

					memcpy(curr_node_id, parent_node_id, 8);
					curr_prefix_len = parent_prefix_len;
				}
			}

			/* Sort ancestors in strict canonical order:
			 * Partition ID ASC -> prefix_len DESC -> node_id ASC */
			if (num_coalesced > 1)
				qsort(coalesced_nodes, num_coalesced, sizeof(MerkleCoalescedNode), merkle_coalesced_node_cmp);

			for (long k = 0; k < num_coalesced; k++)
			{
				MerkleCoalescedNode *node = &coalesced_nodes[k];

				if (node->count_delta == 0 && merkle_hash_is_zero(&node->xor_delta))
					continue;

				if (merkle_apply_coalesced_node(catalog_rel, pkey_idx_rel, slot, update_slot, estate,
												plans, curr_index_oid, node, NULL) != MERKLE_NODE_UPDATE_OK)
				{
					merkle_route_cache_invalidate(curr_index_oid, node->partition_id, NULL);
					elog(ERROR, "Merkle node update failed for index %u partition %d prefix %d",
						 curr_index_oid, node->partition_id, node->prefix_len);
				}
			}
			pfree(routes);
			CommandCounterIncrement();
		}
		PG_FINALLY();
		{
			if (coalesced_nodes != NULL)
			{
				pfree(coalesced_nodes);
				coalesced_nodes = NULL;
			}
			if (slot != NULL)
			{
				ExecDropSingleTupleTableSlot(slot);
				slot = NULL;
			}
			if (update_slot != NULL)
			{
				ExecDropSingleTupleTableSlot(update_slot);
				update_slot = NULL;
			}
			if (rri != NULL)
			{
				ExecCloseIndices(rri);
				pfree(rri);
				rri = NULL;
			}
			if (estate != NULL)
			{
				FreeExecutorState(estate);
				estate = NULL;
			}
			if (pkey_idx_rel != NULL)
			{
				index_close(pkey_idx_rel, RowExclusiveLock);
				pkey_idx_rel = NULL;
			}
			if (catalog_rel != NULL)
			{
				table_close(catalog_rel, RowExclusiveLock);
				catalog_rel = NULL;
			}
		}
		PG_END_TRY();
	}

	if (num_pending_sm > 0)
	{
		int k;
		CommandCounterIncrement();

		if (num_pending_sm > 1)
			qsort(pending_sm, num_pending_sm, sizeof(PendingSplitMerge), merkle_pending_sm_cmp);

		for (k = 0; k < num_pending_sm; k++)
		{
			Datum values[3];
			bytea *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			int spi_rc;
			MerklePlanCacheEntry *plans;

			merkle_sync_prepare_plans(pending_sm[k].index_oid);
			plans = merkle_get_plans(pending_sm[k].index_oid);

			SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(node_id_bytea), pending_sm[k].node_id, 8);
			values[0] = Int16GetDatum((int16) pending_sm[k].partition_id);
			values[1] = PointerGetDatum(node_id_bytea);
			values[2] = Int16GetDatum((int16) pending_sm[k].prefix_len);

			PushActiveSnapshot(GetLatestSnapshot());
			spi_rc = SPI_execute_plan(plans->sync_check_count_plan, values, NULL, false, 1);
			PopActiveSnapshot();

			if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool isnull;
				int32 latest_count = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
				SPI_freetuptable(SPI_tuptable);
				merkle_check_split_merge_guarded(pending_sm[k].index_oid,
												 pending_sm[k].partition_id,
												 pending_sm[k].node_id,
												 pending_sm[k].prefix_len,
												 latest_count,
												 pending_sm[k].split_thresh,
												 pending_sm[k].merge_thresh,
												 pending_sm[k].is_split);
			}
			else if (SPI_tuptable != NULL)
				SPI_freetuptable(SPI_tuptable);

			pfree(node_id_bytea);
		}
		num_pending_sm = 0;
	}

	pfree(sorted_entries);
}

void
merkle_apply_staged_synchronous_safe(HTAB *combined_delta_map)
{
	Oid save_userid;
	int save_sec_context;
	int save_xact_iso_level;
	int spi_rc;
	bool pushed_snapshot = false;

	if (combined_delta_map == NULL || hash_get_num_entries(combined_delta_map) == 0)
		return;

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
	{
		SetUserIdAndSecContext(save_userid, save_sec_context);
		if (pushed_snapshot)
			PopActiveSnapshot();
		elog(ERROR, "merkle_apply_staged_synchronous_safe SPI_connect failed: %d", spi_rc);
	}

	/*
	 * The BCDB worker already performs deterministic serial-equivalent
	 * conflict detection from the transaction read/write sets.  The rows in
	 * merkle_node are an internal commutative XOR aggregate, not application
	 * data: concurrent updates to the same ancestor are protected by normal
	 * row locking and are rolled back with the enclosing transaction.  Letting
	 * PostgreSQL SSI observe the route reads followed by the aggregate updates
	 * turns every hot ancestor into a false serialization-failure source and
	 * causes the whole user transaction to restart.  Keep the outer transaction
	 * and its snapshot intact, but suppress SSI checks while this internal
	 * maintenance is executed.  Restore the caller's isolation level on every
	 * exit path so the rest of the transaction retains its original contract.
	 */
	save_xact_iso_level = XactIsoLevel;
	XactIsoLevel = XACT_READ_COMMITTED;

	PG_TRY();
	{
		merkle_apply_staged_synchronous_impl(combined_delta_map);
	}
	PG_CATCH();
	{
		XactIsoLevel = save_xact_iso_level;
		SPI_finish();
		if (pushed_snapshot)
			PopActiveSnapshot();
		SetUserIdAndSecContext(save_userid, save_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	XactIsoLevel = save_xact_iso_level;

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "merkle_apply_staged_synchronous_safe SPI_finish failed");

	if (pushed_snapshot)
		PopActiveSnapshot();

	SetUserIdAndSecContext(save_userid, save_sec_context);
}
