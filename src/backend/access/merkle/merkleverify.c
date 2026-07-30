/*-------------------------------------------------------------------------
 *
 * merkleverify.c
 *    SQL-callable verification functions for Merkle index
 *
 * These functions allow users to verify the integrity of their data
 * by recomputing the Merkle tree from table data and comparing with
 * the stored tree.
 *
 * Copyright (c) 2026, Neel Parekh
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkleverify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"


PG_FUNCTION_INFO_V1(merkle_leaf_id);
PG_FUNCTION_INFO_V1(merkle_verify);
PG_FUNCTION_INFO_V1(merkle_verify_index);
PG_FUNCTION_INFO_V1(merkle_root_hash);
PG_FUNCTION_INFO_V1(merkle_root_hash_index);
PG_FUNCTION_INFO_V1(merkle_tree_stats);
PG_FUNCTION_INFO_V1(merkle_node_hash);
PG_FUNCTION_INFO_V1(merkle_leaf_tuples);
PG_FUNCTION_INFO_V1(merkle_bucket_for_key);
PG_FUNCTION_INFO_V1(merkle_get_node_hash);
PG_FUNCTION_INFO_V1(merkle_get_child_hashes);
PG_FUNCTION_INFO_V1(merkle_get_node_hashes);
PG_FUNCTION_INFO_V1(merkle_get_children_batch);
PG_FUNCTION_INFO_V1(merkle_get_leaf_members);
PG_FUNCTION_INFO_V1(merkle_get_partition_root_hash);
PG_FUNCTION_INFO_V1(merkle_get_partition_root_hashes);
PG_FUNCTION_INFO_V1(merkle_recovery_profile_reset);
PG_FUNCTION_INFO_V1(merkle_recovery_profile_stats);

/*
 * find_merkle_index() - Find the Merkle index on a table
 *
 * Returns the OID of the first Merkle index found, or InvalidOid if none.
 */
static Oid
find_merkle_index(Oid relid)
{
    Relation    rel;
    List       *indexList;
    ListCell   *lc;
    Oid         result = InvalidOid;
    char        relkind;
    
    /*
     * Verify that the OID refers to a regular table, not an index or other
     * relation type. This prevents confusing errors when users accidentally
     * pass an index OID instead of a table OID.
     */
    relkind = get_rel_relkind(relid);
    if (relkind == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("relation with OID %u does not exist", relid)));
    
    if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("\"%.128s\" is not a table",
                        get_rel_name(relid)),
                 errhint("Merkle verification functions expect a table OID, not an index or other object.")));
    
    rel = table_open(relid, AccessShareLock);
    indexList = RelationGetIndexList(rel);
    
    foreach(lc, indexList)
    {
        Oid         indexOid = lfirst_oid(lc);
        Relation    indexRel;

        indexRel = index_open(indexOid, AccessShareLock);
        
        /* Check if this is a Merkle index by checking access method */
        if (indexRel->rd_rel->relam == MERKLE_AM_OID)
        {
			if (OidIsValid(result))
			{
				index_close(indexRel, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("multiple Merkle indexes found on table \"%.128s\"",
								get_rel_name(relid)),
						 errhint("Use index-specific Merkle APIs instead of table-based ones.")));
			}
            result = indexOid;
        }
        
        index_close(indexRel, AccessShareLock);
    }
    
    list_free(indexList);
    table_close(rel, AccessShareLock);
    
    return result;
}

static Oid
resolve_merkle_index_arg(Oid relid)
{
    char relkind = get_rel_relkind(relid);

    if (relkind == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("relation with OID %u does not exist", relid)));

    if (relkind == RELKIND_INDEX)
    {
        Relation indexRel;
        Oid      result;

        indexRel = index_open(relid, AccessShareLock);
        if (indexRel->rd_rel->relam != MERKLE_AM_OID)
        {
            index_close(indexRel, AccessShareLock);
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("\"%.128s\" is not a merkle index",
                            get_rel_name(relid))));
        }
        result = RelationGetRelid(indexRel);
        index_close(indexRel, AccessShareLock);
        return result;
    }

    return find_merkle_index(relid);
}

/*
 * Stabilize both the protected heap and its Merkle index, then recheck the
 * durable freshness gate.  Terminalization changes the heap under a
 * RowExclusiveLock and the page applier changes the index, so this lock order
 * makes every localization helper linearize on one committed position.
 * The heap lock is intentionally held to transaction end; all SRFs
 * materialize their output before returning.
 */
static Relation
merkle_open_consistent_index(Oid index_oid)
{
	Oid heap_oid;
	Relation heap_rel;
	Relation index_rel;
	int save_policy;

	/* Temporarily force APPLY policy so pending deltas are automatically applied */
	save_policy = merkle_read_lag_policy;
	merkle_read_lag_policy = MERKLE_READ_LAG_APPLY;
	merkle_require_fresh();
	merkle_read_lag_policy = save_policy;

	heap_oid = IndexGetRelation(index_oid, false);
	heap_rel = table_open(heap_oid, ShareLock);
	/* Keep the ShareLock until transaction end, but release the relcache ref
	 * before opening/reading the index. */
	table_close(heap_rel, NoLock);
	index_rel = index_open(index_oid, ShareLock);
	if (index_rel->rd_rel->relam != MERKLE_AM_OID ||
		index_rel->rd_index->indrelid != heap_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation %u is not a Merkle index on relation %u",
						index_oid, heap_oid)));
	return index_rel;
}

/*
 * merkle_verify() - Verify Merkle tree integrity.
 *
 * This is the main user-facing verification tool. It performs a full audit:
 * 1. Scans the entire heap table (the actual data).
 * 2. Recomputes what the Merkle tree *should* look like based on that data.
 * 3. Compares this recomputed tree with the stored Merkle index.
 * 
 * Returns TRUE if everything matches exactly.
 * Logs WARNINGS for any specific node mismatches found.
 * 
 * Usage: SELECT merkle_verify('tablename');
 */
Datum
merkle_verify(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid indexOid = find_merkle_index(relid);

	if (!OidIsValid(indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no merkle index found on table %s",
						get_rel_name(relid))));

	return DirectFunctionCall1(merkle_verify_index, ObjectIdGetDatum(indexOid));
}

Datum
merkle_root_hash(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid indexOid = find_merkle_index(relid);

	if (!OidIsValid(indexOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no merkle index found on table %s",
						get_rel_name(relid))));

	return DirectFunctionCall1(merkle_root_hash_index, ObjectIdGetDatum(indexOid));
}

/*
 * merkle_verify_index() - P0.6: index-specific Merkle verification.
 *
 * Accepts a regclass argument that must resolve to a Merkle index directly.
 * This avoids the multi-index bug in merkle_verify() where only the first
 * Merkle index on a table was ever checked.
 *
 * Usage: SELECT merkle_verify_index('myindex'::regclass);
 */
Datum
merkle_verify_index(PG_FUNCTION_ARGS)
{
	Oid indexOid = PG_GETARG_OID(0);
	Oid heapOid;
	Relation heapRel;
	Relation indexRel;
	bool match = false;
	MerkleHash stored_root_hash;
	MerkleHash heap_tuple_xor_hash;
	bool found_catalog_leaves = false;
	int spi_rc;

	indexRel = merkle_open_consistent_index(indexOid);
	heapOid = IndexGetRelation(indexOid, false);
	heapRel = table_open(heapOid, AccessShareLock);

	merkle_hash_zero(&stored_root_hash);

	/* 1. Dynamic Index: Fetch root hash from catalog ariabc_internal.merkle_node */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Oid sel_types[1] = {OIDOID};
		Datum sel_vals[1] = {ObjectIdGetDatum(indexOid)};

		spi_rc = SPI_execute_with_args(
			"SELECT hash FROM ariabc_internal.merkle_node"
			" WHERE index_oid = $1 AND prefix_len = 0",
			1, sel_types, sel_vals, NULL, true, 1);

		if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
		{
			bool isnull;
			Datum h_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
			if (!isnull)
			{
				bytea *h_b = DatumGetByteaPP(h_d);
				memcpy(stored_root_hash.data, VARDATA_ANY(h_b), MERKLE_HASH_BYTES);
				found_catalog_leaves = true;
			}
		}
		SPI_finish();
	}

	/* 2. Recompute XOR sum of all heap tuples */
	{
		TupleTableSlot *slot = table_slot_create(heapRel, NULL);
		TableScanDesc scan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);

		merkle_hash_zero(&heap_tuple_xor_hash);
		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			MerkleHash th;
			merkle_compute_slot_hash(heapRel, slot, &th);
			merkle_hash_xor(&heap_tuple_xor_hash, &th);
		}
		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);
	}

	index_close(indexRel, NoLock);
	table_close(heapRel, NoLock);

	match = (memcmp(stored_root_hash.data, heap_tuple_xor_hash.data, MERKLE_HASH_BYTES) == 0);
	if (!match)
	{
		elog(WARNING, "merkle_verify_index mismatch: found_catalog=%d stored=%s heap_xor=%s",
			 found_catalog_leaves,
			 merkle_hash_to_hex(&stored_root_hash),
			 merkle_hash_to_hex(&heap_tuple_xor_hash));
	}
	PG_RETURN_BOOL(match);
}

Datum
merkle_root_hash_index(PG_FUNCTION_ARGS)
{
	Oid indexOid = PG_GETARG_OID(0);
	Relation indexRel;
	MerkleHash root_h;
	char *result = NULL;
	bool found = false;

	indexRel = merkle_open_consistent_index(indexOid);
	index_close(indexRel, NoLock);

	{
		int spi_rc;
		Oid argtypes[3] = {OIDOID, BYTEAOID, INT2OID};
		Datum values[3];
		bytea *zero_id_bytea = (bytea *) palloc0(VARHDRSZ + 8);

		SET_VARSIZE(zero_id_bytea, VARHDRSZ + 8);

		if (SPI_connect() == SPI_OK_CONNECT)
		{
			values[0] = ObjectIdGetDatum(indexOid);
			values[1] = PointerGetDatum(zero_id_bytea);
			values[2] = Int16GetDatum(0);

			spi_rc = SPI_execute_with_args(
				"SELECT hash FROM ariabc_internal.merkle_node"
				" WHERE index_oid = $1 AND node_id = $2 AND prefix_len = $3",
				3, argtypes, values, NULL, true, 1);

			if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool isnull;
				Datum hash_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
				if (!isnull)
				{
					bytea *h_b = DatumGetByteaPP(hash_d);
					memcpy(root_h.data, VARDATA_ANY(h_b), MERKLE_HASH_BYTES);
					found = true;
				}
			}
			SPI_finish();
		}
		pfree(zero_id_bytea);
	}

	if (found)
		result = merkle_hash_to_hex(&root_h);
	else
		result = pstrdup("0000000000000000000000000000000000000000000000000000000000000000");

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * merkle_tree_stats() - Return statistics about the Merkle tree
 *
 * Returns JSON with tree configuration and statistics.
 *
 * Usage: SELECT merkle_tree_stats('tablename');
 */
Datum
merkle_tree_stats(PG_FUNCTION_ARGS)
{
    StringInfoData  keybuf;
    int             totalNodes = 0;
    int             leafNodes = 0;
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        heapRel;
    Relation        indexRel;
    Buffer          metabuf;
    Page            metapage;
    MerkleMetaPageData *meta;
    StringInfoData  buf;
    int             nkeys;
    int             i;
    TupleDesc       heapTupdesc;
    int             fanout = 0;
	int             split_threshold = 0;
	int             merge_threshold = 0;
	MerkleRecoveryStatusData recovery_status;
	const char     *recovery_state_name;

	merkle_get_recovery_status(&recovery_status);
	switch (recovery_status.state)
	{
		case MERKLE_STATE_READY:
			recovery_state_name = "READY";
			break;
		case MERKLE_STATE_CATCHING_UP:
			recovery_state_name = "CATCHING_UP";
			break;
		case MERKLE_STATE_REBUILD_REQUIRED:
			recovery_state_name = "REBUILD_REQUIRED";
			break;
		default:
			recovery_state_name = "INVALID";
			break;
	}
    
    /* Find the Merkle index on this table */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    /* Open heap and index */
    heapRel = table_open(relid, AccessShareLock);
    indexRel = index_open(indexOid, AccessShareLock);
    heapTupdesc = RelationGetDescr(heapRel);

	/* Read metadata */
	merkle_read_meta(indexRel, &fanout, &split_threshold, &merge_threshold);
    
    /* Read metadata page directly for version fields */
    metabuf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
    LockBuffer(metabuf, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuf);
    meta = MerklePageGetMeta(metapage);
    
    /* Query dynamic catalog node counts via SPI */
    if (SPI_connect() == SPI_OK_CONNECT)
    {
        Oid sel_types[1] = {OIDOID};
        Datum sel_vals[1] = {ObjectIdGetDatum(indexOid)};
        int spi_rc;

        spi_rc = SPI_execute_with_args(
            "SELECT count(*), count(*) FILTER (WHERE is_leaf) FROM ariabc_internal.merkle_node WHERE index_oid = $1",
            1, sel_types, sel_vals, NULL, true, 0);

        if (spi_rc == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool isnull1, isnull2;
            Datum d1 = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull1);
            Datum d2 = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull2);
            if (!isnull1) totalNodes = (int) DatumGetInt64(d1);
            if (!isnull2) leafNodes = (int) DatumGetInt64(d2);
        }
        SPI_finish();
    }
    
    /* Get indexed column names from pg_index */
    nkeys = indexRel->rd_index->indnkeyatts;
    initStringInfo(&keybuf);
    appendStringInfoChar(&keybuf, '[');
    for (i = 0; i < nkeys; i++)
    {
        int16 attnum = indexRel->rd_index->indkey.values[i];
        const char *colname;
        
        if (i > 0)
            appendStringInfoString(&keybuf, ", ");
        
        if (attnum > 0 && attnum <= heapTupdesc->natts)
            colname = NameStr(TupleDescAttr(heapTupdesc, attnum - 1)->attname);
        else
            colname = "?";
        
        appendStringInfo(&keybuf, "\"%s\"", colname);
    }
    appendStringInfoChar(&keybuf, ']');
    
    /* Build JSON result */
    initStringInfo(&buf);
    appendStringInfo(&buf, 
                     "{\"version\": %u, "
                     "\"fanout\": %d, "
                     "\"split_threshold\": %d, "
                     "\"merge_threshold\": %d, "
                     "\"total_nodes\": %d, "
                     "\"leaf_nodes\": %d, "
                     "\"hash_bits\": %d, "
					 "\"route_format_version\": %u, "
					 "\"row_hash_format_version\": %u, "
					 "\"baseline_apply_seq\": %llu, "
					 "\"crash_recovery\": \"ordered_committed_delta_wal\", "
					 "\"recovery_state\": \"%s\", "
					 "\"applied_seq\": %llu, "
					 "\"target_seq\": %llu, "
					 "\"lag_items\": %llu, "
                     "\"index_keys\": %s}",
                     meta->version,
                     fanout,
                     split_threshold,
                     merge_threshold,
                     totalNodes,
                     leafNodes,
                     MERKLE_HASH_BITS,
					 meta->routeFormatVersion,
					 meta->rowHashFormatVersion,
					 (unsigned long long) meta->baselineApplySeq,
					 recovery_state_name,
					 (unsigned long long) recovery_status.applied_seq,
					 (unsigned long long) recovery_status.target_seq,
					 (unsigned long long)
						(recovery_status.target_seq > recovery_status.applied_seq ?
						 recovery_status.target_seq - recovery_status.applied_seq : 0),
                     keybuf.data);
    
    UnlockReleaseBuffer(metabuf);
    index_close(indexRel, AccessShareLock);
    table_close(heapRel, AccessShareLock);
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * merkle_node_hash() - Set-returning function to view all node hashes
 *
 * Returns a table of all nodes in the Merkle tree with their hashes.
 * Can be filtered with WHERE clause on nodeid column.
 *
 * Usage:
 *   SELECT * FROM merkle_node_hash('tablename'::regclass);
 *   SELECT * FROM merkle_node_hash('tablename'::regclass) WHERE nodeid = '10_4';
 *
 * Output columns: nodeid, partition, node_in_partition, is_leaf, hash
 */
/*
 * Data structure to hold pre-computed node information for SRF iteration.
 * Defined at file scope to avoid duplicate typedef in per-call section.
 */
typedef struct NodeHashData
{
    Datum  *nodeids;        /* "partition_nodeInPartition" formatted string */
    Datum  *partitions;     /* partition index */
    Datum  *nodeinpartitions;    /* 1-indexed node position within partition */
    bool   *isleafs;        /* true if this is a leaf node */
    Datum  *leafids;        /* global leaf ID (NULL for non-leaves) */
    bool   *leafidnulls;    /* true if leafid should be NULL */
    Datum  *hashes;         /* hex-formatted hash value */
} NodeHashData;

/*
 * merkle_node_hash() - Debugging tool to inspect tree state.
 *
 * This function returns the raw internal state of the Merkle tree.
 * It dumps every node, its ID, location, and current hash value.
 * 
 * Use this to pinpoint exactly *where* the tree is corrupt or to understand
 * the tree structure (partitions, leaves, parents).
 * 
 * It is a Set Returning Function (SRF), so query it like a table.
 *
 * Usage: SELECT * FROM merkle_node_hash('tablename');
 */
Datum
merkle_node_hash(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_node_hash is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}



/*
 * merkle_leaf_tuples() - Show tuple-to-leaf mapping (bucketing view)
 *
 * Scans the table and groups tuples by their Merkle leaf partition.
 * Supports multi-column indexes - shows composite keys.
 *
 * Usage:
 *   SELECT * FROM merkle_leaf_tuples('tablename'::regclass);
 *   SELECT * FROM merkle_leaf_tuples('tablename'::regclass) WHERE leaf_id = 50;
 *
 * Output columns: leaf_id, tuple_count, keys
 */
Datum
merkle_leaf_tuples(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_leaf_tuples is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_leaf_id(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_leaf_id is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_bucket_for_key(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_bucket_for_key is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_get_node_hash(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_node_hash is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_get_partition_root_hash(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_partition_root_hash is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_get_partition_root_hashes(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_partition_root_hashes is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_get_child_hashes(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_child_hashes is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}








static Tuplestorestate *
merkle_begin_materialized_srf(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext oldcontext;
	Tuplestorestate *tupstore;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize) || rsinfo->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode is required for this Merkle function")));

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	*tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = *tupdesc;
	MemoryContextSwitchTo(oldcontext);
	return tupstore;
}


/* Batched paired lookup: partitions[i], node_in_partitions[i]. */
Datum
merkle_get_node_hashes(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_node_hashes is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

/* Batched child lookup for paired parent coordinates. */
Datum
merkle_get_children_batch(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_children_batch is not supported for dynamic Merkle trees")));
}

/*
 * Selective leaf membership.  The generated predicate exactly matches the
 * supported functional B-tree bucket expression, allowing an index scan.
 */
Datum
merkle_get_leaf_members(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("merkle_get_leaf_members is not supported for dynamic Merkle trees")));
	PG_RETURN_NULL();
}

Datum
merkle_recovery_profile_reset(PG_FUNCTION_ARGS)
{
	MemSet(&merkle_recovery_profile_state, 0, sizeof(merkle_recovery_profile_state));
	merkle_recovery_profile_reset_generation++;
	PG_RETURN_VOID();
}

Datum
merkle_recovery_profile_stats(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	uint64		row_hash_compute_us;
	uint64		tree_path_update_us;
	uint64		row_hash_compute_ns;
	uint64		tree_path_update_ns;

	row_hash_compute_us =
		INSTR_TIME_GET_MICROSEC(merkle_recovery_profile_state.row_hash_compute_time);
	tree_path_update_us =
		INSTR_TIME_GET_MICROSEC(merkle_recovery_profile_state.tree_path_update_time);
	row_hash_compute_ns =
		(uint64) (INSTR_TIME_GET_DOUBLE(merkle_recovery_profile_state.row_hash_compute_time) * 1000000000.0);
	tree_path_update_ns =
		(uint64) (INSTR_TIME_GET_DOUBLE(merkle_recovery_profile_state.tree_path_update_time) * 1000000000.0);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{"
					 "\"schema_version\":1,"
					 "\"backend_pid\":%d,"
					 "\"reset_generation\":%llu,"
					 "\"enabled\":%s,"
					 "\"root_hash_helper_calls\":%llu,"
					 "\"root_hash_nodes_returned\":%llu,"
					 "\"root_hash_helper_us\":%llu,"
					 "\"child_hash_helper_calls\":%llu,"
					 "\"child_hash_nodes_returned\":%llu,"
					 "\"child_hash_helper_us\":%llu,"
					 "\"row_hash_compute_calls\":%llu,"
					 "\"row_hash_compute_us\":%llu,"
					 "\"row_hash_compute_ns\":%llu,"
					 "\"tree_path_update_calls\":%llu,"
					 "\"tree_path_nodes_touched\":%llu,"
					 "\"tree_path_update_us\":%llu,"
					 "\"tree_path_update_ns\":%llu"
					 "}",
					 MyProcPid,
					 (unsigned long long) merkle_recovery_profile_reset_generation,
					 merkle_recovery_profile_enabled ? "true" : "false",
					 (unsigned long long) merkle_recovery_profile_state.root_hash_helper_calls,
					 (unsigned long long) merkle_recovery_profile_state.root_hash_nodes_returned,
					 (unsigned long long) merkle_recovery_profile_state.root_hash_helper_us,
					 (unsigned long long) merkle_recovery_profile_state.child_hash_helper_calls,
					 (unsigned long long) merkle_recovery_profile_state.child_hash_nodes_returned,
					 (unsigned long long) merkle_recovery_profile_state.child_hash_helper_us,
					 (unsigned long long) merkle_recovery_profile_state.row_hash_compute_calls,
					 (unsigned long long) row_hash_compute_us,
					 (unsigned long long) row_hash_compute_ns,
					 (unsigned long long) merkle_recovery_profile_state.tree_path_update_calls,
					 (unsigned long long) merkle_recovery_profile_state.tree_path_nodes_touched,
					 (unsigned long long) tree_path_update_us,
					 (unsigned long long) tree_path_update_ns);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(merkle_get_descendants_batch);

Datum
merkle_get_descendants_batch(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	bytea *node_id = PG_GETARG_BYTEA_PP(1);
	int16 prefix_len = PG_GETARG_INT16(2);
	int32 max_depth = PG_GETARG_INT32(3);
	Oid indexOid = resolve_merkle_index_arg(relid);
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	StringInfoData query;
	int spi_rc;
	Oid argtypes[4] = {OIDOID, BYTEAOID, INT2OID, INT4OID};
	Datum args[4];
	int i;

	Relation indexRel;
	int fanout = DYNAMIC_MERKLE_FANOUT;
	int bits_per_split;

	indexRel = index_open(indexOid, AccessShareLock);
	merkle_read_meta(indexRel, &fanout, NULL, NULL);
	index_close(indexRel, AccessShareLock);
	bits_per_split = merkle_bits_per_split_for_fanout(fanout);

	tupstore = merkle_begin_materialized_srf(fcinfo, &tupdesc);

	initStringInfo(&query);
	appendStringInfo(&query,
		"WITH RECURSIVE tree AS ("
		"  SELECT node_id, prefix_len, is_leaf, tuple_count, hash, 0 AS depth"
		"    FROM ariabc_internal.merkle_node"
		"   WHERE index_oid = $1 AND node_id = $2 AND prefix_len = $3"
		"  UNION ALL"
		"  SELECT c.node_id, c.prefix_len, c.is_leaf, c.tuple_count, c.hash, p.depth + 1"
		"    FROM ariabc_internal.merkle_node c"
		"    JOIN tree p ON c.index_oid = $1"
		"               AND c.prefix_len = p.prefix_len + %d"
		"               AND (p.prefix_len = 0 OR (get_byte(c.node_id, (p.prefix_len - 1) / 8) >> (7 - ((p.prefix_len - 1) %% 8)) = get_byte(p.node_id, (p.prefix_len - 1) / 8) >> (7 - ((p.prefix_len - 1) %% 8)) AND (p.prefix_len < 8 OR substring(c.node_id from 1 for (p.prefix_len / 8)) = substring(p.node_id from 1 for (p.prefix_len / 8)))))"
		"   WHERE p.depth < $4"
		")"
		"SELECT node_id, prefix_len, is_leaf, hash"
		"  FROM tree"
		" ORDER BY prefix_len, node_id",
		bits_per_split);

	args[0] = ObjectIdGetDatum(indexOid);
	args[1] = PointerGetDatum(node_id);
	args[2] = Int16GetDatum(prefix_len);
	args[3] = Int32GetDatum(max_depth);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed in merkle_get_descendants_batch");

	spi_rc = SPI_execute_with_args(query.data, 4, argtypes, args, NULL, true, 0);
	if (spi_rc != SPI_OK_SELECT)
		elog(ERROR, "merkle_get_descendants_batch query failed: %d", spi_rc);

	for (i = 0; i < (int) SPI_processed; i++)
	{
		HeapTuple spi_tuple = SPI_tuptable->vals[i];
		bool isnull[4];
		Datum values[4];

		values[0] = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 1, &isnull[0]);
		values[1] = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 2, &isnull[1]);
		values[2] = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 3, &isnull[2]);
		values[3] = SPI_getbinval(spi_tuple, SPI_tuptable->tupdesc, 4, &isnull[3]);

		tuplestore_putvalues(tupstore, tupdesc, values, isnull);
	}

	SPI_finish();
	tuplestore_donestoring(tupstore);
	pfree(query.data);

	PG_RETURN_NULL();
}
