/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Merkle index build and initialization
 *
 * This file implements the index build functions that create a new
 * Merkle index from existing table data.
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merklebuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "catalog/pg_am_d.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "lib/stringinfo.h"

/*
 * Per-tuple callback state for index build
 */
typedef struct
{
    Relation    indexRel;
    Relation    heapRel;
	IndexFetchTableData *heapFetch;
	TupleTableSlot *heapSlot;
    double      indtuples;
    int         nkeys;          /* Number of index key columns */
    int         fanout;

	/* Dynamic index support */
	MerkleTupleHashEntry *entries;	/* in-memory tuples for dynamic build */
	int			max_entries;
	int			num_entries;
	int			bits_per_split;
	int			split_threshold;
	int			merge_threshold;
} MerkleBuildState;

static void merkle_emit_build_nodes_report(Relation indexRel,
                                          MerkleBuildState *buildstate);

static void
merkle_emit_build_nodes_report(Relation indexRel, MerkleBuildState *buildstate)
{
	/* Logging is disabled during index creation (CREATE INDEX / REINDEX) */
	(void) indexRel;
	(void) buildstate;
}

static int
merkle_entry_key_cmp(const void *a, const void *b)
{
	const MerkleTupleHashEntry *ea = (const MerkleTupleHashEntry *) a;
	const MerkleTupleHashEntry *eb = (const MerkleTupleHashEntry *) b;
	return memcmp(ea->key_hash, eb->key_hash, 8);
}

/*
 * merkle_build_callback() - Process one tuple during index build
 */
static void
merkle_build_callback(Relation indexRel,
                      ItemPointer tid,
                      Datum *values,
                      bool *isnull,
                      bool tupleIsAlive,
                      void *state)
{
    MerkleBuildState *buildstate = (MerkleBuildState *) state;
    MerkleHash      hash;
	MerkleRoute     route;

    /* Only process live tuples */
    if (!tupleIsAlive)
	{
        return;
	}
    
	/* Compute routing through the same relation-aware path used by DML. */
	merkle_compute_route(indexRel, values, isnull, buildstate->nkeys, &route);
    
	/*
	 * The heap index-build scan rewrites the TID of a live heap-only tuple to
	 * the root TID of its HOT chain.  Fetching that exact row version with
	 * table_tuple_fetch_row_version() therefore sees the dead root (and used
	 * to hash zero), even though values/isnull describe the live successor.
	 * Follow the HOT chain exactly as a normal index lookup does and hash the
	 * visible row image already selected by the build snapshot.
	 */
	{
		bool call_again = false;
		bool all_dead = false;

		ExecClearTuple(buildstate->heapSlot);
		if (!table_index_fetch_tuple(buildstate->heapFetch, tid, SnapshotSelf,
									 buildstate->heapSlot, &call_again,
									 &all_dead))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("could not fetch live heap row while building Merkle index"),
					 errdetail("Heap TID (%u,%u) did not resolve to a visible HOT-chain member.",
							   ItemPointerGetBlockNumber(tid),
							   ItemPointerGetOffsetNumber(tid))));

		merkle_compute_slot_hash(buildstate->heapRel, buildstate->heapSlot,
								 &hash);
		table_index_fetch_reset(buildstate->heapFetch);
	}
    
	/* Dynamic indexing tuple tracking */
	if (buildstate->num_entries >= buildstate->max_entries)
	{
		buildstate->max_entries *= 2;
		buildstate->entries = (MerkleTupleHashEntry *) repalloc(buildstate->entries, buildstate->max_entries * sizeof(MerkleTupleHashEntry));
	}

	memcpy(buildstate->entries[buildstate->num_entries].key_hash, route.route_digest, 8);
	memcpy(&buildstate->entries[buildstate->num_entries].tuple_hash, &hash, sizeof(MerkleHash));
	buildstate->num_entries++;

    buildstate->indtuples += 1;
}

/*
 * merkleBuild() - Build a new Merkle index
 *
 * This is called when CREATE INDEX is executed. We scan the entire
 * heap table and build the Merkle tree from all existing rows.
 */
IndexBuildResult *
merkleBuild(Relation heapRel, Relation indexRel, struct IndexInfo *indexInfo)
{
    IndexBuildResult   *result;
    MerkleBuildState    buildstate;
    double              reltuples;
    MerkleOptions      *opts;
    int                 totalLeaves;
	MerkleRecoveryStatusData recovery_status;

	buildstate.heapFetch = NULL;
	buildstate.heapSlot = NULL;
	MemSet(&recovery_status, 0, sizeof(recovery_status));

    PG_TRY();
    {
	if (heapRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes require a permanent logged table"),
				 errhint("Use a logged table; TEMP and UNLOGGED Merkle indexes are not supported.")));

	/*
	 * A rebuild scans the already-committed heap state.  Rebuilding while the
	 * ordered delta stream is behind would include those rows here and then
	 * XOR them a second time when the old backlog is replayed.  The same risk
	 * exists if this transaction staged DML against the pre-REINDEX relfilenode.
	 */
	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot build or reindex a Merkle index after table changes in the same transaction"),
				 errhint("Commit the table changes and synchronize Merkle recovery before rebuilding the index.")));
	merkle_get_recovery_status(&recovery_status);
	if (recovery_status.state != MERKLE_STATE_READY)
	{
		/* A failed/mismatched existing tree is repaired by an explicit
		 * non-concurrent REINDEX.  Do not allow CREATE INDEX to bypass a
		 * database-wide INVALID/REBUILD_REQUIRED gate, and never rebuild while
		 * the committed prefix is behind the heap. */
		if ((recovery_status.state == MERKLE_STATE_INVALID ||
			 recovery_status.state == MERKLE_STATE_REBUILD_REQUIRED) &&
			recovery_status.applied_seq == recovery_status.target_seq &&
			indexRel->rd_createSubid == InvalidSubTransactionId)
		{
			merkle_mark_recovery_state(MERKLE_STATE_READY, NULL);
			recovery_status.state = MERKLE_STATE_READY;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot build or reindex a Merkle index while committed deltas are pending"),
					 errdetail("state=%d applied_seq=%llu target_seq=%llu",
							   (int) recovery_status.state,
							   (unsigned long long) recovery_status.applied_seq,
							   (unsigned long long) recovery_status.target_seq),
					 errhint("Run REINDEX on the existing Merkle index after the committed prefix is caught up.")));
	}
    /* Get user-specified options or defaults */
    opts = merkle_get_options(indexRel);
    
    /*
     * Enforce single Merkle index per table
     */
    {
        List       *indexList;
        ListCell   *lc;
        Oid         currentIndexOid = RelationGetRelid(indexRel);

        indexList =    RelationGetIndexList(heapRel);
        foreach(lc, indexList)
        {
            Oid         indexOid = lfirst_oid(lc);
            Relation    otherIndexRel;

            /* Skip the index we are currently building */
            if (indexOid == currentIndexOid)
                continue;

            otherIndexRel = index_open(indexOid, AccessShareLock);
            if (otherIndexRel->rd_rel->relam == MERKLE_AM_OID)
            {
                index_close(otherIndexRel, AccessShareLock);
                list_free(indexList);
                ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("table \"%s\" already has a Merkle index",
                                RelationGetRelationName(heapRel)),
                         errhint("Only one Merkle index is allowed per table as it hashes the entire row.")));
            }
            index_close(otherIndexRel, AccessShareLock);
        }
        list_free(indexList);
    }

    /*
     * Initialize the index metadata page
     */
	merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts,
					 recovery_status.applied_seq);
    
    /*
     * Prepare in-memory entry tracking for dynamic build.
     */
    buildstate.fanout = opts->fanout;
	buildstate.split_threshold = opts->split_threshold;
	buildstate.merge_threshold = opts->merge_threshold;
	buildstate.max_entries = 1000000; /* Start with a large buffer */
	buildstate.entries = (MerkleTupleHashEntry *) palloc(buildstate.max_entries * sizeof(MerkleTupleHashEntry));
	buildstate.num_entries = 0;
	buildstate.bits_per_split = merkle_bits_per_split_for_fanout(buildstate.fanout);

    /* Free options after use */
    pfree(opts);
    
    /*
     * Prepare build state
     */
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
	buildstate.heapFetch = table_index_fetch_begin(heapRel);
	buildstate.heapSlot = table_slot_create(heapRel, NULL);
    
    /*
     * Scan the heap and build the index
     */
    reltuples = table_index_build_scan(heapRel, indexRel, indexInfo,
                                       true,   /* allow_sync */
                                       false,  /* progress */
                                       merkle_build_callback,
                                       (void *) &buildstate,
                                       NULL);  /* use heap scan */

	table_index_fetch_end(buildstate.heapFetch);
	buildstate.heapFetch = NULL;
	ExecDropSingleTupleTableSlot(buildstate.heapSlot);
	buildstate.heapSlot = NULL;

	merkle_emit_build_nodes_report(indexRel, &buildstate);

	/*
	 * Dynamic Merkle Tree catalog initialization.
	 * Build the dynamic tree strictly in memory and directly insert nodes
	 * into ariabc_internal.merkle_node.
	 */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Oid index_oid = RelationGetRelid(indexRel);
		uint8 zero_node_id[8];
		MerkleHash root_hash;
		int i;

		memset(zero_node_id, 0, 8);
		memset(&root_hash, 0, sizeof(MerkleHash));

		for (i = 0; i < buildstate.num_entries; i++)
			merkle_hash_xor(&root_hash, &buildstate.entries[i].tuple_hash);

		if (buildstate.num_entries > 1)
			qsort(buildstate.entries, buildstate.num_entries,
				  sizeof(MerkleTupleHashEntry), merkle_entry_key_cmp);

		if (buildstate.indtuples <= buildstate.split_threshold)
		{
			bytea *root_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			bytea *root_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
			Oid argtypes[5] = {OIDOID, BYTEAOID, INT2OID, INT8OID, BYTEAOID};
			Datum values[5];

			SET_VARSIZE(root_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(root_id_bytea), zero_node_id, 8);

			SET_VARSIZE(root_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
			memcpy(VARDATA(root_hash_bytea), root_hash.data, MERKLE_HASH_BYTES);

			values[0] = ObjectIdGetDatum(index_oid);
			values[1] = PointerGetDatum(root_id_bytea);
			values[2] = Int16GetDatum((int16) 0);
			values[3] = Int64GetDatum((int64) buildstate.indtuples);
			values[4] = PointerGetDatum(root_hash_bytea);

			SPI_execute_with_args(
				"INSERT INTO ariabc_internal.merkle_node"
				" (index_oid, node_id, prefix_len, is_leaf, tuple_count, hash)"
				" VALUES ($1, $2, $3, true, $4, $5)"
				" ON CONFLICT (index_oid, node_id, prefix_len) DO UPDATE"
				"   SET is_leaf = true, tuple_count = EXCLUDED.tuple_count, hash = EXCLUDED.hash",
				5, argtypes, values, NULL, false, 1);

			pfree(root_id_bytea);
			pfree(root_hash_bytea);
		}
		else
		{
			bytea *root_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			bytea *root_hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
			Oid argtypes[5] = {OIDOID, BYTEAOID, INT2OID, INT8OID, BYTEAOID};
			Datum values[5];

			SET_VARSIZE(root_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(root_id_bytea), zero_node_id, 8);

			SET_VARSIZE(root_hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
			memcpy(VARDATA(root_hash_bytea), root_hash.data, MERKLE_HASH_BYTES);

			values[0] = ObjectIdGetDatum(index_oid);
			values[1] = PointerGetDatum(root_id_bytea);
			values[2] = Int16GetDatum((int16) 0);
			values[3] = Int64GetDatum((int64) buildstate.indtuples);
			values[4] = PointerGetDatum(root_hash_bytea);

			SPI_execute_with_args(
				"INSERT INTO ariabc_internal.merkle_node"
				" (index_oid, node_id, prefix_len, is_leaf, tuple_count, hash)"
				" VALUES ($1, $2, $3, true, $4, $5)"
				" ON CONFLICT (index_oid, node_id, prefix_len) DO UPDATE"
				"   SET tuple_count = EXCLUDED.tuple_count, hash = EXCLUDED.hash",
				5, argtypes, values, NULL, false, 1);

			pfree(root_id_bytea);
			pfree(root_hash_bytea);

			merkle_do_split_in_memory(index_oid, zero_node_id, 0, buildstate.entries, buildstate.num_entries, buildstate.fanout, buildstate.bits_per_split, buildstate.split_threshold);
		}

		SPI_finish();
	}

	if (buildstate.entries)
		pfree(buildstate.entries);

    /*
     * Return statistics
     */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    }
    PG_CATCH();
    {
		if (buildstate.heapFetch != NULL)
			table_index_fetch_end(buildstate.heapFetch);
		if (buildstate.heapSlot != NULL)
			ExecDropSingleTupleTableSlot(buildstate.heapSlot);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result;
}

/*
 * merkleBuildempty() - Build an empty Merkle index
 *
 * PostgreSQL may call this AM hook while preparing an INIT fork.  v7 rejects
 * temporary and unlogged relations before reaching this path, but keeping the
 * initializer defensive makes an accidental non-permanent call fail closed.
 */
void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    MerkleMetaPageData *meta;
    MerkleOptions *opts;
    int         numPartitions;
    int         leavesPerPartition;
    int         fanout;
    int         nodesPerPartition;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    int         pageNum;
	MerkleRecoveryStatusData recovery_status;

	if (indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes cannot be created for TEMP or UNLOGGED relations"),
				 errhint("Use a permanent logged table.")));

	/* TRUNCATE/INIT-fork creation starts with an empty tree.  Its page
	 * watermarks must begin at the already-applied committed prefix; zero would
	 * make the applier replay historical deltas into the new empty relfilenode. */
	MemSet(&recovery_status, 0, sizeof(recovery_status));
	merkle_get_recovery_status(&recovery_status);
	if (recovery_status.managed &&
		recovery_status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot initialize a Merkle index while recovery is not READY"),
				 errdetail("state=%d applied_seq=%llu target_seq=%llu",
						   (int) recovery_status.state,
						   (unsigned long long) recovery_status.applied_seq,
						   (unsigned long long) recovery_status.target_seq)));

	/*
     * Construct metadata page using defaults
     */
    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
	/* Respect reloptions for the defensive INIT-fork path. */
    opts = merkle_get_options(indexRel);
    fanout = opts->fanout;

    if (fanout < 2 || fanout > 1024)
        fanout = MERKLE_DEFAULT_FANOUT;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;  /* Will be set on first insert */
    meta->fanout = fanout;
    meta->split_threshold = opts->split_threshold;
    meta->merge_threshold = opts->merge_threshold;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = recovery_status.managed ?
		recovery_status.applied_seq : 0;

    pfree(opts);
    
    /*
     * Make sure we have the smgr relation open
     */
    RelationOpenSmgr(indexRel);

    /*
     * Write metadata page
     */
    PageSetChecksumInplace(metapage, MERKLE_METAPAGE_BLKNO);
    smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_METAPAGE_BLKNO,
              (char *) metapage, true);
    log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                MERKLE_METAPAGE_BLKNO, metapage, true);
    
    /*
     * Sync to disk
     */
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
}
