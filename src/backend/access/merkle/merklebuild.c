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
#include "access/xloginsert.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/index.h"
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
    int         numPartitions;
    int         leavesPerPartition;
    int         nodesPerPartition;
    int         fanout;
    int         internalNodes;  /* nodesPerPartition - leavesPerPartition */
    int         leafStart;      /* 1-indexed start position of leaves */
    int         totalLeaves;    /* numPartitions * leavesPerPartition */
    int         totalNodes;     /* numPartitions * nodesPerPartition */
    int         nodesPerPage;
    int         numTreePages;
    MerkleHash *nodeHashes;     /* per-node accumulated hashes (0-based) */
	bool        dynamic;
	int         dynamicMaxKeyBytes;
	MerkleDynamicBuildState *dynamicBuild;
} MerkleBuildState;

static void merkle_emit_build_nodes_report(Relation indexRel,
                                          MerkleBuildState *buildstate);

static void
merkle_emit_build_nodes_report(Relation indexRel, MerkleBuildState *buildstate)
{
    bool saved_is_bcdb_worker;
    int  partition;

    if (!merkle_update_detection)
        return;
    if (merkle_update_detection_suppress)
        return;
    if (buildstate == NULL || buildstate->nodeHashes == NULL)
        return;

    saved_is_bcdb_worker = is_bcdb_worker;

    PG_TRY();
    {
        StringInfoData out;
        bool first = true;

        is_bcdb_worker = false;

        initStringInfo(&out);

        for (partition = 0; partition < buildstate->numPartitions; partition++)
        {
            int base = partition * buildstate->nodesPerPartition;
            MerkleHash *h = &buildstate->nodeHashes[base];
            char       *hex;

            if (merkle_hash_is_zero(h))
                continue;

            hex = merkle_hash_to_hex(h);

            if (!first)
                appendStringInfoString(&out, " ");
            appendStringInfo(&out, "(%d, %s)", partition, hex);
            first = false;

            pfree(hex);
        }

        if (!first)
            ereport(NOTICE,
                    (errmsg("BCDB_MERKLE_ROOTS: %s", out.data)));

        pfree(out.data);

        is_bcdb_worker = saved_is_bcdb_worker;
    }
    PG_CATCH();
    {
        is_bcdb_worker = saved_is_bcdb_worker;
        FlushErrorState();
    }
    PG_END_TRY();
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
	MerkleItemIdentity identity;

    /* Only process live tuples */
    if (!tupleIsAlive)
	{
        return;
	}
    
	MemSet(&identity, 0, sizeof(identity));
	/* Compute routing through the same relation-aware path used by DML. */
	if (buildstate->dynamic)
	{
		merkle_compute_dynamic_item_identity(indexRel, values, isnull,
										 buildstate->nkeys,
										 buildstate->numPartitions,
										 buildstate->dynamicMaxKeyBytes,
										 &identity);
		route = identity.route;
	}
	else
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
    
    /*
     * Build optimization: during CREATE INDEX/REINDEX, avoid per-tuple buffer
     * traffic by accumulating XOR only at the leaf node in memory, then
     * constructing internal nodes once at the end.
     */
	if (buildstate->dynamic)
	{
		merkle_dynamic_build_add(buildstate->dynamicBuild, &identity, &hash);
		pfree(identity.key_data);
	}
	else
	{
		int partitionId = route.partition_id;
		int leafPos = route.leaf_id % buildstate->leavesPerPartition;
        int nodeInPartition = buildstate->leafStart + leafPos;
        int nodeIdx = partitionId * buildstate->nodesPerPartition + (nodeInPartition - 1);

        merkle_hash_xor(&buildstate->nodeHashes[nodeIdx], &hash);
    }
    
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
	buildstate.dynamic = false;
	buildstate.dynamicMaxKeyBytes = MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES;
	buildstate.dynamicBuild = NULL;
	MemSet(&recovery_status, 0, sizeof(recovery_status));

    PG_TRY();
    {
	if (heapRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes require a permanent logged table"),
				 errhint("Use a logged table; TEMP and UNLOGGED Merkle indexes are not supported.")));
	if (RelationGetIndexPredicate(indexRel) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("partial Merkle indexes are not supported"),
				 errdetail("Merkle integrity maintenance covers every live heap row and cannot safely skip predicate-false UPDATE or DELETE transitions."),
					 errhint("Create a non-partial Merkle index and REINDEX any legacy partial Merkle index.")));
	opts = merkle_get_options(indexRel);
	if (!opts->dynamic)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("native v8 Merkle indexes require dynamic=true"),
				 errhint("Create the index with the native dynamic Merkle options.")));
	if (opts->update_mode != MERKLE_UPDATE_SYNCHRONOUS_COW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pending-log Merkle indexes are no longer supported"),
				 errhint("REINDEX with update_mode=synchronous_cow.")));

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
	if (recovery_status.state != MERKLE_STATE_READY &&
		!(opts->dynamic &&
		  opts->update_mode == MERKLE_UPDATE_SYNCHRONOUS_COW))
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
	buildstate.dynamic = opts->dynamic;
	buildstate.dynamicMaxKeyBytes = opts->max_key_bytes;
    totalLeaves = opts->partitions * opts->leaves_per_partition;
    
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
     * Initialize the index storage with user-specified tree dimensions
     */
	merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts,
					 recovery_status.applied_seq);
    
    /*
     * Prepare in-memory node hash array for build accumulation.
     */
    buildstate.numPartitions = opts->partitions;
    buildstate.leavesPerPartition = opts->leaves_per_partition;
    buildstate.fanout = opts->fanout;
    buildstate.nodesPerPartition = (int) (((int64) buildstate.fanout * (int64) buildstate.leavesPerPartition - 1) /
                                          (buildstate.fanout - 1));
    buildstate.internalNodes = buildstate.nodesPerPartition - buildstate.leavesPerPartition;
    buildstate.leafStart = buildstate.internalNodes + 1;
    buildstate.totalLeaves = totalLeaves;
    buildstate.totalNodes = buildstate.numPartitions * buildstate.nodesPerPartition;
    buildstate.nodesPerPage = (int) MERKLE_MAX_NODES_PER_PAGE;
    buildstate.numTreePages = (buildstate.totalNodes + buildstate.nodesPerPage - 1) / buildstate.nodesPerPage;
	buildstate.nodeHashes = buildstate.dynamic ? NULL :
		(MerkleHash *) palloc0(sizeof(MerkleHash) * buildstate.totalNodes);

    /* Free options after use */
    pfree(opts);
    
    /*
     * Prepare build state
     */
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
	if (buildstate.dynamic)
		buildstate.dynamicBuild = merkle_dynamic_build_begin(indexRel, heapRel,
			buildstate.nkeys, recovery_status.applied_seq);
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

    /*
     * Finalize: compute internal nodes from leaves, then write the completed
     * Merkle tree to the index pages.
     */
	if (buildstate.dynamic)
	{
		merkle_dynamic_build_finish(buildstate.dynamicBuild);
		buildstate.dynamicBuild = NULL;
	}
	else
	{
        int partition;
        int nodeIdx = 0;
        int pageNum;

        /* Construct internal nodes per partition (children first). */
        for (partition = 0; partition < buildstate.numPartitions; partition++)
        {
            int base = partition * buildstate.nodesPerPartition;
            int i;

            for (i = buildstate.internalNodes; i >= 1; i--)
            {
                int child;
                int firstChildIdx = base + buildstate.fanout * (i - 1) + 1;
                MerkleHash h = buildstate.nodeHashes[firstChildIdx];

                for (child = 2; child <= buildstate.fanout; child++)
                    merkle_hash_xor(&h, &buildstate.nodeHashes[base + buildstate.fanout * (i - 1) + child]);

                buildstate.nodeHashes[base + (i - 1)] = h;
		}
	}

        /* Write nodes to index pages in on-disk layout order. */
        for (pageNum = 0; pageNum < buildstate.numTreePages; pageNum++)
        {
            Buffer      buf;
            Page        page;
            MerkleNode *nodes;
			MerklePageOpaqueData *opaque;
            int         nodesThisPage;
            int         i;
			int         pageContentBytes;

            nodesThisPage = Min(buildstate.nodesPerPage, buildstate.totalNodes - nodeIdx);

            buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            nodes = (MerkleNode *) PageGetContents(page);
			opaque = MerklePageGetOpaque(page);
			if (opaque->magic != MERKLE_PAGE_OPAQUE_MAGIC ||
				opaque->version != MERKLE_PAGE_OPAQUE_VERSION)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("invalid Merkle page opaque data during index build")));
			opaque->last_applied_seq = recovery_status.applied_seq;
			pageContentBytes = (int) ((char *) PageGetSpecialPointer(page) -
									  (char *) PageGetContents(page));

            for (i = 0; i < nodesThisPage; i++)
            {
                nodes[i].nodeId = nodeIdx + i;
                nodes[i].hash = buildstate.nodeHashes[nodeIdx + i];
            }

            if (nodesThisPage * (int)sizeof(MerkleNode) < pageContentBytes)
            {
                memset(((char *) nodes) + nodesThisPage * sizeof(MerkleNode), 0,
                       pageContentBytes - nodesThisPage * sizeof(MerkleNode));
            }

            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);

            nodeIdx += nodesThisPage;
        }
    }

	/*
	 * The bulk build dirties index pages without per-page WAL.  Log the final
	 * main-fork image, as the built-in GIN/GiST builders do, so an immediate
	 * crash after CREATE INDEX cannot recover an empty relation fork.
	 */
	if (RelationNeedsWAL(indexRel))
		log_newpage_range(indexRel, MAIN_FORKNUM, 0,
						  RelationGetNumberOfBlocks(indexRel), true);
    
    /*
     * Return statistics
     */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    
	if (!buildstate.dynamic)
		merkle_emit_build_nodes_report(indexRel, &buildstate);
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
    numPartitions = opts->partitions;
	leavesPerPartition = opts->dynamic ? 1 : opts->leaves_per_partition;
	fanout = opts->fanout;

    if (fanout < 2 || fanout > 1024)
        fanout = MERKLE_DEFAULT_FANOUT;

    nodesPerPartition = (int) (((int64) fanout * (int64) leavesPerPartition - 1) / (fanout - 1));

    nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
    totalNodes = numPartitions * nodesPerPartition;
    numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;  /* Will be set on first insert */
    meta->numPartitions = numPartitions;
    meta->leavesPerPartition = leavesPerPartition;
    meta->nodesPerPartition = nodesPerPartition;
    meta->totalNodes = totalNodes;
    meta->nodesPerPage = nodesPerPage;
    meta->numTreePages = numTreePages;
    meta->fanout = fanout;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = recovery_status.managed ?
		recovery_status.applied_seq : 0;
	if (opts->dynamic)
	{
		meta->dynamicMagic = MERKLE_DYNAMIC_META_MAGIC;
		meta->dynamicLayoutVersion = MERKLE_DYNAMIC_LAYOUT_VERSION;
		meta->dynamicFlags = 1;
		meta->dynamicLogicalFanout = opts->fanout;
		meta->dynamicLeafCapacity = opts->leaf_capacity;
		meta->dynamicMergeThreshold = opts->merge_threshold;
		meta->dynamicLeafByteCapacity = opts->leaf_byte_capacity;
		meta->dynamicMaxKeyBytes = opts->max_key_bytes;
		meta->nativeDirectoryStart = MERKLE_TREE_START_BLKNO;
		meta->nativeDirectoryPages = numTreePages;
		meta->nativeFormatFlags = MERKLE_NATIVE_MODE_SYNCHRONOUS_COW;
	}
	((PageHeader) metapage)->pd_lower =
		(LocationIndex) ((char *) meta + sizeof(*meta) - (char *) metapage);

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
     * Construct and write tree node pages
     */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         i;

        treepage = (Page) palloc(BLCKSZ);
		PageInit(treepage, BLCKSZ, MERKLE_PAGE_SPECIAL_SIZE);
		{
			MerklePageOpaqueData *opaque = MerklePageGetOpaque(treepage);

			opaque->magic = MERKLE_PAGE_OPAQUE_MAGIC;
			opaque->version = MERKLE_PAGE_OPAQUE_VERSION;
			opaque->flags = 0;
		opaque->last_applied_seq = recovery_status.managed ?
				recovery_status.applied_seq : 0;
		}
        
        nodes = (MerkleNode *) PageGetContents(treepage);
		memset(nodes, 0, (char *) PageGetSpecialPointer(treepage) -
						 (char *) nodes);
		((PageHeader) treepage)->pd_lower =
			(LocationIndex) ((char *) nodes +
							 nodesPerPage * sizeof(MerkleNode) -
							 (char *) treepage);
        
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        for (i = 0; i < nodesThisPage; i++)
        {
            nodes[i].nodeId = nodeIdx + i;
            merkle_hash_zero(&nodes[i].hash);
        }
        
        nodeIdx += nodesThisPage;

        PageSetChecksumInplace(treepage, MERKLE_TREE_START_BLKNO + pageNum);
        smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_TREE_START_BLKNO + pageNum,
                  (char *) treepage, true);
        log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                    MERKLE_TREE_START_BLKNO + pageNum, treepage, true);
        
        pfree(treepage);
    }
    
    /*
     * Sync to disk
     */
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
}
