/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Merkle index build and initialization
 *
 * This file implements the index build functions that create a new
 * Merkle index from existing table data.
 *
 * Copyright (c) 2026, Neel Parekh
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
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "catalog/pg_am_d.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Per-tuple callback state for index build
 */
typedef struct
{
    Relation    indexRel;
    Relation    heapRel;
    double      indtuples;
    int         nkeys;          /* Number of index key columns */
    int         totalLeaves;    /* Total leaves from options */
} MerkleBuildState;

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
    int             partitionId;
    TupleDesc       tupdesc;
    
    /* Only process live tuples */
    if (!tupleIsAlive)
        return;
    
    tupdesc = RelationGetDescr(indexRel);
    
    /* Compute partition ID using multi-column support and dynamic leaf count */
    partitionId = merkle_compute_partition_id(values, isnull,
                                                     buildstate->nkeys,
                                                     tupdesc,
                                                     buildstate->totalLeaves);
    
    /* Compute hash of the full row */
    merkle_compute_row_hash(buildstate->heapRel, tid, &hash);
    
    /* Update the Merkle tree path */
    merkle_update_tree_path(indexRel, partitionId, &hash, true);
    
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
    
    /* Get user-specified options or defaults */
    opts = merkle_get_options(indexRel);
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
    merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts);
    
    /* Free options after use */
    pfree(opts);
    
    /*
     * Prepare build state
     */
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
    buildstate.totalLeaves = totalLeaves;
    
    /*
     * Scan the heap and build the index
     */
    reltuples = table_index_build_scan(heapRel, indexRel, indexInfo,
                                       true,   /* allow_sync */
                                       false,  /* progress */
                                       merkle_build_callback,
                                       (void *) &buildstate,
                                       NULL);  /* use heap scan */
    
    /*
     * Return statistics
     */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    
    return result;
}

/*
 * merkleBuildempty() - Build an empty Merkle index
 *
 * This function is part of the PostgreSQL Index AM interface. It is specifically
 * called for UNLOGGED tables to create the 'initial fork' (INIT_FORKNUM).
 *
 * Unlogged tables do not write WAL, so on a crash/restart, PostgreSQL truncates
 * the index to the state created by this function. While AriaBC (blockchain) 
 * typically uses logged durable tables, this function is required for completeness
 * and to support UNLOGGED relations.
 */
void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    MerkleMetaPageData *meta;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    int         pageNum;

    /*
     * Construct metadata page using defaults
     */
    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
    nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
    totalNodes = MERKLE_TOTAL_NODES;
    numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;  /* Will be set on first insert */
    meta->numPartitions = MERKLE_NUM_PARTITIONS;
    meta->leavesPerPartition = MERKLE_LEAVES_PER_PARTITION;
    meta->nodesPerPartition = MERKLE_NODES_PER_PARTITION;
    meta->totalNodes = totalNodes;
    meta->nodesPerPage = nodesPerPage;
    meta->numTreePages = numTreePages;
    
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
        PageInit(treepage, BLCKSZ, 0);
        
        nodes = (MerkleNode *) PageGetContents(treepage);
        memset(nodes, 0, BLCKSZ - MAXALIGN(SizeOfPageHeaderData));
        
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
