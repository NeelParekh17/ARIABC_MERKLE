/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Merkle index build and initialization
 *
 * This file implements the index build functions that create a new
 * Merkle index from existing table data.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
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
    totalLeaves = opts->subtrees * opts->leaves_per_tree;
    
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
 * This is called when CREATE INDEX is executed on an empty table,
 * or during recovery when we need to create the init fork.
 */
void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    Page        treepage;
    MerkleMetaPageData *meta;
    MerkleNode *nodes;
    int         i;

    /*
     * Construct metadata page
     */
    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;  /* Will be set on first insert */
    meta->numSubtrees = MERKLE_NUM_SUBTREES;
    meta->leavesPerTree = MERKLE_LEAVES_PER_TREE;
    meta->nodesPerTree = MERKLE_NODES_PER_TREE;
    meta->totalNodes = MERKLE_TOTAL_NODES;
    
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
     * Construct tree node page
     */
    treepage = (Page) palloc(BLCKSZ);
    PageInit(treepage, BLCKSZ, 0);
    
    nodes = (MerkleNode *) PageGetContents(treepage);
    for (i = 0; i < MERKLE_TOTAL_NODES; i++)
    {
        nodes[i].nodeId = i;
        merkle_hash_zero(&nodes[i].hash);
    }
    
    /*
     * Write tree page
     */
    PageSetChecksumInplace(treepage, MERKLE_TREE_START_BLKNO);
    smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_TREE_START_BLKNO,
              (char *) treepage, true);
    log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                MERKLE_TREE_START_BLKNO, treepage, true);
    
    /*
     * Sync to disk
     */
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
    pfree(treepage);
}
