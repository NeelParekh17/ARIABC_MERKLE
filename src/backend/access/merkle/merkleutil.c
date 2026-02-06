/*-------------------------------------------------------------------------
 *
 * merkleutil.c
 *    Utility functions for Merkle tree operations
 *
 * This file contains helper functions for hash computation, XOR operations,
 * tree traversal, and page access.
 *
 * Copyright (c) 2026, Neel Parekh
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkleutil.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "common/blake3.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "access/xact.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "access/relation.h"

/*
 * Pending operation for transaction rollback
 */
typedef struct MerklePendingOp
{
    Oid         relid;
    int         leafId;
    MerkleHash  hash;
} MerklePendingOp;

static List *pendingOps = NIL;
static bool xactCallbackRegistered = false;

/*
 * merkle_xact_callback() - Handle transaction commit/abort
 *
 * If the transaction aborts, we must UNDO all changes made to the Merkle tree
 * because we updated the shared buffers directly without WAL-based rollback.
 * Since XOR is its own inverse (A ^ B ^ B = A), we simply re-apply the XORs.
 */
static void
merkle_xact_callback(XactEvent event, void *arg)
{
    ListCell *lc;

    if (event == XACT_EVENT_COMMIT)
    {
        list_free_deep(pendingOps);
        pendingOps = NIL;
    }
    else if (event == XACT_EVENT_ABORT)
    {
        ereport(NOTICE, (errmsg("merkle_xact_callback: ABORT detected, %d pending ops", list_length(pendingOps))));
        foreach(lc, pendingOps)
        {
            MerklePendingOp *op = (MerklePendingOp *) lfirst(lc);
            Relation        rel;
            
            rel = try_relation_open(op->relid, RowExclusiveLock);
            if (rel != NULL)
            {
                /* Read tree configuration from metadata */
                int         leavesPerPartition;
                int         nodesPerPartition;
                int         nodesPerPage;
                Buffer      metabuf;
                Page        metapage;
                MerkleMetaPageData *meta;
                
                metabuf = ReadBuffer(rel, MERKLE_METAPAGE_BLKNO);
                LockBuffer(metabuf, BUFFER_LOCK_SHARE);
                metapage = BufferGetPage(metabuf);
                meta = MerklePageGetMeta(metapage);
                leavesPerPartition = meta->leavesPerPartition;
                nodesPerPartition = meta->nodesPerPartition;
                nodesPerPage = meta->nodesPerPage;
                UnlockReleaseBuffer(metabuf);
                
                ereport(NOTICE, (errmsg("merkle_xact_callback: Undoing op for leaf %d hash %s", 
                     op->leafId, merkle_hash_to_hex(&op->hash))));
                     
                /* Undo tree update using dynamic values with multi-page support */
                {
                    int         partitionId = op->leafId / leavesPerPartition;
                    int         nodeInPartition = (op->leafId % leavesPerPartition) + leavesPerPartition;
                    int         nodeId = nodeInPartition + (partitionId * nodesPerPartition);
                    int         currentPageBlkno = -1;
                    Buffer      buf = InvalidBuffer;
                    Page        page = NULL;
                    MerkleNode *nodes = NULL;
                    
                    while (nodeInPartition > 0)
                    {
                        int         actualNodeIdx = nodeId - 1;
                        int         pageNum = actualNodeIdx / nodesPerPage;
                        int         idxInPage = actualNodeIdx % nodesPerPage;
                        BlockNumber blkno = MERKLE_TREE_START_BLKNO + pageNum;
                        
                        /* Switch pages if needed */
                        if ((int)blkno != currentPageBlkno)
                        {
                            if (BufferIsValid(buf))
                            {
                                MarkBufferDirty(buf);
                                UnlockReleaseBuffer(buf);
                            }
                            
                            buf = ReadBuffer(rel, blkno);
                            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                            page = BufferGetPage(buf);
                            nodes = (MerkleNode *) PageGetContents(page);
                            currentPageBlkno = blkno;
                        }
                        
                        merkle_hash_xor(&nodes[idxInPage].hash, &op->hash);
                        nodeInPartition = nodeInPartition / 2;
                        nodeId = nodeInPartition + (partitionId * nodesPerPartition);
                    }
                    
                    if (BufferIsValid(buf))
                    {
                        MarkBufferDirty(buf);
                        UnlockReleaseBuffer(buf);
                    }
                }

                relation_close(rel, RowExclusiveLock);
            }
            else
            {
                ereport(NOTICE, (errmsg("merkle_xact_callback: Could not open relation %u", op->relid)));
            }
        }
        
        list_free_deep(pendingOps);
        pendingOps = NIL;
    }
}

/*
 * merkle_hash_xor() - XOR two hashes together
 *
 * dest = dest XOR src
 * This is the core operation for Merkle tree updates.
 */
void
merkle_hash_xor(MerkleHash *dest, const MerkleHash *src)
{
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
        dest->data[i] ^= src->data[i];
}

/*
 * merkle_hash_zero() - Set hash to all zeros
 */
void
merkle_hash_zero(MerkleHash *hash)
{
    memset(hash->data, 0, MERKLE_HASH_BYTES);
}

/*
 * merkle_hash_is_zero() - Check if hash is all zeros
 */
bool
merkle_hash_is_zero(const MerkleHash *hash)
{
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
    {
        if (hash->data[i] != 0)
            return false;
    }
    return true;
}

/*
 * merkle_hash_to_hex() - Convert hash to hex string for display
 *
 * Returns a palloc'd string.
 */
char *
merkle_hash_to_hex(const MerkleHash *hash)
{
    char *result = palloc(MERKLE_HASH_BYTES * 2 + 1);
    int i;
    
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
        sprintf(result + (i * 2), "%02x", hash->data[i]);
    
    result[MERKLE_HASH_BYTES * 2] = '\0';
    return result;
}

/*
 * hex_char_to_int() - Convert hex character to integer value
 */
static int
hex_char_to_int(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    else if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    else if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    else
        return 0;
}

/*
 * merkle_compute_row_hash() - Compute the integrity hash for a single row.
 *
 * This function handles the "hashing the entire row" part of the Merkle index.
 * It iterates over every column in the heap tuple (row), converts it to its
 * text representation, concatenates them all with delimiters, and then computes
 * a BLAKE3 hash of this long string.
 *
 * The resulting 256-bit hash is what gets stored in the Merkle tree leaves.
 *
 * NOTE: This relies on the standard type output functions. If a type's output
 * logic changes, the hash will change, causing verification failures.
 */
void
merkle_compute_row_hash(Relation heapRel, ItemPointer tid, MerkleHash *result)
{
    TupleDesc       tupdesc;
    TupleTableSlot *slot;
    StringInfoData  buf;
    blake3_hasher   hasher;
    int             i;
    
    /*
     * CRITICAL FIX: Validate ItemPointer before attempting to fetch tuple.
     * Invalid TIDs (offset=0 or block=Invalid) can occur during BCDB operations 
     * and will cause fetch failures. Return zero hash for these cases.
     */
    if (!ItemPointerIsValid(tid) || 
        ItemPointerGetBlockNumberNoCheck(tid) == InvalidBlockNumber)
    {
        /* 
         * Changed from WARNING to DEBUG1 because these invalid TIDs are expected 
         * during optimistic BCDB worker operations and shouldn't spam the logs.
         */
        elog(DEBUG1, "merkle_compute_row_hash: skipping invalid tid (blk=%u, off=%u)",
             ItemPointerGetBlockNumberNoCheck(tid),
             ItemPointerGetOffsetNumberNoCheck(tid));
        merkle_hash_zero(result);
        return;
    }
    
    tupdesc = RelationGetDescr(heapRel);
    
    /* Create a slot to hold the tuple */
    slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);
    
    /* Ensure resource cleanup if an error occurs during processing */
    PG_TRY();
    {
        /*
         * Use SnapshotSelf to see our own uncommitted changes.
         * During INSERT, the tuple is in the heap but not yet committed,
         * so GetActiveSnapshot() won't see it.
         */
        if (!table_tuple_fetch_row_version(heapRel, tid, SnapshotSelf, slot))
        {
            /* Tuple not found, return zero hash */
            merkle_hash_zero(result);
        }
        else
        {
            /* Build concatenated string of all column values */
            initStringInfo(&buf);
            
            for (i = 0; i < tupdesc->natts; i++)
            {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                Datum       val;
                bool        isnull;
                Oid         typoutput;
                bool        typIsVarlena;
                char       *str;
                
                /* Skip dropped columns */
                if (attr->attisdropped)
                    continue;
                
                val = slot_getattr(slot, i + 1, &isnull);
                
                if (isnull)
                {
                    appendStringInfoString(&buf, "*null*");
                }
                else
                {
                    /* Get output function for this type */
                    getTypeOutputInfo(attr->atttypid, &typoutput, &typIsVarlena);
                    str = OidOutputFunctionCall(typoutput, val);
                    appendStringInfo(&buf, "*%s", str);
                    pfree(str);
                }
            }
            
            /*
             * Compute BLAKE3 hash - produces 32 bytes (256 bits) directly
             * BLAKE3 is faster than MD5 and cryptographically secure
             */
            blake3_hasher_init(&hasher);
            blake3_hasher_update(&hasher, buf.data, buf.len);
            blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
            
            pfree(buf.data);
        }
    }
    PG_CATCH();
    {
        /* Clean up slot even on error to prevent leaks */
        ExecDropSingleTupleTableSlot(slot);
        PG_RE_THROW();
    }
    PG_END_TRY();

    ExecDropSingleTupleTableSlot(slot);
}

/*
 * merkle_compute_partition_id_single() - Internal helper for single-key partition
 *
 * Uses modular arithmetic to distribute keys across leaves:
 * pid = (key * TREE_BASE) % TOTAL_LEAVES
 */
static int
merkle_compute_partition_id_single(Datum key, Oid keytype, int numLeaves)
{
    int64   keyval;
    int     pid;
    
    /*
     * Convert key to integer for partition calculation.
     * For non-integer types, we hash the key value.
     */
    switch (keytype)
    {
        case INT2OID:
            keyval = DatumGetInt16(key);
            break;
        case INT4OID:
            keyval = DatumGetInt32(key);
            break;
        case INT8OID:
            keyval = DatumGetInt64(key);
            break;
        default:
            {
                /*
                 * For other types, compute a hash of the output string
                 * and use that as the key value.
                 */
                Oid         typoutput;
                bool        typIsVarlena;
                char       *str;
                uint32      hash = 0;
                char       *p;
                
                getTypeOutputInfo(keytype, &typoutput, &typIsVarlena);
                str = OidOutputFunctionCall(typoutput, key);
                
                /* Simple string hash */
                for (p = str; *p != '\0'; p++)
                    hash = hash * 31 + (unsigned char) *p;
                
                pfree(str);
                keyval = (int64) hash;
            }
            break;
    }
    
    /* Ensure positive value */
    if (keyval < 0)
        keyval = -keyval;
    
    /* Compute partition ID */
    pid = (int) (keyval % numLeaves);
    
    return pid;
}

/*
 * merkle_compute_partition_id() - Determine which leaf a row maps to.
 *
 * This logic decides the "position" of a row in the Merkle tree.
 * The index key(s) are hashed (modulo total_leaves) to pick a leaf index.
 * 
 * - Single-key optimization: If the key is an integer, we use modular arithmetic directly
 *   for better distribution.
 * - Multi-key/Non-integer: We stringify the keys, hash them, and then modulo.
 *
 * Important: This mapping must be deterministic!
 */
int
merkle_compute_partition_id(Datum *values, bool *isnull, int nkeys,
                            TupleDesc tupdesc, int numLeaves)
{
    StringInfoData buf;
    uint64      hash = 0;
    char       *p;
    int         i;
    
    /* If only one key, use the optimized single-key path */
    if (nkeys == 1)
    {
        if (isnull[0])
            return 0;
        return merkle_compute_partition_id_single(values[0], 
                   TupleDescAttr(tupdesc, 0)->atttypid, numLeaves);
    }
    
    /* Build concatenated string of all key values */
    initStringInfo(&buf);
    
    for (i = 0; i < nkeys; i++)
    {
        if (isnull[i])
        {
            appendStringInfoString(&buf, "*null*");
        }
        else
        {
            Oid typoutput;
            bool typIsVarlena;
            char *str;
            Oid atttypid = TupleDescAttr(tupdesc, i)->atttypid;
            
            getTypeOutputInfo(atttypid, &typoutput, &typIsVarlena);
            str = OidOutputFunctionCall(typoutput, values[i]);
            appendStringInfo(&buf, "*%s*", str);
            pfree(str);
        }
    }
    
    /* Hash the combined string using djb2 algorithm */
    for (p = buf.data; *p != '\0'; p++)
        hash = hash * 33 + (unsigned char) *p;
    
    pfree(buf.data);
    
    return (int)(hash % numLeaves);
}

/*
 * merkle_update_tree_path() - Propagate a hash change up the tree.
 *
 * This function is the core tree maintenance routine. When a row is inserted or deleted:
 * 1. We identify which leaf it affects (`leafId`).
 * 2. We find the node corresponding to that leaf.
 * 3. We XOR the row's hash into that leaf node.
 * 4. We then move up to the parent node and XOR the hash there too.
 * 5. We repeat until we reach the root of the partition.
 *
 * XOR Property:
 *   Tree_Hash_New = Tree_Hash_Old XOR Row_Hash
 *   This works for both INSERT (adding the hash) and DELETE (removing the hash),
 *   because A XOR B XOR B = A.
 *
 * Multi-page support:
 *   The tree structure is flattened into an array of nodes spread across multiple
 *   database pages. This function handles the logic of calculating which page and offset
 *   a node resides in.
 */
void
merkle_update_tree_path(Relation indexRel, int leafId, MerkleHash *hash, bool isXorIn)
{
    int         partitionId;
    int         nodeInPartition;
    int         nodeId;
    int         leavesPerPartition;
    int         nodesPerPartition;
    int         nodesPerPage;
    int         currentPageBlkno = -1;
    Buffer      buf = InvalidBuffer;
    Page        page = NULL;
    MerkleNode *nodes = NULL;
    MemoryContext oldContext;
    MerklePendingOp *op;
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, NULL, &leavesPerPartition, &nodesPerPartition, NULL, NULL,
                          &nodesPerPage, NULL);
    
    /*
     * Register transaction callback if not done yet.
     * This ensures we can undo changes if the transaction aborts.
     */
    if (!xactCallbackRegistered)
    {
        RegisterXactCallback(merkle_xact_callback, NULL);
        xactCallbackRegistered = true;
    }
    
    /*
     * Record this operation in the pending list for potential rollback.
     * We use TopTransactionContext to ensure the list survives until commit/abort.
     */
    oldContext = MemoryContextSwitchTo(TopTransactionContext);
    
    op = (MerklePendingOp *) palloc(sizeof(MerklePendingOp));
    op->relid = RelationGetRelid(indexRel);
    op->leafId = leafId;
    op->hash = *hash;
    
    pendingOps = lappend(pendingOps, op);
    
    MemoryContextSwitchTo(oldContext);
    
    /* Calculate partition and node positions using dynamic values */
    partitionId = leafId / leavesPerPartition;
    nodeInPartition = (leafId % leavesPerPartition) + leavesPerPartition;
    nodeId = nodeInPartition + (partitionId * nodesPerPartition);
    
    /* Walk from leaf to root, XORing at each level */
    while (nodeInPartition > 0)
    {
        int         actualNodeIdx = nodeId - 1;  /* 0-based index */
        int         pageNum = actualNodeIdx / nodesPerPage;
        int         idxInPage = actualNodeIdx % nodesPerPage;
        BlockNumber blkno = MERKLE_TREE_START_BLKNO + pageNum;
        
        /* Switch pages if needed */
        if ((int)blkno != currentPageBlkno)
        {
            /* Release previous page if held */
            if (BufferIsValid(buf))
            {
                MarkBufferDirty(buf);
                UnlockReleaseBuffer(buf);
            }
            
            /* Read new page */
            buf = ReadBuffer(indexRel, blkno);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            nodes = (MerkleNode *) PageGetContents(page);
            currentPageBlkno = blkno;
        }
        
        /* XOR hash into this node */
        merkle_hash_xor(&nodes[idxInPage].hash, hash);
        
        /* Move to parent */
        nodeInPartition = nodeInPartition / 2;
        nodeId = nodeInPartition + (partitionId * nodesPerPartition);
    }
    
    /* Release the last page if held */
    if (BufferIsValid(buf))
    {
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * merkle_read_meta() - Read tree configuration from index metadata
 *
 * This reads the metadata page and returns the tree configuration.
 * Handles backward compatibility: if nodesPerPage is 0 (old format index),
 * we compute the values from the stored configuration.
 * 
 * Handles backward compatibility: if nodesPerPage is 0 (old format index),
 * we compute the values from the stored configuration.
 */
void
merkle_read_meta(Relation indexRel, int *numPartitions, int *leavesPerPartition,
                 int *nodesPerPartition, int *totalNodes, int *totalLeaves,
                 int *nodesPerPage, int *numTreePages)
{
    Buffer              buf;
    Page                page;
    MerkleMetaPageData *meta;
    int                 npp;    /* local nodesPerPage */
    int                 ntp;    /* local numTreePages */
    
    buf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    meta = MerklePageGetMeta(page);
    
    /* Read values from metadata */
    if (numPartitions)
        *numPartitions = meta->numPartitions;
    if (leavesPerPartition)
        *leavesPerPartition = meta->leavesPerPartition;
    if (nodesPerPartition)
        *nodesPerPartition = meta->nodesPerPartition;
    if (totalNodes)
        *totalNodes = meta->totalNodes;
    if (totalLeaves)
        *totalLeaves = meta->numPartitions * meta->leavesPerPartition;
    
    /* 
     * Backward compatibility: old indexes don't have nodesPerPage/numTreePages.
     * If nodesPerPage is 0 (old format), compute these values.
     */
    npp = meta->nodesPerPage;
    if (npp <= 0)
    {
        /* Old format index - compute from constants */
        npp = (int)MERKLE_MAX_NODES_PER_PAGE;
        ntp = (meta->totalNodes + npp - 1) / npp;  /* ceiling division */
    }
    else
    {
        ntp = meta->numTreePages;
    }
    
    if (nodesPerPage)
        *nodesPerPage = npp;
    if (numTreePages)
        *numTreePages = ntp;
    
    UnlockReleaseBuffer(buf);
}

/*
 * merkle_init_tree() - Initialize Merkle tree structure
 *
 * Creates metadata page and as many tree node pages as needed.
 * Uses the provided options or defaults if opts is NULL.
 * 
 * The tree can span multiple pages - no size limit!
 * 
 * Memory management: Caller should ensure opts is properly allocated
 * and freed after this call if needed.
 */
void
merkle_init_tree(Relation indexRel, Oid heapOid, MerkleOptions *opts)
{
    Buffer          metabuf;
    Page            metapage;
    MerkleMetaPageData *meta;
    int             numPartitions;
    int             leavesPerPartition;
    int             nodesPerPartition;
    int             totalNodes;
    int             nodesPerPage;
    int             numTreePages;
    int             nodeIdx;
    int             pageNum;
    
    /* Use provided options or defaults */
    if (opts != NULL)
    {
        numPartitions = opts->partitions;
        leavesPerPartition = opts->leaves_per_partition;
    }
    else
    {
        numPartitions = MERKLE_NUM_PARTITIONS;
        leavesPerPartition = MERKLE_LEAVES_PER_PARTITION;
    }
    
    /* Calculate derived values */
    nodesPerPartition = 2 * leavesPerPartition - 1;
    totalNodes = numPartitions * nodesPerPartition;
    nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
    numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;  /* ceiling division */
    
    /* Initialize metadata page */
    metabuf = ReadBuffer(indexRel, P_NEW);
    Assert(BufferGetBlockNumber(metabuf) == MERKLE_METAPAGE_BLKNO);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    metapage = BufferGetPage(metabuf);
    PageInit(metapage, BLCKSZ, 0);
    
    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = heapOid;
    meta->numPartitions = numPartitions;
    meta->leavesPerPartition = leavesPerPartition;
    meta->nodesPerPartition = nodesPerPartition;
    meta->totalNodes = totalNodes;
    meta->nodesPerPage = nodesPerPage;
    meta->numTreePages = numTreePages;
    
    MarkBufferDirty(metabuf);
    UnlockReleaseBuffer(metabuf);
    
    /* Initialize tree node pages - allocate as many as needed */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      treebuf;
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         i;
        
        treebuf = ReadBuffer(indexRel, P_NEW);
        LockBuffer(treebuf, BUFFER_LOCK_EXCLUSIVE);
        treepage = BufferGetPage(treebuf);
        PageInit(treepage, BLCKSZ, 0);
        
        /* Zero the entire page content area */
        nodes = (MerkleNode *) PageGetContents(treepage);
        memset(nodes, 0, BLCKSZ - MAXALIGN(SizeOfPageHeaderData));
        
        /* Calculate how many nodes go on this page */
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        /* Initialize nodes with their IDs */
        for (i = 0; i < nodesThisPage; i++)
        {
            nodes[i].nodeId = nodeIdx + i;
            /* hash is already zero from memset */
        }
        
        nodeIdx += nodesThisPage;
        
        MarkBufferDirty(treebuf);
        UnlockReleaseBuffer(treebuf);
    }
}

/* End of file */
