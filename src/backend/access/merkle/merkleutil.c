/*-------------------------------------------------------------------------
 *
 * merkleutil.c
 *    Utility functions for Merkle tree operations
 *
 * This file contains helper functions for hash computation, XOR operations,
 * tree traversal, and page access.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
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
#include "common/md5.h"
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
                int         leavesPerTree;
                int         nodesPerTree;
                int         nodesPerPage;
                Buffer      metabuf;
                Page        metapage;
                MerkleMetaPageData *meta;
                
                metabuf = ReadBuffer(rel, MERKLE_METAPAGE_BLKNO);
                LockBuffer(metabuf, BUFFER_LOCK_SHARE);
                metapage = BufferGetPage(metabuf);
                meta = MerklePageGetMeta(metapage);
                leavesPerTree = meta->leavesPerTree;
                nodesPerTree = meta->nodesPerTree;
                nodesPerPage = meta->nodesPerPage;
                UnlockReleaseBuffer(metabuf);
                
                ereport(NOTICE, (errmsg("merkle_xact_callback: Undoing op for leaf %d hash %s", 
                     op->leafId, merkle_hash_to_hex(&op->hash))));
                     
                /* Undo tree update using dynamic values with multi-page support */
                {
                    int         treeId = op->leafId / leavesPerTree;
                    int         nodeInTree = (op->leafId % leavesPerTree) + leavesPerTree;
                    int         nodeId = nodeInTree + (treeId * nodesPerTree);
                    int         currentPageBlkno = -1;
                    Buffer      buf = InvalidBuffer;
                    Page        page = NULL;
                    MerkleNode *nodes = NULL;
                    
                    while (nodeInTree > 0)
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
                        nodeInTree = nodeInTree / 2;
                        nodeId = nodeInTree + (treeId * nodesPerTree);
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
 * merkle_compute_row_hash() - Compute hash for a table row
 *
 * This fetches the tuple from the heap and computes its MD5 hash,
 * then extracts the first 40 bits (5 bytes) as the Merkle hash.
 *
 * The row data is concatenated as: *col1*col2*col3*...
 * NULL values are represented as *null*
 */
void
merkle_compute_row_hash(Relation heapRel, ItemPointer tid, MerkleHash *result)
{
    TupleDesc       tupdesc;
    TupleTableSlot *slot;
    StringInfoData  buf;
    char            md5_result[33];  /* MD5 hex = 32 chars + null */
    int             i;
    bool            found;
    
    tupdesc = RelationGetDescr(heapRel);
    
    /* Create a slot to hold the tuple */
    slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);
    
    /*
     * Use SnapshotSelf to see our own uncommitted changes.
     * During INSERT, the tuple is in the heap but not yet committed,
     * so GetActiveSnapshot() won't see it.
     */
    if (!table_tuple_fetch_row_version(heapRel, tid, SnapshotSelf, slot))
    {
        /* Tuple not found, return zero hash */
        merkle_hash_zero(result);
        ExecDropSingleTupleTableSlot(slot);
        return;
    }
    
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
    
    /* Compute MD5 hash - pg_md5_hash produces 32 hex chars */
    found = pg_md5_hash(buf.data, buf.len, md5_result);
    
    if (!found)
    {
        /* MD5 failed, return zero hash */
        merkle_hash_zero(result);
    }
    else
    {
        /*
         * Extract first 16 hex chars (8 bytes = 64 bits)
         * MD5 produces 32 hex chars, we use first 16 for full 64-bit hash
         */
        for (i = 0; i < MERKLE_HASH_BYTES; i++)
        {
            result->data[i] = (hex_char_to_int(md5_result[i * 2]) << 4) |
                              hex_char_to_int(md5_result[i * 2 + 1]);
        }
    }
    
    pfree(buf.data);
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
 * merkle_compute_partition_id() - Compute partition for key(s)
 *
 * Handles both single and multi-column indexes.
 * For single-column, uses optimized integer path.
 * For multi-column, concatenates string representations of all keys.
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
 * merkle_update_tree_path() - Update tree from leaf to root
 *
 * This XORs the hash into the leaf node and propagates the change
 * up to the subtree root.
 *
 * Supports multi-page trees - nodes are distributed across multiple pages.
 *
 * isXorIn: true for INSERT (XOR in), false for DELETE (XOR out - same operation)
 */
void
merkle_update_tree_path(Relation indexRel, int leafId, MerkleHash *hash, bool isXorIn)
{
    int         treeId;
    int         nodeInTree;
    int         nodeId;
    int         leavesPerTree;
    int         nodesPerTree;
    int         nodesPerPage;
    int         currentPageBlkno = -1;
    Buffer      buf = InvalidBuffer;
    Page        page = NULL;
    MerkleNode *nodes = NULL;
    MemoryContext oldContext;
    MerklePendingOp *op;
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, NULL, &leavesPerTree, &nodesPerTree, NULL, NULL,
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
    
    /* Calculate tree and node positions using dynamic values */
    treeId = leafId / leavesPerTree;
    nodeInTree = (leafId % leavesPerTree) + leavesPerTree;
    nodeId = nodeInTree + (treeId * nodesPerTree);
    
    /* Walk from leaf to root, XORing at each level */
    while (nodeInTree > 0)
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
        nodeInTree = nodeInTree / 2;
        nodeId = nodeInTree + (treeId * nodesPerTree);
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
merkle_read_meta(Relation indexRel, int *numSubtrees, int *leavesPerTree,
                 int *nodesPerTree, int *totalNodes, int *totalLeaves,
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
    if (numSubtrees)
        *numSubtrees = meta->numSubtrees;
    if (leavesPerTree)
        *leavesPerTree = meta->leavesPerTree;
    if (nodesPerTree)
        *nodesPerTree = meta->nodesPerTree;
    if (totalNodes)
        *totalNodes = meta->totalNodes;
    if (totalLeaves)
        *totalLeaves = meta->numSubtrees * meta->leavesPerTree;
    
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
    int             numSubtrees;
    int             leavesPerTree;
    int             nodesPerTree;
    int             totalNodes;
    int             nodesPerPage;
    int             numTreePages;
    int             nodeIdx;
    int             pageNum;
    
    /* Use provided options or defaults */
    if (opts != NULL)
    {
        numSubtrees = opts->subtrees;
        leavesPerTree = opts->leaves_per_tree;
    }
    else
    {
        numSubtrees = MERKLE_NUM_SUBTREES;
        leavesPerTree = MERKLE_LEAVES_PER_TREE;
    }
    
    /* Calculate derived values */
    nodesPerTree = 2 * leavesPerTree - 1;
    totalNodes = numSubtrees * nodesPerTree;
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
    meta->numSubtrees = numSubtrees;
    meta->leavesPerTree = leavesPerTree;
    meta->nodesPerTree = nodesPerTree;
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
