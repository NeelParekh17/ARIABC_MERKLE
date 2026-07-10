/*-------------------------------------------------------------------------
 *
 * merkleutil.c
 *    Utility functions for Merkle tree operations
 *
 * This file contains helper functions for hash computation, XOR operations,
 * tree traversal, and page access.
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
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "access/relation.h"
#include "portability/instr_time.h"

static bool
merkle_is_power_of(int value, int base)
{
    if (value < 1 || base < 2)
        return false;

    while ((value % base) == 0)
        value /= base;

    return (value == 1);
}

/*
 * Pending operation for transaction rollback
 */
typedef struct MerklePendingOp
{
    Relation            indexRel;
    Oid                 relid;
    SubTransactionId    subxid;
    int                 leafId;
    MerkleHash          hash;
    int                 leavesPerPartition;
    int                 nodesPerPartition;
    int                 fanout;
    int                 totalNodes;
    int                 totalLeaves;
    int                 nodesPerPage;
    int                 numTreePages;
} MerklePendingOp;

static List *pendingOps = NIL;
static bool xactCallbackRegistered = false;
static bool subxactCallbackRegistered = false;

/*
 * Per-backend, per-transaction cache of merkle metapage values.
 *
 * merkle_read_meta() pins + share-locks the metapage (block 0) on every call.
 * During apply_optim_writes(), we can call merkle_update_tree_path() dozens
 * of times on the SAME index within one tx — each call doing the metapage
 * round-trip. Once the tree geometry (partitions, fanout, etc.) is read,
 * it's stable for the lifetime of the relation (geometry changes require
 * AccessExclusiveLock, so no concurrent DML can change it).
 *
 * Cache is cleared on XactCallback so a new txn always re-reads at least
 * once (protects against REINDEX-induced geometry changes between txns).
 */
#define MERKLE_META_CACHE_SLOTS 4
typedef struct MerkleMetaCacheEntry {
    Oid  relid;              /* InvalidOid => empty slot */
    int  numPartitions;
    int  leavesPerPartition;
    int  nodesPerPartition;
    int  totalNodes;
    int  totalLeaves;
    int  nodesPerPage;
    int  numTreePages;
    int  fanout;
} MerkleMetaCacheEntry;
static MerkleMetaCacheEntry merkle_meta_cache[MERKLE_META_CACHE_SLOTS];
static bool merkle_meta_cache_registered = false;

static void
merkle_meta_cache_clear(void)
{
    int i;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
        merkle_meta_cache[i].relid = InvalidOid;
}

static void
merkle_meta_cache_xact_callback(XactEvent event, void *arg)
{
    (void) arg;
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT ||
        event == XACT_EVENT_PREPARE)
    {
        merkle_meta_cache_clear();
    }
}

static const MerkleMetaCacheEntry *
merkle_meta_cache_lookup(Oid relid)
{
    int i;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
        if (merkle_meta_cache[i].relid == relid)
            return &merkle_meta_cache[i];
    return NULL;
}

static void
merkle_meta_cache_store(Oid relid,
                        int numPartitions, int leavesPerPartition,
                        int nodesPerPartition, int totalNodes, int totalLeaves,
                        int nodesPerPage, int numTreePages, int fanout)
{
    int i;
    /* LRU-ish: replace first empty slot, else replace slot 0. */
    int target = 0;
    for (i = 0; i < MERKLE_META_CACHE_SLOTS; ++i)
    {
        if (merkle_meta_cache[i].relid == InvalidOid) { target = i; break; }
        if (merkle_meta_cache[i].relid == relid)      { target = i; break; }
    }
    if (!merkle_meta_cache_registered)
    {
        RegisterXactCallback(merkle_meta_cache_xact_callback, NULL);
        merkle_meta_cache_registered = true;
    }
    merkle_meta_cache[target].relid              = relid;
    merkle_meta_cache[target].numPartitions      = numPartitions;
    merkle_meta_cache[target].leavesPerPartition = leavesPerPartition;
    merkle_meta_cache[target].nodesPerPartition  = nodesPerPartition;
    merkle_meta_cache[target].totalNodes         = totalNodes;
    merkle_meta_cache[target].totalLeaves        = totalLeaves;
    merkle_meta_cache[target].nodesPerPage       = nodesPerPage;
    merkle_meta_cache[target].numTreePages       = numTreePages;
    merkle_meta_cache[target].fanout             = fanout;
}

/*
 * Optional per-transaction reporting of touched nodes (GUC-controlled).
 * We record the pre-change hash for each touched node and, at PRE_COMMIT,
 * emit a NOTICE table of nodes whose final hash differs from the initial.
 */
typedef struct MerkleTouchedNode
{
    Oid                 indexRelid;
    SubTransactionId    subxid;
    int                 partition;       /* subtree/partition id */
    int                 nodeInPartition; /* 1-indexed */
    int                 actualNodeIdx;   /* 0-based global node index */
    int                 pageNum;
    int                 idxInPage;
    int                 nodesPerPage;
    int                 numTreePages;
    MerkleHash          initialHash;
    MerkleHash          finalHash;
    bool                finalValid;
    bool                changed;
} MerkleTouchedNode;

static List *touchedNodes = NIL;

static void merkle_undo_pending_op(MerklePendingOp *op);
static void merkle_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                    SubTransactionId parentSubid, void *arg);
static void merkle_track_touched_node(Relation indexRel, int partition,
                                      int nodeInPartition, int actualNodeIdx,
                                      int pageNum, int idxInPage,
                                      int nodesPerPage, int numTreePages,
                                      const MerkleHash *oldHash);
static void merkle_emit_touched_nodes_report(void);

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

    (void) arg;

	if (event == XACT_EVENT_PRE_PREPARE && pendingOps != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("prepared transactions are not supported after Merkle index updates"),
				 errdetail("Merkle rollback state is backend-local and cannot be transferred to a prepared transaction."),
				 errhint("Commit or roll back this transaction normally; use REINDEX after an unclean server shutdown.")));

    /*
     * IMPORTANT: We must emit the report at PRE_COMMIT, not COMMIT.
     *
     * At XACT_EVENT_COMMIT, PostgreSQL has already ended the transaction
     * from the relcache perspective (ProcArrayEndTransaction ran), and
     * opening relations can trip IsTransactionState() assertions.
     *
     * Also, do not emit from parallel workers (PARALLEL_PRE_COMMIT), since
     * they may not hold the relevant relation locks and shouldn't be doing
     * extra I/O/NOTICE output during leader commit.
     */
    if (event == XACT_EVENT_PRE_COMMIT)
    {
        merkle_emit_touched_nodes_report();
        return;
    }

    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT)
    {

        list_free_deep(pendingOps);
        pendingOps = NIL;

        list_free_deep(touchedNodes);
        touchedNodes = NIL;
    }
    else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
    {
        foreach(lc, pendingOps)
        {
            MerklePendingOp *op = (MerklePendingOp *) lfirst(lc);
            merkle_undo_pending_op(op);
        }
        
        list_free_deep(pendingOps);
        pendingOps = NIL;

        list_free_deep(touchedNodes);
        touchedNodes = NIL;
    }
    else if (event == XACT_EVENT_PREPARE)
    {
        /*
		 * PRE_PREPARE rejected transactions with Merkle mutations, so only
		 * read-only Merkle use can reach this event.
         */
        list_free_deep(pendingOps);
        pendingOps = NIL;

        list_free_deep(touchedNodes);
        touchedNodes = NIL;
    }
}

static void
merkle_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                        SubTransactionId parentSubid, void *arg)
{
    ListCell *lc;

    (void) parentSubid;
    (void) arg;

    if (event != SUBXACT_EVENT_ABORT_SUB)
        return;

    /*
     * Rollback-to-savepoint: undo all operations done in this subxact and
     * any nested subxacts created after it (subxid is monotonically increasing
     * within a top-level xact).
     */
    foreach(lc, pendingOps)
    {
        MerklePendingOp *op = (MerklePendingOp *) lfirst(lc);

        if (op->subxid < mySubid)
            continue;

        merkle_undo_pending_op(op);
        pfree(op);
        pendingOps = foreach_delete_current(pendingOps, lc);
    }

    foreach(lc, touchedNodes)
    {
        MerkleTouchedNode *entry = (MerkleTouchedNode *) lfirst(lc);

        if (entry->subxid < mySubid)
            continue;

        pfree(entry);
        touchedNodes = foreach_delete_current(touchedNodes, lc);
    }
}

static void
merkle_undo_pending_op(MerklePendingOp *op)
{
    Relation    indexRel;
    int         leavesPerPartition;
    int         nodesPerPartition;
    int         fanout;
    int         totalNodes;
    int         totalLeaves;
    int         nodesPerPage;
    int         numTreePages;
    int         partitionId;
    int         nodeInPartition;
    int         nodeId;
    int         leafStart;
    int         currentPageBlkno = -1;
    Buffer      buf = InvalidBuffer;
    Page        page = NULL;
    MerkleNode *nodes = NULL;

    if (op == NULL)
        return;

    indexRel = op->indexRel;
    if (indexRel == NULL)
    {
        ereport(DEBUG5,
                (errmsg("merkle_xact_callback: missing relation for undo (relid=%u)", op->relid)));
        return;
    }

    leavesPerPartition = op->leavesPerPartition;
    nodesPerPartition = op->nodesPerPartition;
    fanout = op->fanout;
    totalNodes = op->totalNodes;
    totalLeaves = op->totalLeaves;
    nodesPerPage = op->nodesPerPage;
    numTreePages = op->numTreePages;

    if (leavesPerPartition <= 0 || nodesPerPartition <= 0 || fanout < 2 ||
        totalNodes <= 0 || totalLeaves <= 0 ||
        nodesPerPage <= 0 || numTreePages <= 0)
    {
        ereport(DEBUG5,
                (errmsg("merkle_xact_callback: skipping undo due to invalid metadata "
                        "(leavesPerPartition=%d nodesPerPartition=%d fanout=%d totalNodes=%d totalLeaves=%d nodesPerPage=%d numTreePages=%d)",
                        leavesPerPartition, nodesPerPartition, fanout, totalNodes, totalLeaves, nodesPerPage, numTreePages)));
        return;
    }

    if (op->leafId < 0 || op->leafId >= totalLeaves)
    {
        ereport(DEBUG5,
                (errmsg("merkle_xact_callback: skipping invalid undo leafId %d (totalLeaves=%d)",
                        op->leafId, totalLeaves)));
        return;
    }

    leafStart = nodesPerPartition - leavesPerPartition + 1;
    if (leafStart < 1)
    {
        ereport(DEBUG5,
                (errmsg("merkle_xact_callback: skipping undo due to invalid leafStart %d", leafStart)));
        return;
    }

    /* Calculate partition and node positions using cached dynamic values */
    partitionId = op->leafId / leavesPerPartition;
    nodeInPartition = (op->leafId % leavesPerPartition) + leafStart;
    nodeId = nodeInPartition + (partitionId * nodesPerPartition);

    /*
     * This runs during transaction abort and must not throw errors, or we'd
     * risk crashing the backend (and potentially corrupting the index further).
     */
    PG_TRY();
    {
        while (nodeInPartition > 0)
        {
            int         actualNodeIdx = nodeId - 1;
            int         pageNum;
            int         idxInPage = actualNodeIdx % nodesPerPage;
            BlockNumber blkno;

            if (actualNodeIdx < 0 || actualNodeIdx >= totalNodes)
            {
                ereport(DEBUG5,
                        (errmsg("merkle_xact_callback: invalid node index %d during undo", actualNodeIdx)));
                break;
            }

            pageNum = actualNodeIdx / nodesPerPage;
            if (pageNum < 0 || pageNum >= numTreePages)
            {
                ereport(DEBUG5,
                        (errmsg("merkle_xact_callback: invalid page number %d during undo", pageNum)));
                break;
            }

            blkno = MERKLE_TREE_START_BLKNO + pageNum;

            /* Switch pages if needed */
            if ((int)blkno != currentPageBlkno)
            {
                if (BufferIsValid(buf))
                {
                    MarkBufferDirty(buf);
                    UnlockReleaseBuffer(buf);
                    buf = InvalidBuffer;
                }

                buf = ReadBuffer(indexRel, blkno);
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                page = BufferGetPage(buf);
                nodes = (MerkleNode *) PageGetContents(page);
                currentPageBlkno = blkno;
            }

            merkle_hash_xor(&nodes[idxInPage].hash, &op->hash);

            /* Move to parent */
            nodeInPartition = (nodeInPartition + fanout - 2) / fanout;
            nodeId = nodeInPartition + (partitionId * nodesPerPartition);
        }

        if (BufferIsValid(buf))
        {
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
    }
    PG_CATCH();
    {
        if (BufferIsValid(buf))
        {
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }

        FlushErrorState();
        ereport(DEBUG5,
                (errmsg("merkle_xact_callback: failed to undo op (relid=%u leafId=%d)",
                        op->relid, op->leafId)));
    }
    PG_END_TRY();
}

static void
merkle_track_touched_node(Relation indexRel, int partition,
                          int nodeInPartition, int actualNodeIdx,
                          int pageNum, int idxInPage,
                          int nodesPerPage, int numTreePages,
                          const MerkleHash *oldHash)
{
    ListCell   *lc;
    MemoryContext oldContext;
    MerkleTouchedNode *entry;
    Oid relid;

    if (indexRel == NULL || oldHash == NULL)
        return;

    relid = RelationGetRelid(indexRel);

    foreach(lc, touchedNodes)
    {
        entry = (MerkleTouchedNode *) lfirst(lc);
        if (entry->indexRelid == relid && entry->actualNodeIdx == actualNodeIdx)
            return; /* already tracked */
    }

    oldContext = MemoryContextSwitchTo(TopTransactionContext);

    entry = (MerkleTouchedNode *) palloc(sizeof(MerkleTouchedNode));
    entry->indexRelid = relid;
    entry->subxid = GetCurrentSubTransactionId();
    entry->partition = partition;
    entry->nodeInPartition = nodeInPartition;
    entry->actualNodeIdx = actualNodeIdx;
    entry->pageNum = pageNum;
    entry->idxInPage = idxInPage;
    entry->nodesPerPage = nodesPerPage;
    entry->numTreePages = numTreePages;
    entry->initialHash = *oldHash;
    merkle_hash_zero(&entry->finalHash);
    entry->finalValid = false;
    entry->changed = false;

    touchedNodes = lappend(touchedNodes, entry);

    MemoryContextSwitchTo(oldContext);
}

static int
merkle_cmp_touch_read_order(const void *a, const void *b)
{
    const MerkleTouchedNode *ea = *(const MerkleTouchedNode * const *) a;
    const MerkleTouchedNode *eb = *(const MerkleTouchedNode * const *) b;

    if (ea->indexRelid != eb->indexRelid)
        return (ea->indexRelid < eb->indexRelid) ? -1 : 1;
    if (ea->pageNum != eb->pageNum)
        return (ea->pageNum < eb->pageNum) ? -1 : 1;
    if (ea->idxInPage != eb->idxInPage)
        return (ea->idxInPage < eb->idxInPage) ? -1 : 1;
    return 0;
}

static int
merkle_cmp_touch_output_order(const void *a, const void *b)
{
    const MerkleTouchedNode *ea = *(const MerkleTouchedNode * const *) a;
    const MerkleTouchedNode *eb = *(const MerkleTouchedNode * const *) b;

    if (ea->indexRelid != eb->indexRelid)
        return (ea->indexRelid < eb->indexRelid) ? -1 : 1;
    if (ea->partition != eb->partition)
        return (ea->partition < eb->partition) ? -1 : 1;
    if (ea->nodeInPartition != eb->nodeInPartition)
        return (ea->nodeInPartition < eb->nodeInPartition) ? -1 : 1;
    return 0;
}

static void
merkle_emit_touched_nodes_report(void)
{
    int         nentries;
    int         i;
    MerkleTouchedNode **entries;
    MerkleTouchedNode **changed;
    int         nchanged = 0;
    ListCell   *lc;
    Oid         currentRelid = InvalidOid;
    Relation    indexRel = NULL;
    int         currentPageNum = -1;
    Buffer      buf = InvalidBuffer;
    Page        page = NULL;
    MerkleNode *nodes = NULL;
    bool        saved_is_bcdb_worker = false;

    if (!merkle_update_detection)
        return;

    nentries = list_length(touchedNodes);
    if (nentries <= 0)
        return;

    entries = (MerkleTouchedNode **) palloc(sizeof(MerkleTouchedNode *) * nentries);
    i = 0;
    foreach(lc, touchedNodes)
        entries[i++] = (MerkleTouchedNode *) lfirst(lc);

    qsort(entries, nentries, sizeof(MerkleTouchedNode *), merkle_cmp_touch_read_order);

    PG_TRY();
    {
        for (i = 0; i < nentries; i++)
        {
            MerkleTouchedNode *e = entries[i];
            BlockNumber blkno;

            if (e->pageNum < 0 || e->pageNum >= e->numTreePages)
                continue;

            if (e->idxInPage < 0 || e->idxInPage >= e->nodesPerPage)
                continue;

            if (e->indexRelid != currentRelid)
            {
                if (BufferIsValid(buf))
                {
                    UnlockReleaseBuffer(buf);
                    buf = InvalidBuffer;
                }
                if (indexRel != NULL)
                {
                    relation_close(indexRel, AccessShareLock);
                    indexRel = NULL;
                }

                indexRel = relation_open(e->indexRelid, AccessShareLock);
                currentRelid = e->indexRelid;
                currentPageNum = -1;
            }

            if (e->pageNum != currentPageNum)
            {
                if (BufferIsValid(buf))
                {
                    UnlockReleaseBuffer(buf);
                    buf = InvalidBuffer;
                }

                blkno = MERKLE_TREE_START_BLKNO + e->pageNum;
                buf = ReadBuffer(indexRel, blkno);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page = BufferGetPage(buf);
                nodes = (MerkleNode *) PageGetContents(page);
                currentPageNum = e->pageNum;
            }

            e->finalHash = nodes[e->idxInPage].hash;
            e->finalValid = true;
            e->changed = (memcmp(&e->initialHash, &e->finalHash, sizeof(MerkleHash)) != 0);
        }

        if (BufferIsValid(buf))
        {
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
        if (indexRel != NULL)
        {
            relation_close(indexRel, AccessShareLock);
            indexRel = NULL;
        }
    }
    PG_CATCH();
    {
        if (BufferIsValid(buf))
        {
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
        if (indexRel != NULL)
        {
            relation_close(indexRel, AccessShareLock);
            indexRel = NULL;
        }

        pfree(entries);
        FlushErrorState();
        return;
    }
    PG_END_TRY();

    /*
     * Collect the root node (node_in_partition=1) for each touched partition.
     * This is the partition "root hash" users typically care about.
     */
    changed = (MerkleTouchedNode **) palloc(sizeof(MerkleTouchedNode *) * nentries);
    for (i = 0; i < nentries; i++)
    {
        MerkleTouchedNode *e = entries[i];
        if (e->finalValid && e->nodeInPartition == 1 && e->changed)
            changed[nchanged++] = e;
    }

    if (nchanged <= 0)
    {
        pfree(entries);
        pfree(changed);
        return;
    }

    qsort(changed, nchanged, sizeof(MerkleTouchedNode *), merkle_cmp_touch_output_order);

    PG_TRY();
    {
        StringInfoData out;

        /*
         * BCDB deterministic transactions (triggered via "s <seq> ...") set
         * is_bcdb_worker=true in the session backend, which suppresses all
         * client-visible NOTICE output in EmitErrorReport().
         *
         * We only want to bypass that suppression for this Merkle report so
         * users can see which partitions/nodes were touched. Preserve the old
         * value and restore it on all paths.
         */
        saved_is_bcdb_worker = is_bcdb_worker;
        is_bcdb_worker = false;

        initStringInfo(&out);

        for (i = 0; i < nchanged; i++)
        {
            MerkleTouchedNode *e = changed[i];
            char *hex = merkle_hash_to_hex(&e->finalHash);

            if (i > 0)
                appendStringInfoString(&out, " ");
            appendStringInfo(&out, "(%d, %s)", e->partition, hex);
            pfree(hex);
        }

        ereport(NOTICE,
                (errmsg("BCDB_MERKLE_ROOTS: %s", out.data)));

        pfree(out.data);

        is_bcdb_worker = saved_is_bcdb_worker;
    }
    PG_CATCH();
    {
        is_bcdb_worker = saved_is_bcdb_worker;

        pfree(entries);
        pfree(changed);
        FlushErrorState();
        return;
    }
    PG_END_TRY();

    pfree(entries);
    pfree(changed);
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

static void
merkle_hash_uint32(blake3_hasher *hasher, uint32 value)
{
	uint8		bytes[4];

	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

/*
 * Hash one materialized row using a versioned, length-prefixed binary format.
 * Type send functions produce PostgreSQL's canonical wire representation and
 * therefore do not depend on TimeZone, DateStyle, locale, or output GUCs.
 */
static void
merkle_hash_slot_canonical(Relation heapRel, TupleTableSlot *slot,
						   MerkleHash *result)
{
	TupleDesc		tupdesc = RelationGetDescr(heapRel);
	blake3_hasher	hasher;
	int				i;
	uint32			live_attributes = 0;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'M', 'R', 'K', 'L'};

	if (slot == NULL || TTS_EMPTY(slot))
	{
		merkle_hash_zero(result);
		return;
	}

	for (i = 0; i < tupdesc->natts; i++)
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			live_attributes++;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, magic, sizeof(magic));
	merkle_hash_uint32(&hasher, MERKLE_ROW_HASH_FORMAT_VERSION);
	merkle_hash_uint32(&hasher, live_attributes);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		Datum		val;
		bool		isnull;
		uint8		null_flag;

		if (attr->attisdropped)
			continue;

		/* Schema descriptor: physical attribute, type identity, and typmod. */
		merkle_hash_uint32(&hasher, (uint32) attr->attnum);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypid);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypmod);

		val = slot_getattr(slot, i + 1, &isnull);
		null_flag = isnull ? 1 : 0;
		blake3_hasher_update(&hasher, &null_flag, sizeof(null_flag));

		if (!isnull)
		{
			Oid			typsend;
			bool		typisvarlena;
			bytea	   *encoded;
			uint32		length;

			getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
			encoded = OidSendFunctionCall(typsend, val);
			length = (uint32) VARSIZE_ANY_EXHDR(encoded);
			merkle_hash_uint32(&hasher, length);
			if (length > 0)
				blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
			pfree(encoded);
		}
	}

	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

/*
 * merkle_compute_row_hash() - Fetch and canonically hash one heap row.
 */
void
merkle_compute_row_hash(Relation heapRel, ItemPointer tid, MerkleHash *result)
{
	TupleDesc		tupdesc;
	TupleTableSlot *slot = NULL;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (profile_enabled)
		INSTR_TIME_SET_CURRENT(start_time);
	if (profile_enabled)
		merkle_recovery_profile_state.row_hash_compute_calls++;

	/*
	 * CRITICAL FIX: Validate ItemPointer before attempting to fetch tuple.
	 * Invalid TIDs (offset=0 or block=Invalid) can occur during BCDB operations
	 * and will cause fetch failures. Return zero hash for these cases.
	 */
	if (!ItemPointerIsValid(tid) ||
		ItemPointerGetBlockNumberNoCheck(tid) == InvalidBlockNumber)
	{
		elog(DEBUG1, "merkle_compute_row_hash: skipping invalid tid (blk=%u, off=%u)",
			 ItemPointerGetBlockNumberNoCheck(tid),
			 ItemPointerGetOffsetNumberNoCheck(tid));
		merkle_hash_zero(result);
		goto profile_done;
	}

	tupdesc = RelationGetDescr(heapRel);
	slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);

	PG_TRY();
	{
		/*
		 * Use SnapshotSelf to see our own uncommitted changes.
		 * During INSERT, the tuple is in the heap but not yet committed,
		 * so GetActiveSnapshot() won't see it.
		 */
		if (!table_tuple_fetch_row_version(heapRel, tid, SnapshotSelf, slot))
		{
			merkle_hash_zero(result);
		}
		else
			merkle_hash_slot_canonical(heapRel, slot, result);
	}
	PG_CATCH();
	{
		if (slot != NULL)
			ExecDropSingleTupleTableSlot(slot);
		if (profile_enabled)
		{
			INSTR_TIME_SET_CURRENT(elapsed_time);
			INSTR_TIME_SUBTRACT(elapsed_time, start_time);
			INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
						   elapsed_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (slot != NULL)
		ExecDropSingleTupleTableSlot(slot);

profile_done:
	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
					   elapsed_time);
	}
}

/*
 * merkle_compute_slot_hash() - Compute integrity hash from an already-fetched slot.
 *
 * This variant avoids heap re-fetch by hashing the visible values currently
 * present in `slot`. It is used when we must defer Merkle mutation until after
 * heap operation success, while still hashing the OLD row image.
 */
void
merkle_compute_slot_hash(Relation heapRel, TupleTableSlot *slot, MerkleHash *result)
{
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		merkle_recovery_profile_state.row_hash_compute_calls++;
	}

	merkle_hash_slot_canonical(heapRel, slot, result);

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		INSTR_TIME_ADD(merkle_recovery_profile_state.row_hash_compute_time,
					   elapsed_time);
	}
}

/*
 * merkle_compute_canonical_route_digest() - Uniform BLAKE3-256 routing digest.
 *
 * All key types (integers, text, composite, NULL) are routed through a
 * versioned, length-prefixed binary-send stream hashed with BLAKE3.  This
 * produces a uniform 256-bit digest from which:
 *
 *   static leaf  = uint64(first 8 digest bytes) % total_leaves
 *   dynamic bits = full 256-bit digest consumed in fixed groups
 *
 * INTEGER KEYS: earlier versions used abs(key) % total_leaves directly, which
 * was incompatible with a future dynamic prefix tree (sequential keys share
 * high bits).  Route format version 2 removes that special path.
 */
static void
merkle_compute_canonical_route_digest(Datum *values, bool *isnull, int nkeys,
									  TupleDesc tupdesc, uint8 digest[MERKLE_HASH_BYTES])
{
	blake3_hasher hasher;
	int			i;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'R', 'O', 'U', 'T'};

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, magic, sizeof(magic));
	merkle_hash_uint32(&hasher, MERKLE_ROUTE_FORMAT_VERSION);
	merkle_hash_uint32(&hasher, (uint32) nkeys);

	for (i = 0; i < nkeys; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		uint8		null_flag = isnull[i] ? 1 : 0;

		merkle_hash_uint32(&hasher, (uint32) (i + 1));
		merkle_hash_uint32(&hasher, (uint32) attr->atttypid);
		merkle_hash_uint32(&hasher, (uint32) attr->atttypmod);
		blake3_hasher_update(&hasher, &null_flag, sizeof(null_flag));

		if (!isnull[i])
		{
			Oid			typsend;
			bool		typisvarlena;
			bytea	   *encoded;
			uint32		length;

			getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
			encoded = OidSendFunctionCall(typsend, values[i]);
			length = (uint32) VARSIZE_ANY_EXHDR(encoded);
			merkle_hash_uint32(&hasher, length);
			if (length > 0)
			{
				blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
			}
			pfree(encoded);
		}
	}

	blake3_hasher_finalize(&hasher, digest, MERKLE_HASH_BYTES);
}

/*
 * merkle_geometry_from_index() - Load and validate one authoritative geometry.
 */
void
merkle_geometry_from_index(Relation indexRel, MerkleGeometry *geometry)
{
	if (geometry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle geometry output cannot be null")));

	merkle_read_meta(indexRel, &geometry->num_partitions,
					 &geometry->leaves_per_partition,
					 &geometry->nodes_per_partition,
					 &geometry->total_nodes, &geometry->total_leaves,
					 NULL, NULL, &geometry->fanout);
	geometry->leaf_start = geometry->nodes_per_partition -
		geometry->leaves_per_partition + 1;
}

int
merkle_geometry_global_node(const MerkleGeometry *geometry, int partition,
							int node_in_partition)
{
	if (geometry == NULL || partition < 0 ||
		partition >= geometry->num_partitions || node_in_partition < 1 ||
		node_in_partition > geometry->nodes_per_partition)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Merkle node coordinates (%d, %d)",
						partition, node_in_partition)));

	return partition * geometry->nodes_per_partition + node_in_partition - 1;
}

int
merkle_geometry_leaf_node(const MerkleGeometry *geometry, int leaf_id)
{
	if (geometry == NULL || leaf_id < 0 || leaf_id >= geometry->total_leaves)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Merkle leaf ID %d is out of range", leaf_id)));

	return geometry->leaf_start + (leaf_id % geometry->leaves_per_partition);
}

int
merkle_geometry_parent_node(const MerkleGeometry *geometry,
							int node_in_partition)
{
	if (geometry == NULL || node_in_partition <= 1 ||
		node_in_partition > geometry->nodes_per_partition)
		return 0;
	return (node_in_partition + geometry->fanout - 2) / geometry->fanout;
}

int
merkle_geometry_child_node(const MerkleGeometry *geometry,
						   int node_in_partition, int child_ordinal)
{
	int child;

	if (geometry == NULL || node_in_partition < 1 ||
		node_in_partition >= geometry->leaf_start || child_ordinal < 0 ||
		child_ordinal >= geometry->fanout)
		return 0;
	child = geometry->fanout * (node_in_partition - 1) + child_ordinal + 2;
	return child <= geometry->nodes_per_partition ? child : 0;
}

/*
 * merkle_compute_route() - Single relation-aware routing entry point.
 *
 * All key types use uniform BLAKE3-256 routing (route format version 2).
 * The 64-bit static_route_value is derived from the first 8 bytes of the digest;
 * the full 256-bit digest is available for future dynamic tree traversal.
 */
void
merkle_compute_route(Relation indexRel, Datum *values, bool *isnull, int nkeys,
					 MerkleRoute *result)
{
	MerkleGeometry geometry;
	TupleDesc		tupdesc;
	uint64			static_route_value = 0;
	int				i;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle route output cannot be null")));

	merkle_geometry_from_index(indexRel, &geometry);
	tupdesc = RelationGetDescr(indexRel);

	/* Uniform BLAKE3 routing for all key types including integers. */
	merkle_compute_canonical_route_digest(values, isnull, nkeys, tupdesc, result->route_digest);

	for (i = 0; i < 8; i++)
		static_route_value = (static_route_value << 8) | result->route_digest[i];

	result->static_route_value = static_route_value;
	result->leaf_id = (int) (static_route_value % (uint64) geometry.total_leaves);
	result->partition_id = result->leaf_id / geometry.leaves_per_partition;
	result->node_in_partition = merkle_geometry_leaf_node(&geometry,
													 result->leaf_id);
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
	int			numPartitions;
	int			partitionId;
	int			nodeInPartition;
	int			nodeId;
	int			leafStart;
	int			leavesPerPartition;
	int			nodesPerPartition;
	int			fanout;
	int			totalNodes;
	int			totalLeaves;
	int			nodesPerPage;
	int			numTreePages;
	int			currentPageBlkno = -1;
	Buffer		buf = InvalidBuffer;
	Page		page = NULL;
	MerkleNode *nodes = NULL;
	bool		profile_enabled = merkle_recovery_profile_enabled;
	instr_time	start_time;
	instr_time	elapsed_time;
	uint64		nodes_touched = 0;

	(void) isXorIn;

	if (profile_enabled)
		INSTR_TIME_SET_CURRENT(start_time);
	if (profile_enabled)
		merkle_recovery_profile_state.tree_path_update_calls++;

	/* Read tree configuration from metadata */
	merkle_read_meta(indexRel,
					 &numPartitions,
					 &leavesPerPartition,
					 &nodesPerPartition,
					 &totalNodes,
					 &totalLeaves,
					 &nodesPerPage,
					 &numTreePages,
					 &fanout);

	/* Safety check: prevent division by zero if metadata is invalid */
	if (leavesPerPartition <= 0)
		return;

	if (leafId < 0 || leafId >= totalLeaves)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("merkle_update_tree_path: leafId %d out of range [0,%d)",
						leafId, totalLeaves)));
	}

	if (fanout < 2)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("merkle_update_tree_path: invalid fanout %d in metadata",
						fanout)));
	}

	leafStart = nodesPerPartition - leavesPerPartition + 1;
	if (leafStart < 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("merkle_update_tree_path: invalid leafStart %d (nodesPerPartition=%d leavesPerPartition=%d)",
						leafStart, nodesPerPartition, leavesPerPartition)));
	}

	if (!merkle_undo_suppress)
	{
		MemoryContext	oldContext;
		MerklePendingOp *op;

		/*
		 * Register transaction callback if not done yet.
		 * This ensures we can undo changes if the transaction aborts.
		 */
		if (!xactCallbackRegistered)
		{
			RegisterXactCallback(merkle_xact_callback, NULL);
			xactCallbackRegistered = true;
		}
		if (!subxactCallbackRegistered)
		{
			RegisterSubXactCallback(merkle_subxact_callback, NULL);
			subxactCallbackRegistered = true;
		}

		/*
		 * Record this operation in the pending list for potential rollback.
		 * We use TopTransactionContext to ensure the list survives until commit/abort.
		 */
		oldContext = MemoryContextSwitchTo(TopTransactionContext);

		op = (MerklePendingOp *) palloc(sizeof(MerklePendingOp));
		op->indexRel = indexRel;
		op->relid = RelationGetRelid(indexRel);
		op->subxid = GetCurrentSubTransactionId();
		op->leafId = leafId;
		op->hash = *hash;
		op->leavesPerPartition = leavesPerPartition;
		op->nodesPerPartition = nodesPerPartition;
		op->fanout = fanout;
		op->totalNodes = totalNodes;
		op->totalLeaves = totalLeaves;
		op->nodesPerPage = nodesPerPage;
		op->numTreePages = numTreePages;

		pendingOps = lappend(pendingOps, op);

		MemoryContextSwitchTo(oldContext);
	}

	/* Calculate partition and node positions using dynamic values */
	partitionId = leafId / leavesPerPartition;
	nodeInPartition = (leafId % leavesPerPartition) + leafStart;
	nodeId = nodeInPartition + (partitionId * nodesPerPartition);

	/* Walk from leaf to root, XORing at each level */
	while (nodeInPartition > 0)
	{
		int			actualNodeIdx = nodeId - 1;	/* 0-based index */
		int			pageNum;
		int			idxInPage = actualNodeIdx % nodesPerPage;
		BlockNumber blkno;

		if (actualNodeIdx < 0 || actualNodeIdx >= totalNodes)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("merkle_update_tree_path: node index %d out of range [0,%d)",
							actualNodeIdx, totalNodes)));
		}

		pageNum = actualNodeIdx / nodesPerPage;
		if (pageNum < 0 || pageNum >= numTreePages)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("merkle_update_tree_path: page number %d out of range [0,%d)",
							pageNum, numTreePages)));
		}

		blkno = MERKLE_TREE_START_BLKNO + pageNum;

		/* Switch pages if needed */
		if ((int) blkno != currentPageBlkno)
		{
			/* Release previous page if held */
			if (BufferIsValid(buf))
			{
				MarkBufferDirty(buf);
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}

			/* Read new page */
			buf = ReadBuffer(indexRel, blkno);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			nodes = (MerkleNode *) PageGetContents(page);
			currentPageBlkno = blkno;
		}

		/* Record pre-change hash for optional commit-time reporting */
		if (merkle_update_detection)
			merkle_track_touched_node(indexRel, partitionId, nodeInPartition,
									  actualNodeIdx, pageNum, idxInPage,
									  nodesPerPage, numTreePages,
									  &nodes[idxInPage].hash);

		/* XOR hash into this node */
		merkle_hash_xor(&nodes[idxInPage].hash, hash);
		nodes_touched++;

		/* Move to parent */
		nodeInPartition = (nodeInPartition + fanout - 2) / fanout;
		nodeId = nodeInPartition + (partitionId * nodesPerPartition);
	}

	/* Release the last page if held */
	if (BufferIsValid(buf))
	{
		MarkBufferDirty(buf);
		UnlockReleaseBuffer(buf);
		buf = InvalidBuffer;
	}

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		merkle_recovery_profile_state.tree_path_nodes_touched += nodes_touched;
		INSTR_TIME_ADD(merkle_recovery_profile_state.tree_path_update_time,
					   elapsed_time);
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
                 int *nodesPerPage, int *numTreePages,
                 int *fanout)
{
    Buffer              buf;
    Page                page;
    MerkleMetaPageData *meta;
    int                 effectiveFanout;
    Oid                 cache_relid = RelationGetRelid(indexRel);
    const MerkleMetaCacheEntry *cached = merkle_meta_cache_lookup(cache_relid);

    if (cached != NULL)
    {
        if (numPartitions)      *numPartitions      = cached->numPartitions;
        if (leavesPerPartition) *leavesPerPartition = cached->leavesPerPartition;
        if (nodesPerPartition)  *nodesPerPartition  = cached->nodesPerPartition;
        if (totalNodes)         *totalNodes         = cached->totalNodes;
        if (totalLeaves)        *totalLeaves        = cached->totalLeaves;
        if (nodesPerPage)       *nodesPerPage       = cached->nodesPerPage;
        if (numTreePages)       *numTreePages       = cached->numTreePages;
        if (fanout)             *fanout             = cached->fanout;
        return;
    }

    buf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    meta = MerklePageGetMeta(page);

	if (meta->version != MERKLE_VERSION ||
		meta->routeFormatVersion != MERKLE_ROUTE_FORMAT_VERSION ||
		meta->rowHashFormatVersion != MERKLE_ROW_HASH_FORMAT_VERSION)
	{
		uint32 stored_version = meta->version;
		uint32 stored_route = meta->routeFormatVersion;
		uint32 stored_row_hash = meta->rowHashFormatVersion;

		UnlockReleaseBuffer(buf);
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("Merkle index \"%s\" uses an incompatible format",
						RelationGetRelationName(indexRel)),
				 errdetail("index version=%u route format=%u row-hash format=%u; server requires version=%u route format=%u row-hash format=%u",
						   stored_version, stored_route, stored_row_hash,
						   MERKLE_VERSION, MERKLE_ROUTE_FORMAT_VERSION,
						   MERKLE_ROW_HASH_FORMAT_VERSION),
				 errhint("REINDEX the Merkle index before using it.")));
	}
    
    /* Validate metadata integrity - corrupted/uninitialized values cause crashes */
    if (meta->numPartitions <= 0 || meta->leavesPerPartition <= 0 ||
        meta->nodesPerPartition <= 0 || meta->totalNodes <= 0 ||
        meta->nodesPerPage <= 0 || meta->numTreePages <= 0)
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("Merkle index \"%s\" has corrupted metadata",
                        RelationGetRelationName(indexRel)),
                 errdetail("numPartitions=%d, leavesPerPartition=%d, nodesPerPartition=%d, totalNodes=%d, nodesPerPage=%d, numTreePages=%d",
                           meta->numPartitions, meta->leavesPerPartition,
                           meta->nodesPerPartition, meta->totalNodes,
                           meta->nodesPerPage, meta->numTreePages),
                 errhint("Try REINDEXing the Merkle index.")));
    }

    if ((int64) meta->numPartitions * (int64) meta->nodesPerPartition != (int64) meta->totalNodes)
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("Merkle index \"%s\" has inconsistent metadata",
                        RelationGetRelationName(indexRel)),
                 errdetail("numPartitions=%d, nodesPerPartition=%d, totalNodes=%d",
                           meta->numPartitions, meta->nodesPerPartition, meta->totalNodes),
                 errhint("Try REINDEXing the Merkle index.")));
    }

    effectiveFanout = MERKLE_DEFAULT_FANOUT;
    if (meta->version >= 5)
        effectiveFanout = meta->fanout;

    if (effectiveFanout < 2 || effectiveFanout > 1024)
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("Merkle index \"%s\" has invalid fanout %d in metadata",
                        RelationGetRelationName(indexRel),
                        effectiveFanout),
                 errhint("Try REINDEXing the Merkle index.")));
    }

    if (!merkle_is_power_of(meta->leavesPerPartition, effectiveFanout))
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("Merkle index \"%s\" has invalid leaves_per_partition %d for fanout %d in metadata",
                        RelationGetRelationName(indexRel),
                        meta->leavesPerPartition,
                        effectiveFanout),
                 errhint("Try REINDEXing the Merkle index.")));
    }
    
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
    if (nodesPerPage)
        *nodesPerPage = meta->nodesPerPage;
    if (numTreePages)
        *numTreePages = meta->numTreePages;
    if (fanout)
        *fanout = effectiveFanout;

    merkle_meta_cache_store(cache_relid,
                            meta->numPartitions,
                            meta->leavesPerPartition,
                            meta->nodesPerPartition,
                            meta->totalNodes,
                            meta->numPartitions * meta->leavesPerPartition,
                            meta->nodesPerPage,
                            meta->numTreePages,
                            effectiveFanout);

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
    int             fanout;
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
        fanout = opts->fanout;
    }
    else
    {
        numPartitions = MERKLE_NUM_PARTITIONS;
        leavesPerPartition = MERKLE_LEAVES_PER_PARTITION;
        fanout = MERKLE_DEFAULT_FANOUT;
    }
    
    /* Calculate derived values */
    if (fanout < 2 || fanout > 1024)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("fanout must be between 2 and 1024")));

    /* For a perfect k-ary tree with L leaves: nodes = (k*L - 1)/(k - 1). */
    if (!merkle_is_power_of(leavesPerPartition, fanout))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("leaves_per_partition must be a power of fanout")));

    if (((int64) fanout * (int64) leavesPerPartition - 1) % (fanout - 1) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("leaves_per_partition must be a power of fanout")));

    nodesPerPartition = (int) (((int64) fanout * (int64) leavesPerPartition - 1) / (fanout - 1));
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
    meta->fanout = fanout;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
    
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
