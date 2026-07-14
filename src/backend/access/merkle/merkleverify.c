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
	Oid heap_oid = IndexGetRelation(index_oid, false);
	Relation heap_rel;
	Relation index_rel;
	MerkleRecoveryStatusData status;

	heap_rel = table_open(heap_oid, ShareLock);
	/* SPI must run before owning a relcache reference/buffer under the caller's
	 * resource owner; doing it afterwards breaks CTAS and materialized SRFs. */
	merkle_get_recovery_status(&status);
	if (!status.managed || status.state != MERKLE_STATE_READY)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle index is not synchronized with committed database state"),
				 errdetail("applied_seq=%llu target_seq=%llu blocked_seq=%llu",
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq,
						   (unsigned long long) status.blocked_seq)));
	}
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
 * Fixed node numbers and leaf buckets only exist in the static v7 layout.
 * Fail explicitly when an old recovery/debug helper is used against a
 * dynamic prefix tree; silently mapping a prefix to a static node number
 * would localize and repair the wrong key range.
 */
static void
merkle_require_static_api(Relation index_rel, const char *function_name)
{
	if (!merkle_index_is_dynamic(index_rel))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s does not support dynamic Merkle indexes",
					function_name),
			 errhint("Use merkle_dynamic_get_partition_roots(), merkle_dynamic_get_ranges(), or merkle_dynamic_get_range_items().")));
}

static void
read_merkle_node_hash_with_meta(Relation indexRel, int nodeIdx,
                                int totalNodes, int nodesPerPage,
                                int numTreePages, MerkleHash *hash)
{
    int          pageNum;
    int          offset;
    Buffer       buf;
    Page         page;
    MerkleNode  *nodes;

    if (nodeIdx < 0 || nodeIdx >= totalNodes)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle node index %d is out of range [0, %d)",
                        nodeIdx, totalNodes)));

    pageNum = nodeIdx / nodesPerPage;
    offset = nodeIdx % nodesPerPage;
    if (pageNum >= numTreePages)
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("merkle node index maps past tree page count")));

    buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    nodes = (MerkleNode *) PageGetContents(page);
    memcpy(hash->data, nodes[offset].hash.data, MERKLE_HASH_BYTES);
    UnlockReleaseBuffer(buf);
}

static int
global_node_index(int partition, int nodeInPartition, int numPartitions,
                  int nodesPerPartition)
{
    if (partition < 0 || partition >= numPartitions)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle partition %d is out of range [0, %d)",
                        partition, numPartitions)));
    if (nodeInPartition < 1 || nodeInPartition > nodesPerPartition)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle node_in_partition %d is out of range [1, %d]",
                        nodeInPartition, nodesPerPartition)));

    return partition * nodesPerPartition + (nodeInPartition - 1);
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
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        heapRel;
    Relation        indexRel;
    TableScanDesc   scan;
    TupleTableSlot *slot;
    MerkleHash     *computedTree;
    bool            match = true;
    int             i;
    int             nkeys;
    int16          *indkey;         /* Heap column numbers for indexed keys */
    Datum          *keyValues;      /* Temporary storage for key values */
    bool           *keyNulls;       /* Temporary storage for null flags */
    int             numPartitions;
    int             leavesPerPartition;
    int             nodesPerPartition;
    int             totalNodes;
    int             fanout;
    int             internalNodes;
	Snapshot        verifysnap;
    
	/* Reject or catch up lag before taking a stable index read lock. */
	merkle_require_fresh();

    /* Find the Merkle index on this table */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
	/* Lock heap first (ShareLock blocks DML), then lock index */
	heapRel = table_open(relid, ShareLock);
	indexRel = merkle_open_consistent_index(indexOid);
    
	/* Establish verification snapshot after locks are held */
	verifysnap = RegisterSnapshot(GetLatestSnapshot());

	if (merkle_index_is_dynamic(indexRel))
	{
		match = merkle_dynamic_verify_relations(heapRel, indexRel, verifysnap);
		if (!match)
		{
			char *reason = psprintf("Dynamic Merkle verification mismatch for index %u",
								indexOid);

			merkle_mark_recovery_state(MERKLE_STATE_INVALID, reason);
			pfree(reason);
		}
		UnregisterSnapshot(verifysnap);
		index_close(indexRel, ShareLock);
		table_close(heapRel, ShareLock);
		PG_RETURN_BOOL(match);
	}


    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition, &nodesPerPartition,
					 &totalNodes, NULL, NULL, NULL, &fanout);

    internalNodes = nodesPerPartition - leavesPerPartition;
    
    /* Get index key information */
    nkeys = indexRel->rd_index->indnkeyatts;
    indkey = indexRel->rd_index->indkey.values;
    
    /* Allocate key value arrays */
    keyValues = (Datum *) palloc(nkeys * sizeof(Datum));
    keyNulls = (bool *) palloc(nkeys * sizeof(bool));
    
    /* Allocate space for computed tree using dynamic size */
    computedTree = (MerkleHash *) palloc0(totalNodes * sizeof(MerkleHash));
    
    /* Scan the heap table and recompute the tree */
    slot = table_slot_create(heapRel, NULL);
	scan = table_beginscan(heapRel, verifysnap, 0, NULL);
    
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        MerkleHash  hash;
		MerkleRoute route;
        int         nodeIdx;
        
        /* Extract all indexed column values from heap tuple */
        for (i = 0; i < nkeys; i++)
        {
            int heapAttr = indkey[i];  /* 1-based heap column number */
            keyValues[i] = slot_getattr(slot, heapAttr, &keyNulls[i]);
        }
        
		merkle_compute_route(indexRel, keyValues, keyNulls, nkeys, &route);
        
		/* Hash the slot directly instead of fetching through row hash */
		merkle_compute_slot_hash(heapRel, slot, &hash);
        
        /*
         * Verification optimization: accumulate XOR only at the leaf node in
         * memory, then construct internal nodes bottom-up after the scan.
         */
		nodeIdx = route.partition_id * nodesPerPartition +
				  (route.node_in_partition - 1);
        merkle_hash_xor(&computedTree[nodeIdx], &hash);
    }
    
    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    
    /*
     * Construct internal nodes bottom-up within each partition:
     * parent = XOR of all children
     */
    {
        int partition;
        
        for (partition = 0; partition < numPartitions; partition++)
        {
            int base = partition * nodesPerPartition;
            int nodeInPartition;
            
            for (nodeInPartition = internalNodes; nodeInPartition >= 1; nodeInPartition--)
            {
                int parentIdx = base + (nodeInPartition - 1);
                int child;
                int firstChildIdx = base + fanout * (nodeInPartition - 1) + 1;
                MerkleHash h = computedTree[firstChildIdx];
                
                for (child = 2; child <= fanout; child++)
                    merkle_hash_xor(&h, &computedTree[base + fanout * (nodeInPartition - 1) + child]);

                computedTree[parentIdx] = h;
            }
        }
    }
    
    /* Compare computed tree with stored tree - supports multi-page storage */
    {
        int nodesPerPage = (int)MERKLE_MAX_NODES_PER_PAGE;
        int numTreePages = (totalNodes + nodesPerPage - 1) / nodesPerPage;
        int nodeIdx = 0;
        int pageNum;
        
        for (pageNum = 0; pageNum < numTreePages; pageNum++)
        {
            Buffer      buf;
            Page        page;
            MerkleNode *storedNodes;
            int         nodesThisPage;
            int         j;
			bool        page_format_ok;
            
            buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
			page_format_ok = PageGetSpecialSize(page) == MERKLE_PAGE_SPECIAL_SIZE;
			if (page_format_ok)
			{
				MerklePageOpaqueData *opaque = MerklePageGetOpaque(page);

				page_format_ok = opaque->magic == MERKLE_PAGE_OPAQUE_MAGIC &&
					opaque->version == MERKLE_PAGE_OPAQUE_VERSION;
			}
			if (page_format_ok &&
				((PageHeader) page)->pd_lower <
				(char *) PageGetContents(page) - (char *) page +
				nodesThisPage * (int) sizeof(MerkleNode))
				page_format_ok = false;
			if (!page_format_ok)
			{
				match = false;
				ereport(WARNING,
						(errmsg("merkle tree page %u has invalid v7 crash-recovery metadata",
								MERKLE_TREE_START_BLKNO + pageNum)));
				UnlockReleaseBuffer(buf);
				nodeIdx += nodesThisPage;
				continue;
			}

			storedNodes = (MerkleNode *) PageGetContents(page);
            
            for (j = 0; j < nodesThisPage; j++)
            {
                if (memcmp(computedTree[nodeIdx + j].data, storedNodes[j].hash.data,
                           MERKLE_HASH_BYTES) != 0)
                {
                    match = false;
                    ereport(WARNING,
                            (errmsg("merkle tree mismatch at node %d: computed %s, stored %s",
                                    nodeIdx + j,
                                    merkle_hash_to_hex(&computedTree[nodeIdx + j]),
                                    merkle_hash_to_hex(&storedNodes[j].hash))));
                }
            }
            
            nodeIdx += nodesThisPage;
            UnlockReleaseBuffer(buf);
        }
    }

	if (!match)
	{
		char *reason = psprintf("Merkle verification mismatch for index %u",
							indexOid);

		merkle_mark_recovery_state(MERKLE_STATE_INVALID, reason);
		pfree(reason);
	}
    
    /* Cleanup */
    pfree(computedTree);
    pfree(keyValues);
    pfree(keyNulls);
	UnregisterSnapshot(verifysnap);
	index_close(indexRel, ShareLock);
	table_close(heapRel, ShareLock);
    
    PG_RETURN_BOOL(match);
}

/*
 * merkle_root_hash() - Get combined root hash of all partitions
 *
 * Returns the XOR of all partition root hashes as a hex string.
 * This provides a single hash representing the entire table's integrity state.
 *
 * Optimized to iterate page-wise to minimize buffer lock/unlock overhead.
 *
 * Usage: SELECT merkle_root_hash('tablename');
 */
Datum
merkle_root_hash(PG_FUNCTION_ARGS)
{
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        indexRel;
    MerkleHash      combinedHash;
    char           *result;
    int             numPartitions;
    int             nodesPerPartition;
    int             nodesPerPage;
    int             numTreePages;
    int             totalNodes;
    int             pageNum;
    int             nodeIdx;
	int             retry;

	for (retry = 0; retry < 10; retry++)
	{
		merkle_require_fresh();
		indexOid = find_merkle_index(relid);
		if (!OidIsValid(indexOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("no merkle index found on table %s",
							get_rel_name(relid))));

		indexRel = merkle_open_consistent_index(indexOid);
		break;
	}
	if (retry >= 10)
        ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not obtain a synchronized Merkle snapshot after 10 retries"),
				 errhint("Retry the query or ensure the Merkle applier is catching up.")));

	if (merkle_index_is_dynamic(indexRel))
	{
		uint64 tuple_count;

		merkle_dynamic_root(indexRel, &combinedHash, &tuple_count);
		index_close(indexRel, ShareLock);
		result = merkle_hash_to_hex(&combinedHash);
		PG_RETURN_TEXT_P(cstring_to_text(result));
	}
    
    /* Read tree configuration from metadata */
    merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition, &totalNodes, NULL,
                     &nodesPerPage, &numTreePages, NULL);
    
    /* 
     * Combine all partition roots by XOR - page-wise iteration.
     * Root of partition i is at global index (i * nodesPerPartition).
     */
    merkle_hash_zero(&combinedHash);
    nodeIdx = 0;
    
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      buf;
        Page        page;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         j;
        
        buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        nodes = (MerkleNode *) PageGetContents(page);
        
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        for (j = 0; j < nodesThisPage; j++)
        {
            int globalIdx = nodeIdx + j;
            
            /* Check if this node is a partition root (first node of each partition) */
            if (globalIdx % nodesPerPartition == 0)
            {
                merkle_hash_xor(&combinedHash, &nodes[j].hash);
            }
        }
        
        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(buf);
    }
    
	index_close(indexRel, ShareLock);
    
    /* Convert to hex string */
    result = merkle_hash_to_hex(&combinedHash);
    
    PG_RETURN_TEXT_P(cstring_to_text(result));
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
	Oid             indexOid = PG_GETARG_OID(0);
	Relation        heapRel;
	Relation        indexRel;
	Oid             heapOid;
	TableScanDesc   scan;
	TupleTableSlot *slot;
	MerkleHash     *computedTree;
	bool            match = true;
	int             i;
	int             nkeys;
	int16          *indkey;
	Datum          *keyValues;
	bool           *keyNulls;
	int             numPartitions;
	int             leavesPerPartition;
	int             nodesPerPartition;
	int             totalNodes;
	int             fanout;
	int             internalNodes;
	Snapshot        verifysnap;

	/* Reject or catch up lag first. */
	merkle_require_fresh();

	/* Validate the argument is actually a Merkle index. */
	{
		char relkind = get_rel_relkind(indexOid);

		if (relkind == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation with OID %u does not exist", indexOid)));
		if (relkind != RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%.128s\" is not an index",
							get_rel_name(indexOid))));
	}

	/* Resolve through the catalog; opening an unlocked relation races DROP. */
	heapOid = IndexGetRelation(indexOid, false);

	/* Lock heap first (ShareLock blocks DML), then index */
	heapRel = table_open(heapOid, ShareLock);
	indexRel = merkle_open_consistent_index(indexOid);
	if (indexRel->rd_rel->relam != MERKLE_AM_OID ||
		indexRel->rd_index->indrelid != heapOid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%.128s\" is not a Merkle index on relation %u",
						get_rel_name(indexOid), heapOid)));

	/* Establish snapshot after locks are held */
	verifysnap = RegisterSnapshot(GetLatestSnapshot());

	if (merkle_index_is_dynamic(indexRel))
	{
		match = merkle_dynamic_verify_relations(heapRel, indexRel, verifysnap);
		if (!match)
		{
			char *reason = psprintf("Dynamic Merkle verification mismatch for index %u",
								indexOid);

			merkle_mark_recovery_state(MERKLE_STATE_INVALID, reason);
			pfree(reason);
		}
		UnregisterSnapshot(verifysnap);
		index_close(indexRel, ShareLock);
		table_close(heapRel, ShareLock);
		PG_RETURN_BOOL(match);
	}


	merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition,
					 &nodesPerPartition, &totalNodes, NULL, NULL, NULL, &fanout);
	internalNodes = nodesPerPartition - leavesPerPartition;

	nkeys = indexRel->rd_index->indnkeyatts;
	indkey = indexRel->rd_index->indkey.values;
	keyValues = (Datum *) palloc(nkeys * sizeof(Datum));
	keyNulls = (bool *) palloc(nkeys * sizeof(bool));
	computedTree = (MerkleHash *) palloc0(totalNodes * sizeof(MerkleHash));

	slot = table_slot_create(heapRel, NULL);
	scan = table_beginscan(heapRel, verifysnap, 0, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		MerkleHash  hash;
		MerkleRoute route;
		int         nodeIdx;

		for (i = 0; i < nkeys; i++)
			keyValues[i] = slot_getattr(slot, indkey[i], &keyNulls[i]);

		merkle_compute_route(indexRel, keyValues, keyNulls, nkeys, &route);
		merkle_compute_slot_hash(heapRel, slot, &hash);

		nodeIdx = route.partition_id * nodesPerPartition +
				  (route.node_in_partition - 1);
		merkle_hash_xor(&computedTree[nodeIdx], &hash);
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	/* Construct internal nodes bottom-up. */
	{
		int partition;

		for (partition = 0; partition < numPartitions; partition++)
		{
			int base = partition * nodesPerPartition;
			int nodeInPartition;

			for (nodeInPartition = internalNodes; nodeInPartition >= 1; nodeInPartition--)
			{
				int parentIdx = base + (nodeInPartition - 1);
				int child;
				int firstChildIdx = base + fanout * (nodeInPartition - 1) + 1;
				MerkleHash h = computedTree[firstChildIdx];

				for (child = 2; child <= fanout; child++)
					merkle_hash_xor(&h, &computedTree[base + fanout * (nodeInPartition - 1) + child]);
				computedTree[parentIdx] = h;
			}
		}
	}

	/* Compare with stored tree. */
	{
		int nodesPerPage;
		int numTreePages;
		int nodeIdx = 0;
		int pageNum;

		merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
						 &nodesPerPage, &numTreePages, NULL);

		for (pageNum = 0; pageNum < numTreePages; pageNum++)
		{
			Buffer      buf;
			Page        page;
			MerkleNode *storedNodes;
			int         nodesThisPage;
			int         j;
			bool        page_ok;

			buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
			page_ok = PageGetSpecialSize(page) == MERKLE_PAGE_SPECIAL_SIZE;
			if (page_ok)
			{
				MerklePageOpaqueData *opaque = MerklePageGetOpaque(page);
				page_ok = opaque->magic == MERKLE_PAGE_OPAQUE_MAGIC &&
						  opaque->version == MERKLE_PAGE_OPAQUE_VERSION;
			}
			if (!page_ok)
			{
				match = false;
				ereport(WARNING,
						(errmsg("merkle tree page %u has invalid v7 metadata",
								MERKLE_TREE_START_BLKNO + pageNum)));
				UnlockReleaseBuffer(buf);
				nodeIdx += nodesThisPage;
				continue;
			}
			storedNodes = (MerkleNode *) PageGetContents(page);
			for (j = 0; j < nodesThisPage; j++)
			{
				if (memcmp(computedTree[nodeIdx + j].data,
						   storedNodes[j].hash.data, MERKLE_HASH_BYTES) != 0)
				{
					match = false;
					ereport(WARNING,
							(errmsg("merkle tree mismatch at node %d in index %u",
									nodeIdx + j, indexOid)));
				}
			}
			nodeIdx += nodesThisPage;
			UnlockReleaseBuffer(buf);
		}
	}

	if (!match)
	{
		char *reason = psprintf("Merkle verification mismatch for index %u",
								indexOid);
		merkle_mark_recovery_state(MERKLE_STATE_INVALID, reason);
		pfree(reason);
	}

	pfree(computedTree);
	pfree(keyValues);
	pfree(keyNulls);
	UnregisterSnapshot(verifysnap);
	index_close(indexRel, ShareLock);
	table_close(heapRel, ShareLock);

	PG_RETURN_BOOL(match);
}

/*
 * merkle_root_hash_index() - P0.6: index-specific root hash.
 *
 * Usage: SELECT merkle_root_hash_index('myindex'::regclass);
 */
Datum
merkle_root_hash_index(PG_FUNCTION_ARGS)
{
	Oid             indexOid = PG_GETARG_OID(0);
	Relation        indexRel;
	MerkleHash      combinedHash;
	char           *result;
	int             numPartitions;
	int             nodesPerPartition;
	int             nodesPerPage;
	int             numTreePages;
	int             totalNodes;
	int             pageNum;
	int             nodeIdx;
	int             retry;

	{
		char relkind = get_rel_relkind(indexOid);
		if (relkind == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation with OID %u does not exist", indexOid)));
		if (relkind != RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%.128s\" is not an index",
							get_rel_name(indexOid))));
	}

	for (retry = 0; retry < 10; retry++)
	{
		merkle_require_fresh();
		indexRel = merkle_open_consistent_index(indexOid);
		if (indexRel->rd_rel->relam != MERKLE_AM_OID)
		{
			index_close(indexRel, ShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%.128s\" is not a Merkle index",
							get_rel_name(indexOid))));
		}
		break;
	}
	if (retry >= 10)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not obtain a synchronized Merkle snapshot after 10 retries"),
				 errhint("Retry the query or ensure the Merkle applier is catching up.")));

	if (merkle_index_is_dynamic(indexRel))
	{
		uint64 tuple_count;

		merkle_dynamic_root(indexRel, &combinedHash, &tuple_count);
		index_close(indexRel, ShareLock);
		result = merkle_hash_to_hex(&combinedHash);
		PG_RETURN_TEXT_P(cstring_to_text(result));
	}

	merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition,
					 &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);

	merkle_hash_zero(&combinedHash);
	nodeIdx = 0;

	for (pageNum = 0; pageNum < numTreePages; pageNum++)
	{
		Buffer      buf;
		Page        page;
		MerkleNode *nodes;
		int         nodesThisPage;
		int         j;

		buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		nodes = (MerkleNode *) PageGetContents(page);
		nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);

		for (j = 0; j < nodesThisPage; j++)
		{
			int globalIdx = nodeIdx + j;
			if (globalIdx % nodesPerPartition == 0)
				merkle_hash_xor(&combinedHash, &nodes[j].hash);
		}
		nodeIdx += nodesThisPage;
		UnlockReleaseBuffer(buf);
	}

	index_close(indexRel, ShareLock);
	result = merkle_hash_to_hex(&combinedHash);
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
    int             nonZeroNodes = 0;
    int             totalNodes;
    int             nodesPerPage;
    int             numTreePages;
    Oid             relid = PG_GETARG_OID(0);
    Oid             indexOid;
    Relation        heapRel;
    Relation        indexRel;
    Buffer          metabuf;
    Page            metapage;
    MerkleMetaPageData *meta;
    StringInfoData  buf;
    int             nodeIdx;
    int             pageNum;
    int             nkeys;
    int             i;
    TupleDesc       heapTupdesc;
    int             fanout;
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
	if (merkle_index_is_dynamic(indexRel))
	{
		char *dynamic_stats = merkle_dynamic_stats_json(indexRel);

		index_close(indexRel, AccessShareLock);
		table_close(heapRel, AccessShareLock);
		PG_RETURN_TEXT_P(cstring_to_text(dynamic_stats));
	}

	/* Validate on-disk/hash formats before exposing metadata. */
	merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
					 NULL, NULL, NULL);
    
    /* Read metadata page */
    metabuf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
    LockBuffer(metabuf, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuf);
    meta = MerklePageGetMeta(metapage);
    
    /* Get values we need before releasing the meta buffer */
    totalNodes = meta->totalNodes;
    nodesPerPage = meta->nodesPerPage;
    numTreePages = meta->numTreePages;
    fanout = (meta->version >= 5) ? meta->fanout : MERKLE_DEFAULT_FANOUT;
    
    /* Count non-zero nodes across all tree pages */
    nodeIdx = 0;
    for (pageNum = 0; pageNum < numTreePages; pageNum++)
    {
        Buffer      treebuf;
        Page        treepage;
        MerkleNode *nodes;
        int         nodesThisPage;
        int         j;
        
        treebuf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
        LockBuffer(treebuf, BUFFER_LOCK_SHARE);
        treepage = BufferGetPage(treebuf);
        nodes = (MerkleNode *) PageGetContents(treepage);
        
        nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
        
        for (j = 0; j < nodesThisPage; j++)
        {
            if (!merkle_hash_is_zero(&nodes[j].hash))
                nonZeroNodes++;
        }
        
        nodeIdx += nodesThisPage;
        UnlockReleaseBuffer(treebuf);
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
                     "\"num_partitions\": %d, "
                     "\"leaves_per_partition\": %d, "
                     "\"fanout\": %d, "
                     "\"nodes_per_partition\": %d, "
                     "\"total_nodes\": %d, "
                     "\"num_pages\": %d, "
                     "\"non_zero_nodes\": %d, "
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
                     meta->numPartitions,
                     meta->leavesPerPartition,
                     fanout,
                     meta->nodesPerPartition,
                     meta->totalNodes,
                     meta->numTreePages,
                     nonZeroNodes,
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
    FuncCallContext *funcctx;
    MemoryContext    oldcontext;
    
    /* First call setup - read all data and cache in memory */
    if (SRF_IS_FIRSTCALL())
    {
        Oid             relid = PG_GETARG_OID(0);
        Oid             indexOid;
        Relation        indexRel;
        TupleDesc       tupdesc;
        NodeHashData   *data;
        int             numPartitions;
        int             nodesPerPartition;
        int             leavesPerPartition;
        int             totalNodes;
        int             nodesPerPage;
        int             numTreePages;
        int             nodeIdx;
        int             pageNum;
        int             leafStart;

		merkle_require_fresh();
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ereport(NOTICE,
				(errmsg("merkle_node_hash() is a debug-only full node dump"),
				 errhint("Use merkle_get_node_hash() or batched operational helpers for recovery.")));
        
        /* Find Merkle index */
        indexOid = find_merkle_index(relid);
        if (!OidIsValid(indexOid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("no merkle index found on table %s",
                            get_rel_name(relid))));
        
		indexRel = merkle_open_consistent_index(indexOid);
		merkle_require_static_api(indexRel, "merkle_node_hash()");

        /* Read tree configuration from metadata */
        merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition, &nodesPerPartition,
                         &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);

        leafStart = nodesPerPartition - leavesPerPartition + 1;
        
        /* Allocate result arrays in multi-call context (will persist across calls) */
        data = palloc(sizeof(NodeHashData));
        data->nodeids = palloc(totalNodes * sizeof(Datum));
        data->partitions = palloc(totalNodes * sizeof(Datum));
        data->nodeinpartitions = palloc(totalNodes * sizeof(Datum));
        data->isleafs = palloc(totalNodes * sizeof(bool));
        data->hashes = palloc(totalNodes * sizeof(Datum));
        data->leafids = palloc(totalNodes * sizeof(Datum));
        data->leafidnulls = palloc(totalNodes * sizeof(bool));
        
        /*
         * Read all node data from tree pages.
         * We iterate page-wise and deep-copy all data into our arrays
         * so we can safely release the buffer locks before returning.
         */
        nodeIdx = 0;
        for (pageNum = 0; pageNum < numTreePages && nodeIdx < totalNodes; pageNum++)
        {
            Buffer      buf;
            Page        page;
            MerkleNode *nodes;
            int         nodesThisPage;
            int         j;
            
            buf = ReadBuffer(indexRel, MERKLE_TREE_START_BLKNO + pageNum);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            nodes = (MerkleNode *) PageGetContents(page);
            
            nodesThisPage = Min(nodesPerPage, totalNodes - nodeIdx);
            
            for (j = 0; j < nodesThisPage; j++)
            {
                int globalIdx = nodeIdx + j;
                int partition = globalIdx / nodesPerPartition;
                int nodeInPartition = (globalIdx % nodesPerPartition) + 1;  /* 1-indexed */
                bool isLeaf = (nodeInPartition >= leafStart);
                int leafId = -1;
                char nodeid_str[32];
                
                /* Format node ID string */
                snprintf(nodeid_str, sizeof(nodeid_str), "%d_%d", partition, nodeInPartition);
                
                /* Compute global leaf ID for leaf nodes */
                if (isLeaf)
                {
                    int leafInPartition = nodeInPartition - leafStart;
                    leafId = partition * leavesPerPartition + leafInPartition;
                }
                
                /* Deep copy all values into the persistent memory context */
                data->nodeids[globalIdx] = CStringGetTextDatum(nodeid_str);
                data->partitions[globalIdx] = Int32GetDatum(partition);
                data->nodeinpartitions[globalIdx] = Int32GetDatum(nodeInPartition);
                data->isleafs[globalIdx] = isLeaf;
                data->hashes[globalIdx] = CStringGetTextDatum(merkle_hash_to_hex(&nodes[j].hash));
                data->leafids[globalIdx] = Int32GetDatum(leafId);
                data->leafidnulls[globalIdx] = !isLeaf;
            }
            
            nodeIdx += nodesThisPage;
            UnlockReleaseBuffer(buf);
        }
        
		index_close(indexRel, ShareLock);
        
        /* Build tuple descriptor for result set */
        tupdesc = CreateTemplateTupleDesc(6);
        TupleDescInitEntry(tupdesc, 1, "nodeid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "partition", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "node_in_partition", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "is_leaf", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5, "leaf_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 6, "hash", TEXTOID, -1, 0);
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = totalNodes;
        funcctx->user_fctx = data;
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    /* Per-call setup - return one row at a time */
    funcctx = SRF_PERCALL_SETUP();
    
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        NodeHashData *data = (NodeHashData *) funcctx->user_fctx;
        int idx = funcctx->call_cntr;
        Datum values[6];
        bool nulls[6] = {false, false, false, false, false, false};
        HeapTuple tuple;
        
        /* Set leaf_id to NULL for non-leaf nodes */
        nulls[4] = data->leafidnulls[idx];
        
        values[0] = data->nodeids[idx];
        values[1] = data->partitions[idx];
        values[2] = data->nodeinpartitions[idx];
        values[3] = BoolGetDatum(data->isleafs[idx]);
        values[4] = data->leafids[idx];
        values[5] = data->hashes[idx];
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    
    SRF_RETURN_DONE(funcctx);
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
    FuncCallContext *funcctx;
    MemoryContext    oldcontext;
    
    if (SRF_IS_FIRSTCALL())
    {
        Oid             relid = PG_GETARG_OID(0);
        Oid             indexOid;
        Relation        heapRel;
        Relation        indexRel;
        TableScanDesc   scan;
        TupleTableSlot *slot;
        TupleDesc       tupdesc;
        TupleDesc       indexTupdesc;
        int             totalLeaves;
        int            *counts;
        StringInfo     *keyLists;  /* Store full key lists as strings */
        int             i;
        int             nkeys;
        int16          *indkey;
        Oid            *keytypes;
        FmgrInfo       *keyoutfuncs;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ereport(WARNING,
				(errmsg("merkle_leaf_tuples() is a debug-only full heap scan"),
				 errhint("Use merkle_get_leaf_members(index, leaf_id) for selective recovery.")));
        
        /* Find Merkle index */
        indexOid = find_merkle_index(relid);
        if (!OidIsValid(indexOid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("no merkle index found on table %s",
                            get_rel_name(relid))));
        
        heapRel = table_open(relid, AccessShareLock);
        indexRel = index_open(indexOid, AccessShareLock);
		merkle_require_static_api(indexRel, "merkle_leaf_tuples()");
        
		merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, &totalLeaves,
						 NULL, NULL, NULL);
        
        /* Get index key info */
        indexTupdesc = RelationGetDescr(indexRel);
        nkeys = indexRel->rd_index->indnkeyatts;
        indkey = indexRel->rd_index->indkey.values;
        
        /* Cache output functions for key display */
        keytypes = palloc(nkeys * sizeof(Oid));
        keyoutfuncs = palloc(nkeys * sizeof(FmgrInfo));
        for (i = 0; i < nkeys; i++)
        {
            Oid typoutput;
            bool typIsVarlena;
            
            keytypes[i] = TupleDescAttr(indexTupdesc, i)->atttypid;
            getTypeOutputInfo(keytypes[i], &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &keyoutfuncs[i]);
        }
        
        /* Allocate arrays for counting and collecting keys */
        counts = palloc0(totalLeaves * sizeof(int));
        keyLists = palloc0(totalLeaves * sizeof(StringInfo));
        for (i = 0; i < totalLeaves; i++)
        {
            keyLists[i] = palloc(sizeof(StringInfoData));
            initStringInfo(keyLists[i]);
            appendStringInfoChar(keyLists[i], '{');
        }
        
        /* Scan heap and collect stats */
        slot = table_slot_create(heapRel, NULL);
        scan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
        
        /* Reuse per-tuple scratch buffers to avoid per-row palloc/pfree churn */
        {
            Datum      *keyValues;
            bool       *keyNulls;
            StringInfoData keyStr;
            
            keyValues = palloc(nkeys * sizeof(Datum));
            keyNulls = palloc(nkeys * sizeof(bool));
            initStringInfo(&keyStr);
        
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                int         leafId;
                int         k;
            
                /* Get all key values */
                for (k = 0; k < nkeys; k++)
                {
                    int heapAttr = indkey[k];
                    keyValues[k] = slot_getattr(slot, heapAttr, &keyNulls[k]);
                }
            
				/* Build a display string; routing itself has one implementation. */
                resetStringInfo(&keyStr);
                if (nkeys == 1)
                {
                    if (keyNulls[0])
                        appendStringInfoString(&keyStr, "NULL");
                    else
                    {
                        char *str = OutputFunctionCall(&keyoutfuncs[0], keyValues[0]);
                        appendStringInfoString(&keyStr, str);
                        pfree(str);
                    }
                }
                else
                {
                    appendStringInfoChar(&keyStr, '(');
                    for (k = 0; k < nkeys; k++)
                    {
                        if (k > 0)
                            appendStringInfoChar(&keyStr, ',');
                        
                        if (keyNulls[k])
                            appendStringInfoString(&keyStr, "NULL");
                        else
                        {
                            char *str = OutputFunctionCall(&keyoutfuncs[k], keyValues[k]);
                            appendStringInfoString(&keyStr, str);
                            pfree(str);
                        }
                    }
                    appendStringInfoChar(&keyStr, ')');
				}

				{
					MerkleRoute route;

					merkle_compute_route(indexRel, keyValues, keyNulls,
										 nkeys, &route);
					leafId = route.leaf_id;
                }
            
                /* Add to key list - no limit, show all keys */
                if (counts[leafId] > 0)
                    appendStringInfoString(keyLists[leafId], ", ");
                appendStringInfoString(keyLists[leafId], keyStr.data);
            
                counts[leafId]++;
            }
            
            pfree(keyStr.data);
            pfree(keyValues);
            pfree(keyNulls);
        }
        
        /* Close key lists */
        for (i = 0; i < totalLeaves; i++)
            appendStringInfoChar(keyLists[i], '}');
        
        table_endscan(scan);
        ExecDropSingleTupleTableSlot(slot);
        index_close(indexRel, AccessShareLock);
        table_close(heapRel, AccessShareLock);
        
        /* Build tuple descriptor */
        tupdesc = CreateTemplateTupleDesc(3);
        TupleDescInitEntry(tupdesc, 1, "leaf_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "tuple_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "keys", TEXTOID, -1, 0); 
        
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = totalLeaves;
        
        /* Store data */
        {
            typedef struct {
                int *counts;
                StringInfo *keyLists;
                int totalLeaves;
            } LeafData;
            
            LeafData *data = palloc(sizeof(LeafData));
            data->counts = counts;
            data->keyLists = keyLists;
            data->totalLeaves = totalLeaves;
            funcctx->user_fctx = data;
        }
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        typedef struct {
            int *counts;
            StringInfo *keyLists;
            int totalLeaves;
        } LeafData;
        
        LeafData *data = (LeafData *) funcctx->user_fctx;
        int leafId = funcctx->call_cntr;
        Datum values[3];
        bool nulls[3] = {false, false, false};
        HeapTuple tuple;
        
        values[0] = Int32GetDatum(leafId);
        values[1] = Int32GetDatum(data->counts[leafId]);
        values[2] = CStringGetTextDatum(data->keyLists[leafId]->data);
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    
    SRF_RETURN_DONE(funcctx);
}

/*
 * merkle_leaf_id() - Compute leaf bucket ID for key value(s)
 *
 * Auto-detects configuration from the merkle index on the table.
 * Supports any number of key columns (determined by the index).
 *
 * Usage:
 *   SELECT merkle_leaf_id('usertable', 1200);           -- 1 key
 *   SELECT merkle_leaf_id('usertable', 1200, 'text');   -- 2 keys
 *   SELECT merkle_leaf_id('usertable', a, b, c, d, e);  -- 5 keys
 */
Datum
merkle_leaf_id(PG_FUNCTION_ARGS)
{
    Oid             relid;
    Oid             indexOid;
    Relation        indexRel;
	MerkleRoute     route;
    TupleDesc       indexTupdesc;
    int             nkeys;
    int             nargs;
    Datum          *keyValues;
    bool           *keyNulls;
    int             i;
    
    /* First arg must be table OID */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("table name cannot be null")));
    
    relid = PG_GETARG_OID(0);
    
    /* Find Merkle index */
    indexOid = find_merkle_index(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on table %s",
                        get_rel_name(relid))));
    
    indexRel = index_open(indexOid, AccessShareLock);
	merkle_require_static_api(indexRel, "merkle_leaf_id()");
    indexTupdesc = RelationGetDescr(indexRel);
    nkeys = indexRel->rd_index->indnkeyatts;
    
    /* Check number of arguments provided (fcinfo->nargs includes table) */
    nargs = PG_NARGS() - 1;  /* Subtract table arg */
    
    if (nargs != nkeys)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle_leaf_id expects %d key argument(s), got %d",
                        nkeys, nargs),
                 errhint("The merkle index on %s has %d key column(s).",
                         get_rel_name(relid), nkeys)));
    
    /* Dynamically allocate arrays based on actual number of keys */
    keyValues = palloc(nkeys * sizeof(Datum));
    keyNulls = palloc(nkeys * sizeof(bool));
    
    /* 
     * Get key values and coerce types if necessary.
     */
    for (i = 0; i < nkeys; i++)
    {
        keyNulls[i] = PG_ARGISNULL(i + 1);
        if (!keyNulls[i])
        {
            Datum argValue = PG_GETARG_DATUM(i + 1);
            Oid argType = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
            Oid expectedType = TupleDescAttr(indexTupdesc, i)->atttypid;
            
            if (argType == UNKNOWNOID)
            {
                Oid typInput;
                Oid typIOParam;
                getTypeInputInfo(expectedType, &typInput, &typIOParam);
                keyValues[i] = OidInputFunctionCall(typInput, 
                                                    DatumGetCString(argValue),
                                                    typIOParam, -1);
            }
            else if (argType != expectedType)
            {
                Oid castFunc;
                CoercionPathType pathtype;
                pathtype = find_coercion_pathway(expectedType, argType, 
                                                 COERCION_IMPLICIT, &castFunc);
                if (pathtype == COERCION_PATH_FUNC && OidIsValid(castFunc))
                    keyValues[i] = OidFunctionCall1(castFunc, argValue);
                else if (pathtype == COERCION_PATH_RELABELTYPE)
                    keyValues[i] = argValue;
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("argument %d has type %s, expected %s",
                                    i + 1, format_type_be(argType),
                                    format_type_be(expectedType))));
            }
            else
                keyValues[i] = argValue;
        }
        else
            keyValues[i] = (Datum) 0;
    }
    
	merkle_compute_route(indexRel, keyValues, keyNulls, nkeys, &route);

    /* Build result tuple */
    {
        TupleDesc tupdesc;
        Datum     values[3];
        bool      nulls[3] = {false, false, false};
        HeapTuple tuple;

        get_call_result_type(fcinfo, NULL, &tupdesc);
        /* NOTE: get_call_result_type already returns a blessed descriptor,
         * so we should NOT call BlessTupleDesc() again to avoid reference leaks */

		values[0] = Int32GetDatum(route.leaf_id);
		values[1] = Int32GetDatum(route.partition_id);
		values[2] = Int32GetDatum(route.node_in_partition);

        tuple = heap_form_tuple(tupdesc, values, nulls);
        
        pfree(keyValues);
        pfree(keyNulls);
        index_close(indexRel, AccessShareLock);
        
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
}

/*
 * merkle_bucket_for_key() - Scalar leaf bucket helper for expression indexes.
 *
 * This mirrors merkle_leaf_id(), but returns only the global leaf ID. It is
 * suitable for a functional B-tree index used by selective recovery.
 */
Datum
merkle_bucket_for_key(PG_FUNCTION_ARGS)
{
    Oid             relid;
    Oid             indexOid;
    Relation        indexRel;
    TupleDesc       indexTupdesc;
    int             nkeys;
    int             nargs;
    Datum          *keyValues;
    bool           *keyNulls;
    int             i;
	MerkleRoute     route;

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("table or merkle index name cannot be null")));

    relid = PG_GETARG_OID(0);
    indexOid = resolve_merkle_index_arg(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on relation %s",
                        get_rel_name(relid))));

    indexRel = index_open(indexOid, AccessShareLock);
	merkle_require_static_api(indexRel, "merkle_bucket_for_key()");
    indexTupdesc = RelationGetDescr(indexRel);
    nkeys = indexRel->rd_index->indnkeyatts;
    nargs = PG_NARGS() - 1;
    if (nargs != nkeys)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("merkle_bucket_for_key expects %d key argument(s), got %d",
                        nkeys, nargs)));

    keyValues = palloc(nkeys * sizeof(Datum));
    keyNulls = palloc(nkeys * sizeof(bool));

    for (i = 0; i < nkeys; i++)
    {
        keyNulls[i] = PG_ARGISNULL(i + 1);
        if (!keyNulls[i])
        {
            Datum argValue = PG_GETARG_DATUM(i + 1);
            Oid argType = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
            Oid expectedType = TupleDescAttr(indexTupdesc, i)->atttypid;

            if (argType == UNKNOWNOID)
            {
                Oid typInput;
                Oid typIOParam;

                getTypeInputInfo(expectedType, &typInput, &typIOParam);
                keyValues[i] = OidInputFunctionCall(typInput,
                                                    DatumGetCString(argValue),
                                                    typIOParam, -1);
            }
            else if (argType != expectedType)
            {
                Oid castFunc;
                CoercionPathType pathtype;

                pathtype = find_coercion_pathway(expectedType, argType,
                                                 COERCION_IMPLICIT, &castFunc);
                if (pathtype == COERCION_PATH_FUNC && OidIsValid(castFunc))
                    keyValues[i] = OidFunctionCall1(castFunc, argValue);
                else if (pathtype == COERCION_PATH_RELABELTYPE)
                    keyValues[i] = argValue;
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("argument %d has type %s, expected %s",
                                    i + 1, format_type_be(argType),
                                    format_type_be(expectedType))));
            }
            else
                keyValues[i] = argValue;
        }
        else
            keyValues[i] = (Datum) 0;
    }

	merkle_compute_route(indexRel, keyValues, keyNulls, nkeys, &route);

    pfree(keyValues);
    pfree(keyNulls);
    index_close(indexRel, AccessShareLock);

	PG_RETURN_INT64((int64) route.leaf_id);
}

Datum
merkle_get_node_hash(PG_FUNCTION_ARGS)
{
    Oid         relid = PG_GETARG_OID(0);
    int32       partition = PG_GETARG_INT32(1);
    int32       nodeInPartition = PG_GETARG_INT32(2);
    Oid         indexOid;
    Relation    indexRel;
    int         numPartitions;
    int         nodesPerPartition;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    MerkleHash  hash;

	merkle_require_fresh();

    indexOid = resolve_merkle_index_arg(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on relation %s",
                        get_rel_name(relid))));

	indexRel = merkle_open_consistent_index(indexOid);
	merkle_require_static_api(indexRel, "merkle_get_node_hash()");
    merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition,
                     &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);
    nodeIdx = global_node_index(partition, nodeInPartition, numPartitions,
                                nodesPerPartition);
    read_merkle_node_hash_with_meta(indexRel, nodeIdx, totalNodes,
                                    nodesPerPage, numTreePages, &hash);
	index_close(indexRel, ShareLock);

    PG_RETURN_TEXT_P(cstring_to_text(merkle_hash_to_hex(&hash)));
}

Datum
merkle_get_partition_root_hash(PG_FUNCTION_ARGS)
{
    Oid         relid = PG_GETARG_OID(0);
    int32       partition = PG_GETARG_INT32(1);
    Oid         indexOid;
    Relation    indexRel;
    int         numPartitions;
    int         nodesPerPartition;
    int         totalNodes;
    int         nodesPerPage;
    int         numTreePages;
    int         nodeIdx;
    MerkleHash  hash;

	merkle_require_fresh();

    indexOid = resolve_merkle_index_arg(relid);
    if (!OidIsValid(indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no merkle index found on relation %s",
                        get_rel_name(relid))));

	indexRel = merkle_open_consistent_index(indexOid);
	merkle_require_static_api(indexRel, "merkle_get_partition_root_hash()");
    merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition,
                     &totalNodes, NULL, &nodesPerPage, &numTreePages, NULL);
    nodeIdx = global_node_index(partition, 1, numPartitions, nodesPerPartition);
    read_merkle_node_hash_with_meta(indexRel, nodeIdx, totalNodes,
                                    nodesPerPage, numTreePages, &hash);
	index_close(indexRel, ShareLock);

    PG_RETURN_TEXT_P(cstring_to_text(merkle_hash_to_hex(&hash)));
}

Datum
merkle_get_partition_root_hashes(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext	oldcontext;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (SRF_IS_FIRSTCALL())
	{
		Oid		relid = PG_GETARG_OID(0);
		Oid		indexOid;
		Relation	indexRel;
		TupleDesc	tupdesc;
		int		numPartitions;
		int		nodesPerPartition;
		int		totalNodes;
		int		nodesPerPage;
		int		numTreePages;
		Datum   *partitions;
		Datum   *hashes;
		int		partition;

		merkle_require_fresh();

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		if (profile_enabled)
			INSTR_TIME_SET_CURRENT(start_time);

		indexOid = resolve_merkle_index_arg(relid);
		if (!OidIsValid(indexOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("no merkle index found on relation %s",
							get_rel_name(relid))));

		indexRel = merkle_open_consistent_index(indexOid);
		merkle_require_static_api(indexRel, "merkle_get_partition_root_hashes()");
		merkle_read_meta(indexRel, &numPartitions, NULL, &nodesPerPartition,
						 &totalNodes, NULL, &nodesPerPage, &numTreePages,
						 NULL);

		partitions = palloc(numPartitions * sizeof(Datum));
		hashes = palloc(numPartitions * sizeof(Datum));

		for (partition = 0; partition < numPartitions; partition++)
		{
			MerkleHash hash;
			int		nodeIdx;

			nodeIdx = global_node_index(partition, 1, numPartitions,
										nodesPerPartition);
			read_merkle_node_hash_with_meta(indexRel, nodeIdx, totalNodes,
											nodesPerPage, numTreePages, &hash);
			partitions[partition] = Int32GetDatum(partition);
			hashes[partition] = CStringGetTextDatum(merkle_hash_to_hex(&hash));
		}

		index_close(indexRel, ShareLock);

		if (profile_enabled)
		{
			INSTR_TIME_SET_CURRENT(elapsed_time);
			INSTR_TIME_SUBTRACT(elapsed_time, start_time);
			merkle_recovery_profile_state.root_hash_helper_calls++;
			merkle_recovery_profile_state.root_hash_nodes_returned += numPartitions;
			merkle_recovery_profile_state.root_hash_helper_us +=
				INSTR_TIME_GET_MICROSEC(elapsed_time);
		}

		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, 1, "partition", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "hash", TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->max_calls = numPartitions;

		{
			typedef struct {
				Datum *partitions;
				Datum *hashes;
			} PartitionRootData;

			PartitionRootData *data = palloc(sizeof(PartitionRootData));
			data->partitions = partitions;
			data->hashes = hashes;
			funcctx->user_fctx = data;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		typedef struct {
			Datum *partitions;
			Datum *hashes;
		} PartitionRootData;

		PartitionRootData *data = (PartitionRootData *) funcctx->user_fctx;
		Datum values[2];
		bool nulls[2] = {false, false};
		HeapTuple tuple;
		int idx = funcctx->call_cntr;

		values[0] = data->partitions[idx];
		values[1] = data->hashes[idx];
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
merkle_get_child_hashes(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext	oldcontext;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	if (SRF_IS_FIRSTCALL())
	{
		Oid		relid = PG_GETARG_OID(0);
		int32	partition = PG_GETARG_INT32(1);
		int32	nodeInPartition = PG_GETARG_INT32(2);
		Oid		indexOid;
		Relation	indexRel;
		TupleDesc	tupdesc;
		int		numPartitions;
		int		nodesPerPartition;
		int		leavesPerPartition;
		int		totalNodes;
		int		nodesPerPage;
		int		numTreePages;
		int		fanout;
		int		internalNodes;
		int		maxChildren;
		int		childCount = 0;
		int		child;
		Datum   *childNodes;
		Datum   *childHashes;

		merkle_require_fresh();

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		if (profile_enabled)
			INSTR_TIME_SET_CURRENT(start_time);

		indexOid = resolve_merkle_index_arg(relid);
		if (!OidIsValid(indexOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("no merkle index found on relation %s",
							get_rel_name(relid))));

		indexRel = merkle_open_consistent_index(indexOid);
		merkle_require_static_api(indexRel, "merkle_get_child_hashes()");
		merkle_read_meta(indexRel, &numPartitions, &leavesPerPartition,
						 &nodesPerPartition, &totalNodes, NULL,
						 &nodesPerPage, &numTreePages, &fanout);
		(void) global_node_index(partition, nodeInPartition, numPartitions,
								 nodesPerPartition);

		internalNodes = nodesPerPartition - leavesPerPartition;
		if (nodeInPartition > internalNodes)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("merkle node_in_partition %d is a leaf and has no children",
							nodeInPartition)));

		maxChildren = fanout;
		childNodes = palloc(maxChildren * sizeof(Datum));
		childHashes = palloc(maxChildren * sizeof(Datum));

		for (child = 1; child <= fanout; child++)
		{
			int childNode = fanout * (nodeInPartition - 1) + child + 1;

			if (childNode <= nodesPerPartition)
			{
				MerkleHash hash;
				int		childIdx;

				childIdx = global_node_index(partition, childNode,
											 numPartitions, nodesPerPartition);
				read_merkle_node_hash_with_meta(indexRel, childIdx, totalNodes,
												nodesPerPage, numTreePages,
												&hash);
				childNodes[childCount] = Int32GetDatum(childNode);
				childHashes[childCount] = CStringGetTextDatum(merkle_hash_to_hex(&hash));
				childCount++;
			}
		}

		index_close(indexRel, ShareLock);

		if (profile_enabled)
		{
			INSTR_TIME_SET_CURRENT(elapsed_time);
			INSTR_TIME_SUBTRACT(elapsed_time, start_time);
			merkle_recovery_profile_state.child_hash_helper_calls++;
			merkle_recovery_profile_state.child_hash_nodes_returned += childCount;
			merkle_recovery_profile_state.child_hash_helper_us +=
				INSTR_TIME_GET_MICROSEC(elapsed_time);
		}

		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupdesc, 1, "child_node_in_partition", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "hash", TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->max_calls = childCount;

		{
			typedef struct {
				Datum *childNodes;
				Datum *childHashes;
			} ChildHashData;

			ChildHashData *data = palloc(sizeof(ChildHashData));
			data->childNodes = childNodes;
			data->childHashes = childHashes;
			funcctx->user_fctx = data;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		typedef struct {
			Datum *childNodes;
			Datum *childHashes;
		} ChildHashData;

		ChildHashData *data = (ChildHashData *) funcctx->user_fctx;
		Datum values[2];
		bool nulls[2] = {false, false};
		HeapTuple tuple;
		int idx = funcctx->call_cntr;

		values[0] = data->childNodes[idx];
		values[1] = data->childHashes[idx];
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
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

static void
merkle_deconstruct_int4_array(ArrayType *array, Datum **values, bool **nulls,
							  int *count, const char *argument_name)
{
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be a one-dimensional integer array", argument_name)));

	deconstruct_array(array, INT4OID, sizeof(int32), true, 'i',
					  values, nulls, count);
}

/* Batched paired lookup: partitions[i], node_in_partitions[i]. */
Datum
merkle_get_node_hashes(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	ArrayType	   *partition_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType	   *node_array = PG_GETARG_ARRAYTYPE_P(2);
	Datum		   *partitions;
	Datum		   *nodes;
	bool		   *partition_nulls;
	bool		   *node_nulls;
	int				partition_count;
	int				node_count;
	Oid				indexOid;
	Relation		indexRel;
	MerkleGeometry geometry;
	int				nodesPerPage;
	int				numTreePages;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	int				i;

	merkle_require_fresh();

	merkle_deconstruct_int4_array(partition_array, &partitions,
								&partition_nulls, &partition_count, "partitions");
	merkle_deconstruct_int4_array(node_array, &nodes, &node_nulls,
								&node_count, "node_in_partitions");
	if (partition_count != node_count)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("partitions and node_in_partitions must have equal lengths")));

	indexOid = resolve_merkle_index_arg(relid);
	indexRel = merkle_open_consistent_index(indexOid);
	merkle_require_static_api(indexRel, "merkle_get_node_hashes()");
	merkle_geometry_from_index(indexRel, &geometry);
	merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
					 &nodesPerPage, &numTreePages, NULL);
	tupstore = merkle_begin_materialized_srf(fcinfo, &tupdesc);

	for (i = 0; i < partition_count; i++)
	{
		MerkleHash hash;
		Datum		out[3];
		bool		outnulls[3] = {false, false, false};
		int			partition;
		int			node;
		int			global;
		char	   *hex;

		if (partition_nulls[i] || node_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("batched Merkle node coordinates cannot contain nulls")));
		partition = DatumGetInt32(partitions[i]);
		node = DatumGetInt32(nodes[i]);
		global = merkle_geometry_global_node(&geometry, partition, node);
		read_merkle_node_hash_with_meta(indexRel, global, geometry.total_nodes,
									nodesPerPage, numTreePages, &hash);
		hex = merkle_hash_to_hex(&hash);
		out[0] = Int32GetDatum(partition);
		out[1] = Int32GetDatum(node);
		out[2] = CStringGetTextDatum(hex);
		tuplestore_putvalues(tupstore, tupdesc, out, outnulls);
		pfree(hex);
	}

	index_close(indexRel, ShareLock);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

/* Batched child lookup for paired parent coordinates. */
Datum
merkle_get_children_batch(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	ArrayType	   *partition_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType	   *node_array = PG_GETARG_ARRAYTYPE_P(2);
	Datum		   *partitions;
	Datum		   *nodes;
	bool		   *partition_nulls;
	bool		   *node_nulls;
	int				partition_count;
	int				node_count;
	Oid				indexOid;
	Relation		indexRel;
	MerkleGeometry geometry;
	int				nodesPerPage;
	int				numTreePages;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	int				i;
	int				children_returned = 0;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time		start_time;
	instr_time		elapsed_time;

	merkle_require_fresh();

	if (profile_enabled)
		INSTR_TIME_SET_CURRENT(start_time);

	merkle_deconstruct_int4_array(partition_array, &partitions,
								&partition_nulls, &partition_count, "partitions");
	merkle_deconstruct_int4_array(node_array, &nodes, &node_nulls,
								&node_count, "node_in_partitions");
	if (partition_count != node_count)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("partitions and node_in_partitions must have equal lengths")));

	indexOid = resolve_merkle_index_arg(relid);
	indexRel = merkle_open_consistent_index(indexOid);
	merkle_require_static_api(indexRel, "merkle_get_children_batch()");
	merkle_geometry_from_index(indexRel, &geometry);
	merkle_read_meta(indexRel, NULL, NULL, NULL, NULL, NULL,
					 &nodesPerPage, &numTreePages, NULL);
	tupstore = merkle_begin_materialized_srf(fcinfo, &tupdesc);

	for (i = 0; i < partition_count; i++)
	{
		int partition;
		int parent;
		int ordinal;

		if (partition_nulls[i] || node_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("batched Merkle node coordinates cannot contain nulls")));
		partition = DatumGetInt32(partitions[i]);
		parent = DatumGetInt32(nodes[i]);
		(void) merkle_geometry_global_node(&geometry, partition, parent);

		for (ordinal = 0; ordinal < geometry.fanout; ordinal++)
		{
			int child = merkle_geometry_child_node(&geometry, parent, ordinal);
			int global;
			MerkleHash hash;
			Datum out[4];
			bool outnulls[4] = {false, false, false, false};
			char *hex;

			if (child == 0)
				continue;
			global = merkle_geometry_global_node(&geometry, partition, child);
			read_merkle_node_hash_with_meta(indexRel, global,
										geometry.total_nodes, nodesPerPage,
										numTreePages, &hash);
			hex = merkle_hash_to_hex(&hash);
			out[0] = Int32GetDatum(partition);
			out[1] = Int32GetDatum(parent);
			out[2] = Int32GetDatum(child);
			out[3] = CStringGetTextDatum(hex);
			tuplestore_putvalues(tupstore, tupdesc, out, outnulls);
			pfree(hex);
			children_returned++;
		}
	}
	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(elapsed_time);
		INSTR_TIME_SUBTRACT(elapsed_time, start_time);
		merkle_recovery_profile_state.child_hash_helper_calls++;
		merkle_recovery_profile_state.child_hash_nodes_returned +=
			children_returned;
		merkle_recovery_profile_state.child_hash_helper_us +=
			INSTR_TIME_GET_MICROSEC(elapsed_time);
	}

	index_close(indexRel, ShareLock);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

/*
 * Selective leaf membership.  The generated predicate exactly matches the
 * supported functional B-tree bucket expression, allowing an index scan.
 */
Datum
merkle_get_leaf_members(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	int32			leaf_id = PG_GETARG_INT32(1);
	Oid				indexOid;
	Oid				heapOid;
	Relation		indexRel;
	Relation		heapRel;
	MerkleGeometry geometry;
	StringInfoData	columns;
	StringInfoData	query;
	char		   *qualified_heap;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	Oid				argtypes[1] = {INT4OID};
	Datum			args[1];
	int				i;
	int				spi_result;

	merkle_require_fresh();

	indexOid = resolve_merkle_index_arg(relid);
	indexRel = merkle_open_consistent_index(indexOid);
	merkle_require_static_api(indexRel, "merkle_get_leaf_members()");
	merkle_geometry_from_index(indexRel, &geometry);
	if (leaf_id < 0 || leaf_id >= geometry.total_leaves)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Merkle leaf ID %d is out of range [0, %d)",
						leaf_id, geometry.total_leaves)));

	heapOid = IndexGetRelation(indexOid, false);
	heapRel = table_open(heapOid, NoLock);
	qualified_heap = quote_qualified_identifier(
		get_namespace_name(RelationGetNamespace(heapRel)),
		RelationGetRelationName(heapRel));
	initStringInfo(&columns);
	for (i = 0; i < indexRel->rd_index->indnkeyatts; i++)
	{
		AttrNumber attnum = indexRel->rd_index->indkey.values[i];

		if (attnum <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("merkle_get_leaf_members does not support expression index keys")));
		if (i > 0)
			appendStringInfoString(&columns, ", ");
		appendStringInfoString(&columns,
			quote_identifier(NameStr(TupleDescAttr(RelationGetDescr(heapRel),
												attnum - 1)->attname)));
	}

	initStringInfo(&query);
	appendStringInfo(&query,
		"SELECT ctid::text, ROW(%s)::text FROM %s "
		"WHERE merkle_bucket_for_key(%u::regclass, %s) = $1",
		columns.data, qualified_heap, indexOid, columns.data);

	tupstore = merkle_begin_materialized_srf(fcinfo, &tupdesc);
	args[0] = Int32GetDatum(leaf_id);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed in merkle_get_leaf_members");
	spi_result = SPI_execute_with_args(query.data, 1, argtypes, args, NULL,
								   true, 0);
	if (spi_result != SPI_OK_SELECT)
		elog(ERROR, "selective Merkle leaf lookup failed: SPI result %d", spi_result);

	for (i = 0; i < (int) SPI_processed; i++)
	{
		HeapTuple spi_tuple = SPI_tuptable->vals[i];
		char *tid_text = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 1);
		char *key_text = SPI_getvalue(spi_tuple, SPI_tuptable->tupdesc, 2);
		Datum out[2];
		bool outnulls[2] = {false, false};

		out[0] = CStringGetTextDatum(tid_text);
		out[1] = CStringGetTextDatum(key_text);
		tuplestore_putvalues(tupstore, tupdesc, out, outnulls);
		pfree(tid_text);
		pfree(key_text);
	}

	SPI_finish();
	tuplestore_donestoring(tupstore);
	table_close(heapRel, NoLock);
	index_close(indexRel, ShareLock);
	pfree(columns.data);
	pfree(query.data);
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
