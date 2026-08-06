/*-------------------------------------------------------------------------
 *
 * merklebuild.c
 *    Build routine for Merkle access method.
 *
 * This file contains the implementation of building a new Merkle index
 * from scratch, including heap table scanning and initial Merkle tree
 * construction.
 *
 * Copyright (c) 2026, AriaBC PostgreSQL Extensions
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merklebuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/merkle.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define MERKLE_ENTRY_CHUNK_SIZE 4194304 /* fixed-size chunk; entries include partition routing */

typedef struct
{
	MerkleTupleHashEntry **chunks;
	int			num_chunks;
	int			max_chunks;
	size_t		num_entries;
	size_t		chunk_size;
} MerkleEntryArray;

static inline MerkleTupleHashEntry *
merkle_entry_at(const MerkleEntryArray *ea, size_t idx)
{
	size_t c = idx / ea->chunk_size;
	size_t o = idx % ea->chunk_size;
	return &ea->chunks[c][o];
}

typedef struct
{
	uint8		node_id[8];
	int		partition_id;
	int16		prefix_len;
	bool		is_leaf;
	int64		tuple_count;
	MerkleHash	hash;
} MerkleNodeRecord;

typedef struct
{
	MerkleNodeRecord *records;
	int			num_records;
	int			max_records;
} MerkleBulkNodeSet;

static void
bulk_node_set_init(MerkleBulkNodeSet *bs, int initial_capacity)
{
	bs->records = (MerkleNodeRecord *) palloc((size_t) initial_capacity * sizeof(MerkleNodeRecord));
	bs->num_records = 0;
	bs->max_records = initial_capacity;
}

static void
bulk_node_set_add(MerkleBulkNodeSet *bs, int partition_id, const uint8 *node_id, int prefix_len,
				  bool is_leaf, int64 tuple_count, const MerkleHash *hash)
{
	MerkleNodeRecord *rec;

	if (bs->num_records >= bs->max_records)
	{
		bs->max_records *= 2;
		bs->records = (MerkleNodeRecord *) repalloc(bs->records,
							(size_t) bs->max_records * sizeof(MerkleNodeRecord));
	}
	rec = &bs->records[bs->num_records++];
	rec->partition_id = partition_id;
	memcpy(rec->node_id, node_id, 8);
	rec->prefix_len   = (int16) prefix_len;
	rec->is_leaf      = is_leaf;
	rec->tuple_count  = tuple_count;
	memcpy(&rec->hash, hash, sizeof(MerkleHash));
}

/*
 * merkle_build_tree_pass1 - compute full tree in C, emit records into bs.
 *
 * Zero-copy slice recursion over sorted MerkleEntryArray.
 * Returns the XOR-hash of the current subtree.
 */
static MerkleHash
merkle_build_tree_pass1(MerkleBulkNodeSet *bs,
						int partition_id,
						const uint8 *node_id, int prefix_len,
						const MerkleEntryArray *ea, size_t start_idx, size_t num_entries,
						int fanout, int bits_per_split, int split_threshold,
						int max_prefix_len)
{
	int		   *bucket_counts;
	int		   *bucket_offsets;
	MerkleHash *bucket_hashes;
	MerkleHash	node_hash;
	int64		total_count = (int64) num_entries;
	int			i;
	int			b;

	merkle_hash_zero(&node_hash);
	if (num_entries <= 0)
		return node_hash;

	bucket_counts = (int *) palloc0(fanout * sizeof(int));
	bucket_offsets = (int *) palloc0(fanout * sizeof(int));
	bucket_hashes = (MerkleHash *) palloc0(fanout * sizeof(MerkleHash));

	b = 0;
	bucket_offsets[0] = 0;
	for (i = 0; i < (int) num_entries; i++)
	{
		MerkleTupleHashEntry *e = merkle_entry_at(ea, start_idx + (size_t) i);
		uint8 val = merkle_next_bits(e->key_hash, prefix_len, bits_per_split);
		while (b < (int) val && b < fanout)
		{
			b++;
			bucket_offsets[b] = i;
		}
		bucket_counts[b]++;
		merkle_hash_xor(&bucket_hashes[b], &e->tuple_hash);
	}
	while (b + 1 < fanout)
	{
		b++;
		bucket_offsets[b] = (int) num_entries;
	}

	for (i = 0; i < fanout; i++)
	{
		uint8	child_node_id[8];
		int		child_prefix_len = prefix_len + bits_per_split;

		merkle_bytea_extend(child_node_id, node_id, prefix_len, (uint8) i, bits_per_split);

		if (bucket_counts[i] > split_threshold && child_prefix_len < max_prefix_len)
		{
			MerkleHash child_hash = merkle_build_tree_pass1(
				bs, partition_id, child_node_id, child_prefix_len,
				ea, start_idx + (size_t) bucket_offsets[i], (size_t) bucket_counts[i],
				fanout, bits_per_split, split_threshold, max_prefix_len);
			merkle_hash_xor(&node_hash, &child_hash);
		}
		else
		{
			bulk_node_set_add(bs, partition_id, child_node_id, child_prefix_len,
							  true, (int64) bucket_counts[i], &bucket_hashes[i]);
			merkle_hash_xor(&node_hash, &bucket_hashes[i]);
		}
	}

	pfree(bucket_counts);
	pfree(bucket_offsets);
	pfree(bucket_hashes);

	/* Emit internal node record after children */
	if (prefix_len > 0)
	bulk_node_set_add(bs, partition_id, node_id, prefix_len, false, total_count, &node_hash);

	return node_hash;
}

#define BULK_INSERT_BATCH 256

static void
merkle_bulk_flush_nodes(Oid index_oid, const MerkleBulkNodeSet *bs)
{
	int i;

	for (i = 0; i < bs->num_records; i += BULK_INSERT_BATCH)
	{
		int			chunk_len = Min(BULK_INSERT_BATCH, bs->num_records - i);
		StringInfoData sql;
		Oid		   *argtypes;
		Datum	   *values;
		char	   *nulls;
		int			j;
		int			ret;

		argtypes = (Oid *) palloc((size_t) chunk_len * 7 * sizeof(Oid));
		values = (Datum *) palloc((size_t) chunk_len * 7 * sizeof(Datum));
		nulls = (char *) palloc((size_t) chunk_len * 7 * sizeof(char));
		memset(nulls, ' ', (size_t) chunk_len * 7);

		initStringInfo(&sql);
		appendStringInfoString(&sql,
			"INSERT INTO ariabc_internal.merkle_node"
			" (index_oid, partition_id, node_id, prefix_len, is_leaf, tuple_count, hash)"
			" VALUES ");

		for (j = 0; j < chunk_len; j++)
		{
			MerkleNodeRecord *r = &bs->records[i + j];
			int			base = j * 7;
			bytea	   *node_id_bytea = (bytea *) palloc(VARHDRSZ + 8);
			bytea	   *hash_bytea = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);

			SET_VARSIZE(node_id_bytea, VARHDRSZ + 8);
			memcpy(VARDATA(node_id_bytea), r->node_id, 8);

			SET_VARSIZE(hash_bytea, VARHDRSZ + MERKLE_HASH_BYTES);
			memcpy(VARDATA(hash_bytea), r->hash.data, MERKLE_HASH_BYTES);

			argtypes[base + 0] = OIDOID;
			argtypes[base + 1] = INT4OID;
			argtypes[base + 2] = BYTEAOID;
			argtypes[base + 3] = INT2OID;
			argtypes[base + 4] = BOOLOID;
			argtypes[base + 5] = INT8OID;
			argtypes[base + 6] = BYTEAOID;

			values[base + 0] = ObjectIdGetDatum(index_oid);
			values[base + 1] = Int32GetDatum(r->partition_id);
			values[base + 2] = PointerGetDatum(node_id_bytea);
			values[base + 3] = Int16GetDatum(r->prefix_len);
			values[base + 4] = BoolGetDatum(r->is_leaf);
			values[base + 5] = Int64GetDatum(r->tuple_count);
			values[base + 6] = PointerGetDatum(hash_bytea);

			if (j > 0)
				appendStringInfoString(&sql, ", ");
			appendStringInfo(&sql, "($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
							 base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7);
		}

		appendStringInfoString(&sql,
			" ON CONFLICT (index_oid, partition_id, node_id, prefix_len) DO UPDATE"
			"   SET is_leaf = EXCLUDED.is_leaf,"
			"       tuple_count = EXCLUDED.tuple_count,"
			"       hash = EXCLUDED.hash");

		ret = SPI_execute_with_args(sql.data, chunk_len * 7, argtypes, values, nulls, false, chunk_len);
		if (ret < 0)
			elog(ERROR, "merkle bulk INSERT failed at batch starting at %d", i);

		if (SPI_tuptable != NULL)
		{
			SPI_freetuptable(SPI_tuptable);
		}

		for (j = 0; j < chunk_len; j++)
		{
			int base = j * 7;
			pfree(DatumGetPointer(values[base + 2]));
			pfree(DatumGetPointer(values[base + 6]));
		}

		pfree(argtypes);
		pfree(values);
		pfree(nulls);
		pfree(sql.data);
	}
}

/*
 * Per-tuple callback state for index build
 */
typedef struct
{
	Relation	indexRel;
	Relation	heapRel;
	IndexFetchTableData *heapFetch;
	TupleTableSlot *heapSlot;
	double		indtuples;
	int			nkeys;			/* Number of index key columns */
	int			fanout;

	/* Chunked tuple tracking */
	MerkleTupleHashEntry **chunks;
	int			num_chunks;
	int			max_chunks;
	size_t		num_entries;

	int			bits_per_split;
	int			split_threshold;
	int			merge_threshold;
	int			num_partitions;
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
	if (ea->partition_id != eb->partition_id)
		return ea->partition_id < eb->partition_id ? -1 : 1;
	return memcmp(ea->key_hash, eb->key_hash, 8);
}

typedef struct
{
	int		chunk_idx;
	size_t	elem_idx;
	size_t	chunk_len;
	MerkleTupleHashEntry *entry;
} KWayHeapNode;

static void
kway_heap_sift_down(KWayHeapNode *heap, int heap_size, int idx)
{
	while (2 * idx + 1 < heap_size)
	{
		int left = 2 * idx + 1;
		int right = 2 * idx + 2;
		int smallest = idx;
		KWayHeapNode tmp;

		if (merkle_entry_key_cmp(heap[left].entry, heap[smallest].entry) < 0)
			smallest = left;
		if (right < heap_size && merkle_entry_key_cmp(heap[right].entry, heap[smallest].entry) < 0)
			smallest = right;

		if (smallest == idx)
			break;

		tmp = heap[idx];
		heap[idx] = heap[smallest];
		heap[smallest] = tmp;
		idx = smallest;
	}
}

static MerkleEntryArray
merkle_prepare_sorted_entries(MerkleBuildState *buildstate)
{
	MerkleEntryArray result;
	int num_chunks = buildstate->num_chunks;
	size_t total_entries = buildstate->num_entries;
	int c;

	result.num_entries = total_entries;
	result.chunk_size = MERKLE_ENTRY_CHUNK_SIZE;

	if (total_entries == 0)
	{
		result.num_chunks = 0;
		result.chunks = NULL;
		return result;
	}

	/* Sort each chunk individually */
	for (c = 0; c < num_chunks; c++)
	{
		size_t len = (c == num_chunks - 1) ?
			(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
			(size_t) MERKLE_ENTRY_CHUNK_SIZE;
		if (len > 1)
			qsort(buildstate->chunks[c], len, sizeof(MerkleTupleHashEntry), merkle_entry_key_cmp);
	}

	if (num_chunks == 1)
	{
		result.num_chunks = 1;
		result.chunks = buildstate->chunks;
		return result;
	}

	/* Merge multiple sorted chunks into a new MerkleEntryArray */
	result.num_chunks = (int) ((total_entries + MERKLE_ENTRY_CHUNK_SIZE - 1) / MERKLE_ENTRY_CHUNK_SIZE);
	result.chunks = (MerkleTupleHashEntry **) MemoryContextAlloc(
		TopTransactionContext, (size_t) result.num_chunks * sizeof(MerkleTupleHashEntry *));

	for (c = 0; c < result.num_chunks; c++)
	{
		size_t cap = (c == result.num_chunks - 1) ?
			(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
			(size_t) MERKLE_ENTRY_CHUNK_SIZE;
		result.chunks[c] = (MerkleTupleHashEntry *) MemoryContextAlloc(
			TopTransactionContext, cap * sizeof(MerkleTupleHashEntry));
	}

	{
		KWayHeapNode *heap = (KWayHeapNode *) palloc(num_chunks * sizeof(KWayHeapNode));
		int heap_size = 0;
		size_t out_idx;
		int i;

		for (c = 0; c < num_chunks; c++)
		{
			size_t len = (c == num_chunks - 1) ?
				(total_entries - (size_t) c * MERKLE_ENTRY_CHUNK_SIZE) :
				(size_t) MERKLE_ENTRY_CHUNK_SIZE;
			if (len > 0)
			{
				heap[heap_size].chunk_idx = c;
				heap[heap_size].elem_idx = 0;
				heap[heap_size].chunk_len = len;
				heap[heap_size].entry = &buildstate->chunks[c][0];
				heap_size++;
			}
		}

		for (i = (heap_size - 2) / 2; i >= 0; i--)
			kway_heap_sift_down(heap, heap_size, i);

		for (out_idx = 0; out_idx < total_entries; out_idx++)
		{
			size_t out_c = out_idx / MERKLE_ENTRY_CHUNK_SIZE;
			size_t out_o = out_idx % MERKLE_ENTRY_CHUNK_SIZE;

			result.chunks[out_c][out_o] = *heap[0].entry;

			heap[0].elem_idx++;
			if (heap[0].elem_idx < heap[0].chunk_len)
			{
				heap[0].entry = &buildstate->chunks[heap[0].chunk_idx][heap[0].elem_idx];
			}
			else
			{
				heap[0] = heap[heap_size - 1];
				heap_size--;
			}
			if (heap_size > 0)
				kway_heap_sift_down(heap, heap_size, 0);
		}

		pfree(heap);

		/* Free original unmerged chunks */
		for (c = 0; c < num_chunks; c++)
			pfree(buildstate->chunks[c]);
		pfree(buildstate->chunks);
	}

	return result;
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
	size_t			chunk_idx;
	size_t			chunk_off;
	MerkleTupleHashEntry *entry;

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
    
	/* Dynamic indexing tuple tracking via chunked allocations */
	chunk_idx = buildstate->num_entries / MERKLE_ENTRY_CHUNK_SIZE;
	chunk_off = buildstate->num_entries % MERKLE_ENTRY_CHUNK_SIZE;

	if (chunk_idx >= (size_t) buildstate->num_chunks)
	{
		if (buildstate->num_chunks >= buildstate->max_chunks)
		{
			int new_max = buildstate->max_chunks ? buildstate->max_chunks * 2 : 16;

			/* repalloc() cannot grow a NULL pointer on the first chunk. */
			if (buildstate->chunks == NULL)
				buildstate->chunks = (MerkleTupleHashEntry **)
					palloc((size_t) new_max * sizeof(MerkleTupleHashEntry *));
			else
				buildstate->chunks = (MerkleTupleHashEntry **)
					repalloc(buildstate->chunks,
							(size_t) new_max * sizeof(MerkleTupleHashEntry *));
			buildstate->max_chunks = new_max;
		}
		buildstate->chunks[buildstate->num_chunks] = (MerkleTupleHashEntry *)
			MemoryContextAlloc(TopTransactionContext,
							   (size_t) MERKLE_ENTRY_CHUNK_SIZE * sizeof(MerkleTupleHashEntry));
		buildstate->num_chunks++;
	}

	entry = &buildstate->chunks[chunk_idx][chunk_off];
	memcpy(entry->key_hash, route.route_digest, 8);
	entry->partition_id = (uint32) route.partition_id;
	memcpy(&entry->tuple_hash, &hash, sizeof(MerkleHash));
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
	MerkleRecoveryStatusData recovery_status;
	MerkleEntryArray	ea;

	buildstate.heapFetch = NULL;
	buildstate.heapSlot = NULL;
	buildstate.chunks = NULL;
	buildstate.num_chunks = 0;
	buildstate.max_chunks = 0;
	buildstate.num_entries = 0;
	MemSet(&ea, 0, sizeof(ea));
	MemSet(&recovery_status, 0, sizeof(recovery_status));

    PG_TRY();
    {
	if (heapRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes require a permanent logged table"),
				 errhint("Use a logged table; TEMP and UNLOGGED Merkle indexes are not supported.")));

	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot build or reindex a Merkle index after table changes in the same transaction"),
				 errhint("Commit the table changes and synchronize Merkle recovery before rebuilding the index.")));
	merkle_get_recovery_status(&recovery_status);
	if (recovery_status.state != MERKLE_STATE_READY)
	{
		if ((recovery_status.state == MERKLE_STATE_INVALID ||
			 recovery_status.state == MERKLE_STATE_REBUILD_REQUIRED) &&
			recovery_status.applied_seq == recovery_status.target_seq)
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
    opts = merkle_get_options(indexRel);
    
    {
        List       *indexList;
        ListCell   *lc;
        Oid         currentIndexOid = RelationGetRelid(indexRel);

        indexList = RelationGetIndexList(heapRel);
        foreach(lc, indexList)
        {
            Oid         indexOid = lfirst_oid(lc);
            Relation    otherIndexRel;

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
    
    buildstate.fanout = opts->fanout;
	buildstate.split_threshold = opts->split_threshold;
	buildstate.merge_threshold = opts->merge_threshold;
	buildstate.num_partitions = opts->num_partitions;
	buildstate.bits_per_split = merkle_bits_per_split_for_fanout(buildstate.fanout);

	merkle_init_tree(indexRel, RelationGetRelid(heapRel), opts,
					 recovery_status.managed ? recovery_status.applied_seq : 0);

    pfree(opts);
    
    buildstate.indexRel = indexRel;
    buildstate.heapRel = heapRel;
    buildstate.indtuples = 0;
    buildstate.nkeys = indexInfo->ii_NumIndexKeyAttrs;
	buildstate.heapFetch = table_index_fetch_begin(heapRel);
	buildstate.heapSlot = table_slot_create(heapRel, NULL);
    
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

	if (SPI_connect() == SPI_OK_CONNECT)
	{
		Oid index_oid = RelationGetRelid(indexRel);
		Oid clear_types[1] = {OIDOID};
		Datum clear_values[1] = {ObjectIdGetDatum(index_oid)};
		ea = merkle_prepare_sorted_entries(&buildstate);
		/* REINDEX must replace, rather than overlay, the previous dynamic
		 * geometry.  This also removes pre-partition-format rows left by an
		 * upgraded cluster before the first partitioned rebuild. */
		SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_node WHERE index_oid = $1",
			1, clear_types, clear_values, NULL, false, 0);
		/* Each partition owns an independent dynamic tree rooted at
		 * (partition_id, node_id=0, prefix_len=0).  Entries are sorted by
		 * partition_id, so each partition is a contiguous slice. */
		for (int partition_id = 0;
			 partition_id < buildstate.num_partitions;
			 partition_id++)
		{
			size_t start = 0;
			size_t count = 0;
			uint8 zero_node_id[8] = {0};
			MerkleHash partition_hash;

			while (start < ea.num_entries &&
				   merkle_entry_at(&ea, start)->partition_id < (uint32) partition_id)
				start++;
			while (start + count < ea.num_entries &&
				   merkle_entry_at(&ea, start + count)->partition_id == (uint32) partition_id)
				count++;

			merkle_hash_zero(&partition_hash);
			for (size_t k = 0; k < count; k++)
				merkle_hash_xor(&partition_hash,
								&merkle_entry_at(&ea, start + k)->tuple_hash);

			if (count <= (size_t) buildstate.split_threshold)
			{
				MerkleBulkNodeSet bulk;
				bulk_node_set_init(&bulk, 1);
				bulk_node_set_add(&bulk, partition_id, zero_node_id, 0, true,
								  (int64) count, &partition_hash);
				merkle_bulk_flush_nodes(index_oid, &bulk);
				pfree(bulk.records);
			}
			else
			{
				MerkleBulkNodeSet bulk;
				MerkleHash computed_root_hash;
				int est_nodes = Max(1024, (int) ((count / buildstate.split_threshold) * 3));

				bulk_node_set_init(&bulk, est_nodes);
				PG_TRY();
				{
					computed_root_hash = merkle_build_tree_pass1(
						&bulk, partition_id, zero_node_id, 0,
						&ea, start, count,
						buildstate.fanout, buildstate.bits_per_split,
						buildstate.split_threshold, MAX_PREFIX_LEN);
					bulk_node_set_add(&bulk, partition_id, zero_node_id, 0, false,
									  (int64) count, &computed_root_hash);
					merkle_bulk_flush_nodes(index_oid, &bulk);
				}
				PG_CATCH();
				{
					if (bulk.records)
					{
						pfree(bulk.records);
						bulk.records = NULL;
					}
					PG_RE_THROW();
				}
				PG_END_TRY();
				if (bulk.records)
					pfree(bulk.records);
			}
		}

		if (SPI_tuptable != NULL)
			SPI_freetuptable(SPI_tuptable);
		SPI_finish();
	}

	if (ea.chunks)
	{
		for (int c = 0; c < ea.num_chunks; c++)
			pfree(ea.chunks[c]);
		pfree(ea.chunks);
		ea.chunks = NULL;
	}

    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    }
    PG_CATCH();
    {
		if (ea.chunks)
		{
			for (int c = 0; c < ea.num_chunks; c++)
				pfree(ea.chunks[c]);
			pfree(ea.chunks);
			ea.chunks = NULL;
		}
		if (buildstate.chunks)
		{
			for (int c = 0; c < buildstate.num_chunks; c++)
				pfree(buildstate.chunks[c]);
			pfree(buildstate.chunks);
			buildstate.chunks = NULL;
		}
		if (buildstate.heapFetch != NULL)
			table_index_fetch_end(buildstate.heapFetch);
		if (buildstate.heapSlot != NULL)
			ExecDropSingleTupleTableSlot(buildstate.heapSlot);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result;
}

void
merkleBuildempty(Relation indexRel)
{
    Page        metapage;
    MerkleMetaPageData *meta;
    MerkleOptions *opts;
    int         fanout;
	MerkleRecoveryStatusData recovery_status;

	if (indexRel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("crash-safe Merkle indexes cannot be created for TEMP or UNLOGGED relations"),
				 errhint("Use a permanent logged table.")));

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

    metapage = (Page) palloc(BLCKSZ);
    PageInit(metapage, BLCKSZ, 0);
    
    opts = merkle_get_options(indexRel);
    fanout = opts->fanout;

    if (fanout < 2 || fanout > 1024)
        fanout = MERKLE_DEFAULT_FANOUT;

    meta = MerklePageGetMeta(metapage);
    meta->version = MERKLE_VERSION;
    meta->heapRelid = InvalidOid;
    meta->fanout = fanout;
    meta->split_threshold = opts->split_threshold;
    meta->merge_threshold = opts->merge_threshold;
	meta->num_partitions = opts->num_partitions;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = recovery_status.managed ?
		recovery_status.applied_seq : 0;

    pfree(opts);
    
    RelationOpenSmgr(indexRel);

    PageSetChecksumInplace(metapage, MERKLE_METAPAGE_BLKNO);
    smgrwrite(indexRel->rd_smgr, INIT_FORKNUM, MERKLE_METAPAGE_BLKNO,
              (char *) metapage, true);
    log_newpage(&indexRel->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
                MERKLE_METAPAGE_BLKNO, metapage, true);
    
    smgrimmedsync(indexRel->rd_smgr, INIT_FORKNUM);
    
    pfree(metapage);
}
