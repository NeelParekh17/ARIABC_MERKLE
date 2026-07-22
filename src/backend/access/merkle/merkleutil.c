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
#include "catalog/pg_am_d.h"
#include "common/blake3.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "access/xact.h"
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

static void
merkle_append_uint32(StringInfo buffer, uint32 value)
{
	uint8 bytes[4];

	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	appendBinaryStringInfo(buffer, (const char *) bytes, sizeof(bytes));
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
bool
merkle_compute_row_hash(Relation heapRel, ItemPointer tid, MerkleHash *result)
{
	TupleDesc		tupdesc;
	TupleTableSlot *slot = NULL;
	bool			success = false;
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
	 * and will cause fetch failures.  The boolean result distinguishes this
	 * failure from a successfully computed, legitimate all-zero digest.
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
			merkle_hash_zero(result);
		else
		{
			merkle_hash_slot_canonical(heapRel, slot, result);
			success = true;
		}
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
	return success;
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
 *   dynamic bits = full 256-bit digest consumed one bit at a time;
 *                  public logical ranges may group five bits (fanout 32)
 *
 * INTEGER KEYS: earlier versions used abs(key) % total_leaves directly, which
 * was incompatible with a future dynamic prefix tree (sequential keys share
 * high bits).  Route format version 2 removes that special path.
 */
static bytea *
merkle_serialize_canonical_key(Datum *values, bool *isnull, int nkeys,
							   TupleDesc tupdesc)
{
	StringInfoData buffer;
	bytea	   *result;
	int			i;
	static const uint8 magic[] = {'A', 'R', 'I', 'A', 'R', 'O', 'U', 'T'};

	if (nkeys <= 0 || nkeys > tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid Merkle key count %d", nkeys)));

	initStringInfo(&buffer);
	appendBinaryStringInfo(&buffer, (const char *) magic, sizeof(magic));
	merkle_append_uint32(&buffer, MERKLE_ROUTE_FORMAT_VERSION);
	merkle_append_uint32(&buffer, (uint32) nkeys);

	for (i = 0; i < nkeys; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		uint8		null_flag = isnull[i] ? 1 : 0;

		merkle_append_uint32(&buffer, (uint32) (i + 1));
		merkle_append_uint32(&buffer, (uint32) attr->atttypid);
		merkle_append_uint32(&buffer, (uint32) attr->atttypmod);
		appendBinaryStringInfo(&buffer, (const char *) &null_flag,
							   sizeof(null_flag));

		if (!isnull[i])
		{
			Oid			typsend;
			bool		typisvarlena;
			bytea	   *encoded;
			uint32		length;

			getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
			encoded = OidSendFunctionCall(typsend, values[i]);
			length = (uint32) VARSIZE_ANY_EXHDR(encoded);
			merkle_append_uint32(&buffer, length);
			if (length > 0)
				appendBinaryStringInfo(&buffer, VARDATA_ANY(encoded), length);
			pfree(encoded);
		}
	}

	result = (bytea *) palloc(VARHDRSZ + buffer.len);
	SET_VARSIZE(result, VARHDRSZ + buffer.len);
	memcpy(VARDATA(result), buffer.data, buffer.len);
	pfree(buffer.data);
	return result;
}

void
merkle_digest_canonical_key_data(const uint8 *key_data, Size key_length,
								 uint8 digest[MERKLE_HASH_BYTES])
{
	blake3_hasher hasher;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, key_data, key_length);
	blake3_hasher_finalize(&hasher, digest, MERKLE_HASH_BYTES);
}

static void
merkle_digest_canonical_key(const bytea *key_data,
							uint8 digest[MERKLE_HASH_BYTES])
{
	merkle_digest_canonical_key_data((const uint8 *) VARDATA_ANY(key_data),
									VARSIZE_ANY_EXHDR(key_data), digest);
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
	bytea		   *key_data;
	uint64			static_route_value = 0;
	int				i;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle route output cannot be null")));

	merkle_geometry_from_index(indexRel, &geometry);
	tupdesc = RelationGetDescr(indexRel);

	/* Uniform BLAKE3 routing for all key types including integers. */
	key_data = merkle_serialize_canonical_key(values, isnull, nkeys, tupdesc);
	merkle_digest_canonical_key(key_data, result->route_digest);
	pfree(key_data);

	for (i = 0; i < 8; i++)
		static_route_value = (static_route_value << 8) | result->route_digest[i];

	result->static_route_value = static_route_value;
	result->leaf_id = (int) (static_route_value % (uint64) geometry.total_leaves);
	result->partition_id = result->leaf_id / geometry.leaves_per_partition;
	result->node_in_partition = merkle_geometry_leaf_node(&geometry,
													 result->leaf_id);
}

void
merkle_compute_dynamic_item_identity(Relation indexRel, Datum *values,
									 bool *isnull, int nkeys,
									 int partitions, int max_key_bytes,
									 MerkleItemIdentity *result)
{
	TupleDesc tupdesc;
	uint64 route_value = 0;
	int i;

	if (result == NULL || indexRel == NULL || partitions <= 0 ||
		max_key_bytes <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("invalid dynamic Merkle item identity arguments")));
	MemSet(result, 0, sizeof(*result));
	tupdesc = RelationGetDescr(indexRel);
	result->key_data = merkle_serialize_canonical_key(values, isnull, nkeys,
												tupdesc);
	if (VARSIZE_ANY_EXHDR(result->key_data) > (Size) max_key_bytes)
	{
		Size key_bytes = VARSIZE_ANY_EXHDR(result->key_data);

		pfree(result->key_data);
		result->key_data = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("canonical dynamic Merkle key is too large"),
				 errdetail("Key is %zu bytes; index maximum is %d bytes.",
						(size_t) key_bytes, max_key_bytes)));
	}
	merkle_digest_canonical_key(result->key_data,
								result->route.route_digest);
	for (i = 0; i < 8; i++)
		route_value = (route_value << 8) | result->route.route_digest[i];
	result->route.static_route_value = route_value;
	result->route.partition_id = (int) (route_value %
		(uint64) partitions);
	result->route.leaf_id = result->route.partition_id;
	result->route.node_in_partition = 1;
}

void
merkle_compute_item_identity(Relation indexRel, Datum *values, bool *isnull,
							 int nkeys, MerkleItemIdentity *result)
{
	MerkleOptions *opts;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Merkle item identity output cannot be null")));
	MemSet(result, 0, sizeof(*result));
	if (!merkle_index_is_dynamic(indexRel))
	{
		merkle_compute_route(indexRel, values, isnull, nkeys, &result->route);
		return;
	}
	opts = merkle_get_options(indexRel);
	merkle_compute_dynamic_item_identity(indexRel, values, isnull, nkeys,
		opts->partitions, opts->max_key_bytes, result);
	pfree(opts);
}

bool
merkle_index_is_dynamic(Relation indexRel)
{
	MerkleOptions *opts;
	bool option_dynamic;
	BlockNumber blocks;

	if (indexRel == NULL || indexRel->rd_rel->relam != MERKLE_AM_OID)
		return false;
	opts = merkle_get_options(indexRel);
	option_dynamic = opts->dynamic;
	pfree(opts);
	blocks = RelationGetNumberOfBlocks(indexRel);
	if (blocks > MERKLE_METAPAGE_BLKNO)
	{
		Buffer buffer = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
		Page page;
		MerkleMetaPageData *meta;
		bool marker_dynamic;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		meta = MerklePageGetMeta(page);
		marker_dynamic = meta->dynamicMagic == MERKLE_DYNAMIC_META_MAGIC;
		UnlockReleaseBuffer(buffer);
		if (marker_dynamic != option_dynamic)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("Merkle index dynamic reloption and layout marker disagree"),
					 errhint("REINDEX the Merkle index.")));
		return marker_dynamic;
	}
	return option_dynamic;
}

int
merkle_get_update_mode(Relation indexRel)
{
	BlockNumber blocks;

	/* Non-dynamic indexes only support pending_log */
	if (!merkle_index_is_dynamic(indexRel))
		return MERKLE_UPDATE_PENDING_LOG;

	blocks = RelationGetNumberOfBlocks(indexRel);
	if (blocks > MERKLE_METAPAGE_BLKNO)
	{
		Buffer buf = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
		Page page;
		MerkleMetaPageData *meta;
		int mode = MERKLE_UPDATE_PENDING_LOG;
		uint32 flags;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		meta = MerklePageGetMeta(page);

		if (meta->version >= 7)
		{
			flags = meta->nativeFormatFlags & MERKLE_NATIVE_MODE_MASK;
			if (flags == MERKLE_NATIVE_MODE_SYNCHRONOUS_COW)
				mode = MERKLE_UPDATE_SYNCHRONOUS_COW;
			else if (flags == MERKLE_NATIVE_MODE_PENDING_LOG)
				mode = MERKLE_UPDATE_PENDING_LOG;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("Merkle index has unknown native update-mode flags 0x%08x",
								flags),
						 errhint("REINDEX the dynamic Merkle index.")));
		}
		UnlockReleaseBuffer(buf);
		return mode;
	}
	else
	{
		/* Metapage not written yet (e.g. index build).  The reloption is the
		 * durable authority; never consult the session GUC here because that
		 * would let REINDEX silently change an existing index's mode. */
		MerkleOptions *opts = merkle_get_options(indexRel);
		int mode = opts ? opts->update_mode : MERKLE_UPDATE_SYNCHRONOUS_COW;
		if (opts)
			pfree(opts);
		return mode;
	}
}

int
merkle_get_update_mode_by_oid(Oid index_oid)
{
	Relation indexRel = index_open(index_oid, AccessShareLock);
	int mode = merkle_get_update_mode(indexRel);
	index_close(indexRel, AccessShareLock);
	return mode;
}

/*
 * merkle_update_tree_path() - Stage one committed-delta leaf change.
 *
 * User transactions never mutate Merkle pages.  The transaction-local delta
 * map is serialized atomically with the heap/ledger change, and the ordered
 * applier is the only normal-runtime page mutator.
 */
void
merkle_update_tree_path(Relation indexRel, int leafId, MerkleHash *hash, bool isXorIn)
{
	MerkleGeometry geometry;
	bool			profile_enabled = merkle_recovery_profile_enabled;
	instr_time	start_time;
	instr_time	elapsed_time;
	uint64		nodes_touched = 0;
	int			node_in_partition;

	(void) isXorIn;

	if (profile_enabled)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		merkle_recovery_profile_state.tree_path_update_calls++;
	}
	if (indexRel == NULL || hash == NULL || merkle_hash_is_zero(hash))
		return;

	/* Route computation already loaded this geometry, so this is a cache hit in
	 * the normal insert path.  Keep the validation here for direct AM callers. */
	merkle_geometry_from_index(indexRel, &geometry);
	if (leafId < 0 || leafId >= geometry.total_leaves)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("merkle_update_tree_path: leafId %d out of range [0,%d)",
						leafId, geometry.total_leaves)));

	/* Preserve the profiler's logical path metric without touching buffers.
	 * Keep this traversal out of the normal synchronous DML path when profiling
	 * is disabled; the staged delta itself is the only required work here. */
	if (profile_enabled)
	{
		node_in_partition = merkle_geometry_leaf_node(&geometry, leafId);
		while (node_in_partition > 0)
		{
			nodes_touched++;
			node_in_partition = merkle_geometry_parent_node(&geometry,
											 node_in_partition);
		}
	}

	merkle_stage_delta(indexRel, leafId, hash);

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
merkle_init_tree(Relation indexRel, Oid heapOid, MerkleOptions *opts,
				 uint64 baseline_apply_seq)
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
		/* Dynamic indexes reserve this region for the native partition
		 * directory.  Immutable nodes and XID-visible roots appended after it
		 * are the authoritative tree; side relations are compatibility-only. */
		leavesPerPartition = opts->dynamic ? 1 : opts->leaves_per_partition;
		fanout = opts->dynamic ? MERKLE_DYNAMIC_LOGICAL_FANOUT : opts->fanout;
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
	if (opts != NULL && opts->dynamic)
	{
		/* Native layout v4 uses one directory page per partition. */
		numTreePages = numPartitions;
	}
	else
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
	meta->baselineApplySeq = baseline_apply_seq;
	if (opts != NULL && opts->dynamic)
	{
		meta->dynamicMagic = MERKLE_DYNAMIC_META_MAGIC;
		meta->dynamicLayoutVersion = MERKLE_DYNAMIC_LAYOUT_VERSION;
		meta->dynamicFlags = 1;
		meta->dynamicLogicalFanout = MERKLE_DYNAMIC_LOGICAL_FANOUT;
		meta->dynamicLeafCapacity = opts->leaf_capacity;
		meta->dynamicMergeThreshold = opts->merge_threshold;
		meta->dynamicLeafByteCapacity = opts->leaf_byte_capacity;
		meta->dynamicMaxKeyBytes = opts->max_key_bytes;
		meta->nativeDirectoryStart = MERKLE_TREE_START_BLKNO;
		meta->nativeDirectoryPages = numTreePages;
		if (opts->update_mode == MERKLE_UPDATE_SYNCHRONOUS_COW)
			meta->nativeFormatFlags = MERKLE_NATIVE_MODE_SYNCHRONOUS_COW;
		else
			meta->nativeFormatFlags = MERKLE_NATIVE_MODE_PENDING_LOG;
	}
	/*
	 * Merkle metadata lives directly in the page content area rather than in
	 * line pointers.  Tell standard-page WAL where that initialized content
	 * ends; otherwise a full-page image is allowed to omit it as the page
	 * "hole", leaving a zero metapage after crash recovery.
	 */
	((PageHeader) metapage)->pd_lower =
		(LocationIndex) ((char *) meta + sizeof(*meta) - (char *) metapage);
    
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
		PageInit(treepage, BLCKSZ, MERKLE_PAGE_SPECIAL_SIZE);
		{
			MerklePageOpaqueData *opaque = MerklePageGetOpaque(treepage);

			opaque->magic = MERKLE_PAGE_OPAQUE_MAGIC;
			opaque->version = MERKLE_PAGE_OPAQUE_VERSION;
			opaque->flags = 0;
			opaque->last_applied_seq = baseline_apply_seq;
		}
        
        /* Zero the entire page content area */
        nodes = (MerkleNode *) PageGetContents(treepage);
		memset(nodes, 0, (char *) PageGetSpecialPointer(treepage) -
						 (char *) nodes);
		/* Generic WAL compares only the page's used lower/upper regions. */
		((PageHeader) treepage)->pd_lower =
			(LocationIndex) ((char *) nodes +
							 nodesPerPage * sizeof(MerkleNode) -
							 (char *) treepage);
        
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

	if (opts != NULL && opts->dynamic)
		merkle_native_init(indexRel, numPartitions, baseline_apply_seq);

}

/* End of file */
