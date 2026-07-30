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
#include "utils/typcache.h"
#include "parser/parse_coerce.h"
#include "funcapi.h"
#include "access/xact.h"
#include "portability/instr_time.h"



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
    int  fanout;
    int  split_threshold;
    int  merge_threshold;
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
merkle_meta_cache_store(Oid relid, int fanout, int split_threshold, int merge_threshold)
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
    merkle_meta_cache[target].relid           = relid;
    merkle_meta_cache[target].fanout          = fanout;
    merkle_meta_cache[target].split_threshold = split_threshold;
    merkle_meta_cache[target].merge_threshold = merge_threshold;
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
void
merkle_hash_slot_canonical_desc(TupleDesc tupdesc, TupleTableSlot *slot,
								MerkleHash *result)
{
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

static void
merkle_hash_slot_canonical(Relation heapRel, TupleTableSlot *slot,
						   MerkleHash *result)
{
	TupleDesc tupdesc = RelationGetDescr(heapRel);
	merkle_hash_slot_canonical_desc(tupdesc, slot, result);
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
 * merkle_compute_route() - Single relation-aware routing entry point.
 *
 * All key types use uniform BLAKE3-256 routing (route format version 4).
 */
void
merkle_compute_route(Relation indexRel, Datum *values, bool *isnull, int nkeys,
					 MerkleRoute *result)
{
	TupleDesc		tupdesc;
	uint64			static_route_value = 0;
	int				i;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("merkle route output cannot be null")));

	tupdesc = RelationGetDescr(indexRel);

	/* Uniform BLAKE3 routing for all key types including integers. */
	merkle_compute_canonical_route_digest(values, isnull, nkeys, tupdesc, result->route_digest);

	for (i = 0; i < 8; i++)
		static_route_value = (static_route_value << 8) | result->route_digest[i];

	result->static_route_value = static_route_value;
}

/*
 * merkle_read_meta() - Read tree configuration from index metadata
 */
void
merkle_read_meta(Relation indexRel, int *fanout,
				 int *split_threshold, int *merge_threshold)
{
	Buffer              buf;
	Page                page;
	MerkleMetaPageData *meta;
	Oid                 cache_relid = RelationGetRelid(indexRel);
	/* Always read fresh metapage from relation buffer pool for 100% accuracy */

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

	if (fanout)          *fanout          = meta->fanout;
	if (split_threshold) *split_threshold = meta->split_threshold;
	if (merge_threshold) *merge_threshold = meta->merge_threshold;

	merkle_meta_cache_store(cache_relid, meta->fanout, meta->split_threshold, meta->merge_threshold);

	UnlockReleaseBuffer(buf);
}

/*
 * merkle_init_tree() - Initialize Merkle tree metadata page
 */
void
merkle_init_tree(Relation indexRel, Oid heapOid, MerkleOptions *opts,
				 uint64 baseline_apply_seq)
{
	Buffer          metabuf;
	Page            metapage;
	MerkleMetaPageData *meta;
	int             fanout;
	int             split_threshold;
	int             merge_threshold;

	if (opts != NULL)
	{
		fanout = opts->fanout;
		split_threshold = opts->split_threshold;
		merge_threshold = opts->merge_threshold;
	}
	else
	{
		fanout = MERKLE_DEFAULT_FANOUT;
		split_threshold = SPLIT_THRESHOLD;
		merge_threshold = MERKLE_MERGE_THRESHOLD;
	}

	metabuf = ReadBuffer(indexRel, P_NEW);
	Assert(BufferGetBlockNumber(metabuf) == MERKLE_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	PageInit(metapage, BLCKSZ, 0);

	meta = MerklePageGetMeta(metapage);
	meta->version = MERKLE_VERSION;
	meta->heapRelid = heapOid;
	meta->fanout = fanout;
	meta->split_threshold = split_threshold;
	meta->merge_threshold = merge_threshold;
	meta->routeFormatVersion = MERKLE_ROUTE_FORMAT_VERSION;
	meta->rowHashFormatVersion = MERKLE_ROW_HASH_FORMAT_VERSION;
	meta->baselineApplySeq = baseline_apply_seq;

	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);
}

PG_FUNCTION_INFO_V1(merkle_key_hash_sql);

Datum
merkle_key_hash_sql(PG_FUNCTION_ARGS)
{
	Datum		val;
	Oid			argtype;
	uint8		digest[MERKLE_HASH_BYTES];
	bytea	   *result;
	TupleDesc   tupdesc;
	bool        isnull;

	if (PG_ARGISNULL(0))
	{
		isnull = true;
		val = (Datum) 0;
	}
	else
	{
		isnull = false;
		val = PG_GETARG_DATUM(0);
	}

	argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(argtype))
		argtype = INT8OID;

	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "key", argtype, -1, 0);

	merkle_compute_canonical_route_digest(&val, &isnull, 1, tupdesc, digest);
	FreeTupleDesc(tupdesc);

	result = (bytea *) palloc(VARHDRSZ + 8);
	SET_VARSIZE(result, VARHDRSZ + 8);
	memcpy(VARDATA(result), digest, 8);

	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(merkle_tuple_hash_sql);

Datum
merkle_tuple_hash_sql(PG_FUNCTION_ARGS)
{
	HeapTupleHeader tuple_header;
	Oid			tup_type;
	int32		tup_typmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	TupleTableSlot *slot;
	MerkleHash	hash;
	bytea	   *result;

	if (PG_ARGISNULL(0))
	{
		result = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
		SET_VARSIZE(result, VARHDRSZ + MERKLE_HASH_BYTES);
		memset(VARDATA(result), 0, MERKLE_HASH_BYTES);
		PG_RETURN_BYTEA_P(result);
	}

	tuple_header = PG_GETARG_HEAPTUPLEHEADER(0);
	tup_type = HeapTupleHeaderGetTypeId(tuple_header);
	tup_typmod = HeapTupleHeaderGetTypMod(tuple_header);
	tupdesc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

	memset(&tuple, 0, sizeof(tuple));
	tuple.t_len = HeapTupleHeaderGetDatumLength(tuple_header);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuple_header;

	slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
	ExecStoreHeapTuple(&tuple, slot, false);

	merkle_hash_slot_canonical_desc(tupdesc, slot, &hash);

	ExecDropSingleTupleTableSlot(slot);
	ReleaseTupleDesc(tupdesc);

	result = (bytea *) palloc(VARHDRSZ + MERKLE_HASH_BYTES);
	SET_VARSIZE(result, VARHDRSZ + MERKLE_HASH_BYTES);
	memcpy(VARDATA(result), hash.data, MERKLE_HASH_BYTES);

	PG_RETURN_BYTEA_P(result);
}

/* End of file */
