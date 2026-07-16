/*-------------------------------------------------------------------------
 *
 * merkledynamic.c
 *    Bounded, prefix-routed dynamic Merkle storage.
 *
 * Static format-v7 indexes continue to use the page tree.  Dynamic indexes
 * opt in through reloptions and retain only a one-node-per-partition page
 * compatibility image; their authoritative state is kept in ordinary,
 * WAL-logged ariabc_internal relations.  Consequently an item transition,
 * every recursive split/merge, its hashes, and its applied marker commit or
 * abort as one PostgreSQL transaction.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/merkle.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type_d.h"
#include "common/blake3.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"

/*
 * CREATE/REINDEX streams into a pre-created UNLOGGED staging relation, sorts one
 * partition at a time, and writes each authoritative item once with its final
 * leaf prefix.  Keep each statement large enough that multi-million-row builds
 * do not pay thousands of SPI/command-counter round trips, but comfortably
 * below the backend stack limit for the fixed Datum vectors used by writers.
 */
#define MERKLE_DYNAMIC_BUILD_BATCH 8192
#define MERKLE_DYNAMIC_ITEM_OVERHEAD 64
#define MERKLE_DYNAMIC_VERIFY_BATCH 4096
#define MERKLE_DYNAMIC_VERIFY_FETCH 2048

typedef struct MerkleDynamicConfig
{
	int partitions;
	int leaf_capacity;
	int merge_threshold;
	int leaf_byte_capacity;
	int max_key_bytes;
	uint64 baseline_seq;
} MerkleDynamicConfig;

typedef struct MerkleDynamicGeneration
{
	Oid index_oid;
	Oid heap_oid;
	RelFileNode rnode;
	MerkleDynamicConfig config;
} MerkleDynamicGeneration;

typedef struct MerkleDynamicBuildBufferedItem
{
	int32 partition_id;
	bytea *key_data;
	uint8 route_digest[MERKLE_HASH_BYTES];
	MerkleHash tuple_hash;
} MerkleDynamicBuildBufferedItem;

struct MerkleDynamicBuildState
{
	MemoryContext context;
	MemoryContext batch_context;
	MerkleDynamicGeneration generation;
	int nkeys;
	int batch_count;
	uint64 item_count;
	uint64 item_bytes;
	MerkleDynamicBuildBufferedItem batch[MERKLE_DYNAMIC_BUILD_BATCH];
};

typedef struct MerkleDynamicLoadedItem
{
	bytea *key_data;
	uint8 route_digest[MERKLE_HASH_BYTES];
	MerkleHash tuple_hash;
	uint64 item_bytes;
	uint16 assigned_prefix_len;
	uint8 assigned_prefix[MERKLE_HASH_BYTES];
} MerkleDynamicLoadedItem;

typedef struct MerkleDynamicBuildNode
{
	int32 partition_id;
	uint16 prefix_len;
	uint8 prefix[MERKLE_HASH_BYTES];
	bool is_leaf;
	uint64 tuple_count;
	uint64 subtree_bytes;
	MerkleHash data_xor;
	MerkleHash structure_hash;
} MerkleDynamicBuildNode;

typedef struct MerkleDynamicNodeVector
{
	MerkleDynamicBuildNode *nodes;
	int count;
	int capacity;
	uint64 leaf_count;
	uint64 max_leaf_items;
	uint16 max_depth;
} MerkleDynamicNodeVector;

typedef struct MerkleDynamicNodeData
{
	bool found;
	bool is_leaf;
	uint64 tuple_count;
	uint64 subtree_bytes;
	MerkleHash data_xor;
	MerkleHash structure_hash;
	uint64 last_seq;
} MerkleDynamicNodeData;

typedef struct MerkleDynamicStructureDelta
{
	int64 node_delta;
	int64 leaf_delta;
	int64 split_delta;
	int64 merge_delta;
	int observed_max_depth;
	int observed_max_leaf_items;
	bool extrema_may_decrease;
} MerkleDynamicStructureDelta;

typedef struct MerkleDynamicRequest
{
	int32 partition_id;
	uint16 prefix_len;
	uint8 prefix[MERKLE_HASH_BYTES];
} MerkleDynamicRequest;

typedef struct MerkleDynamicVerifyNodeKey
{
	int32 partition_id;
	uint16 prefix_len;
	uint16 padding;
	uint8 prefix[MERKLE_HASH_BYTES];
} MerkleDynamicVerifyNodeKey;

typedef struct MerkleDynamicVerifyNode
{
	MerkleDynamicVerifyNodeKey key;
	bool is_leaf;
	bool leaf_checked;
	uint64 tuple_count;
	uint64 subtree_bytes;
	MerkleHash data_xor;
	MerkleHash structure_hash;
	uint64 last_seq;
} MerkleDynamicVerifyNode;

typedef struct MerkleDynamicBatchNode
{
	MerkleDynamicVerifyNodeKey key;
	ItemPointerData tid;
	MerkleHash xor_delta;
	MerkleHash data_xor;
	MerkleHash structure_hash;
	uint64 tuple_count;
	uint64 subtree_bytes;
	bool affected;
	bool found;
	bool is_leaf;
	bool structure_computed;
} MerkleDynamicBatchNode;

static bytea *dynamic_bytea(const uint8 *data, Size len);
static void dynamic_read_meta(Relation indexRel, MerkleDynamicGeneration *gen);
static void dynamic_require_relations(void);
static void dynamic_prefix(const uint8 digest[MERKLE_HASH_BYTES], int bits,
						   uint8 result[MERKLE_HASH_BYTES]);
static bool dynamic_prefix_matches(const uint8 digest[MERKLE_HASH_BYTES],
								   const uint8 prefix[MERKLE_HASH_BYTES],
								   int bits);
static int dynamic_route_bit(const uint8 digest[MERKLE_HASH_BYTES], int bit);
static uint64 dynamic_route_value(const uint8 digest[MERKLE_HASH_BYTES]);
static uint64 dynamic_item_bytes(const bytea *key_data);
static char *dynamic_single_key_text(Relation indexRel, const bytea *key_data);

static bool
dynamic_bytes_are_zero(const uint8 *data, Size length)
{
	Size i;

	for (i = 0; i < length; i++)
		if (data[i] != 0)
			return false;
	return true;
}

/* Make writes issued by one SPI call visible to the next SPI read. */
static void
dynamic_advance_command_counter(void)
{
	CommandCounterIncrement();
	if (ActiveSnapshotSet())
		UpdateActiveSnapshotCommandId();
}

static bytea *
dynamic_bytea(const uint8 *data, Size len)
{
	bytea *result = (bytea *) palloc(VARHDRSZ + len);

	SET_VARSIZE(result, VARHDRSZ + len);
	if (len > 0)
		memcpy(VARDATA(result), data, len);
	return result;
}

static void
dynamic_hash_from_datum(Datum value, MerkleHash *hash, const char *column)
{
	bytea *bytes = DatumGetByteaPP(value);

	if (VARSIZE_ANY_EXHDR(bytes) != MERKLE_HASH_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle %s has invalid length %zu", column,
						(size_t) VARSIZE_ANY_EXHDR(bytes))));
	memcpy(hash->data, VARDATA_ANY(bytes), MERKLE_HASH_BYTES);
}

static uint64
dynamic_route_value(const uint8 digest[MERKLE_HASH_BYTES])
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value = (value << 8) | digest[i];
	return value;
}

static int
dynamic_route_bit(const uint8 digest[MERKLE_HASH_BYTES], int bit)
{
	Assert(bit >= 0 && bit < MERKLE_HASH_BITS);
	return (digest[bit / 8] >> (7 - (bit % 8))) & 1;
}

static void
dynamic_prefix(const uint8 digest[MERKLE_HASH_BYTES], int bits,
			   uint8 result[MERKLE_HASH_BYTES])
{
	uint8 source_copy[MERKLE_HASH_BYTES];
	const uint8 *source = digest;
	int full_bytes;
	int remaining;

	if (bits < 0 || bits > MERKLE_HASH_BITS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dynamic Merkle prefix length %d is out of range", bits)));
	if (digest == result)
	{
		memcpy(source_copy, digest, MERKLE_HASH_BYTES);
		source = source_copy;
	}
	MemSet(result, 0, MERKLE_HASH_BYTES);
	full_bytes = bits / 8;
	remaining = bits % 8;
	if (full_bytes > 0)
		memcpy(result, source, full_bytes);
	if (remaining > 0)
		result[full_bytes] = source[full_bytes] & (uint8) (0xff << (8 - remaining));
}

static bool
dynamic_prefix_matches(const uint8 digest[MERKLE_HASH_BYTES],
					   const uint8 prefix[MERKLE_HASH_BYTES], int bits)
{
	uint8 canonical[MERKLE_HASH_BYTES];

	dynamic_prefix(digest, bits, canonical);
	return memcmp(canonical, prefix, MERKLE_HASH_BYTES) == 0;
}

static uint64
dynamic_item_bytes(const bytea *key_data)
{
	return (uint64) VARSIZE_ANY_EXHDR(key_data) +
		MERKLE_HASH_BYTES + MERKLE_HASH_BYTES + MERKLE_DYNAMIC_ITEM_OVERHEAD;
}

static void
dynamic_require_relations(void)
{
	Oid namespace_oid = get_namespace_oid("ariabc_internal", true);
	static const char *const names[] = {
		"merkle_dynamic_state",
		"merkle_dynamic_node",
		"merkle_dynamic_leaf_item",
		"merkle_dynamic_build_stage",
		"merkle_dynamic_seen"
	};
	int i;

	if (!OidIsValid(namespace_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("ariabc_internal schema is not installed"),
				 errhint("Run scripts/distributed/sql/raft_apply_ledger_schema.sql.")));
	for (i = 0; i < lengthof(names); i++)
		if (!OidIsValid(get_relname_relid(names[i], namespace_oid)))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("required dynamic Merkle relation ariabc_internal.%s is missing",
							names[i]),
					 errhint("Run scripts/distributed/sql/raft_apply_ledger_schema.sql.")));
}

static void
dynamic_read_meta(Relation indexRel, MerkleDynamicGeneration *gen)
{
	Buffer buffer;
	Page page;
	MerkleMetaPageData *meta;
	MerkleOptions *opts;

	if (indexRel == NULL || indexRel->rd_rel->relam != MERKLE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation is not a Merkle index")));
	if (!merkle_index_is_dynamic(indexRel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("Merkle index \"%s\" is not dynamic",
						RelationGetRelationName(indexRel))));

	buffer = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	meta = MerklePageGetMeta(page);
	if (meta->version != MERKLE_VERSION ||
		meta->dynamicMagic != MERKLE_DYNAMIC_META_MAGIC ||
		meta->dynamicLayoutVersion != MERKLE_DYNAMIC_LAYOUT_VERSION ||
		meta->dynamicLogicalFanout != MERKLE_DYNAMIC_LOGICAL_FANOUT ||
		meta->dynamicLeafCapacity == 0 ||
		meta->dynamicMergeThreshold >= meta->dynamicLeafCapacity ||
		meta->dynamicMaxKeyBytes == 0 ||
		meta->dynamicMaxKeyBytes > MERKLE_DYNAMIC_MAX_KEY_BYTES)
	{
		UnlockReleaseBuffer(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("dynamic Merkle index \"%s\" has an invalid layout marker",
						RelationGetRelationName(indexRel)),
				 errhint("REINDEX the dynamic Merkle index.")));
	}

	MemSet(gen, 0, sizeof(*gen));
	gen->index_oid = RelationGetRelid(indexRel);
	gen->heap_oid = meta->heapRelid;
	gen->rnode = indexRel->rd_node;
	gen->config.partitions = meta->numPartitions;
	gen->config.leaf_capacity = (int) meta->dynamicLeafCapacity;
	gen->config.merge_threshold = (int) meta->dynamicMergeThreshold;
	gen->config.leaf_byte_capacity = (int) meta->dynamicLeafByteCapacity;
	gen->config.max_key_bytes = (int) meta->dynamicMaxKeyBytes;
	gen->config.baseline_seq = meta->baselineApplySeq;
	UnlockReleaseBuffer(buffer);

	/* Reloptions and the durable marker must agree; neither silently wins. */
	opts = merkle_get_options(indexRel);
	if (!opts->dynamic || opts->partitions != gen->config.partitions ||
		opts->fanout != MERKLE_DYNAMIC_LOGICAL_FANOUT ||
		opts->leaf_capacity != gen->config.leaf_capacity ||
		opts->merge_threshold != gen->config.merge_threshold ||
		opts->leaf_byte_capacity != gen->config.leaf_byte_capacity ||
		opts->max_key_bytes != gen->config.max_key_bytes)
	{
		pfree(opts);
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("dynamic Merkle reloptions do not match the durable layout marker"),
				 errhint("REINDEX the dynamic Merkle index.")));
	}
	pfree(opts);
}

void
merkle_dynamic_validate_key_index(Relation heapRel, Relation merkleIndexRel,
								  int nkeys)
{
	List *index_list;
	ListCell *cell;
	bool found = false;
	int i;

	if (nkeys <= 0 || nkeys != merkleIndexRel->rd_index->indnkeyatts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("dynamic Merkle index must have at least one plain key column")));
	if (RelationGetIndexPredicate(merkleIndexRel) != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("dynamic Merkle indexes cannot be partial"),
				 errdetail("Dynamic Merkle integrity covers every live heap row.")));
	for (i = 0; i < nkeys; i++)
	{
		AttrNumber attno = merkleIndexRel->rd_index->indkey.values[i];
		Form_pg_attribute attr;

		if (attno <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("dynamic Merkle indexes do not support expression keys")));
		attr = TupleDescAttr(RelationGetDescr(heapRel), attno - 1);
		if (!attr->attnotnull)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("dynamic Merkle key column \"%s\" must be NOT NULL",
							NameStr(attr->attname))));
	}

	index_list = RelationGetIndexList(heapRel);
	foreach(cell, index_list)
	{
		Oid oid = lfirst_oid(cell);
		Relation candidate;
		bool matches = true;

		if (oid == RelationGetRelid(merkleIndexRel))
			continue;
		candidate = index_open(oid, AccessShareLock);
		if (!candidate->rd_index->indisunique ||
			!candidate->rd_index->indimmediate ||
			!candidate->rd_index->indisvalid ||
			!candidate->rd_index->indisready ||
			candidate->rd_index->indnkeyatts != nkeys ||
			RelationGetIndexExpressions(candidate) != NIL ||
			RelationGetIndexPredicate(candidate) != NIL)
			matches = false;
		for (i = 0; matches && i < nkeys; i++)
			if (candidate->rd_index->indkey.values[i] <= 0 ||
				candidate->rd_index->indkey.values[i] !=
				merkleIndexRel->rd_index->indkey.values[i])
				matches = false;
		index_close(candidate, AccessShareLock);
		if (matches)
		{
			found = true;
			break;
		}
	}
	list_free(index_list);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("dynamic Merkle index requires a matching unique key index"),
				 errdetail("The unique index must use the same NOT NULL, non-expression key columns in the same order and must not be partial.")));
}

static ArrayType *
dynamic_construct_array(Datum *values, int count, Oid element_type)
{
	int16 typlen;
	bool typbyval;
	char typalign;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	return construct_array(values, count, element_type, typlen, typbyval,
						   typalign);
}

static void
dynamic_build_flush(MerkleDynamicBuildState *state)
{
	Datum partitions[MERKLE_DYNAMIC_BUILD_BATCH];
	Datum keys[MERKLE_DYNAMIC_BUILD_BATCH];
	Datum routes[MERKLE_DYNAMIC_BUILD_BATCH];
	Datum hashes[MERKLE_DYNAMIC_BUILD_BATCH];
	Oid argtypes[8] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4ARRAYOID,
		BYTEAARRAYOID,BYTEAARRAYOID,BYTEAARRAYOID};
	Datum args[8];
	char nulls[8] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '};
	MemoryContext old_context;
	int i;
	int rc;

	if (state->batch_count == 0)
		return;
	old_context = MemoryContextSwitchTo(state->batch_context);
	for (i = 0; i < state->batch_count; i++)
	{
		partitions[i] = Int32GetDatum(state->batch[i].partition_id);
		keys[i] = PointerGetDatum(state->batch[i].key_data);
		routes[i] = PointerGetDatum(dynamic_bytea(state->batch[i].route_digest,
											 MERKLE_HASH_BYTES));
		hashes[i] = PointerGetDatum(dynamic_bytea(state->batch[i].tuple_hash.data,
											 MERKLE_HASH_BYTES));
	}
	args[0] = ObjectIdGetDatum(state->generation.index_oid);
	args[1] = ObjectIdGetDatum(state->generation.rnode.spcNode);
	args[2] = ObjectIdGetDatum(state->generation.rnode.dbNode);
	args[3] = ObjectIdGetDatum(state->generation.rnode.relNode);
	args[4] = PointerGetDatum(dynamic_construct_array(partitions,
		state->batch_count, INT4OID));
	args[5] = PointerGetDatum(dynamic_construct_array(keys,
		state->batch_count, BYTEAOID));
	args[6] = PointerGetDatum(dynamic_construct_array(routes,
		state->batch_count, BYTEAOID));
	args[7] = PointerGetDatum(dynamic_construct_array(hashes,
		state->batch_count, BYTEAOID));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle build SPI_connect failed");
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_build_stage "
		" (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,key_data,"
		"  route_digest,tuple_hash) "
		"SELECT $1,$2,$3,$4,u.partition_id,u.key_data,u.route_digest,u.tuple_hash "
		"FROM unnest($5::int4[],$6::bytea[],$7::bytea[],$8::bytea[]) "
		"AS u(partition_id,key_data,route_digest,tuple_hash)",
		8,argtypes,args,nulls,false,0);
	if (rc != SPI_OK_INSERT)
		elog(ERROR, "dynamic Merkle build staging insert failed: %d", rc);
	SPI_finish();
	dynamic_advance_command_counter();
	MemoryContextSwitchTo(old_context);
	MemoryContextReset(state->batch_context);
	state->batch_count = 0;
}

static MerkleDynamicBuildState *
dynamic_build_begin_impl(Relation indexRel, Relation heapRel, int nkeys,
						   uint64 baseline_seq)
{
	MerkleDynamicBuildState *state;
	MemoryContext context;
	MemoryContext old_context;
	Oid state_types[12] = {OIDOID,OIDOID,OIDOID,OIDOID,OIDOID,
		INT4OID,INT4OID,INT4OID,INT4OID,INT4OID,INT4OID,INT8OID};
	Datum state_args[12];
	char state_nulls[12];
	Oid node_types[6] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT8OID};
	Datum node_args[6];
	char node_nulls[6] = {' ',' ',' ',' ',' ',' '};
	Oid delete_type = OIDOID;
	Datum delete_arg;
	char delete_null = ' ';
	int rc;

	dynamic_require_relations();
	merkle_dynamic_validate_key_index(heapRel, indexRel, nkeys);
	context = AllocSetContextCreate(CurrentMemoryContext,
		"dynamic Merkle build", ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(context);
	state = palloc0(sizeof(*state));
	state->context = context;
	state->batch_context = AllocSetContextCreate(context,
		"dynamic Merkle build batch", ALLOCSET_DEFAULT_SIZES);
	dynamic_read_meta(indexRel, &state->generation);
	state->generation.heap_oid = RelationGetRelid(heapRel);
	state->generation.config.baseline_seq = baseline_seq;
	state->nkeys = nkeys;
	MemoryContextSwitchTo(old_context);

	MemSet(state_nulls, ' ', sizeof(state_nulls));
	state_args[0] = ObjectIdGetDatum(state->generation.index_oid);
	state_args[1] = ObjectIdGetDatum(state->generation.rnode.spcNode);
	state_args[2] = ObjectIdGetDatum(state->generation.rnode.dbNode);
	state_args[3] = ObjectIdGetDatum(state->generation.rnode.relNode);
	state_args[4] = ObjectIdGetDatum(state->generation.heap_oid);
	state_args[5] = Int32GetDatum(state->generation.config.partitions);
	state_args[6] = Int32GetDatum(MERKLE_DYNAMIC_LOGICAL_FANOUT);
	state_args[7] = Int32GetDatum(state->generation.config.leaf_capacity);
	state_args[8] = Int32GetDatum(state->generation.config.merge_threshold);
	state_args[9] = Int32GetDatum(state->generation.config.leaf_byte_capacity);
	state_args[10] = Int32GetDatum(state->generation.config.max_key_bytes);
	state_args[11] = Int64GetDatum((int64) baseline_seq);
	delete_arg = state_args[0];
	memcpy(node_args, state_args, sizeof(Datum) * 4);
	node_args[4] = state_args[5];
	node_args[5] = state_args[11];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle build-state SPI_connect failed");
	rc = SPI_execute_with_args(
		"DELETE FROM ariabc_internal.merkle_dynamic_state WHERE index_oid=$1",
		1, &delete_type, &delete_arg, &delete_null, false, 0);
	if (rc != SPI_OK_DELETE)
		elog(ERROR, "dynamic Merkle old-generation cleanup failed: %d", rc);
	dynamic_advance_command_counter();
	rc = SPI_execute_with_args(
		"DELETE FROM ariabc_internal.merkle_dynamic_build_stage WHERE index_oid=$1",
		1,&delete_type,&delete_arg,&delete_null,false,0);
	if (rc != SPI_OK_DELETE)
		elog(ERROR, "dynamic Merkle orphaned build staging cleanup failed: %d", rc);
	dynamic_advance_command_counter();
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_state "
		" (index_oid,rnode_spc,rnode_db,rnode_rel,heap_oid,partitions,logical_fanout,"
		"  leaf_capacity,merge_threshold,leaf_byte_capacity,max_key_bytes,applied_seq) "
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
		12, state_types, state_args, state_nulls, false, 0);
	if (rc != SPI_OK_INSERT)
		elog(ERROR, "dynamic Merkle state insert failed: %d", rc);
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_node "
		" (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,prefix_bytes,"
		"  is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,last_seq) "
		"SELECT $1,$2,$3,$4,p,0,decode(repeat('00',32),'hex'),true,0,0,"
		"       decode(repeat('00',32),'hex'),decode(repeat('00',32),'hex'),$6 "
		"  FROM generate_series(0,$5-1) AS p",
		6, node_types, node_args, node_nulls, false, 0);
	if (rc != SPI_OK_INSERT)
		elog(ERROR, "dynamic Merkle build-state initialization failed: %d", rc);
	SPI_finish();
	dynamic_advance_command_counter();
	return state;
}

static void
dynamic_build_add_impl(MerkleDynamicBuildState *state,
						 const MerkleItemIdentity *identity,
						 const MerkleHash *hash)
{
	MerkleDynamicBuildBufferedItem *item;
	MemoryContext old_context;
	Size key_bytes;

	if (state == NULL || identity == NULL || identity->key_data == NULL ||
		hash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("invalid dynamic Merkle build item")));
	key_bytes = VARSIZE_ANY_EXHDR(identity->key_data);
	if (key_bytes > (Size) state->generation.config.max_key_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("canonical dynamic Merkle key is too large"),
				 errdetail("Key is %zu bytes; index maximum is %d bytes.",
						(size_t) key_bytes,
						state->generation.config.max_key_bytes)));
	if (identity->route.partition_id < 0 ||
		identity->route.partition_id >= state->generation.config.partitions ||
		identity->route.partition_id !=
		(int32) (dynamic_route_value(identity->route.route_digest) %
				   (uint64) state->generation.config.partitions))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle build item has inconsistent partition routing")));

	old_context = MemoryContextSwitchTo(state->batch_context);
	item = &state->batch[state->batch_count++];
	item->partition_id = identity->route.partition_id;
	item->key_data = (bytea *) palloc(VARSIZE_ANY(identity->key_data));
	memcpy(item->key_data, identity->key_data, VARSIZE_ANY(identity->key_data));
	memcpy(item->route_digest, identity->route.route_digest,
		   MERKLE_HASH_BYTES);
	item->tuple_hash = *hash;
	MemoryContextSwitchTo(old_context);
	state->item_count++;
	if (dynamic_item_bytes(identity->key_data) >
		(uint64) state->generation.config.leaf_byte_capacity)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("one dynamic Merkle item exceeds leaf_byte_capacity")));
	state->item_bytes += dynamic_item_bytes(identity->key_data);
	if (state->batch_count == MERKLE_DYNAMIC_BUILD_BATCH)
		dynamic_build_flush(state);
}

static void
dynamic_hash_u16(blake3_hasher *hasher, uint16 value)
{
	uint8 bytes[2];

	bytes[0] = (uint8) (value >> 8);
	bytes[1] = (uint8) value;
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

static void
dynamic_hash_u32(blake3_hasher *hasher, uint32 value)
{
	uint8 bytes[4];

	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

static void
dynamic_hash_u64(blake3_hasher *hasher, uint64 value)
{
	uint8 bytes[8];
	int i;

	for (i = 7; i >= 0; i--)
	{
		bytes[i] = (uint8) value;
		value >>= 8;
	}
	blake3_hasher_update(hasher, bytes, sizeof(bytes));
}

static void
dynamic_leaf_structure_hash(int32 partition_id, uint16 prefix_len,
							const uint8 prefix[MERKLE_HASH_BYTES],
							MerkleDynamicLoadedItem *items,
							int lo, int hi, const MerkleHash *data_xor,
							MerkleHash *result)
{
	blake3_hasher hasher;
	static const uint8 domain[] = {'A','R','I','D','Y','N','L','1'};
	int i;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, domain, sizeof(domain));
	dynamic_hash_u32(&hasher, (uint32) partition_id);
	dynamic_hash_u16(&hasher, prefix_len);
	blake3_hasher_update(&hasher, prefix, MERKLE_HASH_BYTES);
	dynamic_hash_u64(&hasher, (uint64) (hi - lo));
	blake3_hasher_update(&hasher, data_xor->data, MERKLE_HASH_BYTES);
	for (i = lo; i < hi; i++)
	{
		Size key_len = VARSIZE_ANY_EXHDR(items[i].key_data);

		dynamic_hash_u32(&hasher, (uint32) key_len);
		blake3_hasher_update(&hasher, VARDATA_ANY(items[i].key_data), key_len);
		blake3_hasher_update(&hasher, items[i].route_digest,
						 MERKLE_HASH_BYTES);
		blake3_hasher_update(&hasher, items[i].tuple_hash.data,
						 MERKLE_HASH_BYTES);
	}
	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

static void
dynamic_internal_structure_hash(int32 partition_id, uint16 prefix_len,
								const uint8 prefix[MERKLE_HASH_BYTES],
								const MerkleDynamicBuildNode *children,
								int child_count, uint64 tuple_count,
								uint64 subtree_bytes,
								const MerkleHash *data_xor,
								MerkleHash *result)
{
	blake3_hasher hasher;
	static const uint8 domain[] = {'A','R','I','D','Y','N','I','1'};
	int i;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, domain, sizeof(domain));
	dynamic_hash_u32(&hasher, (uint32) partition_id);
	dynamic_hash_u16(&hasher, prefix_len);
	blake3_hasher_update(&hasher, prefix, MERKLE_HASH_BYTES);
	dynamic_hash_u64(&hasher, tuple_count);
	dynamic_hash_u64(&hasher, subtree_bytes);
	blake3_hasher_update(&hasher, data_xor->data, MERKLE_HASH_BYTES);
	dynamic_hash_u32(&hasher, (uint32) child_count);
	for (i = 0; i < child_count; i++)
	{
		dynamic_hash_u16(&hasher, children[i].prefix_len);
		blake3_hasher_update(&hasher, children[i].prefix,
						 MERKLE_HASH_BYTES);
		blake3_hasher_update(&hasher, children[i].structure_hash.data,
						 MERKLE_HASH_BYTES);
	}
	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

static void
dynamic_node_vector_append(MerkleDynamicNodeVector *vector,
						   const MerkleDynamicBuildNode *node)
{
	if (vector->count == vector->capacity)
	{
		vector->capacity = vector->capacity == 0 ? 128 : vector->capacity * 2;
		vector->nodes = vector->nodes == NULL ?
			palloc(sizeof(*vector->nodes) * vector->capacity) :
			repalloc(vector->nodes, sizeof(*vector->nodes) * vector->capacity);
	}
	vector->nodes[vector->count++] = *node;
	if (node->is_leaf)
	{
		vector->leaf_count++;
		vector->max_leaf_items = Max(vector->max_leaf_items,
									 node->tuple_count);
	}
	vector->max_depth = Max(vector->max_depth, node->prefix_len);
}

static int
dynamic_route_chunk(const uint8 digest[MERKLE_HASH_BYTES], int start,
					int width)
{
	int result = 0;
	int i;

	for (i = 0; i < width; i++)
		result = (result << 1) | dynamic_route_bit(digest, start + i);
	return result;
}

static MerkleDynamicBuildNode
dynamic_build_subtree(MerkleDynamicBuildState *state,
					  MerkleDynamicLoadedItem *items,
					  uint64 *cumulative_bytes, int32 partition_id,
					  int lo, int hi, uint16 prefix_len,
					  const uint8 prefix[MERKLE_HASH_BYTES],
					  MerkleDynamicNodeVector *vector)
{
	MerkleDynamicBuildNode node;
	uint64 bytes = cumulative_bytes[hi] - cumulative_bytes[lo];
	int count = hi - lo;
	int i;

	MemSet(&node, 0, sizeof(node));
	node.partition_id = partition_id;
	node.prefix_len = prefix_len;
	memcpy(node.prefix, prefix, MERKLE_HASH_BYTES);
	node.tuple_count = count;
	node.subtree_bytes = bytes;

	if (count <= state->generation.config.leaf_capacity &&
		bytes <= (uint64) state->generation.config.leaf_byte_capacity)
	{
		node.is_leaf = true;
		merkle_hash_zero(&node.data_xor);
		for (i = lo; i < hi; i++)
		{
			merkle_hash_xor(&node.data_xor, &items[i].tuple_hash);
			items[i].assigned_prefix_len = prefix_len;
			memcpy(items[i].assigned_prefix, prefix, MERKLE_HASH_BYTES);
		}
		dynamic_leaf_structure_hash(partition_id, prefix_len, prefix,
								items, lo, hi, &node.data_xor,
								&node.structure_hash);
		dynamic_node_vector_append(vector, &node);
		return node;
	}

	if (prefix_len == MERKLE_HASH_BITS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle route-digest collision exceeds leaf capacity"),
				 errdetail("%d distinct canonical keys are indistinguishable after all 256 routing bits.",
						count)));
	else
	{
		int step = 1;
		int pos = lo;
		MerkleDynamicBuildNode children[MERKLE_DYNAMIC_LOGICAL_FANOUT];
		int child_count = 0;

		node.is_leaf = false;
		merkle_hash_zero(&node.data_xor);
		while (pos < hi)
		{
			int ordinal = dynamic_route_chunk(items[pos].route_digest,
										prefix_len, step);
			int end = pos + 1;
			uint8 child_prefix[MERKLE_HASH_BYTES];

			while (end < hi &&
				dynamic_route_chunk(items[end].route_digest,
								prefix_len, step) == ordinal)
				end++;
			dynamic_prefix(items[pos].route_digest, prefix_len + step,
						   child_prefix);
			children[child_count++] = dynamic_build_subtree(state, items,
				cumulative_bytes, partition_id, pos, end, prefix_len + step,
				child_prefix, vector);
			pos = end;
		}
		for (i = 0; i < child_count; i++)
			merkle_hash_xor(&node.data_xor, &children[i].data_xor);
		dynamic_internal_structure_hash(partition_id, prefix_len, prefix,
			children, child_count, node.tuple_count, node.subtree_bytes,
			&node.data_xor, &node.structure_hash);
		dynamic_node_vector_append(vector, &node);
		return node;
	}
}

static void
dynamic_store_partition(MerkleDynamicBuildState *state, int partition_id,
						MerkleDynamicLoadedItem *items, int item_count,
						MerkleDynamicNodeVector *vector)
{
	Oid delete_types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID};
	Datum delete_args[5];
	char delete_nulls[5] = {' ',' ',' ',' ',' '};
	int offset;
	int rc;

	delete_args[0] = ObjectIdGetDatum(state->generation.index_oid);
	delete_args[1] = ObjectIdGetDatum(state->generation.rnode.spcNode);
	delete_args[2] = ObjectIdGetDatum(state->generation.rnode.dbNode);
	delete_args[3] = ObjectIdGetDatum(state->generation.rnode.relNode);
	delete_args[4] = Int32GetDatum(partition_id);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle partition-store SPI_connect failed");
	rc = SPI_execute_with_args(
		"DELETE FROM ariabc_internal.merkle_dynamic_node "
		" WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"   AND partition_id=$5",
		5, delete_types, delete_args, delete_nulls, false, 0);
	if (rc != SPI_OK_DELETE)
		elog(ERROR, "dynamic Merkle partition node reset failed: %d", rc);
	dynamic_advance_command_counter();

	for (offset = 0; offset < vector->count; offset += MERKLE_DYNAMIC_BUILD_BATCH)
	{
		int count = Min(MERKLE_DYNAMIC_BUILD_BATCH, vector->count - offset);
		Datum prefix_lens[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum prefixes[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum leaves[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum tuple_counts[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum subtree_bytes[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum data_xors[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum structures[MERKLE_DYNAMIC_BUILD_BATCH];
		Oid types[13] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,
			INT4ARRAYOID,BYTEAARRAYOID,BOOLARRAYOID,INT8ARRAYOID,
			INT8ARRAYOID,BYTEAARRAYOID,BYTEAARRAYOID,INT8OID};
		Datum args[13];
		char nulls[13] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
		int i;

		for (i = 0; i < count; i++)
		{
			MerkleDynamicBuildNode *node = &vector->nodes[offset + i];

			prefix_lens[i] = Int32GetDatum(node->prefix_len);
			prefixes[i] = PointerGetDatum(dynamic_bytea(node->prefix,
				MERKLE_HASH_BYTES));
			leaves[i] = BoolGetDatum(node->is_leaf);
			tuple_counts[i] = Int64GetDatum((int64) node->tuple_count);
			subtree_bytes[i] = Int64GetDatum((int64) node->subtree_bytes);
			data_xors[i] = PointerGetDatum(dynamic_bytea(node->data_xor.data,
				MERKLE_HASH_BYTES));
			structures[i] = PointerGetDatum(dynamic_bytea(
				node->structure_hash.data, MERKLE_HASH_BYTES));
		}
		memcpy(args, delete_args, sizeof(Datum) * 5);
		args[5] = PointerGetDatum(dynamic_construct_array(prefix_lens,count,INT4OID));
		args[6] = PointerGetDatum(dynamic_construct_array(prefixes,count,BYTEAOID));
		args[7] = PointerGetDatum(dynamic_construct_array(leaves,count,BOOLOID));
		args[8] = PointerGetDatum(dynamic_construct_array(tuple_counts,count,INT8OID));
		args[9] = PointerGetDatum(dynamic_construct_array(subtree_bytes,count,INT8OID));
		args[10] = PointerGetDatum(dynamic_construct_array(data_xors,count,BYTEAOID));
		args[11] = PointerGetDatum(dynamic_construct_array(structures,count,BYTEAOID));
		args[12] = Int64GetDatum((int64) state->generation.config.baseline_seq);
		rc = SPI_execute_with_args(
			"INSERT INTO ariabc_internal.merkle_dynamic_node "
			" (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,prefix_bytes,"
			"  is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,last_seq) "
			"SELECT $1,$2,$3,$4,$5,u.prefix_len,u.prefix_bytes,u.is_leaf,"
			"       u.tuple_count,u.subtree_bytes,u.data_xor,u.structure_hash,$13 "
			"  FROM unnest($6::int4[],$7::bytea[],$8::bool[],$9::int8[],"
			"              $10::int8[],$11::bytea[],$12::bytea[]) "
			"       AS u(prefix_len,prefix_bytes,is_leaf,tuple_count,subtree_bytes,"
			"            data_xor,structure_hash)",
			13, types, args, nulls, false, 0);
		if (rc != SPI_OK_INSERT)
			elog(ERROR, "dynamic Merkle node batch insert failed: %d", rc);
	}
	dynamic_advance_command_counter();

	for (offset = 0; offset < item_count; offset += MERKLE_DYNAMIC_BUILD_BATCH)
	{
		int count = Min(MERKLE_DYNAMIC_BUILD_BATCH, item_count - offset);
		Datum keys[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum prefix_lens[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum prefixes[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum routes[MERKLE_DYNAMIC_BUILD_BATCH];
		Datum hashes[MERKLE_DYNAMIC_BUILD_BATCH];
		Oid types[11] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,
			BYTEAARRAYOID,INT4ARRAYOID,BYTEAARRAYOID,BYTEAARRAYOID,
			BYTEAARRAYOID,INT8OID};
		Datum args[11];
		char nulls[11] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
		int i;

		for (i = 0; i < count; i++)
		{
			keys[i] = PointerGetDatum(items[offset + i].key_data);
			prefix_lens[i] = Int32GetDatum(items[offset + i].assigned_prefix_len);
			prefixes[i] = PointerGetDatum(dynamic_bytea(
				items[offset + i].assigned_prefix, MERKLE_HASH_BYTES));
			routes[i] = PointerGetDatum(dynamic_bytea(
				items[offset + i].route_digest,MERKLE_HASH_BYTES));
			hashes[i] = PointerGetDatum(dynamic_bytea(
				items[offset + i].tuple_hash.data,MERKLE_HASH_BYTES));
		}
		memcpy(args, delete_args, sizeof(Datum) * 4);
		args[4] = Int32GetDatum(partition_id);
		args[5] = PointerGetDatum(dynamic_construct_array(keys,count,BYTEAOID));
		args[6] = PointerGetDatum(dynamic_construct_array(prefix_lens,count,INT4OID));
		args[7] = PointerGetDatum(dynamic_construct_array(prefixes,count,BYTEAOID));
		args[8] = PointerGetDatum(dynamic_construct_array(routes,count,BYTEAOID));
		args[9] = PointerGetDatum(dynamic_construct_array(hashes,count,BYTEAOID));
		args[10] = Int64GetDatum((int64) state->generation.config.baseline_seq);
		rc = SPI_execute_with_args(
			"INSERT INTO ariabc_internal.merkle_dynamic_leaf_item "
			" (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,"
			"  prefix_bytes,key_data,route_digest,tuple_hash,last_seq) "
			"SELECT $1,$2,$3,$4,$5,u.prefix_len::smallint,u.prefix_bytes,"
			"       u.key_data,u.route_digest,u.tuple_hash,$11 "
			"  FROM unnest($6::bytea[],$7::int4[],$8::bytea[],$9::bytea[],"
			"              $10::bytea[]) "
			"       AS u(key_data,prefix_len,prefix_bytes,route_digest,tuple_hash)",
			11, types, args, nulls, false, 0);
		if (rc != SPI_OK_INSERT || SPI_processed != (uint64) count)
			elog(ERROR, "dynamic Merkle final item batch insert failed");
	}
	SPI_finish();
	dynamic_advance_command_counter();
}

static void
dynamic_build_finish_impl(MerkleDynamicBuildState *state)
{
	MemoryContext partition_context;
	uint64 node_count = 0;
	uint64 leaf_count = 0;
	uint64 loaded_item_count = 0;
	uint64 max_leaf_items = 0;
	uint16 max_depth = 0;
	int partition;

	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dynamic Merkle build state is null")));
	dynamic_build_flush(state);
	partition_context = AllocSetContextCreate(state->context,
		"dynamic Merkle partition build", ALLOCSET_DEFAULT_SIZES);

	for (partition = 0; partition < state->generation.config.partitions; partition++)
	{
		Oid types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID};
		Datum args[5];
		char nulls[5] = {' ',' ',' ',' ',' '};
		MerkleDynamicLoadedItem *items;
		uint64 *cumulative_bytes;
		MerkleDynamicNodeVector vector;
		MemoryContext old_context;
		uint8 root_prefix[MERKLE_HASH_BYTES] = {0};
		int item_count;
		int rc;
		int i;

		MemoryContextReset(partition_context);
		old_context = MemoryContextSwitchTo(partition_context);
		args[0] = ObjectIdGetDatum(state->generation.index_oid);
		args[1] = ObjectIdGetDatum(state->generation.rnode.spcNode);
		args[2] = ObjectIdGetDatum(state->generation.rnode.dbNode);
		args[3] = ObjectIdGetDatum(state->generation.rnode.relNode);
		args[4] = Int32GetDatum(partition);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle partition load SPI_connect failed");
		rc = SPI_execute_with_args(
			"SELECT key_data,route_digest,tuple_hash "
			"FROM ariabc_internal.merkle_dynamic_build_stage "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND partition_id=$5 ORDER BY route_digest,key_data",
			5,types,args,nulls,true,0);
		if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
			elog(ERROR, "dynamic Merkle partition load failed");
		item_count = (int) SPI_processed;
		/*
		 * SPI_connect() installs a private temporary memory context.  Anything
		 * allocated with palloc() here would be released by SPI_finish(), but
		 * the subtree builder and partition writer consume these items after
		 * that call.  Allocate every surviving object in partition_context
		 * explicitly; this is especially important once a partition exceeds
		 * leaf_capacity and building the internal frontier allocates enough
		 * memory to reuse the freed SPI chunks.
		 */
		items = MemoryContextAllocZero(partition_context,
			Max(item_count, 1) * sizeof(*items));
		cumulative_bytes = MemoryContextAllocZero(partition_context,
			(item_count + 1) * sizeof(uint64));
		for (i = 0; i < item_count; i++)
		{
			HeapTuple tuple = SPI_tuptable->vals[i];
			TupleDesc desc = SPI_tuptable->tupdesc;
			bool isnull;
			Datum datum;
			bytea *value;
			Size value_size;

			datum = SPI_getbinval(tuple,desc,1,&isnull);
			if (isnull)
				elog(ERROR, "null dynamic Merkle key during build");
			value = DatumGetByteaPP(datum);
			value_size = VARSIZE_ANY(value);
			items[i].key_data = MemoryContextAlloc(partition_context,
				value_size);
			memcpy(items[i].key_data, value, value_size);
			datum = SPI_getbinval(tuple,desc,2,&isnull);
			if (isnull)
				elog(ERROR, "null dynamic Merkle route during build");
			value = DatumGetByteaPP(datum);
			if (VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
				elog(ERROR, "invalid dynamic Merkle route during build");
			memcpy(items[i].route_digest,VARDATA_ANY(value),MERKLE_HASH_BYTES);
			datum = SPI_getbinval(tuple,desc,3,&isnull);
			if (isnull)
				elog(ERROR, "null dynamic Merkle tuple hash during build");
			dynamic_hash_from_datum(datum,
				&items[i].tuple_hash,"tuple_hash");
			items[i].item_bytes = dynamic_item_bytes(items[i].key_data);
			cumulative_bytes[i + 1] = cumulative_bytes[i] + items[i].item_bytes;
		}
		SPI_finish();
		MemSet(&vector, 0, sizeof(vector));
		(void) dynamic_build_subtree(state, items, cumulative_bytes, partition,
			0, item_count, 0, root_prefix, &vector);
		dynamic_store_partition(state, partition, items, item_count, &vector);
		loaded_item_count += item_count;
		node_count += vector.count;
		leaf_count += vector.leaf_count;
		max_leaf_items = Max(max_leaf_items, vector.max_leaf_items);
		max_depth = Max(max_depth, vector.max_depth);
		MemoryContextSwitchTo(old_context);
	}
	if (loaded_item_count != state->item_count)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle build staging rows are incomplete"),
				 errdetail("loaded=%llu expected=%llu",
					(unsigned long long) loaded_item_count,
					(unsigned long long) state->item_count)));

	{
		Oid types[10] = {INT8OID,INT8OID,INT8OID,INT8OID,INT4OID,INT4OID,
			OIDOID,OIDOID,OIDOID,OIDOID};
		Datum args[10];
		char nulls[10] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
		int rc;

		args[0] = Int64GetDatum((int64) state->item_count);
		args[1] = Int64GetDatum((int64) state->item_bytes);
		args[2] = Int64GetDatum((int64) node_count);
		args[3] = Int64GetDatum((int64) leaf_count);
		args[4] = Int32GetDatum(max_depth);
		args[5] = Int32GetDatum((int32) max_leaf_items);
		args[6] = ObjectIdGetDatum(state->generation.index_oid);
		args[7] = ObjectIdGetDatum(state->generation.rnode.spcNode);
		args[8] = ObjectIdGetDatum(state->generation.rnode.dbNode);
		args[9] = ObjectIdGetDatum(state->generation.rnode.relNode);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle final-state SPI_connect failed");
		rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_dynamic_state "
			"   SET build_complete=true,item_count=$1,item_bytes=$2,node_count=$3,"
			"       leaf_count=$4,max_depth=$5,max_leaf_items=$6,stats_dirty=false,"
			"       updated_at=clock_timestamp() "
			" WHERE index_oid=$7 AND rnode_spc=$8 AND rnode_db=$9 AND rnode_rel=$10",
			10, types, args, nulls, false, 0);
		if (rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle final-state update failed");
		rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_dynamic_build_stage "
			"WHERE index_oid=$7 AND rnode_spc=$8 AND rnode_db=$9 AND rnode_rel=$10",
			10,types,args,nulls,false,0);
		if (rc != SPI_OK_DELETE)
			elog(ERROR, "dynamic Merkle staging-table cleanup failed: %d", rc);
		SPI_finish();
	}
	MemoryContextDelete(state->context);
}

MerkleDynamicBuildState *
merkle_dynamic_build_begin(Relation indexRel, Relation heapRel, int nkeys,
						   uint64 baseline_seq)
{
	Oid saved_userid;
	int saved_sec_context;
	MerkleDynamicBuildState *result = NULL;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		result = dynamic_build_begin_impl(indexRel, heapRel, nkeys,
			baseline_seq);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
	return result;
}

void
merkle_dynamic_build_add(MerkleDynamicBuildState *state,
						 const MerkleItemIdentity *identity,
						 const MerkleHash *hash)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_build_add_impl(state, identity, hash);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
}

void
merkle_dynamic_build_finish(MerkleDynamicBuildState *state)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_build_finish_impl(state);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
}

static void
dynamic_generation_args(const MerkleDynamicGeneration *gen, Datum args[4])
{
	args[0] = ObjectIdGetDatum(gen->index_oid);
	args[1] = ObjectIdGetDatum(gen->rnode.spcNode);
	args[2] = ObjectIdGetDatum(gen->rnode.dbNode);
	args[3] = ObjectIdGetDatum(gen->rnode.relNode);
}

static int
dynamic_parent_depth(int depth)
{
	if (depth <= 0)
		return -1;
	return depth - 1;
}

static int
dynamic_child_depth(int depth)
{
	if (depth >= MERKLE_HASH_BITS)
		return -1;
	return depth + 1;
}

static void
dynamic_child_prefix(const uint8 parent[MERKLE_HASH_BYTES], int parent_depth,
					 int ordinal, int width,
					 uint8 result[MERKLE_HASH_BYTES])
{
	int i;

	memcpy(result, parent, MERKLE_HASH_BYTES);
	for (i = 0; i < width; i++)
	{
		int bitno = parent_depth + i;
		uint8 mask = (uint8) (1U << (7 - (bitno % 8)));

		if ((ordinal >> (width - i - 1)) & 1)
			result[bitno / 8] |= mask;
		else
			result[bitno / 8] &= ~mask;
	}
	dynamic_prefix(result, parent_depth + width, result);
}

static bool
dynamic_hash_equal(const MerkleHash *left, const MerkleHash *right)
{
	return memcmp(left->data, right->data, MERKLE_HASH_BYTES) == 0;
}

/* SPI must already be connected. */
static MerkleDynamicNodeData
dynamic_load_node_spi(const MerkleDynamicGeneration *gen, int partition_id,
					  int prefix_len,
					  const uint8 prefix[MERKLE_HASH_BYTES], bool lock_row)
{
	Oid types[7] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,BYTEAOID};
	Datum args[7];
	char nulls[7] = {' ',' ',' ',' ',' ',' ',' '};
	MerkleDynamicNodeData node;
	bytea *prefix_value;
	int rc;
	const char *query = lock_row ?
		"SELECT is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,last_seq "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 AND prefix_len=$6 AND prefix_bytes=$7 FOR UPDATE" :
		"SELECT is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,last_seq "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 AND prefix_len=$6 AND prefix_bytes=$7";

	MemSet(&node, 0, sizeof(node));
	dynamic_generation_args(gen, args);
	args[4] = Int32GetDatum(partition_id);
	args[5] = Int32GetDatum(prefix_len);
	prefix_value = dynamic_bytea(prefix, MERKLE_HASH_BYTES);
	args[6] = PointerGetDatum(prefix_value);
	rc = SPI_execute_with_args(query, 7, types, args, nulls, !lock_row, 1);
	pfree(prefix_value);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "dynamic Merkle node lookup failed: %d", rc);
	if (SPI_processed == 0)
		return node;
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("duplicate dynamic Merkle node identity")));
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;

		node.found = true;
		node.is_leaf = DatumGetBool(SPI_getbinval(tuple,desc,1,&isnull));
		if (isnull)
			elog(ERROR, "null dynamic Merkle node kind");
		node.tuple_count = (uint64) DatumGetInt64(SPI_getbinval(tuple,desc,2,&isnull));
		node.subtree_bytes = (uint64) DatumGetInt64(SPI_getbinval(tuple,desc,3,&isnull));
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,4,&isnull),
			&node.data_xor,"node data_xor");
		if (isnull)
			elog(ERROR, "null dynamic Merkle node data_xor");
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,5,&isnull),
			&node.structure_hash,"node structure_hash");
		node.last_seq = (uint64) DatumGetInt64(SPI_getbinval(tuple,desc,6,&isnull));
	}
	return node;
}

static void
dynamic_write_node_spi(const MerkleDynamicGeneration *gen, int partition_id,
					   int prefix_len,
					   const uint8 prefix[MERKLE_HASH_BYTES], bool is_leaf,
					   uint64 tuple_count, uint64 subtree_bytes,
					   const MerkleHash *data_xor,
					   const MerkleHash *structure_hash, uint64 seq)
{
	Oid types[13] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,BYTEAOID,
		BOOLOID,INT8OID,INT8OID,BYTEAOID,BYTEAOID,INT8OID};
	Datum args[13];
	char nulls[13] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
	bytea *prefix_value;
	bytea *xor_value;
	bytea *structure_value;
	int rc;

	dynamic_generation_args(gen, args);
	args[4] = Int32GetDatum(partition_id);
	args[5] = Int32GetDatum(prefix_len);
	prefix_value = dynamic_bytea(prefix, MERKLE_HASH_BYTES);
	xor_value = dynamic_bytea(data_xor->data, MERKLE_HASH_BYTES);
	structure_value = dynamic_bytea(structure_hash->data, MERKLE_HASH_BYTES);
	args[6] = PointerGetDatum(prefix_value);
	args[7] = BoolGetDatum(is_leaf);
	args[8] = Int64GetDatum((int64) tuple_count);
	args[9] = Int64GetDatum((int64) subtree_bytes);
	args[10] = PointerGetDatum(xor_value);
	args[11] = PointerGetDatum(structure_value);
	args[12] = Int64GetDatum((int64) seq);
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_node "
		" (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,prefix_bytes,"
		"  is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,last_seq) "
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) "
		"ON CONFLICT (index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,prefix_bytes) "
		"DO UPDATE SET is_leaf=excluded.is_leaf,tuple_count=excluded.tuple_count,"
		" subtree_bytes=excluded.subtree_bytes,data_xor=excluded.data_xor,"
		" structure_hash=excluded.structure_hash,last_seq=excluded.last_seq",
		13, types, args, nulls, false, 0);
	pfree(prefix_value);
	pfree(xor_value);
	pfree(structure_value);
	if (rc != SPI_OK_INSERT || SPI_processed != 1)
		elog(ERROR, "dynamic Merkle node write failed: %d", rc);
}

static MerkleDynamicLoadedItem *
dynamic_load_items_spi(const MerkleDynamicGeneration *gen, int partition_id,
					   int prefix_len,
					   const uint8 prefix[MERKLE_HASH_BYTES], bool exact_leaf,
					   int *count_out)
{
	Oid types[7] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,BYTEAOID};
	Datum args[7];
	char nulls[7] = {' ',' ',' ',' ',' ',' ',' '};
	bytea *prefix_value = dynamic_bytea(prefix, MERKLE_HASH_BYTES);
	MerkleDynamicLoadedItem *items;
	int count;
	int rc;
	int i;
	const char *query = exact_leaf ?
		"SELECT key_data,route_digest,tuple_hash FROM ariabc_internal.merkle_dynamic_leaf_item "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 AND prefix_len=$6 AND prefix_bytes=$7 "
		"ORDER BY route_digest,key_data" :
		"SELECT key_data,route_digest,tuple_hash FROM ariabc_internal.merkle_dynamic_leaf_item "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 ORDER BY route_digest,key_data";

	dynamic_generation_args(gen,args);
	args[4] = Int32GetDatum(partition_id);
	args[5] = Int32GetDatum(prefix_len);
	args[6] = PointerGetDatum(prefix_value);
	rc = SPI_execute_with_args(query, exact_leaf ? 7 : 5, types, args, nulls,
		true, 0);
	pfree(prefix_value);
	if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
		elog(ERROR, "dynamic Merkle item load failed: %d", rc);
	count = (int) SPI_processed;
	items = palloc0(Max(count,1) * sizeof(*items));
	for (i = 0; i < count; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		bytea *value;

		items[i].key_data = DatumGetByteaPCopy(SPI_getbinval(tuple,desc,1,&isnull));
		if (isnull)
			elog(ERROR, "null dynamic Merkle item key");
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,2,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle item route digest");
		memcpy(items[i].route_digest,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,3,&isnull),
			&items[i].tuple_hash,"item tuple_hash");
		items[i].item_bytes = dynamic_item_bytes(items[i].key_data);
	}
	*count_out = count;
	return items;
}

static void
dynamic_delete_node_spi(const MerkleDynamicGeneration *gen, int partition_id,
						int prefix_len,
						const uint8 prefix[MERKLE_HASH_BYTES])
{
	Oid types[7] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,BYTEAOID};
	Datum args[7];
	char nulls[7] = {' ',' ',' ',' ',' ',' ',' '};
	bytea *prefix_value = dynamic_bytea(prefix, MERKLE_HASH_BYTES);
	int rc;

	dynamic_generation_args(gen,args);
	args[4] = Int32GetDatum(partition_id);
	args[5] = Int32GetDatum(prefix_len);
	args[6] = PointerGetDatum(prefix_value);
	rc = SPI_execute_with_args(
		"DELETE FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 AND prefix_len=$6 AND prefix_bytes=$7",
		7,types,args,nulls,false,0);
	pfree(prefix_value);
	if (rc != SPI_OK_DELETE)
		elog(ERROR, "dynamic Merkle node delete failed: %d", rc);
}

static void
dynamic_recompute_node_spi(const MerkleDynamicGeneration *gen,
					   int partition_id, int prefix_len,
					   const uint8 prefix[MERKLE_HASH_BYTES], uint64 seq)
{
	MerkleDynamicNodeData node = dynamic_load_node_spi(gen, partition_id,
		prefix_len, prefix, true);
	MerkleHash structure;

	if (!node.found)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle path contains a missing node")));
	if (node.is_leaf)
	{
		MerkleDynamicLoadedItem *items;
		MerkleHash data_xor;
		uint64 bytes = 0;
		int count;
		int i;

		items = dynamic_load_items_spi(gen, partition_id, prefix_len, prefix,
			true, &count);
		merkle_hash_zero(&data_xor);
		for (i = 0; i < count; i++)
		{
			if (!dynamic_prefix_matches(items[i].route_digest, prefix,
				prefix_len))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle item is assigned outside its leaf prefix")));
			merkle_hash_xor(&data_xor, &items[i].tuple_hash);
			bytes += items[i].item_bytes;
		}
		if ((uint64) count != node.tuple_count || bytes != node.subtree_bytes ||
			!dynamic_hash_equal(&data_xor, &node.data_xor))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle leaf summary is inconsistent with its items"),
					 errdetail("partition=%d prefix_len=%d node_count=%llu item_count=%d node_bytes=%llu item_bytes=%llu xor_match=%s",
						partition_id, prefix_len,
						(unsigned long long) node.tuple_count, count,
						(unsigned long long) node.subtree_bytes,
						(unsigned long long) bytes,
						dynamic_hash_equal(&data_xor, &node.data_xor) ?
						"true" : "false")));
		dynamic_leaf_structure_hash(partition_id, prefix_len, prefix, items,
			0, count, &data_xor, &structure);
	}
	else
	{
		int child_depth = dynamic_child_depth(prefix_len);
		int width;
		int ordinal;
		MerkleDynamicBuildNode children[MERKLE_DYNAMIC_LOGICAL_FANOUT];
		int child_count = 0;
		uint64 count = 0;
		uint64 bytes = 0;
		MerkleHash data_xor;

		if (child_depth < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle internal node exists at maximum depth")));
		width = child_depth - prefix_len;
		merkle_hash_zero(&data_xor);
		for (ordinal = 0; ordinal < (1 << width); ordinal++)
		{
			uint8 child_prefix[MERKLE_HASH_BYTES];
			MerkleDynamicNodeData child;

			dynamic_child_prefix(prefix, prefix_len, ordinal, width,
				child_prefix);
			child = dynamic_load_node_spi(gen, partition_id, child_depth,
				child_prefix, false);
			if (!child.found)
				continue;
			MemSet(&children[child_count], 0, sizeof(children[child_count]));
			children[child_count].partition_id = partition_id;
			children[child_count].prefix_len = child_depth;
			memcpy(children[child_count].prefix, child_prefix,
				MERKLE_HASH_BYTES);
			children[child_count].is_leaf = child.is_leaf;
			children[child_count].tuple_count = child.tuple_count;
			children[child_count].subtree_bytes = child.subtree_bytes;
			children[child_count].data_xor = child.data_xor;
			children[child_count].structure_hash = child.structure_hash;
			child_count++;
			count += child.tuple_count;
			bytes += child.subtree_bytes;
			merkle_hash_xor(&data_xor, &child.data_xor);
		}
		if (child_count == 0 || count != node.tuple_count ||
			bytes != node.subtree_bytes ||
			!dynamic_hash_equal(&data_xor, &node.data_xor))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle internal summary is inconsistent with its children")));
		dynamic_internal_structure_hash(partition_id, prefix_len, prefix,
			children, child_count, count, bytes, &data_xor, &structure);
	}
	dynamic_write_node_spi(gen, partition_id, prefix_len, prefix, node.is_leaf,
		node.tuple_count, node.subtree_bytes, &node.data_xor, &structure, seq);
}

static void
dynamic_recompute_path_spi(const MerkleDynamicGeneration *gen,
					   int partition_id,
					   const uint8 route_digest[MERKLE_HASH_BYTES],
					   int start_depth, uint64 seq)
{
	int depth = start_depth;

	while (depth >= 0)
	{
		uint8 prefix[MERKLE_HASH_BYTES];

		dynamic_prefix(route_digest, depth, prefix);
		dynamic_recompute_node_spi(gen, partition_id, depth, prefix, seq);
		dynamic_advance_command_counter();
		depth = dynamic_parent_depth(depth);
	}
}

static void
dynamic_assign_item_prefixes_spi(const MerkleDynamicGeneration *gen,
							 int partition_id,
							 MerkleDynamicLoadedItem *items,
							 int item_count, uint64 seq)
{
	Datum keys[MERKLE_DYNAMIC_BUILD_BATCH];
	Datum prefix_lens[MERKLE_DYNAMIC_BUILD_BATCH];
	Datum prefixes[MERKLE_DYNAMIC_BUILD_BATCH];
	Oid types[9] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,
		BYTEAARRAYOID,INT4ARRAYOID,BYTEAARRAYOID,INT8OID};
	Datum args[9];
	char nulls[9] = {' ',' ',' ',' ',' ',' ',' ',' ',' '};
	int offset;

	for (offset = 0; offset < item_count; offset += MERKLE_DYNAMIC_BUILD_BATCH)
	{
		int count = Min(MERKLE_DYNAMIC_BUILD_BATCH, item_count - offset);
		int i;
		int rc;

		for (i = 0; i < count; i++)
		{
			MerkleDynamicLoadedItem *item = &items[offset + i];

			keys[i] = PointerGetDatum(item->key_data);
			prefix_lens[i] = Int32GetDatum(item->assigned_prefix_len);
			prefixes[i] = PointerGetDatum(dynamic_bytea(item->assigned_prefix,
				MERKLE_HASH_BYTES));
		}
		dynamic_generation_args(gen,args);
		args[4] = Int32GetDatum(partition_id);
		args[5] = PointerGetDatum(dynamic_construct_array(keys,count,BYTEAOID));
		args[6] = PointerGetDatum(dynamic_construct_array(prefix_lens,count,INT4OID));
		args[7] = PointerGetDatum(dynamic_construct_array(prefixes,count,BYTEAOID));
		args[8] = Int64GetDatum((int64) seq);
		rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_dynamic_leaf_item AS item SET "
			"prefix_len=moved.prefix_len::smallint,"
			"prefix_bytes=moved.prefix_bytes,last_seq=$9 "
			"FROM unnest($6::bytea[],$7::int4[],$8::bytea[]) "
			"AS moved(key_data,prefix_len,prefix_bytes) "
			"WHERE item.index_oid=$1 AND item.rnode_spc=$2 "
			"AND item.rnode_db=$3 AND item.rnode_rel=$4 "
			"AND item.partition_id=$5 AND item.key_data=moved.key_data",
			9,types,args,nulls,false,0);
		for (i = 0; i < count; i++)
			pfree(DatumGetPointer(prefixes[i]));
		if (rc != SPI_OK_UPDATE || SPI_processed != (uint64) count)
			elog(ERROR, "dynamic Merkle set-based item move failed");
	}
}

static void
dynamic_split_leaf_spi(const MerkleDynamicGeneration *gen, int partition_id,
					   int prefix_len,
					   const uint8 prefix[MERKLE_HASH_BYTES], uint64 seq,
					   MerkleDynamicStructureDelta *delta)
{
	MerkleDynamicNodeData old_node = dynamic_load_node_spi(gen, partition_id,
		prefix_len, prefix, true);
	MerkleDynamicLoadedItem *items;
	MerkleDynamicNodeVector vector;
	MerkleDynamicBuildState pseudo_state;
	uint64 *cumulative;
	int item_count;
	int i;

	if (!old_node.found || !old_node.is_leaf)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle split target is not a leaf")));
	if (old_node.tuple_count <= (uint64) gen->config.leaf_capacity &&
		old_node.subtree_bytes <= (uint64) gen->config.leaf_byte_capacity)
		return;
	items = dynamic_load_items_spi(gen, partition_id, prefix_len, prefix,
		true, &item_count);
	if ((uint64) item_count != old_node.tuple_count)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle split leaf count changed")));
	cumulative = palloc0((item_count + 1) * sizeof(uint64));
	for (i = 0; i < item_count; i++)
		cumulative[i + 1] = cumulative[i] + items[i].item_bytes;
	MemSet(&pseudo_state, 0, sizeof(pseudo_state));
	pseudo_state.generation = *gen;
	MemSet(&vector, 0, sizeof(vector));
	(void) dynamic_build_subtree(&pseudo_state, items, cumulative, partition_id,
		0, item_count, prefix_len, prefix, &vector);
	if (vector.count <= 1 || vector.nodes[vector.count - 1].is_leaf)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dynamic Merkle split made no structural progress")));
	dynamic_delete_node_spi(gen, partition_id, prefix_len, prefix);
	dynamic_advance_command_counter();
	for (i = 0; i < vector.count; i++)
	{
		MerkleDynamicBuildNode *node = &vector.nodes[i];

		dynamic_write_node_spi(gen, partition_id, node->prefix_len,
			node->prefix, node->is_leaf, node->tuple_count,
			node->subtree_bytes, &node->data_xor, &node->structure_hash, seq);
	}
	dynamic_advance_command_counter();
	dynamic_assign_item_prefixes_spi(gen,partition_id,items,item_count,seq);
	dynamic_advance_command_counter();
	delta->node_delta += vector.count - 1;
	delta->leaf_delta += (int64) vector.leaf_count - 1;
	delta->split_delta += vector.count - (int) vector.leaf_count;
	delta->observed_max_depth = Max(delta->observed_max_depth,
		(int) vector.max_depth);
	delta->observed_max_leaf_items = Max(delta->observed_max_leaf_items,
		(int) vector.max_leaf_items);
	/* Splitting the unique fullest leaf can lower the exact global maximum. */
	delta->extrema_may_decrease = true;
}

static int
dynamic_locate_leaf_spi(const MerkleDynamicGeneration *gen, int partition_id,
						const uint8 route_digest[MERKLE_HASH_BYTES],
						uint64 seq, uint8 leaf_prefix[MERKLE_HASH_BYTES],
						MerkleDynamicStructureDelta *delta)
{
	int depth = 0;
	uint8 prefix[MERKLE_HASH_BYTES] = {0};

	for (;;)
	{
		MerkleDynamicNodeData node = dynamic_load_node_spi(gen, partition_id,
			depth, prefix, true);

		if (!node.found)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle traversal reached a missing node")));
		if (node.is_leaf)
		{
			memcpy(leaf_prefix, prefix, MERKLE_HASH_BYTES);
			return depth;
		}
		else
		{
			int child_depth = dynamic_child_depth(depth);
			MerkleDynamicNodeData child;
			MerkleHash zero;
			MerkleHash structure;
			MerkleDynamicLoadedItem *no_items = NULL;

			if (child_depth < 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle internal node at maximum depth")));
			dynamic_prefix(route_digest, child_depth, prefix);
			child = dynamic_load_node_spi(gen, partition_id, child_depth,
				prefix, true);
			if (!child.found)
			{
				merkle_hash_zero(&zero);
				dynamic_leaf_structure_hash(partition_id, child_depth, prefix,
					no_items, 0, 0, &zero, &structure);
				dynamic_write_node_spi(gen, partition_id, child_depth, prefix,
					true, 0, 0, &zero, &structure, seq);
				dynamic_advance_command_counter();
				delta->node_delta++;
				delta->leaf_delta++;
				delta->observed_max_depth = Max(delta->observed_max_depth,
					child_depth);
			}
			depth = child_depth;
		}
	}
}

static bool
dynamic_prefix_upper_bound(const uint8 prefix[MERKLE_HASH_BYTES], int prefix_len,
					   uint8 upper[MERKLE_HASH_BYTES])
{
	int bit;

	if (prefix_len == 0)
		return false;
	memcpy(upper, prefix, MERKLE_HASH_BYTES);
	for (bit = prefix_len - 1; bit >= 0; bit--)
	{
		uint8 mask = (uint8) (1U << (7 - (bit % 8)));

		if ((upper[bit / 8] & mask) == 0)
		{
			upper[bit / 8] |= mask;
			dynamic_prefix(upper, prefix_len, upper);
			return true;
		}
		upper[bit / 8] &= ~mask;
	}
	return false;
}

static MerkleDynamicLoadedItem *
dynamic_load_range_items_spi(const MerkleDynamicGeneration *gen,
						 int partition_id, int prefix_len,
						 const uint8 prefix[MERKLE_HASH_BYTES], int *count_out)
{
	Oid types[7] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,BYTEAOID,BYTEAOID};
	Datum args[7];
	char nulls[7] = {' ',' ',' ',' ',' ',' ',' '};
	uint8 upper_bytes[MERKLE_HASH_BYTES];
	bool has_upper = dynamic_prefix_upper_bound(prefix, prefix_len, upper_bytes);
	bytea *lower = dynamic_bytea(prefix, MERKLE_HASH_BYTES);
	bytea *upper = has_upper ? dynamic_bytea(upper_bytes,MERKLE_HASH_BYTES) : NULL;
	MerkleDynamicLoadedItem *items;
	int count;
	int rc;
	int i;

	dynamic_generation_args(gen,args);
	args[4] = Int32GetDatum(partition_id);
	args[5] = PointerGetDatum(lower);
	args[6] = has_upper ? PointerGetDatum(upper) : (Datum) 0;
	if (prefix_len == 0)
		rc = SPI_execute_with_args(
			"SELECT key_data,route_digest,tuple_hash "
			"FROM ariabc_internal.merkle_dynamic_leaf_item "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND partition_id=$5 ORDER BY route_digest,key_data",
			5,types,args,nulls,true,0);
	else if (has_upper)
		rc = SPI_execute_with_args(
			"SELECT key_data,route_digest,tuple_hash "
			"FROM ariabc_internal.merkle_dynamic_leaf_item "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND partition_id=$5 AND route_digest >= $6 AND route_digest < $7 "
			"ORDER BY route_digest,key_data",
			7,types,args,nulls,true,0);
	else
		rc = SPI_execute_with_args(
			"SELECT key_data,route_digest,tuple_hash "
			"FROM ariabc_internal.merkle_dynamic_leaf_item "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND partition_id=$5 AND route_digest >= $6 "
			"ORDER BY route_digest,key_data",
			6,types,args,nulls,true,0);
	pfree(lower);
	if (upper != NULL)
		pfree(upper);
	if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
		elog(ERROR, "dynamic Merkle route-range scan failed: %d", rc);
	count = (int) SPI_processed;
	items = palloc0(Max(count,1) * sizeof(*items));
	for (i = 0; i < count; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		bytea *value;

		items[i].key_data = DatumGetByteaPCopy(SPI_getbinval(tuple,desc,1,&isnull));
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,2,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle route-range item");
		memcpy(items[i].route_digest,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,3,&isnull),
			&items[i].tuple_hash,"range tuple_hash");
		items[i].item_bytes = dynamic_item_bytes(items[i].key_data);
	}
	*count_out = count;
	return items;
}

static int
dynamic_merge_after_delete_spi(const MerkleDynamicGeneration *gen,
						   int partition_id,
						   const uint8 route_digest[MERKLE_HASH_BYTES],
						   int leaf_depth, uint64 seq,
						   int *merged_depth_out,
						   MerkleDynamicStructureDelta *delta)
{
	int depth;
	int target_depth = -1;
	uint8 target_prefix[MERKLE_HASH_BYTES];
	MerkleDynamicNodeData target;

	for (depth = 0; depth < leaf_depth; depth = dynamic_child_depth(depth))
	{
		uint8 prefix[MERKLE_HASH_BYTES];
		MerkleDynamicNodeData node;

		dynamic_prefix(route_digest, depth, prefix);
		node = dynamic_load_node_spi(gen, partition_id, depth, prefix, true);
		if (!node.found)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle merge path is incomplete")));
		if (!node.is_leaf &&
			node.tuple_count <= (uint64) gen->config.merge_threshold &&
			node.tuple_count <= (uint64) gen->config.leaf_capacity &&
			node.subtree_bytes <= (uint64) gen->config.leaf_byte_capacity)
		{
			target_depth = depth;
			memcpy(target_prefix, prefix, MERKLE_HASH_BYTES);
			target = node;
			break;
		}
		if (depth == 255)
			break;
	}
	if (target_depth < 0)
	{
		*merged_depth_out = leaf_depth;
		return 0;
	}
	else
	{
		MerkleDynamicLoadedItem *items;
		int item_count;
		int i;
		MerkleHash data_xor;
		MerkleHash structure;
		uint64 bytes = 0;
		Oid scan_types[8] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,
			BYTEAOID,BYTEAOID};
		Datum scan_args[8];
		char scan_nulls[8] = {' ',' ',' ',' ',' ',' ',' ',' '};
		uint8 upper_bytes[MERKLE_HASH_BYTES];
		bool has_upper = dynamic_prefix_upper_bound(target_prefix,target_depth,
			upper_bytes);
		bytea *lower = dynamic_bytea(target_prefix,MERKLE_HASH_BYTES);
		bytea *upper = has_upper ? dynamic_bytea(upper_bytes,
			MERKLE_HASH_BYTES) : NULL;
		int rc;
		int64 descendant_count;
		int64 descendant_leaf_count;

		items = dynamic_load_range_items_spi(gen, partition_id, target_depth,
			target_prefix, &item_count);
		if ((uint64) item_count != target.tuple_count ||
			item_count > gen->config.merge_threshold)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle merge range summary is inconsistent")));
		merkle_hash_zero(&data_xor);
		for (i = 0; i < item_count; i++)
		{
			merkle_hash_xor(&data_xor,&items[i].tuple_hash);
			bytes += items[i].item_bytes;
		}
		if (bytes != target.subtree_bytes ||
			!dynamic_hash_equal(&data_xor,&target.data_xor))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle merge data conservation check failed")));

		dynamic_generation_args(gen,scan_args);
		scan_args[4] = Int32GetDatum(partition_id);
		scan_args[5] = Int32GetDatum(target_depth);
		scan_args[6] = PointerGetDatum(lower);
		scan_args[7] = has_upper ? PointerGetDatum(upper) : (Datum) 0;
		if (has_upper)
			rc = SPI_execute_with_args(
				"WITH removed AS ("
				" DELETE FROM ariabc_internal.merkle_dynamic_node "
				" WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
				" AND partition_id=$5 AND prefix_len>$6 "
				" AND prefix_bytes >= $7 AND prefix_bytes < $8 RETURNING is_leaf) "
				"SELECT count(*)::bigint,"
				" count(*) FILTER (WHERE is_leaf)::bigint FROM removed",
				8,scan_types,scan_args,scan_nulls,false,1);
		else
			rc = SPI_execute_with_args(
				"WITH removed AS ("
				" DELETE FROM ariabc_internal.merkle_dynamic_node "
				" WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
				" AND partition_id=$5 AND prefix_len>$6 "
				" AND prefix_bytes >= $7 RETURNING is_leaf) "
				"SELECT count(*)::bigint,"
				" count(*) FILTER (WHERE is_leaf)::bigint FROM removed",
				7,scan_types,scan_args,scan_nulls,false,1);
		pfree(lower);
		if (upper != NULL)
			pfree(upper);
		if (rc != SPI_OK_SELECT || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle indexed descendant delete failed");
		{
			bool isnull;

			descendant_count = DatumGetInt64(SPI_getbinval(
				SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
			if (isnull)
				elog(ERROR, "null dynamic Merkle descendant count");
			descendant_leaf_count = DatumGetInt64(SPI_getbinval(
				SPI_tuptable->vals[0],SPI_tuptable->tupdesc,2,&isnull));
			if (isnull)
				elog(ERROR, "null dynamic Merkle descendant leaf count");
		}
		if (descendant_count <= 0 || descendant_leaf_count <= 0 ||
			descendant_leaf_count > descendant_count)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle merge target has no valid descendants")));
		dynamic_advance_command_counter();
		for (i = 0; i < item_count; i++)
		{
			items[i].assigned_prefix_len = target_depth;
			memcpy(items[i].assigned_prefix,target_prefix,MERKLE_HASH_BYTES);
		}
		dynamic_assign_item_prefixes_spi(gen,partition_id,items,item_count,seq);
		dynamic_advance_command_counter();
		dynamic_leaf_structure_hash(partition_id,target_depth,target_prefix,
			items,0,item_count,&data_xor,&structure);
		dynamic_write_node_spi(gen,partition_id,target_depth,target_prefix,true,
			item_count,bytes,&data_xor,&structure,seq);
		dynamic_advance_command_counter();
		delta->node_delta -= descendant_count;
		delta->leaf_delta += 1 - descendant_leaf_count;
		delta->merge_delta++;
		delta->observed_max_leaf_items = Max(delta->observed_max_leaf_items,
			item_count);
		delta->extrema_may_decrease = true;
		*merged_depth_out = target_depth;
		return 1;
	}
}

typedef struct MerkleDynamicExistingItem
{
	bool found;
	ItemPointerData tid;
	int partition_id;
	int prefix_len;
	uint8 prefix[MERKLE_HASH_BYTES];
	uint8 route_digest[MERKLE_HASH_BYTES];
	MerkleHash tuple_hash;
	uint64 last_seq;
} MerkleDynamicExistingItem;

static MerkleDynamicExistingItem
dynamic_load_existing_item_spi(const MerkleDynamicGeneration *gen,
						   const bytea *key_data, bool lock_row)
{
	Oid types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,BYTEAOID};
	Datum args[5];
	char nulls[5] = {' ',' ',' ',' ',' '};
	MerkleDynamicExistingItem item;
	int rc;
	const char *query = lock_row ?
		"SELECT partition_id,prefix_len,prefix_bytes,route_digest,tuple_hash,last_seq "
		"FROM ariabc_internal.merkle_dynamic_leaf_item "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND key_data=$5 FOR UPDATE" :
		"SELECT partition_id,prefix_len,prefix_bytes,route_digest,tuple_hash,last_seq "
		"FROM ariabc_internal.merkle_dynamic_leaf_item "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND key_data=$5";

	MemSet(&item,0,sizeof(item));
	dynamic_generation_args(gen,args);
	args[4] = PointerGetDatum(key_data);
	rc = SPI_execute_with_args(query,5,types,args,nulls,!lock_row,1);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "dynamic Merkle item identity lookup failed: %d", rc);
	if (SPI_processed == 0)
		return item;
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("duplicate canonical dynamic Merkle item identity")));
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		bytea *value;

		item.found = true;
		item.partition_id = DatumGetInt32(SPI_getbinval(tuple,desc,1,&isnull));
		item.prefix_len = DatumGetInt16(SPI_getbinval(tuple,desc,2,&isnull));
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,3,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle assigned prefix");
		memcpy(item.prefix,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,4,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle stored route digest");
		memcpy(item.route_digest,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,5,&isnull),
			&item.tuple_hash,"stored tuple_hash");
		item.last_seq = (uint64) DatumGetInt64(SPI_getbinval(tuple,desc,6,&isnull));
	}
	return item;
}

static void
dynamic_apply_item_row_spi(const MerkleDynamicGeneration *gen,
					   const MerkleDynamicTransition *transition,
					   int leaf_depth,
					   const uint8 leaf_prefix[MERKLE_HASH_BYTES],
					   const MerkleDynamicExistingItem *existing)
{
	int rc;

	if (transition->has_new && existing->found)
	{
		Oid types[7] = {BYTEAOID,INT8OID,OIDOID,OIDOID,OIDOID,OIDOID,BYTEAOID};
		Datum args[7];
		char nulls[7] = {' ',' ',' ',' ',' ',' ',' '};
		bytea *hash = dynamic_bytea(transition->new_hash.data,MERKLE_HASH_BYTES);

		args[0] = PointerGetDatum(hash);
		args[1] = Int64GetDatum((int64) transition->seq);
		dynamic_generation_args(gen,&args[2]);
		args[6] = PointerGetDatum(transition->key_data);
		rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_dynamic_leaf_item SET tuple_hash=$1,last_seq=$2 "
			"WHERE index_oid=$3 AND rnode_spc=$4 AND rnode_db=$5 AND rnode_rel=$6 "
			"AND key_data=$7",
			7,types,args,nulls,false,0);
		pfree(hash);
		if (rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle item update failed");
	}
	else if (transition->has_new)
	{
		Oid types[11] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,INT4OID,BYTEAOID,
			BYTEAOID,BYTEAOID,BYTEAOID,INT8OID};
		Datum args[11];
		char nulls[11] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
		bytea *prefix = dynamic_bytea(leaf_prefix,MERKLE_HASH_BYTES);
		bytea *route = dynamic_bytea(transition->route_digest,MERKLE_HASH_BYTES);
		bytea *hash = dynamic_bytea(transition->new_hash.data,MERKLE_HASH_BYTES);

		dynamic_generation_args(gen,args);
		args[4] = Int32GetDatum(transition->partition_id);
		args[5] = Int32GetDatum(leaf_depth);
		args[6] = PointerGetDatum(prefix);
		args[7] = PointerGetDatum(transition->key_data);
		args[8] = PointerGetDatum(route);
		args[9] = PointerGetDatum(hash);
		args[10] = Int64GetDatum((int64) transition->seq);
		rc = SPI_execute_with_args(
			"INSERT INTO ariabc_internal.merkle_dynamic_leaf_item "
			"(index_oid,rnode_spc,rnode_db,rnode_rel,partition_id,prefix_len,prefix_bytes,"
			" key_data,route_digest,tuple_hash,last_seq) "
			"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
			11,types,args,nulls,false,0);
		pfree(prefix);
		pfree(route);
		pfree(hash);
		if (rc != SPI_OK_INSERT || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle item insert failed");
	}
	else
	{
		Oid types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,BYTEAOID};
		Datum args[5];
		char nulls[5] = {' ',' ',' ',' ',' '};

		dynamic_generation_args(gen,args);
		args[4] = PointerGetDatum(transition->key_data);
		rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_dynamic_leaf_item "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND key_data=$5",
			5,types,args,nulls,false,0);
		if (rc != SPI_OK_DELETE || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle item delete failed");
	}
}

static void
dynamic_update_ancestor_summaries_spi(const MerkleDynamicGeneration *gen,
						  const MerkleDynamicTransition *transition,
						  int leaf_depth, int count_delta,
						  int64 bytes_delta,
						  const MerkleHash *xor_delta)
{
	int depth = leaf_depth;

	while (depth >= 0)
	{
		uint8 prefix[MERKLE_HASH_BYTES];
		MerkleDynamicNodeData node;
		int64 new_count;
		int64 new_bytes;

		dynamic_prefix(transition->route_digest,depth,prefix);
		node = dynamic_load_node_spi(gen,transition->partition_id,depth,prefix,true);
		if (!node.found)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle update path is incomplete")));
		new_count = (int64) node.tuple_count + count_delta;
		new_bytes = (int64) node.subtree_bytes + bytes_delta;
		if (new_count < 0 || new_bytes < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle summary underflow")));
		merkle_hash_xor(&node.data_xor,xor_delta);
		/* structure_hash is recomputed after the final split/merge shape. */
		dynamic_write_node_spi(gen,transition->partition_id,depth,prefix,node.is_leaf,
			(uint64) new_count,(uint64) new_bytes,&node.data_xor,
			&node.structure_hash,transition->seq);
		depth = dynamic_parent_depth(depth);
	}
}

static uint64
dynamic_validate_state_spi(const MerkleDynamicGeneration *gen)
{
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	int rc;

	dynamic_generation_args(gen,args);
	rc = SPI_execute_with_args(
		"SELECT heap_oid,partitions,logical_fanout,leaf_capacity,merge_threshold,"
		"leaf_byte_capacity,max_key_bytes,build_complete "
		"FROM ariabc_internal.merkle_dynamic_state "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"FOR UPDATE",
		4,types,args,nulls,false,1);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dynamic Merkle generation state is missing"),
				 errhint("REINDEX the dynamic Merkle index.")));
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		Oid heap_oid = DatumGetObjectId(SPI_getbinval(tuple,desc,1,&isnull));
		int partitions = DatumGetInt32(SPI_getbinval(tuple,desc,2,&isnull));
		int fanout = DatumGetInt32(SPI_getbinval(tuple,desc,3,&isnull));
		int leaf_capacity = DatumGetInt32(SPI_getbinval(tuple,desc,4,&isnull));
		int merge_threshold = DatumGetInt32(SPI_getbinval(tuple,desc,5,&isnull));
		int byte_capacity = DatumGetInt32(SPI_getbinval(tuple,desc,6,&isnull));
		int max_key = DatumGetInt32(SPI_getbinval(tuple,desc,7,&isnull));
		bool complete = DatumGetBool(SPI_getbinval(tuple,desc,8,&isnull));
		if (heap_oid != gen->heap_oid || partitions != gen->config.partitions ||
			fanout != MERKLE_DYNAMIC_LOGICAL_FANOUT ||
			leaf_capacity != gen->config.leaf_capacity ||
			merge_threshold != gen->config.merge_threshold ||
			byte_capacity != gen->config.leaf_byte_capacity ||
			max_key != gen->config.max_key_bytes || !complete)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle state does not match its index generation")));
	}
	rc = SPI_execute_with_args(
		"SELECT applied_seq FROM ariabc_internal.merkle_dynamic_state "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
		4,types,args,nulls,true,1);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "dynamic Merkle applied sequence read failed");
	{
		bool isnull;
		uint64 result = (uint64) DatumGetInt64(SPI_getbinval(
			SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));

		if (isnull)
			elog(ERROR, "null dynamic Merkle applied sequence");
		return result;
	}
}

static void
dynamic_refresh_state_extrema_spi(const MerkleDynamicGeneration *gen)
{
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	int rc;

	/*
	 * Exact extrema can decrease after a split, merge, or delete.  Keep those
	 * global aggregates out of the mutation path and pay for one indexed-
	 * generation scan only when statistics or the full verifier are requested.
	 */
	dynamic_generation_args(gen,args);
	rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_dynamic_state AS s SET "
		" max_depth=exact.max_depth,max_leaf_items=exact.max_leaf_items,"
		" stats_dirty=false,updated_at=clock_timestamp() "
		"FROM (SELECT COALESCE(max(prefix_len),0)::integer AS max_depth,"
		" COALESCE(max(tuple_count) FILTER (WHERE is_leaf),0)::integer "
		" AS max_leaf_items "
		" FROM ariabc_internal.merkle_dynamic_node "
		" WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4) exact "
		"WHERE s.index_oid=$1 AND s.rnode_spc=$2 AND s.rnode_db=$3 "
		"AND s.rnode_rel=$4 AND s.stats_dirty",
		4,types,args,nulls,false,0);
	if (rc != SPI_OK_UPDATE)
		elog(ERROR, "dynamic Merkle exact extrema refresh failed: %d", rc);
	if (SPI_processed == 1)
		dynamic_advance_command_counter();
}

static bool
dynamic_seen_insert_spi(const MerkleDynamicGeneration *gen,
						const MerkleDynamicTransition *transition)
{
	Oid prune_types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID};
	Datum args[6];
	char nulls[6] = {' ',' ',' ',' ',' ',' '};
	Oid insert_types[6] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID,BYTEAOID};
	int rc;
	bool prune_old_sequences;

	dynamic_generation_args(gen,args);
	args[4] = Int64GetDatum((int64) transition->seq);
	args[5] = PointerGetDatum(transition->key_data);
	/*
	 * The generation row is already locked FOR UPDATE by
	 * dynamic_validate_state_spi().  Advance this durable per-sequence marker
	 * once, so a batch containing hundreds of item transitions does not repeat
	 * the same historical DELETE hundreds of times.
	 */
	rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_dynamic_state "
		"SET seen_pruned_seq=$5 "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND seen_pruned_seq < $5",
		5,prune_types,args,nulls,false,0);
	if (rc != SPI_OK_UPDATE)
		elog(ERROR, "dynamic Merkle idempotence prune marker failed");
	prune_old_sequences = SPI_processed == 1;
	if (prune_old_sequences)
	{
		rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_dynamic_seen "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND apply_seq < $5",
			5,prune_types,args,nulls,false,0);
		if (rc != SPI_OK_DELETE)
			elog(ERROR, "dynamic Merkle idempotence pruning failed");
	}
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_seen "
		"(index_oid,rnode_spc,rnode_db,rnode_rel,apply_seq,key_data) "
		"VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING 1",
		6,insert_types,args,nulls,false,1);
	if (rc != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "dynamic Merkle idempotence insert failed: %d", rc);
	return SPI_processed == 1;
}

static void
dynamic_update_state_stats_spi(const MerkleDynamicGeneration *gen,
						   const MerkleDynamicTransition *transition,
						   int count_delta, int64 bytes_delta,
						   const MerkleDynamicStructureDelta *delta)
{
	Oid types[14] = {INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,
		INT4OID,INT4OID,BOOLOID,OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[14];
	char nulls[14] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
	int rc;

	args[0] = Int64GetDatum((int64) transition->seq);
	args[1] = Int64GetDatum((int64) count_delta);
	args[2] = Int64GetDatum(bytes_delta);
	args[3] = Int64GetDatum(delta->node_delta);
	args[4] = Int64GetDatum(delta->leaf_delta);
	args[5] = Int64GetDatum(delta->split_delta);
	args[6] = Int64GetDatum(delta->merge_delta);
	args[7] = Int32GetDatum(delta->observed_max_depth);
	args[8] = Int32GetDatum(delta->observed_max_leaf_items);
	args[9] = BoolGetDatum(delta->extrema_may_decrease);
	dynamic_generation_args(gen,&args[10]);
	if (count_delta == 0 && bytes_delta == 0 &&
		delta->node_delta == 0 && delta->leaf_delta == 0 &&
		delta->split_delta == 0 && delta->merge_delta == 0 &&
		!delta->extrema_may_decrease)
	{
		rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_dynamic_state SET "
			" applied_seq=GREATEST(applied_seq,$1),updated_at=clock_timestamp() "
			"WHERE index_oid=$11 AND rnode_spc=$12 AND rnode_db=$13 AND rnode_rel=$14",
			14,types,args,nulls,false,0);
		if (rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle state position update failed");
		return;
	}
	rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_dynamic_state AS s SET "
		" applied_seq=GREATEST(s.applied_seq,$1),item_count=s.item_count+$2,"
		" item_bytes=s.item_bytes+$3,node_count=s.node_count+$4,"
		" leaf_count=s.leaf_count+$5,split_count=s.split_count+$6,"
		" merge_count=s.merge_count+$7,"
		" max_depth=GREATEST(s.max_depth,$8),"
		" max_leaf_items=GREATEST(s.max_leaf_items,$9),"
		" stats_dirty=s.stats_dirty OR $10,"
		" updated_at=clock_timestamp() "
		"WHERE s.index_oid=$11 AND s.rnode_spc=$12 AND s.rnode_db=$13 AND s.rnode_rel=$14",
		14,types,args,nulls,false,0);
	if (rc != SPI_OK_UPDATE || SPI_processed != 1)
		elog(ERROR, "dynamic Merkle state statistics update failed");
}

static void
dynamic_apply_transition_impl(const MerkleDynamicTransition *transition)
{
	Relation indexRel;
	MerkleDynamicGeneration gen;
	uint8 computed_route[MERKLE_HASH_BYTES];
	blake3_hasher route_hasher;
	uint64 applied_seq;
	MerkleDynamicExistingItem existing;
	uint8 leaf_prefix[MERKLE_HASH_BYTES];
	int leaf_depth;
	int count_delta;
	int64 bytes_delta;
	MerkleHash xor_delta;
	MerkleDynamicStructureDelta structure_delta;
	int recompute_depth;
	int rc;

	MemSet(&structure_delta,0,sizeof(structure_delta));

	if (transition == NULL || transition->key_data == NULL ||
		!OidIsValid(transition->index_oid) || transition->seq == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid dynamic Merkle transition")));
	if (!transition->has_old && !transition->has_new)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dynamic Merkle transition has neither old nor new state")));

	indexRel = index_open(transition->index_oid,RowExclusiveLock);
	dynamic_read_meta(indexRel,&gen);
	if (gen.rnode.spcNode != transition->index_rnode.spcNode ||
		gen.rnode.dbNode != transition->index_rnode.dbNode ||
		gen.rnode.relNode != transition->index_rnode.relNode)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dynamic Merkle transition targets a stale index generation")));
	if (VARSIZE_ANY_EXHDR(transition->key_data) >
		(Size) gen.config.max_key_bytes ||
		dynamic_item_bytes(transition->key_data) >
		(uint64) gen.config.leaf_byte_capacity)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("dynamic Merkle transition key exceeds index capacity")));
	blake3_hasher_init(&route_hasher);
	blake3_hasher_update(&route_hasher,VARDATA_ANY(transition->key_data),
		VARSIZE_ANY_EXHDR(transition->key_data));
	blake3_hasher_finalize(&route_hasher,computed_route,MERKLE_HASH_BYTES);
	if (memcmp(computed_route,transition->route_digest,MERKLE_HASH_BYTES) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle transition route digest does not match its canonical key")));
	if (transition->partition_id < 0 ||
		transition->partition_id >= gen.config.partitions ||
		transition->partition_id !=
		(int32) (dynamic_route_value(computed_route) %
				   (uint64) gen.config.partitions))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle transition has an invalid partition")));
	dynamic_require_relations();
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle apply SPI_connect failed");
	applied_seq = dynamic_validate_state_spi(&gen);
	if (transition->seq < applied_seq)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("stale dynamic Merkle transition sequence"),
				 errdetail("transition=%llu applied=%llu",
						(unsigned long long) transition->seq,
						(unsigned long long) applied_seq)));
	if (!dynamic_seen_insert_spi(&gen,transition))
	{
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "dynamic Merkle duplicate apply SPI_finish failed");
		index_close(indexRel,RowExclusiveLock);
		return;
	}

	existing = dynamic_load_existing_item_spi(&gen,transition->key_data,true);
	if (transition->has_old)
	{
		if (!existing.found)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle delete/update identity is missing")));
		if (!dynamic_hash_equal(&existing.tuple_hash,&transition->old_hash))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle old tuple hash does not match stored state")));
	}
	else if (existing.found)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle insert identity already exists")));
	if (existing.found &&
		(existing.partition_id != transition->partition_id ||
		 memcmp(existing.route_digest,transition->route_digest,
			MERKLE_HASH_BYTES) != 0 ||
		 !dynamic_prefix_matches(transition->route_digest,existing.prefix,
			existing.prefix_len)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("stored dynamic Merkle item routing is inconsistent")));

	leaf_depth = dynamic_locate_leaf_spi(&gen,transition->partition_id,
		transition->route_digest,transition->seq,leaf_prefix,&structure_delta);
	if (existing.found &&
		(existing.prefix_len != leaf_depth ||
		 memcmp(existing.prefix,leaf_prefix,MERKLE_HASH_BYTES) != 0))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle item assignment disagrees with traversal")));

	merkle_hash_zero(&xor_delta);
	if (transition->has_old)
		merkle_hash_xor(&xor_delta,&transition->old_hash);
	if (transition->has_new)
		merkle_hash_xor(&xor_delta,&transition->new_hash);
	count_delta = transition->has_new - transition->has_old;
	bytes_delta = (int64) dynamic_item_bytes(transition->key_data) * count_delta;
	dynamic_apply_item_row_spi(&gen,transition,leaf_depth,leaf_prefix,&existing);
	dynamic_advance_command_counter();
	dynamic_update_ancestor_summaries_spi(&gen,transition,leaf_depth,
		count_delta,bytes_delta,&xor_delta);
	dynamic_advance_command_counter();

	if (count_delta > 0)
	{
		MerkleDynamicNodeData leaf = dynamic_load_node_spi(&gen,
			transition->partition_id,leaf_depth,leaf_prefix,true);

		structure_delta.observed_max_depth = Max(
			structure_delta.observed_max_depth,leaf_depth);
		structure_delta.observed_max_leaf_items = Max(
			structure_delta.observed_max_leaf_items,(int) leaf.tuple_count);

		if (leaf.tuple_count > (uint64) gen.config.leaf_capacity ||
			leaf.subtree_bytes > (uint64) gen.config.leaf_byte_capacity)
		{
			dynamic_split_leaf_spi(&gen,transition->partition_id,
				leaf_depth,leaf_prefix,transition->seq,&structure_delta);
			recompute_depth = dynamic_parent_depth(leaf_depth);
		}
		else
			recompute_depth = leaf_depth;
	}
	else if (count_delta < 0)
	{
		int merged_depth;
		int merge_delta;

		merge_delta = dynamic_merge_after_delete_spi(&gen,
			transition->partition_id,transition->route_digest,leaf_depth,
			transition->seq,&merged_depth,&structure_delta);
		structure_delta.extrema_may_decrease = true;
		recompute_depth = merge_delta > 0 ? dynamic_parent_depth(merged_depth) :
			leaf_depth;
	}
	else
		recompute_depth = leaf_depth;
	if (recompute_depth >= 0)
		dynamic_recompute_path_spi(&gen,transition->partition_id,
			transition->route_digest,recompute_depth,transition->seq);
	dynamic_update_state_stats_spi(&gen,transition,count_delta,bytes_delta,
		&structure_delta);
	dynamic_advance_command_counter();
	rc = SPI_finish();
	if (rc != SPI_OK_FINISH)
		elog(ERROR, "dynamic Merkle apply SPI_finish failed: %d", rc);
	index_close(indexRel,RowExclusiveLock);
}

static bool
dynamic_seen_insert_batch_spi(const MerkleDynamicGeneration *gen,
						  const MerkleDynamicTransition *transitions, int count)
{
	Oid marker_types[5] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID};
	Oid insert_types[6] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID,BYTEAARRAYOID};
	Datum args[6];
	Datum *keys = palloc(sizeof(*keys) * count);
	char nulls[6] = {' ',' ',' ',' ',' ',' '};
	int rc;
	int i;

	dynamic_generation_args(gen,args);
	args[4] = Int64GetDatum((int64) transitions[0].seq);
	for (i = 0; i < count; i++)
		keys[i] = PointerGetDatum(transitions[i].key_data);
	args[5] = PointerGetDatum(dynamic_construct_array(keys,count,BYTEAOID));
	rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_dynamic_state SET seen_pruned_seq=$5 "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND seen_pruned_seq < $5",
		5,marker_types,args,nulls,false,0);
	if (rc != SPI_OK_UPDATE)
		elog(ERROR, "dynamic Merkle batch prune marker failed");
	if (SPI_processed == 1)
	{
		rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_dynamic_seen "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND apply_seq < $5",
			5,marker_types,args,nulls,false,0);
		if (rc != SPI_OK_DELETE)
			elog(ERROR, "dynamic Merkle batch idempotence pruning failed");
	}
	rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_dynamic_seen "
		"(index_oid,rnode_spc,rnode_db,rnode_rel,apply_seq,key_data) "
		"SELECT $1,$2,$3,$4,$5,u.key_data FROM unnest($6::bytea[]) u(key_data) "
		"ON CONFLICT DO NOTHING RETURNING 1",
		6,insert_types,args,nulls,false,0);
	if (rc != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "dynamic Merkle batch idempotence insert failed");
	if (SPI_processed == 0)
		return false;
	if (SPI_processed != (uint64) count)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle batch is only partially idempotent")));
	return true;
}

static MerkleDynamicExistingItem *
dynamic_load_existing_items_batch_spi(const MerkleDynamicGeneration *gen,
								  const MerkleDynamicTransition *transitions,
								  int count)
{
	Oid types[8] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4ARRAYOID,
		INT4ARRAYOID,BYTEAARRAYOID,BYTEAARRAYOID};
	Datum args[8];
	Datum *ordinals = palloc(sizeof(*ordinals) * count);
	Datum *partitions = palloc(sizeof(*partitions) * count);
	Datum *routes = palloc(sizeof(*routes) * count);
	Datum *keys = palloc(sizeof(*keys) * count);
	char nulls[8] = {' ',' ',' ',' ',' ',' ',' ',' '};
	MerkleDynamicExistingItem *items = palloc0(sizeof(*items) * count);
	int rc;
	int i;

	for (i = 0; i < count; i++)
	{
		ordinals[i] = Int32GetDatum(i);
		partitions[i] = Int32GetDatum(transitions[i].partition_id);
		routes[i] = PointerGetDatum(dynamic_bytea(
			transitions[i].route_digest,MERKLE_HASH_BYTES));
		keys[i] = PointerGetDatum(transitions[i].key_data);
	}
	dynamic_generation_args(gen,args);
	args[4] = PointerGetDatum(dynamic_construct_array(ordinals,count,INT4OID));
	args[5] = PointerGetDatum(dynamic_construct_array(partitions,count,INT4OID));
	args[6] = PointerGetDatum(dynamic_construct_array(routes,count,BYTEAOID));
	args[7] = PointerGetDatum(dynamic_construct_array(keys,count,BYTEAOID));
	rc = SPI_execute_with_args(
		"WITH request(ordinal,partition_id,route_digest,key_data) AS ("
		" SELECT * FROM unnest($5::int4[],$6::int4[],$7::bytea[],$8::bytea[])"
		") SELECT r.ordinal,i.partition_id,i.prefix_len,i.prefix_bytes,"
		" i.route_digest,i.tuple_hash,i.last_seq,i.ctid FROM request r"
		" CROSS JOIN LATERAL ("
		"  SELECT partition_id,prefix_len,prefix_bytes,route_digest,tuple_hash,last_seq,ctid"
		"  FROM ariabc_internal.merkle_dynamic_leaf_item i"
		"  WHERE i.index_oid=$1 AND i.rnode_spc=$2 AND i.rnode_db=$3"
		"  AND i.rnode_rel=$4 AND i.partition_id=r.partition_id"
		"  AND i.route_digest=r.route_digest AND i.key_data=r.key_data FOR UPDATE"
		" ) i ORDER BY r.ordinal",
		8,types,args,nulls,false,0);
	if (rc != SPI_OK_SELECT || SPI_processed != (uint64) count)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle batch item identity set is incomplete")));
	for (i = 0; i < count; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		MerkleDynamicExistingItem *item;
		bytea *value;
		bool isnull;
		int ordinal = DatumGetInt32(SPI_getbinval(tuple,desc,1,&isnull));

		if (isnull || ordinal < 0 || ordinal >= count || items[ordinal].found)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle batch item ordinal is invalid")));
		item = &items[ordinal];
		item->found = true;
		item->partition_id = DatumGetInt32(SPI_getbinval(tuple,desc,2,&isnull));
		item->prefix_len = DatumGetInt16(SPI_getbinval(tuple,desc,3,&isnull));
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,4,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle batch item prefix");
		memcpy(item->prefix,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,5,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			elog(ERROR, "invalid dynamic Merkle batch item route");
		memcpy(item->route_digest,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,6,&isnull),
			&item->tuple_hash,"stored tuple_hash");
		item->last_seq = (uint64) DatumGetInt64(
			SPI_getbinval(tuple,desc,7,&isnull));
		{
			Datum tid_datum = SPI_getbinval(tuple,desc,8,&isnull);

			if (isnull)
				elog(ERROR, "invalid dynamic Merkle batch item ctid");
			ItemPointerCopy((ItemPointer) DatumGetPointer(tid_datum),
							&item->tid);
		}
		if (!ItemPointerIsValid(&item->tid))
			elog(ERROR, "invalid dynamic Merkle batch item ctid");
	}
	return items;
}

static void
dynamic_update_items_batch_spi(const MerkleDynamicGeneration *gen,
						   const MerkleDynamicTransition *transitions,
						   const MerkleDynamicExistingItem *existing, int count)
{
	Oid types[8] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID,TIDOID,
		BYTEAOID,BYTEAOID};
	Datum args[8];
	char nulls[8] = {' ',' ',' ',' ',' ',' ',' ',' '};
	SPIPlanPtr plan;
	int rc;
	int i;

	dynamic_generation_args(gen,args);
	args[4] = Int64GetDatum((int64) transitions[0].seq);
	plan = SPI_prepare(
		"UPDATE ariabc_internal.merkle_dynamic_leaf_item i "
		"SET tuple_hash=$8,last_seq=$5 "
		"WHERE i.index_oid=$1 AND i.rnode_spc=$2 AND i.rnode_db=$3"
		" AND i.rnode_rel=$4 AND i.ctid=$6 AND i.tuple_hash=$7",
		8,types);
	if (plan == NULL)
		elog(ERROR, "dynamic Merkle batch item update prepare failed");
	for (i = 0; i < count; i++)
	{
		bytea *old_hash = dynamic_bytea(
			transitions[i].old_hash.data,MERKLE_HASH_BYTES);
		bytea *new_hash = dynamic_bytea(
			transitions[i].new_hash.data,MERKLE_HASH_BYTES);

		args[5] = PointerGetDatum(&existing[i].tid);
		args[6] = PointerGetDatum(old_hash);
		args[7] = PointerGetDatum(new_hash);
		rc = SPI_execute_plan(plan,args,nulls,false,0);
		pfree(old_hash);
		pfree(new_hash);
		if (rc != SPI_OK_UPDATE || SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle batch old tuple hash does not match stored state")));
	}
	SPI_freeplan(plan);
}

static int
dynamic_batch_node_ptr_cmp(const void *left, const void *right)
{
	const MerkleDynamicBatchNode *a = *(MerkleDynamicBatchNode *const *) left;
	const MerkleDynamicBatchNode *b = *(MerkleDynamicBatchNode *const *) right;
	int cmp;

	if (a->key.prefix_len != b->key.prefix_len)
		return a->key.prefix_len > b->key.prefix_len ? -1 : 1;
	if (a->key.partition_id != b->key.partition_id)
		return a->key.partition_id < b->key.partition_id ? -1 : 1;
	cmp = memcmp(a->key.prefix,b->key.prefix,MERKLE_HASH_BYTES);
	return cmp < 0 ? -1 : cmp > 0 ? 1 : 0;
}

static int
dynamic_batch_node_lookup_cmp(const void *left, const void *right)
{
	const MerkleDynamicBatchNode *a = *(MerkleDynamicBatchNode *const *) left;
	const MerkleDynamicBatchNode *b = *(MerkleDynamicBatchNode *const *) right;
	int cmp;

	if (a->key.partition_id != b->key.partition_id)
		return a->key.partition_id < b->key.partition_id ? -1 : 1;
	if (a->key.prefix_len != b->key.prefix_len)
		return a->key.prefix_len < b->key.prefix_len ? -1 : 1;
	cmp = memcmp(a->key.prefix,b->key.prefix,MERKLE_HASH_BYTES);
	return cmp < 0 ? -1 : cmp > 0 ? 1 : 0;
}

static void
dynamic_update_batch_nodes_spi(const MerkleDynamicGeneration *gen, HTAB *nodes,
						   uint64 seq, MerkleDynamicBatchNode ***ordered_out,
						   int *count_out)
{
	HASH_SEQ_STATUS scan;
	MerkleDynamicBatchNode *entry;
	MerkleDynamicBatchNode **all_entries;
	MerkleDynamicBatchNode **ordered;
	Oid select_types[7] = {OIDOID,OIDOID,OIDOID,OIDOID,INT4OID,
		INT4OID,BYTEAOID};
	Oid update_types[8] = {OIDOID,OIDOID,OIDOID,OIDOID,INT8OID,TIDOID,
		BYTEAOID,BYTEAOID};
	Datum args[10];
	char nulls[10] = {' ',' ',' ',' ',' ',' ',' ',' ',' ',' '};
	SPIPlanPtr select_plan;
	SPIPlanPtr update_plan;
	int total_count = (int) hash_get_num_entries(nodes);
	int count = 0;
	int rc;
	int i = 0;

	all_entries = palloc(sizeof(*all_entries) * total_count);
	ordered = palloc(sizeof(*ordered) * total_count);
	hash_seq_init(&scan,nodes);
	while ((entry = hash_seq_search(&scan)) != NULL)
		all_entries[i++] = entry;
	Assert(i == total_count);
	qsort(all_entries,total_count,sizeof(*all_entries),
		dynamic_batch_node_lookup_cmp);
	dynamic_generation_args(gen,args);
	select_plan = SPI_prepare(
		"SELECT is_leaf,tuple_count,subtree_bytes,data_xor,structure_hash,ctid "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND partition_id=$5 AND prefix_len=$6 AND prefix_bytes=$7 FOR UPDATE",
		7,select_types);
	if (select_plan == NULL)
		elog(ERROR, "dynamic Merkle batch node lookup prepare failed");
	for (i = 0; i < total_count; i++)
	{
		bytea *prefix;

		entry = all_entries[i];
		args[4] = Int32GetDatum(entry->key.partition_id);
		args[5] = Int32GetDatum(entry->key.prefix_len);
		prefix = dynamic_bytea(entry->key.prefix,MERKLE_HASH_BYTES);
		args[6] = PointerGetDatum(prefix);
		rc = SPI_execute_plan(select_plan,args,nulls,false,1);
		pfree(prefix);
		if (rc != SPI_OK_SELECT || SPI_processed > 1)
			elog(ERROR, "dynamic Merkle batch node lookup failed: %d", rc);
		if (SPI_processed == 0)
		{
			entry->found = false;
			if (entry->affected)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle batch update path is incomplete")));
			continue;
		}
		{
			HeapTuple tuple = SPI_tuptable->vals[0];
			TupleDesc desc = SPI_tuptable->tupdesc;
			MerkleHash current;
			bool isnull;
			Datum tid_datum;

			entry->found = true;
			entry->is_leaf = DatumGetBool(
				SPI_getbinval(tuple,desc,1,&isnull));
			if (isnull)
				elog(ERROR, "null dynamic Merkle batch node kind");
			entry->tuple_count = (uint64) DatumGetInt64(
				SPI_getbinval(tuple,desc,2,&isnull));
			entry->subtree_bytes = (uint64) DatumGetInt64(
				SPI_getbinval(tuple,desc,3,&isnull));
			dynamic_hash_from_datum(SPI_getbinval(tuple,desc,4,&isnull),
				&current,"node data_xor");
			entry->data_xor = current;
			dynamic_hash_from_datum(SPI_getbinval(tuple,desc,5,&isnull),
				&entry->structure_hash,"node structure_hash");
			tid_datum = SPI_getbinval(tuple,desc,6,&isnull);

			if (isnull)
				elog(ERROR, "invalid dynamic Merkle batch node ctid");
			ItemPointerCopy((ItemPointer) DatumGetPointer(tid_datum),&entry->tid);
		}
		if (entry->affected)
		{
			merkle_hash_xor(&entry->data_xor,&entry->xor_delta);
			ordered[count++] = entry;
		}
		else
			entry->structure_computed = true;
	}
	SPI_freeplan(select_plan);
	qsort(ordered,count,sizeof(*ordered),dynamic_batch_node_ptr_cmp);
	for (i = 0; i < count; i++)
	{
		MerkleDynamicBatchNode *node = ordered[i];

		if (node->is_leaf)
		{
			MerkleDynamicLoadedItem *items;
			MerkleHash data_xor;
			uint64 bytes = 0;
			int item_count;
			int item;

			items = dynamic_load_items_spi(gen,node->key.partition_id,
				node->key.prefix_len,node->key.prefix,true,&item_count);
			merkle_hash_zero(&data_xor);
			for (item = 0; item < item_count; item++)
			{
				if (!dynamic_prefix_matches(items[item].route_digest,
					node->key.prefix,node->key.prefix_len))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("dynamic Merkle batch item is outside its leaf prefix")));
				bytes += items[item].item_bytes;
				merkle_hash_xor(&data_xor,&items[item].tuple_hash);
			}
			if ((uint64) item_count != node->tuple_count ||
				bytes != node->subtree_bytes ||
				!dynamic_hash_equal(&data_xor,&node->data_xor))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle batch leaf summary is inconsistent")));
			dynamic_leaf_structure_hash(node->key.partition_id,
				node->key.prefix_len,node->key.prefix,items,0,item_count,
				&node->data_xor,&node->structure_hash);
		}
		else
		{
			MerkleDynamicBuildNode children[2];
			MerkleHash data_xor;
			uint64 tuple_count = 0;
			uint64 subtree_bytes = 0;
			int child_count = 0;
			int ordinal;
			int child_depth = dynamic_child_depth(node->key.prefix_len);

			if (child_depth < 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle batch internal node has invalid depth")));
			merkle_hash_zero(&data_xor);
			for (ordinal = 0; ordinal < 2; ordinal++)
			{
				MerkleDynamicVerifyNodeKey child_key;
				MerkleDynamicBatchNode *batch_child;
				MerkleHash child_data_xor;
				MerkleHash child_structure;
				uint64 child_count_value;
				uint64 child_bytes;
				bool child_leaf;
				bool found;

				MemSet(&child_key,0,sizeof(child_key));
				child_key.partition_id = node->key.partition_id;
				child_key.prefix_len = (uint16) child_depth;
				dynamic_child_prefix(node->key.prefix,node->key.prefix_len,
					ordinal,1,child_key.prefix);
				batch_child = hash_search(nodes,&child_key,HASH_FIND,&found);
				if (found && batch_child != NULL && batch_child->found)
				{
					if (!batch_child->structure_computed)
						elog(ERROR, "dynamic Merkle batch child ordering is invalid");
					child_data_xor = batch_child->data_xor;
					child_structure = batch_child->structure_hash;
					child_count_value = batch_child->tuple_count;
					child_bytes = batch_child->subtree_bytes;
					child_leaf = batch_child->is_leaf;
				}
				else
					continue;
				MemSet(&children[child_count],0,sizeof(children[child_count]));
				children[child_count].partition_id = node->key.partition_id;
				children[child_count].prefix_len = (uint16) child_depth;
				memcpy(children[child_count].prefix,child_key.prefix,
					MERKLE_HASH_BYTES);
				children[child_count].is_leaf = child_leaf;
				children[child_count].tuple_count = child_count_value;
				children[child_count].subtree_bytes = child_bytes;
				children[child_count].data_xor = child_data_xor;
				children[child_count].structure_hash = child_structure;
				child_count++;
				tuple_count += child_count_value;
				subtree_bytes += child_bytes;
				merkle_hash_xor(&data_xor,&child_data_xor);
			}
			if (child_count == 0 || tuple_count != node->tuple_count ||
				subtree_bytes != node->subtree_bytes ||
				!dynamic_hash_equal(&data_xor,&node->data_xor))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle batch internal summary is inconsistent")));
			dynamic_internal_structure_hash(node->key.partition_id,
				node->key.prefix_len,node->key.prefix,children,child_count,
				node->tuple_count,node->subtree_bytes,&node->data_xor,
				&node->structure_hash);
		}
		node->structure_computed = true;
	}
	dynamic_generation_args(gen,args);
	args[4] = Int64GetDatum((int64) seq);
	update_plan = SPI_prepare(
		"UPDATE ariabc_internal.merkle_dynamic_node n "
		"SET data_xor=$7,structure_hash=$8,last_seq=$5 "
		"WHERE n.index_oid=$1 AND n.rnode_spc=$2 AND n.rnode_db=$3"
		" AND n.rnode_rel=$4 AND n.ctid=$6",
		8,update_types);
	if (update_plan == NULL)
		elog(ERROR, "dynamic Merkle batch node update prepare failed");
	for (i = 0; i < count; i++)
	{
		bytea *data_xor = dynamic_bytea(ordered[i]->data_xor.data,
			MERKLE_HASH_BYTES);
		bytea *structure = dynamic_bytea(ordered[i]->structure_hash.data,
			MERKLE_HASH_BYTES);

		args[5] = PointerGetDatum(&ordered[i]->tid);
		args[6] = PointerGetDatum(data_xor);
		args[7] = PointerGetDatum(structure);
		rc = SPI_execute_plan(update_plan,args,nulls,false,0);
		pfree(data_xor);
		pfree(structure);
		if (rc != SPI_OK_UPDATE || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle batch node summary update failed");
	}
	SPI_freeplan(update_plan);
	*ordered_out = ordered;
	*count_out = count;
}

static void
dynamic_apply_update_batch_impl(const MerkleDynamicTransition *transitions,
								int count)
{
	Relation indexRel;
	MerkleDynamicGeneration gen;
	MerkleDynamicExistingItem *existing;
	HASHCTL ctl;
	HTAB *nodes;
	MerkleDynamicBatchNode **ordered;
	MerkleDynamicStructureDelta structure_delta;
	uint64 applied_seq;
	instr_time profile_start;
	instr_time profile_end;
	double seen_ms;
	double existing_ms;
	double item_ms;
	double node_ms;
	int node_count;
	int rc;
	int i;

	if (count <= 0)
		return;
	indexRel = index_open(transitions[0].index_oid,RowExclusiveLock);
	dynamic_read_meta(indexRel,&gen);
	for (i = 0; i < count; i++)
	{
		uint8 route[MERKLE_HASH_BYTES];
		blake3_hasher hasher;

		if (!transitions[i].has_old || !transitions[i].has_new ||
			transitions[i].seq != transitions[0].seq ||
			transitions[i].index_oid != gen.index_oid ||
			!RelFileNodeEquals(transitions[i].index_rnode,gen.rnode))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid dynamic Merkle update batch")));
		blake3_hasher_init(&hasher);
		blake3_hasher_update(&hasher,VARDATA_ANY(transitions[i].key_data),
			VARSIZE_ANY_EXHDR(transitions[i].key_data));
		blake3_hasher_finalize(&hasher,route,MERKLE_HASH_BYTES);
		if (memcmp(route,transitions[i].route_digest,MERKLE_HASH_BYTES) != 0 ||
			transitions[i].partition_id < 0 ||
			transitions[i].partition_id >= gen.config.partitions ||
			transitions[i].partition_id != (int32) (dynamic_route_value(route) %
				(uint64) gen.config.partitions))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle batch item routing is invalid")));
	}
	dynamic_require_relations();
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle batch apply SPI_connect failed");
	applied_seq = dynamic_validate_state_spi(&gen);
	if (transitions[0].seq < applied_seq)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("stale dynamic Merkle update batch sequence")));
	INSTR_TIME_SET_CURRENT(profile_start);
	if (!dynamic_seen_insert_batch_spi(&gen,transitions,count))
	{
		SPI_finish();
		index_close(indexRel,RowExclusiveLock);
		return;
	}
	INSTR_TIME_SET_CURRENT(profile_end);
	INSTR_TIME_SUBTRACT(profile_end,profile_start);
	seen_ms = INSTR_TIME_GET_MILLISEC(profile_end);
	INSTR_TIME_SET_CURRENT(profile_start);
	existing = dynamic_load_existing_items_batch_spi(&gen,transitions,count);
	INSTR_TIME_SET_CURRENT(profile_end);
	INSTR_TIME_SUBTRACT(profile_end,profile_start);
	existing_ms = INSTR_TIME_GET_MILLISEC(profile_end);
	MemSet(&ctl,0,sizeof(ctl));
	ctl.keysize = sizeof(MerkleDynamicVerifyNodeKey);
	ctl.entrysize = sizeof(MerkleDynamicBatchNode);
	ctl.hcxt = CurrentMemoryContext;
	nodes = hash_create("dynamic Merkle batch nodes",count * 8,&ctl,
		HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	for (i = 0; i < count; i++)
	{
		MerkleHash delta;
		int depth;

		if (!existing[i].found ||
			existing[i].partition_id != transitions[i].partition_id ||
			memcmp(existing[i].route_digest,transitions[i].route_digest,
				MERKLE_HASH_BYTES) != 0 ||
			!dynamic_hash_equal(&existing[i].tuple_hash,&transitions[i].old_hash))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle batch old item state does not match")));
		delta = transitions[i].old_hash;
		merkle_hash_xor(&delta,&transitions[i].new_hash);
		for (depth = existing[i].prefix_len; depth >= 0; depth--)
		{
			MerkleDynamicVerifyNodeKey key;
			MerkleDynamicBatchNode *node;
			bool found;

			MemSet(&key,0,sizeof(key));
			key.partition_id = transitions[i].partition_id;
			key.prefix_len = (uint16) depth;
			dynamic_prefix(transitions[i].route_digest,depth,key.prefix);
			node = hash_search(nodes,&key,HASH_ENTER,&found);
			if (!found)
				merkle_hash_zero(&node->xor_delta);
			node->affected = true;
			merkle_hash_xor(&node->xor_delta,&delta);
		}
	}
	{
		HASH_SEQ_STATUS scan;
		MerkleDynamicBatchNode *node;
		MerkleDynamicVerifyNodeKey *parents;
		int parent_count = 0;
		int parent_capacity = (int) hash_get_num_entries(nodes);

		parents = palloc(sizeof(*parents) * Max(parent_capacity,1));
		hash_seq_init(&scan,nodes);
		while ((node = hash_seq_search(&scan)) != NULL)
			if (node->affected && node->key.prefix_len < MERKLE_HASH_BITS)
				parents[parent_count++] = node->key;
		for (i = 0; i < parent_count; i++)
		{
			int ordinal;

			for (ordinal = 0; ordinal < 2; ordinal++)
			{
				MerkleDynamicVerifyNodeKey child_key;
				MerkleDynamicBatchNode *child;
				bool found;

				MemSet(&child_key,0,sizeof(child_key));
				child_key.partition_id = parents[i].partition_id;
				child_key.prefix_len = parents[i].prefix_len + 1;
				dynamic_child_prefix(parents[i].prefix,parents[i].prefix_len,
					ordinal,1,child_key.prefix);
				child = hash_search(nodes,&child_key,HASH_ENTER,&found);
				if (!found)
				{
					merkle_hash_zero(&child->xor_delta);
					child->affected = false;
				}
			}
		}
	}
	INSTR_TIME_SET_CURRENT(profile_start);
	dynamic_update_items_batch_spi(&gen,transitions,existing,count);
	dynamic_advance_command_counter();
	INSTR_TIME_SET_CURRENT(profile_end);
	INSTR_TIME_SUBTRACT(profile_end,profile_start);
	item_ms = INSTR_TIME_GET_MILLISEC(profile_end);
	INSTR_TIME_SET_CURRENT(profile_start);
	dynamic_update_batch_nodes_spi(&gen,nodes,transitions[0].seq,
		&ordered,&node_count);
	dynamic_advance_command_counter();
	INSTR_TIME_SET_CURRENT(profile_end);
	INSTR_TIME_SUBTRACT(profile_end,profile_start);
	node_ms = INSTR_TIME_GET_MILLISEC(profile_end);
	for (i = 0; i < count; i++)
	{
		MerkleDynamicVerifyNodeKey key;
		MerkleDynamicBatchNode *leaf;
		bool found;

		MemSet(&key,0,sizeof(key));
		key.partition_id = transitions[i].partition_id;
		key.prefix_len = (uint16) existing[i].prefix_len;
		memcpy(key.prefix,existing[i].prefix,MERKLE_HASH_BYTES);
		leaf = hash_search(nodes,&key,HASH_FIND,&found);
		if (!found || leaf == NULL || !leaf->is_leaf)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle batch item leaf assignment is invalid")));
	}
	MemSet(&structure_delta,0,sizeof(structure_delta));
	dynamic_update_state_stats_spi(&gen,&transitions[0],0,0,&structure_delta);
	dynamic_advance_command_counter();
	ereport(LOG,
			(errmsg("dynamic Merkle update batch profile"),
			 errdetail("items=%d affected_nodes=%d seen_ms=%.3f existing_ms=%.3f item_ms=%.3f node_ms=%.3f",
				count,node_count,seen_ms,existing_ms,item_ms,node_ms)));
	rc = SPI_finish();
	if (rc != SPI_OK_FINISH)
		elog(ERROR, "dynamic Merkle batch apply SPI_finish failed: %d", rc);
	index_close(indexRel,RowExclusiveLock);
}

void
merkle_dynamic_apply_transition(const MerkleDynamicTransition *transition)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_apply_transition_impl(transition);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
}

void
merkle_dynamic_apply_update_batch(const MerkleDynamicTransition *transitions,
								  int count)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_apply_update_batch_impl(transitions,count);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
}

static void
dynamic_verify_node_key(MerkleDynamicVerifyNodeKey *key, int partition_id,
						int prefix_len,
						const uint8 prefix[MERKLE_HASH_BYTES])
{
	MemSet(key, 0, sizeof(*key));
	key->partition_id = partition_id;
	key->prefix_len = (uint16) prefix_len;
	memcpy(key->prefix, prefix, MERKLE_HASH_BYTES);
}

static HTAB *
dynamic_verify_create_node_map(MemoryContext context)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MerkleDynamicVerifyNodeKey);
	ctl.entrysize = sizeof(MerkleDynamicVerifyNode);
	ctl.hcxt = context;
	return hash_create("dynamic Merkle verification nodes", 1024, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static MerkleDynamicVerifyNode *
dynamic_verify_find_node(HTAB *nodes, int partition_id, int prefix_len,
						 const uint8 prefix[MERKLE_HASH_BYTES])
{
	MerkleDynamicVerifyNodeKey key;

	dynamic_verify_node_key(&key, partition_id, prefix_len, prefix);
	return hash_search(nodes, &key, HASH_FIND, NULL);
}

static bool
dynamic_verify_leaf(MerkleDynamicVerifyNode *node,
					MerkleDynamicLoadedItem *items, int count,
					const MerkleDynamicGeneration *gen)
{
	MerkleHash data_xor;
	MerkleHash structure;
	uint64 bytes = 0;
	int i;

	if (node == NULL || !node->is_leaf || node->leaf_checked ||
		count < 0 || count > gen->config.leaf_capacity)
		return false;
	merkle_hash_zero(&data_xor);
	for (i = 0; i < count; i++)
	{
		bytes += items[i].item_bytes;
		merkle_hash_xor(&data_xor, &items[i].tuple_hash);
	}
	if (bytes > (uint64) gen->config.leaf_byte_capacity ||
		node->tuple_count != (uint64) count ||
		node->subtree_bytes != bytes ||
		!dynamic_hash_equal(&node->data_xor, &data_xor))
		return false;
	dynamic_leaf_structure_hash(node->key.partition_id, node->key.prefix_len,
		node->key.prefix, items, 0, count, &data_xor, &structure);
	if (!dynamic_hash_equal(&node->structure_hash, &structure))
		return false;
	node->leaf_checked = true;
	return true;
}

static bool
dynamic_verify_heap_batch_spi(const MerkleDynamicGeneration *gen,
						  Datum *keys, Datum *partitions,
						  Datum *routes, Datum *hashes, int count)
{
	Oid types[8] = {OIDOID,OIDOID,OIDOID,OIDOID,
		BYTEAARRAYOID,INT4ARRAYOID,BYTEAARRAYOID,BYTEAARRAYOID};
	Datum args[8];
	char nulls[8] = {' ',' ',' ',' ',' ',' ',' ',' '};
	bool isnull;
	int64 mismatches;
	int rc;

	if (count <= 0)
		return true;
	dynamic_generation_args(gen, args);
	args[4] = PointerGetDatum(dynamic_construct_array(keys, count, BYTEAOID));
	args[5] = PointerGetDatum(dynamic_construct_array(partitions, count, INT4OID));
	args[6] = PointerGetDatum(dynamic_construct_array(routes, count, BYTEAOID));
	args[7] = PointerGetDatum(dynamic_construct_array(hashes, count, BYTEAOID));
	rc = SPI_execute_with_args(
		"SELECT count(*) FROM "
		" unnest($5::bytea[],$6::int4[],$7::bytea[],$8::bytea[]) "
		" AS wanted(key_data,partition_id,route_digest,tuple_hash) "
		"LEFT JOIN ariabc_internal.merkle_dynamic_leaf_item AS stored "
		" ON stored.index_oid=$1 AND stored.rnode_spc=$2 "
		"AND stored.rnode_db=$3 AND stored.rnode_rel=$4 "
		"AND stored.key_data=wanted.key_data "
		"WHERE stored.key_data IS NULL "
		"OR stored.partition_id<>wanted.partition_id "
		"OR stored.route_digest<>wanted.route_digest "
		"OR stored.tuple_hash<>wanted.tuple_hash",
		8, types, args, nulls, true, 1);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "dynamic Merkle heap verification batch failed: %d", rc);
	mismatches = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
		SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		elog(ERROR, "dynamic Merkle heap verification returned a null count");
	return mismatches == 0;
}

static bool
dynamic_verify_relations_impl(Relation heapRel, Relation indexRel,
						  Snapshot snapshot)
{
	MerkleDynamicGeneration gen;
	MemoryContext verify_context;
	MemoryContext batch_context;
	MemoryContext old_context;
	HTAB *nodes;
	Portal portal;
	Oid generation_types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum generation_args[4];
	char generation_nulls[4] = {' ',' ',' ',' '};
	uint64 applied_seq;
	uint64 expected_items;
	uint64 expected_item_bytes;
	uint64 expected_nodes;
	uint64 expected_leaves;
	uint64 expected_max_leaf_items;
	int expected_max_depth;
	bool build_complete;
	bool match = true;
	uint64 actual_items = 0;
	uint64 actual_item_bytes = 0;
	uint64 actual_leaves = 0;
	uint64 actual_max_leaf_items = 0;
	int actual_max_depth = 0;
	MerkleDynamicVerifyNode **node_vector;
	long node_count;
	long node_pos = 0;
	HASH_SEQ_STATUS node_seq;
	MerkleDynamicVerifyNode *node;
	MerkleHash *heap_xors;
	uint64 *heap_counts;
	int rc;
	int i;

	if (heapRel == NULL || indexRel == NULL || snapshot == NULL ||
		indexRel->rd_index->indrelid != RelationGetRelid(heapRel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid relations supplied for dynamic Merkle verification")));
	dynamic_read_meta(indexRel, &gen);
	if (gen.heap_oid != RelationGetRelid(heapRel))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("dynamic Merkle heap identity does not match the index")));
	dynamic_require_relations();
	verify_context = AllocSetContextCreate(CurrentMemoryContext,
		"dynamic Merkle verification", ALLOCSET_DEFAULT_SIZES);
	batch_context = AllocSetContextCreate(verify_context,
		"dynamic Merkle verification batch", ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(verify_context);
	nodes = dynamic_verify_create_node_map(verify_context);
	heap_xors = palloc0(sizeof(*heap_xors) * gen.config.partitions);
	heap_counts = palloc0(sizeof(*heap_counts) * gen.config.partitions);
	dynamic_generation_args(&gen, generation_args);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle verification SPI_connect failed");
	applied_seq = dynamic_validate_state_spi(&gen);
	dynamic_refresh_state_extrema_spi(&gen);
	rc = SPI_execute_with_args(
		"SELECT build_complete,item_count,item_bytes,node_count,leaf_count,"
		"max_depth,max_leaf_items FROM ariabc_internal.merkle_dynamic_state "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
		4, generation_types, generation_args, generation_nulls, true, 1);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "dynamic Merkle verification state read failed");
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;

		build_complete = DatumGetBool(SPI_getbinval(tuple,desc,1,&isnull));
		expected_items = (uint64) DatumGetInt64(
			SPI_getbinval(tuple,desc,2,&isnull));
		expected_item_bytes = (uint64) DatumGetInt64(
			SPI_getbinval(tuple,desc,3,&isnull));
		expected_nodes = (uint64) DatumGetInt64(
			SPI_getbinval(tuple,desc,4,&isnull));
		expected_leaves = (uint64) DatumGetInt64(
			SPI_getbinval(tuple,desc,5,&isnull));
		expected_max_depth = DatumGetInt32(
			SPI_getbinval(tuple,desc,6,&isnull));
		expected_max_leaf_items = (uint64) DatumGetInt32(
			SPI_getbinval(tuple,desc,7,&isnull));
	}
	if (!build_complete)
		match = false;

	portal = SPI_cursor_open_with_args(NULL,
		"SELECT partition_id,prefix_len,prefix_bytes,is_leaf,tuple_count,"
		"subtree_bytes,data_xor,structure_hash,last_seq "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"ORDER BY partition_id,prefix_len,prefix_bytes",
		4, generation_types, generation_args, generation_nulls, true, 0);
	if (portal == NULL)
		elog(ERROR, "could not open dynamic Merkle node verification cursor");
	for (;;)
	{
		SPI_cursor_fetch(portal, true, MERKLE_DYNAMIC_VERIFY_FETCH);
		if (SPI_processed == 0)
			break;
		for (i = 0; i < (int) SPI_processed; i++)
		{
			HeapTuple tuple = SPI_tuptable->vals[i];
			TupleDesc desc = SPI_tuptable->tupdesc;
			MerkleDynamicVerifyNodeKey key;
			MerkleDynamicVerifyNode *entry;
			bytea *prefix_value;
			uint8 canonical[MERKLE_HASH_BYTES];
			bool isnull;
			bool found;
			int partition_id;
			int prefix_len;

			partition_id = DatumGetInt32(SPI_getbinval(tuple,desc,1,&isnull));
			prefix_len = DatumGetInt16(SPI_getbinval(tuple,desc,2,&isnull));
			prefix_value = DatumGetByteaPP(SPI_getbinval(tuple,desc,3,&isnull));
			if (isnull || partition_id < 0 ||
				partition_id >= gen.config.partitions || prefix_len < 0 ||
				prefix_len > MERKLE_HASH_BITS ||
				VARSIZE_ANY_EXHDR(prefix_value) != MERKLE_HASH_BYTES)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle node has an invalid identity")));
			dynamic_prefix((uint8 *) VARDATA_ANY(prefix_value), prefix_len,
				canonical);
			if (memcmp(canonical, VARDATA_ANY(prefix_value),
				MERKLE_HASH_BYTES) != 0)
				match = false;
			dynamic_verify_node_key(&key, partition_id, prefix_len, canonical);
			entry = hash_search(nodes, &key, HASH_ENTER, &found);
			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("duplicate dynamic Merkle node identity")));
			/* dynahash copies the key but does not zero the data area. */
			entry->leaf_checked = false;
			entry->is_leaf = DatumGetBool(
				SPI_getbinval(tuple,desc,4,&isnull));
			entry->tuple_count = (uint64) DatumGetInt64(
				SPI_getbinval(tuple,desc,5,&isnull));
			entry->subtree_bytes = (uint64) DatumGetInt64(
				SPI_getbinval(tuple,desc,6,&isnull));
			dynamic_hash_from_datum(SPI_getbinval(tuple,desc,7,&isnull),
				&entry->data_xor,"verification node data_xor");
			dynamic_hash_from_datum(SPI_getbinval(tuple,desc,8,&isnull),
				&entry->structure_hash,"verification node structure_hash");
			entry->last_seq = (uint64) DatumGetInt64(
				SPI_getbinval(tuple,desc,9,&isnull));
			if (entry->last_seq > applied_seq)
				match = false;
		}
	}
	SPI_cursor_close(portal);
	node_count = hash_get_num_entries(nodes);
	node_vector = palloc(sizeof(*node_vector) * Max(node_count, 1));
	hash_seq_init(&node_seq, nodes);
	while ((node = hash_seq_search(&node_seq)) != NULL)
		node_vector[node_pos++] = node;
	Assert(node_pos == node_count);

	/* Stream authoritative leaf items in physical-leaf order. */
	portal = SPI_cursor_open_with_args(NULL,
		"SELECT partition_id,prefix_len,prefix_bytes,key_data,route_digest,"
		"tuple_hash,last_seq FROM ariabc_internal.merkle_dynamic_leaf_item "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"ORDER BY partition_id,prefix_len,prefix_bytes,route_digest,key_data",
		4, generation_types, generation_args, generation_nulls, true, 0);
	if (portal == NULL)
		elog(ERROR, "could not open dynamic Merkle item verification cursor");
	{
		MerkleDynamicVerifyNode *current_leaf = NULL;
		MerkleDynamicLoadedItem *items = palloc0(sizeof(*items) *
			(gen.config.leaf_capacity + 1));
		int item_count = 0;
		bool overflow = false;

		for (;;)
		{
			SPI_cursor_fetch(portal, true, MERKLE_DYNAMIC_VERIFY_FETCH);
			if (SPI_processed == 0)
				break;
			for (i = 0; i < (int) SPI_processed; i++)
			{
				HeapTuple tuple = SPI_tuptable->vals[i];
				TupleDesc desc = SPI_tuptable->tupdesc;
				bytea *prefix_value;
				bytea *key_data;
				bytea *route_value;
				bytea *hash_value;
				MerkleDynamicVerifyNode *leaf;
				uint8 route[MERKLE_HASH_BYTES];
				uint8 computed_route[MERKLE_HASH_BYTES];
				blake3_hasher route_hasher;
				bool isnull;
				int partition_id;
				int prefix_len;
				uint64 last_seq;

				partition_id = DatumGetInt32(
					SPI_getbinval(tuple,desc,1,&isnull));
				prefix_len = DatumGetInt16(
					SPI_getbinval(tuple,desc,2,&isnull));
				prefix_value = DatumGetByteaPP(
					SPI_getbinval(tuple,desc,3,&isnull));
				if (isnull || VARSIZE_ANY_EXHDR(prefix_value) != MERKLE_HASH_BYTES)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("dynamic Merkle item has an invalid leaf prefix")));
				leaf = dynamic_verify_find_node(nodes, partition_id, prefix_len,
					(uint8 *) VARDATA_ANY(prefix_value));
				if (leaf != current_leaf)
				{
					if (current_leaf != NULL)
					{
						if (overflow || !dynamic_verify_leaf(current_leaf, items,
							item_count, &gen))
							match = false;
						while (item_count > 0)
							pfree(items[--item_count].key_data);
					}
					current_leaf = leaf;
					overflow = false;
				}
				if (leaf == NULL || !leaf->is_leaf)
					match = false;
				key_data = DatumGetByteaPP(
					SPI_getbinval(tuple,desc,4,&isnull));
				route_value = DatumGetByteaPP(
					SPI_getbinval(tuple,desc,5,&isnull));
				hash_value = DatumGetByteaPP(
					SPI_getbinval(tuple,desc,6,&isnull));
				last_seq = (uint64) DatumGetInt64(
					SPI_getbinval(tuple,desc,7,&isnull));
				if (leaf != NULL && leaf->last_seq < last_seq)
					match = false;
				if (VARSIZE_ANY_EXHDR(route_value) != MERKLE_HASH_BYTES ||
					VARSIZE_ANY_EXHDR(hash_value) != MERKLE_HASH_BYTES)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("dynamic Merkle item has an invalid digest length")));
				if (VARSIZE_ANY_EXHDR(key_data) == 0 ||
					VARSIZE_ANY_EXHDR(key_data) > (Size) gen.config.max_key_bytes ||
					last_seq > applied_seq)
					match = false;
				memcpy(route, VARDATA_ANY(route_value), MERKLE_HASH_BYTES);
				blake3_hasher_init(&route_hasher);
				blake3_hasher_update(&route_hasher,VARDATA_ANY(key_data),
					VARSIZE_ANY_EXHDR(key_data));
				blake3_hasher_finalize(&route_hasher,computed_route,
					MERKLE_HASH_BYTES);
				if (memcmp(route,computed_route,MERKLE_HASH_BYTES) != 0 ||
					partition_id != (int32) (dynamic_route_value(route) %
						(uint64) gen.config.partitions) ||
					!dynamic_prefix_matches(route,
						(uint8 *) VARDATA_ANY(prefix_value),prefix_len))
					match = false;
				actual_items++;
				actual_item_bytes += dynamic_item_bytes(key_data);
				if (item_count >= gen.config.leaf_capacity)
					overflow = true;
				else
				{
					items[item_count].key_data = DatumGetByteaPCopy(
						PointerGetDatum(key_data));
					memcpy(items[item_count].route_digest,route,
						MERKLE_HASH_BYTES);
					memcpy(items[item_count].tuple_hash.data,
						VARDATA_ANY(hash_value),MERKLE_HASH_BYTES);
					items[item_count].item_bytes = dynamic_item_bytes(key_data);
					item_count++;
				}
			}
		}
		if (current_leaf != NULL)
		{
			if (overflow || !dynamic_verify_leaf(current_leaf,items,item_count,&gen))
				match = false;
			while (item_count > 0)
				pfree(items[--item_count].key_data);
		}
	}
	SPI_cursor_close(portal);

	/* Validate every physical node bottom-up, including empty leaves. */
	for (node_pos = 0; node_pos < node_count; node_pos++)
	{
		MerkleDynamicVerifyNode *entry = node_vector[node_pos];

		actual_max_depth = Max(actual_max_depth, (int) entry->key.prefix_len);
		if (entry->is_leaf)
		{
			actual_leaves++;
			actual_max_leaf_items = Max(actual_max_leaf_items,
				entry->tuple_count);
			if (!entry->leaf_checked &&
				!dynamic_verify_leaf(entry, NULL, 0, &gen))
				match = false;
		}
		else if (entry->key.prefix_len == MERKLE_HASH_BITS)
			match = false;
		if (entry->key.prefix_len == 0)
			continue;
		{
			uint8 parent_prefix[MERKLE_HASH_BYTES];
			MerkleDynamicVerifyNode *parent;

			dynamic_prefix(entry->key.prefix, entry->key.prefix_len - 1,
				parent_prefix);
			parent = dynamic_verify_find_node(nodes, entry->key.partition_id,
				entry->key.prefix_len - 1, parent_prefix);
			if (parent == NULL || parent->is_leaf ||
				parent->last_seq < entry->last_seq)
				match = false;
		}
	}
	for (i = MERKLE_HASH_BITS - 1; i >= 0; i--)
	{
		for (node_pos = 0; node_pos < node_count; node_pos++)
		{
			MerkleDynamicVerifyNode *entry = node_vector[node_pos];
			MerkleDynamicBuildNode children[2];
			MerkleHash data_xor;
			MerkleHash structure;
			uint64 count = 0;
			uint64 bytes = 0;
			int child_count = 0;
			int ordinal;

			if (entry->is_leaf || entry->key.prefix_len != i)
				continue;
			merkle_hash_zero(&data_xor);
			for (ordinal = 0; ordinal < 2; ordinal++)
			{
				uint8 child_prefix[MERKLE_HASH_BYTES];
				MerkleDynamicVerifyNode *child;

				dynamic_child_prefix(entry->key.prefix, i, ordinal, 1,
					child_prefix);
				child = dynamic_verify_find_node(nodes,entry->key.partition_id,
					i + 1,child_prefix);
				if (child == NULL)
					continue;
				MemSet(&children[child_count],0,sizeof(children[child_count]));
				children[child_count].partition_id = entry->key.partition_id;
				children[child_count].prefix_len = i + 1;
				memcpy(children[child_count].prefix,child_prefix,
					MERKLE_HASH_BYTES);
				children[child_count].is_leaf = child->is_leaf;
				children[child_count].tuple_count = child->tuple_count;
				children[child_count].subtree_bytes = child->subtree_bytes;
				children[child_count].data_xor = child->data_xor;
				children[child_count].structure_hash = child->structure_hash;
				child_count++;
				count += child->tuple_count;
				bytes += child->subtree_bytes;
				merkle_hash_xor(&data_xor,&child->data_xor);
			}
			if (child_count == 0 || count != entry->tuple_count ||
				bytes != entry->subtree_bytes ||
				!dynamic_hash_equal(&data_xor,&entry->data_xor) ||
				(count <= (uint64) gen.config.merge_threshold &&
				 bytes <= (uint64) gen.config.leaf_byte_capacity))
				match = false;
			dynamic_internal_structure_hash(entry->key.partition_id,i,
				entry->key.prefix,children,child_count,count,bytes,&data_xor,
				&structure);
			if (!dynamic_hash_equal(&structure,&entry->structure_hash))
				match = false;
		}
	}

	/* Every partition has exactly one root and state statistics are exact. */
	for (i = 0; i < gen.config.partitions; i++)
	{
		uint8 root_prefix[MERKLE_HASH_BYTES] = {0};
		MerkleDynamicVerifyNode *root = dynamic_verify_find_node(nodes,i,0,
			root_prefix);

		if (root == NULL)
			match = false;
	}
	if ((uint64) node_count != expected_nodes || actual_items != expected_items ||
		actual_item_bytes != expected_item_bytes ||
		actual_leaves != expected_leaves ||
		actual_max_depth != expected_max_depth ||
		actual_max_leaf_items != expected_max_leaf_items)
		match = false;

	/* Recompute every heap row and prove an exact key/route/hash side-row match. */
	{
		TableScanDesc scan;
		TupleTableSlot *slot;
		int nkeys = indexRel->rd_index->indnkeyatts;
		Datum *key_values = palloc(sizeof(*key_values) * nkeys);
		bool *key_nulls = palloc(sizeof(*key_nulls) * nkeys);
		Datum *batch_keys = palloc(sizeof(*batch_keys) * MERKLE_DYNAMIC_VERIFY_BATCH);
		Datum *batch_partitions = palloc(sizeof(*batch_partitions) * MERKLE_DYNAMIC_VERIFY_BATCH);
		Datum *batch_routes = palloc(sizeof(*batch_routes) * MERKLE_DYNAMIC_VERIFY_BATCH);
		Datum *batch_hashes = palloc(sizeof(*batch_hashes) * MERKLE_DYNAMIC_VERIFY_BATCH);
		int batch_count = 0;
		uint64 heap_total = 0;

		slot = table_slot_create(heapRel,NULL);
		scan = table_beginscan(heapRel,snapshot,0,NULL);
		while (table_scan_getnextslot(scan,ForwardScanDirection,slot))
		{
			MerkleItemIdentity identity;
			MerkleHash tuple_hash;
			MemoryContext prior;
			int keyno;

			for (keyno = 0; keyno < nkeys; keyno++)
			{
				AttrNumber attno = indexRel->rd_index->indkey.values[keyno];

				if (attno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("dynamic Merkle index has an expression key")));
				key_values[keyno] = slot_getattr(slot,attno,&key_nulls[keyno]);
			}
			prior = MemoryContextSwitchTo(batch_context);
			merkle_compute_dynamic_item_identity(indexRel,key_values,key_nulls,
				nkeys,gen.config.partitions,gen.config.max_key_bytes,&identity);
			merkle_compute_slot_hash(heapRel,slot,&tuple_hash);
			batch_keys[batch_count] = PointerGetDatum(identity.key_data);
			batch_partitions[batch_count] = Int32GetDatum(
				identity.route.partition_id);
			batch_routes[batch_count] = PointerGetDatum(dynamic_bytea(
				identity.route.route_digest,MERKLE_HASH_BYTES));
			batch_hashes[batch_count] = PointerGetDatum(dynamic_bytea(
				tuple_hash.data,MERKLE_HASH_BYTES));
			MemoryContextSwitchTo(prior);
			heap_counts[identity.route.partition_id]++;
			merkle_hash_xor(&heap_xors[identity.route.partition_id],&tuple_hash);
			heap_total++;
			batch_count++;
			if (batch_count == MERKLE_DYNAMIC_VERIFY_BATCH)
			{
				prior = MemoryContextSwitchTo(batch_context);
				if (!dynamic_verify_heap_batch_spi(&gen,batch_keys,
					batch_partitions,batch_routes,batch_hashes,batch_count))
					match = false;
				MemoryContextSwitchTo(prior);
				MemoryContextReset(batch_context);
				batch_count = 0;
			}
			ExecClearTuple(slot);
		}
		if (batch_count > 0)
		{
			MemoryContext prior = MemoryContextSwitchTo(batch_context);

			if (!dynamic_verify_heap_batch_spi(&gen,batch_keys,batch_partitions,
				batch_routes,batch_hashes,batch_count))
				match = false;
			MemoryContextSwitchTo(prior);
		}
		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);
		if (heap_total != actual_items)
			match = false;
	}
	for (i = 0; i < gen.config.partitions; i++)
	{
		uint8 root_prefix[MERKLE_HASH_BYTES] = {0};
		MerkleDynamicVerifyNode *root = dynamic_verify_find_node(nodes,i,0,
			root_prefix);

		if (root == NULL || root->tuple_count != heap_counts[i] ||
			!dynamic_hash_equal(&root->data_xor,&heap_xors[i]))
			match = false;
	}
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "dynamic Merkle verification SPI_finish failed");
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(verify_context);
	return match;
}

bool
merkle_dynamic_verify_relations(Relation heapRel, Relation indexRel,
							Snapshot snapshot)
{
	Oid saved_userid;
	int saved_sec_context;
	bool result = false;
	bool pushed_active = false;

	if (snapshot != InvalidSnapshot)
	{
		PushCopiedSnapshot(snapshot);
		pushed_active = true;
	}

	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		result = dynamic_verify_relations_impl(heapRel,indexRel,snapshot);
	}
	PG_CATCH();
	{
		if (pushed_active)
			PopActiveSnapshot();
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (pushed_active)
		PopActiveSnapshot();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	return result;
}

static void
dynamic_root_impl(Relation indexRel, MerkleHash *hash, uint64 *tuple_count)
{
	MerkleDynamicGeneration gen;
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	blake3_hasher hasher;
	static const uint8 domain[] = {'A','R','I','D','Y','N','R','1'};
	uint64 total = 0;
	int rc;
	int i;

	dynamic_read_meta(indexRel,&gen);
	dynamic_require_relations();
	dynamic_generation_args(&gen,args);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle root SPI_connect failed");
	dynamic_validate_state_spi(&gen);
	rc = SPI_execute_with_args(
		"SELECT partition_id,tuple_count,data_xor "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND prefix_len=0 AND prefix_bytes=decode(repeat('00',32),'hex') "
		"ORDER BY partition_id",
		4,types,args,nulls,true,0);
	if (rc != SPI_OK_SELECT || SPI_processed != (uint64) gen.config.partitions)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle partition-root set is incomplete")));
	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher,domain,sizeof(domain));
	dynamic_hash_u32(&hasher,MERKLE_DYNAMIC_LAYOUT_VERSION);
	dynamic_hash_u32(&hasher,(uint32) gen.config.partitions);
	for (i = 0; i < gen.config.partitions; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		int partition_id = DatumGetInt32(SPI_getbinval(tuple,desc,1,&isnull));
		uint64 count = (uint64) DatumGetInt64(SPI_getbinval(tuple,desc,2,&isnull));
		MerkleHash data_xor;

		if (partition_id != i)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle partition roots are not canonical")));
		dynamic_hash_from_datum(SPI_getbinval(tuple,desc,3,&isnull),
			&data_xor,"partition data_xor");
		dynamic_hash_u32(&hasher,(uint32) partition_id);
		dynamic_hash_u64(&hasher,count);
		blake3_hasher_update(&hasher,data_xor.data,MERKLE_HASH_BYTES);
		total += count;
	}
	blake3_hasher_finalize(&hasher,hash->data,MERKLE_HASH_BYTES);
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "dynamic Merkle root SPI_finish failed");
	if (tuple_count != NULL)
		*tuple_count = total;
}

void
merkle_dynamic_root(Relation indexRel, MerkleHash *hash, uint64 *tuple_count)
{
	Oid saved_userid;
	int saved_sec_context;

	if (hash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dynamic Merkle root output cannot be null")));
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_root_impl(indexRel,hash,tuple_count);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
}

static char *
dynamic_stats_json_impl(Relation indexRel)
{
	MerkleDynamicGeneration gen;
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	char *result;
	MemoryContext caller_context = CurrentMemoryContext;
	int rc;

	dynamic_read_meta(indexRel,&gen);
	dynamic_require_relations();
	dynamic_generation_args(&gen,args);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dynamic Merkle stats SPI_connect failed");
	dynamic_validate_state_spi(&gen);
	dynamic_refresh_state_extrema_spi(&gen);
	rc = SPI_execute_with_args(
		"SELECT jsonb_build_object("
		" 'state',CASE WHEN build_complete AND structure_failures=0 THEN 'READY' ELSE 'INVALID' END,"
		" 'build_complete',build_complete,'partitions',partitions,"
		" 'logical_fanout',logical_fanout,'leaf_capacity',leaf_capacity,"
		" 'merge_threshold',merge_threshold,'leaf_byte_capacity',leaf_byte_capacity,"
		" 'max_key_bytes',max_key_bytes,'applied_seq',applied_seq,"
		" 'seen_pruned_seq',seen_pruned_seq,"
		" 'item_count',item_count,'item_bytes',item_bytes,'node_count',node_count,"
		" 'leaf_count',leaf_count,'max_depth',max_depth,"
		" 'max_leaf_items',max_leaf_items,'split_count',split_count,"
		" 'merge_count',merge_count,'structure_failures',structure_failures,"
		" 'stats_dirty',stats_dirty)::text "
		"FROM ariabc_internal.merkle_dynamic_state "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
		4,types,args,nulls,true,1);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dynamic Merkle statistics state is missing")));
	{
		bool isnull;
		char *value = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
			SPI_tuptable->tupdesc,1,&isnull));
		MemoryContext old_context;

		if (isnull)
			elog(ERROR, "null dynamic Merkle stats JSON");
		/* SPI_finish() destroys SPI-owned allocations.  Copy the JSON into
		 * the caller's context before ending the SPI session. */
		old_context = MemoryContextSwitchTo(caller_context);
		result = pstrdup(value);
		MemoryContextSwitchTo(old_context);
		pfree(value);
	}
	SPI_finish();
	return result;
}

char *
merkle_dynamic_stats_json(Relation indexRel)
{
	Oid saved_userid;
	int saved_sec_context;
	char *result = NULL;

	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		result = dynamic_stats_json_impl(indexRel);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	return result;
}

void
merkle_dynamic_vacuum_stats(Relation indexRel, IndexBulkDeleteResult *stats)
{
	MerkleDynamicGeneration gen;
	Oid saved_userid;
	int saved_sec_context;
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	int rc;

	if (stats == NULL)
		return;
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		dynamic_read_meta(indexRel,&gen);
		dynamic_require_relations();
		dynamic_generation_args(&gen,args);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle vacuum stats SPI_connect failed");
		rc = SPI_execute_with_args(
			"SELECT item_count,node_count FROM ariabc_internal.merkle_dynamic_state "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
			4,types,args,nulls,true,1);
		if (rc != SPI_OK_SELECT || SPI_processed != 1)
			elog(ERROR, "dynamic Merkle vacuum state is missing");
		{
			bool isnull;
			uint64 items = (uint64) DatumGetInt64(SPI_getbinval(
				SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
			uint64 nodes = (uint64) DatumGetInt64(SPI_getbinval(
				SPI_tuptable->vals[0],SPI_tuptable->tupdesc,2,&isnull));

			stats->num_index_tuples = (double) items;
			stats->num_pages = (BlockNumber) Max((uint64) 1,
				(nodes * sizeof(MerkleDynamicBuildNode) + BLCKSZ - 1) / BLCKSZ);
			stats->estimated_count = false;
		}
		SPI_finish();
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
}

void
merkle_dynamic_drop_state(Oid index_oid, RelFileNode index_rnode)
{
	Oid saved_userid;
	int saved_sec_context;
	Oid namespace_oid;
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	int rc;

	namespace_oid = get_namespace_oid("ariabc_internal",true);
	if (!OidIsValid(namespace_oid) ||
		!OidIsValid(get_relname_relid("merkle_dynamic_state",namespace_oid)))
		return;
	args[0] = ObjectIdGetDatum(index_oid);
	args[1] = ObjectIdGetDatum(index_rnode.spcNode);
	args[2] = ObjectIdGetDatum(index_rnode.dbNode);
	args[3] = ObjectIdGetDatum(index_rnode.relNode);
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle drop-state SPI_connect failed");
		rc = SPI_execute_with_args(
			"DELETE FROM ariabc_internal.merkle_dynamic_state "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
			4,types,args,nulls,false,0);
		if (rc != SPI_OK_DELETE)
			elog(ERROR, "dynamic Merkle generation cleanup failed: %d", rc);
		SPI_finish();
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
}

static Relation
dynamic_open_index_arg(Oid relid, LOCKMODE lockmode)
{
	Relation rel = relation_open(relid,lockmode);

	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		if (rel->rd_rel->relam != MERKLE_AM_OID ||
			!merkle_index_is_dynamic(rel))
		{
			relation_close(rel,lockmode);
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("relation is not a dynamic Merkle index")));
		}
		return rel;
	}
	else
	{
		List *indexes = RelationGetIndexList(rel);
		ListCell *cell;
		Oid found = InvalidOid;

		foreach(cell,indexes)
		{
			Oid index_oid = lfirst_oid(cell);
			Relation candidate = index_open(index_oid,AccessShareLock);

			if (candidate->rd_rel->relam == MERKLE_AM_OID &&
				merkle_index_is_dynamic(candidate))
				found = index_oid;
			index_close(candidate,AccessShareLock);
			if (OidIsValid(found))
				break;
		}
		list_free(indexes);
		relation_close(rel,lockmode);
		if (!OidIsValid(found))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation has no dynamic Merkle index")));
		return index_open(found,lockmode);
	}
}

static Tuplestorestate *
dynamic_begin_materialized_srf(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext old_context;
	Tuplestorestate *tupstore;

	if (rsinfo == NULL || !IsA(rsinfo,ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize) || rsinfo->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode is required for this dynamic Merkle function")));
	old_context = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	*tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
	tupstore = tuplestore_begin_heap(true,false,work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = *tupdesc;
	MemoryContextSwitchTo(old_context);
	return tupstore;
}

static MerkleDynamicRequest *
dynamic_parse_requests_spi(Jsonb *json, int *count_out)
{
	Oid type = JSONBOID;
	Datum arg = PointerGetDatum(json);
	char null = ' ';
	MerkleDynamicRequest *requests;
	int count;
	int rc;
	int i;

	rc = SPI_execute_with_args(
		"SELECT partition_id,prefix_length,decode(prefix_value,'hex') "
		"FROM pg_catalog.jsonb_to_recordset($1) "
		" AS r(partition_id integer,prefix_length integer,prefix_value text)",
		1,&type,&arg,&null,true,0);
	if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
		elog(ERROR, "dynamic Merkle range request parsing failed: %d", rc);
	count = (int) SPI_processed;
	requests = palloc0(Max(count,1) * sizeof(*requests));
	for (i = 0; i < count; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		int32 prefix_len;
		bytea *value;
		uint8 canonical[MERKLE_HASH_BYTES];

		requests[i].partition_id = DatumGetInt32(SPI_getbinval(tuple,desc,1,&isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("dynamic Merkle range partition_id cannot be null")));
		prefix_len = DatumGetInt32(SPI_getbinval(tuple,desc,2,&isnull));
		if (isnull || prefix_len < 0 || prefix_len > MERKLE_HASH_BITS)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle range prefix_length is invalid")));
		requests[i].prefix_len = (uint16) prefix_len;
		value = DatumGetByteaPP(SPI_getbinval(tuple,desc,3,&isnull));
		if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle prefix_value must contain exactly 64 hex digits")));
		memcpy(requests[i].prefix,VARDATA_ANY(value),MERKLE_HASH_BYTES);
		dynamic_prefix(requests[i].prefix,requests[i].prefix_len,canonical);
		if (memcmp(canonical,requests[i].prefix,MERKLE_HASH_BYTES) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle prefix_value has nonzero bits after prefix_length")));
	}
	*count_out = count;
	return requests;
}

static int
dynamic_request_ptr_cmp(const void *left, const void *right)
{
	const MerkleDynamicRequest *a = *(MerkleDynamicRequest *const *) left;
	const MerkleDynamicRequest *b = *(MerkleDynamicRequest *const *) right;
	int cmp;

	if (a->partition_id != b->partition_id)
		return a->partition_id < b->partition_id ? -1 : 1;
	if (a->prefix_len != b->prefix_len)
		return a->prefix_len < b->prefix_len ? -1 : 1;
	cmp = memcmp(a->prefix,b->prefix,MERKLE_HASH_BYTES);
	return cmp < 0 ? -1 : cmp > 0 ? 1 : 0;
}

/*
 * Resolve exact physical nodes through the primary-key index.  A single
 * Native B-tree probes make the access path independent of planner estimates.
 * A batched SQL/LATERAL lookup was faster than scalar SPI calls at small sizes,
 * but PostgreSQL still selected a side-table sequential scan at 5M rows.
 *
 * Because the physical tree splits one bit at a time, an absent exact node
 * cannot have a descendant: the requested logical range is empty or is
 * contained by a physical leaf.  In that case the route index can derive the
 * summary from at most leaf_capacity items.
 */
static MerkleDynamicNodeData *
dynamic_range_summaries_spi(const MerkleDynamicGeneration *gen,
							const MerkleDynamicRequest *requests, int count)
{
	Oid namespace_oid;
	Oid node_oid;
	Oid node_index_oid;
	Relation node_rel;
	Relation node_index_rel;
	MerkleDynamicRequest **ordered;
	MerkleDynamicNodeData *results;
	bool *exact_found;
	int i;

	results = palloc0(Max(count,1) * sizeof(*results));
	if (count == 0)
		return results;
	for (i = 0; i < count; i++)
	{
		if (requests[i].partition_id < 0 ||
			requests[i].partition_id >= gen->config.partitions)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle range partition %d is out of bounds",
							requests[i].partition_id)));
	}
	exact_found = palloc0(sizeof(*exact_found) * count);
	ordered = palloc(sizeof(*ordered) * count);
	for (i = 0; i < count; i++)
		ordered[i] = (MerkleDynamicRequest *) &requests[i];
	qsort(ordered,count,sizeof(*ordered),dynamic_request_ptr_cmp);
	namespace_oid = get_namespace_oid("ariabc_internal",false);
	node_oid = get_relname_relid("merkle_dynamic_node",namespace_oid);
	node_index_oid = get_relname_relid("merkle_dynamic_node_pkey",namespace_oid);
	if (!OidIsValid(node_oid) || !OidIsValid(node_index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("dynamic Merkle node identity index is missing")));
	node_rel = table_open(node_oid,AccessShareLock);
	node_index_rel = index_open(node_index_oid,AccessShareLock);
	for (i = 0; i < count; i++)
	{
		MerkleDynamicRequest *request = ordered[i];
		int ordinal = (int) (request - requests);
		ScanKeyData keys[7];
		SysScanDesc scan;
		HeapTuple tuple;
		bytea *prefix = dynamic_bytea(request->prefix,MERKLE_HASH_BYTES);
		bool isnull;

		ScanKeyInit(&keys[0],1,BTEqualStrategyNumber,F_OIDEQ,
			ObjectIdGetDatum(gen->index_oid));
		ScanKeyInit(&keys[1],2,BTEqualStrategyNumber,F_OIDEQ,
			ObjectIdGetDatum(gen->rnode.spcNode));
		ScanKeyInit(&keys[2],3,BTEqualStrategyNumber,F_OIDEQ,
			ObjectIdGetDatum(gen->rnode.dbNode));
		ScanKeyInit(&keys[3],4,BTEqualStrategyNumber,F_OIDEQ,
			ObjectIdGetDatum(gen->rnode.relNode));
		ScanKeyInit(&keys[4],5,BTEqualStrategyNumber,F_INT4EQ,
			Int32GetDatum(request->partition_id));
		ScanKeyInit(&keys[5],6,BTEqualStrategyNumber,F_INT2EQ,
			Int16GetDatum((int16) request->prefix_len));
		ScanKeyInit(&keys[6],7,BTEqualStrategyNumber,F_BYTEAEQ,
			PointerGetDatum(prefix));
		scan = systable_beginscan_ordered(node_rel,node_index_rel,
			GetActiveSnapshot(),lengthof(keys),keys);
		tuple = systable_getnext_ordered(scan,ForwardScanDirection);
		if (HeapTupleIsValid(tuple))
		{
			TupleDesc desc = RelationGetDescr(node_rel);

			results[ordinal].found = true;
			exact_found[ordinal] = true;
			results[ordinal].is_leaf = DatumGetBool(
				heap_getattr(tuple,8,desc,&isnull));
			if (isnull)
				elog(ERROR, "null dynamic Merkle range node kind");
			results[ordinal].tuple_count = (uint64) DatumGetInt64(
				heap_getattr(tuple,9,desc,&isnull));
			results[ordinal].subtree_bytes = (uint64) DatumGetInt64(
				heap_getattr(tuple,10,desc,&isnull));
			dynamic_hash_from_datum(heap_getattr(tuple,11,desc,&isnull),
				&results[ordinal].data_xor,"range node data_xor");
			dynamic_hash_from_datum(heap_getattr(tuple,12,desc,&isnull),
				&results[ordinal].structure_hash,"range node structure_hash");
			results[ordinal].last_seq = (uint64) DatumGetInt64(
				heap_getattr(tuple,13,desc,&isnull));
			if (HeapTupleIsValid(systable_getnext_ordered(scan,
				ForwardScanDirection)))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("duplicate dynamic Merkle node identity")));
		}
		systable_endscan_ordered(scan);
		pfree(prefix);
		if (!results[ordinal].found)
		{
			results[ordinal].found = true;
			results[ordinal].is_leaf = true;
		}
	}
	index_close(node_index_rel,AccessShareLock);
	table_close(node_rel,AccessShareLock);
	for (i = 0; i < count; i++)
		if (!exact_found[i])
		{
			MerkleDynamicLoadedItem *items;
			int item_count;
			int item;

			items = dynamic_load_range_items_spi(gen,
				requests[i].partition_id,
				requests[i].prefix_len,requests[i].prefix,&item_count);
			if (item_count > gen->config.leaf_capacity)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("absent dynamic Merkle node contains an unbounded item range")));
			for (item = 0; item < item_count; item++)
			{
				results[i].tuple_count++;
				results[i].subtree_bytes += items[item].item_bytes;
				merkle_hash_xor(&results[i].data_xor,&items[item].tuple_hash);
			}
		}
	/* Every absent exact node was materialized as an empty bounded leaf above. */
	for (i = 0; i < count; i++)
	{
		Assert(results[i].found);
	}
	return results;
}

static uint32
dynamic_read_u32(const uint8 **cursor, const uint8 *end)
{
	const uint8 *p = *cursor;
	uint32 value;

	if (end - p < 4)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("truncated canonical dynamic Merkle key")));
	value = ((uint32) p[0] << 24) | ((uint32) p[1] << 16) |
		((uint32) p[2] << 8) | (uint32) p[3];
	*cursor += 4;
	return value;
}

static char *
dynamic_single_key_text(Relation indexRel, const bytea *key_data)
{
	const uint8 *cursor = (const uint8 *) VARDATA_ANY(key_data);
	const uint8 *end = cursor + VARSIZE_ANY_EXHDR(key_data);
	static const uint8 magic[] = {'A','R','I','A','R','O','U','T'};
	uint32 version;
	uint32 nkeys;
	uint32 attno;
	Oid type_oid;
	int32 typmod;
	uint8 null_flag;
	uint32 length;
	Oid receive;
	Oid typioparam;
	Oid output;
	bool typisvarlena;
	StringInfoData buffer;
	Datum value;
	char *result;

	if (end - cursor < (int) sizeof(magic) ||
		memcmp(cursor,magic,sizeof(magic)) != 0)
		return NULL;
	cursor += sizeof(magic);
	version = dynamic_read_u32(&cursor,end);
	nkeys = dynamic_read_u32(&cursor,end);
	if (version != MERKLE_ROUTE_FORMAT_VERSION || nkeys != 1)
		return NULL;
	attno = dynamic_read_u32(&cursor,end);
	type_oid = (Oid) dynamic_read_u32(&cursor,end);
	typmod = (int32) dynamic_read_u32(&cursor,end);
	if (cursor >= end)
		return NULL;
	null_flag = *cursor++;
	if (attno != 1 || null_flag != 0 ||
		type_oid != TupleDescAttr(RelationGetDescr(indexRel),0)->atttypid)
		return NULL;
	length = dynamic_read_u32(&cursor,end);
	if ((uint64) (end - cursor) != length)
		return NULL;
	getTypeBinaryInputInfo(type_oid,&receive,&typioparam);
	buffer.data = (char *) cursor;
	buffer.len = length;
	buffer.maxlen = length;
	buffer.cursor = 0;
	value = OidReceiveFunctionCall(receive,&buffer,typioparam,typmod);
	if (buffer.cursor != (int) length)
		return NULL;
	getTypeOutputInfo(type_oid,&output,&typisvarlena);
	result = OidOutputFunctionCall(output,value);
	return result;
}

PG_FUNCTION_INFO_V1(merkle_dynamic_get_partition_roots);
PG_FUNCTION_INFO_V1(merkle_dynamic_get_ranges);
PG_FUNCTION_INFO_V1(merkle_dynamic_get_range_items);
PG_FUNCTION_INFO_V1(merkle_dynamic_get_leaf_frontier);
PG_FUNCTION_INFO_V1(merkle_dynamic_tree_stats);
PG_FUNCTION_INFO_V1(merkle_dynamic_verify);

static void
dynamic_materialize_leaf_frontier_spi(const MerkleDynamicGeneration *gen,
							  Tuplestorestate *tupstore, TupleDesc tupdesc)
{
	Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
	Datum args[4];
	char nulls[4] = {' ',' ',' ',' '};
	int rc;
	int i;

	dynamic_generation_args(gen,args);
	rc = SPI_execute_with_args(
		"SELECT partition_id,prefix_len,prefix_bytes,tuple_count,data_xor,is_leaf "
		"FROM ariabc_internal.merkle_dynamic_node "
		"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
		"AND is_leaf ORDER BY partition_id,prefix_len,prefix_bytes",
		4,types,args,nulls,true,0);
	if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
		elog(ERROR, "dynamic Merkle leaf frontier query failed");
	for (i = 0; i < (int) SPI_processed; i++)
	{
		Datum out[6];
		bool outnulls[6] = {false,false,false,false,false,false};
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;

		out[0] = SPI_getbinval(tuple,desc,1,&isnull);
		out[1] = Int32GetDatum(DatumGetInt16(
			SPI_getbinval(tuple,desc,2,&isnull)));
		out[2] = PointerGetDatum(DatumGetByteaPCopy(
			SPI_getbinval(tuple,desc,3,&isnull)));
		out[3] = SPI_getbinval(tuple,desc,4,&isnull);
		out[4] = PointerGetDatum(DatumGetByteaPCopy(
			SPI_getbinval(tuple,desc,5,&isnull)));
		out[5] = BoolGetDatum(true);
		tuplestore_putvalues(tupstore,tupdesc,out,outnulls);
	}
}

Datum
merkle_dynamic_get_partition_roots(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid saved_userid;
	int saved_sec_context;
	Relation indexRel = NULL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = dynamic_begin_materialized_srf(fcinfo,&tupdesc);

	merkle_require_fresh();
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		MerkleDynamicGeneration gen;
		Oid types[4] = {OIDOID,OIDOID,OIDOID,OIDOID};
		Datum args[4];
		char nulls[4] = {' ',' ',' ',' '};
		int rc;
		int i;

		indexRel = dynamic_open_index_arg(relid,ShareLock);
		dynamic_read_meta(indexRel,&gen);
		dynamic_require_relations();
		dynamic_generation_args(&gen,args);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle roots SPI_connect failed");
		dynamic_validate_state_spi(&gen);
		rc = SPI_execute_with_args(
			"SELECT partition_id,prefix_len,prefix_bytes,tuple_count,data_xor,is_leaf "
			"FROM ariabc_internal.merkle_dynamic_node "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND prefix_len=0 ORDER BY partition_id",
			4,types,args,nulls,true,0);
		if (rc != SPI_OK_SELECT || SPI_processed != (uint64) gen.config.partitions)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("dynamic Merkle root set is incomplete")));
		for (i = 0; i < (int) SPI_processed; i++)
		{
			Datum out[6];
			bool outnulls[6] = {false,false,false,false,false,false};
			HeapTuple tuple = SPI_tuptable->vals[i];
			TupleDesc desc = SPI_tuptable->tupdesc;
			bool isnull;
			int partition_id;
			int prefix_len;
			bytea *prefix;

			partition_id = DatumGetInt32(
				SPI_getbinval(tuple,desc,1,&isnull));
			if (isnull || partition_id != i)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle partition roots are not canonical")));
			prefix_len = DatumGetInt16(
				SPI_getbinval(tuple,desc,2,&isnull));
			prefix = DatumGetByteaPP(
				SPI_getbinval(tuple,desc,3,&isnull));
			if (isnull || prefix_len != 0 ||
				VARSIZE_ANY_EXHDR(prefix) != MERKLE_HASH_BYTES ||
				!dynamic_bytes_are_zero((uint8 *) VARDATA_ANY(prefix),
					MERKLE_HASH_BYTES))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle partition root has an invalid prefix")));

			out[0] = Int32GetDatum(partition_id);
			out[1] = Int32GetDatum(prefix_len);
			out[2] = PointerGetDatum(DatumGetByteaPCopy(
				PointerGetDatum(prefix)));
			out[3] = SPI_getbinval(tuple,desc,4,&isnull);
			out[4] = PointerGetDatum(DatumGetByteaPCopy(SPI_getbinval(tuple,desc,5,&isnull)));
			out[5] = SPI_getbinval(tuple,desc,6,&isnull);
			tuplestore_putvalues(tupstore,tupdesc,out,outnulls);
		}
		SPI_finish();
		index_close(indexRel,ShareLock);
		indexRel = NULL;
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

Datum
merkle_dynamic_get_leaf_frontier(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid saved_userid;
	int saved_sec_context;
	Relation indexRel = NULL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = dynamic_begin_materialized_srf(fcinfo,&tupdesc);

	merkle_require_fresh();
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		MerkleDynamicGeneration gen;

		indexRel = dynamic_open_index_arg(relid,ShareLock);
		dynamic_read_meta(indexRel,&gen);
		dynamic_require_relations();
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle leaf-frontier SPI_connect failed");
		dynamic_validate_state_spi(&gen);
		dynamic_materialize_leaf_frontier_spi(&gen,tupstore,tupdesc);
		SPI_finish();
		index_close(indexRel,ShareLock);
		indexRel = NULL;
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

Datum
merkle_dynamic_get_ranges(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	bool all_leaves = PG_ARGISNULL(1);
	Jsonb *json = all_leaves ? NULL : PG_GETARG_JSONB_P(1);
	Oid saved_userid;
	int saved_sec_context;
	Relation indexRel = NULL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = dynamic_begin_materialized_srf(fcinfo,&tupdesc);

	merkle_require_fresh();
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		MerkleDynamicGeneration gen;

		indexRel = dynamic_open_index_arg(relid,ShareLock);
		dynamic_read_meta(indexRel,&gen);
		dynamic_require_relations();
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle ranges SPI_connect failed");
		dynamic_validate_state_spi(&gen);
		if (all_leaves)
			dynamic_materialize_leaf_frontier_spi(&gen,tupstore,tupdesc);
		else
		{
			MerkleDynamicRequest *requests;
			MerkleDynamicNodeData *summaries;
			int count;
			int i;

			requests = dynamic_parse_requests_spi(json,&count);
			summaries = dynamic_range_summaries_spi(&gen,requests,count);
			for (i = 0; i < count; i++)
			{
				Datum out[6];
				bool outnulls[6] = {false,false,false,false,false,false};

				out[0] = Int32GetDatum(requests[i].partition_id);
				out[1] = Int32GetDatum(requests[i].prefix_len);
				out[2] = PointerGetDatum(dynamic_bytea(requests[i].prefix,
					MERKLE_HASH_BYTES));
				out[3] = Int64GetDatum((int64) summaries[i].tuple_count);
				out[4] = PointerGetDatum(dynamic_bytea(summaries[i].data_xor.data,
					MERKLE_HASH_BYTES));
				out[5] = BoolGetDatum(summaries[i].is_leaf);
				tuplestore_putvalues(tupstore,tupdesc,out,outnulls);
			}
		}
		SPI_finish();
		index_close(indexRel,ShareLock);
		indexRel = NULL;
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

Datum
merkle_dynamic_get_range_items(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Jsonb *json;
	Oid saved_userid;
	int saved_sec_context;
	Relation indexRel = NULL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = dynamic_begin_materialized_srf(fcinfo,&tupdesc);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dynamic Merkle range-items request cannot be null")));
	json = PG_GETARG_JSONB_P(1);
	merkle_require_fresh();
	GetUserIdAndSecContext(&saved_userid,&saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
		saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		MerkleDynamicGeneration gen;
		MerkleDynamicRequest *requests;
		MerkleDynamicNodeData *summaries;
		int request_count;
		int r;

		indexRel = dynamic_open_index_arg(relid,ShareLock);
		dynamic_read_meta(indexRel,&gen);
		dynamic_require_relations();
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "dynamic Merkle range-items SPI_connect failed");
		dynamic_validate_state_spi(&gen);
		requests = dynamic_parse_requests_spi(json,&request_count);
		summaries = dynamic_range_summaries_spi(&gen,requests,request_count);
		for (r = 0; r < request_count; r++)
		{
			MerkleDynamicLoadedItem *items;
			int item_count;
			uint64 bytes = 0;
			int i;

			if (summaries[r].tuple_count > (uint64) gen.config.leaf_capacity ||
				summaries[r].subtree_bytes > (uint64) gen.config.leaf_byte_capacity)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("requested dynamic Merkle range is not bounded"),
						 errhint("Descend the logical range before requesting item summaries.")));
			items = dynamic_load_range_items_spi(&gen,
				requests[r].partition_id,
				requests[r].prefix_len,requests[r].prefix,&item_count);
			if ((uint64) item_count != summaries[r].tuple_count)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle range item count changed during read")));
			for (i = 0; i < item_count; i++)
			{
				Datum out[7];
				bool outnulls[7] = {false,false,false,false,false,false,false};
				char *key_text = dynamic_single_key_text(indexRel,items[i].key_data);

				bytes += items[i].item_bytes;
				out[0] = Int32GetDatum(requests[r].partition_id);
				out[1] = Int32GetDatum(requests[r].prefix_len);
				out[2] = PointerGetDatum(dynamic_bytea(requests[r].prefix,
					MERKLE_HASH_BYTES));
				out[3] = PointerGetDatum(items[i].key_data);
				if (key_text == NULL)
				{
					out[4] = (Datum) 0;
					outnulls[4] = true;
				}
				else
					out[4] = CStringGetTextDatum(key_text);
				out[5] = PointerGetDatum(dynamic_bytea(items[i].route_digest,
					MERKLE_HASH_BYTES));
				out[6] = PointerGetDatum(dynamic_bytea(items[i].tuple_hash.data,
					MERKLE_HASH_BYTES));
				tuplestore_putvalues(tupstore,tupdesc,out,outnulls);
			}
			if (bytes != summaries[r].subtree_bytes)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("dynamic Merkle range byte summary is inconsistent")));
		}
		SPI_finish();
		index_close(indexRel,ShareLock);
		indexRel = NULL;
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid,saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid,saved_sec_context);
	tuplestore_donestoring(tupstore);
	PG_RETURN_NULL();
}

Datum
merkle_dynamic_tree_stats(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Relation indexRel;
	char *json;
	Datum result;

	merkle_require_fresh();
	indexRel = dynamic_open_index_arg(relid,ShareLock);
	json = merkle_dynamic_stats_json(indexRel);
	index_close(indexRel,ShareLock);
	result = DirectFunctionCall1(jsonb_in,CStringGetDatum(json));
	pfree(json);
	PG_RETURN_DATUM(result);
}

Datum
merkle_dynamic_verify(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid index_oid = InvalidOid;
	Oid heap_oid;
	Relation heapRel;
	Relation indexRel;
	bool match;
	char relkind;

	merkle_require_fresh();
	relkind = get_rel_relkind(relid);
	if (relkind == RELKIND_INDEX)
	{
		index_oid = relid;
		heap_oid = IndexGetRelation(index_oid,false);
		heapRel = table_open(heap_oid,ShareLock);
	}
	else if (relkind == RELKIND_RELATION ||
			 relkind == RELKIND_PARTITIONED_TABLE)
	{
		List *indexes;
		ListCell *cell;

		heap_oid = relid;
		heapRel = table_open(heap_oid,ShareLock);
		indexes = RelationGetIndexList(heapRel);
		foreach(cell,indexes)
		{
			Oid candidate_oid = lfirst_oid(cell);
			Relation candidate = index_open(candidate_oid,AccessShareLock);

			if (candidate->rd_rel->relam == MERKLE_AM_OID &&
				merkle_index_is_dynamic(candidate))
			{
				if (OidIsValid(index_oid))
				{
					index_close(candidate,AccessShareLock);
					list_free(indexes);
					table_close(heapRel,ShareLock);
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("relation has multiple dynamic Merkle indexes")));
				}
				index_oid = candidate_oid;
			}
			index_close(candidate,AccessShareLock);
		}
		list_free(indexes);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation is not a table or index")));
	if (!OidIsValid(index_oid))
	{
		table_close(heapRel,ShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("relation has no dynamic Merkle index")));
	}
	indexRel = index_open(index_oid,ShareLock);
	if (indexRel->rd_rel->relam != MERKLE_AM_OID ||
		!merkle_index_is_dynamic(indexRel) ||
		indexRel->rd_index->indrelid != heap_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation is not a dynamic Merkle index")));
	match = merkle_dynamic_verify_relations(heapRel,indexRel,GetLatestSnapshot());
	index_close(indexRel,ShareLock);
	table_close(heapRel,ShareLock);
	if (!match)
	{
		char *reason = psprintf("Dynamic Merkle verification mismatch for index %u",
			index_oid);

		merkle_mark_recovery_state(MERKLE_STATE_INVALID,reason);
		pfree(reason);
	}
	PG_RETURN_BOOL(match);
}
