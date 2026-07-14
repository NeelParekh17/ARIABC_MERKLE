/*-------------------------------------------------------------------------
 *
 * merkledelta.c
 *    Transaction-local Merkle delta staging and durable local enqueue.
 *
 * Physical Merkle pages are deliberately not modified by user transactions.
 * Each transaction aggregates XOR deltas by index/leaf and persists one
 * compact binary blob.  The ordered applier in merkleapply.c is the only
 * normal-runtime v7 page mutator.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/merkle.h"
#include "access/xact.h"
#include "bcdb/globals.h"
#include "bcdb/shm_transaction.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct MerkleDeltaKey
{
	Oid			index_oid;
	RelFileNode index_rnode;
	int32		leaf_id;
} MerkleDeltaKey;

typedef struct MerkleDeltaEntry
{
	MerkleDeltaKey key;
	MerkleHash	xor_delta;
} MerkleDeltaEntry;

/*
 * A dynamic entry is keyed by the complete route digest.  The canonical key
 * bytes are retained as a collision check and as the durable repair identity.
 * If two different canonical keys ever produce the same BLAKE3-256 route
 * digest, fail closed instead of silently merging them.
 */
typedef struct MerkleDynamicDeltaKey
{
	Oid			index_oid;
	RelFileNode index_rnode;
	uint8		route_digest[MERKLE_HASH_BYTES];
} MerkleDynamicDeltaKey;

typedef struct MerkleDynamicDeltaEntry
{
	MerkleDynamicDeltaKey key;
	int32		partition_id;
	bytea	   *key_data;
	bool		has_old;
	bool		has_new;
	MerkleHash	old_hash;
	MerkleHash	new_hash;
} MerkleDynamicDeltaEntry;

typedef struct MerkleSubxactFrame
{
	SubTransactionId subxid;
	MemoryContext context;
	HTAB	   *entries;
	HTAB	   *dynamic_entries;
	struct MerkleSubxactFrame *next;
} MerkleSubxactFrame;

typedef enum MerkleSerializedEntryKind
{
	MERKLE_SERIALIZED_STATIC = 1,
	MERKLE_SERIALIZED_DYNAMIC = 2
} MerkleSerializedEntryKind;

typedef struct MerkleSerializedEntry
{
	MerkleSerializedEntryKind kind;
	union
	{
		MerkleDeltaEntry static_entry;
		MerkleDynamicDeltaEntry dynamic_entry;
	} value;
} MerkleSerializedEntry;

#define MERKLE_DELTA_V2_FLAG_HAS_OLD       (1U << 0)
#define MERKLE_DELTA_V2_FLAG_HAS_NEW       (1U << 1)

static MerkleSubxactFrame *merkle_delta_frames = NULL;
static bool merkle_delta_callbacks_registered = false;
static bool merkle_staged_delta_persisted = false;
static uint64 merkle_serialized_entry_count = 0;
static uint64 merkle_delta_generation = 0;
static uint64 merkle_serialized_generation = 0;

static void merkle_delta_xact_callback(XactEvent event, void *arg);
static void merkle_delta_subxact_callback(SubXactEvent event,
										  SubTransactionId mySubid,
										  SubTransactionId parentSubid,
										  void *arg);

void
merkle_crash_failpoint(const char *name)
{
	const char *configured = getenv("ARIABC_MERKLE_FAILPOINT");
	const char *action = getenv("ARIABC_MERKLE_FAILPOINT_ACTION");

	if (configured == NULL || strcmp(configured, name) != 0)
		return;

	elog(LOG, "MERKLE_FAILPOINT_REACHED name=%s pid=%d", name, MyProcPid);
	if (action != NULL && strcmp(action, "backend_kill") == 0)
		(void) kill(MyProcPid, SIGKILL);
	else if (action != NULL && strcmp(action, "postmaster_kill") == 0)
	{
		(void) kill(PostmasterPid, SIGKILL);
		(void) kill(MyProcPid, SIGKILL);
	}
}

static HTAB *
merkle_delta_create_map(MemoryContext context)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MerkleDeltaKey);
	ctl.entrysize = sizeof(MerkleDeltaEntry);
	ctl.hcxt = context;

	return hash_create("Merkle transaction deltas", 32, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
merkle_dynamic_delta_create_map(MemoryContext context)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MerkleDynamicDeltaKey);
	ctl.entrysize = sizeof(MerkleDynamicDeltaEntry);
	ctl.hcxt = context;

	return hash_create("Dynamic Merkle transaction deltas", 32, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static MerkleSubxactFrame *
merkle_delta_find_frame(SubTransactionId subxid)
{
	MerkleSubxactFrame *frame;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		if (frame->subxid == subxid)
			return frame;
	return NULL;
}

static MerkleSubxactFrame *
merkle_delta_get_frame(SubTransactionId subxid, bool create)
{
	MerkleSubxactFrame *frame = merkle_delta_find_frame(subxid);
	MemoryContext old_context;

	if (frame != NULL || !create)
		return frame;

	old_context = MemoryContextSwitchTo(TopTransactionContext);
	frame = palloc0(sizeof(*frame));
	frame->subxid = subxid;
	frame->context = AllocSetContextCreate(TopTransactionContext,
										 "Merkle subtransaction deltas",
										 ALLOCSET_SMALL_SIZES);
	frame->entries = merkle_delta_create_map(frame->context);
	frame->dynamic_entries =
		merkle_dynamic_delta_create_map(frame->context);
	frame->next = merkle_delta_frames;
	merkle_delta_frames = frame;
	MemoryContextSwitchTo(old_context);

	return frame;
}

static bool
merkle_hash_equal(const MerkleHash *left, const MerkleHash *right)
{
	return memcmp(left->data, right->data, MERKLE_HASH_BYTES) == 0;
}

static bool
merkle_bytea_equal(const bytea *left, const bytea *right)
{
	Size left_len;
	Size right_len;

	if (left == NULL || right == NULL)
		return left == right;
	left_len = VARSIZE_ANY_EXHDR(left);
	right_len = VARSIZE_ANY_EXHDR(right);
	return left_len == right_len &&
		memcmp(VARDATA_ANY(left), VARDATA_ANY(right), left_len) == 0;
}

static bytea *
merkle_copy_bytea(MemoryContext context, const bytea *source)
{
	bytea *copy;
	MemoryContext old_context;
	Size size;

	if (source == NULL)
		return NULL;
	size = VARSIZE_ANY(source);
	if (!AllocSizeIsValid(size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("dynamic Merkle key is too large")));
	old_context = MemoryContextSwitchTo(context);
	copy = palloc(size);
	memcpy(copy, source, size);
	MemoryContextSwitchTo(old_context);
	return copy;
}

/* Compose source after target: target.old -> target.new -> source.new. */
static void
merkle_dynamic_delta_compose(MerkleDynamicDeltaEntry *target,
							 const MerkleDynamicDeltaEntry *source,
							 MemoryContext context)
{
	if (!merkle_bytea_equal(target->key_data, source->key_data))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle route-digest collision detected"),
				 errdetail("Two distinct canonical keys produced the same 256-bit route digest.")));
	if (target->partition_id != source->partition_id)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("dynamic Merkle key changed partition during one transaction")));
	if (target->has_new != source->has_old ||
		(target->has_new &&
		 !merkle_hash_equal(&target->new_hash, &source->old_hash)))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("non-contiguous dynamic Merkle transitions for one key"),
				 errdetail("The staged row image before a mutation does not match the prior staged row image after the preceding mutation.")));

	target->has_new = source->has_new;
	if (source->has_new)
		target->new_hash = source->new_hash;
	else
		merkle_hash_zero(&target->new_hash);

	/* A net identity transition carries no durable work. */
	if (target->has_old && target->has_new &&
		merkle_hash_equal(&target->old_hash, &target->new_hash))
	{
		target->has_old = false;
		target->has_new = false;
		merkle_hash_zero(&target->old_hash);
		merkle_hash_zero(&target->new_hash);
	}

	(void) context;
}

static void
merkle_dynamic_delta_merge_one(HTAB *target_map,
							   const MerkleDynamicDeltaEntry *source,
							   MemoryContext context)
{
	MerkleDynamicDeltaEntry *target;
	bool found;

	target = hash_search(target_map, &source->key, HASH_ENTER, &found);
	if (!found)
	{
		*target = *source;
		target->key_data = merkle_copy_bytea(context, source->key_data);
	}
	else
		merkle_dynamic_delta_compose(target, source, context);

	if (!target->has_old && !target->has_new)
	{
		if (target->key_data != NULL)
			pfree(target->key_data);
		(void) hash_search(target_map, &source->key, HASH_REMOVE, NULL);
	}
}

static void
merkle_delta_merge_one(HTAB *target, const MerkleDeltaEntry *source)
{
	MerkleDeltaEntry *entry;
	bool		found;

	entry = hash_search(target, &source->key, HASH_ENTER, &found);
	if (!found)
	{
		entry->key = source->key;
		merkle_hash_zero(&entry->xor_delta);
	}
	merkle_hash_xor(&entry->xor_delta, &source->xor_delta);
	if (merkle_hash_is_zero(&entry->xor_delta))
		(void) hash_search(target, &source->key, HASH_REMOVE, NULL);
}

static void
merkle_delta_unlink_frame(MerkleSubxactFrame *frame)
{
	MerkleSubxactFrame **link;

	for (link = &merkle_delta_frames; *link != NULL; link = &(*link)->next)
	{
		if (*link == frame)
		{
			*link = frame->next;
			MemoryContextDelete(frame->context);
			pfree(frame);
			return;
		}
	}
}

static void
merkle_delta_reset(void)
{
	while (merkle_delta_frames != NULL)
		merkle_delta_unlink_frame(merkle_delta_frames);
	merkle_staged_delta_persisted = false;
	merkle_serialized_entry_count = 0;
	merkle_delta_generation = 0;
	merkle_serialized_generation = 0;
}

static void
merkle_delta_register_callbacks(void)
{
	if (merkle_delta_callbacks_registered)
		return;

	RegisterXactCallback(merkle_delta_xact_callback, NULL);
	RegisterSubXactCallback(merkle_delta_subxact_callback, NULL);
	merkle_delta_callbacks_registered = true;
}

void
merkle_stage_delta(Relation indexRel, int leafId, const MerkleHash *hash)
{
	MerkleSubxactFrame *frame;
	MerkleDeltaKey key;
	MerkleDeltaEntry *entry;
	bool		found;

	if (indexRel == NULL || hash == NULL || merkle_hash_is_zero(hash))
		return;
	if (merkle_staged_delta_persisted)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("Merkle data changes are not allowed after durable delta finalization"),
				errhint("Terminalize the transaction only after all table changes are complete.")));
	merkle_delta_generation++;

	merkle_delta_register_callbacks();
	frame = merkle_delta_get_frame(GetCurrentSubTransactionId(), true);

	MemSet(&key, 0, sizeof(key));
	key.index_oid = RelationGetRelid(indexRel);
	key.index_rnode = indexRel->rd_node;
	key.leaf_id = leafId;

	entry = hash_search(frame->entries, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->key = key;
		merkle_hash_zero(&entry->xor_delta);
	}
	merkle_hash_xor(&entry->xor_delta, hash);
	if (merkle_hash_is_zero(&entry->xor_delta))
		(void) hash_search(frame->entries, &key, HASH_REMOVE, NULL);

	merkle_crash_failpoint("after_merkle_delta_staged");
}

void
merkle_stage_item_delta(Relation indexRel,
						const MerkleItemIdentity *identity,
						const MerkleHash *hash, bool is_insert)
{
	MerkleSubxactFrame *frame;
	MerkleDynamicDeltaEntry source;
	MerkleDynamicDeltaEntry *entry;

	if (indexRel == NULL || identity == NULL || hash == NULL)
		return;
	if (!merkle_index_is_dynamic(indexRel))
	{
		/* Static XOR deltas historically use zero as the no-op sentinel.  A
		 * dynamic item has an independent presence flag and tuple count, so its
		 * (cryptographically possible) all-zero row digest is still real data. */
		if (merkle_hash_is_zero(hash))
			return;
		merkle_update_tree_path(indexRel, identity->route.leaf_id,
							(MerkleHash *) hash, is_insert);
		return;
	}
	if (identity->key_data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dynamic Merkle item identity has no canonical key")));
	if (merkle_staged_delta_persisted)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("Merkle data changes are not allowed after durable delta finalization"),
				 errhint("Terminalize the transaction only after all table changes are complete.")));

	merkle_delta_generation++;
	merkle_delta_register_callbacks();
	frame = merkle_delta_get_frame(GetCurrentSubTransactionId(), true);

	MemSet(&source, 0, sizeof(source));
	source.key.index_oid = RelationGetRelid(indexRel);
	source.key.index_rnode = indexRel->rd_node;
	memcpy(source.key.route_digest, identity->route.route_digest,
		   MERKLE_HASH_BYTES);
	source.partition_id = identity->route.partition_id;
	source.key_data = identity->key_data;
	if (is_insert)
	{
		source.has_new = true;
		source.new_hash = *hash;
	}
	else
	{
		source.has_old = true;
		source.old_hash = *hash;
	}

	entry = hash_search(frame->dynamic_entries, &source.key, HASH_FIND, NULL);
	if (entry == NULL)
		merkle_dynamic_delta_merge_one(frame->dynamic_entries, &source,
								   frame->context);
	else
	{
		merkle_dynamic_delta_compose(entry, &source, frame->context);
		if (!entry->has_old && !entry->has_new)
		{
			if (entry->key_data != NULL)
				pfree(entry->key_data);
			(void) hash_search(frame->dynamic_entries, &source.key,
							  HASH_REMOVE, NULL);
		}
	}

	merkle_crash_failpoint("after_merkle_dynamic_delta_staged");
}

bool
merkle_has_staged_delta(void)
{
	MerkleSubxactFrame *frame;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		if (hash_get_num_entries(frame->entries) > 0 ||
			hash_get_num_entries(frame->dynamic_entries) > 0)
			return true;
	return false;
}

static uint64
merkle_staged_entry_count(void)
{
	MerkleSubxactFrame *frame;
	uint64 count = 0;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
		count += (uint64) hash_get_num_entries(frame->entries) +
			(uint64) hash_get_num_entries(frame->dynamic_entries);
	return count;
}

static Oid
merkle_serialized_index_oid(const MerkleSerializedEntry *entry)
{
	return entry->kind == MERKLE_SERIALIZED_STATIC ?
		entry->value.static_entry.key.index_oid :
		entry->value.dynamic_entry.key.index_oid;
}

static const RelFileNode *
merkle_serialized_rnode(const MerkleSerializedEntry *entry)
{
	return entry->kind == MERKLE_SERIALIZED_STATIC ?
		&entry->value.static_entry.key.index_rnode :
		&entry->value.dynamic_entry.key.index_rnode;
}

static int
merkle_serialized_entry_cmp(const void *left, const void *right)
{
	const MerkleSerializedEntry *a = (const MerkleSerializedEntry *) left;
	const MerkleSerializedEntry *b = (const MerkleSerializedEntry *) right;
	Oid a_oid = merkle_serialized_index_oid(a);
	Oid b_oid = merkle_serialized_index_oid(b);
	const RelFileNode *a_rnode = merkle_serialized_rnode(a);
	const RelFileNode *b_rnode = merkle_serialized_rnode(b);

	if (a_oid != b_oid)
		return a_oid < b_oid ? -1 : 1;
	if (a_rnode->spcNode != b_rnode->spcNode)
		return a_rnode->spcNode < b_rnode->spcNode ? -1 : 1;
	if (a_rnode->dbNode != b_rnode->dbNode)
		return a_rnode->dbNode < b_rnode->dbNode ? -1 : 1;
	if (a_rnode->relNode != b_rnode->relNode)
		return a_rnode->relNode < b_rnode->relNode ? -1 : 1;
	if (a->kind != b->kind)
		return a->kind < b->kind ? -1 : 1;
	if (a->kind == MERKLE_SERIALIZED_STATIC)
	{
		int32 a_leaf = a->value.static_entry.key.leaf_id;
		int32 b_leaf = b->value.static_entry.key.leaf_id;

		if (a_leaf != b_leaf)
			return a_leaf < b_leaf ? -1 : 1;
		return 0;
	}
	else
	{
		const MerkleDynamicDeltaEntry *ad = &a->value.dynamic_entry;
		const MerkleDynamicDeltaEntry *bd = &b->value.dynamic_entry;
		int cmp;
		Size a_len;
		Size b_len;

		if (ad->partition_id != bd->partition_id)
			return ad->partition_id < bd->partition_id ? -1 : 1;
		cmp = memcmp(ad->key.route_digest, bd->key.route_digest,
					 MERKLE_HASH_BYTES);
		if (cmp != 0)
			return cmp;
		a_len = VARSIZE_ANY_EXHDR(ad->key_data);
		b_len = VARSIZE_ANY_EXHDR(bd->key_data);
		cmp = memcmp(VARDATA_ANY(ad->key_data), VARDATA_ANY(bd->key_data),
					 Min(a_len, b_len));
		if (cmp != 0)
			return cmp;
		if (a_len != b_len)
			return a_len < b_len ? -1 : 1;
		return 0;
	}
}

static void
merkle_delta_put_u32(char *dst, uint32 value)
{
	value = pg_hton32(value);
	memcpy(dst, &value, sizeof(value));
}

static void
merkle_delta_put_u64(char *dst, uint64 value)
{
	value = pg_hton64(value);
	memcpy(dst, &value, sizeof(value));
}

bytea *
merkle_serialize_staged_delta(uint64 raft_log_index, uint32 item_ordinal,
							  int *delta_version)
{
	MemoryContext caller_context;
	MemoryContext old_context;
	MemoryContext serialize_context;
	HTAB	   *combined;
	HTAB	   *dynamic_combined;
	MerkleSubxactFrame *frame;
	HASH_SEQ_STATUS seq;
	MerkleDeltaEntry *entry;
	MerkleDynamicDeltaEntry *dynamic_entry;
	MerkleSerializedEntry *sorted;
	long		count;
	long		i = 0;
	Size		payload_bytes;
	Size		total_bytes;
	bytea	   *result;
	char	   *header;
	char	   *payload;
	char		crc_header[MERKLE_DELTA_HEADER_BYTES];
	pg_crc32c	crc;
	bool		use_v2;
	uint32		wire_version;

	if (delta_version != NULL)
		*delta_version = 0;
	if (!merkle_has_staged_delta())
		return NULL;

	caller_context = CurrentMemoryContext;
	serialize_context = AllocSetContextCreate(caller_context,
											"Merkle delta serialization",
											ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(serialize_context);
	combined = merkle_delta_create_map(serialize_context);
	dynamic_combined = merkle_dynamic_delta_create_map(serialize_context);
	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		hash_seq_init(&seq, frame->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(combined, entry);
		hash_seq_init(&seq, frame->dynamic_entries);
		while ((dynamic_entry = hash_seq_search(&seq)) != NULL)
			merkle_dynamic_delta_merge_one(dynamic_combined, dynamic_entry,
								   serialize_context);
	}

	count = hash_get_num_entries(combined) +
		hash_get_num_entries(dynamic_combined);
	if (count <= 0)
	{
		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(serialize_context);
		return NULL;
	}
	use_v2 = hash_get_num_entries(dynamic_combined) > 0;
	wire_version = use_v2 ? MERKLE_DELTA_VERSION :
		MERKLE_DELTA_LEGACY_VERSION;

	sorted = palloc(sizeof(*sorted) * count);
	hash_seq_init(&seq, combined);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		sorted[i].kind = MERKLE_SERIALIZED_STATIC;
		sorted[i].value.static_entry = *entry;
		i++;
	}
	hash_seq_init(&seq, dynamic_combined);
	while ((dynamic_entry = hash_seq_search(&seq)) != NULL)
	{
		sorted[i].kind = MERKLE_SERIALIZED_DYNAMIC;
		sorted[i].value.dynamic_entry = *dynamic_entry;
		i++;
	}
	Assert(i == count);
	qsort(sorted, count, sizeof(*sorted), merkle_serialized_entry_cmp);

	if (!use_v2)
		payload_bytes = (Size) count * MERKLE_DELTA_ENTRY_BYTES;
	else
	{
		payload_bytes = 0;
		for (i = 0; i < count; i++)
		{
			Size key_len = 0;

			if (sorted[i].kind == MERKLE_SERIALIZED_DYNAMIC)
				key_len = VARSIZE_ANY_EXHDR(
					sorted[i].value.dynamic_entry.key_data);
			if (key_len > PG_UINT32_MAX - MERKLE_DELTA_V2_ENTRY_FIXED_BYTES ||
				payload_bytes > PG_UINT32_MAX -
				(MERKLE_DELTA_V2_ENTRY_FIXED_BYTES + key_len))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("Merkle delta batch is too large")));
			payload_bytes += MERKLE_DELTA_V2_ENTRY_FIXED_BYTES + key_len;
		}
	}
	total_bytes = VARHDRSZ + MERKLE_DELTA_HEADER_BYTES + payload_bytes;
	if (!AllocSizeIsValid(total_bytes) || payload_bytes > PG_UINT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Merkle delta batch is too large")));

	result = MemoryContextAllocZero(caller_context, total_bytes);
	SET_VARSIZE(result, total_bytes);
	header = VARDATA(result);
	payload = header + MERKLE_DELTA_HEADER_BYTES;

	if (!use_v2)
	{
		for (i = 0; i < count; i++)
		{
			const MerkleDeltaEntry *static_entry;
			char *dst = payload + ((Size) i * MERKLE_DELTA_ENTRY_BYTES);

			Assert(sorted[i].kind == MERKLE_SERIALIZED_STATIC);
			static_entry = &sorted[i].value.static_entry;
			merkle_delta_put_u32(dst + 0, static_entry->key.index_oid);
			merkle_delta_put_u32(dst + 4,
							 static_entry->key.index_rnode.spcNode);
			merkle_delta_put_u32(dst + 8,
							 static_entry->key.index_rnode.dbNode);
			merkle_delta_put_u32(dst + 12,
							 static_entry->key.index_rnode.relNode);
			merkle_delta_put_u32(dst + 16,
							 (uint32) static_entry->key.leaf_id);
			merkle_delta_put_u32(dst + 20, MERKLE_VERSION);
			memcpy(dst + 24, static_entry->xor_delta.data,
				   MERKLE_HASH_BYTES);
		}
	}
	else
	{
		Size payload_offset = 0;

	for (i = 0; i < count; i++)
	{
		const RelFileNode *rnode = merkle_serialized_rnode(&sorted[i]);
		Oid index_oid = merkle_serialized_index_oid(&sorted[i]);
		Size key_len = 0;
		Size entry_bytes;
		uint32 target;
		uint32 entry_flags = 0;
		char *dst;

		if (sorted[i].kind == MERKLE_SERIALIZED_DYNAMIC)
			key_len = VARSIZE_ANY_EXHDR(
				sorted[i].value.dynamic_entry.key_data);
		entry_bytes = MERKLE_DELTA_V2_ENTRY_FIXED_BYTES + key_len;
		dst = payload + payload_offset;
		target = sorted[i].kind == MERKLE_SERIALIZED_STATIC ?
			(uint32) sorted[i].value.static_entry.key.leaf_id :
			(uint32) sorted[i].value.dynamic_entry.partition_id;
		if (sorted[i].kind == MERKLE_SERIALIZED_DYNAMIC)
		{
			if (sorted[i].value.dynamic_entry.has_old)
				entry_flags |= MERKLE_DELTA_V2_FLAG_HAS_OLD;
			if (sorted[i].value.dynamic_entry.has_new)
				entry_flags |= MERKLE_DELTA_V2_FLAG_HAS_NEW;
		}

		merkle_delta_put_u32(dst + 0, (uint32) entry_bytes);
		merkle_delta_put_u32(dst + 4, (uint32) sorted[i].kind);
		merkle_delta_put_u32(dst + 8, index_oid);
		merkle_delta_put_u32(dst + 12, rnode->spcNode);
		merkle_delta_put_u32(dst + 16, rnode->dbNode);
		merkle_delta_put_u32(dst + 20, rnode->relNode);
		merkle_delta_put_u32(dst + 24, MERKLE_VERSION);
		merkle_delta_put_u32(dst + 28, target);
		merkle_delta_put_u32(dst + 32, entry_flags);
		merkle_delta_put_u32(dst + 36, (uint32) key_len);
		if (sorted[i].kind == MERKLE_SERIALIZED_STATIC)
			memcpy(dst + 136, sorted[i].value.static_entry.xor_delta.data,
				   MERKLE_HASH_BYTES);
		else
		{
			const MerkleDynamicDeltaEntry *dynamic =
				&sorted[i].value.dynamic_entry;

			memcpy(dst + 40, dynamic->key.route_digest,
				   MERKLE_HASH_BYTES);
			if (dynamic->has_old)
				memcpy(dst + 72, dynamic->old_hash.data,
					   MERKLE_HASH_BYTES);
			if (dynamic->has_new)
				memcpy(dst + 104, dynamic->new_hash.data,
					   MERKLE_HASH_BYTES);
			if (key_len > 0)
				memcpy(dst + MERKLE_DELTA_V2_ENTRY_FIXED_BYTES,
					   VARDATA_ANY(dynamic->key_data), key_len);
		}
		payload_offset += entry_bytes;
	}
	Assert(payload_offset == payload_bytes);
	}

	merkle_delta_put_u32(header + 0, MERKLE_DELTA_MAGIC);
	merkle_delta_put_u32(header + 4, wire_version);
	merkle_delta_put_u32(header + 8, raft_log_index != 0 ? 1 : 0);
	merkle_delta_put_u32(header + 12, (uint32) count);
	merkle_delta_put_u32(header + 16, (uint32) payload_bytes);
	merkle_delta_put_u64(header + 24, raft_log_index);
	merkle_delta_put_u32(header + 32, item_ordinal);
	merkle_delta_put_u32(header + 36, 0);
	memcpy(crc_header, header, MERKLE_DELTA_HEADER_BYTES);
	memset(crc_header + 20, 0, sizeof(uint32));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, crc_header, sizeof(crc_header));
	COMP_CRC32C(crc, payload, payload_bytes);
	FIN_CRC32C(crc);
	merkle_delta_put_u32(header + 20, (uint32) crc);
	merkle_serialized_entry_count = (uint64) count;
	merkle_serialized_generation = merkle_delta_generation;
	if (delta_version != NULL)
		*delta_version = (int) wire_version;

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(serialize_context);
	return result;
}

void
merkle_mark_staged_delta_persisted(void)
{
	if (!merkle_has_staged_delta())
		return;
	if (merkle_serialized_entry_count == 0 ||
		merkle_serialized_generation != merkle_delta_generation)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Merkle delta changed after terminal serialization"),
				 errdetail("serialized_generation=%llu current_generation=%llu staged_entries=%llu",
						   (unsigned long long) merkle_serialized_generation,
						   (unsigned long long) merkle_delta_generation,
						   (unsigned long long) merkle_staged_entry_count())));
	merkle_staged_delta_persisted = true;
}

static void
merkle_persist_local_delta_impl(void)
{
	bytea	   *blob;
	int			delta_version;
	int			spi_rc;
	bool		isnull;
	Datum		seq_datum;
	uint64		apply_seq;
	bool		deterministic_bcdb_seq;
	Oid			argtypes[3] = {INT8OID, INT4OID, BYTEAOID};
	Datum		values[3];
	char		nulls[3] = {' ', ' ', ' '};
	bool		pushed_snapshot = false;

	if (!merkle_has_staged_delta())
		return;

	if (is_bcdb_worker && activeTx != NULL && activeTx->raft_ledger_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("safe-ledger transaction reached PRE_COMMIT with an unpersisted Merkle delta"),
				 errdetail("raft_log_index=%llu item_ordinal=%u",
						   (unsigned long long) activeTx->raft_log_index,
						   (unsigned) activeTx->raft_item_ordinal)));
	deterministic_bcdb_seq = enable_merkle_index && is_bcdb_worker &&
		activeTx != NULL && !activeTx->raft_ledger_enabled &&
		activeTx->tx_id != BCDBInvalidTid;
	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle local delta SPI_connect failed: %d", spi_rc);

	if (deterministic_bcdb_seq)
	{
		/*
		 * Direct DET workers must never contend on the singleton SQL allocator
		 * while holding user-row locks.  Their transaction id is already a
		 * replica-agreed total order and gives every writer a distinct key.
		 * Read-only txids intentionally leave holes; merkle_apply_until_impl()
		 * may cross only holes proven terminal by last_committed_tx_id.
		 */
		if (activeTx->tx_id < 0 || activeTx->tx_id == PG_INT32_MAX)
			elog(ERROR, "BCDB transaction id cannot be represented as a Merkle sequence");
		apply_seq = (uint64) activeTx->tx_id + 1;
	}
	else
	{
		spi_rc = SPI_execute(
			"UPDATE ariabc_internal.merkle_apply_counter"
			"   SET next_seq = next_seq + 1"
			" WHERE singleton"
			" RETURNING next_seq",
			false, 1);
		if (spi_rc != SPI_OK_UPDATE_RETURNING || SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("Merkle crash-safety state is not initialized"),
					 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));

		seq_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								  1, &isnull);
		if (isnull || DatumGetInt64(seq_datum) <= 0)
			elog(ERROR, "Merkle apply counter returned an invalid sequence");
		apply_seq = (uint64) DatumGetInt64(seq_datum);
	}

	blob = merkle_serialize_staged_delta(0, 0, &delta_version);
	if (blob == NULL)
		elog(ERROR, "Merkle staged delta disappeared during PRE_COMMIT");
	if (delta_version != MERKLE_DELTA_LEGACY_VERSION &&
		delta_version != MERKLE_DELTA_VERSION)
		elog(ERROR, "Merkle serialization returned invalid version %d",
			 delta_version);

	values[0] = Int64GetDatum((int64) apply_seq);
	values[1] = Int32GetDatum(delta_version);
	values[2] = PointerGetDatum(blob);
	spi_rc = SPI_execute_with_args(
		"INSERT INTO ariabc_internal.merkle_local_delta"
		"       (apply_seq, delta_version, delta_blob)"
		" VALUES ($1, $2, $3)",
		3, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_INSERT || SPI_processed != 1)
		elog(ERROR, "failed to persist local Merkle delta at sequence %llu",
			 (unsigned long long) apply_seq);

	/*
	 * P0.2: Advance terminal_prefix_seq in the same transaction as the
	 * delta insert.  A local delta row is terminal by definition (it is
	 * committed by this transaction), so as soon as next_seq moves to
	 * apply_seq, the prefix can advance to include it.
	 */
	if (!deterministic_bcdb_seq)
		(void) merkle_advance_terminal_prefix_spi();

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle local delta SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();

	merkle_staged_delta_persisted = true;
	merkle_crash_failpoint("after_merkle_delta_ledger_written");
	merkle_crash_failpoint("after_merkle_delta_queue_written");
}

/* The durable queue is intentionally not writable by PUBLIC.  DML against a
 * Merkle-indexed table still has to work for ordinary table owners, so run
 * this backend-internal SPI mutation under the bootstrap superuser context
 * and restore the caller identity on every path. */
static void
merkle_persist_local_delta(void)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		merkle_persist_local_delta_impl();
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
merkle_delta_xact_callback(XactEvent event, void *arg)
{
	(void) arg;

	if (event == XACT_EVENT_PRE_PREPARE && merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("prepared transactions are not supported after Merkle index updates"),
				 errdetail("Crash-safe Merkle deltas must be committed by their originating backend.")));

	if (event == XACT_EVENT_PRE_COMMIT)
	{
		if (merkle_has_staged_delta() && !merkle_staged_delta_persisted)
			merkle_persist_local_delta();
		return;
	}

	if (event == XACT_EVENT_PARALLEL_PRE_COMMIT && merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel workers cannot persist Merkle deltas")));

	if (event == XACT_EVENT_COMMIT && merkle_staged_delta_persisted)
		merkle_crash_failpoint("after_user_transaction_commit");

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PARALLEL_ABORT || event == XACT_EVENT_PREPARE)
		merkle_delta_reset();
}

static void
merkle_delta_subxact_callback(SubXactEvent event,
								  SubTransactionId mySubid,
								  SubTransactionId parentSubid,
								  void *arg)
{
	MerkleSubxactFrame *child;

	(void) arg;
	child = merkle_delta_find_frame(mySubid);
	if (child == NULL)
		return;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		MerkleSubxactFrame *parent = merkle_delta_get_frame(parentSubid, true);
		HASH_SEQ_STATUS seq;
		MerkleDeltaEntry *entry;
		MerkleDynamicDeltaEntry *dynamic_entry;

		hash_seq_init(&seq, child->entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
			merkle_delta_merge_one(parent->entries, entry);
		hash_seq_init(&seq, child->dynamic_entries);
		while ((dynamic_entry = hash_seq_search(&seq)) != NULL)
			merkle_dynamic_delta_merge_one(parent->dynamic_entries,
								   dynamic_entry,
								   parent->context);
		merkle_delta_unlink_frame(child);
	}
	else if (event == SUBXACT_EVENT_ABORT_SUB)
		merkle_delta_unlink_frame(child);
}
