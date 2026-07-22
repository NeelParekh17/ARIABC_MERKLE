/*-------------------------------------------------------------------------
 *
 * merkledelta.c
 *    Transaction-local Merkle delta staging and durable local enqueue.
 *
 * Physical Merkle pages are deliberately not modified by user transactions.
 * Each transaction aggregates native dynamic transitions by index/partition
 * and publishes the resulting COW roots at commit.
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

/* Canonical persisted representation of a Raft epoch: the first eight
 * bytes of the 32-byte digest interpreted as an unsigned big-endian integer.
 * All writers therefore agree on the same value regardless of host endian. */
static uint64
merkle_raft_epoch_sequence(const uint8 epoch_id[BCDB_RAFT_DIGEST_BYTES])
{
	uint64 value;

	memcpy(&value, epoch_id, sizeof(value));
	return pg_ntoh64(value);
}

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
static bool merkle_native_roots_published = false;
static uint64 merkle_delta_generation = 0;

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
	merkle_native_roots_published = false;
	merkle_delta_generation = 0;
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
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("native v8 Merkle maintenance requires a dynamic index"),
				 errhint("REINDEX the index with dynamic=true.")));
	}
	if (identity->key_data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dynamic Merkle item identity has no canonical key")));

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

bool
merkle_has_synchronous_staged_delta(void)
{
	MerkleSubxactFrame *frame;

	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		HASH_SEQ_STATUS status;
		MerkleDynamicDeltaEntry *dentry;

		hash_seq_init(&status, frame->dynamic_entries);
		while ((dentry = hash_seq_search(&status)) != NULL)
		{
			if (merkle_get_update_mode_by_oid(dentry->key.index_oid) == MERKLE_UPDATE_SYNCHRONOUS_COW)
			{
				hash_seq_term(&status);
				return true;
			}
		}
	}
	return false;
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

static void
merkle_publish_native_staged_dynamic(void)
{
	MemoryContext context;
	MemoryContext old_context;
	HTAB *combined;
	MerkleSubxactFrame *frame;
	HASH_SEQ_STATUS seq;
	MerkleDynamicDeltaEntry *entry;
	MerkleSerializedEntry *ordered;
	long count;
	long i = 0;
	long start;
	uint64 apply_seq;
	uint16 sequence_domain;
	uint64 sequence_epoch = 0;

	/* Deterministic workers expose their replica-agreed order.  Ordinary
	 * PostgreSQL transactions use their top-level XID as local provenance. */
	if (is_bcdb_worker && activeTx != NULL &&
		activeTx->tx_id != BCDBInvalidTid)
	{
		sequence_domain = MERKLE_SEQUENCE_RAFT;
		sequence_epoch = merkle_raft_epoch_sequence(activeTx->raft_epoch_id);
		apply_seq = (uint64) activeTx->tx_id + 1;
	}
	else
	{
		sequence_domain = MERKLE_SEQUENCE_LOCAL_XID;
		apply_seq = (uint64) GetTopTransactionId();
	}

	context = AllocSetContextCreate(CurrentMemoryContext,
		"native Merkle precommit", ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(context);
	combined = merkle_dynamic_delta_create_map(context);
	for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
	{
		hash_seq_init(&seq, frame->dynamic_entries);
		while ((entry = hash_seq_search(&seq)) != NULL)
		{
			if (!OidIsValid(entry->key.index_oid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("strict Merkle delta map contains index OID 0")));
			/* PRE_COMMIT owns only strict native transitions.  Pending-mode
			 * entries must remain in the durable queue and be materialized by
			 * the ordered applier exactly once. */
			if (merkle_get_update_mode_by_oid(entry->key.index_oid) ==
				MERKLE_UPDATE_SYNCHRONOUS_COW)
				merkle_dynamic_delta_merge_one(combined, entry, context);
		}
	}
	count = hash_get_num_entries(combined);
	if (count == 0)
	{
		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(context);
		return;
	}
	ordered = palloc(sizeof(*ordered) * count);
	hash_seq_init(&seq, combined);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		ordered[i].kind = MERKLE_SERIALIZED_DYNAMIC;
		ordered[i].value.dynamic_entry = *entry;
		i++;
	}
	qsort(ordered, count, sizeof(*ordered), merkle_serialized_entry_cmp);
	for (start = 0; start < count; )
	{
		long end = start + 1;
		MerkleDynamicTransition *transitions;
		Oid index_oid = ordered[start].value.dynamic_entry.key.index_oid;
		long j;

		while (end < count &&
			ordered[end].value.dynamic_entry.key.index_oid == index_oid)
			end++;
		transitions = palloc0(sizeof(*transitions) * (end - start));
		for (j = start; j < end; j++)
		{
			MerkleDynamicDeltaEntry *source =
				&ordered[j].value.dynamic_entry;
			MerkleDynamicTransition *target = &transitions[j - start];

			target->index_oid = source->key.index_oid;
			target->index_rnode = source->key.index_rnode;
			target->partition_id = source->partition_id;
			memcpy(target->route_digest, source->key.route_digest,
				MERKLE_HASH_BYTES);
			target->key_data = source->key_data;
			target->has_old = source->has_old;
			target->has_new = source->has_new;
			target->old_hash = source->old_hash;
			target->new_hash = source->new_hash;
		}
		merkle_native_publish_strict_transitions(transitions,
			(int) (end - start), sequence_domain, sequence_epoch, apply_seq);
		pfree(transitions);
		start = end;
	}
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
	merkle_native_roots_published = true;
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
		if (merkle_has_synchronous_staged_delta() &&
			!merkle_native_roots_published)
			merkle_publish_native_staged_dynamic();
		return;
	}

	if (event == XACT_EVENT_PARALLEL_PRE_COMMIT && merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel workers cannot persist Merkle deltas")));

	if (event == XACT_EVENT_COMMIT &&
		merkle_native_roots_published)
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
