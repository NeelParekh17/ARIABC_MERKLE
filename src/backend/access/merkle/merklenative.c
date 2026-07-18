/*-------------------------------------------------------------------------
 *
 * merklenative.c
 *    PostgreSQL-native, XID-visible dynamic Merkle root journal.
 *
 * Root versions are immutable index tuples.  The mutable partition directory
 * is only a physical head hint: it may point at an aborted version, and root
 * selection walks backward until transaction visibility accepts a version.
 * Consequently Generic WAL redo of an aborted writer cannot publish its root.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/merkle.h"
#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "common/blake3.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "storage/bufmgr.h"
#include "storage/buffile.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/indexfsm.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/tuplesort.h"
#include "utils/guc.h"

#define MERKLE_NATIVE_PAGE_DIRECTORY 1
#define MERKLE_NATIVE_PAGE_APPEND    2
#define MERKLE_NATIVE_PAGE_FREE      3
#define MERKLE_NATIVE_ROOT_MAX_WALK  1000000
#define MERKLE_NATIVE_MAX_RECORD_SIZE \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	 MERKLE_NATIVE_PAGE_SPECIAL_SIZE - sizeof(ItemIdData))

/* Allocation hint only; every use is revalidated under an exclusive buffer
 * lock, so backend reuse, REINDEX, and concurrent fillers are harmless. */
static Oid native_append_hint_oid = InvalidOid;
static RelFileNode native_append_hint_rnode;
static BlockNumber native_append_hint_block = InvalidBlockNumber;

typedef struct NativeItem
{
	uint8 route[MERKLE_HASH_BYTES];
	MerkleHash hash;
	uint32 key_length;
	char *key;
} NativeItem;

typedef struct NativeItemVector
{
	NativeItem *items;
	int count;
	int capacity;
} NativeItemVector;

typedef struct NativeSpoolPosition
{
	int32 file_no;
	off_t offset;
} NativeSpoolPosition;

typedef struct NativePartitionSpool
{
	BufFile *data;
	BufFile *positions;
	uint64 count;
	uint64 bytes;
} NativePartitionSpool;

#define MERKLE_NATIVE_BUFFILE_SEGMENT_SIZE UINT64CONST(0x40000000)

typedef struct NativeConfig
{
	int partitions;
	int leaf_capacity;
	int merge_threshold;
	uint64 leaf_byte_capacity;
	int max_key_bytes;
} NativeConfig;

struct MerkleNativeBuildState
{
	MemoryContext context;
	Oid index_oid;
	NativeConfig config;
	uint64 baseline_apply_seq;
	Tuplesortstate *sort;
};

static void native_validate_page(Page page, uint16 expected_type,
								 BlockNumber block);
static void native_lock_partition(Relation indexRel, int partition_id);
static void native_publish_one(Relation indexRel, int partition_id,
							   const MerkleNativeRootVersion *input);
static bool native_route_has_prefix(
	const uint8 route[MERKLE_HASH_BYTES],
	const uint8 prefix[MERKLE_HASH_BYTES], int bits);
static int native_route_bit(const uint8 route[MERKLE_HASH_BYTES], int bit);
static uint64 native_item_bytes(const NativeItem *item);

static bool
native_page_has_record_space(Page page, Size record_size)
{
	return PageGetExactFreeSpace(page) >=
		MAXALIGN(record_size) + sizeof(ItemIdData);
}

static uint32
native_record_checksum(const void *record, Size size)
{
	char *copy = palloc(size);
	MerkleNativeRecordHeader *header;
	pg_crc32c crc;

	memcpy(copy, record, size);
	header = (MerkleNativeRecordHeader *) copy;
	header->checksum = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, copy, size);
	FIN_CRC32C(crc);
	pfree(copy);
	return (uint32) crc;
}

static int
native_item_cmp(const void *av, const void *bv)
{
	const NativeItem *a = av;
	const NativeItem *b = bv;
	int cmp = memcmp(a->route, b->route, MERKLE_HASH_BYTES);

	if (cmp != 0)
		return cmp;
	cmp = memcmp(a->key, b->key, Min(a->key_length, b->key_length));
	if (cmp != 0)
		return cmp;
	return a->key_length < b->key_length ? -1 :
		a->key_length > b->key_length ? 1 : 0;
}

static int
native_route_bit(const uint8 route[MERKLE_HASH_BYTES], int bit)
{
	return (route[bit / 8] >> (7 - (bit % 8))) & 1;
}

static void
native_canonical_prefix(const uint8 route[MERKLE_HASH_BYTES], int bits,
						uint8 prefix[MERKLE_HASH_BYTES])
{
	int bytes = bits / 8;
	int remain = bits % 8;

	MemSet(prefix, 0, MERKLE_HASH_BYTES);
	if (bytes > 0)
		memcpy(prefix, route, bytes);
	if (remain > 0)
		prefix[bytes] = route[bytes] & (uint8) (0xff << (8 - remain));
}

static void
native_hash_u32(blake3_hasher *hasher, uint32 value)
{
	value = pg_hton32(value);
	blake3_hasher_update(hasher, &value, sizeof(value));
}

static void
native_hash_u64(blake3_hasher *hasher, uint64 value)
{
	value = pg_hton64(value);
	blake3_hasher_update(hasher, &value, sizeof(value));
}

static void
native_read_config(Relation indexRel, NativeConfig *config)
{
	Buffer buffer = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
	Page page;
	MerkleMetaPageData *meta;

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	meta = MerklePageGetMeta(page);
	if (meta->dynamicMagic != MERKLE_DYNAMIC_META_MAGIC ||
		meta->dynamicLayoutVersion != MERKLE_DYNAMIC_LAYOUT_VERSION)
	{
		UnlockReleaseBuffer(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("dynamic Merkle index requires REINDEX for native layout v%d",
						MERKLE_DYNAMIC_LAYOUT_VERSION)));
	}
	config->partitions = meta->numPartitions;
	config->leaf_capacity = meta->dynamicLeafCapacity;
	config->merge_threshold = meta->dynamicMergeThreshold;
	config->leaf_byte_capacity = meta->dynamicLeafByteCapacity;
	config->max_key_bytes = meta->dynamicMaxKeyBytes;
	UnlockReleaseBuffer(buffer);
}

static int
native_directory_capacity(void)
{
	/* One mutable root head per page prevents unrelated hot partitions from
	 * contending on a shared directory buffer.  Layout v4 reserves exactly
	 * one directory page for each partition. */
	return 1;
}

static BlockNumber
native_directory_block(int partition_id)
{
	return MERKLE_TREE_START_BLKNO +
		(partition_id / native_directory_capacity());
}

static int
native_directory_slot(int partition_id)
{
	return partition_id % native_directory_capacity();
}

static void
native_invalid_locator(MerkleNativeLocator *locator)
{
	locator->block = InvalidBlockNumber;
	locator->offset = MERKLE_NATIVE_INVALID_OFFSET;
	locator->reserved = 0;
	locator->page_generation = 0;
}

static bool
native_locator_valid(const MerkleNativeLocator *locator)
{
	return BlockNumberIsValid(locator->block) &&
		OffsetNumberIsValid(locator->offset) &&
		locator->page_generation != 0;
}

static void
native_validate_locator_generation(Page page,
								const MerkleNativeLocator *locator)
{
	MerkleNativePageOpaqueData *opaque = MerkleNativePageGetOpaque(page);

	if (opaque->page_generation != locator->page_generation)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle locator generation mismatch at block %u",
						locator->block),
				 errdetail("locator generation=%u page generation=%u; the page was reused or the locator was corrupted",
						   locator->page_generation, opaque->page_generation),
				 errhint("REINDEX the native Merkle index.")));
}

/*
 * native_append_record
 *
 * Append an immutable native record to the index relation and return its
 * physical locator.  The relation extension lock is taken ONLY when a new
	 * physical block must be allocated (P_NEW path).  The existing-append-page
 * fast-path and the FSM-reuse path operate under the buffer lock alone,
 * which allows concurrent writers in different partitions to proceed without
 * serialising on the global extension lock.
 *
 * This fixes the bottleneck identified in plan_left.md §5.
 */
static MerkleNativeLocator
native_append_record(Relation indexRel, const void *record, Size size)
{
	Buffer buffer = InvalidBuffer;
	Page page;
	Page target;
	BlockNumber nblocks;
	BlockNumber block;
	bool initialized = false;
	uint32 new_generation = 0;
	GenericXLogState *state;
	OffsetNumber offset;
	MerkleNativeLocator result;

	if (size > MERKLE_NATIVE_MAX_RECORD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("native Merkle record of %zu bytes exceeds one-page limit",
						size)));

	/*
	 * Fast path 1: backend-local append hint.  Re-check under the buffer
	 * lock that the hinted page is still an APPEND page with room.
	 */
	if (native_append_hint_oid == RelationGetRelid(indexRel) &&
		RelFileNodeEquals(native_append_hint_rnode, indexRel->rd_node) &&
		BlockNumberIsValid(native_append_hint_block))
	{
		nblocks = RelationGetNumberOfBlocks(indexRel);
		if (native_append_hint_block < nblocks)
		{
			buffer = ReadBuffer(indexRel, native_append_hint_block);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);
			if (!PageIsNew(page) &&
				PageGetSpecialSize(page) == MERKLE_NATIVE_PAGE_SPECIAL_SIZE &&
				MerkleNativePageGetOpaque(page)->magic == MERKLE_NATIVE_PAGE_MAGIC &&
				MerkleNativePageGetOpaque(page)->version == MERKLE_NATIVE_PAGE_VERSION &&
				MerkleNativePageGetOpaque(page)->page_type == MERKLE_NATIVE_PAGE_APPEND &&
				native_page_has_record_space(page, size))
			{
				/* Hint is still valid; use this page directly. */
				goto write_record;
			}
			UnlockReleaseBuffer(buffer);
			buffer = InvalidBuffer;
		}
	}

	/*
	 * Fast path 2: FSM recycle.  GetFreeIndexPage does not require the
	 * extension lock.
	 */
	block = GetFreeIndexPage(indexRel);
	nblocks = RelationGetNumberOfBlocks(indexRel);
	if (BlockNumberIsValid(block) && block < nblocks)
	{
		buffer = ReadBuffer(indexRel, block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (!PageIsNew(page) &&
			PageGetSpecialSize(page) == MERKLE_NATIVE_PAGE_SPECIAL_SIZE &&
			MerkleNativePageGetOpaque(page)->magic == MERKLE_NATIVE_PAGE_MAGIC &&
			MerkleNativePageGetOpaque(page)->version == MERKLE_NATIVE_PAGE_VERSION &&
			MerkleNativePageGetOpaque(page)->page_type == MERKLE_NATIVE_PAGE_FREE &&
			native_page_has_record_space(page, size))
		{
			/* Reserve FREE -> APPEND.  The real page is initialized only on the
			 * GenericXLog temporary copy below. */
			uint32 generation = MerkleNativePageGetOpaque(page)->page_generation;

			if (generation == PG_UINT32_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("native Merkle page generation exhausted at block %u",
								block)));
			new_generation = generation + 1;
			initialized = true;
			goto write_record;
		}
		UnlockReleaseBuffer(buffer);
		buffer = InvalidBuffer;
	}

	/*
	 * Fast path 3: last block in the relation, if it is an APPEND page
	 * with enough free space.  Still no extension lock needed.
	 */
	if (nblocks > MERKLE_TREE_START_BLKNO)
	{
		block = nblocks - 1;
		buffer = ReadBuffer(indexRel, block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (!PageIsNew(page) &&
			PageGetSpecialSize(page) == MERKLE_NATIVE_PAGE_SPECIAL_SIZE &&
			MerkleNativePageGetOpaque(page)->magic == MERKLE_NATIVE_PAGE_MAGIC &&
			MerkleNativePageGetOpaque(page)->version == MERKLE_NATIVE_PAGE_VERSION &&
			MerkleNativePageGetOpaque(page)->page_type == MERKLE_NATIVE_PAGE_APPEND &&
			native_page_has_record_space(page, size))
			goto write_record;
		UnlockReleaseBuffer(buffer);
		buffer = InvalidBuffer;
	}

	/*
	 * Slow path: extend the relation.  The extension lock is taken only
	 * here.  Keep it until the new buffer is exclusively locked: releasing it
	 * between ReadBuffer(P_NEW) and LockBuffer lets another backend select the
	 * new zero page as the relation's last block, initialize it, and then have
	 * this backend overwrite that record while initializing the same page.
	 * The buffer lock is the handoff that makes the new block safe to expose.
	 */
	LockRelationForExtension(indexRel, ExclusiveLock);
	buffer = ReadBuffer(indexRel, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	UnlockRelationForExtension(indexRel, ExclusiveLock);
	page = BufferGetPage(buffer);
	new_generation = 1;
	initialized = true;

write_record:
	state = GenericXLogStart(indexRel);
	target = GenericXLogRegisterBuffer(state, buffer,
		initialized ? GENERIC_XLOG_FULL_IMAGE : 0);
	/* The registered page is a GenericXLog-owned image.  Exercise the
	 * register/finish crash boundary before any caller-visible page mutation. */
	merkle_crash_failpoint("after_native_register_before_finish");
	if (initialized)
	{
		MerkleNativePageOpaqueData *opaque;

		PageInit(target, BLCKSZ, MERKLE_NATIVE_PAGE_SPECIAL_SIZE);
		opaque = MerkleNativePageGetOpaque(target);
		opaque->magic = MERKLE_NATIVE_PAGE_MAGIC;
		opaque->version = MERKLE_NATIVE_PAGE_VERSION;
		opaque->page_type = MERKLE_NATIVE_PAGE_APPEND;
		opaque->page_generation = new_generation;
	}
	offset = PageAddItem(target, (Item) record, size, InvalidOffsetNumber,
		false, false);
	if (!OffsetNumberIsValid(offset))
		elog(ERROR, "could not append native Merkle record");
	result.block = BufferGetBlockNumber(buffer);
	result.offset = offset;
	result.reserved = 0;
	result.page_generation =
		MerkleNativePageGetOpaque(target)->page_generation;
	GenericXLogFinish(state);
	merkle_crash_failpoint("after_native_record_wal");
	native_append_hint_oid = RelationGetRelid(indexRel);
	native_append_hint_rnode = indexRel->rd_node;
	native_append_hint_block = result.block;
	UnlockReleaseBuffer(buffer);
	return result;
}

static void *
native_read_record(Relation indexRel, const MerkleNativeLocator *locator,
				   uint16 expected_type, Size minimum_size, Size *size_out)
{
	Buffer buffer;
	Page page;
	ItemId itemid;
	Size size;
	void *copy;
	MerkleNativeRecordHeader *header;

	if (!native_locator_valid(locator))
		elog(ERROR, "invalid native Merkle record locator");
	/*
	 * Guard against the impossible-block-number failure (plan_left.md §1).
	 * Check the relation size before attempting ReadBuffer so we get a
	 * clear ERROR with context rather than a backend crash or silent
	 * access to a recycled page.
	 */
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(indexRel);

		if (locator->block >= nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle record locator block %u is out of range "
							"(relation has %u blocks)",
							locator->block, nblocks),
					 errdetail("This indicates a stale or corrupted locator. "
							   "The index may need REINDEX.")));
	}
	buffer = ReadBuffer(indexRel, locator->block);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	native_validate_page(page, MERKLE_NATIVE_PAGE_APPEND, locator->block);
	native_validate_locator_generation(page, locator);
	if (locator->offset > PageGetMaxOffsetNumber(page))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle record offset is out of range")));
	itemid = PageGetItemId(page, locator->offset);
	size = ItemIdIsNormal(itemid) ? ItemIdGetLength(itemid) : 0;
	if (size < minimum_size)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle record is truncated")));
	copy = palloc(size);
	memcpy(copy, PageGetItem(page, itemid), size);
	UnlockReleaseBuffer(buffer);
	header = copy;
	if (header->magic != MERKLE_NATIVE_RECORD_MAGIC ||
		header->version != MERKLE_NATIVE_RECORD_VERSION ||
		(expected_type != 0 && header->type != expected_type) ||
		header->size != size ||
		header->checksum != native_record_checksum(copy, size))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle record checksum, type, or version is invalid")));
	if (size_out != NULL)
		*size_out = size;
	return copy;
}

static void
native_validate_page(Page page, uint16 expected_type, BlockNumber block)
{
	MerkleNativePageOpaqueData *opaque;

	if (PageIsNew(page) ||
		PageGetSpecialSize(page) != MERKLE_NATIVE_PAGE_SPECIAL_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid native Merkle page at block %u", block)));
	opaque = MerkleNativePageGetOpaque(page);
	if (opaque->magic != MERKLE_NATIVE_PAGE_MAGIC ||
		opaque->version != MERKLE_NATIVE_PAGE_VERSION ||
		opaque->page_type != expected_type)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid native Merkle page envelope at block %u", block)));
}

static uint32
native_root_checksum(const MerkleNativeRootVersion *root)
{
	MerkleNativeRootVersion copy = *root;
	pg_crc32c crc;

	copy.checksum = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &copy, sizeof(copy));
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static bool
native_root_committed(const MerkleNativeRootVersion *root)
{
	if ((root->flags & MERKLE_NATIVE_ROOT_ABORTED_HINT) != 0)
		return false;
	if ((root->flags & MERKLE_NATIVE_ROOT_FROZEN_COMMITTED) != 0 ||
		root->creator_xid == FrozenTransactionId)
		return true;
	if (!TransactionIdIsValid(root->creator_xid))
		return false;
	if (TransactionIdIsCurrentTransactionId(root->creator_xid))
		return true;
	return TransactionIdDidCommit(root->creator_xid);
}

static bool
native_root_visible(const MerkleNativeRootVersion *root)
{
	Snapshot snapshot;

	if (!native_root_committed(root))
		return false;
	if (root->creator_xid == FrozenTransactionId ||
		(root->flags & MERKLE_NATIVE_ROOT_FROZEN_COMMITTED) != 0 ||
		TransactionIdIsCurrentTransactionId(root->creator_xid))
		return true;
	snapshot = ActiveSnapshotSet() ? GetActiveSnapshot() : NULL;
	return snapshot == NULL || !XidInMVCCSnapshot(root->creator_xid, snapshot);
}

static MerkleNativeRootVersion
native_read_root(Relation indexRel, const MerkleNativeLocator *locator)
{
	Buffer buffer;
	Page page;
	ItemId itemid;
	MerkleNativeRootVersion root;

	if (!native_locator_valid(locator))
		elog(ERROR, "attempted to read an invalid native Merkle locator");
	/* Block-range guard: fail with clear provenance rather than silently
	 * accessing a recycled or non-existent page (plan_left.md §1). */
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(indexRel);

		if (locator->block >= nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle root locator block %u is out of range "
							"(relation has %u blocks)",
							locator->block, nblocks),
					 errdetail("The root chain contains a stale or corrupted locator. "
							   "Run REINDEX to rebuild the native Merkle index.")));
	}
	buffer = ReadBuffer(indexRel, locator->block);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	native_validate_page(page, MERKLE_NATIVE_PAGE_APPEND, locator->block);
	native_validate_locator_generation(page, locator);
	if (locator->offset > PageGetMaxOffsetNumber(page))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle root offset is out of range")));
	itemid = PageGetItemId(page, locator->offset);
	if (!ItemIdIsNormal(itemid) || ItemIdGetLength(itemid) != sizeof(root))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid native Merkle root record")));
	memcpy(&root, PageGetItem(page, itemid), sizeof(root));
	UnlockReleaseBuffer(buffer);
	if (root.magic != MERKLE_NATIVE_ROOT_MAGIC ||
		root.version != MERKLE_NATIVE_ROOT_VERSION ||
		root.checksum != native_root_checksum(&root))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle root record checksum or version is invalid")));
	return root;
}

static MerkleNativePartitionEntry
native_read_directory(Relation indexRel, int partition_id)
{
	BlockNumber block = native_directory_block(partition_id);
	Buffer buffer = ReadBuffer(indexRel, block);
	Page page;
	MerkleNativePartitionEntry entry;

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	native_validate_page(page, MERKLE_NATIVE_PAGE_DIRECTORY, block);
	memcpy(&entry,
		   ((MerkleNativePartitionEntry *) PageGetContents(page)) +
		   native_directory_slot(partition_id), sizeof(entry));
	UnlockReleaseBuffer(buffer);
	return entry;
}

static bool
native_visible_root(Relation indexRel, int partition_id,
					MerkleNativeRootVersion *result)
{
	MerkleNativePartitionEntry entry =
		native_read_directory(indexRel, partition_id);
	MerkleNativeLocator locator = entry.root_head;
	int walked = 0;

	while (native_locator_valid(&locator))
	{
		MerkleNativeRootVersion root = native_read_root(indexRel, &locator);

		if (root.partition_id != (uint32) partition_id)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle root chain crosses partitions")));
		if (native_root_visible(&root))
		{
			*result = root;
			return true;
		}
		locator = root.previous_version;
		if (++walked > MERKLE_NATIVE_ROOT_MAX_WALK)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("native Merkle root chain is unreasonably long")));
	}
	return false;
}

static bool
native_latest_root_for_write(Relation indexRel, int partition_id,
							 MerkleNativeRootVersion *result)
{
	MerkleNativePartitionEntry entry =
		native_read_directory(indexRel, partition_id);
	MerkleNativeLocator locator = entry.root_head;
	int walked = 0;

	while (native_locator_valid(&locator))
	{
		MerkleNativeRootVersion root = native_read_root(indexRel, &locator);

		if (root.partition_id != (uint32) partition_id)
			elog(ERROR, "native Merkle root chain crosses partitions");
		if (native_root_committed(&root))
		{
			*result = root;
			return true;
		}
		locator = root.previous_version;
		if (++walked > MERKLE_NATIVE_ROOT_MAX_WALK)
			elog(ERROR, "native Merkle root chain is unreasonably long");
	}
	return false;
}

static void
native_vector_push(NativeItemVector *vector, const NativeItem *item)
{
	if (vector->count == vector->capacity)
	{
		vector->capacity = vector->capacity == 0 ? 32 : vector->capacity * 2;
		vector->items = vector->items == NULL ?
			palloc(sizeof(*vector->items) * vector->capacity) :
			repalloc(vector->items, sizeof(*vector->items) * vector->capacity);
	}
	vector->items[vector->count++] = *item;
}

static void
native_vector_free(NativeItemVector *vector)
{
	int i;

	for (i = 0; i < vector->count; i++)
		if (vector->items[i].key != NULL)
			pfree(vector->items[i].key);
	if (vector->items != NULL)
		pfree(vector->items);
	MemSet(vector, 0, sizeof(*vector));
}

static void
native_spool_write_exact(BufFile *file, const void *data, Size size)
{
	if (BufFileWrite(file, (void *) data, size) != size)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write native Merkle temporary build file")));
}

static void
native_spool_read_exact(BufFile *file, void *data, Size size)
{
	if (BufFileRead(file, data, size) != size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("native Merkle temporary build file is truncated")));
}

static NativePartitionSpool *
native_spool_create(void)
{
	NativePartitionSpool *spool = palloc0(sizeof(*spool));

	spool->data = BufFileCreateTemp(false);
	spool->positions = BufFileCreateTemp(false);
	return spool;
}

static void
native_spool_close(NativePartitionSpool *spool)
{
	if (spool == NULL)
		return;
	BufFileClose(spool->data);
	BufFileClose(spool->positions);
	pfree(spool);
}

static void
native_spool_append(NativePartitionSpool *spool, const NativeItem *item)
{
	NativeSpoolPosition position;
	uint32 key_length = item->key_length;

	BufFileTell(spool->data, &position.file_no, &position.offset);
	native_spool_write_exact(spool->positions, &position, sizeof(position));
	native_spool_write_exact(spool->data, item->route, MERKLE_HASH_BYTES);
	native_spool_write_exact(spool->data, &item->hash, sizeof(item->hash));
	native_spool_write_exact(spool->data, &key_length, sizeof(key_length));
	native_spool_write_exact(spool->data, item->key, item->key_length);
	spool->count++;
	spool->bytes += native_item_bytes(item);
}

static void
native_spool_seek_ordinal(NativePartitionSpool *spool, uint64 ordinal)
{
	uint64 absolute = ordinal * sizeof(NativeSpoolPosition);
	int file_no = (int) (absolute / MERKLE_NATIVE_BUFFILE_SEGMENT_SIZE);
	off_t offset = (off_t) (absolute % MERKLE_NATIVE_BUFFILE_SEGMENT_SIZE);

	if (ordinal >= spool->count ||
		BufFileSeek(spool->positions, file_no, offset, SEEK_SET) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("native Merkle temporary build ordinal is invalid")));
}

static void
native_spool_read_position(NativePartitionSpool *spool, uint64 ordinal,
						   NativeSpoolPosition *position)
{
	native_spool_seek_ordinal(spool, ordinal);
	native_spool_read_exact(spool->positions, position, sizeof(*position));
	if (BufFileSeek(spool->data, position->file_no, position->offset,
					SEEK_SET) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("native Merkle temporary build position is invalid")));
}

static void
native_spool_read_route(NativePartitionSpool *spool, uint64 ordinal,
						uint8 route[MERKLE_HASH_BYTES])
{
	NativeSpoolPosition position;

	native_spool_read_position(spool, ordinal, &position);
	native_spool_read_exact(spool->data, route, MERKLE_HASH_BYTES);
}

static uint64
native_spool_read_leaf(NativePartitionSpool *spool, uint64 first,
					   uint64 count, const NativeConfig *config,
					   NativeItemVector *leaf)
{
	NativeSpoolPosition position;
	uint64 bytes = 0;
	uint64 i;

	if (count == 0)
		return 0;
	/* Data records are appended in ordinal order.  Seek once to the first
	 * record and consume the bounded leaf sequentially; seeking through the
	 * position file for every item made large builds perform O(N) random I/O. */
	native_spool_read_position(spool, first, &position);
	for (i = 0; i < count; i++)
	{
		NativeItem item;

		native_spool_read_exact(spool->data, item.route, MERKLE_HASH_BYTES);
		native_spool_read_exact(spool->data, &item.hash, sizeof(item.hash));
		native_spool_read_exact(spool->data, &item.key_length,
			sizeof(item.key_length));
		if (item.key_length > (uint32) config->max_key_bytes ||
			native_item_bytes(&item) > config->leaf_byte_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("native Merkle temporary build key is invalid")));
		item.key = palloc(item.key_length);
		native_spool_read_exact(spool->data, item.key, item.key_length);
		bytes += native_item_bytes(&item);
		native_vector_push(leaf, &item);
	}
	return bytes;
}

/* Spillable verifier/build sort key: partition, route, canonical key and
 * tuple hash, all in network order so bytea's btree comparator is portable. */
static bytea *
native_pack_sort_item(int partition, const NativeItem *item)
{
	uint32 p = pg_hton32((uint32) partition);
	uint32 key_length = pg_hton32(item->key_length);
	Size payload = sizeof(p) + MERKLE_HASH_BYTES + sizeof(key_length) +
		item->key_length + MERKLE_HASH_BYTES;
	bytea *packed = palloc(VARHDRSZ + payload);
	char *cursor = VARDATA(packed);

	SET_VARSIZE(packed, VARHDRSZ + payload);
	memcpy(cursor, &p, sizeof(p));
	cursor += sizeof(p);
	memcpy(cursor, item->route, MERKLE_HASH_BYTES);
	cursor += MERKLE_HASH_BYTES;
	memcpy(cursor, &key_length, sizeof(key_length));
	cursor += sizeof(key_length);
	memcpy(cursor, item->key, item->key_length);
	cursor += item->key_length;
	memcpy(cursor, item->hash.data, MERKLE_HASH_BYTES);
	return packed;
}

static int
native_compare_packed(Datum left, Datum right)
{
	bytea *a = DatumGetByteaPP(left);
	bytea *b = DatumGetByteaPP(right);
	Size alen = VARSIZE_ANY_EXHDR(a);
	Size blen = VARSIZE_ANY_EXHDR(b);
	int cmp = memcmp(VARDATA_ANY(a), VARDATA_ANY(b), Min(alen, blen));

	if (cmp != 0)
		return cmp;
	return alen == blen ? 0 : (alen < blen ? -1 : 1);
}

static void
native_unpack_sort_item(Datum value, int *partition, NativeItem *item)
{
	bytea *packed = DatumGetByteaPP(value);
	Size payload = VARSIZE_ANY_EXHDR(packed);
	const char *cursor = VARDATA_ANY(packed);
	uint32 network_partition;
	uint32 network_key_length;

	if (payload < sizeof(network_partition) + MERKLE_HASH_BYTES +
		sizeof(network_key_length) + MERKLE_HASH_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid packed native Merkle build item")));
	memcpy(&network_partition, cursor, sizeof(network_partition));
	cursor += sizeof(network_partition);
	memcpy(item->route, cursor, MERKLE_HASH_BYTES);
	cursor += MERKLE_HASH_BYTES;
	memcpy(&network_key_length, cursor, sizeof(network_key_length));
	cursor += sizeof(network_key_length);
	item->key_length = pg_ntoh32(network_key_length);
	if (payload != sizeof(network_partition) + MERKLE_HASH_BYTES +
		sizeof(network_key_length) + item->key_length + MERKLE_HASH_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid packed native Merkle build item length")));
	item->key = palloc(item->key_length);
	memcpy(item->key, cursor, item->key_length);
	cursor += item->key_length;
	memcpy(item->hash.data, cursor, MERKLE_HASH_BYTES);
	*partition = (int) pg_ntoh32(network_partition);
}

static uint64
native_item_bytes(const NativeItem *item)
{
	return (uint64) item->key_length + 64;
}

static MerkleNativeNodeRecord *
native_read_node(Relation indexRel, const MerkleNativeLocator *locator)
{
	Size size;
	MerkleNativeNodeRecord *node = native_read_record(indexRel, locator,
		0, sizeof(*node), &size);

	if (size != sizeof(*node) ||
		(node->header.type != MERKLE_NATIVE_RECORD_INTERNAL &&
		 node->header.type != MERKLE_NATIVE_RECORD_LEAF) ||
		(node->header.type == MERKLE_NATIVE_RECORD_LEAF &&
		 (node->flags & MERKLE_NATIVE_NODE_LEAF) == 0))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle node has invalid size")));
	return node;
}

static void
native_load_leaf_items(Relation indexRel, const MerkleNativeNodeRecord *leaf,
					   NativeItemVector *vector)
{
	MerkleNativeLocator locator = leaf->item_head;
	uint64 seen = 0;
	NativeItem previous;
	bool have_previous = false;

	if ((leaf->flags & MERKLE_NATIVE_NODE_LEAF) == 0)
		elog(ERROR, "native Merkle item load requested for internal node");
	while (native_locator_valid(&locator))
	{
		Size size;
		MerkleNativeRecordHeader *header = native_read_record(indexRel, &locator,
			0, sizeof(*header), &size);
		MerkleNativeLocator next;

		if (header->type == MERKLE_NATIVE_RECORD_ITEM)
		{
			MerkleNativeItemRecord *record = (MerkleNativeItemRecord *) header;
			NativeItem item;

			if (record->key_length > size - sizeof(*record) ||
				size != sizeof(*record) + record->key_length)
				elog(ERROR, "native Merkle item key length is invalid");
			memcpy(item.route, record->route_digest, MERKLE_HASH_BYTES);
			item.hash = record->tuple_hash;
			item.key_length = record->key_length;
			item.key = palloc(item.key_length);
			memcpy(item.key, ((char *) record) + sizeof(*record), item.key_length);
			if (have_previous && native_item_cmp(&previous, &item) >= 0)
				elog(ERROR, "native Merkle leaf items are not in canonical order");
			native_vector_push(vector, &item);
			previous = item;
			have_previous = true;
			next = record->next;
			seen++;
		}
		else if (header->type == MERKLE_NATIVE_RECORD_ITEM_CHUNK)
		{
			MerkleNativeItemChunkRecord *chunk =
				(MerkleNativeItemChunkRecord *) header;
			char *cursor;
			char *end;
			uint32 i;

			if (size < sizeof(*chunk) ||
				chunk->payload_bytes != size - sizeof(*chunk))
				elog(ERROR, "native Merkle item chunk size is invalid");
			cursor = ((char *) chunk) + sizeof(*chunk);
			end = ((char *) chunk) + size;
			for (i = 0; i < chunk->item_count; i++)
			{
				MerkleNativePackedItem *packed;
				NativeItem item;

				if (end - cursor < (int) sizeof(*packed))
					elog(ERROR, "native Merkle item chunk is truncated");
				packed = (MerkleNativePackedItem *) cursor;
				if (packed->key_length > (uint32) (end - cursor - sizeof(*packed)))
					elog(ERROR, "native Merkle packed key length is invalid");
				memcpy(item.route, packed->route_digest, MERKLE_HASH_BYTES);
				item.hash = packed->tuple_hash;
				item.key_length = packed->key_length;
				item.key = palloc(item.key_length);
				memcpy(item.key, cursor + sizeof(*packed), item.key_length);
				if (have_previous && native_item_cmp(&previous, &item) >= 0)
					elog(ERROR, "native Merkle leaf items are not in canonical order");
				native_vector_push(vector, &item);
				previous = item;
				have_previous = true;
				cursor += MAXALIGN(sizeof(*packed) + item.key_length);
				seen++;
			}
			if (cursor != end)
				elog(ERROR, "native Merkle item chunk has trailing bytes");
			next = chunk->next;
		}
		else
			elog(ERROR, "invalid native Merkle item record type");
		pfree(header);
		locator = next;
		if (seen > leaf->tuple_count)
			elog(ERROR, "native Merkle leaf item chain is cyclic or oversized");
	}
	if (seen != leaf->tuple_count)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle leaf count does not match item chain")));
}

static void
native_hash_leaf(int partition, int prefix_len,
				 const uint8 prefix[MERKLE_HASH_BYTES],
				 const NativeItem *items, int count, uint64 bytes,
				 const MerkleHash *data_xor, MerkleHash *result)
{
	blake3_hasher hasher;
	static const char domain[] = "ARIABC_NATIVE_LEAF_V1";
	int i;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, domain, sizeof(domain) - 1);
	native_hash_u32(&hasher, partition);
	native_hash_u32(&hasher, prefix_len);
	blake3_hasher_update(&hasher, prefix, MERKLE_HASH_BYTES);
	native_hash_u64(&hasher, count);
	native_hash_u64(&hasher, bytes);
	blake3_hasher_update(&hasher, data_xor->data, MERKLE_HASH_BYTES);
	for (i = 0; i < count; i++)
	{
		blake3_hasher_update(&hasher, items[i].route, MERKLE_HASH_BYTES);
		native_hash_u32(&hasher, items[i].key_length);
		blake3_hasher_update(&hasher, items[i].key, items[i].key_length);
		blake3_hasher_update(&hasher, items[i].hash.data, MERKLE_HASH_BYTES);
	}
	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

static void
native_hash_item_content(const NativeItem *item, MerkleHash *result)
{
	blake3_hasher hasher;
	static const char domain[] = "ARIABC_NATIVE_ITEM_CONTENT_V1";

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, domain, sizeof(domain) - 1);
	blake3_hasher_update(&hasher, item->route, MERKLE_HASH_BYTES);
	native_hash_u32(&hasher, item->key_length);
	blake3_hasher_update(&hasher, item->key, item->key_length);
	blake3_hasher_update(&hasher, item->hash.data, MERKLE_HASH_BYTES);
	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

static void
native_hash_internal(const MerkleNativeNodeRecord *left,
					 const MerkleNativeNodeRecord *right, int partition,
					 int prefix_len, const uint8 prefix[MERKLE_HASH_BYTES],
					 const MerkleHash *data_xor, uint64 count, uint64 bytes,
					 MerkleHash *result)
{
	blake3_hasher hasher;
	static const char domain[] = "ARIABC_NATIVE_INTERNAL_V1";
	const MerkleNativeNodeRecord *children[2] = {left, right};
	int i;

	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, domain, sizeof(domain) - 1);
	native_hash_u32(&hasher, partition);
	native_hash_u32(&hasher, prefix_len);
	blake3_hasher_update(&hasher, prefix, MERKLE_HASH_BYTES);
	native_hash_u64(&hasher, count);
	native_hash_u64(&hasher, bytes);
	blake3_hasher_update(&hasher, data_xor->data, MERKLE_HASH_BYTES);
	for (i = 0; i < 2; i++)
	{
		native_hash_u32(&hasher, children[i]->prefix_len);
		blake3_hasher_update(&hasher, children[i]->prefix, MERKLE_HASH_BYTES);
		blake3_hasher_update(&hasher, children[i]->structure_hash.data,
			MERKLE_HASH_BYTES);
	}
	blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}

static MerkleNativeLocator
native_write_leaf(Relation indexRel, int partition, int prefix_len,
				  const uint8 prefix[MERKLE_HASH_BYTES], NativeItem *items,
				  int count, MerkleNativeNodeRecord *summary)
{
	MerkleNativeLocator head;
	MerkleHash data_xor;
	MerkleHash content_xor;
	uint64 bytes = 0;
	int i;

	native_invalid_locator(&head);
	merkle_hash_zero(&data_xor);
	merkle_hash_zero(&content_xor);
	qsort(items, count, sizeof(*items), native_item_cmp);
	for (i = count; i > 0; )
	{
		int end = i;
		int start = end;
		Size payload = 0;
		Size size;
		MerkleNativeItemChunkRecord *chunk;
		char *cursor;

		while (start > 0)
		{
			Size item_size = MAXALIGN(sizeof(MerkleNativePackedItem) +
				items[start - 1].key_length);

			if (sizeof(*chunk) + payload + item_size >
				MERKLE_NATIVE_MAX_RECORD_SIZE)
				break;
			payload += item_size;
			start--;
		}
		if (start == end)
			elog(ERROR, "one native Merkle item cannot fit in a packed chunk");
		size = sizeof(*chunk) + payload;
		chunk = palloc0(size);
		chunk->header.magic = MERKLE_NATIVE_RECORD_MAGIC;
		chunk->header.version = MERKLE_NATIVE_RECORD_VERSION;
		chunk->header.type = MERKLE_NATIVE_RECORD_ITEM_CHUNK;
		chunk->header.size = size;
		chunk->next = head;
		chunk->item_count = end - start;
		chunk->payload_bytes = payload;
		cursor = ((char *) chunk) + sizeof(*chunk);
		for (i = start; i < end; i++)
		{
			MerkleNativePackedItem *packed =
				(MerkleNativePackedItem *) cursor;

			memcpy(packed->route_digest, items[i].route, MERKLE_HASH_BYTES);
			packed->tuple_hash = items[i].hash;
			packed->key_length = items[i].key_length;
			memcpy(cursor + sizeof(*packed), items[i].key,
				items[i].key_length);
			cursor += MAXALIGN(sizeof(*packed) + items[i].key_length);
		}
		chunk->header.checksum = native_record_checksum(chunk, size);
		head = native_append_record(indexRel, chunk, size);
		pfree(chunk);
		i = start;
	}
	for (i = 0; i < count; i++)
	{
		MerkleHash item_content;

		merkle_hash_xor(&data_xor, &items[i].hash);
		native_hash_item_content(&items[i], &item_content);
		merkle_hash_xor(&content_xor, &item_content);
		bytes += native_item_bytes(&items[i]);
	}
	MemSet(summary, 0, sizeof(*summary));
	summary->header.magic = MERKLE_NATIVE_RECORD_MAGIC;
	summary->header.version = MERKLE_NATIVE_RECORD_VERSION;
	summary->header.type = MERKLE_NATIVE_RECORD_LEAF;
	summary->header.size = sizeof(*summary);
	summary->partition_id = partition;
	summary->prefix_len = prefix_len;
	summary->flags = MERKLE_NATIVE_NODE_LEAF;
	memcpy(summary->prefix, prefix, MERKLE_HASH_BYTES);
	summary->tuple_count = count;
	summary->subtree_bytes = bytes;
	summary->data_xor = data_xor;
	summary->content_xor = content_xor;
	native_hash_leaf(partition, prefix_len, prefix, items, count, bytes,
		&data_xor, &summary->structure_hash);
	native_invalid_locator(&summary->left);
	native_invalid_locator(&summary->right);
	summary->item_head = head;
	summary->header.checksum = native_record_checksum(summary, sizeof(*summary));
	return native_append_record(indexRel, summary, sizeof(*summary));
}

static MerkleNativeLocator
native_build_subtree(Relation indexRel, const NativeConfig *config,
					 int partition, NativeItem *items, int count,
					 int minimum_prefix, MerkleNativeNodeRecord *summary,
					 uint64 *split_counter)
{
	uint64 bytes = 0;
	uint8 prefix[MERKLE_HASH_BYTES];
	int branch;
	int split;
	int i;

	MemSet(prefix, 0, sizeof(prefix));
	for (i = 0; i < count; i++)
		bytes += native_item_bytes(&items[i]);
	if (count <= config->leaf_capacity && bytes <= config->leaf_byte_capacity)
	{
		native_canonical_prefix(count > 0 ? items[0].route : prefix,
			minimum_prefix, prefix);
		return native_write_leaf(indexRel, partition, minimum_prefix, prefix,
			items, count, summary);
	}
	qsort(items, count, sizeof(*items), native_item_cmp);
	for (branch = minimum_prefix; branch < MERKLE_HASH_BITS; branch++)
		if (native_route_bit(items[0].route, branch) !=
			native_route_bit(items[count - 1].route, branch))
			break;
	if (branch >= MERKLE_HASH_BITS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("distinct Merkle keys share one full route digest and exceed leaf bounds")));
	for (split = 0; split < count; split++)
		if (native_route_bit(items[split].route, branch) != 0)
			break;
	{
		MerkleNativeNodeRecord left;
		MerkleNativeNodeRecord right;
		MerkleNativeLocator left_locator = native_build_subtree(indexRel, config,
			partition, items, split, branch + 1, &left, split_counter);
		MerkleNativeLocator right_locator = native_build_subtree(indexRel, config,
			partition, items + split, count - split, branch + 1, &right,
			split_counter);
		if (split_counter != NULL)
			(*split_counter)++;

		MemSet(summary, 0, sizeof(*summary));
		summary->header.magic = MERKLE_NATIVE_RECORD_MAGIC;
		summary->header.version = MERKLE_NATIVE_RECORD_VERSION;
		summary->header.type = MERKLE_NATIVE_RECORD_INTERNAL;
		summary->header.size = sizeof(*summary);
		summary->partition_id = partition;
		summary->prefix_len = branch;
		native_canonical_prefix(items[0].route, branch, summary->prefix);
		summary->tuple_count = left.tuple_count + right.tuple_count;
		summary->subtree_bytes = left.subtree_bytes + right.subtree_bytes;
		summary->data_xor = left.data_xor;
		merkle_hash_xor(&summary->data_xor, &right.data_xor);
		summary->content_xor = left.content_xor;
		merkle_hash_xor(&summary->content_xor, &right.content_xor);
		summary->left = left_locator;
		summary->right = right_locator;
		native_invalid_locator(&summary->item_head);
		native_hash_internal(&left, &right, partition, branch,
			summary->prefix, &summary->data_xor, summary->tuple_count,
			summary->subtree_bytes, &summary->structure_hash);
		summary->header.checksum = native_record_checksum(summary,
			sizeof(*summary));
		return native_append_record(indexRel, summary, sizeof(*summary));
	}
}

static MerkleNativeLocator
native_build_spooled_range(Relation indexRel, const NativeConfig *config,
						   int partition, NativePartitionSpool *spool,
						   uint64 first, uint64 count, int minimum_prefix,
						   MerkleNativeNodeRecord *summary)
{
	uint8 first_route[MERKLE_HASH_BYTES];
	uint8 last_route[MERKLE_HASH_BYTES];
	uint8 prefix[MERKLE_HASH_BYTES];
	int branch;

	MemSet(first_route, 0, sizeof(first_route));
	MemSet(last_route, 0, sizeof(last_route));
	MemSet(prefix, 0, sizeof(prefix));
	if (count == 0)
		return native_write_leaf(indexRel, partition, minimum_prefix, prefix,
			NULL, 0, summary);
	if (count <= (uint64) config->leaf_capacity)
	{
		NativeItemVector leaf = {0};
		uint64 bytes = native_spool_read_leaf(spool, first, count, config,
			&leaf);
		if (bytes <= config->leaf_byte_capacity)
		{
			MerkleNativeLocator locator;

			native_canonical_prefix(leaf.items[0].route, minimum_prefix,
				prefix);
			locator = native_write_leaf(indexRel, partition, minimum_prefix,
				prefix, leaf.items, leaf.count, summary);
			native_vector_free(&leaf);
			return locator;
		}
		native_vector_free(&leaf);
	}
	native_spool_read_route(spool, first, first_route);
	native_spool_read_route(spool, first + count - 1, last_route);
	for (branch = minimum_prefix; branch < MERKLE_HASH_BITS; branch++)
		if (native_route_bit(first_route, branch) !=
			native_route_bit(last_route, branch))
			break;
	if (branch >= MERKLE_HASH_BITS)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("distinct Merkle keys share one full route digest and exceed leaf bounds")));
	{
		uint64 low = first;
		uint64 high = first + count;
		uint64 split;
		MerkleNativeNodeRecord left;
		MerkleNativeNodeRecord right;
		MerkleNativeLocator left_locator;
		MerkleNativeLocator right_locator;

		while (low < high)
		{
			uint64 middle = low + (high - low) / 2;
			uint8 route[MERKLE_HASH_BYTES];

			native_spool_read_route(spool, middle, route);
			if (native_route_bit(route, branch) == 0)
				low = middle + 1;
			else
				high = middle;
		}
		split = low;
		if (split == first || split == first + count)
			elog(ERROR, "native Merkle temporary build split is invalid");
		left_locator = native_build_spooled_range(indexRel, config,
			partition, spool, first, split - first, branch + 1, &left);
		right_locator = native_build_spooled_range(indexRel, config,
			partition, spool, split, first + count - split, branch + 1,
			&right);

		MemSet(summary, 0, sizeof(*summary));
		summary->header.magic = MERKLE_NATIVE_RECORD_MAGIC;
		summary->header.version = MERKLE_NATIVE_RECORD_VERSION;
		summary->header.type = MERKLE_NATIVE_RECORD_INTERNAL;
		summary->header.size = sizeof(*summary);
		summary->partition_id = partition;
		summary->prefix_len = branch;
		native_canonical_prefix(first_route, branch, summary->prefix);
		summary->tuple_count = left.tuple_count + right.tuple_count;
		summary->subtree_bytes = left.subtree_bytes + right.subtree_bytes;
		summary->data_xor = left.data_xor;
		merkle_hash_xor(&summary->data_xor, &right.data_xor);
		summary->content_xor = left.content_xor;
		merkle_hash_xor(&summary->content_xor, &right.content_xor);
		summary->left = left_locator;
		summary->right = right_locator;
		native_invalid_locator(&summary->item_head);
		native_hash_internal(&left, &right, partition, branch,
			summary->prefix, &summary->data_xor, summary->tuple_count,
			summary->subtree_bytes, &summary->structure_hash);
		summary->header.checksum = native_record_checksum(summary,
			sizeof(*summary));
		return native_append_record(indexRel, summary, sizeof(*summary));
	}
}

void
merkle_native_init(Relation indexRel, int partitions,
				   uint64 baseline_apply_seq)
{
	int capacity = native_directory_capacity();
	int pages = (partitions + capacity - 1) / capacity;
	int page_no;

	(void) baseline_apply_seq;
	for (page_no = 0; page_no < pages; page_no++)
	{
		BlockNumber block = MERKLE_TREE_START_BLKNO + page_no;
		Buffer buffer = ReadBuffer(indexRel, block);
		Page target;
		MerkleNativePageOpaqueData *opaque;
		MerkleNativePartitionEntry *entries;
		GenericXLogState *state;
		int first = page_no * capacity;
		int count = Min(capacity, partitions - first);
		int i;

		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(indexRel);
		target = GenericXLogRegisterBuffer(state, buffer,
			GENERIC_XLOG_FULL_IMAGE);
		PageInit(target, BLCKSZ, MERKLE_NATIVE_PAGE_SPECIAL_SIZE);
		opaque = MerkleNativePageGetOpaque(target);
		opaque->magic = MERKLE_NATIVE_PAGE_MAGIC;
		opaque->version = MERKLE_NATIVE_PAGE_VERSION;
		opaque->page_type = MERKLE_NATIVE_PAGE_DIRECTORY;
		opaque->page_generation = 1;
		entries = (MerkleNativePartitionEntry *) PageGetContents(target);
		MemSet(entries, 0, count * sizeof(*entries));
		for (i = 0; i < count; i++)
			native_invalid_locator(&entries[i].root_head);
		((PageHeader) target)->pd_lower =
			(LocationIndex) ((char *) (entries + count) - (char *) target);
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buffer);
	}
}

bool
merkle_native_is_ready(Relation indexRel)
{
	Buffer buffer;
	Page page;
	MerkleMetaPageData *meta;
	bool ready;

	buffer = ReadBuffer(indexRel, MERKLE_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	meta = MerklePageGetMeta(page);
	ready = meta->dynamicMagic == MERKLE_DYNAMIC_META_MAGIC &&
		meta->dynamicLayoutVersion == MERKLE_DYNAMIC_LAYOUT_VERSION;
	UnlockReleaseBuffer(buffer);
	return ready;
}

MerkleNativeBuildState *
merkle_native_build_begin(Relation indexRel, uint64 baseline_apply_seq)
{
	MemoryContext context;
	MemoryContext old;
	MerkleNativeBuildState *state;

	context = AllocSetContextCreate(CurrentMemoryContext,
		"native Merkle page build", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(context);
	state = palloc0(sizeof(*state));
	state->context = context;
	state->index_oid = RelationGetRelid(indexRel);
	state->baseline_apply_seq = baseline_apply_seq;
	native_read_config(indexRel, &state->config);
	state->sort = tuplesort_begin_datum(BYTEAOID, ByteaLessOperator, InvalidOid,
		false, maintenance_work_mem, NULL, false);
	MemoryContextSwitchTo(old);
	return state;
}

void
merkle_native_build_add(MerkleNativeBuildState *state,
						const MerkleItemIdentity *identity,
						const MerkleHash *hash)
{
	NativeItem item;
	MemoryContext old;
	bytea *packed;
	int partition;

	if (state == NULL || identity == NULL || identity->key_data == NULL ||
		hash == NULL)
		elog(ERROR, "invalid native Merkle build item");
	partition = identity->route.partition_id;
	if (partition < 0 || partition >= state->config.partitions)
		elog(ERROR, "native Merkle build partition is out of bounds");
	item.key_length = VARSIZE_ANY_EXHDR(identity->key_data);
	if (item.key_length > (uint32) state->config.max_key_bytes ||
		native_item_bytes(&item) > state->config.leaf_byte_capacity)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("canonical native Merkle key exceeds configured leaf bounds")));
	memcpy(item.route, identity->route.route_digest, MERKLE_HASH_BYTES);
	item.hash = *hash;
	item.key = VARDATA_ANY(identity->key_data);
	old = MemoryContextSwitchTo(state->context);
	packed = native_pack_sort_item(partition, &item);
	tuplesort_putdatum(state->sort, PointerGetDatum(packed), false);
	pfree(packed);
	MemoryContextSwitchTo(old);
}

static void
native_publish_build_partition(Relation indexRel, const NativeConfig *config,
							   int partition, NativePartitionSpool *spool,
							   uint64 baseline_apply_seq)
{
	MerkleNativeNodeRecord node;
	MerkleNativeRootVersion root;
	MerkleNativeLocator root_node;

	root_node = native_build_spooled_range(indexRel, config, partition,
		spool, 0, spool == NULL ? 0 : spool->count, 0, &node);
	MemSet(&root, 0, sizeof(root));
	root.magic = MERKLE_NATIVE_ROOT_MAGIC;
	root.version = MERKLE_NATIVE_ROOT_VERSION;
	root.creator_xid = GetTopTransactionId();
	root.partition_id = partition;
	root.sequence_domain = MERKLE_SEQUENCE_LOCAL_XID;
	root.sequence_flags = MERKLE_SEQUENCE_FLAG_BUILD_BASELINE;
	root.sequence_epoch = 0;
	root.sequence_value = baseline_apply_seq;
	root.tuple_count = node.tuple_count;
	root.subtree_bytes = node.subtree_bytes;
	root.data_xor = node.data_xor;
	root.content_xor = node.content_xor;
	root.structure_hash = node.structure_hash;
	root.root_node = root_node;
	native_publish_one(indexRel, partition, &root);
}

void
merkle_native_build_finish(MerkleNativeBuildState *state)
{
	Relation indexRel;
	int partition = -1;
	NativePartitionSpool *spool = NULL;
	Datum value;
	bool isnull;
	int next_partition;

	if (state == NULL)
		elog(ERROR, "invalid native Merkle build state");
	indexRel = index_open(state->index_oid, RowExclusiveLock);
	for (partition = 0; partition < state->config.partitions; partition++)
		native_lock_partition(indexRel, partition);
	partition = -1;
	tuplesort_performsort(state->sort);
	while (tuplesort_getdatum(state->sort, true, &value, &isnull, NULL))
	{
		NativeItem item;
		int item_partition;

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("native Merkle build sort produced NULL item")));
		native_unpack_sort_item(value, &item_partition, &item);
		if (item_partition < 0 || item_partition >= state->config.partitions)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("native Merkle build partition is out of bounds")));
		if (partition != item_partition)
		{
			if (partition >= 0)
			{
				native_publish_build_partition(indexRel, &state->config,
					partition, spool, state->baseline_apply_seq);
				native_spool_close(spool);
				spool = NULL;
			}
			for (next_partition = Max(partition + 1, 0);
				next_partition < item_partition; next_partition++)
			{
				native_publish_build_partition(indexRel, &state->config,
					next_partition, NULL, state->baseline_apply_seq);
			}
			partition = item_partition;
			spool = native_spool_create();
		}
		native_spool_append(spool, &item);
		pfree(item.key);
		pfree(DatumGetPointer(value));
	}
	if (partition >= 0)
	{
		native_publish_build_partition(indexRel, &state->config, partition,
			spool, state->baseline_apply_seq);
		native_spool_close(spool);
		spool = NULL;
	}
	for (next_partition = Max(partition + 1, 0);
		next_partition < state->config.partitions; next_partition++)
		native_publish_build_partition(indexRel, &state->config, next_partition,
			NULL, state->baseline_apply_seq);
	index_close(indexRel, RowExclusiveLock);
	tuplesort_end(state->sort);
	MemoryContextDelete(state->context);
}

static void
native_publish_one(Relation indexRel, int partition_id,
				   const MerkleNativeRootVersion *input)
{
	BlockNumber dirblock = native_directory_block(partition_id);
	Buffer dirbuf = ReadBuffer(indexRel, dirblock);
	Buffer appendbuf;
	Page dirpage;
	Page appendpage;
	MerkleNativePartitionEntry *entry;
	MerkleNativeRootVersion root = *input;
	GenericXLogState *state;
	Page target_dir;
	Page target_append;
	OffsetNumber offset;
	bool initialized = false;
	uint32 new_generation = 0;
	BlockNumber nblocks;
	BlockNumber appendblock;

	LockBuffer(dirbuf, BUFFER_LOCK_EXCLUSIVE);
	dirpage = BufferGetPage(dirbuf);
	native_validate_page(dirpage, MERKLE_NATIVE_PAGE_DIRECTORY, dirblock);
	entry = ((MerkleNativePartitionEntry *) PageGetContents(dirpage)) +
		native_directory_slot(partition_id);
	root.previous_version = entry->root_head;
	root.version_no = entry->last_allocated_version + 1;
	root.checksum = native_root_checksum(&root);

	nblocks = RelationGetNumberOfBlocks(indexRel);
	appendblock = nblocks > MERKLE_TREE_START_BLKNO ? nblocks - 1 : P_NEW;
	/* The newest block is a directory page during the first publication.  Do
	 * not try to lock the already-held directory buffer a second time. */
	if (appendblock == dirblock)
		appendblock = P_NEW;
	if (BlockNumberIsValid(appendblock) && appendblock != P_NEW)
	{
		appendbuf = ReadBuffer(indexRel, appendblock);
		LockBuffer(appendbuf, BUFFER_LOCK_EXCLUSIVE);
		appendpage = BufferGetPage(appendbuf);
		if (PageIsNew(appendpage) ||
			PageGetSpecialSize(appendpage) != MERKLE_NATIVE_PAGE_SPECIAL_SIZE ||
			MerkleNativePageGetOpaque(appendpage)->magic != MERKLE_NATIVE_PAGE_MAGIC ||
			MerkleNativePageGetOpaque(appendpage)->page_type != MERKLE_NATIVE_PAGE_APPEND ||
			!native_page_has_record_space(appendpage, sizeof(root)))
		{
			UnlockReleaseBuffer(appendbuf);
			appendblock = P_NEW;
		}
	}
	if (appendblock == P_NEW)
	{
		LockRelationForExtension(indexRel, ExclusiveLock);
		appendbuf = ReadBuffer(indexRel, P_NEW);
		LockBuffer(appendbuf, BUFFER_LOCK_EXCLUSIVE);
		/* See native_append_record(): do not expose an unlocked zero page. */
		UnlockRelationForExtension(indexRel, ExclusiveLock);
	}
	appendpage = BufferGetPage(appendbuf);
	if (PageIsNew(appendpage))
	{
		new_generation = 1;
		initialized = true;
	}

	state = GenericXLogStart(indexRel);
	target_dir = GenericXLogRegisterBuffer(state, dirbuf, 0);
	target_append = GenericXLogRegisterBuffer(state, appendbuf,
		initialized ? GENERIC_XLOG_FULL_IMAGE : 0);
	if (initialized)
	{
		MerkleNativePageOpaqueData *opaque;

		PageInit(target_append, BLCKSZ, MERKLE_NATIVE_PAGE_SPECIAL_SIZE);
		opaque = MerkleNativePageGetOpaque(target_append);
		opaque->magic = MERKLE_NATIVE_PAGE_MAGIC;
		opaque->version = MERKLE_NATIVE_PAGE_VERSION;
		opaque->page_type = MERKLE_NATIVE_PAGE_APPEND;
		opaque->page_generation = new_generation;
	}
	entry = ((MerkleNativePartitionEntry *) PageGetContents(target_dir)) +
		native_directory_slot(partition_id);
	offset = PageAddItem(target_append, (Item) &root, sizeof(root),
		InvalidOffsetNumber, false, false);
	if (!OffsetNumberIsValid(offset))
		elog(ERROR, "could not append native Merkle root version");
	entry->root_head.block = BufferGetBlockNumber(appendbuf);
	entry->root_head.offset = offset;
	entry->root_head.reserved = 0;
	entry->root_head.page_generation =
		MerkleNativePageGetOpaque(target_append)->page_generation;
	entry->last_allocated_version = root.version_no;
	GenericXLogFinish(state);
	merkle_crash_failpoint("after_native_root_wal_before_commit");
	UnlockReleaseBuffer(appendbuf);
	UnlockReleaseBuffer(dirbuf);
}

static void
native_lock_partition(Relation indexRel, int partition_id)
{
	LockDatabaseObject(MERKLE_AM_OID, RelationGetRelid(indexRel),
		(uint16) (partition_id + 1), ExclusiveLock);
}

void
merkle_native_build_from_oracle(Relation indexRel, uint64 baseline_apply_seq)
{
	NativeConfig config;
	Oid types[5] = {OIDOID, OIDOID, OIDOID, OIDOID, INT4OID};
	Datum args[5];
	char nulls[5] = {' ', ' ', ' ', ' ', ' '};
	int partition;

	native_read_config(indexRel, &config);
	for (partition = 0; partition < config.partitions; partition++)
		native_lock_partition(indexRel, partition);
	args[0] = ObjectIdGetDatum(RelationGetRelid(indexRel));
	args[1] = ObjectIdGetDatum(indexRel->rd_node.spcNode);
	args[2] = ObjectIdGetDatum(indexRel->rd_node.dbNode);
	args[3] = ObjectIdGetDatum(indexRel->rd_node.relNode);
	for (partition = 0; partition < config.partitions; partition++)
	{
		NativePartitionSpool *spool = native_spool_create();
		Portal portal;
		bool done = false;

		args[4] = Int32GetDatum(partition);
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "native Merkle build SPI_connect failed");
		portal = SPI_cursor_open_with_args(NULL,
			"SELECT route_digest,key_data,tuple_hash "
			"FROM ariabc_internal.merkle_dynamic_leaf_item "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4 "
			"AND partition_id=$5 ORDER BY route_digest,key_data",
			5, types, args, nulls, true, 0);
		if (portal == NULL)
			elog(ERROR, "native Merkle build partition cursor failed");
		while (!done)
		{
			uint64 row;

			SPI_cursor_fetch(portal, true, 1024);
			done = SPI_processed == 0;
			for (row = 0; row < SPI_processed; row++)
			{
				HeapTuple tuple = SPI_tuptable->vals[row];
				TupleDesc desc = SPI_tuptable->tupdesc;
				NativeItem item;
				bool isnull;
				bytea *value;

				value = DatumGetByteaPP(SPI_getbinval(tuple, desc, 1,
					&isnull));
				if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
					elog(ERROR, "invalid native Merkle build route");
				memcpy(item.route, VARDATA_ANY(value), MERKLE_HASH_BYTES);
				value = DatumGetByteaPP(SPI_getbinval(tuple, desc, 2,
					&isnull));
				if (isnull || VARSIZE_ANY_EXHDR(value) > config.max_key_bytes)
					elog(ERROR, "invalid native Merkle build key");
				item.key_length = VARSIZE_ANY_EXHDR(value);
				item.key = VARDATA_ANY(value);
				if (native_item_bytes(&item) > config.leaf_byte_capacity)
					elog(ERROR, "invalid native Merkle build key size");
				value = DatumGetByteaPP(SPI_getbinval(tuple, desc, 3,
					&isnull));
				if (isnull || VARSIZE_ANY_EXHDR(value) != MERKLE_HASH_BYTES)
					elog(ERROR, "invalid native Merkle build tuple hash");
				memcpy(item.hash.data, VARDATA_ANY(value), MERKLE_HASH_BYTES);
				native_spool_append(spool, &item);
			}
		}
		SPI_cursor_close(portal);
		SPI_finish();
		native_publish_build_partition(indexRel, &config, partition, spool,
			baseline_apply_seq);
		native_spool_close(spool);
	}
}

static void
native_collect_items(Relation indexRel, const MerkleNativeLocator *locator,
					 NativeItemVector *vector)
{
	MerkleNativeNodeRecord *node = native_read_node(indexRel, locator);

	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
		native_load_leaf_items(indexRel, node, vector);
	else
	{
		native_collect_items(indexRel, &node->left, vector);
		native_collect_items(indexRel, &node->right, vector);
	}
	pfree(node);
}

static int
native_find_item(NativeItemVector *vector, const uint8 route[MERKLE_HASH_BYTES],
				 const char *key, uint32 key_length)
{
	int i;

	for (i = 0; i < vector->count; i++)
		if (memcmp(vector->items[i].route, route, MERKLE_HASH_BYTES) == 0 &&
			vector->items[i].key_length == key_length &&
			memcmp(vector->items[i].key, key, key_length) == 0)
			return i;
	return -1;
}

static void
native_apply_items(NativeItemVector *vector,
				   const MerkleDynamicTransition *transitions, int count)
{
	int i;

	for (i = 0; i < count; i++)
	{
		const MerkleDynamicTransition *transition = &transitions[i];
		uint32 key_length = VARSIZE_ANY_EXHDR(transition->key_data);
		const char *key = VARDATA_ANY(transition->key_data);
		int found = native_find_item(vector, transition->route_digest,
			key, key_length);

		if (transition->has_old)
		{
			if (found < 0 || memcmp(vector->items[found].hash.data,
				transition->old_hash.data, MERKLE_HASH_BYTES) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("native Merkle old item does not match committed tree")));
		}
		else if (found >= 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("native Merkle insert key already exists")));
		if (transition->has_new)
		{
			if (found >= 0)
				vector->items[found].hash = transition->new_hash;
			else
			{
				NativeItem item;

				memcpy(item.route, transition->route_digest,
					MERKLE_HASH_BYTES);
				item.hash = transition->new_hash;
				item.key_length = key_length;
				item.key = palloc(key_length);
				memcpy(item.key, key, key_length);
				native_vector_push(vector, &item);
			}
		}
		else
		{
			Assert(found >= 0);
			/* The key is owned by the vector.  Release it before moving the
			 * remaining entries so deletes do not leak one allocation per
			 * transition. */
			if (vector->items[found].key != NULL)
				pfree(vector->items[found].key);
			if (found + 1 < vector->count)
				memmove(&vector->items[found], &vector->items[found + 1],
					(vector->count - found - 1) * sizeof(*vector->items));
			vector->count--;
		}
	}
}

static MerkleNativeLocator
native_apply_batch_node(Relation indexRel, const NativeConfig *config,
						int partition, const MerkleNativeLocator *locator,
						const MerkleDynamicTransition *transitions, int count,
						int minimum_prefix, MerkleNativeNodeRecord *summary,
						uint64 *split_counter, uint64 *merge_counter)
{
	MerkleNativeNodeRecord *old = native_read_node(indexRel, locator);
	int i;

	/*
	 * Patricia-style prefix divergence (plan_left.md §8).
	 *
	 * When one or more incoming routes diverge before the old node's
	 * compressed prefix we must create a new branch at the first
	 * differing bit (the Longest Common Prefix of old_prefix and each
	 * diverging route).
	 *
	 * Old design: native_collect_items() entire old subtree → O(N items).
	 * New design: compute LCP → reuse old subtree locator as one child →
	 *             build only the new item path → O(depth + new items).
	 *
	 * The old subtree locator is reused verbatim; its immutable records
	 * are never touched.  Only the new ancestors (from LCP to root) are
	 * written.
	 *
	 * We handle the diverging transitions first.  Any transitions that DO
	 * match the old prefix are applied recursively as normal.
	 */
	{
		bool has_diverging = false;
		int diverge_bit = MERKLE_HASH_BITS; /* LCP of all diverging routes */
		int j;

		for (i = 0; i < count; i++)
		{
			if (!native_route_has_prefix(transitions[i].route_digest,
				old->prefix, old->prefix_len))
			{
				/* Find the LCP of old->prefix and this new route. */
				int lcp;

				for (lcp = minimum_prefix; lcp < old->prefix_len; lcp++)
				{
					if (native_route_bit(transitions[i].route_digest, lcp) !=
						native_route_bit(old->prefix, lcp))
						break;
				}
				if (lcp < diverge_bit)
					diverge_bit = lcp;
				has_diverging = true;
			}
		}

		if (has_diverging)
		{
			/*
			 * Split the transition batch into:
			 *   - diverging_transitions (those that don't share old prefix)
			 *   - matching_transitions (those that share the full old prefix)
			 *
			 * Build the new branch node at diverge_bit.
			 * One child = old subtree (reused locator + its cached summary).
			 * Other child = built from matching+diverging routes that go that way.
			 */
			int old_side;     /* bit at diverge_bit for old subtree */
			int old_count = 0;
			int new_count = 0;
			MerkleDynamicTransition *old_batch;
			MerkleDynamicTransition *new_batch;
			MerkleNativeNodeRecord branch_left;
			MerkleNativeNodeRecord branch_right;
			MerkleNativeNodeRecord old_side_node;
			MerkleNativeLocator old_side_locator;
			MerkleNativeLocator branch_left_locator;
			MerkleNativeLocator branch_right_locator;
			MerkleNativeNodeRecord result_node;
			MerkleNativeLocator result_loc;

			old_side = native_route_bit(old->prefix, diverge_bit);
			old_batch = palloc(sizeof(*old_batch) * count);
			new_batch = palloc(sizeof(*new_batch) * count);
			for (j = 0; j < count; j++)
			{
				if (native_route_bit(transitions[j].route_digest,
					diverge_bit) == old_side)
					old_batch[old_count++] = transitions[j];
				else
					new_batch[new_count++] = transitions[j];
			}

			/* The old-side batch may contain routes that diverge again at a
			 * deeper bit; recurse so each depth gets its own partition. */
			if (old_count > 0)
				old_side_locator = native_apply_batch_node(indexRel, config,
					partition, locator, old_batch, old_count, diverge_bit + 1,
					&old_side_node, split_counter, merge_counter);
			else
			{
				old_side_node = *old;
				old_side_locator = *locator;
			}
			if (old_side == 0)
			{
				branch_left = old_side_node;
				branch_left_locator = old_side_locator;
			}
			else
			{
				branch_right = old_side_node;
				branch_right_locator = old_side_locator;
			}
			{
				NativeItemVector new_items = {0};

				for (j = 0; j < new_count; j++)
				{
					NativeItem item;
					uint32 klen;

					if (!new_batch[j].has_new)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("native Merkle diverging delete targets non-existent item")));
					klen = VARSIZE_ANY_EXHDR(new_batch[j].key_data);
					memcpy(item.route, new_batch[j].route_digest, MERKLE_HASH_BYTES);
					item.hash = new_batch[j].new_hash;
					item.key_length = klen;
					item.key = palloc(klen);
					memcpy(item.key, VARDATA_ANY(new_batch[j].key_data), klen);
					native_vector_push(&new_items, &item);
				}
				if (new_items.count == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("native Merkle divergence produced an empty branch")));
				if (old_side == 0)
					branch_right_locator = native_build_subtree(indexRel, config,
						partition, new_items.items, new_items.count,
						diverge_bit + 1, &branch_right, split_counter);
				else
					branch_left_locator = native_build_subtree(indexRel, config,
						partition, new_items.items, new_items.count,
						diverge_bit + 1, &branch_left, split_counter);
				native_vector_free(&new_items);
			}

			/* Build the new branch internal node. */
			MemSet(&result_node, 0, sizeof(result_node));
			result_node.header.magic   = MERKLE_NATIVE_RECORD_MAGIC;
			result_node.header.version = MERKLE_NATIVE_RECORD_VERSION;
			result_node.header.type    = MERKLE_NATIVE_RECORD_INTERNAL;
			result_node.header.size    = sizeof(result_node);
			result_node.partition_id   = partition;
			result_node.prefix_len     = diverge_bit;
			/* Prefix bytes up to diverge_bit are shared with old->prefix. */
			native_canonical_prefix(old->prefix, diverge_bit,
				result_node.prefix);
			result_node.tuple_count  = branch_left.tuple_count +
				branch_right.tuple_count;
			result_node.subtree_bytes = branch_left.subtree_bytes +
				branch_right.subtree_bytes;
			result_node.data_xor = branch_left.data_xor;
			merkle_hash_xor(&result_node.data_xor, &branch_right.data_xor);
			result_node.content_xor = branch_left.content_xor;
			merkle_hash_xor(&result_node.content_xor, &branch_right.content_xor);
			result_node.left  = branch_left_locator;
			result_node.right = branch_right_locator;
			native_invalid_locator(&result_node.item_head);
			native_hash_internal(&branch_left, &branch_right, partition,
				diverge_bit, result_node.prefix, &result_node.data_xor,
				result_node.tuple_count, result_node.subtree_bytes,
				&result_node.structure_hash);
			result_node.header.checksum = native_record_checksum(&result_node,
				sizeof(result_node));
			result_loc = native_append_record(indexRel, &result_node,
												 sizeof(result_node));
			if (split_counter != NULL)
				(*split_counter)++;
			*summary = result_node;
			pfree(old);
			pfree(old_batch);
			pfree(new_batch);
			return result_loc;
		}
	}

	if ((old->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		NativeItemVector vector = {0};
		MerkleNativeLocator result;

		native_load_leaf_items(indexRel, old, &vector);
		native_apply_items(&vector, transitions, count);
		result = native_build_subtree(indexRel, config, partition,
			vector.items, vector.count, minimum_prefix, summary, split_counter);
		native_vector_free(&vector);
		pfree(old);
		return result;
	}
	else
	{
		int split = 0;
		MerkleNativeNodeRecord left;
		MerkleNativeNodeRecord right;
		MerkleNativeLocator left_locator = old->left;
		MerkleNativeLocator right_locator = old->right;
		MerkleNativeLocator result;

		while (split < count &&
			native_route_bit(transitions[split].route_digest,
				old->prefix_len) == 0)
			split++;
		if (split > 0)
			left_locator = native_apply_batch_node(indexRel, config, partition,
				&old->left, transitions, split, old->prefix_len + 1, &left,
				split_counter, merge_counter);
		else
		{
			MerkleNativeNodeRecord *loaded = native_read_node(indexRel, &old->left);
			left = *loaded;
			pfree(loaded);
		}
		if (split < count)
			right_locator = native_apply_batch_node(indexRel, config, partition,
				&old->right, transitions + split, count - split,
				old->prefix_len + 1, &right, split_counter, merge_counter);
		else
		{
			MerkleNativeNodeRecord *loaded = native_read_node(indexRel, &old->right);
			right = *loaded;
			pfree(loaded);
		}
		if (left.tuple_count == 0)
		{
			*summary = right;
			pfree(old);
			return right_locator;
		}
		if (right.tuple_count == 0)
		{
			*summary = left;
			pfree(old);
			return left_locator;
		}
		/*
		 * Merge only when BOTH the count threshold and the byte-capacity
		 * bound are satisfied.  A count-only check allowed merging two
		 * children with large composite keys that could not physically fit
		 * inside one leaf record.
		 */
		if (left.tuple_count + right.tuple_count <=
			(uint64) config->merge_threshold &&
			left.tuple_count + right.tuple_count <=
			(uint64) config->leaf_capacity &&
			left.subtree_bytes + right.subtree_bytes <=
			config->leaf_byte_capacity)
		{
			NativeItemVector vector = {0};

			native_collect_items(indexRel, &left_locator, &vector);
			native_collect_items(indexRel, &right_locator, &vector);
			result = native_write_leaf(indexRel, partition, old->prefix_len,
				old->prefix, vector.items, vector.count, summary);
			if (merge_counter != NULL)
				(*merge_counter)++;
			native_vector_free(&vector);
			pfree(old);
			return result;
		}
		*summary = *old;
		summary->tuple_count = left.tuple_count + right.tuple_count;
		summary->subtree_bytes = left.subtree_bytes + right.subtree_bytes;
		summary->data_xor = left.data_xor;
		merkle_hash_xor(&summary->data_xor, &right.data_xor);
		summary->content_xor = left.content_xor;
		merkle_hash_xor(&summary->content_xor, &right.content_xor);
		summary->left = left_locator;
		summary->right = right_locator;
		native_hash_internal(&left, &right, partition, summary->prefix_len,
			summary->prefix, &summary->data_xor, summary->tuple_count,
			summary->subtree_bytes, &summary->structure_hash);
		summary->header.checksum = native_record_checksum(summary,
			sizeof(*summary));
		result = native_append_record(indexRel, summary, sizeof(*summary));
		pfree(old);
		return result;
	}
}

static void
native_apply_transitions_authorized(
							const MerkleDynamicTransition *transitions,
							int count, uint16 sequence_domain,
							uint64 sequence_epoch, uint64 sequence_value,
							int expected_mode)
{
	Relation indexRel;
	NativeConfig config;
	int i;
	uint64 profile_splits = 0;
	uint64 profile_merges = 0;

	if (count <= 0)
		return;
	if (!OidIsValid(transitions[0].index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("native Merkle transition has invalid index OID %u",
					 transitions[0].index_oid),
				 errdetail("mutation authority=%s",
					 expected_mode == MERKLE_UPDATE_PENDING_LOG ?
					 "pending_log" : "synchronous_cow")));
	indexRel = index_open(transitions[0].index_oid, RowExclusiveLock);
	if (expected_mode >= 0 && merkle_get_update_mode(indexRel) != expected_mode)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("native Merkle mutation authority does not match index update mode"),
				 errdetail("requested authority %s but index is configured %s",
					 expected_mode == MERKLE_UPDATE_SYNCHRONOUS_COW ?
					 "synchronous_cow" : "pending_log",
					 merkle_get_update_mode(indexRel) ==
					 MERKLE_UPDATE_SYNCHRONOUS_COW ?
					 "synchronous_cow" : "pending_log")));
	if (!merkle_native_is_ready(indexRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("dynamic Merkle index requires REINDEX for native layout v%d",
						MERKLE_DYNAMIC_LAYOUT_VERSION)));
	native_read_config(indexRel, &config);
	for (i = 0; i < count; i++)
	{
		int p = transitions[i].partition_id;

		if (transitions[i].index_oid != RelationGetRelid(indexRel) ||
			p < 0 || p >= config.partitions)
			elog(ERROR, "invalid native Merkle transition batch");
		if (i > 0 && transitions[i - 1].partition_id > p)
			elog(ERROR, "native Merkle transitions are not partition ordered");
	}
	for (i = 0; i < count; )
	{
		int partition = transitions[i].partition_id;
		int end = i + 1;

		native_lock_partition(indexRel, partition);
		while (end < count && transitions[end].partition_id == partition)
			end++;
		i = end;
	}
	for (i = 0; i < count; )
	{
		int partition = transitions[i].partition_id;
		int end = i + 1;
		MerkleNativeRootVersion old_root;
		MerkleNativeRootVersion root;
		MerkleNativeNodeRecord node;
		MerkleNativeLocator root_node;
		uint64 split_count = 0;
		uint64 merge_count = 0;

		while (end < count && transitions[end].partition_id == partition)
			end++;
		if (!native_latest_root_for_write(indexRel, partition, &old_root) ||
			!native_locator_valid(&old_root.root_node))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle partition %d has no materialized visible root",
							partition)));
		root_node = native_apply_batch_node(indexRel, &config, partition,
			&old_root.root_node, transitions + i, end - i, 0, &node,
			merkle_native_profile_enabled ? &split_count : NULL,
			merkle_native_profile_enabled ? &merge_count : NULL);
		profile_splits += split_count;
		profile_merges += merge_count;
		merkle_crash_failpoint("before_native_root_publication");
		MemSet(&root, 0, sizeof(root));
		root.magic = MERKLE_NATIVE_ROOT_MAGIC;
		root.version = MERKLE_NATIVE_ROOT_VERSION;
		root.creator_xid = GetTopTransactionId();
		root.partition_id = partition;
		root.sequence_domain = sequence_domain;
		root.sequence_epoch = sequence_epoch;
		root.sequence_value = sequence_value;
		root.tuple_count = node.tuple_count;
		root.subtree_bytes = node.subtree_bytes;
		root.data_xor = node.data_xor;
		root.content_xor = node.content_xor;
		root.structure_hash = node.structure_hash;
		root.root_node = root_node;
		native_publish_one(indexRel, partition, &root);
		i = end;
	}
	if (merkle_native_profile_enabled && (profile_splits > 0 || profile_merges > 0))
	{
		Datum args[6];
		Oid types[6] = {OIDOID, OIDOID, OIDOID, OIDOID, INT8OID, INT8OID};
		Oid fallback_types[3] = {OIDOID, INT8OID, INT8OID};
		Datum fallback_args[3];
		int rc;

		args[0] = ObjectIdGetDatum(RelationGetRelid(indexRel));
		args[1] = ObjectIdGetDatum(indexRel->rd_node.spcNode);
		args[2] = ObjectIdGetDatum(indexRel->rd_node.dbNode);
		args[3] = ObjectIdGetDatum(indexRel->rd_node.relNode);
		args[4] = Int64GetDatum((int64) profile_splits);
		args[5] = Int64GetDatum((int64) profile_merges);
		ereport(LOG,
			(errmsg("NATIVE_MERKLE_PROFILE index_oid=%u splits=%llu merges=%llu",
					RelationGetRelid(indexRel),
					(unsigned long long) profile_splits,
					(unsigned long long) profile_merges)));
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "could not connect to SPI for native Merkle profiling");
		rc = SPI_execute_with_args(
			"UPDATE ariabc_internal.merkle_dynamic_state "
			"SET split_count=split_count+$5, merge_count=merge_count+$6, "
			"updated_at=clock_timestamp() "
			"WHERE index_oid=$1 AND rnode_spc=$2 AND rnode_db=$3 AND rnode_rel=$4",
			6, types, args, NULL, false, 0);
		/* A REINDEX/build race can leave the catalog generation tuple newer
		 * than the relation opened for this apply.  Preserve the profile
		 * rather than dropping it; the index OID is still an unambiguous
		 * profile owner and the state table may contain one live generation. */
		if (rc == SPI_OK_UPDATE && SPI_processed == 0)
		{
			fallback_args[0] = args[0];
			fallback_args[1] = args[4];
			fallback_args[2] = args[5];
			rc = SPI_execute_with_args(
				"UPDATE ariabc_internal.merkle_dynamic_state "
				"SET split_count=split_count+$2, merge_count=merge_count+$3, "
				"updated_at=clock_timestamp() WHERE index_oid=$1",
				3, fallback_types, fallback_args, NULL, false, 0);
		}
		if (rc != SPI_OK_UPDATE || SPI_processed < 1)
			ereport(DEBUG1,
				(errmsg("native Merkle profiling state update skipped: no matching dynamic state row")));
		SPI_finish();
	}
	index_close(indexRel, RowExclusiveLock);
}

void
merkle_native_publish_strict_transitions(
							const MerkleDynamicTransition *transitions,
							int count, uint16 sequence_domain,
							uint64 sequence_epoch, uint64 sequence_value)
{
	native_apply_transitions_authorized(transitions, count, sequence_domain,
		sequence_epoch, sequence_value, MERKLE_UPDATE_SYNCHRONOUS_COW);
}

void
merkle_native_materialize_pending_transitions(
							const MerkleDynamicTransition *transitions,
							int count, uint16 sequence_domain,
							uint64 sequence_epoch, uint64 sequence_value)
{
	native_apply_transitions_authorized(transitions, count, sequence_domain,
		sequence_epoch, sequence_value, MERKLE_UPDATE_PENDING_LOG);
}

/*
 * merkle_native_root_combined
 *
 * Compute the combined (data + structure) global root by hashing ordered
 * partition content commitments AND structure_hash values together.
 *
 * This closes the correctness gap identified in plan_left.md §2: the old
 * root committed only to the data multiset (data_xor) and tuple counts.
 * Two trees with identical data but different prefix topology produced the
 * same root.  Each node now carries a topology-independent XOR of canonical
 * item commitments for the data root, while structure_hash remains the
 * topology-sensitive commitment.  Both global roots are therefore O(partitions)
 * to read and have genuinely separate semantics.
 *
 * combined_root = H(index-domain
 *                   || layout_version
 *                   || route_hash_version
 *                   || row_hash_version
 *                   || partition_count
 *                   || ordered partition_data_xors
 *                   || ordered partition_structure_hashes)
 */
static void
native_compute_commitments(Relation indexRel,
						   MerkleHash *data_root,
						   MerkleHash *structure_root,
						   MerkleHash *combined_root,
						   uint64 *tuple_count)
{
	blake3_hasher data_hasher;
	blake3_hasher structure_hasher;
	blake3_hasher combined_hasher;
	static const uint8 data_domain[] = {'A','R','I','D','A','T','A','1'};
	static const uint8 structure_domain[] = {'A','R','I','S','T','R','1'};
	static const uint8 combined_domain[] = {'A','R','I','D','Y','N','R','3'};
	uint32 layout_v = pg_hton32(MERKLE_DYNAMIC_LAYOUT_VERSION);
	uint32 route_v  = pg_hton32(MERKLE_ROUTE_FORMAT_VERSION);
	uint32 row_v    = pg_hton32(MERKLE_ROW_HASH_FORMAT_VERSION);
	int partitions;
	uint32 network_partitions;
	uint64 total = 0;
	int partition;
	MerkleNativeRootVersion *roots;

	merkle_read_meta(indexRel, &partitions, NULL, NULL, NULL, NULL,
		NULL, NULL, NULL);
	roots = palloc(sizeof(*roots) * partitions);
	for (partition = 0; partition < partitions; partition++)
	{
		if (!native_visible_root(indexRel, partition, &roots[partition]))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle partition %d has no visible root", partition)));
		total += roots[partition].tuple_count;
	}
	network_partitions = pg_hton32((uint32) partitions);
	blake3_hasher_init(&data_hasher);
	blake3_hasher_update(&data_hasher, data_domain, sizeof(data_domain));
	blake3_hasher_update(&data_hasher, &layout_v, sizeof(layout_v));
	blake3_hasher_update(&data_hasher, &route_v, sizeof(route_v));
	blake3_hasher_update(&data_hasher, &row_v, sizeof(row_v));
	blake3_hasher_update(&data_hasher, &network_partitions,
		sizeof(network_partitions));
	blake3_hasher_init(&structure_hasher);
	blake3_hasher_update(&structure_hasher, structure_domain,
		sizeof(structure_domain));
	blake3_hasher_update(&structure_hasher, &layout_v, sizeof(layout_v));
	blake3_hasher_update(&structure_hasher, &network_partitions,
		sizeof(network_partitions));
	for (partition = 0; partition < partitions; partition++)
	{
		uint32 p     = pg_hton32((uint32) partition);
		uint64 count = pg_hton64(roots[partition].tuple_count);

		blake3_hasher_update(&data_hasher, &p, sizeof(p));
		blake3_hasher_update(&data_hasher, &count, sizeof(count));
		/* Content is an order-independent XOR of canonical item commitments;
		 * unlike structure_hash it is intentionally independent of topology. */
		blake3_hasher_update(&data_hasher,
			roots[partition].content_xor.data, MERKLE_HASH_BYTES);
		blake3_hasher_update(&structure_hasher, &p, sizeof(p));
		blake3_hasher_update(&structure_hasher, &count, sizeof(count));
		blake3_hasher_update(&structure_hasher,
			roots[partition].structure_hash.data,
			MERKLE_HASH_BYTES);
	}
	blake3_hasher_finalize(&data_hasher, data_root->data, MERKLE_HASH_BYTES);
	blake3_hasher_finalize(&structure_hasher, structure_root->data,
		MERKLE_HASH_BYTES);
	blake3_hasher_init(&combined_hasher);
	blake3_hasher_update(&combined_hasher, combined_domain,
		sizeof(combined_domain));
	blake3_hasher_update(&combined_hasher, &layout_v, sizeof(layout_v));
	blake3_hasher_update(&combined_hasher, &route_v, sizeof(route_v));
	blake3_hasher_update(&combined_hasher, &row_v, sizeof(row_v));
	blake3_hasher_update(&combined_hasher, &network_partitions,
		sizeof(network_partitions));
	blake3_hasher_update(&combined_hasher, data_root->data, MERKLE_HASH_BYTES);
	blake3_hasher_update(&combined_hasher, structure_root->data,
		MERKLE_HASH_BYTES);
	blake3_hasher_finalize(&combined_hasher, combined_root->data,
		MERKLE_HASH_BYTES);
	if (tuple_count != NULL)
		*tuple_count = total;
	pfree(roots);
}

void
merkle_native_root(Relation indexRel, MerkleHash *hash, uint64 *tuple_count)
{
	MerkleHash data_root;
	MerkleHash structure_root;

	native_compute_commitments(indexRel, &data_root, &structure_root, hash,
		 tuple_count);
}

static bool
native_verify_node(Relation indexRel, const NativeConfig *config,
				   int partition, const MerkleNativeLocator *locator,
				   Tuplesortstate *native_sort,
				   MerkleNativeNodeRecord *summary,
				   int depth)
{
	MerkleNativeNodeRecord *node;
	uint8 canonical[MERKLE_HASH_BYTES];
	bool match;

	if (depth > MERKLE_HASH_BITS + 1)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle verification exceeded maximum tree depth")));
	node = native_read_node(indexRel, locator);
	match = node->partition_id == (uint32) partition;
	if (node->prefix_len > MERKLE_HASH_BITS)
		match = false;
	else
		native_canonical_prefix(node->prefix, node->prefix_len, canonical);
	if (node->prefix_len > MERKLE_HASH_BITS ||
		memcmp(canonical, node->prefix, MERKLE_HASH_BYTES) != 0 ||
		(node->flags & ~MERKLE_NATIVE_NODE_LEAF) != 0)
		match = false;

	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		NativeItemVector leaf = {0};
		MerkleHash xor;
		MerkleHash content;
		MerkleHash structure;
		uint64 bytes = 0;
		int i;

		native_load_leaf_items(indexRel, node, &leaf);
		merkle_hash_zero(&xor);
		merkle_hash_zero(&content);
		for (i = 0; i < leaf.count; i++)
		{
			uint8 prefix[MERKLE_HASH_BYTES];
			MerkleHash item_content;

			native_canonical_prefix(leaf.items[i].route, node->prefix_len,
				prefix);
			if (memcmp(prefix, node->prefix, MERKLE_HASH_BYTES) != 0)
				match = false;
			merkle_hash_xor(&xor, &leaf.items[i].hash);
			native_hash_item_content(&leaf.items[i], &item_content);
			merkle_hash_xor(&content, &item_content);
			bytes += native_item_bytes(&leaf.items[i]);
			{
				bytea *packed = native_pack_sort_item(partition, &leaf.items[i]);

				tuplesort_putdatum(native_sort, PointerGetDatum(packed), false);
				pfree(packed);
			}
		}
		native_hash_leaf(partition, node->prefix_len, node->prefix,
			leaf.items, leaf.count, bytes, &xor, &structure);
		if (leaf.count > config->leaf_capacity ||
			bytes > config->leaf_byte_capacity ||
			node->tuple_count != (uint64) leaf.count ||
			node->subtree_bytes != bytes ||
			memcmp(node->data_xor.data, xor.data, MERKLE_HASH_BYTES) != 0 ||
			memcmp(node->content_xor.data, content.data, MERKLE_HASH_BYTES) != 0 ||
			memcmp(node->structure_hash.data, structure.data,
				MERKLE_HASH_BYTES) != 0)
			match = false;
		native_vector_free(&leaf);
	}
	else
	{
		MerkleNativeNodeRecord left;
		MerkleNativeNodeRecord right;
		MerkleHash xor;
		MerkleHash content;
		MerkleHash structure;

		if (!native_locator_valid(&node->left) ||
			!native_locator_valid(&node->right))
			match = false;
		else
		{
			if (!native_verify_node(indexRel, config, partition, &node->left,
				native_sort, &left, depth + 1))
				match = false;
			if (!native_verify_node(indexRel, config, partition, &node->right,
				native_sort, &right, depth + 1))
				match = false;
			xor = left.data_xor;
			merkle_hash_xor(&xor, &right.data_xor);
			content = left.content_xor;
			merkle_hash_xor(&content, &right.content_xor);
			native_hash_internal(&left, &right, partition, node->prefix_len,
				node->prefix, &xor, left.tuple_count + right.tuple_count,
				left.subtree_bytes + right.subtree_bytes, &structure);
			if (left.prefix_len <= node->prefix_len ||
				right.prefix_len <= node->prefix_len ||
				!native_route_has_prefix(left.prefix, node->prefix,
					node->prefix_len) ||
				!native_route_has_prefix(right.prefix, node->prefix,
					node->prefix_len) ||
				native_route_bit(left.prefix, node->prefix_len) != 0 ||
				native_route_bit(right.prefix, node->prefix_len) != 1 ||
				node->tuple_count != left.tuple_count + right.tuple_count ||
				node->subtree_bytes != left.subtree_bytes + right.subtree_bytes ||
				memcmp(node->data_xor.data, xor.data, MERKLE_HASH_BYTES) != 0 ||
				memcmp(node->content_xor.data, content.data, MERKLE_HASH_BYTES) != 0 ||
				memcmp(node->structure_hash.data, structure.data,
					MERKLE_HASH_BYTES) != 0)
				match = false;
		}
	}
	*summary = *node;
	pfree(node);
	return match;
}

bool
merkle_native_verify_relations(Relation heapRel, Relation indexRel,
							Snapshot snapshot)
{
	NativeConfig config;
	int nkeys = indexRel->rd_index->indnkeyatts;
	Datum *values = palloc(sizeof(*values) * nkeys);
	bool *nulls = palloc(sizeof(*nulls) * nkeys);
	bool match = true;
	Tuplesortstate *native_sort;
	Tuplesortstate *heap_sort;
	int partition;

	native_read_config(indexRel, &config);
	native_sort = tuplesort_begin_datum(BYTEAOID, ByteaLessOperator, InvalidOid,
		false, work_mem, NULL, false);
	heap_sort = tuplesort_begin_datum(BYTEAOID, ByteaLessOperator, InvalidOid,
		false, work_mem, NULL, false);

	/*
	 * Verify every native partition first, then scan the heap exactly once.
	 * The prior implementation rescanned the heap once per partition, which
	 * made verification O(partitions * heap_rows) for a perfectly healthy
	 * index. Both sides now use PostgreSQL datum tuplesorts, so memory is
	 * bounded by work_mem and large indexes spill to temporary files.
	 */
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativeRootVersion root;
		MerkleNativeNodeRecord node;
		bool partition_match;

		/* ---- Structural verification of the native partition tree ---- */
		partition_match = native_visible_root(indexRel, partition, &root);
		if (partition_match)
			partition_match = native_locator_valid(&root.root_node) &&
				native_verify_node(indexRel, &config, partition,
					&root.root_node, native_sort, &node, 0);
		if (partition_match)
			partition_match = root.tuple_count == node.tuple_count &&
				root.subtree_bytes == node.subtree_bytes &&
				memcmp(root.data_xor.data, node.data_xor.data,
					MERKLE_HASH_BYTES) == 0 &&
				memcmp(root.content_xor.data, node.content_xor.data,
					MERKLE_HASH_BYTES) == 0 &&
				memcmp(root.structure_hash.data, node.structure_hash.data,
					MERKLE_HASH_BYTES) == 0;
		if (!partition_match)
			match = false;
	}

	/* ---- One heap scan: route each tuple into the spillable sort ---- */
	{
		TableScanDesc scan = table_beginscan(heapRel, snapshot, 0, NULL);
		TupleTableSlot *slot = table_slot_create(heapRel, NULL);
		int i;

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			MerkleItemIdentity identity;
			NativeItem item;

			for (i = 0; i < nkeys; i++)
			{
				AttrNumber attno = indexRel->rd_index->indkey.values[i];

				if (attno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("native Merkle verification does not support expression keys")));
				values[i] = slot_getattr(slot, attno, &nulls[i]);
			}
			merkle_compute_dynamic_item_identity(indexRel, values, nulls,
				nkeys, config.partitions, config.max_key_bytes, &identity);
			memcpy(item.route, identity.route.route_digest, MERKLE_HASH_BYTES);
			merkle_compute_slot_hash(heapRel, slot, &item.hash);
			item.key_length = VARSIZE_ANY_EXHDR(identity.key_data);
			item.key = palloc(item.key_length);
			memcpy(item.key, VARDATA_ANY(identity.key_data), item.key_length);
			{
				bytea *packed = native_pack_sort_item(identity.route.partition_id,
					&item);

				tuplesort_putdatum(heap_sort, PointerGetDatum(packed), false);
				pfree(packed);
			}
			pfree(item.key);
			pfree(identity.key_data);
			ExecClearTuple(slot);
		}
		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);
	}

	/* ---- Compare the two canonical spill streams ---- */
	tuplesort_performsort(native_sort);
	tuplesort_performsort(heap_sort);
	{
		Datum native_value;
		Datum heap_value;
		bool native_null;
		bool heap_null;
		bool native_has;
		bool heap_has;

		do
		{
			native_has = tuplesort_getdatum(native_sort, true,
				&native_value, &native_null, NULL);
			heap_has = tuplesort_getdatum(heap_sort, true,
				&heap_value, &heap_null, NULL);
			if (native_has != heap_has ||
				(native_has && (native_null != heap_null ||
					(native_null == false &&
					 native_compare_packed(native_value, heap_value) != 0))))
				match = false;
			if (native_has && !native_null)
				pfree(DatumGetPointer(native_value));
			if (heap_has && !heap_null)
				pfree(DatumGetPointer(heap_value));
		} while (native_has && heap_has && match);
	}
	tuplesort_end(native_sort);
	tuplesort_end(heap_sort);
	pfree(values);
	pfree(nulls);
	return match;
}


static void
native_mark_tree(Relation indexRel, const MerkleNativeLocator *locator,
				 bool *reachable, BlockNumber nblocks, int depth)
{
	MerkleNativeNodeRecord *node;

	if (!native_locator_valid(locator) || locator->block >= nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle reachable locator is out of bounds")));
	if (depth > MERKLE_HASH_BITS + 1)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("native Merkle tree depth is invalid")));
	reachable[locator->block] = true;
	node = native_read_node(indexRel, locator);
	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		MerkleNativeLocator item = node->item_head;
		uint64 seen = 0;

		while (native_locator_valid(&item))
		{
			MerkleNativeRecordHeader *record;
			Size size;

			if (item.block >= nblocks)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("native Merkle item chain is invalid during VACUUM")));
			reachable[item.block] = true;
			record = native_read_record(indexRel, &item,
				0, sizeof(*record), &size);
			if (record->type == MERKLE_NATIVE_RECORD_ITEM)
			{
				MerkleNativeItemRecord *single =
					(MerkleNativeItemRecord *) record;

				item = single->next;
				seen++;
			}
			else if (record->type == MERKLE_NATIVE_RECORD_ITEM_CHUNK &&
					 size >= sizeof(MerkleNativeItemChunkRecord))
			{
				MerkleNativeItemChunkRecord *chunk =
					(MerkleNativeItemChunkRecord *) record;

				item = chunk->next;
				seen += chunk->item_count;
			}
			else
				elog(ERROR, "invalid native Merkle item record during VACUUM");
			pfree(record);
			if (seen > node->tuple_count)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("native Merkle item chain is invalid during VACUUM")));
		}
		if (seen != node->tuple_count)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("native Merkle item count is invalid during VACUUM")));
	}
	else
	{
		native_mark_tree(indexRel, &node->left, reachable, nblocks, depth + 1);
		native_mark_tree(indexRel, &node->right, reachable, nblocks, depth + 1);
	}
	pfree(node);
}

static void
native_hint_root(Relation indexRel, const MerkleNativeLocator *locator,
				 bool freeze, bool aborted)
{
	Buffer buffer = ReadBuffer(indexRel, locator->block);
	Page page;
	Page target;
	ItemId itemid;
	MerkleNativeRootVersion *root;
	GenericXLogState *state;

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);
	native_validate_page(page, MERKLE_NATIVE_PAGE_APPEND, locator->block);
	native_validate_locator_generation(page, locator);
	itemid = PageGetItemId(page, locator->offset);
	if (!ItemIdIsNormal(itemid) || ItemIdGetLength(itemid) != sizeof(*root))
		elog(ERROR, "invalid native Merkle root during VACUUM");
	state = GenericXLogStart(indexRel);
	target = GenericXLogRegisterBuffer(state, buffer, 0);
	itemid = PageGetItemId(target, locator->offset);
	root = (MerkleNativeRootVersion *) PageGetItem(target, itemid);
	if (freeze)
	{
		root->creator_xid = FrozenTransactionId;
		root->flags |= MERKLE_NATIVE_ROOT_FROZEN_COMMITTED;
		native_invalid_locator(&root->previous_version);
	}
	if (aborted)
		root->flags |= MERKLE_NATIVE_ROOT_ABORTED_HINT;
	root->checksum = native_root_checksum(root);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

void
merkle_native_vacuum(Relation indexRel, IndexBulkDeleteResult *stats)
{
	NativeConfig config;
	TransactionId oldest_xmin;
	BlockNumber nblocks;
	bool *reachable;
	int directory_pages;
	int partition;
	BlockNumber block;
	uint64 live_items = 0;

	if (!merkle_native_is_ready(indexRel))
		return;
	native_read_config(indexRel, &config);
	for (partition = 0; partition < config.partitions; partition++)
		native_lock_partition(indexRel, partition);
	nblocks = RelationGetNumberOfBlocks(indexRel);
	reachable = palloc0(sizeof(*reachable) * Max(nblocks, 1));
	directory_pages = (config.partitions + native_directory_capacity() - 1) /
		native_directory_capacity();
	for (block = 0; block < MERKLE_TREE_START_BLKNO + directory_pages &&
		 block < nblocks; block++)
		reachable[block] = true;
	oldest_xmin = GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM);
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativePartitionEntry entry =
			native_read_directory(indexRel, partition);
		MerkleNativeLocator locator = entry.root_head;
		int walked = 0;
		bool counted_current = false;

		while (native_locator_valid(&locator))
		{
			MerkleNativeRootVersion root = native_read_root(indexRel, &locator);
			bool committed = (root.flags &
				MERKLE_NATIVE_ROOT_FROZEN_COMMITTED) != 0 ||
				root.creator_xid == FrozenTransactionId ||
				TransactionIdDidCommit(root.creator_xid);
			bool aborted = !committed &&
				TransactionIdDidAbort(root.creator_xid);

			if (locator.block >= nblocks)
				elog(ERROR, "native Merkle root is out of bounds during VACUUM");
			reachable[locator.block] = true;
			if (committed)
			{
				native_mark_tree(indexRel, &root.root_node, reachable,
					nblocks, 0);
				if (!counted_current)
				{
					live_items += root.tuple_count;
					counted_current = true;
				}
				if (root.creator_xid == FrozenTransactionId ||
					TransactionIdPrecedes(root.creator_xid, oldest_xmin))
				{
					if ((root.flags & MERKLE_NATIVE_ROOT_FROZEN_COMMITTED) == 0 ||
						native_locator_valid(&root.previous_version))
						native_hint_root(indexRel, &locator, true, false);
					break;
				}
			}
			else if (aborted &&
					 (root.flags & MERKLE_NATIVE_ROOT_ABORTED_HINT) == 0)
				native_hint_root(indexRel, &locator, false, true);
			locator = root.previous_version;
			if (++walked > MERKLE_NATIVE_ROOT_MAX_WALK)
				elog(ERROR, "native Merkle root chain is too long during VACUUM");
		}
	}
	for (block = MERKLE_TREE_START_BLKNO + directory_pages;
		 block < nblocks; block++)
	{
		Buffer buffer;
		Page page;
		Page target;
		GenericXLogState *state;

		if (reachable[block])
			continue;
		buffer = ReadBuffer(indexRel, block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (PageIsNew(page) ||
			PageGetSpecialSize(page) != MERKLE_NATIVE_PAGE_SPECIAL_SIZE ||
			MerkleNativePageGetOpaque(page)->page_type != MERKLE_NATIVE_PAGE_APPEND)
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}
		state = GenericXLogStart(indexRel);
		target = GenericXLogRegisterBuffer(state, buffer,
			GENERIC_XLOG_FULL_IMAGE);
		{
			uint32 generation = MerkleNativePageGetOpaque(page)->page_generation;

			if (generation == 0)
				generation = 1;
		PageInit(target, BLCKSZ, MERKLE_NATIVE_PAGE_SPECIAL_SIZE);
		MerkleNativePageGetOpaque(target)->magic = MERKLE_NATIVE_PAGE_MAGIC;
		MerkleNativePageGetOpaque(target)->version = MERKLE_NATIVE_PAGE_VERSION;
		MerkleNativePageGetOpaque(target)->page_type = MERKLE_NATIVE_PAGE_FREE;
		MerkleNativePageGetOpaque(target)->page_generation = generation;
		}
		GenericXLogFinish(state);
		RecordFreeIndexPage(indexRel, block);
		if (native_append_hint_oid == RelationGetRelid(indexRel) &&
			RelFileNodeEquals(native_append_hint_rnode, indexRel->rd_node) &&
			native_append_hint_block == block)
			native_append_hint_block = InvalidBlockNumber;
		UnlockReleaseBuffer(buffer);
	}
	IndexFreeSpaceMapVacuum(indexRel);
	stats->num_pages = nblocks;
	stats->num_index_tuples = live_items;
	pfree(reachable);
}

typedef struct NativeRangeRequest
{
	int partition;
	int prefix_len;
	uint8 prefix[MERKLE_HASH_BYTES];
} NativeRangeRequest;

static bytea *
native_bytea(const void *data, Size len)
{
	bytea *result = palloc(VARHDRSZ + len);

	SET_VARSIZE(result, VARHDRSZ + len);
	memcpy(VARDATA(result), data, len);
	return result;
}

static Relation
native_open_index_arg(Oid relid, LOCKMODE lockmode)
{
	Relation rel = relation_open(relid, lockmode);

	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		if (rel->rd_rel->relam != MERKLE_AM_OID ||
			!merkle_index_is_dynamic(rel) || !merkle_native_is_ready(rel))
		{
			relation_close(rel, lockmode);
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("relation is not a native dynamic Merkle index")));
		}
		return rel;
	}
	else
	{
		List *indexes = RelationGetIndexList(rel);
		ListCell *cell;
		Oid found = InvalidOid;

		foreach(cell, indexes)
		{
			Oid oid = lfirst_oid(cell);
			Relation candidate = index_open(oid, AccessShareLock);

			if (candidate->rd_rel->relam == MERKLE_AM_OID &&
				merkle_index_is_dynamic(candidate) &&
				merkle_native_is_ready(candidate))
				found = oid;
			index_close(candidate, AccessShareLock);
			if (OidIsValid(found))
				break;
		}
		list_free(indexes);
		relation_close(rel, lockmode);
		if (!OidIsValid(found))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation has no native dynamic Merkle index")));
		return index_open(found, lockmode);
	}
}

static Tuplestorestate *
native_begin_srf(FunctionCallInfo fcinfo, TupleDesc *desc)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext old;
	Tuplestorestate *store;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize) || rsinfo->expectedDesc == NULL)
		elog(ERROR, "native Merkle helper requires materialize mode");
	old = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	*desc = CreateTupleDescCopy(rsinfo->expectedDesc);
	store = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = store;
	rsinfo->setDesc = *desc;
	MemoryContextSwitchTo(old);
	return store;
}

static bool
native_route_has_prefix(const uint8 route[MERKLE_HASH_BYTES],
						const uint8 prefix[MERKLE_HASH_BYTES], int bits)
{
	int bytes = bits / 8;
	int remain = bits % 8;

	if (bytes > 0 && memcmp(route, prefix, bytes) != 0)
		return false;
	if (remain > 0)
	{
		uint8 mask = (uint8) (0xff << (8 - remain));

		if ((route[bytes] & mask) != (prefix[bytes] & mask))
			return false;
	}
	return true;
}

static NativeRangeRequest *
native_parse_ranges(Jsonb *json, const NativeConfig *config, int *count_out)
{
	MemoryContext caller_context = CurrentMemoryContext;
	MemoryContext spi_context;
	Oid type = JSONBOID;
	Datum arg = PointerGetDatum(json);
	char null = ' ';
	NativeRangeRequest *requests;
	int rc;
	int count;
	int i;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "native Merkle range parser SPI_connect failed");
	rc = SPI_execute_with_args(
		"SELECT partition_id,prefix_length,decode(prefix_value,'hex') "
		"FROM pg_catalog.jsonb_to_recordset($1) "
		"AS r(partition_id integer,prefix_length integer,prefix_value text)",
		1, &type, &arg, &null, true, 0);
	if (rc != SPI_OK_SELECT || SPI_processed > INT_MAX)
		elog(ERROR, "native Merkle range request parsing failed");
	count = (int) SPI_processed;
	/* SPI_connect() installs a temporary procedure context that SPI_finish()
	 * destroys.  The parsed request vector is returned to the caller, so it
	 * must be owned by the caller's context.  Returning an SPI-owned vector
	 * produced deterministic use-after-free locators in multi-range reads. */
	spi_context = MemoryContextSwitchTo(caller_context);
	requests = palloc0(sizeof(*requests) * Max(count, 1));
	MemoryContextSwitchTo(spi_context);
	for (i = 0; i < count; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		bool isnull;
		Datum value;
		bytea *prefix;
		uint8 canonical[MERKLE_HASH_BYTES];

		value = SPI_getbinval(tuple, desc, 1, &isnull);
		if (isnull)
			elog(ERROR, "native Merkle range partition cannot be null");
		requests[i].partition = DatumGetInt32(value);
		value = SPI_getbinval(tuple, desc, 2, &isnull);
		if (isnull)
			elog(ERROR, "native Merkle range prefix length cannot be null");
		requests[i].prefix_len = DatumGetInt32(value);
		value = SPI_getbinval(tuple, desc, 3, &isnull);
		prefix = isnull ? NULL : DatumGetByteaPP(value);
		if (requests[i].partition < 0 ||
			requests[i].partition >= config->partitions ||
			requests[i].prefix_len < 0 ||
			requests[i].prefix_len > MERKLE_HASH_BITS || prefix == NULL ||
			VARSIZE_ANY_EXHDR(prefix) != MERKLE_HASH_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid native Merkle range request")));
		memcpy(requests[i].prefix, VARDATA_ANY(prefix), MERKLE_HASH_BYTES);
		native_canonical_prefix(requests[i].prefix,
			requests[i].prefix_len, canonical);
		if (memcmp(canonical, requests[i].prefix, MERKLE_HASH_BYTES) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("native Merkle range prefix is not canonical")));
	}
	SPI_finish();
	*count_out = count;
	return requests;
}

static void
native_put_summary(Tuplestorestate *store, TupleDesc desc, int partition,
				   int prefix_len, const uint8 *prefix, uint64 count,
				   const MerkleHash *xor, bool is_leaf)
{
	Datum out[6];
	bool nulls[6] = {false, false, false, false, false, false};

	out[0] = Int32GetDatum(partition);
	out[1] = Int32GetDatum(prefix_len);
	out[2] = PointerGetDatum(native_bytea(prefix, MERKLE_HASH_BYTES));
	out[3] = Int64GetDatum((int64) count);
	out[4] = PointerGetDatum(native_bytea(xor->data, MERKLE_HASH_BYTES));
	out[5] = BoolGetDatum(is_leaf);
	tuplestore_putvalues(store, desc, out, nulls);
}

Datum
merkle_native_get_partition_roots(PG_FUNCTION_ARGS)
{
	Relation indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	NativeConfig config;
	TupleDesc desc;
	Tuplestorestate *store = native_begin_srf(fcinfo, &desc);
	uint8 zero[MERKLE_HASH_BYTES] = {0};
	int partition;

	native_read_config(indexRel, &config);
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativeRootVersion root;
		MerkleNativeNodeRecord *node;

		if (!native_visible_root(indexRel, partition, &root))
			elog(ERROR, "native Merkle partition has no visible root");
		node = native_read_node(indexRel, &root.root_node);
		native_put_summary(store, desc, partition, 0, zero,
			root.tuple_count, &root.data_xor,
			(node->flags & MERKLE_NATIVE_NODE_LEAF) != 0);
		pfree(node);
	}
	index_close(indexRel, ShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}

static void
native_emit_frontier(Relation indexRel, const MerkleNativeLocator *locator,
					 Tuplestorestate *store, TupleDesc desc)
{
	MerkleNativeNodeRecord *node = native_read_node(indexRel, locator);

	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
		native_put_summary(store, desc, node->partition_id, node->prefix_len,
			node->prefix, node->tuple_count, &node->data_xor, true);
	else
	{
		native_emit_frontier(indexRel, &node->left, store, desc);
		native_emit_frontier(indexRel, &node->right, store, desc);
	}
	pfree(node);
}

Datum
merkle_native_get_leaf_frontier(PG_FUNCTION_ARGS)
{
	Relation indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	NativeConfig config;
	TupleDesc desc;
	Tuplestorestate *store = native_begin_srf(fcinfo, &desc);
	int partition;

	native_read_config(indexRel, &config);
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativeRootVersion root;

		if (!native_visible_root(indexRel, partition, &root))
			elog(ERROR, "native Merkle partition has no visible root");
		native_emit_frontier(indexRel, &root.root_node, store, desc);
	}
	index_close(indexRel, ShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}

/*
 * native_traverse_range_summary
 *
 * Prefix-tree traversal that accumulates the data_xor and tuple count for
 * all items whose route digest starts with the requested prefix.  This
 * replaces the old O(N) collect-all-then-filter pattern.
 *
 * Complexity: O(tree_depth + matching_frontier_nodes)
 *
 * Traversal rules:
 *   1. If the node's stored prefix is DISJOINT with the request prefix, stop.
 *   2. If the request prefix is a PREFIX of the node's stored prefix (node is
 *      entirely inside the request range), count the whole subtree.
 *   3. If the node's stored prefix is a PREFIX of the request prefix (we need
 *      to keep descending), recurse into both children.
 *   4. At a leaf, scan only matching items.
 */
static void
native_traverse_range_summary(Relation indexRel,
							  const MerkleNativeLocator *locator,
							  const uint8 req_prefix[MERKLE_HASH_BYTES],
							  int req_bits,
							  uint64 *matched, uint64 *bytes,
							  MerkleHash *xor_accum)
{
	MerkleNativeNodeRecord *node = native_read_node(indexRel, locator);

	/*
	 * Case 1: request prefix and node prefix are disjoint — stop.
	 *
	 * Two prefixes are disjoint when neither is a prefix of the other.  In
	 * a binary prefix tree this means the first differing bit is within the
	 * shorter prefix length.
	 */
	if (!native_route_has_prefix(node->prefix, req_prefix,
								 Min(node->prefix_len, req_bits)) &&
		!native_route_has_prefix(req_prefix, node->prefix,
								 Min(node->prefix_len, req_bits)))
	{
		pfree(node);
		return;
	}

	/*
	 * Case 2: node is entirely inside the requested range — count the whole
	 * subtree without reading further children.
	 */
	if (req_bits <= node->prefix_len &&
		native_route_has_prefix(node->prefix, req_prefix, req_bits))
	{
		*matched += node->tuple_count;
		*bytes   += node->subtree_bytes;
		merkle_hash_xor(xor_accum, &node->data_xor);
		pfree(node);
		return;
	}

	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		/* Case 4: leaf — scan items individually for prefix match. */
		NativeItemVector leaf = {0};
		int i;

		native_load_leaf_items(indexRel, node, &leaf);
		for (i = 0; i < leaf.count; i++)
		{
			if (native_route_has_prefix(leaf.items[i].route, req_prefix, req_bits))
			{
				(*matched)++;
				*bytes += native_item_bytes(&leaf.items[i]);
				merkle_hash_xor(xor_accum, &leaf.items[i].hash);
			}
		}
		native_vector_free(&leaf);
	}
	else
	{
		/* Case 3: internal node — recurse into both children. */
		native_traverse_range_summary(indexRel, &node->left,
									  req_prefix, req_bits,
									  matched, bytes, xor_accum);
		native_traverse_range_summary(indexRel, &node->right,
									  req_prefix, req_bits,
									  matched, bytes, xor_accum);
	}
	pfree(node);
}

/*
 * native_traverse_range_items
 *
 * Prefix-tree traversal that emits one row per matching item into a
 * tuplestore.  Only descends into nodes that overlap the requested prefix.
 *
 * This replaces the O(N) collect-all-then-filter approach in
 * merkle_native_get_range_items.
 */
static void
native_traverse_range_items(Relation indexRel,
							const MerkleNativeLocator *locator,
							const uint8 req_prefix[MERKLE_HASH_BYTES],
							int req_bits, int partition, int out_prefix_len,
							Tuplestorestate *store, TupleDesc desc)
{
	MerkleNativeNodeRecord *node = native_read_node(indexRel, locator);

	/* Disjoint — stop. */
	if (!native_route_has_prefix(node->prefix, req_prefix,
								 Min(node->prefix_len, req_bits)) &&
		!native_route_has_prefix(req_prefix, node->prefix,
								 Min(node->prefix_len, req_bits)))
	{
		pfree(node);
		return;
	}

	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		NativeItemVector leaf = {0};
		int i;

		native_load_leaf_items(indexRel, node, &leaf);
		for (i = 0; i < leaf.count; i++)
		{
			if (native_route_has_prefix(leaf.items[i].route, req_prefix, req_bits))
			{
				Datum out[7];
				bool nulls[7] = {false, false, false, false, false, false, false};
				bytea *key_data = native_bytea(leaf.items[i].key,
					leaf.items[i].key_length);
				char *key_text = merkle_dynamic_single_key_text(indexRel,
					key_data);

				out[0] = Int32GetDatum(partition);
				out[1] = Int32GetDatum(out_prefix_len);
				out[2] = PointerGetDatum(native_bytea(req_prefix, MERKLE_HASH_BYTES));
				out[3] = PointerGetDatum(key_data);
				if (key_text == NULL)
				{
					out[4] = (Datum) 0;
					nulls[4] = true;
				}
				else
					out[4] = CStringGetTextDatum(key_text);
				out[5] = PointerGetDatum(native_bytea(leaf.items[i].route,
					MERKLE_HASH_BYTES));
				out[6] = PointerGetDatum(native_bytea(leaf.items[i].hash.data,
					MERKLE_HASH_BYTES));
				tuplestore_putvalues(store, desc, out, nulls);
			}
		}
		native_vector_free(&leaf);
	}
	else
	{
		native_traverse_range_items(indexRel, &node->left, req_prefix, req_bits,
									partition, out_prefix_len, store, desc);
		native_traverse_range_items(indexRel, &node->right, req_prefix, req_bits,
									partition, out_prefix_len, store, desc);
	}
	pfree(node);
}

Datum
merkle_native_get_ranges(PG_FUNCTION_ARGS)
{
	Relation indexRel;
	NativeConfig config;
	TupleDesc desc;
	Tuplestorestate *store;
	NativeRangeRequest *requests;
	int count;
	int r;

	if (PG_ARGISNULL(1))
		return merkle_native_get_leaf_frontier(fcinfo);
	indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	native_read_config(indexRel, &config);
	store = native_begin_srf(fcinfo, &desc);
	requests = native_parse_ranges(PG_GETARG_JSONB_P(1), &config, &count);
	for (r = 0; r < count; r++)
	{
		MerkleNativeRootVersion root;
		MerkleHash xor;
		uint64 bytes = 0;
		uint64 matched = 0;

		if (!native_visible_root(indexRel, requests[r].partition, &root))
			elog(ERROR, "native Merkle partition has no visible root");
		merkle_hash_zero(&xor);
		/*
		 * Use the prefix-tree traversal instead of collecting all items
		 * (plan_left.md §7).  Complexity is now O(depth + matching_frontier)
		 * rather than O(partition_items).
		 */
		native_traverse_range_summary(indexRel, &root.root_node,
									  requests[r].prefix, requests[r].prefix_len,
									  &matched, &bytes, &xor);
		native_put_summary(store, desc, requests[r].partition,
			requests[r].prefix_len, requests[r].prefix, matched, &xor,
			matched <= (uint64) config.leaf_capacity &&
			bytes <= config.leaf_byte_capacity);
	}
	index_close(indexRel, ShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}

Datum
merkle_native_get_range_items(PG_FUNCTION_ARGS)
{
	Relation indexRel;
	NativeConfig config;
	TupleDesc desc;
	Tuplestorestate *store;
	NativeRangeRequest *requests;
	int count;
	int r;

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("native Merkle range-items request cannot be null")));
	indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	native_read_config(indexRel, &config);
	store = native_begin_srf(fcinfo, &desc);
	requests = native_parse_ranges(PG_GETARG_JSONB_P(1), &config, &count);
	for (r = 0; r < count; r++)
	{
		MerkleNativeRootVersion root;
		/*
		 * Validate that the requested range is leaf-bounded before streaming
		 * items.  Use the summary traversal (O(depth)) for this check rather
		 * than collecting all items first.
		 */
		MerkleHash xor;
		uint64 matched = 0;
		uint64 bytes = 0;

		if (!native_visible_root(indexRel, requests[r].partition, &root))
			elog(ERROR, "native Merkle partition has no visible root");
		merkle_hash_zero(&xor);
		native_traverse_range_summary(indexRel, &root.root_node,
									  requests[r].prefix, requests[r].prefix_len,
									  &matched, &bytes, &xor);
		if (matched > (uint64) config.leaf_capacity ||
			bytes > config.leaf_byte_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested native Merkle range is not bounded"),
					 errhint("Descend the logical range before requesting items.")));
		/*
		 * Now stream the matching items using the prefix-tree traversal
		 * (plan_left.md §7).  Complexity is O(depth + matching_leaf_items).
		 */
		native_traverse_range_items(indexRel, &root.root_node,
									requests[r].prefix, requests[r].prefix_len,
									requests[r].partition, requests[r].prefix_len,
									store, desc);
	}
	index_close(indexRel, ShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}

static void
native_count_nodes(Relation indexRel, const MerkleNativeLocator *locator,
				   uint64 *nodes, uint64 *leaves, uint64 *max_depth,
				   uint64 depth, uint64 *max_leaf_items)
{
	MerkleNativeNodeRecord *node = native_read_node(indexRel, locator);

	(*nodes)++;
	*max_depth = Max(*max_depth, depth);
	if ((node->flags & MERKLE_NATIVE_NODE_LEAF) != 0)
	{
		(*leaves)++;
		*max_leaf_items = Max(*max_leaf_items, node->tuple_count);
	}
	else
	{
		native_count_nodes(indexRel, &node->left, nodes, leaves, max_depth,
			depth + 1, max_leaf_items);
		native_count_nodes(indexRel, &node->right, nodes, leaves, max_depth,
			depth + 1, max_leaf_items);
	}
	pfree(node);
}

Datum
merkle_native_tree_stats(PG_FUNCTION_ARGS)
{
	Relation indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	NativeConfig config;
	uint64 nodes = 0;
	uint64 leaves = 0;
	uint64 items = 0;
	uint64 bytes = 0;
	uint64 max_depth = 0;
	uint64 max_leaf_items = 0;
	uint64 min_seq = PG_UINT64_MAX;
	uint64 max_seq = 0;
	uint16 sequence_domain = 0;
	uint16 sequence_flags = 0;
	uint64 sequence_epoch = 0;
	bool have_ordered_sequence = false;
	bool have_baseline_sequence = false;
	bool mixed_sequence = false;
	BlockNumber pages = RelationGetNumberOfBlocks(indexRel);
	int partition;
	char *json;
	Datum result;
	int mode;
	const char *mode_str;
	MerkleHash data_root;
	MerkleHash structure_root;
	MerkleHash combined_root;

	native_read_config(indexRel, &config);
	native_compute_commitments(indexRel, &data_root, &structure_root,
		&combined_root, NULL);
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativeRootVersion root;

		if (!native_visible_root(indexRel, partition, &root))
			elog(ERROR, "native Merkle partition has no visible root");
		items += root.tuple_count;
		bytes += root.subtree_bytes;
		if ((root.sequence_flags & MERKLE_SEQUENCE_FLAG_BUILD_BASELINE) != 0)
		{
			have_baseline_sequence = true;
			if (!have_ordered_sequence)
				sequence_flags |= root.sequence_flags;
		}
		else if (!have_ordered_sequence)
		{
			sequence_domain = root.sequence_domain;
			sequence_flags = root.sequence_flags;
			sequence_epoch = root.sequence_epoch;
			have_ordered_sequence = true;
		}
		else if (root.sequence_domain != sequence_domain ||
				 root.sequence_epoch != sequence_epoch)
			mixed_sequence = true;
		min_seq = Min(min_seq, root.sequence_value);
		max_seq = Max(max_seq, root.sequence_value);
		native_count_nodes(indexRel, &root.root_node, &nodes, &leaves,
			&max_depth, 0, &max_leaf_items);
	}
	if (mixed_sequence)
	{
		/* A real ordered marker must never be emitted for mixed non-baseline
		 * provenance.  Build baselines are compatible fallbacks and are
		 * intentionally ignored when at least one ordered root exists. */
		sequence_domain = 0;
		sequence_flags = 0;
		sequence_epoch = 0;
	}
	else if (!have_ordered_sequence && have_baseline_sequence)
	{
		sequence_domain = MERKLE_SEQUENCE_LOCAL_XID;
		sequence_epoch = 0;
	}
	mode = merkle_get_update_mode(indexRel);
	mode_str = (mode == MERKLE_UPDATE_SYNCHRONOUS_COW) ? "synchronous_cow" : "pending_log";

	json = psprintf("{\"authority\":\"native_index_pages\","
		"\"update_mode\":\"%s\","
		"\"data_root\":\"%s\",\"structure_root\":\"%s\","
		"\"combined_root\":\"%s\","
		"\"layout_version\":%d,\"partitions\":%d,\"item_count\":%llu,"
		"\"item_bytes\":%llu,\"node_count\":%llu,\"leaf_count\":%llu,"
		"\"max_depth\":%llu,\"max_leaf_items\":%llu,\"pages\":%u,"
		"\"min_apply_seq\":%llu,\"max_apply_seq\":%llu,"
		"\"sequence_domain\":%u,\"sequence_flags\":%u,\"sequence_epoch\":%llu}",
		mode_str, merkle_hash_to_hex(&data_root),
		merkle_hash_to_hex(&structure_root), merkle_hash_to_hex(&combined_root),
		MERKLE_DYNAMIC_LAYOUT_VERSION, config.partitions,
		(unsigned long long) items, (unsigned long long) bytes,
		(unsigned long long) nodes, (unsigned long long) leaves,
		(unsigned long long) max_depth, (unsigned long long) max_leaf_items,
		pages, (unsigned long long) (min_seq == PG_UINT64_MAX ? 0 : min_seq),
		(unsigned long long) max_seq,
		(unsigned int) sequence_domain, (unsigned int) sequence_flags,
		(unsigned long long) sequence_epoch);
	index_close(indexRel, ShareLock);
	result = DirectFunctionCall1(jsonb_in, CStringGetDatum(json));
	pfree(json);
	PG_RETURN_DATUM(result);
}

Datum
merkle_native_partition_roots_at(PG_FUNCTION_ARGS)
{
	Relation indexRel = native_open_index_arg(PG_GETARG_OID(0), ShareLock);
	NativeConfig config;
	bool typed = PG_NARGS() >= 4;
	bool current = !typed && PG_ARGISNULL(1);
	uint16 requested_domain = 0;
	uint64 requested_epoch = 0;
	uint64 requested = current ? 0 : (uint64) PG_GETARG_INT64(1);
	TupleDesc desc;
	Tuplestorestate *store = native_begin_srf(fcinfo, &desc);
	int partition;
	bool lineage_certified = !typed;

	if (typed)
	{
		if (PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("native Merkle typed marker requires domain, epoch and value")));
		if (PG_GETARG_INT64(3) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("native Merkle marker value cannot be negative")));
		requested_domain = (uint16) PG_GETARG_INT32(1);
		requested_epoch = (uint64) PG_GETARG_INT64(2);
		requested = (uint64) PG_GETARG_INT64(3);
		if (requested_domain == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("native Merkle marker domain cannot be zero")));
	}
	else if (!current && PG_GETARG_INT64(1) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("native Merkle apply sequence cannot be negative")));
	native_read_config(indexRel, &config);
	/* A build baseline is lineage-neutral until at least one committed root
	 * proves that this index has entered the requested ordered domain/epoch.
	 * Once certified, an untouched partition may use its baseline at or before
	 * the marker; a partition with an ordered root always uses that root. */
	if (typed)
	{
		int lineage_partition;

		for (lineage_partition = 0;
			 lineage_partition < config.partitions && !lineage_certified;
			 lineage_partition++)
		{
			MerkleNativePartitionEntry lineage_entry =
				native_read_directory(indexRel, lineage_partition);
			MerkleNativeLocator lineage_locator = lineage_entry.root_head;
			int lineage_walked = 0;

			while (native_locator_valid(&lineage_locator))
			{
				MerkleNativeRootVersion candidate =
					native_read_root(indexRel, &lineage_locator);

				if (native_root_visible(&candidate) &&
					(candidate.sequence_flags &
					 MERKLE_SEQUENCE_FLAG_BUILD_BASELINE) == 0 &&
					candidate.sequence_domain == requested_domain &&
					candidate.sequence_epoch == requested_epoch &&
					candidate.sequence_value <= requested)
				{
					lineage_certified = true;
					break;
				}
				lineage_locator = candidate.previous_version;
				if (++lineage_walked > MERKLE_NATIVE_ROOT_MAX_WALK)
					elog(ERROR, "native Merkle root chain is unreasonably long");
			}
		}
		if (!lineage_certified)
			ereport(ERROR,
					(errcode(ERRCODE_NO_DATA_FOUND),
					 errmsg("native Merkle marker lineage is not certified"),
					 errdetail("no committed non-baseline root exists for domain %u epoch %llu at or before sequence %llu",
							   (unsigned int) requested_domain,
							   (unsigned long long) requested_epoch,
							   (unsigned long long) requested)));
	}
	for (partition = 0; partition < config.partitions; partition++)
	{
		MerkleNativePartitionEntry entry =
			native_read_directory(indexRel, partition);
		MerkleNativeLocator locator = entry.root_head;
		MerkleNativeRootVersion root;
		MerkleNativeRootVersion baseline;
		bool found = false;
		bool have_baseline = false;
		int walked = 0;

		while (native_locator_valid(&locator))
		{
			MerkleNativeRootVersion candidate =
				native_read_root(indexRel, &locator);
			if (native_root_visible(&candidate) &&
				(!typed || (candidate.sequence_domain == requested_domain &&
				 candidate.sequence_epoch == requested_epoch)) &&
				(current || candidate.sequence_value <= requested))
			{
				root = candidate;
				found = true;
				break;
			}
			if (typed && native_root_visible(&candidate) &&
				(candidate.sequence_flags & MERKLE_SEQUENCE_FLAG_BUILD_BASELINE) != 0 &&
				candidate.sequence_value <= requested && !have_baseline)
			{
				baseline = candidate;
				have_baseline = true;
			}
			locator = candidate.previous_version;
			if (++walked > MERKLE_NATIVE_ROOT_MAX_WALK)
				elog(ERROR, "native Merkle root chain is unreasonably long");
		}
		if (!found && have_baseline)
		{
			root = baseline;
			found = true;
		}
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_NO_DATA_FOUND),
					 errmsg("partition %d has no visible native Merkle root at apply sequence %llu",
						partition, (unsigned long long) requested)));
		{
			Datum out[12];
			bool nulls[12] = {false, false, false, false, false, false,
				false, false, false, false, false, false};

			out[0] = Int32GetDatum(partition);
			out[1] = Int64GetDatum((int64) root.sequence_value);
			out[2] = Int16GetDatum((int16) root.sequence_domain);
			out[3] = Int16GetDatum((int16) root.sequence_flags);
			out[4] = Int64GetDatum((int64) root.sequence_epoch);
			out[5] = Int64GetDatum((int64) root.version_no);
			out[6] = TransactionIdGetDatum(root.creator_xid);
			out[7] = BoolGetDatum(root.creator_xid == FrozenTransactionId ||
				(root.flags & MERKLE_NATIVE_ROOT_FROZEN_COMMITTED) != 0);
			out[8] = Int64GetDatum((int64) root.tuple_count);
			out[9] = PointerGetDatum(native_bytea(root.data_xor.data,
				MERKLE_HASH_BYTES));
			if (desc->natts == 12)
			{
				out[10] = PointerGetDatum(native_bytea(root.content_xor.data,
					MERKLE_HASH_BYTES));
				out[11] = PointerGetDatum(native_bytea(root.structure_hash.data,
					MERKLE_HASH_BYTES));
			}
			else
				out[10] = PointerGetDatum(native_bytea(root.structure_hash.data,
					MERKLE_HASH_BYTES));
			tuplestore_putvalues(store, desc, out, nulls);
		}
	}
	index_close(indexRel, ShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}
