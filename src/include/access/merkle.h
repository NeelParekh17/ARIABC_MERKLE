/*-------------------------------------------------------------------------
 *
 * merkle.h
 *    Merkle tree integrity index access method definitions
 *
 * This implements a Merkle tree as a PostgreSQL index type for data
 * integrity verification. On every INSERT/UPDATE/DELETE, the Merkle
 * tree is automatically updated by PostgreSQL's index infrastructure.
 *
 * src/include/access/merkle.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MERKLE_H
#define MERKLE_H

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "nodes/execnodes.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

/* GUC: Enable/disable Merkle index updates */
extern bool enable_merkle_index;
/* Internal executor guard while generic UPDATE index insertion is running. */
extern bool merkle_index_maintenance_suppress;
/* GUC: Emit NOTICE lines for Merkle build roots */
extern bool merkle_update_detection;
extern bool merkle_recovery_profile_enabled;
#define MERKLE_UPDATE_SYNCHRONOUS_COW 0
#define MERKLE_UPDATE_PENDING_LOG     1
#define MERKLE_NATIVE_MODE_SYNCHRONOUS_COW 0x0001
#define MERKLE_NATIVE_MODE_PENDING_LOG     0x0002
#define MERKLE_NATIVE_MODE_MASK            0x0003
/* GUC: reject stale reads (error) or synchronously catch up (wait). */
extern int merkle_read_lag_policy;
#define MERKLE_APPLY_DEFAULT_BATCH_ITEMS 256
#define MERKLE_APPLY_DEFAULT_BATCH_BYTES (1024 * 1024)
#define MERKLE_APPLY_DEFAULT_BATCH_PAGES 128
#define MERKLE_APPLY_DEFAULT_BATCH_TIME_MS 1
extern int merkle_apply_batch_items;
extern int merkle_apply_batch_bytes;
extern int merkle_apply_batch_pages;
extern int merkle_apply_batch_time_ms;
#define MERKLE_READ_LAG_ERROR 0
#define MERKLE_READ_LAG_WAIT  1
#define MERKLE_READ_LAG_APPLY 2
/*
 * GUC: Suppress Merkle update-detection output during Merkle index builds
 * (CREATE INDEX / REINDEX).
 */
extern bool merkle_update_detection_suppress;
typedef struct MerkleRecoveryProfileStats
{
	uint64		root_hash_helper_calls;
	uint64		root_hash_nodes_returned;
	uint64		root_hash_helper_us;
	uint64		child_hash_helper_calls;
	uint64		child_hash_nodes_returned;
	uint64		child_hash_helper_us;
	uint64		row_hash_compute_calls;
	instr_time	row_hash_compute_time;
	uint64		tree_path_update_calls;
	uint64		tree_path_nodes_touched;
	instr_time	tree_path_update_time;
} MerkleRecoveryProfileStats;

extern uint64 merkle_recovery_profile_reset_generation;
extern MerkleRecoveryProfileStats merkle_recovery_profile_state;

/*
 * Merkle tree configuration constants
 * 
 * The tree is organized as multiple partitions for better distribution:
 * - NUM_PARTITIONS: Number of independent partitions
 * - LEAVES_PER_PARTITION: Leaf buckets in each partition
 * - DEFAULT_FANOUT: Branching factor (children per internal node)
 * - NODES_PER_PARTITION (default): Total nodes per partition for a perfect
 *   k-ary tree with k=DEFAULT_FANOUT
 * - TOTAL_LEAVES (default): NUM_PARTITIONS * LEAVES_PER_PARTITION
 * - TOTAL_NODES (default): NUM_PARTITIONS * NODES_PER_PARTITION
 *
 * NOTE: With 256-bit (32-byte) hashes, the default tree spans multiple pages.
 */
#define MERKLE_NUM_PARTITIONS       58
#define MERKLE_LEAVES_PER_PARTITION 1024
#define MERKLE_DEFAULT_FANOUT       32  /* branching factor */
#define MERKLE_NODES_PER_PARTITION  1057 /* (32 * 1024 - 1) / (32 - 1) */
#define MERKLE_TOTAL_LEAVES         59392 /* NUM_PARTITIONS * LEAVES_PER_PARTITION */
#define MERKLE_TOTAL_NODES          61306 /* NUM_PARTITIONS * NODES_PER_PARTITION */

/*
 * Hash configuration
 * Using 256-bit (32-byte) hashes from BLAKE3
 * 
 * NOTE: Upgraded to 256-bit for maximum security and performance.
 * Each node now takes 36 bytes (4 nodeId + 32 hash).
 * 
 * Security: Collision threshold is now 2^128 (astronomical)
 * Performance: BLAKE3 is faster than MD5 and cryptographically secure
 */
#define MERKLE_HASH_BITS            256
#define MERKLE_HASH_BYTES           32
#define MERKLE_BLAKE3_LEN           32  /* BLAKE3 output = 32 bytes */

/*
 * Page layout constants
 */
#define MERKLE_METAPAGE_BLKNO       0
#define MERKLE_TREE_START_BLKNO     1
#define MERKLE_VERSION              7   /* WAL-safe committed-delta format */
/*
 * Route format 2: uniform BLAKE3-256 for ALL key types including integers.
 * Format 1 preserved abs(int) % total_leaves for single integer keys; that
 * was unsafe for a future dynamic prefix tree.  Rebuild all indexes.
 */
#define MERKLE_ROUTE_FORMAT_VERSION 2
#define MERKLE_ROW_HASH_FORMAT_VERSION 1

/* Versioned, endian-stable durable transaction-delta encoding. */
#define MERKLE_DELTA_MAGIC          ((uint32) 0x4D444C54) /* "MDLT" */
#define MERKLE_DELTA_LEGACY_VERSION 1
#define MERKLE_DELTA_VERSION        2
#define MERKLE_DELTA_HEADER_BYTES   40
#define MERKLE_DELTA_ENTRY_BYTES    56
#define MERKLE_DELTA_V2_HEADER_BYTES 40
#define MERKLE_DELTA_V2_ENTRY_FIXED_BYTES 168

/* Dynamic Merkle format.  The static v7 page layout remains unchanged. */
#define MERKLE_DYNAMIC_META_MAGIC          ((uint32) 0x44594E4D) /* "DYNM" */
#define MERKLE_DYNAMIC_LAYOUT_VERSION      5
#define MERKLE_DYNAMIC_LOGICAL_FANOUT      32
#define MERKLE_DYNAMIC_DEFAULT_LEAF_CAPACITY 32
#define MERKLE_DYNAMIC_DEFAULT_MERGE_THRESHOLD 8
#define MERKLE_DYNAMIC_DEFAULT_LEAF_BYTE_CAPACITY (64 * 1024)
#define MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES 1024
#define MERKLE_DYNAMIC_MAX_KEY_BYTES       2000

/*
 * Format-version tags for the combined global root commitment
 * (plan_left.md §2).  Increment either when the corresponding hash
 * construction changes so that historical roots remain distinguishable.
 */


/* Native dynamic format: authoritative XID-visible roots plus immutable
 * internal, leaf, and canonical-item records in the index relation. */
#define MERKLE_NATIVE_PAGE_MAGIC           ((uint32) 0x4D4E5047) /* "MNPG" */
#define MERKLE_NATIVE_PAGE_VERSION         2
#define MERKLE_NATIVE_ROOT_MAGIC           ((uint32) 0x4D4E5254) /* "MNRT" */
#define MERKLE_NATIVE_ROOT_VERSION          2
#define MERKLE_NATIVE_RECORD_MAGIC          ((uint32) 0x4D4E5243) /* "MNRC" */
#define MERKLE_NATIVE_RECORD_VERSION        2
#define MERKLE_NATIVE_RECORD_INTERNAL       1
#define MERKLE_NATIVE_RECORD_LEAF           2
#define MERKLE_NATIVE_RECORD_ITEM           3
#define MERKLE_NATIVE_RECORD_ITEM_CHUNK     4
#define MERKLE_NATIVE_INVALID_OFFSET        InvalidOffsetNumber

typedef struct MerkleNativePageOpaqueData
{
	uint32      magic;
	uint16      version;
	uint16      page_type;
	/* Incremented whenever a FREE page is reused for append records. */
	uint32      page_generation;
} MerkleNativePageOpaqueData;

#define MERKLE_NATIVE_PAGE_SPECIAL_SIZE \
	MAXALIGN(sizeof(MerkleNativePageOpaqueData))
#define MerkleNativePageGetOpaque(page) \
	((MerkleNativePageOpaqueData *) PageGetSpecialPointer(page))


/*
 * Calculate how many nodes fit per page
 * Each node: 4 bytes (nodeId) + 32 bytes (hash) = 36 bytes
 * Page size 8192, minus header ~24 bytes = ~8168 usable
 * Max ~226 nodes per page.
 */
#define MERKLE_PAGE_OPAQUE_MAGIC     ((uint32) 0x4D504147) /* "MPAG" */
#define MERKLE_PAGE_OPAQUE_VERSION   1

/*
 * Every v7 tree page records the exact globally ordered delta sequence that
 * it has consumed.  The hash changes and this position are emitted in the
 * same Generic WAL record.
 */
typedef struct MerklePageOpaqueData
{
	uint32		magic;
	uint16		version;
	uint16		flags;
	uint64		last_applied_seq;
} MerklePageOpaqueData;

#define MerklePageGetOpaque(page) \
	((MerklePageOpaqueData *) PageGetSpecialPointer(page))

#define MERKLE_PAGE_SPECIAL_SIZE MAXALIGN(sizeof(MerklePageOpaqueData))
#define MERKLE_MAX_NODES_PER_PAGE \
	((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MERKLE_PAGE_SPECIAL_SIZE) / sizeof(MerkleNode))

/*
 * MerkleHash - 256-bit hash value stored in 32 bytes
 */
typedef struct MerkleHash
{
    uint8       data[MERKLE_HASH_BYTES];
} MerkleHash;

typedef struct MerkleNativeLocator
{
	BlockNumber block;
	OffsetNumber offset;
	uint16      reserved;
	uint32      page_generation;
} MerkleNativeLocator;

typedef struct MerkleNativePartitionEntry
{
	MerkleNativeLocator root_head;
	uint64      last_allocated_version;
} MerkleNativePartitionEntry;

typedef struct MerkleNativeRootVersion
{
	uint32      magic;
	uint16      version;
	uint16      flags;
	TransactionId creator_xid;
	uint32      partition_id;
	uint16      sequence_domain;
	uint16      sequence_flags;
	uint64      sequence_epoch;
	uint64      sequence_value;
	uint64      version_no;
	uint64      tuple_count;
	uint64      subtree_bytes;
	MerkleHash  data_xor;
	MerkleHash  content_xor;
	MerkleHash  structure_hash;
	MerkleNativeLocator root_node;
	MerkleNativeLocator previous_version;
	uint32      checksum;
} MerkleNativeRootVersion;

#define MERKLE_NATIVE_ROOT_FROZEN_COMMITTED (1U << 0)
#define MERKLE_NATIVE_ROOT_ABORTED_HINT      (1U << 1)

#define MERKLE_SEQUENCE_RAFT           1
#define MERKLE_SEQUENCE_LOCAL_XID      2
/* A baseline is an initial materialization in the same ordering domain as
 * the index's normal sequence.  It is a flag, never a third domain. */
#define MERKLE_SEQUENCE_FLAG_BUILD_BASELINE (1U << 0)

typedef struct MerkleNativeRecordHeader
{
	uint32      magic;
	uint16      version;
	uint16      type;
	uint32      size;
	uint32      checksum;
} MerkleNativeRecordHeader;

typedef struct MerkleNativeNodeRecord
{
	MerkleNativeRecordHeader header;
	uint32      partition_id;
	uint16      prefix_len;
	uint16      flags;
	uint8       prefix[MERKLE_HASH_BYTES];
	uint64      tuple_count;
	uint64      subtree_bytes;
	MerkleHash  data_xor;
	MerkleHash  content_xor;
	MerkleHash  structure_hash;
	MerkleNativeLocator left;
	MerkleNativeLocator right;
	MerkleNativeLocator item_head;
} MerkleNativeNodeRecord;

#define MERKLE_NATIVE_NODE_LEAF (1U << 0)

typedef struct MerkleNativeItemRecord
{
	MerkleNativeRecordHeader header;
	MerkleNativeLocator next;
	uint8       route_digest[MERKLE_HASH_BYTES];
	MerkleHash  tuple_hash;
	uint32      key_length;
	/* canonical key bytes follow */
} MerkleNativeItemRecord;

typedef struct MerkleNativeItemChunkRecord
{
	MerkleNativeRecordHeader header;
	MerkleNativeLocator next;
	uint32      item_count;
	uint32      payload_bytes;
	/* packed MerkleNativePackedItem records follow */
} MerkleNativeItemChunkRecord;

typedef struct MerkleNativePackedItem
{
	uint8       route_digest[MERKLE_HASH_BYTES];
	MerkleHash  tuple_hash;
	uint32      key_length;
	/* canonical key bytes follow */
} MerkleNativePackedItem;

/*
 * MerkleNode - A single node in the Merkle tree
 */
typedef struct MerkleNode
{
    int32       nodeId;     /* node identifier (1-indexed) */
    MerkleHash  hash;       /* XOR-aggregated hash value */
} MerkleNode;

/*
 * MerkleMetaPageData - Metadata stored on page 0
 */
typedef struct MerkleMetaPageData
{
    uint32          version;            /* format version */
    Oid             heapRelid;          /* OID of indexed table */
    int32           numPartitions;      /* number of partitions */
    int32           leavesPerPartition; /* leaves per partition */
    int32           nodesPerPartition;  /* nodes per partition */
    int32           totalNodes;         /* total nodes in tree */
    int32           nodesPerPage;       /* how many nodes fit per page */
    int32           numTreePages;       /* number of pages for tree nodes */
    int32           fanout;             /* branching factor (children per internal node) */
	uint32          routeFormatVersion; /* deterministic key-routing format */
	uint32          rowHashFormatVersion; /* canonical row serialization format */
	uint64          baselineApplySeq;   /* heap snapshot represented at build */
	/* Appended extension: zero for every static v7 index. */
	uint32          dynamicMagic;
	uint16          dynamicLayoutVersion;
	uint16          dynamicFlags;
	uint32          dynamicLogicalFanout;
	uint32          dynamicLeafCapacity;
	uint32          dynamicMergeThreshold;
	uint32          dynamicLeafByteCapacity;
	uint32          dynamicMaxKeyBytes;
	BlockNumber     nativeDirectoryStart;
	uint32          nativeDirectoryPages;
	uint32          nativeFormatFlags;
} MerkleMetaPageData;

#define MerklePageGetMeta(page) \
    ((MerkleMetaPageData *) PageGetContents(page))

/*
 * MerkleOptions - User-configurable options for Merkle index
 * Parsed from CREATE INDEX ... WITH (partitions=X, leaves_per_partition=Y, fanout=Z)
 */
typedef struct MerkleOptions
{
    int32       vl_len_;        /* varlena header (required) */
    int         partitions;
    int         leaves_per_partition;
    int         fanout;
	bool        dynamic;
	int         leaf_capacity;
	int         merge_threshold;
	int         leaf_byte_capacity;
	int         max_key_bytes;
	int         update_mode;
} MerkleOptions;

/*
 * Authoritative output of key routing.  Static trees consume leaf_id while a
 * future dynamic tree can consume route_digest (all 32 bytes of the BLAKE3
 * hash) without reimplementing routing.  The eight-byte static_route_value is
 * derived from the first eight bytes of route_digest; do NOT use route_hash
 * (that field no longer exists).
 */
typedef struct MerkleRoute
{
	uint8		route_digest[MERKLE_HASH_BYTES];
	uint64		static_route_value;
	int			leaf_id;
	int			partition_id;
	int			node_in_partition;
} MerkleRoute;

/*
 * Canonical identity of one indexed item.  key_data is a versioned varlena
 * allocated in the caller's CurrentMemoryContext.  Staging functions must
 * deep-copy it before returning; callers may pfree it immediately afterward.
 */
typedef struct MerkleItemIdentity
{
	MerkleRoute route;
	bytea      *key_data;
} MerkleItemIdentity;

typedef struct MerkleDynamicTransition
{
	uint64      seq;
	uint16      sequence_domain;
	uint64      sequence_epoch;
	Oid         index_oid;
	RelFileNode index_rnode;
	int32       partition_id;
	uint8       route_digest[MERKLE_HASH_BYTES];
	bytea      *key_data;
	bool        has_old;
	bool        has_new;
	MerkleHash  old_hash;
	MerkleHash  new_hash;
} MerkleDynamicTransition;

typedef struct MerkleDynamicBuildState MerkleDynamicBuildState;
typedef struct MerkleNativeBuildState MerkleNativeBuildState;

/* Arithmetic-only perfect-tree geometry shared by all Merkle code paths. */
typedef struct MerkleGeometry
{
	int			num_partitions;
	int			leaves_per_partition;
	int			nodes_per_partition;
	int			total_nodes;
	int			total_leaves;
	int			fanout;
	int			leaf_start;
} MerkleGeometry;

typedef enum MerkleRecoveryState
{
	MERKLE_STATE_READY = 0,
	MERKLE_STATE_CATCHING_UP,
	MERKLE_STATE_REBUILD_REQUIRED,
	MERKLE_STATE_INVALID,
	MERKLE_STATE_BLOCKED_ON_GAP
} MerkleRecoveryState;

typedef struct MerkleRecoveryStatusData
{
	MerkleRecoveryState state;
	uint64		applied_seq;
	uint64		target_seq;
	uint64		terminal_prefix_seq;
	uint64		highest_terminal_seq;
	uint64		blocked_seq;
	bool		managed;
	char		error_text[256];
} MerkleRecoveryStatusData;

/*
 * Handler function - returns IndexAmRoutine
 */
extern Datum merklehandler(PG_FUNCTION_ARGS);

/*
 * Reloptions parsing
 */
extern bytea *merkle_options(Datum reloptions, bool validate);
extern MerkleOptions *merkle_get_options(Relation indexRel);
extern int merkle_get_update_mode(Relation indexRel);
extern int merkle_get_update_mode_by_oid(Oid index_oid);
extern bool merkle_has_pending_staged_delta(void);
extern bool merkle_has_synchronous_staged_delta(void);

/*
 * Helper to read tree config from metadata
 * (nodesPerPage and numTreePages can be NULL if not needed)
 */
extern void merkle_read_meta(Relation indexRel, int *numPartitions,
                             int *leavesPerPartition, int *nodesPerPartition,
                             int *totalNodes, int *totalLeaves,
                             int *nodesPerPage, int *numTreePages,
                             int *fanout);

/*
 * Index build functions
 */
extern IndexBuildResult *merkleBuild(Relation heapRel, Relation indexRel,
                                     struct IndexInfo *indexInfo);
extern void merkleBuildempty(Relation indexRel);

/*
 * Index modification functions
 */
extern bool merkleInsert(Relation indexRel, Datum *values, bool *isnull,
                         ItemPointer ht_ctid, Relation heapRel,
                         IndexUniqueCheck checkUnique,
                         struct IndexInfo *indexInfo);

extern IndexBulkDeleteResult *merkleBulkdelete(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback,
                                               void *callback_state);

extern IndexBulkDeleteResult *merkleVacuumcleanup(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats);

/*
 * Cost estimation
 */
extern void merkleCostEstimate(struct PlannerInfo *root,
                               struct IndexPath *path,
                               double loop_count,
                               Cost *indexStartupCost,
                               Cost *indexTotalCost,
                               Selectivity *indexSelectivity,
                               double *indexCorrelation,
                               double *indexPages);

/* Core Merkle tree operations. */
extern bool merkle_compute_row_hash(Relation heapRel, ItemPointer tid,
                                    MerkleHash *result);
extern void merkle_compute_slot_hash(Relation heapRel, TupleTableSlot *slot,
                                     MerkleHash *result);
extern void merkle_compute_route(Relation indexRel, Datum *values,
								 bool *isnull, int nkeys,
								 MerkleRoute *result);
extern void merkle_compute_item_identity(Relation indexRel, Datum *values,
									 bool *isnull, int nkeys,
									 MerkleItemIdentity *result);
extern void merkle_compute_dynamic_item_identity(Relation indexRel,
										 Datum *values, bool *isnull,
										 int nkeys, int partitions,
										 int max_key_bytes,
										 MerkleItemIdentity *result);
extern bool merkle_index_is_dynamic(Relation indexRel);
extern void merkle_native_init(Relation indexRel, int partitions,
							   uint64 baseline_apply_seq);
extern void merkle_native_build_from_oracle(Relation indexRel,
									uint64 baseline_apply_seq);
extern MerkleNativeBuildState *merkle_native_build_begin(Relation indexRel,
												 uint64 baseline_apply_seq);
extern void merkle_native_build_add(MerkleNativeBuildState *state,
									const MerkleItemIdentity *identity,
									const MerkleHash *hash);
extern void merkle_native_build_finish(MerkleNativeBuildState *state);
/* Mutation entry points are deliberately split by authority.  Strict COW
 * publication is only valid for an index configured synchronous_cow; pending
 * materialization is only valid for pending_log.  Keeping these as separate
 * APIs prevents a caller from accidentally publishing a root in the wrong
 * ordering domain. */
extern void merkle_native_publish_strict_transitions(
									 const MerkleDynamicTransition *transitions,
									 int count, uint16 sequence_domain,
									 uint64 sequence_epoch,
									 uint64 sequence_value);
extern void merkle_native_materialize_pending_transitions(
									 const MerkleDynamicTransition *transitions,
									 int count, uint16 sequence_domain,
									 uint64 sequence_epoch,
									 uint64 sequence_value);
extern void merkle_native_root(Relation indexRel, MerkleHash *hash,
							   uint64 *tuple_count);
extern bool merkle_native_verify_relations(Relation heapRel,
									Relation indexRel, Snapshot snapshot);
extern bool merkle_native_is_ready(Relation indexRel);
extern void merkle_native_vacuum(Relation indexRel,
							 IndexBulkDeleteResult *stats);
extern Datum merkle_native_get_partition_roots(PG_FUNCTION_ARGS);
extern Datum merkle_native_get_ranges(PG_FUNCTION_ARGS);
extern Datum merkle_native_get_range_items(PG_FUNCTION_ARGS);
extern Datum merkle_native_get_leaf_frontier(PG_FUNCTION_ARGS);
extern Datum merkle_native_tree_stats(PG_FUNCTION_ARGS);
extern Datum merkle_native_partition_roots_at(PG_FUNCTION_ARGS);
extern void merkle_stage_item_delta(Relation indexRel,
									const MerkleItemIdentity *identity,
									const MerkleHash *hash, bool is_insert);
extern void merkle_dynamic_apply_transition(const MerkleDynamicTransition *transition);
extern void merkle_dynamic_apply_update_batch(const MerkleDynamicTransition *transitions,
										  int count);
extern MerkleDynamicBuildState *merkle_dynamic_build_begin(Relation indexRel,
														 Relation heapRel,
														 int nkeys,
														 uint64 baseline_seq);
extern void merkle_dynamic_build_add(MerkleDynamicBuildState *state,
									 const MerkleItemIdentity *identity,
									 const MerkleHash *hash);
extern void merkle_dynamic_build_finish(MerkleDynamicBuildState *state);
extern void merkle_dynamic_validate_key_index(Relation heapRel,
											  Relation merkleIndexRel,
											  int nkeys);
extern bool merkle_dynamic_verify_relations(Relation heapRel,
											Relation indexRel,
											Snapshot snapshot);
extern void merkle_dynamic_root(Relation indexRel, MerkleHash *hash,
								uint64 *tuple_count);
extern char *merkle_dynamic_stats_json(Relation indexRel);
extern char *merkle_dynamic_single_key_text(Relation indexRel,
											const bytea *key_data);
extern void merkle_dynamic_vacuum_stats(Relation indexRel,
									IndexBulkDeleteResult *stats);
extern void merkle_dynamic_drop_state(Oid index_oid,
								  RelFileNode index_rnode);
extern bool merkle_relation_has_index(Relation rel);
extern void merkle_reject_ddl(Relation rel, const char *command);
extern void merkle_reject_concurrent_ddl(Oid index_oid, const char *command);
extern void merkle_geometry_from_index(Relation indexRel,
								   MerkleGeometry *geometry);
extern int merkle_geometry_global_node(const MerkleGeometry *geometry,
								   int partition, int node_in_partition);
extern int merkle_geometry_leaf_node(const MerkleGeometry *geometry,
								 int leaf_id);
extern int merkle_geometry_parent_node(const MerkleGeometry *geometry,
								   int node_in_partition);
extern int merkle_geometry_child_node(const MerkleGeometry *geometry,
								  int node_in_partition, int child_ordinal);
extern void merkle_update_tree_path(Relation indexRel, int leafId,
                                    MerkleHash *hash, bool isXorIn);
extern void merkle_stage_delta(Relation indexRel, int leafId,
								 const MerkleHash *hash);
extern bytea *merkle_serialize_staged_delta(uint64 raft_log_index,
										 uint32 item_ordinal,
										 int *delta_version);
extern void merkle_mark_staged_delta_persisted(void);
extern bool merkle_has_staged_delta(void);
extern void merkle_crash_failpoint(const char *name);
extern void merkle_init_tree(Relation indexRel, Oid heapOid,
							 MerkleOptions *opts, uint64 baseline_apply_seq);

/* Ordered committed-delta applier and freshness gates. */
extern uint64 merkle_apply_pending_internal(void);
extern uint64 merkle_apply_until_internal(uint64 required_seq);
extern uint64 merkle_raft_apply_target(const uint8 *epoch_id,
									   uint64 raft_log_index,
									   uint32 item_ordinal);
extern void merkle_get_recovery_status(MerkleRecoveryStatusData *status);
extern void merkle_require_fresh(void);
extern void merkle_mark_recovery_state(MerkleRecoveryState state,
									 const char *reason);
extern uint64 merkle_advance_terminal_prefix_spi(void);
extern Datum merkle_rebuild_legacy_indexes(PG_FUNCTION_ARGS);

/*
 * XOR operations on hashes
 */
extern void merkle_hash_xor(MerkleHash *dest, const MerkleHash *src);
extern void merkle_hash_zero(MerkleHash *hash);
extern bool merkle_hash_is_zero(const MerkleHash *hash);
extern char *merkle_hash_to_hex(const MerkleHash *hash);

/*
 * SQL-callable verification functions
 */
extern Datum merkle_verify(PG_FUNCTION_ARGS);
extern Datum merkle_verify_index(PG_FUNCTION_ARGS);
extern Datum merkle_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_root_hash_index(PG_FUNCTION_ARGS);
extern Datum merkle_tree_stats(PG_FUNCTION_ARGS);
extern Datum merkle_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_tuples(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_id(PG_FUNCTION_ARGS);
extern Datum merkle_bucket_for_key(PG_FUNCTION_ARGS);
extern Datum merkle_get_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_get_child_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_get_node_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_get_children_batch(PG_FUNCTION_ARGS);
extern Datum merkle_get_leaf_members(PG_FUNCTION_ARGS);
extern Datum merkle_get_partition_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_get_partition_root_hashes(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_profile_reset(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_profile_stats(PG_FUNCTION_ARGS);
extern Datum merkle_recovery_status(PG_FUNCTION_ARGS);
extern Datum merkle_apply_pending_sql(PG_FUNCTION_ARGS);
extern Datum merkle_apply_until_sql(PG_FUNCTION_ARGS);
extern Datum merkle_dynamic_verify(PG_FUNCTION_ARGS);
extern Datum merkle_dynamic_get_partition_roots(PG_FUNCTION_ARGS);
extern Datum merkle_dynamic_get_ranges(PG_FUNCTION_ARGS);
extern Datum merkle_dynamic_get_range_items(PG_FUNCTION_ARGS);
extern Datum merkle_dynamic_tree_stats(PG_FUNCTION_ARGS);

#endif /* MERKLE_H */
