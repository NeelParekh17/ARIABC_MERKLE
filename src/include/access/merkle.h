/*-------------------------------------------------------------------------
 *
 * merkle.h
 *    Merkle tree integrity index access method definitions
 *
 * This implements a Merkle tree as a PostgreSQL index type for data
 * integrity verification. On every INSERT/UPDATE/DELETE, the Merkle
 * tree is automatically updated by PostgreSQL's index infrastructure.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
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
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "utils/relcache.h"

/* GUC: Enable/disable Merkle index updates */
extern bool enable_merkle_index;

/*
 * Merkle tree configuration constants
 * 
 * The tree is organized as multiple subtrees for better distribution:
 * - NUM_SUBTREES: Number of independent subtrees (95 to fit in one page)
 * - LEAVES_PER_TREE: Leaves in each subtree (4, giving tree height 3)
 * - NODES_PER_TREE: Total nodes per subtree (2 * leaves - 1 = 7)
 * - TOTAL_LEAVES: Total leaf positions (95 * 4 = 380)
 * - TOTAL_NODES: Total nodes across all subtrees (95 * 7 = 665)
 *
 * Due to struct alignment, MerkleNode is 12 bytes (not 9).
 * 665 * 12 = 7980 bytes < 8192 - header ≈ 8168 usable bytes
 */
#define MERKLE_NUM_SUBTREES         95
#define MERKLE_LEAVES_PER_TREE      4
#define MERKLE_TREE_BASE            4   /* branching factor */
#define MERKLE_NODES_PER_TREE       7   /* 2 * LEAVES_PER_TREE - 1 */
#define MERKLE_TOTAL_LEAVES         380 /* NUM_SUBTREES * LEAVES_PER_TREE */
#define MERKLE_TOTAL_NODES          665 /* NUM_SUBTREES * NODES_PER_TREE */

/*
 * Hash configuration
 * Using 64-bit (8-byte) hashes derived from MD5 prefix
 * 
 * NOTE: We upgraded from 40-bit to 64-bit at no storage cost!
 * Due to struct alignment, MerkleNode was already 12 bytes
 * (4 nodeId + 5 hash + 3 padding). Now we use all 8 bytes
 * for the hash instead of wasting 3 bytes on padding.
 * 
 * Security: Collision threshold goes from 2^20 to 2^32 (4 billion rows)
 */
#define MERKLE_HASH_BITS            64
#define MERKLE_HASH_BYTES           8
#define MERKLE_MD5_PREFIX_LEN       16  /* Full MD5 hex = 32 chars, use first 16 for 8 bytes */

/*
 * Page layout constants
 */
#define MERKLE_METAPAGE_BLKNO       0
#define MERKLE_TREE_START_BLKNO     1
#define MERKLE_VERSION              2   /* Bump version for 64-bit hash format */

/*
 * Calculate how many nodes fit per page
 * Each node: 4 bytes (nodeId) + 8 bytes (hash) = 12 bytes (same as before!)
 * Page size 8192, minus header ~24 bytes = ~8168 usable
 * Max ~680 nodes per page, we use 665
 */
#define MERKLE_MAX_NODES_PER_PAGE   ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) / sizeof(MerkleNode))

/*
 * MerkleHash - 64-bit hash value stored in 8 bytes
 */
typedef struct MerkleHash
{
    uint8       data[MERKLE_HASH_BYTES];
} MerkleHash;

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
    int32           numSubtrees;        /* number of subtrees */
    int32           leavesPerTree;      /* leaves per subtree */
    int32           nodesPerTree;       /* nodes per subtree */
    int32           totalNodes;         /* total nodes in tree */
    int32           nodesPerPage;       /* how many nodes fit per page */
    int32           numTreePages;       /* number of pages for tree nodes */
} MerkleMetaPageData;

#define MerklePageGetMeta(page) \
    ((MerkleMetaPageData *) PageGetContents(page))

/*
 * MerkleOptions - User-configurable options for Merkle index
 * Parsed from CREATE INDEX ... WITH (subtrees=X, leaves_per_tree=Y)
 */
typedef struct MerkleOptions
{
    int32       vl_len_;        /* varlena header (required) */
    int         subtrees;
    int         leaves_per_tree;
} MerkleOptions;

/*
 * Handler function - returns IndexAmRoutine
 */
extern Datum merklehandler(PG_FUNCTION_ARGS);

/*
 * Reloptions parsing
 */
extern bytea *merkle_options(Datum reloptions, bool validate);
extern MerkleOptions *merkle_get_options(Relation indexRel);

/*
 * Helper to read tree config from metadata
 * (nodesPerPage and numTreePages can be NULL if not needed)
 */
extern void merkle_read_meta(Relation indexRel, int *numSubtrees,
                             int *leavesPerTree, int *nodesPerTree,
                             int *totalNodes, int *totalLeaves,
                             int *nodesPerPage, int *numTreePages);

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

/*
 * Core Merkle tree operations
 * (merkle_compute_partition_id handles both single and multi-column keys)
 */
extern void merkle_compute_row_hash(Relation heapRel, ItemPointer tid,
                                    MerkleHash *result);
extern int  merkle_compute_partition_id(Datum *values, bool *isnull,
                                        int nkeys, TupleDesc tupdesc,
                                        int numLeaves);
extern void merkle_update_tree_path(Relation indexRel, int leafId,
                                    MerkleHash *hash, bool isXorIn);
extern void merkle_init_tree(Relation indexRel, Oid heapOid,
                             MerkleOptions *opts);

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
extern Datum merkle_root_hash(PG_FUNCTION_ARGS);
extern Datum merkle_tree_stats(PG_FUNCTION_ARGS);
extern Datum merkle_node_hash(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_tuples(PG_FUNCTION_ARGS);
extern Datum merkle_leaf_id(PG_FUNCTION_ARGS);

#endif /* MERKLE_H */
