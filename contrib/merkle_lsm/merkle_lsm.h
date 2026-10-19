/* Multi-level Merkle lookup AM.  Copyright (c) 2026, AriaBC contributors.
 * Scan/merge design derived from lsm3, Copyright (c) 2020 Postgres Professional.
 * Distributed under the PostgreSQL License; see LICENSE.
 */
#ifndef MERKLE_LSM_H
#define MERKLE_LSM_H

#include "access/nbtree.h"
#include "access/relscan.h"
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"

#define MLSM_MAGIC 0x4d4c534d
#define MLSM_VERSION 1
#define MLSM_MAX_LEVELS 4
#define MLSM_MAX_COMPONENTS 7
#define MLSM_IDLE 0
#define MLSM_COPY 1
#define MLSM_COPIED 2

typedef struct MlOptions
{
	BTOptions	bt;
	int			levels;
	int			top_size;
	int			size_ratio;
} MlOptions;

/* One WAL-logged page in an internal metadata AM relation, never in a
 * B-tree metapage (nbtree full-page WAL rebuilds that page independently). */
typedef struct MlMeta
{
	uint32		magic;
	uint32		version;
	Oid			base;
	int			levels;
	int			top_size;
	int			size_ratio;
	Oid			component[MLSM_MAX_LEVELS - 1][2];
	int			active[MLSM_MAX_LEVELS - 1];
	int			phase;
	int			merge_level;
	int			merge_slot;
	bool		flush;
	uint64		epoch;
	uint64		merges[MLSM_MAX_LEVELS - 1];
	int64		last_us;
	int64		started_us;
} MlMeta;

static inline IndexScanDesc
ml_btbegin(Relation relation, int nkeys, int norderbys)
{
	IndexScanDesc scan = btbeginscan(relation, nkeys, norderbys);

	scan->parallel_scan = NULL;
	return scan;
}

static inline void
ml_btend(IndexScanDesc scan)
{
	btendscan(scan);
	IndexScanEnd(scan);
}

extern void ml_read_meta(Relation meta, MlMeta *data);
extern void ml_write_meta(Relation meta, const MlMeta *data);
extern Relation ml_open_meta(Relation base, LOCKMODE mode, bool missing_ok);
extern void ml_empty_btree(Relation index);
extern void ml_initialize_meta(Relation meta, const MlMeta *data);
extern bool ml_merge_step(Oid baseoid);

#endif
