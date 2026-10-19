/*-------------------------------------------------------------------------
 * merkle_lsm.c -- PostgreSQL multi-level lookup index access method.
 *
 * Copyright (c) 2026, AriaBC contributors.
 * Scan/merge design derived from lsm3, Copyright (c) 2020 Postgres Professional.
 * Distributed under the PostgreSQL License; see LICENSE.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_opclass.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/index_selfuncs.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "merkle_lsm.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(merkle_lsm_handler);
PG_FUNCTION_INFO_V1(merkle_lsm_meta_handler);
PG_FUNCTION_INFO_V1(merkle_lsm_components);
PG_FUNCTION_INFO_V1(merkle_lsm_flush);
PG_FUNCTION_INFO_V1(merkle_lsm_config);
PG_FUNCTION_INFO_V1(merkle_lsm_stats);

static relopt_kind ml_relopt_kind;
static Oid ml_cached_base;
static Oid ml_cached_meta;

static void
ml_relcache_invalidate(Datum arg, Oid relid)
{
	ml_cached_base = InvalidOid;
	ml_cached_meta = InvalidOid;
}
extern void ml_register_workers(void);
void _PG_init(void);

typedef struct MlScan
{
	int			count;
	Relation	relation[MLSM_MAX_COMPONENTS];
	IndexScanDesc scan[MLSM_MAX_COMPONENTS];
	bool		eof[MLSM_MAX_COMPONENTS];
	bool		started;
	IndexTuple	last;
	BTScanInsert compare;
} MlScan;

/* Metadata relations have a distinct AM, so no nbtree operation can interpret
 * their pages. The sole discovery step follows internal pg_depend edges. */
Relation
ml_open_meta(Relation base, LOCKMODE mode, bool missing_ok)
{
	Oid			metaid = InvalidOid;

	if (ml_cached_base == RelationGetRelid(base))
		metaid = ml_cached_meta;
	else
	{
		Relation	dep = table_open(DependRelationId, AccessShareLock);
		ScanKeyData keys[2];
		SysScanDesc scan;
		HeapTuple	tuple;
		Oid			metaam = get_am_oid("merkle_lsm_meta", false);

		ScanKeyInit(&keys[0], Anum_pg_depend_refclassid,
					BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationRelationId));
		ScanKeyInit(&keys[1], Anum_pg_depend_refobjid,
					BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(base)));
		scan = systable_beginscan(dep, DependReferenceIndexId, true, NULL, 2, keys);
		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			Form_pg_depend d = (Form_pg_depend) GETSTRUCT(tuple);
			HeapTuple	ct;

			if (d->classid != RelationRelationId || d->deptype != DEPENDENCY_INTERNAL)
				continue;
			ct = SearchSysCache1(RELOID, ObjectIdGetDatum(d->objid));
			if (HeapTupleIsValid(ct))
			{
				if (((Form_pg_class) GETSTRUCT(ct))->relam == metaam)
					metaid = d->objid;
				ReleaseSysCache(ct);
			}
		}
		systable_endscan(scan);
		table_close(dep, AccessShareLock);
		if (OidIsValid(metaid))
		{
			ml_cached_base = RelationGetRelid(base);
			ml_cached_meta = metaid;
		}
	}
	if (!OidIsValid(metaid))
	{
		if (missing_ok)
			return NULL;
		elog(ERROR, "merkle_lsm metadata is missing for index %u", RelationGetRelid(base));
	}
	return index_open(metaid, mode);
}

static MlMeta *
ml_page_meta(Page page)
{
	return (MlMeta *) PageGetContents(page);
}

void
ml_read_meta(Relation meta, MlMeta *data)
{
	Buffer		buffer = ReadBuffer(meta, 0);

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	memcpy(data, ml_page_meta(BufferGetPage(buffer)), sizeof(*data));
	UnlockReleaseBuffer(buffer);
	if (data->magic != MLSM_MAGIC || data->version != MLSM_VERSION)
		elog(ERROR, "invalid merkle_lsm metadata in relation %u", RelationGetRelid(meta));
}

/* The caller serializes state changes with the single merger lock, or holds
 * the metadata buffer lock exclusively (flush only changes the flush bit). */
void
ml_write_meta(Relation meta, const MlMeta *data)
{
	Buffer		buffer = ReadBuffer(meta, 0);
	GenericXLogState *state;
	Page		page;
	bool		flush;

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	flush = ml_page_meta(BufferGetPage(buffer))->flush;
	state = GenericXLogStart(meta);
	page = GenericXLogRegisterBuffer(state, buffer, 0);
	memcpy(ml_page_meta(page), data, sizeof(*data));
	/* A concurrent flush request must survive a merger state transition. The
	 * merger clears it separately after inspecting all levels under lock. */
	ml_page_meta(page)->flush |= flush;
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

void
ml_initialize_meta(Relation meta, const MlMeta *data)
{
	Buffer		buffer = ReadBuffer(meta, P_NEW);
	GenericXLogState *state;
	Page		page;

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(meta);
	page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
	PageInit(page, BLCKSZ, 0);
	memcpy(ml_page_meta(page), data, sizeof(*data));
	((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData) + sizeof(*data);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

void
ml_empty_btree(Relation index)
{
	Buffer		buffer = ReadBuffer(index, P_NEW);
	GenericXLogState *state;
	Page		page;

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
	_bt_initmetapage(page, P_NONE, 0);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

/* Internal components must not appear in RelationGetIndexList: COPY and
 * ExecOpenIndices would otherwise retain locks on every frozen component. */
static void
ml_hide_component(Oid oid, Relation heap, Oid base)
{
	Relation	catalog = table_open(IndexRelationId, RowExclusiveLock);
	HeapTuple	tuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(oid));
	Form_pg_index form = (Form_pg_index) GETSTRUCT(tuple);
	ObjectAddress child;
	ObjectAddress parent;

	form->indisvalid = false;
	form->indisready = false;
	form->indislive = false;
	CatalogTupleUpdate(catalog, &tuple->t_self, tuple);
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
	ObjectAddressSet(child, RelationRelationId, oid);
	ObjectAddressSet(parent, RelationRelationId, base);
	recordDependencyOn(&child, &parent, DEPENDENCY_INTERNAL);
	CacheInvalidateRelcache(heap);
	CommandCounterIncrement();
}

static Oid
ml_create_component(Relation heap, Relation base, IndexInfo *info,
					const char *suffix, Oid am, Oid *classes)
{
	List	   *names = NIL;
	char		name[NAMEDATALEN];
	Oid			oid;
	int			i;

	/* The OID keeps names short, independent of the user's index name, and
	 * unique within the database even after a base rename. */
	snprintf(name, sizeof(name), "ml_%u_%s", RelationGetRelid(base), suffix);
	for (i = 0; i < info->ii_NumIndexAttrs; i++)
		names = lappend(names, pstrdup(NameStr(TupleDescAttr(RelationGetDescr(base), i)->attname)));
	oid = index_create(heap, name, InvalidOid, InvalidOid, InvalidOid,
					   InvalidOid, info, names, am, base->rd_rel->reltablespace,
					   base->rd_indcollation, classes, base->rd_indoption,
					   (Datum) 0, INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_INVALID,
					   0, false, true, NULL);
	ml_hide_component(oid, heap, RelationGetRelid(base));
	{
		ObjectAddress child;
		ObjectAddress method;

		/* index_create normally gets its AM dependency through an opclass.
		 * The private metadata AM deliberately has no public opclasses. */
		ObjectAddressSet(child, RelationRelationId, oid);
		ObjectAddressSet(method, AccessMethodRelationId, am);
		recordDependencyOn(&child, &method, DEPENDENCY_NORMAL);
	}
	return oid;
}

static IndexBuildResult *
ml_build(Relation heap, Relation base, IndexInfo *info)
{
	IndexBuildResult *result;
	MlMeta		data;
	MlOptions  *options = (MlOptions *) base->rd_options;
	Relation	meta;
	Oid			classes[INDEX_MAX_KEYS];
	IndexInfo  *component_info;
	int			level;
	int			slot;
	int			i;

	if (heap->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT || info->ii_Concurrent)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("merkle_lsm requires a permanent table and nonconcurrent index build")));
	meta = ml_open_meta(base, AccessExclusiveLock, true);
	memset(&data, 0, sizeof(data));
	data.magic = MLSM_MAGIC;
	data.version = MLSM_VERSION;
	data.base = RelationGetRelid(base);
	data.levels = options ? options->levels : 3;
	data.top_size = options ? options->top_size : 1024;
	data.size_ratio = options ? options->size_ratio : 16;
	{
		Relation build_relation = palloc(sizeof(RelationData));
		Form_pg_class build_class = palloc(sizeof(FormData_pg_class));
		IndexInfo build_info = *info;

		/* tuplesort requires a B-tree descriptor. A private view avoids any
		 * relcache relam mutation. Its smgr owner must also have a lifetime
		 * bounded by this call, including the ERROR path. */
		RelationCloseSmgr(base);
		*build_relation = *base;
		*build_class = *base->rd_rel;
		build_class->relam = BTREE_AM_OID;
		build_relation->rd_rel = build_class;
		build_relation->rd_amcache = NULL;
		build_info.ii_ParallelWorkers = 0;
		PG_TRY();
		{
			result = btbuild(heap, build_relation, &build_info);
			info->ii_BrokenHotChain = build_info.ii_BrokenHotChain;
			RelationCloseSmgr(build_relation);
		}
		PG_CATCH();
		{
			RelationCloseSmgr(build_relation);
			if (build_relation->rd_amcache)
				pfree(build_relation->rd_amcache);
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (build_relation->rd_amcache)
			pfree(build_relation->rd_amcache);
		pfree(build_class);
		pfree(build_relation);
	}
	if (meta != NULL)
	{
		MlMeta		old;

		ml_read_meta(meta, &old);
		if (old.levels != data.levels)
			ereport(ERROR, (errmsg("changing merkle_lsm levels requires DROP INDEX and CREATE INDEX")));
		memcpy(data.component, old.component, sizeof(data.component));
		for (level = 0; level < data.levels - 1; level++)
			for (slot = 0; slot < 2; slot++)
			{
				Relation c = index_open(data.component[level][slot], AccessExclusiveLock);

				TransferPredicateLocksToHeapRelation(c);
				RelationSetNewRelfilenode(c, c->rd_rel->relpersistence);
				ml_empty_btree(c);
				index_close(c, NoLock);
			}
		RelationSetNewRelfilenode(meta, meta->rd_rel->relpersistence);
	}
	else
	{
		/* The public opclasses mirror the built-in B-tree names. */
		for (i = 0; i < info->ii_NumIndexKeyAttrs; i++)
		{
			Oid			classid = get_index_column_opclass(RelationGetRelid(base), i + 1);
			HeapTuple	tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(classid));
			Form_pg_opclass opclass = (Form_pg_opclass) GETSTRUCT(tuple);
			List	   *name = list_make2(makeString("pg_catalog"), makeString(pstrdup(NameStr(opclass->opcname))));

			classes[i] = get_opclass_oid(BTREE_AM_OID, name, false);
			ReleaseSysCache(tuple);
		}
		component_info = BuildIndexInfo(base);
		component_info->ii_Concurrent = false;
		component_info->ii_Unique = false;
		for (level = 0; level < data.levels - 1; level++)
			for (slot = 0; slot < 2; slot++)
			{
				char		suffix[24];
				Relation	component;

				snprintf(suffix, sizeof(suffix), "l%d_s%d", level, slot);
				data.component[level][slot] = ml_create_component(heap, base, component_info, suffix, BTREE_AM_OID, classes);
				component = index_open(data.component[level][slot], AccessExclusiveLock);
				ml_empty_btree(component);
				index_close(component, NoLock);
			}
		meta = index_open(ml_create_component(heap, base, component_info, "meta",
											 get_am_oid("merkle_lsm_meta", false), classes), AccessExclusiveLock);
	}
	ml_initialize_meta(meta, &data);
	index_close(meta, NoLock);
	return result;
}

static bytea *
ml_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(MlOptions, bt.fillfactor)},
		{"vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL, offsetof(MlOptions, bt.vacuum_cleanup_index_scale_factor)},
		{"levels", RELOPT_TYPE_INT, offsetof(MlOptions, levels)},
		{"top_index_size", RELOPT_TYPE_INT, offsetof(MlOptions, top_size)},
		{"size_ratio", RELOPT_TYPE_INT, offsetof(MlOptions, size_ratio)}
	};

	return build_reloptions(reloptions, validate, ml_relopt_kind,
							sizeof(MlOptions), tab, lengthof(tab));
}

static bool
ml_insert(Relation base, Datum *values, bool *isnull, ItemPointer tid,
		  Relation heap, IndexUniqueCheck check, IndexInfo *info)
{
	Relation	meta = ml_open_meta(base, AccessShareLock, false);
	Buffer		buffer = ReadBuffer(meta, 0);
	MlMeta	   *data;
	Relation	active;

	/* Freeze waits only for insert callbacks, never for transaction completion.
	 * Keep the SHARE buffer lock until btinsert has released its page locks.
	 * ERROR cleanup releases buffer locks and relation references normally. */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	data = ml_page_meta(BufferGetPage(buffer));
	active = index_open(data->component[0][data->active[0]], RowExclusiveLock);
	btinsert(active, values, isnull, tid, heap, UNIQUE_CHECK_NO, info);
	index_close(active, RowExclusiveLock);
	UnlockReleaseBuffer(buffer);
	index_close(meta, AccessShareLock);
	return false;
}

/* Compare using the actual opclass order function; no relcache relam writes
 * and no dependency on sortsupport's B-tree-AM-only assertion. */
static int
ml_compare(MlScan *so, IndexTuple a, IndexTuple b, TupleDesc desc)
{
	int			i;

	for (i = 0; i < so->compare->keysz; i++)
	{
		ScanKey		key = &so->compare->scankeys[i];
		bool		anull;
		bool		bnull;
		Datum		av = index_getattr(a, i + 1, desc, &anull);
		Datum		bv = index_getattr(b, i + 1, desc, &bnull);
		int32		cmp;

		if (anull || bnull)
		{
			if (anull && bnull)
				continue;
			return (anull == ((key->sk_flags & SK_BT_NULLS_FIRST) != 0)) ? -1 : 1;
		}
		cmp = DatumGetInt32(FunctionCall2Coll(&key->sk_func, key->sk_collation, av, bv));
		if (cmp != 0)
			return (key->sk_flags & SK_BT_DESC) ? (cmp < 0 ? 1 : -1) : (cmp < 0 ? -1 : 1);
	}
	return ItemPointerCompare(&a->t_tid, &b->t_tid);
}

static IndexScanDesc
ml_beginscan(Relation base, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(base, nkeys, norderbys);
	MlScan	   *so = palloc0(sizeof(*so));
	Relation	meta = ml_open_meta(base, AccessShareLock, true);
	MlMeta		data;
	int			i;
	int			j;

	so->relation[so->count++] = base;
	if (meta)
	{
		ml_read_meta(meta, &data);
		index_close(meta, AccessShareLock);
		for (i = 0; i < data.levels - 1; i++)
			for (j = 0; j < 2; j++)
				so->relation[so->count++] = index_open(data.component[i][j], AccessShareLock);
	}
	so->compare = _bt_mkscankey(base, NULL);
	/* With no tuple, nbtree initializes keysz to zero. We compare full keys. */
	so->compare->keysz = IndexRelationGetNumberOfKeyAttributes(base);
	for (i = 0; i < so->count; i++)
	{
		so->scan[i] = ml_btbegin(so->relation[i], nkeys, norderbys);
		so->scan[i]->xs_want_itup = true;
	}
	scan->opaque = so;
	scan->xs_itupdesc = RelationGetDescr(base);
	return scan;
}

static void
ml_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	MlScan	   *so = scan->opaque;
	int			i;

	if (so->last)
		pfree(so->last);
	so->last = NULL;
	so->started = false;
	for (i = 0; i < so->count; i++)
	{
		btrescan(so->scan[i], keys, nkeys, orderbys, norderbys);
		so->eof[i] = false;
	}
}

static bool
ml_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	MlScan	   *so = scan->opaque;
	int			best = -1;
	int			i;

	if (!ScanDirectionIsForward(dir))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("merkle_lsm supports forward scans only")));
	for (i = 0; i < so->count; i++)
	{
		IndexScanDesc part = so->scan[i];

		part->xs_snapshot = scan->xs_snapshot;
		if (!so->started)
			so->eof[i] = !btgettuple(part, dir);
		/* Skip every copy of the last emitted (key,TID), including repeated
		 * copies within one component and duplicates in three or more trees. */
		while (!so->eof[i] && so->last &&
			   ml_compare(so, part->xs_itup, so->last, scan->xs_itupdesc) == 0)
			so->eof[i] = !btgettuple(part, dir);
		if (!so->eof[i] && (best < 0 ||
			ml_compare(so, part->xs_itup, so->scan[best]->xs_itup, scan->xs_itupdesc) < 0))
			best = i;
	}
	so->started = true;
	if (best < 0)
		return false;
	if (so->last)
		pfree(so->last);
	so->last = CopyIndexTuple(so->scan[best]->xs_itup);
	scan->xs_heaptid = so->scan[best]->xs_heaptid;
	scan->xs_recheck = false;
	scan->xs_itup = so->last;
	return true;
}

static int64
ml_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
	MlScan	   *so = scan->opaque;
	int64		count = 0;
	int			i;

	for (i = 0; i < so->count; i++)
	{
		so->scan[i]->xs_snapshot = scan->xs_snapshot;
		count += btgetbitmap(so->scan[i], bitmap);
	}
	return count;
}

static void
ml_endscan(IndexScanDesc scan)
{
	MlScan	   *so = scan->opaque;
	int			i;

	for (i = 0; i < so->count; i++)
	{
		ml_btend(so->scan[i]);
		if (i > 0)
			index_close(so->relation[i], AccessShareLock);
	}
	if (so->last)
		pfree(so->last);
	pfree(so->compare);
	pfree(so);
}

/* VACUUM owns the heap ShareUpdateExclusiveLock. The merger takes that same
 * lock, so it cannot reintroduce a TID after this sweep removed it. */
static IndexBulkDeleteResult *
ml_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			  IndexBulkDeleteCallback callback, void *state)
{
	Relation	meta = ml_open_meta(info->index, AccessShareLock, false);
	MlMeta		data;
	int			i;
	int			j;

	ml_read_meta(meta, &data);
	index_close(meta, AccessShareLock);
	stats = btbulkdelete(info, stats, callback, state);
	for (i = 0; i < data.levels - 1; i++)
		for (j = 0; j < 2; j++)
		{
			IndexVacuumInfo part = *info;
			IndexBulkDeleteResult *partstats;

			part.index = index_open(data.component[i][j], RowExclusiveLock);
			partstats = btbulkdelete(&part, NULL, callback, state);
			if (partstats)
			{
				partstats = btvacuumcleanup(&part, partstats);
				stats->num_index_tuples += partstats->num_index_tuples;
				stats->tuples_removed += partstats->tuples_removed;
				stats->estimated_count |= partstats->estimated_count;
				pfree(partstats);
			}
			index_close(part.index, RowExclusiveLock);
		}
	return stats;
}

static void
ml_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				Cost *startup, Cost *total, Selectivity *selectivity,
				double *correlation, double *pages)
{
	/* The ordinary estimator knows the heap's row count but not our hidden
	 * trees. Charge one additional root descent for each of the six possible
	 * upper components; this is deliberately conservative for levels=2/3. */
	btcostestimate(root, path, loop_count, startup, total, selectivity, correlation, pages);
	*startup += 6 * (random_page_cost + 8 * cpu_operator_cost);
	*total += 6 * (random_page_cost + 8 * cpu_operator_cost);
	*correlation = 0;
}

Datum
merkle_lsm_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *am = (IndexAmRoutine *) DatumGetPointer(bthandler(fcinfo));

	am->amcanunique = false;
	am->amcanbackward = false;
	am->amcanparallel = false;
	am->amcanorder = true;
	am->amsearcharray = false;
	am->amclusterable = false;
	/* Component B-trees take their own page/relation SIRead locks. New user
	 * entries only enter the active L0 tree, which every scan also reads (an
	 * empty tree is locked as a whole); merger copies are not new logical
	 * entries, and source replacement transfers its locks to the heap. A
	 * whole-index lock from indexam.c would make each Merkle split conflict
	 * with every concurrent insert. */
	am->ampredlocks = true;
	am->ambuild = ml_build;
	am->aminsert = ml_insert;
	am->ambulkdelete = ml_bulkdelete;
	am->amoptions = ml_options;
	am->amcostestimate = ml_costestimate;
	am->ambeginscan = ml_beginscan;
	am->amrescan = ml_rescan;
	am->amgettuple = ml_gettuple;
	am->amgetbitmap = ml_getbitmap;
	am->amendscan = ml_endscan;
	am->ammarkpos = NULL;
	am->amrestrpos = NULL;
	am->amestimateparallelscan = NULL;
	am->aminitparallelscan = NULL;
	am->amparallelrescan = NULL;
	PG_RETURN_POINTER(am);
}

Datum
merkle_lsm_meta_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *am = makeNode(IndexAmRoutine);

	/* Private storage: only index_create(SKIP_BUILD) may construct one. */
	am->amstrategies = BTMaxStrategyNumber;
	am->amsupport = BTNProcs;
	am->amcanmulticol = true;
	am->amcaninclude = true;
	am->amkeytype = InvalidOid;
	PG_RETURN_POINTER(am);
}

static Relation
ml_checked_base(Oid oid)
{
	Relation	base = index_open(oid, AccessShareLock);

	if (base->rd_indam->ambuild != ml_build)
		ereport(ERROR, (errmsg("relation %u is not a merkle_lsm index", oid)));
	return base;
}

Datum
merkle_lsm_config(PG_FUNCTION_ARGS)
{
	Relation	base = ml_checked_base(PG_GETARG_OID(0));
	Relation	meta = ml_open_meta(base, AccessShareLock, false);
	MlMeta		data;
	char	   *json;

	ml_read_meta(meta, &data);
	index_close(meta, AccessShareLock);
	index_close(base, AccessShareLock);
	json = psprintf("{\"levels\":%d,\"top_index_size\":%d,\"size_ratio\":%d}",
					data.levels, data.top_size, data.size_ratio);
	PG_RETURN_DATUM(DirectFunctionCall1(json_in, CStringGetDatum(json)));
}

/* Cheap enough to poll during a benchmark: one metadata page and smgr sizes,
 * no component scans (merkle_lsm_components counts entries by scanning). */
Datum
merkle_lsm_stats(PG_FUNCTION_ARGS)
{
	Relation	base = ml_checked_base(PG_GETARG_OID(0));
	Relation	meta = ml_open_meta(base, AccessShareLock, false);
	MlMeta		data;
	StringInfoData buf;
	int			level;
	int			slot;

	ml_read_meta(meta, &data);
	index_close(meta, AccessShareLock);
	initStringInfo(&buf);
	appendStringInfo(&buf, "{\"phase\":\"%s\",\"merge_level\":%d,\"epoch\":" UINT64_FORMAT
					 ",\"flush\":%s,\"last_merge_us\":" INT64_FORMAT ",\"merges\":[",
					 data.phase == MLSM_COPY ? "copy" : (data.phase == MLSM_COPIED ? "copied" : "idle"),
					 data.merge_level, data.epoch, data.flush ? "true" : "false", data.last_us);
	for (level = 0; level < data.levels - 1; level++)
		appendStringInfo(&buf, "%s" UINT64_FORMAT, level ? "," : "", data.merges[level]);
	appendStringInfoString(&buf, "],\"active\":[");
	for (level = 0; level < data.levels - 1; level++)
		appendStringInfo(&buf, "%s%d", level ? "," : "", data.active[level]);
	appendStringInfoString(&buf, "],\"bytes\":[");
	for (level = 0; level < data.levels - 1; level++)
	{
		appendStringInfo(&buf, "%s[", level ? "," : "");
		for (slot = 0; slot < 2; slot++)
		{
			Relation	part = index_open(data.component[level][slot], AccessShareLock);

			appendStringInfo(&buf, "%s" INT64_FORMAT, slot ? "," : "",
							 (int64) RelationGetNumberOfBlocks(part) * BLCKSZ);
			index_close(part, AccessShareLock);
		}
		appendStringInfoChar(&buf, ']');
	}
	appendStringInfo(&buf, "],\"base_bytes\":" INT64_FORMAT "}",
					 (int64) RelationGetNumberOfBlocks(base) * BLCKSZ);
	index_close(base, AccessShareLock);
	PG_RETURN_DATUM(DirectFunctionCall1(json_in, CStringGetDatum(buf.data)));
}

Datum
merkle_lsm_flush(PG_FUNCTION_ARGS)
{
	Relation	base = ml_checked_base(PG_GETARG_OID(0));
	Relation	meta;
	Buffer		buffer;
	GenericXLogState *state;
	Page		page;
	MlMeta		data;

	if (!pg_class_ownercheck(RelationGetRelid(base), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, RelationGetRelationName(base));
	meta = ml_open_meta(base, AccessShareLock, false);
	buffer = ReadBuffer(meta, 0);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(meta);
	page = GenericXLogRegisterBuffer(state, buffer, 0);
	ml_page_meta(page)->flush = true;
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		ml_read_meta(meta, &data);
		if (!data.flush && data.phase == MLSM_IDLE)
			break;
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 100L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	index_close(meta, AccessShareLock);
	index_close(base, AccessShareLock);
	PG_RETURN_VOID();
}

Datum
merkle_lsm_components(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	desc;
	Tuplestorestate *store;
	MemoryContext old;
	Relation	base = ml_checked_base(PG_GETARG_OID(0));
	Relation	meta = ml_open_meta(base, AccessShareLock, false);
	MlMeta		data;
	int			level;
	int			slot;

	if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "record result type required");
	old = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	store = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = store;
	rsinfo->setDesc = BlessTupleDesc(desc);
	MemoryContextSwitchTo(old);
	ml_read_meta(meta, &data);
	for (level = 0; level < data.levels; level++)
		for (slot = 0; slot < (level == data.levels - 1 ? 1 : 2); slot++)
		{
			Relation	part = level == data.levels - 1 ? base : index_open(data.component[level][slot], AccessShareLock);
			IndexScanDesc scan = ml_btbegin(part, 0, 0);
			int64		entries = 0;
			Datum		values[9];
			bool		nulls[9] = {false};
			const char *role;

			scan->xs_snapshot = SnapshotAny;
			btrescan(scan, NULL, 0, NULL, 0);
			while (btgettuple(scan, ForwardScanDirection))
				entries++;
			ml_btend(scan);
			role = level == data.levels - 1 ? "base" :
				(data.phase != MLSM_IDLE && data.merge_level == level && data.merge_slot == slot ?
				 (data.phase == MLSM_COPY ? "copying" : "copied") :
				 (data.active[level] == slot ? "active" : "idle"));
			values[0] = Int32GetDatum(level);
			values[1] = Int32GetDatum(slot);
			values[2] = ObjectIdGetDatum(RelationGetRelid(part));
			values[3] = CStringGetTextDatum(role);
			values[4] = Int64GetDatum((int64) RelationGetNumberOfBlocks(part) * BLCKSZ);
			values[5] = Int64GetDatum(entries);
			values[6] = Int64GetDatum(data.epoch);
			values[7] = Int64GetDatum(level == data.levels - 1 ? 0 : data.merges[level]);
			values[8] = Int64GetDatum(data.last_us);
			tuplestore_putvalues(store, desc, values, nulls);
			if (part != base)
				index_close(part, AccessShareLock);
		}
	index_close(meta, AccessShareLock);
	index_close(base, AccessShareLock);
	tuplestore_donestoring(store);
	PG_RETURN_NULL();
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR, (errmsg("merkle_lsm must be loaded through shared_preload_libraries")));
	CacheRegisterRelcacheCallback(ml_relcache_invalidate, (Datum) 0);
	ml_relopt_kind = add_reloption_kind();
	add_int_reloption(ml_relopt_kind, "levels", "Number of LSM levels", 3, 2, 4, AccessExclusiveLock);
	add_int_reloption(ml_relopt_kind, "top_index_size", "L0 target size in KiB", 1024, 16, INT_MAX, AccessExclusiveLock);
	add_int_reloption(ml_relopt_kind, "size_ratio", "Target size ratio between levels", 16, 2, 1024, AccessExclusiveLock);
	add_int_reloption(ml_relopt_kind, "fillfactor", "B-tree fill factor", 90, 10, 100, ShareUpdateExclusiveLock);
	add_real_reloption(ml_relopt_kind, "vacuum_cleanup_index_scale_factor", "B-tree vacuum cleanup scale", -1, 0, 1e10, ShareUpdateExclusiveLock);
	if (process_shared_preload_libraries_in_progress)
		ml_register_workers();
}
