/*-------------------------------------------------------------------------
 *
 * merklelookup.c
 *	  Reader for the Merkle lookup index.
 *
 * The lookup index is an ordinary secondary index (btree or merkle_lsm)
 * created by merkle_create_lookup_index() with exactly three key columns:
 *
 *     (merkle_partition_for_hash(H, P), H, merkle_tuple_hash(<table>.*))
 *
 * where H is the Merkle route hash of the row's key.  Rows of one Merkle
 * node are one range of the first two columns, and every entry carries the
 * row's 8-byte tuple hash.  Because the third column depends on the whole
 * row, PostgreSQL never performs HOT updates on the table: every INSERT and
 * UPDATE adds an entry for the new row version, and entries of deleted or
 * superseded versions stay until VACUUM removes them.  Readers therefore
 * filter entries by heap visibility, exactly like an index-only scan.
 *
 * merkle_lookup_scan() returns (key hash, tuple hash) for the visible rows
 * of one node range without reading heap pages that the visibility map
 * marks all-visible.  Node splits and replica repair use it instead of
 * re-reading and re-hashing every row from the heap.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/merkle.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/primnodes.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

/* Name of the function at the top of an index expression, or NULL. */
static const char *
lookup_expr_func_name(Node *expr, int nargs)
{
	FuncExpr   *f;

	if (expr == NULL || !IsA(expr, FuncExpr))
		return NULL;
	f = (FuncExpr *) expr;
	if (list_length(f->args) != nargs)
		return NULL;
	return get_func_name(f->funcid);
}

/*
 * Does this index have the lookup-index shape described in the file header?
 * num_partitions > 0 also requires the partition count to match the Merkle
 * index, so a lookup index built for another geometry is never used.
 */
static bool
merkle_index_is_lookup(Relation indexRel, int num_partitions)
{
	Form_pg_index idx = indexRel->rd_index;
	List	   *exprs;
	Node	   *e_part;
	Node	   *e_hash;
	Node	   *e_tuple;
	const char *name;
	FuncExpr   *part_fn;
	FuncExpr   *tuple_fn;
	Node	   *part_count;
	Node	   *row_arg;
	int			i;

	if (!idx->indisvalid || !idx->indisready || !idx->indislive)
		return false;
	if (idx->indnkeyatts != 3 || RelationGetIndexPredicate(indexRel) != NIL)
		return false;
	if (indexRel->rd_indam == NULL || indexRel->rd_indam->amgettuple == NULL)
		return false;
	for (i = 0; i < 3; i++)
		if (idx->indkey.values[i] != 0)
			return false;

	exprs = RelationGetIndexExpressions(indexRel);
	if (list_length(exprs) < 3)
		return false;
	e_part = (Node *) linitial(exprs);
	e_hash = (Node *) lsecond(exprs);
	e_tuple = (Node *) lthird(exprs);

	name = lookup_expr_func_name(e_part, 2);
	if (name == NULL || strcmp(name, "merkle_partition_for_hash") != 0)
		return false;
	part_fn = (FuncExpr *) e_part;
	if (!equal(linitial(part_fn->args), e_hash))
		return false;
	part_count = (Node *) lsecond(part_fn->args);
	if (num_partitions > 0)
	{
		Const	   *c;

		if (!IsA(part_count, Const))
			return false;
		c = (Const *) part_count;
		if (c->constisnull || c->consttype != INT4OID ||
			DatumGetInt32(c->constvalue) != num_partitions)
			return false;
	}

	name = lookup_expr_func_name(e_hash, 1);
	if (name == NULL)
		name = lookup_expr_func_name(e_hash, 4);
	if (name == NULL ||
		(strcmp(name, "merkle_key_hash") != 0 &&
		 strcmp(name, "merkle_key_hash_routed") != 0))
		return false;

	name = lookup_expr_func_name(e_tuple, 1);
	if (name == NULL || strcmp(name, "merkle_tuple_hash") != 0)
		return false;
	tuple_fn = (FuncExpr *) e_tuple;
	row_arg = (Node *) linitial(tuple_fn->args);
	if (!IsA(row_arg, Var) || ((Var *) row_arg)->varattno != 0)
		return false;

	return true;
}

/*
 * merkle_find_lookup_index() - OID of the heap's lookup index, or InvalidOid.
 *
 * An index built by CREATE INDEX over broken HOT chains is unusable to
 * snapshots older than its creation (indcheckxmin), as for the planner.
 */
Oid
merkle_find_lookup_index(Relation heapRel, int num_partitions, Snapshot snapshot)
{
	List	   *indexList = RelationGetIndexList(heapRel);
	ListCell   *lc;
	Oid			result = InvalidOid;

	foreach(lc, indexList)
	{
		Oid			indexOid = lfirst_oid(lc);
		Relation	indexRel = index_open(indexOid, AccessShareLock);
		bool		ok = merkle_index_is_lookup(indexRel, num_partitions);

		if (ok && indexRel->rd_index->indcheckxmin)
		{
			TransactionId xmin = HeapTupleHeaderGetXmin(indexRel->rd_indextuple->t_data);

			if (snapshot == NULL || !TransactionIdPrecedes(xmin, snapshot->xmin))
				ok = false;
		}
		index_close(indexRel, AccessShareLock);
		if (ok)
		{
			result = indexOid;
			break;
		}
	}
	list_free(indexList);
	return result;
}

static bytea *
lookup_bytea8(const uint8 *bytes)
{
	bytea	   *b = (bytea *) palloc(VARHDRSZ + 8);

	SET_VARSIZE(b, VARHDRSZ + 8);
	memcpy(VARDATA(b), bytes, 8);
	return b;
}

/*
 * merkle_lookup_scan() - (key hash, tuple hash) of the rows visible to
 * snapshot whose route hash lies in [lower, upper] within partition_id.
 *
 * Values come from the index entries.  A heap page is read only when the
 * visibility map does not mark it all-visible, to decide whether the entry's
 * row version is visible.  The table has no HOT chains while the lookup
 * index exists (its whole-row column blocks HOT), so the entry always
 * describes the version it points to.
 */
MerkleTupleHashEntry *
merkle_lookup_scan(Relation heapRel, Oid lookupOid, int partition_id,
				   const uint8 *lower, const uint8 *upper, Snapshot snapshot,
				   int *num_entries, uint64 *heap_fetches)
{
	Relation	indexRel;
	IndexScanDesc scan;
	ScanKeyData keys[3];
	TupleTableSlot *slot;
	Buffer		vmbuf = InvalidBuffer;
	ItemPointer tid;
	MerkleTupleHashEntry *entries;
	int			n = 0;
	int			cap = 256;
	uint64		fetches = 0;
	bytea	   *lo_b = lookup_bytea8(lower);
	bytea	   *hi_b = lookup_bytea8(upper);

	if (snapshot == NULL || !IsMVCCSnapshot(snapshot))
		elog(ERROR, "Merkle lookup scans require an MVCC snapshot");

	indexRel = index_open(lookupOid, AccessShareLock);

	ScanKeyInit(&keys[0], 1, BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum((int16) partition_id));
	ScanKeyInit(&keys[1], 2, BTGreaterEqualStrategyNumber, F_BYTEAGE,
				PointerGetDatum(lo_b));
	ScanKeyInit(&keys[2], 2, BTLessEqualStrategyNumber, F_BYTEALE,
				PointerGetDatum(hi_b));

	scan = index_beginscan(heapRel, indexRel, snapshot, 3, 0);
	scan->xs_want_itup = true;
	index_rescan(scan, keys, 3, NULL, 0);
	slot = table_slot_create(heapRel, NULL);

	entries = (MerkleTupleHashEntry *) palloc(cap * sizeof(MerkleTupleHashEntry));

	while ((tid = index_getnext_tid(scan, ForwardScanDirection)) != NULL)
	{
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		bytea	   *kh;
		bytea	   *th;

		CHECK_FOR_INTERRUPTS();

		if (!VM_ALL_VISIBLE(heapRel, ItemPointerGetBlockNumber(tid), &vmbuf))
		{
			fetches++;
			if (!index_fetch_heap(scan, slot))
				continue;
			ExecClearTuple(slot);
			if (scan->xs_heap_continue)
				elog(ERROR, "Merkle lookup index \"%s\" points into a HOT chain",
					 RelationGetRelationName(indexRel));
		}
		else
			PredicateLockPage(heapRel, ItemPointerGetBlockNumber(tid), snapshot);

		if (scan->xs_itup == NULL)
			elog(ERROR, "Merkle lookup index \"%s\" cannot return index tuples",
				 RelationGetRelationName(indexRel));
		index_deform_tuple(scan->xs_itup, scan->xs_itupdesc, values, isnull);
		if (isnull[1] || isnull[2])
			elog(ERROR, "Merkle lookup index \"%s\" holds a NULL hash",
				 RelationGetRelationName(indexRel));
		kh = DatumGetByteaPP(values[1]);
		th = DatumGetByteaPP(values[2]);
		if (VARSIZE_ANY_EXHDR(kh) != 8 || VARSIZE_ANY_EXHDR(th) != MERKLE_TUPLE_HASH_BYTES)
			elog(ERROR, "Merkle lookup index \"%s\" holds a hash of unexpected length",
				 RelationGetRelationName(indexRel));

		if (n == cap)
		{
			cap *= 2;
			entries = (MerkleTupleHashEntry *) repalloc(entries, cap * sizeof(MerkleTupleHashEntry));
		}
		memcpy(entries[n].key_hash, VARDATA_ANY(kh), 8);
		entries[n].partition_id = (uint32) partition_id;
		memcpy(entries[n].tuple_hash.data, VARDATA_ANY(th), MERKLE_TUPLE_HASH_BYTES);
		n++;
	}

	if (BufferIsValid(vmbuf))
		ReleaseBuffer(vmbuf);
	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(indexRel, AccessShareLock);
	pfree(lo_b);
	pfree(hi_b);

	*num_entries = n;
	if (heap_fetches)
		*heap_fetches = fetches;
	return entries;
}

/* The table's (first) Merkle index, or InvalidOid. */
static Oid
merkle_index_of_table(Relation heapRel)
{
	List	   *indexList = RelationGetIndexList(heapRel);
	ListCell   *lc;
	Oid			result = InvalidOid;

	foreach(lc, indexList)
	{
		Oid			indexOid = lfirst_oid(lc);
		Relation	indexRel = index_open(indexOid, AccessShareLock);
		bool		is_merkle = (indexRel->rd_rel->relam == MERKLE_AM_OID);

		index_close(indexRel, AccessShareLock);
		if (is_merkle)
		{
			result = indexOid;
			break;
		}
	}
	list_free(indexList);
	return result;
}

static int
merkle_table_partitions(Oid merkleIndexOid)
{
	Relation	indexRel = index_open(merkleIndexOid, AccessShareLock);
	int			num_partitions = MERKLE_DEFAULT_PARTITIONS;

	merkle_read_meta(indexRel, NULL, NULL, NULL, &num_partitions);
	index_close(indexRel, AccessShareLock);
	return num_partitions;
}

/*
 * merkle_lookup_index(table regclass) RETURNS regclass
 *
 * The lookup index that splits and repair will use, or NULL if the table has
 * none of the current shape (for example one built before tuple hashes were
 * stored in it).
 */
Datum
merkle_lookup_index_sql(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	heapRel;
	Oid			merkleOid;
	Oid			lookupOid = InvalidOid;

	heapRel = table_open(relid, AccessShareLock);
	merkleOid = merkle_index_of_table(heapRel);
	if (OidIsValid(merkleOid))
		lookupOid = merkle_find_lookup_index(heapRel,
											 merkle_table_partitions(merkleOid),
											 GetActiveSnapshot());
	table_close(heapRel, AccessShareLock);

	if (!OidIsValid(lookupOid))
		PG_RETURN_NULL();
	PG_RETURN_OID(lookupOid);
}

/*
 * merkle_range_entries(tbl regclass, partition int4, lo bytea, hi bytea)
 *   RETURNS TABLE(key_hash bytea, tuple_hash bytea)
 *
 * (key hash, tuple hash) of every row of tbl visible to the current snapshot
 * whose Merkle route lies in partition and [lo, hi].  Uses the lookup index
 * when the table has one of the current shape, and otherwise re-hashes the
 * rows from the heap.  Replica repair compares these lists between replicas
 * and fetches only rows whose pair differs.
 */
Datum
merkle_range_entries_sql(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			relid = PG_GETARG_OID(0);
	int32		partition_id = PG_GETARG_INT32(1);
	bytea	   *lo = PG_GETARG_BYTEA_PP(2);
	bytea	   *hi = PG_GETARG_BYTEA_PP(3);
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;
	Relation	heapRel;
	Oid			merkleOid;
	AclResult	aclresult;
	MerkleTupleHashEntry *entries;
	int			num_entries = 0;
	int			i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("merkle_range_entries must be called in a context that accepts a set")));
	if (VARSIZE_ANY_EXHDR(lo) != 8 || VARSIZE_ANY_EXHDR(hi) != 8)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("merkle_range_entries bounds must be 8-byte route hashes")));
	if (partition_id < 0 || partition_id > MERKLE_MAX_PARTITIONS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Merkle partition %d is out of range", partition_id)));

	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relid));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "merkle_range_entries return type must be a row type");

	heapRel = table_open(relid, AccessShareLock);
	merkleOid = merkle_index_of_table(heapRel);
	table_close(heapRel, AccessShareLock);
	if (!OidIsValid(merkleOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("table \"%s\" has no Merkle index", get_rel_name(relid))));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed in merkle_range_entries");
	entries = merkle_collect_node_entries(merkleOid, partition_id,
										  (const uint8 *) VARDATA_ANY(lo),
										  (const uint8 *) VARDATA_ANY(hi),
										  GetActiveSnapshot(), &num_entries, NULL);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	for (i = 0; i < num_entries; i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = PointerGetDatum(lookup_bytea8(entries[i].key_hash));
		values[1] = PointerGetDatum(lookup_bytea8(entries[i].tuple_hash.data));
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	MemoryContextSwitchTo(oldcontext);
	SPI_finish();

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	return (Datum) 0;
}
