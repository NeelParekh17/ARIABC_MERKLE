/*-------------------------------------------------------------------------
 *
 * merkle.c
 *    Merkle tree integrity index access method - main handler
 *
 * This file implements the IndexAmRoutine handler function that returns
 * the callback function pointers for the merkle access method.
 *
 * IDENTIFICATION
 *    src/backend/access/merkle/merkle.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/merkle.h"
#include "access/reloptions.h"
#include "catalog/pg_am_d.h"
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"

/* GUC: Enable/disable Merkle index updates */
bool enable_merkle_index = true;
bool merkle_apply_synchronous_direct = true;
bool merkle_index_maintenance_suppress = false;
/* GUC: Emit NOTICE lines for touched Merkle nodes on commit */
bool merkle_update_detection = false;
/* GUC: Enable backend-local Merkle recovery profiling */
bool merkle_recovery_profile_enabled = false;
/* GUC: fail stale Merkle reads by default; optionally catch up synchronously. */
int merkle_read_lag_policy = MERKLE_READ_LAG_ERROR;
int merkle_apply_batch_items = MERKLE_APPLY_DEFAULT_BATCH_ITEMS;
int merkle_apply_batch_bytes = MERKLE_APPLY_DEFAULT_BATCH_BYTES;
int merkle_apply_batch_pages = MERKLE_APPLY_DEFAULT_BATCH_PAGES;
int merkle_apply_batch_time_ms = MERKLE_APPLY_DEFAULT_BATCH_TIME_MS;
/*
 * GUC: Suppress Merkle update-detection output during Merkle index builds
 * (CREATE INDEX / REINDEX).
 *
 * When enabled, Merkle index builds will not emit the touched-node report even
 * if merkle_update_detection is on. Default is enabled to avoid noisy output.
 */
bool merkle_update_detection_suppress = true;
uint64 merkle_recovery_profile_reset_generation = 0;
MerkleRecoveryProfileStats merkle_recovery_profile_state = {0};

/*
 * Merkle index reloption definitions using standard framework
 */
static relopt_kind merkle_relopt_kind;
static bool merkle_relopts_registered = false;

static bool
merkle_is_power_of(int value, int base)
{
    if (value < 1 || base < 2)
        return false;

    while ((value % base) == 0)
        value /= base;

    return (value == 1);
}

bool
merkle_relation_has_index(Relation rel)
{
	List *index_list;
	ListCell *lc;
	bool found = false;

	if (rel == NULL)
		return false;
	if (rel->rd_rel->relkind == RELKIND_INDEX ||
		rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		return rel->rd_rel->relam == MERKLE_AM_OID;
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return false;

	index_list = RelationGetIndexList(rel);
	foreach(lc, index_list)
	{
		Relation index_rel = index_open(lfirst_oid(lc), AccessShareLock);

		if (index_rel->rd_rel->relam == MERKLE_AM_OID)
			found = true;
		index_close(index_rel, AccessShareLock);
		if (found)
			break;
	}
	list_free(index_list);
	return found;
}

void
merkle_reject_ddl(Relation rel, const char *command)
{
	MerkleRecoveryStatusData status;

	if (!merkle_relation_has_index(rel))
		return;
	/* Row hashes include the complete heap row and routing metadata.  Until a
	 * rewrite-aware Merkle rebuild protocol exists, any ALTER TABLE that can
	 * change the row descriptor or relfilenode is fail-closed even when the
	 * committed delta prefix is currently caught up. */
	if (command != NULL && strncmp(command, "alter ", 6) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot %s while a table has a Merkle index", command),
				 errhint("Drop or rebuild the Merkle index before altering the table.")));
	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot %s while this transaction has staged Merkle deltas",
						command),
				 errhint("Commit or roll back the table changes before DDL.")));
	merkle_get_recovery_status(&status);
	if (status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot %s while committed Merkle deltas are pending",
						command),
				 errdetail("applied_seq=%llu target_seq=%llu",
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq),
					 errhint("Wait for synchronous Merkle maintenance to reach READY before changing or dropping the relation.")));
}

/*
 * merkle_reject_concurrent_ddl() - P0.4: unconditionally reject concurrent
 * DDL operations that the queued-delta format cannot safely support.
 *
 * REINDEX CONCURRENTLY, CREATE INDEX CONCURRENTLY, and DROP INDEX CONCURRENTLY
 * change the relfilenode while DML may continue.  The Merkle delta format
 * cannot handle this safely.  Do NOT route through merkle_reject_ddl() because
 * that function is conditional on recovery state; these commands must always
 * be rejected regardless of current recovery readiness.
 */
void
merkle_reject_concurrent_ddl(Oid index_oid, const char *command)
{
	Relation	irel;
	bool		is_merkle;

	if (!OidIsValid(index_oid))
		return;
	irel = index_open(index_oid, AccessShareLock);
	is_merkle = (irel->rd_rel->relam == MERKLE_AM_OID);
	index_close(irel, AccessShareLock);
	if (is_merkle)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s is not supported for Merkle indexes", command),
				 errhint("Use non-concurrent REINDEX instead.")));
}

/*
 * merkle_register_relopts() - Register merkle reloptions with PostgreSQL
 *
 * This should be called once to register our options.
 */
static void
merkle_register_relopts(void)
{
	if (merkle_relopts_registered) /* already registered */
		return;

	merkle_relopt_kind = add_reloption_kind();

	add_int_reloption(merkle_relopt_kind, "fanout",
					  "Branching factor (children per internal node)",
					  MERKLE_DEFAULT_FANOUT, 2, 1024, AccessExclusiveLock);

	add_int_reloption(merkle_relopt_kind, "split_threshold",
					  "Node size to trigger a split",
					  0, 2, 100000, AccessExclusiveLock);

	add_int_reloption(merkle_relopt_kind, "merge_threshold",
					  "Node size to trigger a merge",
					  0, 1, 100000, AccessExclusiveLock);

	add_int_reloption(merkle_relopt_kind, "partitions",
					  "Number of independent hash-routed Merkle partitions",
					  MERKLE_DEFAULT_PARTITIONS, 1, MERKLE_MAX_PARTITIONS, AccessExclusiveLock);

	add_int_reloption(merkle_relopt_kind, "partition_key_columns",
					  "Leading index key columns that select a partition group (0 = full-key hash routing)",
					  0, 0, INDEX_MAX_KEYS, AccessExclusiveLock);

	add_int_reloption(merkle_relopt_kind, "subpartitions",
					  "Partitions per leading-key group when partition_key_columns > 0",
					  1, 1, MERKLE_MAX_PARTITIONS, AccessExclusiveLock);

	merkle_relopts_registered = true;
}

/* Reloption parsing table */
static relopt_parse_elt merkle_relopt_tab[] = {
	{"fanout", RELOPT_TYPE_INT, offsetof(MerkleOptions, fanout)},
	{"split_threshold", RELOPT_TYPE_INT, offsetof(MerkleOptions, split_threshold)},
	{"merge_threshold", RELOPT_TYPE_INT, offsetof(MerkleOptions, merge_threshold)},
	{"partitions", RELOPT_TYPE_INT, offsetof(MerkleOptions, num_partitions)},
	{"partition_key_columns", RELOPT_TYPE_INT, offsetof(MerkleOptions, partition_key_columns)},
	{"subpartitions", RELOPT_TYPE_INT, offsetof(MerkleOptions, subpartitions)}
};

/*
 * PG reloptions accept defaults outside the explicit input range.  Zero
 * therefore marks an omitted threshold; an explicit zero is still rejected
 * by the reloptions parser.  Resolve before validation or metapage creation.
 */
static void
merkle_resolve_thresholds(MerkleOptions *opts)
{
	if (opts->split_threshold == 0)
		opts->split_threshold = (opts->fanout == 32) ?
			MERKLE_FANOUT32_SPLIT_THRESHOLD : SPLIT_THRESHOLD;
	if (opts->merge_threshold == 0)
		opts->merge_threshold = Max(1, opts->split_threshold / 4);
}

/*
 * merkle_options() - Parse reloptions for merkle index
 *
 * This is called during CREATE INDEX to parse WITH clause options.
 */
bytea *
merkle_options(Datum reloptions, bool validate)
{
	MerkleOptions *opts;

	/* Ensure our reloptions are registered */
	merkle_register_relopts();

	opts = (MerkleOptions *) build_reloptions(reloptions, validate,
											   merkle_relopt_kind,
											   sizeof(MerkleOptions),
											   merkle_relopt_tab,
											   lengthof(merkle_relopt_tab));

	if (opts != NULL)
		merkle_resolve_thresholds(opts);

	if (validate && opts != NULL)
	{
		if (opts->fanout < 2 || opts->fanout > 1024 ||
			opts->num_partitions < 1 || opts->num_partitions > MERKLE_MAX_PARTITIONS)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("fanout must be between 2 and 1024 and partitions must be between 1 and %d",
							MERKLE_MAX_PARTITIONS)));
		}
		if (opts->split_threshold < 2 || opts->split_threshold > 100000)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("split_threshold (%d) must be between 2 and 100000",
							opts->split_threshold)));
		}
		if (opts->merge_threshold < 1 || opts->merge_threshold > 100000)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("merge_threshold (%d) must be between 1 and 100000",
							opts->merge_threshold)));
		}
		if (opts->merge_threshold >= opts->split_threshold)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("merge_threshold (%d) must be strictly less than split_threshold (%d)",
							opts->merge_threshold, opts->split_threshold)));
		}
		if (opts->partition_key_columns > 0 &&
			opts->num_partitions % opts->subpartitions != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("partitions (%d) must be a multiple of subpartitions (%d)",
							opts->num_partitions, opts->subpartitions)));
		}
	}

	return (bytea *) opts;
}

/*
 * merkle_get_options() - Extract options from index relation
 *
 * Returns MerkleOptions with user settings or defaults if not set.
 */
MerkleOptions *
merkle_get_options(Relation indexRel)
{
	MerkleOptions *opts;
	bytea *relopts;

	relopts = indexRel->rd_options;
	if (relopts == NULL)
	{
		/* No options specified, return defaults */
		opts = (MerkleOptions *) palloc0(sizeof(MerkleOptions));
		SET_VARSIZE(opts, sizeof(MerkleOptions));
		opts->fanout = MERKLE_DEFAULT_FANOUT;
		merkle_resolve_thresholds(opts);
		opts->num_partitions = MERKLE_DEFAULT_PARTITIONS;
		opts->partition_key_columns = 0;
		opts->subpartitions = 1;
		return opts;
	}

	/*
	 * Options were stored - copy and validate.
	 * The options are stored with local_reloptions format which includes
	 * a varlena header followed by the option values at their defined offsets.
	 */
	opts = (MerkleOptions *) palloc0(sizeof(MerkleOptions));
	memcpy(opts, relopts, Min(VARSIZE(relopts), sizeof(MerkleOptions)));
	SET_VARSIZE(opts, sizeof(MerkleOptions));

	/* Backward compatibility: older rd_options blobs won't have fanout */
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, fanout) + sizeof(int)))
		opts->fanout = MERKLE_DEFAULT_FANOUT;

	/* Backward compatibility: resolve any missing thresholds independently. */
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, split_threshold) + sizeof(int)))
		opts->split_threshold = 0;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, merge_threshold) + sizeof(int)))
		opts->merge_threshold = 0;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, num_partitions) + sizeof(int)))
		opts->num_partitions = MERKLE_DEFAULT_PARTITIONS;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, subpartitions) + sizeof(int)))
	{
		opts->partition_key_columns = 0;
		opts->subpartitions = 1;
	}

	/* Validate options - if values look corrupt, use defaults independently */
	if (opts->fanout < 2 || opts->fanout > 1024)
		opts->fanout = MERKLE_DEFAULT_FANOUT;

	if (opts->num_partitions < 1 || opts->num_partitions > MERKLE_MAX_PARTITIONS)
		opts->num_partitions = MERKLE_DEFAULT_PARTITIONS;

	merkle_resolve_thresholds(opts);

	if (opts->split_threshold < 2 || opts->split_threshold > 100000 ||
		opts->merge_threshold < 1 || opts->merge_threshold > 100000 ||
		opts->merge_threshold >= opts->split_threshold)
	{
		opts->split_threshold = 0;
		opts->merge_threshold = 0;
		merkle_resolve_thresholds(opts);
	}
	if (opts->partition_key_columns < 0 || opts->partition_key_columns > INDEX_MAX_KEYS ||
		opts->subpartitions < 1 ||
		(opts->partition_key_columns > 0 && opts->num_partitions % opts->subpartitions != 0))
	{
		opts->partition_key_columns = 0;
		opts->subpartitions = 1;
	}

	return opts;
}

PG_FUNCTION_INFO_V1(merklehandler);

/*
 * merklehandler() - Return IndexAmRoutine for merkle access method
 *
 * This is the entry point that PostgreSQL calls when loading the access method.
 * We return a structure containing pointers to all the callback functions
 * that implement the Merkle index operations.
 */
Datum
merklehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
    
    /* Ensure reloptions are registered when AM is loaded */
    merkle_register_relopts();

    /*
     * Index properties
     * 
     * The Merkle index is NOT a traditional search index - it's for
     * integrity verification. So most search-related properties are false.
     */
    amroutine->amstrategies = 0;            /* no operator strategies */
    amroutine->amsupport = 0;               /* no support functions (partition logic is inline) */
    amroutine->amcanorder = false;          /* cannot order results */
    amroutine->amcanorderbyop = false;      /* no ordering operators */
    amroutine->amcanbackward = false;       /* no backward scans */
    amroutine->amcanunique = false;         /* not for uniqueness */
    amroutine->amcanmulticol = true;        /* multi-column keys supported */
    amroutine->amoptionalkey = true;        /* key is optional for scan */
    amroutine->amsearcharray = false;       /* no array searches */
    amroutine->amsearchnulls = false;       /* no null searches */
    amroutine->amstorage = false;           /* no special storage */
    amroutine->amclusterable = false;       /* cannot cluster on */
    amroutine->ampredlocks = false;         /* no predicate locks */
    amroutine->amcanparallel = false;       /* no parallel scans */
    amroutine->amcaninclude = false;        /* no included columns */
    amroutine->amkeytype = InvalidOid;      /* no specific key type */

    /*
     * Callback functions
     */
    /* Build functions */
    amroutine->ambuild = merkleBuild;
    amroutine->ambuildempty = merkleBuildempty;
    
    /* Insert/delete functions */
    amroutine->aminsert = merkleInsert;
    amroutine->ambulkdelete = merkleBulkdelete;
    amroutine->amvacuumcleanup = merkleVacuumcleanup;
    
    /* Scan functions - NOT SUPPORTED for Merkle index */
    /* 
     * The Merkle index does not support traditional index scans.
     * Verification is done through explicit SQL functions (merkle_verify, etc.)
     * which read the index pages directly via ReadBuffer().
     */
    amroutine->amcanreturn = NULL;          /* no index-only scans */
    amroutine->amcostestimate = merkleCostEstimate;
    amroutine->amoptions = merkle_options;  /* parse partitions, leaves_per_partition */
    amroutine->amproperty = NULL;           /* no special properties */
    amroutine->ambuildphasename = NULL;     /* no build phases */
    amroutine->amvalidate = NULL;           /* no opclass validation needed */
    amroutine->ambeginscan = NULL;          /* no scan support */
    amroutine->amrescan = NULL;             /* no scan support */
    amroutine->amgettuple = NULL;           /* no scan support */
    amroutine->amgetbitmap = NULL;          /* no bitmap scans */
    amroutine->amendscan = NULL;            /* no scan support */
    amroutine->ammarkpos = NULL;            /* no mark/restore */
    amroutine->amrestrpos = NULL;
    
    /* Parallel scan functions */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}

/*
 * merkleCostEstimate() - Estimate cost of scanning merkle index
 *
 * Since the merkle index is not used for searching but for verification,
 * we return minimal costs. The optimizer should never choose this index
 * for actual query processing.
 */
void
merkleCostEstimate(struct PlannerInfo *root,
                   struct IndexPath *path,
                   double loop_count,
                   Cost *indexStartupCost,
                   Cost *indexTotalCost,
                   Selectivity *indexSelectivity,
                   double *indexCorrelation,
                   double *indexPages)
{
    /*
     * Return very high costs so the optimizer never chooses this
     * for normal query processing. The merkle index is only for
     * integrity verification through explicit function calls.
     */
    *indexStartupCost = 1.0e10;
    *indexTotalCost = 1.0e10;
    *indexSelectivity = 0.0;
    *indexCorrelation = 0.0;
    *indexPages = 1;
}
