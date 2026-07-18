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
#include "catalog/pg_type.h"
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "access/generic_xlog.h"
#include "storage/bufmgr.h"

/* GUC: Enable/disable Merkle index updates */
bool enable_merkle_index = true;
bool merkle_index_maintenance_suppress = false;
/* GUC: Emit NOTICE lines for touched Merkle nodes on commit */
bool merkle_update_detection = false;
/* GUC: Enable backend-local Merkle recovery profiling */
bool merkle_recovery_profile_enabled = false;
bool merkle_native_profile_enabled = false;
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
	bool all_native_synchronous = true;
	bool found_merkle = false;

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
	{
		List *indexes;
		ListCell *cell;

		if (rel->rd_rel->relkind == RELKIND_INDEX)
		{
			found_merkle = rel->rd_rel->relam == MERKLE_AM_OID;
			all_native_synchronous = found_merkle &&
				merkle_index_is_dynamic(rel) &&
				merkle_get_update_mode(rel) == MERKLE_UPDATE_SYNCHRONOUS_COW &&
				merkle_native_is_ready(rel);
			indexes = NIL;
		}
		else
		{
			indexes = RelationGetIndexList(rel);
		}

		foreach(cell, indexes)
		{
			Relation indexRel = index_open(lfirst_oid(cell), AccessShareLock);

			if (indexRel->rd_rel->relam == MERKLE_AM_OID)
			{
				found_merkle = true;
				if (!(merkle_index_is_dynamic(indexRel) &&
					  merkle_get_update_mode(indexRel) == MERKLE_UPDATE_SYNCHRONOUS_COW &&
					  merkle_native_is_ready(indexRel)))
					all_native_synchronous = false;
			}
			index_close(indexRel, AccessShareLock);
		}
		list_free(indexes);
	}
	if (found_merkle && all_native_synchronous)
		return;
	merkle_get_recovery_status(&status);
	if (status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot %s while committed Merkle deltas are pending",
						command),
				 errdetail("applied_seq=%llu target_seq=%llu",
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq),
				 errhint("Run SELECT merkle_apply_pending() before changing or dropping the relation.")));
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

static relopt_enum_elt_def merkleUpdateModeValues[] =
{
	{"synchronous_cow", MERKLE_UPDATE_SYNCHRONOUS_COW},
	{"pending_log", MERKLE_UPDATE_PENDING_LOG},
	{(const char *) NULL}
};

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
    
    add_int_reloption(merkle_relopt_kind, "partitions",
                      "Number of partitions in the merkle index",
                      MERKLE_NUM_PARTITIONS, 1, 10000, AccessExclusiveLock);
    
    add_int_reloption(merkle_relopt_kind, "leaves_per_partition",
                      "Number of leaves per partition (must be power of fanout)",
                      MERKLE_LEAVES_PER_PARTITION, 2, 1024, AccessExclusiveLock);

    add_int_reloption(merkle_relopt_kind, "fanout",
                      "Branching factor (children per internal node)",
                      MERKLE_DEFAULT_FANOUT, 2, 1024, AccessExclusiveLock);

	add_bool_reloption(merkle_relopt_kind, "dynamic",
					   "Use the bounded dynamic Merkle layout",
					   false, AccessExclusiveLock);
	add_int_reloption(merkle_relopt_kind, "leaf_capacity",
					  "Maximum item count in a dynamic Merkle leaf",
					  MERKLE_DYNAMIC_DEFAULT_LEAF_CAPACITY,
					  1, 1024, AccessExclusiveLock);
	add_int_reloption(merkle_relopt_kind, "merge_threshold",
					  "Maximum subtree item count eligible for a dynamic merge",
					  MERKLE_DYNAMIC_DEFAULT_MERGE_THRESHOLD,
					  0, 1023, AccessExclusiveLock);
	add_int_reloption(merkle_relopt_kind, "leaf_byte_capacity",
					  "Maximum canonical summary bytes in a dynamic Merkle leaf",
					  MERKLE_DYNAMIC_DEFAULT_LEAF_BYTE_CAPACITY,
					  1024, 16 * 1024 * 1024, AccessExclusiveLock);
	add_int_reloption(merkle_relopt_kind, "max_key_bytes",
					  "Maximum canonical dynamic Merkle key size",
					  MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES,
					  64, MERKLE_DYNAMIC_MAX_KEY_BYTES, AccessExclusiveLock);
	add_enum_reloption(merkle_relopt_kind, "update_mode",
					   "Chooses exact native COW or lagging pending-log Merkle updates",
					   merkleUpdateModeValues,
					   MERKLE_UPDATE_SYNCHRONOUS_COW,
					   "Valid values are \"synchronous_cow\" and \"pending_log\".",
					   AccessExclusiveLock);
    
    merkle_relopts_registered = true;
}

/* Reloption parsing table */
static relopt_parse_elt merkle_relopt_tab[] = {
    {"partitions", RELOPT_TYPE_INT, offsetof(MerkleOptions, partitions)},
    {"leaves_per_partition", RELOPT_TYPE_INT, offsetof(MerkleOptions, leaves_per_partition)},
	{"fanout", RELOPT_TYPE_INT, offsetof(MerkleOptions, fanout)},
	{"dynamic", RELOPT_TYPE_BOOL, offsetof(MerkleOptions, dynamic)},
	{"leaf_capacity", RELOPT_TYPE_INT, offsetof(MerkleOptions, leaf_capacity)},
	{"merge_threshold", RELOPT_TYPE_INT, offsetof(MerkleOptions, merge_threshold)},
	{"leaf_byte_capacity", RELOPT_TYPE_INT, offsetof(MerkleOptions, leaf_byte_capacity)},
	{"max_key_bytes", RELOPT_TYPE_INT, offsetof(MerkleOptions, max_key_bytes)},
	{"update_mode", RELOPT_TYPE_ENUM, offsetof(MerkleOptions, update_mode)}
};

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
	{
		bool update_mode_specified = false;
		if (PointerIsValid(DatumGetPointer(reloptions)))
		{
			ArrayType *array = DatumGetArrayTypeP(reloptions);
			Datum *optiondatums;
			int noptions;
			int i;

			deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, NULL, &noptions);
			for (i = 0; i < noptions; i++)
			{
				char *text_str = VARDATA(optiondatums[i]);
				int text_len = VARSIZE(optiondatums[i]) - VARHDRSZ;

				if (text_len > 12 && strncmp(text_str, "update_mode=", 12) == 0)
				{
					update_mode_specified = true;
					break;
				}
			}
			pfree(optiondatums);
		}

		/* The reloption, not the session GUC, is the durable authority.  A
		 * missing option always means the production-safe native default;
		 * this prevents a later REINDEX from changing mode because a session
		 * or postmaster GUC changed.  Compatibility mode must be explicit. */
		if (!update_mode_specified)
			opts->update_mode = MERKLE_UPDATE_SYNCHRONOUS_COW;
	}

    if (validate && opts != NULL)
	{
        if (opts->fanout < 2 || opts->fanout > 1024)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("fanout must be between 2 and 1024")));
        }

        /* Check if leaves_per_partition is a power of fanout */
        if (!merkle_is_power_of(opts->leaves_per_partition, opts->fanout))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("leaves_per_partition must be a power of fanout"),
                     errhint("For fanout=%d, suggested values: %d, %d, %d, ...",
                             opts->fanout,
                             opts->fanout,
                             opts->fanout * opts->fanout,
                             opts->fanout * opts->fanout * opts->fanout)));
        }

		if (opts->dynamic && opts->fanout != MERKLE_DYNAMIC_LOGICAL_FANOUT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle indexes require fanout=32")));
		if (opts->dynamic && opts->merge_threshold >= opts->leaf_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle merge_threshold must be less than leaf_capacity")));
		if (opts->dynamic && opts->max_key_bytes > opts->leaf_byte_capacity)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dynamic Merkle max_key_bytes cannot exceed leaf_byte_capacity")));
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
        opts->partitions = MERKLE_NUM_PARTITIONS;
        opts->leaves_per_partition = MERKLE_LEAVES_PER_PARTITION;
        opts->fanout = MERKLE_DEFAULT_FANOUT;
		opts->dynamic = false;
		opts->leaf_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_CAPACITY;
		opts->merge_threshold = MERKLE_DYNAMIC_DEFAULT_MERGE_THRESHOLD;
		opts->leaf_byte_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_BYTE_CAPACITY;
		opts->max_key_bytes = MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES;
		opts->update_mode = MERKLE_UPDATE_SYNCHRONOUS_COW;
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
	{
		opts->fanout = MERKLE_DEFAULT_FANOUT;
	}
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, dynamic) + sizeof(bool)))
		opts->dynamic = false;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, leaf_capacity) + sizeof(int)))
		opts->leaf_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_CAPACITY;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, merge_threshold) + sizeof(int)))
		opts->merge_threshold = MERKLE_DYNAMIC_DEFAULT_MERGE_THRESHOLD;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, leaf_byte_capacity) + sizeof(int)))
		opts->leaf_byte_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_BYTE_CAPACITY;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, max_key_bytes) + sizeof(int)))
		opts->max_key_bytes = MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES;
	if (VARSIZE(relopts) < (offsetof(MerkleOptions, update_mode) + sizeof(int)))
		opts->update_mode = MERKLE_UPDATE_SYNCHRONOUS_COW;
    
	/* Legacy static blobs may fall back; dynamic corruption must fail closed. */
    if (opts->partitions <= 0 || opts->partitions > 10000 ||
        opts->leaves_per_partition <= 0 || opts->leaves_per_partition > 1024 ||
        opts->fanout < 2 || opts->fanout > 1024 ||
		!merkle_is_power_of(opts->leaves_per_partition, opts->fanout) ||
		opts->leaf_capacity < 1 || opts->leaf_capacity > 1024 ||
		opts->merge_threshold < 0 ||
		opts->merge_threshold >= opts->leaf_capacity ||
		opts->leaf_byte_capacity < 1024 ||
		opts->leaf_byte_capacity > 16 * 1024 * 1024 ||
		opts->max_key_bytes < 64 ||
		opts->max_key_bytes > MERKLE_DYNAMIC_MAX_KEY_BYTES ||
		(opts->dynamic && opts->max_key_bytes > opts->leaf_byte_capacity) ||
		(opts->dynamic && opts->fanout != MERKLE_DYNAMIC_LOGICAL_FANOUT) ||
		(opts->update_mode != MERKLE_UPDATE_SYNCHRONOUS_COW &&
		 opts->update_mode != MERKLE_UPDATE_PENDING_LOG))
    {
		if (opts->dynamic)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("dynamic Merkle index has invalid reloptions"),
					 errhint("REINDEX the dynamic Merkle index after correcting its options.")));
        opts->partitions = MERKLE_NUM_PARTITIONS;
        opts->leaves_per_partition = MERKLE_LEAVES_PER_PARTITION;
        opts->fanout = MERKLE_DEFAULT_FANOUT;
		opts->dynamic = false;
		opts->leaf_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_CAPACITY;
		opts->merge_threshold = MERKLE_DYNAMIC_DEFAULT_MERGE_THRESHOLD;
		opts->leaf_byte_capacity = MERKLE_DYNAMIC_DEFAULT_LEAF_BYTE_CAPACITY;
		opts->max_key_bytes = MERKLE_DYNAMIC_DEFAULT_MAX_KEY_BYTES;
		opts->update_mode = MERKLE_UPDATE_SYNCHRONOUS_COW;
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
