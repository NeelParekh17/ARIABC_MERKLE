/*-------------------------------------------------------------------------
 *
 * merkle.c
 *    Merkle tree integrity index access method - main handler
 *
 * This file implements the IndexAmRoutine handler function that returns
 * the callback function pointers for the merkle access method.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
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
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"

/* GUC: Enable/disable Merkle index updates */
bool enable_merkle_index = true;

/*
 * Merkle index reloption definitions using standard framework
 */
static relopt_kind merkle_relopt_kind;
static bool merkle_relopts_registered = false;

/*
 * merkle_register_relopts() - Register merkle reloptions with PostgreSQL
 *
 * This should be called once to register our options.
 */
static void
merkle_register_relopts(void)
{
    if (merkle_relopts_registered) //if merkle options are passed then return
        return;
    
    merkle_relopt_kind = add_reloption_kind();
    
    add_int_reloption(merkle_relopt_kind, "subtrees",
                      "Number of subtrees in the merkle index",
                      MERKLE_NUM_SUBTREES, 1, 10000, AccessExclusiveLock);
    
    add_int_reloption(merkle_relopt_kind, "leaves_per_tree",
                      "Number of leaves per subtree (must be power of 2)",
                      MERKLE_LEAVES_PER_TREE, 2, 1024, AccessExclusiveLock);
    
    merkle_relopts_registered = true;
}

/* Reloption parsing table */
static relopt_parse_elt merkle_relopt_tab[] = {
    {"subtrees", RELOPT_TYPE_INT, offsetof(MerkleOptions, subtrees)},
    {"leaves_per_tree", RELOPT_TYPE_INT, offsetof(MerkleOptions, leaves_per_tree)}
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
        opts->subtrees = MERKLE_NUM_SUBTREES;
        opts->leaves_per_tree = MERKLE_LEAVES_PER_TREE;
        return opts;
    }
    
    /*
     * Options were stored - copy and validate.
     * The options are stored with local_reloptions format which includes
     * a varlena header followed by the option values at their defined offsets.
     */
    opts = (MerkleOptions *) palloc(VARSIZE(relopts));
    memcpy(opts, relopts, VARSIZE(relopts));
    
    /* Validate options - if values look corrupt, use defaults */
    if (opts->subtrees <= 0 || opts->subtrees > 10000 ||
        opts->leaves_per_tree <= 0 || opts->leaves_per_tree > 1024)
    {
        ereport(NOTICE,
                (errmsg("merkle: invalid options detected (subtrees=%d, leaves=%d), using defaults",
                        opts->subtrees, opts->leaves_per_tree)));
        opts->subtrees = MERKLE_NUM_SUBTREES;
        opts->leaves_per_tree = MERKLE_LEAVES_PER_TREE;
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
    amroutine->amoptions = merkle_options;  /* parse subtrees, leaves_per_tree */
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
