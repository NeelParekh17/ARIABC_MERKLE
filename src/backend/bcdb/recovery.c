/*-------------------------------------------------------------------------
 *
 * recovery.c
 *	  Online replica recovery support for deterministic BCDB execution.
 *
 * bcdb_cut_snapshot_export(boundary, timeout_ms)
 *	  Export an MVCC snapshot whose visible state is exactly the
 *	  deterministic prefix 0..boundary, without pausing execution.
 *
 *	  Deterministic transactions may commit out of order (tx 105 before
 *	  tx 104), so an ordinary snapshot taken at any instant is generally not a
 *	  prefix.  Instead the caller opens a REPEATABLE READ "keeper" transaction
 *	  before any transaction beyond the boundary has been dispatched (the
 *	  replicated server does this from its Raft commit thread).  The keeper's
 *	  xmin keeps every row version those later transactions overwrite from
 *	  being pruned.  This function then waits (inside the keeper backend only)
 *	  until the committed watermark reaches the boundary, takes a fresh
 *	  snapshot, and additionally marks as in-progress the xids of every
 *	  transaction beyond the boundary that already committed.  Those xids were
 *	  recorded before commit by bcdb_note_precommit_xid().  The snapshot's
 *	  xmin is lowered to the keeper's xmin, which (a) makes those xids
 *	  eligible for the in-progress check and (b) satisfies ImportSnapshot's
 *	  requirement that the source transaction's xmin not follow ours.
 *	  suboverflowed forces pg_subtrans lookups so rows written in BCDB
 *	  subtransactions map to their hidden top-level xid.
 *
 * bcdb_recovery_rebase(boundary)
 *	  After a damaged replica's tables were restored to the state of the
 *	  prefix 0..boundary, reset BCDB's in-memory deterministic state so that
 *	  transaction boundary+1 is the next to execute.  Caller guarantees the
 *	  replica's executor is drained.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "bcdb/shm_block.h"
#include "bcdb/shm_transaction.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(bcdb_cut_snapshot_export);
PG_FUNCTION_INFO_V1(bcdb_recovery_rebase);

static int
xid_cmp(const void *a, const void *b)
{
	TransactionId xa = *(const TransactionId *) a;
	TransactionId xb = *(const TransactionId *) b;

	if (xa < xb)
		return -1;
	if (xa > xb)
		return 1;
	return 0;
}

Datum
bcdb_cut_snapshot_export(PG_FUNCTION_ARGS)
{
	int32		boundary = PG_GETARG_INT32(0);
	int32		timeout_ms = PG_GETARG_INT32(1);
	TransactionId keeper_xmin;
	TimestampTz deadline;
	SnapshotData fresh;
	SnapshotData cut;
	BCBlock    *blk;
	TransactionId *hidden;
	int			nhidden = 0;
	int			max_xcnt;
	int			merged;
	char	   *snapshot_id;

	if (!IsTransactionBlock() || !IsolationUsesXactSnapshot())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("bcdb_cut_snapshot_export must run inside a REPEATABLE READ transaction block")));
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("bcdb_cut_snapshot_export cannot run in a subtransaction")));

	/* The keeper's transaction snapshot was taken by an earlier statement. */
	keeper_xmin = MyPgXact->xmin;
	if (!TransactionIdIsNormal(keeper_xmin))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bcdb_cut_snapshot_export: keeper transaction has no snapshot xmin")));

	/* Wait (in this backend only) until every tx <= boundary has committed. */
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										   timeout_ms > 0 ? timeout_ms : 30000);
	while (get_last_committed_txid(NULL) < boundary)
	{
		CHECK_FOR_INTERRUPTS();
		if (GetCurrentTimestamp() >= deadline)
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("bcdb_cut_snapshot_export: timed out waiting for committed watermark %d (current %d)",
							boundary, (int) get_last_committed_txid(NULL))));
		pg_usleep(100L);
	}

	MemSet(&fresh, 0, sizeof(fresh));
	fresh.xip = (TransactionId *) palloc(GetMaxSnapshotXidCount() * sizeof(TransactionId));
	fresh.subxip = (TransactionId *) palloc(GetMaxSnapshotSubxidCount() * sizeof(TransactionId));
	GetSnapshotData(&fresh);

	/* Collect writers beyond the boundary that already recorded a commit xid. */
	blk = bcdb_get_block1();
	hidden = (TransactionId *) palloc(BCDB_RESULT_RING_CAPACITY * sizeof(TransactionId));
	if (blk != NULL)
	{
		for (int i = 1; i <= BCDB_RESULT_RING_CAPACITY; i++)
		{
			BCTxID		id = boundary + i;
			int			slot = (int) (id % (BCTxID) BCDB_RESULT_RING_CAPACITY);
			BCTxID		tag;
			TransactionId xid;

			if (slot < 0)
				slot += BCDB_RESULT_RING_CAPACITY;
			tag = (BCTxID) __atomic_load_n(&blk->precommit_txid[slot], __ATOMIC_ACQUIRE);
			if (tag != id)
				continue;
			xid = __atomic_load_n(&blk->precommit_xid[slot], __ATOMIC_RELAXED);
			if (!TransactionIdIsNormal(xid))
				continue;
			if (TransactionIdPrecedes(xid, keeper_xmin))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("bcdb_cut_snapshot_export: tx %d (xid %u) beyond boundary %d precedes keeper xmin %u; keeper opened too late",
								(int) id, xid, boundary, keeper_xmin)));
			if (!TransactionIdPrecedes(xid, fresh.xmax))
				continue;		/* not visible to the fresh snapshot anyway */
			hidden[nhidden++] = xid;
		}
	}

	/*
	 * Every writer beyond the boundary must still own its slot.  A slot is
	 * only reused by a transaction >= boundary + capacity, which raises
	 * precommit_max_txid before storing its tag; reading the high-water mark
	 * after the scan therefore detects any overwrite that could have hidden
	 * an xid from us.
	 */
	if (blk != NULL)
	{
		BCTxID		hwm = (BCTxID) __atomic_load_n(&blk->precommit_max_txid, __ATOMIC_SEQ_CST);

		if (hwm >= (BCTxID) boundary + (BCTxID) BCDB_RESULT_RING_CAPACITY)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("bcdb_cut_snapshot_export: execution ran %d transactions past boundary %d before the cut (window %d); retry with a newer boundary",
							(int) (hwm - boundary), boundary, BCDB_RESULT_RING_CAPACITY)));
	}

	/* Merge fresh in-progress xids with the hidden writers, deduplicated. */
	max_xcnt = GetMaxSnapshotXidCount();
	if (fresh.xcnt + nhidden >= max_xcnt)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("bcdb_cut_snapshot_export: %d in-progress + %d hidden xids exceed snapshot capacity %d",
						fresh.xcnt, nhidden, max_xcnt)));

	MemSet(&cut, 0, sizeof(cut));
	cut.snapshot_type = SNAPSHOT_MVCC;
	cut.xmin = keeper_xmin;
	cut.xmax = fresh.xmax;
	cut.xip = (TransactionId *) palloc((fresh.xcnt + nhidden + 1) * sizeof(TransactionId));
	memcpy(cut.xip, fresh.xip, fresh.xcnt * sizeof(TransactionId));
	memcpy(cut.xip + fresh.xcnt, hidden, nhidden * sizeof(TransactionId));
	merged = fresh.xcnt + nhidden;
	if (merged > 1)
	{
		int			w = 1;

		qsort(cut.xip, merged, sizeof(TransactionId), xid_cmp);
		for (int r = 1; r < merged; r++)
			if (cut.xip[r] != cut.xip[w - 1])
				cut.xip[w++] = cut.xip[r];
		merged = w;
	}
	cut.xcnt = merged;
	cut.subxip = NULL;
	cut.subxcnt = 0;
	cut.suboverflowed = true;
	cut.takenDuringRecovery = fresh.takenDuringRecovery;
	cut.copied = false;
	cut.curcid = GetCurrentCommandId(false);
	cut.speculativeToken = 0;
	cut.active_count = 0;
	cut.regd_count = 0;
	cut.whenTaken = fresh.whenTaken;
	cut.lsn = fresh.lsn;

	snapshot_id = ExportSnapshot(&cut);

	elog(LOG,
		 "BCDB_RECOVERY_CUT boundary=%d snapshot=%s keeper_xmin=%u xmax=%u in_progress=%d hidden=%d",
		 boundary, snapshot_id, keeper_xmin, cut.xmax, fresh.xcnt, nhidden);

	PG_RETURN_TEXT_P(cstring_to_text(snapshot_id));
}

Datum
bcdb_recovery_rebase(PG_FUNCTION_ARGS)
{
	int32		boundary = PG_GETARG_INT32(0);

	if (boundary < -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("bcdb_recovery_rebase: invalid boundary %d", boundary)));

	if (!bcdb_tx_pool_is_empty())
		clear_tx_pool();
	bcdb_ws_tables_clear_all();
	bcdb_rebase_block1((BCTxID) boundary);

	elog(LOG, "BCDB_RECOVERY_REBASE boundary=%d", boundary);
	PG_RETURN_BOOL(true);
}
