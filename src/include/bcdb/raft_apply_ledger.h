/*-------------------------------------------------------------------------
 *
 * raft_apply_ledger.h
 *    Crash-safe Raft → BCDB → PostgreSQL apply ledger (Phase D).
 *
 *    Each BCDB worker calls bcdb_raft_ledger_claim() at the start of its
 *    top-level transaction (inside the PG_TRY block, after BeginCommand).
 *    If a terminal ledger row already exists the worker must call
 *    bcdb_complete_replayed_item() instead of running business SQL.
 *
 * IDENTIFICATION
 *    src/include/bcdb/raft_apply_ledger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BCDB_RAFT_APPLY_LEDGER_H
#define BCDB_RAFT_APPLY_LEDGER_H

#include "postgres.h"
#include "bcdb/shm_transaction.h"

/*
 * Ledger item state constants — must match the SQL schema CHECK constraint.
 */
#define RAFT_ITEM_STATE_CLAIMED       1
#define RAFT_ITEM_STATE_APPLIED_OK    2
#define RAFT_ITEM_STATE_APPLIED_ERROR 3

/*
 * Result of bcdb_raft_ledger_claim().
 */
typedef enum
{
	RAFT_CLAIM_OWNED,        /* new row; worker owns first execution */
	RAFT_CLAIM_REPLAY_OK,    /* pre-existing APPLIED_OK; skip SQL, replay result */
	RAFT_CLAIM_REPLAY_ERROR, /* pre-existing APPLIED_ERROR; skip SQL, replay error */
	RAFT_CLAIM_DISABLED,     /* raft_ledger_enabled = false; legacy mode */
} RaftClaimResult;

/*
 * bcdb_raft_ledger_claim
 *
 * Called inside the worker's top-level PostgreSQL transaction via SPI.
 * Inserts a CLAIMED row (ON CONFLICT DO NOTHING) and reads back the state.
 *
 * Returns one of the RAFT_CLAIM_* constants.  Caller must NOT free
 * out_result_payload or out_error_payload — they point into SPI memory
 * and are valid until the end of the current SPI call.
 *
 * On RAFT_CLAIM_OWNED: caller continues with normal BCDB execution.
 * On RAFT_CLAIM_REPLAY_*: caller must call bcdb_complete_replayed_item().
 * On RAFT_CLAIM_DISABLED: caller ignores ledger entirely.
 */
extern RaftClaimResult bcdb_raft_ledger_claim(
		BCDBShmXact  *tx,
		char        **out_result_payload,   /* set on REPLAY_OK   */
		int          *out_result_fmtver,
		char        **out_error_payload,    /* set on REPLAY_ERROR */
		int          *out_error_fmtver,
		char        **out_sqlstate          /* set on REPLAY_ERROR */
);

/*
 * bcdb_raft_ledger_finalize_ok
 *
 * Called inside the same top-level transaction just before finish_xact_command().
 * Updates the CLAIMED row to APPLIED_OK and writes the canonical result/digest.
 * Must be called only when the business SQL succeeded.
 */
extern void bcdb_raft_ledger_finalize_ok(
		BCDBShmXact  *tx,
		const char   *result_payload,
		int           result_fmtver
);

/*
 * bcdb_raft_ledger_finalize_error
 *
 * Called inside the surviving parent transaction after unwinding BCDB
 * subtransactions on a deterministic error.
 * Updates the CLAIMED row to APPLIED_ERROR.
 */
extern void bcdb_raft_ledger_finalize_error(
		BCDBShmXact  *tx,
		const char   *sqlstate,
		const char   *error_payload,
		int           error_fmtver
);

/*
 * Called in the owned apply path immediately before finish_xact_command().
 * A safe-ledger transaction may commit only with APPLIED_OK or APPLIED_ERROR.
 */
extern void bcdb_raft_ledger_assert_terminal(BCDBShmXact *tx);



/*
 * bcdb_complete_replayed_item
 *
 * Called when the ledger reports a terminal row for this item.
 * Publishes the stored terminal result/error to the result ring and
 * advances all required watermarks without running business SQL.
 *
 * stored_terminal is either result_payload (APPLIED_OK) or error_payload
 * (APPLIED_ERROR).  is_error distinguishes the two cases.
 */
extern void bcdb_complete_replayed_item(
		BCDBShmXact  *tx,
		const char   *stored_terminal,
		bool          is_error,
		TransactionId replay_xid
);

extern void bcdb_finish_terminal_item(
		BCDBShmXact  *tx,
		const char   *terminal_payload,
		bool          is_error,
		bool          is_replay,
		TransactionId committed_xid
);

extern void bcdb_maybe_trigger_safe_failpoint(
		const char   *name,
		BCDBShmXact  *tx,
		const char   *phase
);

#endif /* BCDB_RAFT_APPLY_LEDGER_H */
