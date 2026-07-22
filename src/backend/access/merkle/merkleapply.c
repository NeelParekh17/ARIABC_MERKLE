/*-------------------------------------------------------------------------
 *
 * merkleapply.c
 *    Merkle recovery-state inspection and fail-closed state updates.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/merkle.h"
#include "catalog/pg_authid_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(merkle_recovery_status);

static bool
merkle_state_relations_exist(void)
{
	Oid namespace_oid = get_namespace_oid("ariabc_internal", true);

	if (!OidIsValid(namespace_oid))
		return false;
	return OidIsValid(get_relname_relid("merkle_apply_state", namespace_oid)) &&
		OidIsValid(get_relname_relid("merkle_apply_counter", namespace_oid)) &&
		OidIsValid(get_relname_relid("merkle_local_delta", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_entry", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_entry_item", namespace_oid)) &&
		OidIsValid(get_relname_relid("raft_apply_item", namespace_oid));
}

static void
merkle_mark_recovery_state_impl(MerkleRecoveryState state, const char *reason)
{
	Oid argtypes[2] = {INT2OID, TEXTOID};
	Datum values[2];
	char nulls[2] = {' ', ' '};
	int spi_rc;

	if (!merkle_state_relations_exist())
		return;
	values[0] = Int16GetDatum((int16) state);
	values[1] = CStringGetTextDatum(reason ? reason : "");
	if (SPI_connect() != SPI_OK_CONNECT)
		return;
	spi_rc = SPI_execute_with_args(
		"UPDATE ariabc_internal.merkle_apply_state"
		"   SET state = $1, error_text = NULLIF($2, ''),"
		"       updated_at = clock_timestamp()"
		" WHERE singleton",
		2, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_UPDATE || SPI_processed != 1)
	{
		(void) SPI_finish();
		return;
	}
	(void) SPI_finish();
}

void
merkle_mark_recovery_state(MerkleRecoveryState state, const char *reason)
{
	Oid saved_userid;
	int saved_sec_context;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		merkle_mark_recovery_state_impl(state, reason);
	}
	PG_CATCH();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	SetUserIdAndSecContext(saved_userid, saved_sec_context);
}

void
merkle_get_recovery_status(MerkleRecoveryStatusData *status)
{
	bool pushed_snapshot = false;
	int spi_rc;
	bool isnull;
	Datum datum;

	MemSet(status, 0, sizeof(*status));
	/*
	 * P0.2: do NOT default to READY when the schema is absent.  An absent
	 * schema with at least one Merkle index is INVALID; without any index
	 * it is unmanaged but acceptable (managed=false, state left as 0).
	 */
	status->state = MERKLE_STATE_INVALID;
	if (!merkle_state_relations_exist())
	{
		status->managed = false;
		status->state = MERKLE_STATE_INVALID;	/* fail closed */
		return;
	}
	status->managed = true;
	status->state = MERKLE_STATE_READY;	/* may be overwritten below */

	PushCopiedSnapshot(GetLatestSnapshot());
	pushed_snapshot = true;
	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "Merkle status SPI_connect failed: %d", spi_rc);

	spi_rc = SPI_execute(
		"SELECT s.applied_seq, s.state, COALESCE(s.error_text, ''),"
		"       c.terminal_prefix_seq,"
		"       GREATEST(c.terminal_prefix_seq,"
		"         COALESCE((SELECT max(merkle_apply_seq)"
		"                     FROM ariabc_internal.raft_apply_item"
		"                    WHERE state IN (2, 3, 4)), 0),"
		"         COALESCE((SELECT max(apply_seq)"
		"                     FROM ariabc_internal.merkle_local_delta), 0))"
		"  FROM ariabc_internal.merkle_apply_state s"
		"  JOIN ariabc_internal.merkle_apply_counter c ON c.singleton"
		" WHERE s.singleton",
		true, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "Merkle apply-state singleton is missing");
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle applied sequence is NULL");
	status->applied_seq = (uint64) DatumGetInt64(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2,
						&isnull);
	if (!isnull)
		status->state = (MerkleRecoveryState) DatumGetInt16(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3,
						&isnull);
	if (!isnull)
		strlcpy(status->error_text, TextDatumGetCString(datum),
				sizeof(status->error_text));
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle terminal_prefix_seq is NULL");
	status->terminal_prefix_seq = (uint64) DatumGetInt64(datum);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle highest terminal sequence is NULL");
	status->highest_terminal_seq = (uint64) DatumGetInt64(datum);
	status->target_seq = status->highest_terminal_seq;

	/* Native v8 roots are committed directly into merkle_dynamic_state.  The
	 * removed pending-log ledger is not an authority for native readiness;
	 * stale ledger rows must not leave an otherwise published native root in
	 * BLOCKED_ON_GAP. */
	spi_rc = SPI_execute(
		"SELECT COALESCE(max(applied_seq), 0)"
		"  FROM ariabc_internal.merkle_dynamic_state"
		" WHERE build_complete",
		true, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "Merkle native state position query failed");
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
						&isnull);
	if (isnull)
		elog(ERROR, "Merkle native state position is NULL");
	status->applied_seq = (uint64) DatumGetInt64(datum);
	status->terminal_prefix_seq = status->applied_seq;
	status->highest_terminal_seq = status->applied_seq;
	status->target_seq = status->applied_seq;

	if (status->state != MERKLE_STATE_INVALID &&
		status->state != MERKLE_STATE_REBUILD_REQUIRED)
	{
		if (status->highest_terminal_seq <= status->applied_seq)
			status->state = MERKLE_STATE_READY;
		else if (status->terminal_prefix_seq > status->applied_seq)
			status->state = MERKLE_STATE_CATCHING_UP;
		else
			status->state = MERKLE_STATE_BLOCKED_ON_GAP;
	}
	if (status->target_seq > status->applied_seq)
		status->blocked_seq = status->applied_seq + 1;

	/*
	 * P0.2 hard invariant: applied_seq must never exceed target_seq.
	 * If it does, the terminal prefix was not advanced when it should have
	 * been — treat this as INVALID to prevent stale roots appearing READY.
	 */
	if (status->applied_seq > status->terminal_prefix_seq)
		status->state = MERKLE_STATE_INVALID;

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "Merkle status SPI_finish failed");
	if (pushed_snapshot)
		PopActiveSnapshot();
}

void
merkle_require_fresh(void)
{
	MerkleRecoveryStatusData status;

	if (merkle_has_staged_delta())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle root cannot be read after uncommitted table changes"),
				 errdetail("The current transaction has staged Merkle deltas that are not yet durable."),
				 errhint("Commit the transaction, then read or apply the Merkle root in a new transaction.")));

	merkle_get_recovery_status(&status);
	/* Avoid entering an internal apply subtransaction on the overwhelmingly
	 * common READY read path.  Besides eliminating needless overhead, this is
	 * required when the function is evaluated inside CTAS/materialized SRFs:
	 * their destination relation is already owned by the caller's resource
	 * owner and must not be crossed by an unnecessary subtransaction.
	 *
	 * WAIT observes an independently advancing applier; APPLY explicitly
	 * permits this backend to help it.
	 */
	if (status.state != MERKLE_STATE_READY &&
		merkle_read_lag_policy == MERKLE_READ_LAG_WAIT)
	{
		int retries;

		/* WAIT never mutates pages in the reader's query/resource owner. */
		for (retries = 0; retries < 1000; retries++)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
			merkle_get_recovery_status(&status);
			if (status.state == MERKLE_STATE_READY ||
				status.state == MERKLE_STATE_INVALID ||
				status.state == MERKLE_STATE_REBUILD_REQUIRED ||
				status.state == MERKLE_STATE_BLOCKED_ON_GAP)
				break;
		}
	}
	/* P0.2: unmanaged state (no schema) must fail closed, not silently pass. */
	if (!status.managed)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle crash-safety state is not initialized"),
				 errhint("Run scripts/distributed/bootstrap_raft_apply_ledger.sh for this database.")));
	if (status.state != MERKLE_STATE_READY)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Merkle index is not synchronized with committed database state"),
				 errdetail("state=%d applied_seq=%llu target_seq=%llu blocked_seq=%llu%s%s",
						   (int) status.state,
						   (unsigned long long) status.applied_seq,
						   (unsigned long long) status.target_seq,
						   (unsigned long long) status.blocked_seq,
						   status.error_text[0] ? " error=" : "",
						   status.error_text[0] ? status.error_text : ""),
					 errhint("Commit native Merkle updates and retry after recovery reaches READY.")));
}

Datum
merkle_recovery_status(PG_FUNCTION_ARGS)
{
	MerkleRecoveryStatusData status;
	StringInfoData out;
	const char *state_name;

	merkle_get_recovery_status(&status);
	switch (status.state)
	{
		case MERKLE_STATE_READY:
			state_name = "READY";
			break;
		case MERKLE_STATE_CATCHING_UP:
			state_name = "CATCHING_UP";
			break;
		case MERKLE_STATE_BLOCKED_ON_GAP:
			state_name = "BLOCKED_ON_GAP";
			break;
		case MERKLE_STATE_REBUILD_REQUIRED:
			state_name = "REBUILD_REQUIRED";
			break;
		default:
			state_name = "INVALID";
			break;
	}

	initStringInfo(&out);
	appendStringInfo(&out,
		"{\"state\":\"%s\",\"managed\":%s,\"applied_seq\":%llu,"
		"\"target_seq\":%llu,\"terminal_prefix_seq\":%llu,"
		"\"highest_terminal_seq\":%llu,\"blocked_seq\":%llu,\"error\":",
		state_name, status.managed ? "true" : "false",
		(unsigned long long) status.applied_seq,
		(unsigned long long) status.target_seq,
		(unsigned long long) status.terminal_prefix_seq,
		(unsigned long long) status.highest_terminal_seq,
		(unsigned long long) status.blocked_seq);
	if (status.error_text[0] != '\0')
		escape_json(&out, status.error_text);
	else
		appendStringInfoString(&out, "null");
	appendStringInfoChar(&out, '}');

	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
