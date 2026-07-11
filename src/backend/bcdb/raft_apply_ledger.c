/*-------------------------------------------------------------------------
 *
 * raft_apply_ledger.c
 *    Crash-safe Raft → BCDB → PostgreSQL apply ledger (Phases D & E).
 *
 * IDENTIFICATION
 *    src/backend/bcdb/raft_apply_ledger.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <openssl/evp.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "storage/condition_variable.h"
#include "access/merkle.h"
#include "access/xact.h"
#include "fmgr.h"
#include "port/pg_bswap.h"

extern void block_cleaning_dt(BCBlockID block_id);

#include "bcdb/raft_apply_ledger.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/shm_block.h"
#include "bcdb/globals.h"
#include "bcdb/worker.h"

#define EMIT_SAFE_LEDGER_XACT(tx, _phase) \
	elog(LOG, "SAFE_LEDGER_XACT phase=%s\n" \
			  "log=%llu ord=%u\n" \
			  "top_xid=%u\n" \
			  "nest_level=%d\n" \
			  "subxid=%u", \
		 (_phase), \
		 (unsigned long long) (tx)->raft_log_index, \
		 (unsigned) (tx)->raft_item_ordinal, \
		 (unsigned) GetTopTransactionIdIfAny(), \
		 GetCurrentTransactionNestLevel(), \
		 (unsigned) GetCurrentSubTransactionId())

/* --------------------------------------------------------------------------
 * Internal helpers
 * -------------------------------------------------------------------------- */

#define BCDB_RESULT_RING_OWNER_BLOCK_ID 1

typedef struct LedgerSpiScope
{
	bool pushed_snapshot;
	bool spi_connected;
} LedgerSpiScope;

static LedgerSpiScope
ledger_spi_begin(void)
{
	LedgerSpiScope scope;
	int spi_rc;

	scope.pushed_snapshot = false;
	scope.spi_connected = false;

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		scope.pushed_snapshot = true;
	}

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
	{
		if (scope.pushed_snapshot)
			PopActiveSnapshot();

		elog(ERROR, "raft_apply_ledger: SPI_connect failed: %d", spi_rc);
	}

	scope.spi_connected = true;
	return scope;
}

static void
ledger_spi_end(LedgerSpiScope *scope)
{
	if (scope->spi_connected)
	{
		int spi_rc = SPI_finish();

		scope->spi_connected = false;
		if (spi_rc != SPI_OK_FINISH)
			elog(ERROR, "raft_apply_ledger: SPI_finish failed: %d", spi_rc);
	}

	if (scope->pushed_snapshot)
	{
		PopActiveSnapshot();
		scope->pushed_snapshot = false;
	}
}

/*
 * make_bytea — create a real bytea datum.
 */
static bytea *
make_bytea(const uint8 *src, Size len)
{
	bytea *out = (bytea *) palloc(VARHDRSZ + len);
	SET_VARSIZE(out, VARHDRSZ + len);
	memcpy(VARDATA(out), src, len);
	return out;
}

static char *
bytea_to_cstring_in_context(bytea *ba, MemoryContext context)
{
	int len = VARSIZE_ANY_EXHDR(ba);
	char *out = (char *) MemoryContextAlloc(context, len + 1);

	memcpy(out, VARDATA_ANY(ba), len);
	out[len] = '\0';
	return out;
}

static BCBlock *
bcdb_result_ring_owner_block(void)
{
	return get_block_by_id(BCDB_RESULT_RING_OWNER_BLOCK_ID, false);
}

static bool
bcdb_parse_uint64_env(const char *name, uint64 *out)
{
	const char *value = getenv(name);
	char *end = NULL;
	unsigned long long parsed;

	if (value == NULL || value[0] == '\0')
		return false;

	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0')
		elog(ERROR, "invalid %s value for safe failpoint: %s", name, value);

	*out = (uint64) parsed;
	return true;
}

void
bcdb_maybe_trigger_safe_failpoint(const char *name,
								  BCDBShmXact *tx,
								  const char *phase)
{
	const char *enabled;
	uint64 filter;
	uint32 expected = 0;
	int node_id = 0;
	const char *node_id_env = getenv("ARIABC_RAFT_NODE_ID");
	char epoch_hex[65];
	int i;

	if (node_id_env != NULL && node_id_env[0] != '\0')
		node_id = atoi(node_id_env);

	if (name == NULL || name[0] == '\0' || tx == NULL || !tx->raft_ledger_enabled)
		return;

	enabled = getenv(name);
	if (enabled == NULL || enabled[0] == '\0')
		return;

	if (bcdb_parse_uint64_env("ARIABC_FAILPOINT_NODE_ID", &filter))
	{
		if (node_id != (int) filter)
			return;
	}

	if (bcdb_parse_uint64_env("ARIABC_FAILPOINT_RAFT_LOG_INDEX", &filter) &&
		tx->raft_log_index != filter)
		return;

	if (bcdb_parse_uint64_env("ARIABC_FAILPOINT_MIN_RAFT_LOG_INDEX", &filter))
	{
		if (filter == 0)
			elog(ERROR,
				 "ARIABC_FAILPOINT_MIN_RAFT_LOG_INDEX must be greater than zero");

		if (tx->raft_log_index < filter)
			return;
	}

	if (bcdb_parse_uint64_env("ARIABC_FAILPOINT_ITEM_ORDINAL", &filter) &&
		tx->raft_item_ordinal != (uint32) filter)
		return;

	if (bcdb_safe_failpoint_fired == NULL)
		elog(ERROR, "safe failpoint requested before shared fired flag initialized");

	if (!pg_atomic_compare_exchange_u32(bcdb_safe_failpoint_fired, &expected, 1))
		return;

	for (i = 0; i < 32; i++)
		sprintf(&epoch_hex[i * 2], "%02x", (unsigned char) tx->raft_epoch_id[i]);
	epoch_hex[64] = '\0';

	const char *cluster_id = getenv("ARIABC_RAFT_CLUSTER_ID");
	if (cluster_id == NULL || cluster_id[0] == '\0')
		cluster_id = "unknown_cluster";

	elog(WARNING,
		 "SAFE_FAILPOINT_TRIGGERED name=%s phase=%s node=%d log=%llu ordinal=%u pid=%d epoch=%s cluster_id=%s",
		 name,
		 phase ? phase : "",
		 node_id,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 MyProcPid,
		 epoch_hex,
		 cluster_id);
	kill(getpid(), SIGKILL);
}

static bool
validate_utf8(const char *str)
{
	const unsigned char *bytes = (const unsigned char *) str;
	while (*bytes)
	{
		if (bytes[0] <= 0x7F)
		{
			bytes += 1;
		}
		else if ((bytes[0] & 0xE0) == 0xC0)
		{
			if ((bytes[1] & 0xC0) != 0x80) return false;
			bytes += 2;
		}
		else if ((bytes[0] & 0xF0) == 0xE0)
		{
			if ((bytes[1] & 0xC0) != 0x80 || (bytes[2] & 0xC0) != 0x80) return false;
			bytes += 3;
		}
		else if ((bytes[0] & 0xF8) == 0xF0)
		{
			if ((bytes[1] & 0xC0) != 0x80 || (bytes[2] & 0xC0) != 0x80 || (bytes[3] & 0xC0) != 0x80) return false;
			bytes += 4;
		}
		else
		{
			return false;
		}
	}
	return true;
}

static void
validate_terminal_payload(BCDBShmXact *tx, const char *payload, int fmtver, bool is_error, const char *sqlstate, const uint8 *digest)
{
	if (payload && !validate_utf8(payload))
	{
		elog(ERROR, "raft_apply_ledger: payload contains invalid UTF-8 bytes");
	}

	if (fmtver != 1)
	{
		elog(ERROR, "raft_apply_ledger: unsupported terminal_format_version=%d (only format 1 is supported)", fmtver);
	}

	BCBlock *block = bcdb_result_ring_owner_block();
	int slots = bcdb_get_runtime_result_ring_slots();
	if (slots < 1) slots = 1;
	int mem_txid = (int)(tx->tx_id % (BCTxID) slots);
	if (mem_txid < 0) mem_txid += slots;
	size_t slot_capacity = (block != NULL) ? sizeof(block->result[mem_txid]) : 1024;

	char digest_hex[BCDB_RAFT_DIGEST_BYTES * 2 + 1];
	int i;
	for (i = 0; i < BCDB_RAFT_DIGEST_BYTES; i++)
	{
		sprintf(digest_hex + (i * 2), "%02x", digest[i]);
	}
	digest_hex[BCDB_RAFT_DIGEST_BYTES * 2] = '\0';

	int formatted_len = snprintf(NULL, 0,
			 "[BCDB_RAFT_COMMIT_CONFIRMED]\n"
			 "raft_log_index=%llu\n"
			 "raft_item_ordinal=%u\n"
			 "terminal_digest=%s\n"
			 "terminal_state=%s\n"
			 "terminal_format_version=%d\n"
			 "postgres_commit_confirmed=1\n"
			 "[PAYLOAD]\n%s",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 digest_hex,
			 is_error ? "ERROR" : "OK",
			 fmtver,
			 payload ? payload : "");

	if (formatted_len < 0 || (size_t) formatted_len >= slot_capacity)
	{
		elog(ERROR,
			 "raft_apply_ledger: terminal envelope for log_index=%llu "
			 "ordinal=%u is %d bytes, which exceeds ring slot capacity of "
			 "%zu bytes (terminal payload too large for version-%d format)",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 formatted_len,
			 slot_capacity,
			 fmtver);
	}
}

/*
 * compute_terminal_digest — SHA-256 of a canonical terminal record.
 *
 * APPLIED_OK:    SHA256("ariabc-terminal-ok-v1"   || fmtver_be32 || payload_len_be32 || payload)
 * APPLIED_ERROR: SHA256("ariabc-terminal-error-v1" || fmtver_be32 || sqlstate_len_be32 || sqlstate || payload_len_be32 || payload)
 */
static void
compute_terminal_digest(bool is_error,
						int   fmtver,
						const char *sqlstate,
						const char *payload,
						uint8 out_digest[BCDB_RAFT_DIGEST_BYTES])
{
	EVP_MD_CTX  *ctx;
	uint32       fmtver_be;
	const char  *prefix    = is_error
							 ? "ariabc-terminal-error-v1"
							 : "ariabc-terminal-ok-v1";
	unsigned int digest_len = BCDB_RAFT_DIGEST_BYTES;
	uint32       payload_len = payload ? (uint32)strlen(payload) : 0;
	uint32       payload_len_be = pg_hton32(payload_len);

	/* Use big-endian so the digest is platform-independent */
	fmtver_be = pg_hton32((uint32) fmtver);

	ctx = EVP_MD_CTX_new();
	if (!ctx)
		elog(ERROR, "raft_apply_ledger: EVP_MD_CTX_new failed");

	if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
		EVP_DigestUpdate(ctx, prefix, strlen(prefix)) != 1 ||
		EVP_DigestUpdate(ctx, &fmtver_be, sizeof(fmtver_be)) != 1)
	{
		EVP_MD_CTX_free(ctx);
		elog(ERROR, "raft_apply_ledger: SHA-256 initialization failed");
	}

	if (is_error)
	{
		const char *state_to_hash = sqlstate ? sqlstate : "XX000";
		uint32      state_len = (uint32)strlen(state_to_hash);
		uint32      state_len_be = pg_hton32(state_len);

		if (EVP_DigestUpdate(ctx, &state_len_be, sizeof(state_len_be)) != 1 ||
			EVP_DigestUpdate(ctx, state_to_hash, state_len) != 1)
		{
			EVP_MD_CTX_free(ctx);
			elog(ERROR, "raft_apply_ledger: SHA-256 update failed");
		}
	}

	if (EVP_DigestUpdate(ctx, &payload_len_be, sizeof(payload_len_be)) != 1 ||
		(payload_len > 0 && EVP_DigestUpdate(ctx, payload, payload_len) != 1) ||
		EVP_DigestFinal_ex(ctx, out_digest, &digest_len) != 1)
	{
		EVP_MD_CTX_free(ctx);
		elog(ERROR, "raft_apply_ledger: SHA-256 finalization failed");
	}
	EVP_MD_CTX_free(ctx);
}

static void
digest_to_hex(const uint8 digest[BCDB_RAFT_DIGEST_BYTES], char out[BCDB_RAFT_DIGEST_BYTES * 2 + 1])
{
	int i;

	for (i = 0; i < BCDB_RAFT_DIGEST_BYTES; i++)
		sprintf(out + (i * 2), "%02x", digest[i]);
	out[BCDB_RAFT_DIGEST_BYTES * 2] = '\0';
}

void
bcdb_prepare_nonterminal_failure(BCDBShmXact *tx,
								 const char *sqlstate,
								 const char *failure_class,
								 bool retryable,
								 BCDBNonterminalFailure *failure)
{
	EVP_MD_CTX  *ctx;
	uint64       log_be;
	uint32       ord_be;
	uint32       retry_be;
	uint32       fmt_be;
	unsigned int digest_len = BCDB_RAFT_DIGEST_BYTES;
	const char  *prefix = "ARIABC_SAFE_NONTERMINAL_FAILURE_V1";
	const char  *state_to_hash = sqlstate ? sqlstate : "XX000";
	const char  *class_to_hash = failure_class ? failure_class : "UNKNOWN";

	if (tx == NULL || failure == NULL)
		elog(ERROR, "raft_apply_ledger: cannot prepare nonterminal failure without tx/failure");
	if (strlen(state_to_hash) != 5)
		elog(ERROR, "raft_apply_ledger: invalid nonterminal SQLSTATE '%s'", state_to_hash);
	if (failure_class == NULL || failure_class[0] == '\0' ||
		strlen(failure_class) >= BCDB_FAILURE_CLASS_MAX)
		elog(ERROR, "raft_apply_ledger: invalid nonterminal failure class '%s'",
			 failure_class ? failure_class : "<null>");

	memset(failure, 0, sizeof(*failure));
	memcpy(failure->sqlstate, state_to_hash, 5);
	failure->sqlstate[5] = '\0';
	strlcpy(failure->failure_class, class_to_hash, sizeof(failure->failure_class));
	failure->retryable = retryable;
	failure->format_version = 1;

	log_be = pg_hton64(tx->raft_log_index);
	ord_be = pg_hton32(tx->raft_item_ordinal);
	retry_be = pg_hton32(retryable ? 1 : 0);
	fmt_be = pg_hton32((uint32) failure->format_version);

	ctx = EVP_MD_CTX_new();
	if (!ctx)
		elog(ERROR, "raft_apply_ledger: EVP_MD_CTX_new failed");

	if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
		EVP_DigestUpdate(ctx, prefix, strlen(prefix)) != 1 ||
		EVP_DigestUpdate(ctx, tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES) != 1 ||
		EVP_DigestUpdate(ctx, &log_be, sizeof(log_be)) != 1 ||
		EVP_DigestUpdate(ctx, &ord_be, sizeof(ord_be)) != 1 ||
		EVP_DigestUpdate(ctx, state_to_hash, 5) != 1 ||
		EVP_DigestUpdate(ctx, class_to_hash, strlen(class_to_hash)) != 1 ||
		EVP_DigestUpdate(ctx, &retry_be, sizeof(retry_be)) != 1 ||
		EVP_DigestUpdate(ctx, &fmt_be, sizeof(fmt_be)) != 1 ||
		EVP_DigestFinal_ex(ctx, failure->digest, &digest_len) != 1)
	{
		EVP_MD_CTX_free(ctx);
		elog(ERROR, "raft_apply_ledger: SHA-256 nonterminal digest failed");
	}
	EVP_MD_CTX_free(ctx);
}

static bool
nonterminal_failure_matches(const BCDBNonterminalFailure *a,
							const BCDBNonterminalFailure *b)
{
	if (a == NULL || b == NULL)
		return false;
	return memcmp(a->digest, b->digest, BCDB_RAFT_DIGEST_BYTES) == 0 &&
		strncmp(a->sqlstate, b->sqlstate, 5) == 0 &&
		a->failure_class[0] != '\0' &&
		b->failure_class[0] != '\0' &&
		strcmp(a->failure_class, b->failure_class) == 0 &&
		a->retryable == b->retryable &&
		a->format_version == b->format_version;
}

/* --------------------------------------------------------------------------
 * D2: bcdb_raft_ledger_claim
 * -------------------------------------------------------------------------- */

RaftClaimResult
bcdb_raft_ledger_claim(BCDBShmXact  *tx,
					   char        **out_result_payload,
					   int          *out_result_fmtver,
					   char        **out_error_payload,
					   int          *out_error_fmtver,
					   char        **out_sqlstate,
					   BCDBNonterminalFailure *out_failure)
{
	int          spi_rc;
	char         sql_buf[2048];
	SPIPlanPtr   existing_plan;

	Oid          argtypes[6];
	Datum        values[6];
	char         nulls[6];
	uint64       existing_state = 0;
	MemoryContext caller_context = CurrentMemoryContext;

	/* Legacy/direct mode — no ledger */
	if (!tx || !tx->raft_ledger_enabled)
		return RAFT_CLAIM_DISABLED;

	/* Connect to SPI inside the existing top-level transaction */
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=spi_connect",
		 MyProcPid,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal);
	LedgerSpiScope spi_scope = ledger_spi_begin();
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=spi_connect_ok",
		 MyProcPid,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal);
	elog(LOG,
		 "LEDGER_STAGE pid=%d log=%llu ord=%u stage=spi_scope_ready pushed_snapshot=%d active_snapshot=%d",
		 MyProcPid,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 spi_scope.pushed_snapshot ? 1 : 0,
		 ActiveSnapshotSet() ? 1 : 0);

	/* Step 0: validate against the entry manifest */
	snprintf(sql_buf, sizeof(sql_buf),
		"SELECT 1"
		"  FROM ariabc_internal.raft_apply_entry e"
		"  JOIN ariabc_internal.raft_apply_entry_item i"
		"    ON e.epoch_id = i.epoch_id AND e.raft_log_index = i.raft_log_index"
		" WHERE e.epoch_id = $1"
		"   AND e.raft_log_index = $2"
		"   AND i.item_ordinal = $3"
		"   AND e.entry_digest = $4"
		"   AND i.item_digest = $5"
		"   AND $3 < e.expected_items");

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	argtypes[3] = BYTEAOID;
	argtypes[4] = BYTEAOID;
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=manifest_args_begin",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=epoch_bytea_ok",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	values[3] = PointerGetDatum(make_bytea(tx->raft_entry_digest, BCDB_RAFT_DIGEST_BYTES));
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=entry_digest_bytea_ok",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);
	values[4] = PointerGetDatum(make_bytea(tx->raft_item_digest, BCDB_RAFT_DIGEST_BYTES));
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=item_digest_bytea_ok",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);
	memset(nulls, ' ', 5);

	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=manifest_spi_before",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);

	EMIT_SAFE_LEDGER_XACT(tx, "claim_manifest_before");
	spi_rc = SPI_execute_with_args(sql_buf, 5, argtypes, values, nulls, true /* read_only */, 1);
	EMIT_SAFE_LEDGER_XACT(tx, "claim_manifest_after");

	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=manifest_spi_after rc=%d processed=%lu",
		 MyProcPid, (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal,
		 spi_rc, (unsigned long) SPI_processed);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1) {
		ledger_spi_end(&spi_scope);
		elog(ERROR, "raft_apply_ledger: claim validation failed (manifest mismatch, missing, or out of bounds) for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index, (unsigned) tx->raft_item_ordinal);
	}
	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=manifest_select_ok",
		 MyProcPid,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal);

	/* Step 1: attempt INSERT (ON CONFLICT DO NOTHING) */
	argtypes[0] = BYTEAOID;   /* epoch_id */
	argtypes[1] = INT8OID;    /* raft_log_index */
	argtypes[2] = INT4OID;    /* item_ordinal */
	argtypes[3] = BYTEAOID;   /* entry_digest */
	argtypes[4] = BYTEAOID;   /* item_digest */
	argtypes[5] = INT2OID;    /* state = CLAIMED */

	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	values[3] = PointerGetDatum(make_bytea(tx->raft_entry_digest, BCDB_RAFT_DIGEST_BYTES));
	values[4] = PointerGetDatum(make_bytea(tx->raft_item_digest,  BCDB_RAFT_DIGEST_BYTES));
	values[5] = Int16GetDatum((int16) RAFT_ITEM_STATE_CLAIMED);
	memset(nulls, ' ', sizeof(nulls));

	snprintf(sql_buf, sizeof(sql_buf),
		"INSERT INTO ariabc_internal.raft_apply_item"
		" (epoch_id, raft_log_index, item_ordinal, entry_digest, item_digest, state, merkle_apply_seq)"
		" SELECT e.epoch_id, e.raft_log_index, i.item_ordinal,"
		"        e.entry_digest, i.item_digest, $6,"
		"        e.merkle_apply_seq_base + i.item_ordinal::bigint"
		"   FROM ariabc_internal.raft_apply_entry e"
		"   JOIN ariabc_internal.raft_apply_entry_item i"
		"     ON i.epoch_id = e.epoch_id"
		"    AND i.raft_log_index = e.raft_log_index"
		"  WHERE e.epoch_id = $1"
		"    AND e.raft_log_index = $2"
		"    AND i.item_ordinal = $3"
		"    AND e.entry_digest = $4"
		"    AND i.item_digest = $5"
		" ON CONFLICT (epoch_id, raft_log_index, item_ordinal) DO NOTHING");

	EMIT_SAFE_LEDGER_XACT(tx, "claim_insert_before");
	spi_rc = SPI_execute_with_args(sql_buf, 6, argtypes, values, nulls,
								   false, 1);
	EMIT_SAFE_LEDGER_XACT(tx, "claim_insert_after");
	uint64 insert_processed = SPI_processed;
	if (spi_rc != SPI_OK_INSERT)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: INSERT CLAIMED failed for log_index=%llu ordinal=%u rc=%d",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 spi_rc);
	}

	elog(LOG, "LEDGER_STAGE pid=%d log=%llu ord=%u stage=claimed_insert rc=%d processed=%lu",
		 MyProcPid,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 spi_rc,
		 (unsigned long) insert_processed);

	/* If we inserted exactly 1 row we own the slot → normal execution */
	if (insert_processed == 1)
	{
		ledger_spi_end(&spi_scope);

		{
			char epoch_hex[65];
			int i;
			const char *cluster_id = getenv("ARIABC_RAFT_CLUSTER_ID");
			int node_id = 0;
			const char *node_id_env = getenv("ARIABC_RAFT_NODE_ID");
			if (node_id_env != NULL && node_id_env[0] != '\0')
				node_id = atoi(node_id_env);
			if (cluster_id == NULL || cluster_id[0] == '\0')
				cluster_id = "unknown_cluster";
			for (i = 0; i < 32; i++)
				sprintf(&epoch_hex[i * 2], "%02x", (unsigned char) tx->raft_epoch_id[i]);
			epoch_hex[64] = '\0';

			elog(LOG,
				 "SAFE_CLAIM_INSERT pid=%d log=%llu ord=%u state=%d epoch=%s cluster_id=%s node_id=%d top_xid=%u nest_level=%d",
				 MyProcPid,
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal,
				 RAFT_ITEM_STATE_CLAIMED,
				 epoch_hex,
				 cluster_id,
				 node_id,
				 GetTopTransactionIdIfAny(),
				 GetCurrentTransactionNestLevel());
		}

		/*
		 * The terminal UPDATE happens later in the same top-level transaction.
		 * Advance the command counter so PostgreSQL does not treat the final
		 * APPLIED_* tuple as an update of a row inserted in the same command.
		 */
		CommandCounterIncrement();
		bcdb_emit_ledger_boundary("ledger_claim");
		return RAFT_CLAIM_OWNED;
	}

	/* Step 2: row already existed — read its current state */
	snprintf(sql_buf, sizeof(sql_buf),
		"SELECT state, result_format_version, result_payload,"
		"       error_format_version, sqlstate_code, error_payload,"
		"       terminal_digest, entry_digest, item_digest,"
		"       failure_digest, failure_sqlstate, failure_class,"
		"       failure_retryable, failure_format_version,"
		"       failure_recorded_at IS NOT NULL"
		"  FROM ariabc_internal.raft_apply_item"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND item_ordinal   = $3");

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	memset(nulls, ' ', 3);

	/*
	 * The conflict may be against a row terminalized by an earlier backend
	 * before this worker's long-lived internal snapshot was refreshed.  Read
	 * the row with GetLatestSnapshot() so preserve/restart replay sees the
	 * committed APPLIED_* version instead of a stale CLAIMED tuple.
	 */
	existing_plan = SPI_prepare(sql_buf, 3, argtypes);
	if (existing_plan == NULL)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: failed to prepare existing row read for "
			 "log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	}

	EMIT_SAFE_LEDGER_XACT(tx, "claim_read_before");
	spi_rc = SPI_execute_snapshot(existing_plan,
								  values,
								  nulls,
								  GetLatestSnapshot(),
								  InvalidSnapshot,
								  true,
								  false,
								  1);
	EMIT_SAFE_LEDGER_XACT(tx, "claim_read_after");
	SPI_freeplan(existing_plan);
	if (spi_rc != SPI_OK_SELECT)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: failed to read existing row for "
			 "log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	}
	if (SPI_processed == 0)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: conflict followed by missing row for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	}
	if (SPI_processed != 1)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: multiple existing rows for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	}

	{
		bool        state_isnull = false;
		bool        res_fmtver_isnull = false;
		bool        res_payload_isnull = false;
		bool        err_fmtver_isnull = false;
		bool        sqlstate_isnull = false;
		bool        err_payload_isnull = false;
		bool        term_digest_isnull = false;
		bool        entry_digest_isnull = false;
		bool        item_digest_isnull = false;
		bool        failure_digest_isnull = false;
		bool        failure_sqlstate_isnull = false;
		bool        failure_class_isnull = false;
		bool        failure_retryable_isnull = false;
		bool        failure_fmtver_isnull = false;
		bool        failure_recorded_isnull = false;

		existing_state = DatumGetInt16(
			SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &state_isnull));

		Datum result_fmtver_d = SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 2, &res_fmtver_isnull);
		Datum result_payload_d = SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 3, &res_payload_isnull);
		Datum err_fmtver_d = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 4, &err_fmtver_isnull);
		Datum sqlstate_d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 5, &sqlstate_isnull);
		Datum err_payload_d = SPI_getbinval(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 6, &err_payload_isnull);
		Datum terminal_digest_d = SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 7, &term_digest_isnull);
		Datum entry_digest_d = SPI_getbinval(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 8, &entry_digest_isnull);
		Datum item_digest_d = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 9, &item_digest_isnull);
		Datum failure_digest_d = SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 10, &failure_digest_isnull);
		Datum failure_sqlstate_d = SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 11, &failure_sqlstate_isnull);
		Datum failure_class_d = SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 12, &failure_class_isnull);
		Datum failure_retryable_d = SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 13, &failure_retryable_isnull);
		Datum failure_fmtver_d = SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 14, &failure_fmtver_isnull);
		Datum failure_recorded_d = SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 15, &failure_recorded_isnull);

		/* Validate entry_digest and item_digest invariants first */
		if (entry_digest_isnull)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "AriaBC ledger corruption: entry_digest is NULL in existing row");
		}
		bytea *db_entry_digest_ba = DatumGetByteaPP(entry_digest_d);
		if (VARSIZE_ANY_EXHDR(db_entry_digest_ba) != BCDB_RAFT_DIGEST_BYTES ||
			memcmp(VARDATA_ANY(db_entry_digest_ba), tx->raft_entry_digest, BCDB_RAFT_DIGEST_BYTES) != 0)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "AriaBC ledger corruption: entry_digest mismatch during replay");
		}

		if (item_digest_isnull)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "AriaBC ledger corruption: item_digest is NULL in existing row");
		}
		bytea *db_item_digest_ba = DatumGetByteaPP(item_digest_d);
		if (VARSIZE_ANY_EXHDR(db_item_digest_ba) != BCDB_RAFT_DIGEST_BYTES ||
			memcmp(VARDATA_ANY(db_item_digest_ba), tx->raft_item_digest, BCDB_RAFT_DIGEST_BYTES) != 0)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "AriaBC ledger corruption: item_digest mismatch during replay");
		}

		if (existing_state == RAFT_ITEM_STATE_APPLIED_OK)
		{
			int   res_fmtver = res_fmtver_isnull ? 1 : DatumGetInt32(result_fmtver_d);
			char *res_payload_str = NULL;
			uint8 recomputed_digest[BCDB_RAFT_DIGEST_BYTES];

			if (res_payload_isnull)
				res_payload_str = MemoryContextStrdup(caller_context, "");
			else
			{
				bytea *ba = DatumGetByteaPP(result_payload_d);
				res_payload_str = bytea_to_cstring_in_context(ba, caller_context);
			}

			compute_terminal_digest(false, res_fmtver, NULL, res_payload_str, recomputed_digest);

			if (term_digest_isnull)
			{
				pfree(res_payload_str);
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: terminal_digest is NULL in existing row");
			}
			bytea *digest_ba = DatumGetByteaPP(terminal_digest_d);
			if (VARSIZE_ANY_EXHDR(digest_ba) != BCDB_RAFT_DIGEST_BYTES ||
				memcmp(VARDATA_ANY(digest_ba), recomputed_digest, BCDB_RAFT_DIGEST_BYTES) != 0)
			{
				pfree(res_payload_str);
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: recomputed terminal digest mismatch during replay");
			}

			memcpy(tx->raft_terminal_digest, recomputed_digest, BCDB_RAFT_DIGEST_BYTES);
			tx->raft_terminal_format_version = res_fmtver;
			tx->raft_terminal_state = RAFT_ITEM_STATE_APPLIED_OK;
			tx->raft_terminal_update_confirmed = true;

			if (out_result_fmtver)
				*out_result_fmtver = res_fmtver;
			if (out_result_payload)
				*out_result_payload = res_payload_str;
			else
				pfree(res_payload_str);

			ledger_spi_end(&spi_scope);
			elog(LOG, "raft_apply_ledger: REPLAY_OK log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
			return RAFT_CLAIM_REPLAY_OK;
		}

		if (existing_state == RAFT_ITEM_STATE_APPLIED_ERROR)
		{
			int   err_fmtver = err_fmtver_isnull ? 1 : DatumGetInt32(err_fmtver_d);
			char *sqlstate_str = sqlstate_isnull ? MemoryContextStrdup(caller_context, "XX000") :
								 MemoryContextStrdup(caller_context,
													 text_to_cstring(DatumGetTextPP(sqlstate_d)));
			char *err_payload_str = NULL;
			uint8 recomputed_digest[BCDB_RAFT_DIGEST_BYTES];

			if (err_payload_isnull)
				err_payload_str = MemoryContextStrdup(caller_context, "");
			else
			{
				bytea *ba = DatumGetByteaPP(err_payload_d);
				err_payload_str = bytea_to_cstring_in_context(ba, caller_context);
			}

			compute_terminal_digest(true, err_fmtver, sqlstate_str, err_payload_str, recomputed_digest);

			if (term_digest_isnull)
			{
				pfree(sqlstate_str);
				pfree(err_payload_str);
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: terminal_digest is NULL in existing row");
			}
			bytea *digest_ba = DatumGetByteaPP(terminal_digest_d);
			if (VARSIZE_ANY_EXHDR(digest_ba) != BCDB_RAFT_DIGEST_BYTES ||
				memcmp(VARDATA_ANY(digest_ba), recomputed_digest, BCDB_RAFT_DIGEST_BYTES) != 0)
			{
				pfree(sqlstate_str);
				pfree(err_payload_str);
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: recomputed terminal digest mismatch during replay");
			}

			memcpy(tx->raft_terminal_digest, recomputed_digest, BCDB_RAFT_DIGEST_BYTES);
			tx->raft_terminal_format_version = err_fmtver;
			tx->raft_terminal_state = RAFT_ITEM_STATE_APPLIED_ERROR;
			tx->raft_terminal_update_confirmed = true;

			if (out_error_fmtver)
				*out_error_fmtver = err_fmtver;
			if (out_sqlstate)
				*out_sqlstate = sqlstate_str;
			else
				pfree(sqlstate_str);
			if (out_error_payload)
				*out_error_payload = err_payload_str;
			else
				pfree(err_payload_str);

			ledger_spi_end(&spi_scope);
			elog(LOG, "raft_apply_ledger: REPLAY_ERROR log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
			return RAFT_CLAIM_REPLAY_ERROR;
		}

		if (existing_state == RAFT_ITEM_STATE_NONTERMINAL_FAILURE)
		{
			BCDBNonterminalFailure stored;
			BCDBNonterminalFailure expected;
			bytea *failure_digest_ba;
			char *sqlstate_str;
			char *failure_class_str;
			bool recorded_present;

			if (failure_digest_isnull || failure_sqlstate_isnull ||
				failure_class_isnull || failure_retryable_isnull ||
				failure_fmtver_isnull || failure_recorded_isnull)
			{
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: incomplete state=4 failure row");
			}

			memset(&stored, 0, sizeof(stored));
			failure_digest_ba = DatumGetByteaPP(failure_digest_d);
			if (VARSIZE_ANY_EXHDR(failure_digest_ba) != BCDB_RAFT_DIGEST_BYTES)
			{
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: invalid state=4 failure_digest length");
			}
			memcpy(stored.digest, VARDATA_ANY(failure_digest_ba), BCDB_RAFT_DIGEST_BYTES);

			sqlstate_str = text_to_cstring(DatumGetTextPP(failure_sqlstate_d));
			failure_class_str = text_to_cstring(DatumGetTextPP(failure_class_d));
			recorded_present = DatumGetBool(failure_recorded_d);
			if (strlen(sqlstate_str) != 5 || !recorded_present ||
				DatumGetInt32(failure_fmtver_d) != 1)
			{
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: invalid state=4 failure metadata");
			}

			strlcpy(stored.sqlstate, sqlstate_str, sizeof(stored.sqlstate));
			strlcpy(stored.failure_class, failure_class_str, sizeof(stored.failure_class));
			stored.retryable = DatumGetBool(failure_retryable_d);
			stored.format_version = DatumGetInt32(failure_fmtver_d);

			bcdb_prepare_nonterminal_failure(tx,
											 stored.sqlstate,
											 stored.failure_class,
											 stored.retryable,
											 &expected);
			if (!nonterminal_failure_matches(&stored, &expected))
			{
				ledger_spi_end(&spi_scope);
				elog(ERROR, "AriaBC ledger corruption: state=4 canonical failure mismatch during replay");
			}

			memcpy(tx->raft_terminal_digest, stored.digest, BCDB_RAFT_DIGEST_BYTES);
			tx->raft_terminal_format_version = stored.format_version;
			tx->raft_terminal_state = RAFT_ITEM_STATE_NONTERMINAL_FAILURE;
			tx->raft_terminal_update_confirmed = true;

			if (out_failure)
				*out_failure = stored;

			ledger_spi_end(&spi_scope);
			elog(LOG,
				 "SAFE_NONTERMINAL_FAILURE_REPLAY log_index=%llu ordinal=%u sqlstate=%s",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal,
				 stored.sqlstate);
			return RAFT_CLAIM_REPLAY_NONTERMINAL_FAILURE;
		}
	}

	ledger_spi_end(&spi_scope);
	if (existing_state == RAFT_ITEM_STATE_CLAIMED)
		elog(ERROR,
			 "raft_apply_ledger: persistent or in-flight duplicate CLAIMED row for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	elog(ERROR,
		 "raft_apply_ledger: unknown existing state=%llu for log_index=%llu ordinal=%u",
		 (unsigned long long) existing_state,
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal);
	return RAFT_CLAIM_DISABLED;
}

/* --------------------------------------------------------------------------
 * D3: bcdb_raft_ledger_finalize_ok / bcdb_raft_ledger_finalize_error
 * -------------------------------------------------------------------------- */

static void
validate_terminal_update_returning(BCDBShmXact *tx,
								  bool is_error,
								  int expected_fmtver,
								  const char *expected_payload,
								  const char *expected_sqlstate,
								  int expected_delta_version,
								  const bytea *expected_delta_blob,
								  int spi_rc,
								  uint64 processed)
{
	Datum state_d;
	Datum digest_d;
	Datum result_fmtver_d;
	Datum error_fmtver_d;
	Datum result_payload_d;
	Datum error_sqlstate_d;
	Datum error_payload_d;
	Datum delta_version_d;
	Datum delta_blob_d;
	bool state_isnull = false;
	bool digest_isnull = false;
	bool result_fmtver_isnull = false;
	bool error_fmtver_isnull = false;
	bool result_payload_isnull = false;
	bool error_sqlstate_isnull = false;
	bool error_payload_isnull = false;
	bool delta_version_isnull = false;
	bool delta_blob_isnull = false;
	int16 state;
	bytea *digest_ba;
	int digest_len;
	int fmtver;
	char *payload = NULL;
	char *sqlstate = NULL;
	uint8 recomputed_digest[BCDB_RAFT_DIGEST_BYTES];
	TransactionId verified_top_xid;
	int verified_nest_level;

	if (spi_rc != SPI_OK_UPDATE_RETURNING || processed != 1 ||
		SPI_tuptable == NULL || SPI_tuptable->vals == NULL)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer did not return exactly one row (SPI_processed=%lu rc=%d) for log_index=%llu ordinal=%u",
			 (unsigned long) processed,
			 spi_rc,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	state_d = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc,
							1,
							&state_isnull);
	digest_d = SPI_getbinval(SPI_tuptable->vals[0],
							 SPI_tuptable->tupdesc,
							 2,
							 &digest_isnull);
	result_fmtver_d = SPI_getbinval(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc,
									3,
									&result_fmtver_isnull);
	result_payload_d = SPI_getbinval(SPI_tuptable->vals[0],
									 SPI_tuptable->tupdesc,
									 4,
									 &result_payload_isnull);
	error_sqlstate_d = SPI_getbinval(SPI_tuptable->vals[0],
									 SPI_tuptable->tupdesc,
									 5,
									 &error_sqlstate_isnull);
	error_payload_d = SPI_getbinval(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc,
									6,
									&error_payload_isnull);
	error_fmtver_d = SPI_getbinval(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc,
								   7,
								   &error_fmtver_isnull);
	delta_version_d = SPI_getbinval(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc,
									8,
									&delta_version_isnull);
	delta_blob_d = SPI_getbinval(SPI_tuptable->vals[0],
								 SPI_tuptable->tupdesc,
								 9,
								 &delta_blob_isnull);

	if (state_isnull)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer returned NULL state for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	state = DatumGetInt16(state_d);

	if ((is_error && state != RAFT_ITEM_STATE_APPLIED_ERROR) ||
		(!is_error && state != RAFT_ITEM_STATE_APPLIED_OK))
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer returned unexpected state=%d for log_index=%llu ordinal=%u",
			 (int) state,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (digest_isnull)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer returned NULL digest for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	digest_ba = DatumGetByteaPP(digest_d);
	digest_len = VARSIZE_ANY_EXHDR(digest_ba);
	if (digest_len != BCDB_RAFT_DIGEST_BYTES ||
		memcmp(VARDATA_ANY(digest_ba),
			   tx->raft_terminal_digest,
			   BCDB_RAFT_DIGEST_BYTES) != 0)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer digest mismatch for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (is_error)
	{
		if (error_fmtver_isnull || error_sqlstate_isnull)
			elog(ERROR,
				 "raft_apply_ledger: terminal finalizer returned incomplete error metadata for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);

		fmtver = DatumGetInt32(error_fmtver_d);
		sqlstate = text_to_cstring(DatumGetTextPP(error_sqlstate_d));
		if (error_payload_isnull)
			payload = pstrdup("");
		else
		{
			bytea *payload_ba = DatumGetByteaPP(error_payload_d);
			int len = VARSIZE_ANY_EXHDR(payload_ba);

			payload = palloc(len + 1);
			memcpy(payload, VARDATA_ANY(payload_ba), len);
			payload[len] = '\0';
		}

		if (fmtver != expected_fmtver || strcmp(sqlstate, expected_sqlstate ? expected_sqlstate : "XX000") != 0)
			elog(ERROR,
				 "raft_apply_ledger: terminal finalizer error metadata mismatch for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);

		compute_terminal_digest(true, fmtver, sqlstate, payload, recomputed_digest);
	}
	else
	{
		if (result_fmtver_isnull)
			elog(ERROR,
				 "raft_apply_ledger: terminal finalizer returned NULL result format version for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);

		fmtver = DatumGetInt32(result_fmtver_d);
		if (result_payload_isnull)
			payload = pstrdup("");
		else
		{
			bytea *payload_ba = DatumGetByteaPP(result_payload_d);
			int len = VARSIZE_ANY_EXHDR(payload_ba);

			payload = palloc(len + 1);
			memcpy(payload, VARDATA_ANY(payload_ba), len);
			payload[len] = '\0';
		}

		if (fmtver != expected_fmtver)
			elog(ERROR,
				 "raft_apply_ledger: terminal finalizer result format mismatch for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
		if (strcmp(payload, expected_payload ? expected_payload : "") != 0)
			elog(ERROR,
				 "raft_apply_ledger: terminal finalizer result payload mismatch for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);

		compute_terminal_digest(false, fmtver, NULL, payload, recomputed_digest);
	}

	if (memcmp(VARDATA_ANY(digest_ba),
			   recomputed_digest,
			   BCDB_RAFT_DIGEST_BYTES) != 0)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer recomputed digest mismatch for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (delta_version_isnull || DatumGetInt32(delta_version_d) != expected_delta_version)
		elog(ERROR,
			 "raft_apply_ledger: terminal finalizer Merkle delta version mismatch for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	if (expected_delta_version == 0)
	{
		if (!delta_blob_isnull)
			elog(ERROR,
				 "raft_apply_ledger: empty Merkle delta unexpectedly stored a blob for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
	}
	else
	{
		bytea *stored_delta;

		if (delta_blob_isnull || expected_delta_blob == NULL)
			elog(ERROR,
				 "raft_apply_ledger: missing Merkle delta blob for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
		stored_delta = DatumGetByteaPP(delta_blob_d);
		if (VARSIZE_ANY_EXHDR(stored_delta) != VARSIZE_ANY_EXHDR(expected_delta_blob) ||
			memcmp(VARDATA_ANY(stored_delta), VARDATA_ANY(expected_delta_blob),
				   VARSIZE_ANY_EXHDR(stored_delta)) != 0)
			elog(ERROR,
				 "raft_apply_ledger: Merkle delta blob mismatch for log_index=%llu ordinal=%u",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
	}

	tx->raft_terminal_update_confirmed = true;
	tx->raft_terminal_returning_verified = true;
	verified_top_xid = GetTopTransactionIdIfAny();
	verified_nest_level = GetCurrentTransactionNestLevel();
	tx->raft_terminal_verified_top_xid = verified_top_xid;
	tx->raft_terminal_verified_nest_level = verified_nest_level;

	{
		char epoch_hex[65];
		int i;
		const char *cluster_id = getenv("ARIABC_RAFT_CLUSTER_ID");
		int node_id = 0;
		const char *node_id_env = getenv("ARIABC_RAFT_NODE_ID");
		if (node_id_env != NULL && node_id_env[0] != '\0')
			node_id = atoi(node_id_env);
		if (cluster_id == NULL || cluster_id[0] == '\0')
			cluster_id = "unknown_cluster";
		for (i = 0; i < 32; i++)
			sprintf(&epoch_hex[i * 2], "%02x", (unsigned char) tx->raft_epoch_id[i]);
		epoch_hex[64] = '\0';

		elog(LOG,
			 "SAFE_FINALIZE_RETURNING_OK pid=%d log=%llu ord=%u state=%d spi_processed=%lu top_xid=%u nest_level=%d epoch=%s cluster_id=%s node_id=%d",
			 MyProcPid,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 (int) state,
			 (unsigned long) processed,
			 (unsigned) verified_top_xid,
			 verified_nest_level,
			 epoch_hex,
			 cluster_id,
			 node_id);
	}
}

void
bcdb_raft_ledger_finalize_ok(BCDBShmXact *tx,
							  const char  *result_payload,
							  int          result_fmtver)
{
	int    spi_rc;
	char   sql_buf[2048];
	uint8  terminal_digest[BCDB_RAFT_DIGEST_BYTES];
	Oid    argtypes[8];
	Datum  values[8];
	char   nulls[8];
	int    payload_len = result_payload ? strlen(result_payload) : 0;
	uint64 processed_ok;
	bytea *merkle_delta_blob;
	int    merkle_delta_version;

	if (!tx || !tx->raft_ledger_enabled)
		return;

	compute_terminal_digest(false, result_fmtver, NULL, result_payload, terminal_digest);
	memcpy(tx->raft_terminal_digest, terminal_digest, BCDB_RAFT_DIGEST_BYTES);
	tx->raft_terminal_format_version = result_fmtver;
	tx->raft_terminal_state = RAFT_ITEM_STATE_APPLIED_OK;

	/* Validate early inside transaction before database state is finalized */
	validate_terminal_payload(tx, result_payload, result_fmtver, false, NULL, terminal_digest);
	merkle_delta_blob = merkle_serialize_staged_delta(tx->raft_log_index,
											 tx->raft_item_ordinal);
	merkle_delta_version = merkle_delta_blob != NULL ? MERKLE_DELTA_VERSION : 0;

	/*
	 * Keep terminalization in a later command ID than the CLAIMED insert even
	 * when worker/SPI call nesting suppresses ordinary command boundaries.
	 */
	CommandCounterIncrement();

	LedgerSpiScope spi_scope = ledger_spi_begin();

	snprintf(sql_buf, sizeof(sql_buf),
		"UPDATE ariabc_internal.raft_apply_item"
		"   SET state = %d,"
		"       result_format_version = $3,"
		"       result_payload        = $4,"
		"       error_format_version  = NULL,"
		"       sqlstate_code         = NULL,"
		"       error_payload         = NULL,"
		"       terminal_digest       = $5,"
		"       merkle_delta_version  = $7,"
		"       merkle_delta_blob     = $8,"
		"       committed_at          = clock_timestamp()"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND item_ordinal = $6"
		"   AND state = %d"
		" RETURNING state, terminal_digest, result_format_version,"
		"           result_payload, sqlstate_code, error_payload,"
		"           error_format_version, merkle_delta_version,"
		"           merkle_delta_blob",
		RAFT_ITEM_STATE_APPLIED_OK, RAFT_ITEM_STATE_CLAIMED);

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	argtypes[3] = BYTEAOID;
	argtypes[4] = BYTEAOID;
	argtypes[5] = INT4OID;
	argtypes[6] = INT4OID;
	argtypes[7] = BYTEAOID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum(result_fmtver);
	values[3] = PointerGetDatum(make_bytea((const uint8 *)(result_payload ? result_payload : ""), payload_len));
	values[4] = PointerGetDatum(make_bytea(terminal_digest, BCDB_RAFT_DIGEST_BYTES));
	values[5] = Int32GetDatum((int32) tx->raft_item_ordinal);
	values[6] = Int32GetDatum(merkle_delta_version);
	if (merkle_delta_blob != NULL)
		values[7] = PointerGetDatum(merkle_delta_blob);
	else
		values[7] = (Datum) 0;
	memset(nulls, ' ', 8);
	if (merkle_delta_blob == NULL)
		nulls[7] = 'n';

	EMIT_SAFE_LEDGER_XACT(tx, "finalize_ok_before");
	spi_rc = SPI_execute_with_args(sql_buf, 8, argtypes, values, nulls,
								   false, 1);
	EMIT_SAFE_LEDGER_XACT(tx, "finalize_ok_after");
	processed_ok = SPI_processed;
	validate_terminal_update_returning(tx, false, result_fmtver, result_payload,
									NULL, merkle_delta_version,
									merkle_delta_blob, spi_rc, processed_ok);
	ledger_spi_end(&spi_scope);
	if (merkle_delta_blob != NULL)
		merkle_mark_staged_delta_persisted();
	merkle_crash_failpoint("after_merkle_delta_ledger_written");
	bcdb_maybe_trigger_safe_failpoint("ARIABC_FAILPOINT_AFTER_MERKLE_DELTA_LEDGER_WRITTEN",
								 tx, "after_merkle_delta_ledger_written");

	/*
	 * SPI mutation is complete, but this worker stays inside an internal
	 * PostgreSQL subtransaction. Advance the command counter so the deferred
	 * constraint trigger sees APPLIED_OK rather than the earlier CLAIMED tuple.
	 */
	CommandCounterIncrement();

	bcdb_emit_ledger_boundary("ledger_finalize_ok");
}

/* --------------------------------------------------------------------------
 * D4: bcdb_raft_ledger_finalize_error
 * -------------------------------------------------------------------------- */

void
bcdb_raft_ledger_finalize_error(BCDBShmXact *tx,
								 const char  *sqlstate,
								 const char  *error_payload,
								 int          error_fmtver)
{
	int    spi_rc;
	char   sql_buf[2048];
	uint8  terminal_digest[BCDB_RAFT_DIGEST_BYTES];
	Oid    argtypes[7];
	Datum  values[7];
	char   nulls[7];
	int    payload_len = error_payload ? strlen(error_payload) : 0;
	uint64 processed_err;

	if (!tx || !tx->raft_ledger_enabled)
		return;
	if (merkle_has_staged_delta())
		elog(ERROR,
			 "raft_apply_ledger: failed transaction retained staged Merkle deltas for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	compute_terminal_digest(true, error_fmtver, sqlstate, error_payload, terminal_digest);
	memcpy(tx->raft_terminal_digest, terminal_digest, BCDB_RAFT_DIGEST_BYTES);
	tx->raft_terminal_format_version = error_fmtver;
	tx->raft_terminal_state = RAFT_ITEM_STATE_APPLIED_ERROR;

	/* Validate early inside transaction before database state is finalized */
	validate_terminal_payload(tx, error_payload, error_fmtver, true, sqlstate, terminal_digest);

	CommandCounterIncrement();

	LedgerSpiScope spi_scope = ledger_spi_begin();

	snprintf(sql_buf, sizeof(sql_buf),
		"UPDATE ariabc_internal.raft_apply_item"
		"   SET state = %d,"
		"       result_format_version = NULL,"
		"       result_payload        = NULL,"
		"       error_format_version  = $3,"
		"       sqlstate_code         = $4,"
		"       error_payload         = $5,"
		"       terminal_digest       = $6,"
		"       merkle_delta_version  = 0,"
		"       merkle_delta_blob     = NULL,"
		"       committed_at          = clock_timestamp()"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND item_ordinal = $7"
		"   AND state = %d"
		" RETURNING state, terminal_digest, result_format_version,"
		"           result_payload, sqlstate_code, error_payload,"
		"           error_format_version, merkle_delta_version,"
		"           merkle_delta_blob",
		RAFT_ITEM_STATE_APPLIED_ERROR, RAFT_ITEM_STATE_CLAIMED);

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	argtypes[3] = TEXTOID;
	argtypes[4] = BYTEAOID;
	argtypes[5] = BYTEAOID;
	argtypes[6] = INT4OID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum(error_fmtver);
	values[3] = PointerGetDatum(cstring_to_text(sqlstate ? sqlstate : "XX000"));
	values[4] = PointerGetDatum(make_bytea((const uint8 *)(error_payload ? error_payload : ""), payload_len));
	values[5] = PointerGetDatum(make_bytea(terminal_digest, BCDB_RAFT_DIGEST_BYTES));
	values[6] = Int32GetDatum((int32) tx->raft_item_ordinal);
	memset(nulls, ' ', 7);

	EMIT_SAFE_LEDGER_XACT(tx, "finalize_error_before");
	spi_rc = SPI_execute_with_args(sql_buf, 7, argtypes, values, nulls,
								   false, 1);
	EMIT_SAFE_LEDGER_XACT(tx, "finalize_error_after");
	processed_err = SPI_processed;
	validate_terminal_update_returning(tx, true, error_fmtver, error_payload,
									sqlstate ? sqlstate : "XX000",
									0, NULL, spi_rc, processed_err);
	ledger_spi_end(&spi_scope);
	merkle_crash_failpoint("after_merkle_delta_ledger_written");

	CommandCounterIncrement();

	bcdb_emit_ledger_boundary("ledger_finalize_error");
}

bool
bcdb_safe_finalize_nonterminal_failure(BCDBShmXact *tx,
									   const BCDBNonterminalFailure *failure,
									   BCDBNonterminalFailure *stored_failure)
{
	LedgerSpiScope spi_scope;
	int    spi_rc;
	Oid    argtypes[8];
	Datum  values[8];
	char   nulls[8];
	char   sql_buf[4096];

	if (!tx || !tx->raft_ledger_enabled || failure == NULL)
		return false;
	if (failure->format_version != 1 ||
		strlen(failure->sqlstate) != 5 ||
		failure->failure_class[0] == '\0' ||
		strnlen(failure->failure_class, BCDB_FAILURE_CLASS_MAX) >= BCDB_FAILURE_CLASS_MAX)
		elog(ERROR, "raft_apply_ledger: invalid nonterminal failure input");

	spi_scope = ledger_spi_begin();

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	argtypes[3] = BYTEAOID;
	argtypes[4] = BYTEAOID;
	argtypes[5] = INT2OID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	values[3] = PointerGetDatum(make_bytea(tx->raft_entry_digest, BCDB_RAFT_DIGEST_BYTES));
	values[4] = PointerGetDatum(make_bytea(tx->raft_item_digest, BCDB_RAFT_DIGEST_BYTES));
	values[5] = Int16GetDatum((int16) RAFT_ITEM_STATE_CLAIMED);
	memset(nulls, ' ', sizeof(nulls));

	snprintf(sql_buf, sizeof(sql_buf),
		"INSERT INTO ariabc_internal.raft_apply_item"
		" (epoch_id, raft_log_index, item_ordinal, entry_digest, item_digest, state, merkle_apply_seq)"
		" SELECT e.epoch_id, e.raft_log_index, i.item_ordinal,"
		"        e.entry_digest, i.item_digest, $6,"
		"        e.merkle_apply_seq_base + i.item_ordinal::bigint"
		"   FROM ariabc_internal.raft_apply_entry e"
		"   JOIN ariabc_internal.raft_apply_entry_item i"
		"     ON e.epoch_id = i.epoch_id"
		"    AND e.raft_log_index = i.raft_log_index"
		"  WHERE e.epoch_id = $1"
		"    AND e.raft_log_index = $2"
		"    AND i.item_ordinal = $3"
		"    AND e.entry_digest = $4"
		"    AND i.item_digest = $5"
		"    AND $3 < e.expected_items"
		" ON CONFLICT (epoch_id, raft_log_index, item_ordinal) DO NOTHING");
	spi_rc = SPI_execute_with_args(sql_buf, 6, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_INSERT)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR, "raft_apply_ledger: state=4 CLAIMED insert failed rc=%d", spi_rc);
	}
	CommandCounterIncrement();

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	memset(nulls, ' ', 3);
	snprintf(sql_buf, sizeof(sql_buf),
		"SELECT state,"
		"       failure_digest, failure_sqlstate, failure_class,"
		"       failure_retryable, failure_format_version,"
		"       failure_recorded_at IS NOT NULL"
		"  FROM ariabc_internal.raft_apply_item"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND item_ordinal = $3"
		" FOR UPDATE");
	spi_rc = SPI_execute_with_args(sql_buf, 3, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR,
			 "raft_apply_ledger: state=4 finalizer could not lock one row for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);
	}

	{
		bool state_isnull = false;
		int16 state = DatumGetInt16(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc,
												  1,
												  &state_isnull));
		if (state_isnull)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "raft_apply_ledger: state=4 finalizer saw NULL state");
		}
		if (state == RAFT_ITEM_STATE_APPLIED_OK ||
			state == RAFT_ITEM_STATE_APPLIED_ERROR)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR,
				 "raft_apply_ledger: refusing to overwrite terminal state=%d with state=4 for log_index=%llu ordinal=%u",
				 (int) state,
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal);
		}
		if (state != RAFT_ITEM_STATE_CLAIMED &&
			state != RAFT_ITEM_STATE_NONTERMINAL_FAILURE)
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "raft_apply_ledger: unexpected state=%d in state=4 finalizer", (int) state);
		}
		if (state == RAFT_ITEM_STATE_CLAIMED)
		{
			Oid upd_argtypes[8];
			Datum upd_values[8];
			char upd_nulls[8];

			upd_argtypes[0] = BYTEAOID;
			upd_argtypes[1] = INT8OID;
			upd_argtypes[2] = INT4OID;
			upd_argtypes[3] = BYTEAOID;
			upd_argtypes[4] = TEXTOID;
			upd_argtypes[5] = TEXTOID;
			upd_argtypes[6] = BOOLOID;
			upd_argtypes[7] = INT4OID;
			upd_values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
			upd_values[1] = Int64GetDatum((int64) tx->raft_log_index);
			upd_values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
			upd_values[3] = PointerGetDatum(make_bytea(failure->digest, BCDB_RAFT_DIGEST_BYTES));
			upd_values[4] = PointerGetDatum(cstring_to_text(failure->sqlstate));
			upd_values[5] = PointerGetDatum(cstring_to_text(failure->failure_class));
			upd_values[6] = BoolGetDatum(failure->retryable);
			upd_values[7] = Int32GetDatum(failure->format_version);
			memset(upd_nulls, ' ', sizeof(upd_nulls));

			snprintf(sql_buf, sizeof(sql_buf),
				"UPDATE ariabc_internal.raft_apply_item"
				"   SET state = %d,"
				"       failure_digest = $4,"
				"       failure_sqlstate = $5::char(5),"
				"       failure_class = $6,"
				"       failure_retryable = $7,"
				"       failure_format_version = $8,"
				"       failure_recorded_at = clock_timestamp(),"
				"       sqlstate_code = NULL,"
				"       terminal_digest = NULL,"
				"       result_payload = NULL,"
				"       error_payload = NULL,"
				"       result_format_version = NULL,"
				"       error_format_version = NULL,"
				"       committed_at = NULL"
				" WHERE epoch_id = $1"
				"   AND raft_log_index = $2"
				"   AND item_ordinal = $3"
				"   AND state = %d",
				RAFT_ITEM_STATE_NONTERMINAL_FAILURE,
				RAFT_ITEM_STATE_CLAIMED);
			spi_rc = SPI_execute_with_args(sql_buf, 8, upd_argtypes, upd_values, upd_nulls, false, 1);
			if (spi_rc != SPI_OK_UPDATE || SPI_processed != 1)
			{
				ledger_spi_end(&spi_scope);
				elog(ERROR, "raft_apply_ledger: state=4 update did not affect exactly one CLAIMED row");
			}
			CommandCounterIncrement();
		}
	}

	argtypes[0] = BYTEAOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	values[0] = PointerGetDatum(make_bytea(tx->raft_epoch_id, BCDB_RAFT_DIGEST_BYTES));
	values[1] = Int64GetDatum((int64) tx->raft_log_index);
	values[2] = Int32GetDatum((int32) tx->raft_item_ordinal);
	memset(nulls, ' ', 3);
	snprintf(sql_buf, sizeof(sql_buf),
		"SELECT failure_digest, failure_sqlstate, failure_class,"
		"       failure_retryable, failure_format_version,"
		"       failure_recorded_at IS NOT NULL,"
		"       sqlstate_code IS NULL,"
		"       terminal_digest IS NULL,"
		"       result_payload IS NULL,"
		"       error_payload IS NULL,"
		"       result_format_version IS NULL,"
		"       error_format_version IS NULL,"
		"       committed_at IS NULL"
		"  FROM ariabc_internal.raft_apply_item"
		" WHERE epoch_id = $1"
		"   AND raft_log_index = $2"
		"   AND item_ordinal = $3"
		"   AND state = %d",
		RAFT_ITEM_STATE_NONTERMINAL_FAILURE);
	spi_rc = SPI_execute_with_args(sql_buf, 3, argtypes, values, nulls, false, 1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
	{
		ledger_spi_end(&spi_scope);
		elog(ERROR, "raft_apply_ledger: state=4 verification did not find exactly one row");
	}
	{
		bool isnull[13];
		Datum retryable_d;
		Datum fmtver_d;
		Datum recorded_d;
		Datum sqlstate_null_d;
		Datum term_null_d;
		Datum result_null_d;
		Datum error_null_d;
		Datum result_fmt_null_d;
		Datum error_fmt_null_d;
		Datum committed_null_d;
		bytea *digest_ba;
		char *sqlstate_str;
		char *class_str;
		int i;

		for (i = 0; i < 13; i++)
			isnull[i] = false;
		digest_ba = DatumGetByteaPP(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc,
												  1,
												  &isnull[0]));
		sqlstate_str = text_to_cstring(DatumGetTextPP(SPI_getbinval(SPI_tuptable->vals[0],
																	SPI_tuptable->tupdesc,
																	2,
																	&isnull[1])));
		class_str = text_to_cstring(DatumGetTextPP(SPI_getbinval(SPI_tuptable->vals[0],
																SPI_tuptable->tupdesc,
																3,
																&isnull[2])));
		retryable_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull[3]);
		fmtver_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &isnull[4]);
		recorded_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull[5]);
		sqlstate_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7, &isnull[6]);
		term_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 8, &isnull[7]);
		result_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 9, &isnull[8]);
		error_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 10, &isnull[9]);
		result_fmt_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 11, &isnull[10]);
		error_fmt_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 12, &isnull[11]);
		committed_null_d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 13, &isnull[12]);
		if (isnull[0] || isnull[1] || isnull[2] || isnull[3] || isnull[4] ||
			isnull[5] || isnull[6] || isnull[7] || isnull[8] || isnull[9] ||
			isnull[10] || isnull[11] || isnull[12] ||
			VARSIZE_ANY_EXHDR(digest_ba) != BCDB_RAFT_DIGEST_BYTES ||
			memcmp(VARDATA_ANY(digest_ba), failure->digest, BCDB_RAFT_DIGEST_BYTES) != 0 ||
			strncmp(sqlstate_str, failure->sqlstate, 5) != 0 ||
			strcmp(class_str, failure->failure_class) != 0 ||
			DatumGetBool(retryable_d) != failure->retryable ||
			DatumGetInt32(fmtver_d) != failure->format_version ||
			!DatumGetBool(recorded_d) ||
			!DatumGetBool(sqlstate_null_d) ||
			!DatumGetBool(term_null_d) ||
			!DatumGetBool(result_null_d) ||
			!DatumGetBool(error_null_d) ||
			!DatumGetBool(result_fmt_null_d) ||
			!DatumGetBool(error_fmt_null_d) ||
			!DatumGetBool(committed_null_d))
		{
			ledger_spi_end(&spi_scope);
			elog(ERROR, "raft_apply_ledger: state=4 verification mismatch");
		}
	}

	if (stored_failure)
		*stored_failure = *failure;
	memcpy(tx->raft_terminal_digest, failure->digest, BCDB_RAFT_DIGEST_BYTES);
	tx->raft_terminal_format_version = failure->format_version;
	tx->raft_terminal_state = RAFT_ITEM_STATE_NONTERMINAL_FAILURE;
	tx->raft_terminal_update_confirmed = true;
	tx->raft_terminal_returning_verified = true;
	tx->raft_terminal_verified_top_xid = GetTopTransactionIdIfAny();
	tx->raft_terminal_verified_nest_level = GetCurrentTransactionNestLevel();

	ledger_spi_end(&spi_scope);
	bcdb_emit_ledger_boundary("ledger_finalize_nonterminal_failure");
	return true;
}

/* --------------------------------------------------------------------------
 * D5: bcdb_raft_ledger_assert_terminal
 * -------------------------------------------------------------------------- */

void
bcdb_raft_ledger_assert_terminal(BCDBShmXact *tx)
{
	TransactionId current_top_xid;
	int           current_nest_level;

	if (!tx || !tx->raft_ledger_enabled)
		return;

	if (tx->raft_terminal_state != RAFT_ITEM_STATE_APPLIED_OK &&
		tx->raft_terminal_state != RAFT_ITEM_STATE_APPLIED_ERROR)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit with nonterminal "
			 "local state=%d for log_index=%llu ordinal=%u",
			 tx->raft_terminal_state,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (!tx->raft_terminal_update_confirmed)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit without confirmed "
			 "terminal ledger update for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (!tx->raft_terminal_returning_verified)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit without verified "
			 "terminal RETURNING row for log_index=%llu ordinal=%u",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	current_top_xid = GetTopTransactionIdIfAny();
	current_nest_level = GetCurrentTransactionNestLevel();

	if (tx->raft_terminal_verified_top_xid != current_top_xid)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit with mismatched "
			 "top xid=%u verified_top_xid=%u for log_index=%llu ordinal=%u",
			 (unsigned) current_top_xid,
			 (unsigned) tx->raft_terminal_verified_top_xid,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (tx->raft_terminal_verified_nest_level != current_nest_level)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit with mismatched "
			 "nest level=%d verified_nest_level=%d for log_index=%llu ordinal=%u",
			 current_nest_level,
			 tx->raft_terminal_verified_nest_level,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	if (current_nest_level != 1)
		elog(ERROR,
			 "raft_apply_ledger: refusing top-level commit outside nest level 1 "
			 "nest_level=%d for log_index=%llu ordinal=%u",
			 current_nest_level,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal);

	{
		char epoch_hex[65];
		int i;
		const char *cluster_id = getenv("ARIABC_RAFT_CLUSTER_ID");
		int node_id = 0;
		const char *node_id_env = getenv("ARIABC_RAFT_NODE_ID");
		if (node_id_env != NULL && node_id_env[0] != '\0')
			node_id = atoi(node_id_env);
		if (cluster_id == NULL || cluster_id[0] == '\0')
			cluster_id = "unknown_cluster";
		for (i = 0; i < 32; i++)
			sprintf(&epoch_hex[i * 2], "%02x", (unsigned char) tx->raft_epoch_id[i]);
		epoch_hex[64] = '\0';

		elog(LOG,
			 "SAFE_TERMINAL_ASSERT_OK pid=%d log=%llu ord=%u state=%d epoch=%s cluster_id=%s node_id=%d top_xid=%u nest_level=%d",
			 MyProcPid,
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 (int) tx->raft_terminal_state,
			 epoch_hex,
			 cluster_id,
			 node_id,
			 (unsigned) current_top_xid,
			 current_nest_level);
	}

	bcdb_emit_ledger_boundary("ledger_terminal_asserted");
}



/* --------------------------------------------------------------------------
 * E1: bcdb_complete_replayed_item / bcdb_finish_terminal_item
 * --------------------------------------------------------------------------
 *
 * P0-E: Terminal payload contract (version 1):
 *
 *   - Terminal payload is UTF-8 text only (no embedded NUL bytes allowed).
 *   - Maximum payload size = result-ring slot capacity minus envelope overhead.
 *     The exact ring slot size is sizeof(block->result[slot]) — never a
 *     hard-coded constant — so that ring resizing automatically adjusts the
 *     documented limit.
 *   - If the formatted envelope exceeds the slot capacity, the call fails
 *     with a deterministic error BEFORE any ledger finalization occurs.
 *     Silent truncation is never permitted.
 *   - The terminal_format_version stored in the ledger row is propagated into
 *     the result-ring envelope; it is NOT hard-coded to 1.
 */

void
bcdb_complete_replayed_item(BCDBShmXact *tx,
							 const char  *stored_terminal,
							 bool         is_error,
							 TransactionId replay_xid)
{
	bcdb_finish_terminal_item(tx, stored_terminal, is_error, true, replay_xid);
}

void
bcdb_finish_terminal_item(BCDBShmXact *tx,
						  const char  *terminal_payload,
						  bool         is_error,
						  bool         is_replay,
						  TransactionId committed_xid)
{
	BCBlock *result_block = NULL;
	BCBlock *committed_block = NULL;
	int      mem_txid;
	int      slots;
	const char *payload = terminal_payload ? terminal_payload : "";
	char digest_hex[BCDB_RAFT_DIGEST_BYTES * 2 + 1];
	char *allocated_payload = NULL;

	if (!tx)
		return;

	if (tx->raft_ledger_enabled)
	{
		int i;
		int formatted_len;

		/*
		 * Determine the exact result-ring slot capacity.  Use the actual
		 * slot size, not a hard-coded constant, so the limit automatically
		 * adjusts if the ring definition changes.
		 *
		 * We need to obtain this before allocation so we can size-check
		 * before writing to the ring.
		 */
		/*
		 * Completion payloads are consumed by bcdb_block_submit_results(),
		 * whose contract is to read from the sentinel result ring (block 1).
		 * The submitted block id may be a transient workload block; writing
		 * there leaves the submitter waiting forever on the sentinel ring.
		 */
		result_block = bcdb_result_ring_owner_block();
		slots = bcdb_get_runtime_result_ring_slots();
		if (slots < 1) slots = 1;
		mem_txid = (int)(tx->tx_id % (BCTxID) slots);
		if (mem_txid < 0) mem_txid += slots;

		/*
		 * Compute the slot capacity.  If we cannot find the block, fall back
		 * to a safe sentinel.  The actual write below also guards on block != NULL.
		 */
		size_t slot_capacity = (result_block != NULL)
			? sizeof(result_block->result[mem_txid])
			: 1024; /* conservative sentinel; write below is guarded */

		/* Build the hex digest for the envelope */
		for (i = 0; i < BCDB_RAFT_DIGEST_BYTES; i++)
		{
			sprintf(digest_hex + (i * 2), "%02x", tx->raft_terminal_digest[i]);
		}
		digest_hex[BCDB_RAFT_DIGEST_BYTES * 2] = '\0';

		/*
		 * Allocate a scratch buffer sized to the full ring slot so we can
		 * detect overflow before committing anything.
		 */
		allocated_payload = (char *) palloc(slot_capacity);

		/*
		 * P0-E: Propagate the actual stored terminal_format_version.
		 * Do NOT hard-code format version as 1 once multiple versions exist.
		 */
		formatted_len = snprintf(allocated_payload, slot_capacity,
				 "[BCDB_RAFT_COMMIT_CONFIRMED]\n"
				 "raft_log_index=%llu\n"
				 "raft_item_ordinal=%u\n"
				 "terminal_digest=%s\n"
				 "terminal_state=%s\n"
				 "terminal_format_version=%d\n"
				 "postgres_commit_confirmed=1\n"
				 "[PAYLOAD]\n%s",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal,
				 digest_hex,
				 is_error ? "ERROR" : "OK",
				 tx->raft_terminal_format_version,    /* actual stored version */
				 payload);

		/*
		 * P0-E: Fail deterministically if the envelope does not fit.
		 * snprintf returns the number of bytes that *would* be written,
		 * so a return value >= slot_capacity means the output was truncated.
		 * We must NOT silently truncate — that would break digest verification.
		 */
		if (formatted_len < 0 || (size_t) formatted_len >= slot_capacity)
		{
			pfree(allocated_payload);
			elog(ERROR,
				 "raft_apply_ledger: terminal envelope for log_index=%llu "
				 "ordinal=%u is %d bytes, which exceeds ring slot capacity of "
				 "%zu bytes (terminal payload too large for version-%d format)",
				 (unsigned long long) tx->raft_log_index,
				 (unsigned) tx->raft_item_ordinal,
				 formatted_len,
				 slot_capacity,
				 tx->raft_terminal_format_version);
		}

		payload = allocated_payload;
	}

	result_block = bcdb_result_ring_owner_block();
	if (tx->block_id_committed != BCDBMaxBid)
		committed_block = get_block_by_id(tx->block_id_committed, false);

	if (tx->raft_ledger_enabled)
	{
		const char *debug = getenv("ARIABC_SAFE_FINISH_ROUTE_DEBUG");

		if (debug != NULL && debug[0] != '\0')
			elog(LOG,
				 "SAFE_FINISH_ROUTE tx=%d result_block=%d committed_block=%d",
				 (int) tx->tx_id,
				 result_block ? (int) result_block->id : -1,
				 committed_block ? (int) committed_block->id : -1);
	}

	if (result_block != NULL)
	{
		slots = bcdb_get_runtime_result_ring_slots();
		if (slots < 1) slots = 1;
		mem_txid = (int)(tx->tx_id % (BCTxID) slots);
		if (mem_txid < 0) mem_txid += slots;

		elog(LOG,
			 "SAFE_RING_WRITE log=%llu ord=%u tx=%d slot=%d payload_len=%zu",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 (int) tx->tx_id,
			 mem_txid,
			 strlen(payload));
		strlcpy(result_block->result[mem_txid], payload,
				sizeof(result_block->result[mem_txid]));

		pg_write_barrier();
		__atomic_store_n(&result_block->result_commit_xid[mem_txid],
						 committed_xid, __ATOMIC_RELEASE);
		__atomic_store_n(&result_block->result_committed_txid[mem_txid],
						 tx->tx_id, __ATOMIC_RELEASE);

		if (tx->block_id_committed == BCDBMaxBid)
			__atomic_store_n(&result_block->result_consumed_txid[mem_txid],
							 (int32)tx->tx_id, __ATOMIC_RELEASE);
	}

	if (allocated_payload != NULL)
	{
		pfree(allocated_payload);
	}

	/*
	 * Safe-ledger replay publishes a terminal result without running the
	 * normal write-set publish path in worker.c.  Keep the Lever-D publish
	 * gate in sync with the committed/replayed prefix; otherwise the first
	 * live transaction after restart can wait forever on published_max=-1
	 * even though last_committed_tx_id has already advanced through the
	 * replayed prefix.
	 */
	if (tx->raft_ledger_enabled)
		mark_published_ready_txid(tx);

	/* Advance commit watermark */
	if (bcdb_serial_gate_source == BCDB_GATE_SRC_LAST_COMMITTED)
		set_last_committed_txid(tx);
	else if (bcdb_advance_commit_watermark)
		advance_last_committed_txid(tx);
	else
	{
		bcdb_wait_for_prev_committed(tx);
		set_last_committed_txid(tx);
	}

	/* Check committed workload block finish condition. */
	if (committed_block != NULL)
	{
		int32 num_finished =
			__sync_add_and_fetch(&committed_block->num_finished, 1);

		if (num_finished == committed_block->num_tx)
		{
			uint32 global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);
			ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
			block_cleaning_dt(committed_block->id);
		}
	}

	if (result_block != NULL)
		ConditionVariableBroadcast(&result_block->condCommit);

	bcdb_emit_ledger_boundary(is_replay ? "ledger_replay_complete" : "ledger_finalize");

	elog(LOG,
		 "raft_apply_ledger: finished item log_index=%llu ordinal=%u "
		 "is_error=%d is_replay=%d fmtver=%d",
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 (int) is_error,
		 (int) is_replay,
		 tx->raft_terminal_format_version);
}

void
bcdb_complete_nonterminal_failure_item(BCDBShmXact *tx,
									   const BCDBNonterminalFailure *failure,
									   bool is_replay)
{
	BCBlock *result_block = NULL;
	BCBlock *committed_block = NULL;
	int      mem_txid;
	int      slots;
	char    *allocated_payload = NULL;
	size_t   slot_capacity;
	int      formatted_len;
	char     digest_hex[BCDB_RAFT_DIGEST_BYTES * 2 + 1];

	if (!tx || failure == NULL)
		return;
	if (failure->failure_class[0] == '\0' ||
		strnlen(failure->failure_class, BCDB_FAILURE_CLASS_MAX) >= BCDB_FAILURE_CLASS_MAX)
		elog(ERROR, "raft_apply_ledger: invalid nonterminal failure class payload");

	result_block = bcdb_result_ring_owner_block();
	if (tx->block_id_committed != BCDBMaxBid)
		committed_block = get_block_by_id(tx->block_id_committed, false);

	slots = bcdb_get_runtime_result_ring_slots();
	if (slots < 1) slots = 1;
	mem_txid = (int)(tx->tx_id % (BCTxID) slots);
	if (mem_txid < 0) mem_txid += slots;

	slot_capacity = (result_block != NULL)
		? sizeof(result_block->result[mem_txid])
		: 1024;

	allocated_payload = (char *) palloc(slot_capacity);
	digest_to_hex(failure->digest, digest_hex);

	formatted_len = snprintf(allocated_payload, slot_capacity,
			 "[BCDB_RAFT_FAILURE_NOTICE]\n"
			 "raft_log_index=%llu\n"
			 "raft_item_ordinal=%u\n"
			 "failure_digest=%s\n"
			 "outcome_state=NONTERMINAL_FAILURE\n"
			 "failure_format_version=%d\n"
			 "failure_notice_committed=1\n"
			 "postgres_commit_confirmed=0\n"
			 "[PAYLOAD]\n"
			 "sqlstate=%s\n"
			 "failure_class=%s\n"
			 "retryable=%d\n",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 digest_hex,
			 failure->format_version,
			 failure->sqlstate,
			 failure->failure_class[0] ? failure->failure_class : "UNKNOWN",
			 failure->retryable ? 1 : 0);

	if (formatted_len < 0 || (size_t) formatted_len >= slot_capacity)
	{
		pfree(allocated_payload);
		elog(ERROR,
			 "raft_apply_ledger: non-terminal failure envelope for log_index=%llu "
			 "ordinal=%u is %d bytes, which exceeds ring slot capacity of %zu bytes",
			 (unsigned long long) tx->raft_log_index,
			 (unsigned) tx->raft_item_ordinal,
			 formatted_len,
			 slot_capacity);
	}

	elog(LOG,
		 "SAFE_NONTERMINAL_FAILURE_ENVELOPE_BUILT log=%llu ord=%u bytes=%d",
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 formatted_len);

	if (result_block != NULL)
	{
		strlcpy(result_block->result[mem_txid], allocated_payload,
				sizeof(result_block->result[mem_txid]));

		pg_write_barrier();
		__atomic_store_n(&result_block->result_commit_xid[mem_txid],
						 InvalidTransactionId, __ATOMIC_RELEASE);
		__atomic_store_n(&result_block->result_committed_txid[mem_txid],
						 tx->tx_id, __ATOMIC_RELEASE);

		if (tx->block_id_committed == BCDBMaxBid)
			__atomic_store_n(&result_block->result_consumed_txid[mem_txid],
							 (int32)tx->tx_id, __ATOMIC_RELEASE);
	}

	pfree(allocated_payload);

	if (tx->raft_ledger_enabled)
		mark_published_ready_txid(tx);

	if (bcdb_serial_gate_source == BCDB_GATE_SRC_LAST_COMMITTED)
		set_last_committed_txid(tx);
	else if (bcdb_advance_commit_watermark)
		advance_last_committed_txid(tx);
	else
	{
		bcdb_wait_for_prev_committed(tx);
		set_last_committed_txid(tx);
	}

	if (committed_block != NULL)
	{
		int32 num_finished =
			__sync_add_and_fetch(&committed_block->num_finished, 1);

		if (num_finished == committed_block->num_tx)
		{
			uint32 global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);
			ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
			block_cleaning_dt(committed_block->id);
		}
	}

	if (result_block != NULL)
		ConditionVariableBroadcast(&result_block->condCommit);

	bcdb_emit_ledger_boundary(is_replay ? "ledger_nonterminal_replay_complete" : "ledger_finalize_nonterminal");

	elog(LOG,
		 "raft_apply_ledger: published non-terminal failure log_index=%llu ordinal=%u sqlstate=%s",
		 (unsigned long long) tx->raft_log_index,
		 (unsigned) tx->raft_item_ordinal,
		 failure->sqlstate);
}
