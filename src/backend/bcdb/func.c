//
// Created by Chris Liu on 2/6/2020.
//

#include "bcdb/func.h"
#include "bcdb/middleware.h"
#include "bcdb/worker.h"
#include "bcdb/shm_block.h"
#include "stdio.h"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include "utils/builtins.h"

static EVP_PKEY *create_public_key_from_pem(const char *public_key_pem);
static bool verify_rsa_sha256_signature(EVP_PKEY *public_key,
										const unsigned char *signature,
										size_t signature_len,
										const char *message,
										size_t message_len);
static bool base64_decode(const char *base64,
						  unsigned char **out,
						  size_t *out_len);
static bool verify_signature(const char *public_key_pem,
							 const char *message,
							 const char *signature_base64);
static bool calc_base64_decoded_length(const char *base64,
									   size_t *decoded_len);

/*
 * Convert the SQL-supplied public key text into an OpenSSL key object.
 *
 * bcdb_verify() accepts a PEM public key, for example:
 *
 *     -----BEGIN PUBLIC KEY-----
 *     ...
 *     -----END PUBLIC KEY-----
 *
 * BIO_new_mem_buf() lets OpenSSL read that in-memory string as if it were a
 * file.  PEM_read_bio_PUBKEY() returns the high-level EVP_PKEY wrapper, which
 * keeps this helper independent of low-level RSA structs.  The returned key is
 * owned by the caller and must be released with EVP_PKEY_free().
 */
static EVP_PKEY *
create_public_key_from_pem(const char *public_key_pem)
{
	BIO *key_bio = NULL;
	EVP_PKEY *public_key = NULL;

	if (public_key_pem == NULL)
		return NULL;

	/* -1 tells OpenSSL to compute the NUL-terminated PEM string length. */
	key_bio = BIO_new_mem_buf(public_key_pem, -1);
	if (key_bio == NULL)
		return NULL;

	public_key = PEM_read_bio_PUBKEY(key_bio, NULL, NULL, NULL);
	BIO_free(key_bio);

	return public_key;
}

/*
 * Verify that the decoded signature bytes match the exact message bytes.
 *
 * EVP_DigestVerify* is the high-level OpenSSL API for signature verification:
 *
 *     Init   -> bind together SHA-256, the public key, and verify mode.
 *     Update -> feed the exact message bytes into the digest operation.
 *     Final  -> compare the supplied signature against the computed digest.
 *
 * EVP_DigestVerifyFinal() returns:
 *
 *     1  signature is valid
 *     0  signature is well-formed but does not match this message/key
 *    <0  OpenSSL/runtime error
 *
 * SQL callers only need the final boolean answer, so this helper returns false
 * for both "invalid signature" and "verification could not be performed".
 */
static bool
verify_rsa_sha256_signature(EVP_PKEY *public_key,
							const unsigned char *signature,
							size_t signature_len,
							const char *message,
							size_t message_len)
{
	EVP_MD_CTX *ctx = NULL;
	bool is_authentic = false;
	int verify_status;

	if (public_key == NULL || signature == NULL || message == NULL)
		return false;

	ctx = EVP_MD_CTX_new();
	if (ctx == NULL)
		return false;

	/*
	 * The ctx object owns the transient digest/verify state only.  It borrows
	 * public_key; verify_signature() still owns and frees the EVP_PKEY.
	 */
	if (EVP_DigestVerifyInit(ctx, NULL, EVP_sha256(), NULL, public_key) <= 0)
		goto cleanup;

	if (EVP_DigestVerifyUpdate(ctx, message, message_len) <= 0)
		goto cleanup;

	verify_status = EVP_DigestVerifyFinal(ctx, signature, signature_len);
	if (verify_status == 1)
		is_authentic = true;

cleanup:
	EVP_MD_CTX_free(ctx);
	return is_authentic;
}

/*
 * Compute the maximum exact byte length expected after Base64 decoding.
 *
 * A Base64 input has four encoded characters for every three decoded bytes.
 * Padding with '=' subtracts one or two decoded bytes from the final group:
 *
 *     no padding:  "QUJD" -> 3 bytes
 *     one '=':     "QUI=" -> 2 bytes
 *     two '=':     "QQ==" -> 1 byte
 *
 * This verifier currently expects compact one-line signatures copied into SQL,
 * so malformed lengths are rejected before the BIO decoder gets involved.
 */
static bool
calc_base64_decoded_length(const char *base64, size_t *decoded_len)
{
	size_t len;
	size_t padding = 0;

	if (base64 == NULL || decoded_len == NULL)
		return false;

	len = strlen(base64);
	if (len < 2 || len % 4 != 0)
		return false;

	if (base64[len - 1] == '=' && base64[len - 2] == '=')
		padding = 2;
	else if (base64[len - 1] == '=')
		padding = 1;

	*decoded_len = (len * 3) / 4 - padding;
	return true;
}

/*
 * Decode a one-line Base64 string into raw signature bytes.
 *
 * Ownership contract:
 *
 *     success: returns true, *out points to malloc-owned bytes, *out_len set
 *     failure: returns false, *out is NULL, *out_len is 0
 *
 * BIO_FLAGS_BASE64_NO_NL intentionally requires the signature to be one line.
 * That matches the bcdb_verify(text,text,text) usage where the signature is a
 * single SQL text argument.  If callers later need multiline Base64, this flag
 * and the length validation should be revisited together.
 */
static bool
base64_decode(const char *base64, unsigned char **out, size_t *out_len)
{
	BIO *bio = NULL;
	BIO *b64 = NULL;
	size_t decoded_len;
	int bytes_read;

	if (out == NULL || out_len == NULL)
		return false;

	*out = NULL;
	*out_len = 0;

	if (!calc_base64_decoded_length(base64, &decoded_len))
		return false;

	*out = (unsigned char *) malloc(decoded_len + 1);
	if (*out == NULL)
		return false;

	/*
	 * The BIO chain is: base64 decoder filter -> memory buffer containing the
	 * encoded SQL argument.  BIO_free_all(bio) frees the whole chain after push.
	 */
	bio = BIO_new_mem_buf(base64, -1);
	b64 = BIO_new(BIO_f_base64());
	if (bio == NULL || b64 == NULL)
		goto error;

	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
	bio = BIO_push(b64, bio);
	b64 = NULL;

	/*
	 * Read no more than the decoded buffer can hold.  A different byte count
	 * means the input did not decode to the length its Base64 padding promised.
	 */
	bytes_read = BIO_read(bio, *out, decoded_len);
	if (bytes_read < 0 || (size_t) bytes_read != decoded_len)
		goto error;

	(*out)[decoded_len] = '\0';
	*out_len = (size_t) bytes_read;
	BIO_free_all(bio);
	return true;

error:
	if (bio != NULL)
		BIO_free_all(bio);
	else if (b64 != NULL)
		BIO_free(b64);

	free(*out);
	*out = NULL;
	*out_len = 0;
	return false;
}

/*
 * Complete bcdb_verify() implementation.
 *
 * Inputs are still plain C strings converted from PostgreSQL text Datums by
 * bcdb_verify().  This helper performs the actual cryptographic pipeline:
 *
 *     1. Parse the PEM public key into EVP_PKEY.
 *     2. Decode the Base64 signature text into raw bytes.
 *     3. Verify the exact message string with RSA/SHA-256.
 *     4. Free every OpenSSL/malloc allocation before returning.
 *
 * Any parse/decode/verify failure returns false instead of throwing.  That
 * keeps the SQL function easy to use in predicates and test queries:
 *
 *     SELECT bcdb_verify(pubkey, message, signature);
 */
static bool
verify_signature(const char *public_key_pem,
				 const char *message,
				 const char *signature_base64)
{
	EVP_PKEY *public_key = NULL;
	unsigned char *signature = NULL;
	size_t signature_len = 0;
	bool is_authentic = false;

	if (public_key_pem == NULL || message == NULL || signature_base64 == NULL)
		return false;

	public_key = create_public_key_from_pem(public_key_pem);
	if (public_key == NULL)
		return false;

	if (!base64_decode(signature_base64, &signature, &signature_len))
		goto cleanup;

	is_authentic = verify_rsa_sha256_signature(public_key,
											   signature,
											   signature_len,
											   message,
											   strlen(message));

cleanup:
	free(signature);
	EVP_PKEY_free(public_key);
	return is_authentic;
}

Datum
bcdb_verify(PG_FUNCTION_ARGS)
{

	char   *publicKey = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char   *plainText = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char   *signature = text_to_cstring(PG_GETARG_TEXT_PP(2));
	bool   ret = verify_signature(publicKey, plainText, signature);
	PG_RETURN_BOOL(ret);
}

/*
Datum
bcdb_dummy_block_commit(PG_FUNCTION_ARGS)
{
    char	   *file_path = PG_GETARG_CSTRING(0);
    int32      block_id = PG_GETARG_INT32(1);

    bcdb_middleware_dummy_block(file_path, block_id);

    PG_RETURN_BOOL(true);
}
*/

/*
Datum
bcdb_tx_file_submit(PG_FUNCTION_ARGS)
{
    char	   *file_path = PG_GETARG_CSTRING(0);

    bcdb_middleware_dummy_submit_tx(file_path);

    PG_RETURN_BOOL(true);
}
*/

/*
 * SQL entrypoint for the legacy one-transaction submit API.
 *
 * The middleware returns the assigned BCDB tx id.  Older callers may have
 * treated the return value as a generic success/snapshot value, so keep the SQL
 * signature unchanged while making the integer meaningful.
 */
Datum
bcdb_tx_submit(PG_FUNCTION_ARGS)
{
    char	   *bcdb_query = PG_GETARG_CSTRING(0);
    int32    snapshot;

    snapshot = bcdb_middleware_submit_tx(bcdb_query);

    PG_RETURN_INT32(snapshot);
}

/*
 * SQL entrypoint for legacy block submission.
 *
 * This waits through bcdb_middleware_submit_block(), but the SQL function only
 * reports boolean success.  New distributed execution uses
 * bcdb_block_submit_results() when it needs per-tx completion records.
 */
Datum
bcdb_block_submit(PG_FUNCTION_ARGS)
{
    char	   *bcdb_query = PG_GETARG_CSTRING(0);

    bcdb_middleware_submit_block(bcdb_query);
#if SAFEDBG
	printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    print_trace();
#endif

    PG_RETURN_BOOL(true);
}

/*
 * SQL entrypoint used by ariabc_pg's deterministic block path.
 *
 * The returned text is a newline-delimited hash/completion payload generated
 * after every result slot in the submitted block is known to belong to the
 * matching tx id.
 */
Datum
bcdb_block_submit_results(PG_FUNCTION_ARGS)
{
    char   *bcdb_query = PG_GETARG_CSTRING(0);
    char   *payload = bcdb_middleware_submit_block_results(bcdb_query);

    if (payload == NULL)
        PG_RETURN_TEXT_P(cstring_to_text(""));

    PG_RETURN_TEXT_P(cstring_to_text(payload));
}

/*
 * Attach a previously submitted transaction hash to a BCDB block.
 *
 * This exists for the older two-step SQL flow.  The middleware validates that
 * the hash exists and that the tx is not being moved between blocks.
 */
Datum
bcdb_add_tx_with_block_id(PG_FUNCTION_ARGS)
{
    char	   *tx_hash = PG_GETARG_CSTRING(0);
    int32      block_id = PG_GETARG_INT32(1);

    bcdb_middleware_set_txs_committed_block(tx_hash, block_id);

    PG_RETURN_BOOL(true);
 
}

/*
 * Compatibility wrapper for the historical commit-release API.
 *
 * Current deterministic workers do not wait on a block-level allow flag; the
 * middleware keeps this path explicit so callers do not crash on missing block
 * ids, but it is not part of the active distributed execution flow.
 */
Datum
bcdb_allow_txs_commit_by_block_id(PG_FUNCTION_ARGS)
{

    int32      block_id = PG_GETARG_INT32(0);

    bcdb_middleware_allow_txs_exec_write_set_and_commit_by_id(block_id);

    PG_RETURN_BOOL(true);

}

/*
 * Wait for a named legacy transaction and report whether it committed.
 *
 * Invalid hashes now raise SQL errors.  A real aborted tx returns false; a
 * committed tx returns true.
 */
Datum
bcdb_check_txs_result(PG_FUNCTION_ARGS)
{

    char	   *tx_hash = PG_GETARG_CSTRING(0);

    bcdb_wait_tx_finish(tx_hash);

    PG_RETURN_BOOL(bcdb_is_tx_commited(tx_hash));

}

/*
 * Historical wait helper.
 *
 * This does not wait for BCDB completion; it only sleeps briefly.  It is kept
 * for compatibility with older generated SQL scripts.
 */
Datum
bcdb_wait_to_finish(PG_FUNCTION_ARGS)
{
    usleep(10000);
    //bcdb_middleware_wait_all_to_finish();
    //set_last_committed_txid();
    //block_meta->num_committed = 0;
    PG_RETURN_BOOL(true);
}

/*
 * Historical status helper.
 *
 * With LOG_STATUS enabled this also triggers block cleaning and returns the
 * accumulated log buffer.  Without LOG_STATUS it returns an empty C string.
 */
Datum
bcdb_check_block_status(PG_FUNCTION_ARGS)
{

    //char            *ret;

#ifdef LOG_STATUS
    sleep(1); 
    for (int i=block_meta->global_bmin; i < block_meta->global_bmin + CLEANING_DELAY_BLOCKS; i++)
        block_cleaning(i);
    PG_RETURN_CSTRING(block_meta->log);
#endif
#ifndef LOG_STATUS
    PG_RETURN_CSTRING("\0");
#endif
}

/*
 * Report BCDB's committed progress counter.
 *
 * In deterministic mode this value is best read as a commit watermark, not as
 * an exact count of SQL transactions completed by the current call.
 */
Datum
bcdb_num_committed(PG_FUNCTION_ARGS)
{
    ereport(LOG, (errmsg("[ZL] num committed: %d", (int)block_meta->num_committed)));
#if SAFEDBG
    printf("\nariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
    printf("ariaMyDbg %s : %s: %d \n\n", __FILE__, __FUNCTION__, __LINE__ );
#endif
    PG_RETURN_INT32((int)block_meta->num_committed);
}

/*
 * Return the deterministic commit-order watermark.
 *
 * The sentinel block is created on demand so callers can query the watermark
 * before any deterministic transaction has completed.
 */
Datum
bcdb_last_committed_txid(PG_FUNCTION_ARGS)
{
    /*
     * Return the commit-order counter used by deterministic BCDB execution.
     *
     * IMPORTANT: Ensure the sentinel block (bid=1) exists so callers can use
     * this value even before any deterministic tx has run.
     */
    (void) get_block_by_id(1, true);
    PG_RETURN_INT32((int) get_last_committed_txid(NULL));
}

/*
 * Reset in-memory BCDB state.
 *
 * This clears BCDB shared-memory metadata via the middleware.  It does not
 * recreate SQL tables or reset external Kafka/Raft state; restore scripts handle
 * those layers separately.
 */
Datum
bcdb_reset(PG_FUNCTION_ARGS)
{
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
#endif
    bcdb_clear_block_txs_store();
    PG_RETURN_BOOL(true);
}

/*
 * Initialize BCDB worker queues from SQL.
 *
 * The second argument is historically called block_size, but the middleware
 * uses it as the requested worker/queue count for the queued BCDB path.
 */
Datum
bcdb_init(PG_FUNCTION_ARGS)
{
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
#endif

    bool is_oep_mode = PG_GETARG_BOOL(0);
    int32 block_size = PG_GETARG_INT32(1);
    bcdb_middleware_init(is_oep_mode, block_size);
    PG_RETURN_BOOL(true);
}

/*
 * Initialize BCDB worker queues plus legacy burst-submit settings.
 *
 * numTx/timeSlot only affect bcdb_middleware_submit_block2(); the active
 * distributed block-submit path does not use the burst sleep behavior.
 */
Datum
bcdb_init2(PG_FUNCTION_ARGS)
{
#if SAFEDBG
    printf("ariaMyDbg %s : %s: %d \n", __FILE__, __FUNCTION__, __LINE__ );
#endif

    bool is_oep_mode = PG_GETARG_BOOL(0);
    int32 block_size = PG_GETARG_INT32(1);
    int32 numTx = PG_GETARG_INT32(2);
    int32 timeSlot = PG_GETARG_INT32(3);
    bcdb_middleware_init2(is_oep_mode, block_size, numTx, timeSlot);
    PG_RETURN_BOOL(true);
}
