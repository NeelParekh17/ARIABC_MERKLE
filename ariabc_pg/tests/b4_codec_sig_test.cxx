// ariabc_pg/tests/b4_codec_sig_test.cxx
// Unit tests for B4 payload codec and signature verification.

#include <iostream>
#include <vector>
#include <atomic>
#include <string>
#include <stdexcept>
#include <cassert>
#include <cstring>
#include <cstdint>

#include <openssl/evp.h>

#include "ed25519_keys.hxx"
#include "result_envelope.hxx"

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "Requirement failed at line " << __LINE__ << ": " << #expr << std::endl; \
            throw std::runtime_error("requirement failed: " #expr); \
        } \
    } while (0)

namespace ariabc_pg {

struct kafka_reply_record {
    uint64_t req_num = 0;
    uint64_t raft_log_idx = 0;
    std::string req_id;
    int node_id = -1;
    int leader_node_id = -1;
    std::string result_hash;
    std::string hash_algo;
    std::string server_sig;
    uint64_t timestamp_ms = 0;
    std::string full_result;
    bool has_full_result = false;

    // B4 / Epoch fields
    uint32_t raft_item_ordinal = 0;
    std::string terminal_state;
    int format_version = 0;
    std::string epoch_hex;
    int wire_version = 3;

    // Must match ariabc_pg_gateway.cxx exactly.
    bool in_envelope = false;
    bool envelope_verified = false;
    uint32_t envelope_signer = 0;

    std::string result_adhash;
};

enum class result_sig_mode_t { HMAC = 0, ED25519 = 1 };
extern result_sig_mode_t g_result_sig_mode;
extern const ed25519_key_roster* g_result_node_keys;
extern std::atomic<uint64_t> g_result_env_verified;
extern std::atomic<uint64_t> g_result_env_unsealed;

// Functions to test
extern std::string test_build_bin_batch_payload_v2(const std::vector<std::string>& req_ids,
                                                   const std::vector<std::string>& results,
                                                   const std::vector<uint64_t>& raft_log_idxs,
                                                   const std::vector<int>& leader_node_hints,
                                                   const std::vector<std::string>& terminal_digests,
                                                   const std::vector<uint32_t>& raft_item_ordinals,
                                                   const std::vector<std::string>& terminal_states,
                                                   const std::vector<int>& format_versions,
                                                   uint16_t node_id,
                                                   const std::string& sig_key,
                                                   const std::string& raft_epoch_hex,
                                                   bool safe_ledger_mode,
                                                   const std::vector<std::string>& result_adhashes = {});

extern bool parse_kafka_payload_records(const std::string& payload,
                                        std::vector<kafka_reply_record>& out);

extern bool verify_result_signature(const kafka_reply_record& rec, const std::string& sig_key);

} // namespace ariabc_pg

using namespace ariabc_pg;

void test_b4_codec_success() {
    std::cout << "Running test_b4_codec_success..." << std::endl;

    std::vector<std::string> req_ids = {"req-1", "req-2"};
    std::vector<std::string> results = {"res1", "res2"};
    std::vector<uint64_t> raft_log_idxs = {101, 102};
    std::vector<int> leader_node_hints = {1, 2};
    std::vector<std::string> terminal_digests = {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    };
    std::vector<uint32_t> raft_item_ordinals = {0, 1};
    std::vector<std::string> terminal_states = {"OK", "ERROR"};
    std::vector<int> format_versions = {1, 1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    const std::vector<std::string> adhashes = {
        "0123456789abcdef0011223344556677",
        "fedcba98765432108899aabbccddeeff"
    };
    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, true, adhashes
    );

    REQUIRE(!payload.empty());
    REQUIRE(payload.size() > 8);
    REQUIRE(payload[0] == 'B');
    REQUIRE(payload[1] == '4');

    std::vector<kafka_reply_record> parsed;
    bool success = parse_kafka_payload_records(payload, parsed);
    REQUIRE(success);
    REQUIRE(parsed.size() == 2);

    REQUIRE(parsed[0].req_id == "req-1");
    REQUIRE(parsed[0].raft_log_idx == 101);
    REQUIRE(parsed[0].raft_item_ordinal == 0);
    REQUIRE(parsed[0].node_id == 1);
    REQUIRE(parsed[0].leader_node_id == 1);
    REQUIRE(parsed[0].epoch_hex == epoch_hex);
    REQUIRE(parsed[0].result_adhash == adhashes[0]);
    REQUIRE(parsed[0].result_hash.empty());  // no SHA-256 of the result text
    REQUIRE(parsed[0].terminal_state == "OK");
    REQUIRE(parsed[0].format_version == 1);

    REQUIRE(parsed[1].req_id == "req-2");
    REQUIRE(parsed[1].raft_log_idx == 102);
    REQUIRE(parsed[1].raft_item_ordinal == 1);
    REQUIRE(parsed[1].node_id == 1);
    REQUIRE(parsed[1].leader_node_id == 2);
    REQUIRE(parsed[1].epoch_hex == epoch_hex);
    REQUIRE(parsed[1].result_adhash == adhashes[1]);
    REQUIRE(parsed[1].terminal_state == "ERROR");
    REQUIRE(parsed[1].format_version == 1);

    // Verify signatures
    REQUIRE(verify_result_signature(parsed[0], sig_key));
    REQUIRE(verify_result_signature(parsed[1], sig_key));

    // Verify signature verification fails with incorrect key
    REQUIRE(!verify_result_signature(parsed[0], "wrong_key"));

    // Verify signature verification fails with modified field
    parsed[0].terminal_state = "ERROR";
    REQUIRE(!verify_result_signature(parsed[0], sig_key));

    std::cout << "test_b4_codec_success passed." << std::endl;
}

// The write and read AdHashes travel as 16 raw bytes (B4: after the epoch,
// B3: after the fixed part), round-trip as 32 hex chars, are covered by the
// HMAC, and default to zero when absent.  No SHA-256 of the result is sent.
void test_result_adhash_wire() {
    std::cout << "Running test_result_adhash_wire..." << std::endl;
    const std::string digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    const std::string sig_key = "my_secret_key";
    const std::string zero(32, '0');
    const std::string a0 = "00112233445566ff8899aabbccddeeff";

    for (const bool safe : {true, false}) {
        const std::string payload = test_build_bin_batch_payload_v2(
            {"req-1", "req-2"}, {"DELETE 1", "DELETE 1"}, {101, 102}, {1, 1},
            {digest, digest}, {0, 1}, {"OK", "ERROR"}, {1, 1}, 1, sig_key, epoch_hex, safe,
            {a0, ""});
        REQUIRE(payload[1] == (safe ? '4' : '3'));
        std::vector<kafka_reply_record> parsed;
        REQUIRE(parse_kafka_payload_records(payload, parsed));
        REQUIRE(parsed.size() == 2);
        REQUIRE(parsed[0].result_adhash == a0);
        REQUIRE(parsed[0].result_hash.empty());
        REQUIRE(parsed[1].result_adhash == zero);
        REQUIRE(parsed[0].terminal_state == "OK");
        REQUIRE(parsed[1].terminal_state == "ERROR");
        REQUIRE(parsed[0].full_result == "DELETE 1");
        REQUIRE(verify_result_signature(parsed[0], sig_key));
        REQUIRE(verify_result_signature(parsed[1], sig_key));

        // The signature binds the write AdHash, the read AdHash and the state.
        kafka_reply_record t = parsed[0];
        t.result_adhash = "00112233445566fe8899aabbccddeeff";
        REQUIRE(!verify_result_signature(t, sig_key));
        t = parsed[0];
        t.result_adhash = "00112233445566ff8899aabbccddeefe";
        REQUIRE(!verify_result_signature(t, sig_key));
        t = parsed[0];
        t.terminal_state = "ERROR";
        REQUIRE(!verify_result_signature(t, sig_key));

        // B4: 47 fixed + 32 epoch + 16 AdHash; B3: 38 fixed + 16 AdHash;
        // then req_id (5) + 64-hex HMAC + result (8).
        const size_t rec = (safe ? 47 + 32 : 38) + 16 + 5 + 64 + 8;
        REQUIRE(payload.size() == 8 + 2 * rec);

        // Every proper prefix fails to parse.
        for (size_t len = 8; len < payload.size(); ++len) {
            std::vector<kafka_reply_record> cut;
            REQUIRE(!parse_kafka_payload_records(payload.substr(0, len), cut));
        }
    }
    std::cout << "test_result_adhash_wire passed." << std::endl;
}

void test_b4_codec_truncation() {
    std::cout << "Running test_b4_codec_truncation..." << std::endl;

    std::vector<std::string> req_ids = {"req-1"};
    std::vector<std::string> results = {"res1"};
    std::vector<uint64_t> raft_log_idxs = {101};
    std::vector<int> leader_node_hints = {1};
    std::vector<std::string> terminal_digests = {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    };
    std::vector<uint32_t> raft_item_ordinals = {0};
    std::vector<std::string> terminal_states = {"OK"};
    std::vector<int> format_versions = {1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, true
    );

    // Truncate payload byte by byte and make sure parse fails
    for (size_t len = 0; len < payload.size(); ++len) {
        std::string truncated = payload.substr(0, len);
        std::vector<kafka_reply_record> parsed;
        bool success = parse_kafka_payload_records(truncated, parsed);
        REQUIRE(!success);
    }

    std::cout << "test_b4_codec_truncation passed." << std::endl;
}

void test_b4_codec_invalid_inputs() {
    std::cout << "Running test_b4_codec_invalid_inputs..." << std::endl;

    std::vector<std::string> req_ids = {"req-1"};
    std::vector<std::string> results = {"res1"};
    std::vector<uint64_t> raft_log_idxs = {101};
    std::vector<int> leader_node_hints = {1};
    std::vector<std::string> terminal_digests = {"invalid_digest"};
    std::vector<uint32_t> raft_item_ordinals = {0};
    std::vector<std::string> terminal_states = {"OK"};
    std::vector<int> format_versions = {1};
    std::string epoch_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    std::string sig_key = "my_secret_key";

    // With safe_ledger_mode = false the digest is ignored (B3 carries no
    // digest) and a missing AdHash is sent as zero.
    std::string payload = test_build_bin_batch_payload_v2(
        req_ids, results, raft_log_idxs, leader_node_hints,
        terminal_digests, raft_item_ordinals, terminal_states,
        format_versions, 1, sig_key, epoch_hex, false
    );
    REQUIRE(!payload.empty());
    std::vector<kafka_reply_record> parsed;
    REQUIRE(parse_kafka_payload_records(payload, parsed));
    REQUIRE(parsed.size() == 1);
    REQUIRE(parsed[0].result_adhash == std::string(32, '0'));
    REQUIRE(parsed[0].result_hash.empty());

    std::cout << "test_b4_codec_invalid_inputs passed." << std::endl;
}

namespace ariabc_pg {
    bool test_vote_store_b4_logic();
}

namespace {

std::shared_ptr<ariabc_pg::ed25519_private_key> gen_private_key(const std::string& path) {
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr);
    EVP_PKEY* pkey = nullptr;
    REQUIRE(pctx && EVP_PKEY_keygen_init(pctx) == 1 && EVP_PKEY_keygen(pctx, &pkey) == 1);
    EVP_PKEY_CTX_free(pctx);
    FILE* f = std::fopen(path.c_str(), "w");
    REQUIRE(f);
    REQUIRE(PEM_write_PrivateKey(f, pkey, nullptr, nullptr, 0, nullptr, nullptr) == 1);
    std::fclose(f);
    EVP_PKEY_free(pkey);
    std::shared_ptr<ariabc_pg::ed25519_private_key> k(new ariabc_pg::ed25519_private_key());
    std::string err;
    REQUIRE(k->load_pem_file(path, err));
    std::remove(path.c_str());
    return k;
}

std::shared_ptr<ariabc_pg::ed25519_public_key> public_of(const ariabc_pg::ed25519_private_key& k) {
    uint8_t raw[ariabc_pg::kEd25519PubBytes];
    REQUIRE(k.raw_public_key(raw));
    std::shared_ptr<ariabc_pg::ed25519_public_key> p(new ariabc_pg::ed25519_public_key());
    std::string err;
    REQUIRE(p->set_raw(raw, err));
    return p;
}

std::string b4_payload_for_node(uint16_t node_id) {
    return test_build_bin_batch_payload_v2(
        {"req-1", "req-2"}, {"res1", "res2"}, {101, 102}, {1, 1},
        {"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
         "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},
        {0, 1}, {"OK", "OK"}, {1, 1}, node_id, /*sig_key=*/"",
        "1111111111111111111111111111111111111111111111111111111111111111", true);
}

} // namespace

// Gateway vote path in Ed25519 mode: a record counts only when the replica
// whose node_id it carries signed the enclosing message.
void test_ed25519_result_votes() {
    std::cout << "Running test_ed25519_result_votes..." << std::endl;
    auto k1 = gen_private_key("b4_test_node_1.key.pem");
    auto k2 = gen_private_key("b4_test_node_2.key.pem");
    ariabc_pg::ed25519_key_roster roster;
    roster.add(1, public_of(*k1));
    roster.add(2, public_of(*k2));

    const std::string p1 = b4_payload_for_node(1);
    std::string sealed1, forged, sealed_hmac_mode;
    REQUIRE(ariabc_pg::seal_result_envelope(*k1, 1, p1, sealed1));
    // Node 2 publishes node 1's records under its own (valid) signature.
    REQUIRE(ariabc_pg::seal_result_envelope(*k2, 2, p1, forged));

    g_result_node_keys = &roster;
    g_result_sig_mode = result_sig_mode_t::ED25519;

    std::vector<kafka_reply_record> parsed;
    const uint64_t verified0 = g_result_env_verified.load();
    const uint64_t unsealed0 = g_result_env_unsealed.load();
    REQUIRE(parse_kafka_payload_records(sealed1, parsed));
    REQUIRE(parsed.size() == 2);
    // A sealed message counts once as verified and never as unsealed.
    REQUIRE(g_result_env_verified.load() == verified0 + 1);
    REQUIRE(g_result_env_unsealed.load() == unsealed0);
    for (const auto& r : parsed) {
        REQUIRE(r.in_envelope && r.envelope_verified && r.envelope_signer == 1);
        REQUIRE(r.server_sig.empty());  // no per-record HMAC in Ed25519 mode
        REQUIRE(verify_result_signature(r, "ignored"));
    }

    REQUIRE(parse_kafka_payload_records(forged, parsed));
    REQUIRE(parsed.size() == 2);
    for (const auto& r : parsed) {
        REQUIRE(r.envelope_verified && r.envelope_signer == 2 && r.node_id == 1);
        REQUIRE(!verify_result_signature(r, "ignored"));
    }

    std::string tampered = sealed1;
    tampered[20] ^= 0x01;
    REQUIRE(parse_kafka_payload_records(tampered, parsed));
    for (const auto& r : parsed) REQUIRE(!verify_result_signature(r, "ignored"));

    // An unsealed payload carries no Ed25519 signature: never a valid vote.
    REQUIRE(parse_kafka_payload_records(p1, parsed));
    for (const auto& r : parsed) REQUIRE(!verify_result_signature(r, "ignored"));

    // Truncated envelope is a parse failure.
    REQUIRE(!parse_kafka_payload_records(sealed1.substr(0, sealed1.size() - 3), parsed));

    // Gateway in legacy HMAC mode does not accept sealed records.
    g_result_sig_mode = result_sig_mode_t::HMAC;
    g_result_node_keys = nullptr;
    REQUIRE(parse_kafka_payload_records(sealed1, parsed));
    for (const auto& r : parsed) REQUIRE(!verify_result_signature(r, ""));
    std::cout << "test_ed25519_result_votes passed." << std::endl;
}

int main() {
    try {
        test_b4_codec_success();
        test_result_adhash_wire();
        test_b4_codec_truncation();
        test_b4_codec_invalid_inputs();
        test_ed25519_result_votes();
        if (!ariabc_pg::test_vote_store_b4_logic()) {
            std::cerr << "test_vote_store_b4_logic failed" << std::endl;
            return 1;
        }
        std::cout << "test_vote_store_b4_logic passed." << std::endl;
        std::cout << "ALL B4 codec & signature unit tests PASSED!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
