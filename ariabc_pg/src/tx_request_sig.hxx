#pragma once

/*
 * Per-client request signatures (Ed25519, see ed25519_keys.hxx).
 *
 * The gateway (client) signs every ordered request item with its own private
 * key.  The signature travels with the item on the gateway->replica frame and
 * inside the Raft log entry, and every replica verifies it in the state
 * machine, from the log bytes, before the item reaches the executor.  Replicas
 * hold only client PUBLIC keys, so a faulty replica (or leader) cannot create
 * a request that verifies: it can drop or reorder requests, but not forge or
 * alter one.
 *
 * Item tag layout (kTxTagBytes = 68 bytes):
 *   u32be client_id || 64-byte Ed25519 signature
 *
 * Signed message (length-prefixed, so no two (req_id, sql) pairs share bytes;
 * client_id is inside the signature, so a tag cannot be relabelled as another
 * client's):
 *   "ariabc.tx-sig.v1" || u32be(client_id) || u64be(len(req_id)) || req_id
 *                      || u64be(len(sql)) || sql
 *
 * The signature covers the SQL exactly as the gateway sent it, including any
 * gateway-assigned "s <seq>" prefix; leader-assigned rewriting happens after
 * verification.  Replay of an old, correctly signed item is NOT detected here.
 */

#include "ed25519_keys.hxx"
#include "wire_protocol.hxx"

#include <cstdint>
#include <cstring>
#include <string>

namespace ariabc_pg {

static_assert(kTxTagBytes == 4 + kEd25519SigBytes, "tx tag = u32 client id + Ed25519 signature");

inline const char* tx_sign_key_file_env() { return "ARIABC_TX_SIGN_KEY_FILE"; }

inline void tx_sig_put_u32_be(uint8_t out[4], uint32_t v) {
    out[0] = static_cast<uint8_t>(v >> 24);
    out[1] = static_cast<uint8_t>(v >> 16);
    out[2] = static_cast<uint8_t>(v >> 8);
    out[3] = static_cast<uint8_t>(v);
}

inline uint32_t tx_sig_get_u32_be(const uint8_t in[4]) {
    return (static_cast<uint32_t>(in[0]) << 24) | (static_cast<uint32_t>(in[1]) << 16) |
           (static_cast<uint32_t>(in[2]) << 8) | static_cast<uint32_t>(in[3]);
}

inline void tx_sig_append_u64_be(std::string& out, uint64_t v) {
    char b[8];
    for (int i = 7; i >= 0; --i) {
        b[i] = static_cast<char>(v & 0xffu);
        v >>= 8;
    }
    out.append(b, 8);
}

inline std::string tx_sig_message(uint32_t client_id, const std::string& req_id, const std::string& sql) {
    static const char kDomain[] = "ariabc.tx-sig.v1";
    std::string m;
    m.reserve(sizeof(kDomain) - 1 + 4 + 16 + req_id.size() + sql.size());
    m.append(kDomain, sizeof(kDomain) - 1);
    uint8_t id_be[4];
    tx_sig_put_u32_be(id_be, client_id);
    m.append(reinterpret_cast<const char*>(id_be), 4);
    tx_sig_append_u64_be(m, req_id.size());
    m.append(req_id);
    tx_sig_append_u64_be(m, sql.size());
    m.append(sql);
    return m;
}

inline uint32_t tx_tag_client_id(const uint8_t tag[kTxTagBytes]) { return tx_sig_get_u32_be(tag); }

// Explicit path, else $ARIABC_TX_SIGN_KEY_FILE, else <key dir>/client_<id>.key.pem.
inline std::string resolve_tx_sign_key_file(const std::string& explicit_path,
                                            const std::string& key_dir,
                                            uint32_t client_id) {
    if (!explicit_path.empty()) return explicit_path;
    const char* env = std::getenv(tx_sign_key_file_env());
    if (env && *env) return std::string(env);
    const std::string dir = resolve_key_dir(key_dir);
    if (dir.empty()) return std::string();
    const std::string p = private_key_path(dir, "client", client_id);
    return key_file_exists(p) ? p : std::string();
}

class tx_request_signer {
public:
    bool load(const std::string& key_file, uint32_t client_id, std::string& err) {
        client_id_ = client_id;
        return key_.load_pem_file(key_file, err);
    }

    bool is_enabled() const { return key_.loaded(); }
    uint32_t client_id() const { return client_id_; }
    const ed25519_private_key& key() const { return key_; }

    bool sign(const std::string& req_id, const std::string& sql, uint8_t out_tag[kTxTagBytes]) const {
        if (!key_.loaded()) return false;
        const std::string m = tx_sig_message(client_id_, req_id, sql);
        tx_sig_put_u32_be(out_tag, client_id_);
        return key_.sign(m.data(), m.size(), out_tag + 4);
    }

private:
    ed25519_private_key key_;
    uint32_t client_id_ = 0;
};

enum class tx_sig_check : uint8_t {
    OK = 0,
    MISSING = 1,
    UNKNOWN_CLIENT = 2,
    BAD_SIGNATURE = 3,
};

inline const char* tx_sig_check_name(tx_sig_check c) {
    switch (c) {
        case tx_sig_check::OK: return "ok";
        case tx_sig_check::MISSING: return "missing";
        case tx_sig_check::UNKNOWN_CLIENT: return "unknown_client";
        case tx_sig_check::BAD_SIGNATURE: return "mismatch";
    }
    return "?";
}

/*
 * Verifies items against the client public-key roster.  The outcome depends
 * only on the item bytes and the roster, so replicas with the same roster
 * reach the same decision for every log entry.
 */
class tx_request_verifier {
public:
    bool load_dir(const std::string& dir, std::string& err) {
        return roster_.load_dir(dir, "client", err);
    }

    void add_client(uint32_t id, std::shared_ptr<ed25519_public_key> key) {
        roster_.add(id, std::move(key));
    }

    const ed25519_key_roster& roster() const { return roster_; }

    tx_sig_check check(const client_api_request_item& item) const {
        if (!item.has_tx_tag) return tx_sig_check::MISSING;
        const uint32_t cid = tx_tag_client_id(item.tx_tag);
        const ed25519_public_key* pk = roster_.find(cid);
        if (!pk) return tx_sig_check::UNKNOWN_CLIENT;
        const std::string m = tx_sig_message(cid, item.req_id, item.sql);
        return pk->verify(m.data(), m.size(), item.tx_tag + 4) ? tx_sig_check::OK
                                                               : tx_sig_check::BAD_SIGNATURE;
    }

private:
    ed25519_key_roster roster_;
};

} // namespace ariabc_pg
