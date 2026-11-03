#pragma once

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>

#include <sys/stat.h>

#include "blake3.h"

namespace ariabc_pg {

/*
 * Request integrity tag: keyed BLAKE3 (a shared-key MAC) over one ordered
 * request item.  The gateway computes it, the tag travels with the item on the
 * gateway->replica frame and inside the Raft log entry, and every replica
 * recomputes it in the state machine before the item reaches the executor.
 *
 * Tagged message (length-prefixed, so no two (req_id, sql) pairs share bytes):
 *   "ariabc.tx-tag.v1" || u64be(len(req_id)) || req_id || u64be(len(sql)) || sql
 *
 * The key is loaded from a key file (64 hex chars = 32 bytes).  There is no
 * built-in key: without a configured key file, signing and verification are
 * unavailable and the caller decides whether that is fatal.
 */
struct blake3_sig_256 {
    uint8_t bytes[32];

    blake3_sig_256() {
        std::memset(bytes, 0, sizeof(bytes));
    }

    bool is_zero() const {
        for (size_t i = 0; i < 32; ++i) {
            if (bytes[i] != 0) return false;
        }
        return true;
    }

    // Constant-time: the comparison must not leak how many leading bytes match.
    bool operator==(const blake3_sig_256& o) const {
        uint8_t diff = 0;
        for (size_t i = 0; i < 32; ++i) diff |= static_cast<uint8_t>(bytes[i] ^ o.bytes[i]);
        return diff == 0;
    }

    bool operator!=(const blake3_sig_256& o) const {
        return !(*this == o);
    }

    std::string to_hex() const {
        static const char hex_digits[] = "0123456789abcdef";
        std::string s;
        s.resize(64);
        for (size_t i = 0; i < 32; ++i) {
            s[i * 2]     = hex_digits[(bytes[i] >> 4) & 0x0f];
            s[i * 2 + 1] = hex_digits[bytes[i] & 0x0f];
        }
        return s;
    }
};

inline const char* tx_tag_key_file_env() { return "ARIABC_TX_TAG_KEY_FILE"; }

// $HOME/.ariabc/tx_tag.key; empty when HOME is unset.
inline std::string default_tx_tag_key_file() {
    const char* home = std::getenv("HOME");
    if (!home || !*home) return std::string();
    return std::string(home) + "/.ariabc/tx_tag.key";
}

// Explicit path, else $ARIABC_TX_TAG_KEY_FILE, else the default path if it exists.
inline std::string resolve_tx_tag_key_file(const std::string& explicit_path) {
    if (!explicit_path.empty()) return explicit_path;
    const char* env = std::getenv(tx_tag_key_file_env());
    if (env && *env) return std::string(env);
    const std::string def = default_tx_tag_key_file();
    struct stat st;
    if (!def.empty() && ::stat(def.c_str(), &st) == 0) return def;
    return std::string();
}

class blake3_tx_signer {
public:
    blake3_tx_signer() {
        std::memset(key_, 0, sizeof(key_));
    }

    // Reads a key file holding exactly 64 hex characters (surrounding whitespace ignored).
    bool load_key_file(const std::string& path, std::string& err) {
        enabled_ = false;
        std::memset(key_, 0, sizeof(key_));
        std::ifstream in(path, std::ios::in | std::ios::binary);
        if (!in) {
            err = "cannot open tx tag key file " + path;
            return false;
        }
        std::stringstream ss;
        ss << in.rdbuf();
        std::string hex;
        for (char c : ss.str()) {
            if (!std::isspace(static_cast<unsigned char>(c))) hex.push_back(c);
        }
        if (hex.size() != 64) {
            err = "tx tag key file " + path + " must hold 64 hex characters (32 bytes)";
            return false;
        }
        for (size_t i = 0; i < 32; ++i) {
            const int hi = hex_nibble(hex[2 * i]);
            const int lo = hex_nibble(hex[2 * i + 1]);
            if (hi < 0 || lo < 0) {
                err = "tx tag key file " + path + " contains a non-hex character";
                std::memset(key_, 0, sizeof(key_));
                return false;
            }
            key_[i] = static_cast<uint8_t>((hi << 4) | lo);
        }
        struct stat st;
        world_readable_ = (::stat(path.c_str(), &st) == 0) && (st.st_mode & 0077) != 0;
        key_path_ = path;
        enabled_ = true;
        return true;
    }

    void set_key(const uint8_t key[32]) {
        std::memcpy(key_, key, 32);
        enabled_ = true;
    }

    bool is_enabled() const { return enabled_; }
    bool key_file_world_readable() const { return world_readable_; }
    const std::string& key_path() const { return key_path_; }

    inline blake3_sig_256 sign(const std::string& req_id, const std::string& sql) const {
        blake3_sig_256 out;
        if (!enabled_) return out;

        static const char kDomain[] = "ariabc.tx-tag.v1";
        uint8_t len_be[8];
        blake3_hasher hasher;
        blake3_hasher_init_keyed(&hasher, key_);
        blake3_hasher_update(&hasher, kDomain, sizeof(kDomain) - 1);
        put_u64_be(len_be, req_id.size());
        blake3_hasher_update(&hasher, len_be, sizeof(len_be));
        blake3_hasher_update(&hasher, req_id.data(), req_id.size());
        put_u64_be(len_be, sql.size());
        blake3_hasher_update(&hasher, len_be, sizeof(len_be));
        blake3_hasher_update(&hasher, sql.data(), sql.size());
        blake3_hasher_finalize(&hasher, out.bytes, 32);
        return out;
    }

    inline bool verify(const std::string& req_id, const std::string& sql, const blake3_sig_256& expected) const {
        if (!enabled_) return false;
        return sign(req_id, sql) == expected;
    }

private:
    static int hex_nibble(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

    static void put_u64_be(uint8_t out[8], uint64_t v) {
        for (int i = 7; i >= 0; --i) {
            out[i] = static_cast<uint8_t>(v & 0xffu);
            v >>= 8;
        }
    }

    bool enabled_ = false;
    bool world_readable_ = false;
    std::string key_path_;
    uint8_t key_[32];
};

} // namespace ariabc_pg
