#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <cstring>
#include <array>
#include <atomic>

#include "blake3.h"

namespace ariabc_pg {

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

    bool operator==(const blake3_sig_256& o) const {
        return std::memcmp(bytes, o.bytes, 32) == 0;
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

class blake3_tx_signer {
public:
    blake3_tx_signer() {
        enabled_ = false;
        std::memset(key_, 0, sizeof(key_));
    }

    bool init(bool enable, const std::string& key_str = "") {
        enabled_ = enable;
        if (!enabled_) {
            std::memset(key_, 0, sizeof(key_));
            return true;
        }
        if (!key_str.empty()) {
            if (key_str.size() == 32) {
                std::memcpy(key_, key_str.data(), 32);
            } else {
                blake3_hasher hasher;
                blake3_hasher_init(&hasher);
                blake3_hasher_update(&hasher, key_str.data(), key_str.size());
                blake3_hasher_finalize(&hasher, key_, 32);
            }
        } else {
            static const uint8_t kDefaultKey[32] = {
                0x41, 0x72, 0x69, 0x61, 0x42, 0x43, 0x5f, 0x47, // "AriaBC_G"
                0x61, 0x74, 0x65, 0x77, 0x61, 0x79, 0x5f, 0x54, // "ateway_T"
                0x78, 0x53, 0x69, 0x67, 0x5f, 0x42, 0x4c, 0x41, // "xSig_BLA"
                0x4b, 0x45, 0x33, 0x5f, 0x4b, 0x65, 0x79, 0x21  // "KE3_Key!"
            };
            std::memcpy(key_, kDefaultKey, 32);
        }
        return true;
    }

    bool is_enabled() const { return enabled_; }

    inline blake3_sig_256 sign(uint64_t req_num, const std::string& req_id, const std::string& sql) const {
        blake3_sig_256 out;
        if (!enabled_) return out;

        blake3_hasher hasher;
        blake3_hasher_init_keyed(&hasher, key_);
        blake3_hasher_update(&hasher, &req_num, sizeof(req_num));
        blake3_hasher_update(&hasher, req_id.data(), req_id.size());
        blake3_hasher_update(&hasher, sql.data(), sql.size());
        blake3_hasher_finalize(&hasher, out.bytes, 32);
        return out;
    }

    inline bool verify(uint64_t req_num, const std::string& req_id, const std::string& sql, const blake3_sig_256& expected) const {
        if (!enabled_) return true;
        blake3_sig_256 computed = sign(req_num, req_id, sql);
        return (computed == expected);
    }

private:
    bool enabled_ = false;
    uint8_t key_[32];
};

} // namespace ariabc_pg
