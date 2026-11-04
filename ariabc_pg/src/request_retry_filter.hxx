#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace ariabc_pg {

/*
 * Replicated duplicate filter for retried requests.
 *
 * A gateway that fails over resubmits every request it has no result for, so a
 * request may reach the Raft log twice.  Every replica feeds the committed items
 * through this filter in log order, so all replicas drop the same items and
 * assign the same deterministic sequence numbers.
 *
 * Only ids of the form "<client>~<session>-<number>" take part: the session is
 * unique per gateway process, so a later run reusing the same numbers is not
 * mistaken for a retry.  Each session tracks the last kRingBits numbers; anything
 * older counts as seen, which is safe because a gateway's outstanding window is
 * far smaller (the gateway enforces kMaxOutstanding).
 */
class request_retry_filter {
public:
    static constexpr uint64_t kRingBits = 1ULL << 20;
    static constexpr uint64_t kMaxOutstanding = kRingBits / 2;

    // True when this id was already seen; otherwise records it and returns false.
    bool seen_or_add(const std::string& req_id) {
        const size_t tilde = req_id.find('~');
        if (tilde == std::string::npos) return false;
        const size_t dash = req_id.rfind('-');
        if (dash == std::string::npos || dash < tilde || dash + 1 >= req_id.size()) return false;
        uint64_t num = 0;
        for (size_t i = dash + 1; i < req_id.size(); ++i) {
            const char c = req_id[i];
            if (c < '0' || c > '9') return false;
            num = num * 10 + static_cast<uint64_t>(c - '0');
        }
        session& s = sessions_[req_id.substr(0, dash)];
        if (s.bits.empty()) {
            s.bits.assign(kRingBits / 64, 0);
            s.max_num = num;
        } else if (num > s.max_num) {
            const uint64_t gap = num - s.max_num;
            if (gap >= kRingBits) {
                std::fill(s.bits.begin(), s.bits.end(), 0);
            } else {
                for (uint64_t n = s.max_num + 1; n <= num; ++n) clear_bit(s, n);
            }
            s.max_num = num;
        } else if (s.max_num - num >= kRingBits) {
            return true;
        }
        uint64_t& word = s.bits[(num % kRingBits) / 64];
        const uint64_t mask = 1ULL << (num % 64);
        if (word & mask) return true;
        word |= mask;
        return false;
    }

    void clear() { sessions_.clear(); }

private:
    struct session {
        uint64_t max_num = 0;
        std::vector<uint64_t> bits;
    };

    static void clear_bit(session& s, uint64_t num) {
        s.bits[(num % kRingBits) / 64] &= ~(1ULL << (num % 64));
    }

    std::unordered_map<std::string, session> sessions_;
};

} // namespace ariabc_pg
