#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace ariabc_pg {

inline int pg_retry_delay_ms(int db_type, const char* sqlstate,
                             int attempt, int configured_backoff_ms) {
    const int cap = std::max(0, configured_backoff_ms);
    if (db_type == 1 || !sqlstate ||
        (std::strcmp(sqlstate, "40001") != 0 &&
         std::strcmp(sqlstate, "40P01") != 0)) {
        return cap;
    }
    // A short MVCC conflict does not warrant parking a PG executor and its
    // connection for 100ms on the first retry. Back off from 1ms only when the
    // same transaction repeatedly conflicts, retaining the configured cap.
    // Deterministic execution and query cancellation retain their old policy.
    const int shift = std::min(30, std::max(0, attempt));
    return std::min(cap, 1 << shift);
}

// PostgreSQL serialization/deadlock retries (never deterministic execution).
inline bool pg_retry_jitter_applies(int db_type, const char* sqlstate) {
    return db_type != 1 && sqlstate &&
           (std::strcmp(sqlstate, "40001") == 0 || std::strcmp(sqlstate, "40P01") == 0);
}

// Full jitter: wait uniformly in [0, ceiling_ms]. Requests that conflicted on
// the same hot row otherwise back off by identical delays and collide again.
inline int pg_retry_full_jitter_ms(int ceiling_ms, uint64_t draw) {
    if (ceiling_ms <= 0) return 0;
    return static_cast<int>(draw % (static_cast<uint64_t>(ceiling_ms) + 1));
}

} // namespace ariabc_pg
