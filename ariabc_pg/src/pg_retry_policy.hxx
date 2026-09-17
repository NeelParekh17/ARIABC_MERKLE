#pragma once

#include <algorithm>
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

} // namespace ariabc_pg
