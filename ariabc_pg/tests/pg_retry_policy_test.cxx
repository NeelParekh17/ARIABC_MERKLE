#include "pg_retry_policy.hxx"
#include <cassert>
#include <climits>

int main() {
    using ariabc_pg::pg_retry_delay_ms;
    for (int db_type : {0, 2}) {
        for (const char* state : {"40001", "40P01"}) {
            assert(pg_retry_delay_ms(db_type, state, 0, 100) == 1);
            assert(pg_retry_delay_ms(db_type, state, 3, 100) == 8);
            assert(pg_retry_delay_ms(db_type, state, INT_MAX, 100) == 100);
            assert(pg_retry_delay_ms(db_type, state, 0, 0) == 0);
            assert(pg_retry_delay_ms(db_type, state, 0, -1) == 0);
        }
    }
    assert(pg_retry_delay_ms(1, "40001", 0, 100) == 100);
    assert(pg_retry_delay_ms(0, "57014", 0, 100) == 100);
    assert(pg_retry_delay_ms(0, nullptr, 0, 100) == 100);

    using ariabc_pg::pg_retry_full_jitter_ms;
    using ariabc_pg::pg_retry_jitter_applies;
    assert(pg_retry_jitter_applies(0, "40001") && pg_retry_jitter_applies(0, "40P01"));
    assert(!pg_retry_jitter_applies(1, "40001"));   // deterministic execution keeps its policy
    assert(!pg_retry_jitter_applies(0, "57014") && !pg_retry_jitter_applies(0, nullptr));
    assert(pg_retry_full_jitter_ms(0, 12345) == 0 && pg_retry_full_jitter_ms(-5, 7) == 0);
    for (uint64_t draw = 0; draw < 1000; ++draw) {
        const int d = pg_retry_full_jitter_ms(100, draw * 2654435761ULL);
        assert(d >= 0 && d <= 100);
    }
    assert(pg_retry_full_jitter_ms(100, 100) == 100);
    assert(pg_retry_full_jitter_ms(100, 101) == 0);
}
