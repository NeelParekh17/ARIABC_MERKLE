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
}
