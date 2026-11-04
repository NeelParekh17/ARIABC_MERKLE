#include "request_retry_filter.hxx"
#include <cassert>
#include <string>

int main() {
    using ariabc_pg::request_retry_filter;

    // Ids without a session never take part, however often they repeat.
    {
        request_retry_filter f;
        assert(!f.seen_or_add("cli-1"));
        assert(!f.seen_or_add("cli-1"));
        assert(!f.seen_or_add(""));
        assert(!f.seen_or_add("cli~s1-"));
        assert(!f.seen_or_add("cli~s1-12x"));
        assert(!f.seen_or_add("cli~s1-12x"));
        assert(!f.seen_or_add("a-b~c"));
    }
    // A retry is seen once; order within the window does not matter.
    {
        request_retry_filter f;
        assert(!f.seen_or_add("cli~s1-5"));
        assert(f.seen_or_add("cli~s1-5"));
        assert(!f.seen_or_add("cli~s1-3"));
        assert(f.seen_or_add("cli~s1-3"));
        assert(!f.seen_or_add("cli~s1-9"));
        assert(!f.seen_or_add("cli~s1-4"));
        assert(f.seen_or_add("cli~s1-9"));
    }
    // A new session with the same numbers is new work, and client ids may hold dashes.
    {
        request_retry_filter f;
        assert(!f.seen_or_add("cli-a~s1-7"));
        assert(!f.seen_or_add("cli-a~s2-7"));
        assert(f.seen_or_add("cli-a~s1-7"));
    }
    // Sliding the window clears stale slots: num + ring lands on the same slot.
    {
        request_retry_filter f;
        const uint64_t ring = request_retry_filter::kRingBits;
        assert(!f.seen_or_add("c~s-1"));
        assert(!f.seen_or_add("c~s-" + std::to_string(1 + ring)));
        assert(f.seen_or_add("c~s-" + std::to_string(1 + ring)));
        // Fell out of the window: must count as seen, never run again.
        assert(f.seen_or_add("c~s-1"));
        // A jump past a whole ring forgets everything in the old window.
        assert(!f.seen_or_add("c~s-" + std::to_string(1 + 5 * ring)));
        assert(!f.seen_or_add("c~s-" + std::to_string(1 + 4 * ring + 1)));
    }
    // Two replicas fed the same log decide the same way.
    {
        request_retry_filter a, b;
        const char* log[] = {"c~s-2", "c~s-1", "c~s-2", "c~t-2", "x-2", "c~s-1", "c~s-3"};
        for (const char* id : log) assert(a.seen_or_add(id) == b.seen_or_add(id));
    }
    return 0;
}
