#include "pg_error_result.hxx"
#include <cassert>

int main() {
    using ariabc_pg::canonical_pg_error_result;
    assert(canonical_pg_error_result("XX000", "merkle_resolve_route_leaf node (index=468274, len=2) not found") ==
           canonical_pg_error_result("XX000", "merkle_resolve_route_leaf node (index=52211, len=2) not found"));
    assert(canonical_pg_error_result("XX000", "merkle_apply_single_coalesced_entry failed after 3 retries for index 468466") ==
           canonical_pg_error_result("XX000", "merkle_apply_single_coalesced_entry failed after 3 retries for index 52403"));
    assert(canonical_pg_error_result("XX000", "internal failure") !=
           canonical_pg_error_result("23505", "internal failure"));
    assert(canonical_pg_error_result("XX000", "failure one") !=
           canonical_pg_error_result("XX000", "failure two"));
    assert(canonical_pg_error_result(nullptr, nullptr).find("ERROR ") == 0);
    const char* message = "TPC-C expected NewOrder rollback: invalid item";
    assert(ariabc_pg::is_expected_user_abort_result(canonical_pg_error_result("TP001", message)));
    assert(ariabc_pg::is_expected_user_abort_result(
        std::string("SELECT 1 ") + ariabc_pg::expected_user_abort_result()));
    assert(!ariabc_pg::is_expected_user_abort_result(canonical_pg_error_result("XX000", message)));
    assert(!ariabc_pg::is_expected_user_abort_result(canonical_pg_error_result("TP001", "different message")));
    assert(!ariabc_pg::is_expected_user_abort_result(
        std::string(ariabc_pg::expected_user_abort_result()) + " trailing error"));
}
