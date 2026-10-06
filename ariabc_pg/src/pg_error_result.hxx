#pragma once

#include <string>

namespace ariabc_pg {

inline const char* expected_user_abort_result() {
    return "USER_ABORT sqlstate=TP001 message=TPC-C expected NewOrder rollback: invalid item";
}

inline bool is_expected_user_abort_result(const std::string& result) {
    return result == expected_user_abort_result() ||
           result == std::string("SELECT 1 ") + expected_user_abort_result();
}

// Local index OIDs are diagnostic identifiers, not deterministic SQL outcomes.
// Retain the error class in the voted result; the original message stays in the
// PostgreSQL/server log. The reserved business abort completes successfully.
inline std::string canonical_pg_error_result(const char* sqlstate, const char* primary) {
    std::string message = primary ? primary : "unknown PostgreSQL error";
    if (sqlstate && std::string(sqlstate) == "TP001" &&
        message == "TPC-C expected NewOrder rollback: invalid item") {
        return expected_user_abort_result();
    }
    if (message.find("merkle_resolve_route_leaf node (index") == 0) {
        message = "merkle_resolve_route_leaf: route node not found";
    } else if (message.find("merkle_apply_single_coalesced_entry failed after ") == 0) {
        const size_t index = message.find(" for index ");
        if (index != std::string::npos) message.resize(index);
    }
    return "ERROR sqlstate=" + std::string(sqlstate ? sqlstate : "XX000") +
           " message=" + message;
}

} // namespace ariabc_pg
