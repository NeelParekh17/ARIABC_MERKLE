#pragma once

#include <string>

namespace ariabc_pg {

// Local index OIDs are diagnostic identifiers, not deterministic SQL outcomes.
// Retain the error class in the voted result; the original message stays in the
// PostgreSQL/server log. In particular, this never converts an error to success.
inline std::string canonical_pg_error_result(const char* sqlstate, const char* primary) {
    std::string message = primary ? primary : "unknown PostgreSQL error";
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
