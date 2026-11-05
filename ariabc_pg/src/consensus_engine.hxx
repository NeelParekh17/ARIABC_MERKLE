#pragma once

#include "async.hxx"
#include "buffer.hxx"

#include <vector>

namespace ariabc_pg {

/*
 * What the server needs from an ordering protocol (--consensus raft|hotstuff).
 * Either implementation delivers entries through state_machine::commit() with a
 * gap-free index and stores them in the log store, so everything above and below
 * this interface is unchanged.  The names match nuraft::raft_server.
 */
class consensus_engine {
public:
    virtual ~consensus_engine() {}

    virtual nuraft::int32 get_leader() const = 0;
    virtual bool is_leader() const = 0;
    virtual bool is_initialized() const = 0;
    virtual nuraft::ulong get_last_log_idx() const = 0;
    virtual bool request_leadership() = 0;
    virtual nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>
        append_entries(const std::vector<nuraft::ptr<nuraft::buffer>>& logs) = 0;
};

} // namespace ariabc_pg
