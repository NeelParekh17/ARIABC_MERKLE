#pragma once

#include "consensus_engine.hxx"
#include "log_store.hxx"
#include "state_machine.hxx"

#include <cstddef>
#include <string>
#include <utility>

namespace ariabc_pg {

struct hotstuff_config {
    int node_id = 0;
    // (member id, host:port) in --raftMembers order; the same on every replica.
    std::vector<std::pair<int, std::string>> members;
    // Holds hs_node_<id>.key for this node and hs_node_<id>.pub for every member
    // (scripts/distributed/gen_hotstuff_keys.sh).
    std::string key_dir;
    // Vote state, saved before every vote.  A new replica creates it; a restarted one
    // reads it and votes again.  Without a usable file a restarted replica never votes
    // or proposes, since it could contradict an earlier vote, and counts as one of the
    // f faults.
    std::string vote_state_path;
    // Restart of a crashed replica on this replica's vote state and log store.
    bool rejoin = false;
    int impeach_ms = 5000;           // no committed block for this long: replace the leader
    size_t max_block_bytes = 4 << 20; // log buffers per block, at least one always fits
};

/*
 * libhotstuff orders blocks but leaves commands to the application: its blocks
 * list command hashes only.  This engine puts the log buffers (the SQL) in the
 * block's `extra` field instead.  `extra` is covered by the block hash and so by
 * the votes and QCs, and it travels with the proposal and with block catch-up, so
 * a replica that can commit a block always holds its SQL.
 *
 * Returns nullptr and sets err when the keys or members are unusable.
 */
nuraft::ptr<consensus_engine> make_hotstuff_engine(const hotstuff_config& cfg,
                                                   nuraft::ptr<nuraft::state_machine> sm,
                                                   nuraft::ptr<nuraft::log_store> store,
                                                   std::string& err);

} // namespace ariabc_pg
