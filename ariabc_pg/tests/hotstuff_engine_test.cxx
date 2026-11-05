// Runs --consensus hotstuff engines in one process, without PostgreSQL: checks the
// log buffers reach commit() with the same index and bytes on every replica.
//
//   hotstuff_engine_test <scratch_dir>
#include "hotstuff_engine.hxx"
#include "hotstuff_vote_state.hxx"
#include "in_memory_log_store.hxx"

#include "cluster_config.hxx"
#include "snapshot.hxx"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

using namespace ariabc_pg;

#define CHECK(c)                                                                \
    do {                                                                        \
        if (!(c)) {                                                             \
            std::cerr << "CHECK FAILED " << __FILE__ << ":" << __LINE__ << ": " \
                      << #c << std::endl;                                       \
            std::exit(1);                                                       \
        }                                                                       \
    } while (0)

namespace {

class recording_sm : public nuraft::state_machine {
public:
    nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong idx, nuraft::buffer& data) override {
        std::lock_guard<std::mutex> lk(mu);
        CHECK(idx == entries.size() + 1);
        // Online recovery replays entries <= the commit point from the log store.
        auto le = store->entry_at(idx);
        CHECK(le && le->get_buf().size() == data.size() &&
              std::memcmp(le->get_buf().data_begin(), data.data_begin(), data.size()) == 0);
        entries.emplace_back(reinterpret_cast<const char*>(data.data_begin()), data.size());
        return nuraft::buffer::alloc(0);
    }
    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    nuraft::ptr<nuraft::snapshot> last_snapshot() override { return nullptr; }
    nuraft::ulong last_commit_index() override { return 0; }
    void create_snapshot(nuraft::snapshot&, nuraft::async_result<bool>::handler_type&) override {}

    std::vector<std::string> copy() {
        std::lock_guard<std::mutex> lk(mu);
        return entries;
    }
    size_t count() {
        std::lock_guard<std::mutex> lk(mu);
        return entries.size();
    }

    std::mutex mu;
    std::vector<std::string> entries;
    nuraft::ptr<nuraft::log_store> store;
};

struct node {
    int id = 0;
    std::shared_ptr<recording_sm> sm = std::make_shared<recording_sm>();
    nuraft::ptr<nuraft::log_store> store = nuraft::cs_new<nuraft::inmem_log_store>();
    nuraft::ptr<consensus_engine> engine;
    std::string vote_state;
};

nuraft::ptr<nuraft::buffer> make_buf(const std::string& s) {
    nuraft::ptr<nuraft::buffer> b = nuraft::buffer::alloc(s.size());
    std::memcpy(b->data_begin(), s.data(), s.size());
    return b;
}

template <typename F>
bool wait_for(F f, int seconds) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (std::chrono::steady_clock::now() < end) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

std::string gen_keys(const std::string& dir, const std::string& script) {
    const std::string cmd = "bash '" + script + "' '" + dir + "' --nodes 1,2,3,4 >/dev/null";
    CHECK(std::system(cmd.c_str()) == 0);
    return dir;
}

void start(node& n, const hotstuff_config& base, bool rejoin) {
    hotstuff_config cfg = base;
    cfg.node_id = n.id;
    cfg.rejoin = rejoin;
    cfg.vote_state_path = n.vote_state;
    n.sm->store = n.store;
    std::string err;
    n.engine = make_hotstuff_engine(cfg, n.sm, n.store, err);
    if (!n.engine) std::cerr << err << std::endl;
    CHECK(n.engine != nullptr);
    CHECK(wait_for([&] { return n.engine->is_initialized(); }, 10));
}

int leader_of(std::vector<node>& nodes) {
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i].engine && nodes[i].engine->is_leader()) return static_cast<int>(i);
    }
    return -1;
}

// The durable log survives a restart; commit() sees every entry again.
void restart(node& n, const hotstuff_config& base, size_t total) {
    const size_t old_slot = n.store->next_slot();
    n.sm = std::make_shared<recording_sm>();
    start(n, base, true);
    CHECK(wait_for([&] { return n.sm->count() >= total; }, 120));
    CHECK(n.store->next_slot() >= old_slot);
}

void submit_retrying(std::vector<node>& nodes, const std::string& body) {
    CHECK(wait_for([&] {
        const int l = leader_of(nodes);
        if (l < 0) return false;
        auto r = nodes[l].engine->append_entries({make_buf(body)});
        return r && r->get_accepted();
    }, 60));
}

// Acceptance is not commitment: a leader that is replaced drops what it accepted and
// the gateway submits it again, as this does.
void commit_on_all_live(std::vector<node>& nodes, const std::string& tag, int count, int seconds) {
    const std::string last = tag + std::to_string(count - 1);
    auto done = [&] {
        for (auto& n : nodes) {
            if (!n.engine) continue;
            auto e = n.sm->copy();
            if (e.empty() || e.back() != last) return false;
        }
        return true;
    };
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (!done()) {
        CHECK(std::chrono::steady_clock::now() < end);
        for (int i = 0; i < count; ++i) submit_retrying(nodes, tag + std::to_string(i));
        wait_for(done, 5);
    }
}

void check_same_prefix(std::vector<node>& nodes, size_t want) {
    std::vector<std::string> first;
    for (auto& n : nodes) {
        if (!n.engine) continue;
        CHECK(wait_for([&] { return n.sm->count() >= want; }, 60));
        auto e = n.sm->copy();
        e.resize(want);
        if (first.empty()) first = e;
        CHECK(e == first);
    }
}

} // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string dir = std::string(argv[1]) + "/hotstuff_engine_test_keys";
    const std::string src = __FILE__;
    const std::string script = src.substr(0, src.rfind("/ariabc_pg/")) + "/scripts/distributed/gen_hotstuff_keys.sh";
    gen_keys(dir, script);
    const std::string state_dir = std::string(argv[1]) + "/hotstuff_engine_test_votes";
    CHECK(std::system(("rm -rf '" + state_dir + "' && mkdir -p '" + state_dir + "'").c_str()) == 0);

    hotstuff_config base;
    base.key_dir = dir;
    base.impeach_ms = 3000;
    const int port0 = 19700 + (getpid() % 200);
    std::vector<node> nodes(4);
    for (int i = 0; i < 4; ++i) {
        nodes[i].id = i + 1;
        nodes[i].vote_state = state_dir + "/" + std::to_string(i + 1) + ".bin";
        base.members.emplace_back(i + 1, "127.0.0.1:" + std::to_string(port0 + i));
    }
    for (auto& n : nodes) start(n, base, false);

    // Normal operation: a leader settles and everyone commits the same bytes in the
    // same order.  The last entries commit with no later traffic, and a body far over
    // the socket buffers and a block's byte limit still gets through.
    CHECK(wait_for([&] { return leader_of(nodes) == 0; }, 30));
    CHECK(wait_for([&] {
        return nodes[0].engine->get_leader() == 1 && nodes[3].engine->get_leader() == 1;
    }, 5));
    CHECK(!nodes[1].engine->append_entries({make_buf("not leader")})->get_accepted());

    const int kBatches = 2000;
    for (int i = 0; i < kBatches; ++i) {
        std::vector<nuraft::ptr<nuraft::buffer>> logs;
        logs.push_back(make_buf("sql-a-" + std::to_string(i)));
        logs.push_back(make_buf("sql-b-" + std::to_string(i)));
        CHECK(nodes[0].engine->append_entries(logs)->get_accepted());
    }
    const std::string big(32u << 20, 'x');
    CHECK(nodes[0].engine->append_entries({make_buf(big)})->get_accepted());
    size_t total = kBatches * 2 + 1;
    check_same_prefix(nodes, total);
    {
        auto e = nodes[2].sm->copy();
        CHECK(e[0] == "sql-a-0" && e[1] == "sql-b-0" && e[total - 2] == "sql-b-1999");
        CHECK(e[total - 1] == big);
        auto le = nodes[2].store->entry_at(1);
        CHECK(le && le->get_buf().size() == 7);
        CHECK(nodes[2].engine->get_last_log_idx() == total);
    }
    std::cout << "ok: normal operation, " << total << " entries" << std::endl;

    // Idle chain: heartbeats keep the leader in place and use no log index.
    std::this_thread::sleep_for(std::chrono::milliseconds(4000));
    CHECK(leader_of(nodes) == 0);
    CHECK(nodes[1].sm->count() == total);
    std::cout << "ok: idle leader kept, no index used" << std::endl;

    // Leader fails: the survivors replace it and keep the same log.
    nodes[0].engine.reset();
    CHECK(wait_for([&] {
        const int l = leader_of(nodes);
        return l > 0 && nodes[3].engine->get_leader() == l + 1;
    }, 40));
    std::cout << "ok: new leader id=" << nodes[leader_of(nodes)].id << std::endl;
    for (int i = 0; i < 50; ++i) {
        submit_retrying(nodes, "after-failover-" + std::to_string(i));
    }
    CHECK(wait_for([&] {
        auto e = nodes[1].sm->copy();
        return !e.empty() && e.back() == "after-failover-49";
    }, 60));
    total = nodes[1].sm->count();
    check_same_prefix(nodes, total);
    std::cout << "ok: commits after failover, " << total << " entries" << std::endl;

    // Restart: the replica restores its vote state, gets the chain again and learns
    // the leader from a block that commits.
    restart(nodes[0], base, total);
    CHECK(nodes[0].store->next_slot() == nodes[0].sm->count() + 1);
    CHECK(!nodes[0].engine->append_entries({make_buf("refused")})->get_accepted());
    CHECK(wait_for([&] {
        const int l = leader_of(nodes);
        return l >= 0 && nodes[0].engine->get_leader() == nodes[l].id;
    }, 60));
    check_same_prefix(nodes, total);
    std::cout << "ok: restart caught up, " << total << " entries, view synced" << std::endl;

    // More restarts than f, one at a time.  Each kill leaves exactly a quorum, so
    // every commit here needs the vote of the replica that restarted just before it;
    // a replica that stayed non-voting would halt ordering at the second kill.
    int last_restarted = 0;
    for (int round = 0; round < 3; ++round) {
        // Rounds 0 and 1 kill a follower, round 2 the leader.
        int victim = leader_of(nodes);
        CHECK(victim >= 0);
        if (round < 2) {
            const int leader = victim;
            for (int i = 0; i < 4; ++i) {
                if (i != leader && i != last_restarted) victim = i;
            }
        }
        nodes[victim].engine.reset();
        const std::string tag = "after-kill-" + std::to_string(round) + "-";
        commit_on_all_live(nodes, tag, 20, 120);
        total = nodes[(victim + 1) % 4].sm->count();
        check_same_prefix(nodes, total);
        restart(nodes[victim], base, total);
        check_same_prefix(nodes, total);
        last_restarted = victim;
        std::cout << "ok: kill " << round << " of id=" << nodes[victim].id
                  << ", commits without it, restarted, " << total << " entries" << std::endl;
    }

    // Two down with the chain frozen, then one restarts having voted above the chain,
    // as a leader does whose last proposals never got out.  Its vote is needed, and it
    // can only vote above that height: rotation must extend the chain past it.
    {
        const int leader = leader_of(nodes);
        CHECK(leader >= 0);
        std::vector<int> down;
        for (int i = 0; i < 4 && down.size() < 2; ++i) {
            if (i != leader) down.push_back(i);
        }
        nodes[down[0]].engine.reset();
        nodes[down[1]].engine.reset();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        {
            hotstuff::VoteState s{0, 0, hotstuff::uint256_t()};
            ariabc_pg::hotstuff_vote_file f;
            CHECK(f.open_existing(nodes[down[0]].vote_state, s));
            s.vheight += 3;
            f.save(s);
        }
        total = nodes[leader].sm->count();
        restart(nodes[down[0]], base, total);
        commit_on_all_live(nodes, "above-chain-", 20, 180);
        total = nodes[down[0]].sm->count();
        restart(nodes[down[1]], base, total);
        check_same_prefix(nodes, total);
        std::cout << "ok: restart with a vote above the chain, " << total << " entries" << std::endl;
    }

    // A restart delivers the whole chain in one go, however long it has become.
    {
        const int kHeight = 15000;
        int down = 0;
        while (down == leader_of(nodes)) ++down;
        nodes[down].engine.reset();
        const int watch = (down + 1) % 4;
        int i = 0;
        auto height = [&] {
            hotstuff::VoteState s{0, 0, hotstuff::uint256_t()};
            ariabc_pg::hotstuff_vote_file f;
            CHECK(f.open_existing(nodes[watch].vote_state, s));
            return static_cast<int>(s.vheight);
        };
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(280);
        while (height() < kHeight) {
            CHECK(std::chrono::steady_clock::now() < end);
            for (int k = 0; k < 50; ++k) {
                submit_retrying(nodes, "chain-" + std::to_string(i++));
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        commit_on_all_live(nodes, "chain-end-", 1, 120);
        total = nodes[watch].sm->count();
        restart(nodes[down], base, total);
        check_same_prefix(nodes, total);
        std::cout << "ok: restart over a chain of height " << height() << std::endl;
    }

    // Vote state lost: it must not vote or claim to lead, ever, but still follows.
    {
        node& n = nodes[1];
        n.engine.reset();
        CHECK(std::remove(n.vote_state.c_str()) == 0);
        commit_on_all_live(nodes, "after-state-lost-", 1, 120);
        total = nodes[2].sm->count();
        restart(n, base, total);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CHECK(!n.engine->is_leader());
        CHECK(n.engine->get_leader() == -1);
        CHECK(!n.engine->append_entries({make_buf("refused")})->get_accepted());
        check_same_prefix(nodes, total);
        std::cout << "ok: vote state lost, follows without voting" << std::endl;
    }

    for (auto& n : nodes) n.engine.reset();
    std::cout << "PASS" << std::endl;
    return 0;
}
