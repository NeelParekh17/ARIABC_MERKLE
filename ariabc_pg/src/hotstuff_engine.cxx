#include "hotstuff_engine.hxx"
#include "hotstuff_vote_state.hxx"

#include "log_entry.hxx"

#include "hotstuff/hotstuff.h"
#include "hotstuff/liveness.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace ariabc_pg {
namespace {

using clock_type = std::chrono::steady_clock;

const size_t kMaxMsgBytes = 256u << 20;
const double kTickSec = 0.001;
const double kPacemakerTimeoutSec = 1;
const int kHeartbeatMs = 250;     // an idle leader's empty blocks show followers it is alive
const int kPeerSettleMs = 3000;   // salticidae retries a peer connection every 2 s
const int kQuorumWaitMs = 10000;  // then start without peers that are still down
// The library delivers a fetched chain recursively, each block resolving the promise
// its child waits on, so a restart needs stack in proportion to the chain: about 1 KiB
// per block, and 8 MiB overflows near 12000 blocks.  Only touched pages use memory,
// and the chain itself exhausts RAM long before this.
const size_t kLoopStackBytes = size_t(1) << 30;

std::string read_hex_file(const std::string& path, size_t bytes, std::string& err) {
    std::ifstream in(path);
    std::string hex;
    if (!(in >> hex) || hex.size() != bytes * 2) {
        err = "bad or missing HotStuff key file " + path;
        return std::string();
    }
    return hex;
}

bool tcp_reachable(const std::string& endpoint) {
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string::npos) return false;
    addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (::getaddrinfo(endpoint.substr(0, colon).c_str(), endpoint.substr(colon + 1).c_str(),
                      &hints, &res) != 0) {
        return false;
    }
    const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    const bool ok = ::connect(fd, res->ai_addr, res->ai_addrlen) == 0;
    ::close(fd);
    ::freeaddrinfo(res);
    return ok;
}

// Tells the engine about every committed block, including those without commands.
class commit_pacemaker : public hotstuff::PaceMakerRR {
public:
    using commit_fn = std::function<void(const hotstuff::block_t&)>;
    commit_pacemaker(salticidae::EventContext ec, commit_fn fn)
        : hotstuff::PaceMakerRR(ec, 1, kPacemakerTimeoutSec, kPacemakerTimeoutSec),
          fn_(std::move(fn)) {}

protected:
    void on_consensus(const hotstuff::block_t& blk) override {
        hotstuff::PMRoundRobinProposer::on_consensus(blk);
        fn_(blk);
    }

private:
    commit_fn fn_;
};

class replica : public hotstuff::HotStuffSecp256k1 {
public:
    using elected_fn = std::function<void()>;
    using persist_fn = std::function<void(const hotstuff::VoteState&)>;
    replica(hotstuff::ReplicaID rid,
            const hotstuff::bytearray_t& key,
            const salticidae::NetAddr& addr,
            hotstuff::pacemaker_bt pm,
            salticidae::EventContext ec,
            const Net::Config& net,
            elected_fn elected,
            persist_fn persist)
        : hotstuff::HotStuffSecp256k1(1, rid, key, addr, std::move(pm), ec, 4, net),
          elected_(std::move(elected)),
          persist_(std::move(persist)) {}

    void do_elected() override { elected_(); }

protected:
    // Commands are not used; on_consensus() delivers the block.
    void state_machine_execute(const hotstuff::Finality&) override {}
    void do_persist_vote(const hotstuff::VoteState& s) override { persist_(s); }

private:
    elected_fn elected_;
    persist_fn persist_;
};

class hotstuff_engine : public consensus_engine {
public:
    hotstuff_engine(const hotstuff_config& cfg,
                    nuraft::ptr<nuraft::state_machine> sm,
                    nuraft::ptr<nuraft::log_store> store)
        : cfg_(cfg), sm_(std::move(sm)), store_(std::move(store)) {}

    ~hotstuff_engine() override { stop(); }

    bool load(std::string& err) {
        for (size_t i = 0; i < cfg_.members.size(); ++i) {
            if (cfg_.members[i].first == cfg_.node_id) self_ = static_cast<int>(i);
            const std::string pub = read_hex_file(
                cfg_.key_dir + "/hs_node_" + std::to_string(cfg_.members[i].first) + ".pub", 33, err);
            if (pub.empty()) return false;
            pubkeys_.push_back(pub);
        }
        if (self_ < 0) {
            err = "--id is not one of the --raftMembers ids";
            return false;
        }
        privkey_ = read_hex_file(cfg_.key_dir + "/hs_node_" + std::to_string(cfg_.node_id) + ".key", 32, err);
        if (privkey_.empty()) return false;
        if (cfg_.vote_state_path.empty()) {
            err = "missing HotStuff vote state path";
            return false;
        }
        view_synced_ = !cfg_.rejoin;
        if (cfg_.rejoin) {
            restored_ = votes_.open_existing(cfg_.vote_state_path, saved_);
            return true;
        }
        return votes_.create(cfg_.vote_state_path, err);
    }

    void start() {
        delivery_ = std::thread([this] { delivery_loop(); });
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, kLoopStackBytes);
        const int rc = pthread_create(&loop_, &attr, [](void* self) -> void* {
            static_cast<hotstuff_engine*>(self)->event_loop();
            return nullptr;
        }, this);
        pthread_attr_destroy(&attr);
        if (rc != 0) {
            std::cerr << "HOTSTUFF_FATAL node=" << cfg_.node_id << ": event loop thread: "
                      << std::strerror(rc) << std::endl;
            std::abort();
        }
        loop_started_ = true;
        if (cfg_.rejoin) {
            peers_ready_.store(true);
        } else {
            peer_wait_ = std::thread([this] { wait_for_peers(); });
        }
    }

    nuraft::int32 get_leader() const override { return leader_id_.load(std::memory_order_relaxed); }
    bool is_leader() const override { return get_leader() == cfg_.node_id; }
    bool is_initialized() const override { return initialized_.load(std::memory_order_acquire); }
    nuraft::ulong get_last_log_idx() const override { return last_idx_.load(std::memory_order_relaxed); }
    bool request_leadership() override { return false; }

    // Like Raft's async mode, acceptance is not commitment: a leader that fails first
    // loses these entries, and the gateway's failover resubmits what has no result.
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>
    append_entries(const std::vector<nuraft::ptr<nuraft::buffer>>& logs) override {
        nuraft::ptr<nuraft::buffer> none;
        auto r = nuraft::cs_new<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>(
            none, nuraft::cmd_result_code::NOT_LEADER);
        if (!is_initialized() || !is_leader() || rotating_.load()) return r;
        {
            std::lock_guard<std::mutex> lk(queue_mu_);
            queue_.insert(queue_.end(), logs.begin(), logs.end());
        }
        r->set_result_code(nuraft::cmd_result_code::OK);
        r->accept();
        // Under load the next QC picks these up first; this only wakes an idle leader.
        if (!wake_pending_.exchange(true)) {
            hs_->get_tcall().async_call([this](salticidae::ThreadCall::Handle&) {
                wake_pending_.store(false);
                pump();
            });
        }
        return r;
    }

private:
    void wait_for_peers() {
        const auto t0 = clock_type::now();
        const size_t peers = cfg_.members.size() - 1;
        const size_t quorum_peers = (cfg_.members.size() - peers / 3) - 1;
        while (!stopping_.load()) {
            size_t up = 0;
            for (size_t i = 0; i < cfg_.members.size(); ++i) {
                if (static_cast<int>(i) != self_ && tcp_reachable(cfg_.members[i].second)) ++up;
            }
            const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                clock_type::now() - t0).count();
            if (up == peers || (up >= quorum_peers && waited >= kQuorumWaitMs)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(kPeerSettleMs));
                peers_ready_.store(true);
                std::cout << "HOTSTUFF_PEERS_READY node=" << cfg_.node_id
                          << " reachable=" << up << "/" << peers << std::endl;
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    void event_loop() {
        try {
            salticidae::EventContext ec;
            ec_ = ec;
            replica::Net::Config net;
            net.max_msg_size(kMaxMsgBytes)
               .max_recv_buff_size(16u << 20)
               .recv_chunk_size(256u << 10)
               .nworker(2);

            pacemaker_ = new commit_pacemaker(
                ec, [this](const hotstuff::block_t& blk) { on_commit(blk); });
            hotstuff::pacemaker_bt pm = pacemaker_;
            hs_.reset(new replica(static_cast<hotstuff::ReplicaID>(self_),
                                  hotstuff::from_hex(privkey_),
                                  salticidae::NetAddr(cfg_.members[self_].second),
                                  std::move(pm), ec, net,
                                  [this] { on_elected(); },
                                  [this](const hotstuff::VoteState& s) { votes_.save(s); }));

            std::vector<std::tuple<salticidae::NetAddr, hotstuff::bytearray_t, hotstuff::bytearray_t>> reps;
            for (size_t i = 0; i < cfg_.members.size(); ++i) {
                reps.emplace_back(salticidae::NetAddr(cfg_.members[i].second),
                                  hotstuff::from_hex(pubkeys_[i]),
                                  hotstuff::bytearray_t(32, 0));  // TLS certificate hash, TLS is off
            }
            hs_->start(reps);
            if (!cfg_.rejoin) {
                votes_.save(hotstuff::VoteState{0, 0, hs_->get_genesis()->get_hash()});
            } else if (restored_) {
                hs_->restore_vote_state(saved_);
            } else {
                hs_->set_vote_disabled(true);
                std::cerr << "HOTSTUFF_NO_VOTE_STATE node=" << cfg_.node_id
                          << " path=" << cfg_.vote_state_path << ": will not vote" << std::endl;
            }
            if (!cfg_.rejoin || restored_) watch_proposals();
            last_progress_ = clock_type::now();
            note_leader();

            salticidae::TimerEvent tick(ec, [this](salticidae::TimerEvent& t) {
                on_tick();
                t.add(kTickSec);
            });
            tick.add(kTickSec);

            initialized_.store(true, std::memory_order_release);
            std::cout << "HOTSTUFF_STARTED node=" << cfg_.node_id << " replicas=" << cfg_.members.size()
                      << " faults_tolerated=" << (cfg_.members.size() - 1) / 3
                      << " rejoin=" << (cfg_.rejoin ? 1 : 0)
                      << " voting=" << ((cfg_.rejoin && !restored_) ? 0 : 1)
                      << " vheight=" << saved_.vheight
                      << " lock_height=" << saved_.lock_height << std::endl;
            ec.dispatch();
            tick.del();
            hs_.reset();
        } catch (const std::exception& e) {
            // Fail-stop: a replica that keeps running after this could order wrongly.
            std::cerr << "HOTSTUFF_FATAL node=" << cfg_.node_id << ": " << e.what() << std::endl;
            std::abort();
        }
    }

    void note_leader() {
        hotstuff::ReplicaID p = hs_->get_pace_maker()->get_proposer();
        // A restarted replica starts from the first leader again, so until it has
        // seen who leads its view means nothing and it must not look like the leader.
        leader_id_.store(view_synced_ ? cfg_.members[p].first : -1, std::memory_order_relaxed);
    }

    // The library's leader view is a local counter.  on_commit() corrects it from here.
    // A restarted replica relies on this; its votes do not, as they go to whoever
    // proposed the block.
    void watch_proposals() {
        hs_->async_wait_receive_proposal().then([this](const hotstuff::Proposal& prop) {
            proposer_of_[prop.blk->get_hash()] = prop.proposer;
            if (hs_->is_restoring()) {
                // Waiting for a higher lock alone could stall: a new leader with an
                // older QC cannot get past the saved lock without this replica's vote.
                hs_->async_deliver_blk(saved_.lock_hash, hs_->get_config().get_peer_id(prop.proposer));
            }
            watch_proposals();
        });
    }

    bool i_am_proposer() { return hs_->get_pace_maker()->get_proposer() == hs_->get_id(); }

    void on_tick() {
        note_leader();
        if (!view_synced_ || !peers_ready_.load()) return;
        const auto now = clock_type::now();
        if (!timing_) {
            timing_ = true;
            last_progress_ = now;
        }
        if (now - last_progress_ > std::chrono::milliseconds(cfg_.impeach_ms)) {
            std::cerr << "HOTSTUFF_IMPEACH node=" << cfg_.node_id
                      << " leader=" << leader_id_.load() << std::endl;
            rotating_ = true;
            last_progress_ = now;
            // Blocks already received may still commit but say nothing about who leads next.
            proposer_of_.clear();
            impeach_count_++;
            hs_->get_pace_maker()->impeach();
            note_leader();
        }
        pump();
    }

    void on_elected() {
        rotating_ = false;
        // A beat from an earlier term may never resolve; pump() must be able to
        // request a new one.  The pacemaker resolves the old one first, which then
        // proposes as usual.
        beats_ = 0;
    }

    void pump() {
        if (!view_synced_ || !peers_ready_.load()) return;
        if (rotating_ || !i_am_proposer()) {
            beats_ = 0;
            drop_queue();
            return;
        }
        if (beats_ > 0) return;
        bool have_entries;
        {
            std::lock_guard<std::mutex> lk(queue_mu_);
            have_entries = !queue_.empty();
        }
        // Entries commit only once later blocks extend theirs, so keep going until
        // they have, then fall back to the heartbeat.
        const bool uncommitted = flush_height_ > committed_height_;
        if (!have_entries && !uncommitted && clock_type::now() < next_heartbeat_) return;
        beats_++;
        hs_->get_pace_maker()->beat().then([this](hotstuff::ReplicaID proposer) {
            if (beats_ > 0) beats_--;
            // The pacemaker waits for a proposal after every beat it resolves.
            if (proposer == hs_->get_id()) propose();
            pump();
        });
    }

    void drop_queue() {
        std::lock_guard<std::mutex> lk(queue_mu_);
        dropped_entries_ += queue_.size();
        queue_.clear();
    }

    void propose() {
        std::vector<nuraft::ptr<nuraft::buffer>> taken;
        size_t bytes = 0;
        {
            std::lock_guard<std::mutex> lk(queue_mu_);
            while (!queue_.empty() &&
                   (taken.empty() || bytes + queue_.front()->size() <= cfg_.max_block_bytes)) {
                bytes += queue_.front()->size();
                taken.push_back(queue_.front());
                queue_.pop_front();
            }
        }
        hotstuff::bytearray_t extra;
        extra.reserve(bytes + taken.size() * sizeof(uint32_t));
        for (const auto& buf : taken) {
            const uint32_t len = static_cast<uint32_t>(buf->size());
            const uint8_t* p = reinterpret_cast<const uint8_t*>(&len);
            extra.insert(extra.end(), p, p + sizeof(len));
            extra.insert(extra.end(), buf->data_begin(), buf->data_begin() + buf->size());
        }
        try {
            hotstuff::block_t blk = hs_->on_propose(std::vector<hotstuff::uint256_t>(),
                                                    hs_->get_pace_maker()->get_parents(),
                                                    std::move(extra));
            if (!taken.empty()) flush_height_ = blk->get_height();
            blocks_proposed_++;
        } catch (const std::runtime_error& e) {
            // The library refuses a block below one this replica already voted for.
            // Keep the entries; the leader is replaced if this does not clear up.
            std::cerr << "HOTSTUFF_PROPOSE_REFUSED node=" << cfg_.node_id << ": " << e.what() << std::endl;
            std::lock_guard<std::mutex> lk(queue_mu_);
            queue_.insert(queue_.begin(), taken.begin(), taken.end());
        }
        next_heartbeat_ = clock_type::now() + std::chrono::milliseconds(kHeartbeatMs);
    }

    void on_commit(const hotstuff::block_t& blk) {
        committed_height_ = blk->get_height();
        last_progress_ = clock_type::now();
        auto it = proposer_of_.find(blk->get_hash());
        if (it != proposer_of_.end()) {
            pacemaker_->sync_proposer(it->second);
            if (!view_synced_) {
                // A proposal this replica received has committed, so it has the chain.
                view_synced_ = true;
                std::cout << "HOTSTUFF_VIEW_SYNCED node=" << cfg_.node_id
                          << " proposer=" << it->second << std::endl;
            }
            proposer_of_.erase(it);
        }
        if (blk->get_extra().empty()) return;
        {
            std::lock_guard<std::mutex> lk(delivery_mu_);
            delivery_q_.push_back(blk);
        }
        delivery_cv_.notify_one();
    }

    // Identical on every replica, since all of them see the same bytes: a block
    // with malformed contents is skipped and uses no log index.
    static bool parse_extra(const hotstuff::bytearray_t& extra,
                            std::vector<nuraft::ptr<nuraft::buffer>>& out) {
        size_t pos = 0;
        while (pos < extra.size()) {
            uint32_t len = 0;
            if (extra.size() - pos < sizeof(len)) return false;
            std::memcpy(&len, extra.data() + pos, sizeof(len));
            pos += sizeof(len);
            if (len == 0 || extra.size() - pos < len) return false;
            nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(len);
            std::memcpy(buf->data_begin(), extra.data() + pos, len);
            buf->pos(0);
            out.push_back(buf);
            pos += len;
        }
        return true;
    }

    void delivery_loop() {
        try {
            uint64_t next_idx = 1;
            while (true) {
                std::deque<hotstuff::block_t> blocks;
                {
                    std::unique_lock<std::mutex> lk(delivery_mu_);
                    delivery_cv_.wait(lk, [this] { return stopping_.load() || !delivery_q_.empty(); });
                    if (stopping_.load() && delivery_q_.empty()) return;
                    blocks.swap(delivery_q_);
                }
                // Same order as Raft: durable in the log first, then applied.
                std::vector<nuraft::ptr<nuraft::buffer>> entries;
                const uint64_t first_idx = next_idx;
                uint64_t first_append = 0;
                uint64_t appended = 0;
                for (const auto& blk : blocks) {
                    std::vector<nuraft::ptr<nuraft::buffer>> parsed;
                    if (!parse_extra(blk->get_extra(), parsed)) {
                        malformed_blocks_++;
                        std::cerr << "HOTSTUFF_MALFORMED_BLOCK height=" << blk->get_height() << std::endl;
                        continue;
                    }
                    for (auto& buf : parsed) {
                        // A rejoining replica sees the whole chain again and already
                        // has its prefix on disk.
                        if (next_idx >= store_->next_slot()) {
                            auto le = nuraft::cs_new<nuraft::log_entry>(blk->get_height(), buf);
                            store_->append(le);
                            if (appended == 0) first_append = next_idx;
                            appended++;
                        }
                        entries.push_back(std::move(buf));
                        next_idx++;
                    }
                }
                if (appended > 0) store_->end_of_append_batch(first_append, appended);
                for (size_t i = 0; i < entries.size(); ++i) {
                    entries[i]->pos(0);
                    sm_->commit(first_idx + i, *entries[i]);
                }
                last_idx_.store(next_idx - 1, std::memory_order_relaxed);
            }
        } catch (const std::exception& e) {
            std::cerr << "HOTSTUFF_DELIVERY_FATAL node=" << cfg_.node_id << ": " << e.what() << std::endl;
            std::abort();
        }
    }

    void stop() {
        if (stopping_.exchange(true)) return;
        if (peer_wait_.joinable()) peer_wait_.join();
        if (loop_started_) {
            while (!initialized_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            salticidae::EventContext ec = ec_;
            hs_->get_tcall().async_call([ec](salticidae::ThreadCall::Handle&) mutable { ec.stop(); });
            pthread_join(loop_, nullptr);
        }
        delivery_cv_.notify_all();
        if (delivery_.joinable()) delivery_.join();
        std::cout << "PROFILE_HOTSTUFF node=" << cfg_.node_id
                  << " blocks_proposed=" << blocks_proposed_
                  << " committed_height=" << committed_height_
                  << " log_idx=" << last_idx_.load()
                  << " impeachments=" << impeach_count_
                  << " dropped_entries=" << dropped_entries_
                  << " malformed_blocks=" << malformed_blocks_.load()
                  << " vote_saves=" << votes_.saves()
                  << " vote_save_us=" << votes_.save_us() << std::endl;
    }

    hotstuff_config cfg_;
    nuraft::ptr<nuraft::state_machine> sm_;
    nuraft::ptr<nuraft::log_store> store_;
    int self_ = -1;
    std::vector<std::string> pubkeys_;
    std::string privkey_;
    hotstuff_vote_file votes_;
    hotstuff::VoteState saved_{0, 0, hotstuff::uint256_t()};
    bool restored_ = false;

    pthread_t loop_{};
    bool loop_started_ = false;
    std::thread delivery_;
    std::thread peer_wait_;
    salticidae::EventContext ec_;
    std::unique_ptr<replica> hs_;
    commit_pacemaker* pacemaker_ = nullptr;  // owned by hs_

    std::atomic<bool> initialized_{false};
    std::atomic<bool> peers_ready_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> wake_pending_{false};
    std::atomic<bool> rotating_{false};  // leader changing; cleared when this replica is elected
    std::atomic<nuraft::int32> leader_id_{-1};
    std::atomic<uint64_t> last_idx_{0};
    std::atomic<uint64_t> malformed_blocks_{0};

    std::mutex queue_mu_;
    std::deque<nuraft::ptr<nuraft::buffer>> queue_;

    std::mutex delivery_mu_;
    std::condition_variable delivery_cv_;
    std::deque<hotstuff::block_t> delivery_q_;

    // Event-loop thread only.
    bool view_synced_ = true;  // false: restarted, must not propose, impeach or look like the leader
    std::unordered_map<hotstuff::uint256_t, hotstuff::ReplicaID> proposer_of_;
    clock_type::time_point last_progress_{};
    clock_type::time_point next_heartbeat_{};
    bool timing_ = false;
    int beats_ = 0;
    uint32_t flush_height_ = 0;
    uint32_t committed_height_ = 0;
    uint64_t blocks_proposed_ = 0;
    uint64_t impeach_count_ = 0;
    uint64_t dropped_entries_ = 0;
};

} // namespace

nuraft::ptr<consensus_engine> make_hotstuff_engine(const hotstuff_config& cfg,
                                                   nuraft::ptr<nuraft::state_machine> sm,
                                                   nuraft::ptr<nuraft::log_store> store,
                                                   std::string& err) {
    auto engine = std::make_shared<hotstuff_engine>(cfg, std::move(sm), std::move(store));
    if (!engine->load(err)) return nullptr;
    engine->start();
    return engine;
}

} // namespace ariabc_pg
