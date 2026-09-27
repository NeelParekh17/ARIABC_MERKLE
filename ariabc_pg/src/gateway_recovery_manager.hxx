#pragma once

#include "ariabc_pg_util.hxx"
#include "wire_protocol.hxx"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace ariabc_pg {

/*
 * Online replica recovery coordinator (ProtectDB Algorithm 2, integrated).
 *
 * Detection
 *   - active:  a transaction result diverges from the majority (vote store
 *              note) or the all-node audit finds a minority hash.
 *   - passive: every interval the replicas cut their state at the SAME future
 *              Raft index T (CUT target=T) and publish a per-table Merkle
 *              digest; a minority digest identifies a corrupted replica.  The
 *              cut never pauses execution (see bcdb_cut_snapshot_export).
 *
 * Recovery of damaged node D (never pauses submitters or healthy replicas)
 *   1. QUARANTINE D (stops applying new entries, stays in Raft; entries it
 *   already started still finish and their votes count only where they match
 *   a healthy replica);   2. CUT a healthy reference H at its commit point once
 *   that covers everything D executed -> (L, B, snapshot);   3. RECOVER D from
 *   H's snapshot: sparse Merkle repair, BCDB rebase to B, replay L+1.. from D's
 *   own Raft log;   4. RELEASE the cut;   5. D's votes for entries > L are
 *   audited again, entries <= L are covered by the installed snapshot.
 */
class gateway_recovery_manager {
public:
    struct vote_hooks {
        std::function<void(int)> begin;                 // node starts recovering
        std::function<void(int, uint64_t)> boundary;    // node covered for idx <= L
        std::function<void(int, bool)> live;            // node caught up (or not)
    };

    gateway_recovery_manager(const std::string& recovery_mode,
                             int recovery_interval_ms,
                             int recovery_db_port,
                             const std::string& recovery_db_user,
                             const std::string& recovery_db_name,
                             const std::string& recovery_db_password,
                             const std::string& /*recovery_table*/,
                             const std::string& /*recovery_nodes*/,
                             const std::string& /*recovery_hook_script*/,
                             const std::string& /*recovery_compare_script*/,
                             const std::vector<host_port>& nodes,
                             const std::vector<int>& raft_node_ids)
        : mode_(recovery_mode),
          interval_ms_(recovery_interval_ms > 0 ? recovery_interval_ms : 1000),
          db_port_(recovery_db_port > 0 ? recovery_db_port : 5438),
          db_user_(recovery_db_user.empty() ? "postgres" : recovery_db_user),
          db_name_(recovery_db_name.empty() ? "postgres" : recovery_db_name),
          db_password_(recovery_db_password),
          nodes_(nodes) {
        for (size_t i = 0; i < nodes_.size(); ++i) {
            node_ids_.push_back((i < raft_node_ids.size()) ? raft_node_ids[i]
                                                           : static_cast<int>(i + 1));
        }
        is_active_ = (mode_ == "active" || mode_ == "both");
        is_passive_ = (mode_ == "passive" || mode_ == "both");
        compare_margin_ = env_u64("ARIABC_RECOVERY_COMPARE_MARGIN", 16);
        catchup_wait_ms_ = env_u64("ARIABC_RECOVERY_CATCHUP_WAIT_MS", 120000);
        compare_timeout_ms_ = env_u64("ARIABC_RECOVERY_COMPARE_TIMEOUT_MS", 30000);
        if (is_active_ || is_passive_) {
            std::cout << "[recovery_mgr] online recovery mode=" << mode_
                      << " compare_interval_ms=" << interval_ms_
                      << " compare_margin=" << compare_margin_
                      << " compare_timeout_ms=" << compare_timeout_ms_
                      << " db_port=" << db_port_ << " nodes=" << nodes_.size()
                      << std::endl;
            worker_thread_ = std::thread(&gateway_recovery_manager::worker_loop, this);
            if (is_passive_) {
                compare_thread_ = std::thread(&gateway_recovery_manager::compare_loop, this);
            }
        }
    }

    ~gateway_recovery_manager() { stop(); }

    void set_vote_hooks(vote_hooks hooks) {
        std::lock_guard<std::mutex> lk(mu_);
        hooks_ = std::move(hooks);
    }

    bool is_active_enabled() const { return is_active_; }
    bool is_passive_enabled() const { return is_passive_; }
    bool enabled() const { return is_active_ || is_passive_; }

    uint64_t triggered_count() const { return triggered_.load(); }
    uint64_t success_count() const { return succeeded_.load(); }
    uint64_t failure_count() const { return failed_.load(); }
    uint64_t total_ms() const { return total_ms_.load(); }
    uint64_t compare_rounds() const { return compare_rounds_.load(); }
    uint64_t compare_mismatches() const { return compare_mismatches_.load(); }

    /* Passive-mode nudge: run a state comparison as soon as possible. */
    void trigger_immediate_check() {
        std::lock_guard<std::mutex> lk(mu_);
        compare_now_ = true;
        cv_.notify_all();
    }

    /* Active detection from a vote-store divergence note. */
    void handle_divergence_note(const std::string& note) {
        if (!enabled() || note.empty()) return;
        if (note.find("vote_store_capacity_exhausted") != std::string::npos) return;
        const int damaged = minority_node_from_note(note);
        if (damaged > 0) {
            request_recovery(damaged, "result_divergence");
        } else if (is_passive_) {
            trigger_immediate_check();
        }
    }

    /*
     * All-node audit found a single minority replica ("...:minority=4"); the
     * majority result stands.  Active mode recovers that replica directly;
     * passive mode only runs a state comparison now, which recovers it if its
     * state really diverged.  Returns true when the outlier is being handled.
     */
    bool handle_audit_mismatch(const std::string& err) {
        if (!enabled()) return false;
        const std::string key = ":minority=";
        const size_t p = err.find(key);
        if (p == std::string::npos) return false;
        const std::string list = err.substr(p + key.size());
        if (list.find(',') != std::string::npos) return false;  // only a single outlier is recoverable
        const int node = std::atoi(list.c_str());
        if (node <= 0) return false;
        if (is_active_) {
            request_recovery(node, "audit_mismatch");
        } else {
            trigger_immediate_check();
        }
        return true;
    }

    void request_recovery(int node_id, const std::string& reason) {
        if (!enabled() || index_of(node_id) < 0) return;
        std::lock_guard<std::mutex> lk(mu_);
        if (stop_) return;
        if (recovering_.count(node_id) || queued_.count(node_id)) return;
        auto cd = cooldown_until_ns_.find(node_id);
        if (cd != cooldown_until_ns_.end() && now_ns() < cd->second) return;
        /* Never recover a majority: at most (n-1)/2 replicas may be out at once. */
        if (static_cast<int>(recovering_.size() + queued_.size()) + 1 >
            static_cast<int>(nodes_.size() - 1) / 2) {
            std::cerr << "[recovery_mgr] refusing recovery of node " << node_id
                      << ": would leave no healthy majority" << std::endl;
            return;
        }
        queued_.insert(node_id);
        task t;
        t.node_id = node_id;
        t.reason = reason;
        t.detected_ns = now_ns();
        queue_.push_back(t);
        triggered_.fetch_add(1);
        std::cout << "[recovery_mgr] DETECTED damaged node=" << node_id
                  << " reason=" << reason << std::endl;
        cv_.notify_all();
    }

    /*
     * End of workload: stop periodic comparisons, wait for in-flight
     * recoveries, then check that every replica holds the same state.
     */
    void stop_passive_and_drain() {
        if (!enabled()) return;
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_compare_ = true;
            cv_.notify_all();
        }
        if (compare_thread_.joinable()) compare_thread_.join();
        const uint64_t deadline = now_ns() + catchup_wait_ms_ * 1000000ULL;
        for (;;) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (queue_.empty() && recovering_.empty()) break;
            }
            if (now_ns() > deadline) {
                std::cerr << "[recovery_mgr] timed out waiting for recoveries to finish" << std::endl;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        final_consistency_check();
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (stop_) return;
            stop_ = true;
            stop_compare_ = true;
            cv_.notify_all();
        }
        if (compare_thread_.joinable()) compare_thread_.join();
        if (worker_thread_.joinable()) worker_thread_.join();
    }

private:
    struct task {
        int node_id = 0;
        std::string reason;
        uint64_t detected_ns = 0;
    };

    struct cut_info {
        bool ok = false;
        uint64_t L = 0;
        int64_t B = -1;
        std::string snapshot;
        std::string digest;
        std::string raw;
    };

    static uint64_t now_ns() {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    }

    static uint64_t env_u64(const char* name, uint64_t def) {
        const char* v = std::getenv(name);
        return (v && *v) ? std::strtoull(v, nullptr, 10) : def;
    }

    static std::string field(const std::string& msg, const std::string& key) {
        const std::string k = " " + key + "=";
        std::string padded = " " + msg;
        const size_t p = padded.find(k);
        if (p == std::string::npos) return std::string();
        const size_t s = p + k.size();
        const size_t e = padded.find(' ', s);
        return padded.substr(s, e == std::string::npos ? std::string::npos : e - s);
    }

    int index_of(int node_id) const {
        for (size_t i = 0; i < node_ids_.size(); ++i) {
            if (node_ids_[i] == node_id) return static_cast<int>(i);
        }
        return -1;
    }

    /* One control round trip to a replica's ariabc_pg_server. */
    bool control(int node_id, const std::string& cmd, int timeout_ms, std::string& reply) {
        reply.clear();
        const int idx = index_of(node_id);
        if (idx < 0) {
            reply = "ERR unknown_node";
            return false;
        }
        const host_port& hp = nodes_[static_cast<size_t>(idx)];
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        if (::getaddrinfo(hp.host.c_str(), std::to_string(hp.port).c_str(), &hints, &res) != 0 || !res) {
            reply = "ERR resolve_failed";
            return false;
        }
        int fd = -1;
        for (addrinfo* ai = res; ai; ai = ai->ai_next) {
            fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) continue;
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
            ::close(fd);
            fd = -1;
        }
        ::freeaddrinfo(res);
        if (fd < 0) {
            reply = "ERR connect_failed";
            return false;
        }
        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        int one = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        client_api_request req;
        req.req_id = "__recovery";
        req.sql = "__ARIABC_CTRL_RECOVERY " + cmd;
        client_api_response resp;
        std::string err;
        const bool ok = write_request_frame(fd, req, err) && read_response_frame(fd, resp, err);
        ::close(fd);
        if (!ok) {
            reply = "ERR io " + err;
            return false;
        }
        reply = resp.msg;
        return resp.status == 0 && reply.rfind("OK", 0) == 0;
    }

    cut_info parse_cut(bool ok, const std::string& reply) {
        cut_info c;
        c.raw = reply;
        c.ok = ok;
        if (!ok) return c;
        c.L = std::strtoull(field(reply, "L").c_str(), nullptr, 10);
        c.B = std::strtoll(field(reply, "B").c_str(), nullptr, 10);
        c.snapshot = field(reply, "snapshot");
        c.digest = field(reply, "digest");
        c.ok = !c.snapshot.empty();
        return c;
    }

    std::vector<int> healthy_nodes_locked() const {
        std::vector<int> out;
        for (int id : node_ids_) {
            if (!recovering_.count(id) && !queued_.count(id)) out.push_back(id);
        }
        return out;
    }

    int minority_node_from_note(const std::string& note) const {
        auto extract = [&](const std::string& key) -> std::string {
            const size_t p = note.find(key);
            if (p == std::string::npos) return std::string();
            const size_t s = p + key.size();
            const size_t e = note.find('"', s);
            return note.substr(s, e == std::string::npos ? std::string::npos : e - s);
        };
        if (note.find("\"type\":\"duplicate_identity_conflict\"") != std::string::npos) {
            const size_t p = note.find("\"node_id\":");
            return p == std::string::npos ? -1 : std::atoi(note.c_str() + p + 10);
        }
        if (note.find("\"type\":\"result_divergence\"") == std::string::npos) return -1;
        /* hash_votes: "h1=c1;h2=c2"; node_results: "id|sig=1|hash=..;.." */
        std::map<std::string, int> votes;
        std::string majority;
        int best = 0;
        std::stringstream hv(extract("\"hash_votes\":\""));
        std::string item;
        while (std::getline(hv, item, ';')) {
            const size_t eq = item.find('=');
            if (eq == std::string::npos) continue;
            const int c = std::atoi(item.c_str() + eq + 1);
            if (c > best) {
                best = c;
                majority = item.substr(0, eq);
            }
        }
        if (best * 2 <= static_cast<int>(nodes_.size())) return -1;  // no strict majority
        std::vector<int> outliers;
        std::stringstream nr(extract("\"node_results\":\""));
        while (std::getline(nr, item, ';')) {
            const int id = std::atoi(item.c_str());
            const std::string sig = field_in_entry(item, "sig=");
            const std::string h = field_in_entry(item, "hash=");
            if (sig == "0" || h != majority) outliers.push_back(id);
        }
        return outliers.size() == 1 ? outliers.front() : -1;
    }

    static std::string field_in_entry(const std::string& entry, const std::string& key) {
        const size_t p = entry.find(key);
        if (p == std::string::npos) return std::string();
        const size_t s = p + key.size();
        const size_t e = entry.find('|', s);
        return entry.substr(s, e == std::string::npos ? std::string::npos : e - s);
    }

    void worker_loop() {
        for (;;) {
            task t;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [&] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                t = queue_.front();
                queue_.pop_front();
                queued_.erase(t.node_id);
                recovering_.insert(t.node_id);
            }
            const bool ok = run_recovery(t);
            {
                std::lock_guard<std::mutex> lk(mu_);
                recovering_.erase(t.node_id);
                cooldown_until_ns_[t.node_id] = now_ns() + 1000ULL * 1000000ULL;
            }
            (ok ? succeeded_ : failed_).fetch_add(1);
        }
    }

    bool run_recovery(const task& t) {
        const uint64_t t0 = now_ns();
        vote_hooks hooks;
        std::vector<int> refs;
        {
            std::lock_guard<std::mutex> lk(mu_);
            hooks = hooks_;
            refs = healthy_nodes_locked();
        }
        if (hooks.begin) hooks.begin(t.node_id);

        std::string reply;
        uint64_t damaged_enqueued = 0;   /* last entry D executed before stopping */
        if (!control(t.node_id, "QUARANTINE", 10000, reply)) {
            std::cerr << "[recovery_mgr] QUARANTINE node=" << t.node_id << " failed: " << reply << std::endl;
        } else {
            damaged_enqueued = std::strtoull(field(reply, "last_enqueued").c_str(), nullptr, 10);
        }
        const uint64_t t_quarantined = now_ns();

        for (int attempt = 1; attempt <= 3; ++attempt) {
            // Prioritize candidate reference replicas by commit progress and responsiveness:
            // Query STATUS on each healthy candidate, sort descending by last_commit so the
            // most up-to-date and responsive replica (e.g. utkarsh over swapping user4) is
            // attempted first, avoiding multi-second snapshot stalls and timeout retries.
            std::vector<std::pair<uint64_t, int>> ranked_refs;
            for (int r : refs) {
                std::string srep;
                uint64_t lc = 0;
                if (control(r, "STATUS", 1000, srep)) {
                    lc = std::strtoull(field(srep, "last_commit").c_str(), nullptr, 10);
                }
                ranked_refs.emplace_back(lc, r);
            }
            std::sort(ranked_refs.rbegin(), ranked_refs.rend());
            std::vector<int> ordered_refs;
            for (const auto& p : ranked_refs) ordered_refs.push_back(p.second);

            for (int ref : ordered_refs) {
                const uint64_t c0 = now_ns();
                /* The boundary must cover every entry D already executed, so D
                 * never re-executes (and re-publishes) an entry.  The reference
                 * normally learns that commit index within milliseconds. */
                const uint64_t commit_deadline = now_ns() + 5000ULL * 1000000ULL;
                while (damaged_enqueued > 0 && now_ns() < commit_deadline &&
                       control(ref, "STATUS", 3000, reply) &&
                       std::strtoull(field(reply, "last_commit").c_str(), nullptr, 10) < damaged_enqueued) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
                const bool cut_ok = control(ref, "CUT target=0 hold=1 digest=1 timeout_ms=30000", 35000, reply);
                cut_info cut = parse_cut(cut_ok, reply);
                const uint64_t c1 = now_ns();
                if (!cut.ok) {
                    std::cerr << "[recovery_mgr] CUT on ref=" << ref << " failed: " << reply << std::endl;
                    continue;
                }
                if (cut.L < damaged_enqueued) {
                    std::string ignored;
                    control(ref, "RELEASE snapshot=" + cut.snapshot, 10000, ignored);
                    std::cerr << "[recovery_mgr] CUT on ref=" << ref << " L=" << cut.L
                              << " is behind node " << t.node_id << " (executed " << damaged_enqueued
                              << "); retrying" << std::endl;
                    continue;
                }
                if (hooks.boundary) hooks.boundary(t.node_id, cut.L);

                const int ref_idx = index_of(ref);
                std::ostringstream cmd;
                cmd << "RECOVER ref_host=" << nodes_[static_cast<size_t>(ref_idx)].host
                    << " ref_port=" << db_port_ << " ref_user=" << db_user_ << " ref_db=" << db_name_
                    << " snapshot=" << cut.snapshot << " L=" << cut.L << " B=" << cut.B
                    << " wait_live_ms=" << catchup_wait_ms_;
                if (!db_password_.empty()) cmd << " ref_password=" << db_password_;
                std::string rreply;
                const bool rec_ok = control(t.node_id, cmd.str(),
                                            static_cast<int>(catchup_wait_ms_) + 120000, rreply);
                std::string ignored;
                control(ref, "RELEASE snapshot=" + cut.snapshot, 10000, ignored);
                const uint64_t r1 = now_ns();

                const std::string repaired_digest = field(rreply, "digest");
                const bool digest_ok = rec_ok && !cut.digest.empty() && repaired_digest == cut.digest;
                const bool live = field(rreply, "live") == "1";
                if (!rec_ok || !digest_ok) {
                    const std::string le = field(rreply, "last_enqueued");
                    if (!le.empty()) {
                        damaged_enqueued = std::max<uint64_t>(damaged_enqueued,
                                                              std::strtoull(le.c_str(), nullptr, 10));
                    }
                    std::cerr << "[recovery_mgr] RECOVER node=" << t.node_id << " from ref=" << ref
                              << " attempt=" << attempt << " failed: " << rreply
                              << " expected_digest=" << cut.digest << std::endl;
                    continue;
                }
                if (hooks.live) hooks.live(t.node_id, live);
                const uint64_t total = (now_ns() - t.detected_ns) / 1000000ULL;
                total_ms_.fetch_add(total);
                std::cout << "RECOVERY_EVENT node=" << t.node_id << " reason=" << t.reason
                          << " result=PASS ref=" << ref << " attempt=" << attempt
                          << " L=" << cut.L << " B=" << cut.B
                          << " detect_to_quarantine_ms=" << (t_quarantined - t.detected_ns) / 1000000ULL
                          << " cut_ms=" << (c1 - c0) / 1000000ULL
                          << " recover_call_ms=" << (r1 - c1) / 1000000ULL
                          << " repair_ms=" << field(rreply, "repair_ms")
                          << " drain_ms=" << field(rreply, "drain_ms")
                          << " catchup_ms=" << field(rreply, "catchup_ms")
                          << " live=" << (live ? 1 : 0)
                          << " mismatched_partitions=" << field(rreply, "mismatched_partitions")
                          << " differing_leaves=" << field(rreply, "differing_leaves")
                          << " rows_deleted=" << field(rreply, "rows_deleted")
                          << " rows_upserted=" << field(rreply, "rows_upserted")
                          << " full_copies=" << field(rreply, "full_copies")
                          << " replay_from=" << field(rreply, "replay_from")
                          << " replay_target=" << field(rreply, "replay_target")
                          << " total_ms=" << total
                          << " digest=" << cut.digest << std::endl;
                if (!live) {
                    wait_live(t.node_id, hooks);
                }
                (void)t0;
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200 * attempt));
        }
        std::cout << "RECOVERY_EVENT node=" << t.node_id << " reason=" << t.reason
                  << " result=FAIL total_ms=" << (now_ns() - t.detected_ns) / 1000000ULL << std::endl;
        if (hooks.live) hooks.live(t.node_id, false);
        return false;
    }

    void wait_live(int node_id, const vote_hooks& hooks) {
        const uint64_t deadline = now_ns() + catchup_wait_ms_ * 1000000ULL;
        std::string reply;
        while (now_ns() < deadline) {
            if (control(node_id, "STATUS", 5000, reply) && field(reply, "mode") == "LIVE") {
                if (hooks.live) hooks.live(node_id, true);
                std::cout << "[recovery_mgr] node=" << node_id << " LIVE " << reply << std::endl;
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        std::cerr << "[recovery_mgr] node=" << node_id << " did not catch up in time: " << reply << std::endl;
    }

    /*
     * Aligned state comparison on every healthy replica.  Under load every
     * replica cuts before the same future Raft entry T; each export waits, in
     * that replica's keeper backend only, until its own execution reaches the
     * boundary, so a round lasts about one execution backlog of the slowest
     * replica and nothing else waits.  When no entries are arriving and every
     * replica has applied the same commit point, they cut at that point.
     */
    void compare_round() {
        std::vector<int> ids;
        {
            std::lock_guard<std::mutex> lk(mu_);
            ids = healthy_nodes_locked();
        }
        if (ids.size() < 2) return;
        uint64_t max_commit = 0;
        uint64_t first_commit = 0;
        bool quiescent = true;
        std::string reply;
        for (size_t i = 0; i < ids.size(); ++i) {
            if (!control(ids[i], "STATUS", 3000, reply)) return;
            const uint64_t c = std::strtoull(field(reply, "last_commit").c_str(), nullptr, 10);
            const uint64_t a = std::strtoull(field(reply, "applied").c_str(), nullptr, 10);
            if (i == 0) first_commit = c;
            if (field(reply, "mode") != "LIVE" || a < c || c != first_commit) quiescent = false;
            max_commit = std::max(max_commit, c);
        }
        if (max_commit == 0) return;
        const int target_wait_ms = std::max(2000, interval_ms_);
        std::ostringstream cmd;
        uint64_t L = 0;
        if (quiescent) {
            L = max_commit;
            cmd << "CUT target=0 hold=0 digest=1 timeout_ms=" << compare_timeout_ms_;
        } else {
            L = max_commit + compare_margin_ - 1;
            cmd << "CUT target=" << (L + 1) << " hold=0 digest=1 timeout_ms=" << compare_timeout_ms_
                << " target_timeout_ms=" << target_wait_ms;
        }
        const int io_timeout_ms = target_wait_ms + static_cast<int>(compare_timeout_ms_) + 5000;
        std::vector<cut_info> cuts(ids.size());
        std::vector<std::thread> th;
        for (size_t i = 0; i < ids.size(); ++i) {
            th.emplace_back([&, i] {
                std::string r;
                const bool ok = control(ids[i], cmd.str(), io_timeout_ms, r);
                cuts[i] = parse_cut(ok, r);
            });
        }
        for (auto& x : th) x.join();
        std::map<std::string, std::vector<int>> groups;
        size_t answered = 0;
        for (size_t i = 0; i < ids.size(); ++i) {
            if (!cuts[i].ok || cuts[i].L != L) continue;
            groups[cuts[i].digest].push_back(ids[i]);
            ++answered;
        }
        if (answered < 2) return;
        compare_rounds_.fetch_add(1);
        if (groups.size() == 1) return;
        compare_mismatches_.fetch_add(1);
        for (const auto& g : groups) {
            if (g.second.size() * 2 > nodes_.size()) {
                for (const auto& other : groups) {
                    if (other.first == g.first) continue;
                    for (int id : other.second) {
                        std::cout << "[recovery_mgr] state digest mismatch at L=" << L
                                  << " node=" << id << " digest=" << other.first
                                  << " majority=" << g.first << std::endl;
                        request_recovery(id, "merkle_compare");
                    }
                }
                return;
            }
        }
        std::cerr << "[recovery_mgr] state digests disagree without a strict majority at L="
                  << L << std::endl;
    }

    void compare_loop() {
        for (;;) {
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait_for(lk, std::chrono::milliseconds(interval_ms_),
                             [&] { return stop_compare_ || compare_now_; });
                if (stop_compare_) return;
                compare_now_ = false;
            }
            compare_round();
        }
    }

    void final_consistency_check() {
        std::string reply;
        for (int round = 0; round < 100; ++round) {
            std::vector<uint64_t> commits;
            bool quiet = true;
            for (int id : node_ids_) {
                if (!control(id, "STATUS", 3000, reply) || field(reply, "mode") != "LIVE") {
                    quiet = false;
                    break;
                }
                const uint64_t c = std::strtoull(field(reply, "last_commit").c_str(), nullptr, 10);
                const uint64_t a = std::strtoull(field(reply, "applied").c_str(), nullptr, 10);
                if (a < c) quiet = false;
                commits.push_back(c);
            }
            if (quiet && !commits.empty() &&
                std::all_of(commits.begin(), commits.end(), [&](uint64_t c) { return c == commits[0]; })) {
                std::map<std::string, std::vector<int>> groups;
                for (int id : node_ids_) {
                    const bool ok = control(id, "CUT target=0 hold=0 digest=1 timeout_ms=10000", 15000, reply);
                    groups[ok ? field(reply, "digest") : std::string("ERR")].push_back(id);
                }
                const bool pass = groups.size() == 1 && groups.begin()->first != "ERR";
                std::cout << "RECOVERY_FINAL_CHECK result=" << (pass ? "PASS" : "FAIL")
                          << " L=" << commits[0] << " groups=" << groups.size()
                          << " digest=" << groups.begin()->first << std::endl;
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout << "RECOVERY_FINAL_CHECK result=SKIPPED reason=replicas_not_quiescent " << reply << std::endl;
    }

    std::string mode_;
    int interval_ms_;
    int db_port_;
    std::string db_user_;
    std::string db_name_;
    std::string db_password_;
    std::vector<host_port> nodes_;
    std::vector<int> node_ids_;
    bool is_active_ = false;
    bool is_passive_ = false;
    uint64_t compare_margin_ = 16;
    uint64_t catchup_wait_ms_ = 120000;
    uint64_t compare_timeout_ms_ = 30000;

    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;
    bool stop_compare_ = false;
    bool compare_now_ = false;
    std::deque<task> queue_;
    std::set<int> queued_;
    std::set<int> recovering_;
    std::map<int, uint64_t> cooldown_until_ns_;
    vote_hooks hooks_;
    std::thread worker_thread_;
    std::thread compare_thread_;

    std::atomic<uint64_t> triggered_{0};
    std::atomic<uint64_t> succeeded_{0};
    std::atomic<uint64_t> failed_{0};
    std::atomic<uint64_t> total_ms_{0};
    std::atomic<uint64_t> compare_rounds_{0};
    std::atomic<uint64_t> compare_mismatches_{0};
};

} // namespace ariabc_pg
