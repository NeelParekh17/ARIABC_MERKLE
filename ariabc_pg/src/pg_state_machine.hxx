#pragma once

#include "pg_executor.hxx"
#include "wire_protocol.hxx"

#include "nuraft.hxx"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ariabc_pg {

class pg_state_machine : public nuraft::state_machine {
public:
    pg_state_machine(int node_id, const db_options& db_opt, const kafka_options& k_opt);
    ~pg_state_machine();

    bool ensure_bcdb_initialized() { return executor_.ensure_bcdb_initialized(); }
    pg_executor_stats executor_stats() const { return executor_.stats(); }
    kafka_producer_stats kafka_stats() const { return executor_.kafka_stats(); }
    bool admission_control_blocked() const { return executor_.admission_control_blocked(); }
    bool wait_for_admission_drain(uint64_t max_wait_ns) {
        return executor_.wait_for_admission_drain(max_wait_ns);
    }

    // Bypass-Raft path: directly enqueue a request with a caller-provided
    // monotonic sequence number, skipping Raft log deserialization.
    void direct_enqueue(const std::string& req_id, const std::string& sql, uint64_t seq) {
        register_result_batch(seq, 1);
        executor_.enqueue(req_id, sql, -1, seq);
        last_committed_idx_.store(seq, std::memory_order_relaxed);
    }
    void direct_enqueue_batch(const std::vector<client_api_request_item>& items, uint64_t first_seq) {
        if (items.empty()) return;
        const uint64_t last_seq = first_seq + static_cast<uint64_t>(items.size() - 1);
        register_result_batch(last_seq, items.size());
        std::vector<std::string> req_ids;
        std::vector<std::string> sqls;
        req_ids.reserve(items.size());
        sqls.reserve(items.size());
        for (const auto& item : items) {
            req_ids.push_back(item.req_id);
            sqls.push_back(item.sql);
        }
        executor_.enqueue_batch(req_ids, sqls, -1, last_seq);
        last_committed_idx_.store(last_seq, std::memory_order_relaxed);
    }

    nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong log_idx,
                                       nuraft::buffer& data) override;

    bool apply_snapshot(nuraft::snapshot& s) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    nuraft::ulong last_commit_index() override;
    void create_snapshot(nuraft::snapshot& s,
                         nuraft::async_result<bool>::handler_type& when_done) override;
    bool wait_for_result(uint64_t result_token, int timeout_ms);

private:
    void register_result_batch(uint64_t result_token, size_t item_count);
    void note_result_applied(uint64_t result_token);

    pg_executor executor_;
    std::atomic<uint64_t> last_committed_idx_;

    nuraft::ptr<nuraft::snapshot> last_snapshot_;
    std::mutex last_snapshot_lock_;

    std::mutex result_mu_;
    std::condition_variable result_cv_;
    std::unordered_map<uint64_t, size_t> pending_result_counts_;
    std::unordered_set<uint64_t> completed_result_tokens_;
};

} // namespace ariabc_pg
