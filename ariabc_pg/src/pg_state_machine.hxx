#pragma once

#include "pg_executor.hxx"

#include "nuraft.hxx"

#include <atomic>
#include <mutex>

namespace ariabc_pg {

class pg_state_machine : public nuraft::state_machine {
public:
    pg_state_machine(int node_id, const db_options& db_opt, const kafka_options& k_opt);
    ~pg_state_machine();

    pg_executor_stats executor_stats() const { return executor_.stats(); }
    kafka_producer_stats kafka_stats() const { return executor_.kafka_stats(); }
    bool admission_control_blocked() const { return executor_.admission_control_blocked(); }
    bool wait_for_admission_drain(uint64_t max_wait_ns) {
        return executor_.wait_for_admission_drain(max_wait_ns);
    }

    // Bypass-Raft path: directly enqueue a request with a caller-provided
    // monotonic sequence number, skipping Raft log deserialization.
    void direct_enqueue(const std::string& req_id, const std::string& sql, uint64_t seq) {
        executor_.enqueue(req_id, sql, -1, seq);
        last_committed_idx_.store(seq, std::memory_order_relaxed);
    }

    nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong log_idx,
                                       nuraft::buffer& data) override;

    bool apply_snapshot(nuraft::snapshot& s) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    nuraft::ulong last_commit_index() override;
    void create_snapshot(nuraft::snapshot& s,
                         nuraft::async_result<bool>::handler_type& when_done) override;

private:
    pg_executor executor_;
    std::atomic<uint64_t> last_committed_idx_;

    nuraft::ptr<nuraft::snapshot> last_snapshot_;
    std::mutex last_snapshot_lock_;
};

} // namespace ariabc_pg
