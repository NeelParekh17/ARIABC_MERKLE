#include "pg_state_machine.hxx"

#include "ariabc_pg_util.hxx"

#include <atomic>
#include <cstdlib>
#include <iostream>

namespace ariabc_pg {

namespace {

bool debug_req_trace_enabled() {
    const char* env = std::getenv("ARIABC_DEBUG_REQ_TRACE");
    if (!env || !*env) return false;
    const std::string s(env);
    return !(s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
}

uint64_t debug_req_trace_limit() {
    const char* env = std::getenv("ARIABC_DEBUG_REQ_TRACE_LIMIT");
    if (!env || !*env) return 32;
    try {
        const unsigned long long n = std::stoull(env);
        return (n == 0ULL) ? 32ULL : static_cast<uint64_t>(n);
    } catch (...) {
        return 32ULL;
    }
}

std::atomic<uint64_t> g_debug_commit_trace_count{0};

void debug_trace_commit(const std::string& req_id, nuraft::ulong log_idx, const std::string& sql) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_commit_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::string sql_head = trim_copy(sql);
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE commit"
              << " idx=" << idx
              << " req_id=" << req_id
              << " log_idx=" << log_idx
              << " sql=" << sql_head
              << std::endl;
}

} // namespace

pg_state_machine::pg_state_machine(int node_id,
                                   const db_options& db_opt,
                                   const kafka_options& k_opt)
    : executor_(node_id, db_opt, k_opt)
    , last_committed_idx_(0)
    , last_snapshot_(nullptr)
    {}

pg_state_machine::~pg_state_machine() {
    executor_.stop();
}

nuraft::ptr<nuraft::buffer> pg_state_machine::commit(const nuraft::ulong log_idx,
                                                     nuraft::buffer& data)
{
    std::string req_id;
    std::string sql;
    int leader_node_hint = -1;
    try {
        nuraft::buffer_serializer bs(data);
        req_id = bs.get_str();
        sql = bs.get_str();
        if (bs.pos() + sizeof(int32_t) <= bs.size()) {
            leader_node_hint = bs.get_i32();
        }
    } catch (const std::exception& e) {
        // Best-effort: keep Raft progressing even if payload is malformed.
        req_id = "ERR";
        sql = std::string("/* commit parse failed: ") + e.what() + " */";
        std::cerr << "pg_state_machine commit parse failed at log " << log_idx
                  << ": " << e.what() << std::endl;
    }

    debug_trace_commit(req_id, log_idx, sql);
    executor_.enqueue(req_id, sql, leader_node_hint, static_cast<uint64_t>(log_idx));
    last_committed_idx_ = log_idx;

    // ACK: {req_id, log_idx}
    const size_t ack_sz = sizeof(int32_t) + req_id.size() + sizeof(uint64_t);
    nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(ack_sz);
    nuraft::buffer_serializer bs_ack(ack);
    bs_ack.put_str(req_id);
    bs_ack.put_u64(log_idx);
    return ack;
}

bool pg_state_machine::apply_snapshot(nuraft::snapshot& s) {
    // No-op logical snapshot (DB snapshot semantics are out of scope).
    // Keep the latest snapshot metadata for Raft.
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    last_snapshot_ = nuraft::snapshot::deserialize(*snp_buf);
    return true;
}

nuraft::ptr<nuraft::snapshot> pg_state_machine::last_snapshot() {
    std::lock_guard<std::mutex> l(last_snapshot_lock_);
    return last_snapshot_;
}

nuraft::ulong pg_state_machine::last_commit_index() {
    return last_committed_idx_;
}

void pg_state_machine::create_snapshot(nuraft::snapshot& s,
                                       nuraft::async_result<bool>::handler_type& when_done)
{
    // No-op snapshot: just persist snapshot metadata.
    {   std::lock_guard<std::mutex> l(last_snapshot_lock_);
        nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
        last_snapshot_ = nuraft::snapshot::deserialize(*snp_buf);
    }
    nuraft::ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}

} // namespace ariabc_pg
