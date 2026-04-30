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

void debug_trace_batch_commit(const raft_request_batch& batch,
                              nuraft::ulong log_idx) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_commit_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    const client_api_request_item* first =
        batch.items.empty() ? nullptr : &batch.items.front();
    std::string sql_head = first ? trim_copy(first->sql) : std::string();
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE commit_batch"
              << " idx=" << idx
              << " items=" << batch.items.size()
              << " first_req_id=" << (first ? first->req_id : std::string("NA"))
              << " log_idx=" << log_idx
              << " sql=" << sql_head
              << std::endl;
}

} // namespace

pg_state_machine::pg_state_machine(int node_id,
                                   const db_options& db_opt,
                                   const kafka_options& k_opt)
    : executor_(node_id,
                db_opt,
                k_opt,
                [this](uint64_t result_token) { note_result_applied(result_token); })
    , last_committed_idx_(0)
    , last_snapshot_(nullptr)
    {}

pg_state_machine::~pg_state_machine() {
    executor_.stop();
}

nuraft::ptr<nuraft::buffer> pg_state_machine::commit(const nuraft::ulong log_idx,
                                                     nuraft::buffer& data)
{
    raft_request_batch batch;
    std::string parse_err;
    if (!parse_raft_request_log(data, batch, parse_err)) {
        client_api_request_item item;
        item.req_id = "ERR";
        item.sql = std::string("/* commit parse failed: ") + parse_err + " */";
        batch.items.push_back(std::move(item));
        batch.leader_node_hint = -1;
        std::cerr << "pg_state_machine commit parse failed at log " << log_idx
                  << ": " << parse_err << std::endl;
    }

    if (batch.items.size() <= 1) {
        const client_api_request_item& item = batch.items.front();
        debug_trace_commit(item.req_id, log_idx, item.sql);
    } else {
        debug_trace_batch_commit(batch, log_idx);
    }
    register_result_batch(static_cast<uint64_t>(log_idx), batch.items.size());
    std::vector<std::string> req_ids;
    std::vector<std::string> sqls;
    req_ids.reserve(batch.items.size());
    sqls.reserve(batch.items.size());
    for (const auto& item : batch.items) {
        req_ids.push_back(item.req_id);
        sqls.push_back(item.sql);
    }
    executor_.enqueue_batch(req_ids,
                            sqls,
                            batch.leader_node_hint,
                            static_cast<uint64_t>(log_idx));
    last_committed_idx_ = log_idx;

    // ACK: {req_id, log_idx}
    const std::string ack_req_id =
        batch.items.empty() ? std::string("ERR") : batch.items.front().req_id;
    const size_t ack_sz = sizeof(int32_t) + ack_req_id.size() + sizeof(uint64_t);
    nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(ack_sz);
    nuraft::buffer_serializer bs_ack(ack);
    bs_ack.put_str(ack_req_id);
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

bool pg_state_machine::wait_for_result(uint64_t result_token, int timeout_ms) {
    if (result_token == 0) return false;
    if (timeout_ms <= 0) timeout_ms = 1;

    std::unique_lock<std::mutex> lk(result_mu_);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    const bool ready = result_cv_.wait_until(lk, deadline, [&] {
        return completed_result_tokens_.find(result_token) != completed_result_tokens_.end();
    });
    if (!ready) return false;
    completed_result_tokens_.erase(result_token);
    return true;
}

void pg_state_machine::register_result_batch(uint64_t result_token, size_t item_count) {
    if (result_token == 0 || item_count == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    pending_result_counts_[result_token] = item_count;
    completed_result_tokens_.erase(result_token);
}

void pg_state_machine::note_result_applied(uint64_t result_token) {
    if (result_token == 0) return;
    std::lock_guard<std::mutex> lk(result_mu_);
    auto it = pending_result_counts_.find(result_token);
    if (it == pending_result_counts_.end()) return;
    if (it->second > 1) {
        --(it->second);
        return;
    }
    pending_result_counts_.erase(it);
    completed_result_tokens_.insert(result_token);
    result_cv_.notify_all();
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
