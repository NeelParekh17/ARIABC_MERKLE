#pragma once

#include "kafka_console.hxx"

#include <poll.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

struct pg_conn;
typedef struct pg_conn PGconn;

namespace ariabc_pg {

struct pg_executor_stats {
    uint64_t enqueued = 0;
    uint64_t dequeued = 0;
    uint64_t q_wait_ns = 0;
    uint64_t queue_delay_ns = 0;
    uint64_t queue_delay_dequeue_ns = 0;
    uint64_t queue_delay_exec_start_ns = 0;
    uint64_t backlog_cur = 0;
    uint64_t inflight_cur = 0;
    uint64_t delayed_cur = 0;

    uint64_t conn_acquire_calls = 0;
    uint64_t conn_acquire_wait_ns = 0;

    uint64_t exec_calls = 0;
    uint64_t exec_ns = 0;
    uint64_t pg_query_ns = 0;
    uint64_t result_format_ns = 0;
    uint64_t retryable_sqlstate_40001 = 0;
    uint64_t retryable_sqlstate_40P01 = 0;
    uint64_t retryable_sqlstate_57014 = 0;
    uint64_t retry_attempts_total = 0;
    uint64_t retry_exhausted_total = 0;

    uint64_t kafka_flush_calls = 0;
    uint64_t kafka_payload_bytes = 0;
    uint64_t kafka_build_payload_ns = 0;
    uint64_t kafka_send_ns = 0;

    uint64_t queue_depth_samples = 0;
    uint64_t queue_depth_sum = 0;
    uint64_t queue_depth_max = 0;
    uint64_t queue_depth_bin_le_16 = 0;
    uint64_t queue_depth_bin_17_64 = 0;
    uint64_t queue_depth_bin_65_256 = 0;
    uint64_t queue_depth_bin_gt_256 = 0;
    uint64_t queue_overload_enter = 0;
    uint64_t queue_overload_exit = 0;
    uint64_t queue_high_watermark = 0;
    uint64_t queue_low_watermark = 0;
    uint64_t queue_depth_cur = 0;
    int queue_overloaded = 0;
};

struct db_options {
    std::string host = "localhost";
    std::string port;
    std::string dbname;
    std::string user = "postgres";
    std::string password;

    // Gateway db type (0=default, 1=det, 2=nondet).
    int db_type = 0;

    // 1=bcdb, 2=safedb, others=plain postgres.
    int safedb = 0;

    // "threaded": existing behavior (N worker threads for N connections).
    // "event": single-thread reactor (async libpq with poll()).
    std::string exec_mode = "threaded";

    int conn_pool_size = 20;
    int max_retries = 10;
    int retry_backoff_ms = 100;
};

struct kafka_options {
    std::string bootstrap;
    std::string result_topic = "topic2";
    std::string result_sig_key;
};

class pg_executor {
public:
    pg_executor(int node_id, const db_options& db_opt, const kafka_options& k_opt);
    ~pg_executor();

    void enqueue(const std::string& req_id,
                 const std::string& sql,
                 int leader_node_hint,
                 uint64_t raft_log_idx);

    pg_executor_stats stats() const;

    kafka_producer_stats kafka_stats() const;

    void stop();

    bool admission_control_blocked() const;

    // Block until admission control is no longer engaged or `deadline_ns` from
    // now elapses (whichever comes first). Returns true if admission cleared
    // before the deadline. This replaces the 1ms sleep-poll on RPC threads and
    // lets the executor notify exactly when the queue drains.
    bool wait_for_admission_drain(uint64_t max_wait_ns);

private:
    struct notice_state;
    static void notice_processor(void* arg, const char* message);

    struct task {
        std::string req_id;
        std::string sql;
        int leader_node_hint = -1;
        uint64_t raft_log_idx = 0;
        uint64_t dispatch_seq = 0;
        uint64_t enqueue_ns = 0;
        uint64_t exec_begin_ns = 0; // set on first attempt start; preserved across retries (event mode)
        int attempt = 0;
    };

    enum class det_tx_state : uint8_t {
        QUEUED = 0,
        SIM_DONE = 1,
        APPLY_READY = 2,
        APPLIED = 3,
    };

    void worker_loop();
    void event_loop();

    PGconn* acquire_conn();
    void release_conn(PGconn* c);

    std::string exec_sql(PGconn* c, const std::string& sql);
    bool exec_det_block_batch(PGconn* c,
                              const std::vector<task>& tasks,
                              std::vector<std::string>& out_results);
    void ensure_bcdb_initialized_for_sql(const std::string& sql);
    uint64_t get_det_tx_seq(const task& t) const;
    void det_mark_tx_state(uint64_t tx_seq, det_tx_state st);
    bool det_wait_for_apply_turn(uint64_t tx_seq);
    void det_finish_apply(uint64_t tx_seq);
    bool wait_for_ordered_emit_turn(uint64_t dispatch_seq);
    void finish_ordered_emit(uint64_t dispatch_seq);

    static bool is_event_mode(const std::string& mode);

    int node_id_;
    db_options db_opt_;
    std::string conninfo_;

    std::mutex pool_mu_;
    std::condition_variable pool_cv_;
    std::queue<PGconn*> pool_;
    std::vector<PGconn*> all_conns_;
    std::unordered_map<PGconn*, notice_state*> notice_state_by_conn_;
    std::vector<std::unique_ptr<notice_state>> notice_states_;

    kafka_options kafka_opt_;
    std::string result_sig_key_;
    kafka_console_producer kafka_prod_;
    bool kafka_enabled_;

    std::atomic<bool> stop_{false};
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::queue<task> q_;
    std::vector<std::thread> threads_;

    // Event-driven (reactor) mode.
    struct delayed_task {
        uint64_t deadline_ns = 0;
        task t;
    };
    struct delayed_cmp {
        bool operator()(const delayed_task& a, const delayed_task& b) const {
            // min-heap by deadline
            return a.deadline_ns > b.deadline_ns;
        }
    };
    std::priority_queue<delayed_task, std::vector<delayed_task>, delayed_cmp> delayed_;

    struct conn_state {
        PGconn* c = nullptr;
        int fd = -1;
        enum class state : uint8_t { IDLE = 0, SENDING = 1, READING = 2 } st = state::IDLE;
        task cur;
        bool has_task = false;
        uint64_t exec_start_ns = 0;
    };
    std::vector<conn_state> conns_;
    std::vector<pollfd> pfds_scratch_;
    std::thread event_thread_;
    bool event_mode_ = false;
    bool det_parallel_workers_ = false;
    bool det_raw_compat_mode_ = false;
    PGconn* bcdb_ctrl_conn_ = nullptr;
    std::mutex bcdb_init_mu_;
    bool bcdb_init_done_ = false;
    bool bcdb_init_failed_ = false;
    int wakeup_rfd_ = -1;
    int wakeup_wfd_ = -1;
    std::atomic<uint64_t> st_backlog_cur_{0};
    std::atomic<uint64_t> st_inflight_cur_{0};
    std::atomic<uint64_t> st_delayed_cur_{0};

    std::atomic<uint64_t> st_enqueued_{0};
    std::atomic<uint64_t> st_dequeued_{0};
    std::atomic<uint64_t> st_q_wait_ns_{0};
    std::atomic<uint64_t> st_queue_delay_ns_{0};
    std::atomic<uint64_t> st_queue_delay_dequeue_ns_{0};
    std::atomic<uint64_t> st_queue_delay_exec_start_ns_{0};
    std::atomic<uint64_t> st_conn_acquire_calls_{0};
    std::atomic<uint64_t> st_conn_acquire_wait_ns_{0};
    std::atomic<uint64_t> st_exec_calls_{0};
    std::atomic<uint64_t> st_exec_ns_{0};
    std::atomic<uint64_t> st_pg_query_ns_{0};
    std::atomic<uint64_t> st_result_format_ns_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_40001_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_40P01_{0};
    std::atomic<uint64_t> st_retryable_sqlstate_57014_{0};
    std::atomic<uint64_t> st_retry_attempts_total_{0};
    std::atomic<uint64_t> st_retry_exhausted_total_{0};
    std::atomic<uint64_t> st_kafka_flush_calls_{0};
    std::atomic<uint64_t> st_kafka_payload_bytes_{0};
    std::atomic<uint64_t> st_kafka_build_payload_ns_{0};
    std::atomic<uint64_t> st_kafka_send_ns_{0};

    size_t queue_high_wm_ = 0;
    size_t queue_low_wm_ = 0;
    std::atomic<uint64_t> st_queue_depth_samples_{0};
    std::atomic<uint64_t> st_queue_depth_sum_{0};
    std::atomic<uint64_t> st_queue_depth_max_{0};
    std::atomic<uint64_t> st_queue_depth_bin_le_16_{0};
    std::atomic<uint64_t> st_queue_depth_bin_17_64_{0};
    std::atomic<uint64_t> st_queue_depth_bin_65_256_{0};
    std::atomic<uint64_t> st_queue_depth_bin_gt_256_{0};
    std::atomic<uint64_t> st_queue_overload_enter_{0};
    std::atomic<uint64_t> st_queue_overload_exit_{0};
    std::atomic<uint64_t> st_queue_depth_cur_{0};
    std::atomic<bool> queue_overloaded_{false};
    std::mutex admission_mu_;
    std::condition_variable admission_cv_;
    std::atomic<bool> st_det_block_seen_{false};
    std::atomic<bool> st_det_block_fallback_seen_{false};
    std::atomic<uint64_t> next_dispatch_seq_{1};
    std::mutex det_emit_mu_;
    std::condition_variable det_emit_cv_;
    uint64_t det_next_emit_seq_ = 1;
    std::mutex det_compat_mu_;
    std::condition_variable det_compat_cv_;
    bool det_compat_inflight_ = false;
    std::mutex det_apply_mu_;
    std::condition_variable det_apply_cv_;
    uint64_t det_next_apply_seq_ = 1;
    bool det_apply_initialized_ = false;
    std::unordered_map<uint64_t, det_tx_state> det_tx_states_;

    bool is_det_prefixed_sql(const std::string& sql) const;
};

} // namespace ariabc_pg
