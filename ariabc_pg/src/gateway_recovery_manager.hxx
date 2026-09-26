#pragma once

#include "ariabc_pg_util.hxx"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>

namespace ariabc_pg {

// Forward declaration
struct gateway_options;

class gateway_recovery_manager {
public:
    struct recovery_task {
        bool is_active = false;
        std::string cmd;
        std::string desc;
        std::string damaged_node_spec;
        uint64_t req_num = 0;
    };

    gateway_recovery_manager(const std::string& recovery_mode,
                             int recovery_interval_ms,
                             int recovery_db_port,
                             const std::string& recovery_db_user,
                             const std::string& recovery_db_name,
                             const std::string& recovery_db_password,
                             const std::string& recovery_table,
                             const std::string& recovery_nodes,
                             const std::string& recovery_hook_script,
                             const std::string& recovery_compare_script,
                             const std::vector<host_port>& nodes,
                             const std::vector<int>& raft_node_ids)
        : mode_(recovery_mode),
          interval_ms_(recovery_interval_ms > 0 ? recovery_interval_ms : 200),
          db_port_(recovery_db_port > 0 ? recovery_db_port : 5438),
          db_user_(recovery_db_user.empty() ? "postgres" : recovery_db_user),
          db_name_(recovery_db_name.empty() ? "postgres" : recovery_db_name),
          db_password_(recovery_db_password),
          table_(recovery_table.empty() ? "auto" : recovery_table),
          nodes_(nodes),
          raft_node_ids_(raft_node_ids),
          stop_(false)
    {
        is_active_ = (mode_ == "active" || mode_ == "both");
        is_passive_ = (mode_ == "passive" || mode_ == "both");

        resolve_scripts(recovery_hook_script, recovery_compare_script);
        resolve_nodes_spec(recovery_nodes);

        if (is_active_ || is_passive_) {
            std::cout << "[recovery_mgr] Initialized ProtectDB Alg 2 Online Recovery:"
                      << " mode=" << mode_
                      << " interval=" << interval_ms_ << "ms"
                      << " db_port=" << db_port_
                      << " table=" << table_
                      << " hook=" << hook_script_
                      << " compare=" << compare_script_
                      << " cluster_spec=" << nodes_spec_csv_
                      << std::endl;

            // Start worker thread for non-blocking asynchronous recovery
            worker_thread_ = std::thread(&gateway_recovery_manager::worker_loop, this);

            // Start passive consistency polling loop if enabled
            if (is_passive_) {
                passive_thread_ = std::thread(&gateway_recovery_manager::passive_loop, this);
            }
        }
    }

    ~gateway_recovery_manager() {
        stop();
    }

    void stop_passive_and_drain() {
        if (!is_passive_) return;
        stop_passive_loop_.store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lk(passive_mu_);
            passive_cv_.notify_all();
        }
        if (passive_thread_.joinable()) {
            passive_thread_.join();
        }

        // Wait for current worker tasks to drain
        while (true) {
            bool empty = false;
            {
                std::lock_guard<std::mutex> lk(queue_mu_);
                empty = task_queue_.empty() && !passive_in_flight_.load(std::memory_order_relaxed);
            }
            if (empty) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        // Run final drain & consistency check to guarantee 0 corruption remains
        std::cout << "[recovery_mgr] Phase 6 workload complete: running final Merkle drain & consistency verification..." << std::endl;
        std::ostringstream cmd;
        cmd << "python3 -u " << compare_script_
            << " --nodes \"" << nodes_spec_csv_ << "\""
            << " --table \"" << table_ << "\""
            << " --once --auto-recover"
            << " --db-user \"" << db_user_ << "\""
            << " --db-name \"" << db_name_ << "\"";
        if (!db_password_.empty()) {
            cmd << " --db-password \"" << db_password_ << "\"";
        }

        recovery_task task;
        task.is_active = false;
        task.cmd = cmd.str();
        task.desc = "final_merkle_drain_check";
        task.damaged_node_spec = "";
        task.req_num = 0;

        bool ok = execute_task_sync(task);
        if (ok) {
            std::cout << "[recovery_mgr] Final cluster Merkle verification: PASS - all replicas fully synchronized with 0 corruption remaining." << std::endl;
        } else {
            std::cerr << "[recovery_mgr] Final cluster Merkle verification: WARNING - mismatch detected during drain!" << std::endl;
        }
    }

    void stop() {
        bool expected = false;
        if (stop_.compare_exchange_strong(expected, true)) {
            stop_passive_loop_.store(true, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lk(passive_mu_);
                passive_cv_.notify_all();
            }
            {
                std::lock_guard<std::mutex> lk(queue_mu_);
                queue_cv_.notify_all();
            }
            if (passive_thread_.joinable()) {
                passive_thread_.join();
            }
            if (worker_thread_.joinable()) {
                worker_thread_.join();
            }
        }
    }

    void trigger_immediate_check() {
        if (!is_passive_) return;
        immediate_trigger_.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lk(passive_mu_);
        passive_cv_.notify_one();
    }

    bool is_active_enabled() const { return is_active_; }
    bool is_passive_enabled() const { return is_passive_; }

    uint64_t triggered_count() const { return recovery_triggered_count_.load(std::memory_order_relaxed); }
    uint64_t success_count() const { return recovery_success_count_.load(std::memory_order_relaxed); }
    uint64_t failure_count() const { return recovery_failure_count_.load(std::memory_order_relaxed); }
    uint64_t total_ms() const { return recovery_total_ms_.load(std::memory_order_relaxed); }

    // In-band per-transaction divergence handler (Active Mode)
    void handle_divergence_note(const std::string& note) {
        if (!is_active_) return;
        if (note.empty()) return;
        if (note.find("vote_store_capacity_exhausted") != std::string::npos) return;

        uint64_t req_num = 0;
        int damaged_node_id = -1;
        int reference_node_id = -1;

        // Parse req_num
        const std::string req_key = "\"req_num\":";
        size_t p_req = note.find(req_key);
        if (p_req != std::string::npos) {
            req_num = std::strtoull(note.c_str() + p_req + req_key.size(), nullptr, 10);
        }

        if (note.find("\"type\":\"result_divergence\"") != std::string::npos) {
            // Parse majority hash from hash_votes
            // Format: "hash_votes":"hash1=count1;hash2=count2"
            std::string majority_hash;
            int max_votes = -1;
            const std::string hv_key = "\"hash_votes\":\"";
            size_t p_hv = note.find(hv_key);
            if (p_hv != std::string::npos) {
                size_t start = p_hv + hv_key.size();
                size_t end = note.find('"', start);
                if (end != std::string::npos) {
                    std::string hv_str = note.substr(start, end - start);
                    std::vector<std::string> pairs = split_char(hv_str, ';');
                    for (const auto& pair : pairs) {
                        size_t eq = pair.find('=');
                        if (eq != std::string::npos) {
                            std::string h = pair.substr(0, eq);
                            int cnt = std::stoi(pair.substr(eq + 1));
                            if (cnt > max_votes) {
                                max_votes = cnt;
                                majority_hash = h;
                            }
                        }
                    }
                }
            }

            // Parse node_results
            // Format: "node_results":"1|sig=1|hash=...;2|sig=1|hash=...;4|sig=1|hash=..."
            const std::string nr_key = "\"node_results\":\"";
            size_t p_nr = note.find(nr_key);
            if (p_nr != std::string::npos) {
                size_t start = p_nr + nr_key.size();
                size_t end = note.find('"', start);
                if (end != std::string::npos) {
                    std::string nr_str = note.substr(start, end - start);
                    std::vector<std::string> entries = split_char(nr_str, ';');
                    for (const auto& entry : entries) {
                        std::vector<std::string> parts = split_char(entry, '|');
                        if (parts.size() >= 3) {
                            int nid = std::stoi(parts[0]);
                            int sig = 1;
                            std::string h;
                            for (size_t k = 1; k < parts.size(); ++k) {
                                if (parts[k].rfind("sig=", 0) == 0) {
                                    sig = std::stoi(parts[k].substr(4));
                                } else if (parts[k].rfind("hash=", 0) == 0) {
                                    h = parts[k].substr(5);
                                }
                            }
                            if (sig == 0 || (!majority_hash.empty() && h != majority_hash)) {
                                if (damaged_node_id == -1) damaged_node_id = nid;
                            } else if (sig == 1 && (majority_hash.empty() || h == majority_hash)) {
                                if (reference_node_id == -1) reference_node_id = nid;
                            }
                        }
                    }
                }
            }
        } else if (note.find("\"type\":\"duplicate_identity_conflict\"") != std::string::npos) {
            const std::string nid_key = "\"node_id\":";
            size_t p_nid = note.find(nid_key);
            if (p_nid != std::string::npos) {
                damaged_node_id = std::stoi(note.substr(p_nid + nid_key.size()));
            }
        }

        if (damaged_node_id == -1) {
            // Could not isolate specific damaged node from note
            return;
        }

        if (reference_node_id == -1) {
            // Pick first node from topology that is not the damaged node
            for (size_t i = 0; i < nodes_.size(); ++i) {
                int nid = (!raft_node_ids_.empty() && i < raft_node_ids_.size()) ? raft_node_ids_[i] : static_cast<int>(i + 1);
                if (nid != damaged_node_id) {
                    reference_node_id = nid;
                    break;
                }
            }
        }

        const std::string dmg_spec = get_node_spec(damaged_node_id);
        const std::string ref_spec = get_node_spec(reference_node_id);

        std::ostringstream cmd;
        cmd << "python3 -u " << hook_script_
            << " --damaged-node \"" << dmg_spec << "\""
            << " --reference-node \"" << ref_spec << "\""
            << " --req-num " << req_num
            << " --table \"" << table_ << "\""
            << " --db-user \"" << db_user_ << "\""
            << " --db-name \"" << db_name_ << "\"";
        if (!db_password_.empty()) {
            cmd << " --db-password \"" << db_password_ << "\"";
        }

        recovery_task task;
        task.is_active = true;
        task.cmd = cmd.str();
        task.desc = "active_recovery(req=" + std::to_string(req_num) + ",dmg=" + std::to_string(damaged_node_id) + ")";
        task.damaged_node_spec = dmg_spec;
        task.req_num = req_num;

        enqueue_task(std::move(task));
    }

private:
    bool execute_task_sync(const recovery_task& t) {
        recovery_triggered_count_.fetch_add(1, std::memory_order_relaxed);
        auto t0 = std::chrono::steady_clock::now();
        std::cout << "[recovery_mgr] >>> START " << t.desc << std::endl;

        std::string full_cmd = t.cmd + " 2>&1";
        FILE* pipe = ::popen(full_cmd.c_str(), "r");
        std::string output;
        int exit_code = -1;
        if (pipe) {
            char buf[512];
            while (::fgets(buf, sizeof(buf), pipe)) {
                output += buf;
                std::cout << "[recovery_mgr] " << buf << std::flush;
            }
            int status = ::pclose(pipe);
            if (WIFEXITED(status)) {
                exit_code = WEXITSTATUS(status);
            }
        } else {
            std::cerr << "[recovery_mgr] popen failed: " << ::strerror(errno) << std::endl;
        }

        auto t1 = std::chrono::steady_clock::now();
        uint64_t dur_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
        recovery_total_ms_.fetch_add(dur_ms, std::memory_order_relaxed);

        if (exit_code == 0) {
            recovery_success_count_.fetch_add(1, std::memory_order_relaxed);
            std::cout << "[recovery_mgr] <<< PASS " << t.desc << " in " << dur_ms << "ms" << std::endl;
            return true;
        } else {
            recovery_failure_count_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "[recovery_mgr] <<< FAIL " << t.desc << " (exit=" << exit_code << ") in " << dur_ms << "ms:\n"
                      << output << std::endl;
            return false;
        }
    }

    void enqueue_task(recovery_task task) {
        std::lock_guard<std::mutex> lk(queue_mu_);
        // De-duplicate if an active recovery for the exact same damaged node is already in flight or queued
        if (task.is_active && !task.damaged_node_spec.empty()) {
            if (active_repairs_.find(task.damaged_node_spec) != active_repairs_.end()) {
                // Already repairing this node, drop duplicate task to avoid stampedes
                return;
            }
            active_repairs_.insert(task.damaged_node_spec);
        }
        if (!task.is_active) {
            if (passive_queued_) {
                return; // Already a passive check waiting in queue
            }
            passive_queued_ = true;
        }

        // Bound queue size to prevent memory explosion under severe failure
        if (task_queue_.size() >= 16) {
            if (!task.is_active) {
                passive_queued_ = false;
                return;
            }
        }

        task_queue_.push(std::move(task));
        queue_cv_.notify_one();
    }

    void worker_loop() {
        while (!stop_.load()) {
            recovery_task t;
            {
                std::unique_lock<std::mutex> lk(queue_mu_);
                queue_cv_.wait(lk, [&] { return !task_queue_.empty() || stop_.load(); });
                if (stop_.load() && task_queue_.empty()) break;
                if (task_queue_.empty()) continue;
                t = std::move(task_queue_.front());
                task_queue_.pop();
                if (!t.is_active) {
                    passive_queued_ = false;
                }
            }

            if (!t.is_active) {
                passive_in_flight_.store(true, std::memory_order_release);
            }

            execute_task_sync(t);

            if (!t.is_active) {
                passive_in_flight_.store(false, std::memory_order_release);
            }

            // Remove from active repairs
            if (t.is_active && !t.damaged_node_spec.empty()) {
                std::lock_guard<std::mutex> lk(queue_mu_);
                active_repairs_.erase(t.damaged_node_spec);
            }
        }
    }

    void passive_loop() {
        const std::chrono::milliseconds interval(std::max(50, interval_ms_));
        while (!stop_.load() && !stop_passive_loop_.load()) {
            {
                std::unique_lock<std::mutex> lk(passive_mu_);
                passive_cv_.wait_for(lk, interval, [&] {
                    return stop_.load() || stop_passive_loop_.load() || immediate_trigger_.load(std::memory_order_relaxed);
                });
                immediate_trigger_.store(false, std::memory_order_relaxed);
            }
            if (stop_.load() || stop_passive_loop_.load()) break;

            if (nodes_spec_csv_.empty()) continue;

            // Skip if worker is already executing a passive check or queue has pending checks
            if (passive_in_flight_.load(std::memory_order_relaxed)) {
                continue;
            }
            {
                std::lock_guard<std::mutex> lk(queue_mu_);
                if (passive_queued_ || task_queue_.size() >= 2) {
                    continue;
                }
            }

            std::ostringstream cmd;
            cmd << "python3 -u " << compare_script_
                << " --nodes \"" << nodes_spec_csv_ << "\""
                << " --table \"" << table_ << "\""
                << " --interval-ms " << interval_ms_
                << " --once --auto-recover"
                << " --db-user \"" << db_user_ << "\""
                << " --db-name \"" << db_name_ << "\"";
            if (!db_password_.empty()) {
                cmd << " --db-password \"" << db_password_ << "\"";
            }

            recovery_task task;
            task.is_active = false;
            task.cmd = cmd.str();
            task.desc = "passive_comparestates_check";
            task.damaged_node_spec = "";
            task.req_num = 0;

            enqueue_task(std::move(task));
        }
    }

    std::string get_node_spec(int node_id) const {
        for (size_t i = 0; i < raft_node_ids_.size(); ++i) {
            if (raft_node_ids_[i] == node_id && i < nodes_.size()) {
                return "node" + std::to_string(node_id) + "=" + nodes_[i].host + ":" + std::to_string(db_port_);
            }
        }
        if (node_id >= 1 && static_cast<size_t>(node_id) <= nodes_.size()) {
            return "node" + std::to_string(node_id) + "=" + nodes_[node_id - 1].host + ":" + std::to_string(db_port_);
        }
        if (!nodes_spec_csv_.empty()) {
            const std::string prefix = "node" + std::to_string(node_id) + "=";
            size_t p = nodes_spec_csv_.find(prefix);
            if (p != std::string::npos) {
                size_t end = nodes_spec_csv_.find(',', p);
                return nodes_spec_csv_.substr(p, (end == std::string::npos ? nodes_spec_csv_.size() - p : end - p));
            }
        }
        if (!nodes_.empty()) {
            return "node" + std::to_string(node_id) + "=" + nodes_[0].host + ":" + std::to_string(db_port_);
        }
        return "node" + std::to_string(node_id) + "=127.0.0.1:" + std::to_string(db_port_);
    }

    void resolve_nodes_spec(const std::string& explicit_nodes) {
        if (!explicit_nodes.empty()) {
            nodes_spec_csv_ = explicit_nodes;
            return;
        }
        std::vector<std::string> items;
        for (size_t i = 0; i < nodes_.size(); ++i) {
            int nid = (!raft_node_ids_.empty() && i < raft_node_ids_.size()) ? raft_node_ids_[i] : static_cast<int>(i + 1);
            items.push_back("node" + std::to_string(nid) + "=" + nodes_[i].host + ":" + std::to_string(db_port_));
        }
        std::ostringstream oss;
        for (size_t i = 0; i < items.size(); ++i) {
            if (i > 0) oss << ",";
            oss << items[i];
        }
        nodes_spec_csv_ = oss.str();
    }

    std::string find_script(const std::string& script_name, const std::string& explicit_path) {
        if (!explicit_path.empty() && ::access(explicit_path.c_str(), R_OK) == 0) {
            return explicit_path;
        }

        const char* env_repo = ::getenv("ARIABC_REPO_ROOT");
        if (env_repo && *env_repo) {
            std::string p = std::string(env_repo) + "/scripts/distributed/recovery/" + script_name;
            if (::access(p.c_str(), R_OK) == 0) return p;
        }

        // Relative to current directory
        std::string p_cwd = "scripts/distributed/recovery/" + script_name;
        if (::access(p_cwd.c_str(), R_OK) == 0) return p_cwd;

        std::string p_rel = "recovery/" + script_name;
        if (::access(p_rel.c_str(), R_OK) == 0) return p_rel;

        // Relative to binary location
        char exe_buf[1024];
        ssize_t len = ::readlink("/proc/self/exe", exe_buf, sizeof(exe_buf) - 1);
        if (len > 0) {
            exe_buf[len] = '\0';
            std::string exe_path(exe_buf);
            size_t pos = exe_path.rfind("/ariabc_pg/");
            if (pos != std::string::npos) {
                std::string repo_root = exe_path.substr(0, pos);
                std::string cand = repo_root + "/scripts/distributed/recovery/" + script_name;
                if (::access(cand.c_str(), R_OK) == 0) return cand;
            }
        }

        return p_cwd;
    }

    void resolve_scripts(const std::string& hook_arg, const std::string& compare_arg) {
        hook_script_ = find_script("active_recovery_hook.py", hook_arg);
        compare_script_ = find_script("compare_states.py", compare_arg);
    }

    std::string mode_;
    int interval_ms_;
    int db_port_;
    std::string db_user_;
    std::string db_name_;
    std::string db_password_;
    std::string table_;
    std::string nodes_spec_csv_;
    std::string hook_script_;
    std::string compare_script_;
    std::vector<host_port> nodes_;
    std::vector<int> raft_node_ids_;

    bool is_active_ = false;
    bool is_passive_ = false;

    std::atomic<bool> stop_;
    std::atomic<bool> stop_passive_loop_{false};
    std::atomic<bool> immediate_trigger_{false};
    std::atomic<bool> passive_in_flight_{false};
    bool passive_queued_ = false;
    std::thread worker_thread_;
    std::thread passive_thread_;

    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::queue<recovery_task> task_queue_;
    std::set<std::string> active_repairs_;

    std::mutex passive_mu_;
    std::condition_variable passive_cv_;

    std::atomic<uint64_t> recovery_triggered_count_{0};
    std::atomic<uint64_t> recovery_success_count_{0};
    std::atomic<uint64_t> recovery_failure_count_{0};
    std::atomic<uint64_t> recovery_total_ms_{0};
};

} // namespace ariabc_pg
