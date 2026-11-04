#pragma once

/*
 * Small persistent thread pool for data-parallel loops (used to verify the
 * Ed25519 signatures of one log entry's items in parallel).  run() blocks
 * until fn(i) has returned for every i in [0, n); the calling thread takes
 * part.  One run() at a time; concurrent callers are serialised.
 */

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace ariabc_pg {

class parallel_for_pool {
public:
    explicit parallel_for_pool(size_t helper_threads) {
        for (size_t i = 0; i < helper_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~parallel_for_pool() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) t.join();
    }

    parallel_for_pool(const parallel_for_pool&) = delete;
    parallel_for_pool& operator=(const parallel_for_pool&) = delete;

    size_t helper_threads() const { return workers_.size(); }

    // Below min_parallel items the loop runs inline on the caller.
    void run(size_t n, const std::function<void(size_t)>& fn, size_t min_parallel = 16) {
        if (n == 0) return;
        if (workers_.empty() || n < min_parallel) {
            for (size_t i = 0; i < n; ++i) fn(i);
            return;
        }
        std::lock_guard<std::mutex> run_lk(run_mu_);
        {
            std::lock_guard<std::mutex> lk(mu_);
            fn_ = &fn;
            n_ = n;
            next_.store(0, std::memory_order_relaxed);
            done_ = 0;
            active_ = workers_.size();
            ++generation_;
        }
        cv_.notify_all();
        const size_t mine = drain();
        std::unique_lock<std::mutex> lk(mu_);
        done_ += mine;
        done_cv_.wait(lk, [this] { return done_ == n_ && active_ == 0; });
        fn_ = nullptr;
    }

private:
    size_t drain() {
        size_t count = 0;
        for (;;) {
            const size_t i = next_.fetch_add(1, std::memory_order_relaxed);
            if (i >= n_) break;
            (*fn_)(i);
            ++count;
        }
        return count;
    }

    void worker_loop() {
        uint64_t seen = 0;
        std::unique_lock<std::mutex> lk(mu_);
        for (;;) {
            cv_.wait(lk, [&] { return stop_ || generation_ != seen; });
            if (stop_) return;
            seen = generation_;
            lk.unlock();
            const size_t mine = drain();
            lk.lock();
            done_ += mine;
            --active_;
            if (done_ == n_ && active_ == 0) done_cv_.notify_one();
        }
    }

    std::vector<std::thread> workers_;
    std::mutex run_mu_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;
    const std::function<void(size_t)>* fn_ = nullptr;
    size_t n_ = 0;
    std::atomic<size_t> next_{0};
    size_t done_ = 0;
    size_t active_ = 0;
    uint64_t generation_ = 0;
    bool stop_ = false;
};

} // namespace ariabc_pg
