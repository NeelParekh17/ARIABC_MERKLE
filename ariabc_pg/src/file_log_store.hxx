/************************************************************************
 * File-based log store for NuRaft.
 *
 * Provides crash-safe durability for Raft log entries. Designed for
 * healthcare-grade systems where data loss on crash is unacceptable.
 *
 * Storage layout (under log_dir/):
 *   log_<index>.entry  — one file per log entry (serialized log_entry)
 *   log_meta.dat       — start_index (8 bytes, little-endian)
 *
 * Design choices:
 *   - One file per entry: simple, crash-safe, easy to compact.
 *   - fsync on every write: guarantees durability at the cost of latency.
 *   - No memory-mapped I/O: simpler error handling for critical systems.
 *
 * Copyright 2026. Licensed under Apache License, Version 2.0.
 ************************************************************************/

#pragma once

#include "log_store.hxx"

#include <atomic>
#include <map>
#include <mutex>
#include <string>

namespace nuraft {

class file_log_store : public log_store {
public:
    /**
     * @param log_dir  Directory to store log files. Created if absent.
     */
    explicit file_log_store(const std::string& log_dir);

    ~file_log_store();

    __nocopy__(file_log_store);

public:
    ulong next_slot() const override;

    ulong start_index() const override;

    ptr<log_entry> last_entry() const override;

    ulong append(ptr<log_entry>& entry) override;

    void write_at(ulong index, ptr<log_entry>& entry) override;

    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;

    ptr<std::vector<ptr<log_entry>>> log_entries_ext(
            ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;

    ptr<log_entry> entry_at(ulong index) override;

    ulong term_at(ulong index) override;

    ptr<buffer> pack(ulong index, int32 cnt) override;

    void apply_pack(ulong index, buffer& pack) override;

    bool compact(ulong last_log_index) override;

    bool flush() override;

    void close();

    ulong last_durable_index() override;

private:
    static ptr<log_entry> make_clone(const ptr<log_entry>& entry);

    /** Serialize and write a log_entry to disk at the given index. */
    bool write_entry_to_disk(ulong index, const ptr<log_entry>& entry);

    /** Read and deserialize a log_entry from disk at the given index. */
    ptr<log_entry> read_entry_from_disk(ulong index) const;

    /** Delete a log entry file from disk. */
    void delete_entry_from_disk(ulong index) const;

    /** Persist the start_index to log_meta.dat. */
    void save_start_index();

    /** Load the start_index from log_meta.dat. */
    void load_start_index();

    /** Scan the log directory and populate the in-memory index. */
    void scan_log_dir();

    /** Build the file path for a given log index. */
    std::string entry_path(ulong index) const;
    std::string meta_path() const;

    /** Log directory path. */
    std::string log_dir_;

    /**
     * In-memory cache of log entries.
     * All entries on disk are also cached here for fast access.
     */
    std::map<ulong, ptr<log_entry>> logs_;

    /** Lock for logs_ and start_idx_. */
    mutable std::mutex logs_lock_;

    /** The index of the first log entry. */
    std::atomic<ulong> start_idx_;
};

} // namespace nuraft
