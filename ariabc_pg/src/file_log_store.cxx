/************************************************************************
 * File-based log store implementation for NuRaft.
 *
 * Each log entry is stored as a separate file: log_dir/log_<index>.entry
 * Metadata (start_index) is stored in: log_dir/log_meta.dat
 *
 * All writes are followed by fsync() to guarantee durability.
 * On startup, the directory is scanned to rebuild the in-memory index.
 *
 * Copyright 2026. Licensed under Apache License, Version 2.0.
 ************************************************************************/

#include "file_log_store.hxx"

#include "nuraft.hxx"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

namespace nuraft {

file_log_store::file_log_store(const std::string& log_dir)
    : log_dir_(log_dir)
    , start_idx_(1)
{
    // Create log directory if it doesn't exist.
    ::mkdir(log_dir.c_str(), 0755);

    // Load metadata and scan existing entries.
    load_start_index();
    scan_log_dir();

    // Ensure dummy entry for index 0 exists in memory (NuRaft convention).
    if (logs_.find(0) == logs_.end()) {
        ptr<buffer> buf = buffer::alloc(sz_ulong);
        logs_[0] = cs_new<log_entry>(0, buf);
    }

    std::cerr << "file_log_store: loaded " << (logs_.size() - 1)
              << " entries from " << log_dir_
              << " start_idx=" << start_idx_.load()
              << std::endl;
}

file_log_store::~file_log_store() {}

std::string file_log_store::entry_path(ulong index) const {
    return log_dir_ + "/log_" + std::to_string(index) + ".entry";
}

std::string file_log_store::meta_path() const {
    return log_dir_ + "/log_meta.dat";
}

ptr<log_entry> file_log_store::make_clone(const ptr<log_entry>& entry) {
    ptr<log_entry> clone = cs_new<log_entry>(
        entry->get_term(),
        buffer::clone(entry->get_buf()),
        entry->get_val_type(),
        entry->get_timestamp(),
        entry->has_crc32(),
        entry->get_crc32(),
        false);
    return clone;
}

bool file_log_store::write_entry_to_disk(ulong index, const ptr<log_entry>& entry) {
    const std::string path = entry_path(index);
    ptr<buffer> serialized = entry->serialize();
    const size_t data_size = serialized->size();

    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "file_log_store: open failed for " << path
                  << ": " << ::strerror(errno) << std::endl;
        return false;
    }

    const char* data = reinterpret_cast<const char*>(serialized->data());
    size_t written = 0;
    while (written < data_size) {
        ssize_t n = ::write(fd, data + written, data_size - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "file_log_store: write failed for " << path
                      << ": " << ::strerror(errno) << std::endl;
            ::close(fd);
            return false;
        }
        written += static_cast<size_t>(n);
    }

    // fsync to guarantee durability — critical for patient safety.
    if (::fsync(fd) != 0) {
        std::cerr << "file_log_store: fsync failed for " << path
                  << ": " << ::strerror(errno) << std::endl;
        ::close(fd);
        return false;
    }

    ::close(fd);
    return true;
}

ptr<log_entry> file_log_store::read_entry_from_disk(ulong index) const {
    const std::string path = entry_path(index);

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;

    struct stat st;
    if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
        ::close(fd);
        return nullptr;
    }

    const size_t file_size = static_cast<size_t>(st.st_size);
    ptr<buffer> buf = buffer::alloc(file_size);
    char* data = reinterpret_cast<char*>(buf->data());

    size_t total_read = 0;
    while (total_read < file_size) {
        ssize_t n = ::read(fd, data + total_read, file_size - total_read);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return nullptr;
        }
        if (n == 0) break; // unexpected EOF
        total_read += static_cast<size_t>(n);
    }
    ::close(fd);

    if (total_read != file_size) return nullptr;

    buf->pos(0);
    return log_entry::deserialize(*buf);
}

void file_log_store::delete_entry_from_disk(ulong index) const {
    const std::string path = entry_path(index);
    ::unlink(path.c_str());
}

void file_log_store::save_start_index() {
    const std::string path = meta_path();
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "file_log_store: failed to save meta: "
                  << ::strerror(errno) << std::endl;
        return;
    }
    ulong idx = start_idx_.load();
    ::write(fd, &idx, sizeof(idx));
    ::fsync(fd);
    ::close(fd);
}

void file_log_store::load_start_index() {
    const std::string path = meta_path();
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        start_idx_ = 1;
        return;
    }
    ulong idx = 1;
    ssize_t n = ::read(fd, &idx, sizeof(idx));
    ::close(fd);
    if (n == sizeof(idx) && idx >= 1) {
        start_idx_ = idx;
    }
}

void file_log_store::scan_log_dir() {
    DIR* dir = ::opendir(log_dir_.c_str());
    if (!dir) return;

    struct dirent* ent;
    while ((ent = ::readdir(dir)) != nullptr) {
        const std::string name(ent->d_name);
        // Parse "log_<index>.entry"
        if (name.size() < 11) continue; // "log_X.entry" = 11 chars min
        if (name.substr(0, 4) != "log_") continue;
        size_t dot_pos = name.find(".entry");
        if (dot_pos == std::string::npos) continue;
        const std::string idx_str = name.substr(4, dot_pos - 4);
        ulong idx = 0;
        try {
            idx = std::stoull(idx_str);
        } catch (...) {
            continue;
        }

        ptr<log_entry> entry = read_entry_from_disk(idx);
        if (entry) {
            logs_[idx] = entry;
        }
    }
    ::closedir(dir);
}

// ---- log_store interface implementation ----

ulong file_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(logs_lock_);
    // Exclude the dummy entry at index 0.
    return start_idx_ + logs_.size() - 1;
}

ulong file_log_store::start_index() const {
    return start_idx_;
}

ptr<log_entry> file_log_store::last_entry() const {
    ulong next_idx = next_slot();
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.find(next_idx - 1);
    if (entry == logs_.end()) {
        entry = logs_.find(0);
    }
    return make_clone(entry->second);
}

ulong file_log_store::append(ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(logs_lock_);
    ulong idx = start_idx_ + logs_.size() - 1;
    logs_[idx] = clone;
    write_entry_to_disk(idx, clone);
    return idx;
}

void file_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    ptr<log_entry> clone = make_clone(entry);

    std::lock_guard<std::mutex> l(logs_lock_);
    // Discard all logs >= index.
    auto itr = logs_.lower_bound(index);
    while (itr != logs_.end()) {
        delete_entry_from_disk(itr->first);
        itr = logs_.erase(itr);
    }
    logs_[index] = clone;
    write_entry_to_disk(index, clone);
}

ptr<std::vector<ptr<log_entry>>>
    file_log_store::log_entries(ulong start, ulong end)
{
    ptr<std::vector<ptr<log_entry>>> ret =
        cs_new<std::vector<ptr<log_entry>>>();
    ret->resize(end - start);
    ulong cc = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(ii);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
                assert(0);
            }
            src = entry->second;
        }
        (*ret)[cc++] = make_clone(src);
    }
    return ret;
}

ptr<std::vector<ptr<log_entry>>>
    file_log_store::log_entries_ext(ulong start, ulong end,
                                    int64 batch_size_hint_in_bytes)
{
    ptr<std::vector<ptr<log_entry>>> ret =
        cs_new<std::vector<ptr<log_entry>>>();
    if (batch_size_hint_in_bytes < 0) return ret;

    size_t accum_size = 0;
    for (ulong ii = start; ii < end; ++ii) {
        ptr<log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(ii);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
                assert(0);
            }
            src = entry->second;
        }
        ret->push_back(make_clone(src));
        accum_size += src->get_buf().size();
        if (batch_size_hint_in_bytes &&
            accum_size >= (ulong)batch_size_hint_in_bytes) break;
    }
    return ret;
}

ptr<log_entry> file_log_store::entry_at(ulong index) {
    ptr<log_entry> src = nullptr;
    {
        std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.find(index);
        if (entry == logs_.end()) {
            entry = logs_.find(0);
        }
        src = entry->second;
    }
    return make_clone(src);
}

ulong file_log_store::term_at(ulong index) {
    ulong term = 0;
    {
        std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.find(index);
        if (entry == logs_.end()) {
            entry = logs_.find(0);
        }
        term = entry->second->get_term();
    }
    return term;
}

ptr<buffer> file_log_store::pack(ulong index, int32 cnt) {
    std::vector<ptr<buffer>> logs;
    size_t size_total = 0;
    for (ulong ii = index; ii < index + cnt; ++ii) {
        ptr<log_entry> le = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            le = logs_[ii];
        }
        assert(le.get());
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back(buf);
    }

    ptr<buffer> buf_out = buffer::alloc(
        sizeof(int32) + cnt * sizeof(int32) + size_total);
    buf_out->pos(0);
    buf_out->put((int32)cnt);
    for (auto& entry : logs) {
        ptr<buffer>& bb = entry;
        buf_out->put((int32)bb->size());
        buf_out->put(*bb);
    }
    return buf_out;
}

void file_log_store::apply_pack(ulong index, buffer& pack) {
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 ii = 0; ii < num_logs; ++ii) {
        ulong cur_idx = index + ii;
        int32 buf_size = pack.get_int();
        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);
        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            logs_[cur_idx] = le;
        }
        write_entry_to_disk(cur_idx, le);
    }

    {
        std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.upper_bound(0);
        if (entry != logs_.end()) {
            start_idx_ = entry->first;
        } else {
            start_idx_ = 1;
        }
    }
    save_start_index();
}

bool file_log_store::compact(ulong last_log_index) {
    std::lock_guard<std::mutex> l(logs_lock_);
    for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
        auto entry = logs_.find(ii);
        if (entry != logs_.end()) {
            delete_entry_from_disk(ii);
            logs_.erase(entry);
        }
    }
    if (start_idx_ <= last_log_index) {
        start_idx_ = last_log_index + 1;
    }
    // save_start_index must be called outside the lock or after releasing.
    // Since logs_lock_ is not recursive, persist synchronously here:
    const std::string path = meta_path();
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        ulong idx = start_idx_.load();
        ::write(fd, &idx, sizeof(idx));
        ::fsync(fd);
        ::close(fd);
    }
    return true;
}

bool file_log_store::flush() {
    // All writes are already fsynced individually.
    // Nothing extra needed.
    return true;
}

void file_log_store::close() {}

ulong file_log_store::last_durable_index() {
    return next_slot() - 1;
}

} // namespace nuraft
