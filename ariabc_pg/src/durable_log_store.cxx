// ariabc_pg/src/durable_log_store.cxx
// Durable append-only segmented Raft log store implementation.

#include "durable_log_store.hxx"
#include "nuraft.hxx"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace ariabc_raft {

durable_log_store::durable_log_store(const std::string& log_dir, uint64_t max_segment_size)
    : log_dir_(log_dir)
    , start_index_(1)
    , last_durable_idx_(0)
    , dirty_(false)
    , max_segment_size_(max_segment_size)
{
    manifest_path_ = log_dir_ + "/manifest.bin";
    open_or_create();
}

durable_log_store::~durable_log_store() {
    close();
}

nuraft::ptr<nuraft::log_entry> durable_log_store::make_clone(const nuraft::ptr<nuraft::log_entry>& e) {
    if (!e) return nullptr;
    return nuraft::cs_new<nuraft::log_entry>(
        e->get_term(),
        nuraft::buffer::clone(e->get_buf()),
        e->get_val_type(),
        e->get_timestamp(),
        e->has_crc32(),
        e->get_crc32(),
        false
    );
}

nuraft::ptr<nuraft::log_entry> durable_log_store::sentinel_entry() const {
    // Dummy entry for index 0.
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(sizeof(uint64_t));
    buf->pos(0);
    buf->put((uint64_t)0);
    return nuraft::cs_new<nuraft::log_entry>(0, buf);
}

uint64_t durable_log_store::next_slot_unlocked() const {
    if (logs_.empty()) {
        return start_index_;
    }
    return logs_.rbegin()->first + 1;
}

nuraft::ulong durable_log_store::next_slot() const {
    std::lock_guard<std::mutex> l(mu_);
    return next_slot_unlocked();
}

nuraft::ulong durable_log_store::start_index() const {
    std::lock_guard<std::mutex> l(mu_);
    return start_index_;
}

nuraft::ptr<nuraft::log_entry> durable_log_store::last_entry() const {
    std::lock_guard<std::mutex> l(mu_);
    if (logs_.empty()) {
        return sentinel_entry();
    }
    return make_clone(logs_.rbegin()->second);
}

std::string durable_log_store::segment_path(uint64_t first_index) const {
    char buf[64];
    snprintf(buf, sizeof(buf), "/segment_%020llu.log", (unsigned long long)first_index);
    return log_dir_ + buf;
}

int durable_log_store::segment_fd(size_t seg_idx) {
    if (segments_[seg_idx].fd < 0) {
        int fd = ::open(segments_[seg_idx].path.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd < 0) {
            throw storage_io_error("Failed to open segment file: " + segments_[seg_idx].path + " error: " + strerror(errno));
        }
        // Seek to end
        if (::lseek(fd, 0, SEEK_END) < 0) {
            ::close(fd);
            throw storage_io_error("Failed to seek end of segment: " + segments_[seg_idx].path);
        }
        segments_[seg_idx].fd = fd;
    }
    return segments_[seg_idx].fd;
}

void durable_log_store::create_segment(uint64_t first_index) {
    Segment seg;
    seg.first_index = first_index;
    seg.path = segment_path(first_index);
    seg.fd = ::open(seg.path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (seg.fd < 0) {
        throw storage_io_error("Failed to create segment file: " + seg.path + " error: " + strerror(errno));
    }
    seg.size = 0;
    seg.is_active = true;

    // Write header
    uint8_t seg_hdr[16];
    memset(seg_hdr, 0, sizeof(seg_hdr));
    ::memcpy(seg_hdr, "RAFT_SEG", 8);
    seg_hdr[8] = (uint8_t)(FORMAT_VER & 0xFF);
    seg_hdr[9] = (uint8_t)((FORMAT_VER >> 8) & 0xFF);
    seg_hdr[10] = (uint8_t)((FORMAT_VER >> 16) & 0xFF);
    seg_hdr[11] = (uint8_t)((FORMAT_VER >> 24) & 0xFF);
    write_full(seg.fd, seg_hdr, sizeof(seg_hdr), seg.path);
    seg.size += sizeof(seg_hdr);

    fdatasync_and_profile(seg.fd, "create_segment fdatasync");
    
    if (!segments_.empty()) {
        segments_.back().is_active = false;
        size_t old_active_idx = segments_.size() - 1;
        if (dirty_segments_.count(old_active_idx)) {
            if (segments_.back().fd >= 0) {
                fdatasync_and_profile(segments_.back().fd, "closing dirty segment sync");
            }
            dirty_segments_.erase(old_active_idx);
        }
        if (segments_.back().fd >= 0) {
            ::close(segments_.back().fd);
            segments_.back().fd = -1;
        }
    }
    segments_.push_back(seg);
    save_manifest();
    fsync_directory_or_throw(log_dir_);
}

void durable_log_store::rotate_segment_if_needed(uint64_t first_record_index) {
    if (segments_.empty()) {
        create_segment(first_record_index);
        return;
    }
    if (segments_.back().size >= max_segment_size_) {
        create_segment(first_record_index);
    }
}

std::vector<uint8_t> durable_log_store::build_header(
    uint32_t rtype, uint64_t raft_idx, uint64_t raft_term,
    uint32_t payload_len, uint32_t payload_crc) const
{
    std::vector<uint8_t> hdr;
    hdr.reserve(HEADER_SIZE);
    put_le32(hdr, MAGIC);
    put_le32(hdr, FORMAT_VER);
    put_le32(hdr, rtype);
    put_le32(hdr, (uint32_t)(HEADER_SIZE + payload_len));
    put_le64(hdr, raft_idx);
    put_le64(hdr, raft_term);
    put_le32(hdr, payload_len);
    put_le32(hdr, payload_crc);
    
    // Header CRC is CRC of the first 40 bytes (HEADER_CRC_INPUT_SIZE)
    uint32_t hcrc = crc32_bytes(hdr.data(), HEADER_CRC_INPUT_SIZE);
    put_le32(hdr, hcrc);
    return hdr;
}

void durable_log_store::write_entry_record(
    uint64_t raft_idx, uint64_t raft_term, const std::vector<uint8_t>& payload)
{
    rotate_segment_if_needed(raft_idx);
    size_t active_idx = segments_.size() - 1;
    int fd = segment_fd(active_idx);
    uint64_t offset = segments_[active_idx].size;

    uint32_t pcrc = crc32_bytes(payload.data(), payload.size());
    std::vector<uint8_t> hdr = build_header(RT_ENTRY, raft_idx, raft_term, payload.size(), pcrc);

    write_full(fd, hdr.data(), hdr.size(), segments_[active_idx].path);
    if (!payload.empty()) {
        write_full(fd, payload.data(), payload.size(), segments_[active_idx].path);
    }

    segments_[active_idx].size += HEADER_SIZE + payload.size();
    
    log_location loc;
    loc.segment_seq = segments_[active_idx].first_index;
    loc.file_offset = offset;
    loc.record_size = HEADER_SIZE + payload.size();
    index_locations_[raft_idx] = loc;

    profile_.bytes_appended += loc.record_size;
    dirty_segments_.insert(active_idx);
}

void durable_log_store::write_truncate_record(uint64_t from_index) {
    // 1. Identify obsolete segments (first_index >= from_index)
    std::vector<Segment> obsolete_segments;
    while (!segments_.empty() && segments_.back().first_index >= from_index) {
        obsolete_segments.push_back(segments_.back());
        size_t idx = segments_.size() - 1;
        dirty_segments_.erase(idx);
        segments_.pop_back();
    }

    // 2. Ensure we have an active segment and write the TRUNCATE marker to it
    rotate_segment_if_needed(from_index);
    size_t active_idx = segments_.size() - 1;
    int fd = segment_fd(active_idx);

    std::vector<uint8_t> hdr = build_header(RT_TRUNCATE, from_index, 0, 0, 0);
    write_full(fd, hdr.data(), hdr.size(), segments_[active_idx].path);
    segments_[active_idx].size += HEADER_SIZE;
    dirty_segments_.insert(active_idx);

    // 3. fdatasync the marker's segment
    fdatasync_and_profile(fd, "truncate marker sync");

    // 4. Update the manifest file on disk
    save_manifest();

    // 5. fsync log directory to commit the manifest update
    fsync_directory_or_throw(log_dir_);

    // 6. Physically unlink the obsolete segments
    for (auto& o_seg : obsolete_segments) {
        if (o_seg.fd >= 0) {
            ::close(o_seg.fd);
            o_seg.fd = -1;
        }
        bool keep = false;
        for (auto& seg : segments_) {
            if (seg.first_index == o_seg.first_index) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            ::unlink(o_seg.path.c_str());
        }
    }

    // 7. fsync log directory again to commit the unlinks
    fsync_directory_or_throw(log_dir_);
}

nuraft::ulong durable_log_store::append(nuraft::ptr<nuraft::log_entry>& entry) {
    std::lock_guard<std::mutex> l(mu_);
    uint64_t idx = next_slot_unlocked();
    
    nuraft::ptr<nuraft::buffer> buf = entry->serialize();
    std::vector<uint8_t> payload(buf->size());
    buf->pos(0);
    ::memcpy(payload.data(), buf->data(), buf->size());

    write_entry_record(idx, entry->get_term(), payload);

    logs_[idx] = make_clone(entry);
    dirty_ = true;

    if (batch_first_ == 0) {
        batch_first_ = idx;
    }
    batch_last_ = idx;

    profile_.append_calls.fetch_add(1, std::memory_order_relaxed);
    return idx;
}

void durable_log_store::write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) {
    std::lock_guard<std::mutex> l(mu_);
    write_truncate_record(index);

    // Truncate in memory cache
    auto it = logs_.lower_bound(index);
    while (it != logs_.end()) {
        index_locations_.erase(it->first);
        it = logs_.erase(it);
    }

    nuraft::ptr<nuraft::buffer> buf = entry->serialize();
    std::vector<uint8_t> payload(buf->size());
    buf->pos(0);
    ::memcpy(payload.data(), buf->data(), buf->size());

    write_entry_record(index, entry->get_term(), payload);
    logs_[index] = make_clone(entry);
    dirty_ = true;

    sync_dirty_segments_unlocked("write_at sync");
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
durable_log_store::log_entries(nuraft::ulong start, nuraft::ulong end) {
    std::lock_guard<std::mutex> l(mu_);
    auto ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    ret->resize(end - start);
    size_t cc = 0;
    for (uint64_t idx = start; idx < end; ++idx) {
        auto it = logs_.find(idx);
        if (it != logs_.end()) {
            (*ret)[cc++] = make_clone(it->second);
        } else {
            (*ret)[cc++] = nullptr;
        }
    }
    return ret;
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
durable_log_store::log_entries_ext(nuraft::ulong start, nuraft::ulong end, nuraft::int64 batch_size_hint_in_bytes) {
    std::lock_guard<std::mutex> l(mu_);
    auto ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    size_t accum_size = 0;
    for (uint64_t idx = start; idx < end; ++idx) {
        auto it = logs_.find(idx);
        if (it == logs_.end()) break;
        ret->push_back(make_clone(it->second));
        accum_size += it->second->get_buf().size();
        if (batch_size_hint_in_bytes && accum_size >= (size_t)batch_size_hint_in_bytes) {
            break;
        }
    }
    return ret;
}

nuraft::ptr<nuraft::log_entry> durable_log_store::entry_at(nuraft::ulong index) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = logs_.find(index);
    if (it == logs_.end()) return nullptr;
    return make_clone(it->second);
}

nuraft::ulong durable_log_store::term_at(nuraft::ulong index) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = logs_.find(index);
    if (it == logs_.end()) return 0;
    return it->second->get_term();
}

nuraft::ptr<nuraft::buffer> durable_log_store::pack(nuraft::ulong index, nuraft::int32 cnt) {
    std::lock_guard<std::mutex> l(mu_);
    if (cnt < 0) {
        throw std::runtime_error("pack: cnt must be non-negative: " + std::to_string(cnt));
    }
    if (index + cnt < index) {
        throw std::runtime_error("pack: index + cnt overflow");
    }
    std::vector<nuraft::ptr<nuraft::buffer>> serialized_logs;
    size_t size_total = 0;
    for (uint64_t ii = index; ii < index + cnt; ++ii) {
        auto it = logs_.find(ii);
        if (it == logs_.end()) {
            throw std::runtime_error("pack: index not found: " + std::to_string(ii));
        }
        nuraft::ptr<nuraft::buffer> buf = it->second->serialize();
        size_total += buf->size();
        if (size_total < buf->size()) {
            throw std::runtime_error("pack: size_total overflow");
        }
        serialized_logs.push_back(buf);
    }
    size_t required_alloc = sizeof(int32_t) + (size_t)cnt * sizeof(int32_t) + size_total;
    if (required_alloc < size_total) {
        throw std::runtime_error("pack: allocation size overflow");
    }
    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(required_alloc);
    buf_out->pos(0);
    buf_out->put((int32_t)cnt);
    for (auto& entry : serialized_logs) {
        buf_out->put((int32_t)entry->size());
        buf_out->put(*entry);
    }
    return buf_out;
}

void durable_log_store::apply_pack(nuraft::ulong index, nuraft::buffer& pack) {
    std::lock_guard<std::mutex> l(mu_);
    if (pack.size() < sizeof(int32_t)) {
        throw std::runtime_error("apply_pack: missing entry count");
    }
    pack.pos(0);
    int32_t num_logs = pack.get_int();

    // Input bounds validation (Item 8 from plan_left.md)
    if (num_logs < 0 || (size_t)num_logs > pack.size()) {
        throw std::runtime_error("apply_pack: invalid/malformed num_logs: " + std::to_string(num_logs));
    }
    if (index + num_logs < index) {
        throw std::runtime_error("apply_pack: index + num_logs overflow");
    }

    // Verify/decode all first
    std::vector<nuraft::ptr<nuraft::log_entry>> decoded_entries;
    decoded_entries.reserve(num_logs);
    for (int32_t ii = 0; ii < num_logs; ++ii) {
        if (pack.pos() + sizeof(int32_t) > pack.size()) {
            throw std::runtime_error("apply_pack: buffer underflow reading entry size");
        }
        int32_t buf_size = pack.get_int();
        if (buf_size < 0 || (size_t)buf_size > (pack.size() - pack.pos())) {
            throw std::runtime_error("apply_pack: entry size out of bounds or negative: " + std::to_string(buf_size));
        }
        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        pack.get(buf_local);
        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf_local);
        if (!le) {
            throw std::runtime_error("apply_pack: failed to deserialize entry");
        }
        decoded_entries.push_back(le);
    }
    if (pack.pos() != pack.size()) {
        throw std::runtime_error("apply_pack: trailing/malformed bytes in pack: pos=" + std::to_string(pack.pos()) + ", size=" + std::to_string(pack.size()));
    }

    // Write truncate record
    write_truncate_record(index);

    // Erase memory cache from index
    auto it = logs_.lower_bound(index);
    while (it != logs_.end()) {
        index_locations_.erase(it->first);
        it = logs_.erase(it);
    }

    // Write entries to segment
    for (int32_t ii = 0; ii < num_logs; ++ii) {
        uint64_t cur_idx = index + ii;
        nuraft::ptr<nuraft::buffer> buf = decoded_entries[ii]->serialize();
        std::vector<uint8_t> payload(buf->size());
        buf->pos(0);
        ::memcpy(payload.data(), buf->data(), buf->size());

        write_entry_record(cur_idx, decoded_entries[ii]->get_term(), payload);
        logs_[cur_idx] = decoded_entries[ii];
    }
    dirty_ = true;

    // Flush to disk
    sync_dirty_segments_unlocked("apply_pack sync");

    // Adjust start index
    auto start_it = logs_.upper_bound(0);
    if (start_it != logs_.end()) {
        start_index_ = start_it->first;
    } else {
        start_index_ = 1;
    }
}

bool durable_log_store::compact(nuraft::ulong last_log_index) {
    // Compaction is explicitly disabled in this phase.
    throw std::runtime_error("Compaction is disabled in this version of durable_log_store.");
}

bool durable_log_store::flush() {
    std::lock_guard<std::mutex> l(mu_);
    if (!dirty_) return true;
    sync_dirty_segments_unlocked("flush sync");
    return true;
}

void durable_log_store::close() {
    std::lock_guard<std::mutex> l(mu_);
    if (dirty_) {
        try {
            sync_dirty_segments_unlocked("close sync");
        } catch (const std::exception& e) {
            std::cerr << "RAFT_STORAGE_FATAL: failed to flush log store on close: " << e.what() << std::endl;
            std::terminate();
        }
    }
    for (auto& seg : segments_) {
        if (seg.fd >= 0) {
            ::close(seg.fd);
            seg.fd = -1;
        }
    }
}

nuraft::ulong durable_log_store::last_durable_index() {
    std::lock_guard<std::mutex> l(mu_);
    return last_durable_idx_;
}

void durable_log_store::end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) {
    std::lock_guard<std::mutex> l(mu_);
    if (cnt == 0) return;

    if (batch_first_ != start || batch_last_ != start + cnt - 1) {
        // Tolerantly handle range mismatch or out-of-order batches.
        // We sync whatever is dirty to maintain durability of all written records.
        sync_dirty_segments_unlocked("end_of_append_batch sync (range mismatch fallback)");
    } else {
        sync_dirty_segments_unlocked("end_of_append_batch sync");
    }

    profile_.append_batches.fetch_add(1, std::memory_order_relaxed);
    profile_.append_batch_entries_total.fetch_add(cnt, std::memory_order_relaxed);

    uint64_t cur_max_entries = profile_.append_batch_entries_max.load(std::memory_order_relaxed);
    while (cnt > cur_max_entries && !profile_.append_batch_entries_max.compare_exchange_weak(cur_max_entries, cnt)) {}

    batch_first_ = 0;
    batch_last_ = 0;
}

void durable_log_store::open_or_create() {
    ensure_directory(log_dir_);
    load_manifest();
    scan_and_recover();
}

void durable_log_store::load_manifest() {
    struct stat st;
    DIR* dir = ::opendir(log_dir_.c_str());
    if (dir) {
        struct dirent* entry;
        bool has_segments = false;
        while ((entry = ::readdir(dir)) != nullptr) {
            std::string name = entry->d_name;
            if (name.find("segment_") == 0 && name.find(".log") != std::string::npos) {
                has_segments = true;
                break;
            }
        }
        ::closedir(dir);
        if (has_segments && ::stat(manifest_path_.c_str(), &st) != 0) {
            throw storage_corruption_error("Missing manifest file in non-empty log directory");
        }
    }

    if (::stat(manifest_path_.c_str(), &st) != 0) {
        // Manifest doesn't exist, create it empty
        save_manifest();
        return;
    }

    std::vector<uint8_t> raw = read_entire_file(manifest_path_);
    if (raw.size() < 16) {
        throw storage_corruption_error("Manifest file too short or corrupt");
    }

    // Check manifest CRC (last 4 bytes are CRC of everything before)
    uint32_t expected_mcrc = get_le32(raw.data() + raw.size() - 4);
    uint32_t actual_mcrc = crc32_bytes(raw.data(), raw.size() - 4);
    if (actual_mcrc != expected_mcrc) {
        throw storage_corruption_error("Manifest file CRC mismatch");
    }

    uint32_t magic = get_le32(raw.data());
    uint32_t ver = get_le32(raw.data() + 4);
    uint32_t num_segs = get_le32(raw.data() + 8);
    if (magic != MAGIC || ver != FORMAT_VER) {
        throw storage_corruption_error("Manifest file corrupt or unknown version");
    }

    // Exact length check
    if (raw.size() != 16 + num_segs * 8) {
        throw storage_corruption_error("Manifest size does not match segment count");
    }

    const uint8_t* p = raw.data() + 12;
    uint64_t prev_first_idx = 0;
    for (uint32_t i = 0; i < num_segs; ++i) {
        uint64_t fidx = get_le64(p);
        p += 8;

        // segment-order validation & duplicate detection
        if (fidx == 0) {
            throw storage_corruption_error("Manifest contains invalid segment first_index=0");
        }
        if (i > 0 && fidx <= prev_first_idx) {
            throw storage_corruption_error("Manifest segments are out of order or duplicate first_index");
        }
        prev_first_idx = fidx;

        Segment seg;
        seg.first_index = fidx;
        seg.path = segment_path(fidx);
        seg.fd = -1;
        seg.size = 0;
        seg.is_active = false;
        segments_.push_back(seg);
    }
    if (!segments_.empty()) {
        segments_.back().is_active = true;
    }
}

void durable_log_store::save_manifest() {
    std::vector<uint8_t> raw;
    put_le32(raw, MAGIC);
    put_le32(raw, FORMAT_VER);
    put_le32(raw, (uint32_t)segments_.size());
    for (auto& seg : segments_) {
        put_le64(raw, seg.first_index);
    }
    uint32_t mcrc = crc32_bytes(raw.data(), raw.size());
    put_le32(raw, mcrc);
    atomic_write_file(manifest_path_, raw);
}

void durable_log_store::process_record_on_recovery(
    const uint8_t* header, const std::vector<uint8_t>& payload, uint64_t file_offset, size_t seg_idx)
{
    uint32_t rtype = get_le32(header + 8);
    uint64_t raft_idx = get_le64(header + 16);
    uint64_t raft_term = get_le64(header + 24);

    if (rtype == RT_ENTRY) {
        nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(payload.size());
        buf->pos(0);
        ::memcpy(buf->data(), payload.data(), payload.size());
        buf->pos(0);
        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf);
        if (!le) {
            throw storage_corruption_error("Failed to deserialize log entry on recovery");
        }
        if (le->get_term() != raft_term) {
            throw storage_corruption_error("Raft term mismatch between header (" + std::to_string(raft_term) + ") and payload (" + std::to_string(le->get_term()) + ") at index " + std::to_string(raft_idx));
        }
        logs_[raft_idx] = le;

        log_location loc;
        loc.segment_seq = segments_[seg_idx].first_index;
        loc.file_offset = file_offset;
        loc.record_size = HEADER_SIZE + payload.size();
        index_locations_[raft_idx] = loc;
    } else if (rtype == RT_TRUNCATE) {
        uint32_t payload_len = get_le32(header + OFF_PAYLOAD_LEN);
        if (payload_len != 0) {
            throw storage_corruption_error("RT_TRUNCATE record payload_len must be 0, got " + std::to_string(payload_len));
        }
        if (raft_term != 0) {
            throw storage_corruption_error("RT_TRUNCATE record term must be 0, got " + std::to_string(raft_term));
        }

        auto it = logs_.lower_bound(raft_idx);
        while (it != logs_.end()) {
            index_locations_.erase(it->first);
            it = logs_.erase(it);
        }
    }
}

void durable_log_store::scan_and_recover() {
    if (segments_.empty()) {
        create_segment(1);
        return;
    }

    logs_.clear();
    index_locations_.clear();
    uint64_t expected_next_idx = 0;

    for (size_t i = 0; i < segments_.size(); ++i) {
        std::string path = segments_[i].path;
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw storage_io_error("Failed to open segment file for recovery scan: " + path);
        }

        struct stat st;
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            throw storage_io_error("fstat failed on segment: " + path);
        }
        uint64_t file_size = st.st_size;
        segments_[i].size = file_size;

        if (file_size < 16) {
            // Unusable segment header, delete it if it is the final segment
            if (i == segments_.size() - 1) {
                ::close(fd);
                ::unlink(path.c_str());
                segments_.pop_back();
                save_manifest();
                fsync_directory_or_throw(log_dir_);
                uint64_t next_idx = (expected_next_idx == 0) ? 1 : expected_next_idx;
                create_segment(next_idx);
                break;
            } else {
                ::close(fd);
                throw storage_corruption_error("Invalid/truncated segment header in non-tail segment: " + path);
            }
        }

        uint8_t seg_hdr[16];
        if (::read(fd, seg_hdr, 16) != 16) {
            ::close(fd);
            throw storage_corruption_error("Failed to read segment header: " + path);
        }
        if (::memcmp(seg_hdr, "RAFT_SEG", 8) != 0) {
            ::close(fd);
            throw storage_corruption_error("Invalid segment magic: " + path);
        }
        uint32_t seg_ver = get_le32(seg_hdr + 8);
        if (seg_ver != FORMAT_VER) {
            ::close(fd);
            throw storage_corruption_error("Invalid segment version: " + path);
        }

        bool is_first_record_in_segment = true;
        uint64_t offset = 16;
        while (offset < file_size) {
            if (offset + HEADER_SIZE > file_size) {
                // Partial final header at EOF
                if (i == segments_.size() - 1) {
                    ::close(fd);
                    fd = -1;
                    int write_fd = ::open(path.c_str(), O_RDWR);
                    if (write_fd < 0) {
                        throw storage_io_error("Cannot open segment for tail repair: " + path);
                    }
                    if (::ftruncate(write_fd, offset) != 0) {
                        ::close(write_fd);
                        throw storage_io_error("ftruncate failed during tail header repair: " + path);
                    }
                    try {
                        fdatasync_and_profile(write_fd, "tail header repair: " + path);
                    } catch (...) {
                        ::close(write_fd);
                        throw;
                    }
                    ::close(write_fd);
                    segments_[i].size = offset;
                    break;
                } else {
                    ::close(fd);
                    throw storage_corruption_error("Partial non-tail header in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            uint8_t header[HEADER_SIZE];
            if (::lseek(fd, offset, SEEK_SET) < 0 || ::read(fd, header, HEADER_SIZE) != (ssize_t)HEADER_SIZE) {
                ::close(fd);
                throw storage_io_error("Failed to read record header at offset " + std::to_string(offset));
            }

            uint32_t magic = get_le32(header + OFF_MAGIC);
            uint32_t ver = get_le32(header + OFF_VERSION);
            uint32_t rtype = get_le32(header + OFF_TYPE);
            uint32_t rec_len = get_le32(header + OFF_RECORD_LEN);
            uint64_t raft_idx = get_le64(header + OFF_INDEX);
            uint64_t raft_term = get_le64(header + OFF_TERM);
            uint32_t payload_len = get_le32(header + OFF_PAYLOAD_LEN);
            uint32_t payload_crc = get_le32(header + OFF_PAYLOAD_CRC);
            uint32_t expected_hcrc = get_le32(header + OFF_HEADER_CRC);
            uint32_t actual_hcrc = crc32_bytes(header, HEADER_CRC_INPUT_SIZE);

            if (magic != MAGIC) {
                ::close(fd);
                throw storage_corruption_error("Invalid record magic in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (ver != FORMAT_VER) {
                ::close(fd);
                throw storage_corruption_error("Invalid record format version in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (rtype != RT_ENTRY && rtype != RT_TRUNCATE) {
                ::close(fd);
                throw storage_corruption_error("Unknown record type in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (actual_hcrc != expected_hcrc) {
                ::close(fd);
                throw storage_corruption_error("Record header CRC mismatch in segment: " + path + " at offset " + std::to_string(offset));
            }
            if (rec_len != HEADER_SIZE + payload_len) {
                ::close(fd);
                throw storage_corruption_error("Record length mismatch in segment: " + path + " at offset " + std::to_string(offset));
            }

            // Record index continuity validation
            if (is_first_record_in_segment) {
                if (raft_idx != segments_[i].first_index) {
                    ::close(fd);
                    throw storage_corruption_error("First record index " + std::to_string(raft_idx) +
                                                   " in segment " + path + " does not match segment first_index " +
                                                   std::to_string(segments_[i].first_index));
                }
                if (expected_next_idx != 0) {
                    if (rtype == RT_ENTRY) {
                        if (raft_idx != expected_next_idx) {
                            ::close(fd);
                            throw storage_corruption_error("Segment boundary index gap detected in " + path +
                                                           ": expected " + std::to_string(expected_next_idx) +
                                                           " but got " + std::to_string(raft_idx));
                        }
                    } else if (rtype == RT_TRUNCATE) {
                        if (raft_idx > expected_next_idx) {
                            ::close(fd);
                            throw storage_corruption_error("Segment boundary truncate index jump forward detected in " + path +
                                                           ": expected <= " + std::to_string(expected_next_idx) +
                                                           " but got " + std::to_string(raft_idx));
                        }
                    }
                }
                is_first_record_in_segment = false;
            } else {
                if (rtype == RT_ENTRY) {
                    if (expected_next_idx != 0 && raft_idx != expected_next_idx) {
                        ::close(fd);
                        throw storage_corruption_error("Index gap/jump detected in " + path + " at offset " + std::to_string(offset) +
                                                       ": expected " + std::to_string(expected_next_idx) +
                                                       " but got " + std::to_string(raft_idx));
                    }
                } else if (rtype == RT_TRUNCATE) {
                    if (expected_next_idx != 0 && raft_idx > expected_next_idx) {
                        ::close(fd);
                        throw storage_corruption_error("Truncate index jump forward detected in " + path + " at offset " + std::to_string(offset) +
                                                       ": expected <= " + std::to_string(expected_next_idx) +
                                                       " but got " + std::to_string(raft_idx));
                    }
                }
            }

            if (rtype == RT_ENTRY) {
                expected_next_idx = raft_idx + 1;
            } else if (rtype == RT_TRUNCATE) {
                expected_next_idx = raft_idx;
            }

            if (offset + HEADER_SIZE + payload_len > file_size) {
                // Partial final payload at EOF
                if (i == segments_.size() - 1) {
                    ::close(fd);
                    fd = -1;
                    int write_fd = ::open(path.c_str(), O_RDWR);
                    if (write_fd < 0) {
                        throw storage_io_error("Cannot open segment for tail payload repair: " + path);
                    }
                    if (::ftruncate(write_fd, offset) != 0) {
                        ::close(write_fd);
                        throw storage_io_error("ftruncate failed during tail payload repair: " + path);
                    }
                    try {
                        fdatasync_and_profile(write_fd, "tail payload repair: " + path);
                    } catch (...) {
                        ::close(write_fd);
                        throw;
                    }
                    ::close(write_fd);
                    segments_[i].size = offset;
                    break;
                } else {
                    ::close(fd);
                    throw storage_corruption_error("Partial non-tail payload in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            std::vector<uint8_t> payload(payload_len);
            if (payload_len > 0) {
                if (::read(fd, payload.data(), payload_len) != (ssize_t)payload_len) {
                    ::close(fd);
                    throw storage_io_error("Failed to read record payload at offset " + std::to_string(offset + HEADER_SIZE));
                }
                uint32_t actual_pcrc = crc32_bytes(payload.data(), payload_len);
                if (actual_pcrc != payload_crc) {
                    ::close(fd);
                    throw storage_corruption_error("CRC mismatch in record payload in segment: " + path + " at offset " + std::to_string(offset));
                }
            }

            process_record_on_recovery(header, payload, offset, i);
            offset += HEADER_SIZE + payload_len;
        }

        if (fd >= 0) {
            ::close(fd);
        }
    }

    // Set start index
    auto start_it = logs_.upper_bound(0);
    if (start_it != logs_.end()) {
        start_index_ = start_it->first;
    } else {
        start_index_ = 1;
    }

    load_durable_watermark_unlocked();
}

void durable_log_store::sync_dirty_segments_unlocked(const std::string& context) {
    if (dirty_segments_.empty()) {
        last_durable_idx_ = next_slot_unlocked() - 1;
        dirty_ = false;
        return;
    }

    for (size_t seg_idx : dirty_segments_) {
        if (seg_idx < segments_.size()) {
            int fd = segment_fd(seg_idx);
            fdatasync_and_profile(fd, context + " (segment " + std::to_string(seg_idx) + ")");
        }
    }

    dirty_segments_.clear();
    last_durable_idx_ = next_slot_unlocked() - 1;
    dirty_ = false;

    save_durable_watermark_unlocked();
}

void durable_log_store::fdatasync_and_profile(int fd, const std::string& context) {
    auto s0 = std::chrono::steady_clock::now();
    if (::fdatasync(fd) != 0) {
        throw storage_io_error("fdatasync failed in " + context + " error: " + strerror(errno));
    }
    auto s1 = std::chrono::steady_clock::now();
    uint64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();
    profile_.fdatasync_total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
    profile_.fdatasync_calls.fetch_add(1, std::memory_order_relaxed);

    uint64_t cur_max = profile_.fdatasync_max_ns.load(std::memory_order_relaxed);
    while (elapsed_ns > cur_max && !profile_.fdatasync_max_ns.compare_exchange_weak(cur_max, elapsed_ns)) {}
}

void durable_log_store::save_durable_watermark_unlocked() {
    uint64_t watermark = last_durable_idx_;
    std::string path = log_dir_ + "/durable_watermark.bin";
    std::string tmp_path = path + ".tmp";

    std::vector<uint8_t> buf(20);
    uint32_t magic = 0xAB1C0D0B;
    uint32_t version = 1;
    
    ::memcpy(buf.data(), &magic, 4);
    ::memcpy(buf.data() + 4, &version, 4);
    ::memcpy(buf.data() + 8, &watermark, 8);
    
    uint32_t crc = crc32_bytes(buf.data(), 16);
    ::memcpy(buf.data() + 16, &crc, 4);

    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw storage_io_error("Failed to open watermark tmp file: " + tmp_path);
    }
    if (::write(fd, buf.data(), buf.size()) != (ssize_t)buf.size()) {
        ::close(fd);
        throw storage_io_error("Failed to write watermark tmp file: " + tmp_path);
    }
    if (::fdatasync(fd) != 0) {
        ::close(fd);
        throw storage_io_error("fdatasync failed for watermark tmp file: " + tmp_path);
    }
    ::close(fd);

    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        throw storage_io_error("Rename failed for watermark: " + tmp_path + " -> " + path);
    }
    fsync_directory_or_throw(log_dir_);
}

void durable_log_store::load_durable_watermark_unlocked() {
    std::string path = log_dir_ + "/durable_watermark.bin";
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) {
        last_durable_idx_ = next_slot_unlocked() - 1;
        return;
    }

    if (st.st_size != 20) {
        throw storage_corruption_error("durable_watermark.bin size mismatch: expected 20, got " + std::to_string(st.st_size));
    }

    std::vector<uint8_t> buf(20);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw storage_io_error("Failed to open durable_watermark.bin: " + path);
    }
    if (::read(fd, buf.data(), buf.size()) != 20) {
        ::close(fd);
        throw storage_io_error("Failed to read durable_watermark.bin: " + path);
    }
    ::close(fd);

    uint32_t magic;
    uint32_t version;
    uint64_t watermark;
    uint32_t expected_crc;

    ::memcpy(&magic, buf.data(), 4);
    ::memcpy(&version, buf.data() + 4, 4);
    ::memcpy(&watermark, buf.data() + 8, 8);
    ::memcpy(&expected_crc, buf.data() + 16, 4);

    if (magic != 0xAB1C0D0B) {
        throw storage_corruption_error("durable_watermark.bin magic mismatch");
    }
    if (version != 1) {
        throw storage_corruption_error("durable_watermark.bin version mismatch");
    }

    uint32_t actual_crc = crc32_bytes(buf.data(), 16);
    if (actual_crc != expected_crc) {
        throw storage_corruption_error("durable_watermark.bin CRC mismatch");
    }

    uint64_t max_idx = next_slot_unlocked() - 1;
    if (watermark > max_idx) {
        last_durable_idx_ = max_idx;
    } else {
        last_durable_idx_ = watermark;
    }
}

void durable_log_store::simulate_crash_close() {
    std::lock_guard<std::mutex> l(mu_);
    dirty_ = false;
    dirty_segments_.clear();
    for (auto& seg : segments_) {
        if (seg.fd >= 0) {
            ::close(seg.fd);
            seg.fd = -1;
        }
    }
}

} // namespace ariabc_raft
