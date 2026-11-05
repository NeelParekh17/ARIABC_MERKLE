#pragma once

#include <stdexcept>  // promise.hpp relies on it

#include "hotstuff/consensus.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

namespace ariabc_pg {

/*
 * HotStuff's vote state on disk.  Two records in separate blocks are written in
 * turn, so a write torn by a crash leaves the previous record intact.  That is
 * safe to fall back to: the torn write had not returned, so its vote never left.
 */
class hotstuff_vote_file {
public:
    hotstuff_vote_file() = default;
    hotstuff_vote_file(const hotstuff_vote_file&) = delete;
    hotstuff_vote_file& operator=(const hotstuff_vote_file&) = delete;
    ~hotstuff_vote_file() {
        if (fd_ >= 0) ::close(fd_);
    }

    // A new replica: refuses a file that exists, whose votes it would forget.
    bool create(const std::string& path, std::string& err) {
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd_ < 0) {
            err = "cannot create HotStuff vote state " + path + ": " + std::strerror(errno) +
                  " (a restarted replica needs --raft-rejoin)";
            return false;
        }
        const size_t slash = path.rfind('/');
        const std::string dir = slash == std::string::npos ? "." : path.substr(0, slash);
        const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        const bool ok = dfd >= 0 && ::fsync(dfd) == 0;
        if (dfd >= 0) ::close(dfd);
        if (!ok) err = "cannot sync directory " + dir + ": " + std::strerror(errno);
        return ok;
    }

    // A restart: false when the file is missing or holds no intact record.
    bool open_existing(const std::string& path, hotstuff::VoteState& state) {
        fd_ = ::open(path.c_str(), O_RDWR);
        if (fd_ < 0) return false;
        bool found = false;
        for (int slot = 0; slot < 2; ++slot) {
            uint8_t rec[kRecordBytes];
            if (::pread(fd_, rec, sizeof(rec), slot * kSlotBytes) != static_cast<ssize_t>(sizeof(rec))) continue;
            hotstuff::VoteState s{0, 0, hotstuff::uint256_t()};
            uint64_t seq = 0;
            if (decode(rec, seq, s) && (!found || seq > seq_)) {
                found = true;
                seq_ = seq;
                state = s;
            }
        }
        return found;
    }

    // Returns once the state is on disk.  A replica that cannot save must not vote.
    void save(const hotstuff::VoteState& s) {
        const auto t0 = std::chrono::steady_clock::now();
        const uint64_t seq = seq_ + 1;
        uint8_t rec[kRecordBytes];
        encode(rec, seq, s);
        if (::pwrite(fd_, rec, sizeof(rec), (seq % 2) * kSlotBytes) != static_cast<ssize_t>(sizeof(rec)) ||
            ::fdatasync(fd_) != 0) {
            std::cerr << "HOTSTUFF_VOTE_STATE_FATAL: " << std::strerror(errno) << std::endl;
            std::abort();
        }
        seq_ = seq;
        saves_++;
        save_us_ += std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - t0).count();
    }

    uint64_t saves() const { return saves_; }
    uint64_t save_us() const { return save_us_; }

private:
    static const uint64_t kMagic = 0x4853564f54453031ULL;  // "HSVOTE01"
    static const size_t kBodyBytes = 8 + 8 + 4 + 4 + 32;
    static const size_t kRecordBytes = kBodyBytes + 32;
    static const size_t kSlotBytes = 4096;

    static hotstuff::bytearray_t checksum(const uint8_t* body) {
        return hotstuff::DataStream(hotstuff::bytearray_t(body, body + kBodyBytes)).get_hash().to_bytes();
    }

    static void encode(uint8_t* rec, uint64_t seq, const hotstuff::VoteState& s) {
        const hotstuff::bytearray_t hash = s.lock_hash.to_bytes();
        std::memcpy(rec, &kMagic, 8);
        std::memcpy(rec + 8, &seq, 8);
        std::memcpy(rec + 16, &s.vheight, 4);
        std::memcpy(rec + 20, &s.lock_height, 4);
        std::memcpy(rec + 24, hash.data(), 32);
        const hotstuff::bytearray_t sum = checksum(rec);
        std::memcpy(rec + kBodyBytes, sum.data(), 32);
    }

    static bool decode(const uint8_t* rec, uint64_t& seq, hotstuff::VoteState& s) {
        uint64_t magic = 0;
        std::memcpy(&magic, rec, 8);
        if (magic != kMagic) return false;
        if (std::memcmp(rec + kBodyBytes, checksum(rec).data(), 32) != 0) return false;
        std::memcpy(&seq, rec + 8, 8);
        std::memcpy(&s.vheight, rec + 16, 4);
        std::memcpy(&s.lock_height, rec + 20, 4);
        s.lock_hash.from_bytes(hotstuff::bytearray_t(rec + 24, rec + 56));
        return true;
    }

    int fd_ = -1;
    uint64_t seq_ = 0;
    uint64_t saves_ = 0;
    uint64_t save_us_ = 0;
};

} // namespace ariabc_pg
