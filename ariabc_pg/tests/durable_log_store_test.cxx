// ariabc_pg/tests/durable_log_store_test.cxx
// Unit tests for durable_log_store.

#include "../src/durable_log_store.hxx"
#include "nuraft.hxx"
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) \
            throw std::runtime_error("requirement failed: " #expr); \
    } while (0)

using namespace ariabc_raft;

nuraft::ptr<nuraft::log_entry> create_test_entry(uint64_t term, const std::string& val) {
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(val.size());
    buf->pos(0);
    buf->put_raw((const uint8_t*)val.data(), val.size());
    buf->pos(0);
    return nuraft::cs_new<nuraft::log_entry>(term, buf);
}

std::string get_entry_val(nuraft::ptr<nuraft::log_entry> entry) {
    if (!entry) return "";
    nuraft::buffer& buf = entry->get_buf();
    buf.pos(0);
    return std::string((const char*)buf.data(), buf.size());
}

void test_append_and_reopen() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 1);

        auto e1 = create_test_entry(1, "hello");
        auto e2 = create_test_entry(1, "world");
        
        REQUIRE(store.append(e1) == 1);
        REQUIRE(store.append(e2) == 2);
        
        // Before flush last durable should be 0 (none synced)
        REQUIRE(store.last_durable_index() == 0);

        store.end_of_append_batch(1, 2);
        REQUIRE(store.last_durable_index() == 2);
    }

    // Reopen and check
    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 3);
        REQUIRE(store.start_index() == 1);
        REQUIRE(store.last_durable_index() == 2);

        auto e1 = store.entry_at(1);
        auto e2 = store.entry_at(2);

        REQUIRE(e1 != nullptr && e1->get_term() == 1);
        REQUIRE(get_entry_val(e1) == "hello");
        REQUIRE(e2 != nullptr && e2->get_term() == 1);
        REQUIRE(get_entry_val(e2) == "world");
    }

    std::cout << "test_append_and_reopen passed" << std::endl;
}

void test_write_at_truncation() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "A");
        auto e2 = create_test_entry(1, "B");
        auto e3 = create_test_entry(1, "C");

        store.append(e1);
        store.append(e2);
        store.append(e3);
        store.end_of_append_batch(1, 3);
    }

    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 4);

        auto e2_new = create_test_entry(2, "B_new");
        // write_at will truncate index >= 2 and write new entry at 2
        store.write_at(2, e2_new);

        REQUIRE(store.next_slot() == 3);
        REQUIRE(get_entry_val(store.entry_at(2)) == "B_new");
        REQUIRE(store.entry_at(3) == nullptr);
    }

    // Reopen and verify truncation persisted
    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 3);
        REQUIRE(get_entry_val(store.entry_at(1)) == "A");
        REQUIRE(get_entry_val(store.entry_at(2)) == "B_new");
        REQUIRE(store.entry_at(3) == nullptr);
    }

    std::cout << "test_write_at_truncation passed" << std::endl;
}

void test_apply_pack() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    nuraft::ptr<nuraft::buffer> packed;
    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "X");
        auto e2 = create_test_entry(2, "Y");
        store.append(e1);
        store.append(e2);
        store.end_of_append_batch(1, 2);
        
        packed = store.pack(1, 2);
    }

    std::string dest_dir = "./test_log_store_dest_dir";
    ::system(("rm -rf " + dest_dir).c_str());
    {
        durable_log_store store(dest_dir);
        store.apply_pack(1, *packed);
    }

    // Reopen dest and check
    {
        durable_log_store store(dest_dir);
        REQUIRE(store.next_slot() == 3);
        REQUIRE(get_entry_val(store.entry_at(1)) == "X");
        REQUIRE(get_entry_val(store.entry_at(2)) == "Y");
    }

    std::cout << "test_apply_pack passed" << std::endl;
}

void test_incomplete_tail_truncation() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "test1");
        store.append(e1);
        store.end_of_append_batch(1, 1);
    }

    // Corrupt/truncate the segment file by appending random garbage at the end
    // (a partial write scenario)
    std::string seg_path = test_dir + "/segment_00000000000000000001.log";
    std::ofstream f(seg_path, std::ios::binary | std::ios::app);
    f << "garb"; // incomplete record
    f.close();

    {
        durable_log_store store(test_dir);
        // The store should detect the incomplete tail record and truncate it, surviving the load
        REQUIRE(store.next_slot() == 2);
        REQUIRE(get_entry_val(store.entry_at(1)) == "test1");
    }

    std::cout << "test_incomplete_tail_truncation passed" << std::endl;
}

void test_corrupt_non_tail_record() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "A");
        auto e2 = create_test_entry(1, "B");
        store.append(e1);
        store.append(e2);
        store.end_of_append_batch(1, 2);
    }

    // Corrupt the header of the first record (offset 16)
    std::string seg_path = test_dir + "/segment_00000000000000000001.log";
    int fd = ::open(seg_path.c_str(), O_WRONLY);
    REQUIRE(fd >= 0);
    ::lseek(fd, 20, SEEK_SET);
    uint32_t garbage = 0xDEADBEEF;
    ::write(fd, &garbage, 4);
    ::close(fd);

    try {
        durable_log_store store(test_dir);
        REQUIRE(false);
    } catch (const storage_corruption_error& e) {
        // Success
    }

    std::cout << "test_corrupt_non_tail_record passed" << std::endl;
}

void test_broken_tail_segment_header() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "A");
        store.append(e1);
        store.end_of_append_batch(1, 1);
    }

    std::string seg_path = test_dir + "/segment_00000000000000000001.log";
    int fd = ::open(seg_path.c_str(), O_WRONLY);
    REQUIRE(fd >= 0);
    REQUIRE(::ftruncate(fd, 8) == 0);
    ::close(fd);

    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 1);
        
        auto e2 = create_test_entry(1, "B");
        store.append(e2);
        store.end_of_append_batch(1, 1);
    }

    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 2);
        REQUIRE(get_entry_val(store.entry_at(1)) == "B");
    }

    std::cout << "test_broken_tail_segment_header passed" << std::endl;
}

// Test: segment rotation occurs, then write_at() rolls back to an earlier index.
// The TRUNCATE_FROM record ends up as the first record in the new segment.
// The segment's first_index must match the TRUNCATE_FROM index, not next_slot().
void test_segment_rotation_with_write_at_rollback() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    const uint64_t SEG_MAX = durable_log_store::SEGMENT_MAX_BYTES;

    // Fill just past one full segment boundary.
    // Each entry serialises to approximately (buf_header + payload) bytes.
    // We write large payloads (~1 MiB each) so we need ~65 entries to cross 64 MiB.
    const size_t PAYLOAD_BYTES = 1024 * 1024; // 1 MiB
    std::string big_payload(PAYLOAD_BYTES, 'X');

    uint64_t last_idx = 0;
    {
        durable_log_store store(test_dir);
        // Write enough entries to exceed SEGMENT_MAX_BYTES and trigger rotation.
        uint64_t bytes_approx = 0;
        while (bytes_approx < SEG_MAX + PAYLOAD_BYTES) {
            auto e = create_test_entry(1, big_payload);
            last_idx = store.append(e);
            store.end_of_append_batch(last_idx, 1);
            bytes_approx += PAYLOAD_BYTES + 50; // rough estimate
        }
        REQUIRE(last_idx >= 65); // sanity: we did write at least ~65 entries
    }

    // Choose a rollback target well inside the first segment.
    uint64_t rollback_idx = last_idx / 2; // something < last_idx

    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == last_idx + 1);

        // write_at forces TRUNCATE_FROM at rollback_idx.
        // Because the active segment is already the second segment (post-rotation),
        // the TRUNCATE_FROM record is the very first record in that segment.
        // Before the fix, the segment's first_index was next_slot_unlocked() = last_idx+1,
        // which did not match the TRUNCATE_FROM index = rollback_idx, causing recovery failure.
        auto new_entry = create_test_entry(2, "after_rollback");
        store.write_at(rollback_idx, new_entry);

        REQUIRE(store.next_slot() == rollback_idx + 1);
        REQUIRE(get_entry_val(store.entry_at(rollback_idx)) == "after_rollback");
    }

    // Reopen and verify: only entries up to rollback_idx survive, with the new one in place.
    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == rollback_idx + 1);
        REQUIRE(get_entry_val(store.entry_at(rollback_idx)) == "after_rollback");
        REQUIRE(store.entry_at(rollback_idx + 1) == nullptr);
    }

    std::cout << "test_segment_rotation_with_write_at_rollback passed" << std::endl;
}

void test_complete_record_bad_crc_fails() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "A");
        uint64_t idx = store.append(e1);
        store.end_of_append_batch(idx, 1);
        auto e2 = create_test_entry(1, "B");
        idx = store.append(e2);
        store.end_of_append_batch(idx, 1);
    }

    std::string seg_path = test_dir + "/segment_00000000000000000001.log";
    
    // Corrupt the last byte of the file (which is the last byte of "B" payload)
    std::fstream fs(seg_path, std::ios::in | std::ios::out | std::ios::binary);
    REQUIRE(fs.is_open());
    fs.seekp(-1, std::ios::end);
    fs.put('Z');
    fs.close();

    try {
        durable_log_store store(test_dir);
        REQUIRE(false); // must throw storage_corruption_error, not truncate!
    } catch (const storage_corruption_error& e) {
        std::cout << "Successfully caught bad CRC in complete tail record: " << e.what() << std::endl;
    }
    std::cout << "test_complete_record_bad_crc_fails passed" << std::endl;
}

void test_partial_payload_at_eof_truncates() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    {
        durable_log_store store(test_dir);
        auto e1 = create_test_entry(1, "A");
        uint64_t idx = store.append(e1);
        store.end_of_append_batch(idx, 1);
        // Write second entry with a larger payload
        std::string large_payload(100, 'B');
        auto e2 = create_test_entry(1, large_payload);
        idx = store.append(e2);
        store.end_of_append_batch(idx, 1);
    }

    std::string seg_path = test_dir + "/segment_00000000000000000001.log";
    
    // Get file size
    struct stat st;
    REQUIRE(::stat(seg_path.c_str(), &st) == 0);
    off_t size = st.st_size;

    // Truncate the file so it has only a part of the second payload
    // Size of second record header is 44, so truncating 20 bytes from the end leaves the header and 80 bytes of payload.
    int fd = ::open(seg_path.c_str(), O_RDWR);
    REQUIRE(fd >= 0);
    REQUIRE(::ftruncate(fd, size - 20) == 0);
    ::close(fd);

    // Reopen. It must recover by truncating the partial record, yielding next_slot() == 2.
    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 2);
        REQUIRE(get_entry_val(store.entry_at(1)) == "A");
        REQUIRE(store.entry_at(2) == nullptr);
    }
    std::cout << "test_partial_payload_at_eof_truncates passed" << std::endl;
}

void test_segment_rollover_during_append_batch() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    const uint64_t SEG_MAX = durable_log_store::SEGMENT_MAX_BYTES;
    const size_t PAYLOAD_BYTES = 1024 * 1024; // 1 MiB
    std::string big_payload(PAYLOAD_BYTES, 'X');

    {
        durable_log_store store(test_dir);
        // Append 70 entries in a single batch (before calling end_of_append_batch)
        uint64_t last_idx = 0;
        for (int i = 0; i < 70; ++i) {
            auto e = create_test_entry(1, big_payload);
            last_idx = store.append(e);
        }
        // Sync them all at once
        store.end_of_append_batch(1, 70);

        REQUIRE(store.next_slot() == 71);
        REQUIRE(get_entry_val(store.entry_at(1)) == big_payload);
        REQUIRE(get_entry_val(store.entry_at(70)) == big_payload);
    }

    // Reopen and verify they are all recovered successfully from both segments
    {
        durable_log_store store(test_dir);
        REQUIRE(store.next_slot() == 71);
        REQUIRE(get_entry_val(store.entry_at(1)) == big_payload);
        REQUIRE(get_entry_val(store.entry_at(70)) == big_payload);
    }
    std::cout << "test_segment_rollover_during_append_batch passed" << std::endl;
}

void test_rollback_after_double_segment_rotation() {
    std::string test_dir = "./test_log_store_dir";
    ::system(("rm -rf " + test_dir).c_str());

    const uint64_t SEG_MAX = 4096;
    std::string payload(1000, 'A');

    {
        durable_log_store store(test_dir, SEG_MAX);

        // 1. Fill segment A (first_index = 1)
        for (int i = 0; i < 4; ++i) {
            auto e = create_test_entry(1, payload);
            store.append(e);
        }

        // 2. Fill segment B until B is also full
        for (int i = 0; i < 4; ++i) {
            auto e = create_test_entry(1, payload);
            store.append(e);
        }

        // 3. write_at(index inside A) -> creates segment C starting with TRUNCATE_FROM
        auto e_new = create_test_entry(1, "NewB");
        store.write_at(3, e_new);
    }

    // 4. Reopen and verify recovery must succeed
    {
        durable_log_store store(test_dir, SEG_MAX);
        REQUIRE(store.next_slot() == 4);
        REQUIRE(get_entry_val(store.entry_at(3)) == "NewB");
    }
    std::cout << "test_rollback_after_double_segment_rotation passed" << std::endl;
}

int main() {
    try {
        test_append_and_reopen();
        test_write_at_truncation();
        test_apply_pack();
        test_incomplete_tail_truncation();
        test_corrupt_non_tail_record();
        test_broken_tail_segment_header();
        test_segment_rotation_with_write_at_rollback();
        test_complete_record_bad_crc_fails();
        test_partial_payload_at_eof_truncates();
        test_segment_rollover_during_append_batch();
        test_rollback_after_double_segment_rotation();
        std::cout << "ALL durable_log_store tests PASSED" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
