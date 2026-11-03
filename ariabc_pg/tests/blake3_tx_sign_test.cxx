// ariabc_pg/tests/blake3_tx_sign_test.cxx
// Request tag tests: keyed BLAKE3 signer, key-file loading, and the tag's
// round trip through every encoding between gateway and state machine
// (blocking frame, nonblocking frame, Raft log entry).

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>

#include "blake3_tx_signer.hxx"
#include "wire_protocol.hxx"
#include "wire_protocol_async.hxx"

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "Requirement failed at line " << __LINE__ << ": " << #expr << std::endl; \
            throw std::runtime_error("requirement failed: " #expr); \
        } \
    } while (0)

namespace {

std::string write_key_file(const std::string& dir, const std::string& name, const std::string& body) {
    const std::string path = dir + "/" + name;
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    out << body;
    out.close();
    ::chmod(path.c_str(), 0600);
    return path;
}

void set_tag(ariabc_pg::client_api_request_item& item, const ariabc_pg::blake3_sig_256& tag) {
    item.has_tx_tag = true;
    std::memcpy(item.tx_tag, tag.bytes, ariabc_pg::kTxTagBytes);
}

bool same_tag(const uint8_t* a, const ariabc_pg::blake3_sig_256& b) {
    return std::memcmp(a, b.bytes, ariabc_pg::kTxTagBytes) == 0;
}

} // namespace

int main(int argc, char** argv) {
    // Key files go next to the test binary, never in /tmp.
    std::string dir = ".";
    if (argc > 1) dir = argv[1];

    std::cout << "Running BLAKE3 request tag tests..." << std::endl;

    const std::string key_a = write_key_file(
        dir, "tx_tag_test_a.key",
        "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff\n");
    const std::string key_b = write_key_file(
        dir, "tx_tag_test_b.key",
        "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100");
    const std::string key_short = write_key_file(dir, "tx_tag_test_short.key", "abcd");
    const std::string key_bad = write_key_file(
        dir, "tx_tag_test_bad.key",
        "zz112233445566778899aabbccddeeff00112233445566778899aabbccddeeff");

    std::string err;
    ariabc_pg::blake3_tx_signer signer;
    REQUIRE(!signer.is_enabled());
    REQUIRE(signer.load_key_file(key_a, err));
    REQUIRE(signer.is_enabled());
    REQUIRE(!signer.key_file_world_readable());

    // Malformed key files are refused; no fallback key exists.
    ariabc_pg::blake3_tx_signer bad;
    REQUIRE(!bad.load_key_file(key_short, err));
    REQUIRE(!bad.load_key_file(key_bad, err));
    REQUIRE(!bad.load_key_file(dir + "/does_not_exist.key", err));
    REQUIRE(!bad.is_enabled());
    REQUIRE(!bad.verify("x", "y", ariabc_pg::blake3_sig_256()));

    const std::string req_id = "cli-12345";
    const std::string sql = "s 00000042 UPDATE usertable SET field1 = 'v' WHERE ycsb_key = 1042;";
    const ariabc_pg::blake3_sig_256 tag = signer.sign(req_id, sql);
    REQUIRE(!tag.is_zero());
    REQUIRE(signer.verify(req_id, sql, tag));

    // Tampering: SQL body, det-seq prefix, req_id, key.
    REQUIRE(!signer.verify(req_id, "s 00000042 UPDATE usertable SET field1 = 'evil' WHERE ycsb_key = 1042;", tag));
    REQUIRE(!signer.verify(req_id, "s 00000043 UPDATE usertable SET field1 = 'v' WHERE ycsb_key = 1042;", tag));
    REQUIRE(!signer.verify("cli-12346", sql, tag));
    ariabc_pg::blake3_tx_signer other;
    REQUIRE(other.load_key_file(key_b, err));
    REQUIRE(!other.verify(req_id, sql, tag));

    // Length prefixes: moving bytes between req_id and sql changes the tag.
    REQUIRE(signer.sign("ab", "c") != signer.sign("a", "bc"));
    ariabc_pg::blake3_sig_256 flipped = tag;
    flipped.bytes[31] ^= 0x01;
    REQUIRE(!signer.verify(req_id, sql, flipped));

    // Blocking frames: single (v2 + tag flag) and batch v2, read back over a socketpair.
    {
        int fds[2];
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        std::memcpy(single.tx_tag, tag.bytes, ariabc_pg::kTxTagBytes);
        REQUIRE(ariabc_pg::write_request_frame(fds[0], single, err));

        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 3; ++i) {
            ariabc_pg::client_api_request_item item;
            item.req_id = "cli-" + std::to_string(i);
            item.sql = "s 0000000" + std::to_string(i) + " SELECT 1";
            if (i != 1) set_tag(item, signer.sign(item.req_id, item.sql));
            batch.batch_items.push_back(item);
        }
        REQUIRE(ariabc_pg::write_request_frame(fds[0], batch, err));

        ariabc_pg::client_api_request untagged;
        untagged.req_id = "legacy";
        untagged.sql = "SELECT 2";
        REQUIRE(ariabc_pg::write_request_frame(fds[0], untagged, err));

        ariabc_pg::client_api_request got;
        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(!got.is_batch() && got.req_id == req_id && got.sql == sql);
        REQUIRE(got.has_tx_tag && same_tag(got.tx_tag, tag));
        REQUIRE(!got.wait_for_terminal);

        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(got.batch_items.size() == 3);
        REQUIRE(got.batch_items[0].has_tx_tag && !got.batch_items[1].has_tx_tag && got.batch_items[2].has_tx_tag);
        for (size_t i = 0; i < 3; ++i) {
            const auto& item = got.batch_items[i];
            REQUIRE(item.sql == batch.batch_items[i].sql);
            if (item.has_tx_tag) {
                ariabc_pg::blake3_sig_256 t;
                std::memcpy(t.bytes, item.tx_tag, 32);
                REQUIRE(signer.verify(item.req_id, item.sql, t));
            }
        }

        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(!got.has_tx_tag && got.req_id == "legacy" && got.sql == "SELECT 2");
        ::close(fds[0]);
        ::close(fds[1]);
    }

    // Nonblocking codec: encode, decode byte by byte (NEED_MORE until complete).
    {
        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 2; ++i) {
            ariabc_pg::client_api_request_item item;
            item.req_id = "a-" + std::to_string(i);
            item.sql = "s 0000010" + std::to_string(i) + " SELECT 3";
            set_tag(item, signer.sign(item.req_id, item.sql));
            batch.batch_items.push_back(item);
        }
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        single.wait_for_terminal = true;
        single.terminal_timeout_ms = 1234;
        std::memcpy(single.tx_tag, tag.bytes, 32);

        for (const auto* req : {&batch, &single}) {
            std::string wire;
            REQUIRE(ariabc_pg::encode_request_frame(*req, wire, err));
            ariabc_pg::io_buffer buf;
            ariabc_pg::client_api_request got;
            ariabc_pg::decode_status st = ariabc_pg::decode_status::NEED_MORE;
            for (size_t i = 0; i < wire.size(); ++i) {
                buf.append(&wire[i], 1);
                st = ariabc_pg::try_decode_request_frame(buf, got, err);
                if (i + 1 < wire.size()) REQUIRE(st == ariabc_pg::decode_status::NEED_MORE);
            }
            REQUIRE(st == ariabc_pg::decode_status::OK);
            REQUIRE(buf.size() == 0);
            if (req->is_batch()) {
                REQUIRE(got.batch_items.size() == 2);
                for (size_t i = 0; i < 2; ++i) {
                    REQUIRE(got.batch_items[i].has_tx_tag);
                    REQUIRE(std::memcmp(got.batch_items[i].tx_tag, req->batch_items[i].tx_tag, 32) == 0);
                    REQUIRE(got.batch_items[i].sql == req->batch_items[i].sql);
                }
            } else {
                REQUIRE(got.has_tx_tag && same_tag(got.tx_tag, tag));
                REQUIRE(got.wait_for_terminal && got.terminal_timeout_ms == 1234);
                REQUIRE(got.req_id == req_id && got.sql == sql);
            }
        }
    }

    // Raft log: a tagged single request and a mixed batch both survive build/parse.
    {
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        std::memcpy(single.tx_tag, tag.bytes, 32);
        nuraft::ptr<nuraft::buffer> log = ariabc_pg::build_raft_request_log(single, 2, err);
        REQUIRE(log);
        ariabc_pg::raft_request_batch parsed;
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 1 && parsed.leader_node_hint == 2);
        REQUIRE(parsed.items[0].has_tx_tag && same_tag(parsed.items[0].tx_tag, tag));
        REQUIRE(parsed.items[0].sql == sql && parsed.items[0].req_id == req_id);

        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 4; ++i) {
            ariabc_pg::client_api_request_item item;
            item.req_id = "b-" + std::to_string(i);
            item.sql = "s 0000020" + std::to_string(i) + " SELECT 4";
            item.has_assigned_det_seq = (i % 2) == 0;
            item.assigned_det_seq = 200 + i;
            if (i != 3) set_tag(item, signer.sign(item.req_id, item.sql));
            batch.batch_items.push_back(item);
        }
        log = ariabc_pg::build_raft_request_log(batch, -1, err);
        REQUIRE(log);
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 4);
        for (size_t i = 0; i < 4; ++i) {
            REQUIRE(parsed.items[i].has_tx_tag == (i != 3));
            REQUIRE(parsed.items[i].has_assigned_det_seq == ((i % 2) == 0));
            REQUIRE(parsed.items[i].assigned_det_seq == 200 + i);
            if (parsed.items[i].has_tx_tag) {
                REQUIRE(std::memcmp(parsed.items[i].tx_tag, batch.batch_items[i].tx_tag, 32) == 0);
            }
        }

        // An untagged single request still uses the legacy entry format.
        ariabc_pg::client_api_request legacy;
        legacy.req_id = "legacy";
        legacy.sql = "SELECT 5";
        log = ariabc_pg::build_raft_request_log(legacy, 1, err);
        REQUIRE(log);
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 1 && !parsed.items[0].has_tx_tag);
    }

    // Micro-benchmark: sign + verify of a YCSB-sized request.
    {
        const int iterations = 100000;
        std::vector<ariabc_pg::blake3_sig_256> tags(iterations);
        std::vector<std::string> ids(iterations);
        for (int i = 0; i < iterations; ++i) ids[i] = "cli-" + std::to_string(i);
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) tags[i] = signer.sign(ids[i], sql);
        const auto t1 = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            if (!signer.verify(ids[i], sql, tags[i])) throw std::runtime_error("verify failed in benchmark");
        }
        const auto t2 = std::chrono::steady_clock::now();
        const double sign_ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / iterations;
        const double verify_ns = std::chrono::duration<double, std::nano>(t2 - t1).count() / iterations;
        std::cout << "avg_sign_ns=" << sign_ns << " avg_verify_ns=" << verify_ns
                  << " sql_bytes=" << sql.size() << std::endl;
    }

    for (const std::string& p : {key_a, key_b, key_short, key_bad}) std::remove(p.c_str());
    std::cout << "ALL BLAKE3 request tag tests PASSED" << std::endl;
    return 0;
}
