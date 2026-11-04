// ariabc_pg/tests/tx_sig_test.cxx
// Ed25519 signature tests:
//  - per-client request signatures: key loading, sign/verify, tampering,
//    cross-client forgery, and the signature's round trip through every
//    encoding between gateway and state machine (blocking frame, nonblocking
//    frame, Raft log entry), including rejection of the retired BLAKE3 formats;
//  - per-replica result envelopes: seal/open, tampering, signer relabelling;
//  - the parallel verification pool;
//  - micro-benchmarks for sign/verify.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/pem.h>

#include "ed25519_keys.hxx"
#include "parallel_for_pool.hxx"
#include "result_envelope.hxx"
#include "tx_request_sig.hxx"
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

using ariabc_pg::kTxTagBytes;

// Writes <dir>/<role>_<id>.key.pem (0600) and <dir>/<role>_<id>.pub.pem.
void gen_key(const std::string& dir, const char* role, uint32_t id) {
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr);
    REQUIRE(pctx);
    EVP_PKEY* pkey = nullptr;
    REQUIRE(EVP_PKEY_keygen_init(pctx) == 1);
    REQUIRE(EVP_PKEY_keygen(pctx, &pkey) == 1);
    EVP_PKEY_CTX_free(pctx);
    const std::string priv = ariabc_pg::private_key_path(dir, role, id);
    const std::string pub = ariabc_pg::public_key_path(dir, role, id);
    FILE* f = std::fopen(priv.c_str(), "w");
    REQUIRE(f);
    REQUIRE(PEM_write_PrivateKey(f, pkey, nullptr, nullptr, 0, nullptr, nullptr) == 1);
    std::fclose(f);
    ::chmod(priv.c_str(), 0600);
    f = std::fopen(pub.c_str(), "w");
    REQUIRE(f);
    REQUIRE(PEM_write_PUBKEY(f, pkey) == 1);
    std::fclose(f);
    EVP_PKEY_free(pkey);
}

void make_dir(const std::string& d) {
    ::mkdir(d.c_str(), 0700);
}

void write_file(const std::string& path, const std::string& body) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    out << body;
}

void remove_tree(const std::string& d, const std::vector<std::string>& names) {
    for (const std::string& n : names) std::remove((d + "/" + n).c_str());
    ::rmdir(d.c_str());
}

ariabc_pg::client_api_request_item make_item(const std::string& req_id, const std::string& sql) {
    ariabc_pg::client_api_request_item item;
    item.req_id = req_id;
    item.sql = sql;
    return item;
}

void sign_item(const ariabc_pg::tx_request_signer& s, ariabc_pg::client_api_request_item& item) {
    REQUIRE(s.sign(item.req_id, item.sql, item.tx_tag));
    item.has_tx_tag = true;
}

} // namespace

int main(int argc, char** argv) {
    // Key files go next to the test binary, never in /tmp.
    std::string base = ".";
    if (argc > 1) base = argv[1];
    const std::string dir = base + "/tx_sig_test_keys";
    const std::string other_dir = base + "/tx_sig_test_keys_other";
    const std::string bad_dir = base + "/tx_sig_test_keys_bad";
    make_dir(dir);
    make_dir(other_dir);
    make_dir(bad_dir);

    std::cout << "Running Ed25519 signature tests..." << std::endl;
    gen_key(dir, "client", 1);
    gen_key(dir, "client", 2);
    gen_key(dir, "node", 1);
    gen_key(dir, "node", 2);
    gen_key(other_dir, "client", 1);  // a different key that claims client id 1
    write_file(bad_dir + "/client_7.pub.pem", "-----BEGIN PUBLIC KEY-----\nnot a key\n-----END PUBLIC KEY-----\n");

    std::string err;

    // ---------------------------------------------------------------- keys
    ariabc_pg::tx_request_signer c1, c2, impostor;
    REQUIRE(!c1.is_enabled());
    REQUIRE(c1.load(ariabc_pg::private_key_path(dir, "client", 1), 1, err));
    REQUIRE(c2.load(ariabc_pg::private_key_path(dir, "client", 2), 2, err));
    REQUIRE(impostor.load(ariabc_pg::private_key_path(other_dir, "client", 1), 1, err));
    REQUIRE(c1.is_enabled() && !c1.key().readable_by_others());
    {
        ariabc_pg::tx_request_signer bad;
        REQUIRE(!bad.load(dir + "/does_not_exist.key.pem", 1, err));
        REQUIRE(!bad.load(ariabc_pg::public_key_path(dir, "client", 1), 1, err));  // public, not private
        REQUIRE(!bad.is_enabled());
        uint8_t tag[kTxTagBytes];
        REQUIRE(!bad.sign("x", "y", tag));
    }

    ariabc_pg::tx_request_verifier verifier;
    REQUIRE(verifier.load_dir(dir, err));
    REQUIRE(verifier.roster().size() == 2);
    REQUIRE(verifier.roster().ids_csv() == "1,2");
    {
        ariabc_pg::tx_request_verifier bad;
        REQUIRE(!bad.load_dir(bad_dir, err));          // corrupt key file fails closed
        REQUIRE(!bad.load_dir(base + "/no_such_dir", err));
        ariabc_pg::ed25519_key_roster nodes;
        REQUIRE(nodes.load_dir(dir, "node", err) && nodes.size() == 2);
    }

    // -------------------------------------------------- request signatures
    const std::string req_id = "cli-12345";
    const std::string sql = "s 00000042 UPDATE usertable SET field1 = 'v' WHERE ycsb_key = 1042;";
    ariabc_pg::client_api_request_item item = make_item(req_id, sql);
    REQUIRE(verifier.check(item) == ariabc_pg::tx_sig_check::MISSING);
    sign_item(c1, item);
    REQUIRE(ariabc_pg::tx_tag_client_id(item.tx_tag) == 1);
    REQUIRE(verifier.check(item) == ariabc_pg::tx_sig_check::OK);

    // Tampering: SQL body, det-seq prefix, req_id, signature bits.
    {
        ariabc_pg::client_api_request_item t = item;
        t.sql = "s 00000042 UPDATE usertable SET field1 = 'evil' WHERE ycsb_key = 1042;";
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
        t = item;
        t.sql = "s 00000043 UPDATE usertable SET field1 = 'v' WHERE ycsb_key = 1042;";
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
        t = item;
        t.req_id = "cli-12346";
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
        t = item;
        t.tx_tag[kTxTagBytes - 1] ^= 0x01;
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
    }
    // Relabelling: client 1's signature presented as client 2's.
    {
        ariabc_pg::client_api_request_item t = item;
        ariabc_pg::tx_sig_put_u32_be(t.tx_tag, 2);
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
        ariabc_pg::tx_sig_put_u32_be(t.tx_tag, 99);
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::UNKNOWN_CLIENT);
    }
    // Forgery: a key that is not client 1's, signing as client 1 (what a
    // faulty replica holding only public keys could at best attempt).
    {
        ariabc_pg::client_api_request_item t = make_item(req_id, sql);
        sign_item(impostor, t);
        REQUIRE(ariabc_pg::tx_tag_client_id(t.tx_tag) == 1);
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
    }
    // Client 2 signs its own request: OK under its own id.
    {
        ariabc_pg::client_api_request_item t = make_item(req_id, sql);
        sign_item(c2, t);
        REQUIRE(verifier.check(t) == ariabc_pg::tx_sig_check::OK);
    }
    // Length prefixes: moving bytes between req_id and sql breaks the signature.
    {
        ariabc_pg::client_api_request_item a = make_item("ab", "c");
        sign_item(c1, a);
        ariabc_pg::client_api_request_item b = a;
        b.req_id = "a";
        b.sql = "bc";
        REQUIRE(verifier.check(a) == ariabc_pg::tx_sig_check::OK);
        REQUIRE(verifier.check(b) == ariabc_pg::tx_sig_check::BAD_SIGNATURE);
    }

    // ------------------------------------------------------ blocking frames
    {
        int fds[2];
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        std::memcpy(single.tx_tag, item.tx_tag, kTxTagBytes);
        REQUIRE(ariabc_pg::write_request_frame(fds[0], single, err));

        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 3; ++i) {
            ariabc_pg::client_api_request_item it = make_item("cli-" + std::to_string(i),
                                                             "s 0000000" + std::to_string(i) + " SELECT 1");
            if (i != 1) sign_item(c1, it);
            batch.batch_items.push_back(it);
        }
        REQUIRE(ariabc_pg::write_request_frame(fds[0], batch, err));

        ariabc_pg::client_api_request unsigned_req;
        unsigned_req.req_id = "legacy";
        unsigned_req.sql = "SELECT 2";
        REQUIRE(ariabc_pg::write_request_frame(fds[0], unsigned_req, err));

        ariabc_pg::client_api_request got;
        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(!got.is_batch() && got.req_id == req_id && got.sql == sql);
        REQUIRE(got.has_tx_tag && std::memcmp(got.tx_tag, item.tx_tag, kTxTagBytes) == 0);
        REQUIRE(!got.wait_for_terminal);
        REQUIRE(verifier.check(ariabc_pg::request_as_item(got)) == ariabc_pg::tx_sig_check::OK);

        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(got.batch_items.size() == 3);
        REQUIRE(got.batch_items[0].has_tx_tag && !got.batch_items[1].has_tx_tag && got.batch_items[2].has_tx_tag);
        for (size_t i = 0; i < 3; ++i) {
            REQUIRE(got.batch_items[i].sql == batch.batch_items[i].sql);
            REQUIRE(verifier.check(got.batch_items[i]) ==
                    (i == 1 ? ariabc_pg::tx_sig_check::MISSING : ariabc_pg::tx_sig_check::OK));
        }

        REQUIRE(ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(!got.has_tx_tag && got.req_id == "legacy" && got.sql == "SELECT 2");
        ::close(fds[0]);
        ::close(fds[1]);
    }

    // Retired formats are rejected, not misparsed: a 0xFFFFFFFD batch frame
    // (32-byte BLAKE3 tags) and a single-v2 frame with the tag flag.
    {
        int fds[2];
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        const uint32_t words[] = {htonl(0xFFFFFFFDu), htonl(1u), htonl(2u), htonl(1u), htonl(1u)};
        REQUIRE(::write(fds[0], words, sizeof(words)) == static_cast<ssize_t>(sizeof(words)));
        ariabc_pg::client_api_request got;
        REQUIRE(!ariabc_pg::read_request_frame(fds[1], got, err));
        ::close(fds[0]);
        ::close(fds[1]);

        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        const uint32_t v2[] = {htonl(0xFFFFFFFEu), htonl(0x2u), htonl(1000u), htonl(1u), htonl(1u)};
        REQUIRE(::write(fds[0], v2, sizeof(v2)) == static_cast<ssize_t>(sizeof(v2)));
        REQUIRE(!ariabc_pg::read_request_frame(fds[1], got, err));
        REQUIRE(err.find("retired") != std::string::npos);
        ::close(fds[0]);
        ::close(fds[1]);

        std::string wire(reinterpret_cast<const char*>(v2), sizeof(v2));
        wire.append(32 + 2, 'x');
        ariabc_pg::io_buffer buf;
        buf.append(wire.data(), wire.size());
        REQUIRE(ariabc_pg::try_decode_request_frame(buf, got, err) == ariabc_pg::decode_status::ERROR);
    }

    // --------------------------------- nonblocking codec, byte-by-byte decode
    {
        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 2; ++i) {
            ariabc_pg::client_api_request_item it = make_item("a-" + std::to_string(i),
                                                             "s 0000010" + std::to_string(i) + " SELECT 3");
            sign_item(c2, it);
            batch.batch_items.push_back(it);
        }
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        single.wait_for_terminal = true;
        single.terminal_timeout_ms = 1234;
        std::memcpy(single.tx_tag, item.tx_tag, kTxTagBytes);

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
                    REQUIRE(verifier.check(got.batch_items[i]) == ariabc_pg::tx_sig_check::OK);
                    REQUIRE(got.batch_items[i].sql == req->batch_items[i].sql);
                }
            } else {
                REQUIRE(got.has_tx_tag && std::memcmp(got.tx_tag, item.tx_tag, kTxTagBytes) == 0);
                REQUIRE(got.wait_for_terminal && got.terminal_timeout_ms == 1234);
                REQUIRE(got.req_id == req_id && got.sql == sql);
            }

            // The blocking reader accepts the same bytes.
            int fds[2];
            REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
            REQUIRE(::write(fds[0], wire.data(), wire.size()) == static_cast<ssize_t>(wire.size()));
            ariabc_pg::client_api_request got2;
            REQUIRE(ariabc_pg::read_request_frame(fds[1], got2, err));
            REQUIRE(got2.item_count() == req->item_count());
            ::close(fds[0]);
            ::close(fds[1]);
        }
    }

    // --------------------------------------------------------- Raft log entry
    {
        ariabc_pg::client_api_request single;
        single.req_id = req_id;
        single.sql = sql;
        single.has_tx_tag = true;
        std::memcpy(single.tx_tag, item.tx_tag, kTxTagBytes);
        nuraft::ptr<nuraft::buffer> log = ariabc_pg::build_raft_request_log(single, 2, err);
        REQUIRE(log);
        ariabc_pg::raft_request_batch parsed;
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 1 && parsed.leader_node_hint == 2);
        REQUIRE(verifier.check(parsed.items[0]) == ariabc_pg::tx_sig_check::OK);

        ariabc_pg::client_api_request batch;
        for (int i = 0; i < 4; ++i) {
            ariabc_pg::client_api_request_item it = make_item("b-" + std::to_string(i),
                                                             "s 0000020" + std::to_string(i) + " SELECT 4");
            it.has_assigned_det_seq = (i % 2) == 0;
            it.assigned_det_seq = 200 + i;
            if (i != 3) sign_item(i == 2 ? c2 : c1, it);
            batch.batch_items.push_back(it);
        }
        log = ariabc_pg::build_raft_request_log(batch, -1, err);
        REQUIRE(log);
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 4);
        for (size_t i = 0; i < 4; ++i) {
            REQUIRE(parsed.items[i].has_tx_tag == (i != 3));
            REQUIRE(parsed.items[i].has_assigned_det_seq == ((i % 2) == 0));
            REQUIRE(parsed.items[i].assigned_det_seq == 200 + i);
            REQUIRE(verifier.check(parsed.items[i]) ==
                    (i == 3 ? ariabc_pg::tx_sig_check::MISSING : ariabc_pg::tx_sig_check::OK));
        }

        // An unsigned single request still uses the legacy entry format.
        ariabc_pg::client_api_request legacy;
        legacy.req_id = "legacy";
        legacy.sql = "SELECT 5";
        log = ariabc_pg::build_raft_request_log(legacy, 1, err);
        REQUIRE(log);
        REQUIRE(ariabc_pg::parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 1 && !parsed.items[0].has_tx_tag);

        // A V3 entry (32-byte BLAKE3 tags) is rejected.
        nuraft::ptr<nuraft::buffer> v3 = nuraft::buffer::alloc(256);
        {
            nuraft::buffer_serializer bs(v3);
            bs.put_str("__ARIABC_RAFT_BATCH_V3__");
            bs.put_i32(1);
            bs.put_u32(1);
            bs.put_str("r");
            bs.put_str("SELECT 1");
        }
        REQUIRE(!ariabc_pg::parse_raft_request_log(*v3, parsed, err));
    }

    // ------------------------------------------------------ result envelopes
    {
        ariabc_pg::ed25519_private_key n1, n2;
        REQUIRE(n1.load_pem_file(ariabc_pg::private_key_path(dir, "node", 1), err));
        REQUIRE(n2.load_pem_file(ariabc_pg::private_key_path(dir, "node", 2), err));
        ariabc_pg::ed25519_key_roster nodes;
        REQUIRE(nodes.load_dir(dir, "node", err));

        std::string inner = "B4";
        inner.append(200, '\x5a');
        std::string sealed;
        REQUIRE(ariabc_pg::seal_result_envelope(n1, 1, inner, sealed));
        REQUIRE(ariabc_pg::is_result_envelope(sealed));
        REQUIRE(sealed.size() == inner.size() + 8 + 64);

        uint16_t signer = 0;
        std::string out;
        REQUIRE(ariabc_pg::open_result_envelope(sealed, &nodes, signer, out) == ariabc_pg::envelope_status::OK);
        REQUIRE(signer == 1 && out == inner);

        // Any changed byte in the inner payload.
        std::string t = sealed;
        t[8 + 100] ^= 0x01;
        REQUIRE(ariabc_pg::open_result_envelope(t, &nodes, signer, out) == ariabc_pg::envelope_status::BAD_SIGNATURE);
        // Relabel node 1's message as node 2's.
        t = sealed;
        t[2] = 2;
        REQUIRE(ariabc_pg::open_result_envelope(t, &nodes, signer, out) == ariabc_pg::envelope_status::BAD_SIGNATURE);
        REQUIRE(signer == 2);
        // Node 2 claims to be node 1 (signs with its own key, header says 1).
        std::string forged;
        REQUIRE(ariabc_pg::seal_result_envelope(n2, 1, inner, forged));
        REQUIRE(ariabc_pg::open_result_envelope(forged, &nodes, signer, out) == ariabc_pg::envelope_status::BAD_SIGNATURE);
        // Unknown signer / no roster.
        std::string unknown;
        REQUIRE(ariabc_pg::seal_result_envelope(n1, 9, inner, unknown));
        REQUIRE(ariabc_pg::open_result_envelope(unknown, &nodes, signer, out) == ariabc_pg::envelope_status::UNKNOWN_SIGNER);
        REQUIRE(ariabc_pg::open_result_envelope(sealed, nullptr, signer, out) == ariabc_pg::envelope_status::UNKNOWN_SIGNER);
        // Truncated / extended.
        REQUIRE(ariabc_pg::open_result_envelope(sealed.substr(0, sealed.size() - 1), &nodes, signer, out) ==
                ariabc_pg::envelope_status::MALFORMED);
        REQUIRE(ariabc_pg::open_result_envelope(sealed + "x", &nodes, signer, out) ==
                ariabc_pg::envelope_status::MALFORMED);
        REQUIRE(!ariabc_pg::is_result_envelope(inner));
    }

    // ------------------------------------------------------ parallel pool
    {
        ariabc_pg::parallel_for_pool pool(3);
        for (size_t n : {0u, 1u, 15u, 16u, 17u, 1000u}) {
            std::vector<std::atomic<int>> hits(n);
            for (auto& h : hits) h.store(0);
            pool.run(n, [&](size_t i) { hits[i].fetch_add(1); });
            for (size_t i = 0; i < n; ++i) REQUIRE(hits[i].load() == 1);
        }
        // Mixed verdicts computed in parallel match the serial ones.
        std::vector<ariabc_pg::client_api_request_item> items;
        for (int i = 0; i < 300; ++i) {
            ariabc_pg::client_api_request_item it = make_item("p-" + std::to_string(i), "SELECT " + std::to_string(i));
            if (i % 5 != 0) sign_item(i % 2 ? c1 : c2, it);
            if (i % 7 == 0 && it.has_tx_tag) it.sql += " ";
            items.push_back(it);
        }
        std::vector<ariabc_pg::tx_sig_check> par(items.size());
        pool.run(items.size(), [&](size_t i) { par[i] = verifier.check(items[i]); });
        for (size_t i = 0; i < items.size(); ++i) REQUIRE(par[i] == verifier.check(items[i]));
    }

    // ------------------------------------------------------ micro-benchmarks
    {
        const int iterations = 5000;
        std::vector<ariabc_pg::client_api_request_item> items(iterations);
        for (int i = 0; i < iterations; ++i) items[i] = make_item("cli-" + std::to_string(i), sql);
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) sign_item(c1, items[i]);
        const auto t1 = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            if (verifier.check(items[i]) != ariabc_pg::tx_sig_check::OK) throw std::runtime_error("verify failed");
        }
        const auto t2 = std::chrono::steady_clock::now();
        ariabc_pg::parallel_for_pool pool(3);
        std::vector<ariabc_pg::tx_sig_check> res(iterations);
        pool.run(iterations, [&](size_t i) { res[i] = verifier.check(items[i]); });
        const auto t3 = std::chrono::steady_clock::now();
        const double sign_ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / iterations;
        const double verify_ns = std::chrono::duration<double, std::nano>(t2 - t1).count() / iterations;
        const double pverify_ns = std::chrono::duration<double, std::nano>(t3 - t2).count() / iterations;

        ariabc_pg::ed25519_private_key n1;
        REQUIRE(n1.load_pem_file(ariabc_pg::private_key_path(dir, "node", 1), err));
        ariabc_pg::ed25519_key_roster nodes;
        REQUIRE(nodes.load_dir(dir, "node", err));
        std::string inner(20 * 110, 'r');  // ~20 B4 records
        std::string sealed, out;
        uint16_t signer = 0;
        const auto e0 = std::chrono::steady_clock::now();
        for (int i = 0; i < 2000; ++i) REQUIRE(ariabc_pg::seal_result_envelope(n1, 1, inner, sealed));
        const auto e1 = std::chrono::steady_clock::now();
        for (int i = 0; i < 2000; ++i) {
            REQUIRE(ariabc_pg::open_result_envelope(sealed, &nodes, signer, out) == ariabc_pg::envelope_status::OK);
        }
        const auto e2 = std::chrono::steady_clock::now();
        std::cout << "request: avg_sign_ns=" << sign_ns << " avg_verify_ns=" << verify_ns
                  << " avg_verify_ns_4threads=" << pverify_ns << " sql_bytes=" << sql.size() << std::endl;
        std::cout << "result message (" << inner.size() << " B): avg_seal_ns="
                  << std::chrono::duration<double, std::nano>(e1 - e0).count() / 2000
                  << " avg_open_ns=" << std::chrono::duration<double, std::nano>(e2 - e1).count() / 2000
                  << std::endl;
    }

    remove_tree(dir, {"client_1.key.pem", "client_1.pub.pem", "client_2.key.pem", "client_2.pub.pem",
                      "node_1.key.pem", "node_1.pub.pem", "node_2.key.pem", "node_2.pub.pem"});
    remove_tree(other_dir, {"client_1.key.pem", "client_1.pub.pem"});
    remove_tree(bad_dir, {"client_7.pub.pem"});
    std::cout << "ALL Ed25519 signature tests PASSED" << std::endl;
    return 0;
}
