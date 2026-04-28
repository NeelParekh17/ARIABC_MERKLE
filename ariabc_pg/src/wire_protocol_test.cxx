#include "wire_protocol.hxx"
#include "wire_protocol_async.hxx"

#include <stdexcept>
#include <string>
#include <vector>
#include <iostream>

#include <unistd.h>

namespace {

using ariabc_pg::client_api_request;
using ariabc_pg::client_api_request_item;
using ariabc_pg::decode_status;
using ariabc_pg::io_buffer;
using ariabc_pg::raft_request_batch;

void require(bool cond, const std::string& msg) {
    if (!cond) {
        throw std::runtime_error(msg);
    }
}

client_api_request make_single_request() {
    client_api_request req;
    req.req_id = "cli-1";
    req.sql = "s 00000001 SELECT 1;";
    return req;
}

client_api_request make_batch_request() {
    client_api_request req;
    req.req_id = "cli-10";
    req.sql = "__ARIABC_BATCH items=3";

    client_api_request_item a;
    a.req_id = "cli-10";
    a.sql = "s 00000010 UPDATE kv SET v = 10 WHERE k = 1;";
    req.batch_items.push_back(a);

    client_api_request_item b;
    b.req_id = "cli-11";
    b.sql = "s 00000011 UPDATE kv SET v = 11 WHERE k = 2;";
    req.batch_items.push_back(b);

    client_api_request_item c;
    c.req_id = "cli-12";
    c.sql = "s 00000012 SELECT v FROM kv WHERE k = 3;";
    req.batch_items.push_back(c);
    return req;
}

void test_sync_single_roundtrip() {
    int fds[2];
    require(::pipe(fds) == 0, "pipe failed");

    client_api_request req = make_single_request();
    std::string err;
    require(ariabc_pg::write_request_frame(fds[1], req, err), "single write failed: " + err);
    ::close(fds[1]);

    client_api_request out;
    require(ariabc_pg::read_request_frame(fds[0], out, err), "single read failed: " + err);
    ::close(fds[0]);

    require(!out.is_batch(), "single request decoded as batch");
    require(out.req_id == req.req_id, "single req_id mismatch");
    require(out.sql == req.sql, "single sql mismatch");
}

void test_sync_batch_roundtrip() {
    int fds[2];
    require(::pipe(fds) == 0, "pipe failed");

    client_api_request req = make_batch_request();
    std::string err;
    require(ariabc_pg::write_request_frame(fds[1], req, err), "batch write failed: " + err);
    ::close(fds[1]);

    client_api_request out;
    require(ariabc_pg::read_request_frame(fds[0], out, err), "batch read failed: " + err);
    ::close(fds[0]);

    require(out.is_batch(), "batch request decoded as single");
    require(out.batch_items.size() == req.batch_items.size(), "batch size mismatch");
    for (size_t i = 0; i < req.batch_items.size(); ++i) {
        require(out.batch_items[i].req_id == req.batch_items[i].req_id, "batch req_id mismatch");
        require(out.batch_items[i].sql == req.batch_items[i].sql, "batch sql mismatch");
    }
}

void test_async_batch_roundtrip() {
    client_api_request req = make_batch_request();
    std::string enc;
    std::string err;
    require(ariabc_pg::encode_request_frame(req, enc, err), "async encode failed: " + err);

    io_buffer buf;
    buf.append(enc.data(), enc.size());
    client_api_request out;
    const decode_status st = ariabc_pg::try_decode_request_frame(buf, out, err);
    require(st == decode_status::OK, "async decode failed: " + err);
    require(out.is_batch(), "async batch decoded as single");
    require(out.batch_items.size() == req.batch_items.size(), "async batch size mismatch");
    require(buf.size() == 0, "async buffer not fully consumed");
}

void test_raft_log_roundtrip_single() {
    client_api_request req = make_single_request();
    std::string err;
    nuraft::ptr<nuraft::buffer> log = ariabc_pg::build_raft_request_log(req, 2, err);
    require(static_cast<bool>(log), "single raft log build failed: " + err);

    raft_request_batch decoded;
    require(ariabc_pg::parse_raft_request_log(*log, decoded, err),
            "single raft log parse failed: " + err);
    require(decoded.items.size() == 1, "single raft log decoded wrong item count");
    require(decoded.leader_node_hint == 2, "single raft leader hint mismatch");
    require(decoded.items[0].req_id == req.req_id, "single raft req_id mismatch");
    require(decoded.items[0].sql == req.sql, "single raft sql mismatch");
}

void test_raft_log_roundtrip_batch_order() {
    client_api_request req = make_batch_request();
    std::string err;
    nuraft::ptr<nuraft::buffer> log = ariabc_pg::build_raft_request_log(req, 3, err);
    require(static_cast<bool>(log), "batch raft log build failed: " + err);

    raft_request_batch decoded;
    require(ariabc_pg::parse_raft_request_log(*log, decoded, err),
            "batch raft log parse failed: " + err);
    require(decoded.items.size() == req.batch_items.size(), "batch raft item count mismatch");
    require(decoded.leader_node_hint == 3, "batch raft leader hint mismatch");
    for (size_t i = 0; i < req.batch_items.size(); ++i) {
        require(decoded.items[i].req_id == req.batch_items[i].req_id,
                "batch raft req_id order mismatch");
        require(decoded.items[i].sql == req.batch_items[i].sql,
                "batch raft sql order mismatch");
    }
}

} // namespace

int main() {
    try {
        test_sync_single_roundtrip();
        test_sync_batch_roundtrip();
        test_async_batch_roundtrip();
        test_raft_log_roundtrip_single();
        test_raft_log_roundtrip_batch_order();
    } catch (const std::exception& e) {
        std::cerr << "wire_protocol_test FAILED: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "wire_protocol_test PASS" << std::endl;
    return 0;
}
