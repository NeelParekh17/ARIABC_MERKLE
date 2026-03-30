#include "wire_protocol_async.hxx"

#include <arpa/inet.h>

#include <cstring>

namespace ariabc_pg {
namespace {

const uint32_t kMaxReqIdBytes = 16u * 1024u * 1024u;
const uint32_t kMaxSqlBytes = 64u * 1024u * 1024u;
const uint32_t kMaxMsgBytes = 16u * 1024u * 1024u;

bool read_u32_be(const char* p, uint32_t& out) {
    if (!p) return false;
    uint32_t tmp = 0;
    std::memcpy(&tmp, p, sizeof(tmp));
    out = ntohl(tmp);
    return true;
}

void append_u32_be(std::string& out, uint32_t v) {
    const uint32_t tmp = htonl(v);
    out.append(reinterpret_cast<const char*>(&tmp), sizeof(tmp));
}

} // namespace

bool encode_request_frame(const client_api_request& req, std::string& out, std::string& err) {
    err.clear();
    if (req.req_id.size() > kMaxReqIdBytes || req.sql.size() > kMaxSqlBytes) {
        err = "request too large";
        return false;
    }
    out.clear();
    out.reserve(8 + req.req_id.size() + req.sql.size());
    append_u32_be(out, static_cast<uint32_t>(req.req_id.size()));
    append_u32_be(out, static_cast<uint32_t>(req.sql.size()));
    if (!req.req_id.empty()) out.append(req.req_id);
    if (!req.sql.empty()) out.append(req.sql);
    return true;
}

bool encode_response_frame(const client_api_response& resp, std::string& out, std::string& err) {
    err.clear();
    if (resp.msg.size() > kMaxMsgBytes) {
        err = "response too large";
        return false;
    }
    out.clear();
    out.reserve(1 + 4 + resp.msg.size());
    const uint8_t st = resp.status;
    out.push_back(static_cast<char>(st));
    append_u32_be(out, static_cast<uint32_t>(resp.msg.size()));
    if (!resp.msg.empty()) out.append(resp.msg);
    return true;
}

decode_status try_decode_request_frame(io_buffer& buf, client_api_request& out_req, std::string& err) {
    err.clear();
    const size_t avail = buf.size();
    if (avail < 8) return decode_status::NEED_MORE;
    const char* p = buf.ptr();
    uint32_t req_len = 0;
    uint32_t sql_len = 0;
    if (!read_u32_be(p, req_len) || !read_u32_be(p + 4, sql_len)) {
        err = "bad header";
        return decode_status::ERROR;
    }
    if (req_len > kMaxReqIdBytes || sql_len > kMaxSqlBytes) {
        err = "frame too large";
        return decode_status::ERROR;
    }
    const size_t total = 8ull + static_cast<size_t>(req_len) + static_cast<size_t>(sql_len);
    if (avail < total) return decode_status::NEED_MORE;
    out_req.req_id.assign(p + 8, p + 8 + req_len);
    out_req.sql.assign(p + 8 + req_len, p + total);
    buf.consume(total);
    return decode_status::OK;
}

decode_status try_decode_response_frame(io_buffer& buf, client_api_response& out_resp, std::string& err) {
    err.clear();
    const size_t avail = buf.size();
    if (avail < 5) return decode_status::NEED_MORE;
    const char* p = buf.ptr();
    const uint8_t st = static_cast<uint8_t>(p[0]);
    uint32_t msg_len = 0;
    if (!read_u32_be(p + 1, msg_len)) {
        err = "bad header";
        return decode_status::ERROR;
    }
    if (msg_len > kMaxMsgBytes) {
        err = "response too large";
        return decode_status::ERROR;
    }
    const size_t total = 5ull + static_cast<size_t>(msg_len);
    if (avail < total) return decode_status::NEED_MORE;
    out_resp.status = st;
    out_resp.msg.assign(p + 5, p + total);
    buf.consume(total);
    return decode_status::OK;
}

} // namespace ariabc_pg

