#include "wire_protocol.hxx"

#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

namespace ariabc_pg {
namespace {

bool read_exact(int fd, void* buf, size_t n, std::string& err) {
    uint8_t* p = reinterpret_cast<uint8_t*>(buf);
    size_t off = 0;
    while (off < n) {
        const ssize_t r = ::read(fd, p + off, n - off);
        if (r == 0) {
            err = "EOF";
            return false;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            err = std::string("read failed: ") + ::strerror(errno);
            return false;
        }
        off += static_cast<size_t>(r);
    }
    return true;
}

bool write_exact(int fd, const void* buf, size_t n, std::string& err) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    size_t off = 0;
    while (off < n) {
        const ssize_t w = ::write(fd, p + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            err = std::string("write failed: ") + ::strerror(errno);
            return false;
        }
        off += static_cast<size_t>(w);
    }
    return true;
}

bool read_u32(int fd, uint32_t& out, std::string& err) {
    uint32_t tmp = 0;
    if (!read_exact(fd, &tmp, sizeof(tmp), err)) return false;
    out = ntohl(tmp);
    return true;
}

bool write_u32(int fd, uint32_t v, std::string& err) {
    const uint32_t tmp = htonl(v);
    return write_exact(fd, &tmp, sizeof(tmp), err);
}

bool read_bytes(int fd, uint32_t n, std::string& out, std::string& err) {
    out.assign(n, '\0');
    if (n == 0) return true;
    return read_exact(fd, &out[0], n, err);
}

} // namespace

bool read_request_frame(int fd, client_api_request& out_req, std::string& err) {
    uint32_t req_len = 0;
    uint32_t sql_len = 0;
    if (!read_u32(fd, req_len, err)) return false;
    if (!read_u32(fd, sql_len, err)) return false;
    if (req_len > (16u * 1024u * 1024u) || sql_len > (64u * 1024u * 1024u)) {
        err = "frame too large";
        return false;
    }
    if (!read_bytes(fd, req_len, out_req.req_id, err)) return false;
    if (!read_bytes(fd, sql_len, out_req.sql, err)) return false;
    return true;
}

bool write_response_frame(int fd, const client_api_response& resp, std::string& err) {
    const uint8_t st = resp.status;
    if (!write_exact(fd, &st, sizeof(st), err)) return false;
    if (resp.msg.size() > (16u * 1024u * 1024u)) {
        err = "response too large";
        return false;
    }
    if (!write_u32(fd, static_cast<uint32_t>(resp.msg.size()), err)) return false;
    if (!resp.msg.empty() && !write_exact(fd, resp.msg.data(), resp.msg.size(), err)) return false;
    return true;
}

bool write_request_frame(int fd, const client_api_request& req, std::string& err) {
    if (req.req_id.size() > (16u * 1024u * 1024u) || req.sql.size() > (64u * 1024u * 1024u)) {
        err = "request too large";
        return false;
    }
    if (!write_u32(fd, static_cast<uint32_t>(req.req_id.size()), err)) return false;
    if (!write_u32(fd, static_cast<uint32_t>(req.sql.size()), err)) return false;
    if (!req.req_id.empty() && !write_exact(fd, req.req_id.data(), req.req_id.size(), err)) return false;
    if (!req.sql.empty() && !write_exact(fd, req.sql.data(), req.sql.size(), err)) return false;
    return true;
}

bool read_response_frame(int fd, client_api_response& out_resp, std::string& err) {
    uint8_t st = 2;
    if (!read_exact(fd, &st, sizeof(st), err)) return false;
    out_resp.status = st;
    uint32_t msg_len = 0;
    if (!read_u32(fd, msg_len, err)) return false;
    if (msg_len > (16u * 1024u * 1024u)) {
        err = "response too large";
        return false;
    }
    if (!read_bytes(fd, msg_len, out_resp.msg, err)) return false;
    return true;
}

} // namespace ariabc_pg

