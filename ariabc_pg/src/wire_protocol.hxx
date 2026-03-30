#pragma once

#include <cstdint>
#include <string>

namespace ariabc_pg {

struct client_api_request {
    std::string req_id;
    std::string sql;
};

struct client_api_response {
    // 0 = OK, 1 = NOT_ACCEPTED, 2 = ERROR
    uint8_t status = 2;
    std::string msg;
};

bool read_request_frame(int fd, client_api_request& out_req, std::string& err);
bool write_response_frame(int fd, const client_api_response& resp, std::string& err);

bool write_request_frame(int fd, const client_api_request& req, std::string& err);
bool read_response_frame(int fd, client_api_response& out_resp, std::string& err);

} // namespace ariabc_pg

