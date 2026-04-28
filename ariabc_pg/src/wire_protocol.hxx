#pragma once

#include "nuraft.hxx"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ariabc_pg {

struct client_api_request_item {
    std::string req_id;
    std::string sql;
};

struct client_api_request {
    std::string req_id;
    std::string sql;
    std::vector<client_api_request_item> batch_items;

    bool is_batch() const { return !batch_items.empty(); }
    size_t item_count() const { return is_batch() ? batch_items.size() : 1u; }
};

struct client_api_response {
    // 0 = OK, 1 = NOT_ACCEPTED, 2 = ERROR
    uint8_t status = 2;
    std::string msg;
};

struct raft_request_batch {
    int leader_node_hint = -1;
    std::vector<client_api_request_item> items;
};

bool read_request_frame(int fd, client_api_request& out_req, std::string& err);
bool write_response_frame(int fd, const client_api_response& resp, std::string& err);

bool write_request_frame(int fd, const client_api_request& req, std::string& err);
bool read_response_frame(int fd, client_api_response& out_resp, std::string& err);

nuraft::ptr<nuraft::buffer> build_raft_request_log(const client_api_request& req,
                                                   int leader_node_hint,
                                                   std::string& err);
bool parse_raft_request_log(nuraft::buffer& data,
                            raft_request_batch& out,
                            std::string& err);

} // namespace ariabc_pg
