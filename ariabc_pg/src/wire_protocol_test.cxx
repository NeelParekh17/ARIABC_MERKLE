#include "wire_protocol.hxx"
#include "wire_protocol_async.hxx"

#include <cstdlib>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

#define REQUIRE(expr) do { if (!(expr)) { \
    std::cerr << __LINE__ << ": " #expr << std::endl; std::abort(); \
} } while (0)

using namespace ariabc_pg;

int main() {
    const char* controls[] = {
        "__ARIABC_CTRL_GET_USER_ABORTS", "__ARIABC_CTRL_FUTURE_COMMAND",
        "__ARIABC_CTRL_GET_USER_ABORTS ", " __ARIABC_CTRL_GET_USER_ABORTS;",
        "\t__ariabc_ctrl_get_user_aborts", "WAIT_RESULT", "WAIT_RESULTS 2 100",
        "wait_result_id req-1", "s 00000001 __ARIABC_CTRL_GET_USER_ABORTS",
        "/* outer /* nested */ comment */ -- comment\n__ARIABC_CTRL_GET_USER_ABORTS",
        "s 123 /* comment */ WAIT_RESULT_ID req-1",
        "s __ARIABC_CTRL_GET_USER_ABORTS", "; ; __ARIABC_CTRL_GET_USER_ABORTS"
    };
    const char* workload[] = {
        "SELECT 1", "s 00000001 SELECT 1", "",
        "SELECT '__ARIABC_CTRL_GET_USER_ABORTS'", "SELECT 1 /* WAIT_RESULT */",
        "/* __ARIABC_CTRL_FOO */ SELECT 1", "-- WAIT_RESULTS\nSELECT 1",
        "SELECT 1 AS wait_result", "SELECT 1 AS __ariabc_ctrl_test"
    };
    for (const char* sql : workload) REQUIRE(!is_control_sql(sql));

    for (const char* sql : controls) {
        REQUIRE(is_control_sql(sql));
        client_api_request req;
        req.req_id = "ctrl-regression";
        req.sql = sql;
        std::string err;
        REQUIRE(!build_raft_request_log(req, 1, err));
        REQUIRE(err == "UNKNOWN_OR_MALFORMED_CONTROL_COMMAND");

        // Controls still travel as single RPCs. Both wire encoders must retain
        // them exactly so the server can dispatch supported commands locally.
        std::string bytes;
        REQUIRE(encode_request_frame(req, bytes, err));
        int fds[2];
        REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        REQUIRE(::write(fds[0], bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
        client_api_request decoded;
        REQUIRE(read_request_frame(fds[1], decoded, err));
        REQUIRE(decoded.sql == sql);
        REQUIRE(decoded.req_id == req.req_id);
        ::close(fds[0]);
        ::close(fds[1]);

        client_api_request_item valid;
        valid.req_id = "valid";
        valid.sql = "SELECT 1";
        req.batch_items = {valid, request_as_item(req)};
        REQUIRE(!build_raft_request_log(req, 1, err));
        REQUIRE(err == "CONTROL_COMMAND_IN_WORKLOAD ord=1");
    }
    for (const char* sql : workload) {
        client_api_request req;
        req.req_id = "workload";
        req.sql = sql;
        std::string err;
        auto log = build_raft_request_log(req, 1, err);
        REQUIRE(log);
        raft_request_batch parsed;
        REQUIRE(parse_raft_request_log(*log, parsed, err));
        REQUIRE(parsed.items.size() == 1);
        REQUIRE(parsed.items[0].sql == sql);
    }
    std::cout << "Control/workload separation and wire round-trip tests PASS" << std::endl;
}
