#!/usr/bin/env python3
import socket
import struct
import sys

def query_node(host, port, command):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((host, port))
        req_id = b""
        sql = command.encode('utf-8')
        s.sendall(struct.pack("!II", len(req_id), len(sql)) + req_id + sql)

        status_byte = s.recv(1)
        if not status_byte:
            return None
        status = struct.unpack("B", status_byte)[0]

        msg_len_bytes = s.recv(4)
        if not msg_len_bytes:
            return None
        msg_len = struct.unpack("!I", msg_len_bytes)[0]

        msg_bytes = b""
        while len(msg_bytes) < msg_len:
            chunk = s.recv(msg_len - len(msg_bytes))
            if not chunk:
                break
            msg_bytes += chunk

        s.close()
        if status == 0:
            return msg_bytes.decode('utf-8').strip()
    except Exception:
        pass
    return None

def main():
    if len(sys.argv) < 3:
        print("Usage: check_leader.py <hosts_csv> <ports_csv> [--check-leader | --print-all]")
        sys.exit(1)

    hosts = sys.argv[1].split(',')
    ports = [int(p) for p in sys.argv[2].split(',')]

    mode = "--check-leader"
    if len(sys.argv) >= 4:
        mode = sys.argv[3]

    if mode == "--check-leader":
        # Find which node is leader (returns its index or ID)
        for i, (host, port) in enumerate(zip(hosts, ports)):
            val = query_node(host, port, "__ARIABC_CTRL_IS_LEADER")
            if val == "1":
                # Found the leader!
                print(i)
                sys.exit(0)
        print("-1")
        sys.exit(1)
    elif mode == "--print-all":
        for i, (host, port) in enumerate(zip(hosts, ports)):
            leader_val = query_node(host, port, "__ARIABC_CTRL_IS_LEADER")
            commit_idx = query_node(host, port, "__ARIABC_CTRL_GET_COMMIT_INDEX")
            print(f"Node {i} ({host}:{port}): is_leader={leader_val} commit_index={commit_idx}")

if __name__ == "__main__":
    main()
