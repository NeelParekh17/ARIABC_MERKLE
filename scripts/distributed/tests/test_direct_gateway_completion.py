"""Run the real gateway against a delayed/failing completion protocol peer.

No database is needed. These tests exercise the distinction between an enqueue
acknowledgement and terminal completion, including every item of a DET batch.
"""
import os
from pathlib import Path
import socket
import socketserver
import struct
import subprocess
import tempfile
import threading
import time
import unittest

BINARY = Path(__file__).resolve().parents[3] / 'ariabc_pg/build/bin/ariabc_pg_gateway'


def receive(sock, size):
    data = b''
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


class Peer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def get_request(self):
        request = super().get_request()
        self.connections += 1
        return request


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        def uint():
            return struct.unpack('!I', receive(self.request, 4))[0]
        def item(first=None):
            a = uint() if first is None else first
            b = uint()
            return receive(self.request, a).decode(), receive(self.request, b).decode()
        try:
            while True:
                header = uint()
                status = 0
                if header == 0xffffffff:
                    items = [item() for _ in range(uint())]
                    self.server.submitted.extend(i[0] for i in items)
                    message = 'ACCEPTED_DIRECT raft_log_idx=2'
                else:
                    req, sql = item(header)
                    if sql.startswith('WAIT_RESULTS '):
                        ids = sql.split()[2:]
                        self.server.waited.extend(ids)
                        time.sleep(self.server.delay)
                        if self.server.disconnect_after is not None and len(self.server.waited) >= self.server.disconnect_after:
                            return
                        if getattr(self.server, 'failed', False):
                            status = 1
                            message = 'WAIT_RESULTS_FAILED reason=retry_exhausted'
                        elif self.server.bad:
                            message = ('WAIT_RESULTS_OK state=COMPLETED completed=' +
                                       str(len(ids) - 1))
                        else:
                            message = ('WAIT_RESULTS_OK state=COMPLETED completed=' +
                                       str(len(ids)))
                    elif sql.startswith('WAIT_RESULT_ID '):
                        req_id = sql.split()[1]
                        self.server.waited.append(req_id)
                        time.sleep(self.server.delay)
                        if self.server.disconnect_after is not None and len(self.server.waited) >= self.server.disconnect_after:
                            return
                        if getattr(self.server, 'failed', False):
                            status = 1
                            message = 'WAIT_RESULT_ID_FAILED req_id=' + req_id + ' reason=retry_exhausted'
                        elif self.server.bad:
                            message = 'ACCEPTED_DIRECT'
                        else:
                            message = ('WAIT_RESULT_ID_OK state=COMPLETED completion_source=test req_id=' + req_id)
                    elif sql.startswith('__ARIABC_CTRL_'):
                        message = '1'
                    else:
                        self.server.submitted.append(req)
                        message = 'ACCEPTED_DIRECT ' + req
                payload = message.encode()
                self.request.sendall(bytes([status]) + struct.pack('!I', len(payload)) + payload)
        except (EOFError, ConnectionError, OSError):
            pass


@unittest.skipUnless(BINARY.exists(), 'build ariabc_pg_gateway first')
class DirectCompletionTests(unittest.TestCase):
    def run_case(self, det, bad, query_count=2, disconnect_after=None, failed=False):
        with Peer(('127.0.0.1', 0), Handler) as peer, tempfile.TemporaryDirectory() as tmp:
            peer.submitted, peer.waited, peer.bad = [], [], bad
            peer.failed = failed
            peer.connections = 0
            peer.delay = .3 if query_count == 2 else .01
            peer.disconnect_after = disconnect_after
            thread = threading.Thread(target=peer.serve_forever, daemon=True)
            thread.start()
            path = Path(tmp) / 'queries.sql'
            path.write_text(''.join(f'SELECT {i};\n' for i in range(query_count)))
            command = [str(BINARY), '--nodes', f'127.0.0.1:{peer.server_address[1]}',
                       '--queryFrom', str(path), '--dbType', '1' if det else '0',
                       '--numTerminals', '1', '--submitMode', 'event', '--waitMajority', '0',
                       '--completionPath', 'direct', '--totalNodes', '1',
                       '--detBatchSize', '2', '--detWindow', '8',
                       '--nondetWindow', '8', '--connFanout', '1']
            if det:
                command += ['--detClientMode', 'event',
                            '--detPipelineDepth', '2', '--detSubmitPipeline', '1',
                            '--detStartSeq', '0', '--reqIdOffset', '1', '--dbConnPoolSize', '1']
            started = time.monotonic()
            try:
                result = subprocess.run(command, text=True, capture_output=True, timeout=10,
                                        env=dict(os.environ, ARIABC_PROFILE='1'))
                elapsed = time.monotonic() - started
            finally:
                peer.shutdown()
                thread.join()
            output = result.stdout + result.stderr
            if disconnect_after is not None:
                self.assertNotEqual(result.returncode, 0, output)
                self.assertIn('direct completion wait failed', output)
            elif bad or failed:
                self.assertNotEqual(result.returncode, 0, output)
                self.assertIn('direct_terminal_success_count=0', output)
            else:
                self.assertEqual(result.returncode, 0, output)
                self.assertCountEqual(peer.waited, peer.submitted)
                self.assertEqual(len(peer.waited), query_count)
                self.assertIn(f'direct_terminal_success_count={query_count}', output)
                self.assertGreaterEqual(elapsed, peer.delay)
                if not det:
                    # One probe socket, one submit socket, and one completion socket
                    # for this terminal, independent of the number of transactions.
                    self.assertEqual(peer.connections, 3)

    def test_pg_waits_for_terminal_results(self):
        self.run_case(False, False)

    def test_pg_rejects_acceptance_as_completion(self):
        self.run_case(False, True)

    def test_pg_rejects_failed_completion(self):
        self.run_case(False, False, failed=True)

    def test_pg_reuses_completion_connection(self):
        self.run_case(False, False, query_count=12)

    def test_pg_completion_disconnect_fails_closed(self):
        self.run_case(False, False, query_count=12, disconnect_after=3)

    def test_det_waits_for_entire_batch(self):
        self.run_case(True, False)

    def test_det_waits_across_multiple_batches(self):
        self.run_case(True, False, query_count=12)

    def test_det_rejects_partial_batch_completion(self):
        self.run_case(True, True)


if __name__ == '__main__':
    unittest.main()
