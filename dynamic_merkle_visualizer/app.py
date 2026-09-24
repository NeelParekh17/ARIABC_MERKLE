#!/usr/bin/env python3
"""Local HTTP UI over the live AriaBC PostgreSQL Merkle index."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import psycopg

if __package__:
    from .db import Inspector, InspectorError, Settings
else:
    from db import Inspector, InspectorError, Settings

STATIC = Path(__file__).with_name("static")


class VisualizerServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, inspector):
        super().__init__(address, Handler)
        self.inspector = inspector
        self.token = secrets.token_urlsafe(32)


class Handler(BaseHTTPRequestHandler):
    server_version = "AriaBCMerkleInspector/1"

    def log_message(self, fmt, *args):
        # Avoid logging query strings, row values, or database connection details.
        if args and isinstance(args[0], str):
            print(f"HTTP {self.command} {urlsplit(self.path).path}", flush=True)

    def respond(self, status, body, content_type="application/json; charset=utf-8"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def same_host(self):
        port = self.server.server_address[1]
        return self.headers.get("Host") in {f"127.0.0.1:{port}", f"localhost:{port}"}

    def do_GET(self):
        self.dispatch(False)

    def do_POST(self):
        self.dispatch(True)

    def dispatch(self, post):
        try:
            if not self.same_host():
                raise InspectorError("Use the local inspector address", 403)
            parsed = urlsplit(self.path)
            path = parsed.path
            inspector = self.server.inspector
            if post:
                origin = self.headers.get("Origin")
                if origin and origin != f"http://{self.headers.get('Host')}":
                    raise InspectorError("Cross-origin writes are rejected", 403)
                if not secrets.compare_digest(self.headers.get("X-Merkle-Token", ""), self.server.token):
                    raise InspectorError("Reload this page before sending a write", 403)
                if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                    raise InspectorError("Use application/json", 415)
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 65536:
                    raise InspectorError("Request body must be between 1 and 65536 bytes", 413)
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise InspectorError("Expected a JSON object")
                if path != "/api/mutate":
                    raise InspectorError("Unknown operation", 404)
                self.respond(200, inspector.mutate(body))
                return
            files = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"), "/style.css": ("style.css", "text/css")}
            if path in files:
                name, mime = files[path]
                self.respond(200, (STATIC / name).read_bytes(), mime + "; charset=utf-8")
                return
            params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            if path == "/api/catalog":
                result = inspector.catalog()
                result["token"] = self.server.token
            elif path == "/api/snapshot":
                result = inspector.snapshot(params.get("index_oid"), params.get("partition", 0),
                    params.get("node_id", "0000000000000000"), params.get("prefix_len", 0), params.get("verify") == "1")
            elif path == "/api/rows":
                result = inspector.rows(params.get("index_oid"), params.get("offset", 0), params.get("partition"),
                    params.get("node_id", "0000000000000000"), params.get("prefix_len", 0))
            else:
                raise InspectorError("Not found", 404)
            self.respond(200, result)
        except InspectorError as exc:
            self.respond(exc.status, {"error": str(exc)})
        except psycopg.Error as exc:
            # Do not echo arbitrary SQL/values or a credential-bearing DSN.
            if isinstance(exc, psycopg.OperationalError) and not exc.sqlstate:
                message = "Cannot connect to PostgreSQL. Check MERKLE_VIZ_CONNINFO and whether the custom server is running."
            elif exc.sqlstate in ("55P03", "57014"):
                message = "Database read/write timed out or is blocked by another transaction. Retry after that work completes."
            else:
                message = exc.diag.message_primary or "PostgreSQL rejected the operation"
            self.respond(409 if exc.sqlstate in ("55P03", "40001") else 400,
                         {"error": message, "sqlstate": exc.sqlstate})
        except (ValueError, TypeError) as exc:
            self.respond(400, {"error": "Invalid request: " + str(exc)[:150]})
        except Exception:
            self.respond(500, {"error": "Inspector failed unexpectedly. Check the server and database logs."})
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=int(os.getenv("MERKLE_VIZ_PORT", "8787")))
    args = parser.parse_args()
    conninfo = os.getenv("MERKLE_VIZ_CONNINFO", "")
    if not conninfo:
        parser.error("Set MERKLE_VIZ_CONNINFO, or run dynamic_merkle_visualizer/demo.py for an isolated real database")
    settings = Settings(conninfo, table=os.getenv("MERKLE_VIZ_TABLE", ""), index=os.getenv("MERKLE_VIZ_INDEX", ""),
                        allow_writes=os.getenv("MERKLE_VIZ_ALLOW_WRITES", "0") == "1")
    server = VisualizerServer(("127.0.0.1", args.port), Inspector(settings))
    print(f"Merkle inspector: http://127.0.0.1:{server.server_address[1]} (writes {'enabled' if settings.allow_writes else 'disabled'})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
