#!/usr/bin/env python3
"""Serve the explorer pages on loopback and open them in the default browser."""

import argparse
import functools
import http.server
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--page', choices=('architecture', 'determinism', 'both'),
                        default='architecture', help='Which explorer to open')
    parser.add_argument('--no-open', action='store_true', help='Print the URL without opening a browser')
    parser.add_argument('--serve', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error('--port must be between 1 and 65535')

    root = Path(__file__).resolve().parents[1]
    names = {'architecture': 'ARCHITECTURE_EXPLORER.html',
             'determinism': 'DETERMINISM_EXPLORER.html'}
    selected = tuple(names.values()) if args.page == 'both' else (names[args.page],)
    pages = [root / name for name in selected]
    for page in pages:
        if not page.is_file():
            parser.error(f'Explorer page is missing: {page}')
    urls = [f'http://127.0.0.1:{args.port}/{page.name}' for page in pages]

    if args.serve:
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(root))
        with http.server.ThreadingHTTPServer(('127.0.0.1', args.port), handler) as server:
            server.serve_forever()
        return

    expected = [page.read_bytes() for page in pages]

    def ready():
        try:
            for url, content in zip(urls, expected):
                with urllib.request.urlopen(url, timeout=1) as response:
                    if response.read() != content:
                        return False
            return True
        except (urllib.error.URLError, TimeoutError, OSError):
            return False

    if not ready():
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', args.port)) == 0:
                parser.error(f'Port {args.port} serves a different page. Choose another --port.')
        state = root / '.bench_tmp' / 'architecture_web'
        state.mkdir(parents=True, exist_ok=True)
        with (state / 'server.log').open('ab') as log:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), '--serve', '--port', str(args.port)],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
            )
        for _ in range(50):
            if ready():
                break
            if process.poll() is not None:
                parser.error(f'Web server could not start. See {state / "server.log"}')
            time.sleep(0.1)
        else:
            parser.error(f'Web server did not become ready. See {state / "server.log"}')
        (state / 'server.pid').write_text(str(process.pid) + '\n')

    for url in urls:
        print(url, flush=True)
        if not args.no_open:
            subprocess.run(['xdg-open', url], check=True)


if __name__ == '__main__':
    main()
