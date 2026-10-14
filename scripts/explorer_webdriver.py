#!/usr/bin/env python3
"""Minimal W3C WebDriver session for the explorer checks: isolated Firefox profile, loopback HTTP
server over the checkout, artifacts under <checkout>/.bench_tmp (never /tmp on lab hosts)."""
import base64
import functools
import http.server
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

GECKO = '/snap/bin/geckodriver'
FIREFOX = '/snap/firefox/current/usr/lib/firefox/firefox'


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


class QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        # The page's freshness check and navigations can abort fetches mid-response.
        if not isinstance(__import__('sys').exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            super().handle_error(request, client_address)


class Session:
    def __init__(self, source, prefix):
        if not Path('/home/neel/Desktop/ariabc_cluster').is_dir() or not Path(GECKO).exists():
            raise SystemExit('Run on lab 10.129.148.247; Firefox/geckodriver required.')
        self.source = Path(source).resolve()
        (self.source / '.bench_tmp').mkdir(exist_ok=True)
        self.out = Path(tempfile.mkdtemp(prefix=prefix + time.strftime('%Y%m%d_'), dir=self.source / '.bench_tmp'))
        self.web = QuietServer(('127.0.0.1', 0), functools.partial(Quiet, directory=str(self.source)))
        threading.Thread(target=self.web.serve_forever, daemon=True).start()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.log = (self.out / 'geckodriver.log').open('w')
        self.driver = subprocess.Popen([GECKO, '--allow-system-access', '--host', '127.0.0.1', '--port', str(port)],
                                       stdout=self.log, stderr=subprocess.STDOUT)
        self.base = f'http://127.0.0.1:{port}'
        self.site = f'http://127.0.0.1:{self.web.server_port}'
        self.sid = None
        self.results = []
        for _ in range(100):
            try:
                self.call('/status')
                break
            except (OSError, RuntimeError):
                time.sleep(.1)
        self.sid = self.call('/session', {'capabilities': {'alwaysMatch': {'browserName': 'firefox', 'moz:firefoxOptions': {
            'binary': FIREFOX, 'args': ['-headless'], 'prefs': {'browser.shell.checkDefaultBrowser': False}}}}})['sessionId']

    def call(self, path, data=None, method=None):
        req = urllib.request.Request(self.base + path, data=json.dumps(data).encode() if data is not None else None,
                                     headers={'Content-Type': 'application/json'}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.load(response)
        except urllib.error.HTTPError as e:
            raise RuntimeError(e.read().decode()) from e
        value = result.get('value')
        if isinstance(value, dict) and 'error' in value:
            raise RuntimeError(value)
        return value

    def js(self, code):
        return self.call(f'/session/{self.sid}/execute/sync', {'script': code, 'args': []})

    def open(self, page):
        self.call(f'/session/{self.sid}/url', {'url': f'{self.site}/{page}'})

    def size(self, width, height=1100):
        self.call(f'/session/{self.sid}/window/rect', {'width': width, 'height': height})

    def check(self, name, condition):
        if not condition:
            raise AssertionError(name)
        self.results.append(name)

    def shot(self, name, settle=.35):
        time.sleep(settle)
        (self.out / (name + '.png')).write_bytes(base64.b64decode(self.call(f'/session/{self.sid}/screenshot')))

    def chrome_pref(self, script):
        self.call(f'/session/{self.sid}/moz/context', {'context': 'chrome'})
        self.call(f'/session/{self.sid}/execute/sync', {'script': script, 'args': []})
        self.call(f'/session/{self.sid}/moz/context', {'context': 'content'})

    def close(self):
        if self.sid:
            try:
                self.call(f'/session/{self.sid}', method='DELETE')
            except Exception:
                pass
        self.driver.terminate()
        try:
            self.driver.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.driver.kill()
            self.driver.wait()
        self.web.shutdown()
        self.web.server_close()
        self.log.close()
