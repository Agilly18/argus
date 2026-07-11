#!/usr/bin/env python3
"""Canberra COP PoC server.

Serves the static page and relays the ACT ESA incidents feed at /esa —
the upstream feed deliberately sends no CORS headers, so a browser page
can't fetch it directly. Run:  python3 serve.py  →  http://localhost:8899
"""
import json
import time
import urllib.request
from http.server import HTTPServer, SimpleHTTPRequestHandler

ESA_FEED = "https://esa.act.gov.au/feeds/allincidents.json"
CACHE_SECONDS = 60  # ESA updates every 60 s; don't hammer them harder

_cache = {"time": 0.0, "body": b"[]"}


def esa_body():
    if time.time() - _cache["time"] > CACHE_SECONDS:
        req = urllib.request.Request(ESA_FEED, headers={"User-Agent": "canberra-cop-poc"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        json.loads(body)  # refuse to cache junk
        _cache.update(time=time.time(), body=body)
    return _cache["body"]


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.rstrip("/") == "/esa":
            try:
                body = esa_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            except Exception:
                body = b'{"error": "esa feed unreachable"}'
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def log_message(self, *args):
        pass  # keep the terminal quiet


if __name__ == "__main__":
    print("Canberra COP PoC → http://localhost:8899/poc.html  (Ctrl-C to stop)")
    HTTPServer(("0.0.0.0", 8899), Handler).serve_forever()
