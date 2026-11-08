#!/usr/bin/env python3
"""Test website for compare.py: a small HTTPS page on :9443 and an HTTP download on
:9001 (/big?n=<bytes>). TLS handshakes run in the per-connection threads, so a
burst of new connections is not serialized behind the accept loop."""
import http.server
import os
import ssl
import sys
import urllib.parse

PAGE = b"<!doctype html><title>hikyaku</title>" + b"x" * 1024
BLOCK = os.urandom(64 << 10)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path != "/big":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(PAGE)))
            self.end_headers()
            self.wfile.write(PAGE)
            return
        size = int(urllib.parse.parse_qs(url.query).get("n", ["10000000"])[0])
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size))
        self.end_headers()
        try:
            while size > 0:
                chunk = BLOCK[: min(size, len(BLOCK))]
                self.wfile.write(chunk)
                size -= len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass


class TLSServer(http.server.ThreadingHTTPServer):
    def __init__(self, address, cert, key):
        super().__init__(address, Handler)
        self.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.tls.load_cert_chain(cert, key)

    def get_request(self):
        sock, addr = super().get_request()
        return self.tls.wrap_socket(sock, server_side=True, do_handshake_on_connect=False), addr


def main():
    cert, key = sys.argv[1], sys.argv[2]
    plain = http.server.ThreadingHTTPServer(("0.0.0.0", 9001), Handler)
    tls = TLSServer(("0.0.0.0", 9443), cert, key)
    for server in (plain, tls):
        server.daemon_threads = True
    import threading

    threading.Thread(target=plain.serve_forever, daemon=True).start()
    print("origin ready", flush=True)
    tls.serve_forever()


if __name__ == "__main__":
    main()
