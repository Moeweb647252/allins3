#!/usr/bin/env python3
"""The four request bounds of spec §9.3, exercised through raw sockets.

`docs/implementation-spec.md:568` requires the header timeout, the body idle
timeout, the backend operation deadline and the total deadline to be *separate*,
"不能用一个很短的总超时阻断所有大对象". Three of them are observable from
outside, so this script drives each with a real client against the running
binary and asserts the observable outcome:

  A. header timeout  — a client that sends half a header block is disconnected
     without ever getting a normal response; ordinary traffic still works.
  B. body idle       — a client that stops sending mid-PUT is abandoned with a
     client error (never a 5xx) and leaves no object; ordinary traffic works.
  C. total deadline  — a request whose body keeps arriving but exceeds the total
     deadline gets `400 RequestTimeout`; ordinary traffic works.

C is deliberately run with a body idle timeout far larger than the trickle gap,
so the two bounds are shown to be independent: the same stalled byte stream is
cut off by B (idle) and by C (total) under different configuration.

The backend operation deadline is the total deadline by construction: a driver
call cannot outlive the request that started it, so the request context carries
the same bound (`S3Handler::scope`); C proves that bound aborts a live request.

The client signs with botocore, like `e2e_drain.sh`, because it has to decide
when (and whether) a body finishes — which no SDK surface expresses.

    scripts/e2e_timeouts.py                       # starts and stops its own servers
    ALLINS3_TIMEOUT_PORT=9107 scripts/e2e_timeouts.py

Like `e2e_cli.sh` and `e2e_drain.sh`, this script owns its server, so run those
one after another. Exits non-zero on the first failed check.
"""

import hashlib
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("ALLINS3_BIN", os.path.join(ROOT, "target", "debug", "allins3"))
PORT = int(os.environ.get("ALLINS3_TIMEOUT_PORT", "9000"))
ENDPOINT = "http://127.0.0.1:%d" % PORT
ACCESS_KEY = os.environ.get("ALLINS3_ACCESS_KEY", "EXAMPLEACCESSKEY")
SECRET_KEY = os.environ.get("ALLINS3_SECRET_KEY", "devsecret")
BUCKET = os.environ.get("ALLINS3_BUCKET", "example-a")
# A well-formed DeleteObjects document, short of the declared content-length.
BODY_XML = b"<Delete><Object><Key>timeouts/nothing.txt</Key></Object></Delete>"

PASS = 0
FAIL = 0


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("[PASS] %s%s" % (name, " -- %s" % detail if detail else ""))
    else:
        FAIL += 1
        print("[FAIL] %s%s" % (name, " -- %s" % detail if detail else ""))


def config_text(header, body_idle, total):
    with open(os.path.join(ROOT, "examples", "dev-config.toml")) as handle:
        text = handle.read()
    text = re.sub(r'^listen = .*$', 'listen = "127.0.0.1:%d"' % PORT, text, flags=re.M)
    text = re.sub(r"^header_timeout_secs = .*$", "header_timeout_secs = %d" % header, text, flags=re.M)
    text = re.sub(r"^body_idle_timeout_secs = .*$", "body_idle_timeout_secs = %d" % body_idle, text, flags=re.M)
    text = re.sub(r"^request_timeout_secs = .*$", "request_timeout_secs = %d" % total, text, flags=re.M)
    return text


class Server:
    """A server started on the timeout config above."""

    def __init__(self, label, header, body_idle, total):
        self.label = label
        work = tempfile.mkdtemp(prefix="allins3-timeouts-")
        self.path = os.path.join(work, "config.toml")
        with open(self.path, "w") as handle:
            handle.write(config_text(header, body_idle, total))
        self.log_path = os.path.join(work, "server.log")
        self.log = open(self.log_path, "w")
        env = dict(os.environ)
        env["ALLINS3_DEV_SECRET"] = SECRET_KEY
        self.process = subprocess.Popen(  # noqa: S603
            [BIN, self.path], cwd=ROOT, env=env, stdout=self.log, stderr=self.log
        )
        self.ready = self._wait_ready()

    def _wait_ready(self):
        for _ in range(60):
            if self.process.poll() is not None:
                return False
            try:
                with urllib.request.urlopen(ENDPOINT + "/readyz", timeout=2) as response:
                    if response.status == 200:
                        return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.log.close()

    def log_text(self):
        with open(self.log_path) as handle:
            return handle.read()


def signed_headers(method, path, content_length, payload_hash):
    request = AWSRequest(
        method=method,
        url=ENDPOINT + path,
        data=b"",
        headers={"content-length": str(content_length), "x-amz-content-sha256": payload_hash},
    )
    SigV4Auth(Credentials(ACCESS_KEY, SECRET_KEY), "s3", "us-east-1").add_auth(request)
    return dict(request.headers.items())


def send_head(sock, method, path, headers):
    head = "%s %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n" % (method, path, PORT)
    head += "".join("%s: %s\r\n" % item for item in headers.items())
    head += "\r\n"
    sock.sendall(head.encode())


def read_status(sock, timeout=20):
    """Reads the response status line, or returns a transport failure marker."""
    sock.settimeout(timeout)
    try:
        line = sock.recv(400).split(b"\r\n", 1)[0].decode("utf-8", "replace")
    except (socket.timeout, OSError) as error:
        return "transport:%s" % type(error).__name__
    parts = line.split(" ")
    if len(parts) < 2:
        return "closed"
    return parts[1]


def request_object(method, key, data=b"", extra=None):
    """An ordinary signed request through urllib, returning the status."""
    path = "/%s/%s" % (BUCKET, key)
    import hashlib

    digest = hashlib.sha256(data).hexdigest()
    request = AWSRequest(method=method, url=ENDPOINT + path, data=data,
                         headers={"x-amz-content-sha256": digest})
    for name, value in (extra or {}).items():
        request.headers[name] = value
    SigV4Auth(Credentials(ACCESS_KEY, SECRET_KEY), "s3", "us-east-1").add_auth(request)
    import urllib.error

    try:
        with urllib.request.urlopen(
            urllib.request.Request(
                ENDPOINT + path,
                data=data if method in ("PUT", "POST") else None,
                headers=dict(request.headers.items()),
                method=method,
            ),
            timeout=20,
        ) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def normal_traffic_works(label):
    key = "timeouts/normal-%d.txt" % int(time.time() * 1000)
    status = request_object("PUT", key, b"hello")
    check("%s: ordinary PUT still succeeds" % label, status == 200, "status=%s" % status)
    status = request_object("GET", key)
    check("%s: the object reads back" % label, status == 200, "status=%s" % status)
    status = request_object("GET", key + ".missing")
    check("%s: a missing key is still a 404, not a timeout" % label, status == 404, "status=%s" % status)


def header_case():
    server = Server("header", header=2, body_idle=0, total=0)
    if not server.ready:
        check("header: server became ready", False, "see %s" % server.log_path)
        server.stop()
        return
    check("header: server became ready", True)

    # Half a request: no terminating blank line, ever.
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    started = time.time()
    sock.sendall(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n")
    result = read_status(sock, timeout=8)
    elapsed = time.time() - started
    sock.close()
    check(
        "header: a stalled header block is cut off without a normal response",
        1.5 <= elapsed <= 6 and result in ("closed", "408", "transport:ConnectionResetError"),
        "%s after %.1fs" % (result, elapsed),
    )
    # A transfer that outlives the header timeout must still complete. The old
    # implementation wrapped the *whole connection* in the header timeout, which
    # cut off every transfer longer than the header bound (spec §9.3).
    key = "timeouts/slow-%d.bin" % int(time.time() * 1000)
    size = 256 * 1024
    headers = signed_headers("PUT", "/%s/%s" % (BUCKET, key), size, "UNSIGNED-PAYLOAD")
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=30)
    started = time.time()
    send_head(sock, "PUT", "/%s/%s" % (BUCKET, key), headers)
    for _ in range(8):  # ~4 s of body, twice the 2 s header bound
        sock.sendall(b"x" * 32768)
        time.sleep(0.5)
    status = read_status(sock, timeout=20)
    elapsed = time.time() - started
    sock.close()
    check(
        "header: a transfer outliving the header timeout still completes",
        status == "200",
        "%s after %.1fs (bound 2s)" % (status, elapsed),
    )
    normal_traffic_works("header")
    server.stop()


def body_idle_case():
    server = Server("body-idle", header=30, body_idle=2, total=0)
    if not server.ready:
        check("body-idle: server became ready", False, "see %s" % server.log_path)
        server.stop()
        return
    check("body-idle: server became ready", True)

    key = "timeouts/stalled-%d.bin" % int(time.time() * 1000)
    size = 1024 * 1024
    headers = signed_headers("PUT", "/%s/%s" % (BUCKET, key), size, "UNSIGNED-PAYLOAD")
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    started = time.time()
    send_head(sock, "PUT", "/%s/%s" % (BUCKET, key), headers)
    sock.sendall(b"x" * 65536)  # then stop sending, well short of content-length
    status = read_status(sock, timeout=10)
    elapsed = time.time() - started
    sock.close()
    check(
        "body-idle: a stalled body is abandoned with a client error",
        status.isdigit() and 400 <= int(status) < 500,
        "%s after %.1fs" % (status, elapsed),
    )
    check(
        "body-idle: the stall is served after the idle bound, not instantly",
        1.5 <= elapsed <= 8,
        "%.1fs" % elapsed,
    )
    check(
        "body-idle: the abandoned upload published nothing",
        request_object("HEAD", key) == 404,
    )

    # A control body (XML) is read by the protocol library itself, which would
    # report the body error as an internal error; the client must still see a
    # client error.
    target = "/%s?delete" % BUCKET
    size = 4096
    headers = signed_headers("POST", target, size, hashlib.sha256(BODY_XML).hexdigest())
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    send_head(sock, "POST", target, headers)
    sock.sendall(BODY_XML)  # then stall, short of content-length
    status = read_status(sock, timeout=10)
    sock.close()
    check(
        "body-idle: a stalled control body is a client error, not a 5xx",
        status.isdigit() and 400 <= int(status) < 500,
        status,
    )

    normal_traffic_works("body-idle")
    server.stop()


def total_deadline_case():
    server = Server("total", header=30, body_idle=30, total=3)
    if not server.ready:
        check("total: server became ready", False, "see %s" % server.log_path)
        server.stop()
        return
    check("total: server became ready", True)

    key = "timeouts/outliving-%d.bin" % int(time.time() * 1000)
    size = 8 * 1024 * 1024
    headers = signed_headers("PUT", "/%s/%s" % (BUCKET, key), size, "UNSIGNED-PAYLOAD")
    sock = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    started = time.time()
    send_head(sock, "PUT", "/%s/%s" % (BUCKET, key), headers)
    status = "transport:None"
    body = b""
    try:
        sent = 0
        while sent < size and time.time() - started < 6:
            sock.sendall(b"x" * 65536)  # trickles: never idle for the 30 s idle bound
            sent += 65536
            time.sleep(0.5)
            sock.settimeout(0.2)
            try:
                chunk = sock.recv(4000)
                if chunk:
                    body = chunk
                    status = chunk.split(b"\r\n", 1)[0].split(b" ")[1].decode()
                    break
            except socket.timeout:
                pass
    except OSError as error:
        status = "transport:%s" % type(error).__name__
    elapsed = time.time() - started
    sock.close()
    check(
        "total: a request outliving the total deadline gets 400 RequestTimeout",
        status == "400" and b"<Code>RequestTimeout</Code>" in body,
        "%s after %.1fs" % (status, elapsed),
    )
    check(
        "total: the deadline fires near the configured bound",
        2 <= elapsed <= 8,
        "%.1fs" % elapsed,
    )
    check(
        "total: the request cut off by the total deadline published nothing",
        request_object("HEAD", key) == 404,
    )
    normal_traffic_works("total")
    server.stop()


def main():
    print("allins3 request bounds (spec §9.3) against the running binary")
    print("endpoint: %s\n" % ENDPOINT)
    header_case()
    body_idle_case()
    total_deadline_case()
    print("\n%d/%d timeout checks passed" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
