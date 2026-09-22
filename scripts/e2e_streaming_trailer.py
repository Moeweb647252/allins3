#!/usr/bin/env python3
"""The `aws-chunked` + checksum-trailer upload path, against a running server.

Neither the AWS CLI nor botocore 1.42 will produce this request form for
`PutObject`/`UploadPart` against an HTTP endpoint: their S3 model places request
checksums in a header (`x-amz-checksum-<algorithm>`) and refuses non-seekable
bodies outright. P3-05's trailer verification therefore needs the request built
by hand, which is what this script does: it frames the body the way an SDK does
for `STREAMING-UNSIGNED-PAYLOAD-TRAILER`, signs it with botocore's SigV4 signer,
and sends it.

    ALLINS3_ENDPOINT=http://127.0.0.1:9000 python3 scripts/e2e_streaming_trailer.py

Exits non-zero if the server does not accept the trailer form, or if it accepts
a body whose trailer disagrees with the bytes.
"""

import base64
import hashlib
import http.client
import os
import sys
import urllib.parse
import zlib

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ENDPOINT = os.environ.get("ALLINS3_ENDPOINT", "http://127.0.0.1:9000")
ACCESS_KEY = os.environ.get("ALLINS3_ACCESS_KEY", "EXAMPLEACCESSKEY")
SECRET_KEY = os.environ.get("ALLINS3_SECRET_KEY", "devsecret")
REGION = os.environ.get("ALLINS3_REGION", "us-east-1")
BUCKET = os.environ.get("ALLINS3_BUCKET", "example-a")

CHECKS = []


def check(name, condition, detail=""):
    CHECKS.append((name, bool(condition)))
    print(f"[{'PASS' if condition else 'FAIL'}] {name}{'' if condition else ': ' + str(detail)}")
    if not condition:
        raise AssertionError(f"{name}: {detail}")


def chunked_body(payload, algorithm, digest):
    """Frames `payload` as `aws-chunked` with a checksum trailer."""
    framed = bytearray()
    step = 65536
    for offset in range(0, len(payload), step):
        piece = payload[offset : offset + step]
        framed += b"%x\r\n%s\r\n" % (len(piece), piece)
    framed += b"0\r\n"
    framed += b"x-amz-checksum-%s:%s\r\n" % (algorithm.encode(), digest.encode())
    framed += b"\r\n"
    return bytes(framed)


def request(method, path, body=None, headers=None, streaming=False):
    """Signs `method path` with SigV4 and sends it.

    Returns `(status, body, headers)`; the headers matter because a `HEAD`
    response carries everything in them.
    """
    headers = dict(headers or {})
    if not streaming:
        # S3 requires the payload hash header on every signed request; botocore
        # only adds it when it signs the payload itself.
        headers.setdefault(
            "x-amz-content-sha256", hashlib.sha256(body or b"").hexdigest()
        )
    signed_request = AWSRequest(
        method=method, url=ENDPOINT + path, data=body, headers=headers
    )
    if streaming:
        # The canonical request names the payload literal rather than the digest
        # of the encoded bytes, which is what a streaming client signs.
        signed_request.context["hashed_payload"] = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    SigV4Auth(Credentials(ACCESS_KEY, SECRET_KEY), "s3", REGION).add_auth(signed_request)

    url = urllib.parse.urlparse(ENDPOINT)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
    connection.request(method, path, body=body, headers=dict(signed_request.headers.items()))
    response = connection.getresponse()
    data = response.read()
    response_headers = {k.lower(): v for k, v in response.getheaders()}
    connection.close()
    return response.status, data, response_headers


def main():
    body = os.urandom(150_000)
    key = "streaming/trailer.bin"
    path = f"/{BUCKET}/{key}"
    framed = chunked_body(body, "crc32", base64.b64encode(zlib.crc32(body).to_bytes(4, "big")).decode())
    headers = {
        "Content-Encoding": "aws-chunked",
        "Content-Type": "application/octet-stream",
        "x-amz-decoded-content-length": str(len(body)),
        "x-amz-trailer": "x-amz-checksum-crc32",
        "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "Content-Length": str(len(framed)),
    }

    status, response, _ = request("PUT", path, framed, headers, streaming=True)
    check("a trailer-signed aws-chunked PUT is accepted", status == 200, (status, response[:200]))

    status, data, _ = request("GET", path)
    check("the decoded body round-trips", status == 200 and data == body, (status, len(data)))

    status, _, head = request("HEAD", path)
    check("HEAD succeeds on the trailer-written object", status == 200, status)
    trailer_crc32 = (
        base64.b64encode(zlib.crc32(body).to_bytes(4, "big")).decode().rstrip("=")
    )
    check(
        "the verified trailer checksum is stored",
        head.get("x-amz-checksum-crc32", "").rstrip("=") == trailer_crc32,
        head.get("x-amz-checksum-crc32"),
    )
    check(
        "the stored checksum type is full-object",
        head.get("x-amz-checksum-type") == "FULL_OBJECT",
        head.get("x-amz-checksum-type"),
    )

    # A trailer whose checksum disagrees with the bytes must be refused, and
    # must publish nothing.
    bad_key = "streaming/trailer-mismatch.bin"
    bad_path = f"/{BUCKET}/{bad_key}"
    bad = chunked_body(body, "crc32", base64.b64encode(b"\x00\x00\x00\x00").decode())
    bad_headers = {**headers, "Content-Length": str(len(bad))}
    status, response, _ = request("PUT", bad_path, bad, bad_headers, streaming=True)
    check(
        "a mismatched trailer checksum is rejected",
        status >= 400 and b"BadDigest" in response,
        (status, response[:200]),
    )
    status, _, bad_head = request("HEAD", bad_path)
    check("a rejected trailer publishes nothing", status == 404, status)
    check(
        "a rejected trailer stored no checksum",
        "x-amz-checksum-crc32" not in bad_head,
        bad_head.get("x-amz-checksum-crc32"),
    )

    print(f"\n{len(CHECKS)}/{len(CHECKS)} streaming-trailer checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"\nSTREAMING-TRAILER FAILED: {e}")
        sys.exit(1)
