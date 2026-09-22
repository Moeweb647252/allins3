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


def chunked_body(payload, algorithm, digest, trailer=True):
    """Frames `payload` as `aws-chunked`, optionally with a checksum trailer.

    `trailer=False` produces a body whose declared trailer never arrives: the
    declaration is in the header, the value is not in the body.
    """
    framed = bytearray()
    step = 65536
    for offset in range(0, len(payload), step):
        piece = payload[offset : offset + step]
        framed += b"%x\r\n%s\r\n" % (len(piece), piece)
    framed += b"0\r\n"
    if trailer:
        framed += b"x-amz-checksum-%s:%s\r\n" % (algorithm.encode(), digest.encode())
    framed += b"\r\n"
    return bytes(framed)


def streaming_headers(decoded_length, framed_length, trailer="x-amz-checksum-crc32"):
    """The headers a streaming client sends for an `aws-chunked` body."""
    return {
        "Content-Encoding": "aws-chunked",
        "Content-Type": "application/octet-stream",
        "x-amz-decoded-content-length": str(decoded_length),
        "x-amz-trailer": trailer,
        "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "Content-Length": str(framed_length),
    }


def s3_client():
    """A boto3 client used only to drive multipart bookkeeping."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


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
    crc32 = base64.b64encode(zlib.crc32(body).to_bytes(4, "big")).decode()
    framed = chunked_body(body, "crc32", crc32)
    headers = streaming_headers(len(body), len(framed))

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

    # A body that declares a trailer but is not `aws-chunked` at all declares a
    # value it never framed; the declaration must not be quietly ignored.
    plain_key = "streaming/trailer-unframed.bin"
    plain_path = f"/{BUCKET}/{plain_key}"
    plain = b"a body with no chunked framing at all"
    plain_headers = {
        "Content-Type": "application/octet-stream",
        "x-amz-trailer": "x-amz-checksum-crc32",
        "Content-Length": str(len(plain)),
    }
    status, response, _ = request("PUT", plain_path, plain, plain_headers)
    check(
        "a declared trailer on an unframed body is rejected",
        status >= 400 and b"BadDigest" in response,
        (status, response[:200]),
    )
    status, _, _ = request("HEAD", plain_path)
    check("an unframed trailer declaration publishes nothing", status == 404, status)

    # Chunked framing that ends after the final chunk, without the declared
    # trailer, is the same declaration with no value.
    silent = chunked_body(body, "crc32", crc32, trailer=False)
    silent_key = "streaming/trailer-silent.bin"
    silence = f"/{BUCKET}/{silent_key}"
    status, response, _ = request(
        "PUT", silence, silent, streaming_headers(len(body), len(silent)), streaming=True
    )
    check(
        "a trailer that is declared but never sent is rejected",
        status >= 400,
        (status, response[:200]),
    )
    status, _, _ = request("HEAD", silence)
    check("a silent trailer publishes nothing", status == 404, status)

    # The same rules apply to `UploadPart`, which is the other entry point that
    # accepts a trailer. The part's algorithm must match the upload's scheme.
    client = s3_client()
    part_key = "streaming/trailer-part.bin"
    upload_id = client.create_multipart_upload(
        Bucket=BUCKET, Key=part_key, ChecksumAlgorithm="CRC32", ChecksumType="FULL_OBJECT"
    )["UploadId"]
    part_query = f"partNumber=1&uploadId={urllib.parse.quote(upload_id, safe='')}"
    part_path = f"/{BUCKET}/{part_key}?{part_query}"
    part_body = os.urandom(70_000)
    part_crc32 = base64.b64encode(zlib.crc32(part_body).to_bytes(4, "big")).decode()
    framed_part = chunked_body(part_body, "crc32", part_crc32)
    status, response, part_head = request(
        "PUT",
        part_path,
        framed_part,
        streaming_headers(len(part_body), len(framed_part)),
        streaming=True,
    )
    check(
        "a chunked UploadPart with a matching trailer is accepted",
        status == 200,
        (status, response[:200]),
    )
    check(
        "the part reports the verified trailer checksum",
        part_head.get("x-amz-checksum-crc32", "").rstrip("=") == part_crc32.rstrip("="),
        part_head.get("x-amz-checksum-crc32"),
    )

    bad_part = chunked_body(part_body, "crc32", base64.b64encode(b"\x00\x00\x00\x00").decode())
    status, response, _ = request(
        "PUT",
        part_path,
        bad_part,
        streaming_headers(len(part_body), len(bad_part)),
        streaming=True,
    )
    check(
        "a chunked UploadPart with a mismatched trailer is rejected",
        status >= 400 and b"BadDigest" in response,
        (status, response[:200]),
    )

    silent_part = chunked_body(part_body, "crc32", part_crc32, trailer=False)
    status, response, _ = request(
        "PUT",
        part_path,
        silent_part,
        streaming_headers(len(part_body), len(silent_part)),
        streaming=True,
    )
    check(
        "a chunked UploadPart with a silent trailer is rejected",
        status >= 400,
        (status, response[:200]),
    )

    parts = client.list_parts(Bucket=BUCKET, Key=part_key, UploadId=upload_id).get("Parts", [])
    check(
        "only the verified part is staged",
        len(parts) == 1 and parts[0]["PartNumber"] == 1,
        parts,
    )

    def decoded_crc32(value):
        """The raw 4 bytes a base64 `ChecksumCRC32` names, padding aside."""
        return base64.b64decode(value)

    expected_crc32 = zlib.crc32(part_body).to_bytes(4, "big")
    check(
        "the staged part keeps its verified checksum",
        decoded_crc32(parts[0]["ChecksumCRC32"]) == expected_crc32,
        parts[0].get("ChecksumCRC32"),
    )

    completed = client.complete_multipart_upload(
        Bucket=BUCKET,
        Key=part_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": parts[0]["ETag"]}]},
    )
    check(
        "the trailer-written part completes with its full-object checksum",
        decoded_crc32(completed["ChecksumCRC32"]) == zlib.crc32(part_body).to_bytes(4, "big"),
        completed.get("ChecksumCRC32"),
    )

    status, _, completed_head = request("HEAD", f"/{BUCKET}/{part_key}")
    check("the completed object is readable", status == 200, status)
    check(
        "the completed object keeps the checksum",
        decoded_crc32(completed_head["x-amz-checksum-crc32"]) == zlib.crc32(part_body).to_bytes(4, "big"),
        completed_head.get("x-amz-checksum-crc32"),
    )

    print(f"\n{len(CHECKS)}/{len(CHECKS)} streaming-trailer checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"\nSTREAMING-TRAILER FAILED: {e}")
        sys.exit(1)
