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
import hmac
import http.client
import os
import re
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


def sign(key, message):
    """HMAC-SHA256, the primitive SigV4 is built from."""
    return hmac.new(key, message, hashlib.sha256).digest()


def signing_key(secret, date, region):
    """The SigV4 signing key for one date/region/`s3` scope."""
    key = f"AWS4{secret}".encode()
    for part in (date, region, "s3", "aws4_request"):
        key = sign(key, part.encode())
    return key


def scope(date, region):
    return f"{date}/{region}/s3/aws4_request"


def chunk_signature(key, amz_date, region, previous, data):
    """`AWS4-HMAC-SHA256-PAYLOAD` over one chunk, chained to the previous one."""
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256-PAYLOAD",
            amz_date,
            scope(amz_date[:8], region),
            previous,
            hashlib.sha256(b"").hexdigest(),
            hashlib.sha256(data).hexdigest(),
        ]
    )
    return hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()


def trailer_signature(key, amz_date, region, previous, canonical_trailers):
    """`AWS4-HMAC-SHA256-TRAILER` over the canonicalized trailer block."""
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256-TRAILER",
            amz_date,
            scope(amz_date[:8], region),
            previous,
            hashlib.sha256(canonical_trailers).hexdigest(),
        ]
    )
    return hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()


def signed_chunked_body(payload, seed, amz_date, region, secret, trailers=None, flip_chunk=None):
    """Frames `payload` as `aws-chunked` with a signature on every chunk.

    `trailers` adds a signed trailer block (`x-amz-checksum-*` plus
    `x-amz-trailer-signature`). `flip_chunk` damages the signature of that
    chunk index, which is how the INV-08 chunk-damage case is built.
    """
    key = signing_key(secret, amz_date[:8], region)
    previous = seed
    framed = bytearray()
    step = 65536
    index = 0
    for offset in range(0, len(payload), step):
        piece = payload[offset : offset + step]
        signature = chunk_signature(key, amz_date, region, previous, piece)
        if flip_chunk == index:
            signature = ("0" if signature[0] != "0" else "1") + signature[1:]
        framed += b"%x;chunk-signature=%s\r\n" % (len(piece), signature.encode())
        framed += piece + b"\r\n"
        previous = signature
        index += 1

    final = chunk_signature(key, amz_date, region, previous, b"")
    framed += b"0;chunk-signature=%s\r\n" % final.encode()
    if trailers:
        canonical = b"".join(f"{name}:{value}\n".encode() for name, value in sorted(trailers))
        framed += canonical.replace(b"\n", b"\r\n")
        framed += b"x-amz-trailer-signature:%s\r\n" % trailer_signature(
            key, amz_date, region, final, canonical
        ).encode()
    framed += b"\r\n"
    return bytes(framed)


def signed_streaming_request(method, path, headers, framed, payload_mode, secret):
    """Signs one `STREAMING-*` request and returns `(headers, seed_signature)`.

    The seed signature is the request signature itself, which is what every
    chunk signature chains from.
    """
    headers = dict(headers)
    headers["x-amz-content-sha256"] = payload_mode
    signed_request = AWSRequest(method=method, url=ENDPOINT + path, data=framed, headers=headers)
    SigV4Auth(Credentials(ACCESS_KEY, secret), "s3", REGION).add_auth(signed_request)
    authorization = dict(signed_request.headers.items())["Authorization"]
    seed = re.search(r"Signature=([0-9a-f]{64})", authorization).group(1)
    return dict(signed_request.headers.items()), seed


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


def signed_streaming_headers(method, path, payload_mode, extra=None, secret=None):
    """Signs a `STREAMING-*` request whose body is not yet framed.

    The canonical request names the payload literal rather than a digest, so the
    signature does not depend on the framed bytes; that is what lets a client
    chain chunk signatures from it.
    """
    base = {
        "Content-Encoding": "aws-chunked",
        "Content-Type": "application/octet-stream",
        "x-amz-content-sha256": payload_mode,
    }
    base.update(extra or {})
    return signed_streaming_request(
        method, path, base, b"", payload_mode, secret or SECRET_KEY
    )


def signed_chunk_checks():
    """The signed `aws-chunked` forms: chunk signatures and trailer signatures.

    INV-08 requires that a damaged chunk signature or a damaged trailer never
    commit, so each damage case is paired with the intact request that must
    succeed — otherwise a rejection could just be a broken request.
    """
    payload = os.urandom(150_000)

    decoded = {"x-amz-decoded-content-length": str(len(payload))}

    # 1. Every chunk signed, no trailer. Every `x-amz-*` header must be covered
    #    by the signature, so `x-amz-decoded-content-length` is signed too.
    key = "streaming/signed-chunks.bin"
    path = f"/{BUCKET}/{key}"
    headers, seed = signed_streaming_headers(
        "PUT", path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decoded
    )
    amz_date = headers["X-Amz-Date"]
    framed = signed_chunked_body(payload, seed, amz_date, REGION, SECRET_KEY)
    status, response, _ = raw_request("PUT", path, framed, headers)
    check(
        "a PUT with a signature on every chunk is accepted",
        status == 200,
        (status, response[:200]),
    )
    status, data, _ = request("GET", path)
    check(
        "the signed-chunk body round-trips",
        status == 200 and data == payload,
        (status, len(data)),
    )

    # 2. The same request with one chunk signature damaged.
    bad_key = "streaming/signed-chunk-damaged.bin"
    bad_path = f"/{BUCKET}/{bad_key}"
    headers, seed = signed_streaming_headers(
        "PUT", bad_path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decoded
    )
    amz_date = headers["X-Amz-Date"]
    framed = signed_chunked_body(
        payload, seed, amz_date, REGION, SECRET_KEY, flip_chunk=1
    )
    status, response, _ = raw_request("PUT", bad_path, framed, headers)
    check(
        "a damaged chunk signature is rejected with the signature error",
        status == 403 and b"SignatureDoesNotMatch" in response,
        (status, response[:200]),
    )
    check(
        "the rejection does not leak the library's internal detail",
        b"AwsChunkedStreamError" not in response and b"InternalError" not in response,
        response[:200],
    )
    status, _, _ = request("HEAD", bad_path)
    check("a damaged chunk signature publishes nothing", status == 404, status)

    # 2b. Framing the server cannot parse at all.
    framing_key = "streaming/signed-framing.bin"
    framing_path = f"/{BUCKET}/{framing_key}"
    headers, seed = signed_streaming_headers(
        "PUT", framing_path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", decoded
    )
    signature = seed.encode()
    malformed = (
        b"zz;chunk-signature="
        + signature
        + b"\r\n"
        + payload
        + b"\r\n0;chunk-signature="
        + signature
        + b"\r\n\r\n"
    )
    status, response, _ = raw_request("PUT", framing_path, malformed, headers)
    check(
        "malformed chunked framing is a client error, not a server fault",
        status == 400 and b"InvalidArgument" in response,
        (status, response[:200]),
    )
    status, _, _ = request("HEAD", framing_path)
    check("malformed framing publishes nothing", status == 404, status)

    # 3. Signed chunks with a signed trailer block.
    trailer_key = "streaming/signed-trailer.bin"
    trailer_path = f"/{BUCKET}/{trailer_key}"
    crc32 = base64.b64encode(zlib.crc32(payload).to_bytes(4, "big")).decode()
    extra = {**decoded, "x-amz-trailer": "x-amz-checksum-crc32"}
    headers, seed = signed_streaming_headers(
        "PUT", trailer_path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", extra
    )
    amz_date = headers["X-Amz-Date"]
    framed = signed_chunked_body(
        payload,
        seed,
        amz_date,
        REGION,
        SECRET_KEY,
        trailers=[("x-amz-checksum-crc32", crc32)],
    )
    status, response, _ = raw_request("PUT", trailer_path, framed, headers)
    check(
        "a signed trailer block is accepted",
        status == 200,
        (status, response[:200]),
    )
    status, _, trailer_head = request("HEAD", trailer_path)
    check(
        "the signed trailer's checksum is stored",
        trailer_head.get("x-amz-checksum-crc32") is not None,
        trailer_head.get("x-amz-checksum-crc32"),
    )

    # 4. A signed trailer whose *checksum value* disagrees with the bytes: the
    #    trailer signature is valid, so only server-side verification can catch
    #    it.
    lying_key = "streaming/signed-trailer-lying.bin"
    lying_path = f"/{BUCKET}/{lying_key}"
    headers, seed = signed_streaming_headers(
        "PUT", lying_path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", extra
    )
    amz_date = headers["X-Amz-Date"]
    wrong = base64.b64encode(b"\x00\x00\x00\x00").decode()
    framed = signed_chunked_body(
        payload,
        seed,
        amz_date,
        REGION,
        SECRET_KEY,
        trailers=[("x-amz-checksum-crc32", wrong)],
    )
    status, response, _ = raw_request("PUT", lying_path, framed, headers)
    check(
        "a signed but wrong trailer checksum is rejected",
        status >= 400 and b"BadDigest" in response,
        (status, response[:200]),
    )
    status, _, _ = request("HEAD", lying_path)
    check("a lying signed trailer publishes nothing", status == 404, status)

    # 5. The trailer block signature itself damaged.
    tampered_key = "streaming/signed-trailer-damaged.bin"
    tampered_path = f"/{BUCKET}/{tampered_key}"
    headers, seed = signed_streaming_headers(
        "PUT", tampered_path, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", extra
    )
    amz_date = headers["X-Amz-Date"]
    framed = signed_chunked_body(
        payload,
        seed,
        amz_date,
        REGION,
        SECRET_KEY,
        trailers=[("x-amz-checksum-crc32", crc32)],
    )
    marker = b"x-amz-trailer-signature:"
    position = framed.index(marker) + len(marker)
    # Same idiom as `flip_chunk`: the replacement must differ from the byte it
    # replaces, or the signature is not actually damaged. Writing a bare `"0"`
    # was a no-op whenever the signature already began with `0` (~1/16 of runs),
    # which accepted the request and left the key behind, failing this case and
    # every later run that reuses the key on the same server.
    damage = b"0" if framed[position : position + 1] != b"0" else b"1"
    framed = framed[:position] + damage + framed[position + 1 :]
    status, response, _ = raw_request("PUT", tampered_path, framed, headers)
    check(
        "a damaged trailer signature is rejected with the signature error",
        status == 403 and b"SignatureDoesNotMatch" in response,
        (status, response[:200]),
    )
    status, _, _ = request("HEAD", tampered_path)
    check("a damaged trailer signature publishes nothing", status == 404, status)


def raw_request(method, path, body=None, headers=None):
    """Sends headers exactly as given: their signature is already computed."""
    url = urllib.parse.urlparse(ENDPOINT)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
    connection.request(method, path, body=body, headers=dict(headers or {}))
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

    # The signed variant of the same request form.
    signed_chunk_checks()

    print(f"\n{len(CHECKS)}/{len(CHECKS)} streaming-trailer checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"\nSTREAMING-TRAILER FAILED: {e}")
        sys.exit(1)
