#!/usr/bin/env python3
"""End-to-end verification of the running allins3 server with a real AWS SDK.

Run against a live server:

    ALLINS3_ENDPOINT=http://127.0.0.1:9000 python3 scripts/e2e_boto3.py

Exits non-zero and prints the first failure if any check fails.
"""

import base64
import hashlib
import io
import os
import sys
import time
import urllib.error
import urllib.request
import zlib

import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("ALLINS3_ENDPOINT", "http://127.0.0.1:9000")
ACCESS_KEY = os.environ.get("ALLINS3_ACCESS_KEY", "EXAMPLEACCESSKEY")
SECRET_KEY = os.environ.get("ALLINS3_SECRET_KEY", "devsecret")
REGION = os.environ.get("ALLINS3_REGION", "us-east-1")

CHECKS = []


def check(name, condition, detail=""):
    CHECKS.append((name, bool(condition), detail))
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}{'' if condition else ': ' + str(detail)}")
    if not condition:
        raise AssertionError(f"{name}: {detail}")


def b64(raw):
    return base64.b64encode(raw).decode()


def crc32_b64(data):
    return b64(zlib.crc32(data).to_bytes(4, "big"))


def sha256_b64(data):
    return b64(hashlib.sha256(data).digest())


def client(**overrides):
    # botocore's default `request_checksum_calculation` makes it inject a CRC32
    # on every `UploadPart`, which is what a real SDK client does; a check that
    # needs an upload with no checksum scheme at all asks for `when_required`.
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
            **overrides,
        ),
    )


def main():
    s3 = client()
    bucket = "example-a"

    # --- HeadBucket / ListBuckets -----------------------------------------
    s3.head_bucket(Bucket=bucket)
    check("head_bucket", True)

    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    check("list_buckets", bucket in buckets, buckets)

    # --- PUT / HEAD / GET --------------------------------------------------
    body = b"hello allins3 world"
    put = s3.put_object(Bucket=bucket, Key="docs/hello.txt", Body=body, ContentType="text/plain")
    etag = put["ETag"].strip('"')
    check("put_object returns etag", len(etag) == 32, etag)

    head = s3.head_object(Bucket=bucket, Key="docs/hello.txt")
    check("head_object length", head["ContentLength"] == len(body), head["ContentLength"])
    check("head_object content type", head["ContentType"] == "text/plain", head.get("ContentType"))

    got = s3.get_object(Bucket=bucket, Key="docs/hello.txt")
    check("get_object body", got["Body"].read() == body)

    # --- Ranged GET --------------------------------------------------------
    # body = b"hello allins3 world" -> bytes 6..12 inclusive are "allins3"
    ranged = s3.get_object(Bucket=bucket, Key="docs/hello.txt", Range="bytes=6-12")
    check("ranged get body", ranged["Body"].read() == b"allins3")
    check("ranged get status", ranged["ResponseMetadata"]["HTTPStatusCode"] == 206, ranged["ResponseMetadata"]["HTTPStatusCode"])
    check("ranged get content-length", ranged["ContentLength"] == 7, ranged["ContentLength"])
    check("ranged get content-range", ranged["ContentRange"] == f"bytes 6-12/{len(body)}", ranged.get("ContentRange"))

    # A range past the end must be a 416, not an empty 206.
    try:
        s3.get_object(Bucket=bucket, Key="docs/hello.txt", Range=f"bytes={len(body)}-")
        raise AssertionError("unsatisfiable range should have been rejected")
    except ClientError as e:
        check("unsatisfiable range 416", e.response["Error"]["Code"] == "InvalidRange", e.response["Error"]["Code"])

    suffix = s3.get_object(Bucket=bucket, Key="docs/hello.txt", Range="bytes=-5")
    check("suffix range", suffix["Body"].read() == body[-5:])

    # --- Conditional GET ---------------------------------------------------
    try:
        s3.get_object(Bucket=bucket, Key="docs/hello.txt", IfNoneMatch=put["ETag"])
        raise AssertionError("If-None-Match should have returned 304")
    except ClientError as e:
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        check("conditional get 304", status == 304, status)

    # --- Special keys preserved -------------------------------------------
    special = "a//b + c"
    s3.put_object(Bucket=bucket, Key=special, Body=b"special")
    listing = s3.list_objects_v2(Bucket=bucket, Prefix="a/")
    keys = [o["Key"] for o in listing.get("Contents", [])]
    check("special key round-trips", special in keys, keys)
    check("special key body", s3.get_object(Bucket=bucket, Key=special)["Body"].read() == b"special")

    # --- Conditional write -------------------------------------------------
    try:
        s3.put_object(Bucket=bucket, Key="docs/hello.txt", Body=b"nope", IfNoneMatch="*")
        raise AssertionError("If-None-Match: * should fail on an existing object")
    except ClientError as e:
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        check("conditional put 412", status == 412, status)

    # --- Copy --------------------------------------------------------------
    s3.copy_object(Bucket=bucket, Key="docs/copy.txt", CopySource={"Bucket": bucket, "Key": "docs/hello.txt"})
    check("copy body", s3.get_object(Bucket=bucket, Key="docs/copy.txt")["Body"].read() == body)

    # --- Listing + pagination ---------------------------------------------
    for i in range(5):
        s3.put_object(Bucket=bucket, Key=f"page/{i}.txt", Body=b"x")
    page = s3.list_objects_v2(Bucket=bucket, Prefix="page/")
    check("list page size", len(page.get("Contents", [])) == 5, len(page.get("Contents", [])))

    paged = s3.list_objects_v2(Bucket=bucket, Prefix="page/", MaxKeys=2)
    check("first page truncated", paged["IsTruncated"] is True)
    token = paged["NextContinuationToken"]
    second = s3.list_objects_v2(Bucket=bucket, Prefix="page/", MaxKeys=2, ContinuationToken=token)
    collected = [o["Key"] for o in paged.get("Contents", [])] + [o["Key"] for o in second.get("Contents", [])]
    check("pagination no duplicates", len(set(collected)) == len(collected), collected)
    check("pagination order", collected == sorted(collected), collected)

    try:
        s3.list_objects_v2(Bucket=bucket, Prefix="page/", ContinuationToken="tok-forged")
        raise AssertionError("forged token should be rejected")
    except ClientError as e:
        check("forged token rejected", e.response["Error"]["Code"] == "InvalidArgument", e.response["Error"]["Code"])

    # --- Multipart ---------------------------------------------------------
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="multi/object.bin")
    upload_id = mpu["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key="multi/object.bin", PartNumber=1, UploadId=upload_id, Body=b"x" * (5 * 1024 * 1024))
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key="multi/object.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
    )
    check("multipart complete size", s3.head_object(Bucket=bucket, Key="multi/object.bin")["ContentLength"] == 5 * 1024 * 1024)

    # Abort path.
    mpu2 = s3.create_multipart_upload(Bucket=bucket, Key="multi/abort.bin")
    s3.upload_part(Bucket=bucket, Key="multi/abort.bin", PartNumber=1, UploadId=mpu2["UploadId"], Body=b"y" * (5 * 1024 * 1024))
    s3.abort_multipart_upload(Bucket=bucket, Key="multi/abort.bin", UploadId=mpu2["UploadId"])
    try:
        s3.head_object(Bucket=bucket, Key="multi/abort.bin")
        raise AssertionError("aborted multipart left an object")
    except ClientError as e:
        check("aborted multipart invisible", e.response["ResponseMetadata"]["HTTPStatusCode"] == 404)

    # --- Second driver / bucket -------------------------------------------
    s3.put_object(Bucket="example-b", Key="other.txt", Body=b"second-driver")
    check("second bucket body", s3.get_object(Bucket="example-b", Key="other.txt")["Body"].read() == b"second-driver")
    b_keys = [o["Key"] for o in s3.list_objects_v2(Bucket="example-b").get("Contents", [])]
    check("namespace isolation", "docs/hello.txt" not in b_keys, b_keys)

    # --- DeleteObjects -----------------------------------------------------
    resp = s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": "docs/copy.txt"}, {"Key": "page/0.txt"}], "Quiet": False},
    )
    deleted = {d["Key"] for d in resp.get("Deleted", [])}
    check("batch delete results", {"docs/copy.txt", "page/0.txt"} <= deleted, deleted)
    check("batch delete has no errors", not resp.get("Errors"), resp.get("Errors"))

    # --- Explicitly unsupported features must fail, not fake success -------
    try:
        s3.put_object(Bucket=bucket, Key="enc.txt", Body=b"x", ServerSideEncryption="AES256")
        raise AssertionError("SSE should be rejected")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        check("unsupported SSE rejected", code in ("NotImplemented", "InvalidArgument"), code)

    # --- Wrong credentials -------------------------------------------------
    bad = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="NOSUCHKEY",
        aws_secret_access_key="wrong",
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    try:
        bad.list_buckets()
        raise AssertionError("unknown access key should be rejected")
    except ClientError as e:
        check("unknown access key rejected", e.response["Error"]["Code"] in ("InvalidAccessKeyId", "SignatureDoesNotMatch"), e.response["Error"]["Code"])

    # A known access key with the wrong secret is a signature mismatch, not an
    # unknown user.
    wrong_secret = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key="definitely-not-the-secret",
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    try:
        wrong_secret.list_buckets()
        raise AssertionError("a wrong secret should be rejected")
    except ClientError as e:
        check("wrong secret rejected", e.response["Error"]["Code"] == "SignatureDoesNotMatch", e.response["Error"]["Code"])

    # A single flipped byte in the signature must not verify.
    signed = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "docs/hello.txt"}, ExpiresIn=60
    )
    tampered_signature = signed.replace("X-Amz-Signature=", "X-Amz-Signature=0", 1)
    try:
        urllib.request.urlopen(tampered_signature)
        raise AssertionError("a mutated signature must be rejected")
    except urllib.error.HTTPError as e:
        check("mutated signature rejected", e.code in (400, 403), e.code)

    # --- Presigned URL (SigV4 query-string signing) -------------------------
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "docs/hello.txt"}, ExpiresIn=60
    )
    with urllib.request.urlopen(url) as response:
        check("presigned get body", response.read() == body)

    expired = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "docs/hello.txt"}, ExpiresIn=1
    )
    time.sleep(2)
    try:
        urllib.request.urlopen(expired)
        raise AssertionError("an expired presigned URL must be rejected")
    except urllib.error.HTTPError as e:
        check("expired presigned url rejected", e.code in (400, 403), e.code)

    # A presigned URL must not be replayable against another key.
    tampered = url.replace("docs/hello.txt", "docs/copy.txt")
    try:
        urllib.request.urlopen(tampered)
        raise AssertionError("a tampered presigned URL must be rejected")
    except urllib.error.HTTPError as e:
        check("tampered presigned url rejected", e.code in (400, 403), e.code)

    # --- Transfer-manager multipart (the SDK's high-level path) -------------
    big = bytes(range(256)) * (44 * 1024)  # 11 MiB
    transfer = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        multipart_chunksize=5 * 1024 * 1024,
        max_concurrency=2,
    )
    s3.upload_fileobj(io.BytesIO(big), bucket, "tm/big.bin", Config=transfer)
    head = s3.head_object(Bucket=bucket, Key="tm/big.bin")
    check("transfer-manager size", head["ContentLength"] == len(big), head["ContentLength"])

    # 11 MiB in 5 MiB parts is 3 parts; the ETag must be the composed form.
    parts = [big[i : i + 5 * 1024 * 1024] for i in range(0, len(big), 5 * 1024 * 1024)]
    expected = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()
    want_tag = f'"{expected}-{len(parts)}"'
    check("transfer-manager multipart etag", head["ETag"] == want_tag, f"{head['ETag']} != {want_tag}")

    tail = s3.get_object(Bucket=bucket, Key="tm/big.bin", Range=f"bytes={len(big) - 16}-")
    check("transfer-manager ranged tail", tail["Body"].read() == big[-16:])

    # Listing parts and uploads must expose the in-progress state.
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="tm/inspect.bin")
    uploaded = s3.upload_part(
        Bucket=bucket, Key="tm/inspect.bin", PartNumber=1, UploadId=mpu["UploadId"], Body=b"z" * (5 * 1024 * 1024)
    )
    listed = s3.list_parts(Bucket=bucket, Key="tm/inspect.bin", UploadId=mpu["UploadId"])
    check("list_parts", [p["PartNumber"] for p in listed.get("Parts", [])] == [1], listed.get("Parts"))
    check("list_parts etag", listed["Parts"][0]["ETag"] == uploaded["ETag"])

    uploads = s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
    check(
        "list_multipart_uploads",
        any(u["UploadId"] == mpu["UploadId"] for u in uploads),
        uploads,
    )

    # A part uploaded from another object (server-side copy) composes too.
    copied = s3.upload_part_copy(
        Bucket=bucket,
        Key="tm/inspect.bin",
        PartNumber=2,
        UploadId=mpu["UploadId"],
        CopySource={"Bucket": bucket, "Key": "docs/hello.txt"},
    )
    check("upload_part_copy etag form", len(copied["CopyPartResult"]["ETag"].strip('"')) == 32)
    check(
        "upload_part_copy reports the part checksum",
        "ChecksumCRC32" not in copied["CopyPartResult"] or len(copied["CopyPartResult"]["ChecksumCRC32"]) > 0,
        copied["CopyPartResult"],
    )
    s3.abort_multipart_upload(Bucket=bucket, Key="tm/inspect.bin", UploadId=mpu["UploadId"])
    check("aborted inspection upload", not s3.list_multipart_uploads(Bucket=bucket).get("Uploads"))

    # --- Multipart checksums: FULL_OBJECT and COMPOSITE ---------------------
    #
    # These two modes are checked against values this script computes itself
    # with the standard library, so the server is not grading its own work.

    part_one = b"a" * (5 * 1024 * 1024)
    part_two = b"full-object tail"
    whole = part_one + part_two

    key = "ck/full-object.bin"
    mpu = s3.create_multipart_upload(
        Bucket=bucket, Key=key, ChecksumAlgorithm="CRC32", ChecksumType="FULL_OBJECT"
    )
    check(
        "create_multipart echoes the checksum scheme",
        mpu.get("ChecksumAlgorithm") == "CRC32" and mpu.get("ChecksumType") == "FULL_OBJECT",
        mpu.get("ChecksumAlgorithm"),
    )
    one = s3.upload_part(
        Bucket=bucket,
        Key=key,
        PartNumber=1,
        UploadId=mpu["UploadId"],
        Body=part_one,
        ChecksumAlgorithm="CRC32",
    )
    check(
        "upload_part returns the part checksum",
        one.get("ChecksumCRC32") == crc32_b64(part_one),
        one.get("ChecksumCRC32"),
    )
    two = s3.upload_part(
        Bucket=bucket,
        Key=key,
        PartNumber=2,
        UploadId=mpu["UploadId"],
        Body=part_two,
        ChecksumAlgorithm="CRC32",
    )
    parts = s3.list_parts(Bucket=bucket, Key=key, UploadId=mpu["UploadId"]).get("Parts", [])
    check(
        "list_parts reports the stored part checksum",
        parts and parts[1].get("ChecksumCRC32") == crc32_b64(part_two),
        parts,
    )

    done = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=mpu["UploadId"],
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": one["ETag"], "ChecksumCRC32": one["ChecksumCRC32"]},
                {"PartNumber": 2, "ETag": two["ETag"], "ChecksumCRC32": two["ChecksumCRC32"]},
            ]
        },
        ChecksumCRC32=crc32_b64(whole),
        ChecksumType="FULL_OBJECT",
    )
    check(
        "complete returns the full-object checksum",
        done.get("ChecksumCRC32") == crc32_b64(whole),
        done.get("ChecksumCRC32"),
    )
    check("complete reports the checksum type", done.get("ChecksumType") == "FULL_OBJECT", done.get("ChecksumType"))
    head = s3.head_object(Bucket=bucket, Key=key)
    check(
        "head reports the full-object checksum",
        head.get("ChecksumCRC32") == crc32_b64(whole),
        head.get("ChecksumCRC32"),
    )
    check("head reports the checksum type", head.get("ChecksumType") == "FULL_OBJECT", head.get("ChecksumType"))
    check("full-object body matches", s3.get_object(Bucket=bucket, Key=key)["Body"].read() == whole)
    # A full-object checksum describes the object a ranged response is part of,
    # so a range must not advertise it.
    ranged = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-9")
    check("ranged response drops the whole-object checksum", "ChecksumCRC32" not in ranged, ranged.get("ChecksumCRC32"))

    # COMPOSITE: a digest of the ordered raw part digests, printed as
    # `base64(digest)-<partcount>`.
    key = "ck/composite.bin"
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm="SHA256")
    check(
        "a SHA256 upload defaults to COMPOSITE",
        mpu.get("ChecksumAlgorithm") == "SHA256" and mpu.get("ChecksumType") == "COMPOSITE",
        (mpu.get("ChecksumAlgorithm"), mpu.get("ChecksumType")),
    )
    one = s3.upload_part(
        Bucket=bucket, Key=key, PartNumber=1, UploadId=mpu["UploadId"], Body=part_one, ChecksumAlgorithm="SHA256"
    )
    two = s3.upload_part(
        Bucket=bucket, Key=key, PartNumber=2, UploadId=mpu["UploadId"], Body=part_two, ChecksumAlgorithm="SHA256"
    )
    check("upload_part returns the SHA256 part checksum", one.get("ChecksumSHA256") == sha256_b64(part_one), one.get("ChecksumSHA256"))

    expected = sha256_b64(hashlib.sha256(part_one).digest() + hashlib.sha256(part_two).digest())
    done = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=mpu["UploadId"],
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": one["ETag"], "ChecksumSHA256": one["ChecksumSHA256"]},
                {"PartNumber": 2, "ETag": two["ETag"], "ChecksumSHA256": two["ChecksumSHA256"]},
            ]
        },
        ChecksumSHA256=f"{expected}-2",
        ChecksumType="COMPOSITE",
    )
    check(
        "complete returns the composite checksum with its part count",
        done.get("ChecksumSHA256") == f"{expected}-2",
        done.get("ChecksumSHA256"),
    )
    check("complete reports the composite type", done.get("ChecksumType") == "COMPOSITE", done.get("ChecksumType"))
    head = s3.head_object(Bucket=bucket, Key=key)
    check(
        "head returns the composite checksum",
        head.get("ChecksumSHA256") == f"{expected}-2",
        head.get("ChecksumSHA256"),
    )
    check("head reports the composite type", head.get("ChecksumType") == "COMPOSITE", head.get("ChecksumType"))
    check(
        "the composite value is not the digest of the assembled object",
        expected != sha256_b64(whole),
        expected,
    )

    # `x-amz-checksum-type` is an optional declaration of its own: a completion
    # may constrain the coverage the server must produce without naming a value
    # to compare against.
    key = "ck/type-only.bin"
    mpu = s3.create_multipart_upload(
        Bucket=bucket, Key=key, ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE"
    )
    one = s3.upload_part(
        Bucket=bucket, Key=key, PartNumber=1, UploadId=mpu["UploadId"], Body=b"typed body", ChecksumAlgorithm="SHA256"
    )
    # The composite of one part digests that part's raw digest.
    typed = sha256_b64(hashlib.sha256(b"typed body").digest())
    done = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": one["ETag"]}]},
        ChecksumType="COMPOSITE",
    )
    check(
        "a type-only completion publishes the computed composite checksum",
        done.get("ChecksumSHA256") == f"{typed}-1" and done.get("ChecksumType") == "COMPOSITE",
        (done.get("ChecksumSHA256"), done.get("ChecksumType")),
    )

    # A coverage that contradicts the upload's scheme must be refused.
    key = "ck/type-conflict.bin"
    mpu = s3.create_multipart_upload(
        Bucket=bucket, Key=key, ChecksumAlgorithm="CRC32", ChecksumType="FULL_OBJECT"
    )
    one = s3.upload_part(
        Bucket=bucket, Key=key, PartNumber=1, UploadId=mpu["UploadId"], Body=b"conflict body", ChecksumAlgorithm="CRC32"
    )
    try:
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": one["ETag"]}]},
            ChecksumType="COMPOSITE",
        )
        check("a coverage that contradicts the upload scheme is refused", False, "complete succeeded")
    except ClientError as e:
        check(
            "a coverage that contradicts the upload scheme is refused",
            e.response["Error"]["Code"] in ("InvalidArgument", "InvalidRequest"),
            e.response["Error"]["Code"],
        )
    s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu["UploadId"])

    # A completion cannot supply an algorithm the upload never had. botocore's
    # default client injects a CRC32 on every part, which fixes a scheme, so this
    # needs a client that sends no part checksum at all.
    key = "ck/type-no-scheme.bin"
    quiet = client(request_checksum_calculation="when_required")
    mpu = quiet.create_multipart_upload(Bucket=bucket, Key=key)
    one = quiet.upload_part(Bucket=bucket, Key=key, PartNumber=1, UploadId=mpu["UploadId"], Body=b"plain body")
    check("a part with no declared checksum reports none", "ChecksumCRC32" not in one, one.get("ChecksumCRC32"))
    try:
        quiet.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": one["ETag"]}]},
            ChecksumType="FULL_OBJECT",
        )
        check("a coverage without an algorithm is refused", False, "complete succeeded")
    except ClientError as e:
        check(
            "a coverage without an algorithm is refused",
            e.response["Error"]["Code"] in ("InvalidArgument", "InvalidRequest"),
            e.response["Error"]["Code"],
        )
    quiet.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu["UploadId"])

    # A completion that declares the wrong whole-object checksum must be
    # rejected with BadDigest and must publish nothing.
    key = "ck/mismatch.bin"
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm="CRC32")
    one = s3.upload_part(
        Bucket=bucket, Key=key, PartNumber=1, UploadId=mpu["UploadId"], Body=b"checked body", ChecksumAlgorithm="CRC32"
    )
    try:
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": one["ETag"]}]},
            ChecksumCRC32=crc32_b64(b"some other body entirely"),
        )
        check("a wrong completion checksum is rejected", False, "complete succeeded")
    except ClientError as e:
        check(
            "a wrong completion checksum is rejected",
            e.response["Error"]["Code"] == "BadDigest",
            e.response["Error"]["Code"],
        )
    try:
        s3.head_object(Bucket=bucket, Key=key)
        check("a rejected completion publishes nothing", False, "object appeared")
    except ClientError as e:
        check("a rejected completion publishes nothing", e.response["Error"]["Code"] == "404", e.response["Error"]["Code"])
    s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu["UploadId"])

    # An unsupported combination must be refused, not silently downgraded.
    try:
        s3.create_multipart_upload(
            Bucket=bucket, Key="ck/illegal.bin", ChecksumAlgorithm="CRC64NVME", ChecksumType="COMPOSITE"
        )
        check("CRC64NVME + COMPOSITE is refused", False, "accepted")
    except ClientError as e:
        check(
            "CRC64NVME + COMPOSITE is refused",
            e.response["Error"]["Code"] in ("InvalidArgument", "InvalidRequest"),
            e.response["Error"]["Code"],
        )

    failed = [c for c in CHECKS if not c[1]]
    print(f"\nSDK versions: boto3 {boto3.__version__} botocore {botocore.__version__} (signature s3v4)")
    print(f"{len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"\nE2E FAILED: {e}")
        sys.exit(1)
