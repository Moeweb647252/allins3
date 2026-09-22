#!/usr/bin/env python3
"""End-to-end verification of the running allins3 server with a real AWS SDK.

Run against a live server:

    ALLINS3_ENDPOINT=http://127.0.0.1:9000 python3 scripts/e2e_boto3.py

Exits non-zero and prints the first failure if any check fails.
"""

import hashlib
import io
import os
import sys
import time
import urllib.error
import urllib.request

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


def client():
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
    s3.abort_multipart_upload(Bucket=bucket, Key="tm/inspect.bin", UploadId=mpu["UploadId"])
    check("aborted inspection upload", not s3.list_multipart_uploads(Bucket=bucket).get("Uploads"))

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
