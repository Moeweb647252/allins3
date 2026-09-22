#!/usr/bin/env bash
# CLI-level compatibility round trip against an allins3 server.
#
# Covers the plan's P3-06 CLI half and the P4-07 high-level path: the AWS CLI's
# `s3api` layer and its `s3` transfer layer, which chooses multipart on its own.
#
#   scripts/e2e_cli.sh                        # starts and stops its own server
#   ALLINS3_ENDPOINT=http://host:port scripts/e2e_cli.sh
#
# The client is a fixed, pinned version of the AWS CLI installed into the
# repository virtualenv (`pip install awscli`); see the compatibility report for
# the recorded version. Exits non-zero on the first failed check.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AWS="${AWS_CLI:-$ROOT/.venv/bin/aws}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python3}"
ENDPOINT="${ALLINS3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${ALLINS3_BUCKET:-example-a}"
BUCKET_B="${ALLINS3_BUCKET_B:-example-b}"

export AWS_ACCESS_KEY_ID="${ALLINS3_ACCESS_KEY:-EXAMPLEACCESSKEY}"
export AWS_SECRET_ACCESS_KEY="${ALLINS3_SECRET_KEY:-devsecret}"
export AWS_DEFAULT_REGION="${ALLINS3_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=""
export AWS_RETRY_MODE=standard
export AWS_MAX_ATTEMPTS=1

WORK="$(mktemp -d)"

# The CLI v1 presigns with SigV2 unless `s3.signature_version` says otherwise,
# and this server deliberately refuses SigV2 (ADR-0007). Pin the client to
# SigV4, which is also what the recorded compatibility matrix runs.
AWS_CONFIG_FILE="$WORK/aws-config"
printf '[default]\ns3 =\n    signature_version = s3v4\n' >"$AWS_CONFIG_FILE"
export AWS_CONFIG_FILE

PASS=0
FAIL=0
SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
}
trap cleanup EXIT

aws() { "$AWS" --endpoint-url "$ENDPOINT" "$@"; }

check_eq() { # name expected actual
  if [ "$2" = "$3" ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$1"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s: expected %s, got %s\n' "$1" "$2" "$3"
  fi
}

check() { # name condition-as-string
  check_eq "$1" "yes" "$2"
}

json() { # dotted.path  <- JSON on stdin
  "$PYTHON" -c '
import json, sys
value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    value = value.get(part, "") if isinstance(value, dict) else ""
print(value)
' "$1"
}

if [ -z "${ALLINS3_ENDPOINT:-}" ]; then
  ALLINS3_DEV_SECRET="${ALLINS3_DEV_SECRET:-devsecret}" \
    "$ROOT/target/debug/allins3" "$ROOT/examples/dev-config.toml" \
    >"${ALLINS3_SERVER_LOG:-/tmp/allins3-cli-server.log}" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    "$PYTHON" - "$ENDPOINT/readyz" <<'PY' >/dev/null 2>&1 && break
import sys, urllib.request
urllib.request.urlopen(sys.argv[1], timeout=1).read()
PY
    sleep 0.2
  done
fi

trap 'cleanup; rm -rf "$WORK"' EXIT

printf 'allins3 CLI round trip\n'
printf 'client: %s\n' "$("$AWS" --version 2>&1)"
printf 'endpoint: %s\n\n' "$ENDPOINT"

# --- s3api: put / head / get with a checksum ------------------------------

printf 'cli round trip body' >"$WORK/hello.txt"
SIZE=$(wc -c <"$WORK/hello.txt" | tr -d ' ')
PUT=$(aws s3api put-object --bucket "$BUCKET" --key cli/hello.txt \
  --body "$WORK/hello.txt" --checksum-algorithm CRC32)
check "put-object reports an ETag" "$([ -n "$(printf '%s' "$PUT" | json ETag)" ] && echo yes || echo no)"
check "put-object reports a checksum" "$([ -n "$(printf '%s' "$PUT" | json ChecksumCRC32)" ] && echo yes || echo no)"

EXPECTED_CRC=$("$PYTHON" -c '
import base64, sys, zlib
data = open(sys.argv[1], "rb").read()
print(base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode())
' "$WORK/hello.txt")

HEAD=$(aws s3api head-object --bucket "$BUCKET" --key cli/hello.txt)
check_eq "head-object length" "$SIZE" "$(printf '%s' "$HEAD" | json ContentLength)"
check_eq "head-object checksum matches an independent CRC32" "$EXPECTED_CRC" "$(printf '%s' "$HEAD" | json ChecksumCRC32)"

aws s3api get-object --bucket "$BUCKET" --key cli/hello.txt "$WORK/downloaded.txt" >/dev/null
check "get-object round trip" "$(cmp -s "$WORK/hello.txt" "$WORK/downloaded.txt" && echo yes || echo no)"

# --- Listing ---------------------------------------------------------------

KEYS=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix cli/ --query 'Contents[].Key' --output text)
check "list-objects-v2 contains the key" "$(printf '%s' "$KEYS" | grep -q 'cli/hello.txt' && echo yes || echo no)"

# --- Conditional write and Range ------------------------------------------

set +e
aws s3api put-object --bucket "$BUCKET" --key cli/hello.txt --body "$WORK/hello.txt" \
  --if-none-match '*' >"$WORK/conditional.txt" 2>&1
CONDITIONAL=$?
set -e
check "a failing precondition is rejected" "$([ "$CONDITIONAL" -ne 0 ] && echo yes || echo no)"
check "the failure is a precondition failure" \
  "$(grep -qE 'PreconditionFailed|412' "$WORK/conditional.txt" && echo yes || echo no)"

aws s3api get-object --bucket "$BUCKET" --key cli/hello.txt --range bytes=4-8 \
  "$WORK/range.txt" >/dev/null
check_eq "ranged get body" "round" "$(cat "$WORK/range.txt")"

# --- Copy ------------------------------------------------------------------

COPIED=$(aws s3api copy-object --bucket "$BUCKET" --key cli/copied.txt \
  --copy-source "$BUCKET/cli/hello.txt")
check "copy-object reports an ETag" "$([ -n "$(printf '%s' "$COPIED" | json CopyObjectResult.ETag)" ] && echo yes || echo no)"
check_eq "the copy has the same checksum" "$EXPECTED_CRC" \
  "$(aws s3api head-object --bucket "$BUCKET" --key cli/copied.txt | json ChecksumCRC32)"

# --- Presigned URL ---------------------------------------------------------

PRESIGNED=$(aws s3 presign "s3://$BUCKET/cli/hello.txt" --expires-in 120)
check_eq "a presigned URL serves the object" "cli round trip body" \
  "$("$PYTHON" -c 'import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode())' "$PRESIGNED")"

# --- Multipart through s3api (explicit checksum scheme) -------------------

PART_ONE="$WORK/part-one.bin"
PART_TWO="$WORK/part-two.bin"
"$PYTHON" -c 'import sys; open(sys.argv[1], "wb").write(b"a" * (5 * 1024 * 1024))' "$PART_ONE"
printf 'cli composite tail' >"$PART_TWO"

MPU=$(aws s3api create-multipart-upload --bucket "$BUCKET" --key cli/multipart.bin \
  --checksum-algorithm SHA256)
UPLOAD_ID=$(printf '%s' "$MPU" | json UploadId)
check_eq "create-multipart-upload echoes the scheme" "SHA256" "$(printf '%s' "$MPU" | json ChecksumAlgorithm)"
check_eq "create-multipart-upload defaults SHA256 to COMPOSITE" "COMPOSITE" "$(printf '%s' "$MPU" | json ChecksumType)"

ONE=$(aws s3api upload-part --bucket "$BUCKET" --key cli/multipart.bin --upload-id "$UPLOAD_ID" \
  --part-number 1 --body "$PART_ONE" --checksum-algorithm SHA256)
TWO=$(aws s3api upload-part --bucket "$BUCKET" --key cli/multipart.bin --upload-id "$UPLOAD_ID" \
  --part-number 2 --body "$PART_TWO" --checksum-algorithm SHA256)
ONE_ETAG=$(printf '%s' "$ONE" | json ETag)
TWO_ETAG=$(printf '%s' "$TWO" | json ETag)
check "upload-part returns a part checksum" "$([ -n "$(printf '%s' "$ONE" | json ChecksumSHA256)" ] && echo yes || echo no)"

PARTS=$(aws s3api list-parts --bucket "$BUCKET" --key cli/multipart.bin --upload-id "$UPLOAD_ID")
check_eq "list-parts lists both parts" "2" "$(printf '%s' "$PARTS" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin)["Parts"]))')"
check "list-parts reports part checksums" \
  "$(printf '%s' "$PARTS" | grep -q 'ChecksumSHA256' && echo yes || echo no)"

COMPOSITE=$("$PYTHON" - "$PART_ONE" "$PART_TWO" <<'PY'
import base64, hashlib, sys
digests = b"".join(hashlib.sha256(open(p, "rb").read()).digest() for p in sys.argv[1:])
print(base64.b64encode(hashlib.sha256(digests).digest()).decode() + "-2")
PY
)
COMPLETE=$(aws s3api complete-multipart-upload --bucket "$BUCKET" --key cli/multipart.bin \
  --upload-id "$UPLOAD_ID" --checksum-sha256 "$COMPOSITE" --checksum-type COMPOSITE \
  --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ONE_ETAG},{\"PartNumber\":2,\"ETag\":$TWO_ETAG}]}")
check_eq "complete returns the composite checksum" "$COMPOSITE" "$(printf '%s' "$COMPLETE" | json ChecksumSHA256)"
check_eq "the composite ETag carries the part count" "yes" \
  "$(printf '%s' "$(printf '%s' "$COMPLETE" | json ETag)" | grep -q -- '-2"' && echo yes || echo no)"

MPU_HEAD=$(aws s3api head-object --bucket "$BUCKET" --key cli/multipart.bin)
check_eq "head-object returns the composite checksum" "$COMPOSITE" "$(printf '%s' "$MPU_HEAD" | json ChecksumSHA256)"
check_eq "head-object returns the checksum type" "COMPOSITE" "$(printf '%s' "$MPU_HEAD" | json ChecksumType)"

# Abort path: an aborted upload must disappear without publishing.
ABORTED_ID=$(aws s3api create-multipart-upload --bucket "$BUCKET" --key cli/aborted.bin | json UploadId)
aws s3api upload-part --bucket "$BUCKET" --key cli/aborted.bin --upload-id "$ABORTED_ID" \
  --part-number 1 --body "$PART_TWO" >/dev/null
aws s3api abort-multipart-upload --bucket "$BUCKET" --key cli/aborted.bin --upload-id "$ABORTED_ID"
set +e
aws s3api head-object --bucket "$BUCKET" --key cli/aborted.bin >"$WORK/aborted.txt" 2>&1
ABORTED_HEAD=$?
set -e
check "an aborted upload publishes nothing" "$([ "$ABORTED_HEAD" -ne 0 ] && echo yes || echo no)"

# --- The high-level `aws s3` transfer layer -------------------------------

mkdir -p "$WORK/src" "$WORK/back"
"$PYTHON" -c '
import sys
open(sys.argv[1], "wb").write(bytes(range(256)) * (11 * 4096))
' "$WORK/src/big.bin"
printf 'small file' >"$WORK/src/small.txt"

aws s3 cp "$WORK/src/big.bin" "s3://$BUCKET/cli/big.bin" >/dev/null
BIG_HEAD=$(aws s3api head-object --bucket "$BUCKET" --key cli/big.bin)
check_eq "aws s3 cp uploads the whole object" "$(wc -c <"$WORK/src/big.bin" | tr -d ' ')" \
  "$(printf '%s' "$BIG_HEAD" | json ContentLength)"
check_eq "the transfer layer's multipart ETag carries the part count" "yes" \
  "$(printf '%s' "$(printf '%s' "$BIG_HEAD" | json ETag)" | grep -q -- '-[0-9]*"' && echo yes || echo no)"

aws s3 cp "s3://$BUCKET/cli/big.bin" "$WORK/back/big.bin" >/dev/null
check "aws s3 cp round trip" "$(cmp -s "$WORK/src/big.bin" "$WORK/back/big.bin" && echo yes || echo no)"

aws s3 sync "$WORK/src" "s3://$BUCKET/cli/synced" >/dev/null
SYNCED=$(aws s3 ls "s3://$BUCKET/cli/synced/")
check_eq "aws s3 sync uploaded both files" "2" "$(printf '%s' "$SYNCED" | grep -c . | tr -d ' ')"

aws s3 rm "s3://$BUCKET/cli/big.bin" >/dev/null
set +e
aws s3api head-object --bucket "$BUCKET" --key cli/big.bin >/dev/null 2>&1
REMOVED=$?
set -e
check "aws s3 rm removed the object" "$([ "$REMOVED" -ne 0 ] && echo yes || echo no)"

# --- delete-objects, and namespace isolation ------------------------------

DELETED=$(aws s3api delete-objects --bucket "$BUCKET" \
  --delete "{\"Objects\":[{\"Key\":\"cli/copied.txt\"},{\"Key\":\"cli/hello.txt\"}],\"Quiet\":false}")
check_eq "delete-objects removes both keys" "2" \
  "$(printf '%s' "$DELETED" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin)["Deleted"]))')"

aws s3api put-object --bucket "$BUCKET_B" --key cli/other.txt --body "$WORK/hello.txt" >/dev/null
OTHER=$(aws s3api list-objects-v2 --bucket "$BUCKET_B" --query 'Contents[].Key' --output text)
check "a second bucket is isolated" "$(printf '%s' "$OTHER" | grep -q 'cli/hello.txt' && echo no || echo yes)"

printf '\n%s/%s CLI checks passed (aws-cli %s)\n' "$PASS" "$((PASS + FAIL))" \
  "$("$AWS" --version 2>&1 | sed 's/^aws-cli\///; s/ .*//')"
[ "$FAIL" -eq 0 ]
