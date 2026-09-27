#!/usr/bin/env bash
# End-to-end check of the local filesystem driver (ADR-0015).
#
# Starts the real server on an fs-backed, production-mode config, exercises the
# S3 API through the pinned AWS CLI, then SIGKILLs the process and restarts it
# over the same directory. The point of the last step is that no in-process
# state helps: the bytes can only come from disk.
#
#   scripts/e2e_fs.sh
#   ALLINS3_BIN=path scripts/e2e_fs.sh   # use an already-built binary
#
# The fs driver is registered in every build, so this needs no `testing`
# feature — unlike the scripts that drive the in-memory test drivers.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AWS="${AWS_CLI:-$ROOT/.venv/bin/aws}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python3}"
WORK="$(mktemp -d)"
ROOT_DIR="$WORK/data"
CONFIG="$WORK/fs-config.toml"
LOG="$WORK/server.log"
SECRET="${ALLINS3_FS_SECRET:-devsecret}"

PORT="$("$PYTHON" - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)"
ENDPOINT="http://127.0.0.1:$PORT"
BUCKET="example-fs"

cat >"$CONFIG" <<EOF
[server]
listen = "127.0.0.1:$PORT"
region = "us-east-1"
addressing = "path"
development = false
health_endpoints = true
state_dir = "$WORK/state"

[[drivers]]
id = "instance-fs"
kind = "fs"
options = { root = "$ROOT_DIR" }

[[buckets]]
name = "$BUCKET"
driver = "instance-fs"
namespace = "namespace-fs"
profile = "multipart"

[[credentials]]
access_key_id = "EXAMPLEACCESSKEY"
secret_ref = "env:ALLINS3_FS_SECRET"
principal = "example-user"

[[grants]]
principal = "example-user"
bucket = "$BUCKET"
actions = ["list", "read", "write", "delete"]
prefix = ""

[limits]
multipart_sweep_interval_secs = 0
EOF

export AWS_ACCESS_KEY_ID="EXAMPLEACCESSKEY"
export AWS_SECRET_ACCESS_KEY="$SECRET"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=""
export AWS_MAX_ATTEMPTS=1
# CLI v1 presigns with SigV2 unless told otherwise, and the server refuses it.
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
trap 'cleanup; rm -rf "$WORK"' EXIT

check() { # name condition
  if [ "$2" = yes ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$1"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$1"
  fi
}

ready() {
  "$PYTHON" - "$ENDPOINT/readyz" <<'PY' >/dev/null 2>&1
import sys, urllib.request
urllib.request.urlopen(sys.argv[1], timeout=1).read()
PY
}

start_server() {
  ALLINS3_FS_SECRET="$SECRET" "$BIN" "$CONFIG" >"$LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    if ready; then
      return 0
    fi
    sleep 0.2
  done
  echo "server never became ready; last log lines:" >&2
  tail -n 20 "$LOG" >&2
  exit 2
}

aws() { "$AWS" --endpoint-url "$ENDPOINT" "$@"; }

# The fs driver needs no feature; a stale binary from another tree would still
# produce a confusing failure, so build unless the caller supplied one.
if [ -z "${ALLINS3_BIN:-}" ]; then
  cargo build --quiet || exit 2
fi
BIN="${ALLINS3_BIN:-$ROOT/target/debug/allins3}"

printf 'allins3 filesystem driver end to end\n'
printf 'endpoint: %s\n\n' "$ENDPOINT"

start_server

# --- plain object ----------------------------------------------------------

printf 'fs e2e body' >"$WORK/body.txt"
aws s3api put-object --bucket "$BUCKET" --key fs/hello.txt --body "$WORK/body.txt" >/dev/null 2>&1
aws s3api get-object --bucket "$BUCKET" --key fs/hello.txt "$WORK/out.txt" >/dev/null 2>&1
check "put/get round trip" "$(cmp -s "$WORK/body.txt" "$WORK/out.txt" && echo yes || echo no)"

# --- multipart through the coordinator over fs staged writes ---------------

MPU=$(aws s3api create-multipart-upload --bucket "$BUCKET" --key fs/mpu.bin 2>/dev/null)
UPLOAD_ID=$(printf '%s' "$MPU" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["UploadId"])')
"$PYTHON" -c 'import sys; open(sys.argv[1],"wb").write(b"a" * (5 * 1024 * 1024))' "$WORK/p1"
printf 'tail' >"$WORK/p2"
E1=$(aws s3api upload-part --bucket "$BUCKET" --key fs/mpu.bin --upload-id "$UPLOAD_ID" \
  --part-number 1 --body "$WORK/p1" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ETag"])')
E2=$(aws s3api upload-part --bucket "$BUCKET" --key fs/mpu.bin --upload-id "$UPLOAD_ID" \
  --part-number 2 --body "$WORK/p2" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ETag"])')
aws s3api complete-multipart-upload --bucket "$BUCKET" --key fs/mpu.bin --upload-id "$UPLOAD_ID" \
  --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$E1},{\"PartNumber\":2,\"ETag\":$E2}]}" >/dev/null 2>&1
EXPECTED=$((5 * 1024 * 1024 + 4))
LEN=$(aws s3api head-object --bucket "$BUCKET" --key fs/mpu.bin 2>/dev/null \
  | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin).get("ContentLength",""))')
check "multipart completes through the coordinator" "$([ "$LEN" = "$EXPECTED" ] && echo yes || echo no)"

# --- an upload left in progress across the crash ---------------------------
#
# The durable-coordinator claim (`ADR-0014`): the process is killed after the
# client was told a part was staged, and that part must still be there — under
# the same upload id — with the upload completable by a fresh process.

IP_MPU=$(aws s3api create-multipart-upload --bucket "$BUCKET" --key fs/inprogress.bin 2>/dev/null)
IP_ID=$(printf '%s' "$IP_MPU" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["UploadId"])')
printf 'acknowledged before the crash' >"$WORK/ip1"
IP_ETAG=$(aws s3api upload-part --bucket "$BUCKET" --key fs/inprogress.bin --upload-id "$IP_ID" \
  --part-number 1 --body "$WORK/ip1" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ETag"])')
IP_LEN=$(wc -c <"$WORK/ip1" | tr -d ' ')

# --- hard restart ----------------------------------------------------------

kill -9 "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null
SERVER_PID=""
start_server

aws s3api get-object --bucket "$BUCKET" --key fs/hello.txt "$WORK/out2.txt" >/dev/null 2>&1
check "an object survives a SIGKILL restart" "$(cmp -s "$WORK/body.txt" "$WORK/out2.txt" && echo yes || echo no)"
LEN2=$(aws s3api head-object --bucket "$BUCKET" --key fs/mpu.bin 2>/dev/null \
  | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin).get("ContentLength",""))')
check "a completed multipart object survives a restart" "$([ "$LEN2" = "$EXPECTED" ] && echo yes || echo no)"

# The acknowledged part is still listed, under the same upload id and ETag.
IP_LIST=$(aws s3api list-parts --bucket "$BUCKET" --key fs/inprogress.bin --upload-id "$IP_ID" 2>/dev/null)
IP_COUNT=$(printf '%s' "$IP_LIST" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin).get("Parts",[])))')
IP_SEEN=$(printf '%s' "$IP_LIST" | "$PYTHON" -c 'import json,sys; parts=json.load(sys.stdin).get("Parts",[]); print(parts[0]["ETag"] if parts else "")')
check "an acknowledged part survives a SIGKILL restart" "$([ "$IP_COUNT" = 1 ] && [ "$IP_SEEN" = "$IP_ETAG" ] && echo yes || echo no)"

# ...and the restored upload completes and publishes the same bytes.
aws s3api complete-multipart-upload --bucket "$BUCKET" --key fs/inprogress.bin --upload-id "$IP_ID" \
  --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$IP_ETAG}]}" >/dev/null 2>&1
IP_DONE=$(aws s3api head-object --bucket "$BUCKET" --key fs/inprogress.bin 2>/dev/null \
  | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin).get("ContentLength",""))')
check "a restored upload completes after a restart" "$([ "$IP_DONE" = "$IP_LEN" ] && echo yes || echo no)"

printf '\n%s/%s filesystem driver checks passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
