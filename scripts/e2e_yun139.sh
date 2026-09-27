#!/usr/bin/env bash
# End-to-end check of the read-only 139 cloud-drive driver (ADR-0017).
#
# Starts the real server against a live drive folder and exercises the read
# path through the pinned AWS CLI: the server must become ready (which proves
# the session is accepted), list the folder, and serve a ranged GET whose byte
# count matches `HeadObject`. A PUT must be refused, proving the bucket is
# genuinely read-only.
#
# The account session is required and never printed:
#
#   export ALLINS3_YUN139_PHONE=19800000000
#   export ALLINS3_YUN139_AUTH_TOKEN='…'     # the `auth_token` cookie
#   export ALLINS3_YUN139_FOLDER_ID='Ft…'    # a non-empty drive folder
#   scripts/e2e_yun139.sh
#
# With no session the script prints a skip notice and exits 0, so it can be wired
# into a gate that only runs where credentials exist. Use `ALLINS3_BIN` to point
# at an already-built binary; otherwise it builds with the required feature.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ -z "${ALLINS3_YUN139_PHONE:-}" ] || [ -z "${ALLINS3_YUN139_AUTH_TOKEN:-}" ] || [ -z "${ALLINS3_YUN139_FOLDER_ID:-}" ]; then
  printf 'SKIP: set ALLINS3_YUN139_PHONE, ALLINS3_YUN139_AUTH_TOKEN and ALLINS3_YUN139_FOLDER_ID to run the yun139 e2e\n'
  exit 0
fi

# The credentials are written into a temporary config as TOML literal strings,
# which cannot represent a single quote; refuse rather than silently corrupt.
for value in "$ALLINS3_YUN139_PHONE" "$ALLINS3_YUN139_AUTH_TOKEN" "$ALLINS3_YUN139_FOLDER_ID"; do
  case "$value" in
    *"'"*)
      printf 'FAIL: credential values must not contain a single quote (TOML literal string)\n' >&2
      exit 2
      ;;
  esac
done

AWS="${AWS_CLI:-$ROOT/.venv/bin/aws}"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python3}"
WORK="$(mktemp -d)"
CONFIG="$WORK/yun139-config.toml"
LOG="$WORK/server.log"
SECRET="${ALLINS3_YUN139_SECRET:-devsecret}"

PORT="$("$PYTHON" - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)"
ENDPOINT="http://127.0.0.1:$PORT"
BUCKET="example-yun139"

cat >"$CONFIG" <<EOF
[server]
listen = "127.0.0.1:$PORT"
region = "us-east-1"
addressing = "path"
development = false
health_endpoints = true

[[drivers]]
id = "instance-yun139"
kind = "yun139"
options = { phone = '$ALLINS3_YUN139_PHONE', auth_token = '$ALLINS3_YUN139_AUTH_TOKEN', folder_id = '$ALLINS3_YUN139_FOLDER_ID' }

[[buckets]]
name = "$BUCKET"
driver = "instance-yun139"
namespace = "yun139"
profile = "core-read"
readonly = true

[[credentials]]
access_key_id = "EXAMPLEACCESSKEY"
secret_ref = "env:ALLINS3_YUN139_SECRET"
principal = "example-user"

[[grants]]
principal = "example-user"
bucket = "$BUCKET"
actions = ["list", "read"]
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
  ALLINS3_YUN139_SECRET="$SECRET" "$BIN" "$CONFIG" >"$LOG" 2>&1 &
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

if [ -z "${ALLINS3_BIN:-}" ]; then
  cargo build --quiet --features yun139 || exit 2
fi
BIN="${ALLINS3_BIN:-$ROOT/target/debug/allins3}"

printf 'allins3 read-only 139 cloud-drive driver end to end\n'
printf 'endpoint: %s\n\n' "$ENDPOINT"

# Readiness runs each driver's `health`, which lists the folder once; reaching
# it at all proves the session and the configured folder work.
start_server
check "the server becomes ready against the live drive" yes

# --- ordered listing -------------------------------------------------------

LISTING=$(aws s3api list-objects-v2 --bucket "$BUCKET" --max-keys 10 2>/dev/null)
KEY=$(printf '%s' "$LISTING" | "$PYTHON" -c 'import json,sys; c=json.load(sys.stdin).get("Contents",[]); print(c[0]["Key"] if c else "")')
check "the drive folder lists at least one object" "$([ -n "$KEY" ] && echo yes || echo no)"

if [ -z "$KEY" ]; then
  printf '\nthe configured folder is empty; stopping after %s/%s checks\n' "$PASS" "$((PASS + FAIL))"
  [ "$FAIL" -eq 0 ]
  exit $?
fi

# --- ranged read matches the reported size ---------------------------------

SIZE=$(aws s3api head-object --bucket "$BUCKET" --key "$KEY" 2>/dev/null \
  | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin).get("ContentLength",""))')
WANT=16
if [ -n "$SIZE" ] && [ "$SIZE" -lt 16 ]; then
  WANT="$SIZE"
fi
aws s3api get-object --bucket "$BUCKET" --key "$KEY" --range "bytes=0-15" "$WORK/part.bin" >/dev/null 2>&1
GOT=$(wc -c <"$WORK/part.bin" 2>/dev/null | tr -d ' ')
check "a ranged GET returns min(16, size) bytes" "$([ "$GOT" = "$WANT" ] && [ -n "$SIZE" ] && echo yes || echo no)"

# --- read-only enforcement -------------------------------------------------

# The grant has no `write`, so a PUT must be refused before it ever reaches the
# drive; this is what makes the read-only declaration real at the API edge.
printf 'should not be written' >"$WORK/forbidden.txt"
if aws s3api put-object --bucket "$BUCKET" --key "$(basename "$WORK")-forbidden.txt" --body "$WORK/forbidden.txt" >/dev/null 2>&1; then
  check "a PUT is refused on a read-only bucket" no
else
  check "a PUT is refused on a read-only bucket" yes
fi

printf '\n%s/%s yun139 driver checks passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
