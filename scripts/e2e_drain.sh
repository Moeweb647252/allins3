#!/usr/bin/env bash
# Drain timing under a real process signal and a real slow client.
#
# Covers the residue the plan records for P5-03 —
# 真实慢客户端与真实进程信号下的排空时序 — and the shutdown order the spec
# prescribes (docs/implementation-spec.md:474 关闭顺序：停止接收新请求 →
# 等待在途请求至 deadline → …). Two timings must hold:
#
#   (1) an upload still in flight when SIGINT arrives keeps the server alive
#       until that operation finishes, and it still gets `200 OK`;
#   (2) an upload that outlives the drain budget makes the server record
#       `drain deadline exceeded; exiting with in-flight requests` and exit only
#       after the deadline, leaving that request unfinished.
#
# In both cases the log must read `shutdown signal received; draining` before
# `shutdown complete`.
#
# Every timing runs twice, once under `SIGINT` and once under `SIGTERM`: a
# process manager sends the latter, and handling only the former let the kernel
# kill the process with in-flight work still running.
#
#   scripts/e2e_drain.sh                          # starts and stops its own server
#   ALLINS3_DRAIN_PORT=9106 scripts/e2e_drain.sh  # on another port
#
# The client is a real signed S3 `PUT` that simply streams its body slowly: the
# script has to decide exactly when the body finishes (and whether it ever
# does), which no SDK surface lets it express. The signature is produced with
# the same botocore the other scripts use, so the server sees an ordinary
# request. Like `e2e_cli.sh`, this script owns its server, so run the two one
# after another. Exits non-zero on the first failed check.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-$ROOT/.venv/bin/python3}"
BIN="${ALLINS3_BIN:-$ROOT/target/debug/allins3}"
PORT="${ALLINS3_DRAIN_PORT:-9000}"
ENDPOINT="http://127.0.0.1:$PORT"
WORK="$(mktemp -d)"

export ALLINS3_DEV_SECRET="${ALLINS3_DEV_SECRET:-devsecret}"
export ALLINS3_SECRET_KEY="${ALLINS3_SECRET_KEY:-devsecret}"

# The pinned example config stays the single source of every other setting; it is
# regenerated only when a different port is asked for.
CONFIG="$ROOT/examples/dev-config.toml"
if [ "$PORT" != "9000" ]; then
  CONFIG="$WORK/config.toml"
  sed "s|^listen = .*|listen = \"127.0.0.1:$PORT\"|" "$ROOT/examples/dev-config.toml" >"$CONFIG"
fi

# The test drivers are compiled in only under the `testing` feature. Build the
# binary this script will run (unless the caller supplied one), so a stale
# default-feature build fails here with cargo's message rather than as a server
# that starts and immediately exits with an unregistered driver kind.
if [ -z "${ALLINS3_BIN:-}" ]; then
  cargo build --features testing --quiet || exit 2
fi

PASS=0
FAIL=0
SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}
trap 'cleanup; rm -rf "$WORK"' EXIT

check() { # name condition-as-string
  if [ "$2" = "yes" ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$1"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$1"
  fi
}

start_server() { # $1 = log file
  "$BIN" "$CONFIG" >"$1" 2>&1 &
  SERVER_PID=$!
  local ready=""
  for _ in $(seq 1 60); do
    ready=$("$PYTHON" - "$ENDPOINT/readyz" <<'PY' 2>/dev/null || true
import sys, urllib.request
try:
    with urllib.request.urlopen(sys.argv[1], timeout=2) as response:
        print(response.status)
except Exception:
    print("")
PY
)
    [ "$ready" = "200" ] && break
    sleep 0.2
  done
  [ "$ready" = "200" ]
}

slow_upload() { # size_bytes chunk_bytes pause_secs -> "STARTED" then "DONE <epoch> <outcome>"
  "$PYTHON" - "$PORT" "$1" "$2" "$3" <<'PY'
import socket, sys, time

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

host, port = "127.0.0.1", int(sys.argv[1])
size, chunk, pause = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
path = "/example-a/drain-held-%d.bin" % int(time.time() * 1000)

# An ordinary signed PUT whose body the script meters out by hand.
headers = {"content-length": str(size), "x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
request = AWSRequest(method="PUT", url="http://%s:%d%s" % (host, port, path), data=b"",
                     headers=headers)
SigV4Auth(Credentials("EXAMPLEACCESSKEY", "devsecret"), "s3", "us-east-1").add_auth(request)

sock = socket.create_connection((host, port), timeout=30)
head = "PUT %s HTTP/1.1\r\nHost: %s:%d\r\n" % (path, host, port)
head += "".join("%s: %s\r\n" % (k, v) for k, v in dict(request.headers.items()).items()) + "\r\n"
sock.sendall(head.encode())
print("STARTED", flush=True)

body = b"x" * chunk
sent = 0
while sent < size:
    piece = body[: size - sent]
    try:
        sock.sendall(piece)
    except OSError as error:
        print("DONE %d transport:%s" % (int(time.time()), type(error).__name__), flush=True)
        raise SystemExit(0)
    sent += len(piece)
    time.sleep(pause)

sock.settimeout(20)
try:
    line = sock.recv(200).split(b"\r\n", 1)[0].decode("utf-8", "replace")
    status = line.split(" ")[1] if " " in line else "?"
    print("DONE %d %s" % (int(time.time()), status), flush=True)
except (socket.timeout, OSError) as error:
    print("DONE %d transport:%s" % (int(time.time()), type(error).__name__), flush=True)
sock.close()
PY
}

line_of() { # pattern file -> first matching line number
  grep -n "$1" "$2" 2>/dev/null | head -1 | cut -d: -f1
}

drain_case() { # label signal size chunk pause secs-before-signal expect-deadline
  local label="$1" signal="$2" size="$3" chunk="$4" pause="$5" after="$6" expect_deadline="$7"
  local log="$WORK/$label-server.log" client="$WORK/$label-client.out"

  if ! start_server "$log"; then
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s: server never became ready; last log lines:\n' "$label"
    tail -n 20 "$log" | sed 's/^/    /'
    return
  fi

  slow_upload "$size" "$chunk" "$pause" >"$client" 2>&1 &
  local uploader=$!
  for _ in $(seq 1 200); do
    grep -q '^STARTED' "$client" 2>/dev/null && break
    sleep 0.05
  done
  sleep "$after"

  local t_signal t_exit elapsed line done outcome draining complete server_rc
  t_signal=$(date +%s)
  kill -"$signal" "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null
  server_rc=$?
  t_exit=$(date +%s)
  SERVER_PID=""
  wait "$uploader" 2>/dev/null

  line="$(grep '^DONE ' "$client" | tail -1)"
  done="$(printf '%s' "$line" | awk '{print $2}')"
  outcome="$(printf '%s' "$line" | awk '{print $3}')"
  elapsed=$((t_exit - t_signal))

  draining="$(line_of 'shutdown signal received; draining' "$log")"
  complete="$(line_of 'shutdown complete' "$log")"
  check "$label: exit status 0, no signal kill" \
    "$([ "${server_rc:-1}" -eq 0 ] && echo yes || echo no)"
  check "$label: log records draining before shutdown complete" \
    "$([ -n "$draining" ] && [ -n "$complete" ] && [ "$draining" -lt "$complete" ] && echo yes || echo no)"

  if [ "$expect_deadline" = "1" ]; then
    check "$label: an upload outliving the budget logs the deadline message" \
      "$(grep -q 'drain deadline exceeded' "$log" && echo yes || echo no)"
    check "$label: the server waits out the deadline before exiting (waited ${elapsed}s)" \
      "$([ "${elapsed:-0}" -ge 8 ] && echo yes || echo no)"
    check "$label: the upload that outlived shutdown did not report success (${outcome:-none})" \
      "$([ -n "$outcome" ] && [ "$outcome" != "200" ] && echo yes || echo no)"
  else
    check "$label: no deadline message when the upload finishes in time" \
      "$(grep -q 'drain deadline exceeded' "$log" && echo no || echo yes)"
    check "$label: the in-flight upload still succeeded (${outcome:-none})" \
      "$([ "$outcome" = "200" ] && echo yes || echo no)"
    # The signal lands ~0.5 s into a ~4 s body, so a server that ignored the
    # in-flight request would exit at ~0 s; waiting for it costs ~3.5 s. The
    # client's own `200` (checked above) is what proves the operation finished.
    check "$label: the server outlived the signal instead of dropping the in-flight request (waited ${elapsed}s)" \
      "$([ -n "$done" ] && [ "${elapsed:-0}" -ge 2 ] && [ "${elapsed:-0}" -lt 8 ] && echo yes || echo no)"
  fi
}

printf 'allins3 drain timing (real SIGINT/SIGTERM against a real slow client)\n'
printf 'endpoint: %s\n\n' "$ENDPOINT"

for signal in INT TERM; do
  # (1) 2 MiB of body over ~4 s; the signal lands ~0.5 s in, so the operation
  #     is in flight and finishes well inside the drain budget.
  drain_case "sig$signal-in-flight" "$signal" 2097152 131072 0.25 0.5 0

  # (2) 8 MiB over ~19 s; the budget expires while the body is still arriving,
  #     so the server must record the deadline and leave that request unfinished.
  drain_case "sig$signal-outlives" "$signal" 8388608 65536 0.15 1 1
done

printf '\n%s/%s drain checks passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
