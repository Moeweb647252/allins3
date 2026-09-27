#!/usr/bin/env bash
# Start a long-lived allins3 instance backed by a real driver.
#
#   scripts/dev_instance.sh start     # build if needed, generate config, launch detached
#   scripts/dev_instance.sh status    # show pid/listener/readiness
#   scripts/dev_instance.sh stop      # graceful shutdown
#   scripts/dev_instance.sh env       # print AWS_* exports for this instance
#   scripts/dev_instance.sh logs      # follow the server log
#
# Two drivers are supported, selected by ALLINS3_DRIVER:
#
#   fs      (default)  a writable bucket backed by a local directory
#   yun139             a read-only bucket backed by a 139 cloud-drive folder
#
# Everything lives outside the repository (default /tmp/allins3-instance).
# Override with INSTANCE_DIR=/some/dir, ALLINS3_PORT=9100, ALLINS3_SECRET=...
#
# The yun139 driver needs a live account session; credentials are read from a
# file and never passed on the command line:
#
#   ALLINS3_DRIVER=yun139 \
#     ALLINS3_YUN139_SESSION_FILE=/tmp/allins3-yun139/session.json \
#     ALLINS3_YUN139_FOLDER_ID='Ft…' \
#     scripts/dev_instance.sh start
#
# The session file is JSON: { "phone": "...", "auth_token": "..." }.
#
# The process is started in its own session (setsid) so it survives the shell
# that launched it; a plain `... &` is reaped when that shell's group exits.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTANCE_DIR="${INSTANCE_DIR:-/tmp/allins3-instance}"
DRIVER="${ALLINS3_DRIVER:-fs}"
# Not "PORT": that name is already set (to 0) in some shell environments.
PORT="${ALLINS3_PORT:-9000}"
# The S3 surface secret. ALLINS3_FS_SECRET is the historical name; both are
# accepted and both are exported to the child so either config reference works.
SECRET="${ALLINS3_SECRET:-${ALLINS3_FS_SECRET:-devsecret}}"
ACCESS_KEY="${ALLINS3_ACCESS_KEY:-EXAMPLEACCESSKEY}"
if [ "$DRIVER" = yun139 ]; then
  BUCKET="${ALLINS3_BUCKET:-example-yun139}"
else
  BUCKET="${ALLINS3_BUCKET:-example-fs}"
fi
BIN="${ALLINS3_BIN:-$ROOT/target/debug/allins3}"
# A caller-supplied binary is trusted as-is; otherwise we always build, because
# the cached binary may lack the feature set the selected driver needs.
BIN_PROVIDED="${ALLINS3_BIN:+1}"
PYTHON="${PYTHON:-$(command -v python3 || echo /usr/bin/python3)}"
SESSION_FILE="${ALLINS3_YUN139_SESSION_FILE:-/tmp/allins3-yun139/session.json}"
FOLDER="${ALLINS3_YUN139_FOLDER_ID:-}"

# Prefer an explicit CLI, then the repository venv (as scripts/e2e_fs.sh does),
# then whatever is on PATH.
if [ -n "${ALLINS3_AWS_CLI:-}" ]; then
  AWS_CLI="$ALLINS3_AWS_CLI"
elif [ -x "$ROOT/.venv/bin/aws" ]; then
  AWS_CLI="$ROOT/.venv/bin/aws"
else
  AWS_CLI="$(command -v aws || true)"
fi

CONFIG="$INSTANCE_DIR/$DRIVER-config.toml"
STATE_DIR="$INSTANCE_DIR/state"
DATA_DIR="$INSTANCE_DIR/data"
LOG="$INSTANCE_DIR/server.log"
PIDFILE="$INSTANCE_DIR/server.pid"
BINFILE="$INSTANCE_DIR/server.bin"
ENDFILE="$INSTANCE_DIR/endpoint"

ENDPOINT="http://127.0.0.1:$PORT"

# A pidfile in /tmp outlives the process it describes, and the kernel recycles
# pids. Record which binary was launched and require the live process to still
# be that binary, so a stale file can never make us kill an unrelated process.
alive() {
  [ -f "$PIDFILE" ] || return 1
  local pid recorded
  pid="$(cat "$PIDFILE" 2>/dev/null)"
  case "$pid" in '' | *[!0-9]*) return 1 ;; esac
  kill -0 "$pid" 2>/dev/null || return 1
  recorded="$(cat "$BINFILE" 2>/dev/null)"
  [ -n "$recorded" ] || return 1
  ps -p "$pid" -o command= 2>/dev/null | grep -qF "$recorded"
}

write_config() {
  case "$DRIVER" in
    fs)
      mkdir -p "$STATE_DIR" "$DATA_DIR"
      cat >"$CONFIG" <<EOF
[server]
listen = "127.0.0.1:$PORT"
region = "us-east-1"
addressing = "path"
development = false
health_endpoints = true
state_dir = "$STATE_DIR"

[[drivers]]
id = "instance-fs"
kind = "fs"
options = { root = "$DATA_DIR" }

[[buckets]]
name = "$BUCKET"
driver = "instance-fs"
namespace = "namespace-fs"
profile = "multipart"

[[credentials]]
access_key_id = "$ACCESS_KEY"
secret_ref = "env:ALLINS3_SECRET"
principal = "example-user"

[[grants]]
principal = "example-user"
bucket = "$BUCKET"
actions = ["list", "read", "write", "delete"]
prefix = ""

[limits]
multipart_sweep_interval_secs = 30
EOF
      ;;
    yun139)
      [ -n "$FOLDER" ] || {
        echo "yun139: ALLINS3_YUN139_FOLDER_ID is required" >&2
        return 1
      }
      local phone token
      phone="$(session_field phone)" || return 1
      token="$(session_field auth_token)" || return 1
      # TOML literal strings cannot represent a single quote; refuse rather
      # than silently write a config that will not parse.
      reject_quote "$phone" phone || return 1
      reject_quote "$token" auth_token || return 1
      reject_quote "$FOLDER" folder_id || return 1
      mkdir -p "$INSTANCE_DIR"
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
options = { phone = '$phone', auth_token = '$token', folder_id = '$FOLDER' }

[[buckets]]
name = "$BUCKET"
driver = "instance-yun139"
namespace = "yun139"
profile = "core-read"
readonly = true

[[credentials]]
access_key_id = "$ACCESS_KEY"
secret_ref = "env:ALLINS3_SECRET"
principal = "example-user"

[[grants]]
principal = "example-user"
bucket = "$BUCKET"
actions = ["list", "read"]
prefix = ""

[limits]
multipart_sweep_interval_secs = 0
EOF
      ;;
    *)
      echo "unknown ALLINS3_DRIVER: $DRIVER (expected fs or yun139)" >&2
      return 2
      ;;
  esac
}

# Reads one field from the JSON session file without ever echoing the whole
# file; the value goes to stdout for the caller to place in the config.
session_field() {
  if [ ! -f "$SESSION_FILE" ]; then
    echo "yun139: session file not found: $SESSION_FILE" >&2
    return 1
  fi
  "$PYTHON" - "$SESSION_FILE" "$1" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
value = data.get(sys.argv[2])
if not value:
    print(f"yun139: session file has no `{sys.argv[2]}`", file=sys.stderr)
    raise SystemExit(1)
print(value)
PY
}

reject_quote() {
  case "$1" in
    *"'"*)
      echo "yun139: $2 must not contain a single quote (TOML literal string)" >&2
      return 1
      ;;
  esac
}

ready() {
  "$PYTHON" - "$ENDPOINT/readyz" <<'PY' >/dev/null 2>&1
import sys, urllib.request
urllib.request.urlopen(sys.argv[1], timeout=1).read()
PY
}

cmd_start() {
  if alive; then
    echo "already running (pid $(cat "$PIDFILE")) on $ENDPOINT"
    return 0
  fi
  if [ -z "$BIN_PROVIDED" ]; then
    echo "building $BIN"
    if [ "$DRIVER" = yun139 ]; then
      (cd "$ROOT" && cargo build --quiet --features yun139) || return 1
    else
      (cd "$ROOT" && cargo build --quiet) || return 1
    fi
  fi
  write_config
  rm -f "$LOG"

  # Detach into a new session so the launcher's process group can die freely.
  "$PYTHON" - "$BIN" "$CONFIG" "$LOG" "$PIDFILE" "$BINFILE" "$SECRET" <<'PY'
import os, subprocess, sys
binary, config, log, pidfile, binfile, secret = sys.argv[1:7]
env = dict(os.environ)
env["ALLINS3_SECRET"] = secret
env["ALLINS3_FS_SECRET"] = secret
with open(log, "ab") as out:
    proc = subprocess.Popen(
        [binary, config],
        stdin=subprocess.DEVNULL,
        stdout=out,
        stderr=out,
        env=env,
        cwd=os.path.dirname(config),
        start_new_session=True,
    )
with open(pidfile, "w") as fh:
    fh.write(str(proc.pid))
with open(binfile, "w") as fh:
    fh.write(binary)
PY

  for _ in $(seq 1 50); do
    if ready; then
      echo "$ENDPOINT" >"$ENDFILE"
      echo "ready: $ENDPOINT (pid $(cat "$PIDFILE"))"
      cmd_env
      return 0
    fi
    if ! alive; then
      echo "server exited during startup; last log lines:" >&2
      tail -n 20 "$LOG" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "server never became ready; last log lines:" >&2
  tail -n 20 "$LOG" >&2
  return 1
}

cmd_stop() {
  if ! alive; then
    echo "not running"
    rm -f "$PIDFILE"
    return 0
  fi
  local pid
  pid="$(cat "$PIDFILE")"
  kill "$pid" 2>/dev/null
  for _ in $(seq 1 50); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  echo "stopped pid $pid"
  rm -f "$PIDFILE"
}

cmd_status() {
  if alive; then
    echo "pid:      $(cat "$PIDFILE")"
  else
    echo "pid:      not running"
    return 1
  fi
  echo "driver:   $DRIVER"
  echo "endpoint: $ENDPOINT"
  echo "log:      $LOG"
  if [ "$DRIVER" = fs ]; then
    echo "data:     $DATA_DIR"
    echo "state:    $STATE_DIR"
  else
    echo "folder:   $FOLDER"
  fi
  if ready; then echo "readyz:   200"; else echo "readyz:   unreachable"; fi
}

cmd_env() {
  cat <<EOF

--- connection info -------------------------------------------------
endpoint:   $ENDPOINT
driver:     $DRIVER
bucket:     $BUCKET
access key: $ACCESS_KEY
secret key: (env:ALLINS3_SECRET, value "$SECRET")

export AWS_ACCESS_KEY_ID=$ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SECRET
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=
export AWS_MAX_ATTEMPTS=1
export AWS_CONFIG_FILE=$INSTANCE_DIR/aws-config

# ${AWS_CLI:-aws} --endpoint-url $ENDPOINT s3 ls s3://$BUCKET/

(config file: $CONFIG)
(aws cli:   ${AWS_CLI:-not found})
EOF
}

cmd_aws_config() {
  mkdir -p "$INSTANCE_DIR"
  printf '[default]\ns3 =\n    signature_version = s3v4\n' >"$INSTANCE_DIR/aws-config"
}

cmd_logs() {
  tail -n "${LINES:-40}" -f "$LOG"
}

case "${1:-status}" in
  start)
    cmd_aws_config
    cmd_start
    ;;
  stop) cmd_stop ;;
  restart)
    cmd_stop
    cmd_aws_config
    cmd_start
    ;;
  status) cmd_status ;;
  env)
    cmd_aws_config
    cmd_env
    ;;
  logs) cmd_logs ;;
  config) write_config && echo "wrote $CONFIG" ;;
  *)
    echo "usage: $0 {start|stop|restart|status|env|logs|config}" >&2
    exit 2
    ;;
esac
