#!/usr/bin/env bash
# The repository's test and lint gate, in one entry point.
#
# Both feature sets are checked on purpose: the default build has no test
# drivers, no contract harness and no protocol harness, so an unused item, an
# unreachable arm or a compile error that only exists there is invisible to an
# `--all-features` run (and vice versa). Note the default `cargo test` runs the
# library unit tests only: every `tests/` target requires the `testing`
# feature, so its integration coverage comes from the `--all-features` run.
#
# Not included: `cargo deny` (licences, duplicate dependencies), which ADR-0005
# records as an outstanding item. CI runs exactly this script.
#
#   scripts/check.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== fmt"
cargo fmt --check

echo "=== clippy (default features)"
cargo clippy --all-targets -- -D warnings

echo "=== clippy (all features)"
cargo clippy --all-targets --all-features -- -D warnings

echo "=== test (default features)"
cargo test

echo "=== test (all features)"
cargo test --all-features

echo "=== spike: protocol adapter (separate workspace)"
(cd spikes/protocol-adapter && cargo test)

echo "all checks passed"
