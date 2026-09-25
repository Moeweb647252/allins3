#!/usr/bin/env bash
# The plan's P5-05 fuzz targets, run over the checked-in seeds.
#
#   scripts/fuzz.sh                      # every target, 20000 runs each
#   scripts/fuzz.sh xml-manifest         # one target
#   ALLINS3_FUZZ_RUNS=200000 scripts/fuzz.sh
#
# `fuzz/` is its own workspace (libFuzzer needs build flags the main crate must
# not carry), so `cargo fmt`/`clippy`/`test` at the root never see the targets;
# this script is how they are built and executed. It needs a nightly toolchain
# and `cargo-fuzz` (`cargo install cargo-fuzz`). A crashing input is written to
# `fuzz/artifacts/<target>/` and the script exits non-zero.
#
# The hand-written seeds in `fuzz/seeds/<target>/` are read-only inputs; fuzzer
# finds accumulate in `fuzz/corpus/<target>/`, which is git-ignored, so a long
# run does not rewrite the seeds.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/fuzz"

RUNS="${ALLINS3_FUZZ_RUNS:-20000}"
TARGETS=("$@")
if [ "${#TARGETS[@]}" -eq 0 ]; then
    TARGETS=(xml-manifest part-manifest signature-inputs)
fi

STATUS=0
for target in "${TARGETS[@]}"; do
    seeds="seeds/$target"
    if [ ! -d "$seeds" ]; then
        echo "unknown target '$target': $seeds does not exist" >&2
        STATUS=1
        continue
    fi
    corpus="corpus/$target"
    mkdir -p "$corpus"
    echo "=== $target: $RUNS runs, seeds=$seeds corpus=$corpus"
    cargo fuzz run "$target" "$corpus" -- "-seed_inputs=$seeds" "-runs=$RUNS"
    if [ "$?" -ne 0 ]; then
        echo "=== $target: FAILED (see artifacts/$target)" >&2
        STATUS=1
    fi
done

exit "$STATUS"
