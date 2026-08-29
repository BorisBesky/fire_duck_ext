#!/usr/bin/env bash
#
# Build and run the DuckDB-free unit tests, with line coverage over the
# modules they exercise.
#
# These modules deliberately have no DuckDB dependency (see
# src/include/firestore_wire.hpp), so this needs nothing but a C++17 compiler
# and the vendored nlohmann/json -- no DuckDB build, no network, no emulator.
#
#   scripts/run_unit_tests.sh                 # build, run, report, enforce
#   scripts/run_unit_tests.sh --filter cursor # run a subset (no enforcement)
#   COVERAGE_MIN=90 scripts/run_unit_tests.sh # different threshold
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT/build/unit}"
COVERAGE_MIN="${COVERAGE_MIN:-95}"
CXX="${CXX:-g++}"
FILTER=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--filter)
		FILTER="$2"
		shift 2
		;;
	--no-coverage)
		COVERAGE_MIN=0
		shift
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

# Sources under measurement. Keep this list in step with the unit tests: a
# module added here without tests will fail the threshold, which is the point.
COVERED_SOURCES=(
	src/firestore_wire.cpp
	src/firestore_paging.cpp
	src/firestore_schema_accumulator.cpp
)

TEST_SOURCES=(
	test/unit/test_main.cpp
	test/unit/test_harness_self.cpp
	test/unit/test_firestore_wire.cpp
	test/unit/test_firestore_paging.cpp
	test/unit/test_firestore_schema_accumulator.cpp
)

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "== compiling unit tests =="
CXXFLAGS=(
	-std=c++17 -O0 -g -Wall -Wextra -Werror
	--coverage
	"-I$ROOT/src/include"
	"-I$ROOT/third_party/nlohmann_json/include"
	"-I$ROOT/test/unit"
)

OBJECTS=()
for source in "${COVERED_SOURCES[@]}" "${TEST_SOURCES[@]}"; do
	object="$(basename "${source%.cpp}").o"
	"$CXX" "${CXXFLAGS[@]}" -c "$ROOT/$source" -o "$object"
	OBJECTS+=("$object")
done
"$CXX" --coverage "${OBJECTS[@]}" -o unit_tests

echo "== running unit tests =="
./unit_tests "$FILTER"

if [[ "$COVERAGE_MIN" -eq 0 || -n "$FILTER" ]]; then
	# A filtered run measures only the tests that ran, so enforcing a
	# threshold against it would be meaningless.
	exit 0
fi

echo "== coverage =="
# gcov reports every file an object pulled in, including system and vendored
# headers; only the modules under measurement are scored.
gcov -o . "${OBJECTS[@]}" >gcov.txt 2>/dev/null || true

COVERAGE_MIN="$COVERAGE_MIN" python3 - "$ROOT" "${COVERED_SOURCES[@]}" <<'PYTHON'
import os
import re
import sys

root, sources = sys.argv[1], sys.argv[2:]
threshold = float(os.environ["COVERAGE_MIN"])

# gcov emits, per file:   File 'path'\n   Lines executed:NN.NN% of N
report = open("gcov.txt").read()
measured = dict(
    (path, (float(percent), int(total)))
    for path, percent, total in re.findall(
        r"File '([^']+)'\nLines executed:([0-9.]+)% of ([0-9]+)", report
    )
)

failed = False
for source in sources:
    absolute = os.path.join(root, source)
    if absolute not in measured:
        print(f"  {source:<45} NO COVERAGE DATA")
        failed = True
        continue
    percent, total = measured[absolute]
    print(f"  {source:<45} {percent:6.2f}%  ({total} lines)")
    if percent < threshold:
        print(f"    below the {threshold:g}% threshold")
        failed = True

if failed:
    print(f"coverage check FAILED (threshold {threshold:g}%)", file=sys.stderr)
    sys.exit(1)
print(f"coverage check passed (threshold {threshold:g}%)")
PYTHON
