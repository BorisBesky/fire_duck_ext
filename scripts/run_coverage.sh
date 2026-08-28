#!/usr/bin/env bash
#
# Coverage over the lines this branch adds or changes.
#
# Whole-file percentages are the wrong measure here: firestore_scanner.cpp and
# firestore_client.cpp are mostly pre-existing code that this work does not
# touch, and counting it either flatters or punishes the change for reasons
# that have nothing to do with it. This reports *diff* coverage -- of the lines
# `git diff` says are new or modified, how many did the tests execute.
#
# Two runs are merged, because the new code is reached from two directions:
#
#   1. scripts/run_unit_tests.sh    -- the DuckDB-free modules, directly
#   2. the SQL + integration suites -- the scanner/client glue, through a real
#                                      DuckDB against the mock Firestore
#
# A line counts as covered if either run executed it.
#
#   scripts/run_coverage.sh                  # build, run everything, enforce
#   COVERAGE_MIN=90 scripts/run_coverage.sh  # different threshold
#   COVERAGE_BASE=main scripts/run_coverage.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COVERAGE_MIN="${COVERAGE_MIN:-95}"
COVERAGE_BASE="${COVERAGE_BASE:-origin/main}"
PORT="${MOCK_PORT:-8124}"
GCOV_DIR="$ROOT/build/coverage"
EXTENSION_OBJECT_DIR="$ROOT/build/release/extension/fire_duck_ext/CMakeFiles/fire_duck_ext_extension.dir/src"

rm -rf "$GCOV_DIR"
mkdir -p "$GCOV_DIR"

echo "############ unit tests ############"
scripts/run_unit_tests.sh
# Prefixed: both builds emit e.g. firestore_wire.cpp.gcov, and one must not
# overwrite the other -- the whole point is to merge their counts.
for report in "$ROOT"/build/unit/*.gcov; do
	[ -e "$report" ] && cp "$report" "$GCOV_DIR/unit-$(basename "$report")"
done

echo
echo "############ building the extension with coverage ############"
cmake -B build/release -DFIRESTORE_COVERAGE=ON -S duckdb >/dev/null
ninja -C build/release >/dev/null
# Discard counters from any earlier run so the report reflects this one.
find "$EXTENSION_OBJECT_DIR" -name '*.gcda' -delete

echo
echo "############ mock Firestore ############"
python3 bench/mock_firestore.py "$PORT" >/dev/null 2>&1 &
MOCK_PID=$!
trap 'kill "$MOCK_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do
	curl -s --noproxy '*' "http://127.0.0.1:$PORT/__health" >/dev/null 2>&1 && break
	sleep 0.5
done

echo
echo "############ SQL + integration tests ############"
FIRESTORE_EMULATOR_HOST="127.0.0.1:$PORT" FIRESTORE_MOCK_COLLECTIONS=1 \
	./build/release/test/unittest test/sql/firestore_page_size.test
MOCK_PORT="$PORT" python3 test/integration/large_collections.py
./build/release/test/unittest "test/*"

echo
echo "############ diff coverage ############"
(cd "$EXTENSION_OBJECT_DIR" && gcov *.gcno >/dev/null 2>&1 || true)
for report in "$EXTENSION_OBJECT_DIR"/*.gcov; do
	[ -e "$report" ] && cp "$report" "$GCOV_DIR/extension-$(basename "$report")"
done

COVERAGE_MIN="$COVERAGE_MIN" COVERAGE_BASE="$COVERAGE_BASE" GCOV_DIR="$GCOV_DIR" \
	python3 scripts/diff_coverage.py
