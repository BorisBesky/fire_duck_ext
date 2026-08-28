#!/usr/bin/env bash
#
# Everything that covers large-collection handling, in one command:
#
#   1. unit tests over the DuckDB-free modules, with a coverage threshold
#   2. the SQL test suite, including the emulator-gated paging tests
#   3. request-level integration tests against bench/mock_firestore.py
#
# The mock stands in for Firestore because these assertions are about scale
# and about what goes on the wire: collections of thousands of documents exist
# instantly, and every request's page size is recorded.
#
#   test/scripts/run_large_collection_tests.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${MOCK_PORT:-8123}"
cd "$ROOT"

if [ ! -x build/release/duckdb ]; then
	echo "build/release/duckdb is missing; run 'make release' first" >&2
	exit 2
fi

echo "############ unit tests (no DuckDB, no network) ############"
scripts/run_unit_tests.sh

echo
echo "############ mock Firestore ############"
python3 bench/mock_firestore.py "$PORT" >/dev/null 2>&1 &
MOCK_PID=$!
trap 'kill "$MOCK_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 60); do
	if curl -s --noproxy '*' "http://127.0.0.1:$PORT/__health" >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
echo "listening on 127.0.0.1:$PORT"

echo
echo "############ SQL tests (paging tests included) ############"
# The other SQL tests assert failures for operations that succeed against a
# live endpoint, so only the paging file runs with an emulator host set.
FIRESTORE_EMULATOR_HOST="127.0.0.1:$PORT" FIRESTORE_MOCK_COLLECTIONS=1 \
	./build/release/test/unittest test/sql/firestore_page_size.test

echo
echo "############ integration tests ############"
MOCK_PORT="$PORT" python3 test/integration/large_collections.py

echo
echo "############ SQL tests (no emulator) ############"
./build/release/test/unittest "test/*"
