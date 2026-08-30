#!/usr/bin/env bash
#
# Validation against a real Firestore, rather than against the mock.
#
# With no arguments this starts the Firestore emulator and runs everything the
# emulator can answer. The tests that need a Google-hosted project -- index
# planning, OAuth token refresh -- are skipped and say so.
#
#   test/scripts/run_real_firestore_tests.sh              # emulator
#   test/scripts/run_real_firestore_tests.sh cursor       # matching tests only
#
# Against a live project, set the endpoint yourself and run the file directly:
#
#   FIRESTORE_TEST_PROJECT=my-project \
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json \
#       python3 test/integration/real_firestore.py
#
# The fixtures are written under the fdx_validation_ prefix and deleted
# afterwards; set FDX_KEEP_DATA=1 to leave them in place for inspection.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${FIRESTORE_EMULATOR_PORT:-8770}"
cd "$ROOT"

if [ ! -x build/release/duckdb ]; then
	echo "build/release/duckdb is missing; run 'make release' first" >&2
	exit 2
fi

# Reuse an emulator that is already listening; starting a second one on the
# same port would leave the tests talking to whichever won the race.
if curl -s --noproxy '*' "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
	echo "using the Firestore emulator already on 127.0.0.1:$PORT"
else
	EMULATOR_JAR="$(ls -1 "$HOME"/.cache/firebase/emulators/cloud-firestore-emulator-*.jar 2>/dev/null | tail -1 || true)"
	if [ -n "$EMULATOR_JAR" ] && command -v java >/dev/null 2>&1; then
		echo "starting $(basename "$EMULATOR_JAR") on 127.0.0.1:$PORT"
		java -jar "$EMULATOR_JAR" --host=127.0.0.1 --port="$PORT" >/tmp/fdx-emulator.log 2>&1 &
		trap 'kill "$!" 2>/dev/null || true' EXIT
	elif command -v firebase >/dev/null 2>&1; then
		echo "starting the emulator through the firebase CLI on 127.0.0.1:$PORT"
		firebase emulators:start --only firestore --project fire-duck-validation >/tmp/fdx-emulator.log 2>&1 &
		trap 'kill "$!" 2>/dev/null || true' EXIT
	else
		echo "no Firestore emulator available: install the firebase CLI, or run" >&2
		echo "test/integration/real_firestore.py against a live project instead." >&2
		exit 2
	fi

	for _ in $(seq 1 60); do
		if curl -s --noproxy '*' "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
			break
		fi
		sleep 0.5
	done
fi

FIRESTORE_EMULATOR_HOST="127.0.0.1:$PORT" python3 test/integration/real_firestore.py "$@"
