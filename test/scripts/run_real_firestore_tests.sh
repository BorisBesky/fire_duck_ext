#!/bin/bash
# Run composite index integration tests against real Firestore
# Tests the happy path where composite indexes exist and the extension
# detects them to perform server-side multi-field ordering.
#
# Usage: ./test/scripts/run_real_firestore_tests.sh /path/to/service-account.json [test-range]
#
# Test range formats (optional, defaults to all tests):
#   1-20      Run tests 1 through 20
#   1,3,5     Run only tests 1, 3, and 5
#   26-       Run tests 26 through the last test
#   -10       Run tests 1 through 10
#   42        Run only test 42
#
# Prerequisites:
#   - gcloud CLI installed
#   - python3 available
#   - Service account needs roles/datastore.user + roles/datastore.indexAdmin
#   - Extension built: make release

set -e

SA_PATH="$1"
if [ -z "$SA_PATH" ] || [ ! -f "$SA_PATH" ]; then
    echo "Usage: $0 /path/to/service-account.json [test-range]"
    echo ""
    echo "Test range formats (optional, defaults to all):"
    echo "  1-20      Run tests 1 through 20"
    echo "  1,3,5     Run only tests 1, 3, and 5"
    echo "  26-       Run tests 26 through the last test"
    echo "  -10       Run tests 1 through 10"
    echo "  42        Run only test 42"
    exit 1
fi

# --- Test range parsing ---
# Compatible with Bash 3.2 (macOS default) — uses comma-separated string
# instead of associative arrays.
TEST_RANGE="${2:-}"

# SELECTED_TESTS is a comma-delimited string of test numbers, e.g. ",1,2,3,"
# The leading/trailing commas enable exact substring matching via grep.
SELECTED_TESTS=""
RUN_ALL=true

if [ -n "$TEST_RANGE" ]; then
    RUN_ALL=false
    if echo "$TEST_RANGE" | grep -qE '^[0-9]+-$'; then
        # Open-ended range: "26-" means 26 through 999
        RANGE_START=$(echo "$TEST_RANGE" | sed 's/-$//')
        for i in $(seq "$RANGE_START" 999); do SELECTED_TESTS="${SELECTED_TESTS},${i}"; done
    elif echo "$TEST_RANGE" | grep -qE '^-[0-9]+$'; then
        # Open-start range: "-10" means 1 through 10
        RANGE_END=$(echo "$TEST_RANGE" | sed 's/^-//')
        for i in $(seq 1 "$RANGE_END"); do SELECTED_TESTS="${SELECTED_TESTS},${i}"; done
    elif echo "$TEST_RANGE" | grep -qE '^[0-9]+-[0-9]+$'; then
        # Closed range: "1-20"
        RANGE_START=$(echo "$TEST_RANGE" | cut -d- -f1)
        RANGE_END=$(echo "$TEST_RANGE" | cut -d- -f2)
        for i in $(seq "$RANGE_START" "$RANGE_END"); do SELECTED_TESTS="${SELECTED_TESTS},${i}"; done
    elif echo "$TEST_RANGE" | grep -qE '^[0-9,]+$'; then
        # Comma-separated list: "1,3,5" or single number "42"
        SELECTED_TESTS=",${TEST_RANGE},"
    else
        echo "Error: Invalid test range format '$TEST_RANGE'"
        echo "Examples: 1-20, 1,3,5, 26-, -10, 42"
        exit 1
    fi
    # Ensure leading/trailing commas for exact matching
    SELECTED_TESTS=",${SELECTED_TESTS#,}"
    SELECTED_TESTS="${SELECTED_TESTS%,},"
    echo "Test filter: $TEST_RANGE"
fi

# Check if a test number should run
should_run() {
    if [ "$RUN_ALL" = true ]; then return 0; fi
    echo "$SELECTED_TESTS" | grep -q ",${1},"
}

# Check if any test in a range should run (for conditional seeding)
any_in_range() {
    local lo="$1" hi="$2"
    if [ "$RUN_ALL" = true ]; then return 0; fi
    for i in $(seq "$lo" "$hi"); do
        if echo "$SELECTED_TESTS" | grep -q ",${i},"; then return 0; fi
    done
    return 1
}

# --- Authentication ---

PROJECT_ID=$(python3 -c "import json; print(json.load(open('$SA_PATH'))['project_id'])")
DATABASE_ID="(default)"

echo "Project: $PROJECT_ID"
echo "Activating service account..."
gcloud auth activate-service-account --key-file="$SA_PATH" --quiet
ACCESS_TOKEN=$(gcloud auth print-access-token)

ADMIN_BASE="https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/${DATABASE_ID}"
DATA_BASE="${ADMIN_BASE}/documents"

# --- DuckDB helpers ---

# Find duckdb binary
if command -v duckdb &> /dev/null; then
    DUCKDB="duckdb"
elif [ -x "./build/release/duckdb" ]; then
    DUCKDB="./build/release/duckdb"
else
    echo "Error: duckdb not found. Install DuckDB or build from source."
    exit 1
fi

EXT_PATH="build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension"

if [ ! -f "$EXT_PATH" ]; then
    echo "Error: Extension not found at $EXT_PATH. Run 'make release' first."
    exit 1
fi

run_query() {
    $DUCKDB -unsigned -csv -noheader -c "
LOAD '${EXT_PATH}';
SET firestore_schema_cache_ttl=0;
CREATE SECRET __fs (TYPE firestore, PROJECT_ID '${PROJECT_ID}', SERVICE_ACCOUNT_JSON '${SA_PATH}');
$1
" 2>&1 | tail -1 | tr -d '[:space:]"'
}

# Like run_query but dumps debug logs to stderr and full output
run_query_debug() {
    echo "  [DEBUG] Full output:" >&2
    FIRESTORE_LOG_LEVEL=debug $DUCKDB -unsigned -csv -noheader -c "
LOAD '${EXT_PATH}';
SET firestore_schema_cache_ttl=0;
CREATE SECRET __fs (TYPE firestore, PROJECT_ID '${PROJECT_ID}', SERVICE_ACCOUNT_JSON '${SA_PATH}');
$1
" 2>&1 | tee /dev/stderr | tail -1 | tr -d '[:space:]"'
}

assert_eq() {
    local actual="$1"
    local expected="$2"
    local msg="$3"
    if [ "$actual" != "$expected" ]; then
        echo "FAIL: $msg (expected '$expected', got '$actual')"
        exit 1
    else
        echo "PASS: $msg"
    fi
}

assert_ge() {
    local actual="$1"
    local threshold="$2"
    local msg="$3"
    if [ "$actual" -lt "$threshold" ]; then
        echo "FAIL: $msg (expected >= $threshold, got $actual)"
        exit 1
    else
        echo "PASS: $msg"
    fi
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="$3"
    # Strip box-drawing chars and collapse whitespace (EXPLAIN wraps in visual boxes)
    local flat
    flat=$(echo "$haystack" | sed 's/[│┌┐└┘├┤┬┴┼─╴╶╵╷═║╔╗╚╝╠╣╦╩╬]//g' | tr '\n' ' ' | sed 's/  */ /g')
    if echo "$flat" | grep -qF "$needle"; then
        echo "PASS: $msg"
    else
        echo "FAIL: $msg (expected output to contain '$needle')"
        echo "  Actual output: $haystack"
        exit 1
    fi
}

assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="$3"
    local flat
    flat=$(echo "$haystack" | sed 's/[│┌┐└┘├┤┬┴┼─╴╶╵╷═║╔╗╚╝╠╣╦╩╬]//g' | tr '\n' ' ' | sed 's/  */ /g')
    if echo "$flat" | grep -qF "$needle"; then
        echo "FAIL: $msg (expected output NOT to contain '$needle')"
        echo "  Actual output: $haystack"
        exit 1
    else
        echo "PASS: $msg"
    fi
}

# Like run_query but returns full multi-line output (for EXPLAIN)
run_explain() {
    $DUCKDB -unsigned -csv -noheader -c "
LOAD '${EXT_PATH}';
SET firestore_schema_cache_ttl=0;
CREATE SECRET __fs (TYPE firestore, PROJECT_ID '${PROJECT_ID}', SERVICE_ACCOUNT_JSON '${SA_PATH}');
$1
" 2>&1
}

# --- Index creation (idempotent) ---

wait_for_index() {
    local INDEX_NAME="$1"
    local MAX_WAIT=180  # seconds
    local WAITED=0

    while [ $WAITED -lt $MAX_WAIT ]; do
        STATE=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
            "https://firestore.googleapis.com/v1/${INDEX_NAME}" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('state','UNKNOWN'))" 2>/dev/null)

        if [ "$STATE" = "READY" ]; then
            echo "  Index is READY"
            return 0
        fi

        echo "  Waiting for index (state: $STATE)..."
        sleep 5
        WAITED=$((WAITED + 5))
    done

    echo "  ERROR: Index not READY after ${MAX_WAIT}s"
    return 1
}

ensure_index() {
    local COLLECTION_ID="$1"
    local SCOPE="$2"
    local FIELDS_JSON="$3"
    local DESCRIPTION="$4"

    echo "Checking index: $DESCRIPTION..."

    # List existing indexes
    INDEXES=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${ADMIN_BASE}/collectionGroups/${COLLECTION_ID}/indexes")

    # Check if matching index already exists (by field paths, order, and scope)
    EXISTING=$(echo "$INDEXES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
target_fields = json.loads('$FIELDS_JSON')
target_scope = '$SCOPE'
for idx in data.get('indexes', []):
    if idx.get('queryScope') != target_scope:
        continue
    idx_fields = [(f['fieldPath'], f.get('order','ASCENDING'))
                  for f in idx.get('fields',[]) if f['fieldPath'] != '__name__']
    tgt = [(f['fieldPath'], f.get('order','ASCENDING')) for f in target_fields]
    if idx_fields == tgt:
        print(idx['name'])
        break
" 2>/dev/null)

    if [ -n "$EXISTING" ]; then
        echo "  Index already exists: $EXISTING"
        wait_for_index "$EXISTING"
        return
    fi

    echo "  Creating index..."
    RESPONSE=$(curl -s -X POST \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"queryScope\": \"$SCOPE\", \"fields\": $FIELDS_JSON}" \
        "${ADMIN_BASE}/collectionGroups/${COLLECTION_ID}/indexes")

    # Extract index name from long-running operation response
    INDEX_NAME=$(echo "$RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
if 'metadata' in data and 'index' in data['metadata']:
    print(data['metadata']['index'])
elif 'name' in data:
    print(data['name'])
else:
    print('')
" 2>/dev/null)

    if [ -z "$INDEX_NAME" ]; then
        echo "  ERROR: Could not extract index name from response"
        echo "  Response: $RESPONSE"
        return 1
    fi

    echo "  Created: $INDEX_NAME"
    wait_for_index "$INDEX_NAME"
}

seed_doc() {
    curl -s -X PATCH \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        -d "$2" \
        "${DATA_BASE}/$1" > /dev/null
}

# --- Conditional setup: indexes + read-test data (tests 1-25) ---

if any_in_range 1 25; then
    echo ""
    echo "=== Ensuring composite indexes ==="

    # Index 1: COLLECTION scope composite (score ASC, name ASC)
    ensure_index "fde_ci_test" "COLLECTION" \
        '[{"fieldPath":"score","order":"ASCENDING"},{"fieldPath":"name","order":"ASCENDING"}]' \
        "fde_ci_test (score ASC, name ASC) COLLECTION"

    # Index 2: COLLECTION_GROUP scope composite (val ASC, label ASC)
    ensure_index "fde_ci_child" "COLLECTION_GROUP" \
        '[{"fieldPath":"val","order":"ASCENDING"},{"fieldPath":"label","order":"ASCENDING"}]' \
        "fde_ci_child (val ASC, label ASC) COLLECTION_GROUP"

    echo ""
    echo "=== Seeding test data ==="

    # Collection: fde_ci_test (5 documents with score + name)
    # score/name pairs chosen so composite ordering is verifiable:
    # score ASC, name ASC → Alpha(10), Charlie(20), Bravo(30), Delta(30), Echo(50)
    # Bravo and Delta share score=30 so the second field (name) breaks ties.
    seed_doc "fde_ci_test/d1" '{"fields":{"score":{"integerValue":"10"},"name":{"stringValue":"Alpha"}}}'
    seed_doc "fde_ci_test/d2" '{"fields":{"score":{"integerValue":"30"},"name":{"stringValue":"Bravo"}}}'
    seed_doc "fde_ci_test/d3" '{"fields":{"score":{"integerValue":"20"},"name":{"stringValue":"Charlie"}}}'
    seed_doc "fde_ci_test/d4" '{"fields":{"score":{"integerValue":"30"},"name":{"stringValue":"Delta"}}}'
    seed_doc "fde_ci_test/d5" '{"fields":{"score":{"integerValue":"50"},"name":{"stringValue":"Echo"}}}'

    # Collection group: fde_ci_parent/{p1,p2}/fde_ci_child (docs with val + label)
    # val ASC, label ASC → A(10), C(10), B(20)
    # A and C share val=10 so the second field (label) breaks ties.
    seed_doc "fde_ci_parent/p1/fde_ci_child/c1" '{"fields":{"val":{"integerValue":"20"},"label":{"stringValue":"B"}}}'
    seed_doc "fde_ci_parent/p1/fde_ci_child/c2" '{"fields":{"val":{"integerValue":"10"},"label":{"stringValue":"A"}}}'
    seed_doc "fde_ci_parent/p2/fde_ci_child/c3" '{"fields":{"val":{"integerValue":"10"},"label":{"stringValue":"C"}}}'

    echo "Test data seeded."
fi

# --- Tests ---

if any_in_range 1 4; then
    echo ""
    echo "=== Running composite index tests ==="
fi

if should_run 1; then
    # Test 1: Composite order_by on COLLECTION — server-side ordering
    echo "Test 1: Composite order_by on collection..."
    RESULT=$(run_query \
        "SELECT string_agg(name, ',') FROM firestore_scan('fde_ci_test', order_by='score, name');")
    assert_eq "$RESULT" "Alpha,Charlie,Bravo,Delta,Echo" \
        "composite order_by='score, name' on collection uses index"
fi

if should_run 2; then
    # Test 2: Composite order_by on COLLECTION_GROUP — server-side ordering
    echo "Test 2: Composite order_by on collection group..."
    RESULT=$(run_query \
        "SELECT string_agg(label, ',') FROM firestore_scan('~fde_ci_child', order_by='val, label');")
    assert_eq "$RESULT" "A,C,B" \
        "composite order_by='val, label' on collection group uses index"
fi

if should_run 3; then
    # Test 3: Single-field order_by still works on collection
    echo "Test 3: Single-field order_by on collection..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test', order_by='score') LIMIT 1;")
    assert_eq "$RESULT" "Alpha" "single-field order_by='score' still works"
fi

if should_run 4; then
    # Test 4: Composite order_by with DESC (no matching DESC index — query still succeeds)
    echo "Test 4: Composite order_by with DESC on collection..."
    RESULT=$(run_query \
        "SELECT count(*) FROM firestore_scan('fde_ci_test', order_by='score DESC, name DESC');")
    assert_eq "$RESULT" "5" \
        "composite order_by DESC gracefully falls back (no DESC index)"
fi

if any_in_range 5 17; then
    echo ""
    echo "=== Running SQL ORDER BY / LIMIT pushdown tests ==="
fi

if should_run 5; then
    # Test 5: SQL ORDER BY pushdown — single field ascending
    echo "Test 5: SQL ORDER BY single field ascending..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 1;")
    assert_eq "$RESULT" "Alpha" \
        "SQL ORDER BY score ASC pushes to Firestore (Alpha has lowest score)"
fi

if should_run 6; then
    # Test 6: SQL ORDER BY pushdown — single field descending
    echo "Test 6: SQL ORDER BY single field descending..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 1;")
    assert_eq "$RESULT" "Echo" \
        "SQL ORDER BY score DESC pushes to Firestore (Echo has highest score)"
fi

if should_run 7; then
    # Test 7: SQL LIMIT pushdown — limits documents fetched from Firestore
    echo "Test 7: SQL LIMIT pushdown..."
    RESULT=$(run_query \
        "SELECT count(*) FROM (SELECT * FROM firestore_scan('fde_ci_test') LIMIT 3);")
    assert_eq "$RESULT" "3" \
        "SQL LIMIT 3 returns exactly 3 rows"
fi

if should_run 8; then
    # Test 8: SQL ORDER BY + LIMIT combined
    echo "Test 8: SQL ORDER BY + LIMIT combined..."
    RESULT=$(run_query \
        "SELECT string_agg(name, ',' ORDER BY score DESC) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 2) sub;")
    assert_eq "$RESULT" "Echo,Delta" \
        "SQL ORDER BY score DESC LIMIT 2 returns top 2 scorers"
fi

if should_run 9; then
    # Test 9: SQL ORDER BY + LIMIT — ascending top 3
    echo "Test 9: SQL ORDER BY + LIMIT ascending top 3..."
    RESULT=$(run_query \
        "SELECT string_agg(name, ',' ORDER BY score) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 3) sub;")
    assert_eq "$RESULT" "Alpha,Charlie,Bravo" \
        "SQL ORDER BY score ASC LIMIT 3 returns bottom 3 scorers"
fi

if should_run 10; then
    # Test 10: Named param order_by takes precedence over SQL ORDER BY
    echo "Test 10: Named param order_by takes precedence over SQL ORDER BY..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test', order_by='score') ORDER BY score DESC LIMIT 1;")
    assert_eq "$RESULT" "Echo" \
        "Named param order_by takes precedence (DuckDB re-sorts client-side)"
fi

if should_run 11; then
    # Test 11: Named scan_limit takes precedence over SQL LIMIT
    echo "Test 11: Named scan_limit takes precedence over SQL LIMIT..."
    RESULT=$(run_query \
        "SELECT count(*) FROM firestore_scan('fde_ci_test', scan_limit=2) LIMIT 10;")
    assert_eq "$RESULT" "2" \
        "Named scan_limit=2 takes precedence over SQL LIMIT 10"
fi

if should_run 12; then
    # Test 12: SQL ORDER BY + LIMIT + WHERE (all three pushed)
    echo "Test 12: SQL ORDER BY + LIMIT + WHERE combined..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') WHERE score > 20 ORDER BY score DESC LIMIT 1;")
    assert_eq "$RESULT" "Echo" \
        "WHERE score > 20 + ORDER BY DESC + LIMIT 1 returns Echo (score=50)"
fi

if should_run 13; then
    # Test 13: SQL ORDER BY on string field
    echo "Test 13: SQL ORDER BY on string field..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY name LIMIT 1;")
    assert_eq "$RESULT" "Alpha" \
        "SQL ORDER BY name ASC returns Alpha first"

    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY name DESC LIMIT 1;")
    assert_eq "$RESULT" "Echo" \
        "SQL ORDER BY name DESC returns Echo first"
fi

if should_run 14; then
    # Test 14: SQL LIMIT + OFFSET pushdown
    echo "Test 14: SQL LIMIT + OFFSET pushdown..."
    RESULT=$(run_query \
        "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 1 OFFSET 1;")
    assert_eq "$RESULT" "Charlie" \
        "ORDER BY score LIMIT 1 OFFSET 1 returns Charlie (second lowest score=20)"
fi

if should_run 15; then
    # Test 15: SQL ORDER BY on collection group
    echo "Test 15: SQL ORDER BY on collection group..."
    RESULT=$(run_query \
        "SELECT label FROM firestore_scan('~fde_ci_child') ORDER BY val LIMIT 1;")
    assert_eq "$RESULT" "A" \
        "SQL ORDER BY val on collection group returns lowest val document"
fi

if should_run 16; then
    # Test 16: SQL LIMIT without ORDER BY on collection group
    echo "Test 16: SQL LIMIT on collection group..."
    RESULT=$(run_query \
        "SELECT count(*) FROM (SELECT * FROM firestore_scan('~fde_ci_child') LIMIT 2) sub;")
    assert_eq "$RESULT" "2" \
        "SQL LIMIT 2 on collection group returns exactly 2 rows"
fi

if should_run 17; then
    # Test 17: Composite SQL ORDER BY (multi-field) — requires composite index
    echo "Test 17: SQL composite ORDER BY (multi-field)..."
    RESULT=$(run_query \
        "SELECT string_agg(name, ',' ORDER BY score, name) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score, name) sub;")
    assert_eq "$RESULT" "Alpha,Charlie,Bravo,Delta,Echo" \
        "SQL ORDER BY score, name uses composite index (same result as named param)"
fi

if any_in_range 18 25; then
    echo ""
    echo "=== Running EXPLAIN plan validation tests ==="
fi

if should_run 18; then
    # Test 18: EXPLAIN shows ORDER BY pushdown
    echo "Test 18: EXPLAIN shows ORDER BY pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 5;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score DESC" \
        "EXPLAIN shows 'Firestore Pushed Order: score DESC'"
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 5" \
        "EXPLAIN shows 'Firestore Pushed Limit: 5'"
fi

if should_run 19; then
    # Test 19: EXPLAIN shows LIMIT-only pushdown (no ORDER BY)
    echo "Test 19: EXPLAIN shows LIMIT-only pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') LIMIT 3;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 3" \
        "EXPLAIN shows 'Firestore Pushed Limit: 3' without ORDER BY"
fi

if should_run 20; then
    # Test 20: EXPLAIN shows ORDER BY-only pushdown (no LIMIT)
    echo "Test 20: EXPLAIN shows ORDER BY-only pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY name;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: name" \
        "EXPLAIN shows 'Firestore Pushed Order: name' without LIMIT"
    assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Limit" \
        "EXPLAIN does not show Pushed Limit when no LIMIT clause"
fi

if should_run 21; then
    # Test 21: EXPLAIN shows multi-field ORDER BY pushdown
    echo "Test 21: EXPLAIN shows multi-field ORDER BY pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score, name LIMIT 2;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score, name" \
        "EXPLAIN shows 'Firestore Pushed Order: score, name' for multi-field"
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 2" \
        "EXPLAIN shows 'Firestore Pushed Limit: 2' alongside multi-field order"
fi

if should_run 22; then
    # Test 22: EXPLAIN shows LIMIT+OFFSET pushdown (limit+offset value)
    echo "Test 22: EXPLAIN shows LIMIT+OFFSET pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 3 OFFSET 2;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score" \
        "EXPLAIN shows ORDER BY pushdown with OFFSET"
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 5" \
        "EXPLAIN shows Pushed Limit: 5 (limit 3 + offset 2)"
fi

if should_run 23; then
    # Test 23: EXPLAIN shows all three: filter + ORDER BY + LIMIT
    echo "Test 23: EXPLAIN shows filter + ORDER BY + LIMIT pushdown..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') WHERE score > 10 ORDER BY score DESC LIMIT 3;")
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Filters:" \
        "EXPLAIN shows filter pushdown alongside ORDER BY + LIMIT"
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score DESC" \
        "EXPLAIN shows ORDER BY pushdown alongside filter + LIMIT"
    assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 3" \
        "EXPLAIN shows LIMIT pushdown alongside filter + ORDER BY"
fi

if should_run 24; then
    # Test 24: EXPLAIN does NOT show SQL pushdown when named params are used
    echo "Test 24: EXPLAIN omits SQL pushdown when named params set..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test', order_by='score', scan_limit=5) ORDER BY name DESC LIMIT 2;")
    assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Order:" \
        "EXPLAIN does not show SQL ORDER BY pushdown when named order_by is set"
    assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Limit:" \
        "EXPLAIN does not show SQL LIMIT pushdown when named scan_limit is set"
fi

if should_run 25; then
    # Test 25: EXPLAIN does NOT show ORDER BY pushdown for aggregate query
    echo "Test 25: EXPLAIN omits ORDER BY pushdown for aggregate queries..."
    EXPLAIN_OUT=$(run_explain \
        "EXPLAIN SELECT name, count(*) as cnt FROM firestore_scan('fde_ci_test') GROUP BY name ORDER BY cnt LIMIT 3;")
    assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Order:" \
        "EXPLAIN does not show ORDER BY pushdown when ORDER BY is on aggregate result"
fi

# =============================================
# Write Operation Tests
# Tests all Firestore write functions against real Firestore
# on both regular collections and collection groups.
# All verifications use Firestore REST API directly to avoid
# masking issues with firestore_scan.
# =============================================

# --- REST API verification helpers (always defined) ---

# Fetch a single document field value via REST API.
# Usage: get_field "collection/doc" "fieldName"
# Returns the string/integer/boolean value, or "NOT_FOUND" if document missing.
get_field() {
    local DOC_PATH="$1"
    local FIELD="$2"
    curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${DATA_BASE}/${DOC_PATH}" \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
if 'error' in data:
    print('NOT_FOUND')
    sys.exit(0)
fields = data.get('fields', {})
f = fields.get('$FIELD', {})
for vtype in ('stringValue', 'integerValue', 'booleanValue', 'doubleValue'):
    if vtype in f:
        print(f[vtype])
        sys.exit(0)
print('MISSING_FIELD')
" 2>/dev/null
}

# Check if a document exists via REST API.
# Usage: doc_exists "collection/doc"  →  "true" or "false"
doc_exists() {
    local DOC_PATH="$1"
    local STATUS
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${DATA_BASE}/${DOC_PATH}")
    if [ "$STATUS" = "200" ]; then
        echo "true"
    else
        echo "false"
    fi
}

# Check if an array field contains a value via REST API.
# Usage: array_contains "collection/doc" "fieldName" "value"  →  "true" or "false"
array_contains() {
    local DOC_PATH="$1"
    local FIELD="$2"
    local VALUE="$3"
    curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${DATA_BASE}/${DOC_PATH}" \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
if 'error' in data:
    print('false')
    sys.exit(0)
fields = data.get('fields', {})
arr = fields.get('$FIELD', {}).get('arrayValue', {}).get('values', [])
for v in arr:
    for vtype in ('stringValue', 'integerValue', 'booleanValue'):
        if vtype in v and str(v[vtype]) == '$VALUE':
            print('true')
            sys.exit(0)
print('false')
" 2>/dev/null
}

# --- Conditional setup: write test data (tests 26-61) ---

if any_in_range 26 61; then
    echo ""
    echo "=== Running write operation tests ==="
    echo "Seeding write test data..."

    # Regular collection: fde_write_test (documents for update/delete/array ops)
    seed_doc "fde_write_test/w1" '{"fields":{"name":{"stringValue":"Alice"},"age":{"integerValue":"30"},"status":{"stringValue":"active"}}}'
    seed_doc "fde_write_test/w2" '{"fields":{"name":{"stringValue":"Bob"},"age":{"integerValue":"25"},"status":{"stringValue":"pending"}}}'
    seed_doc "fde_write_test/w3" '{"fields":{"name":{"stringValue":"Charlie"},"age":{"integerValue":"35"},"status":{"stringValue":"inactive"}}}'
    seed_doc "fde_write_test/w4" '{"fields":{"name":{"stringValue":"Diana"},"age":{"integerValue":"28"},"status":{"stringValue":"pending"},"tags":{"arrayValue":{"values":[{"stringValue":"vip"},{"stringValue":"beta"}]}}}}'
    seed_doc "fde_write_test/w5" '{"fields":{"name":{"stringValue":"Eve"},"age":{"integerValue":"40"},"status":{"stringValue":"active"},"tags":{"arrayValue":{"values":[{"stringValue":"admin"},{"stringValue":"beta"}]}}}}'

    # Collection group: fde_write_parent/{p1,p2}/fde_write_child (for ~ prefix tests)
    seed_doc "fde_write_parent/p1/fde_write_child/cg1" '{"fields":{"label":{"stringValue":"X"},"priority":{"integerValue":"1"},"done":{"booleanValue":false},"items":{"arrayValue":{"values":[{"stringValue":"a"},{"stringValue":"b"}]}}}}'
    seed_doc "fde_write_parent/p1/fde_write_child/cg2" '{"fields":{"label":{"stringValue":"Y"},"priority":{"integerValue":"2"},"done":{"booleanValue":false},"items":{"arrayValue":{"values":[{"stringValue":"c"}]}}}}'
    seed_doc "fde_write_parent/p2/fde_write_child/cg3" '{"fields":{"label":{"stringValue":"Z"},"priority":{"integerValue":"3"},"done":{"booleanValue":true},"items":{"arrayValue":{"values":[{"stringValue":"d"},{"stringValue":"e"}]}}}}'

    echo "Write test data seeded."
fi

if should_run 26; then
    echo ""
    echo "--- firestore_insert (collection) ---"
    echo "Test 26: Insert with auto-generated ID..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('fde_write_test', (SELECT 'Frank' AS name, 50 AS age, 'new' AS status));")
    assert_eq "$RESULT" "1" \
        "firestore_insert auto-ID inserts 1 document"
fi

if should_run 27; then
    echo "Test 27: Insert with explicit document_id..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('fde_write_test', (SELECT 'w_ins1' AS id, 'Grace' AS name, 22 AS age), document_id := 'id');")
    assert_eq "$RESULT" "1" \
        "firestore_insert with document_id inserts 1 document"

    echo "Test 27b: Verify explicitly inserted document (REST)..."
    RESULT=$(get_field "fde_write_test/w_ins1" "name")
    assert_eq "$RESULT" "Grace" \
        "REST: inserted document w_ins1 has name=Grace"
fi

if should_run 28; then
    echo "Test 28: Insert multiple rows..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('fde_write_test',
            (SELECT * FROM (VALUES ('w_ins2', 'Hank', 33), ('w_ins3', 'Ivy', 27)) AS t(id, name, age)),
            document_id := 'id');")
    assert_eq "$RESULT" "2" \
        "firestore_insert inserts 2 documents"

    echo "Test 28b: Verify multi-insert documents (REST)..."
    RESULT=$(get_field "fde_write_test/w_ins2" "name")
    assert_eq "$RESULT" "Hank" \
        "REST: w_ins2 has name=Hank"
    RESULT=$(get_field "fde_write_test/w_ins3" "name")
    assert_eq "$RESULT" "Ivy" \
        "REST: w_ins3 has name=Ivy"
fi

if should_run 29; then
    echo "Test 29: Insert with mixed types..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('fde_write_test',
            (SELECT 'w_ins4' AS id, 'Jack' AS name, 3.14 AS score, true AS active),
            document_id := 'id');")
    assert_eq "$RESULT" "1" \
        "firestore_insert with mixed types inserts 1 document"

    echo "Test 29b: Verify mixed types (REST)..."
    RESULT=$(get_field "fde_write_test/w_ins4" "name")
    assert_eq "$RESULT" "Jack" \
        "REST: w_ins4 has name=Jack"
    RESULT=$(get_field "fde_write_test/w_ins4" "active")
    assert_eq "$RESULT" "True" \
        "REST: w_ins4 has active=true"
fi

if should_run 30; then
    echo ""
    echo "--- firestore_insert (collection group) ---"
    echo "Test 30: Insert into subcollection..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('~fde_write_child',
            (SELECT 'cg_ins1' AS id, 'Inserted' AS label, 99 AS priority),
            document_id := 'id');")
    assert_eq "$RESULT" "1" \
        "firestore_insert into subcollection inserts 1 document"

    echo "Test 30b: Verify inserted subcollection document (REST)..."
    RESULT=$(get_field "fde_write_parent/p1/fde_write_child/cg_ins1" "label")
    assert_eq "$RESULT" "Inserted" \
        "REST: subcollection doc cg_ins1 has label=Inserted"
fi

if should_run 31; then
    echo "Test 31: Insert into second parent's subcollection..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_insert('fde_write_parent/p2/fde_write_child',
            (SELECT 'cg_ins2' AS id, 'AlsoInserted' AS label, 88 AS priority),
            document_id := 'id');")
    assert_eq "$RESULT" "1" \
        "firestore_insert into second subcollection inserts 1 document"

    echo "Test 31b: Verify second subcollection insert (REST)..."
    RESULT=$(get_field "fde_write_parent/p2/fde_write_child/cg_ins2" "label")
    assert_eq "$RESULT" "AlsoInserted" \
        "REST: subcollection doc cg_ins2 has label=AlsoInserted"
fi

if should_run 32; then
    echo ""
    echo "--- firestore_update (collection) ---"
    echo "Test 32: Update single field..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update('fde_write_test', 'w1', 'status', 'updated');")
    assert_eq "$RESULT" "1" \
        "firestore_update single field returns 1"

    echo "Test 32b: Verify update (REST)..."
    RESULT=$(get_field "fde_write_test/w1" "status")
    assert_eq "$RESULT" "updated" \
        "REST: w1 status changed to 'updated'"
fi

if should_run 33; then
    echo "Test 33: Update multiple fields..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update('fde_write_test', 'w2', 'status', 'reviewed', 'age', 26);")
    assert_eq "$RESULT" "1" \
        "firestore_update multiple fields returns 1"

    echo "Test 33b: Verify multi-field update (REST)..."
    RESULT=$(get_field "fde_write_test/w2" "status")
    assert_eq "$RESULT" "reviewed" \
        "REST: w2 status changed to 'reviewed'"
    RESULT=$(get_field "fde_write_test/w2" "age")
    assert_eq "$RESULT" "26" \
        "REST: w2 age changed to 26"
fi

if should_run 34; then
    echo "Test 34: Update with boolean value..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update('fde_write_test', 'w3', 'active', true);")
    assert_eq "$RESULT" "1" \
        "firestore_update with boolean returns 1"

    echo "Test 34b: Verify boolean update (REST)..."
    RESULT=$(get_field "fde_write_test/w3" "active")
    assert_eq "$RESULT" "True" \
        "REST: w3 active changed to true"
fi

if should_run 35; then
    echo ""
    echo "--- firestore_update (collection group) ---"
    echo "Test 35: Update via collection group prefix..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update('~fde_write_child', 'fde_write_parent/p1/fde_write_child/cg1', 'done', true);")
    assert_eq "$RESULT" "1" \
        "firestore_update via ~collection_group returns 1"

    echo "Test 35b: Verify collection group update (REST)..."
    RESULT=$(get_field "fde_write_parent/p1/fde_write_child/cg1" "done")
    assert_eq "$RESULT" "True" \
        "REST: cg1 done changed to true via collection group update"
fi

if should_run 36; then
    echo "Test 36: Update second parent document via collection group..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update('~fde_write_child', 'fde_write_parent/p2/fde_write_child/cg3', 'label', 'Z-updated');")
    assert_eq "$RESULT" "1" \
        "firestore_update on cg3 via ~collection_group returns 1"

    echo "Test 36b: Verify second parent update (REST)..."
    RESULT=$(get_field "fde_write_parent/p2/fde_write_child/cg3" "label")
    assert_eq "$RESULT" "Z-updated" \
        "REST: cg3 label changed to 'Z-updated'"
fi

if should_run 37; then
    echo ""
    echo "--- firestore_update_batch (collection) ---"
    echo "Test 37: Batch update multiple documents..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update_batch('fde_write_test', ['w1', 'w2', 'w3'], 'status', 'batch_updated');")
    assert_eq "$RESULT" "3" \
        "firestore_update_batch updates 3 documents"

    echo "Test 37b: Verify batch update (REST)..."
    RESULT=$(get_field "fde_write_test/w1" "status")
    assert_eq "$RESULT" "batch_updated" "REST: w1 status=batch_updated"
    RESULT=$(get_field "fde_write_test/w2" "status")
    assert_eq "$RESULT" "batch_updated" "REST: w2 status=batch_updated"
    RESULT=$(get_field "fde_write_test/w3" "status")
    assert_eq "$RESULT" "batch_updated" "REST: w3 status=batch_updated"
fi

if should_run 38; then
    echo "Test 38: Batch update with variable pattern..."
    RESULT=$(run_query "
SET VARIABLE target_ids = (
    SELECT list(__document_id) FROM firestore_scan('fde_write_test')
    WHERE __document_id IN ('w4', 'w5')
);
SELECT * FROM firestore_update_batch('fde_write_test', getvariable('target_ids'), 'status', 'var_updated');
")
    assert_eq "$RESULT" "2" \
        "batch update via variable updates 2 documents"

    echo "Test 38b: Verify variable batch update (REST)..."
    RESULT=$(get_field "fde_write_test/w4" "status")
    assert_eq "$RESULT" "var_updated" "REST: w4 status=var_updated"
    RESULT=$(get_field "fde_write_test/w5" "status")
    assert_eq "$RESULT" "var_updated" "REST: w5 status=var_updated"
fi

if should_run 39; then
    echo "Test 39: Batch update multiple fields..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update_batch('fde_write_test', ['w1', 'w2'], 'status', 'multi', 'age', 99);")
    assert_eq "$RESULT" "2" \
        "batch update with 2 fields updates 2 documents"

    echo "Test 39b: Verify multi-field batch update (REST)..."
    RESULT=$(get_field "fde_write_test/w1" "status")
    assert_eq "$RESULT" "multi" "REST: w1 status=multi"
    RESULT=$(get_field "fde_write_test/w1" "age")
    assert_eq "$RESULT" "99" "REST: w1 age=99"
fi

if should_run 40; then
    echo "Test 40: Batch update empty list..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update_batch('fde_write_test', []::VARCHAR[], 'status', 'noop');")
    assert_eq "$RESULT" "0" \
        "batch update with empty list returns 0"
fi

if should_run 41; then
    echo ""
    echo "--- firestore_update_batch (collection group) ---"
    echo "Test 41: Batch update via collection group prefix..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update_batch('~fde_write_child',
            ['fde_write_parent/p1/fde_write_child/cg1', 'fde_write_parent/p1/fde_write_child/cg2'],
            'priority', 0);")
    assert_eq "$RESULT" "2" \
        "batch update via ~collection_group updates 2 documents"

    echo "Test 41b: Verify collection group batch update (REST)..."
    RESULT=$(get_field "fde_write_parent/p1/fde_write_child/cg1" "priority")
    assert_eq "$RESULT" "0" "REST: cg1 priority=0"
    RESULT=$(get_field "fde_write_parent/p1/fde_write_child/cg2" "priority")
    assert_eq "$RESULT" "0" "REST: cg2 priority=0"
fi

if should_run 42; then
    echo "Test 42: Batch update collection group with variable pattern..."
    RESULT=$(run_query "
SET VARIABLE cg_ids = (
    SELECT list(__document_id) FROM firestore_scan('~fde_write_child')
    WHERE __document_id LIKE '%/p2/%'
);
SELECT * FROM firestore_update_batch('~fde_write_child', getvariable('cg_ids'), 'done', false);
")
    assert_ge "$RESULT" "1" \
        "batch update collection group via variable updates at least 1 document"

    echo "Test 42b: Verify collection group variable batch update (REST)..."
    RESULT=$(get_field "fde_write_parent/p2/fde_write_child/cg3" "done")
    assert_eq "$RESULT" "False" "REST: cg3 done=false"
fi

if should_run 43; then
    echo "Test 43: Batch update collection group empty list..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_update_batch('~fde_write_child', []::VARCHAR[], 'priority', 999);")
    assert_eq "$RESULT" "0" \
        "batch update ~collection_group with empty list returns 0"
fi

if should_run 44; then
    echo ""
    echo "--- firestore_array_union (collection) ---"
    echo "Test 44: Array union add new element..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_union('fde_write_test', 'w4', 'tags', ['premium']);")
    assert_eq "$RESULT" "1" \
        "array_union returns 1"

    echo "Test 44b: Verify array union (REST)..."
    RESULT=$(array_contains "fde_write_test/w4" "tags" "premium")
    assert_eq "$RESULT" "true" \
        "REST: w4 tags contains 'premium' after array_union"
fi

if should_run 45; then
    echo "Test 45: Array union with existing element (no-dup)..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_union('fde_write_test', 'w4', 'tags', ['vip']);")
    assert_eq "$RESULT" "1" \
        "array_union with existing element returns 1"

    echo "Test 45b: Verify no duplicate after union (REST)..."
    VIP_COUNT=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${DATA_BASE}/fde_write_test/w4" \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
arr = data.get('fields',{}).get('tags',{}).get('arrayValue',{}).get('values',[])
print(sum(1 for v in arr if v.get('stringValue') == 'vip'))
" 2>/dev/null)
    assert_eq "$VIP_COUNT" "1" \
        "REST: 'vip' appears exactly once (no duplicate)"
fi

if should_run 46; then
    echo ""
    echo "--- firestore_array_union (collection group) ---"
    echo "Test 46: Array union via collection group..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_union('~fde_write_child',
            'fde_write_parent/p1/fde_write_child/cg1', 'items', ['f']);")
    assert_eq "$RESULT" "1" \
        "array_union via ~collection_group returns 1"

    echo "Test 46b: Verify collection group array union (REST)..."
    RESULT=$(array_contains "fde_write_parent/p1/fde_write_child/cg1" "items" "f")
    assert_eq "$RESULT" "true" \
        "REST: cg1 items contains 'f' after array_union"
fi

if should_run 47; then
    echo ""
    echo "--- firestore_array_remove (collection) ---"
    echo "Test 47: Array remove element..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_remove('fde_write_test', 'w5', 'tags', ['beta']);")
    assert_eq "$RESULT" "1" \
        "array_remove returns 1"

    echo "Test 47b: Verify array remove (REST)..."
    RESULT=$(array_contains "fde_write_test/w5" "tags" "beta")
    assert_eq "$RESULT" "false" \
        "REST: w5 tags no longer contains 'beta' after array_remove"

    echo "Test 47c: Verify remaining elements intact (REST)..."
    RESULT=$(array_contains "fde_write_test/w5" "tags" "admin")
    assert_eq "$RESULT" "true" \
        "REST: w5 tags still contains 'admin'"
fi

if should_run 48; then
    echo ""
    echo "--- firestore_array_remove (collection group) ---"
    echo "Test 48: Array remove via collection group..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_remove('~fde_write_child',
            'fde_write_parent/p2/fde_write_child/cg3', 'items', ['d']);")
    assert_eq "$RESULT" "1" \
        "array_remove via ~collection_group returns 1"

    echo "Test 48b: Verify collection group array remove (REST)..."
    RESULT=$(array_contains "fde_write_parent/p2/fde_write_child/cg3" "items" "d")
    assert_eq "$RESULT" "false" \
        "REST: cg3 items no longer contains 'd' after array_remove"

    echo "Test 48c: Verify remaining elements intact (REST)..."
    RESULT=$(array_contains "fde_write_parent/p2/fde_write_child/cg3" "items" "e")
    assert_eq "$RESULT" "true" \
        "REST: cg3 items still contains 'e'"
fi

if should_run 49; then
    echo ""
    echo "--- firestore_array_append (collection) ---"
    echo "Test 49: Array append element..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_append('fde_write_test', 'w4', 'tags', ['newitem']);")
    assert_eq "$RESULT" "1" \
        "array_append returns 1"

    echo "Test 49b: Verify array append (REST)..."
    RESULT=$(array_contains "fde_write_test/w4" "tags" "newitem")
    assert_eq "$RESULT" "true" \
        "REST: w4 tags contains 'newitem' after array_append"
fi

if should_run 50; then
    echo ""
    echo "--- firestore_array_append (collection group) ---"
    echo "Test 50: Array append via collection group..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_array_append('~fde_write_child',
            'fde_write_parent/p1/fde_write_child/cg2', 'items', ['appended']);")
    assert_eq "$RESULT" "1" \
        "array_append via ~collection_group returns 1"

    echo "Test 50b: Verify collection group array append (REST)..."
    RESULT=$(array_contains "fde_write_parent/p1/fde_write_child/cg2" "items" "appended")
    assert_eq "$RESULT" "true" \
        "REST: cg2 items contains 'appended' after array_append"
fi

if should_run 51; then
    echo ""
    echo "--- firestore_delete (collection) ---"
    echo "Test 51: Delete single document..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete('fde_write_test', 'w_ins1');")
    assert_eq "$RESULT" "1" \
        "firestore_delete returns 1"

    echo "Test 51b: Verify deletion (REST)..."
    RESULT=$(doc_exists "fde_write_test/w_ins1")
    assert_eq "$RESULT" "false" \
        "REST: w_ins1 no longer exists after delete"
fi

if should_run 52; then
    # Firestore DELETE is idempotent — returns success even for non-existent docs.
    echo "Test 52: Delete non-existent document (idempotent)..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete('fde_write_test', 'does_not_exist_xyz');")
    assert_eq "$RESULT" "1" \
        "firestore_delete non-existent document returns 1 (Firestore DELETE is idempotent)"
fi

if should_run 53; then
    echo ""
    echo "--- firestore_delete (collection group) ---"
    echo "Test 53: Delete via collection group prefix..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete('~fde_write_child', 'fde_write_parent/p1/fde_write_child/cg_ins1');")
    assert_eq "$RESULT" "1" \
        "firestore_delete via ~collection_group returns 1"

    echo "Test 53b: Verify collection group delete (REST)..."
    RESULT=$(doc_exists "fde_write_parent/p1/fde_write_child/cg_ins1")
    assert_eq "$RESULT" "false" \
        "REST: cg_ins1 no longer exists after collection group delete"
fi

if should_run 54; then
    echo "Test 54: Delete second subcollection doc via collection group..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete('~fde_write_child', 'fde_write_parent/p2/fde_write_child/cg_ins2');")
    assert_eq "$RESULT" "1" \
        "firestore_delete cg_ins2 via ~collection_group returns 1"

    echo "Test 54b: Verify second collection group delete (REST)..."
    RESULT=$(doc_exists "fde_write_parent/p2/fde_write_child/cg_ins2")
    assert_eq "$RESULT" "false" \
        "REST: cg_ins2 no longer exists after collection group delete"
fi

if should_run 55; then
    echo ""
    echo "--- firestore_delete_batch (collection) ---"
    echo "Test 55: Batch delete multiple documents..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete_batch('fde_write_test', ['w_ins2', 'w_ins3']);")
    assert_eq "$RESULT" "2" \
        "firestore_delete_batch deletes 2 documents"

    echo "Test 55b: Verify batch delete (REST)..."
    RESULT=$(doc_exists "fde_write_test/w_ins2")
    assert_eq "$RESULT" "false" "REST: w_ins2 no longer exists"
    RESULT=$(doc_exists "fde_write_test/w_ins3")
    assert_eq "$RESULT" "false" "REST: w_ins3 no longer exists"
fi

if should_run 56; then
    echo "Test 56: Batch delete with variable pattern..."
    RESULT=$(run_query "
SET VARIABLE del_ids = (
    SELECT list(__document_id) FROM firestore_scan('fde_write_test')
    WHERE __document_id = 'w_ins4'
);
SELECT * FROM firestore_delete_batch('fde_write_test', getvariable('del_ids'));
")
    assert_eq "$RESULT" "1" \
        "batch delete via variable deletes 1 document"

    echo "Test 56b: Verify variable batch delete (REST)..."
    RESULT=$(doc_exists "fde_write_test/w_ins4")
    assert_eq "$RESULT" "false" "REST: w_ins4 no longer exists"
fi

if should_run 57; then
    echo "Test 57: Batch delete empty list..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete_batch('fde_write_test', []::VARCHAR[]);")
    assert_eq "$RESULT" "0" \
        "batch delete with empty list returns 0"
fi

if should_run 58; then
    echo ""
    echo "--- firestore_delete_batch (collection group) ---"
    echo "Test 58: Batch delete via collection group prefix..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete_batch('~fde_write_child',
            ['fde_write_parent/p1/fde_write_child/cg1', 'fde_write_parent/p1/fde_write_child/cg2']);")
    assert_eq "$RESULT" "2" \
        "batch delete via ~collection_group deletes 2 documents"

    echo "Test 58b: Verify collection group batch delete (REST)..."
    RESULT=$(doc_exists "fde_write_parent/p1/fde_write_child/cg1")
    assert_eq "$RESULT" "false" "REST: cg1 no longer exists"
    RESULT=$(doc_exists "fde_write_parent/p1/fde_write_child/cg2")
    assert_eq "$RESULT" "false" "REST: cg2 no longer exists"
fi

if should_run 59; then
    echo "Test 59: Batch delete collection group with variable pattern..."
    RESULT=$(run_query "
SET VARIABLE cg_del_ids = (
    SELECT list(__document_id) FROM firestore_scan('~fde_write_child')
    WHERE __document_id LIKE '%/cg3'
);
SELECT * FROM firestore_delete_batch('~fde_write_child', getvariable('cg_del_ids'));
")
    assert_eq "$RESULT" "1" \
        "batch delete collection group via variable deletes 1 document"

    echo "Test 59b: Verify collection group variable batch delete (REST)..."
    RESULT=$(doc_exists "fde_write_parent/p2/fde_write_child/cg3")
    assert_eq "$RESULT" "false" "REST: cg3 no longer exists"
fi

if should_run 60; then
    echo "Test 60: Batch delete collection group empty list..."
    RESULT=$(run_query \
        "SELECT * FROM firestore_delete_batch('~fde_write_child', []::VARCHAR[]);")
    assert_eq "$RESULT" "0" \
        "batch delete ~collection_group with empty list returns 0"
fi

if should_run 61; then
    echo ""
    echo "--- End-to-end insert + verify + delete roundtrip ---"
    echo "Test 61: Insert-verify-delete roundtrip on collection group..."
    run_query "SELECT * FROM firestore_insert('fde_write_parent/p1/fde_write_child',
        (SELECT 'rt1' AS id, 'Roundtrip1' AS label, 10 AS priority), document_id := 'id');" > /dev/null
    run_query "SELECT * FROM firestore_insert('fde_write_parent/p2/fde_write_child',
        (SELECT 'rt2' AS id, 'Roundtrip2' AS label, 20 AS priority), document_id := 'id');" > /dev/null

    RESULT=$(doc_exists "fde_write_parent/p1/fde_write_child/rt1")
    assert_eq "$RESULT" "true" "REST: rt1 exists after insert"
    RESULT=$(get_field "fde_write_parent/p2/fde_write_child/rt2" "label")
    assert_eq "$RESULT" "Roundtrip2" "REST: rt2 has label=Roundtrip2"

    RESULT=$(run_query \
        "SELECT * FROM firestore_delete_batch('~fde_write_child',
            ['fde_write_parent/p1/fde_write_child/rt1', 'fde_write_parent/p2/fde_write_child/rt2']);")
    assert_eq "$RESULT" "2" \
        "collection group batch delete cleans up 2 roundtrip documents"

    RESULT=$(doc_exists "fde_write_parent/p1/fde_write_child/rt1")
    assert_eq "$RESULT" "false" "REST: rt1 deleted"
    RESULT=$(doc_exists "fde_write_parent/p2/fde_write_child/rt2")
    assert_eq "$RESULT" "false" "REST: rt2 deleted"
fi

# --- Cleanup ---

echo ""
echo "=== Cleaning up test data ==="

delete_doc() {
    curl -s -X DELETE -H "Authorization: Bearer $ACCESS_TOKEN" "${DATA_BASE}/$1" > /dev/null
}

# Cleanup read/scan test data (tests 1-25)
if any_in_range 1 25; then
    delete_doc "fde_ci_test/d1"
    delete_doc "fde_ci_test/d2"
    delete_doc "fde_ci_test/d3"
    delete_doc "fde_ci_test/d4"
    delete_doc "fde_ci_test/d5"
    delete_doc "fde_ci_parent/p1/fde_ci_child/c1"
    delete_doc "fde_ci_parent/p1/fde_ci_child/c2"
    delete_doc "fde_ci_parent/p2/fde_ci_child/c3"
fi

# Cleanup write test data (tests 26-61)
if any_in_range 26 61; then
    # Seeded docs (some may already be deleted by delete tests)
    delete_doc "fde_write_test/w1"
    delete_doc "fde_write_test/w2"
    delete_doc "fde_write_test/w3"
    delete_doc "fde_write_test/w4"
    delete_doc "fde_write_test/w5"
    # Inserted docs (may already be deleted by delete tests)
    delete_doc "fde_write_test/w_ins1"
    delete_doc "fde_write_test/w_ins2"
    delete_doc "fde_write_test/w_ins3"
    delete_doc "fde_write_test/w_ins4"
    # Collection group docs (seeded + inserted, may already be deleted)
    delete_doc "fde_write_parent/p1/fde_write_child/cg1"
    delete_doc "fde_write_parent/p1/fde_write_child/cg2"
    delete_doc "fde_write_parent/p2/fde_write_child/cg3"
    delete_doc "fde_write_parent/p1/fde_write_child/cg_ins1"
    delete_doc "fde_write_parent/p2/fde_write_child/cg_ins2"
    # Auto-ID insert doc: clean up any remaining unknown docs in the collection
    # (the auto-ID doc from Test 26 gets an unknown ID from Firestore)
    REMAINING_AUTO=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "${DATA_BASE}/fde_write_test?pageSize=50" \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
known = {'w1','w2','w3','w4','w5','w_ins1','w_ins2','w_ins3','w_ins4'}
for doc in data.get('documents', []):
    name = doc['name'].split('/')[-1]
    if name not in known:
        print(name)
" 2>/dev/null)
    for AUTO_ID in $REMAINING_AUTO; do
        delete_doc "fde_write_test/$AUTO_ID"
    done
fi

echo "Test data cleaned up. (Indexes left in place for idempotency.)"
echo ""
echo "=== All real Firestore tests passed! ==="
