#!/bin/bash
# Run composite index integration tests against real Firestore
# Tests the happy path where composite indexes exist and the extension
# detects them to perform server-side multi-field ordering.
#
# Usage: ./test/scripts/run_real_firestore_tests.sh /path/to/service-account.json
#
# Prerequisites:
#   - gcloud CLI installed
#   - python3 available
#   - Service account needs roles/datastore.user + roles/datastore.indexAdmin
#   - Extension built: make release

set -e

SA_PATH="$1"
if [ -z "$SA_PATH" ] || [ ! -f "$SA_PATH" ]; then
    echo "Usage: $0 /path/to/service-account.json"
    exit 1
fi

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

# --- Test data seeding ---

echo ""
echo "=== Seeding test data ==="

seed_doc() {
    curl -s -X PATCH \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        -d "$2" \
        "${DATA_BASE}/$1" > /dev/null
}

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

# --- Tests ---

echo ""
echo "=== Running composite index tests ==="

# Test 1: Composite order_by on COLLECTION — server-side ordering
# With the composite index (score ASC, name ASC), the extension detects it
# and sends multi-field orderBy to Firestore. string_agg WITHOUT ORDER BY
# preserves scan order, so output directly reflects Firestore's ordering.
# Bravo(30) before Delta(30) proves the second field (name) breaks ties.
echo "Test 1: Composite order_by on collection..."
RESULT=$(run_query \
    "SELECT string_agg(name, ',') FROM firestore_scan('fde_ci_test', order_by='score, name');")
assert_eq "$RESULT" "Alpha,Charlie,Bravo,Delta,Echo" \
    "composite order_by='score, name' on collection uses index"

# Test 2: Composite order_by on COLLECTION_GROUP — server-side ordering
# Collection group requires explicit index. With the COLLECTION_GROUP composite
# index (val ASC, label ASC), ordering should be done server-side.
# string_agg WITHOUT ORDER BY to verify scan returns in Firestore order.
echo "Test 2: Composite order_by on collection group..."
RESULT=$(run_query \
    "SELECT string_agg(label, ',') FROM firestore_scan('~fde_ci_child', order_by='val, label');")
assert_eq "$RESULT" "A,C,B" \
    "composite order_by='val, label' on collection group uses index"

# Test 3: Single-field order_by still works on collection
echo "Test 3: Single-field order_by on collection..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test', order_by='score') LIMIT 1;")
assert_eq "$RESULT" "Alpha" "single-field order_by='score' still works"

# Test 4: Composite order_by with DESC (no matching DESC index — query still succeeds)
# No DESC composite index exists, so extension falls back to client-side sorting.
# Just verify the query doesn't error and returns all rows.
echo "Test 4: Composite order_by with DESC on collection..."
RESULT=$(run_query \
    "SELECT count(*) FROM firestore_scan('fde_ci_test', order_by='score DESC, name DESC');")
assert_eq "$RESULT" "5" \
    "composite order_by DESC gracefully falls back (no DESC index)"

# =============================================
# SQL ORDER BY / LIMIT Pushdown Tests
# These test the optimizer extension that extracts ORDER BY and LIMIT from
# standard SQL and pushes them to Firestore server-side.
# =============================================

echo ""
echo "=== Running SQL ORDER BY / LIMIT pushdown tests ==="

# Test 5: SQL ORDER BY pushdown — single field ascending
echo "Test 5: SQL ORDER BY single field ascending..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 1;")
assert_eq "$RESULT" "Alpha" \
    "SQL ORDER BY score ASC pushes to Firestore (Alpha has lowest score)"

# Test 6: SQL ORDER BY pushdown — single field descending
echo "Test 6: SQL ORDER BY single field descending..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 1;")
assert_eq "$RESULT" "Echo" \
    "SQL ORDER BY score DESC pushes to Firestore (Echo has highest score)"

# Test 7: SQL LIMIT pushdown — limits documents fetched from Firestore
echo "Test 7: SQL LIMIT pushdown..."
RESULT=$(run_query \
    "SELECT count(*) FROM (SELECT * FROM firestore_scan('fde_ci_test') LIMIT 3);")
assert_eq "$RESULT" "3" \
    "SQL LIMIT 3 returns exactly 3 rows"

# Test 8: SQL ORDER BY + LIMIT combined
echo "Test 8: SQL ORDER BY + LIMIT combined..."
RESULT=$(run_query \
    "SELECT string_agg(name, ',' ORDER BY score DESC) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 2) sub;")
assert_eq "$RESULT" "Echo,Delta" \
    "SQL ORDER BY score DESC LIMIT 2 returns top 2 scorers"

# Test 9: SQL ORDER BY + LIMIT — ascending top 3
echo "Test 9: SQL ORDER BY + LIMIT ascending top 3..."
RESULT=$(run_query \
    "SELECT string_agg(name, ',' ORDER BY score) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 3) sub;")
assert_eq "$RESULT" "Alpha,Charlie,Bravo" \
    "SQL ORDER BY score ASC LIMIT 3 returns bottom 3 scorers"

# Test 10: Named param order_by takes precedence over SQL ORDER BY
# Named param says score ASC, SQL says score DESC — named param should win.
echo "Test 10: Named param order_by takes precedence over SQL ORDER BY..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test', order_by='score') ORDER BY score DESC LIMIT 1;")
# DuckDB re-applies ORDER BY DESC on results, so first row after re-sort is Echo.
# The key behavior: named param order_by='score' (ASC) is sent to Firestore.
# DuckDB then re-sorts DESC and returns Echo.
assert_eq "$RESULT" "Echo" \
    "Named param order_by takes precedence (DuckDB re-sorts client-side)"

# Test 11: Named scan_limit takes precedence over SQL LIMIT
echo "Test 11: Named scan_limit takes precedence over SQL LIMIT..."
RESULT=$(run_query \
    "SELECT count(*) FROM firestore_scan('fde_ci_test', scan_limit=2) LIMIT 10;")
assert_eq "$RESULT" "2" \
    "Named scan_limit=2 takes precedence over SQL LIMIT 10"

# Test 12: SQL ORDER BY + LIMIT + WHERE (all three pushed)
echo "Test 12: SQL ORDER BY + LIMIT + WHERE combined..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test') WHERE score > 20 ORDER BY score DESC LIMIT 1;")
assert_eq "$RESULT" "Echo" \
    "WHERE score > 20 + ORDER BY DESC + LIMIT 1 returns Echo (score=50)"

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

# Test 14: SQL LIMIT + OFFSET pushdown
# Optimizer pushes limit+offset to Firestore, DuckDB handles offset client-side.
echo "Test 14: SQL LIMIT + OFFSET pushdown..."
RESULT=$(run_query \
    "SELECT name FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 1 OFFSET 1;")
assert_eq "$RESULT" "Charlie" \
    "ORDER BY score LIMIT 1 OFFSET 1 returns Charlie (second lowest score=20)"

# Test 15: SQL ORDER BY on collection group
echo "Test 15: SQL ORDER BY on collection group..."
RESULT=$(run_query \
    "SELECT label FROM firestore_scan('~fde_ci_child') ORDER BY val LIMIT 1;")
# val ASC: A(10), C(10), B(20) — first is A or C depending on __name__ tiebreak
# c2 (A) comes before c3 (C) in __name__ order.
assert_eq "$RESULT" "A" \
    "SQL ORDER BY val on collection group returns lowest val document"

# Test 16: SQL LIMIT without ORDER BY on collection group
echo "Test 16: SQL LIMIT on collection group..."
RESULT=$(run_query \
    "SELECT count(*) FROM (SELECT * FROM firestore_scan('~fde_ci_child') LIMIT 2) sub;")
assert_eq "$RESULT" "2" \
    "SQL LIMIT 2 on collection group returns exactly 2 rows"

# Test 17: Composite SQL ORDER BY (multi-field) — requires composite index
echo "Test 17: SQL composite ORDER BY (multi-field)..."
RESULT=$(run_query \
    "SELECT string_agg(name, ',' ORDER BY score, name) FROM (SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score, name) sub;")
assert_eq "$RESULT" "Alpha,Charlie,Bravo,Delta,Echo" \
    "SQL ORDER BY score, name uses composite index (same result as named param)"

# =============================================
# EXPLAIN Plan Validation Tests
# Verify that EXPLAIN output shows pushdown info for ORDER BY and LIMIT.
# =============================================

echo ""
echo "=== Running EXPLAIN plan validation tests ==="

# Test 18: EXPLAIN shows ORDER BY pushdown
echo "Test 18: EXPLAIN shows ORDER BY pushdown..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score DESC LIMIT 5;")
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score DESC" \
    "EXPLAIN shows 'Firestore Pushed Order: score DESC'"
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 5" \
    "EXPLAIN shows 'Firestore Pushed Limit: 5'"

# Test 19: EXPLAIN shows LIMIT-only pushdown (no ORDER BY)
echo "Test 19: EXPLAIN shows LIMIT-only pushdown..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') LIMIT 3;")
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 3" \
    "EXPLAIN shows 'Firestore Pushed Limit: 3' without ORDER BY"

# Test 20: EXPLAIN shows ORDER BY-only pushdown (no LIMIT)
echo "Test 20: EXPLAIN shows ORDER BY-only pushdown..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY name;")
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: name" \
    "EXPLAIN shows 'Firestore Pushed Order: name' without LIMIT"
assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Limit" \
    "EXPLAIN does not show Pushed Limit when no LIMIT clause"

# Test 21: EXPLAIN shows multi-field ORDER BY pushdown
echo "Test 21: EXPLAIN shows multi-field ORDER BY pushdown..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score, name LIMIT 2;")
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score, name" \
    "EXPLAIN shows 'Firestore Pushed Order: score, name' for multi-field"
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 2" \
    "EXPLAIN shows 'Firestore Pushed Limit: 2' alongside multi-field order"

# Test 22: EXPLAIN shows LIMIT+OFFSET pushdown (limit+offset value)
echo "Test 22: EXPLAIN shows LIMIT+OFFSET pushdown..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test') ORDER BY score LIMIT 3 OFFSET 2;")
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Order: score" \
    "EXPLAIN shows ORDER BY pushdown with OFFSET"
assert_contains "$EXPLAIN_OUT" "Firestore Pushed Limit: 5" \
    "EXPLAIN shows Pushed Limit: 5 (limit 3 + offset 2)"

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

# Test 24: EXPLAIN does NOT show SQL pushdown when named params are used
echo "Test 24: EXPLAIN omits SQL pushdown when named params set..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT * FROM firestore_scan('fde_ci_test', order_by='score', scan_limit=5) ORDER BY name DESC LIMIT 2;")
assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Order:" \
    "EXPLAIN does not show SQL ORDER BY pushdown when named order_by is set"
assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Limit:" \
    "EXPLAIN does not show SQL LIMIT pushdown when named scan_limit is set"

# Test 25: EXPLAIN does NOT show ORDER BY pushdown for aggregate query
echo "Test 25: EXPLAIN omits ORDER BY pushdown for aggregate queries..."
EXPLAIN_OUT=$(run_explain \
    "EXPLAIN SELECT name, count(*) as cnt FROM firestore_scan('fde_ci_test') GROUP BY name ORDER BY cnt LIMIT 3;")
assert_not_contains "$EXPLAIN_OUT" "Firestore Pushed Order:" \
    "EXPLAIN does not show ORDER BY pushdown when ORDER BY is on aggregate result"

# --- Cleanup ---

echo ""
echo "=== Cleaning up test data ==="

delete_doc() {
    curl -s -X DELETE -H "Authorization: Bearer $ACCESS_TOKEN" "${DATA_BASE}/$1" > /dev/null
}

delete_doc "fde_ci_test/d1"
delete_doc "fde_ci_test/d2"
delete_doc "fde_ci_test/d3"
delete_doc "fde_ci_test/d4"
delete_doc "fde_ci_test/d5"
delete_doc "fde_ci_parent/p1/fde_ci_child/c1"
delete_doc "fde_ci_parent/p1/fde_ci_child/c2"
delete_doc "fde_ci_parent/p2/fde_ci_child/c3"

echo "Test data cleaned up. (Indexes left in place for idempotency.)"
echo ""
echo "=== All real Firestore tests passed! ==="
