#!/bin/bash
# Run FireDuckExt integration tests against Firebase Emulator
# This script is called by firebase emulators:exec

set -e

# Find duckdb binary - prefer system installation over local build
if command -v duckdb &> /dev/null; then
    DUCKDB="duckdb"
elif [ -x "./build/release/duckdb" ]; then
    DUCKDB="./build/release/duckdb"
else
    echo "Error: duckdb not found. Install DuckDB or build from source."
    exit 1
fi

EXT_PATH="build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension"

# Helper: run a DuckDB query and return the trimmed last line of output
run_query() {
    $DUCKDB -unsigned -csv -noheader -c "
LOAD '${EXT_PATH}';
CREATE SECRET __q (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
$1
" 2>&1 | tail -1 | tr -d '[:space:]"'
}

# Helper: assert equality
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

# Helper: assert numeric comparison
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

echo "=== FireDuckExt Integration Tests ==="
echo "FIRESTORE_EMULATOR_HOST: $FIRESTORE_EMULATOR_HOST"

# Wait for emulator to be fully ready
sleep 3

# Seed test data
echo "Seeding test data..."

# Users collection
curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users?documentId=user1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Alice"}, "age": {"integerValue": "30"}, "status": {"stringValue": "active"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users?documentId=user2" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Bob"}, "age": {"integerValue": "25"}, "status": {"stringValue": "pending"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users?documentId=user3" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Charlie"}, "age": {"integerValue": "35"}, "status": {"stringValue": "inactive"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users?documentId=user4" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Diana"}, "age": {"integerValue": "28"}, "status": {"stringValue": "pending"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users?documentId=user5" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Eve"}, "age": {"integerValue": "40"}, "status": {"stringValue": "active"}}}' > /dev/null

# Products collection
curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/products?documentId=prod1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Widget"}, "price": {"doubleValue": 9.99}, "inStock": {"booleanValue": true}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/products?documentId=prod2" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Gadget"}, "price": {"doubleValue": 19.99}, "inStock": {"booleanValue": false}}}' > /dev/null

# Nested collections - users/{userId}/orders
curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users/user1/orders?documentId=order1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"product": {"stringValue": "Widget"}, "quantity": {"integerValue": "2"}, "status": {"stringValue": "shipped"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users/user1/orders?documentId=order2" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"product": {"stringValue": "Gadget"}, "quantity": {"integerValue": "1"}, "status": {"stringValue": "pending"}}}' > /dev/null

curl -s -X POST "http://$FIRESTORE_EMULATOR_HOST/v1/projects/test-project/databases/(default)/documents/users/user2/orders?documentId=order1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"product": {"stringValue": "Widget"}, "quantity": {"integerValue": "5"}, "status": {"stringValue": "pending"}}}' > /dev/null

echo "Test data seeded."

# Run integration tests
echo ""
echo "Running integration tests..."

# Test 1: Basic scan - extension loads
echo "Test 1: Basic firestore_scan..."
LOAD_RESULT=$(run_query "SELECT 'ok';")
assert_eq "$LOAD_RESULT" "ok" "Extension loads successfully"

# Test 2: Verify functions exist
echo "Test 2: Verify all functions registered..."
FUNC_COUNT=$(run_query "SELECT count(*) FROM duckdb_functions() WHERE function_name LIKE 'firestore%';")
assert_ge "$FUNC_COUNT" 9 "At least 9 firestore functions registered (got $FUNC_COUNT)"

# Test 3: Secret management
echo "Test 3: Secret management..."
SECRET_RESULT=$(run_query "
DROP SECRET __q;
CREATE SECRET s1 (TYPE firestore, PROJECT_ID 'p1', API_KEY 'k1');
CREATE SECRET s2 (TYPE firestore, PROJECT_ID 'p2', API_KEY 'k2', DATABASE 'custom-db');
DROP SECRET s1;
DROP SECRET s2;
SELECT 'ok';
")
assert_eq "$SECRET_RESULT" "ok" "Secret create/drop works"

# Test 4: Read data from emulator
echo "Test 4: Reading data from emulator..."
USER_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('users');")
assert_eq "$USER_COUNT" "5" "Read 5 users from emulator"

ALICE_AGE=$(run_query "SELECT age FROM firestore_scan('users') WHERE name = 'Alice';")
assert_eq "$ALICE_AGE" "30" "Alice has age 30"

# Test 4b: __document_id projection regression test
echo "Test 4b: __document_id projection regression test..."
COL_COUNT=$(run_query "
CREATE TEMP TABLE scan_result AS SELECT * FROM firestore_scan('users') LIMIT 1;
SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'scan_result';
")
assert_ge "$COL_COUNT" 2 "SELECT * returns multiple columns ($COL_COUNT)"

ROW_COUNT=$(run_query "SELECT COUNT(*) FROM firestore_scan('users');")
assert_eq "$ROW_COUNT" "5" "SELECT * returns 5 rows"

# Test 5: DuckDB-side filtering
echo "Test 5: DuckDB-side filtering..."
PENDING_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('users') WHERE status = 'pending';")
assert_eq "$PENDING_COUNT" "2" "Found 2 pending users"

# Test 6: Nested collection scan
echo "Test 6: Nested collection scan..."
ORDER_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('users/user1/orders');")
assert_eq "$ORDER_COUNT" "2" "User1 has 2 orders"

ORDER_PRODUCT=$(run_query "SELECT product FROM firestore_scan('users/user1/orders') WHERE __document_id = 'order1';")
assert_eq "$ORDER_PRODUCT" "Widget" "Order1 product is Widget"

# Test 7: Collection group query
echo "Test 7: Collection group query (all orders across all users)..."
ALL_ORDERS=$(run_query "SELECT count(*) FROM firestore_scan('~orders');")
assert_eq "$ALL_ORDERS" "3" "Collection group returns 3 total orders"

# Test 8: Complex filtering with aggregation
echo "Test 8: Complex filtering with aggregation..."
ABOVE_AVG=$(run_query "
SELECT count(*) FROM firestore_scan('users')
WHERE age > (SELECT AVG(age) FROM firestore_scan('users'));
")
assert_eq "$ABOVE_AVG" "2" "2 users above average age"

ACTIVE_COUNT=$(run_query "
SELECT count FROM (SELECT status, COUNT(*) as count FROM firestore_scan('users') GROUP BY status) WHERE status = 'active';
")
assert_eq "$ACTIVE_COUNT" "2" "2 active users"

# Test 9: Single document update
echo "Test 9: Single document update..."
run_query "SELECT * FROM firestore_update('users', 'user1', 'status', 'verified');" > /dev/null

UPDATED_STATUS=$(run_query "SELECT status FROM firestore_scan('users') WHERE __document_id = 'user1';")
assert_eq "$UPDATED_STATUS" "verified" "User1 status updated to verified"

# Test 10: Batch update with DuckDB filtering
echo "Test 10: Batch update with DuckDB filtering..."
run_query "
SET VARIABLE pending_ids = (SELECT list(__document_id) FROM firestore_scan('users') WHERE status = 'pending');
SELECT * FROM firestore_update_batch('users', getvariable('pending_ids'), 'status', 'reviewed');
" > /dev/null

REMAINING_PENDING=$(run_query "SELECT count(*) FROM firestore_scan('users') WHERE status = 'pending';")
assert_eq "$REMAINING_PENDING" "0" "No pending users remain after batch update"

REVIEWED_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('users') WHERE status = 'reviewed';")
assert_eq "$REVIEWED_COUNT" "2" "2 users now reviewed"

# Test 11: Single document delete
echo "Test 11: Single document delete..."
BEFORE_DELETE=$(run_query "SELECT count(*) FROM firestore_scan('products');")
assert_eq "$BEFORE_DELETE" "2" "2 products before delete"

run_query "SELECT * FROM firestore_delete('products', 'prod2');" > /dev/null

AFTER_DELETE=$(run_query "SELECT count(*) FROM firestore_scan('products');")
assert_eq "$AFTER_DELETE" "1" "1 product after delete"

# Test 12: Batch delete with filtering
echo "Test 12: Batch delete with filtering..."
run_query "
SET VARIABLE inactive_ids = (SELECT list(__document_id) FROM firestore_scan('users') WHERE status = 'inactive');
SELECT * FROM firestore_delete_batch('users', getvariable('inactive_ids'));
" > /dev/null

INACTIVE_REMAINING=$(run_query "SELECT count(*) FROM firestore_scan('users') WHERE status = 'inactive';")
assert_eq "$INACTIVE_REMAINING" "0" "No inactive users remain after batch delete"

TOTAL_USERS=$(run_query "SELECT count(*) FROM firestore_scan('users');")
assert_eq "$TOTAL_USERS" "4" "4 users remain after deleting 1 inactive"

# Test 13: Array operations - setup
echo "Test 13: Array operations setup..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/array_test?documentId=arr1" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":{"stringValue":"Array Test Doc"},"tags":{"arrayValue":{"values":[{"stringValue":"initial"},{"stringValue":"tag"}]}},"scores":{"arrayValue":{"values":[{"integerValue":"10"},{"integerValue":"20"}]}}}}' > /dev/null

INIT_TAG_COUNT=$(run_query "SELECT list_count(tags) FROM firestore_scan('array_test');")
assert_eq "$INIT_TAG_COUNT" "2" "Initial array has 2 tags"

# Test 14: Array append (allows duplicates)
echo "Test 14: Array append (allows duplicates)..."
run_query "SELECT * FROM firestore_array_append('array_test', 'arr1', 'tags', ['new', 'initial']);" > /dev/null

TAG_COUNT=$(run_query "SELECT list_count(tags) FROM firestore_scan('array_test');")
assert_eq "$TAG_COUNT" "4" "Array has 4 tags after append (duplicates allowed)"

INITIAL_OCCURRENCES=$(run_query "SELECT list_count(list_filter(tags, x -> x = 'initial')) FROM firestore_scan('array_test');")
assert_eq "$INITIAL_OCCURRENCES" "2" "'initial' appears twice (duplicate created)"

# Test 15: Array union (no duplicates)
echo "Test 15: Array union (no duplicates)..."
TAG_COUNT_BEFORE=$(run_query "SELECT list_count(tags) FROM firestore_scan('array_test');")
run_query "SELECT * FROM firestore_array_union('array_test', 'arr1', 'tags', ['premium', 'initial']);" > /dev/null

TAG_COUNT_AFTER=$(run_query "SELECT list_count(tags) FROM firestore_scan('array_test');")
HAS_PREMIUM=$(run_query "SELECT list_contains(tags, 'premium') FROM firestore_scan('array_test');")
assert_eq "$HAS_PREMIUM" "true" "Union added 'premium'"

# Union should add only 'premium' (1 new element), not 'initial' (already exists)
EXPECTED_AFTER=$((TAG_COUNT_BEFORE + 1))
assert_eq "$TAG_COUNT_AFTER" "$EXPECTED_AFTER" "Union added exactly 1 new element (premium)"

# Test 16: Array remove
echo "Test 16: Array remove..."
run_query "SELECT * FROM firestore_array_remove('array_test', 'arr1', 'tags', ['initial']);" > /dev/null

INITIAL_COUNT=$(run_query "SELECT list_count(list_filter(tags, x -> x = 'initial')) FROM firestore_scan('array_test');")
assert_eq "$INITIAL_COUNT" "0" "All 'initial' entries removed"

HAS_PREMIUM=$(run_query "SELECT list_contains(tags, 'premium') FROM firestore_scan('array_test');")
assert_eq "$HAS_PREMIUM" "true" "'premium' still present after removing 'initial'"

HAS_TAG=$(run_query "SELECT list_contains(tags, 'tag') FROM firestore_scan('array_test');")
assert_eq "$HAS_TAG" "true" "'tag' still present after removing 'initial'"

# Test 17: Numeric array operations
echo "Test 17: Numeric array operations..."
run_query "SELECT * FROM firestore_array_append('array_test', 'arr1', 'scores', [30, 40]);" > /dev/null

SCORE_COUNT=$(run_query "SELECT list_count(scores) FROM firestore_scan('array_test');")
assert_eq "$SCORE_COUNT" "4" "Scores array has 4 elements after append"

SCORE_SUM=$(run_query "SELECT list_aggregate(scores, 'sum') FROM firestore_scan('array_test');")
assert_eq "$SCORE_SUM" "100" "Sum of scores is 100"

# Test 18: Array contains check with list_contains
echo "Test 18: Array contains filtering..."
HAS_PREMIUM=$(run_query "SELECT list_contains(tags, 'premium') FROM firestore_scan('array_test');")
assert_eq "$HAS_PREMIUM" "true" "list_contains finds 'premium'"

HAS_INITIAL=$(run_query "SELECT list_contains(tags, 'initial') FROM firestore_scan('array_test');")
assert_eq "$HAS_INITIAL" "false" "list_contains correctly reports 'initial' missing"

# Cleanup array test data
echo "Cleaning up array test data..."
run_query "SELECT * FROM firestore_delete('array_test', 'arr1');" > /dev/null

# Test 19: GeoPoint type (reading)
echo "Test 19: GeoPoint type support..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/locations?documentId=loc1" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":{"stringValue":"San Francisco"},"coordinates":{"geoPointValue":{"latitude":37.7749,"longitude":-122.4194}}}}' > /dev/null

GEO_TYPE=$(run_query "SELECT column_type FROM (DESCRIBE SELECT coordinates FROM firestore_scan('locations'));")
assert_eq "$GEO_TYPE" "STRUCT(latitudeDOUBLE,longitudeDOUBLE)" "GeoPoint is STRUCT type"

GEO_LAT=$(run_query "SELECT round(coordinates.latitude, 4) FROM firestore_scan('locations');")
assert_eq "$GEO_LAT" "37.7749" "GeoPoint latitude read correctly"

GEO_LNG=$(run_query "SELECT round(coordinates.longitude, 4) FROM firestore_scan('locations');")
assert_eq "$GEO_LNG" "-122.4194" "GeoPoint longitude read correctly"

# Test 20: GeoPoint type (writing)
echo "Test 20: GeoPoint update..."
run_query "
SELECT * FROM firestore_update('locations', 'loc1',
    'coordinates', {'latitude': 40.7128, 'longitude': -74.0060}::STRUCT(latitude DOUBLE, longitude DOUBLE),
    'name', 'New York'
);
" > /dev/null

LAT_VALUE=$(run_query "SELECT round(coordinates.latitude, 4) FROM firestore_scan('locations');")
assert_eq "$LAT_VALUE" "40.7128" "GeoPoint latitude updated to 40.7128"

LNG_VALUE=$(run_query "SELECT round(coordinates.longitude, 3) FROM firestore_scan('locations');")
assert_eq "$LNG_VALUE" "-74.006" "GeoPoint longitude updated to -74.006"

NAME_VALUE=$(run_query "SELECT name FROM firestore_scan('locations');")
assert_eq "$NAME_VALUE" "NewYork" "Name updated to New York"

# Test 21: Reference type (reading)
echo "Test 21: Reference type support..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/posts?documentId=post1" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"title":{"stringValue":"My Post"},"author":{"referenceValue":"projects/test-project/databases/(default)/documents/users/user1"}}}' > /dev/null

REF_TYPE=$(run_query "SELECT column_type FROM (DESCRIBE SELECT author FROM firestore_scan('posts'));")
assert_eq "$REF_TYPE" "VARCHAR" "Reference stored as VARCHAR"

AUTHOR_ID=$(run_query "SELECT split_part(author, '/', -1) FROM firestore_scan('posts');")
assert_eq "$AUTHOR_ID" "user1" "Reference points to user1"

TITLE=$(run_query "SELECT title FROM firestore_scan('posts');")
assert_eq "$TITLE" "MyPost" "Title is My Post"

# Test 22: Bytes type (reading)
echo "Test 22: Bytes type support..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/files?documentId=file1" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"filename":{"stringValue":"test.txt"},"content":{"bytesValue":"SGVsbG8gRmlyZXN0b3JlIQ=="}}}' > /dev/null

BYTES_TYPE=$(run_query "SELECT column_type FROM (DESCRIBE SELECT content FROM firestore_scan('files'));")
assert_eq "$BYTES_TYPE" "BLOB" "Bytes stored as BLOB"

DECODED=$(run_query "SELECT content::VARCHAR FROM firestore_scan('files');")
assert_eq "$DECODED" "HelloFirestore!" "Bytes decoded from base64 correctly"

# Test 23: Reference type update
echo "Test 23: Reference type update..."
run_query "
SELECT * FROM firestore_update('posts', 'post1',
    'author', 'projects/test-project/databases/(default)/documents/users/user2',
    'title', 'Updated Post'
);
" > /dev/null

AUTHOR_ID=$(run_query "SELECT split_part(author, '/', -1) FROM firestore_scan('posts');")
assert_eq "$AUTHOR_ID" "user2" "Reference updated to user2"

TITLE=$(run_query "SELECT title FROM firestore_scan('posts');")
assert_eq "$TITLE" "UpdatedPost" "Title updated"

# Test 24: Bytes type update
echo "Test 24: Bytes type update..."
run_query "
SELECT * FROM firestore_update('files', 'file1',
    'content', 'New binary data!'::BLOB,
    'filename', 'updated.txt'
);
" > /dev/null

UPDATED_CONTENT=$(run_query "SELECT content::VARCHAR FROM firestore_scan('files');")
assert_eq "$UPDATED_CONTENT" "Newbinarydata!" "Bytes updated correctly (round-trip)"

UPDATED_NAME=$(run_query "SELECT filename FROM firestore_scan('files');")
assert_eq "$UPDATED_NAME" "updated.txt" "Filename updated"

# Test 25: Multiple GeoPoint updates (batch scenario)
echo "Test 25: Multiple location updates..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/locations?documentId=loc2" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":{"stringValue":"Los Angeles"},"coordinates":{"geoPointValue":{"latitude":34.0522,"longitude":-118.2437}}}}' > /dev/null

curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/locations?documentId=loc3" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":{"stringValue":"Chicago"},"coordinates":{"geoPointValue":{"latitude":41.8781,"longitude":-87.6298}}}}' > /dev/null

LOC_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('locations');")
assert_eq "$LOC_COUNT" "3" "3 locations seeded"

run_query "
SET VARIABLE loc_ids = (SELECT list(__document_id) FROM firestore_scan('locations'));
SELECT * FROM firestore_update_batch('locations', getvariable('loc_ids'), 'verified', true);
" > /dev/null

VERIFIED_COUNT=$(run_query "SELECT count(*) FROM firestore_scan('locations') WHERE verified = true;")
assert_eq "$VERIFIED_COUNT" "3" "All 3 locations verified after batch update"

# Test 26: GeoPoint calculations (bounding box and distance)
echo "Test 26: GeoPoint calculations..."

# Los Angeles (34.0522) is NOT in the 35-45 bounding box; New York (40.7128) and Chicago (41.8781) ARE
BBOX_COUNT=$(run_query "
SELECT count(*) FROM firestore_scan('locations')
WHERE coordinates.latitude BETWEEN 35 AND 45
  AND coordinates.longitude BETWEEN -125 AND -70;
")
assert_eq "$BBOX_COUNT" "2" "Bounding box (35-45 lat) contains 2 locations"

# Los Angeles should be closest to Seattle (47.6062, -122.3321) by simple distance
CLOSEST=$(run_query "
SELECT __document_id FROM firestore_scan('locations')
ORDER BY sqrt(power(coordinates.latitude - 47.6062, 2) + power(coordinates.longitude - (-122.3321), 2))
LIMIT 1;
")
assert_eq "$CLOSEST" "loc2" "Los Angeles is closest to Seattle by Euclidean distance"

# Cleanup special types test data
echo "Cleaning up special types test data..."
run_query "
SELECT * FROM firestore_delete('locations', 'loc1');
SELECT * FROM firestore_delete('locations', 'loc2');
SELECT * FROM firestore_delete('locations', 'loc3');
SELECT * FROM firestore_delete('posts', 'post1');
SELECT * FROM firestore_delete('files', 'file1');
" > /dev/null

echo ""
echo "=== All integration tests passed! ==="
