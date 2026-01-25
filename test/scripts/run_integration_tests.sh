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

# Test 1: Basic scan
echo "Test 1: Basic firestore_scan..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET test_secret (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key-for-emulator'
);

-- Note: The emulator doesn't require real authentication
-- but our extension still needs credentials configured

SELECT 'Extension loaded successfully' as status;
"

# Test 2: Verify functions exist
echo "Test 2: Verify all functions registered..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
SELECT function_name FROM duckdb_functions()
WHERE function_name LIKE 'firestore%'
ORDER BY function_name;
"

# Test 3: Secret management
echo "Test 3: Secret management..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET s1 (TYPE firestore, PROJECT_ID 'p1', API_KEY 'k1');
CREATE SECRET s2 (TYPE firestore, PROJECT_ID 'p2', API_KEY 'k2', DATABASE 'custom-db');
DROP SECRET s1;
DROP SECRET s2;

SELECT 'Secret management works' as status;
"

# Test 4: Read data from emulator
echo "Test 4: Reading data from emulator..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET emulator_secret (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Scan users collection
SELECT __document_id, name, status FROM firestore_scan('users') ORDER BY name;
"

# Test 4b: __document_id projection regression test
# This test ensures SELECT * returns all columns (not just __document_id) and correct row count
# Regression: COLUMN_IDENTIFIER_ROW_ID handling caused SELECT * to return only __document_id with 0 rows
echo "Test 4b: __document_id projection regression test..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET projection_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Test 1: SELECT * must return correct row count (5 users seeded)
-- This was broken: returned 0 rows when COLUMN_IDENTIFIER_ROW_ID was mishandled
SELECT 'Row count test:' as label;
SELECT CASE
    WHEN (SELECT COUNT(*) FROM firestore_scan('users')) = 5
    THEN 'PASS: SELECT * returned 5 rows'
    ELSE 'FAIL: SELECT * returned ' || (SELECT COUNT(*) FROM firestore_scan('users')) || ' rows, expected 5'
END as result;

-- Test 2: Selecting only __document_id should work and return data
SELECT 'Selecting only __document_id:' as label;
SELECT __document_id FROM firestore_scan('users') ORDER BY __document_id;

-- Test 3: Selecting __document_id with other specific columns should work
SELECT 'Selecting __document_id with name:' as label;
SELECT __document_id, name FROM firestore_scan('users') ORDER BY name;

-- Test 4: SELECT * should include __document_id as first column plus all data columns
-- This was broken: only showed __document_id column, missing name/age/status
SELECT 'Full SELECT * output (should show __document_id, name, age, status):' as label;
SELECT * FROM firestore_scan('users') ORDER BY name LIMIT 2;

-- Test 5: Verify we can access data columns (proves schema inference worked)
SELECT 'Data column access test:' as label;
SELECT name, age, status FROM firestore_scan('users') WHERE name = 'Alice';
"

# Verify the regression didn't occur - check that SELECT * returns more than just __document_id
echo "Verifying SELECT * returns multiple columns..."
COL_COUNT=$($DUCKDB -unsigned -csv -noheader -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
CREATE SECRET col_count_test (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
CREATE TEMP TABLE scan_result AS SELECT * FROM firestore_scan('users') LIMIT 1;
SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'scan_result';
" 2>&1 | tail -1)

if [ "$COL_COUNT" -le 1 ]; then
    echo "FAIL: SELECT * returned only $COL_COUNT column(s). Expected multiple columns (__document_id + data columns)."
    exit 1
else
    echo "PASS: SELECT * returned $COL_COUNT columns"
fi

# Verify row count is not 0
echo "Verifying row count is correct..."
ROW_COUNT=$($DUCKDB -unsigned -csv -noheader -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
CREATE SECRET row_count_test (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
SELECT COUNT(*) FROM firestore_scan('users');
" 2>&1 | tail -1)

if [ "$ROW_COUNT" -eq 0 ]; then
    echo "FAIL: SELECT * returned 0 rows. Expected 5 rows."
    exit 1
else
    echo "PASS: SELECT * returned $ROW_COUNT rows"
fi

# Test 5: DuckDB-side filtering pattern (the key use case!)
echo "Test 5: DuckDB-side filtering for batch operations..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET filter_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- This is the pattern: filter in DuckDB, extract IDs, use in batch operation

-- Step 1: Find all pending users (filter in DuckDB)
SELECT 'Pending users:' as label;
SELECT __document_id, name, status
FROM firestore_scan('users')
WHERE status = 'pending';

-- Step 2: Extract document IDs into a list
SELECT 'Document IDs to update:' as label;
SELECT list(__document_id) as ids_to_update
FROM firestore_scan('users')
WHERE status = 'pending';

-- Step 3: Use with batch update (this would work with real credentials)
-- SELECT * FROM firestore_update_batch('users',
--     (SELECT list(__document_id) FROM firestore_scan('users') WHERE status = 'pending'),
--     'status', 'reviewed'
-- );
"

# Test 6: Nested collection scan
echo "Test 6: Nested collection scan..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET nested_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Scan nested orders for user1
SELECT __document_id, product, status
FROM firestore_scan('users/user1/orders')
ORDER BY product;
"

# Test 7: Collection group query
echo "Test 7: Collection group query (all orders across all users)..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET group_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Collection group query returns full path as __document_id
SELECT __document_id, product, status
FROM firestore_scan('~orders')
ORDER BY product;
"

# Test 8: Complex filtering with aggregation
echo "Test 8: Complex filtering with aggregation..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET agg_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Find users above average age
SELECT __document_id, name, age
FROM firestore_scan('users')
WHERE age > (SELECT AVG(age) FROM firestore_scan('users'));

-- Count by status
SELECT status, COUNT(*) as count
FROM firestore_scan('users')
GROUP BY status
ORDER BY status;
"

# Test 9: Single document update
echo "Test 9: Single document update..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET update_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Update a single document
SELECT * FROM firestore_update('users', 'user1', 'status', 'verified');

-- Verify update
SELECT __document_id, name, status
FROM firestore_scan('users')
WHERE __document_id = 'user1';
"

# Test 10: Batch update with DuckDB filtering
echo "Test 10: Batch update with DuckDB filtering..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET batch_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Step 1: Get IDs of pending users and store in variable
-- (DuckDB doesn't allow subqueries directly in table function arguments)
SET VARIABLE pending_ids = (SELECT list(__document_id) FROM firestore_scan('users') WHERE status = 'pending');
SELECT 'Pending user IDs:' as label, getvariable('pending_ids') as ids;

-- Step 2: Batch update all pending users to 'reviewed' status
SELECT * FROM firestore_update_batch('users', getvariable('pending_ids'), 'status', 'reviewed');

-- Step 3: Verify - should be no more pending users
SELECT COUNT(*) as pending_count
FROM firestore_scan('users')
WHERE status = 'pending';
"

# Test 11: Single document delete
echo "Test 11: Single document delete..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET delete_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Count before delete
SELECT 'Before delete:' as label, COUNT(*) as count FROM firestore_scan('products');

-- Delete a product
SELECT * FROM firestore_delete('products', 'prod2');

-- Count after delete
SELECT 'After delete:' as label, COUNT(*) as count FROM firestore_scan('products');
"

# Test 12: Batch delete with filtering
echo "Test 12: Batch delete with filtering..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET batch_delete_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Step 1: Get IDs of inactive users and store in variable
SET VARIABLE inactive_ids = (SELECT list(__document_id) FROM firestore_scan('users') WHERE status = 'inactive');
SELECT 'Inactive user IDs to delete:' as label, getvariable('inactive_ids') as ids;

-- Step 2: Delete all inactive users
SELECT * FROM firestore_delete_batch('users', getvariable('inactive_ids'));

-- Step 3: Verify
SELECT __document_id, name, status FROM firestore_scan('users') ORDER BY name;
"

# Test 13: Array operations - setup
echo "Test 13: Array operations setup..."
curl -s -X POST "http://${FIRESTORE_EMULATOR_HOST}/v1/projects/test-project/databases/(default)/documents/array_test?documentId=arr1" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":{"stringValue":"Array Test Doc"},"tags":{"arrayValue":{"values":[{"stringValue":"initial"},{"stringValue":"tag"}]}},"scores":{"arrayValue":{"values":[{"integerValue":"10"},{"integerValue":"20"}]}}}}' > /dev/null

$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET array_ops_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Verify initial state
SELECT 'Initial array state:' as label;
SELECT __document_id, name, tags, scores FROM firestore_scan('array_test');
"

# Test 14: Array append (allows duplicates)
echo "Test 14: Array append (allows duplicates)..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET array_append_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Append elements (including duplicate 'initial')
SELECT * FROM firestore_array_append('array_test', 'arr1', 'tags', ['new', 'initial']);

-- Verify duplicates were added
SELECT 'After append:' as label;
SELECT tags FROM firestore_scan('array_test');
"

# Verify append created duplicates
echo "Verifying array_append allows duplicates..."
TAG_COUNT=$($DUCKDB -unsigned -csv -noheader -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
CREATE SECRET append_verify (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
SELECT list_count(tags) FROM firestore_scan('array_test');
" 2>&1 | tail -1)

if [ "$TAG_COUNT" -lt 4 ]; then
    echo "FAIL: array_append should have created duplicates. Expected 4+ tags, got $TAG_COUNT"
    exit 1
else
    echo "PASS: array_append created duplicates ($TAG_COUNT tags)"
fi

# Test 15: Array union (no duplicates)
echo "Test 15: Array union (no duplicates)..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET array_union_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Union with existing and new elements
SELECT * FROM firestore_array_union('array_test', 'arr1', 'tags', ['premium', 'initial']);

-- Verify - 'initial' should not be added again, but 'premium' should
SELECT 'After union:' as label;
SELECT tags FROM firestore_scan('array_test');
"

# Test 16: Array remove
echo "Test 16: Array remove..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET array_remove_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Remove all instances of 'initial'
SELECT * FROM firestore_array_remove('array_test', 'arr1', 'tags', ['initial']);

-- Verify all 'initial' entries were removed
SELECT 'After remove:' as label;
SELECT tags FROM firestore_scan('array_test');
"

# Verify remove worked - 'initial' should be gone
echo "Verifying array_remove removed all instances..."
INITIAL_COUNT=$($DUCKDB -unsigned -csv -noheader -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
CREATE SECRET remove_verify (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
SELECT list_count(list_filter(tags, x -> x = 'initial')) FROM firestore_scan('array_test');
" 2>&1 | tail -1)

if [ "$INITIAL_COUNT" -ne 0 ]; then
    echo "FAIL: array_remove should have removed all 'initial' entries. Found $INITIAL_COUNT"
    exit 1
else
    echo "PASS: array_remove removed all 'initial' entries"
fi

# Test 17: Numeric array operations
echo "Test 17: Numeric array operations..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET numeric_array_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Append numeric values
SELECT * FROM firestore_array_append('array_test', 'arr1', 'scores', [30, 40]);

-- Verify and use list aggregations
SELECT 'Numeric array after append:' as label;
SELECT scores,
       list_aggregate(scores, 'sum') as total,
       list_aggregate(scores, 'avg') as average
FROM firestore_scan('array_test');
"

# Test 18: Array contains check with list_contains
echo "Test 18: Array contains filtering..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';

CREATE SECRET contains_test (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);

-- Check array membership
SELECT __document_id,
       list_contains(tags, 'premium') as has_premium,
       list_contains(tags, 'initial') as has_initial
FROM firestore_scan('array_test');
"

# Cleanup array test data
echo "Cleaning up array test data..."
$DUCKDB -unsigned -c "
LOAD 'build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension';
CREATE SECRET cleanup (TYPE firestore, PROJECT_ID 'test-project', API_KEY 'fake-key');
SELECT * FROM firestore_delete('array_test', 'arr1');
"

echo ""
echo "=== All integration tests passed! ==="
