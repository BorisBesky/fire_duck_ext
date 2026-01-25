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

echo ""
echo "=== All integration tests passed! ==="
