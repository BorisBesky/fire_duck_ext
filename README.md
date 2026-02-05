# FireDuckExt - DuckDB Extension for Google Cloud Firestore

Query Google Cloud Firestore directly from DuckDB using SQL.

## Features

- **Read data** from Firestore collections with `firestore_scan()`
- **Insert data** from subqueries, CSVs, or tables with `firestore_insert()`
- **Update and delete** with `firestore_update()`, `firestore_delete()`
- **Batch operations** for bulk updates and deletes
- **Array transforms** with `firestore_array_union()`, `firestore_array_remove()`, `firestore_array_append()`
- **Collection group queries** for querying across nested collections
- **Filter pushdown** sends supported WHERE clauses to Firestore for faster queries
- **Vector embedding support** with Firestore vector fields mapped to `ARRAY(DOUBLE, N)`
- **DuckDB secret management** for secure credential storage

## Quick Start

```sql
-- Load the extension
LOAD fire_duck_ext;

-- Configure credentials
CREATE SECRET my_firestore (
    TYPE firestore,
    PROJECT_ID 'my-gcp-project',
    SERVICE_ACCOUNT_JSON '/path/to/credentials.json'
);

-- Query a collection
SELECT * FROM firestore_scan('users');

-- Filter with SQL
SELECT __document_id, name, email
FROM firestore_scan('users')
WHERE status = 'active';

-- Insert documents from a subquery
call firestore_insert('users', (
    SELECT 'Alice' AS name, 30 AS age
));

-- Insert with explicit document IDs
call firestore_insert('users',
    (SELECT 'alice123' AS id, 'Alice' AS name, 30 AS age),
    document_id := 'id');

-- Update documents
call firestore_update('users', 'user123', 'status', 'verified');

-- Batch update with DuckDB filtering
SET VARIABLE ids = (
    SELECT list(__document_id)
    FROM firestore_scan('users')
    WHERE status = 'pending'
);
call firestore_update_batch('users', getvariable('ids'), 'status', 'reviewed');
```

## Authentication

### Service Account (Recommended for production)
```sql
CREATE SECRET prod_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);
```

### API Key (For development/testing)
```sql
CREATE SECRET dev_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'your-api-key'
);
```
### Use environment variable
```sql
# Set the path to your service account JSON file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Then run DuckDB - no secret creation needed!
duckdb
```

### Firebase Emulator
```sql
-- Set environment variable first
-- export FIRESTORE_EMULATOR_HOST=localhost:8080

CREATE SECRET emulator (
    TYPE firestore,
    PROJECT_ID 'test-project',
    API_KEY 'fake-key'
);
```

## Functions

| Function | Description |
|----------|-------------|
| `firestore_scan('collection')` | Read all documents from a collection |
| `firestore_scan('~collection')` | Collection group query (all subcollections) |
| `firestore_insert('collection', (SELECT ...), document_id := 'col')` | Insert documents from a subquery |
| `firestore_update('collection', 'doc_id', 'field1', value1, ...)` | Update fields on a single document |
| `firestore_delete('collection', 'doc_id')` | Delete a document |
| `firestore_update_batch('collection', ['id1', ...], 'field1', value1, ...)` | Batch update |
| `firestore_delete_batch('collection', ['id1', ...])` | Batch delete |
| `firestore_array_union('collection', 'doc_id', 'field', ['v1', ...])` | Add to array (no duplicates) |
| `firestore_array_remove('collection', 'doc_id', 'field', ['v1', ...])` | Remove from array |
| `firestore_array_append('collection', 'doc_id', 'field', ['v1', ...])` | Append to array |

## Insert

`firestore_insert` takes a collection name and a subquery, and inserts each row as a Firestore document.

```sql
-- Auto-generated document IDs
call firestore_insert('users', (
    SELECT name, age FROM read_csv('new_users.csv')
));

-- Explicit document IDs from a column
call firestore_insert('users',
    (SELECT user_id, name, age FROM read_csv('new_users.csv')),
    document_id := 'user_id');

-- Insert from a DuckDB table
call firestore_insert('employees',
    (SELECT * FROM employee_staging),
    document_id := 'emp_id');

-- Insert into nested collections
call firestore_insert('users/user1/notes', (
    SELECT 'note1' AS id, 'Remember to buy milk' AS content
), document_id := 'id');
```

The `document_id` parameter specifies which column to use as the Firestore document ID. That column is excluded from the document fields. If omitted, Firestore auto-generates IDs.

## Type Mapping

| Firestore Type | DuckDB Type |
|----------------|-------------|
| string | VARCHAR |
| integer | BIGINT |
| double | DOUBLE |
| boolean | BOOLEAN |
| timestamp | TIMESTAMP |
| array | LIST |
| map | VARCHAR (JSON string) |
| vector | ARRAY(DOUBLE, N) |
| null | NULL |
| geoPoint | STRUCT(latitude DOUBLE, longitude DOUBLE) |
| reference | VARCHAR |
| bytes | BLOB |

### Vector Embeddings

Firestore vector fields (created via `FieldValue.vector()`) are mapped to DuckDB's fixed-size `ARRAY(DOUBLE, N)` type, where N is the vector dimension inferred from the data.

```sql
-- Read vectors
SELECT label, vector FROM firestore_scan('embeddings');
-- label: cat, vector: [1.0, 2.0, 3.0]

-- Access individual elements (1-indexed)
SELECT label, vector[1] AS first_dim FROM firestore_scan('embeddings');

-- Write vectors back (preserves Firestore vector format for vector search)
call firestore_update('embeddings', 'emb1',
    'vector', [100.0, 200.0, 300.0]::DOUBLE[3]);

-- Compute distances between vectors
SELECT a.label, b.label,
    sqrt(list_sum(list_transform(
        generate_series(1, 3),
        i -> power(a.vector[i] - b.vector[i], 2)
    ))) AS distance
FROM firestore_scan('embeddings') a, firestore_scan('embeddings') b
WHERE a.label < b.label;
```

## Filter Pushdown

The extension pushes supported WHERE clauses to Firestore's query API to reduce data transfer. Supported filters:

- Equality: `field = value`
- Inequality: `field != value`
- Range: `field > value`, `field >= value`, `field < value`, `field <= value`
- IN: `field IN ('a', 'b', 'c')`
- IS NOT NULL: `field IS NOT NULL`

DuckDB re-applies all filters after the scan for correctness, so unsupported filters (LIKE, IS NULL, OR, etc.) still work -- they just scan all documents first.

Use `EXPLAIN` to see which filters are pushed down:

```sql
EXPLAIN SELECT * FROM firestore_scan('users') WHERE status = 'active' AND age > 25;
-- Shows "Firestore Pushed Filters: status EQUAL 'active', age GREATER_THAN 25"
```

## Building from Source

### Prerequisites
- CMake 3.5+
- C++17 compiler
- vcpkg for dependency management

### Build Steps

```bash
# Clone the repository
git clone --recurse-submodules https://github.com/yourusername/fire_duck_ext.git
cd fire_duck_ext

# Set up vcpkg
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build
make release

# Run tests
make test
```

### Build Output

```
./build/release/duckdb                                        # DuckDB shell with extension
./build/release/test/unittest                                 # Test runner
./build/release/extension/fire_duck_ext/fire_duck_ext.duckdb_extension  # Loadable extension
```

## Running Integration Tests

Integration tests require the Firebase Emulator:

```bash
# Install Firebase CLI
npm install -g firebase-tools

# Run tests with emulator
firebase emulators:exec --only firestore --project test-project \
    "./test/scripts/run_integration_tests.sh"
```

## License

MIT License
