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
- **SQL ORDER BY / LIMIT pushdown** for faster top-N and sorted scans
- **Collection ID listings** by scanning a Firestore document path
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

`SERVICE_ACCOUNT_JSON` accepts either a **file path** or the **full JSON text** of a service account key.

**File path:**
```sql
CREATE SECRET prod_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);
```

**Inline JSON:**
```sql
CREATE SECRET prod_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '{
        "type": "service_account",
        "project_id": "my-project",
        "private_key_id": "key123abc",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
        "client_email": "firestore-sa@my-project.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }'
);
```

### API Key (For development/testing)

An API key provides unauthenticated access and is suitable for development, testing, or accessing public Firestore databases. API key auth does not support `batchWrite`, so batch operations fall back to individual requests.

```sql
CREATE SECRET dev_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourApiKeyHere'
);
```

### Environment Variable
```bash
# Set the path to your service account JSON file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Then run DuckDB - no secret creation needed!
duckdb
```
The extension automatically reads `GOOGLE_APPLICATION_CREDENTIALS` on startup and creates an internal secret that matches all databases (`DATABASE '*'`), so no `CREATE SECRET` is needed.

### Custom Database
```sql
-- Single named database (with service account)
CREATE SECRET my_secret (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/credentials.json',
    DATABASE 'my-database'
);

-- Single named database (with API key)
CREATE SECRET my_secret (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourApiKeyHere',
    DATABASE 'my-database'
);

-- Multiple databases
CREATE SECRET my_secret (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/credentials.json',
    DATABASES ['(default)', 'my-other-db']
);

-- Wildcard (matches all databases)
CREATE SECRET my_secret (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/credentials.json',
    DATABASE '*'
);
```
If `DATABASE`/`DATABASES` is omitted, it defaults to `(default)`.

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
| `firestore_scan('collection/doc_id')` | List direct subcollection IDs under a document path |
| `firestore_insert('collection', (SELECT ...), document_id := 'col')` | Insert documents from a subquery |
| `firestore_update('collection', 'doc_id', 'field1', value1, ...)` | Update fields on a single document |
| `firestore_delete('collection', 'doc_id')` | Delete a document |
| `firestore_update_batch('collection', ['id1', ...], 'field1', value1, ...)` | Batch update |
| `firestore_delete_batch('collection', ['id1', ...])` | Batch delete |
| `firestore_array_union('collection', 'doc_id', 'field', ['v1', ...])` | Add to array (no duplicates) |
| `firestore_array_remove('collection', 'doc_id', 'field', ['v1', ...])` | Remove from array |
| `firestore_array_append('collection', 'doc_id', 'field', ['v1', ...])` | Append to array |

## Named Parameters

All functions accept these credential override parameters, allowing per-call control over which project and database to target:

| Parameter | Type | Description |
|-----------|------|-------------|
| `database` | VARCHAR | Override the database ID for this call (instead of the secret's default). |
| `project_id` | VARCHAR | Override the project ID for this call. |
| `api_key` | VARCHAR | Override the API key for this call. |
| `credentials` | VARCHAR | Path to a service account JSON file to use for this call. |

```sql
-- Write to a specific database
CALL firestore_update('users', 'user1', 'status', 'active', database='my-other-db');

-- Read from a specific database
SELECT * FROM firestore_scan('users', database='my-other-db');
```

### Scan Parameters

`firestore_scan` accepts additional parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `scan_limit` | BIGINT | Maximum number of rows to fetch from Firestore. When combined with a `WHERE` clause, the limit is only enforced if filter pushdown succeeds; if pushdown fails, `scan_limit` is ignored so no matching rows are lost. SQL `LIMIT` can also be pushed down automatically, and named `scan_limit` takes precedence when both are present. |
| `order_by` | VARCHAR | Server-side ordering. Specify one or more fields separated by commas, each optionally followed by `DESC` (e.g. `'score'`, `'score DESC'`, `'score DESC, name ASC'`). SQL `ORDER BY` can also be pushed down automatically, and named `order_by` takes precedence when both are present. Multi-field ordering requires a composite index. |
| `show_missing` | BOOLEAN | Include phantom documents that have no fields but serve as parent paths for subcollections. Default: `true`. |

```sql
-- Fetch only the top 10 documents ordered by score
SELECT * FROM firestore_scan('leaderboard', order_by='score DESC', scan_limit=10);

-- Multi-field ordering
SELECT * FROM firestore_scan('leaderboard', order_by='category, score DESC');

-- Exclude phantom/missing documents
SELECT * FROM firestore_scan('users', show_missing=false);
```

### Insert Parameters

`firestore_insert` accepts one additional parameter:

| Parameter | Type | Description |
|-----------|------|-------------|
| `document_id` | VARCHAR | Column name to use as the Firestore document ID. That column is excluded from the document fields. If omitted, Firestore auto-generates IDs. |

```sql
-- Auto-generated document IDs
CALL firestore_insert('users', (
    SELECT name, age FROM read_csv('new_users.csv')
));

-- Explicit document IDs from a column
CALL firestore_insert('users',
    (SELECT user_id, name, age FROM read_csv('new_users.csv')),
    document_id := 'user_id');

-- Insert from a DuckDB table
CALL firestore_insert('employees',
    (SELECT * FROM employee_staging),
    document_id := 'emp_id');

-- Insert into nested collections
CALL firestore_insert('users/user1/notes', (
    SELECT 'note1' AS id, 'Remember to buy milk' AS content
), document_id := 'id');
```

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

## SQL ORDER BY / LIMIT Pushdown

`firestore_scan()` can automatically push simple SQL `ORDER BY`, `LIMIT`, and `OFFSET` clauses down to Firestore, reducing the number of documents fetched for sorted and top-N queries.

Supported patterns include:

- `ORDER BY field`
- `ORDER BY field DESC`
- `ORDER BY field1, field2`
- `LIMIT n`
- `ORDER BY ... LIMIT n`
- `ORDER BY ... LIMIT n OFFSET m` (pushed as `LIMIT n + m`, with DuckDB applying the final offset)

Named parameters still work and take precedence over SQL pushdown:

- If `order_by=` is provided, that server-side ordering is used and DuckDB applies any SQL `ORDER BY` afterward.
- If `scan_limit=` is provided, that fetch limit is used and DuckDB applies any SQL `LIMIT` afterward.

```sql
-- SQL ORDER BY + LIMIT pushed to Firestore
SELECT name, score
FROM firestore_scan('leaderboard')
ORDER BY score DESC
LIMIT 5;

-- Multi-field SQL ORDER BY pushdown
SELECT *
FROM firestore_scan('leaderboard')
ORDER BY category, score DESC
LIMIT 10;

-- Named parameters override SQL pushdown
SELECT name
FROM firestore_scan('leaderboard', order_by='score', scan_limit=10)
ORDER BY name DESC
LIMIT 3;
```

Use `EXPLAIN` to verify when SQL ordering and limits are being pushed:

```sql
EXPLAIN
SELECT *
FROM firestore_scan('leaderboard')
WHERE status = 'active'
ORDER BY score DESC
LIMIT 5;

-- Shows:
-- Firestore Pushed Filters: status EQUAL 'active'
-- Firestore Pushed Order: score DESC
-- Firestore Pushed Limit: 5
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

## Collection ID Listings

When `firestore_scan()` is given a document path instead of a collection path, it lists that document's direct subcollection IDs instead of reading documents. This is useful for discovering unknown nested collection names.

The result contains a single `__document_id` column, where each row is a subcollection ID:

```sql
-- List direct subcollections under users/user1
SELECT __document_id
FROM firestore_scan('users/user1');

-- Example results:
-- orders
-- notes
-- settings
```

Document-path scans support:

- Pagination across large numbers of subcollections
- SQL `ORDER BY __document_id` pushdown
- SQL `LIMIT` pushdown
- Named `order_by='__document_id'` or `order_by='__document_id DESC'`, plus `scan_limit=...`

Other ordering expressions still work, but they are evaluated in DuckDB after fetching the subcollection IDs.

```sql
-- Server-side sort and limit on subcollection IDs
SELECT __document_id
FROM firestore_scan('users/user1')
ORDER BY __document_id DESC
LIMIT 5;
```

## Missing Documents

By default, `firestore_scan()` includes "phantom" documents — documents that have no fields but serve as parent paths for subcollections. This matches the behavior of the Firebase Console and is controlled by the `show_missing` parameter (default: `true`).

```sql
-- Default: includes phantom/missing documents
SELECT * FROM firestore_scan('artifacts/default-app-id/users');

-- Opt out to only return documents with fields
SELECT * FROM firestore_scan('artifacts/default-app-id/users', show_missing=false);
```

When a collection contains only phantom documents (no fields at all), the result includes just the `__document_id` column, letting you discover document IDs for navigating into subcollections.

> **Note:** The Firestore Emulator does not support `showMissing`. The extension detects the emulator automatically and skips the parameter.

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

# Extension only build:
cmake --build build/release --config Release --target fire_duck_ext_loadable_extension

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
