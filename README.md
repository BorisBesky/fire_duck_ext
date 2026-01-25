# FireDuckExt - DuckDB Extension for Google Cloud Firestore

Query Google Cloud Firestore directly from DuckDB using SQL.

## Features

- **Read data** from Firestore collections with `firestore_scan()`
- **Write data** with `firestore_insert()`, `firestore_update()`, `firestore_delete()`
- **Batch operations** for bulk updates and deletes
- **Collection group queries** for querying across nested collections
- **DuckDB secret management** for secure credential storage
- **Full SQL filtering** - filter, aggregate, and join Firestore data using DuckDB's SQL engine

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

-- Update documents
SELECT * FROM firestore_update('users', 'user123', 'status', 'verified');

-- Batch update with DuckDB filtering
SET VARIABLE ids = (
    SELECT list(__document_id)
    FROM firestore_scan('users')
    WHERE status = 'pending'
);
SELECT * FROM firestore_update_batch('users', getvariable('ids'), 'status', 'reviewed');
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
| `firestore_insert('collection', 'doc_id', ...)` | Insert a new document |
| `firestore_update('collection', 'doc_id', 'field', 'value')` | Update a single field |
| `firestore_delete('collection', 'doc_id')` | Delete a document |
| `firestore_update_batch('collection', ['id1', 'id2'], 'field', 'value')` | Batch update |
| `firestore_delete_batch('collection', ['id1', 'id2'])` | Batch delete |

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

## Type Mapping

| Firestore Type | DuckDB Type |
|----------------|-------------|
| string | VARCHAR |
| integer | BIGINT |
| double | DOUBLE |
| boolean | BOOLEAN |
| timestamp | TIMESTAMP |
| array | LIST |
| map | STRUCT |
| null | NULL |
| geoPoint | STRUCT(latitude DOUBLE, longitude DOUBLE) |
| reference | VARCHAR |

## License

MIT License
