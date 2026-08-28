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
- **Streaming scans** that page through collections of any size, with a tunable page size for large documents
- **Projection pushdown** so only the selected fields cross the wire
- **Count pushdown** answering `COUNT(*)` from Firestore's aggregation API without reading documents
- **Parallel scans** splitting a collection into document-key ranges read concurrently
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

`SERVICE_ACCOUNT_JSON` must be a **file path** to a service account key file
readable by the process running DuckDB. The extension opens and reads that
file itself — it does **not** accept the JSON content inline; passing JSON
text directly fails with `Failed to open service account file: {...}`.

```sql
CREATE SECRET prod_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);
```

> **Never expose a service-account key to a browser, JavaScript, or SQL text
> that could be inspected.** This key authenticates via Google Cloud IAM, and
> requests made with it **bypass Firestore Security Rules entirely** — see
> [Admin-only features](#admin-only-features-index-metadata). A leaked key can
> be replayed by any native client (not just this extension) with the full
> access its IAM role grants, regardless of what your Security Rules say.
> Service-account auth is rejected outright on the
> [WebAssembly build](#webassembly-duckdb-wasm) for the
> same reason. If you need authenticated, Security-Rules-respecting access
> from a browser, use
> [Firebase Auth user](#firebase-auth-user-authenticated-browser-safe)
> credentials instead — never a service account.

### API Key (For development/testing)

An API key provides unauthenticated access and is suitable for development, testing, or accessing public Firestore databases. API key auth does not support `batchWrite`, so batch operations fall back to individual requests.

```sql
CREATE SECRET dev_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourApiKeyHere'
);
```

### Firebase Auth User (authenticated; browser-safe)

Firebase user authentication gives **authenticated** access that respects Security Rules (`request.auth != null`), unlike a bare API key. It needs no OpenSSL, so it also works in the [WebAssembly/browser build](#webassembly-duckdb-wasm). Provide the public Web `API_KEY` plus a sign-in method.

**Email / password:**
```sql
CREATE SECRET user_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourWebApiKey',
    EMAIL 'user@example.com',
    PASSWORD 'hunter2'
);
```

**Anonymous:**
```sql
CREATE SECRET anon_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourWebApiKey',
    ANONYMOUS true
);
```

**Pre-obtained ID token** (e.g. minted by your host app):
```sql
CREATE SECRET token_firestore (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIzaSyYourWebApiKey',  -- optional; enables auto-refresh
    ID_TOKEN 'eyJhbGciOi...',
    REFRESH_TOKEN 'AMf-...'         -- optional; auto-refreshed on expiry
);
```

The extension signs in via the Firebase Auth REST API, sends `Authorization: Bearer <id_token>` on requests, and refreshes the token automatically on expiry.

#### Anonymous sign-in vs. plain API key

Both pass the API key, but only anonymous sign-in actually authenticates a user — the API key alone leaves the request **unauthenticated**:

| | API key only | `ANONYMOUS true` |
| --- | --- | --- |
| Signs in / obtains an ID token | No | Yes (`accounts:signUp`) |
| `request.auth` in Security Rules | `null` | non-null, with a `uid` |
| Creates a Firebase Auth user | No | Yes (anonymous; counts toward Auth quota) |
| Sends `Authorization: Bearer` | No | Yes |
| Passes `allow read: if true` | ✅ | ✅ |
| Passes `allow read: if request.auth != null` | ❌ | ✅ |

Use a **plain API key** for public collections whose rules allow unauthenticated reads. Use **`ANONYMOUS true`** when your rules require a signed-in user (`request.auth != null`) but you don't need a specific identity — e.g. per-session/per-device data. Each anonymous sign-in gets a fresh `uid`, so it suits "authenticated but identity-agnostic" rules; for a specific known user, use email/password or a pre-obtained `ID_TOKEN`.

### Admin-only features (index metadata)

Some features rely on Firestore's **index metadata** (composite indexes and single-field index configuration), which lives on the Firestore **Admin API**. That API is gated by Google Cloud IAM, **not** by Security Rules — so only **service-account** auth can read it. **API key** and **Firebase user** auth are non-admin: the extension cannot query this metadata for them, skips the Admin API, and assumes Firestore's default single-field indexes.

As a result, features that depend on admin metadata are unavailable to non-admin auth — most notably **composite-index detection for multi-field `ORDER BY`**. With a service account, the extension detects an existing composite index and pushes multi-field ordering to Firestore; without one, such a query falls back to a single-field server-side sort (re-sorted in DuckDB) or surfaces Firestore's *"query requires an index"* error at runtime. Single-field filter and order pushdown are unaffected, since Firestore's default single-field indexes are assumed to exist.

| Feature | Service account | API key / Firebase user |
| --- | --- | --- |
| Single-field filter / `ORDER BY` pushdown | ✅ | ✅ |
| Composite-index detection (multi-field `ORDER BY`) | ✅ | ❌ (assumes defaults) |
| `show_missing:=true` (phantom-document listing) | ✅ | ❌ (403; use `show_missing:=false`) |

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
| `firestore_connect('database')` | Set the active database for the session; used by subsequent calls until `firestore_disconnect()` |
| `firestore_disconnect()` | Clear the session's active database |

## Batch Operations

`firestore_update_batch()` and `firestore_delete_batch()` group writes into requests of up to 500 operations each using Firestore's `batchWrite` API. `batchWrite` is **not atomic** — individual writes within a batch may succeed or fail independently. If `batchWrite` is unavailable (API key auth does not support it), the extension falls back to individual requests automatically.

```sql
-- Batch update: mark all pending users as reviewed
SET VARIABLE ids = (
    SELECT list(__document_id)
    FROM firestore_scan('users')
    WHERE status = 'pending'
);
CALL firestore_update_batch('users', getvariable('ids'), 'status', 'reviewed');
```

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
CALL firestore_update('users', 'user1', 'status', 'active', database:='my-other-db');

-- Read from a specific database
SELECT * FROM firestore_scan('users', database:='my-other-db');
```

### Scan Parameters

`firestore_scan` accepts additional parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `scan_limit` | BIGINT | Maximum number of rows to fetch from Firestore. When combined with a `WHERE` clause, the limit is only enforced if filter pushdown succeeds; if pushdown fails, `scan_limit` is ignored so no matching rows are lost. SQL `LIMIT` can also be pushed down automatically, and named `scan_limit` takes precedence when both are present. |
| `order_by` | VARCHAR | Server-side ordering. Specify one or more fields separated by commas, each optionally followed by `DESC` (e.g. `'score'`, `'score DESC'`, `'score DESC, name ASC'`). SQL `ORDER BY` can also be pushed down automatically, and named `order_by` takes precedence when both are present. Multi-field ordering requires a composite index. |
| `show_missing` | BOOLEAN | Include phantom documents that have no fields but serve as parent paths for subcollections. Default: `true`. |
| `map_encoding` | VARCHAR | How Firestore `map` fields are surfaced: `'wire'` (default), `'json'`, or `'variant'`. See [Map Encoding](#map-encoding). |
| `schema_sample_size` | BIGINT | Documents sampled to infer the schema. Default `1000`; `-1` samples every document. Overrides the `firestore_schema_sample_size` setting. See [Schema Inference](#schema-inference-and-unmapped-fields). |
| `unmapped_column` | BOOLEAN | Append a `__unmapped` column carrying any field not present in the inferred schema. Default: `false`. |
| `columns` | STRUCT | Declare the schema explicitly (e.g. `columns:={'id':'VARCHAR','score':'BIGINT'}`), skipping inference and its sampling request entirely. |
| `page_size` | BIGINT | Documents fetched per Firestore round trip, 1-1000 (values outside that range are clamped). Default `1000`; overrides the `firestore_page_size` setting. Lower it for collections of large documents. See [Large Collections](#large-collections). |

```sql
-- Fetch only the top 10 documents ordered by score
SELECT * FROM firestore_scan('leaderboard', order_by:='score DESC', scan_limit:=10);

-- Multi-field ordering
SELECT * FROM firestore_scan('leaderboard', order_by:='category, score DESC');

-- Exclude phantom/missing documents
SELECT * FROM firestore_scan('users', show_missing:=false);

-- Smaller pages for a collection of large documents
SELECT * FROM firestore_scan('scanned_documents', page_size:=50);
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
| map | VARCHAR, JSON or VARIANT (see [Map Encoding](#map-encoding)) |
| vector | ARRAY(DOUBLE, N) |
| null | NULL |
| geoPoint | STRUCT(latitude DOUBLE, longitude DOUBLE) |
| reference | VARCHAR |
| bytes | BLOB |

### Null Semantics

Both missing fields and explicit Firestore null values appear as `NULL` in DuckDB — there is no way to distinguish the two on read.

Writing `NULL` to a field sets it to an explicit Firestore null value; it does not delete the field from the document.

`WHERE field IS NULL` is not pushed down to Firestore: Firestore's `IS_NULL` operator only matches fields that exist and are explicitly null, while DuckDB also treats *missing* fields as `NULL`, so pushing the filter down would miss documents where the field is simply absent. `WHERE field IS NOT NULL` is pushed down safely.

### Schema Inference and Unmapped Fields

The schema is inferred by sampling documents at bind time. Firestore is
schemaless and returns documents in `__name__` order, so a field introduced
later in a collection can fall outside the sample — in which case it is **not**
a column and its data does not appear in results.

Three controls address this:

```sql
-- Sample deeper (default 1000; -1 reads every document)
SELECT * FROM firestore_scan('events', schema_sample_size:=-1);

-- Keep whatever the schema missed, in a catch-all column
SELECT __unmapped FROM firestore_scan('events', unmapped_column:=true);

-- Skip inference entirely and declare the schema yourself
SELECT * FROM firestore_scan('events',
    columns:={'user_id':'VARCHAR', 'score':'BIGINT'});
```

Whenever a document carries a field the schema does not have, a warning naming
that field is logged once per scan (set `FIRESTORE_LOG_LEVEL=WARN` to see it),
so the omission is never silent.

`__unmapped` follows `map_encoding`: `VARIANT` when `map_encoding:='variant'`
(so `__unmapped.some_field` works), otherwise `JSON`. It is NULL for documents
that have no extra fields.

The sample size can also be set globally:

```sql
SET firestore_schema_sample_size = 5000;   -- -1 to sample everything
```

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `firestore_schema_cache_ttl` | `3600` | Seconds an inferred schema is cached; `0` disables caching. |
| `firestore_schema_sample_size` | `1000` | Documents sampled to infer a schema; `-1` samples every document. |
| `firestore_page_size` | `1000` | Documents fetched per round trip, clamped to Firestore's 1-1000 range. |
| `firestore_page_byte_budget` | `67108864` | Uncompressed bytes a page may weigh before the scan requests fewer documents; `0` disables the guard. See [Large Collections](#large-collections). |
| `firestore_max_threads` | `4` | Threads one scan may split across, reading separate key ranges; `1` disables parallel scanning, and the value is capped at 64. |

Sampling streams: each page is folded into the inferred schema and released, so
`schema_sample_size:=-1` costs one page of memory rather than the whole
collection. Collection-group scans (`~` prefix) sample through cursor
pagination and so honour `schema_sample_size` like any other scan.

### Map Encoding

Firestore maps are schemaless: keys and value types vary from document to
document, so no single fixed column type describes them all. `map_encoding`
selects how they are surfaced.

| Mode | Column type | Reaching a leaf |
|------|-------------|-----------------|
| `'wire'` (default) | VARCHAR | `json_extract_string(m, '$.a.mapValue.fields.b.stringValue')` |
| `'json'` | JSON | `json_extract_string(m, '$.a.b')` |
| `'variant'` | VARIANT | `m.a.b` |

```sql
-- Default: raw Firestore wire format, type wrappers included
SELECT payload FROM firestore_scan('events');
-- {"user":{"mapValue":{"fields":{"id":{"integerValue":"7"}}}}}

-- Natural JSON: type wrappers stripped, integers become numbers
SELECT payload FROM firestore_scan('events', map_encoding:='json');
-- {"user":{"id":7}}

-- VARIANT: dot access, per-value types preserved
SELECT payload.user.id, variant_typeof(payload.user.id)
FROM firestore_scan('events', map_encoding:='variant');
-- 7, INT64
```

`'variant'` is the most faithful representation of Firestore's data model:
documents with different keys, and documents with *different types at the same
path*, are all preserved — a row missing the key yields SQL NULL rather than an
error. `'wire'` remains the default so existing queries keep working.

Maps nested inside arrays follow the same setting; because a LIST child cannot
itself be VARIANT, `'variant'` renders those as natural JSON strings.

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

- If `order_by:=` is provided, that server-side ordering is used and DuckDB applies any SQL `ORDER BY` afterward.
- If `scan_limit:=` is provided, that fetch limit is used and DuckDB applies any SQL `LIMIT` afterward.

> **Multi-field ordering needs a composite index, which the extension can only detect with service-account auth.** See [Admin-only features](#admin-only-features-index-metadata) — with API-key or Firebase-user auth, multi-field `ORDER BY` can't be confirmed against a composite index and may fall back to a client-side sort or hit Firestore's "requires an index" error.

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
FROM firestore_scan('leaderboard', order_by:='score', scan_limit:=10)
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

## Collection Group Queries

Use the `~collection` prefix to query across every subcollection with a given name, regardless of its parent document. `firestore_scan('~orders')` reads all documents from every subcollection named `orders` anywhere in the database — for example `users/user1/orders` and `users/user2/orders` together.

```sql
SELECT __document_id, product, quantity
FROM firestore_scan('~orders')
WHERE status = 'shipped';
```

Collection-group scans paginate with a `__name__` cursor, so they read the
whole collection group rather than stopping at the first page.

Composite-index detection for multi-field `ORDER BY` on a collection group requires service-account auth — see [Admin-only features](#admin-only-features-index-metadata).

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
- Named `order_by:='__document_id'` or `order_by:='__document_id DESC'`, plus `scan_limit:=...`

Other ordering expressions still work, but they are evaluated in DuckDB after fetching the subcollection IDs.

```sql
-- Server-side sort and limit on subcollection IDs
SELECT __document_id
FROM firestore_scan('users/user1')
ORDER BY __document_id DESC
LIMIT 5;
```

## Large Collections

A scan streams: the extension holds one page of documents at a time and asks
for the next only as DuckDB consumes the current one, so memory does not grow
with the size of the collection.

Two knobs control what a page costs.

```sql
-- Documents per round trip (1-1000, default 1000)
SET firestore_page_size = 100;
SELECT * FROM firestore_scan('scanned_documents', page_size:=50);  -- per query

-- Uncompressed bytes a page may weigh before the scan asks for fewer
-- documents per request (default 64 MiB; 0 disables the guard)
SET firestore_page_byte_budget = 16777216;
```

`page_size` matters when documents are large. Firestore allows a document to be
1 MiB, so a full 1000-document page can be ~1 GiB of JSON before it is parsed.
The default suits ordinary collections; lower it when documents are big enough
that a full page will not fit comfortably in memory. It applies to the
bind-time schema-sampling request too, which is the first request a query
makes and runs before any `WHERE` or `LIMIT` can reduce it.

`firestore_page_byte_budget` is a safety net for when you do not know the
document sizes in advance. If a page comes back heavier than the budget, the
scan reduces the page size for the rest of that query — roughly to what the
budget divided by the observed per-document size allows — and logs a warning
naming the new size. The page size only ever decreases within a scan: growing
it back would spend round trips rediscovering a limit already found. Ordinary
collections never come close to the budget, so nothing shrinks and no round
trips are added.

### Counting without reading

A bare `COUNT(*)` reads none of its input's values, so the whole answer is how
many rows there are. Where that is safe, the scan asks Firestore for the number
with `:runAggregationQuery` and fetches no documents:

```sql
-- One request, no documents transferred
SELECT count(*) FROM firestore_scan('events', show_missing:=false);
```

Measured against the mock over 200,000 documents: 200 requests and 84.24 MiB
become 1 request and nothing.

`show_missing:=false` is required on an ordinary collection, and is why the
default does not take this path. Phantom documents — those that exist only to
parent a subcollection — are returned as rows by a scan with `show_missing`
(the default) but are never counted by an aggregation query, so the two would
disagree. Collection-group scans never included phantom documents in the first
place, so `firestore_scan('~events')` is counted server-side either way.

The count is used only when nothing can read a value: no `WHERE`, no
`GROUP BY`, no `DISTINCT`, no `LIMIT` between the count and the scan, and
`count(<column>)` rather than `count(*)` does not qualify. `EXPLAIN` shows
`Firestore Pushed Count` when it applies. If the endpoint is unavailable —
older emulators, restricted credentials — the scan reads the documents
instead, which is slower but never wrong.

### Reading a collection with several threads

Firestore's REST pagination is sequential: the next page needs the previous
one's cursor, so a large scan is a chain of round trips whose latency cannot be
hidden. Ranges of the key space are independent, though, so the scan can cut
the collection into ranges and read several at once:

```sql
-- Up to 4 threads by default; 1 disables parallel scanning
SET firestore_max_threads = 8;
SELECT * FROM firestore_scan('events', show_missing:=false);
```

Each thread takes a range, pages through it with its own connection, and takes
another when it finishes — so an uneven key distribution costs balance, not
correctness. Measured against the mock at 50 ms simulated round-trip latency
over 20,000 documents: 2.33 s on one thread, 1.23 s on four.

Balance does depend on the keys. Firestore auto-ids are 20 characters drawn
uniformly from `[A-Za-z0-9]`, and the ranges are cut evenly over that space, so
auto-ids spread well. Keys chosen by hand — e-mail addresses, timestamps,
sequence numbers — will pile into one range: still correct, just no faster than
a single thread.

Parallel scanning applies only where a range split returns exactly the rows a
sequential scan would, which means all of:

- `show_missing:=false`. Ranges are cursors, which only `runQuery` supports,
  and `runQuery` never returns phantom documents while a `show_missing` scan
  does.
- No `ORDER BY`, no `LIMIT`, and no `WHERE` that reaches Firestore. Ordering is
  undone by reading ranges concurrently, a limit cannot be enforced per range,
  and a pushed filter needs its own ordering, which conflicts with ordering by
  `__name__`.
- An ordinary collection — not a collection group (whose names span parent
  paths) and not a document path.

Anything else scans sequentially, exactly as before.

Reducing the transfer itself is usually better than paging around it:

- Only the columns a query selects are requested. The scan sends Firestore a
  `mask.fieldPaths` (or `select.fields` on a collection group) built from the
  projection, so unselected fields never cross the wire — selecting 1 of 40
  columns from 5,000 documents moves 1.11 MiB instead of 7.03 MiB. Two cases
  opt out: `unmapped_column:=true`, which is defined as everything the schema
  does not cover, and a query needing no fields at all, which the
  `documents.list` URL has no way to express (`runQuery` uses Firestore's
  keys-only `select __name__`).
- `WHERE` clauses that Firestore can serve are pushed down, so filtered rows
  never cross the wire — see [Filter Pushdown](#filter-pushdown).
- `scan_limit:=` and SQL `LIMIT` bound how much is fetched — see
  [SQL ORDER BY / LIMIT Pushdown](#sql-order-by--limit-pushdown).
- `columns:={...}` skips schema inference and its sampling request entirely.

If a query still fails on memory after the scan is bounded, the pressure is
likely above the scan rather than in it: a `GROUP BY`, `ORDER BY`, or join
holding results that the scan is only feeding. Those are DuckDB's own
operators, so `SET memory_limit` and a writable `SET temp_directory` are what
let them spill to disk.

## Missing Documents

By default, `firestore_scan()` includes "phantom" documents — documents that have no fields but serve as parent paths for subcollections. This matches the behavior of the Firebase Console and is controlled by the `show_missing` parameter (default: `true`).

```sql
-- Default: includes phantom/missing documents
SELECT * FROM firestore_scan('artifacts/default-app-id/users');

-- Opt out to only return documents with fields
SELECT * FROM firestore_scan('artifacts/default-app-id/users', show_missing:=false);
```

When a collection contains only phantom documents (no fields at all), the result includes just the `__document_id` column, letting you discover document IDs for navigating into subcollections.

> **Note:** The Firestore Emulator does not support `showMissing`. The extension detects the emulator automatically and skips the parameter.

> **Important — `show_missing:=true` requires privileged (service-account) access.** Listing
> phantom/missing documents (`showMissing=true`, the default) is an Admin-oriented operation.
> The Admin SDK / a service account bypasses Security Rules and can do it, but over
> **rules-governed access (API key or Firebase user ID token)** Firestore rejects it with
> `403 PERMISSION_DENIED: Missing or insufficient permissions` — **even when your rules grant
> `allow read: if true`** (a plain `list` is permitted; enumerating missing documents is not).
> If you authenticate with an API key or a Firebase user token, pass `show_missing:=false`:
>
> ```sql
> SELECT * FROM firestore_scan('artifacts/default-app-id/users/<uid>/math_whiz_data',
>                              show_missing:=false)
> WHERE role = 'student';
> ```
>
> The 403 is reported for the scan's schema-inference `listDocuments` request (it carries
> `showMissing=true&pageSize=100`), not for your documents — your read rules are unaffected.

## WebAssembly (DuckDB-WASM)

The extension builds and runs under DuckDB-WASM (e.g. in the browser). HTTP is routed through DuckDB's `HTTPUtil` instead of raw sockets, so reads, filtered `:runQuery` scans (including collection groups), and writes all work in the browser.

Authentication in WASM is limited to **API key** and **Firebase Auth user** credentials (email/password, anonymous, or a pre-obtained ID token) — see [Authentication](#authentication). **Service-account** auth is rejected outright on WASM; if you're building a browser app, use Firebase Auth user credentials for Security-Rules-respecting access.

Two consequences of rules-governed (API key / Firebase user) access apply on any platform, but matter most for browser apps:
- `show_missing := true` (the default) lists phantom/missing documents, which is an Admin-only operation; Firestore returns `403 PERMISSION_DENIED` over rules-governed access even when rules allow the read. Pass `show_missing := false` — see [Missing Documents](#missing-documents).
- The Firestore Admin API (index metadata) is reachable only with a service account; for API-key / Firebase-user auth the extension skips it and assumes default single-field indexes — see [Admin-only features](#admin-only-features-index-metadata).

The WASM build must match the DuckDB version bundled by the `@duckdb/duckdb-wasm` runtime it is loaded into — the C++ extension ABI is not stable across versions.

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
