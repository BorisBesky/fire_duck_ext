# fire_duck_ext — nested datatypes & large collections: measured assessment

Measured on `main` @ `62e9d80`, DuckDB v1.5.4 release build, Apple M-series (16 core),
against `bench/mock_firestore.py` over loopback.

Reproduce:

```bash
nohup python3 bench/mock_firestore.py 8099 &
python3 bench/run_bench.py                    # all groups
python3 bench/run_bench.py nested map-depth   # one group
```

**Caveat on absolute numbers.** The mock serves pre-serialised pages over
loopback, so throughput here is an upper bound: it excludes TLS, WAN latency,
and Firestore's own service time. That biases *against* the network findings —
on real Firestore every network-side result below gets larger, not smaller.
The WAN section re-introduces latency explicitly.

---

## 1. How nested datatypes are handled

| Firestore type | DuckDB type produced | Verdict |
|---|---|---|
| `arrayValue` | `LIST(T)`, element type inferred by majority vote | native |
| `geoPointValue` | `STRUCT(latitude DOUBLE, longitude DOUBLE)` | native |
| vector (`__type__: __vector__`) | `ARRAY(DOUBLE, N)`, N from first sample | native |
| **`mapValue`** | **`VARCHAR` holding raw Firestore wire JSON** | **leaky** |

Arrays and vectors are converted properly. Maps are the outlier
([firestore_types.cpp:344-350](../src/firestore_types.cpp#L344)): the value is
`fv["mapValue"]["fields"].dump()` — the *undecoded* wire format, type wrappers
and all.

Actual output for a 3-level map:

```
{"child":{"mapValue":{"fields":{"child":{"mapValue":{"fields":{
  "leaf0":{"stringValue":"alpha-0000000-0"},"leaf1":{"integerValue":"1"}}}},
  "label":{"stringValue":"level-2"}}}},"label":{"stringValue":"level-3"}}
```

Two consequences:

1. **Every nesting level costs two extra JSON path segments.** Reaching a leaf
   three levels down requires
   `json_extract_string(payload,'$.child.mapValue.fields.child.mapValue.fields.leaf0.stringValue')`
   instead of `$.child.child.leaf0`. Integers arrive as strings and need a cast.
   The workaround also requires the `json` extension.
2. **The stored string is ~2.1x larger than equivalent natural JSON**
   (measured: depth 1 → 2.00x, depth 4 → 2.07x, depth 8 → 2.10x).

The same raw dump is used for maps nested inside arrays
([firestore_types.cpp:273-279](../src/firestore_types.cpp#L273)), so a
`LIST` of maps is a list of wire-format strings.

### Conversion cost by shape — 20,000 documents

`count(<col>)` forces materialisation; `count(*)` does not (projection pushdown
asks the scan for zero columns, so `SetDuckDBValue` never runs). The difference
isolates conversion.

| shape | materialised | `count(*)` | conversion | conversion share | MiB |
|---|---|---|---|---|---|
| 8 scalar fields | 0.103 s | 0.084 s | 0.019 s | 18% | 8.45 |
| map, 8 leaves | 0.129 s | 0.105 s | 0.024 s | 19% | 10.30 |
| array, 8 strings | 0.123 s | 0.091 s | 0.032 s | 26% | 8.98 |
| array, 8 ints | 0.116 s | 0.090 s | 0.026 s | 22% | 8.78 |
| vector, 8 dims | 0.125 s | 0.102 s | 0.023 s | 18% | 9.85 |

**No shape is pathologically slow.** Cost tracks payload size at a near-constant
~10 ms/MiB. The nested-type problem is not CPU per value — it is that nesting
*inflates bytes*, and bytes are the currency.

### Depth and width scale linearly, not quadratically

Map depth (materialised, 20k docs):

| depth | time | MiB | ms/MiB |
|---|---|---|---|
| 1 | 0.092 s | 7.61 | 12.1 |
| 2 | 0.112 s | 8.91 | 12.6 |
| 4 | 0.152 s | 11.52 | 13.2 |
| 8 | 0.233 s | 16.73 | 13.9 |
| 16 | 0.389 s | 27.31 | 14.2 |

Per-byte cost rises only 17% from depth 1 to 16 — deep nesting is not a
blow-up. Time grows 4.2x because *bytes* grow 3.6x.

Array width (20k docs):

| elements | materialised | `count(*)` | conversion | conversion share |
|---|---|---|---|---|
| 1 | 0.060 s | 0.050 s | 0.010 s | 17% |
| 4 | 0.088 s | 0.069 s | 0.019 s | 22% |
| 16 | 0.191 s | 0.134 s | 0.057 s | 30% |
| 64 | 0.589 s | 0.389 s | 0.200 s | 34% |

Linear in element count (~156 ns/element at width 64), but conversion's *share*
grows to a third of runtime. This is the one place worth micro-optimising:
`FirestoreValueToDuckDB` builds a throwaway `vector<Value>` (one heap `Value`
per element) which `SetDuckDBValue` then copies element-by-element into the
`ListVector` ([firestore_types.cpp:214-308](../src/firestore_types.cpp#L214),
[:618-663](../src/firestore_types.cpp#L618)) — two passes and N temporaries per
list per row. Writing straight into the child vector would remove both.

*(An earlier hypothesis that per-row `ListVector::Reserve` causes O(n²) copying
is wrong: `VectorListBuffer::Reserve` grows via `NextPowerOfTwo`, so appends
amortise. Discarded.)*

---

## 2. How large collections are handled

Full scan, 8 scalar fields, materialised:

| documents | time | docs/s | requests | connections | MiB |
|---|---|---|---|---|---|
| 1,000 | 0.005 s | ~200k | 2 | 3 | 0.46 |
| 10,000 | 0.052 s | 192k | 11 | 12 | 4.24 |
| 50,000 | 0.258 s | 194k | 51 | 52 | 21.05 |
| 200,000 | 1.040 s | 192k | 201 | 202 | 84.28 |

**Scaling is clean and linear** — no quadratic behaviour, stable ~192k docs/s
and ~81 MiB/s. Memory stays bounded (one 1000-document page at a time).

The problems are all in what goes over the wire.

### 2a. Zero connection reuse — every page opens a new TCP connection

`max_requests_on_one_conn = 1` for all 51 requests of a 50k scan. The mock
speaks HTTP/1.1 with `Content-Length`, and a control run with `curl` reused one
connection for 5 requests (`max_on_one_conn=5`), so this is the client's
behaviour, not a server limitation.

Cause: `httplib::Client cli(scheme_host)` is constructed **inside**
`MakeRequest` ([firestore_client.cpp:186](../src/firestore_client.cpp#L186)) and
destroyed when it returns — one connect + TLS handshake per page.

A/B under simulated WAN (50,000 docs, 51 requests):

| condition | time |
|---|---|
| loopback, no latency | 0.228 s |
| 15 ms/request, connections free *(= what keep-alive gives)* | 1.577 s |
| 15 ms/request + 40 ms/new connection *(actual behaviour)* | **4.255 s** |

**Reusing the connection is a 2.7x speedup** on this workload, and the gap grows
linearly with collection size. Against production Firestore over TLS, the
per-connection cost is real.

### 2b. No compression negotiated

`requests_advertising_gzip = 0` on every request. Firestore's wire format is
extremely repetitive (`{"stringValue":…}` per field), so it compresses hard:

| one 1000-document page | size |
|---|---|
| as transferred today | 430.3 KiB |
| same bytes, gzip | 31.9 KiB |

**13.5x more bytes than necessary.** The 200k scan moves 84.28 MiB where ~6.2 MiB
would do.

### 2c. Projection is not pushed to Firestore

`projection_pushdown = true` is set, but `mask.fieldPaths` is never sent
(`requests_with_field_mask = 0` in every run).

| 20k docs, 40 columns | time | transferred |
|---|---|---|
| all 40 columns | 0.450 s | 28.24 MiB |
| 1 of 40 columns | 0.341 s | **28.24 MiB** |

Projection saves 24% CPU by skipping conversion, and **zero network**. The REST
API supports `mask.fieldPaths` on `documents.list` and `select.fields` on
`runQuery`.

### 2d. `scan_limit` is silently ignored above one page — and downloads everything

| `scan_limit` | rows returned | documents fetched | transferred |
|---|---|---|---|
| 500 | 500 | 600 | 0.25 MiB |
| 1000 | 1000 | 1100 | 0.46 MiB |
| **1001** | **200,000** | **200,100** | **84.28 MiB** |
| **5000** | **200,000** | **200,100** | **84.28 MiB** |

The threshold is exactly the 1000-document page size. Root cause
([firestore_scanner.cpp:826](../src/firestore_scanner.cpp#L826)):

```cpp
idx_t total_returned = global_state.current_index;
```

`current_index` is an index *within the current page* and is reset to `0` on
every page fetch ([:880](../src/firestore_scanner.cpp#L880),
[:904](../src/firestore_scanner.cpp#L904)). It therefore never reaches a limit
larger than one page, and the scan runs to the end of the collection. There is
no running-total field in `FirestoreScanGlobalState`.

SQL `LIMIT` is unaffected — it goes through the optimizer extension and works
correctly (`LIMIT 5000` fetched 7,100 documents).

### 2e. Schema inference samples 100 documents, silently dropping later fields

`InferSchema(collection, 100, …)`
([firestore_scanner.cpp:400](../src/firestore_scanner.cpp#L400)). A field first
appearing at document 500 of 2,000 — present on **75% of the collection** — does
not appear in `DESCRIBE` and is absent from every result. No warning is emitted.

This is realistic for Firestore, where documents are returned in `__name__`
order and schemas drift over time.

### 2f. Secondary (identified by inspection, not individually measured)

- Every field access does two lookups: `doc.fields.contains(col_name)` then
  `doc.fields[col_name]` ([firestore_scanner.cpp:940-941](../src/firestore_scanner.cpp#L940)).
  One `find()` would halve it — 2 × columns × rows ordered-map probes.
- `ParseDocument` deep-copies each document's fields
  (`doc.fields = doc_json["fields"]`,
  [firestore_client.cpp:303](../src/firestore_client.cpp#L303)) while the parsed
  response is still alive, roughly doubling peak memory per page. A `std::move`
  from a non-const parameter would avoid it.
- `MaxThreads()` returns 1 and pages are fetched inline on the execution thread,
  so fetch and conversion never overlap.
- Every bind that misses the schema cache costs one extra 100-document request
  (visible as `docs_served = N + 100` throughout).

---

## 3. Recommendations, ranked by measured impact

| # | Change | Evidence | Effort | Status |
|---|---|---|---|---|
| 1 | Reuse one `httplib::Client` per host, stored on `FirestoreClient` | 2.7x on WAN A/B | small | **done** |
| 2 | Enable gzip (`CPPHTTPLIB_ZLIB_SUPPORT` + zlib in `vcpkg.json`) | 13.5x fewer bytes | small | **done** |
| 4 | Fix `scan_limit` with a running-total counter | correctness: 200x over-read | trivial | **done** |
| 3 | Send `mask.fieldPaths` / `select.fields` for projected columns | 28.24 MiB → ~0.9 MiB at 1-of-40 | medium | **done** |
| 5 | Prefetch page N+1 while converting page N | remaining 1.5 s of RTT | medium | **superseded by #12** |
| 6 | Decode `mapValue` — `map_encoding` = wire / json / variant | dot access, types preserved, +7% scan | medium | **done (prototype)** |
| 7 | Raise/expose the inference sample; warn on unsampled fields | silent loss of a field on 75% of docs | small | **done** |
| 8 | Single `find()` per field; `std::move` document fields | ~2 map probes/field/row; ~2x page memory | trivial | **done** |
| 9 | Paginate collection-group scans | correctness: silently truncated at 1000 documents | small | **done** |
| 10 | Stream schema inference instead of buffering the sample | `schema_sample_size:=-1` held the whole collection at bind time | small | **done** |
| 11 | Configurable + self-limiting page size (`page_size`, `firestore_page_byte_budget`) | a page of 1 MiB documents is ~1 GiB before parsing | small | **done** |
| 12 | Parallel scan by `__name__` range | 2.33 s -> 1.23 s at 50 ms RTT, 20k documents | large | **done** |

---

## 4. Implemented: #1 connection reuse and #4 `scan_limit`

### #4 — `scan_limit` (correctness)

`FirestoreScanGlobalState` gained a `rows_emitted` counter that survives page
turnover; the limit check reads it instead of `current_index`.

| `scan_limit` | rows before | rows after | docs fetched before → after |
|---|---|---|---|
| 500 | 500 | 500 | 600 → 600 |
| 1000 | 1000 | 1000 | 1100 → 1100 |
| 1001 | **200,000** | **1001** | 200,100 → 2,100 |
| 2048 | **200,000** | **2048** | 200,100 → 3,100 |
| 5000 | **200,000** | **5000** | 200,100 → 5,100 |
| 50000 | **200,000** | **50,000** | 200,100 → 50,100 |

Fetching is now proportional to the limit. The residue (e.g. 2,100 documents for
a limit of 1,001) is one 1000-document page of unavoidable over-read plus the
100-document schema sample.

### #1 — connection reuse (performance)

`httplib::Client` moved from a per-request local in `MakeRequest` to a member
built once per host, plus `set_keep_alive(true)` — httplib defaults
`keep_alive_` to `false` and sends `Connection: close`, so hoisting the object
alone would not have reused the socket.

50,000-document scan, 51 requests:

| metric | before | after |
|---|---|---|
| TCP connections | 52 | 3 |
| max requests on one connection | 1 | 50 |
| loopback, no latency | 0.228 s | 0.224 s |
| WAN: 15 ms/request + 40 ms/connection | 4.255 s | **1.519 s** |

**2.8x faster** under simulated WAN, no change on loopback. The 3 remaining
connections are the scan's client (50 requests), the separate `FirestoreClient`
that `FirestoreScanBind` builds for schema inference (1), and the harness's own
reset call — sharing one client between bind and execution would remove one more.

### #2 — gzip (transfer)

`CPPHTTPLIB_ZLIB_SUPPORT` defined alongside `CPPHTTPLIB_OPENSSL_SUPPORT`, plus
`find_package(ZLIB)` / `ZLIB::ZLIB` and a `zlib` entry in `vcpkg.json`. httplib
then advertises `Accept-Encoding: gzip, deflate` automatically and inflates
responses before the extension sees them (`decompress_` already defaulted to
`true`), so no call-site changes were needed.

50,000-document scan: all 51 requests negotiate gzip, **21.05 MiB → 1.56 MiB
(13.5x)**.

**Compression is not free, and on a fast link it can lose.** Time depends on
whether bandwidth or CPU is the constraint:

| link | gzip off | gzip on | effect |
|---|---|---|---|
| loopback (unthrottled) | 0.207 s | 0.262 s | **1.27x slower** |
| 1 Gbps | 0.529 s | 0.283 s | 1.9x faster |
| 100 Mbps | 2.789 s | 0.503 s | **5.5x faster** |

Loopback has no bandwidth to save, so only the inflate cost shows. That case is
the Firestore *emulator*; production Firestore is always remote, where the win
is large. If local-emulator throughput ever matters, this could be made
conditional on `FIRESTORE_EMULATOR_HOST` — not done, as the absolute cost is
~55 ms per 50k documents.

No symbol conflict with DuckDB's vendored compression: DuckDB uses `miniz`,
whose symbols live under `duckdb_miniz`/`mz_*`, not zlib's `inflate`/`deflate`.

### #6 — `map_encoding` (prototype)

New named parameter with three modes. `'wire'` stays the default, so nothing
existing changes unless asked.

| mode | column type | reaching a leaf 2 deep |
|---|---|---|
| `wire` (default) | VARCHAR | `$.child.mapValue.fields.leaf0.stringValue` |
| `json` | JSON | `$.child.leaf0` |
| `variant` | VARIANT | `payload.child.leaf0` |

VARIANT is built by recursing into `VariantValue` and calling
`VariantValue::ToVARIANT` once per chunk — DuckDB constructs a VARIANT vector
from a whole chunk, not cell by cell, so the scanner accumulates one
`VariantValue` per emitted row. A default-constructed (MISSING) entry becomes
SQL NULL, which is what a document lacking the field should produce.

**Scan cost, 20,000 documents** (median of 3, schema cache warm):

| shape | wire | json | variant | variant vs wire |
|---|---|---|---|---|
| map depth 1 | 0.120 s | 0.123 s | 0.116 s | −3% |
| map depth 4 | 0.193 s | 0.204 s | 0.198 s | +3% |
| map depth 8 | 0.289 s | 0.320 s | 0.310 s | +7% |
| map, 16 flat leaves | 0.281 s | 0.299 s | 0.293 s | +4% |

Isolating conversion at depth 8 against the shared no-materialise baseline
(0.240 s):

| mode | total | conversion |
|---|---|---|
| wire | 0.290 s | 0.050 s |
| json | 0.320 s | 0.080 s |
| variant | 0.321 s | 0.081 s |

So VARIANT costs ~1.6x the *conversion* work of a raw `.dump()`, but conversion
is only ~17% of the scan, so end-to-end it is +7% at depth 8 and free at depth 1.
That is the price for dot access and type fidelity.

**Type fidelity** — verified on scanned data: `variant_typeof` reports `INT64`
for Firestore's string-encoded integers, plus `DOUBLE` and `BOOL_FALSE`/
`BOOL_TRUE`. Documents with differing keys, or differing types at the same path,
are preserved; a missing key yields SQL NULL rather than an error. No fixed
schema is involved, so unlike a STRUCT mapping there is no silent key dropping.

The schema cache key includes the encoding — without that, switching
`map_encoding` inside one session would reuse the previous schema. Verified:
`wire → variant → json → wire` in a single session yields
`VARCHAR / VARIANT / JSON / VARCHAR`.

Known limitation: a LIST child cannot be VARIANT, so maps nested *inside arrays*
render as natural JSON strings under `'variant'` rather than as VARIANT.

### #7 — schema sampling (silent field loss)

`InferSchema` issued exactly one page request capped at `min(sample_size, 1000)`
with the caller passing 100 — so a field appearing later simply was not a
column, with no error and no warning.

Four changes:

1. **Real pagination + configurable depth.** `InferSchema` now pages until the
   sample is filled or the collection is exhausted. `schema_sample_size:=N`
   (named parameter) or `firestore_schema_sample_size` (setting); `-1` samples
   every document. **Default raised 100 → 1000**, which costs the same single
   request it already made.
2. **A warning on every unmapped field**, once per distinct name per scan.
3. **`unmapped_column:=true`** appends a `__unmapped` catch-all, typed to follow
   `map_encoding` (VARIANT or JSON), NULL when a document has no extra fields.
4. **`columns:={'name':'TYPE'}`** declares the schema outright, skipping
   inference and its request.

Verified against the mock (`late` shape now takes the appearance index as its
parameter, so a field can be placed past the sample window):

| case | before | after |
|---|---|---|
| field at doc 500 / 2000 (the reported bug) | absent | **in the schema** |
| field at doc 1500 / 3000, default sample | absent, silent | absent but **warns by name** |
| …with `schema_sample_size:=-1` | absent | **in the schema** |
| …with `unmapped_column:=true` | absent | **recoverable** via `__unmapped` |

`columns:={...}` also removes the bind-time sampling request: 4 requests /
4,000 documents → 3 requests / 3,000.

**Cost of the always-on detection: none measurable.** The check is a linear
merge of the document's fields against a sorted column list — both sides are
already key-sorted (nlohmann objects are `std::map`-backed), so there is no
hashing or allocation. 200,000 documents × 8 columns, gzip disabled to match the
original baseline conditions: **1.026–1.054 s vs the 1.040 s baseline**, i.e.
within noise. (A naive reading of the gzip-enabled scaling run suggests +20%,
but that delta is gzip's loopback decompression, not this check.)

The schema cache key gained `schema_sample_size`, `unmapped_column` and
`show_missing` — the last of which was **already missing before this change**,
so two scans differing only in `show_missing` previously shared a cache entry.
Verified in one session: default → 4 columns, `-1` → 5, `unmapped_column` → 5,
`show_missing:=false` → 4, default again → 4.

Known limitation: collection-group scans (`~`) use `runQuery`, which has no
page-token pagination, so their sample stays bounded by one request of ≤1000
documents whatever `schema_sample_size` says. Documented.

### Verification

- 9 SQL test files, 253 assertions (11 new sampling/columns cases) — pass.
- Full Firebase-emulator integration suite (74 tests, including insert, update,
  delete and batch writes) — pass. This matters because connection reuse changes
  the transport for *every* verb, not just the GETs a scan issues.
- WASM (`make wasm_eh`, emsdk 3.1.71) — **builds clean and verified**:
  - `node test/wasm/validate_wasm_module.mjs` — compiles as valid WebAssembly,
    is a SIDE_MODULE, exports the entrypoint, and **no OpenSSL/socket symbols
    across 828 imports**.
  - `node test/wasm/validate_wasm_functional.mjs` — 4 checks pass; the extension
    installs, loads and registers its functions under DuckDB-WASM.
  - zlib provably stayed out: zero `zlib` mentions in the WASM build log, no
    `ZLIB` entries in `build/wasm_eh/CMakeCache.txt`, and 0 zlib symbols in
    `libfire_duck_ext_extension.a` (the native archive has 10).
  - By construction: `find_package(ZLIB)` and the `ZLIB::ZLIB` link are inside
    the existing `if (NOT CLANG_TIDY AND NOT EMSCRIPTEN)` guards,
    `CPPHTTPLIB_ZLIB_SUPPORT` is defined inside the `#else` (non-WASM) arm of the
    transport `#ifdef`, `vcpkg.json` marks zlib `"platform": "!emscripten"`, and
    the keep-alive client, its accessor and the httplib forward declaration are
    all behind `#ifndef __EMSCRIPTEN__`. The WASM path still reaches the network
    through DuckDB's `HTTPUtil`, and the browser's `fetch()` negotiates and
    decodes gzip on its own — so WASM gets compression without linking zlib.
- `description.yml` and `README.md` reviewed per `CLAUDE.md`: both already
  document `scan_limit` as an honoured fetch limit, so the fix brings the
  implementation in line with the existing text rather than changing it. No
  documentation edit needed; connection reuse is internal.

---

## 5. Implemented: #8, #9, #10, #11 — bounded memory on large collections

### #9 — collection-group scans stopped at 1000 documents (correctness)

`CollectionGroupQuery` issued one `:runQuery` with `limit` set to the page size
and returned no continuation, and `InitGlobal` set `next_page_token = ""` with
`uses_run_query` left false — so the scan loop took its "no page token, we're
done" branch after the first page. `SELECT count(*)` over a collection group
larger than one page returned 1000 and reported success.

Measured against the mock, before and after:

| collection group | before | after |
|---|---|---|
| 300 documents | 300 | 300 |
| 2,000 documents | 1,000 | 2,000 |
| 4,500 documents | **1,000** | **4,500** |

Collection groups now go through the same cursor pagination the filter-pushdown
path already used: the query is built with `__name__` appended to its ordering
(making the order total, so a page boundary cannot drop or repeat a document),
and each subsequent page resumes from a `startAt` cursor built from the last
document. `count(DISTINCT __document_id)` equals `count(*)` across page
boundaries.

The same cursor path fixed collection-group *schema inference*, which was
bounded by a single request: a field first appearing at document 1500 was
invisible to `schema_sample_size:=-1` on a `~collection` scan, and is now
found.

### #10 — schema inference streamed

`InferSchema` accumulated every sampled document into one vector before walking
it, so `schema_sample_size:=-1` pulled an entire collection into memory *at
bind time* — before any `LIMIT` or `WHERE` could reduce it. It now folds each
page into a `FirestoreSchemaAccumulator` and releases it, so peak memory is one
page whatever the sample depth. Inferred types are unchanged: first-seen type
per field, array element type by majority with ties broken alphabetically,
vector dimension from the first occurrence that carries one.

### #11 — page size is configurable, and self-limiting

The page size was hardcoded at 1000 in three places. It is now `page_size:=N`
(named parameter) or `firestore_page_size` (setting), clamped to Firestore's
1–1000 range, and it governs the bind-time sampling request as well — the
request that runs first and cannot be reduced by a filter.

`firestore_page_byte_budget` (default 64 MiB, 0 disables) covers the case where
document sizes are not known in advance: after each page the scan compares the
uncompressed body size against the budget and, if it is over, reduces the page
size to roughly `budget / observed bytes per document`. Verified against the
mock's `fat` shape — 20 KiB documents, 2 MiB budget:

| request | documents asked for |
|---|---|
| schema sample | 10 |
| first page | 1000 |
| every page after | 156 |

All 2,200 rows still arrive. The page size only decreases within a scan;
growing it back would spend round trips rediscovering a limit already found.
On ordinary collections no page approaches the budget, so nothing shrinks and
no round trips are added — asserted by a test, so the guard cannot start
costing round trips unnoticed.

### #8 — per-row and per-page overhead

`ParseDocument` took its JSON by const reference and deep-copied
`doc_json["fields"]` while the parsed response was still alive, roughly
doubling peak memory per page; it now takes an rvalue reference and moves.
Field lookup in the scan loop did `contains()` then `operator[]` — two ordered
probes per column per row — and now does one `find()`.

### Testing

The pure logic (paging policy, cursor construction, orderBy construction,
schema accumulation, wire-format helpers) was moved into three DuckDB-free
modules so it can be exercised directly: `scripts/run_unit_tests.sh` compiles
them with `--coverage`, runs 58 cases, and enforces a per-file threshold.
`test/integration/large_collections.py` covers the scanner and client glue
through a real DuckDB against the mock, asserting on request counts and page
sizes as well as rows. `scripts/run_coverage.sh` merges both runs and reports
coverage of the lines this change adds or modifies — 98% at the time of
writing; the remainder is `GetDocument`/`CreateDocument` (covered by the
emulator suite, not the mock) and a `FunctionData::Equals` clause.

---

## 6. Implemented: #3 — projection reaches the wire

`projection_pushdown = true` was set, so DuckDB skipped converting unselected
columns, but no mask was ever sent: every field of every document was
transferred regardless of what the query asked for.

The scan now builds a `FirestoreProjection` from `bind_data.projected_columns`
and sends it as `mask.fieldPaths` on `documents.list` and `select.fields` on
`runQuery`. Measured against the mock, 5,000 documents of 40 fields, with the
schema sample held at 5 documents so the unmasked bind-time request does not
dominate:

| query | transferred |
|---|---|
| `count(*)` over `SELECT *` (40 columns) | 7.03 MiB |
| `count(f0)` (1 column) | **1.11 MiB** |
| `count(f0..f3)` (4 columns) | 1.52 MiB |

6.3x on a 1-of-40 projection. The floor is document names and timestamps,
which a mask cannot remove.

Three details the implementation has to get right:

- **Field names are not identifiers.** A Firestore field may be called `a.b`,
  and a field path is dot-separated, so unquoted it addresses `b` inside a map
  called `a` — the column would come back empty rather than wrong. Names that
  are not simple identifiers are backtick-quoted (escaping backticks and
  backslashes) and percent-encoded into the query string. Names beginning
  `__` are quoted too: unquoted, `__name__` means the document's resource
  name, not a field of that name.
- **`unmapped_column:=true` cannot be masked** — that column is defined as
  every field the schema does not cover, so a mask would empty it by
  construction. The projection is dropped entirely in that case.
- **Keys-only is not expressible on `documents.list`.** An absent mask means
  "all fields" and a URL cannot carry an empty repeated parameter, so a query
  needing no fields sends no mask there. `runQuery` has the documented
  keys-only form (`select __name__`) and uses it.

The mock now honours masks and `select` clauses, including unquoting backticks
the way Firestore does — without that it would look for a key spelled with the
backticks still on, and a quoting bug in the extension would show up as an
empty column rather than a failure.

---

## 7. Implemented: count pushdown

`SELECT count(*)` used to read every document to count them: projection
pushdown meant DuckDB never converted the values, but all of them still
crossed the wire. Firestore's `:runAggregationQuery` returns the number
directly.

200,000 documents, against the mock:

| | requests | documents | transferred | time |
|---|---|---|---|---|
| before | 200 | 200,000 | 84.24 MiB | 11.25 s |
| after | **1** | **0** | **0** | **0.018 s** |

### Recognising the case

The projection cannot reveal it. DuckDB does not ask a table function for zero
columns; for `count(*)` it projects the first column, which here is
`__document_id` — indistinguishable from someone selecting it. So the
optimizer extension recognises the plan shape instead: a `LogicalAggregate`
with no groups and exactly one `count_star` expression, no DISTINCT and no
FILTER, above the scan with nothing but projections in between. A FILTER or
LIMIT between the aggregate and the scan changes which rows are counted, so
the flag stops there.

### The phantom-document constraint

Aggregation queries never count documents that exist only to parent a
subcollection. A scan with `show_missing` (the default) returns them as rows.
So on an ordinary collection the count is pushed only with
`show_missing:=false` — otherwise the fast answer would quietly differ from
the slow one. Collection groups never included phantom documents, so they are
always eligible.

`upTo` carries an effective `scan_limit` into the request so Firestore stops
counting early, and the result is capped client-side regardless.

A deployment without the endpoint (older emulators, restricted credentials)
answers 501; the scan falls back to reading documents, which is slower and
never wrong.

---

## 8. Implemented: #12 — parallel scan by key range

Firestore's REST pagination is sequential by construction: the next page needs
the previous page's cursor. `MaxThreads()` returned 1 and pages were fetched
inline on the execution thread, so a large scan was a chain of round trips with
nothing overlapping them.

Ranges of the key space are independent, so the scan now cuts the collection
into ranges and reads several at once. Each thread claims a range, pages
through it with its own client and its own paging policy, and claims another
when it finishes. There are four times as many ranges as threads, so a thread
that draws a light range takes the next one instead of idling.

20,000 documents against the mock with 50 ms per request and 80 ms per
connection:

| threads | time |
|---|---|
| 1 | 2.33 s |
| 2 | 1.57 s |
| 4 | 1.23 s |
| 8 | 1.18 s |

Real speedup is bounded here by the mock itself (single Python process), and
the flattening past four threads is partly that. Parallel scanning also issues
*more* requests than sequential — 32 against 21 for 20,000 documents — because
each range's last page is partial; it wins by overlapping them, not by making
fewer.

### Where the correctness sits

- **Boundary ownership.** Firestore's cursor `before` flag means opposite
  things on a start and an end cursor. The range's lower bound is `startAt`
  (`before: true`, inclusive) and its upper bound `endBefore` (`before: true`,
  exclusive), so a document whose id is exactly a boundary is read by the range
  starting there and skipped by the one ending there. Both flags the same and
  the document is either read twice or lost — the first draft had this wrong.
- **Coverage.** The first range has no lower bound and the last no upper one,
  so every id lands somewhere whatever characters it uses. A unit test asserts
  exactly-one-range membership over ids spanning the whole byte ordering, at
  several partition counts.
- **Balance, separately.** Boundaries are cut evenly over Firestore's auto-id
  alphabet. Hand-chosen keys pile into one range: still correct, just not
  faster.

### Where it does not apply

`show_missing:=false` is required, because ranges are cursors and only
`runQuery` supports those, and `runQuery` never returns phantom documents while
a `show_missing` scan does. Also excluded: an ORDER BY (undone by reading
ranges concurrently), a LIMIT (not enforceable per range), a pushed filter
(needs its own ordering, which conflicts with ordering by `__name__`),
collection groups (names span parent paths) and document paths. Everything else
scans sequentially exactly as before.

### Prerequisite: credentials were not thread-safe

`RefreshTokenIfNeeded` mutated the shared `FirestoreCredentials` with no lock,
and every client built from a secret shares one. Sequentially that never
mattered; with one client per thread, an expiring service-account or Firebase
token would be read while another thread rewrote it. The cached token is now
guarded by a mutex held across the refresh, so one thread refreshes and the
rest wake to a valid token.

### On the mock

Its cursor handling was an approximation — it recovered an offset by parsing a
`/docNNNNNNNN` suffix. It now holds each collection's ids in sorted order and
resolves `startAt`/`endAt` by bisecting them, honouring `before` on both sides;
otherwise it could not have caught the boundary bug above. Collections may also
be prefixed `auto_` to give documents Firestore-shaped auto-ids, since
sequential `doc00000000` keys all sort into a single range.

---

## 9. A/B against the pre-change binary

Everything above was measured while the work was being done, mostly by turning
a feature off and on within one build. That controls for machine and compiler
but is not an old-commit-vs-new-commit comparison, and none of it came from the
benchmark harness -- which was not run at all, so nothing checked whether the
untouched paths had got slower.

This section is the real thing: `bench/run_bench.py` run twice against the same
mock, once with a binary built from `072e51e` (the branch point) and once from
the branch, with `DUCKDB_BIN` selecting between them. Scenarios use only syntax
both builds understand, so the contrast comes from the binary rather than from
different SQL.

```bash
python3 bench/mock_firestore.py 8099 &
git checkout main && make release && cp build/release/duckdb /tmp/duckdb-base
git checkout - && make release
DUCKDB_BIN=/tmp/duckdb-base python3 bench/run_bench.py --json before.json
python3 bench/run_bench.py --json after.json
python3 bench/compare_ab.py before.json after.json
```

The `MiB` column is bytes on the wire, i.e. after gzip. Elsewhere in this
document the figures quoted from the mock's own counters are uncompressed;
they measure different things and are not comparable to each other.

### What got faster

| scenario | before | after | |
|---|---|---|---|
| `count(*)` over 200k documents, `show_missing:=false` | 11.117 s, 200 requests, 200,000 documents | **0.002 s, 1 request, 0 documents** | the aggregation query |
| 1 of 40 columns, 20k documents | 3.18 MiB | **0.31 MiB** | 10.3x fewer bytes |
| all 40 columns, 20k documents | 1.319 s | 1.045 s | 1.26x |
| scan of 20k auto-id documents @20 ms RTT | 1.446 s | 0.808 s | 1.79x, opt-in |

`count(*)` with the default `show_missing` is unchanged at ~11 s, which is the
intended behaviour: a scan returns phantom documents that an aggregation query
would not count, so it stays on the reading path.

### What the A/B caught

**Parallel scanning regressed clustered keys by 2x**, and was on by default.
Measured over 20,000 documents on loopback: auto-ids 1.08 s -> 0.67 s on four
threads, but sequential ids 1.02 s -> **2.05 s**. Every range but one comes
back empty, the work funnels through one thread regardless, and runQuery's
per-document envelope costs about 22% more bytes than documents.list (5.44 MiB
against 4.41 MiB uncompressed). The default is now 1, and the README no longer
claims clustered keys are merely "no faster".

**`ParseDocument`'s `std::move` costs about 17% on array-heavy documents.**
Introduced to avoid deep-copying a page's fields while the parsed response is
still alive -- roughly halving peak memory per page -- it turns out to cost
real time on documents with large JSON structures, and nothing on scalar ones:

| `count(*)` over 20k documents | median | vs branch point |
|---|---|---|
| `072e51e` (copies) | 1.408 s | 1.00x |
| branch HEAD (moves) | 1.652 s | **1.17x slower** |
| HEAD with only that line reverted | 1.362 s | 0.97x |

Localised by interleaved measurement (base and branch alternating, 7-9 pairs,
median, run-to-run spread 3-6%): the gap is in the scan, not in schema
inference -- binding alone is 0.79x, i.e. faster -- and the wire traffic is
byte-identical, 21 requests and 39.78 MiB uncompressed either way. Inlining the
wire-format helpers that the same commit moved into their own translation unit
does not recover it, so cross-translation-unit call overhead is not the cause.
The likely mechanism is locality: copying produces a fresh compact subtree and
frees the parsed response as a block, while moving leaves each document's
fields pointing into nodes scattered through the response's allocations.

This is a genuine trade-off -- roughly 2x peak page memory against ~17% CPU on
array-heavy scans -- and it is left as it is for now, because bounding memory
is what this branch set out to do and the page-byte budget only shrinks a page
after it has already been held. Reproduce with:

```sql
SELECT count(*) FROM firestore_scan('bench_arr_64_20000',
    project_id:='bench-project', api_key:='benchkey');
```

### Regression check

Every other group is unchanged within noise. Re-measured at 5 repeats after the
first sweep at 1 repeat showed wobbles: map depths 1-8, array widths 1-64, the
nested-shape set, `scaling` at 1k/10k/50k/200k documents, both limit groups and
the wire group all come out `=`. So the row-emission refactor into shared
helpers, and the change from `contains()` plus `operator[]` to a single
`find()` per field, cost nothing measurable -- and a projected scalar scan
(`count(f0)` over flat8) is 15% *faster* thanks to the field mask.

One measurement lesson worth recording: at one repeat the sweep reported
`map depth 16` and `array 64 elems [no materialize]` as 1.28x and 1.39x slower,
and `map depth 16` as *faster* than `map depth 8` on the base binary, which is
impossible. Single-run medians on a shared container are not trustworthy at
this effect size; the numbers above come from interleaved runs with the spread
reported alongside.
