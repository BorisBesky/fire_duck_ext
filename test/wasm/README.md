# WASM validation tests

These tests validate the **DuckDB-WASM** build of `fire_duck_ext`. The native
SQLLogicTests in [`../sql`](../sql) do not exercise the WASM target, and — as the
DuckDB-WASM ecosystem likes to put it — *compiling isn't running*: a WASM build can
pass CI and still fail to load or run. These tests close that gap.

There are three layers, fastest/most-robust first:

| Script | Runtime needed | What it proves |
| --- | --- | --- |
| `validate_wasm_module.mjs` | Node only (no deps) | The `.wasm` is valid, is an Emscripten side module, **imports no OpenSSL/socket symbols** (the core of [issue #5](https://github.com/BorisBesky/fire_duck_ext/issues/5)), and exports the extension entrypoint. |
| `validate_wasm_functional.mjs` | Node + `@duckdb/duckdb-wasm` | The extension actually **loads** under DuckDB-WASM, the `firestore` secret type and all `firestore_*` functions register, and argument validation fires. |
| `validate_wasm_browser.mjs` | Node + Playwright (Chromium) + network | The extension loads in DuckDB-WASM **in a browser**, and requests to `firestore.googleapis.com` go out over every transport path — GET (`documents.list`), POST (`:runQuery`), PATCH (as POST + `X-HTTP-Method-Override`), DELETE — with Firestore's JSON error coming back and being parsed. |

The first two avoid network and filesystem, so they are deterministic. The browser
layer needs the network but no credentials: its requests name a project that does not
exist with a key that is not valid, so Firestore refuses each one with a 400/403, and
that refusal — with Google's message intact — is what it checks for. Nothing is read
or written. None of the layers covers service-account auth, which the extension
rejects on WASM, steering users to **API-key** or **Firebase-user** auth.

> **Why a browser?** DuckDB-WASM's HTTP layer issues synchronous `XMLHttpRequest`s.
> Node has no `XMLHttpRequest`, and there DuckDB-WASM hands the extension an empty
> 404 for every request without anything leaving the process — so no Node-based test
> (and no `run_sql.mjs` session) can reach Firestore.

## 1. Build the WASM artifact

The structural test needs a built `fire_duck_ext.duckdb_extension.wasm`. The functional
test additionally loads it into a DuckDB-WASM runtime.

```bash
# From the repo root. Produces build/wasm_eh/.../fire_duck_ext.duckdb_extension.wasm
make wasm_eh
```

(`wasm_mvp` / `wasm_threads` work too; the scripts auto-discover the newest artifact
under `build/wasm_*/`, or accept an explicit path / `EXT_WASM`.)

## 2. Structural validation (no dependencies)

```bash
node test/wasm/validate_wasm_module.mjs
# or point at a specific artifact:
node test/wasm/validate_wasm_module.mjs path/to/fire_duck_ext.duckdb_extension.wasm
```

## 3. Functional validation (DuckDB-WASM)

```bash
npm --prefix test/wasm install
node test/wasm/validate_wasm_functional.mjs
```

## 4. Browser validation (DuckDB-WASM in Chromium)

```bash
npx --prefix test/wasm playwright install chromium
node test/wasm/validate_wasm_browser.mjs
# or drive an installed Chrome instead of downloading Chromium:
PLAYWRIGHT_CHANNEL=chrome node test/wasm/validate_wasm_browser.mjs
```

The page loads DuckDB-WASM from jsDelivr at the version installed in
`test/wasm/node_modules`, so both runtimes are the same build.
`WASM_SKIP_NETWORK=1` skips the Firestore probes and checks loading only.

## 5. Running ad-hoc SQL (`run_sql.mjs`)

Loads the WASM build and runs whatever SQL you give it — on argv, from a `.sql`
file, or piped on stdin. It runs under Node, so it cannot reach Firestore (see
*Why a browser?* above); for live queries use `serve_browser.mjs`, which serves an
interactive page to run them in a browser.

```bash
node test/wasm/run_sql.mjs "SELECT 1 + 1, list_value(1,2,3)"
node test/wasm/run_sql.mjs queries.sql
echo "SELECT * FROM firestore_scan('users') LIMIT 5;" | node test/wasm/run_sql.mjs
```

When both `FIRESTORE_PROJECT_ID` and `FIRESTORE_API_KEY` are set, the runner
creates an API-key `firestore` secret before running your SQL (service-account
auth is not supported under WASM):

```bash
FIRESTORE_PROJECT_ID=my-project FIRESTORE_API_KEY=AIza... \
  node test/wasm/run_sql.mjs "SELECT __document_id, * FROM firestore_scan('users') LIMIT 10"
```

| Env var | Meaning |
| --- | --- |
| `FIRESTORE_PROJECT_ID` | GCP project id (with `FIRESTORE_API_KEY`, creates the secret) |
| `FIRESTORE_API_KEY` | Firebase API key |
| `FIRESTORE_DATABASE` | optional database id (default `(default)`) |
| `EXT_WASM` | path to the `.wasm` artifact (else newest under `build/wasm_*/`) |
| `WASM_SQL_TIMEOUT_MS` | watchdog before forcing exit (default `120000`) |

Informational lines are printed to stderr (prefixed `#`) so stdout carries only
the SQL and results.

## Authenticated access on WASM

> **The DuckDB-WASM HTTP layer is not the problem.** Earlier notes here claimed two
> duckdb-wasm transport bugs (`:customMethod` URLs → 404, and `Authorization: Bearer`
> not honored). Both were **wrong** — they were artifacts of a DuckDB **version/ABI
> mismatch** (the extension was built against DuckDB v1.5.0 but loaded into a v1.5.4
> `@duckdb/duckdb-wasm` runtime, force-loaded past the safety check). That mismatch
> corrupted scan execution and surfaced as a `table index is out of bounds` crash and
> misleading 404s. With versions matched (see *Version matching* below), browser
> DuckDB-WASM correctly performs:
> - `listDocuments` reads (colon-free GET),
> - `:runQuery` — WHERE/ORDER pushdown **and** collection-group scans (`~group`,
>   `allDescendants=true`), i.e. colon `:customMethod` URLs reach Firestore fine,
> - Firebase Auth sign-in (`accounts:signInWithPassword`, also a colon `:customMethod`
>   POST) — email/password sign-in then authenticated `SELECT` is verified in-browser,
> - request headers including `Authorization`.

Firestore access tiers:

| Auth mode | Authenticated? | On WASM |
| --- | --- | --- |
| API key | no (`request.auth == null`) | works; Security Rules apply (rules must allow the read) |
| Service account | yes (admin; bypasses rules) | **native only** (needs RS256/OpenSSL) |
| Firebase user — email+password | yes | **verified in-browser** (sign-in is `accounts:signInWithPassword`, a colon `:customMethod`) |
| Firebase user — anonymous | yes | same code path (`accounts:signUp`, colon `:customMethod`); expected to work |
| Firebase user — pre-obtained ID token | yes | works (see *Token passthrough*) |

### Two real gotchas (not transport bugs)

1. **`show_missing:=true` (the default) needs a service account.** Listing phantom/missing
   documents is an Admin-oriented operation; over rules-governed access (API key or
   Firebase user token) Firestore returns `403 PERMISSION_DENIED` **even with
   `allow read: if true`**. Pass `show_missing:=false`:

   ```sql
   SELECT * FROM firestore_scan('my_collection', show_missing:=false) WHERE field = 'x';
   ```

2. **The Firestore Admin API (index metadata) is IAM-gated, not rules-gated.** API-key /
   Firebase-user auth can never reach `…/collectionGroups/…/indexes` (it returns 403).
   The extension now **skips that call entirely** for non-service-account auth and assumes
   Firestore's default single-field indexes, so it no longer emits a doomed 403 request.

### Token passthrough
The extension accepts a **pre-obtained Firebase ID token** (mint it host-side — a normal
browser `fetch` to the Firebase Auth REST API works), so no in-extension sign-in is needed:

```sql
CREATE SECRET fs (
    TYPE firestore,
    PROJECT_ID 'my-project',
    API_KEY 'AIza…',           -- optional; enables auto-refresh via securetoken
    ID_TOKEN 'eyJhbGciOi…',     -- from your host-side sign-in
    REFRESH_TOKEN 'AMf-…'       -- optional; auto-refreshed on expiry
);
SELECT * FROM firestore_scan('my_collection', show_missing:=false) LIMIT 10;
```

The extension sends `Authorization: Bearer <id_token>` for reads and refreshes via
`securetoken` (colon-free) on expiry. **Verified on native** (it authenticates and
respects Security Rules); the WASM transport forwards the same request unchanged.

### Version matching (important)

`@duckdb/duckdb-wasm` bundles its own copy of DuckDB. For `LOAD` to accept the
extension, that bundled DuckDB must share a **minor version** with the DuckDB this
extension was built against (see `duckdb_version` in
[`.github/workflows/MainDistributionPipeline.yml`](../../.github/workflows/MainDistributionPipeline.yml)).
The harness sets `allow_extensions_metadata_mismatch=true` to absorb a *patch*-level
skew (e.g. extension v1.5.0 into runtime v1.5.4), but a *minor* mismatch is rejected.

At the time of writing, **stable `@duckdb/duckdb-wasm` releases still bundle DuckDB
1.4.x**; newer DuckDB is only in `1.33.1-dev*` builds:

| DuckDB | `@duckdb/duckdb-wasm` | Emscripten | Bundle names |
| --- | --- | --- | --- |
| v2.0.0 | `1.33.1-dev110.0` (pinned in [`package.json`](package.json)) | 6.0.9 | `duckdb-base.wasm`, `duckdb-node-base.worker.cjs` |
| v1.5.4 | `1.33.1-dev57.0` | 3.1.71 | `duckdb-eh.wasm`, `duckdb-node-eh.worker.cjs` |

`loader.mjs` accepts either set of bundle names. To validate a DuckDB 1.5 build,
install the matching runtime over the pinned one:
`npm --prefix test/wasm install --no-save @duckdb/duckdb-wasm@1.33.1-dev57.0`.
Bump the pins to stable releases once they ship. To check what a given version bundles:

```bash
node -e "const p=require('path'),r=require('module').createRequire(process.cwd()+'/test/wasm/');\
const d=r('@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs');\
const D=p.dirname(r.resolve('@duckdb/duckdb-wasm'));\
d.createDuckDB({eh:{mainModule:p.join(D,'duckdb-eh.wasm'),mainWorker:null}},new d.VoidLogger(),d.NODE_RUNTIME)\
.then(x=>x.instantiate().then(()=>console.log('bundles DuckDB',x.getVersion())))"
```

### DuckDB 1.5's browser runtime misreports failed requests

Under `@duckdb/duckdb-wasm` `1.33.1-dev57.0` (DuckDB 1.5.4), a request Firestore
refuses reaches the extension as a **404 with no body** (GET, POST) or as **success**
(PATCH, DELETE) — the browser's network log shows the real 403 going by. So on DuckDB
1.5 WASM a write that Firestore rejected can look as if it succeeded. The DuckDB 2.0
runtime (`1.33.1-dev110.0`) reports them correctly, which is why CI only runs the
browser layer's Firestore probes on 2.0. Prefer DuckDB 2.0 for WASM.

### How loading works

The functional harness uses the **async** DuckDB-WASM API (the synchronous
"blocking" API cannot `INSTALL` an extension — fetching is async). It serves the
built `.wasm` from a tiny in-process HTTP server and points
`custom_extension_repository` at it. The Node worker is started with
`new Worker(path, { type: "module" })` so `web-worker` loads DuckDB's CommonJS
worker via `import()` rather than `importScripts` (which would throw
`module is not defined`).

## CI

[`.github/workflows/wasm-validation.yml`](../../.github/workflows/wasm-validation.yml)
builds the `wasm_eh` target for DuckDB 2.0 and 1.5, each with the Emscripten version
DuckDB builds that release with, and runs all three layers against the matching
runtime on every push/PR (the Firestore probes on 2.0 only — see above).
