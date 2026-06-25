# WASM validation tests

These tests validate the **DuckDB-WASM** build of `fire_duck_ext`. The native
SQLLogicTests in [`../sql`](../sql) do not exercise the WASM target, and — as the
DuckDB-WASM ecosystem likes to put it — *compiling isn't running*: a WASM build can
pass CI and still fail to load or run. These tests close that gap.

There are two layers, fastest/most-robust first:

| Script | Runtime needed | What it proves |
| --- | --- | --- |
| `validate_wasm_module.mjs` | Node only (no deps) | The `.wasm` is valid, is an Emscripten side module, **imports no OpenSSL/socket symbols** (the core of [issue #5](https://github.com/BorisBesky/fire_duck_ext/issues/5)), and exports the extension entrypoint. |
| `validate_wasm_functional.mjs` | Node + `@duckdb/duckdb-wasm` | The extension actually **loads** under DuckDB-WASM, the `firestore` secret type and all `firestore_*` functions register, and argument validation fires. |

Both deliberately avoid network and filesystem, so they are deterministic. They do
**not** cover live Firestore queries (network/CORS) or service-account auth — under
WASM the extension intentionally errors on service-account auth and steers users to
**API-key auth** (see the extension's WASM notes).

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

### Version matching

`@duckdb/duckdb-wasm` bundles its own copy of DuckDB. For `LOAD` to accept the
extension, that bundled DuckDB must share a **minor version** with the DuckDB this
extension was built against (currently **v1.5.x**; see `duckdb_version` in
[`.github/workflows/MainDistributionPipeline.yml`](../../.github/workflows/MainDistributionPipeline.yml)).
The harness sets `allow_extensions_metadata_mismatch=true` to tolerate a patch-level
skew, but a *minor* mismatch will still be rejected — bump the `@duckdb/duckdb-wasm`
version in [`package.json`](package.json) to match if needed.

## CI

[`.github/workflows/wasm-validation.yml`](../../.github/workflows/wasm-validation.yml)
builds the `wasm_eh` target and runs both layers on every push/PR.
