#!/usr/bin/env node
// Functional validation of fire_duck_ext under DuckDB-WASM.
//
// This actually instantiates DuckDB-WASM in Node, loads the locally built
// extension, and runs queries — the "running", not just "compiling", half of the
// validation. It deliberately exercises only the paths that need neither network
// nor a filesystem, so it is deterministic in CI:
//
//   1. The extension LOADs under WASM (the headline fix for issue #5).
//   2. The `firestore` secret type is registered (API-key secret creation works).
//   3. All firestore_* table functions are registered.
//   4. Argument validation fires (a wrong-typed call raises a Binder Error).
//
// What it does NOT cover (by design): live Firestore queries (network/CORS) and
// service-account auth (requires reading a key file, unavailable under WASM — the
// extension intentionally errors there and steers users to API-key auth).
//
// Requirements:
//   - A built artifact: build/wasm_eh/.../fire_duck_ext.duckdb_extension.wasm
//     (or pass a path / set EXT_WASM).
//   - `@duckdb/duckdb-wasm` whose bundled DuckDB version matches the version this
//     extension was built against (see test/wasm/README.md). Install with:
//       npm --prefix test/wasm install
//
// Usage:
//   node validate_wasm_functional.mjs [path/to/fire_duck_ext.duckdb_extension.wasm]

import { readFile } from "node:fs/promises";
import { existsSync, statSync, globSync } from "node:fs";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const EXT_NAME = "fire_duck_ext";
const EXT_FILENAME = `${EXT_NAME}.duckdb_extension.wasm`;

const EXPECTED_FUNCTIONS = [
	"firestore_scan",
	"firestore_insert",
	"firestore_update",
	"firestore_delete",
	"firestore_update_batch",
	"firestore_delete_batch",
	"firestore_array_union",
	"firestore_array_remove",
	"firestore_array_append",
	"firestore_connect",
	"firestore_disconnect",
	"firestore_clear_cache",
];

function resolveArtifact(argPath) {
	if (argPath) {
		if (!existsSync(argPath)) throw new Error(`Artifact not found: ${argPath}`);
		return argPath;
	}
	const candidates = globSync(`build/wasm_*/**/${EXT_FILENAME}`, { cwd: REPO_ROOT })
		.map((p) => path.join(REPO_ROOT, p))
		.filter((p) => existsSync(p))
		.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
	if (candidates.length === 0) {
		throw new Error(
			`No ${EXT_FILENAME} found under build/wasm_*/. Build it (e.g. \`make wasm_eh\`) or pass the path.`,
		);
	}
	return candidates[0];
}

// Serve the extension for any request path that ends in the extension filename,
// so we do not depend on the exact URL DuckDB-WASM constructs for a custom repo.
function startExtensionServer(extBytes) {
	return new Promise((resolve) => {
		const server = createServer((req, res) => {
			const url = req.url || "";
			res.setHeader("Access-Control-Allow-Origin", "*");
			if (url.endsWith(`${EXT_FILENAME}.gz`)) {
				res.setHeader("Content-Type", "application/wasm");
				res.setHeader("Content-Encoding", "gzip");
				res.end(gzipSync(extBytes));
			} else if (url.endsWith(EXT_FILENAME)) {
				res.setHeader("Content-Type", "application/wasm");
				res.end(extBytes);
			} else {
				res.statusCode = 404;
				res.end("not found");
			}
		});
		server.listen(0, "127.0.0.1", () => resolve(server));
	});
}

function selectEhBundle() {
	const DIST = path.dirname(require.resolve("@duckdb/duckdb-wasm"));
	const findOne = (candidates) => candidates.find((f) => existsSync(path.join(DIST, f)));
	const mainModule = findOne(["duckdb-eh.wasm"]);
	const mainWorker = findOne(["duckdb-node-eh.worker.cjs", "duckdb-node-eh.worker.js"]);
	if (!mainModule || !mainWorker) {
		throw new Error(
			`Could not locate the DuckDB-WASM 'eh' Node bundle under ${DIST}. ` +
				`Check the @duckdb/duckdb-wasm version.`,
		);
	}
	return { mainModule: path.join(DIST, mainModule), mainWorker: path.join(DIST, mainWorker) };
}

async function main() {
	const artifact = resolveArtifact(process.argv[2]);
	const extBytes = await readFile(artifact);
	console.log(`Loading ${path.relative(REPO_ROOT, artifact)} (${extBytes.length} bytes) into DuckDB-WASM\n`);

	const duckdb = await import("@duckdb/duckdb-wasm");
	const { default: Worker } = await import("web-worker");

	const server = await startExtensionServer(extBytes);
	const { port } = server.address();
	const repo = `http://127.0.0.1:${port}`;

	const bundle = selectEhBundle();
	const worker = new Worker(bundle.mainWorker);
	const logger = new duckdb.VoidLogger();
	const db = new duckdb.AsyncDuckDB(logger, worker);
	await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
	// In-memory is the default when no path is given.
	await db.open({ allowUnsignedExtensions: true });
	const conn = await db.connect();

	const checks = [];
	const run = async (name, fn) => {
		try {
			await fn();
			checks.push({ ok: true, name });
			console.log(`  ✓ ${name}`);
		} catch (e) {
			checks.push({ ok: false, name, err: e.message });
			console.log(`  ✗ ${name}\n      ${e.message}`);
		}
	};

	// 1. The extension loads under WASM.
	await run("extension installs and loads under WASM", async () => {
		await conn.query(`SET custom_extension_repository='${repo}'`);
		// Tolerate a patch-level skew between the extension's build version and the
		// DuckDB version bundled by @duckdb/duckdb-wasm (both must share a minor).
		try {
			await conn.query(`SET allow_extensions_metadata_mismatch=true`);
		} catch (_) {
			/* setting unavailable on this build — ignore */
		}
		await conn.query(`INSTALL ${EXT_NAME}`);
		await conn.query(`LOAD ${EXT_NAME}`);
	});

	// 2. Secret type registered (API-key auth — the supported WASM path).
	await run("firestore API-key secret can be created", async () => {
		await conn.query(
			`CREATE SECRET wasm_validation (TYPE firestore, PROJECT_ID 'demo', API_KEY 'demo-key')`,
		);
	});

	// 3. All firestore_* table functions registered.
	await run("all firestore_* table functions are registered", async () => {
		const result = await conn.query(
			`SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'firestore_%'`,
		);
		const present = new Set(result.toArray().map((r) => r.function_name));
		const missing = EXPECTED_FUNCTIONS.filter((f) => !present.has(f));
		if (missing.length) throw new Error(`missing functions: ${missing.join(", ")}`);
	});

	// 4. Argument validation works (wrong-typed call -> Binder Error, no network).
	await run("wrong-typed call raises a binder error", async () => {
		let threw = false;
		try {
			await conn.query(`SELECT * FROM firestore_update_batch('users', 'not-a-list', 'f', 'v')`);
		} catch (e) {
			threw = true;
			if (!/binder|no function matches/i.test(e.message)) {
				throw new Error(`expected a binder error, got: ${e.message}`);
			}
		}
		if (!threw) throw new Error("expected the wrong-typed call to raise an error");
	});

	await conn.close();
	await db.terminate();
	await worker.terminate();
	await new Promise((r) => server.close(r));

	const failures = checks.filter((c) => !c.ok).length;
	console.log("");
	if (failures > 0) {
		console.log(`FAILED (${failures} of ${checks.length} checks)`);
		process.exit(1);
	}
	console.log(`OK — ${checks.length} functional checks passed under DuckDB-WASM`);
	process.exit(0);
}

main().catch((e) => {
	console.error(`\nError: ${e.stack || e.message}`);
	process.exit(2);
});
