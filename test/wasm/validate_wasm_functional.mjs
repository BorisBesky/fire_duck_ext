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
//   4. firestore_update_batch carries its typed (LIST) signature.
//
// What it does NOT cover (by design): live Firestore queries (network/CORS) and
// service-account auth (requires reading a key file, unavailable under WASM — the
// extension intentionally errors there and steers users to API-key auth). To run
// arbitrary SQL — including against a real Firestore database — use run_sql.mjs.
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

import path from "node:path";
import { connectWithExtension, resolveArtifact, REPO_ROOT } from "./loader.mjs";

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

async function main() {
	// Safety net: the DuckDB-WASM worker thread can keep Node alive (or a query can
	// stall on a version mismatch), so never hang indefinitely. unref() so the timer
	// itself doesn't keep the process running.
	setTimeout(() => {
		console.error("\nTimed out after 60s — forcing exit. (Often a DuckDB-WASM/extension version mismatch.)");
		process.exit(3);
	}, 60_000).unref();

	const artifact = resolveArtifact(process.argv[2]);
	console.log(`Loading ${path.relative(REPO_ROOT, artifact)} into DuckDB-WASM\n`);

	const checks = [];
	let handles;
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

	// 1. The extension installs + loads under WASM.
	await run("extension installs and loads under WASM", async () => {
		handles = await connectWithExtension(artifact);
	});

	if (handles) {
		const { conn } = handles;

		// 2. Secret type registered (API-key auth — the supported WASM path).
		await run("firestore API-key secret can be created", async () => {
			await conn.query(`CREATE SECRET wasm_validation (TYPE firestore, PROJECT_ID 'demo', API_KEY 'demo-key')`);
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

		// 4. Functions register with their correct typed signatures. We check that
		//    firestore_update_batch's second argument is a LIST (VARCHAR[]) — the type
		//    that makes a wrong-typed call fail to bind. (Asserting the signature avoids
		//    triggering a binder error, which the worker would noisily log to stderr.)
		await run("firestore_update_batch has its typed LIST signature", async () => {
			const result = await conn.query(
				`SELECT parameter_types FROM duckdb_functions() WHERE function_name = 'firestore_update_batch' LIMIT 1`,
			);
			const rows = result.toArray();
			if (rows.length === 0) throw new Error("firestore_update_batch not registered");
			const types = Array.from(rows[0].parameter_types ?? []).map(String);
			if (!types.some((t) => t.endsWith("[]"))) {
				throw new Error(`expected a LIST parameter, got: [${types.join(", ")}]`);
			}
		});
	}

	const failures = checks.filter((c) => !c.ok).length;
	console.log("");
	console.log(
		failures > 0
			? `FAILED (${failures} of ${checks.length} checks)`
			: `OK — ${checks.length} functional checks passed under DuckDB-WASM`,
	);

	// Best-effort, non-blocking teardown, then hard-exit. We deliberately do NOT
	// `await` worker/db termination: the DuckDB-WASM worker's terminate() can stall
	// and keep Node alive, hanging the process *after* the result is printed.
	// process.exit() tears the worker thread and server down regardless.
	handles?.server?.close();
	process.exit(failures > 0 ? 1 : 0);
}

main().catch((e) => {
	console.error(`\nError: ${e.stack || e.message}`);
	process.exit(2);
});
