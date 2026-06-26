// Shared DuckDB-WASM loader for the fire_duck_ext validation/runner scripts.
//
// Encapsulates: locating the built `.wasm`, serving it over a tiny HTTP server
// (the async DuckDB-WASM runtime fetches extensions over HTTP), and installing +
// loading it into a fresh connection. Used by validate_wasm_functional.mjs and
// run_sql.mjs so the loading logic lives in one place.

import { readFile } from "node:fs/promises";
import { existsSync, statSync, globSync } from "node:fs";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const REPO_ROOT = path.resolve(__dirname, "..", "..");
export const EXT_NAME = "fire_duck_ext";
export const EXT_FILENAME = `${EXT_NAME}.duckdb_extension.wasm`;

export function resolveArtifact(argPath) {
	const candidate = argPath || process.env.EXT_WASM;
	if (candidate) {
		if (!existsSync(candidate)) throw new Error(`Artifact not found: ${candidate}`);
		return candidate;
	}
	const found = globSync(`build/wasm_*/**/${EXT_FILENAME}`, { cwd: REPO_ROOT })
		.map((p) => path.join(REPO_ROOT, p))
		.filter((p) => existsSync(p))
		.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
	if (found.length === 0) {
		throw new Error(
			`No ${EXT_FILENAME} found under build/wasm_*/. Build it (e.g. \`make wasm_eh\`), ` +
				`pass a path, or set EXT_WASM.`,
		);
	}
	return found[0];
}

// Serve the extension for any request path ending in the extension filename, so we
// do not depend on the exact URL DuckDB-WASM constructs for a custom repository.
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
		throw new Error(`Could not locate the DuckDB-WASM 'eh' Node bundle under ${DIST}.`);
	}
	return { mainModule: path.join(DIST, mainModule), mainWorker: path.join(DIST, mainWorker) };
}

// Instantiate DuckDB-WASM, INSTALL + LOAD the extension, and return an open
// connection plus the handles. Callers force-exit (process.exit) when done — the
// DuckDB-WASM worker can stall on terminate(), so graceful teardown is avoided.
export async function connectWithExtension(artifactPath) {
	const artifact = resolveArtifact(artifactPath);
	const extBytes = await readFile(artifact);

	const duckdb = await import("@duckdb/duckdb-wasm");
	const { default: Worker } = await import("web-worker");

	const server = await startExtensionServer(extBytes);
	const repo = `http://127.0.0.1:${server.address().port}`;

	const bundle = selectEhBundle();
	// `type: "module"` makes web-worker load DuckDB's CommonJS Node worker via
	// import() (so require/module are available) instead of importScripts.
	const worker = new Worker(bundle.mainWorker, { type: "module" });
	const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
	await db.instantiate(bundle.mainModule);
	await db.open({ allowUnsignedExtensions: true });

	const conn = await db.connect();
	// Tolerate a patch-level skew between the extension's build version and the
	// DuckDB version bundled by @duckdb/duckdb-wasm (both must share a minor).
	await conn.query("SET allow_extensions_metadata_mismatch=true").catch(() => {});
	await conn.query(`SET custom_extension_repository='${repo}'`);
	await conn.query(`INSTALL ${EXT_NAME}`);
	await conn.query(`LOAD ${EXT_NAME}`);

	return { artifact, duckdb, db, conn, worker, server };
}
