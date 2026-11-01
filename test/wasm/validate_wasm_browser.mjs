#!/usr/bin/env node
// Browser validation of fire_duck_ext under DuckDB-WASM.
//
// The Node layers (validate_wasm_module.mjs, validate_wasm_functional.mjs) prove
// the extension loads and registers, but they cannot prove that it reaches
// Firestore: DuckDB-WASM's HTTP layer issues synchronous XMLHttpRequests, and
// under Node, which has no XMLHttpRequest, every request comes back empty (as a
// 404) without leaving the process. A browser is where DuckDB-WASM really runs,
// so this layer loads the extension into DuckDB-WASM in headless Chromium and
// sends real requests to firestore.googleapis.com, one per transport path:
//
//   GET     documents.list                    firestore_scan('probe')
//   POST    :runQuery                         firestore_scan('~probe')
//   PATCH   (POST + X-HTTP-Method-Override)   firestore_update(...)
//   DELETE                                    firestore_delete(...)
//
// They use a project that does not exist and a key that is not valid, so no
// credentials are needed and nothing is read or written. What is asserted is
// that each request crossed the network and Firestore's own JSON error came
// back and was parsed (HTTP 400 or 403 with Google's message), rather than the
// request failing in the transport or returning an empty body.
//
// The page loads DuckDB-WASM from jsDelivr at the version installed in
// test/wasm/node_modules, so it runs the same pinned build as the Node layers
// (see package.json). Not from node_modules itself: the browser build imports
// apache-arrow by bare name, which only a bundler or the CDN's +esm resolves.
//
// Requirements:
//   npm --prefix test/wasm install
//   npx --prefix test/wasm playwright install chromium   (or PLAYWRIGHT_CHANNEL=chrome
//                                                         to drive an installed Chrome)
//
// Usage:
//   node validate_wasm_browser.mjs [path/to/fire_duck_ext.duckdb_extension.wasm]
//
// Env:
//   EXT_WASM              artifact path override
//   PLAYWRIGHT_CHANNEL    e.g. "chrome" to use an installed browser
//   WASM_SKIP_NETWORK=1   skip the Firestore probes (loading DuckDB-WASM itself
//                         still needs jsDelivr)

import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
import path from "node:path";
import { createRequire } from "node:module";
import { resolveArtifact, EXT_FILENAME, EXT_NAME, REPO_ROOT } from "./loader.mjs";

const require = createRequire(import.meta.url);
// Not require("@duckdb/duckdb-wasm/package.json"): the package's exports map omits it.
const DIST = path.dirname(require.resolve("@duckdb/duckdb-wasm"));
const DUCKDB_WASM_VERSION = JSON.parse(readFileSync(path.join(DIST, "..", "package.json"), "utf8")).version;
const SKIP_NETWORK = process.env.WASM_SKIP_NETWORK === "1";

const PROBE_SECRET =
	"CREATE SECRET probe (TYPE firestore, PROJECT_ID 'fire-duck-ext-wasm-probe', API_KEY 'not-a-real-key')";

const NETWORK_PROBES = [
	["GET documents.list", "SELECT * FROM firestore_scan('probe', show_missing:=false)"],
	["POST :runQuery", "SELECT * FROM firestore_scan('~probe')"],
	["PATCH via X-HTTP-Method-Override", "CALL firestore_update('probe', 'doc1', 'field', 1)"],
	["DELETE", "CALL firestore_delete('probe', 'doc1')"],
];

// selectBundle() picks among whatever variants the pinned version ships (eh,
// mvp and coi for builds bundling DuckDB 1.5; base and threads for 2.0). The
// worker is cross-origin, hence the importScripts shim.
const PAGE = `<!doctype html>
<meta charset="utf-8">
<title>fire_duck_ext browser validation</title>
<script type="module">
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION}/+esm";
try {
	const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
	const workerUrl = URL.createObjectURL(
		new Blob(['importScripts("' + bundle.mainWorker + '");'], { type: "text/javascript" }),
	);
	const worker = new Worker(workerUrl);
	const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
	await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
	URL.revokeObjectURL(workerUrl);
	await db.open({ allowUnsignedExtensions: true });
	const conn = await db.connect();
	await conn.query("SET allow_extensions_metadata_mismatch=true").catch(() => {});
	await conn.query("SET custom_extension_repository='" + location.origin + "'");
	await conn.query("INSTALL ${EXT_NAME}");
	await conn.query("LOAD ${EXT_NAME}");
	window.validator = {
		version: await db.getVersion(),
		bundle: bundle.mainModule.split("/").pop(),
		async query(sql) {
			const result = await conn.query(sql);
			return JSON.parse(JSON.stringify(result.toArray(), (_, v) => (typeof v === "bigint" ? Number(v) : v)));
		},
		async error(sql) {
			try {
				await conn.query(sql);
				return null;
			} catch (e) {
				return String(e?.message ?? e);
			}
		},
	};
} catch (e) {
	window.validatorError = String(e?.stack ?? e?.message ?? e);
}
</script>`;

function startServer(extBytes) {
	return new Promise((resolve) => {
		const server = createServer((req, res) => {
			const url = (req.url || "/").split("?")[0];
			// DuckDB-WASM fetches the extension from its worker, whose blob URL
			// makes the request cross-origin.
			res.setHeader("Access-Control-Allow-Origin", "*");
			if (url === "/") {
				res.setHeader("Content-Type", "text/html; charset=utf-8");
				res.end(PAGE);
			} else if (url.endsWith(`${EXT_FILENAME}.gz`)) {
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

async function main() {
	setTimeout(() => {
		console.error("\nTimed out after 180s — forcing exit.");
		process.exit(3);
	}, 180_000).unref();

	const artifact = resolveArtifact(process.argv[2]);
	const extBytes = await readFile(artifact);
	const server = await startServer(extBytes);
	const origin = `http://127.0.0.1:${server.address().port}`;

	const { chromium } = await import("playwright");
	const browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHANNEL || undefined });

	const checks = [];
	const run = async (name, fn) => {
		try {
			const detail = await fn();
			checks.push({ ok: true });
			console.log(`  ✓ ${name}${detail ? `\n      ${detail}` : ""}`);
		} catch (e) {
			checks.push({ ok: false });
			console.log(`  ✗ ${name}\n      ${e.message}`);
		}
	};

	try {
		const page = await browser.newPage();
		// Shown when loading fails: a module that does not resolve or a worker
		// that does not start reports nothing to the page itself.
		const consoleLines = [];
		page.on("console", (message) => consoleLines.push(`[${message.type()}] ${message.text()}`));
		page.on("pageerror", (error) => consoleLines.push(`[pageerror] ${error.message}`));
		page.on("requestfailed", (request) => consoleLines.push(`[requestfailed] ${request.url()}`));

		console.log(
			`Loading ${path.relative(REPO_ROOT, artifact)} into DuckDB-WASM ${DUCKDB_WASM_VERSION} in Chromium\n`,
		);

		await run("extension installs and loads in the browser", async () => {
			await page.goto(origin);
			let failure;
			try {
				await page.waitForFunction(() => window.validator || window.validatorError, null, { timeout: 120_000 });
				failure = await page.evaluate(() => window.validatorError);
			} catch (e) {
				failure = e.message;
			}
			if (failure) throw new Error([failure, ...consoleLines.slice(-15)].join("\n      "));
			const info = await page.evaluate(() => ({ version: window.validator.version, bundle: window.validator.bundle }));
			return `DuckDB ${info.version} (${info.bundle})`;
		});

		if (checks[0].ok) {
			await run("firestore API-key secret can be created", async () => {
				await page.evaluate((sql) => window.validator.query(sql), PROBE_SECRET);
			});

			if (SKIP_NETWORK) {
				console.log("  • Firestore probes skipped (WASM_SKIP_NETWORK=1)");
			} else {
				for (const [name, sql] of NETWORK_PROBES) {
					await run(`${name} reaches Firestore and its error is parsed`, async () => {
						const error = await page.evaluate((statement) => window.validator.error(statement), sql);
						if (error === null) throw new Error("expected Firestore to refuse the probe, but it succeeded");
						// A transport failure, or a reply whose body never arrived, has
						// no status of 400/403 with Google's message attached.
						const status = /status=(\d+)/.exec(error)?.[1];
						if (!["400", "403"].includes(status) || /Unknown error|HTTP request failed/.test(error)) {
							throw new Error(`expected Firestore's HTTP 400/403 error, got: ${error}`);
						}
						return error.replace(/\s*\{.*$/s, "");
					});
				}
			}
		}
	} finally {
		await browser.close().catch(() => {});
		server.close();
	}

	const failures = checks.filter((c) => !c.ok).length;
	console.log("");
	console.log(
		failures > 0
			? `FAILED (${failures} of ${checks.length} checks)`
			: `OK — ${checks.length} browser checks passed under DuckDB-WASM`,
	);
	process.exit(failures > 0 ? 1 : 0);
}

main().catch((e) => {
	console.error(`\nError: ${e.stack || e.message}`);
	process.exit(2);
});
