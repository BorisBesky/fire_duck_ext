#!/usr/bin/env node
// Serves the DuckDB-WASM browser validator (test/wasm/browser/index.html) plus the
// locally-built fire_duck_ext .wasm, so you can validate the extension in a real
// browser — including whether Firestore `:runQuery` / Firebase `:signUp` URLs behave
// there (they should; the 404 seen under Node is a duckdb-wasm Node-runtime quirk).
//
// Usage:
//   node test/wasm/serve_browser.mjs [path/to/fire_duck_ext.duckdb_extension.wasm]
//   # then open the printed http://localhost:PORT/ URL
// Env: PORT (default 8321), EXT_WASM (artifact path override)

import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveArtifact, EXT_FILENAME, REPO_ROOT } from "./loader.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const artifact = resolveArtifact(process.argv[2]);
const extBytes = await readFile(artifact);
const indexHtml = await readFile(path.join(__dirname, "browser", "index.html"));
const PORT = Number(process.env.PORT || 8321);

const server = createServer((req, res) => {
	const url = (req.url || "/").split("?")[0];
	res.setHeader("Access-Control-Allow-Origin", "*");
	if (url === "/" || url === "/index.html") {
		res.setHeader("Content-Type", "text/html; charset=utf-8");
		res.end(indexHtml);
	} else if (url.endsWith(EXT_FILENAME)) {
		// Catch-all: return the artifact regardless of the version/arch path DuckDB-WASM builds.
		res.setHeader("Content-Type", "application/wasm");
		res.end(extBytes);
	} else {
		res.statusCode = 404;
		res.end("not found");
	}
});

server.listen(PORT, "127.0.0.1", () => {
	console.log("fire_duck_ext — DuckDB-WASM browser validator");
	console.log(`  artifact: ${path.relative(REPO_ROOT, artifact)} (${extBytes.length} bytes)`);
	console.log(`  open:     http://localhost:${PORT}/`);
	console.log("  (Ctrl-C to stop)");
});
