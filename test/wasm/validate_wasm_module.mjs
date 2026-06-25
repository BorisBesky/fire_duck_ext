#!/usr/bin/env node
// Structural validation of the DuckDB-WASM build of fire_duck_ext.
//
// "Compiling isn't running" — but a number of regressions are visible purely in
// the shape of the produced `.wasm` side module, with no runtime required. This
// script validates exactly those, using only Node built-ins (no npm install):
//
//   1. The artifact is a valid WebAssembly module (it compiles).
//   2. It is an Emscripten SIDE_MODULE (has a `dylink.0` custom section).
//   3. It imports NO OpenSSL / raw-socket symbols. This is the crux of issue #5:
//      the loadable module only resolves the libraries in its import table against
//      the host DuckDB module, so any leftover OpenSSL/socket symbol would make the
//      module fail to load. After routing HTTP through HTTPUtil and base64 through
//      DuckDB's Blob, none of these should remain.
//   4. It exports the extension entrypoint (`fire_duck_ext_init*`).
//
// Usage:
//   node validate_wasm_module.mjs [path/to/fire_duck_ext.duckdb_extension.wasm]
// If no path is given, the newest artifact under build/wasm_*/ is used.

import { readFile } from "node:fs/promises";
import { existsSync, statSync } from "node:fs";
import { globSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const EXT_FILENAME = "fire_duck_ext.duckdb_extension.wasm";

// Import names (with an optional leading underscore) that must NOT appear in the
// side module's import table. These are C symbols from OpenSSL and the socket
// syscall surface; Emscripten leaves C symbols unmangled, so prefix matching here
// will not collide with mangled C++ DuckDB symbols (which begin with `_Z`).
const FORBIDDEN_IMPORT_PATTERNS = [
	// OpenSSL
	/^_?EVP_/,
	/^_?BIO_/,
	/^_?PEM_(read|write)/,
	/^_?RSA_/,
	/^_?SSL_/,
	/^_?OPENSSL_/,
	/^_?ERR_(get_error|error_string)/,
	/^_?CRYPTO_/,
	/^_?BN_/,
	/^_?X509_/,
	// mbedTLS / wolfSSL, in case httplib ever pulled an alternative TLS backend
	/^_?mbedtls_/,
	/^_?wolfSSL_/,
	// Raw sockets (Emscripten routes these through __syscall_*)
	/^_?__syscall_(socket|connect|bind|sendto|recvfrom|accept|listen|getsockname|getpeername)\b/,
	/^_?getaddrinfo$/,
	/^_?freeaddrinfo$/,
	/^_?gethostbyname/,
];

function resolveArtifact(argPath) {
	if (argPath) {
		if (!existsSync(argPath)) {
			throw new Error(`Artifact not found: ${argPath}`);
		}
		return argPath;
	}
	const candidates = globSync(`build/wasm_*/**/${EXT_FILENAME}`, { cwd: REPO_ROOT })
		.map((p) => path.join(REPO_ROOT, p))
		.filter((p) => existsSync(p))
		.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
	if (candidates.length === 0) {
		throw new Error(
			`No ${EXT_FILENAME} found under build/wasm_*/. ` +
				`Build it first (e.g. \`make wasm_eh\`) or pass the path explicitly.`,
		);
	}
	return candidates[0];
}

function decodeCustomSectionName(bytes) {
	// A custom section payload starts with a LEB128-length-prefixed UTF-8 name.
	let len = 0;
	let shift = 0;
	let i = 0;
	while (i < bytes.length) {
		const b = bytes[i++];
		len |= (b & 0x7f) << shift;
		if ((b & 0x80) === 0) break;
		shift += 7;
	}
	return Buffer.from(bytes.slice(i, i + len)).toString("utf8");
}

async function main() {
	const artifact = resolveArtifact(process.argv[2]);
	const bytes = await readFile(artifact);
	console.log(`Validating WASM module: ${path.relative(REPO_ROOT, artifact)} (${bytes.length} bytes)\n`);

	const results = []; // { level: 'pass'|'fail'|'info', msg }
	const pass = (msg) => results.push({ level: "pass", msg });
	const fail = (msg) => results.push({ level: "fail", msg });
	const info = (msg) => results.push({ level: "info", msg });

	// 1. Valid WebAssembly.
	let mod;
	try {
		mod = await WebAssembly.compile(bytes);
		pass("module compiles as valid WebAssembly");
	} catch (e) {
		fail(`module is not valid WebAssembly: ${e.message}`);
		report(results);
		process.exit(1);
	}

	const imports = WebAssembly.Module.imports(mod);
	const exports = WebAssembly.Module.exports(mod);

	// 2. Emscripten side module marker.
	const dylink =
		WebAssembly.Module.customSections(mod, "dylink.0").length +
		WebAssembly.Module.customSections(mod, "dylink").length;
	if (dylink > 0) {
		pass("is an Emscripten SIDE_MODULE (dylink section present)");
	} else {
		fail("missing dylink section — does not look like an Emscripten SIDE_MODULE");
	}

	// 3. No OpenSSL / socket imports (the core of issue #5).
	const offending = imports.filter((imp) =>
		FORBIDDEN_IMPORT_PATTERNS.some((re) => re.test(imp.name)),
	);
	if (offending.length === 0) {
		pass(`no OpenSSL/socket symbols in the import table (${imports.length} imports scanned)`);
	} else {
		fail(
			`forbidden OpenSSL/socket imports present (the module would fail to load):\n` +
				offending.map((i) => `      - ${i.module}.${i.name}`).join("\n"),
		);
	}

	// 4. Extension entrypoint exported.
	const entry = exports.find((e) => /fire_duck_ext.*init/i.test(e.name));
	if (entry) {
		pass(`exports the extension entrypoint (${entry.name})`);
	} else {
		fail(
			"no `fire_duck_ext*init*` export found — extension entrypoint missing.\n" +
				`      exports seen: ${exports.map((e) => e.name).join(", ") || "(none)"}`,
		);
	}

	// 5. Signature section (informational — local builds are unsigned).
	const sig = WebAssembly.Module.customSections(mod, "duckdb_signature");
	info(
		sig.length > 0
			? "duckdb_signature custom section present (signed build)"
			: "no duckdb_signature section (unsigned local build — load with allowUnsignedExtensions)",
	);

	return report(results);
}

function report(results) {
	const icon = { pass: "✓", fail: "✗", info: "•" };
	for (const r of results) {
		console.log(`  ${icon[r.level]} ${r.msg}`);
	}
	const failures = results.filter((r) => r.level === "fail").length;
	console.log("");
	if (failures > 0) {
		console.log(`FAILED (${failures} check${failures === 1 ? "" : "s"})`);
		process.exit(1);
	}
	console.log("OK — structural validation passed");
	process.exit(0);
}

main().catch((e) => {
	console.error(`Error: ${e.message}`);
	process.exit(2);
});
