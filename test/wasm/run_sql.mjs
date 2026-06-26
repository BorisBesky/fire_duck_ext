#!/usr/bin/env node
// Ad-hoc SQL runner against the DuckDB-WASM build of fire_duck_ext.
//
// Loads the locally built extension into DuckDB-WASM, optionally creates an
// API-key `firestore` secret from the environment, then runs your SQL and prints
// the results. Handy for poking at the WASM build the way the real runtime would.
//
// Usage:
//   node run_sql.mjs "SELECT 1 + 1, list_value(1,2,3)"
//   node run_sql.mjs queries.sql                 # arg is an existing .sql file
//   echo "SELECT * FROM firestore_scan('users') LIMIT 5;" | node run_sql.mjs
//
// Firestore I/O (firestore_scan/insert/...) needs a reachable database. Set:
//   FIRESTORE_PROJECT_ID   GCP project id    (with FIRESTORE_API_KEY, creates a secret)
//   FIRESTORE_API_KEY      Firebase API key
//   FIRESTORE_DATABASE     optional database id (default: (default))
// Service-account auth is NOT supported under WASM — API key only.
//
// Other env:
//   EXT_WASM               path to the .wasm artifact (else newest under build/wasm_*/)
//   WASM_SQL_TIMEOUT_MS    watchdog before forcing exit (default 120000)

import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { connectWithExtension, resolveArtifact, REPO_ROOT } from "./loader.mjs";

const TIMEOUT_MS = Number(process.env.WASM_SQL_TIMEOUT_MS || 120_000);

const sqlLiteral = (s) => `'${String(s).replace(/'/g, "''")}'`;

function readStdin() {
	return new Promise((resolve) => {
		if (process.stdin.isTTY) return resolve("");
		let data = "";
		process.stdin.setEncoding("utf8");
		process.stdin.on("data", (c) => (data += c));
		process.stdin.on("end", () => resolve(data));
	});
}

// Naive split on ';' — adequate for a dev helper. Statements containing a literal
// ';' (e.g. inside a string) should be passed one per invocation.
const splitStatements = (sql) =>
	sql
		.split(";")
		.map((s) => s.trim())
		.filter(Boolean);

async function getSql() {
	const arg = process.argv[2];
	if (arg) {
		if (arg.endsWith(".sql") && existsSync(arg)) return readFileSync(arg, "utf8");
		return process.argv.slice(2).join(" ");
	}
	return (await readStdin()).trim();
}

async function main() {
	const watchdog = setTimeout(() => {
		console.error(`\nTimed out after ${TIMEOUT_MS}ms — forcing exit.`);
		process.exit(3);
	}, TIMEOUT_MS);
	watchdog.unref();

	const sql = await getSql();
	if (!sql) {
		console.error(
			'usage: node run_sql.mjs "<SQL>"   (or pipe SQL on stdin, or pass a .sql file)\n' +
				"set FIRESTORE_PROJECT_ID + FIRESTORE_API_KEY to query a real Firestore database.",
		);
		process.exit(2);
	}

	const artifact = resolveArtifact();
	console.error(`# loading ${path.relative(REPO_ROOT, artifact)} into DuckDB-WASM`);
	const { conn } = await connectWithExtension(artifact);

	// Create an API-key secret from the environment, if provided.
	const projectId = process.env.FIRESTORE_PROJECT_ID;
	const apiKey = process.env.FIRESTORE_API_KEY;
	if (projectId && apiKey) {
		const database = process.env.FIRESTORE_DATABASE;
		const opts = [
			"TYPE firestore",
			`PROJECT_ID ${sqlLiteral(projectId)}`,
			`API_KEY ${sqlLiteral(apiKey)}`,
		];
		if (database) opts.push(`DATABASE ${sqlLiteral(database)}`);
		await conn.query(`CREATE SECRET fire_duck_ext_runner (${opts.join(", ")})`);
		console.error(
			`# created firestore API-key secret for project '${projectId}'` +
				(database ? ` (database '${database}')` : ""),
		);
	} else if (projectId || apiKey) {
		console.error("# note: FIRESTORE_PROJECT_ID and FIRESTORE_API_KEY must BOTH be set to create a secret.");
	} else {
		console.error("# note: no Firestore credentials in env — plain SQL works; firestore_* I/O will not.");
	}

	const replacer = (_, v) => (typeof v === "bigint" ? Number(v) : v);
	let hadError = false;
	for (const stmt of splitStatements(sql)) {
		console.log(`\nSQL> ${stmt}`);
		try {
			const rows = (await conn.query(stmt)).toArray();
			if (rows.length === 0) {
				console.log("(no rows)");
				continue;
			}
			for (const r of rows) console.log(JSON.stringify(r, replacer));
			console.log(`(${rows.length} row${rows.length === 1 ? "" : "s"})`);
		} catch (e) {
			hadError = true;
			console.log(`ERROR: ${String(e.message).split("\n")[0]}`);
		}
	}

	process.exit(hadError ? 1 : 0);
}

main().catch((e) => {
	console.error(`\nError: ${e.stack || e.message}`);
	process.exit(2);
});
