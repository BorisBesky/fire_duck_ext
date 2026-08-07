#!/usr/bin/env python3
"""
Performance harness for fire_duck_ext: nested datatypes and large collections.

Each scenario runs a query through the real DuckDB shell with the real
extension against the instrumented mock Firestore (bench/mock_firestore.py).
For every scenario we record wall time reported by DuckDB itself (`.timer on`,
so process startup is excluded) plus what the extension put on the wire.

Usage:
    python3 bench/run_bench.py                # full suite
    python3 bench/run_bench.py nested         # one group
    python3 bench/run_bench.py --json out.json
"""

import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DUCKDB = os.path.join(ROOT, "build", "release", "duckdb")
PORT = int(os.environ.get("MOCK_PORT", "8099"))
BASE = f"http://127.0.0.1:{PORT}"
REPEATS = int(os.environ.get("BENCH_REPEATS", "3"))

RUNTIME_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")


# ---------------------------------------------------------------- mock control

def mock_get(path):
    with urllib.request.urlopen(BASE + path, timeout=30) as r:
        return json.load(r)


def mock_post(path):
    req = urllib.request.Request(BASE + path, data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def prewarm(collection, page_size=1000):
    req = urllib.request.Request(
        f"{BASE}/__prewarm?collection={collection}&pageSize={page_size}",
        data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=1800) as r:
        return json.load(r)


def wait_for_mock(timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            mock_get("/__health")
            return True
        except Exception:
            time.sleep(0.2)
    return False


# ---------------------------------------------------------------- duckdb driver

# `.timer on` goes AFTER the LOAD so extension loading is not itself timed --
# otherwise times[0] is the LOAD and every measurement is off by one.
PREAMBLE = """LOAD fire_duck_ext;
.timer on
"""


def shape_columns(collection):
    """Data columns a synthetic collection exposes (mirrors mock_firestore)."""
    _prefix, shape, param, _count = collection.split("_")
    param = int(param)
    if shape in ("flat", "wide", "mixed"):
        return [f"f{j}" for j in range(param)]
    if shape == "late":
        return ["f0", "f1", "f2"]  # param is the appearance index, not a field count
    if shape in ("map", "mapwide"):
        return ["doc_no", "payload"]
    if shape == "arr":
        return ["doc_no", "tags"]
    if shape == "arrint":
        return ["doc_no", "nums"]
    if shape == "vec":
        return ["doc_no", "embedding"]
    raise ValueError(shape)


def materializing_scan(collection, extra=""):
    """
    A query that forces the scan to actually build every column.

    `SELECT count(*)` is NOT usable for this: DuckDB's projection pushdown asks
    the scan for zero columns, so SetDuckDBValue never runs and value
    conversion is skipped entirely. count(<col>) needs each column's validity,
    which requires the scan to materialise it, while adding negligible
    aggregate cost of its own.
    """
    cols = ", ".join(f"count({c})" for c in shape_columns(collection))
    args = f"'{collection}', project_id:='bench-project', api_key:='benchkey'"
    if extra:
        args += ", " + extra
    return f"SELECT {cols} FROM firestore_scan({args})"


def scan(collection, cols="*", extra="", where="", limit_clause=""):
    args = f"'{collection}', project_id:='bench-project', api_key:='benchkey'"
    if extra:
        args += ", " + extra
    q = f"SELECT {cols} FROM firestore_scan({args})"
    if where:
        q += f" WHERE {where}"
    if limit_clause:
        q += f" {limit_clause}"
    return q


def run_sql(statements, timeout=1200):
    """Run statements in one duckdb session; return (stdout, [run_times])."""
    script = PREAMBLE + "\n".join(s.rstrip(";") + ";" for s in statements) + "\n"
    env = dict(os.environ)
    env["FIRESTORE_EMULATOR_HOST"] = f"127.0.0.1:{PORT}"
    # Feed the script on stdin rather than -c: the CLI only honours dot-commands
    # such as `.timer on` when reading a script stream.
    p = subprocess.run(
        [DUCKDB, "-unsigned"],
        input=script, capture_output=True, text=True, env=env,
        timeout=timeout, cwd=ROOT,
    )
    out = p.stdout + p.stderr
    times = [float(m) for m in RUNTIME_RE.findall(out)]
    return out, times, p.returncode


COLLECTION_IN_QUERY = re.compile(r"firestore_scan\('([^']+)'")


def measure(label, query, repeats=REPEATS, warmup=True, group=""):
    """
    Time `query`. A warmup run populates the extension's schema cache so the
    measured runs reflect steady-state scanning rather than one-off schema
    inference. Stats are captured for a single clean run.
    """
    # Generate + cache the collection's pages server-side first, so Python
    # document generation never falls inside a timed run.
    m = COLLECTION_IN_QUERY.search(query)
    if m:
        try:
            prewarm(m.group(1))
        except Exception as e:
            print(f"  prewarm failed for {m.group(1)}: {e}", file=sys.stderr)

    stmts = []
    if warmup:
        stmts.append(query)
    stmts += [query] * repeats

    # Timed runs (schema cache warm after the first statement).
    out, times, rc = run_sql(stmts)
    if rc != 0 or len(times) < (1 + repeats if warmup else repeats):
        return {"label": label, "group": group, "error": _first_error(out),
                "raw": out[-1500:]}

    measured = times[1:] if warmup else times

    # Separate clean run purely for wire stats (counters zeroed first). The
    # schema cache lives in the extension process, which restarts here, so this
    # run includes schema inference -- reported separately as bind_requests.
    mock_post("/__reset")
    out2, times2, rc2 = run_sql([query])
    stats = mock_get("/__stats")

    return {
        "label": label,
        "group": group,
        "query": query,
        "median_s": round(statistics.median(measured), 4),
        "min_s": round(min(measured), 4),
        "cold_s": round(times[0], 4) if warmup else None,
        "requests": stats["requests"],
        "connections": stats["connections"],
        "reqs_per_conn": stats["requests_per_connection"],
        "max_reqs_one_conn": stats["max_requests_on_one_conn"],
        "mib_out": stats["mib_out"],
        "docs_served": stats["docs_served"],
        "gzip_advertised": stats["requests_advertising_gzip"],
        "field_mask_used": stats["requests_with_field_mask"],
        "ops": stats["requests_by_op"],
        "page_sizes": stats["page_sizes_requested"],
    }


def _first_error(out):
    for line in out.splitlines():
        if "Error" in line or "error" in line:
            return line.strip()[:300]
    return out.strip()[-300:]


def scalar(query):
    """
    Run a single-value query and return it as an int.

    Uses -noheader -list so stdout is just the value; scraping digits out of
    the default box-drawing table picks up column widths and row counts too.
    """
    env = dict(os.environ)
    env["FIRESTORE_EMULATOR_HOST"] = f"127.0.0.1:{PORT}"
    p = subprocess.run(
        [DUCKDB, "-unsigned", "-noheader", "-list"],
        input=f"LOAD fire_duck_ext;\n{query};\n",
        capture_output=True, text=True, env=env, timeout=1200, cwd=ROOT,
    )
    for line in reversed(p.stdout.strip().splitlines()):
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None


# ---------------------------------------------------------------- scenarios

def group_scaling():
    """How does a plain full scan scale with collection size?"""
    rows = []
    for n in (1000, 10000, 50000, 200000):
        rows.append(measure(f"flat8 x {n:,} docs",
                            materializing_scan(f"bench_flat_8_{n}"),
                            group="scaling"))
    return rows


def group_nested():
    """Cost of each Firestore value shape at a fixed document count."""
    n = 20000
    specs = [
        ("8 scalar fields (baseline)", f"bench_flat_8_{n}"),
        ("map, 8 leaves, depth 1", f"bench_mapwide_8_{n}"),
        ("array, 8 strings", f"bench_arr_8_{n}"),
        ("array, 8 ints", f"bench_arrint_8_{n}"),
        ("vector, 8 dims", f"bench_vec_8_{n}"),
    ]
    rows = []
    for lbl, coll in specs:
        rows.append(measure(lbl, materializing_scan(coll), group="nested-shape"))
        rows.append(measure(lbl + "  [no materialize]", scan(coll, "count(*)"),
                            group="nested-shape"))
    return rows


def group_map_depth():
    """Does nested-map cost grow with nesting depth?"""
    n = 20000
    return [measure(f"map depth {d}", materializing_scan(f"bench_map_{d}_{n}"),
                    group="map-depth")
            for d in (1, 2, 4, 8, 16)]


def group_array_width():
    """Does array cost grow with element count?"""
    n = 20000
    rows = []
    for w in (1, 4, 16, 64):
        coll = f"bench_arr_{w}_{n}"
        rows.append(measure(f"array {w} elems", materializing_scan(coll),
                            group="array-width"))
        rows.append(measure(f"array {w} elems  [no materialize]",
                            scan(coll, "count(*)"), group="array-width"))
    return rows


def group_projection():
    """Does projecting one column reduce what is fetched from Firestore?"""
    n = 20000
    coll = f"bench_wide_40_{n}"
    return [
        measure("wide40: all 40 columns", materializing_scan(coll), group="projection"),
        measure("wide40: 1 of 40 columns",
                f"SELECT count(f0) FROM firestore_scan('{coll}', "
                f"project_id:='bench-project', api_key:='benchkey')",
                group="projection"),
    ]


def group_limit():
    """Is scan_limit honoured, and does it stop fetching early?"""
    rows = []
    coll = "bench_flat_8_200000"
    for lim in (500, 1000, 1001, 5000):
        r = measure(f"scan_limit={lim:,}",
                    scan(coll, "count(*)", extra=f"scan_limit:={lim}"),
                    group="limit")
        if "error" not in r:
            r["rows_returned"] = scalar(scan(coll, "count(*)", extra=f"scan_limit:={lim}"))
            r["rows_expected"] = lim
        rows.append(r)
    return rows


def group_sql_limit():
    """SQL LIMIT (as opposed to the scan_limit named parameter)."""
    coll = "bench_flat_8_200000"
    rows = []
    for lim in (10, 5000):
        r = measure(f"SQL LIMIT {lim:,}",
                    scan(coll, "*", limit_clause=f"LIMIT {lim}"),
                    group="sql-limit")
        rows.append(r)
    return rows


def group_wire():
    """Transfer efficiency: compression and payload amplification."""
    n = 20000
    coll = f"bench_flat_8_{n}"
    r = measure("flat8 20k: bytes on the wire", materializing_scan(coll), group="wire")

    # What gzip would have saved, measured on the identical payload.
    mock_post("/__reset")
    req = urllib.request.Request(
        f"{BASE}/v1/projects/bench-project/databases/(default)/documents/{coll}?pageSize=1000",
        headers={"Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        gz = len(resp.read())
    s = mock_get("/__stats")
    raw = s["bytes_out_uncompressed"]
    r["gzip_ratio_same_payload"] = round(raw / gz, 2) if gz else None
    r["page_raw_kib"] = round(raw / 1024, 1)
    r["page_gzip_kib"] = round(gz / 1024, 1)
    return [r]


def group_map_encoding():
    """
    Compare the three map_encoding modes on map-shaped collections.

    All three run the same materialising query, so the difference is purely the
    cost of producing the payload column: `.dump()` of the wire format (wire),
    a recursive unwrap then `.dump()` (json), or building a VariantValue tree
    and one ToVARIANT per chunk (variant).
    """
    n = 20000
    rows = []
    for label, coll in [("map depth 1", f"bench_map_1_{n}"),
                        ("map depth 4", f"bench_map_4_{n}"),
                        ("map depth 8", f"bench_map_8_{n}"),
                        ("map 16 leaves flat", f"bench_mapwide_16_{n}")]:
        for enc in ("wire", "json", "variant"):
            rows.append(measure(f"{label} [{enc}]",
                                materializing_scan(coll, extra=f"map_encoding:='{enc}'"),
                                group="map-encoding"))
    return rows


GROUPS = {
    "map-encoding": group_map_encoding,
    "scaling": group_scaling,
    "nested": group_nested,
    "map-depth": group_map_depth,
    "array-width": group_array_width,
    "projection": group_projection,
    "limit": group_limit,
    "sql-limit": group_sql_limit,
    "wire": group_wire,
}


# ---------------------------------------------------------------- reporting

def fmt(rows):
    lines = []
    cur = None
    for r in rows:
        if r.get("group") != cur:
            cur = r.get("group")
            lines.append(f"\n=== {cur} " + "=" * (60 - len(str(cur))))
            lines.append(f"{'scenario':<34}{'median s':>10}{'docs':>9}{'reqs':>7}"
                         f"{'conns':>7}{'MiB':>8}{'mask':>6}{'gzip':>6}")
        if "error" in r:
            lines.append(f"{r['label']:<34}  ERROR: {r['error'][:70]}")
            continue
        extra = ""
        if "rows_returned" in r:
            ok = "OK" if r["rows_returned"] == r["rows_expected"] else "MISMATCH"
            extra = f"  rows={r['rows_returned']} (want {r['rows_expected']}) {ok}"
        if "gzip_ratio_same_payload" in r:
            extra = (f"  1 page raw={r['page_raw_kib']}KiB "
                     f"gzip={r['page_gzip_kib']}KiB ratio={r['gzip_ratio_same_payload']}x")
        lines.append(
            f"{r['label']:<34}{r['median_s']:>10.3f}{r['docs_served']:>9}"
            f"{r['requests']:>7}{r['connections']:>7}{r['mib_out']:>8.2f}"
            f"{r['field_mask_used']:>6}{r['gzip_advertised']:>6}{extra}")
    return "\n".join(lines)


def main():
    argv = sys.argv[1:]
    json_out = None
    if "--json" in argv:
        i = argv.index("--json")
        json_out = argv[i + 1]
        del argv[i:i + 2]          # drop the flag AND its value
    args = [a for a in argv if not a.startswith("--")]

    if not os.path.exists(DUCKDB):
        sys.exit(f"duckdb shell not found at {DUCKDB} -- run `make release` first")
    if not wait_for_mock():
        sys.exit(f"mock firestore not reachable at {BASE} -- start bench/mock_firestore.py")

    selected = args or list(GROUPS)
    rows = []
    for name in selected:
        if name not in GROUPS:
            sys.exit(f"unknown group {name!r}; choose from {', '.join(GROUPS)}")
        print(f"# running group: {name}", file=sys.stderr, flush=True)
        rows.extend(GROUPS[name]())

    print(fmt(rows))
    if json_out:
        with open(json_out, "w") as f:
            json.dump(rows, f, indent=2)
        print(f"\nwrote {json_out}")


if __name__ == "__main__":
    main()
