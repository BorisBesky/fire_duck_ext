#!/usr/bin/env python3
"""
Instrumented mock Firestore REST server for fire_duck_ext performance testing.

Serves synthetic collections whose shape and size are encoded in the collection
name, so any workload can be materialised instantly without seeding an emulator.

Why a mock instead of the Firebase emulator:
  * Collections of 10^5+ documents exist immediately (no seeding cost).
  * Page bodies are pre-serialised and cached, so the *server* is never the
    bottleneck -- measured time is client-side work plus loopback transfer.
  * It records exactly what the extension puts on the wire: how many TCP
    connections it opens, how many requests ride each connection, whether it
    advertises Accept-Encoding, and whether it sends mask.fieldPaths.

Collection naming grammar:  [auto_]bench_<shape>_<param>_<count>

An `auto_` prefix gives documents Firestore-style auto-ids -- 20 characters
drawn from [0-9A-Za-z], spread evenly -- instead of the sequential doc00000000
form. Range-partitioned parallel scans cut the key space evenly, so only
auto-id-shaped keys land in more than one partition.

  flat     <n_fields>   n scalar fields (string/int/double/bool, round robin)
  wide     <n_fields>   same as flat; used for projection experiments
  map      <depth>      one mapValue nested <depth> levels, 4 scalar leaves
  mapwide  <n_leaves>   one mapValue, depth 1, <n_leaves> scalar leaves
  arr      <n_elems>    one arrayValue of <n_elems> strings
  arrint   <n_elems>    one arrayValue of <n_elems> integers
  arrdbl   <n_elems>    one arrayValue of <n_elems> doubles
  arrbool  <n_elems>    one arrayValue of <n_elems> booleans
  arrts    <n_elems>    one arrayValue of <n_elems> timestamps
  vec      <n_dims>     one Firestore vector (__vector__) of <n_dims> doubles
  mixed    <n_fields>   scalar fields whose TYPE varies per document
  late     <from_idx>   3 base fields; `late_field` appears from document <from_idx>
  fat      <kib>        one string field of <kib> KiB, for page-weight limits
  odd      <n_fields>   field names needing backtick quoting in a field path

Control endpoints:
  GET  /__stats          instrumentation counters as JSON
  POST /__reset          zero the counters
  GET  /__health         readiness probe
  POST /__fail_runquery?enabled=1
                         make :runQuery return HTTP 500, so the extension's
                         pushdown-failure fallback can be exercised
  POST /__fail_aggregation?enabled=1
                         make :runAggregationQuery return HTTP 501, standing in
                         for a deployment that does not offer the endpoint
  POST /__delay?ms=N     set the per-request delay at runtime, overriding
                         MOCK_DELAY_MS. Latency is what makes concurrency
                         observable: without it requests finish too quickly to
                         overlap, whether or not the client is issuing them in
                         parallel.
"""

import bisect
import gzip
import hashlib
import json
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

# Artificial per-request latency. Loopback RTT is ~0, which hides the cost of
# opening a fresh connection per request -- the dominant term against real
# Firestore over a WAN. MOCK_DELAY_MS injects a realistic RTT so that cost
# becomes visible. Applied per *connection setup* and per *request* separately
# so keep-alive reuse can be distinguished from raw request count.
CONNECT_DELAY_MS = float(os.environ.get("MOCK_CONNECT_DELAY_MS", "0"))

# Mutable so a test can introduce latency for one scenario without needing its
# own server. Read under no lock: a float assignment is atomic enough here, and
# a request landing either side of a change is fine.
REQUEST_DELAY_MS = float(os.environ.get("MOCK_DELAY_MS", "0"))

# Link bandwidth in megabits/sec, applied to bytes actually put on the wire.
# Loopback is effectively infinite bandwidth, which makes compression look like
# pure overhead; throttling reveals the point where sending fewer bytes wins.
BANDWIDTH_MBPS = float(os.environ.get("MOCK_BANDWIDTH_MBPS", "0"))

# Ignore Accept-Encoding and always send identity. Lets one binary be measured
# both with and without compression for a clean A/B.
DISABLE_GZIP = os.environ.get("MOCK_DISABLE_GZIP", "") == "1"

# --------------------------------------------------------------------------
# Instrumentation
# --------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.requests = 0
        self.bytes_out = 0
        self.bytes_out_uncompressed = 0
        self.connections = 0
        self.requests_by_op = {}
        self.page_sizes = []
        self.page_sizes_in_order = []
        self.accept_encoding_requests = 0
        self.gzip_responses = 0
        self.mask_requests = 0
        self.max_requests_on_one_conn = 0
        self.docs_served = 0
        self.in_flight = 0
        self.max_in_flight = 0

    def snapshot(self):
        with self.lock:
            return {
                "requests": self.requests,
                "connections": self.connections,
                "requests_per_connection": (
                    round(self.requests / self.connections, 2) if self.connections else 0
                ),
                "max_requests_on_one_conn": self.max_requests_on_one_conn,
                "bytes_out": self.bytes_out,
                "bytes_out_uncompressed": self.bytes_out_uncompressed,
                "mib_out": round(self.bytes_out / (1024 * 1024), 2),
                "mib_out_uncompressed": round(self.bytes_out_uncompressed / (1024 * 1024), 2),
                "docs_served": self.docs_served,
                "requests_by_op": dict(self.requests_by_op),
                "page_sizes_requested": sorted(set(self.page_sizes)),
                "page_sizes_in_order": list(self.page_sizes_in_order),
                "requests_advertising_gzip": self.accept_encoding_requests,
                "responses_gzipped": self.gzip_responses,
                "requests_with_field_mask": self.mask_requests,
                # Peak number of requests being served at once. A sequential
                # scan never exceeds 1; anything above that is the extension
                # genuinely overlapping round trips.
                "max_concurrent_requests": self.max_in_flight,
            }


# Capabilities a caller can require before reusing an already-running mock.
# Add a name here whenever a shape or control endpoint is added that tests
# depend on.
FEATURES = {"fat_shape", "array_type_shapes", "fail_runquery", "page_sizes_in_order", "odd_shape",
            "field_mask", "runquery_select", "aggregation_query", "fail_aggregation",
            "key_range_cursors", "auto_ids", "concurrency_stats", "runtime_delay"}

STATS = Stats()

# When set, :runQuery answers 500. The extension is supposed to fall back to
# documents.list and let DuckDB apply the filters itself; this makes that
# path reachable without breaking the server for every other test.
FAIL_RUNQUERY = threading.Event()

# When set, :runAggregationQuery answers 501. Older emulators and restricted
# credentials do not offer the endpoint, and the extension is supposed to fall
# back to scanning rather than fail the query.
FAIL_AGGREGATION = threading.Event()

# Per-connection request counter. One thread == one TCP connection under
# ThreadingHTTPServer, so thread-local state is per-connection state.
CONN_LOCAL = threading.local()

# --------------------------------------------------------------------------
# Synthetic document generation
# --------------------------------------------------------------------------

COLLECTION_RE = re.compile(r"^(auto_)?bench_([a-z]+)_(\d+)_(\d+)$")

# Firestore's auto-id alphabet, in the byte order Firestore sorts by.
ID_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

_WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
          "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"]


def _scalar(kind, i, j):
    """Deterministic scalar in Firestore wire format."""
    if kind == 0:
        return {"stringValue": f"{_WORDS[(i + j) % len(_WORDS)]}-{i:07d}-{j}"}
    if kind == 1:
        return {"integerValue": str((i * 2654435761 + j) % 1000000)}
    if kind == 2:
        return {"doubleValue": ((i * 37 + j * 11) % 10000) / 100.0}
    return {"booleanValue": (i + j) % 2 == 0}


def _nested_map(depth, i):
    """mapValue nested `depth` levels; innermost level holds 4 scalar leaves."""
    if depth <= 1:
        return {"mapValue": {"fields": {
            f"leaf{k}": _scalar(k % 4, i, k) for k in range(4)
        }}}
    return {"mapValue": {"fields": {
        "label": {"stringValue": f"level-{depth}"},
        "child": _nested_map(depth - 1, i),
    }}}


def make_fields(shape, param, i):
    if shape in ("flat", "wide"):
        return {f"f{j}": _scalar(j % 4, i, j) for j in range(param)}

    if shape == "map":
        return {
            "doc_no": {"integerValue": str(i)},
            "payload": _nested_map(param, i),
        }

    if shape == "mapwide":
        return {
            "doc_no": {"integerValue": str(i)},
            "payload": {"mapValue": {"fields": {
                f"leaf{k}": _scalar(k % 4, i, k) for k in range(param)
            }}},
        }

    if shape == "arr":
        return {
            "doc_no": {"integerValue": str(i)},
            "tags": {"arrayValue": {"values": [
                {"stringValue": f"{_WORDS[(i + k) % len(_WORDS)]}-{k}"} for k in range(param)
            ]}},
        }

    if shape == "arrint":
        return {
            "doc_no": {"integerValue": str(i)},
            "nums": {"arrayValue": {"values": [
                {"integerValue": str((i + k) % 100000)} for k in range(param)
            ]}},
        }

    if shape == "arrdbl":
        return {
            "doc_no": {"integerValue": str(i)},
            "nums": {"arrayValue": {"values": [
                {"doubleValue": ((i + k) % 1000) / 8.0} for k in range(param)
            ]}},
        }

    if shape == "arrbool":
        return {
            "doc_no": {"integerValue": str(i)},
            "flags": {"arrayValue": {"values": [
                {"booleanValue": (i + k) % 2 == 0} for k in range(param)
            ]}},
        }

    if shape == "arrts":
        return {
            "doc_no": {"integerValue": str(i)},
            "stamps": {"arrayValue": {"values": [
                {"timestampValue": f"2026-01-{(k % 28) + 1:02d}T00:00:00.000000Z"} for k in range(param)
            ]}},
        }

    if shape == "vec":
        return {
            "doc_no": {"integerValue": str(i)},
            "embedding": {"mapValue": {"fields": {
                "__type__": {"stringValue": "__vector__"},
                "value": {"arrayValue": {"values": [
                    {"doubleValue": ((i * 31 + k * 7) % 2000) / 1000.0} for k in range(param)
                ]}},
            }}},
        }

    if shape == "late":
        # Three base fields on every document, plus `late_field` only from
        # document index `param` onwards. Set param above the schema sample size
        # to reproduce a field that inference cannot see.
        out = {f"f{j}": _scalar(j % 4, i, j) for j in range(3)}
        if i >= param:
            out["late_field"] = {"stringValue": f"late-{i}"}
        return out

    if shape == "fat":
        # One large string field. Firestore allows documents up to 1 MiB, so a
        # full 1000-document page can weigh far more than a process can hold --
        # this shape makes that regime reachable in a test.
        filler = _WORDS[i % len(_WORDS)] * (param * 1024 // 8 + 1)
        return {
            "doc_no": {"integerValue": str(i)},
            "blob": {"stringValue": filler[: param * 1024]},
        }

    if shape == "odd":
        # Names a Firestore field path cannot carry unquoted: a dot would read
        # as a path into a nested map, a space and a leading digit are not
        # valid identifier characters.
        out = {"plain": _scalar(0, i, 0)}
        names = ["a.b", "with space", "2digit", "back`tick"]
        for j in range(min(param, len(names))):
            out[names[j]] = _scalar(j % 4, i, j + 1)
        return out

    if shape == "mixed":
        # Field type cycles with the document index, so a schema inferred from
        # the first page is wrong for most later documents.
        out = {}
        for j in range(param):
            out[f"f{j}"] = _scalar((i + j) % 4, i, j)
        return out

    raise ValueError(f"unknown shape {shape!r}")


def unquote_field_path(field_path):
    """Undo the backtick quoting Firestore requires for awkward field names.

    A mask or select arrives as `a.b` for a field literally named "a.b";
    without unquoting here the mock would look for a key that includes the
    backticks and quietly return nothing, which would let a quoting bug in the
    extension pass as an empty column.
    """
    if len(field_path) >= 2 and field_path.startswith("`") and field_path.endswith("`"):
        inner = field_path[1:-1]
        out, escaped = [], False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        return "".join(out)
    return field_path


def parse_collection(name):
    m = COLLECTION_RE.match(name)
    if not m:
        return None
    return m.group(2), int(m.group(3)), int(m.group(4)), bool(m.group(1))


def _auto_id(i):
    """A deterministic stand-in for a Firestore auto-id: 20 chars, spread evenly."""
    digest = hashlib.sha1(str(i).encode()).digest()
    return "".join(ID_ALPHABET[b % len(ID_ALPHABET)] for b in digest[:20])


DOCUMENT_IDS = {}
DOCUMENT_IDS_LOCK = threading.Lock()


def document_ids(collection):
    """Document ids in the order Firestore returns them: sorted by name.

    Held as one sorted list per collection so a key range can be resolved by
    bisecting it, which is what makes the mock's cursor handling exact rather
    than an approximation of Firestore's.
    """
    with DOCUMENT_IDS_LOCK:
        hit = DOCUMENT_IDS.get(collection)
    if hit is not None:
        return hit

    parsed = parse_collection(collection)
    if parsed is None:
        return None
    _shape, _param, total, auto = parsed

    if auto:
        ids = sorted(_auto_id(i) for i in range(total))
    else:
        # Zero-padded, so lexicographic order is numeric order.
        ids = [f"doc{i:08d}" for i in range(total)]

    with DOCUMENT_IDS_LOCK:
        DOCUMENT_IDS[collection] = ids
    return ids


def cursor_document_id(cursor):
    """The document id a cursor's __name__ value points at, or None.

    A cursor carries one value per orderBy entry, so the reference is only the
    first value when the query orders by __name__ alone. Ordering by a field
    puts that field's value first and the reference last -- reading values[0]
    blindly yields no reference at all, which resolves to the start of the
    collection and makes pagination loop forever.
    """
    for value in cursor.get("values", []):
        reference = value.get("referenceValue")
        if reference:
            return reference.rsplit("/", 1)[-1]
    return None


# --------------------------------------------------------------------------
# Page cache -- pre-serialised bodies keep the server off the critical path
# --------------------------------------------------------------------------

PAGE_CACHE = {}
PAGE_CACHE_LOCK = threading.Lock()
DB_PREFIX = "projects/bench-project/databases/(default)/documents"


def build_page(collection, offset, page_size, mask=None):
    key = (collection, offset, page_size, tuple(mask) if mask else None)
    with PAGE_CACHE_LOCK:
        hit = PAGE_CACHE.get(key)
    if hit is not None:
        return hit

    parsed = parse_collection(collection)
    if parsed is None:
        return None
    shape, param, total, _auto = parsed
    ids = document_ids(collection)

    end = min(offset + page_size, total)
    wanted = {unquote_field_path(f) for f in mask} if mask else None
    docs = []
    for i in range(offset, end):
        fields = make_fields(shape, param, i)
        if wanted is not None:
            fields = {k: v for k, v in fields.items() if k in wanted}
        docs.append({
            "name": f"{DB_PREFIX}/{collection}/{ids[i]}",
            "fields": fields,
            "createTime": "2026-01-01T00:00:00.000000Z",
            "updateTime": "2026-01-01T00:00:00.000000Z",
        })

    body = {"documents": docs}
    if end < total:
        body["nextPageToken"] = f"off:{end}"

    raw = json.dumps(body, separators=(",", ":")).encode()
    result = (raw, len(docs))
    with PAGE_CACHE_LOCK:
        PAGE_CACHE[key] = result
    return result


def build_runquery_page(collection, offset, limit, select=None, stop=None):
    key = ("__rq__", collection, offset, limit, tuple(select) if select is not None else None, stop)
    with PAGE_CACHE_LOCK:
        hit = PAGE_CACHE.get(key)
    if hit is not None:
        return hit

    parsed = parse_collection(collection)
    if parsed is None:
        return None
    shape, param, total, _auto = parsed
    ids = document_ids(collection)

    end = min(offset + limit, total if stop is None else stop)
    wanted = None
    if select is not None:
        # select __name__ is Firestore's keys-only projection: names, no fields.
        wanted = {unquote_field_path(f) for f in select if f != "__name__"}
    out = []
    for i in range(offset, end):
        fields = make_fields(shape, param, i)
        if wanted is not None:
            fields = {k: v for k, v in fields.items() if k in wanted}
        out.append({
            "document": {
                "name": f"{DB_PREFIX}/{collection}/{ids[i]}",
                "fields": fields,
                "createTime": "2026-01-01T00:00:00.000000Z",
                "updateTime": "2026-01-01T00:00:00.000000Z",
            },
            "readTime": "2026-01-01T00:00:00.000000Z",
        })
    if not out:
        out = [{"readTime": "2026-01-01T00:00:00.000000Z"}]

    raw = json.dumps(out, separators=(",", ":")).encode()
    result = (raw, max(0, end - offset))
    with PAGE_CACHE_LOCK:
        PAGE_CACHE[key] = result
    return result


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    # HTTP/1.1 + Content-Length means keep-alive is available to any client
    # that wants it. If the extension still opens one connection per request,
    # that is the extension's behaviour and not a limitation of this server.
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # silence per-request logging

    # -- helpers -----------------------------------------------------------

    def _count(self, op, docs=0, page_size=None, mask=False):
        with STATS.lock:
            STATS.requests += 1
            STATS.requests_by_op[op] = STATS.requests_by_op.get(op, 0) + 1
            STATS.docs_served += docs
            if page_size is not None:
                STATS.page_sizes.append(page_size)
                STATS.page_sizes_in_order.append(page_size)
            if mask:
                STATS.mask_requests += 1
            # ThreadingHTTPServer handles each TCP connection on exactly one
            # thread, and that thread loops over every keep-alive request on
            # the connection -- so a thread-local counter *is* a per-connection
            # counter. (Keying on id(socket) is wrong: CPython recycles the id
            # of a closed socket, merging distinct connections into one bucket.)
            CONN_LOCAL.n = getattr(CONN_LOCAL, "n", 0) + 1
            if CONN_LOCAL.n > STATS.max_requests_on_one_conn:
                STATS.max_requests_on_one_conn = CONN_LOCAL.n

    def _send(self, payload, status=200, ctype="application/json"):
        # The delay stands in for network latency, so it is also the window in
        # which concurrency is observable.
        with STATS.lock:
            STATS.in_flight += 1
            if STATS.in_flight > STATS.max_in_flight:
                STATS.max_in_flight = STATS.in_flight
        try:
            if REQUEST_DELAY_MS:
                time.sleep(REQUEST_DELAY_MS / 1000.0)
        finally:
            with STATS.lock:
                STATS.in_flight -= 1
        uncompressed = len(payload)
        accepts_gzip = (not DISABLE_GZIP) and \
            "gzip" in (self.headers.get("Accept-Encoding") or "").lower()
        gzipped = False
        if accepts_gzip and uncompressed > 1024:
            payload = gzip.compress(payload, 5)
            gzipped = True

        with STATS.lock:
            if accepts_gzip:
                STATS.accept_encoding_requests += 1
            if gzipped:
                STATS.gzip_responses += 1
            STATS.bytes_out += len(payload)
            STATS.bytes_out_uncompressed += uncompressed

        # Charge transmission time for the bytes actually sent, so a smaller
        # (compressed) body genuinely costs less link time.
        if BANDWIDTH_MBPS:
            time.sleep(len(payload) * 8.0 / (BANDWIDTH_MBPS * 1_000_000.0))

        self.send_response(status)
        self.send_header("Content-Type", ctype)
        if gzipped:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj, status=200):
        self._send(json.dumps(obj).encode(), status)

    def _error(self, status, message):
        self._json({"error": {"code": status, "message": message,
                              "status": "NOT_FOUND" if status == 404 else "ERROR"}}, status)

    # -- routes ------------------------------------------------------------

    def do_GET(self):
        u = urlparse(self.path)
        path, q = u.path, parse_qs(u.query)

        if path == "/__stats":
            self._json(STATS.snapshot()); return
        if path == "/__health":
            # `features` lets a caller that finds a server already on the port
            # tell whether it is this version of the mock. Reusing an older one
            # fails in confusing ways -- a missing shape 404s, a missing control
            # endpoint is silently ignored -- so callers check before reusing.
            self._json({"ok": True, "features": sorted(FEATURES)}); return

        # Admin API: indexes. Report none so the extension takes its documented
        # "assume default single-field indexes" path.
        if "/collectionGroups/" in path and path.endswith("/indexes"):
            self._count("admin_indexes"); self._json({}); return
        if "/collectionGroups/__default__/fields/" in path:
            self._count("admin_fields")
            self._json({"indexConfig": {"indexes": [{"fields": []}]}}); return

        # documents.list
        m = re.match(r"^/v1/projects/[^/]+/databases/[^/]+/documents/(.+)$", path)
        if not m:
            self._error(404, "unsupported path"); return

        collection = unquote(m.group(1))
        page_size = int(q.get("pageSize", ["1000"])[0])
        token = q.get("pageToken", [""])[0]
        offset = int(token.split(":")[1]) if token.startswith("off:") else 0
        mask = q.get("mask.fieldPaths")

        page = build_page(collection, offset, page_size, mask)
        if page is None:
            self._count("list_404")
            self._error(404, f"Collection '{collection}' not found"); return

        raw, ndocs = page
        self._count("list", docs=ndocs, page_size=page_size, mask=bool(mask))
        self._send(raw)

    def do_POST(self):
        u = urlparse(self.path)
        path = u.path

        if path == "/__delay":
            global REQUEST_DELAY_MS
            REQUEST_DELAY_MS = float(parse_qs(u.query).get("ms", ["0"])[0])
            self._json({"delay_ms": REQUEST_DELAY_MS})
            return

        if path == "/__fail_aggregation":
            enabled = parse_qs(u.query).get("enabled", ["1"])[0] == "1"
            if enabled:
                FAIL_AGGREGATION.set()
            else:
                FAIL_AGGREGATION.clear()
            self._json({"fail_aggregation": enabled})
            return

        if path == "/__fail_runquery":
            enabled = parse_qs(u.query).get("enabled", ["1"])[0] == "1"
            if enabled:
                FAIL_RUNQUERY.set()
            else:
                FAIL_RUNQUERY.clear()
            self._json({"fail_runquery": enabled})
            return

        if path == "/__reset":
            with STATS.lock:
                STATS.reset()
            # Must be OUTSIDE the `with`: _send() re-acquires STATS.lock, and a
            # plain threading.Lock is not reentrant -- calling it while holding
            # the lock deadlocks the connection thread permanently.
            self._json({"reset": True})
            return

        # Build and cache every page of a collection up front. Document
        # generation is pure Python and would otherwise land inside a timed
        # run, measuring this server rather than the extension.
        if path == "/__prewarm":
            q = parse_qs(u.query)
            collection = q.get("collection", [""])[0]
            page_size = int(q.get("pageSize", ["1000"])[0])
            parsed = parse_collection(collection)
            if parsed is None:
                self._error(404, f"unknown collection {collection}")
                return
            total = parsed[2]
            t0 = time.time()
            for off in range(0, total, page_size):
                build_page(collection, off, page_size)
            self._json({"collection": collection,
                        "pages": (total + page_size - 1) // page_size,
                        "seconds": round(time.time() - t0, 2)})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b"{}"

        if path.endswith(":runAggregationQuery"):
            if FAIL_AGGREGATION.is_set():
                self._count("aggregation_failed")
                self._error(501, "aggregation queries disabled for this test")
                return
            try:
                req = json.loads(body)
            except json.JSONDecodeError:
                self._error(400, "bad json"); return

            aggregation = req.get("structuredAggregationQuery", {})
            sq = aggregation.get("structuredQuery", {})
            frm = (sq.get("from") or [{}])[0]
            collection = frm.get("collectionId", "")
            parsed = parse_collection(collection)
            if parsed is None:
                self._count("aggregation_404")
                self._error(404, f"Collection '{collection}' not found"); return

            total = parsed[2]
            for agg in aggregation.get("aggregations", []):
                up_to = agg.get("count", {}).get("upTo")
                if up_to is not None:
                    total = min(total, int(up_to))

            # Counting transfers no documents, which is the whole point; the
            # docs_served counter must stay flat so a test can prove it.
            self._count("aggregation")
            self._json([{"result": {"aggregateFields": {"count": {"integerValue": str(total)}}},
                         "readTime": "2026-01-01T00:00:00.000000Z"}])
            return

        if path.endswith(":runQuery"):
            if FAIL_RUNQUERY.is_set():
                self._count("runquery_failed")
                self._error(500, "runQuery disabled for this test")
                return
            try:
                req = json.loads(body)
            except json.JSONDecodeError:
                self._error(400, "bad json"); return

            sq = req.get("structuredQuery", {})
            frm = (sq.get("from") or [{}])[0]
            collection = frm.get("collectionId", "")
            limit = int(sq.get("limit", 1000))

            # Resolve the cursors the way Firestore does, by position in the
            # collection's name ordering. `before` says which side of the given
            # position the cursor sits on, and it means opposite things on a
            # start and an end cursor -- getting that wrong here would hide the
            # same mistake in the extension.
            ids = document_ids(collection)
            if ids is None:
                self._count("runquery_404")
                self._error(404, f"Collection '{collection}' not found"); return

            offset = 0
            start_at = sq.get("startAt")
            if start_at:
                start_id = cursor_document_id(start_at)
                if start_id is not None:
                    if start_at.get("before"):
                        offset = bisect.bisect_left(ids, start_id)   # startAt: inclusive
                    else:
                        offset = bisect.bisect_right(ids, start_id)  # startAfter: exclusive

            stop = None
            end_at = sq.get("endAt")
            if end_at:
                end_id = cursor_document_id(end_at)
                if end_id is not None:
                    if end_at.get("before"):
                        stop = bisect.bisect_left(ids, end_id)       # endBefore: exclusive
                    else:
                        stop = bisect.bisect_right(ids, end_id)      # endAt: inclusive

            select = None
            if "select" in sq:
                select = [f.get("fieldPath", "") for f in sq["select"].get("fields", [])]

            page = build_runquery_page(collection, offset, limit, select, stop)
            if page is None:
                self._count("runquery_404")
                self._error(404, f"Collection '{collection}' not found"); return
            raw, ndocs = page
            self._count("runquery", docs=ndocs, page_size=limit, mask=select is not None)
            self._send(raw); return

        if path.endswith(":listCollectionIds"):
            self._count("list_collection_ids")
            self._json({"collectionIds": []}); return

        if path.endswith(":batchWrite") or path.endswith(":commit"):
            self._count("write")
            self._json({"writeResults": []}); return

        self._error(404, "unsupported path")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def process_request(self, request, client_address):
        with STATS.lock:
            STATS.connections += 1
        # Cost paid once per TCP connection. Stands in for the TCP+TLS
        # handshake a real Firestore endpoint charges for every new connection;
        # a client that reuses connections pays it once, not per request.
        if CONNECT_DELAY_MS:
            time.sleep(CONNECT_DELAY_MS / 1000.0)
        super().process_request(request, client_address)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    srv = Server(("127.0.0.1", port), Handler)
    print(f"mock firestore listening on 127.0.0.1:{port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
