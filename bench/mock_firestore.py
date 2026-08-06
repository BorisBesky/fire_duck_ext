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

Collection naming grammar:  bench_<shape>_<param>_<count>
  flat     <n_fields>   n scalar fields (string/int/double/bool, round robin)
  wide     <n_fields>   same as flat; used for projection experiments
  map      <depth>      one mapValue nested <depth> levels, 4 scalar leaves
  mapwide  <n_leaves>   one mapValue, depth 1, <n_leaves> scalar leaves
  arr      <n_elems>    one arrayValue of <n_elems> strings
  arrint   <n_elems>    one arrayValue of <n_elems> integers
  vec      <n_dims>     one Firestore vector (__vector__) of <n_dims> doubles
  mixed    <n_fields>   scalar fields whose TYPE varies per document

Control endpoints:
  GET  /__stats   instrumentation counters as JSON
  POST /__reset   zero the counters
  GET  /__health  readiness probe
"""

import gzip
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
REQUEST_DELAY_MS = float(os.environ.get("MOCK_DELAY_MS", "0"))

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
        self.accept_encoding_requests = 0
        self.gzip_responses = 0
        self.mask_requests = 0
        self.max_requests_on_one_conn = 0
        self.docs_served = 0

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
                "requests_advertising_gzip": self.accept_encoding_requests,
                "responses_gzipped": self.gzip_responses,
                "requests_with_field_mask": self.mask_requests,
            }


STATS = Stats()

# Per-connection request counter. One thread == one TCP connection under
# ThreadingHTTPServer, so thread-local state is per-connection state.
CONN_LOCAL = threading.local()

# --------------------------------------------------------------------------
# Synthetic document generation
# --------------------------------------------------------------------------

COLLECTION_RE = re.compile(r"^bench_([a-z]+)_(\d+)_(\d+)$")

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
        # Every document has f0..f{param-1}; documents at index >= 500 carry an
        # extra field. Schema inference only samples the first 100 documents,
        # so this field is invisible to it.
        out = {f"f{j}": _scalar(j % 4, i, j) for j in range(param)}
        if i >= 500:
            out["late_field"] = {"stringValue": f"late-{i}"}
        return out

    if shape == "mixed":
        # Field type cycles with the document index, so a schema inferred from
        # the first page is wrong for most later documents.
        out = {}
        for j in range(param):
            out[f"f{j}"] = _scalar((i + j) % 4, i, j)
        return out

    raise ValueError(f"unknown shape {shape!r}")


def parse_collection(name):
    m = COLLECTION_RE.match(name)
    if not m:
        return None
    return m.group(1), int(m.group(2)), int(m.group(3))


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
    shape, param, total = parsed

    end = min(offset + page_size, total)
    docs = []
    for i in range(offset, end):
        fields = make_fields(shape, param, i)
        if mask:
            fields = {k: v for k, v in fields.items() if k in mask}
        docs.append({
            "name": f"{DB_PREFIX}/{collection}/doc{i:08d}",
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


def build_runquery_page(collection, offset, limit):
    key = ("__rq__", collection, offset, limit)
    with PAGE_CACHE_LOCK:
        hit = PAGE_CACHE.get(key)
    if hit is not None:
        return hit

    parsed = parse_collection(collection)
    if parsed is None:
        return None
    shape, param, total = parsed

    end = min(offset + limit, total)
    out = []
    for i in range(offset, end):
        out.append({
            "document": {
                "name": f"{DB_PREFIX}/{collection}/doc{i:08d}",
                "fields": make_fields(shape, param, i),
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
        if REQUEST_DELAY_MS:
            time.sleep(REQUEST_DELAY_MS / 1000.0)
        uncompressed = len(payload)
        accepts_gzip = "gzip" in (self.headers.get("Accept-Encoding") or "").lower()
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
            self._json({"ok": True}); return

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

        if path.endswith(":runQuery"):
            try:
                req = json.loads(body)
            except json.JSONDecodeError:
                self._error(400, "bad json"); return

            sq = req.get("structuredQuery", {})
            frm = (sq.get("from") or [{}])[0]
            collection = frm.get("collectionId", "")
            limit = int(sq.get("limit", 1000))

            offset = 0
            start_at = sq.get("startAt")
            if start_at:
                for v in start_at.get("values", []):
                    ref = v.get("referenceValue")
                    if ref and "/doc" in ref:
                        try:
                            offset = int(ref.rsplit("/doc", 1)[1]) + 1
                        except ValueError:
                            pass

            page = build_runquery_page(collection, offset, limit)
            if page is None:
                self._count("runquery_404")
                self._error(404, f"Collection '{collection}' not found"); return
            raw, ndocs = page
            self._count("runquery", docs=ndocs, page_size=limit)
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
