#!/usr/bin/env python3
"""
Seeding and direct-REST helpers for tests that run against a real Firestore.

The mock in bench/mock_firestore.py synthesises collections from their names,
which makes it fast but also makes it an echo of what this extension believes
Firestore does. These helpers write real documents to a real endpoint -- the
emulator, or a live project -- so a test can check that belief against the
server itself.

Two things live here:

  * Seeder, which writes documents and cleans them up again.
  * Direct REST calls (run_query, aggregate, list_documents), so a test can
    ask the server a question without going through the extension. That is
    what makes it possible to assert the extension agrees with Firestore
    rather than only with itself.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

# The database id appears in two forms and they are not interchangeable:
# a URL path needs `(default)` percent-encoded, while a document's resource
# name -- the `name` field the API stores and returns -- takes it literally.
DEFAULT_DATABASE = "(default)"


class FirestoreError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


class Firestore:
    """A direct REST client for one project/database."""

    def __init__(self, host, project, database=DEFAULT_DATABASE, bearer=None, api_key=None, secure=None):
        self.project = project
        self.database = database
        self.bearer = bearer
        self.api_key = api_key
        scheme = "https" if (secure if secure is not None else not host.startswith("127.")) else "http"
        encoded_db = urllib.parse.quote(database, safe="")
        self.base = f"{scheme}://{host}/v1/projects/{project}/databases/{encoded_db}/documents"
        self.name_prefix = f"projects/{project}/databases/{database}/documents"

    # -- transport ---------------------------------------------------------

    def _request(self, method, url, body=None, timeout=120):
        if self.api_key:
            url += ("&" if "?" in url else "?") + "key=" + self.api_key
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if self.bearer:
            headers["Authorization"] = "Bearer " + self.bearer
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        # Firestore is reached directly, never through the agent proxy: the
        # emulator is on loopback, and a live endpoint is named explicitly.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            raise FirestoreError(error.code, error.read().decode("utf-8", "replace")) from None
        return json.loads(raw) if raw else {}

    # -- writing -----------------------------------------------------------

    # Firestore caps a commit at 500 writes, and the request itself at about
    # 10 MiB. Heavy fixtures hit the byte limit long before the write limit,
    # so batches are cut on whichever comes first.
    MAX_WRITES_PER_COMMIT = 500
    MAX_COMMIT_BYTES = 4 * 1024 * 1024

    def commit(self, writes):
        """Apply writes, batched to stay under Firestore's commit limits."""
        batch = []
        batch_bytes = 0
        for write in writes:
            size = len(json.dumps(write))
            if batch and (len(batch) >= self.MAX_WRITES_PER_COMMIT or batch_bytes + size > self.MAX_COMMIT_BYTES):
                self._request("POST", self.base + ":commit", {"writes": batch})
                batch, batch_bytes = [], 0
            batch.append(write)
            batch_bytes += size
        if batch:
            self._request("POST", self.base + ":commit", {"writes": batch})

    def seed(self, collection, documents):
        """Write {document_id: fields} into `collection`.

        `fields` is already in Firestore's wire form, because half the point
        of these tests is to control exactly what the server stores.
        """
        writes = [
            {"update": {"name": f"{self.name_prefix}/{collection}/{doc_id}", "fields": fields}}
            for doc_id, fields in documents.items()
        ]
        self.commit(writes)

    def delete_collection(self, collection):
        """Delete every document in a collection, including missing ones.

        Missing documents -- those that exist only to parent a subcollection --
        are not returned by an ordinary list, so they are collected with
        showMissing and deleted by name like any other.
        """
        ids = self.document_ids(collection, show_missing=True)
        if not ids:
            return
        self.commit([{"delete": f"{self.name_prefix}/{collection}/{doc_id}"} for doc_id in ids])

    # -- reading -----------------------------------------------------------

    def list_documents(self, collection, page_size=300, show_missing=False, mask=None):
        """Every document of a collection, in the server's own name order."""
        documents = []
        token = ""
        while True:
            url = f"{self.base}/{collection}?pageSize={page_size}"
            if show_missing:
                url += "&showMissing=true"
            for field in mask or []:
                url += "&mask.fieldPaths=" + urllib.parse.quote(field, safe="")
            if token:
                url += "&pageToken=" + urllib.parse.quote(token, safe="")
            page = self._request("GET", url)
            documents.extend(page.get("documents", []))
            token = page.get("nextPageToken", "")
            if not token:
                return documents

    def document_ids(self, collection, show_missing=False):
        return [doc["name"].rsplit("/", 1)[-1] for doc in self.list_documents(collection, show_missing=show_missing)]

    def run_query(self, structured_query, parent=None):
        """Raw :runQuery. Returns the documents, dropping readTime-only rows."""
        url = self.base + (("/" + parent) if parent else "") + ":runQuery"
        results = self._request("POST", url, {"structuredQuery": structured_query})
        return [row["document"] for row in results if isinstance(row, dict) and "document" in row]

    def aggregate_count(self, collection, all_descendants=False, up_to=None):
        """Raw :runAggregationQuery COUNT, optionally bounded by upTo."""
        count = {} if up_to is None else {"upTo": str(up_to)}
        body = {
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": collection, "allDescendants": all_descendants}]},
                "aggregations": [{"count": count, "alias": "n"}],
            }
        }
        results = self._request("POST", self.base + ":runAggregationQuery", body)
        for row in results:
            fields = row.get("result", {}).get("aggregateFields", {})
            if "n" in fields:
                return int(fields["n"]["integerValue"])
        raise AssertionError("aggregation returned no count")


# ---------------------------------------------------------------- wire values


def string(value):
    return {"stringValue": value}


def integer(value):
    return {"integerValue": str(value)}


def double(value):
    return {"doubleValue": value}


def boolean(value):
    return {"booleanValue": value}


def null():
    return {"nullValue": None}


def timestamp(value):
    return {"timestampValue": value}


def bytes_value(base64_text):
    return {"bytesValue": base64_text}


def geo(latitude, longitude):
    return {"geoPointValue": {"latitude": latitude, "longitude": longitude}}


def reference(name):
    return {"referenceValue": name}


def array(values):
    return {"arrayValue": {"values": values}}


def mapping(fields):
    return {"mapValue": {"fields": fields}}


def vector(values):
    """Firestore's vector encoding: a tagged map, not a bare array."""
    return mapping(
        {
            "__type__": string("__vector__"),
            "value": array([double(v) for v in values]),
        }
    )


# ---------------------------------------------------------------- environment


def from_environment():
    """Build a client from the environment, or return None if unconfigured.

    Two ways in:

      FIRESTORE_EMULATOR_HOST -- the emulator. `owner` is its documented
      admin bearer token, which metadata operations such as showMissing
      require; without it the emulator answers those with 403.

      FIRESTORE_TEST_HOST + FIRESTORE_TEST_PROJECT (+ FIRESTORE_TEST_TOKEN or
      FIRESTORE_TEST_API_KEY) -- a live endpoint.
    """
    emulator = os.environ.get("FIRESTORE_EMULATOR_HOST", "")
    if emulator:
        project = os.environ.get("FIRESTORE_TEST_PROJECT", "fire-duck-validation")
        return Firestore(emulator, project, bearer="owner", secure=False)

    host = os.environ.get("FIRESTORE_TEST_HOST", "firestore.googleapis.com")
    project = os.environ.get("FIRESTORE_TEST_PROJECT", "")
    if not project:
        return None
    return Firestore(
        host,
        project,
        database=os.environ.get("FIRESTORE_TEST_DATABASE", DEFAULT_DATABASE),
        bearer=os.environ.get("FIRESTORE_TEST_TOKEN") or None,
        api_key=os.environ.get("FIRESTORE_TEST_API_KEY") or None,
        secure=True,
    )
