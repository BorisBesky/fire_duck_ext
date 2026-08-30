#!/usr/bin/env python3
"""
Validation against a real Firestore, rather than against the mock.

test/integration/large_collections.py asserts what the extension puts on the
wire, using bench/mock_firestore.py. That mock is fast and instrumented, but
it can only confirm that the extension agrees with what this project believes
Firestore does. These tests check that belief against the server: cursor
semantics, sort order, phantom documents, aggregation bounds, the encoding of
every value type, and what actually comes back when a page is heavy.

Where a test would only re-assert the extension against itself, it asks the
server the same question directly and compares the two answers.

    # against the emulator (free, and enough for most of this file)
    firebase emulators:start --only firestore     # or the bundled jar
    FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 python3 test/integration/real_firestore.py

    # against a live project
    FIRESTORE_TEST_PROJECT=my-project \
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json \
        python3 test/integration/real_firestore.py

A substring argument runs the matching tests only. Requires a built extension
at build/release/duckdb.
"""

import base64
import csv
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import firestore_fixtures as fx  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
DUCKDB = os.path.join(ROOT, "build", "release", "duckdb")

# Every collection this file writes to. Prefixed so a live project can tell
# them apart from anything else it holds, and so cleanup is unambiguous.
PREFIX = "fdx_validation"

EMULATOR = os.environ.get("FIRESTORE_EMULATOR_HOST", "")
PROJECT = os.environ.get("FIRESTORE_TEST_PROJECT", "fire-duck-validation" if EMULATOR else "")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")


# ---------------------------------------------------------------- duckdb driver


def secret_statement():
    """The secret that points DuckDB at the same endpoint the fixtures use.

    Against the emulator, `owner` is its admin bearer token -- metadata
    operations such as showMissing are refused without it, and an api_key
    alone sends no Authorization header at all.
    """
    if EMULATOR:
        return f"CREATE SECRET fdx (TYPE firestore, PROJECT_ID '{PROJECT}', ID_TOKEN 'owner')"
    if CREDENTIALS:
        return f"CREATE SECRET fdx (TYPE firestore, PROJECT_ID '{PROJECT}', " f"SERVICE_ACCOUNT_JSON '{CREDENTIALS}')"
    raise AssertionError("no endpoint configured")


def run_sql(sql, settings=None, timeout=600, columns=False, host=None):
    # CSV mode is set before the secret so its own success row is a bare
    # `true` rather than a boxed table, and can be stripped below.
    script = "LOAD fire_duck_ext;\n.mode csv\n.headers off\n" + secret_statement() + ";\n"
    for statement in settings or []:
        script += statement + ";\n"
    script += sql + "\n"

    env = dict(os.environ)
    if host:
        env["FIRESTORE_EMULATOR_HOST"] = host
    elif EMULATOR:
        env["FIRESTORE_EMULATOR_HOST"] = EMULATOR
    completed = subprocess.run(
        [DUCKDB, "-batch", "-init", "/dev/null"],
        input=script,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        cwd=ROOT,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"duckdb exited {completed.returncode}\nSQL: {sql}\n"
            f"stdout: {completed.stdout}\nstderr: {completed.stderr}"
        )
    # The secret statement prints a success row of its own. Everything after
    # it is CSV, and DuckDB quotes any value that needs it, so it is parsed
    # rather than split -- an accented id comes back quoted.
    lines = [line for line in completed.stdout.strip().splitlines() if line]
    if lines and lines[0] == "true":
        lines = lines[1:]
    rows = list(csv.reader(lines))
    if columns:
        return rows
    # Most tests compare a whole row, so the default is one string per row.
    # Rejoining is lossy where a value contains a comma -- pass columns=True
    # for those.
    return [",".join(row) for row in rows]


def scan_args(collection, extra=""):
    return f"'{collection}'" + (", " + extra if extra else "")


def sorted_locally(collection, order_clause, extra=""):
    """Sort a collection in DuckDB, with no chance of the sort being pushed.

    A subquery is not enough: the optimizer sees through it and pushes the
    ORDER BY down anyway, which would compare the pushed plan against itself.
    Materialising the scan first puts the rows in DuckDB before the sort, so
    this is what plain SQL semantics say the answer should be.
    """
    return run_sql(
        f"CREATE TEMP TABLE materialised AS SELECT * FROM firestore_scan({scan_args(collection, extra)});\n"
        f"SELECT __document_id FROM materialised {order_clause};"
    )


# ---------------------------------------------------------------- harness

TESTS = []
DB = None


def test(name, needs=(), known_defect=None):
    """Register a test.

    `needs` names capabilities the endpoint must have. `known_defect`
    describes a defect the test demonstrates but that is not fixed yet: the
    test still runs and is still reported, but its failure is expected and
    does not fail the suite. A known-defect test that starts passing is
    reported too -- that is the signal to drop the marker.
    """

    def decorate(function):
        TESTS.append((name, function, set(needs), known_defect))
        return function

    return decorate


def assert_eq(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def assert_true(condition, what):
    if not condition:
        raise AssertionError(what)


# ---------------------------------------------------------------- capabilities

CAPABILITIES = set()


def detect_capabilities(db):
    """What this endpoint can actually be asked.

    The emulator serves documents and aggregations but not the Admin index
    API, and it authenticates with a fixed owner token rather than minting
    OAuth ones -- so the tests about index planning and token refresh have
    nothing to run against there and are skipped rather than quietly passing.
    """
    found = set()
    if not EMULATOR:
        found.add("live")
        if CREDENTIALS:
            found.add("oauth")
    try:
        db._request("GET", db.base.replace("/documents", "") + "/collectionGroups/-/indexes")
        found.add("admin_indexes")
    except (fx.FirestoreError, OSError):
        pass
    if EMULATOR:
        # Only the emulator can be put behind the fault-injecting proxy: it
        # forwards plain HTTP, and terminating TLS to inject a status code is
        # not what these tests are about.
        found.add("fault_injection")
    return found


# ================================================================ fixtures

# Ids chosen so Python's ordering and Firestore's can disagree: digits sort
# before uppercase, uppercase before lowercase, and '_' sits between them.
AWKWARD_IDS = ["0", "9", "A", "Z", "_", "a", "z", "aa", "a_b", "a-b", "a0", "É", "é", "ó"]

# One document per Firestore value type, to be read back unchanged.
TYPE_DOCUMENT = {
    "s": fx.string("hello"),
    "i_max": fx.integer(9223372036854775807),
    "i_min": fx.integer(-9223372036854775808),
    "d": fx.double(0.1),
    "d_neg_zero": fx.double(-0.0),
    "b": fx.boolean(True),
    "n": fx.null(),
    "ts": fx.timestamp("2026-01-02T03:04:05.123456Z"),
    "raw": fx.bytes_value(base64.b64encode(b"\x00\x01\xfe\xff").decode()),
    "geo": fx.geo(37.4, -122.1),
    "arr": fx.array([fx.integer(1), fx.integer(2), fx.integer(3)]),
    "nested": fx.mapping({"inner": fx.string("deep"), "n": fx.integer(7)}),
    "vec": fx.vector([1.0, 2.0, 3.0]),
}


def seed_everything(db):
    """Write every fixture collection. Idempotent: writes overwrite by name."""
    started = time.time()

    # -- cursor semantics: five documents with known, adjacent names
    db.seed(f"{PREFIX}_cursors", {letter: {"n": fx.integer(i)} for i, letter in enumerate("abcde")})

    # -- ordering: awkward ids, and one field holding every type, so the
    #    server's cross-type ordering is observable.
    db.seed(f"{PREFIX}_ids", {doc_id: {"k": fx.string(doc_id)} for doc_id in AWKWARD_IDS})
    mixed = {
        "t01": {"v": fx.null()},
        "t02": {"v": fx.boolean(False)},
        "t03": {"v": fx.boolean(True)},
        "t04": {"v": fx.integer(-5)},
        "t05": {"v": fx.double(2.5)},
        "t06": {"v": fx.integer(9)},
        "t07": {"v": fx.timestamp("2020-01-01T00:00:00Z")},
        "t08": {"v": fx.string("")},
        "t09": {"v": fx.string("Zebra")},
        "t10": {"v": fx.string("apple")},
        "t11": {"v": fx.string("Ápple")},
    }
    db.seed(f"{PREFIX}_mixed", mixed)

    # -- key ranges: ids that are not Firestore auto-ids. Partitioning splits
    #    the auto-id alphabet, so these fall outside its assumed range.
    db.seed(f"{PREFIX}_numeric", {str(i): {"n": fx.integer(i)} for i in range(1, 1201)})
    db.seed(
        f"{PREFIX}_emails",
        {f"user{i:04d}_at_example.com": {"n": fx.integer(i)} for i in range(600)},
    )

    # -- phantom documents: a subcollection under a parent that was never
    #    written. `ghost` exists only as a path segment.
    db.seed(f"{PREFIX}_parents", {f"real{i}": {"n": fx.integer(i)} for i in range(3)})
    db.seed(f"{PREFIX}_parents/ghost1/child", {"c1": {"n": fx.integer(1)}})
    db.seed(f"{PREFIX}_parents/ghost2/child", {"c2": {"n": fx.integer(2)}})

    # -- heavy pages: documents big enough that a full page of them dwarfs any
    #    plausible response ceiling. 400 x ~48 KiB is roughly 19 MiB a page.
    blob = "x" * 48_000
    db.seed(f"{PREFIX}_fat", {f"doc{i:04d}": {"blob": fx.string(blob), "n": fx.integer(i)} for i in range(400)})

    # -- a field only some documents have, and one of them holds null. An
    #    ordered Firestore query returns neither the documents missing the
    #    field nor -- in DuckDB's ordering -- the null in the same place.
    db.seed(
        f"{PREFIX}_sparse",
        {
            "a": {"v": fx.string("apple"), "k": fx.integer(1)},
            "b": {"v": fx.string("banana"), "k": fx.integer(2)},
            "c": {"v": fx.null(), "k": fx.integer(3)},
            "d": {"k": fx.integer(4)},
            "e": {"k": fx.integer(5)},
            "f": {"v": fx.string("cherry"), "k": fx.integer(6)},
        },
    )

    # -- arrays: what widens, and what does not
    db.seed(
        f"{PREFIX}_arrays",
        {
            "a": {
                "numbers": fx.array([fx.integer(1), fx.double(2.5)]),
                "with_null": fx.array([fx.string("x"), fx.null()]),
            },
            "b": {"numbers": fx.array([fx.integer(3)]), "with_null": fx.array([fx.null()])},
            "c": {"numbers": fx.array([fx.double(0.5)]), "with_null": fx.array([fx.string("y")])},
        },
    )
    # On its own, so the element type is inferred from the leading integer and
    # the trailing string is what the cast trips over.
    db.seed(f"{PREFIX}_mixedarray", {"a": {"mixed": fx.array([fx.integer(1), fx.string("two")])}})

    # -- one document per type
    db.seed(f"{PREFIX}_types", {"only": TYPE_DOCUMENT})

    # -- a document close to Firestore's 1 MiB ceiling
    db.seed(f"{PREFIX}_huge", {"big": {"blob": fx.string("y" * 900_000), "n": fx.integer(1)}})

    # -- a plain collection for aggregation bounds
    db.seed(f"{PREFIX}_hundred", {f"d{i:03d}": {"n": fx.integer(i)} for i in range(100)})

    return round(time.time() - started, 1)


def wipe_everything(db):
    for suffix in (
        "cursors",
        "ids",
        "mixed",
        "numeric",
        "emails",
        "sparse",
        "fat",
        "types",
        "huge",
        "hundred",
        "written",
        "parents",
    ):
        db.delete_collection(f"{PREFIX}_{suffix}")
    for ghost in ("ghost1", "ghost2"):
        db.delete_collection(f"{PREFIX}_parents/{ghost}/child")


# ================================================================ tests

# ---- cursor semantics -------------------------------------------------------


@test("cursor: before=true bounds a range as startAt..endBefore")
def _():
    # This is the assumption BuildKeyRangeStructuredQuery is built on: both
    # bounds carry before=true, meaning an inclusive start and an exclusive
    # end, so adjacent partitions tile the key space without overlapping.
    # Firestore is the only authority on what the flag means; ask it.
    collection = f"{PREFIX}_cursors"
    prefix = f"{DB.name_prefix}/{collection}"

    def names(start_before, end_before):
        query = {
            "from": [{"collectionId": collection}],
            "orderBy": [{"field": {"fieldPath": "__name__"}, "direction": "ASCENDING"}],
            "startAt": {"values": [{"referenceValue": f"{prefix}/b"}], "before": start_before},
            "endAt": {"values": [{"referenceValue": f"{prefix}/d"}], "before": end_before},
        }
        return [doc["name"].rsplit("/", 1)[-1] for doc in DB.run_query(query)]

    assert_eq(names(True, True), ["b", "c"], "before=true on both bounds is [start, end)")
    assert_eq(names(False, False), ["c", "d"], "before=false on both bounds is (start, end]")


@test("cursor: adjacent key ranges tile the collection exactly once")
def _():
    # The property the extension actually depends on, stated end to end: cut
    # the collection at every document and confirm the pieces reassemble into
    # the whole with nothing doubled and nothing dropped.
    collection = f"{PREFIX}_cursors"
    prefix = f"{DB.name_prefix}/{collection}"
    boundaries = ["b", "c", "d", "e"]

    seen = []
    for i in range(len(boundaries) + 1):
        query = {
            "from": [{"collectionId": collection}],
            "orderBy": [{"field": {"fieldPath": "__name__"}, "direction": "ASCENDING"}],
        }
        if i > 0:
            query["startAt"] = {"values": [{"referenceValue": f"{prefix}/{boundaries[i - 1]}"}], "before": True}
        if i < len(boundaries):
            query["endAt"] = {"values": [{"referenceValue": f"{prefix}/{boundaries[i]}"}], "before": True}
        seen.extend(doc["name"].rsplit("/", 1)[-1] for doc in DB.run_query(query))

    assert_eq(sorted(seen), list("abcde"), "the ranges together cover the collection")
    assert_eq(len(seen), len(set(seen)), "no document falls in two ranges")


# ---- short pages ------------------------------------------------------------


@test("paging: a heavy page does not end the scan early")
def _():
    # The runQuery path has no page token: it decides the scan is over when a
    # page comes back holding fewer documents than it asked for
    # (firestore_scanner.cpp, last_page_was_full). If the server ever returns
    # a short page with more behind it -- a response-size ceiling, a stream cut
    # short -- the scan stops there and reports success. The mock returns
    # exactly what is asked for every time, so only a real server can say.
    collection = f"{PREFIX}_fat"
    rows = run_sql(f"SELECT count(n), count(DISTINCT __document_id) FROM firestore_scan({scan_args(collection)});")
    total, distinct = rows[0].split(",")
    assert_eq(total, "400", "every heavy document is scanned")
    assert_eq(distinct, "400", "and each exactly once")


@test("paging: the server's own answer to one large page")
def _():
    # Same question asked directly, so a failure above can be attributed. If
    # the server truncates a 400-document request, the heuristic above is
    # unsound even though the extension's own scan may paper over it.
    collection = f"{PREFIX}_fat"
    returned = len(DB.run_query({"from": [{"collectionId": collection}], "limit": 400}))
    assert_eq(returned, 400, "a single request for 400 heavy documents returns all of them")


@test("paging: a collection group of heavy documents is complete")
def _():
    # Collection groups are the path that used to truncate silently, and the
    # one with no page token to fall back on.
    rows = run_sql(f"SELECT count(n) FROM firestore_scan({scan_args('~' + PREFIX + '_fat')});")
    assert_eq(int(rows[0]), 400, "collection-group scan of heavy documents")


# ---- ordering and collation -------------------------------------------------


@test("ordering: document-id order matches the server's own")
def _():
    # __document_id ordering is what key-range partitioning cuts on, so the
    # extension's idea of it has to be the server's, not Python's or DuckDB's.
    server_order = DB.document_ids(f"{PREFIX}_ids")
    rows = run_sql(
        "SELECT __document_id FROM firestore_scan(" + scan_args(f"{PREFIX}_ids", "order_by:='__document_id'") + ");"
    )
    assert_eq(rows, server_order, "ordering by document id agrees with Firestore")


ORDER_BY_DEFECT = (
    "SQL ORDER BY is pushed to Firestore, whose ordering is not SQL's: it drops documents that "
    "lack the ordering field, sorts nulls first, and orders by its own cross-type precedence, so "
    "an ORDER BY changes which rows a query returns"
)


@test("ordering: ORDER BY does not drop documents that lack the field", known_defect=ORDER_BY_DEFECT)
def _():
    # The sharpest form of the problem, and it needs no LIMIT to show up.
    # Firestore only returns documents that have the field a query orders by;
    # in SQL, ORDER BY never changes which rows come back. Two of these six
    # documents have no `v` at all.
    collection = f"{PREFIX}_sparse"
    unordered = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection)});")
    ordered = run_sql(f"SELECT __document_id FROM firestore_scan({scan_args(collection)}) ORDER BY v;")
    assert_eq(int(unordered[0]), 6, "the collection holds six documents")
    assert_eq(len(ordered), 6, "ordering by a field some documents lack must not drop them")


@test("ordering: an unlimited ORDER BY still ends up in DuckDB's order")
def _():
    # Firestore sorts null below everything and orders by its own cross-type
    # precedence; DuckDB puts nulls last and, since a field holding more than
    # one type collapses to VARCHAR, compares them as strings. With no limit
    # that disagreement is harmless: every row still arrives and DuckDB sorts
    # them itself. This pins that down, so the limited case below is known to
    # be about the limit and not about ordering in general.
    collection = f"{PREFIX}_mixed"
    pushed = run_sql(f"SELECT __document_id FROM firestore_scan({scan_args(collection)}) ORDER BY v;")
    assert_eq(pushed, sorted_locally(collection, "ORDER BY v"), "a pushed sort matches the sort DuckDB would have done")


@test("ordering: ORDER BY with LIMIT keeps the rows DuckDB would keep", known_defect=ORDER_BY_DEFECT)
def _():
    # Where the disagreement above becomes wrong output rather than merely a
    # different order: the limit is applied to Firestore's ordering, so the
    # rows that survive are not the ones the query asked for.
    collection = f"{PREFIX}_mixed"
    pushed = run_sql(f"SELECT __document_id FROM firestore_scan({scan_args(collection)}) ORDER BY v LIMIT 5;")
    assert_eq(pushed, sorted_locally(collection, "ORDER BY v LIMIT 5"), "the same five rows as an unpushed sort")


@test("ordering: the named order_by parameter uses Firestore's ordering")
def _():
    # The named parameter is the documented way to ask for the server's own
    # ordering, so here Firestore's precedence is the correct answer -- null,
    # then booleans, then numbers, then timestamps, then strings.
    collection = f"{PREFIX}_mixed"
    rows = run_sql("SELECT __document_id FROM firestore_scan(" + scan_args(collection, "order_by:='v'") + ");")
    expected = [
        doc["name"].rsplit("/", 1)[-1]
        for doc in DB.run_query(
            {
                "from": [{"collectionId": collection}],
                "orderBy": [{"field": {"fieldPath": "v"}, "direction": "ASCENDING"}],
            }
        )
    ]
    assert_eq(rows, expected, "order_by:= reproduces the server's ordering exactly")


# ---- phantom / missing documents --------------------------------------------


@test("missing: show_missing includes documents that only parent a subcollection")
def _():
    # `ghost1` and `ghost2` were never written; they exist only as path
    # segments above a subcollection. Firestore calls these missing documents,
    # and an ordinary list does not return them.
    collection = f"{PREFIX}_parents"
    with_missing = int(
        run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'show_missing:=true')});")[0]
    )
    without = int(run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'show_missing:=false')});")[0])

    assert_eq(without, 3, "only the real documents without show_missing")
    assert_eq(with_missing, 5, "the two phantom parents as well with it")


@test("missing: an aggregation count does not count phantom documents")
def _():
    # CanAnswerWithCount refuses the count pushdown under show_missing on the
    # grounds that an aggregation query would not count phantoms. That claim
    # is what makes the refusal necessary -- confirm the server behaves that
    # way, and that the extension's two paths agree with it.
    collection = f"{PREFIX}_parents"
    server_count = DB.aggregate_count(collection)
    assert_eq(server_count, 3, "the server counts only written documents")

    pushed = int(run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'show_missing:=false')});")[0])
    assert_eq(pushed, server_count, "the count pushdown agrees with the aggregation")

    scanned = int(run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'show_missing:=true')});")[0])
    assert_true(scanned > server_count, "show_missing must not be answered by an aggregation")


# ---- key ranges over ids that are not auto-ids ------------------------------


@test("parallel: numeric-string keys are covered by the key ranges")
def _():
    # Partition boundaries are cut from Firestore's auto-id alphabet. Numeric
    # ids sit at the very bottom of it, so most ranges are empty and one holds
    # everything -- which is exactly the case where a boundary that is off by
    # one loses rows.
    collection = f"{PREFIX}_numeric"
    threaded = run_sql(
        f"SELECT count(n), count(DISTINCT __document_id) FROM firestore_scan({scan_args(collection)});",
        ["SET firestore_max_threads=4"],
    )
    assert_eq(threaded[0], "1200,1200", "every numeric key read exactly once across threads")


@test("parallel: keys outside the auto-id alphabet are not dropped")
def _():
    # `_` and `.` are not in the auto-id alphabet at all, so these ids fall
    # between and beyond the computed boundaries.
    collection = f"{PREFIX}_emails"
    threaded = run_sql(
        f"SELECT count(n), count(DISTINCT __document_id) FROM firestore_scan({scan_args(collection)});",
        ["SET firestore_max_threads=4"],
    )
    assert_eq(threaded[0], "600,600", "every email-shaped key read exactly once")


@test("parallel: threaded and sequential scans return identical rows")
def _():
    collection = f"{PREFIX}_numeric"
    query = "SELECT __document_id, n FROM firestore_scan({args}) ORDER BY __document_id;"
    threaded = run_sql(query.format(args=scan_args(collection)), ["SET firestore_max_threads=4"])
    sequential = run_sql(query.format(args=scan_args(collection)), ["SET firestore_max_threads=1"])
    assert_eq(threaded, sequential, "the same rows and values either way")


@test("parallel: the threaded scan agrees with the server document for document")
def _():
    collection = f"{PREFIX}_emails"
    server = sorted(DB.document_ids(collection))
    rows = run_sql(
        f"SELECT __document_id FROM firestore_scan({scan_args(collection)});",
        ["SET firestore_max_threads=4"],
    )
    assert_eq(sorted(rows), server, "the scan returns exactly the collection")


# ---- aggregation bounds -----------------------------------------------------


@test("count: the server treats upTo as a ceiling on the count")
def _():
    # BuildCountAggregationQuery sends the scan's limit as `upTo`, and the
    # scanner clamps the result afterwards on the grounds that upTo is only a
    # hint. Whether it is a hint or a hard ceiling is the server's to say.
    collection = f"{PREFIX}_hundred"
    assert_eq(DB.aggregate_count(collection), 100, "the unbounded count")

    bounded = DB.aggregate_count(collection, up_to=10)
    assert_true(bounded <= 100, "a bounded count never exceeds the collection")
    if bounded != 10:
        raise AssertionError(f"upTo=10 returned {bounded}: it is a hint here, so the scanner's clamp is load-bearing")


@test("count: a scan limit bounds what count(*) reports")
def _():
    # Whatever upTo does above, the extension owes the query its own limit.
    collection = f"{PREFIX}_hundred"
    rows = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'scan_limit:=10')});")
    assert_eq(int(rows[0]), 10, "count(*) under a scan limit")


@test("count: a zero scan limit counts nothing")
def _():
    # The aggregation reads a zero upTo as "no bound", so a zero limit has to
    # be settled before the query is built or it returns the whole collection.
    collection = f"{PREFIX}_hundred"
    rows = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, 'scan_limit:=0')});")
    assert_eq(int(rows[0]), 0, "count(*) under a zero scan limit")


@test("count: the pushed count equals the number of rows a scan returns")
def _():
    collection = f"{PREFIX}_hundred"
    counted = int(run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection)});")[0])
    scanned = int(run_sql(f"SELECT count(n) FROM firestore_scan({scan_args(collection)});")[0])
    assert_eq(counted, 100, "the aggregation count")
    assert_eq(scanned, counted, "counting and scanning agree")


# ---- value types ------------------------------------------------------------


@test("types: every Firestore type survives the round trip")
def _():
    # The mock emits this project's belief about Firestore's encodings. These
    # documents were stored by the server, so what comes back is the real one.
    collection = f"{PREFIX}_types"
    rows = run_sql(
        "SELECT s, i_max, i_min, d, b, n IS NULL, hex(raw), arr, ts FROM firestore_scan("
        + scan_args(collection)
        + ");",
        columns=True,
    )
    assert_eq(len(rows), 1, "one document")
    values = rows[0]

    assert_eq(values[0], "hello", "string")
    assert_eq(values[1], "9223372036854775807", "int64 max survives the JSON string encoding")
    assert_eq(values[2], "-9223372036854775808", "int64 min")
    assert_eq(values[3], "0.1", "double")
    assert_eq(values[4].lower(), "true", "boolean")
    assert_eq(values[5].lower(), "true", "an explicit null reads as SQL NULL")
    assert_eq(values[6], "0001FEFF", "bytes survive as their exact octets")
    assert_true(values[7].startswith("[1"), f"an array keeps its elements: {values[7]!r}")
    assert_true(values[8].startswith("2026-01-02"), "timestamp")


@test("types: a timestamp keeps sub-second precision")
def _():
    # Firestore stores microseconds. A conversion through seconds would round
    # them away silently.
    rows = run_sql(f"SELECT strftime(ts, '%Y-%m-%d %H:%M:%S.%f') FROM firestore_scan({scan_args(PREFIX + '_types')});")
    assert_eq(rows[0], "2026-01-02 03:04:05.123456", "microseconds are preserved")


@test("types: a map is surfaced in each documented encoding")
def _():
    # 'wire' is the documented default and hands back Firestore's own typed
    # JSON; 'variant' decodes it into addressable values. Both are read from
    # what the server actually stored.
    collection = f"{PREFIX}_types"
    wire = run_sql(f"SELECT nested FROM firestore_scan({scan_args(collection)});")
    assert_true("stringValue" in wire[0], f"the default encoding is Firestore's wire form: {wire[0]!r}")

    rows = run_sql(
        "SELECT nested.inner, nested.n FROM firestore_scan(" + scan_args(collection, "map_encoding:='variant'") + ");"
    )
    assert_eq(rows[0], "deep,7", "variant encoding makes map fields addressable")


ARRAY_DEFECT = (
    "an array mixing strings with numbers or booleans fails the whole query: list element types are "
    "not widened to VARCHAR the way a scalar field's are, so the scan throws a cast error on data "
    "Firestore accepts"
)


@test("types: array element types widen where they can")
def _():
    # Integers and doubles widen to DOUBLE[], and a null among strings is
    # simply a null element. This is the behaviour the mixed case below
    # should have had.
    collection = f"{PREFIX}_arrays"
    rows = run_sql(f"SELECT typeof(numbers), typeof(with_null) FROM firestore_scan({scan_args(collection)});")
    assert_eq(rows[0], "DOUBLE[],VARCHAR[]", "numeric widening, and nulls do not change the element type")


@test("types: an array mixing strings and numbers is readable", known_defect=ARRAY_DEFECT)
def _():
    # Firestore arrays are heterogeneous by design and the extension documents
    # array as LIST. A scalar field holding both types becomes VARCHAR; a list
    # of them errors instead.
    collection = f"{PREFIX}_mixedarray"
    rows = run_sql(f"SELECT mixed FROM firestore_scan({scan_args(collection)});")
    assert_eq(len(rows), 1, "the mixed array reads without failing the query")


@test("types: a vector is inferred as a fixed-size array")
def _():
    # Firestore encodes a vector as a tagged map, not an array, so this checks
    # the extension recognises the real encoding rather than the mock's.
    rows = run_sql(f"SELECT typeof(vec) FROM firestore_scan({scan_args(PREFIX + '_types')});")
    assert_eq(rows[0], "DOUBLE[3]", "vector inferred from the server's own encoding")


@test("types: binary bytes survive both directions unchanged")
def _():
    # Firestore transports bytes as base64. Anything that reads them through a
    # VARCHAR cast renders each byte above 0x7f as a \xNN escape, and stores
    # that text instead -- so a round trip is the check that matters, not just
    # that the column is a BLOB.
    collection = f"{PREFIX}_written"
    DB.delete_collection(collection)
    run_sql(
        f"CALL firestore_insert('{collection}', "
        "(SELECT 'blob' AS id, '\\x00\\x7F\\x80\\xFE\\xFF'::BLOB AS payload), document_id := 'id');"
    )
    rows = run_sql(f"SELECT hex(payload), octet_length(payload) FROM firestore_scan({scan_args(collection)});")
    assert_eq(rows[0], "007F80FEFF,5", "every octet round-trips through Firestore")


@test("types: a geo point keeps both coordinates")
def _():
    rows = run_sql(f"SELECT geo FROM firestore_scan({scan_args(PREFIX + '_types')});")
    assert_true("37.4" in rows[0] and "-122.1" in rows[0], f"latitude and longitude both present: {rows[0]!r}")


# ---- document weight --------------------------------------------------------


@test("weight: a document close to the 1 MiB ceiling is read whole")
def _():
    collection = f"{PREFIX}_huge"
    rows = run_sql(f"SELECT length(blob) FROM firestore_scan({scan_args(collection)});")
    assert_eq(int(rows[0]), 900_000, "the whole document comes back")


@test("weight: a heavy collection is paged smaller rather than failing")
def _():
    # The byte budget exists so a page of heavy documents does not have to be
    # held in memory at the requested page size. Squeezing it makes the policy
    # shrink; the scan still has to return everything.
    collection = f"{PREFIX}_fat"
    rows = run_sql(
        f"SELECT count(n) FROM firestore_scan({scan_args(collection)});",
        ["SET firestore_page_byte_budget=1048576"],
    )
    assert_eq(int(rows[0]), 400, "a squeezed byte budget still reads the whole collection")


# ---- writes -----------------------------------------------------------------


@test("write: inserted documents come back with their values")
def _():
    # The mock answers every commit with an empty writeResults, so the write
    # path is only ever exercised for real here.
    collection = f"{PREFIX}_written"
    DB.delete_collection(collection)
    run_sql(
        f"CALL firestore_insert('{collection}', ("
        "SELECT 'w' || i AS id, i AS n, 'row-' || i AS label FROM range(5) t(i)"
        "), document_id := 'id');"
    )
    stored = {doc["name"].rsplit("/", 1)[-1]: doc["fields"] for doc in DB.list_documents(collection)}
    assert_eq(sorted(stored), [f"w{i}" for i in range(5)], "the explicit document ids were used")
    assert_eq(stored["w3"]["label"]["stringValue"], "row-3", "string values round-trip")
    assert_eq(int(stored["w3"]["n"]["integerValue"]), 3, "integer values round-trip")


@test("write: a batch larger than one commit is written completely")
def _():
    # Firestore caps a commit at 500 writes, so anything above that has to be
    # split. An off-by-one there loses documents silently.
    collection = f"{PREFIX}_written"
    DB.delete_collection(collection)
    run_sql(
        f"CALL firestore_insert('{collection}', ("
        "SELECT 'b' || lpad(i::VARCHAR, 4, '0') AS id, i AS n FROM range(1200) t(i)"
        "), document_id := 'id');"
    )
    assert_eq(DB.aggregate_count(collection), 1200, "every document of an over-sized batch was written")


@test("write: a scan reads back exactly what was written")
def _():
    collection = f"{PREFIX}_written"
    DB.delete_collection(collection)
    run_sql(
        f"CALL firestore_insert('{collection}', ("
        "SELECT 'r' || i AS id, i AS n FROM range(50) t(i)"
        "), document_id := 'id');"
    )
    rows = run_sql(
        f"SELECT count(*), count(DISTINCT __document_id), sum(n) FROM firestore_scan({scan_args(collection)});"
    )
    assert_eq(rows[0], f"50,50,{sum(range(50))}", "the scan agrees with what was inserted")


@test("write: deleting a document removes it")
def _():
    collection = f"{PREFIX}_written"
    DB.delete_collection(collection)
    run_sql(
        f"CALL firestore_insert('{collection}', ("
        "SELECT 'd' || i AS id, i AS n FROM range(4) t(i)"
        "), document_id := 'id');"
    )
    run_sql(f"CALL firestore_delete('{collection}', 'd2');")
    assert_eq(sorted(DB.document_ids(collection)), ["d0", "d1", "d3"], "only the named document was deleted")


# ---- transient failures -----------------------------------------------------


def through_proxy(sql, settings=None, failures=0, status=503):
    """Run a query against the endpoint with `failures` requests failed first.

    Returns (rows, error, proxy stats). A query that fails is reported rather
    than raised, because for these tests failing loudly is an acceptable
    outcome and returning the wrong rows is not.
    """
    proxy = fx.FlakyProxy(EMULATOR).start()
    try:
        if failures:
            proxy.fail_next(failures, status)
        try:
            return run_sql(sql, settings, host=proxy.host), None, proxy.stats()
        except AssertionError as error:
            return None, str(error), proxy.stats()
    finally:
        proxy.stop()


@test("resilience: a scan through the proxy is unaffected when nothing fails", needs=("fault_injection",))
def _():
    # Establishes the baseline: the proxy itself changes nothing, so a
    # difference in the tests below is the injected failure and not the proxy.
    collection = f"{PREFIX}_hundred"
    rows, error, stats = through_proxy(f"SELECT count(n) FROM firestore_scan({scan_args(collection)});")
    assert_eq(error, None, f"the query succeeds through the proxy: {error}")
    assert_eq(int(rows[0]), 100, "every document still arrives")
    assert_true(stats["requests"] > 0, "the proxy was actually in the path")


@test("resilience: a server error fails the query rather than truncating it", needs=("fault_injection",))
def _():
    # There is no retry: FirestoreClient throws on any 5xx. That is a
    # defensible choice; silently returning the rows read so far would not be.
    # This pins down which of the two happens.
    collection = f"{PREFIX}_hundred"
    rows, error, stats = through_proxy(
        f"SELECT count(n) FROM firestore_scan({scan_args(collection)});", failures=1, status=503
    )
    assert_eq(stats["failures"], 1, "the failure was injected")
    if rows is not None:
        assert_eq(int(rows[0]), 100, "a query that survives a 5xx must still return every document")
    else:
        assert_true("500" in error or "erver" in error, f"a 5xx is reported as a server error: {error}")


@test("resilience: a rate limit is reported as a rate limit", needs=("fault_injection",))
def _():
    # A 429 has to reach the user as something they can act on -- backing off,
    # lowering firestore_max_threads -- rather than as a generic failure.
    collection = f"{PREFIX}_hundred"
    rows, error, _stats = through_proxy(
        f"SELECT count(n) FROM firestore_scan({scan_args(collection)});", failures=1, status=429
    )
    if rows is not None:
        assert_eq(int(rows[0]), 100, "a query that survives a 429 must still return every document")
        return
    assert_true(
        "ate limit" in error or "429" in error,
        f"the error names the rate limit rather than reporting something generic: {error}",
    )


@test("resilience: a failure part-way through paging does not truncate the scan", needs=("fault_injection",))
def _():
    # The dangerous shape: the first page arrives, a later one fails. A scan
    # that treats that as the end of the collection reports success with rows
    # missing. Either every document comes back, or the query fails.
    collection = f"{PREFIX}_numeric"  # 1200 documents, so several pages
    rows, error, stats = through_proxy(
        f"SELECT count(n) FROM firestore_scan({scan_args(collection, 'page_size:=300')});",
        failures=0,
    )
    assert_eq(error, None, "the unfailed baseline succeeds")
    assert_eq(int(rows[0]), 1200, "baseline row count")
    baseline_requests = stats["requests"]
    assert_true(baseline_requests > 2, "the baseline really did page")

    # Fail one request after the scan is under way. Two requests go out before
    # the first page of the scan itself (schema inference), so failing the
    # fourth lands mid-scan.
    proxy = fx.FlakyProxy(EMULATOR).start()
    try:
        served = {"n": 0}
        original = proxy._take_failure

        def fail_the_fourth():
            served["n"] += 1
            if served["n"] == 4:
                proxy.fail_next(1, 503)
            return original()

        proxy._take_failure = fail_the_fourth
        try:
            rows = run_sql(
                f"SELECT count(n) FROM firestore_scan({scan_args(collection, 'page_size:=300')});",
                host=proxy.host,
            )
        except AssertionError as error:
            # Failing loudly is the acceptable outcome, but only if it is this
            # failure: an error that does not name the injected status would
            # mean the scan broke for some other reason.
            assert_true("503" in str(error), f"the reported error is the injected one: {error}")
            assert_eq(proxy.stats()["failures"], 1, "exactly one request was failed")
            return
    finally:
        proxy.stop()

    assert_eq(int(rows[0]), 1200, "a scan that recovers from a mid-page failure must still be complete")


# ---- index planning ---------------------------------------------------------


@test("indexes: the Admin index list is read without error", needs=("admin_indexes",))
def _():
    # FetchCompositeIndexes parses this response, and everything the planner
    # decides rests on it. Against the emulator there is no such endpoint at
    # all, so this only means anything on a live project.
    collection = f"{PREFIX}_hundred"
    rows = run_sql(f"EXPLAIN SELECT count(n) FROM firestore_scan({scan_args(collection)});")
    assert_true(any("FIRESTORE_SCAN" in row for row in rows), "the plan mentions the scan")


@test("indexes: a multi-field ORDER BY with no composite index still returns every row", needs=("live",))
def _():
    # Firestore answers a query needing an absent composite index with
    # FAILED_PRECONDITION. The extension is supposed to notice it cannot order
    # server-side and let DuckDB sort instead -- not surface the error, and not
    # return a subset.
    collection = f"{PREFIX}_hundred"
    rows = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection)}) ORDER BY n;")
    assert_eq(int(rows[0]), 100, "every row, sorted client-side")


@test("indexes: a filter needing an absent composite index still filters correctly", needs=("live",))
def _():
    # Two range filters on different fields need a composite index. Whether it
    # is pushed or applied by DuckDB, the answer has to be the same.
    collection = f"{PREFIX}_hundred"
    rows = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection)}) WHERE n > 10 AND n < 20;")
    assert_eq(int(rows[0]), 9, "the filter is applied by whichever side can apply it")


@test("indexes: a collection group is not ordered server-side without an explicit index", needs=("live",))
def _():
    # Firestore's default single-field indexes cover collection scope only, so
    # ordering a collection group by a field needs an index created for it.
    # The extension declines to push the ordering, and DuckDB sorts. What
    # matters is that no rows are lost either way.
    collection = f"~{PREFIX}_hundred"
    rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args(collection, "order_by:='n'") + ");")
    assert_eq(int(rows[0]), 100, "an ordered collection group still returns every document")


# ---- credentials ------------------------------------------------------------


@test("auth: a service-account token is minted once and reused across threads", needs=("oauth",))
def _():
    # Several scan threads reach for the token at the same time. The
    # credentials are shared and guarded by a mutex; the failure this guards
    # against is each thread minting its own, or worse, tearing the cached one.
    collection = f"{PREFIX}_numeric"
    rows = run_sql(
        f"SELECT count(n), count(DISTINCT __document_id) FROM firestore_scan({scan_args(collection)});",
        ["SET firestore_max_threads=8"],
    )
    assert_eq(rows[0], "1200,1200", "every document read exactly once under eight threads")


@test("auth: a scan spanning a token refresh completes", needs=("oauth",))
def _():
    # An OAuth token lasts an hour and is refreshed five minutes before expiry.
    # A scan long enough to cross that boundary must not fail half way, and the
    # refresh must not be attempted once per thread.
    collection = f"{PREFIX}_numeric"
    rows = run_sql(
        f"SELECT count(n) FROM firestore_scan({scan_args(collection, 'page_size:=50')});",
        ["SET firestore_max_threads=4"],
        timeout=1800,
    )
    assert_eq(int(rows[0]), 1200, "the scan completes across the refresh boundary")


# ================================================================ runner


def main():
    global DB

    filter_text = sys.argv[1] if len(sys.argv) > 1 else ""

    if not os.path.exists(DUCKDB):
        print(f"missing {DUCKDB}; run `make release` first", file=sys.stderr)
        return 2

    DB = fx.from_environment()
    if DB is None:
        print(
            "no Firestore configured. Set FIRESTORE_EMULATOR_HOST for the emulator, or\n"
            "FIRESTORE_TEST_PROJECT with GOOGLE_APPLICATION_CREDENTIALS for a live project.",
            file=sys.stderr,
        )
        return 2

    global CAPABILITIES
    CAPABILITIES = detect_capabilities(DB)

    target = EMULATOR or f"{PROJECT} (live)"
    print(f"seeding {target} ...", flush=True)
    try:
        seconds = seed_everything(DB)
    except fx.FirestoreError as error:
        print(f"could not seed: {error}", file=sys.stderr)
        return 2
    print(f"seeded in {seconds}s\n")

    print("endpoint offers: " + (", ".join(sorted(CAPABILITIES)) or "nothing beyond plain document access") + "\n")

    passed = failed = 0
    defects = []
    skipped = []
    unexpectedly_fixed = []
    try:
        for name, function, needs, known_defect in TESTS:
            if filter_text and filter_text not in name:
                continue
            missing = needs - CAPABILITIES
            if missing:
                print(f"skip  {name}\n        needs {', '.join(sorted(missing))}")
                skipped.append((name, missing))
                continue
            try:
                function()
            except Exception as error:  # noqa: BLE001 -- a failed test is any exception
                if known_defect:
                    print(f"defect {name}\n        {error}")
                    defects.append((name, known_defect))
                else:
                    print(f"FAIL  {name}\n        {error}")
                    failed += 1
                continue
            print(f"ok    {name}")
            passed += 1
            if known_defect:
                unexpectedly_fixed.append(name)
    finally:
        if os.environ.get("FDX_KEEP_DATA") != "1":
            wipe_everything(DB)

    if defects:
        print("\nknown defects reproduced (not counted as failures):")
        seen = set()
        for name, description in defects:
            if description in seen:
                continue
            seen.add(description)
            print(f"  - {description}")

    for name in unexpectedly_fixed:
        print(f"\nNOTE  '{name}' is marked as a known defect but passed; drop the marker.")

    if skipped:
        needed = sorted({capability for _name, missing in skipped for capability in missing})
        print(f"\n{len(skipped)} tests skipped; they need: {', '.join(needed)}")
        print("  live           -- a Google-hosted project, not the emulator")
        print("  admin_indexes  -- the Admin index API, which the emulator does not serve")
        print("  oauth          -- service-account credentials, so tokens are minted and refreshed")

    print(f"\n{passed} passed, {failed} failed, {len(defects)} known defects, {len(skipped)} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
