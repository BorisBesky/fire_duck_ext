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


def run_sql(sql, settings=None, timeout=600):
    # CSV mode is set before the secret so its own success row is a bare
    # `true` rather than a boxed table, and can be stripped below.
    script = "LOAD fire_duck_ext;\n.mode csv\n.headers off\n" + secret_statement() + ";\n"
    for statement in settings or []:
        script += statement + ";\n"
    script += sql + "\n"

    env = dict(os.environ)
    if EMULATOR:
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
    return [",".join(row) for row in csv.reader(lines)]


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
    "by": fx.bytes_value(base64.b64encode(b"\x00\x01\xfe\xff").decode()),
    "geo": fx.geo(37.4, -122.1),
    "arr": fx.array([fx.integer(1), fx.string("two"), fx.boolean(False)]),
    "map": fx.mapping({"inner": fx.string("deep"), "n": fx.integer(7)}),
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

    target = EMULATOR or f"{PROJECT} (live)"
    print(f"seeding {target} ...", flush=True)
    try:
        seconds = seed_everything(DB)
    except fx.FirestoreError as error:
        print(f"could not seed: {error}", file=sys.stderr)
        return 2
    print(f"seeded in {seconds}s\n")

    passed = failed = 0
    defects = []
    unexpectedly_fixed = []
    try:
        for name, function, _needs, known_defect in TESTS:
            if filter_text and filter_text not in name:
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

    print(f"\n{passed} passed, {failed} failed, {len(defects)} known defects")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
