#!/usr/bin/env python3
"""
Integration tests for large-collection handling.

These cover the parts that unit tests cannot reach: the scanner and client
glue that only exists in terms of real HTTP round trips -- pagination,
per-request page sizes, and what the extension actually puts on the wire.
The pure logic underneath is covered by scripts/run_unit_tests.sh.

Runs against bench/mock_firestore.py, which serves synthetic collections
named bench_<shape>_<param>_<count> and reports per-request instrumentation,
so an assertion can be about request counts and page sizes rather than only
about rows.

    python3 test/integration/large_collections.py            # all tests
    python3 test/integration/large_collections.py collection # substring filter

Requires a built extension at build/release/duckdb.
"""

import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
DUCKDB = os.path.join(ROOT, "build", "release", "duckdb")
MOCK = os.path.join(ROOT, "bench", "mock_firestore.py")
PORT = int(os.environ.get("MOCK_PORT", "8123"))
BASE = f"http://127.0.0.1:{PORT}"

# Firestore's own per-page cap, which the extension clamps to.
MAX_PAGE_SIZE = 1000

# Mock capabilities these tests need. A server already listening on the port is
# only reused when it reports all of them; an older one would fail in confusing
# ways rather than obviously.
REQUIRED_MOCK_FEATURES = {
    "fat_shape",
    "array_type_shapes",
    "fail_runquery",
    "page_sizes_in_order",
    "odd_shape",
    "field_mask",
    "runquery_select",
    "aggregation_query",
    "fail_aggregation",
    "key_range_cursors",
    "auto_ids",
    "concurrency_stats",
    "runtime_delay",
    "cursor_validation",
}


# ---------------------------------------------------------------- mock control


def mock_get(path):
    # The mock is on loopback; an HTTPS proxy in the environment must not
    # intercept it.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(BASE + path, timeout=60) as response:
        return json.load(response)


def mock_post(path):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(BASE + path, data=b"", method="POST")
    with opener.open(request, timeout=60) as response:
        return json.load(response)


def reset_stats():
    mock_post("/__reset")


def set_runquery_failure(enabled):
    mock_post(f"/__fail_runquery?enabled={1 if enabled else 0}")


def set_aggregation_failure(enabled):
    mock_post(f"/__fail_aggregation?enabled={1 if enabled else 0}")


def set_request_delay(milliseconds):
    """Introduce latency, which is what makes overlapping requests observable."""
    mock_post(f"/__delay?ms={milliseconds}")


def stats():
    return mock_get("/__stats")


# ---------------------------------------------------------------- duckdb driver


def run_sql(sql, settings=None):
    """Run SQL through the real CLI against the mock, returning CSV rows."""
    script = "LOAD fire_duck_ext;\n"
    for statement in settings or []:
        script += statement + ";\n"
    script += ".mode csv\n.headers off\n" + sql + "\n"

    env = dict(os.environ)
    env["FIRESTORE_EMULATOR_HOST"] = f"127.0.0.1:{PORT}"
    completed = subprocess.run(
        [DUCKDB, "-batch", "-init", "/dev/null"],
        input=script,
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
        cwd=ROOT,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"duckdb exited {completed.returncode}\nSQL: {sql}\n"
            f"stdout: {completed.stdout}\nstderr: {completed.stderr}"
        )
    return [line for line in completed.stdout.strip().splitlines() if line]


def scan_args(collection, extra=""):
    args = f"'{collection}', project_id:='bench-project', api_key:='benchkey'"
    return args + (", " + extra if extra else "")


def count_rows(collection, extra="", settings=None):
    rows = run_sql(f"SELECT count(*) FROM firestore_scan({scan_args(collection, extra)});", settings)
    return int(rows[0])


# ---------------------------------------------------------------- harness

TESTS = []


def test(name):
    def decorate(function):
        TESTS.append((name, function))
        return function

    return decorate


def assert_eq(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def assert_page_size(sizes, expected, what):
    """Every request is at most `expected`, and `expected` is actually used.

    Not every request lands exactly on the page size: the last request of a
    bounded schema sample asks only for the documents still wanted, so a
    sample of 1000 at a page size of 400 ends with a request for 200.
    """
    if max(sizes) != expected:
        raise AssertionError(f"{what}: largest request was {max(sizes)}, expected {expected}; sizes {sizes}")


# ================================================================ the tests

# ---- collection groups: pagination past the first page ----------------------
#
# Before this change a collection-group scan issued exactly one runQuery with
# `limit` set to the page size and never asked for a second page, so any
# collection group larger than 1000 documents silently returned its first 1000
# and reported success. These are the regression tests for that.


@test("collection group: a collection group larger than one page returns every document")
def _():
    assert_eq(count_rows("~bench_flat_4_4500"), 4500, "rows from a 4500-document collection group")


@test("collection group: a collection group smaller than one page returns every document")
def _():
    assert_eq(count_rows("~bench_flat_4_300"), 300, "rows from a 300-document collection group")


@test("collection group: a size that is an exact multiple of the page size terminates")
def _():
    # The last full page looks identical to a page with more behind it, so the
    # scan has to issue one more request and see it come back empty. Getting
    # this wrong either drops the tail or loops forever.
    assert_eq(count_rows("~bench_flat_4_2000"), 2000, "rows from a 2000-document collection group")


@test("collection group: a custom page size paginates correctly")
def _():
    assert_eq(
        count_rows("~bench_flat_4_1500", "page_size:=100"),
        1500,
        "rows from a 1500-document collection group at page_size 100",
    )


@test("collection group: documents are not duplicated across page boundaries")
def _():
    # Cursor pagination without a total order can repeat or skip rows at a page
    # edge. Distinct document ids must still equal the row count.
    rows = run_sql(
        "SELECT count(*), count(DISTINCT __document_id) FROM firestore_scan(" + scan_args("~bench_flat_4_2500") + ");"
    )
    total, distinct = rows[0].split(",")
    assert_eq(total, "2500", "total rows")
    assert_eq(distinct, "2500", "distinct document ids")


def assert_cursors_aligned(counters, context):
    """Every cursor the extension sent named a real position in the query.

    The mock checks each cursor against the orderBy of the query carrying it,
    which is how Firestore reads one. A mismatch means the next page starts
    somewhere other than where the last one ended -- rows repeat, go missing,
    or the scan restarts -- and the mock does not have to reproduce that
    outcome for the cursor itself to be wrong.
    """
    if counters["cursor_mismatches"]:
        raise AssertionError(
            f"{context}: {counters['cursor_mismatches']} cursors did not match their orderBy: "
            + "; ".join(counters["cursor_mismatch_samples"])
        )


@test("collection group: an ordered scan pages without repeating documents")
def _():
    # order_by puts f1 ahead of __name__ in the query's ordering, so each
    # page's cursor has to carry that document's f1 -- even though the query
    # selects only f0. Asking Firestore for f0 alone leaves f1 out of the
    # response and the cursor holds a null in its place.
    reset_stats()
    rows = run_sql(
        "SELECT count(f0), count(DISTINCT __document_id) FROM firestore_scan("
        + scan_args("~bench_flat_4_2400", "order_by:='f1'")
        + ");"
    )
    total, distinct = rows[0].split(",")
    assert_eq(total, "2400", "rows from an ordered collection group")
    assert_eq(distinct, "2400", "distinct document ids")

    counters = stats()
    if counters["requests_by_op"].get("runquery", 0) < 3:
        raise AssertionError("2400 documents at 1000 per page should have taken several cursor-paged requests")
    assert_cursors_aligned(counters, "ordered collection-group scan")


@test("collection group: scan_limit stops the scan early")
def _():
    reset_stats()
    assert_eq(count_rows("~bench_flat_4_5000", "scan_limit:=1200"), 1200, "rows under scan_limit")
    served = stats()["docs_served"]
    # Bounded by the limit plus at most one page of over-read and the schema
    # sample -- not the whole 5000-document collection group.
    if served > 1200 + 2 * MAX_PAGE_SIZE:
        raise AssertionError(f"scan_limit fetched {served} documents, expected far fewer than the whole collection")


# ---- collection group schema inference --------------------------------------


@test("collection group: schema sampling pages past the first request")
def _():
    # `late_field` first appears at document 1500, past a single 1000-document
    # request. Sampling everything has to paginate the collection group to see
    # it -- which it previously could not do at all.
    rows = run_sql(
        "SELECT count(late_field) FROM firestore_scan("
        + scan_args("~bench_late_1500_3000", "schema_sample_size:=-1")
        + ");"
    )
    assert_eq(int(rows[0]), 1500, "documents carrying the late field")


# ---- streaming schema inference ---------------------------------------------


@test("inference: sampling every document of a large collection still infers late fields")
def _():
    rows = run_sql(
        "SELECT count(late_field) FROM firestore_scan("
        + scan_args("bench_late_1500_3000", "schema_sample_size:=-1")
        + ");"
    )
    assert_eq(int(rows[0]), 1500, "documents carrying the late field")


@test("inference: a bounded sample requests exactly the documents it asked for")
def _():
    reset_stats()
    run_sql(
        "SELECT count(*) FROM firestore_scan("
        + scan_args("bench_flat_4_5000", "schema_sample_size:=250, scan_limit:=10")
        + ");"
    )
    sizes = stats()["page_sizes_in_order"]
    assert_eq(sizes[0], 250, "first request is the schema sample, sized to the sample")


# ---- page size --------------------------------------------------------------


@test("page size: the named parameter sets the size of every request")
def _():
    reset_stats()
    assert_eq(count_rows("bench_flat_4_2500", "page_size:=250"), 2500, "rows at page_size 250")
    assert_page_size(
        stats()["page_sizes_in_order"],
        250,
        "every request, including the bind-time schema sample, respects the page size",
    )


@test("page size: the session setting applies when no parameter is given")
def _():
    reset_stats()
    assert_eq(
        count_rows("bench_flat_4_1200", settings=["SET firestore_page_size=400"]),
        1200,
        "rows at firestore_page_size 400",
    )
    assert_page_size(stats()["page_sizes_in_order"], 400, "requests use the session page size")


@test("page size: the named parameter overrides the session setting")
def _():
    reset_stats()
    assert_eq(
        count_rows("bench_flat_4_1200", "page_size:=300", settings=["SET firestore_page_size=900"]),
        1200,
        "rows when both are set",
    )
    assert_page_size(stats()["page_sizes_in_order"], 300, "the named parameter wins")


@test("page size: values outside Firestore's range are clamped, not rejected")
def _():
    reset_stats()
    assert_eq(count_rows("bench_flat_4_1200", "page_size:=99999"), 1200, "rows above the cap")
    assert_page_size(stats()["page_sizes_in_order"], MAX_PAGE_SIZE, "clamped down to Firestore's cap")

    reset_stats()
    # A page size of zero would fetch nothing and stall the scan.
    assert_eq(count_rows("bench_flat_4_5", "page_size:=0"), 5, "rows at a page size of zero")
    assert_eq(set(stats()["page_sizes_in_order"]), {1}, "clamped up to one document per request")


@test("page size: the setting is clamped when it is set")
def _():
    rows = run_sql("SELECT current_setting('firestore_page_size');", ["SET firestore_page_size=100000"])
    assert_eq(int(rows[0]), MAX_PAGE_SIZE, "setting clamped to Firestore's cap")

    rows = run_sql("SELECT current_setting('firestore_page_size');", ["SET firestore_page_size=-3"])
    assert_eq(int(rows[0]), 1, "setting clamped up to one")


# ---- adaptive page weight ---------------------------------------------------


@test("page weight: a page over the byte budget shrinks the next request")
def _():
    # 20 KiB documents: a 1000-document page is ~20 MiB, well over a 2 MiB
    # budget, so the scan should drop to roughly budget/document-size.
    reset_stats()
    assert_eq(
        count_rows("bench_fat_20_2200", "schema_sample_size:=10", settings=["SET firestore_page_byte_budget=2097152"]),
        2200,
        "every row still arrives after shrinking",
    )

    sizes = stats()["page_sizes_in_order"]
    assert_eq(sizes[0], 10, "the schema sample is unaffected")
    assert_eq(sizes[1], MAX_PAGE_SIZE, "the first scan page uses the configured size")
    if not (0 < sizes[2] < MAX_PAGE_SIZE):
        raise AssertionError(f"expected a shrunken page size after the first page, got {sizes}")
    if len(set(sizes[2:])) != 1:
        raise AssertionError(f"page size should settle after shrinking, got {sizes}")


@test("page weight: a zero budget disables shrinking")
def _():
    reset_stats()
    assert_eq(
        count_rows("bench_fat_20_2200", "schema_sample_size:=10", settings=["SET firestore_page_byte_budget=0"]),
        2200,
        "rows with the guard disabled",
    )
    scan_sizes = set(stats()["page_sizes_in_order"][1:])
    assert_eq(scan_sizes, {MAX_PAGE_SIZE}, "page size never moves when the budget is disabled")


@test("page weight: an ordinary collection never shrinks")
def _():
    # The guard must be invisible on normal data, or it would cost round trips
    # for nothing.
    reset_stats()
    assert_eq(count_rows("bench_flat_8_3000", "schema_sample_size:=10"), 3000, "rows from an ordinary collection")
    assert_eq(set(stats()["page_sizes_in_order"][1:]), {MAX_PAGE_SIZE}, "page size stays at the default")


@test("page weight: shrinking is bounded below by one document per request")
def _():
    # A budget far smaller than a single document cannot be honoured; the scan
    # must still complete rather than stall on a page size of zero.
    assert_eq(
        count_rows("bench_fat_20_1100", "schema_sample_size:=5", settings=["SET firestore_page_byte_budget=1"]),
        1100,
        "rows under an unsatisfiable budget",
    )


# ---- ordering and pushdown paths --------------------------------------------


@test("ordering: a filter and an ORDER BY are pushed to Firestore together")
def _():
    # Exercises the pushdown query builder's orderBy path: the ordering has to
    # carry a __name__ tiebreaker, or cursor pagination loses rows at page
    # boundaries and the counts below diverge.
    #
    # The mock does not evaluate `where`, so it answers with documents the
    # filter would have excluded. That is harmless here: the extension leaves
    # the original predicate in the plan for DuckDB to re-verify, so the row
    # count is still the true one -- it just does not shrink the transfer the
    # way real Firestore would.
    reset_stats()
    ordered = run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan("
        + scan_args("bench_flat_4_2500")
        + ") WHERE f3 = true ORDER BY f1);"
    )
    assert_eq("runquery" in stats()["requests_by_op"], True, "the filter reached Firestore")

    plain = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
    assert_eq(ordered[0], plain[0], "ordering must not change how many rows a filtered scan returns")
    if int(plain[0]) == 0:
        raise AssertionError("expected the filter to match something")


@test("ordering: a multi-field ORDER BY without a composite index sorts client-side")
def _():
    # No composite index exists, so the ordering cannot be sent to Firestore.
    # The scan must still deliver every row for DuckDB to sort.
    rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500", "order_by:='f0, f1'") + ");")
    assert_eq(int(rows[0]), 2500, "rows when ordering stays client-side")


@test("pushdown: a runQuery failure falls back to a full scan without losing rows")
def _():
    # When the filtered query fails, the extension re-runs it unfiltered and
    # lets DuckDB apply the predicate. The fallback has to page like any other
    # scan, or it truncates the result.
    set_runquery_failure(True)
    try:
        rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
        fallback_total = int(rows[0])
    finally:
        set_runquery_failure(False)

    rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
    assert_eq(fallback_total, int(rows[0]), "the fallback returns the same rows as pushdown")
    if fallback_total == 0:
        raise AssertionError("expected the filter to match something")


@test("pushdown: an ordered filtered scan resumes from cursors that name a real position")
def _():
    # The filtered scan pages through runQuery cursors, and f1 leads the
    # query's ordering, so every cursor must carry that document's f1 --
    # even though the query selects only f0. Asking Firestore for f0 alone
    # leaves f1 out of the response and the cursor holds a null instead,
    # which is not where the previous page ended.
    reset_stats()
    rows = run_sql(
        "SELECT count(f0), count(DISTINCT __document_id) FROM firestore_scan("
        + scan_args("bench_flat_4_2400", "show_missing:=false, order_by:='f1'")
        + ") WHERE f3 = true;"
    )
    total, distinct = rows[0].split(",")
    assert_eq(total, "1200", "rows matching the pushed filter")
    assert_eq(distinct, "1200", "distinct document ids")

    counters = stats()
    if counters["requests_by_op"].get("runquery", 0) < 2:
        raise AssertionError("the filtered scan should have paged through more than one runQuery request")
    assert_cursors_aligned(counters, "ordered filtered scan")


@test("pushdown: an ordered filtered scan of no columns still asks for the ordering field")
def _():
    # count(*) projects nothing, so the request would be keys-only -- but the
    # cursor is still built from f1, which a keys-only response does not carry.
    # A pushed filter keeps this off the aggregation shortcut.
    reset_stats()
    rows = run_sql(
        "SELECT count(*) FROM firestore_scan("
        + scan_args("bench_flat_4_2400", "show_missing:=false, order_by:='f1'")
        + ") WHERE f3 = true;"
    )
    assert_eq(int(rows[0]), 1200, "rows matching the pushed filter")
    assert_cursors_aligned(stats(), "keys-only ordered filtered scan")


@test("pushdown: the collection-group fallback also pages")
def _():
    set_runquery_failure(True)
    try:
        # Both the filtered query and the unfiltered retry go through
        # runQuery for a collection group, so this one has to surface the
        # failure rather than silently return a short result.
        rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("~bench_flat_4_2500") + ") WHERE f3 = true;")
        raise AssertionError(f"expected an error when runQuery is unavailable, got {rows}")
    except AssertionError as error:
        if "expected an error" in str(error):
            raise
    finally:
        set_runquery_failure(False)


# ---- inferred element types -------------------------------------------------


@test("inference: array element types map onto DuckDB list types")
def _():
    for collection, column, expected in [
        ("bench_arr_4_20", "tags", "VARCHAR[]"),
        ("bench_arrint_4_20", "nums", "BIGINT[]"),
        ("bench_arrdbl_4_20", "nums", "DOUBLE[]"),
        ("bench_arrbool_4_20", "flags", "BOOLEAN[]"),
        ("bench_arrts_4_20", "stamps", "TIMESTAMP[]"),
    ]:
        rows = run_sql(f"SELECT typeof({column}) FROM firestore_scan({scan_args(collection)}) LIMIT 1;")
        assert_eq(rows[0], expected, f"{collection}.{column} element type")


# ---- projection pushdown ----------------------------------------------------


@test("projection: selecting one column of many shrinks the transfer")
def _():
    # Projection pushdown was enabled for DuckDB but never reached the wire:
    # every field of every document was transferred whatever the query asked
    # for. A small schema sample keeps the unmasked bind-time request from
    # dominating the comparison.
    sample = "schema_sample_size:=5"

    reset_stats()
    assert_eq(
        run_sql("SELECT count(f0) FROM firestore_scan(" + scan_args("bench_wide_40_5000", sample) + ");")[0],
        "5000",
        "rows with one column projected",
    )
    narrow = stats()
    assert_eq(narrow["requests_with_field_mask"] > 0, True, "the scan requests sent a field mask")

    reset_stats()
    run_sql("SELECT count(*) FROM (SELECT * FROM firestore_scan(" + scan_args("bench_wide_40_5000", sample) + "));")
    wide = stats()
    assert_eq(wide["requests_with_field_mask"], 0, "selecting every column sends no mask")

    if narrow["bytes_out_uncompressed"] >= wide["bytes_out_uncompressed"] / 2:
        raise AssertionError(
            f"expected projecting 1 of 40 columns to at least halve the transfer, "
            f"got {narrow['bytes_out_uncompressed']} vs {wide['bytes_out_uncompressed']} bytes"
        )


@test("projection: projected values are still correct across page boundaries")
def _():
    rows = run_sql(
        "SELECT count(f0), count(f7), count(__document_id) FROM firestore_scan("
        + scan_args("bench_wide_40_2500", "schema_sample_size:=5")
        + ");"
    )
    assert_eq(rows[0], "2500,2500,2500", "every projected column is filled on every page")


@test("projection: field names needing backtick quoting round-trip")
def _():
    # A field named "a.b" read as an unquoted field path would address b inside
    # a map called a -- so it would come back empty rather than wrong, which is
    # exactly the kind of bug a row count would not catch.
    rows = run_sql(
        'SELECT count("a.b"), count("with space"), count("2digit"), count("back`tick"), count(plain) '
        "FROM firestore_scan(" + scan_args("bench_odd_4_200") + ");"
    )
    assert_eq(rows[0], "200,200,200,200,200", "awkward field names survive the mask")


@test("projection: values behind awkward names are the real values")
def _():
    masked = run_sql('SELECT "a.b" FROM firestore_scan(' + scan_args("bench_odd_4_200") + ") ORDER BY 1 LIMIT 3;")
    # Same column, but fetched with no mask at all, so the two paths must agree.
    unmasked = run_sql(
        'SELECT "a.b" FROM firestore_scan(' + scan_args("bench_odd_4_200", "unmapped_column:=true") + ") "
        "ORDER BY 1 LIMIT 3;"
    )
    assert_eq(masked, unmasked, "masked and unmasked reads of the same column agree")


@test("projection: __document_id alone needs no fields")
def _():
    rows = run_sql(
        "SELECT count(__document_id), count(DISTINCT __document_id) FROM firestore_scan("
        + scan_args("bench_flat_8_2500")
        + ");"
    )
    assert_eq(rows[0], "2500,2500", "document ids come back without asking for any field")


@test("projection: an unmapped catch-all column disables the mask")
def _():
    # __unmapped is defined as every field the schema does not cover, so a
    # mask would empty it by construction.
    reset_stats()
    rows = run_sql(
        "SELECT count(__unmapped) FROM firestore_scan("
        + scan_args("bench_late_1500_3000", "schema_sample_size:=5, unmapped_column:=true")
        + ");"
    )
    assert_eq(stats()["requests_with_field_mask"], 0, "no mask is sent when __unmapped is selected")
    if int(rows[0]) == 0:
        raise AssertionError("expected __unmapped to carry the fields the sample missed")


@test("projection: a filter on an unselected column still filters correctly")
def _():
    filtered = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
    projected = run_sql("SELECT count(f0) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
    assert_eq(filtered[0], projected[0], "projecting a different column does not change which rows match")


@test("projection: collection groups project through select")
def _():
    # count(f0) rather than count(*): a bare count(*) is answered by an
    # aggregation query these days and never reaches the scan at all.
    reset_stats()
    rows = run_sql(
        "SELECT count(f0) FROM firestore_scan(" + scan_args("~bench_wide_40_2500", "schema_sample_size:=5") + ");"
    )
    assert_eq(rows[0], "2500", "rows from a projected collection-group scan")
    assert_eq(stats()["requests_with_field_mask"] > 0, True, "collection-group requests carry a select clause")


# ---- count pushdown ---------------------------------------------------------
#
# A bare COUNT(*) reads none of its input's values, so Firestore can answer it
# with :runAggregationQuery and send no documents at all. The interesting part
# is everything that must NOT take that path.


def counted(collection, extra="", sql=None):
    """Run a query and report (result, whether Firestore was asked to count)."""
    reset_stats()
    args = scan_args(collection, extra)
    rows = run_sql(sql.format(args=args) if sql else f"SELECT count(*) FROM firestore_scan({args});")
    return rows[0], "aggregation" in stats()["requests_by_op"]


@test("count: a bare count(*) is answered without fetching documents")
def _():
    reset_stats()
    assert_eq(
        count_rows("bench_flat_8_200000", "show_missing:=false, columns:={'f0':'VARCHAR'}"),
        200000,
        "count over 200k documents",
    )
    counters = stats()
    assert_eq(counters["requests_by_op"], {"aggregation": 1}, "exactly one request, and it is the count")
    assert_eq(counters["docs_served"], 0, "no documents transferred")


@test("count: a collection group counts without phantom-document ambiguity")
def _():
    # Aggregation queries never count documents that exist only to parent a
    # subcollection -- and neither does a collection-group scan, so the two
    # agree whatever show_missing says.
    result, pushed = counted("~bench_flat_8_2000")
    assert_eq(result, "2000", "collection group count")
    assert_eq(pushed, True, "collection groups can always be counted server-side")


@test("count: show_missing keeps the count on the scanning path")
def _():
    # With show_missing a scan returns phantom documents as rows and an
    # aggregation query would not count them, so the two would disagree.
    result, pushed = counted("bench_flat_8_2000")
    assert_eq(result, "2000", "count still correct")
    assert_eq(pushed, False, "show_missing must not be answered by a count")


@test("count: anything that reads a value is not answered by a count")
def _():
    for label, sql in [
        ("a filter", "SELECT count(*) FROM firestore_scan({args}) WHERE f3 = true;"),
        ("count of a column", "SELECT count(f0) FROM firestore_scan({args});"),
        ("count distinct", "SELECT count(DISTINCT f0) FROM firestore_scan({args});"),
        ("grouped count", "SELECT count(*) FROM firestore_scan({args}) GROUP BY f3;"),
        ("count over a limit", "SELECT count(*) FROM (SELECT * FROM firestore_scan({args}) LIMIT 50);"),
        ("count of __document_id", "SELECT count(__document_id) FROM firestore_scan({args});"),
        ("sum", "SELECT sum(f1) FROM firestore_scan({args});"),
    ]:
        _result, pushed = counted("bench_flat_8_2000", "show_missing:=false", sql)
        if pushed:
            raise AssertionError(f"{label} must not be answered by a count")


@test("count: the counted rows are the rows the scan would have produced")
def _():
    # The scanning and counting paths must agree exactly.
    scanned, scan_pushed = counted(
        "bench_flat_8_2000", "show_missing:=false", "SELECT count(f0) FROM firestore_scan({args});"
    )
    assert_eq(scan_pushed, False, "count(f0) reads values, so it scans")
    pushed_result, was_pushed = counted("bench_flat_8_2000", "show_missing:=false")
    assert_eq(was_pushed, True, "count(*) is pushed")
    assert_eq(pushed_result, scanned, "counting and scanning agree")


@test("count: a limit bounds the counted rows")
def _():
    result, pushed = counted("bench_flat_8_2000", "show_missing:=false, scan_limit:=300")
    assert_eq(result, "300", "scan_limit caps the count")
    assert_eq(pushed, True, "a bounded count is still pushed")

    # A LIMIT above the aggregate limits rows of output, not rows counted.
    result, pushed = counted(
        "bench_flat_8_2000", "show_missing:=false", "SELECT count(*) FROM firestore_scan({args}) LIMIT 5;"
    )
    assert_eq(result, "2000", "a limit above the aggregate does not change the count")


@test("count: a zero limit is answered without asking Firestore anything")
def _():
    # Zero rows is the answer whatever the collection holds. It must not reach
    # the aggregation query, where a zero upTo means "no bound" and would come
    # back with the size of the whole collection.
    # Columns are given so schema inference does not read the collection and
    # the request counters describe the scan alone.
    fixed = "show_missing:=false, columns:={'f0':'VARCHAR'}"
    for label, limit in [("zero", 0), ("negative", -1)]:
        result, pushed = counted("bench_flat_8_2000", f"{fixed}, scan_limit:={limit}")
        assert_eq(result, "0", f"count(*) under a {label} scan_limit")
        assert_eq(pushed, False, f"a {label} scan_limit needs no aggregation query")
        assert_eq(stats()["requests_by_op"], {}, f"a {label} scan_limit sends no request at all")

    # The scanning path has always agreed; keep the two answering alike.
    scanned, _ = counted(
        "bench_flat_8_2000", f"{fixed}, scan_limit:=0", "SELECT count(f0) FROM firestore_scan({args});"
    )
    assert_eq(scanned, "0", "count(f0) under a zero scan_limit")


@test("count: an unavailable aggregation endpoint falls back to scanning")
def _():
    # Older emulators and restricted credentials do not offer the endpoint. A
    # slower answer is fine; a failed query is not.
    set_aggregation_failure(True)
    try:
        result, _ = counted("bench_flat_8_2000", "show_missing:=false")
        assert_eq(result, "2000", "the fallback still produces the right count")
        assert_eq("list" in stats()["requests_by_op"], True, "the fallback read the documents")
    finally:
        set_aggregation_failure(False)


# ---- parallel scanning ------------------------------------------------------
#
# A collection can be read by several threads at once by cutting its
# document-name space into ranges. Firestore's pagination is sequential within
# a range, but the ranges are independent, so the round trips overlap. The
# tests that matter are the ones proving the rows are still exactly the rows a
# sequential scan produces.
#
# The `auto_` collections carry Firestore-style auto-ids, which is what spreads
# documents across ranges; sequential doc00000000 keys all land in one.

PARALLEL = "show_missing:=false, schema_sample_size:=5"

# Parallel scanning is opt-in -- whether splitting a collection helps depends on
# how its document ids are distributed, which the extension cannot know -- so
# every test here that wants threads asks for them.
THREADED = ["SET firestore_max_threads=4"]


def scanned_rows(collection, extra="", settings=THREADED):
    """Row count via a query that actually reads documents.

    Deliberately not count(*): with show_missing:=false that is answered by an
    aggregation query and never reaches the scan, so it would say nothing at
    all about how the scan behaves.
    """
    rows = run_sql(f"SELECT count(f0) FROM firestore_scan({scan_args(collection, extra)});", settings)
    return int(rows[0])


@test("parallel: a scan split across threads returns every document exactly once")
def _():
    rows = run_sql(
        "SELECT count(*), count(DISTINCT __document_id) FROM firestore_scan("
        + scan_args("auto_bench_flat_8_20000", PARALLEL)
        + ");",
        THREADED,
    )
    # A range boundary that both neighbours claim would inflate the first
    # number; one that neither claims would deflate both.
    assert_eq(rows[0], "20000,20000", "every document once, none lost, none doubled")


@test("parallel: the rows match what a sequential scan produces, value for value")
def _():
    # A checksum over ids and a field: same documents, same contents.
    query = "SELECT count(*), sum(hash(__document_id)), sum(hash(f0)), sum(hash(f5)) FROM firestore_scan({args});"
    threaded = run_sql(query.format(args=scan_args("auto_bench_flat_8_20000", PARALLEL)), THREADED)
    sequential = run_sql(
        query.format(args=scan_args("auto_bench_flat_8_20000", PARALLEL)), ["SET firestore_max_threads=1"]
    )
    assert_eq(threaded, sequential, "parallel and sequential scans agree exactly")


@test("parallel: threads actually overlap their requests")
def _():
    # Without latency the mock answers faster than the client can issue the
    # next request, so nothing overlaps whether or not the scan is parallel.
    # Big enough that every range holds more than one page: a collection small
    # enough to answer each range in a single request can finish before the
    # next thread has started.
    set_request_delay(30)
    try:
        reset_stats()
        assert_eq(scanned_rows("auto_bench_flat_8_20000", PARALLEL), 20000, "rows from the parallel scan")
        peak = stats()["max_concurrent_requests"]
    finally:
        set_request_delay(0)
    if peak < 2:
        raise AssertionError(f"expected overlapping requests, peak concurrency was {peak}")


@test("parallel: overlapping requests make a latency-bound scan faster")
def _():
    # The whole point: Firestore pagination is sequential within a range, so
    # the only way to hide round-trip time is to read ranges at once.
    collection = "auto_bench_flat_8_20000"
    set_request_delay(30)
    try:
        # The mock caches serialised pages, so a cold run pays generation cost
        # a warm one does not. Warm both shapes first, or this measures the
        # cache rather than the concurrency.
        scanned_rows(collection, PARALLEL, ["SET firestore_max_threads=1"])
        scanned_rows(collection, PARALLEL)

        started = time.time()
        assert_eq(scanned_rows(collection, PARALLEL, ["SET firestore_max_threads=1"]), 20000, "sequential rows")
        sequential_seconds = time.time() - started

        started = time.time()
        assert_eq(scanned_rows(collection, PARALLEL), 20000, "parallel rows")
        parallel_seconds = time.time() - started
    finally:
        set_request_delay(0)

    if parallel_seconds >= sequential_seconds:
        raise AssertionError(
            f"expected the parallel scan to be faster: {parallel_seconds:.2f}s vs {sequential_seconds:.2f}s"
        )


@test("parallel: one thread is the sequential path")
def _():
    reset_stats()
    assert_eq(
        scanned_rows("auto_bench_flat_8_5000", PARALLEL, ["SET firestore_max_threads=1"]),
        5000,
        "rows with parallelism disabled",
    )
    assert_eq("runquery" in stats()["requests_by_op"], False, "no key-range queries were issued")


@test("parallel: keys that all fall in one range are still read correctly")
def _():
    # Sequential doc00000000-style ids sort below every range boundary, so one
    # range holds the lot. Unbalanced, but it must not be wrong.
    rows = run_sql(
        "SELECT count(*), count(DISTINCT __document_id) FROM firestore_scan("
        + scan_args("bench_flat_8_5000", PARALLEL)
        + ");",
        THREADED,
    )
    assert_eq(rows[0], "5000,5000", "every document once, however the keys are distributed")


@test("parallel: a collection smaller than the partition count is read correctly")
def _():
    # Most ranges are empty; the scan must not stall or lose the few documents
    # that do exist.
    assert_eq(scanned_rows("auto_bench_flat_8_7", PARALLEL), 7, "rows from a tiny collection")


@test("parallel: projection and page size still apply per thread")
def _():
    reset_stats()
    rows = run_sql(
        "SELECT count(f0) FROM firestore_scan("
        + scan_args("auto_bench_wide_40_5000", PARALLEL + ", page_size:=250")
        + ");",
        THREADED,
    )
    assert_eq(rows[0], "5000", "rows from a projected, small-paged parallel scan")
    counters = stats()
    assert_eq(counters["requests_with_field_mask"] > 0, True, "range queries carry a select clause")
    assert_page_size(counters["page_sizes_in_order"], 250, "range queries honour the page size")


@test("parallel: the conditions that make range splitting inexact fall back")
def _():
    # Each of these would return different rows, or a different number of them,
    # if it were split across ranges.
    for label, extra, sql, settings in [
        ("show_missing", "schema_sample_size:=5", None, None),
        ("an ORDER BY", PARALLEL + ", order_by:='f0'", None, None),
        ("a scan_limit", PARALLEL + ", scan_limit:=100", None, None),
        ("a document path", "", "SELECT count(*) FROM firestore_scan({args});", None),
    ]:
        collection = "auto_bench_flat_8_5000" if label != "a document path" else "users/user1"
        reset_stats()
        args = scan_args(collection, extra)
        run_sql(
            sql.format(args=args) if sql else f"SELECT count(f0) FROM firestore_scan({args});", settings or THREADED
        )
        if stats()["max_concurrent_requests"] > 1:
            raise AssertionError(f"{label} must not be scanned in parallel")


@test("parallel: a filtered scan is not split, and still returns the right rows")
def _():
    # A pushed filter needs its own ordering, which conflicts with ordering by
    # __name__ for the range cursors. It uses runquery either way, so compare
    # rows rather than request shape.
    threaded = run_sql(
        "SELECT count(*) FROM firestore_scan(" + scan_args("auto_bench_flat_8_5000", PARALLEL) + ") WHERE f3 = true;",
        THREADED,
    )
    sequential = run_sql(
        "SELECT count(*) FROM firestore_scan(" + scan_args("auto_bench_flat_8_5000", PARALLEL) + ") WHERE f3 = true;",
        ["SET firestore_max_threads=1"],
    )
    assert_eq(threaded, sequential, "a filtered scan returns the same rows either way")


@test("parallel: a collection group is not split by key range")
def _():
    # Collection-group names span parent paths, which this partitioning does
    # not know how to cut.
    reset_stats()
    assert_eq(scanned_rows("~auto_bench_flat_8_5000", PARALLEL), 5000, "collection group rows")
    if stats()["max_concurrent_requests"] > 1:
        raise AssertionError("collection groups must not be split by key range")


@test("parallel: an unmapped field is warned about once, not once per thread")
def _():
    # The reported set is shared across threads, so the warning is per scan.
    # Mainly this asserts the shared set is not corrupted by concurrent use.
    rows = run_sql(
        "SELECT count(*) FROM firestore_scan("
        + scan_args("auto_bench_late_1500_5000", "show_missing:=false, schema_sample_size:=5")
        + ");",
        THREADED,
    )
    assert_eq(rows[0], "5000", "the scan completes with fields outside the schema")


# ---- regressions on the paths this change touched ---------------------------


@test("regression: a plain multi-page collection still returns every document")
def _():
    assert_eq(count_rows("bench_flat_8_4500"), 4500, "rows from a 4500-document collection")


@test("regression: scan_limit larger than one page is still honoured")
def _():
    assert_eq(count_rows("bench_flat_4_5000", "scan_limit:=2048"), 2048, "rows under scan_limit")


@test("regression: SQL LIMIT is still pushed down")
def _():
    reset_stats()
    rows = run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan(" + scan_args("bench_flat_4_5000") + ") LIMIT 1500);"
    )
    assert_eq(int(rows[0]), 1500, "rows under SQL LIMIT")
    served = stats()["docs_served"]
    if served > 1500 + 2 * MAX_PAGE_SIZE:
        raise AssertionError(f"LIMIT fetched {served} documents, expected pushdown to bound it")


@test("regression: ORDER BY with LIMIT still returns the right number of rows")
def _():
    rows = run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan("
        + scan_args("bench_flat_4_3000")
        + ") ORDER BY f1 LIMIT 700);"
    )
    assert_eq(int(rows[0]), 700, "rows from ORDER BY with LIMIT")


@test("orderby pushdown: off by default, the whole collection is read and sorted here")
def _():
    # Firestore omits documents that lack the ordering field and sorts by its
    # own rules, so by default the sort stays in DuckDB -- which means the
    # limit cannot go to the server either, and every document is fetched.
    reset_stats()
    rows = run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan("
        + scan_args("bench_flat_4_3000")
        + ") ORDER BY f1 LIMIT 700);"
    )
    assert_eq(int(rows[0]), 700, "the query still returns 700 rows")
    served = stats()["docs_served"]
    if served < 3000:
        raise AssertionError(f"expected the whole collection to be read, got {served} documents")


@test("orderby pushdown: turning it on bounds what is fetched")
def _():
    # The round trips the setting exists to save: with the ordering sent to
    # Firestore the limit goes with it, so the scan stops after 700 documents
    # instead of reading 3000.
    reset_stats()
    rows = run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan("
        + scan_args("bench_flat_4_3000")
        + ") ORDER BY f1 LIMIT 700);",
        ["SET firestore_orderby_pushdown=true"],
    )
    assert_eq(int(rows[0]), 700, "the query still returns 700 rows")
    served = stats()["docs_served"]
    if served >= 3000:
        raise AssertionError(f"expected the limit to bound the fetch, got {served} documents")


@test("orderby pushdown: the scan parameter overrides the setting")
def _():
    reset_stats()
    run_sql(
        "SELECT count(*) FROM (SELECT * FROM firestore_scan("
        + scan_args("bench_flat_4_3000", "orderby_pushdown:=false")
        + ") ORDER BY f1 LIMIT 700);",
        ["SET firestore_orderby_pushdown=true"],
    )
    served = stats()["docs_served"]
    if served < 3000:
        raise AssertionError(f"the scan parameter should have kept the sort local, got {served} documents")


@test("regression: projected columns still materialise across page boundaries")
def _():
    rows = run_sql(
        "SELECT count(f0), count(f1), count(__document_id) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ");"
    )
    assert_eq(rows[0], "2500,2500,2500", "every projected column is filled on every page")


@test("regression: nested and list values still convert across page boundaries")
def _():
    rows = run_sql("SELECT count(payload) FROM firestore_scan(" + scan_args("bench_map_3_2500") + ");")
    assert_eq(int(rows[0]), 2500, "map values across pages")

    rows = run_sql("SELECT count(tags) FROM firestore_scan(" + scan_args("bench_arr_4_2500") + ");")
    assert_eq(int(rows[0]), 2500, "array values across pages")


@test("regression: vector dimension is still inferred")
def _():
    rows = run_sql("SELECT typeof(embedding) FROM firestore_scan(" + scan_args("bench_vec_8_10") + ") LIMIT 1;")
    assert_eq(rows[0], "DOUBLE[8]", "vector inferred as a fixed-size array")


@test("regression: a filter DuckDB applies itself still sees every document")
def _():
    # The mock reports no indexes, so nothing is pushed down and DuckDB
    # filters client-side -- which requires full pages to be delivered.
    rows = run_sql("SELECT count(*) FROM firestore_scan(" + scan_args("bench_flat_4_2500") + ") WHERE f3 = true;")
    total = int(rows[0])
    if not (0 < total < 2500):
        raise AssertionError(f"expected a proper subset to match the filter, got {total}")


# ================================================================ runner


def main():
    filter_text = sys.argv[1] if len(sys.argv) > 1 else ""

    if not os.path.exists(DUCKDB):
        print(f"missing {DUCKDB}; run `make release` first", file=sys.stderr)
        return 2

    # Reuse a mock already listening on this port. Binding a second one would
    # succeed (SO_REUSEADDR) and then split requests between two servers, so a
    # flag like /__fail_runquery set on one would be invisible to the other.
    server = None
    try:
        health = mock_get("/__health")
        missing = REQUIRED_MOCK_FEATURES - set(health.get("features", []))
        if missing:
            print(
                f"a mock firestore is already on 127.0.0.1:{PORT} but is missing {sorted(missing)}; "
                "stop it and re-run",
                file=sys.stderr,
            )
            return 2
        print(f"using the mock firestore already on 127.0.0.1:{PORT}")
    except Exception:
        server = subprocess.Popen(
            [sys.executable, MOCK, str(PORT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                mock_get("/__health")
                break
            except Exception:
                time.sleep(0.2)
        else:
            print("mock firestore did not come up", file=sys.stderr)
            return 2

        passed, failures = 0, []
        for name, body in TESTS:
            if filter_text and filter_text not in name:
                continue
            try:
                body()
                passed += 1
                print(f"ok    {name}")
            except Exception as error:
                failures.append((name, error))
                print(f"FAIL  {name}\n        {error}")

        print(f"\n{passed} passed, {len(failures)} failed")
        return 1 if failures else 0
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
