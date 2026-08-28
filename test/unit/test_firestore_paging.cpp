#include "test_harness.hpp"
#include "firestore_paging.hpp"

using duckdb::FirestorePageSizePolicy;
using duckdb::json;
using duckdb::OrderByField;

namespace {

constexpr int64_t kMiB = 1024 * 1024;

std::string FieldPathAt(const json &order_by_array, size_t index) {
	return order_by_array[index]["field"]["fieldPath"].get<std::string>();
}

std::string DirectionAt(const json &order_by_array, size_t index) {
	return order_by_array[index]["direction"].get<std::string>();
}

} // namespace

// ---------------------------------------------------------------- clamping

FD_TEST("paging: page size is clamped into Firestore's accepted range") {
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(500), 500);
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(duckdb::FIRESTORE_MAX_PAGE_SIZE), duckdb::FIRESTORE_MAX_PAGE_SIZE);
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(duckdb::FIRESTORE_MIN_PAGE_SIZE), duckdb::FIRESTORE_MIN_PAGE_SIZE);
	// Above the cap Firestore truncates silently, so clamp rather than ask.
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(5000), duckdb::FIRESTORE_MAX_PAGE_SIZE);
	// Zero or negative would mean "fetch nothing" and stall the scan.
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(0), duckdb::FIRESTORE_MIN_PAGE_SIZE);
	FD_REQUIRE_EQ(duckdb::ClampFirestorePageSize(-17), duckdb::FIRESTORE_MIN_PAGE_SIZE);
}

FD_TEST("paging: a negative byte budget means disabled") {
	FD_REQUIRE_EQ(duckdb::NormalizeFirestorePageByteBudget(-1), 0);
	FD_REQUIRE_EQ(duckdb::NormalizeFirestorePageByteBudget(0), 0);
	FD_REQUIRE_EQ(duckdb::NormalizeFirestorePageByteBudget(1234), 1234);
}

// ---------------------------------------------------------------- policy defaults

FD_TEST("policy: the default policy asks for a full page") {
	FirestorePageSizePolicy policy;
	FD_REQUIRE_EQ(policy.CurrentPageSize(), duckdb::FIRESTORE_DEFAULT_PAGE_SIZE);
	FD_REQUIRE_EQ(policy.ByteBudget(), duckdb::FIRESTORE_DEFAULT_PAGE_BYTE_BUDGET);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 0);
}

FD_TEST("policy: construction clamps both inputs") {
	FirestorePageSizePolicy policy(9999, -5);
	FD_REQUIRE_EQ(policy.CurrentPageSize(), duckdb::FIRESTORE_MAX_PAGE_SIZE);
	FD_REQUIRE_EQ(policy.ByteBudget(), 0);
}

// ---------------------------------------------------------------- request sizing

FD_TEST("policy: a cap below the page size wins, and a cap above it does not") {
	FirestorePageSizePolicy policy(200, 0);
	FD_REQUIRE_EQ(policy.NextRequestSize(50), 50);   // scan_limit smaller than a page
	FD_REQUIRE_EQ(policy.NextRequestSize(500), 200); // scan_limit larger than a page
	FD_REQUIRE_EQ(policy.NextRequestSize(200), 200); // exactly equal
	FD_REQUIRE_EQ(policy.NextRequestSize(0), 200);   // no cap
	FD_REQUIRE_EQ(policy.NextRequestSize(), 200);    // default argument
	FD_REQUIRE_EQ(policy.NextRequestSize(-1), 200);  // a negative cap is no cap
}

// ---------------------------------------------------------------- adaptive shrink

FD_TEST("policy: a page within budget leaves the page size alone") {
	FirestorePageSizePolicy policy(1000, 64 * kMiB);
	FD_REQUIRE_FALSE(policy.ObserveResponse(1000, 8 * kMiB));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1000);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 0);
	// Exactly at the budget is still within it.
	FD_REQUIRE_FALSE(policy.ObserveResponse(1000, 64 * kMiB));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1000);
}

FD_TEST("policy: an over-budget page shrinks proportionally in one step") {
	// 1000 documents weighing 640 MB is 640 kB each; a 64 MB budget fits
	// exactly 100 of them, so one observation lands there directly rather
	// than stepping down over further round trips.
	FirestorePageSizePolicy policy(1000, 64 * 1000 * 1000);
	FD_REQUIRE(policy.ObserveResponse(1000, 640LL * 1000 * 1000));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 100);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 1);
}

FD_TEST("policy: documents at Firestore's 1 MiB ceiling drive the page size right down") {
	FirestorePageSizePolicy policy(1000, 64 * kMiB);
	FD_REQUIRE(policy.ObserveResponse(1000, 1000 * kMiB));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 64);
}

FD_TEST("policy: shrinking is monotonic -- a later light page does not grow it back") {
	FirestorePageSizePolicy policy(1000, 64 * 1000 * 1000);
	FD_REQUIRE(policy.ObserveResponse(1000, 640LL * 1000 * 1000));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 100);

	FD_REQUIRE_FALSE(policy.ObserveResponse(100, 1 * kMiB));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 100);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 1);
}

FD_TEST("policy: repeated overruns keep shrinking") {
	FirestorePageSizePolicy policy(1000, 64 * kMiB);
	FD_REQUIRE(policy.ObserveResponse(1000, 128 * kMiB));
	const int64_t after_first = policy.CurrentPageSize();
	FD_REQUIRE(after_first < 1000);

	FD_REQUIRE(policy.ObserveResponse(after_first, 128 * kMiB));
	FD_REQUIRE(policy.CurrentPageSize() < after_first);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 2);
}

FD_TEST("policy: small budgets shrink step by step toward the minimum") {
	FirestorePageSizePolicy policy(4, 8);
	// 4 documents, 9 bytes: 3 bytes each (rounded up), budget fits 2.
	FD_REQUIRE(policy.ObserveResponse(4, 9));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 2);

	// 2 documents, 9 bytes: 5 bytes each, budget fits 1.
	FD_REQUIRE(policy.ObserveResponse(2, 9));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1);
}

FD_TEST("policy: a page bigger than the current size still forces progress") {
	// Observing a response measured before an earlier shrink: 100 documents
	// weighing 101 bytes against a 100 byte budget estimates 50 -- at or
	// above the page size of 10, which would leave the scan re-requesting a
	// size that just overran. The result has to come down regardless.
	FirestorePageSizePolicy policy(10, 100);
	FD_REQUIRE(policy.ObserveResponse(100, 101));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 9);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 1);
}

FD_TEST("policy: an over-budget page cannot shrink below one document") {
	FirestorePageSizePolicy policy(2, 1);
	FD_REQUIRE(policy.ObserveResponse(2, 1000));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), duckdb::FIRESTORE_MIN_PAGE_SIZE);

	// Already minimal: a further overrun changes nothing, and must not report
	// a shrink that did not happen.
	FD_REQUIRE_FALSE(policy.ObserveResponse(1, 1000));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), duckdb::FIRESTORE_MIN_PAGE_SIZE);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 1);
}

FD_TEST("policy: a zero budget disables the guard entirely") {
	FirestorePageSizePolicy policy(1000, 0);
	FD_REQUIRE_FALSE(policy.ObserveResponse(1000, 100LL * 1024 * kMiB));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1000);
	FD_REQUIRE_EQ(policy.ShrinkCount(), 0);
}

FD_TEST("policy: an empty response carries no size information") {
	// A page that returned nothing cannot say what a document weighs, so the
	// bytes (headers, an error envelope) must not be attributed to documents.
	FirestorePageSizePolicy policy(1000, 8);
	FD_REQUIRE_FALSE(policy.ObserveResponse(0, 4096));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1000);
	FD_REQUIRE_FALSE(policy.ObserveResponse(-1, 4096));
	FD_REQUIRE_EQ(policy.CurrentPageSize(), 1000);
}

// ---------------------------------------------------------------- orderBy

FD_TEST("orderBy: an empty ordering becomes __name__ ascending") {
	json order_by = duckdb::BuildOrderByArray({});
	FD_REQUIRE_EQ(order_by.size(), 1u);
	FD_REQUIRE_EQ(FieldPathAt(order_by, 0), std::string("__name__"));
	FD_REQUIRE_EQ(DirectionAt(order_by, 0), std::string("ASCENDING"));
}

FD_TEST("orderBy: __name__ is appended following the last field's direction") {
	// Firestore rejects mixed directions without a composite index covering
	// them, so the tiebreaker has to match its neighbour.
	json order_by = duckdb::BuildOrderByArray({{"score", "DESCENDING"}});
	FD_REQUIRE_EQ(order_by.size(), 2u);
	FD_REQUIRE_EQ(FieldPathAt(order_by, 0), std::string("score"));
	FD_REQUIRE_EQ(FieldPathAt(order_by, 1), std::string("__name__"));
	FD_REQUIRE_EQ(DirectionAt(order_by, 1), std::string("DESCENDING"));
}

FD_TEST("orderBy: multiple fields keep their order and follow the last direction") {
	json order_by = duckdb::BuildOrderByArray({{"a", "ASCENDING"}, {"b", "DESCENDING"}});
	FD_REQUIRE_EQ(order_by.size(), 3u);
	FD_REQUIRE_EQ(FieldPathAt(order_by, 0), std::string("a"));
	FD_REQUIRE_EQ(FieldPathAt(order_by, 1), std::string("b"));
	FD_REQUIRE_EQ(FieldPathAt(order_by, 2), std::string("__name__"));
	FD_REQUIRE_EQ(DirectionAt(order_by, 2), std::string("DESCENDING"));
}

FD_TEST("orderBy: an explicit __name__ is not duplicated") {
	json order_by = duckdb::BuildOrderByArray({{"__name__", "DESCENDING"}});
	FD_REQUIRE_EQ(order_by.size(), 1u);
	FD_REQUIRE_EQ(DirectionAt(order_by, 0), std::string("DESCENDING"));

	json trailing = duckdb::BuildOrderByArray({{"a", "ASCENDING"}, {"__name__", "ASCENDING"}});
	FD_REQUIRE_EQ(trailing.size(), 2u);
}

// ---------------------------------------------------------------- collection group query

FD_TEST("collection group: the query is always ordered so it can be paginated") {
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {}, 250);
	FD_REQUIRE_EQ(query["from"][0]["collectionId"].get<std::string>(), std::string("orders"));
	FD_REQUIRE(query["from"][0]["allDescendants"].get<bool>());
	FD_REQUIRE_EQ(query["limit"].get<int64_t>(), 250);
	FD_REQUIRE_EQ(query["orderBy"].size(), 1u);
	FD_REQUIRE_EQ(FieldPathAt(query["orderBy"], 0), std::string("__name__"));
}

FD_TEST("collection group: a caller's ordering is preserved and the limit clamped") {
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {{"total", "DESCENDING"}}, 100000);
	FD_REQUIRE_EQ(query["limit"].get<int64_t>(), duckdb::FIRESTORE_MAX_PAGE_SIZE);
	FD_REQUIRE_EQ(query["orderBy"].size(), 2u);
	FD_REQUIRE_EQ(FieldPathAt(query["orderBy"], 0), std::string("total"));
}

// ---------------------------------------------------------------- startAt cursors

FD_TEST("cursor: with no ordering the document name is the only cursor value") {
	json cursor = duckdb::BuildStartAtCursor(json::object(), "projects/p/databases/d/documents/c/doc1", json::object());
	FD_REQUIRE_EQ(cursor["values"].size(), 1u);
	FD_REQUIRE_EQ(cursor["values"][0]["referenceValue"].get<std::string>(),
	              std::string("projects/p/databases/d/documents/c/doc1"));
	FD_REQUIRE_FALSE(cursor["before"].get<bool>());
}

FD_TEST("cursor: a non-array or empty orderBy falls back to the document name") {
	json not_an_array = {{"orderBy", "nonsense"}};
	FD_REQUIRE_EQ(duckdb::BuildStartAtCursor(not_an_array, "doc", json::object())["values"].size(), 1u);

	json empty = {{"orderBy", json::array()}};
	FD_REQUIRE_EQ(duckdb::BuildStartAtCursor(empty, "doc", json::object())["values"].size(), 1u);
}

FD_TEST("cursor: one value per orderBy entry, in order") {
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {{"total", "ASCENDING"}}, 100);
	json fields = {{"total", {{"integerValue", "42"}}}};

	json cursor = duckdb::BuildStartAtCursor(query, "docs/order7", fields);
	FD_REQUIRE_EQ(cursor["values"].size(), 2u);
	FD_REQUIRE_EQ(cursor["values"][0]["integerValue"].get<std::string>(), std::string("42"));
	FD_REQUIRE_EQ(cursor["values"][1]["referenceValue"].get<std::string>(), std::string("docs/order7"));
}

FD_TEST("cursor: a field the document lacks contributes an explicit null") {
	// Firestore rejects a cursor with fewer values than orderBy entries, so
	// the slot has to be filled rather than skipped.
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {{"total", "ASCENDING"}}, 100);
	json cursor = duckdb::BuildStartAtCursor(query, "docs/order7", json::object());
	FD_REQUIRE_EQ(cursor["values"].size(), 2u);
	FD_REQUIRE(cursor["values"][0].contains("nullValue"));
}

FD_TEST("cursor: a nested ordering field resolves through the map") {
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {{"customer.tier", "ASCENDING"}}, 100);
	json fields = {{"customer", json {{"mapValue", {{"fields", json {{"tier", {{"stringValue", "gold"}}}}}}}}}};

	json cursor = duckdb::BuildStartAtCursor(query, "docs/order7", fields);
	FD_REQUIRE_EQ(cursor["values"][0]["stringValue"].get<std::string>(), std::string("gold"));
}

FD_TEST("cursor: null document fields resolve to nulls rather than crashing") {
	// Phantom documents carry no fields at all.
	json query = duckdb::BuildCollectionGroupStructuredQuery("orders", {{"total", "ASCENDING"}}, 100);
	json cursor = duckdb::BuildStartAtCursor(query, "docs/order7", json(nullptr));
	FD_REQUIRE(cursor["values"][0].contains("nullValue"));
}

FD_TEST("cursor: a malformed orderBy entry still yields an aligned value") {
	json query = {
	    {"orderBy", json::array({json::object(), json {{"field", "not-an-object"}},
	                             json {{"field", {{"fieldPath", 7}}}}, json {{"field", {{"fieldPath", ""}}}}})}};
	json cursor = duckdb::BuildStartAtCursor(query, "docs/order7", json::object());
	FD_REQUIRE_EQ(cursor["values"].size(), 4u);
	for (const auto &value : cursor["values"]) {
		FD_REQUIRE_EQ(value["referenceValue"].get<std::string>(), std::string("docs/order7"));
	}
}

// ---------------------------------------------------------------- field paths

FD_TEST("field path: a simple identifier is left alone") {
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("name"), std::string("name"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("_private"), std::string("_private"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("field2"), std::string("field2"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("CamelCase_9"), std::string("CamelCase_9"));
}

FD_TEST("field path: a name containing a dot is quoted") {
	// Unquoted, Firestore would read "a.b" as a path into a nested map and
	// return the wrong field -- or none.
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("a.b"), std::string("`a.b`"));
}

FD_TEST("field path: names needing quotes get them") {
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("with space"), std::string("`with space`"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("2leading_digit"), std::string("`2leading_digit`"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("dash-ed"), std::string("`dash-ed`"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath(""), std::string("``"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("na\xc3\xafve"), std::string("`na\xc3\xafve`"));
}

FD_TEST("field path: a reserved-looking name is quoted so it stays a field") {
	// Unquoted, __name__ means the document's resource name rather than a
	// field that happens to be called that.
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("__name__"), std::string("`__name__`"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("__id"), std::string("`__id`"));
	// A single leading underscore is not reserved.
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("_x"), std::string("_x"));
}

FD_TEST("field path: backticks and backslashes inside a quoted name are escaped") {
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("back`tick"), std::string("`back\\`tick`"));
	FD_REQUIRE_EQ(duckdb::QuoteFirestoreFieldPath("back\\slash"), std::string("`back\\\\slash`"));
}

// ---------------------------------------------------------------- url encoding

FD_TEST("url encoding: unreserved characters pass through") {
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("abcXYZ019-_.~"), std::string("abcXYZ019-_.~"));
}

FD_TEST("url encoding: everything else is percent-encoded") {
	// A backtick-quoted field name is full of characters that would otherwise
	// terminate or corrupt the query string.
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("`a.b`"), std::string("%60a.b%60"));
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("a b"), std::string("a%20b"));
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("a&b=c"), std::string("a%26b%3Dc"));
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("100%"), std::string("100%25"));
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue(""), std::string(""));
}

FD_TEST("url encoding: non-ASCII bytes are encoded, not mangled") {
	// UTF-8 is encoded byte by byte; a signed char must not sign-extend into
	// the hex conversion.
	FD_REQUIRE_EQ(duckdb::UrlEncodeQueryValue("\xc3\xa9"), std::string("%C3%A9"));
}

// ---------------------------------------------------------------- select clause

FD_TEST("select: projected fields become a select clause") {
	duckdb::FirestoreProjection projection;
	projection.masked = true;
	projection.field_paths = {"name", "score"};

	json select = duckdb::BuildSelectClause(projection);
	FD_REQUIRE_EQ(select["fields"].size(), 2u);
	FD_REQUIRE_EQ(select["fields"][0]["fieldPath"].get<std::string>(), std::string("name"));
	FD_REQUIRE_EQ(select["fields"][1]["fieldPath"].get<std::string>(), std::string("score"));
}

FD_TEST("select: field names are quoted on the way into the clause") {
	duckdb::FirestoreProjection projection;
	projection.masked = true;
	projection.field_paths = {"a.b"};
	FD_REQUIRE_EQ(duckdb::BuildSelectClause(projection)["fields"][0]["fieldPath"].get<std::string>(),
	              std::string("`a.b`"));
}

FD_TEST("select: wanting no fields becomes Firestore's keys-only projection") {
	duckdb::FirestoreProjection projection;
	projection.masked = true;

	FD_REQUIRE(projection.KeysOnly());
	json select = duckdb::BuildSelectClause(projection);
	FD_REQUIRE_EQ(select["fields"].size(), 1u);
	FD_REQUIRE_EQ(select["fields"][0]["fieldPath"].get<std::string>(), std::string("__name__"));
}

FD_TEST("select: an unmasked projection is never keys-only") {
	// masked == false means "send everything", which is the opposite of
	// keys-only and must not be confused with it just because the field list
	// is empty.
	duckdb::FirestoreProjection projection;
	FD_REQUIRE_FALSE(projection.masked);
	FD_REQUIRE_FALSE(projection.KeysOnly());
}
