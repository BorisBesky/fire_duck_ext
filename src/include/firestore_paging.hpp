#pragma once

// Paging policy and Firestore query construction.
//
// Like firestore_wire.hpp this header carries no DuckDB dependency, so the
// paging rules can be unit-tested directly. See firestore_wire.hpp for why.

#include "firestore_wire.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {

// Firestore caps a single documents.list page -- and a single runQuery
// `limit` -- at 1000 documents. Asking for more is not an error, it is
// silently truncated, so the extension clamps instead.
constexpr int64_t FIRESTORE_MAX_PAGE_SIZE = 1000;
constexpr int64_t FIRESTORE_MIN_PAGE_SIZE = 1;
constexpr int64_t FIRESTORE_DEFAULT_PAGE_SIZE = 1000;

// Default ceiling on the uncompressed bytes one page may weigh before the
// page size is reduced. A Firestore document can be 1 MiB, so a full
// 1000-document page can in principle be ~1 GiB of JSON -- several times that
// once parsed into a DOM. 64 MiB keeps a page comfortably in memory while
// staying far above any ordinary collection's page weight, so normal scans
// never shrink at all. 0 disables the guard.
constexpr int64_t FIRESTORE_DEFAULT_PAGE_BYTE_BUDGET = 64LL * 1024 * 1024;

// Bring a requested page size into Firestore's accepted range.
int64_t ClampFirestorePageSize(int64_t requested);

// Normalize a byte budget: anything negative means "disabled", same as 0.
int64_t NormalizeFirestorePageByteBudget(int64_t requested);

// Chooses how many documents to request per round trip, and shrinks that
// number when pages turn out to be heavier than the byte budget allows.
//
// The page size only ever decreases within one scan. Growing it back after a
// shrink would oscillate on collections whose document sizes vary, spending
// round trips to rediscover a limit already found; a scan that has met big
// documents once is likely to meet them again.
class FirestorePageSizePolicy {
public:
	FirestorePageSizePolicy();
	FirestorePageSizePolicy(int64_t page_size, int64_t byte_budget);

	int64_t CurrentPageSize() const {
		return page_size_;
	}
	int64_t ByteBudget() const {
		return byte_budget_;
	}
	// How many times ObserveResponse has reduced the page size.
	int64_t ShrinkCount() const {
		return shrinks_;
	}

	// Documents to request next. `cap` bounds the request further when
	// positive -- a scan_limit below the page size, say; pass 0 for no cap.
	int64_t NextRequestSize(int64_t cap = 0) const;

	// Report what a response actually cost. `response_bytes` is the
	// uncompressed body size, which is what the JSON DOM is built from.
	// Returns true when the page size was reduced as a result.
	bool ObserveResponse(int64_t documents_returned, int64_t response_bytes);

private:
	int64_t page_size_;
	int64_t byte_budget_;
	int64_t shrinks_ = 0;
};

// Build a structured-query orderBy array, appending __name__ when the caller's
// ordering does not already include it.
//
// Cursor pagination needs a total order: without __name__ as a tiebreaker,
// documents sharing an ordering key can be returned twice or skipped entirely
// across page boundaries. Firestore rejects mixed directions unless a
// composite index covers them, so the appended __name__ follows the direction
// of the last ordering field.
json BuildOrderByArray(const std::vector<OrderByField> &order_by);

// Build the StructuredQuery for an unfiltered collection-group scan.
// Always ordered, so the result can be paginated with a startAt cursor.
json BuildCollectionGroupStructuredQuery(const std::string &collection_id, const std::vector<OrderByField> &order_by,
                                         int64_t page_size);

// Quote a Firestore field name for use in a field path.
//
// A field path is dot-separated, so a field whose name contains a dot has to
// be backtick-quoted or Firestore reads it as a path into a nested map and
// returns the wrong thing (or nothing). Names starting with `__` are quoted
// too: `__name__` unquoted means the document's resource name, not a field
// that happens to be called that.
std::string QuoteFirestoreFieldPath(const std::string &field_name);

// Percent-encode a value for a URL query parameter. Field names are arbitrary
// Firestore strings and reach the wire as query parameters in a document mask.
std::string UrlEncodeQueryValue(const std::string &value);

// What a scan needs Firestore to send back for each document.
struct FirestoreProjection {
	// Field names to request. Empty with `masked` set means keys only.
	std::vector<std::string> field_paths;

	// False when every field is needed and no mask may be sent -- an unmapped
	// catch-all column, for instance, is defined as "whatever the schema does
	// not cover", so masking would empty it.
	bool masked = false;

	// True when no document fields are wanted at all, only names. runQuery
	// expresses this as select __name__; documents.list cannot express it (an
	// absent mask parameter means "all fields"), so that path sends no mask.
	bool KeysOnly() const {
		return masked && field_paths.empty();
	}
};

// Build the `select` clause for a StructuredQuery. A projection wanting no
// fields becomes Firestore's documented keys-only form, select __name__.
json BuildSelectClause(const FirestoreProjection &projection);

// Build the body of a :runAggregationQuery that counts a collection.
//
// `up_to` bounds the count: Firestore stops once it reaches that many, which
// is all a query under a LIMIT needs. Omit it to count everything.
json BuildCountAggregationQuery(const std::string &collection_id, bool all_descendants, int64_t up_to = 0);

// Read the count out of a :runAggregationQuery response. Returns false when
// the response does not carry one, which the caller treats as "fall back to
// scanning" rather than as an error.
bool ParseCountAggregationResponse(const json &response, int64_t &count_out);

// Build the `startAt` cursor that resumes a runQuery after `last_document`.
//
// The cursor must carry one value per orderBy entry, in order, or Firestore
// rejects it -- so an entry whose field the document lacks contributes an
// explicit null rather than being omitted.
json BuildStartAtCursor(const json &structured_query, const std::string &last_document_name,
                        const json &last_document_fields);

} // namespace duckdb
