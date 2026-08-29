#include "firestore_paging.hpp"

#include <exception>
#include <string>
#include <vector>

namespace duckdb {

int64_t ClampFirestorePageSize(int64_t requested) {
	if (requested < FIRESTORE_MIN_PAGE_SIZE) {
		return FIRESTORE_MIN_PAGE_SIZE;
	}
	if (requested > FIRESTORE_MAX_PAGE_SIZE) {
		return FIRESTORE_MAX_PAGE_SIZE;
	}
	return requested;
}

int64_t NormalizeFirestorePageByteBudget(int64_t requested) {
	return requested < 0 ? 0 : requested;
}

FirestorePageSizePolicy::FirestorePageSizePolicy()
    : FirestorePageSizePolicy(FIRESTORE_DEFAULT_PAGE_SIZE, FIRESTORE_DEFAULT_PAGE_BYTE_BUDGET) {
}

FirestorePageSizePolicy::FirestorePageSizePolicy(int64_t page_size, int64_t byte_budget)
    : page_size_(ClampFirestorePageSize(page_size)), byte_budget_(NormalizeFirestorePageByteBudget(byte_budget)) {
}

int64_t FirestorePageSizePolicy::NextRequestSize(int64_t cap) const {
	if (cap > 0 && cap < page_size_) {
		return cap;
	}
	return page_size_;
}

bool FirestorePageSizePolicy::ObserveResponse(int64_t documents_returned, int64_t response_bytes) {
	if (byte_budget_ <= 0) {
		return false; // Guard disabled.
	}
	if (response_bytes <= byte_budget_) {
		return false;
	}
	if (documents_returned <= 0) {
		return false; // No documents to attribute the bytes to.
	}
	if (page_size_ <= FIRESTORE_MIN_PAGE_SIZE) {
		return false; // Already asking for the smallest page Firestore serves.
	}

	// Round up, so a page of documents each just over the per-document share
	// is not judged to fit.
	const int64_t bytes_per_document = (response_bytes + documents_returned - 1) / documents_returned;
	int64_t target = byte_budget_ / bytes_per_document;

	// An over-budget page must always lead to a smaller one, or a scan could
	// keep re-requesting the size that just overran. The estimate only lands
	// at or above the current size when the observed page held more documents
	// than the policy would now ask for -- a response measured after an
	// earlier shrink, say.
	const int64_t ceiling = page_size_ - 1;
	if (target > ceiling) {
		target = ceiling;
	}
	// The budget can be smaller than a single document. One document per
	// request is then the closest the policy can get; Firestore has no
	// smaller page.
	if (target < FIRESTORE_MIN_PAGE_SIZE) {
		target = FIRESTORE_MIN_PAGE_SIZE;
	}

	page_size_ = target;
	shrinks_++;
	return true;
}

json BuildOrderByArray(const std::vector<OrderByField> &order_by) {
	json order_by_array = json::array();
	bool has_name = false;
	for (const auto &field : order_by) {
		order_by_array.push_back({{"field", {{"fieldPath", field.field_path}}}, {"direction", field.direction}});
		if (field.field_path == "__name__") {
			has_name = true;
		}
	}
	if (!has_name) {
		const std::string direction = order_by.empty() ? "ASCENDING" : order_by.back().direction;
		order_by_array.push_back({{"field", {{"fieldPath", "__name__"}}}, {"direction", direction}});
	}
	return order_by_array;
}

json BuildCollectionGroupStructuredQuery(const std::string &collection_id, const std::vector<OrderByField> &order_by,
                                         int64_t page_size) {
	json structured_query;
	structured_query["from"] = {{{"collectionId", collection_id}, {"allDescendants", true}}};
	structured_query["orderBy"] = BuildOrderByArray(order_by);
	structured_query["limit"] = ClampFirestorePageSize(page_size);
	return structured_query;
}

std::string QuoteFirestoreFieldPath(const std::string &field_name) {
	// Firestore accepts a segment unquoted only when it matches
	// [a-zA-Z_][a-zA-Z_0-9]*. `__`-prefixed names are excluded here as well:
	// they are Firestore's reserved space, and __name__ in particular already
	// means the document's resource name.
	bool simple = !field_name.empty() && field_name.rfind("__", 0) != 0;
	if (simple) {
		for (size_t i = 0; i < field_name.size(); i++) {
			const char c = field_name[i];
			const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
			const bool digit = c >= '0' && c <= '9';
			if (!(alpha || (i > 0 && digit))) {
				simple = false;
				break;
			}
		}
	}
	if (simple) {
		return field_name;
	}

	// Backtick-quoted, with backslashes and backticks escaped.
	std::string quoted = "`";
	for (const char c : field_name) {
		if (c == '\\' || c == '`') {
			quoted += '\\';
		}
		quoted += c;
	}
	quoted += '`';
	return quoted;
}

std::string UrlEncodeQueryValue(const std::string &value) {
	static const char *kHexDigits = "0123456789ABCDEF";
	std::string encoded;
	encoded.reserve(value.size());
	for (const char c : value) {
		const bool unreserved = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		                        c == '-' || c == '_' || c == '.' || c == '~';
		if (unreserved) {
			encoded += c;
			continue;
		}
		const auto byte = static_cast<unsigned char>(c);
		encoded += '%';
		encoded += kHexDigits[byte >> 4];
		encoded += kHexDigits[byte & 0x0F];
	}
	return encoded;
}

json BuildSelectClause(const FirestoreProjection &projection) {
	json fields = json::array();
	if (projection.field_paths.empty()) {
		// Firestore's keys-only projection: every document comes back with a
		// name and no fields.
		fields.push_back({{"fieldPath", "__name__"}});
	} else {
		for (const auto &field_path : projection.field_paths) {
			fields.push_back({{"fieldPath", QuoteFirestoreFieldPath(field_path)}});
		}
	}
	return json {{"fields", fields}};
}

namespace {

// Firestore's auto-id alphabet, in the byte order Firestore sorts by: digits,
// then upper case, then lower case. Auto-ids draw uniformly from these 62
// characters, so cutting the space evenly over this alphabet cuts a
// collection of auto-ids into roughly equal parts.
const char *const kSortedIdAlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr int64_t kIdAlphabetSize = 62;

// Characters per boundary. Three gives 62^3 = 238,328 distinct cut points --
// far more than any useful thread count, while keeping the boundary strings
// short enough to read in a log.
constexpr int kBoundaryLength = 3;

// Render `value` as a fixed-width boundary over the sorted alphabet.
std::string RenderBoundary(int64_t value) {
	std::string boundary(kBoundaryLength, kSortedIdAlphabet[0]);
	for (int position = kBoundaryLength - 1; position >= 0; position--) {
		boundary[position] = kSortedIdAlphabet[value % kIdAlphabetSize];
		value /= kIdAlphabetSize;
	}
	return boundary;
}

} // namespace

std::vector<FirestoreKeyRange> BuildKeyRangePartitions(int64_t partitions) {
	std::vector<FirestoreKeyRange> ranges;
	if (partitions <= 1) {
		ranges.push_back(FirestoreKeyRange {});
		return ranges;
	}

	int64_t total_points = 1;
	for (int i = 0; i < kBoundaryLength; i++) {
		total_points *= kIdAlphabetSize;
	}
	// More partitions than distinct cut points would produce duplicate
	// boundaries and therefore empty ranges.
	if (partitions > total_points) {
		partitions = total_points;
	}

	std::string previous_boundary;
	for (int64_t index = 1; index < partitions; index++) {
		const std::string boundary = RenderBoundary(index * total_points / partitions);
		ranges.push_back(FirestoreKeyRange {previous_boundary, boundary});
		previous_boundary = boundary;
	}
	ranges.push_back(FirestoreKeyRange {previous_boundary, std::string()});
	return ranges;
}

json BuildKeyRangeStructuredQuery(const std::string &collection_id, const std::string &document_path_prefix,
                                  const FirestoreKeyRange &range, int64_t page_size,
                                  const FirestoreProjection &projection) {
	json structured_query;
	structured_query["from"] = {{{"collectionId", collection_id}, {"allDescendants", false}}};
	structured_query["orderBy"] = BuildOrderByArray({});
	structured_query["limit"] = ClampFirestorePageSize(page_size);
	if (projection.masked) {
		structured_query["select"] = BuildSelectClause(projection);
	}

	// Cursors on __name__ take the document's full resource name.
	//
	// Firestore's `before` flag says which side of the given position the
	// cursor sits on, and the two bounds need opposite answers: a document
	// whose id is exactly a boundary must be read by the range that starts
	// there and skipped by the one that ends there. Get this wrong in the same
	// direction on both and the document is either read twice or lost.
	if (!range.start_document_id.empty()) {
		// before=true on a start cursor is startAt: inclusive.
		structured_query["startAt"] = {
		    {"values", json::array({json {{"referenceValue", document_path_prefix + "/" + range.start_document_id}}})},
		    {"before", true}};
	}
	if (!range.end_document_id.empty()) {
		// before=true on an end cursor is endBefore: exclusive.
		structured_query["endAt"] = {
		    {"values", json::array({json {{"referenceValue", document_path_prefix + "/" + range.end_document_id}}})},
		    {"before", true}};
	}
	return structured_query;
}

json BuildCountAggregationQuery(const std::string &collection_id, bool all_descendants, int64_t up_to) {
	json structured_query;
	structured_query["from"] = {{{"collectionId", collection_id}, {"allDescendants", all_descendants}}};

	json count = json::object();
	if (up_to > 0) {
		// Int64 fields cross the Firestore wire as strings.
		count["upTo"] = std::to_string(up_to);
	}

	return json {{"structuredAggregationQuery",
	              {{"structuredQuery", structured_query},
	               {"aggregations", json::array({json {{"alias", "count"}, {"count", count}}})}}}};
}

bool ParseCountAggregationResponse(const json &response, int64_t &count_out) {
	// The response is an array of results, each carrying the aggregate fields
	// under the aliases the request asked for.
	const json *result = nullptr;
	if (response.is_array()) {
		for (const auto &entry : response) {
			if (entry.is_object() && entry.contains("result")) {
				result = &entry["result"];
				break;
			}
		}
	} else if (response.is_object() && response.contains("result")) {
		result = &response["result"];
	}

	if (result == nullptr || !result->is_object() || !result->contains("aggregateFields")) {
		return false;
	}
	const auto &aggregate_fields = (*result)["aggregateFields"];
	if (!aggregate_fields.is_object() || !aggregate_fields.contains("count")) {
		return false;
	}
	const auto &count_value = aggregate_fields["count"];
	if (!count_value.is_object() || !count_value.contains("integerValue")) {
		return false;
	}

	const auto &integer_value = count_value["integerValue"];
	try {
		if (integer_value.is_string()) {
			count_out = std::stoll(integer_value.get<std::string>());
		} else if (integer_value.is_number_integer()) {
			count_out = integer_value.get<int64_t>();
		} else {
			return false;
		}
	} catch (const std::exception &) {
		return false; // Out of range or not a number after all.
	}
	return count_out >= 0;
}

json BuildStartAtCursor(const json &structured_query, const std::string &last_document_name,
                        const json &last_document_fields) {
	json values = json::array();

	auto order_by = structured_query.find("orderBy");
	if (order_by == structured_query.end() || !order_by->is_array() || order_by->empty()) {
		// No declared ordering: Firestore orders by __name__ implicitly, so
		// that is the only cursor value the query can take.
		values.push_back({{"referenceValue", last_document_name}});
		return json {{"values", values}, {"before", false}};
	}

	for (const auto &entry : *order_by) {
		std::string field_path;
		auto field = entry.find("field");
		if (field != entry.end() && field->is_object()) {
			auto path = field->find("fieldPath");
			if (path != field->end() && path->is_string()) {
				field_path = path->get<std::string>();
			}
		}

		// A malformed entry still needs a cursor value to keep the array
		// aligned with orderBy; the document's own name is the safe filler.
		if (field_path.empty() || field_path == "__name__") {
			values.push_back({{"referenceValue", last_document_name}});
			continue;
		}

		const json *resolved = ResolveFirestoreFieldPath(last_document_fields, field_path);
		if (resolved != nullptr) {
			values.push_back(*resolved);
		} else {
			values.push_back({{"nullValue", nullptr}});
		}
	}

	return json {{"values", values}, {"before", false}};
}

} // namespace duckdb
