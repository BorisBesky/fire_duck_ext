#include "firestore_paging.hpp"

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
