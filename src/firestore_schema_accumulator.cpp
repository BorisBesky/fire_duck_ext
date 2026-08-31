#include "firestore_schema_accumulator.hpp"

#include <vector>

namespace duckdb {

FirestoreSchemaAccumulator::FirestoreSchemaAccumulator(int64_t sample_size) : sample_size_(sample_size) {
}

bool FirestoreSchemaAccumulator::IsFull() const {
	if (sample_size_ <= 0) {
		return false;
	}
	return documents_seen_ >= sample_size_;
}

int64_t FirestoreSchemaAccumulator::RemainingSample() const {
	if (sample_size_ <= 0) {
		return -1;
	}
	const int64_t remaining = sample_size_ - documents_seen_;
	return remaining > 0 ? remaining : 0;
}

void FirestoreSchemaAccumulator::AddDocument(const json &fields) {
	documents_seen_++;

	if (fields.is_null() || fields.empty() || !fields.is_object()) {
		return;
	}

	for (auto it = fields.begin(); it != fields.end(); ++it) {
		const std::string &field_name = it.key();
		const json &field_value = it.value();
		const std::string type_name = GetFirestoreTypeName(field_value);

		auto &summary = fields_[field_name];
		if (summary.type_name.empty()) {
			summary.type_name = type_name;
		}
		summary.documents_present++;
		summary.type_names.insert(type_name);

		if (type_name == "arrayValue" && field_value["arrayValue"].contains("values")) {
			for (const auto &element : field_value["arrayValue"]["values"]) {
				const std::string element_type = GetFirestoreTypeName(element);
				if (element_type != "nullValue") {
					summary.array_element_types[element_type]++;
				}
			}
		}

		if (type_name == "vectorValue" && !summary.has_vector_dimension && FirestoreVectorHasValues(field_value)) {
			summary.vector_dimension = FirestoreVectorDimension(field_value);
			summary.has_vector_dimension = true;
		}
	}
}

FirestoreSchemaAccumulator::OrderingSafety FirestoreSchemaAccumulator::Ordering(bool sample_exhaustive) const {
	OrderingSafety safety;
	safety.documents_sampled = documents_seen_;
	safety.exhaustive = sample_exhaustive;

	for (const auto &entry : fields_) {
		const std::string &field_name = entry.first;
		const auto &summary = entry.second;

		if (summary.documents_present < documents_seen_) {
			const int64_t absent = documents_seen_ - summary.documents_present;
			safety.reasons[field_name] = std::to_string(absent) + " of " + std::to_string(documents_seen_) +
			                             " sampled documents do not have it, and Firestore returns no document that "
			                             "lacks the field it is ordered by";
			continue;
		}
		if (summary.type_names.size() > 1) {
			std::string types;
			for (const auto &type_name : summary.type_names) {
				types += (types.empty() ? "" : ", ") + type_name;
			}
			safety.reasons[field_name] =
			    "it holds more than one type (" + types +
			    "), so DuckDB compares it as text while Firestore orders it by its own type precedence";
			continue;
		}
		safety.safe_fields.insert(field_name);
	}
	return safety;
}

std::string FirestoreSchemaAccumulator::WidenElementTypes(const std::map<std::string, int64_t> &element_types) {
	// Types counted zero times were never actually seen.
	std::vector<std::string> present;
	for (const auto &entry : element_types) {
		if (entry.second > 0) {
			present.push_back(entry.first);
		}
	}

	if (present.empty()) {
		return "stringValue";
	}
	if (present.size() == 1) {
		return present.front();
	}
	// The one mix with a narrower home than string: a number column holds both.
	if (present.size() == 2 && present[0] == "doubleValue" && present[1] == "integerValue") {
		return "doubleValue";
	}
	// Everything else renders as text, the same fallback a scalar field holding
	// more than one type already takes.
	return "stringValue";
}

} // namespace duckdb
