#include "firestore_schema_accumulator.hpp"

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

std::string FirestoreSchemaAccumulator::MajorityElementType(const std::map<std::string, int64_t> &element_types) {
	std::string best_type = "stringValue";
	int64_t best_count = 0;
	// std::map iterates in key order, and the comparison is strict, so a tie
	// resolves to the alphabetically first type name.
	for (const auto &entry : element_types) {
		if (entry.second > best_count) {
			best_count = entry.second;
			best_type = entry.first;
		}
	}
	return best_type;
}

} // namespace duckdb
