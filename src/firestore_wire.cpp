#include "firestore_wire.hpp"

#include <vector>

namespace duckdb {

bool IsFirestoreNull(const json &value) {
	return value.contains("nullValue");
}

bool IsFirestoreVector(const json &value) {
	if (!value.contains("mapValue"))
		return false;
	const auto &mv = value["mapValue"];
	if (!mv.contains("fields"))
		return false;
	const auto &fields = mv["fields"];
	if (!fields.contains("__type__"))
		return false;
	const auto &type_field = fields["__type__"];
	if (!type_field.contains("stringValue"))
		return false;
	if (type_field["stringValue"].get<std::string>() != "__vector__")
		return false;
	if (!fields.contains("value"))
		return false;
	const auto &value_field = fields["value"];
	if (!value_field.contains("arrayValue"))
		return false;
	return true;
}

uint64_t FirestoreVectorDimension(const json &value) {
	if (!IsFirestoreVector(value)) {
		return 0;
	}
	const auto &arr = value["mapValue"]["fields"]["value"]["arrayValue"];
	if (arr.contains("values")) {
		return static_cast<uint64_t>(arr["values"].size());
	}
	return 0;
}

bool FirestoreVectorHasValues(const json &value) {
	if (!IsFirestoreVector(value)) {
		return false;
	}
	return value["mapValue"]["fields"]["value"]["arrayValue"].contains("values");
}

std::string GetFirestoreTypeName(const json &value) {
	if (value.contains("stringValue"))
		return "stringValue";
	if (value.contains("integerValue"))
		return "integerValue";
	if (value.contains("doubleValue"))
		return "doubleValue";
	if (value.contains("booleanValue"))
		return "booleanValue";
	if (value.contains("timestampValue"))
		return "timestampValue";
	if (value.contains("geoPointValue"))
		return "geoPointValue";
	if (value.contains("arrayValue"))
		return "arrayValue";
	if (IsFirestoreVector(value))
		return "vectorValue";
	if (value.contains("mapValue"))
		return "mapValue";
	if (value.contains("referenceValue"))
		return "referenceValue";
	if (value.contains("bytesValue"))
		return "bytesValue";
	if (value.contains("nullValue"))
		return "nullValue";
	return "unknown";
}

const json *ResolveFirestoreFieldPath(const json &fields, const std::string &field_path) {
	if (!fields.is_object() || field_path.empty()) {
		return nullptr;
	}

	// A key matching the whole path verbatim wins: Firestore allows a field
	// literally named "a.b", and reading it as a path into a nested map would
	// silently return a different value.
	auto direct = fields.find(field_path);
	if (direct != fields.end()) {
		return &(*direct);
	}

	if (field_path.find('.') == std::string::npos) {
		return nullptr;
	}

	// Walk "a.b.c" through nested mapValues.
	std::vector<std::string> segments;
	size_t start = 0;
	while (true) {
		const size_t dot = field_path.find('.', start);
		if (dot == std::string::npos) {
			segments.push_back(field_path.substr(start));
			break;
		}
		segments.push_back(field_path.substr(start, dot - start));
		start = dot + 1;
	}

	const json *current_fields = &fields;
	const json *resolved = nullptr;
	for (size_t i = 0; i < segments.size(); i++) {
		if (segments[i].empty() || !current_fields->is_object()) {
			return nullptr;
		}
		auto it = current_fields->find(segments[i]);
		if (it == current_fields->end()) {
			return nullptr;
		}
		resolved = &(*it);
		if (i + 1 == segments.size()) {
			break;
		}
		// More segments to go, so this one has to be a map to descend into.
		if (!resolved->contains("mapValue")) {
			return nullptr;
		}
		const auto &map_value = (*resolved)["mapValue"];
		if (!map_value.contains("fields")) {
			return nullptr;
		}
		current_fields = &map_value["fields"];
	}
	return resolved;
}

} // namespace duckdb
