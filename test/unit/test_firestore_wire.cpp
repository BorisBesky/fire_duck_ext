#include "test_harness.hpp"
#include "firestore_wire.hpp"

using duckdb::json;

namespace {

// Wraps a fields object into a Firestore mapValue. Nesting these by hand
// reads as a wall of braces; this keeps the test data legible.
json Map(json fields) {
	return json {{"mapValue", {{"fields", std::move(fields)}}}};
}

json Vector(std::initializer_list<double> values) {
	json elements = json::array();
	for (double value : values) {
		elements.push_back({{"doubleValue", value}});
	}
	return json {
	    {"mapValue",
	     {{"fields",
	       {{"__type__", {{"stringValue", "__vector__"}}}, {"value", {{"arrayValue", {{"values", elements}}}}}}}}}};
}

} // namespace

// ---------------------------------------------------------------- IsFirestoreNull

FD_TEST("wire: IsFirestoreNull recognises explicit nulls only") {
	FD_REQUIRE(duckdb::IsFirestoreNull(json {{"nullValue", nullptr}}));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreNull(json {{"stringValue", ""}}));
	FD_REQUIRE_FALSE(duckdb::IsFirestoreNull(json::object()));
}

// ---------------------------------------------------------------- IsFirestoreVector

FD_TEST("wire: IsFirestoreVector accepts a well-formed vector") {
	FD_REQUIRE(duckdb::IsFirestoreVector(Vector({1.0, 2.0, 3.0})));
}

FD_TEST("wire: IsFirestoreVector rejects every malformed shape") {
	// Each case removes exactly one thing a vector needs, so every early
	// return in IsFirestoreVector is exercised.
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(json {{"stringValue", "x"}}));                       // no mapValue
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(json {{"mapValue", json::object()}}));               // no fields
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(json {{"mapValue", {{"fields", json::object()}}}})); // no __type__

	json wrong_type_wrapper = Vector({1.0});
	wrong_type_wrapper["mapValue"]["fields"]["__type__"] = json {{"integerValue", "1"}};
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(wrong_type_wrapper)); // __type__ is not a string

	json wrong_marker = Vector({1.0});
	wrong_marker["mapValue"]["fields"]["__type__"]["stringValue"] = "__not_a_vector__";
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(wrong_marker)); // wrong marker text

	json no_value = Vector({1.0});
	no_value["mapValue"]["fields"].erase("value");
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(no_value)); // no value field

	json value_not_array = Vector({1.0});
	value_not_array["mapValue"]["fields"]["value"] = json {{"stringValue", "x"}};
	FD_REQUIRE_FALSE(duckdb::IsFirestoreVector(value_not_array)); // value is not an arrayValue
}

// ---------------------------------------------------------------- vector dimension

FD_TEST("wire: FirestoreVectorDimension counts elements") {
	FD_REQUIRE_EQ(duckdb::FirestoreVectorDimension(Vector({1.0, 2.0, 3.0})), 3u);
	FD_REQUIRE_EQ(duckdb::FirestoreVectorDimension(Vector({})), 0u);
	FD_REQUIRE_EQ(duckdb::FirestoreVectorDimension(json {{"stringValue", "x"}}), 0u);
}

FD_TEST("wire: a vector with no values array reports dimension unknown") {
	json no_values = Vector({1.0});
	no_values["mapValue"]["fields"]["value"]["arrayValue"] = json::object();

	// Still a vector -- arrayValue is present, just empty of a values key.
	FD_REQUIRE(duckdb::IsFirestoreVector(no_values));
	FD_REQUIRE_EQ(duckdb::FirestoreVectorDimension(no_values), 0u);
	FD_REQUIRE_FALSE(duckdb::FirestoreVectorHasValues(no_values));
}

FD_TEST("wire: FirestoreVectorHasValues separates empty from absent") {
	FD_REQUIRE(duckdb::FirestoreVectorHasValues(Vector({})));                        // present but empty
	FD_REQUIRE(duckdb::FirestoreVectorHasValues(Vector({1.0})));                     // present and populated
	FD_REQUIRE_FALSE(duckdb::FirestoreVectorHasValues(json {{"stringValue", "x"}})); // not a vector
}

// ---------------------------------------------------------------- type names

FD_TEST("wire: GetFirestoreTypeName covers every wrapper") {
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"stringValue", "s"}}), std::string("stringValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"integerValue", "1"}}), std::string("integerValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"doubleValue", 1.5}}), std::string("doubleValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"booleanValue", true}}), std::string("booleanValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"timestampValue", "2026-01-01T00:00:00Z"}}),
	              std::string("timestampValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"geoPointValue", {{"latitude", 1.0}, {"longitude", 2.0}}}}),
	              std::string("geoPointValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"arrayValue", {{"values", json::array()}}}}),
	              std::string("arrayValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(Vector({1.0})), std::string("vectorValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"mapValue", {{"fields", json::object()}}}}),
	              std::string("mapValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"referenceValue", "projects/p/databases/d/documents/c/x"}}),
	              std::string("referenceValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"bytesValue", "AAA="}}), std::string("bytesValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json {{"nullValue", nullptr}}), std::string("nullValue"));
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(json::object()), std::string("unknown"));
}

FD_TEST("wire: a vector is reported as a vector, not as the map it is encoded in") {
	// The vector check must come before the plain mapValue check, or every
	// embedding would be typed as a map.
	FD_REQUIRE_EQ(duckdb::GetFirestoreTypeName(Vector({1.0, 2.0})), std::string("vectorValue"));
}

// ---------------------------------------------------------------- field paths

FD_TEST("path: a top-level field resolves") {
	json fields = {{"name", {{"stringValue", "ada"}}}};
	const json *resolved = duckdb::ResolveFirestoreFieldPath(fields, "name");
	FD_REQUIRE(resolved != nullptr);
	FD_REQUIRE_EQ((*resolved)["stringValue"].get<std::string>(), std::string("ada"));
}

FD_TEST("path: a literal key containing a dot wins over nested interpretation") {
	// Firestore permits a field genuinely named "a.b"; reading it as a path
	// into a nested map would silently return the wrong value.
	json fields = {{"a.b", {{"stringValue", "literal"}}}, {"a", Map({{"b", {{"stringValue", "nested"}}}})}};
	const json *resolved = duckdb::ResolveFirestoreFieldPath(fields, "a.b");
	FD_REQUIRE(resolved != nullptr);
	FD_REQUIRE_EQ((*resolved)["stringValue"].get<std::string>(), std::string("literal"));
}

FD_TEST("path: a nested path walks through mapValues") {
	json fields = {{"a", Map({{"b", Map({{"c", {{"integerValue", "7"}}}})}})}};
	const json *resolved = duckdb::ResolveFirestoreFieldPath(fields, "a.b.c");
	FD_REQUIRE(resolved != nullptr);
	FD_REQUIRE_EQ((*resolved)["integerValue"].get<std::string>(), std::string("7"));
}

FD_TEST("path: unresolvable paths return null") {
	json fields = {{"a", Map({{"b", {{"integerValue", "1"}}}})}, {"scalar", {{"stringValue", "s"}}}};

	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "missing") == nullptr);       // no such key
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "a.missing") == nullptr);     // missing segment
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "scalar.deeper") == nullptr); // not a map
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "a.b.c") == nullptr);         // leaf is not a map
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "") == nullptr);              // empty path
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "a..b") == nullptr);          // empty segment
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, ".a") == nullptr);            // leading dot
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "a.") == nullptr);            // trailing dot
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(json(nullptr), "a") == nullptr);      // no fields at all
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(json::array(), "a") == nullptr);      // fields is not an object
}

FD_TEST("path: a map without a fields object stops the walk") {
	json fields = {{"a", {{"mapValue", json::object()}}}};
	FD_REQUIRE(duckdb::ResolveFirestoreFieldPath(fields, "a.b") == nullptr);
}
