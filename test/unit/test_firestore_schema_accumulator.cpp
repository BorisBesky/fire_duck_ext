#include "test_harness.hpp"
#include "firestore_schema_accumulator.hpp"

using duckdb::FirestoreSchemaAccumulator;
using duckdb::json;

namespace {

json Vector(size_t dimension) {
	json elements = json::array();
	for (size_t i = 0; i < dimension; i++) {
		elements.push_back({{"doubleValue", static_cast<double>(i)}});
	}
	return json {
	    {"mapValue",
	     {{"fields",
	       {{"__type__", {{"stringValue", "__vector__"}}}, {"value", {{"arrayValue", {{"values", elements}}}}}}}}}};
}

json Array(std::initializer_list<json> values) {
	json elements = json::array();
	for (const auto &value : values) {
		elements.push_back(value);
	}
	return json {{"arrayValue", {{"values", elements}}}};
}

} // namespace

// ---------------------------------------------------------------- sample bounds

FD_TEST("accumulator: a bounded sample fills and then reports full") {
	FirestoreSchemaAccumulator accumulator(2);
	FD_REQUIRE_FALSE(accumulator.IsFull());
	FD_REQUIRE_EQ(accumulator.RemainingSample(), 2);

	accumulator.AddDocument(json {{"a", {{"stringValue", "x"}}}});
	FD_REQUIRE_FALSE(accumulator.IsFull());
	FD_REQUIRE_EQ(accumulator.RemainingSample(), 1);

	accumulator.AddDocument(json {{"a", {{"stringValue", "y"}}}});
	FD_REQUIRE(accumulator.IsFull());
	FD_REQUIRE_EQ(accumulator.RemainingSample(), 0);
	FD_REQUIRE_EQ(accumulator.DocumentsSeen(), 2);
}

FD_TEST("accumulator: overshooting the sample keeps remaining at zero") {
	FirestoreSchemaAccumulator accumulator(1);
	accumulator.AddDocument(json {{"a", {{"stringValue", "x"}}}});
	accumulator.AddDocument(json {{"b", {{"stringValue", "y"}}}});
	FD_REQUIRE(accumulator.IsFull());
	FD_REQUIRE_EQ(accumulator.RemainingSample(), 0);
	// Documents past the boundary still contribute -- discarding them would
	// throw away a field for no benefit once it has been paid for.
	FD_REQUIRE_EQ(accumulator.Fields().size(), 2u);
}

FD_TEST("accumulator: an unlimited sample is never full") {
	for (int64_t sample_size : {int64_t(0), int64_t(-1), int64_t(-1000)}) {
		FirestoreSchemaAccumulator accumulator(sample_size);
		FD_REQUIRE_FALSE(accumulator.IsFull());
		FD_REQUIRE_EQ(accumulator.RemainingSample(), -1);
		accumulator.AddDocument(json {{"a", {{"stringValue", "x"}}}});
		FD_REQUIRE_FALSE(accumulator.IsFull());
		FD_REQUIRE_EQ(accumulator.RemainingSample(), -1);
	}
}

// ---------------------------------------------------------------- phantom documents

FD_TEST("accumulator: phantom documents count toward the sample but add no fields") {
	// A Firestore document that exists only to parent a subcollection has no
	// fields. It still consumed a slot on the wire, so it counts.
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json::object());
	accumulator.AddDocument(json(nullptr));
	accumulator.AddDocument(json::array()); // not an object: nothing to read
	FD_REQUIRE_EQ(accumulator.DocumentsSeen(), 3);
	FD_REQUIRE(accumulator.Fields().empty());
}

FD_TEST("accumulator: a collection fronted by phantoms still learns later fields") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json::object());
	accumulator.AddDocument(json {{"late", {{"integerValue", "1"}}}});
	FD_REQUIRE_EQ(accumulator.Fields().size(), 1u);
	FD_REQUIRE_EQ(accumulator.Fields().at("late").type_name, std::string("integerValue"));
}

// ---------------------------------------------------------------- type capture

FD_TEST("accumulator: the first type seen for a field wins") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"f", {{"integerValue", "1"}}}});
	accumulator.AddDocument(json {{"f", {{"stringValue", "later"}}}});
	FD_REQUIRE_EQ(accumulator.Fields().at("f").type_name, std::string("integerValue"));
}

FD_TEST("accumulator: fields from many documents are unioned") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"a", {{"stringValue", "x"}}}});
	accumulator.AddDocument(json {{"b", {{"booleanValue", true}}}});
	accumulator.AddDocument(json {{"a", {{"stringValue", "y"}}}, {"c", {{"doubleValue", 1.5}}}});

	const auto &fields = accumulator.Fields();
	FD_REQUIRE_EQ(fields.size(), 3u);
	FD_REQUIRE_EQ(fields.at("a").type_name, std::string("stringValue"));
	FD_REQUIRE_EQ(fields.at("b").type_name, std::string("booleanValue"));
	FD_REQUIRE_EQ(fields.at("c").type_name, std::string("doubleValue"));
}

FD_TEST("accumulator: an unrecognised wrapper is recorded as unknown") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"weird", json::object()}});
	FD_REQUIRE_EQ(accumulator.Fields().at("weird").type_name, std::string("unknown"));
}

// ---------------------------------------------------------------- arrays

FD_TEST("accumulator: array element types are counted across documents") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"tags", Array({json {{"integerValue", "1"}}, json {{"integerValue", "2"}}})}});
	accumulator.AddDocument(json {{"tags", Array({json {{"stringValue", "x"}}})}});

	const auto &counts = accumulator.Fields().at("tags").array_element_types;
	FD_REQUIRE_EQ(counts.at("integerValue"), 2);
	FD_REQUIRE_EQ(counts.at("stringValue"), 1);
	FD_REQUIRE_EQ(FirestoreSchemaAccumulator::MajorityElementType(counts), std::string("integerValue"));
}

FD_TEST("accumulator: explicit nulls do not vote for an element type") {
	// A null element says nothing about what the list holds.
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {
	    {"tags", Array({json {{"nullValue", nullptr}}, json {{"nullValue", nullptr}}, json {{"doubleValue", 1.0}}})}});

	const auto &counts = accumulator.Fields().at("tags").array_element_types;
	FD_REQUIRE_EQ(counts.size(), 1u);
	FD_REQUIRE_EQ(FirestoreSchemaAccumulator::MajorityElementType(counts), std::string("doubleValue"));
}

FD_TEST("accumulator: an arrayValue with no values array is still typed as an array") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"tags", {{"arrayValue", json::object()}}}});
	FD_REQUIRE_EQ(accumulator.Fields().at("tags").type_name, std::string("arrayValue"));
	FD_REQUIRE(accumulator.Fields().at("tags").array_element_types.empty());
}

FD_TEST("accumulator: majority element type falls back to string when nothing was learned") {
	FD_REQUIRE_EQ(FirestoreSchemaAccumulator::MajorityElementType({}), std::string("stringValue"));
}

FD_TEST("accumulator: an element type tie resolves to the alphabetically first name") {
	// Deterministic beats arbitrary: the same collection must infer the same
	// schema on every bind, cache hit or not.
	std::map<std::string, int64_t> counts {{"stringValue", 3}, {"integerValue", 3}};
	FD_REQUIRE_EQ(FirestoreSchemaAccumulator::MajorityElementType(counts), std::string("integerValue"));
}

// ---------------------------------------------------------------- vectors

FD_TEST("accumulator: vector dimension comes from the first occurrence") {
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"embedding", Vector(8)}});
	accumulator.AddDocument(json {{"embedding", Vector(16)}});

	const auto &summary = accumulator.Fields().at("embedding");
	FD_REQUIRE_EQ(summary.type_name, std::string("vectorValue"));
	FD_REQUIRE(summary.has_vector_dimension);
	FD_REQUIRE_EQ(summary.vector_dimension, 8u);
}

FD_TEST("accumulator: an occurrence carrying no values array leaves the dimension open") {
	// Nothing was learned, so a later well-formed vector must still be able
	// to supply the dimension.
	json no_values = Vector(4);
	no_values["mapValue"]["fields"]["value"]["arrayValue"] = json::object();

	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"embedding", no_values}});
	FD_REQUIRE_FALSE(accumulator.Fields().at("embedding").has_vector_dimension);

	accumulator.AddDocument(json {{"embedding", Vector(12)}});
	FD_REQUIRE(accumulator.Fields().at("embedding").has_vector_dimension);
	FD_REQUIRE_EQ(accumulator.Fields().at("embedding").vector_dimension, 12u);
}

FD_TEST("accumulator: an empty vector fixes the dimension at zero") {
	// Distinct from "no values array": this occurrence did say something, so
	// it settles the field and the caller falls back to an unsized list.
	FirestoreSchemaAccumulator accumulator(-1);
	accumulator.AddDocument(json {{"embedding", Vector(0)}});
	accumulator.AddDocument(json {{"embedding", Vector(6)}});

	const auto &summary = accumulator.Fields().at("embedding");
	FD_REQUIRE(summary.has_vector_dimension);
	FD_REQUIRE_EQ(summary.vector_dimension, 0u);
}

// ---------------------------------------------------------------- memory shape

FD_TEST("accumulator: memory is bounded by field count, not document count") {
	// The point of streaming inference: sampling every document of a large
	// collection must not retain the documents.
	FirestoreSchemaAccumulator accumulator(-1);
	for (int i = 0; i < 50000; i++) {
		accumulator.AddDocument(json {{"a", {{"integerValue", std::to_string(i)}}}, {"b", {{"stringValue", "value"}}}});
	}
	FD_REQUIRE_EQ(accumulator.DocumentsSeen(), 50000);
	FD_REQUIRE_EQ(accumulator.Fields().size(), 2u);
}
