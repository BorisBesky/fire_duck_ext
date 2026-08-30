#pragma once

// Streaming schema inference.
//
// Schema inference used to hold every sampled document in one vector and walk
// it at the end, so `schema_sample_size := -1` ("sample everything") pulled an
// entire collection into memory at bind time -- before any LIMIT or filter
// could reduce it. This accumulator folds each page into a fixed-size summary
// instead, letting the caller drop the documents as soon as they are seen.
//
// DuckDB-free by design (see firestore_wire.hpp): it summarises Firestore type
// names, and the caller maps those onto LogicalTypes.

#include "firestore_wire.hpp"
#include <cstdint>
#include <map>
#include <string>

namespace duckdb {

class FirestoreSchemaAccumulator {
public:
	struct FieldSummary {
		// Firestore type wrapper first seen for this field. Later documents
		// that disagree do not change it.
		std::string type_name;
		// For arrayValue fields: how often each element type was seen, used to
		// pick the list's element type by majority. Explicit nulls are not
		// counted -- they say nothing about the element type.
		std::map<std::string, int64_t> array_element_types;
		// For vector fields: element count from the first occurrence that
		// carried a `values` array. 0 means the dimension is unknown and the
		// caller should fall back to a plain list.
		uint64_t vector_dimension = 0;
		bool has_vector_dimension = false;
	};

	// `sample_size` <= 0 means "sample every document".
	explicit FirestoreSchemaAccumulator(int64_t sample_size);

	// True once enough documents have been seen. Always false for an
	// unlimited sample.
	bool IsFull() const;

	// Documents still wanted, or -1 when the sample is unlimited. 0 when full.
	int64_t RemainingSample() const;

	// Fold one document's `fields` object into the summary. Documents with no
	// fields (Firestore "phantom" documents, which exist only to parent a
	// subcollection) count toward the sample but contribute no columns --
	// otherwise a collection fronted by phantoms would sample nothing.
	void AddDocument(const json &fields);

	int64_t DocumentsSeen() const {
		return documents_seen_;
	}

	const std::map<std::string, FieldSummary> &Fields() const {
		return fields_;
	}

	// Element type to use for an array field: the one type its elements have,
	// or the narrowest type that can hold all of them.
	//
	// Firestore arrays are heterogeneous by design, so an element type has to
	// be chosen that every element fits. Picking the most common one instead
	// left the rest to fail: a list of an integer and a string became BIGINT
	// and the scan threw on the string, failing the whole query over data
	// Firestore accepts. Integers and doubles widen to double, since that is
	// what a number column does; any other mix widens to string, which is
	// what a scalar field holding several types already does.
	//
	// An empty count map yields "stringValue" -- the safest rendering for an
	// element type nothing was learned about.
	static std::string WidenElementTypes(const std::map<std::string, int64_t> &element_types);

private:
	int64_t sample_size_; // <= 0 means unlimited
	int64_t documents_seen_ = 0;
	std::map<std::string, FieldSummary> fields_;
};

} // namespace duckdb
