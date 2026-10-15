#pragma once

// Building VARIANT vectors across DuckDB versions.
//
// DuckDB 1.5 builds a VARIANT vector from a tree of VariantValue nodes
// (VariantValue::ToVARIANT). DuckDB 2.0 removed that tree and builds the vector
// in a single pass with VariantBuilder instead. FirestoreVariant gives the
// scanner one tree type for both: on 1.5 it is VariantValue itself, and on 2.0
// it is a small tree with the same interface that is streamed into
// VariantBuilder by FirestoreVariantsToVector.

#include "duckdb.hpp"

#if __has_include("duckdb/common/types/variant_value.hpp")
#include "duckdb/common/types/variant_value.hpp"
#define FIRESTORE_DUCKDB_HAS_VARIANT_VALUE 1
#else
#include "duckdb/common/map.hpp"
#endif

namespace duckdb {

#ifdef FIRESTORE_DUCKDB_HAS_VARIANT_VALUE

using FirestoreVariant = VariantValue;
using FirestoreVariantType = VariantValueType;

// Build the VARIANT vector `result` from one value per row. A MISSING or NULL
// row becomes SQL NULL.
inline void FirestoreVariantsToVector(vector<FirestoreVariant> &input, Vector &result) {
	VariantValue::ToVARIANT(input, result);
}

#else

enum class FirestoreVariantType : uint8_t { PRIMITIVE, OBJECT, ARRAY, MISSING };

// The subset of DuckDB 1.5's VariantValue the scanner uses.
struct FirestoreVariant {
public:
	FirestoreVariant() : value_type(FirestoreVariantType::MISSING) {
	}
	explicit FirestoreVariant(FirestoreVariantType type) : value_type(type) {
	}
	explicit FirestoreVariant(Value &&val)
	    : value_type(FirestoreVariantType::PRIMITIVE), primitive_value(std::move(val)) {
	}
	FirestoreVariant(const FirestoreVariant &) = delete;
	FirestoreVariant &operator=(const FirestoreVariant &) = delete;
	FirestoreVariant(FirestoreVariant &&) noexcept = default;
	FirestoreVariant &operator=(FirestoreVariant &&) noexcept = default;

public:
	bool IsNull() const {
		return value_type == FirestoreVariantType::PRIMITIVE && primitive_value.IsNull();
	}
	bool IsMissing() const {
		return value_type == FirestoreVariantType::MISSING;
	}
	static FirestoreVariant NullValue() {
		return FirestoreVariant(Value());
	}

	// As in VariantValue: a MISSING child is dropped, and for a repeated key
	// the first value wins.
	void AddChild(const string &key, FirestoreVariant &&val);
	void AddItem(FirestoreVariant &&val);
	void ReserveItems(idx_t count);

	const map<string, FirestoreVariant> &ObjectChildren() const {
		return object_children;
	}
	const vector<FirestoreVariant> &ArrayItems() const {
		return array_items;
	}

public:
	FirestoreVariantType value_type;
	Value primitive_value;

private:
	// Ordered by key: VariantBuilder expects an object's children sorted.
	map<string, FirestoreVariant> object_children;
	vector<FirestoreVariant> array_items;
};

// Build the VARIANT vector `result` from one value per row. A MISSING or NULL
// row becomes SQL NULL.
void FirestoreVariantsToVector(vector<FirestoreVariant> &input, Vector &result);

#endif

} // namespace duckdb
