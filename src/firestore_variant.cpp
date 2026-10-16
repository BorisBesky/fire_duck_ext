#include "firestore_variant.hpp"

#ifndef FIRESTORE_DUCKDB_HAS_VARIANT_VALUE

#include "duckdb/common/types/variant/variant_builder.hpp"

namespace duckdb {

void FirestoreVariant::AddChild(const string &key, FirestoreVariant &&val) {
	D_ASSERT(value_type == FirestoreVariantType::OBJECT);
	if (val.IsMissing()) {
		return;
	}
	object_children.emplace(key, std::move(val));
}

void FirestoreVariant::AddItem(FirestoreVariant &&val) {
	D_ASSERT(value_type == FirestoreVariantType::ARRAY);
	array_items.push_back(std::move(val));
}

void FirestoreVariant::ReserveItems(idx_t count) {
	D_ASSERT(value_type == FirestoreVariantType::ARRAY);
	array_items.reserve(count);
}

// Emit exactly one value for `value` (VariantBuilder's contract for children).
static void EmitFirestoreVariant(const FirestoreVariant &value, VariantBuilder &builder) {
	switch (value.value_type) {
	case FirestoreVariantType::MISSING:
		builder.EmitNull();
		return;
	case FirestoreVariantType::PRIMITIVE:
		if (value.primitive_value.IsNull()) {
			builder.EmitNull();
		} else {
			builder.EmitPrimitive(value.primitive_value, NumericCast<uint32_t>(builder.blob.size()));
		}
		return;
	case FirestoreVariantType::OBJECT: {
		// EmitObject addresses children by index; the map is already sorted.
		vector<const pair<const string, FirestoreVariant> *> children;
		children.reserve(value.ObjectChildren().size());
		for (auto &entry : value.ObjectChildren()) {
			children.push_back(&entry);
		}
		builder.EmitObject(
		    children.size(),
		    [&](idx_t i) {
			    auto &key = children[i]->first;
			    return string_t(key.c_str(), NumericCast<uint32_t>(key.size()));
		    },
		    [&](idx_t i) { EmitFirestoreVariant(children[i]->second, builder); });
		return;
	}
	case FirestoreVariantType::ARRAY: {
		auto &items = value.ArrayItems();
		builder.EmitArray(items.size(), [&](idx_t i) { EmitFirestoreVariant(items[i], builder); });
		return;
	}
	}
	throw InternalException("Unknown FirestoreVariantType");
}

namespace {

// A row source for BuildVariant.
struct FirestoreVariantSource {
	explicit FirestoreVariantSource(const vector<FirestoreVariant> &input) : input(input) {
	}

	// Returns whether the row is SQL NULL, in which case nothing is emitted.
	bool Emit(idx_t row, VariantBuilder &builder) const {
		auto &value = input[row];
		if (value.IsNull() || value.IsMissing()) {
			return true;
		}
		EmitFirestoreVariant(value, builder);
		return false;
	}

	const vector<FirestoreVariant> &input;
};

} // namespace

void FirestoreVariantsToVector(vector<FirestoreVariant> &input, Vector &result) {
	FirestoreVariantSource source(input);
	BuildVariant(source, input.size(), result);
}

} // namespace duckdb

#endif
