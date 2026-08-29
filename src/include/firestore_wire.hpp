#pragma once

// Firestore wire-format helpers.
//
// This header is deliberately free of any DuckDB dependency: it sees only
// nlohmann/json and the standard library. That keeps the pure, heavily
// branching parts of the extension (type detection, paging policy, schema
// accumulation) compilable and unit-testable on their own, without linking
// DuckDB. Anything needing LogicalType, Vector or ClientContext belongs in
// firestore_types.hpp instead.

#include <nlohmann/json.hpp>
#include <cstdint>
#include <string>

namespace duckdb {

using json = nlohmann::json;

// Parsed representation of a single order_by field
// (e.g. "score DESC" -> {score, DESCENDING}).
struct OrderByField {
	std::string field_path;
	std::string direction; // "ASCENDING" or "DESCENDING"
};

// True when the value carries Firestore's explicit null.
bool IsFirestoreNull(const json &value);

// True when a mapValue encodes a Firestore vector (embedding):
//   { "mapValue": { "fields": {
//       "__type__": { "stringValue": "__vector__" },
//       "value":    { "arrayValue": { "values": [ {"doubleValue": ...}, ... ] } }
//   }}}
bool IsFirestoreVector(const json &value);

// Element count of a Firestore vector value. 0 when the value is not a vector
// or carries no elements, which callers treat as "dimension unknown".
uint64_t FirestoreVectorDimension(const json &value);

// True when a vector value carries a `values` array at all -- distinct from
// carrying an empty one. Schema inference uses this to tell "this occurrence
// told us nothing, keep looking" from "this occurrence says the dimension
// is 0", which reach the same fallback but through different paths.
bool FirestoreVectorHasValues(const json &value);

// Name of the Firestore type wrapper a value uses ("stringValue",
// "arrayValue", "vectorValue", ...), or "unknown" for an unrecognised shape.
std::string GetFirestoreTypeName(const json &value);

// Resolve a Firestore field path against a document's `fields` object.
//
// A literal key wins first, so a field genuinely named "a.b" still resolves.
// Otherwise the path is split on '.' and walked through nested mapValues,
// which is how Firestore addresses nested fields in orderBy and cursors.
// Returns nullptr when the path does not resolve.
const json *ResolveFirestoreFieldPath(const json &fields, const std::string &field_path);

} // namespace duckdb
