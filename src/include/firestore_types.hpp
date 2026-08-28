#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "firestore_wire.hpp"
#include <nlohmann/json.hpp>
#include <vector>
#include <string>

namespace duckdb {

// Wire-format helpers (IsFirestoreNull, GetFirestoreTypeName, ...) live in
// firestore_wire.hpp, which carries no DuckDB dependency so it can be unit
// tested on its own. They are re-exported here by that include.

// Type mapping:
// Firestore Type      -> DuckDB Type
// stringValue         -> VARCHAR
// integerValue        -> BIGINT
// doubleValue         -> DOUBLE
// booleanValue        -> BOOLEAN
// nullValue           -> NULL (any type)
// timestampValue      -> TIMESTAMP
// geoPointValue       -> STRUCT(latitude DOUBLE, longitude DOUBLE)
// arrayValue          -> LIST(inferred type)
// mapValue            -> JSON (VARCHAR)
// vectorValue         -> ARRAY(DOUBLE, N) (Firestore vector embedding)
// referenceValue      -> VARCHAR (document path)
// bytesValue          -> BLOB

// Infer DuckDB type from a Firestore value
LogicalType InferDuckDBType(const json &firestore_value);

// Infer DuckDB type from Firestore type name
// How a Firestore mapValue is surfaced to DuckDB.
//
// Firestore maps are schemaless: keys and value types vary per document, so no
// fixed column type describes them all. These are the three encodings the
// scanner can produce, selected with the `map_encoding` named parameter.
enum class FirestoreMapEncoding : uint8_t {
	// VARCHAR holding the raw Firestore wire JSON, type wrappers included:
	//   {"a":{"stringValue":"x"}}
	// Leaves need paths like $.a.mapValue.fields.b.stringValue. Historical
	// default, kept so existing queries keep working.
	WIRE,
	// VARCHAR (JSON alias) holding natural JSON: {"a":"x"}. Leaf paths become
	// $.a.b and integers are numbers, but every access re-parses the string.
	JSON,
	// Native DuckDB VARIANT: dot access (payload.a.b), per-value types via
	// variant_typeof, and no fixed schema -- differing keys and differing types
	// at the same path across documents are all preserved.
	VARIANT,
};

// Parse a `map_encoding` parameter value. Throws on an unknown name.
FirestoreMapEncoding ParseMapEncoding(const std::string &name);
const char *MapEncodingName(FirestoreMapEncoding encoding);

LogicalType FirestoreTypeToDuckDB(const std::string &firestore_type,
                                  FirestoreMapEncoding map_encoding = FirestoreMapEncoding::WIRE);

// Strip Firestore's type wrappers, yielding natural JSON.
// {"a":{"integerValue":"1"}} -> {"a":1}
json UnwrapFirestoreValue(const json &firestore_value);

// Build a DuckDB VariantValue from a Firestore value, preserving per-value
// types and arbitrary nesting.
VariantValue FirestoreValueToVariant(const json &firestore_value);

// Convert Firestore JSON value to DuckDB Value
Value FirestoreValueToDuckDB(const json &firestore_value, const LogicalType &target_type,
                             FirestoreMapEncoding map_encoding = FirestoreMapEncoding::WIRE);

// Convert DuckDB Value to Firestore JSON format
json DuckDBValueToFirestore(const Value &value, const LogicalType &source_type);

// Set a value in a DuckDB vector from Firestore JSON
// Note: VARIANT columns are NOT written here. DuckDB builds a VARIANT vector
// from a whole chunk at once (VariantValue::ToVARIANT), so the scanner
// accumulates VariantValues per row and converts once per chunk.
void SetDuckDBValue(Vector &vector, idx_t index, const json &firestore_value, const LogicalType &type,
                    FirestoreMapEncoding map_encoding = FirestoreMapEncoding::WIRE);

// Column information inferred from documents
struct InferredColumn {
	std::string name;
	LogicalType type;
	bool nullable;
	int64_t occurrence_count;
};

// Infer schema from a collection of documents
std::vector<InferredColumn> InferSchemaFromDocuments(const std::vector<json> &document_fields, idx_t sample_size = 100);

// Helper: Extract raw value from Firestore format
json ExtractFirestoreValue(const json &firestore_value);

} // namespace duckdb
