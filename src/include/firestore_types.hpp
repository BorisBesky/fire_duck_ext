#pragma once

#include "duckdb.hpp"
#include <nlohmann/json.hpp>
#include <vector>
#include <string>

namespace duckdb {

using json = nlohmann::json;

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
// referenceValue      -> VARCHAR (document path)
// bytesValue          -> BLOB

// Infer DuckDB type from a Firestore value
LogicalType InferDuckDBType(const json &firestore_value);

// Infer DuckDB type from Firestore type name
LogicalType FirestoreTypeToDuckDB(const std::string &firestore_type);

// Convert Firestore JSON value to DuckDB Value
Value FirestoreValueToDuckDB(const json &firestore_value, const LogicalType &target_type);

// Convert DuckDB Value to Firestore JSON format
json DuckDBValueToFirestore(const Value &value, const LogicalType &source_type);

// Set a value in a DuckDB vector from Firestore JSON
void SetDuckDBValue(Vector &vector, idx_t index,
                    const json &firestore_value,
                    const LogicalType &type);

// Column information inferred from documents
struct InferredColumn {
    std::string name;
    LogicalType type;
    bool nullable;
    int64_t occurrence_count;
};

// Infer schema from a collection of documents
std::vector<InferredColumn> InferSchemaFromDocuments(
    const std::vector<json> &document_fields,
    idx_t sample_size = 100
);

// Helper: Check if a Firestore value is null
bool IsFirestoreNull(const json &value);

// Helper: Get the Firestore type name from a value
std::string GetFirestoreTypeName(const json &value);

// Helper: Extract raw value from Firestore format
json ExtractFirestoreValue(const json &firestore_value);

} // namespace duckdb
