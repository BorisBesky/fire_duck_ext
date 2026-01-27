#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include <nlohmann/json.hpp>
#include <vector>
#include <string>
#include <set>
#include <memory>

namespace duckdb {

using json = nlohmann::json;

// Represents a single field in a Firestore index
struct FirestoreIndexField {
    std::string field_path;
    enum class Mode { ASCENDING, DESCENDING, ARRAY_CONTAINS } mode;
};

// Represents a Firestore index (composite or single-field)
struct FirestoreIndex {
    std::string name;
    std::vector<FirestoreIndexField> fields;
    enum class QueryScope { COLLECTION, COLLECTION_GROUP } query_scope;
    enum class State { CREATING, READY, NEEDS_REPAIR } state;
    bool is_single_field;
};

// Represents a single filter that can be pushed to Firestore
struct FirestorePushdownFilter {
    std::string field_path;
    std::string firestore_op;       // EQUAL, LESS_THAN, etc.
    json firestore_value;           // Value in Firestore JSON format
    bool is_unary = false;          // true for IS_NULL / IS_NOT_NULL
    std::string unary_op;           // "IS_NULL" or "IS_NOT_NULL"
    bool is_in_filter = false;      // true for IN filters
    std::vector<json> in_values;    // Values for IN filter
    bool is_equality = false;       // EQUAL, NOT_EQUAL, IN, NOT_IN, IS_NULL, IS_NOT_NULL
};

// Result of analyzing DuckDB filters against Firestore indexes
struct FirestoreFilterResult {
    std::vector<FirestorePushdownFilter> pushed_filters;
    bool has_pushdown() const { return !pushed_filters.empty(); }
};

// Cache of available indexes for a collection
struct FirestoreIndexCache {
    std::vector<FirestoreIndex> composite_indexes;
    std::vector<FirestoreIndex> single_field_indexes;
    bool default_single_field_enabled = true;
    bool fetch_succeeded = false;
};

// Convert a DuckDB TableFilter into Firestore pushdown filters
std::vector<FirestorePushdownFilter> ConvertDuckDBFilter(
    const std::string &field_name,
    const LogicalType &field_type,
    const TableFilter &filter
);

// Build Firestore StructuredQuery `where` clause JSON from pushed filters
json BuildWhereClause(const std::vector<FirestorePushdownFilter> &filters);

// Check if a single-field index exists for a field
bool HasSingleFieldIndex(
    const std::string &field_path,
    const FirestoreIndexCache &cache,
    FirestoreIndex::QueryScope scope
);

// Find a composite index matching equality fields + a range field
bool FindMatchingCompositeIndex(
    const std::set<std::string> &eq_fields,
    const std::string &range_field,
    const FirestoreIndexCache &cache,
    FirestoreIndex::QueryScope scope
);

// Match DuckDB filters against available indexes, return what can be pushed down
FirestoreFilterResult MatchFiltersToIndexes(
    const std::vector<FirestorePushdownFilter> &candidate_filters,
    const FirestoreIndexCache &index_cache,
    bool is_collection_group
);

// Convert a DuckDB Expression tree into Firestore pushdown filters
// Used by pushdown_complex_filter callback (operates on Expression trees, not TableFilter)
// column_names/column_types are the bind-time schema (excluding __document_id)
// table_index is the LogicalGet's table_index for matching column references
std::vector<FirestorePushdownFilter> ConvertExpressionToFilters(
    const Expression &expr,
    idx_t table_index,
    const std::vector<std::string> &column_names,
    const std::vector<LogicalType> &column_types
);

} // namespace duckdb
