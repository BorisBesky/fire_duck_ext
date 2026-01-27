#include "firestore_index.hpp"
#include "firestore_types.hpp"
#include "firestore_logger.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

// Check for InFilter - may or may not be available depending on DuckDB version
// We handle it via filter_type check at runtime

std::vector<FirestorePushdownFilter> ConvertDuckDBFilter(
    const std::string &field_name,
    const LogicalType &field_type,
    const TableFilter &filter
) {
    std::vector<FirestorePushdownFilter> result;

    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &const_filter = filter.Cast<ConstantFilter>();
            FirestorePushdownFilter pf;
            pf.field_path = field_name;

            switch (const_filter.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:
                    pf.firestore_op = "EQUAL";
                    pf.is_equality = true;
                    break;
                case ExpressionType::COMPARE_NOTEQUAL:
                    pf.firestore_op = "NOT_EQUAL";
                    pf.is_equality = true;
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    pf.firestore_op = "LESS_THAN";
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    pf.firestore_op = "LESS_THAN_OR_EQUAL";
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    pf.firestore_op = "GREATER_THAN";
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    pf.firestore_op = "GREATER_THAN_OR_EQUAL";
                    break;
                default:
                    FS_LOG_DEBUG("Unsupported comparison type for pushdown on field: " + field_name);
                    return {};
            }

            pf.firestore_value = DuckDBValueToFirestore(const_filter.constant, field_type);
            result.push_back(std::move(pf));
            break;
        }

        case TableFilterType::IS_NULL: {
            FirestorePushdownFilter pf;
            pf.field_path = field_name;
            pf.is_unary = true;
            pf.unary_op = "IS_NULL";
            pf.is_equality = true;
            result.push_back(std::move(pf));
            break;
        }

        case TableFilterType::IS_NOT_NULL: {
            FirestorePushdownFilter pf;
            pf.field_path = field_name;
            pf.is_unary = true;
            pf.unary_op = "IS_NOT_NULL";
            pf.is_equality = true;
            result.push_back(std::move(pf));
            break;
        }

        case TableFilterType::CONJUNCTION_AND: {
            auto &conj = filter.Cast<ConjunctionAndFilter>();
            for (auto &child : conj.child_filters) {
                auto child_filters = ConvertDuckDBFilter(field_name, field_type, *child);
                result.insert(result.end(),
                    std::make_move_iterator(child_filters.begin()),
                    std::make_move_iterator(child_filters.end()));
            }
            break;
        }

        default:
            // ConjunctionOrFilter and other types are not pushed down
            FS_LOG_DEBUG("Filter type not supported for pushdown on field: " + field_name);
            return {};
    }

    return result;
}

json BuildWhereClause(const std::vector<FirestorePushdownFilter> &filters) {
    if (filters.empty()) {
        return {};
    }

    std::vector<json> filter_jsons;

    for (auto &f : filters) {
        if (f.is_unary) {
            filter_jsons.push_back({
                {"unaryFilter", {
                    {"field", {{"fieldPath", f.field_path}}},
                    {"op", f.unary_op}
                }}
            });
        } else if (f.is_in_filter) {
            json array_value = json::array();
            for (auto &v : f.in_values) {
                array_value.push_back(v);
            }
            filter_jsons.push_back({
                {"fieldFilter", {
                    {"field", {{"fieldPath", f.field_path}}},
                    {"op", f.firestore_op},
                    {"value", {{"arrayValue", {{"values", array_value}}}}}
                }}
            });
        } else {
            filter_jsons.push_back({
                {"fieldFilter", {
                    {"field", {{"fieldPath", f.field_path}}},
                    {"op", f.firestore_op},
                    {"value", f.firestore_value}
                }}
            });
        }
    }

    if (filter_jsons.size() == 1) {
        return filter_jsons[0];
    }

    // Multiple filters: wrap in compositeFilter with AND
    return {
        {"compositeFilter", {
            {"op", "AND"},
            {"filters", filter_jsons}
        }}
    };
}

bool HasSingleFieldIndex(
    const std::string &field_path,
    const FirestoreIndexCache &cache,
    FirestoreIndex::QueryScope scope
) {
    // If default single-field indexing is enabled, every field has indexes
    if (cache.default_single_field_enabled) {
        return true;
    }

    // Check explicit single-field indexes
    for (auto &idx : cache.single_field_indexes) {
        if (idx.query_scope == scope &&
            idx.state == FirestoreIndex::State::READY &&
            idx.fields.size() == 1 &&
            idx.fields[0].field_path == field_path) {
            return true;
        }
    }
    return false;
}

bool FindMatchingCompositeIndex(
    const std::set<std::string> &eq_fields,
    const std::string &range_field,
    const FirestoreIndexCache &cache,
    FirestoreIndex::QueryScope scope
) {
    for (auto &idx : cache.composite_indexes) {
        if (idx.query_scope != scope || idx.state != FirestoreIndex::State::READY) {
            continue;
        }

        // Collect field paths from this index (excluding __name__)
        std::set<std::string> idx_fields;
        for (auto &f : idx.fields) {
            if (f.field_path != "__name__") {
                idx_fields.insert(f.field_path);
            }
        }

        // Check all equality fields are covered
        bool all_eq_covered = true;
        for (auto &ef : eq_fields) {
            if (idx_fields.find(ef) == idx_fields.end()) {
                all_eq_covered = false;
                break;
            }
        }

        // Check range field is covered
        bool range_covered = idx_fields.find(range_field) != idx_fields.end();

        if (all_eq_covered && range_covered) {
            return true;
        }
    }
    return false;
}

FirestoreFilterResult MatchFiltersToIndexes(
    const std::vector<FirestorePushdownFilter> &candidate_filters,
    const FirestoreIndexCache &index_cache,
    bool is_collection_group
) {
    FirestoreFilterResult result;

    if (!index_cache.fetch_succeeded) {
        FS_LOG_DEBUG("Index cache not available, skipping filter pushdown");
        return result;
    }

    if (candidate_filters.empty()) {
        return result;
    }

    auto required_scope = is_collection_group
        ? FirestoreIndex::QueryScope::COLLECTION_GROUP
        : FirestoreIndex::QueryScope::COLLECTION;

    // Separate into equality and range filters
    std::vector<const FirestorePushdownFilter *> equality_filters;
    std::vector<const FirestorePushdownFilter *> range_filters;

    for (auto &f : candidate_filters) {
        if (f.is_equality || f.is_unary) {
            equality_filters.push_back(&f);
        } else {
            range_filters.push_back(&f);
        }
    }

    // Case 1: Only equality filters - each needs a single-field index
    if (range_filters.empty()) {
        for (auto *ef : equality_filters) {
            if (HasSingleFieldIndex(ef->field_path, index_cache, required_scope)) {
                result.pushed_filters.push_back(*ef);
            } else {
                FS_LOG_DEBUG("No single-field index for equality filter on: " + ef->field_path);
            }
        }
        return result;
    }

    // Case 2: Only range filters on a single field
    if (equality_filters.empty()) {
        // Check if all range filters are on the same field
        std::set<std::string> range_fields;
        for (auto *rf : range_filters) {
            range_fields.insert(rf->field_path);
        }

        if (range_fields.size() == 1) {
            // Single field range - needs single-field index
            if (HasSingleFieldIndex(range_filters[0]->field_path, index_cache, required_scope)) {
                for (auto *rf : range_filters) {
                    result.pushed_filters.push_back(*rf);
                }
            } else {
                FS_LOG_DEBUG("No single-field index for range filter on: " + range_filters[0]->field_path);
            }
        } else {
            // Firestore doesn't support range on multiple fields
            // Push down range filters for only the first field
            std::string first_field = *range_fields.begin();
            if (HasSingleFieldIndex(first_field, index_cache, required_scope)) {
                for (auto *rf : range_filters) {
                    if (rf->field_path == first_field) {
                        result.pushed_filters.push_back(*rf);
                    }
                }
                FS_LOG_DEBUG("Range on multiple fields; only pushing down filters on: " + first_field);
            }
        }
        return result;
    }

    // Case 3: Equality + range on different fields - needs composite index
    std::set<std::string> eq_field_set;
    for (auto *ef : equality_filters) {
        eq_field_set.insert(ef->field_path);
    }

    // Collect unique range fields
    std::set<std::string> range_field_set;
    for (auto *rf : range_filters) {
        range_field_set.insert(rf->field_path);
    }

    // Firestore requires range filters on a single field
    // Pick the first range field for composite index lookup
    std::string primary_range_field = *range_field_set.begin();

    bool found_composite = FindMatchingCompositeIndex(
        eq_field_set, primary_range_field, index_cache, required_scope);

    if (found_composite) {
        // Push all equality + range filters for the primary range field
        for (auto *ef : equality_filters) {
            result.pushed_filters.push_back(*ef);
        }
        for (auto *rf : range_filters) {
            if (rf->field_path == primary_range_field) {
                result.pushed_filters.push_back(*rf);
            }
        }
        FS_LOG_DEBUG("Composite index found, pushing down " +
                     std::to_string(result.pushed_filters.size()) + " filters");
    } else {
        // No composite index - fall back to pushing only equality filters
        FS_LOG_DEBUG("No composite index found for equality+range, pushing only equality filters");
        for (auto *ef : equality_filters) {
            if (HasSingleFieldIndex(ef->field_path, index_cache, required_scope)) {
                result.pushed_filters.push_back(*ef);
            }
        }
    }

    return result;
}

} // namespace duckdb
