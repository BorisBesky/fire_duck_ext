#include "firestore_index.hpp"
#include "firestore_types.hpp"
#include "firestore_logger.hpp"
#include "firestore_duckdb_compat.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

// --- OrderByField utilities ---

static std::string TrimWhitespace(const std::string &s) {
	size_t start = s.find_first_not_of(" \t\r\n");
	if (start == std::string::npos) {
		return "";
	}
	size_t end = s.find_last_not_of(" \t\r\n");
	return s.substr(start, end - start + 1);
}

std::vector<OrderByField> ParseOrderByString(const std::string &order_by_str) {
	std::vector<OrderByField> result;
	if (order_by_str.empty()) {
		return result;
	}

	// Split on commas
	std::string remaining = order_by_str;
	while (!remaining.empty()) {
		std::string token;
		size_t comma = remaining.find(',');
		if (comma != std::string::npos) {
			token = remaining.substr(0, comma);
			remaining = remaining.substr(comma + 1);
		} else {
			token = remaining;
			remaining.clear();
		}

		token = TrimWhitespace(token);
		if (token.empty()) {
			continue;
		}

		OrderByField field;
		field.direction = "ASCENDING"; // default

		// Split on space to get field name and optional direction
		size_t space = token.find(' ');
		if (space != std::string::npos) {
			field.field_path = token.substr(0, space);
			std::string dir_str = TrimWhitespace(token.substr(space + 1));
			if (dir_str == "DESC" || dir_str == "desc" || dir_str == "DESCENDING" || dir_str == "descending") {
				field.direction = "DESCENDING";
			}
			// ASC/asc/ASCENDING/ascending or anything else -> ASCENDING (default)
		} else {
			field.field_path = token;
		}

		result.push_back(std::move(field));
	}

	return result;
}

std::string FormatOrderByForREST(const std::vector<OrderByField> &fields) {
	std::string result;
	for (size_t i = 0; i < fields.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += fields[i].field_path;
		if (fields[i].direction == "DESCENDING") {
			result += " desc";
		}
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
			filter_jsons.push_back({{"unaryFilter", {{"field", {{"fieldPath", f.field_path}}}, {"op", f.unary_op}}}});
		} else if (f.is_in_filter) {
			json array_value = json::array();
			for (auto &v : f.in_values) {
				array_value.push_back(v);
			}
			filter_jsons.push_back({{"fieldFilter",
			                         {{"field", {{"fieldPath", f.field_path}}},
			                          {"op", f.firestore_op},
			                          {"value", {{"arrayValue", {{"values", array_value}}}}}}}});
		} else {
			filter_jsons.push_back(
			    {{"fieldFilter",
			      {{"field", {{"fieldPath", f.field_path}}}, {"op", f.firestore_op}, {"value", f.firestore_value}}}});
		}
	}

	if (filter_jsons.size() == 1) {
		return filter_jsons[0];
	}

	// Multiple filters: wrap in compositeFilter with AND
	return {{"compositeFilter", {{"op", "AND"}, {"filters", filter_jsons}}}};
}

bool HasSingleFieldIndex(const std::string &field_path, const FirestoreIndexCache &cache,
                         FirestoreIndex::QueryScope scope) {
	// Default single-field indexes only cover COLLECTION scope, not COLLECTION_GROUP.
	// Collection group queries require explicit collection group-scoped indexes.
	if (cache.default_single_field_enabled && scope == FirestoreIndex::QueryScope::COLLECTION) {
		return true;
	}

	// Check explicit single-field indexes
	for (auto &idx : cache.single_field_indexes) {
		if (idx.query_scope == scope && idx.state == FirestoreIndex::State::READY && idx.fields.size() == 1 &&
		    idx.fields[0].field_path == field_path) {
			return true;
		}
	}
	return false;
}

bool FindMatchingCompositeIndex(const std::set<std::string> &eq_fields, const std::string &range_field,
                                const FirestoreIndexCache &cache, FirestoreIndex::QueryScope scope) {
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

bool HasCompositeOrderByIndex(const std::vector<OrderByField> &order_by_fields, const FirestoreIndexCache &cache,
                              FirestoreIndex::QueryScope scope) {
	for (auto &idx : cache.composite_indexes) {
		if (idx.query_scope != scope || idx.state != FirestoreIndex::State::READY) {
			continue;
		}

		// Collect non-__name__ fields from the index
		std::vector<const FirestoreIndexField *> idx_fields;
		for (auto &f : idx.fields) {
			if (f.field_path != "__name__") {
				idx_fields.push_back(&f);
			}
		}

		// Must have at least as many fields as order_by_fields
		if (idx_fields.size() < order_by_fields.size()) {
			continue;
		}

		// Check prefix match with exact order and direction
		bool match = true;
		for (size_t i = 0; i < order_by_fields.size(); i++) {
			if (idx_fields[i]->field_path != order_by_fields[i].field_path) {
				match = false;
				break;
			}
			auto expected_mode = (order_by_fields[i].direction == "DESCENDING") ? FirestoreIndexField::Mode::DESCENDING
			                                                                    : FirestoreIndexField::Mode::ASCENDING;
			if (idx_fields[i]->mode != expected_mode) {
				match = false;
				break;
			}
		}
		if (match) {
			return true;
		}
	}
	return false;
}

FirestoreFilterResult MatchFiltersToIndexes(const std::vector<FirestorePushdownFilter> &candidate_filters,
                                            const FirestoreIndexCache &index_cache, bool is_collection_group) {
	FirestoreFilterResult result;

	if (!index_cache.fetch_succeeded) {
		FS_LOG_DEBUG("Index cache not available, skipping filter pushdown");
		return result;
	}

	if (candidate_filters.empty()) {
		return result;
	}

	auto required_scope =
	    is_collection_group ? FirestoreIndex::QueryScope::COLLECTION_GROUP : FirestoreIndex::QueryScope::COLLECTION;

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

	bool found_composite = FindMatchingCompositeIndex(eq_field_set, primary_range_field, index_cache, required_scope);

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
		FS_LOG_DEBUG("Composite index found, pushing down " + std::to_string(result.pushed_filters.size()) +
		             " filters");
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

std::vector<FirestorePushdownFilter> ConvertExpressionToFilters(const Expression &expr, idx_t table_index,
                                                                const std::vector<std::string> &all_column_names,
                                                                const std::vector<LogicalType> &all_column_types,
                                                                const std::vector<idx_t> &column_id_map) {
	std::vector<FirestorePushdownFilter> result;

	// Helper: resolve a BoundColumnRefExpression to a field name and type.
	// binding.column_index is the position in LogicalGet's column_ids array,
	// column_id_map[binding.column_index] gives the original bind-time column index
	// into all_column_names / all_column_types.
	// Returns false if the column can't be resolved to a pushable field.
	auto resolve_column = [&](const BoundColumnRefExpression &col_ref, std::string &out_name,
	                          LogicalType &out_type) -> bool {
		if (FirestoreBindingTable(col_ref) != table_index) {
			return false;
		}
		idx_t binding_idx = FirestoreBindingColumn(col_ref);
		if (binding_idx >= column_id_map.size()) {
			return false;
		}
		idx_t original_col_idx = column_id_map[binding_idx];
		// Skip __document_id (column 0) and virtual row id
		if (original_col_idx == 0 || original_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			return false;
		}
		if (original_col_idx >= all_column_names.size()) {
			return false;
		}
		out_name = all_column_names[original_col_idx];
		out_type = all_column_types[original_col_idx];
		return true;
	};

	// Handle AND conjunction - recurse into children
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : FirestoreChildren(conj)) {
			auto child_filters =
			    ConvertExpressionToFilters(*child, table_index, all_column_names, all_column_types, column_id_map);
			result.insert(result.end(), std::make_move_iterator(child_filters.begin()),
			              std::make_move_iterator(child_filters.end()));
		}
		return result;
	}

	// Handle OR conjunction - detect OR-of-equalities-on-same-field pattern for Firestore IN
	// DuckDB rewrites `x IN ('a', 'b')` to `(x = 'a') OR (x = 'b')`
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		if (FirestoreChildren(conj).empty()) {
			return {};
		}

		// Check if all children are equality comparisons on the same column
		std::string common_field;
		LogicalType common_type;
		std::vector<json> values;

		for (auto &child : FirestoreChildren(conj)) {
			// Each child must be a comparison expression with EQUAL
			if (!FirestoreIsComparison(*child) || child->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
				return {}; // Not all children are equalities - can't convert to IN
			}
			auto &left = FirestoreComparisonLeft(*child);
			auto &right = FirestoreComparisonRight(*child);

			const BoundColumnRefExpression *col_ref = nullptr;
			const BoundConstantExpression *const_val = nullptr;

			if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				col_ref = &left.Cast<BoundColumnRefExpression>();
				const_val = &right.Cast<BoundConstantExpression>();
			} else if (right.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			           left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				col_ref = &right.Cast<BoundColumnRefExpression>();
				const_val = &left.Cast<BoundConstantExpression>();
			} else {
				return {};
			}

			std::string field_name;
			LogicalType field_type;
			if (!resolve_column(*col_ref, field_name, field_type)) {
				return {};
			}

			if (common_field.empty()) {
				common_field = field_name;
				common_type = field_type;
			} else if (common_field != field_name) {
				return {}; // Different columns - can't convert to single IN filter
			}

			values.push_back(DuckDBValueToFirestore(FirestoreConstantValue(*const_val), common_type));
		}

		// Firestore IN filter has a limit of 30 values
		if (values.size() > 30) {
			FS_LOG_DEBUG("IN filter on " + common_field + " has " + std::to_string(values.size()) +
			             " values (max 30), skipping pushdown");
			return {};
		}

		FirestorePushdownFilter pf;
		pf.field_path = common_field;
		pf.firestore_op = "IN";
		pf.is_in_filter = true;
		pf.is_equality = true;
		pf.in_values = std::move(values);
		result.push_back(std::move(pf));
		return result;
	}

	// Handle IS NOT NULL (BoundOperatorExpression)
	// Note: IS_NULL is NOT pushed down because Firestore's IS_NULL only matches documents
	// where the field exists and is explicitly null, not documents where the field is missing.
	// DuckDB treats missing fields as NULL, so pushing IS_NULL would incorrectly exclude
	// documents that lack the field entirely. IS_NOT_NULL is safe to push because Firestore
	// correctly excludes both missing-field and explicitly-null documents.
	if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &children = FirestoreChildren(expr.Cast<BoundOperatorExpression>());
		if (children.size() == 1 && children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = children[0]->Cast<BoundColumnRefExpression>();
			std::string field_name;
			LogicalType field_type;
			if (!resolve_column(col_ref, field_name, field_type)) {
				return {};
			}

			FirestorePushdownFilter pf;
			pf.field_path = field_name;
			pf.is_unary = true;
			pf.unary_op = "IS_NOT_NULL";
			pf.is_equality = true;
			result.push_back(std::move(pf));
		}
		return result;
	}

	// Handle COMPARE_IN / COMPARE_NOT_IN (BoundOperatorExpression)
	// DuckDB represents `x IN ('a', 'b')` as BoundOperatorExpression with type COMPARE_IN
	// children[0] = column ref, children[1..n] = constant values
	if (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
	    expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
		auto &children = FirestoreChildren(expr.Cast<BoundOperatorExpression>());
		if (children.size() < 2) {
			return {};
		}

		// First child must be a column reference
		if (children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return {};
		}
		auto &col_ref = children[0]->Cast<BoundColumnRefExpression>();
		std::string field_name;
		LogicalType field_type;
		if (!resolve_column(col_ref, field_name, field_type)) {
			return {};
		}

		// Remaining children must be constants
		std::vector<json> values;
		for (size_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return {};
			}
			auto &const_val = children[i]->Cast<BoundConstantExpression>();
			values.push_back(DuckDBValueToFirestore(FirestoreConstantValue(const_val), field_type));
		}

		// Firestore IN filter has a limit of 30 values
		if (values.size() > 30) {
			return {};
		}

		FirestorePushdownFilter pf;
		pf.field_path = field_name;
		pf.firestore_op = (expr.GetExpressionType() == ExpressionType::COMPARE_IN) ? "IN" : "NOT_IN";
		pf.is_in_filter = true;
		pf.is_equality = true;
		pf.in_values = std::move(values);
		result.push_back(std::move(pf));
		return result;
	}

	// Handle comparison expressions (=, <>, <, <=, >, >=)
	if (FirestoreIsComparison(expr)) {
		auto &left = FirestoreComparisonLeft(expr);
		auto &right = FirestoreComparisonRight(expr);

		// Determine which side is the column and which is the constant
		const BoundColumnRefExpression *col_ref = nullptr;
		const BoundConstantExpression *const_val = nullptr;
		bool reversed = false;

		if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &left.Cast<BoundColumnRefExpression>();
			const_val = &right.Cast<BoundConstantExpression>();
		} else if (right.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		           left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &right.Cast<BoundColumnRefExpression>();
			const_val = &left.Cast<BoundConstantExpression>();
			reversed = true;
		} else {
			// Neither side is a simple column ref + constant
			return {};
		}

		std::string field_name;
		LogicalType field_type;
		if (!resolve_column(*col_ref, field_name, field_type)) {
			return {};
		}

		FirestorePushdownFilter pf;
		pf.field_path = field_name;

		ExpressionType cmp_type = expr.GetExpressionType();
		// If the constant is on the left (e.g., 5 < x), flip the comparison
		if (reversed) {
			switch (cmp_type) {
			case ExpressionType::COMPARE_LESSTHAN:
				cmp_type = ExpressionType::COMPARE_GREATERTHAN;
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				cmp_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				cmp_type = ExpressionType::COMPARE_LESSTHAN;
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				cmp_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
				break;
			default:
				break; // EQUAL and NOT_EQUAL are symmetric
			}
		}

		switch (cmp_type) {
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
			FS_LOG_DEBUG("Unsupported comparison type for expression pushdown on field: " + pf.field_path);
			return {};
		}

		pf.firestore_value = DuckDBValueToFirestore(FirestoreConstantValue(*const_val), field_type);
		result.push_back(std::move(pf));
		return result;
	}

	// Unsupported expression type - skip silently
	FS_LOG_DEBUG("Expression type not supported for pushdown: " +
	             std::to_string(static_cast<int>(expr.GetExpressionType())));
	return {};
}

} // namespace duckdb
