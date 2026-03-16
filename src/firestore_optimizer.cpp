#include "firestore_optimizer.hpp"
#include "firestore_scanner.hpp"
#include "firestore_logger.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"

namespace duckdb {

static int CountPathSegments(const std::string &path) {
	int count = 0;
	bool in_segment = false;
	for (char c : path) {
		if (c == '/') {
			in_segment = false;
		} else if (!in_segment) {
			in_segment = true;
			count++;
		}
	}
	return count;
}

static bool IsDocumentPathCollection(const std::string &collection) {
	if (!collection.empty() && collection[0] == '~') {
		return false;
	}
	int segments = CountPathSegments(collection);
	return segments >= 2 && segments % 2 == 0;
}

// Returns true if this operator type blocks ORDER BY / LIMIT from applying to child scans.
// These operators transform result cardinality, ordering, or semantics in ways that make
// the parent's ORDER BY / LIMIT irrelevant to the underlying scan.
static bool IsBarrierOperator(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return true;
	default:
		return false;
	}
}

// Resolve a BoundColumnRefExpression through projection layers to find the column index
// in the target LogicalGet. ORDER BY expressions reference the projection's table_index,
// not the scan's table_index, so we need to trace through projections to find the
// underlying scan column.
//
// Note: ColumnBinding.column_index is a position in LogicalGet::column_ids, NOT in
// get.names directly. We map through column_ids to get the original column index.
//
// Returns true and sets out_col_idx if resolution succeeds.
static bool ResolveColumnThroughProjections(const BoundColumnRefExpression &colref, LogicalGet &get,
                                            const std::vector<LogicalProjection *> &projections, idx_t &out_col_idx) {
	// Direct reference to the scan table
	if (colref.binding.table_index == get.table_index) {
		auto &col_ids = get.GetColumnIds();
		idx_t binding_idx = colref.binding.column_index;
		if (binding_idx >= col_ids.size()) {
			return false;
		}
		out_col_idx = col_ids[binding_idx].GetPrimaryIndex();
		return true;
	}

	// Try to resolve through tracked projections
	for (size_t pi = 0; pi < projections.size(); pi++) {
		auto *proj = projections[pi];
		if (colref.binding.table_index != proj->table_index) {
			continue;
		}
		idx_t proj_col_idx = colref.binding.column_index;
		if (proj_col_idx >= proj->expressions.size()) {
			continue;
		}
		// The projection expression at this index should be a BoundColumnRefExpression
		// pointing to the underlying table
		auto &proj_expr = *proj->expressions[proj_col_idx];
		if (proj_expr.expression_class != ExpressionClass::BOUND_COLUMN_REF) {
			FS_LOG_DEBUG("Projection expression is not a column ref, skipping ORDER BY pushdown");
			return false;
		}
		auto &inner_colref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_colref.binding.table_index == get.table_index) {
			auto &col_ids = get.GetColumnIds();
			idx_t binding_idx = inner_colref.binding.column_index;
			if (binding_idx >= col_ids.size()) {
				return false;
			}
			out_col_idx = col_ids[binding_idx].GetPrimaryIndex();
			return true;
		}
		// Could be another projection layer - recurse
		return ResolveColumnThroughProjections(inner_colref, get, projections, out_col_idx);
	}

	return false;
}

// Try to extract ORDER BY fields from a LogicalOrder node and inject them into bind_data.
// Returns true if extraction succeeded.
// Only pushes simple column references that map to the firestore_scan's table.
// Resolves column references through projection layers since ORDER BY expressions
// reference the projection's table_index, not the scan's table_index directly.
static bool TryExtractOrderBy(LogicalGet &get, const vector<BoundOrderByNode> &orders, FirestoreScanBindData &bind_data,
                              const std::vector<LogicalProjection *> &projections) {
	std::vector<OrderByField> fields;

	for (size_t oi = 0; oi < orders.size(); oi++) {
		auto &ob = orders[oi];
		// Only handle simple column references - skip expressions like score + 1
		if (ob.expression->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
			FS_LOG_DEBUG("ORDER BY expression is not a simple column ref, skipping pushdown");
			return false;
		}

		auto &colref = ob.expression->Cast<BoundColumnRefExpression>();

		// Resolve through projections to find the underlying scan column
		idx_t col_idx;
		if (!ResolveColumnThroughProjections(colref, get, projections, col_idx)) {
			FS_LOG_DEBUG("ORDER BY references a different table, skipping pushdown");
			return false;
		}

		if (col_idx >= get.names.size()) {
			FS_LOG_DEBUG("ORDER BY column index out of range, skipping pushdown");
			return false;
		}

		std::string field_name = get.names[col_idx];
		// __document_id maps to __name__ in Firestore
		if (field_name == "__document_id") {
			field_name = "__name__";
		}

		OrderByField field;
		field.field_path = field_name;
		field.direction = (ob.type == OrderType::DESCENDING) ? "DESCENDING" : "ASCENDING";
		fields.push_back(std::move(field));
	}

	bind_data.sql_pushed_order_by = std::move(fields);

	std::string order_str;
	for (auto &f : bind_data.sql_pushed_order_by) {
		if (!order_str.empty()) {
			order_str += ", ";
		}
		order_str += f.field_path;
		if (f.direction == "DESCENDING") {
			order_str += " DESC";
		}
	}
	FS_LOG_DEBUG("SQL ORDER BY pushdown: " + order_str);

	return true;
}

// Try to extract LIMIT (and optionally OFFSET) from a LogicalLimit node and inject into bind_data.
// Returns true if extraction succeeded.
static bool TryExtractLimit(LogicalLimit &limit_op, FirestoreScanBindData &bind_data) {
	// Only handle constant integer limits
	if (limit_op.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}

	int64_t limit_value = static_cast<int64_t>(limit_op.limit_val.GetConstantValue());

	// Handle OFFSET: push limit + offset as the scan limit so Firestore fetches enough rows
	// for DuckDB to apply the offset client-side
	if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		int64_t offset_value = static_cast<int64_t>(limit_op.offset_val.GetConstantValue());
		limit_value += offset_value;
		FS_LOG_DEBUG("SQL LIMIT pushdown with OFFSET: limit=" + std::to_string(limit_value));
	} else if (limit_op.offset_val.Type() != LimitNodeType::UNSET) {
		// Non-constant offset (expression) - skip pushdown entirely
		return false;
	} else {
		FS_LOG_DEBUG("SQL LIMIT pushdown: " + std::to_string(limit_value));
	}

	bind_data.sql_pushed_limit = limit_value;
	return true;
}

static bool TryExtractLimit(LogicalTopN &topn_op, FirestoreScanBindData &bind_data) {
	int64_t limit_value = static_cast<int64_t>(topn_op.limit + topn_op.offset);
	FS_LOG_DEBUG("SQL TOP_N pushdown: limit=" + std::to_string(limit_value));
	bind_data.sql_pushed_limit = limit_value;
	return true;
}

// Recursively walk the logical plan tree top-down, tracking the nearest
// LogicalOrder, LogicalLimit, and LogicalProjection ancestors. When we find
// a LogicalGet for firestore_scan, inject the extracted ORDER BY / LIMIT
// into bind_data.
static void WalkPlanTree(LogicalOperator &op, LogicalOrder *current_order, LogicalLimit *current_limit,
                         LogicalTopN *current_topn, std::vector<LogicalProjection *> &projections) {
	// Save the current projection stack size so we can restore it when unwinding
	auto saved_projection_size = projections.size();

	// If this is a barrier operator, reset tracking - ORDER BY / LIMIT above
	// a barrier don't apply to scans below it
	if (IsBarrierOperator(op.type)) {
		current_order = nullptr;
		current_limit = nullptr;
		current_topn = nullptr;
		projections.clear();
	}

	// Track ORDER BY, LIMIT, and Projection nodes as we descend
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		current_limit = &op.Cast<LogicalLimit>();
		current_topn = nullptr;
	}
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		current_order = &op.Cast<LogicalOrder>();
		current_topn = nullptr;
	}
	if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		current_topn = &op.Cast<LogicalTopN>();
		current_order = nullptr;
		current_limit = nullptr;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		projections.push_back(&op.Cast<LogicalProjection>());
	}

	// Check if this is a firestore_scan LogicalGet
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.name == "firestore_scan" && get.bind_data) {
			auto &bind_data = get.bind_data->CastNoConst<FirestoreScanBindData>();
			bind_data.sql_pushed_order_by.clear();
			bind_data.sql_pushed_limit.reset();

			// Document-path scans return virtual rows derived from listCollectionIds,
			// not Firestore documents. Extract ORDER BY / LIMIT into docpath-specific fields.
			if (bind_data.is_document_path || IsDocumentPathCollection(bind_data.collection)) {
				// Extract ORDER BY __document_id into docpath_named_order if not set by named param
				if (bind_data.docpath_named_order == DocPathOrderType::NONE) {
					const auto *order_nodes =
					    current_topn ? &current_topn->orders : (current_order ? &current_order->orders : nullptr);
					if (order_nodes && order_nodes->size() == 1) {
						auto &ob = (*order_nodes)[0];
						if (ob.expression->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
							auto &colref = ob.expression->Cast<BoundColumnRefExpression>();
							idx_t col_idx;
							if (ResolveColumnThroughProjections(colref, get, projections, col_idx) &&
							    col_idx < get.names.size() && get.names[col_idx] == "__document_id") {
								bind_data.docpath_named_order = ob.type == OrderType::DESCENDING
								                                    ? DocPathOrderType::DESCENDING
								                                    : DocPathOrderType::ASCENDING;
							}
						}
					}
				}

				// Extract LIMIT using existing TryExtractLimit (writes to sql_pushed_limit)
				if (!bind_data.limit.has_value() && current_limit) {
					TryExtractLimit(*current_limit, bind_data);
				}
				if (!bind_data.limit.has_value() && current_topn) {
					TryExtractLimit(*current_topn, bind_data);
				}

				// EXPLAIN info
				auto &existing = get.extra_info.file_filters;
				existing.clear();
				if (bind_data.docpath_named_order != DocPathOrderType::NONE) {
					existing = "DocPath Order: __document_id";
					if (bind_data.docpath_named_order == DocPathOrderType::DESCENDING) {
						existing += " DESC";
					}
					auto eff_limit = bind_data.limit.has_value() ? bind_data.limit : bind_data.sql_pushed_limit;
					if (eff_limit.has_value()) {
						existing += " LIMIT " + std::to_string(eff_limit.value());
					}
				}
				return;
			}

			// Only inject ORDER BY if named param was not already set
			const auto *order_nodes =
			    current_topn ? &current_topn->orders : (current_order ? &current_order->orders : nullptr);
			if (bind_data.parsed_order_by.empty() && order_nodes) {
				if (TryExtractOrderBy(get, *order_nodes, bind_data, projections)) {
					// Build EXPLAIN info
					std::string info;
					for (auto &f : bind_data.sql_pushed_order_by) {
						if (!info.empty()) {
							info += ", ";
						}
						info += f.field_path;
						if (f.direction == "DESCENDING") {
							info += " DESC";
						}
					}
					auto &existing = get.extra_info.file_filters;
					if (!existing.empty()) {
						existing += " | ";
					}
					existing += "Firestore Pushed Order: " + info;
				}
			}

			// Only inject LIMIT if named scan_limit was not already set.
			// Skip when an ORDER BY exists but was not pushed, because applying the
			// LIMIT server-side would cut off rows before DuckDB can perform the
			// client-side sort.
			bool named_order_blocks_limit = !bind_data.parsed_order_by.empty() && bind_data.sql_pushed_order_by.empty();
			bool sql_order_blocks_limit = order_nodes && bind_data.sql_pushed_order_by.empty();
			if (!bind_data.limit.has_value() && !named_order_blocks_limit && !sql_order_blocks_limit && current_limit) {
				if (TryExtractLimit(*current_limit, bind_data)) {
					auto &existing = get.extra_info.file_filters;
					if (!existing.empty()) {
						existing += " | ";
					}
					existing += "Firestore Pushed Limit: " + std::to_string(bind_data.sql_pushed_limit.value());
				}
			}
			if (!bind_data.limit.has_value() && !named_order_blocks_limit && !sql_order_blocks_limit && current_topn) {
				if (TryExtractLimit(*current_topn, bind_data)) {
					auto &existing = get.extra_info.file_filters;
					if (!existing.empty()) {
						existing += " | ";
					}
					existing += "Firestore Pushed Limit: " + std::to_string(bind_data.sql_pushed_limit.value());
				}
			}
		}
	}

	// Recurse into children
	for (auto &child : op.children) {
		WalkPlanTree(*child, current_order, current_limit, current_topn, projections);
	}

	// Restore the projection stack to its previous size for the caller
	projections.resize(saved_projection_size);
}

void FirestorePreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	std::vector<LogicalProjection *> projections;
	WalkPlanTree(*plan, nullptr, nullptr, nullptr, projections);
}

} // namespace duckdb
