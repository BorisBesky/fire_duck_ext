#pragma once

// One source tree for DuckDB 1.5 and DuckDB 2.0.
//
// DuckDB 2.0 changed several internal APIs this extension uses: names became
// Identifier rather than string, FlatVector::GetData returns const data (writes
// go through GetDataMutable), table functions declare their parameters through
// a FunctionSignature, comparisons are bound as function expressions, and most
// bound-expression and binding members became private behind accessors. The
// helpers below paper over those differences so the rest of the code can be
// written once. Everything is selected at compile time; on DuckDB 1.5 each
// helper compiles to exactly the code it replaced.

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

// DuckDB 2.0 split the vector helpers into duckdb/common/vector/. Not
// identifier.hpp: DuckDB 1.5.6 has that header too, but not the 2.0 APIs.
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#define FIRESTORE_DUCKDB_V2 1
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#endif

#include <string>
#include <utility>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Names
//===--------------------------------------------------------------------===//
#ifdef FIRESTORE_DUCKDB_V2
// The element type of the `names` a table function's bind fills in, and of
// LogicalGet::names.
using FirestoreName = Identifier;
inline const string &FirestoreNameString(const Identifier &name) {
	return name.GetIdentifierName();
}
#else
using FirestoreName = string;
inline const string &FirestoreNameString(const string &name) {
	return name;
}
#endif

inline vector<string> FirestoreNameStrings(const vector<FirestoreName> &names) {
	vector<string> result;
	result.reserve(names.size());
	for (auto &name : names) {
		result.push_back(FirestoreNameString(name));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Writing to flat vectors
//===--------------------------------------------------------------------===//
template <class T>
inline T *FirestoreMutableData(Vector &vector) {
#ifdef FIRESTORE_DUCKDB_V2
	return FlatVector::GetDataMutable<T>(vector);
#else
	return FlatVector::GetData<T>(vector);
#endif
}

// StructVector::GetEntries holds unique_ptr<Vector> on DuckDB 1.5 and Vector
// on 2.0.
inline Vector &FirestoreStructEntry(Vector &entry) {
	return entry;
}
inline Vector &FirestoreStructEntry(unique_ptr<Vector> &entry) {
	return *entry;
}

// Set how many rows a table function produced. DuckDB 2.0 vectors carry their
// own size, which DataChunk::SetCardinality (deprecated there) leaves alone.
inline void FirestoreSetCardinality(DataChunk &output, idx_t count) {
#ifdef FIRESTORE_DUCKDB_V2
	output.SetChildCardinality(count);
#else
	output.SetCardinality(count);
#endif
}

//===--------------------------------------------------------------------===//
// Table function parameters
//===--------------------------------------------------------------------===//
// Accept any number of trailing positional arguments of `type`. Call before
// FirestoreAddNamedParameters: DuckDB 2.0 lays parameters out as
// [positional | *args | named].
inline void FirestoreSetVarArgs(TableFunction &function, LogicalType type) {
#ifdef FIRESTORE_DUCKDB_V2
	function.GetSignature().AddArgs("args", std::move(type));
#else
	function.varargs = std::move(type);
#endif
}

// Declare optional `name := value` parameters. A call that leaves one out
// does not pass it, on either version.
inline void FirestoreAddNamedParameters(TableFunction &function,
                                        const vector<std::pair<string, LogicalType>> &parameters) {
#ifdef FIRESTORE_DUCKDB_V2
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		for (auto &parameter : parameters) {
			options.Add(Identifier(parameter.first), parameter.second);
		}
	});
#else
	for (auto &parameter : parameters) {
		function.named_parameters[parameter.first] = parameter.second;
	}
#endif
}

// The positional parameter types, excluding varargs.
inline vector<LogicalType> FirestorePositionalArgumentTypes(const TableFunction &function) {
#ifdef FIRESTORE_DUCKDB_V2
	vector<LogicalType> result;
	for (auto &parameter : function.GetSignature().GetParameters()) {
		if (parameter.AcceptsPosition() && !parameter.IsVariadic()) {
			result.push_back(parameter.GetType());
		}
	}
	return result;
#else
	return function.arguments;
#endif
}

// The names of the named parameters, in the order duckdb_functions() lists
// them. On DuckDB 1.5, pass the copy GetFunctionByOffset returned.
inline vector<string> FirestoreNamedParameterNames(const TableFunction &function) {
	vector<string> result;
#ifdef FIRESTORE_DUCKDB_V2
	auto kwargs = function.GetSignature().GetTypedKwargs();
	if (kwargs) {
		for (auto &option : kwargs->GetOptions()) {
			result.push_back(option.name.GetIdentifierName());
		}
	}
#else
	// duckdb_functions() iterates the named_parameters of the copy that
	// GetFunctionByOffset returns, so `function` must be that copy, iterated
	// as is. The map is unordered, and with libc++ and MSVC copying it again
	// changes its order, which would pair names with the wrong types.
	for (auto &parameter : function.named_parameters) {
		result.push_back(parameter.first);
	}
#endif
	return result;
}

inline const string &FirestoreFunctionName(const LogicalGet &get) {
#ifdef FIRESTORE_DUCKDB_V2
	return get.function.GetName().GetIdentifierName();
#else
	return get.function.name;
#endif
}

//===--------------------------------------------------------------------===//
// Bound expressions and bindings
//===--------------------------------------------------------------------===//
#ifdef FIRESTORE_DUCKDB_V2
inline idx_t FirestoreIndexValue(const TableIndex &index) {
	return index.index;
}
inline idx_t FirestoreIndexValue(const ProjectionIndex &index) {
	return index.GetIndex();
}
inline const ColumnBinding &FirestoreBinding(const BoundColumnRefExpression &colref) {
	return colref.Binding();
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundConjunctionExpression &expr) {
	return expr.GetChildren();
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundOperatorExpression &expr) {
	return expr.GetChildren();
}
inline const Value &FirestoreConstantValue(const BoundConstantExpression &expr) {
	return expr.GetValue();
}
// A binary comparison (=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM).
inline bool FirestoreIsComparison(const Expression &expr) {
	return BoundComparisonExpression::IsComparison(expr);
}
inline const Expression &FirestoreComparisonLeft(const Expression &comparison) {
	return BoundComparisonExpression::Left(comparison.Cast<BoundFunctionExpression>());
}
inline const Expression &FirestoreComparisonRight(const Expression &comparison) {
	return BoundComparisonExpression::Right(comparison.Cast<BoundFunctionExpression>());
}
inline const string &FirestoreAggregateName(const BoundAggregateExpression &expr) {
	return expr.Function().GetName().GetIdentifierName();
}
inline bool FirestoreAggregateHasFilter(const BoundAggregateExpression &expr) {
	return expr.GetFilter() != nullptr;
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundAggregateExpression &expr) {
	return expr.GetChildren();
}
#else
inline idx_t FirestoreIndexValue(idx_t index) {
	return index;
}
inline const ColumnBinding &FirestoreBinding(const BoundColumnRefExpression &colref) {
	return colref.binding;
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundConjunctionExpression &expr) {
	return expr.children;
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundOperatorExpression &expr) {
	return expr.children;
}
inline const Value &FirestoreConstantValue(const BoundConstantExpression &expr) {
	return expr.value;
}
inline bool FirestoreIsComparison(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON;
}
inline const Expression &FirestoreComparisonLeft(const Expression &comparison) {
	return *comparison.Cast<BoundComparisonExpression>().left;
}
inline const Expression &FirestoreComparisonRight(const Expression &comparison) {
	return *comparison.Cast<BoundComparisonExpression>().right;
}
inline const string &FirestoreAggregateName(const BoundAggregateExpression &expr) {
	return expr.function.name;
}
inline bool FirestoreAggregateHasFilter(const BoundAggregateExpression &expr) {
	return expr.filter != nullptr;
}
inline const vector<unique_ptr<Expression>> &FirestoreChildren(const BoundAggregateExpression &expr) {
	return expr.children;
}
#endif

// The table a column reference reads from, and its position in that table's
// projected columns.
inline idx_t FirestoreBindingTable(const BoundColumnRefExpression &colref) {
	return FirestoreIndexValue(FirestoreBinding(colref).table_index);
}
inline idx_t FirestoreBindingColumn(const BoundColumnRefExpression &colref) {
	return FirestoreIndexValue(FirestoreBinding(colref).column_index);
}

} // namespace duckdb
