#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Pre-optimize function that walks the logical plan tree to extract
// ORDER BY and LIMIT clauses above firestore_scan LogicalGet nodes
// and injects them into FirestoreScanBindData for server-side pushdown.
// The original ORDER BY / LIMIT nodes are left in place so DuckDB
// always re-verifies results (correctness guarantee).
void FirestorePreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
