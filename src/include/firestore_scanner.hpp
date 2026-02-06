#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "firestore_client.hpp"
#include "firestore_index.hpp"

namespace duckdb {

class ExtensionLoader;

// Bind data - stores parameters from SQL call
struct FirestoreScanBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> column_names; // All column names (excluding __document_id)
	std::vector<LogicalType> column_types; // All column types (excluding __document_id)

	// Projection info - which columns to actually output
	std::vector<idx_t> projected_columns; // Indices into column_names/column_types

	// Query options
	std::optional<int64_t> limit;
	std::optional<std::string> order_by;

	// Collection group query flag - when true, __document_id returns full path
	bool is_collection_group = false;

	// Index cache - populated at bind time for filter pushdown
	std::shared_ptr<FirestoreIndexCache> index_cache;

	// Candidate filters extracted by pushdown_complex_filter callback
	// These are stored here so InitGlobal can use them to build the Firestore query.
	// All original DuckDB expressions are left intact so DuckDB re-verifies results.
	std::vector<FirestorePushdownFilter> candidate_pushdown_filters;
};

// Global state - shared across threads
struct FirestoreScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	std::vector<FirestoreDocument> documents;
	idx_t current_index;
	bool finished;
	std::string next_page_token;

	// Filter pushdown state
	FirestoreFilterResult pushdown_result;
	json structured_query;       // Cached StructuredQuery for pagination
	bool uses_run_query = false; // Whether using :runQuery (true when filters pushed)

	// Pagination optimization: track page size to detect end of results
	int64_t query_page_size = 1000;   // The page size used in the query
	bool last_page_was_full = true;   // Whether last fetch returned a full page

	FirestoreScanGlobalState() : current_index(0), finished(false) {
	}

	idx_t MaxThreads() const override {
		return 1;
	} // REST API is sequential
};

// Local state - per-thread state (minimal for single-threaded)
struct FirestoreScanLocalState : public LocalTableFunctionState {
	// Single-threaded, no local state needed
};

// Register the firestore_scan function
void RegisterFirestoreScanFunction(ExtensionLoader &loader);

// Table function callbacks
unique_ptr<FunctionData> FirestoreScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> FirestoreScanInitGlobal(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> FirestoreScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state);

void FirestoreScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
