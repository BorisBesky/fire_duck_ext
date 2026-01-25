#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "firestore_client.hpp"

namespace duckdb {

class ExtensionLoader;

// Bind data - stores parameters from SQL call
struct FirestoreScanBindData : public TableFunctionData {
    std::string collection;
    std::shared_ptr<FirestoreCredentials> credentials;
    std::vector<std::string> column_names;   // All column names (excluding __document_id)
    std::vector<LogicalType> column_types;   // All column types (excluding __document_id)

    // Projection info - which columns to actually output
    std::vector<idx_t> projected_columns;    // Indices into column_names/column_types

    // Query options
    std::optional<int64_t> limit;
    std::optional<std::string> order_by;

    // Collection group query flag - when true, __document_id returns full path
    bool is_collection_group = false;
};

// Global state - shared across threads
struct FirestoreScanGlobalState : public GlobalTableFunctionState {
    std::unique_ptr<FirestoreClient> client;
    std::vector<FirestoreDocument> documents;
    idx_t current_index;
    bool finished;
    std::string next_page_token;

    FirestoreScanGlobalState() : current_index(0), finished(false) {}

    idx_t MaxThreads() const override { return 1; }  // REST API is sequential
};

// Local state - per-thread state (minimal for single-threaded)
struct FirestoreScanLocalState : public LocalTableFunctionState {
    // Single-threaded, no local state needed
};

// Register the firestore_scan function
void RegisterFirestoreScanFunction(ExtensionLoader &loader);

// Table function callbacks
unique_ptr<FunctionData> FirestoreScanBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
);

unique_ptr<GlobalTableFunctionState> FirestoreScanInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input
);

unique_ptr<LocalTableFunctionState> FirestoreScanInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state
);

void FirestoreScanFunction(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output
);

} // namespace duckdb
