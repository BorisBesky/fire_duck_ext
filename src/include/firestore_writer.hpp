#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "firestore_client.hpp"

namespace duckdb {

class ExtensionLoader;

// Bind data for write operations
struct FirestoreWriteBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;

	// For specifying document ID on insert
	std::optional<std::string> document_id_column;
};

// Global state for write operations
struct FirestoreWriteGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	idx_t rows_written;

	FirestoreWriteGlobalState() : rows_written(0) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Register write-related functions
void RegisterFirestoreWriteFunctions(ExtensionLoader &loader);

// INSERT function: firestore_insert('collection', columns...)
void RegisterFirestoreInsertFunction(ExtensionLoader &loader);

// UPDATE function: firestore_update('collection', 'doc_id', 'field1', value1, ...)
void RegisterFirestoreUpdateFunction(ExtensionLoader &loader);

// DELETE function: firestore_delete('collection', 'doc_id')
void RegisterFirestoreDeleteFunction(ExtensionLoader &loader);

// BATCH UPDATE: firestore_update_batch('collection', list_of_ids, 'field1', value1, ...)
// Usage: SELECT * FROM firestore_update_batch('users',
//            (SELECT list(__document_id) FROM firestore_scan('users') WHERE status='pending'),
//            'status', 'active');
void RegisterFirestoreUpdateBatchFunction(ExtensionLoader &loader);

// BATCH DELETE: firestore_delete_batch('collection', list_of_ids)
// Usage: SELECT * FROM firestore_delete_batch('users',
//            (SELECT list(__document_id) FROM firestore_scan('users') WHERE status='deleted'));
void RegisterFirestoreDeleteBatchFunction(ExtensionLoader &loader);

// For COPY TO firestore
void RegisterFirestoreCopyFunction(ExtensionLoader &loader);

} // namespace duckdb
