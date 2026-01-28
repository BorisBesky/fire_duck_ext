#include "firestore_writer.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "firestore_logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

// Bind data for firestore_insert
struct FirestoreInsertBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;
	std::optional<std::string> document_id_param;
};

// Global state for insert operation
struct FirestoreInsertGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	idx_t rows_inserted;
	std::vector<json> batch_writes;

	FirestoreInsertGlobalState() : rows_inserted(0) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind function for firestore_insert
static unique_ptr<FunctionData> FirestoreInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreInsertBindData>();

	// Get collection name
	result->collection = input.inputs[0].GetValue<string>();

	// Process named parameters
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		} else if (kv.first == "document_id") {
			result->document_id_param = kv.second.GetValue<string>();
		}
	}

	// Resolve credentials
	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for insert operation.");
	}

	// Return type is a count of inserted rows
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

// Init global state for insert
static unique_ptr<GlobalTableFunctionState> FirestoreInsertInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<FirestoreInsertBindData>();
	auto global_state = make_uniq<FirestoreInsertGlobalState>();
	global_state->client = make_uniq<FirestoreClient>(bind_data.credentials);
	return std::move(global_state);
}

// Local state (minimal)
static unique_ptr<LocalTableFunctionState> FirestoreInsertInitLocal(ExecutionContext &context,
                                                                    TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state) {
	return make_uniq<LocalTableFunctionState>();
}

// Insert function execution
static void FirestoreInsertFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreInsertBindData>();
	auto &global_state = data.global_state->Cast<FirestoreInsertGlobalState>();

	// This function returns the count after all inserts
	// The actual insertion happens in the in_out_function
	FlatVector::GetData<int64_t>(output.data[0])[0] = global_state.rows_inserted;
	output.SetCardinality(1);
}

// ============================================================================
// UPDATE function implementation
// Usage: SELECT * FROM firestore_update('collection', 'doc_id', 'field1', value1, ...)
// ============================================================================

struct FirestoreUpdateBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::string document_id;
	std::vector<std::string> field_names;
	std::vector<Value> field_values;
	bool executed;

	FirestoreUpdateBindData() : executed(false) {
	}
};

struct FirestoreUpdateGlobalState : public GlobalTableFunctionState {
	bool done;
	FirestoreUpdateGlobalState() : done(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreUpdateBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreUpdateBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();

	// Parse remaining args as field_name/value pairs
	// Must have even number of remaining args
	if ((input.inputs.size() - 2) % 2 != 0) {
		throw BinderException(
		    "firestore_update requires field name/value pairs. "
		    "Usage: firestore_update('collection', 'doc_id', 'field1', value1, 'field2', value2, ...)");
	}

	for (idx_t i = 2; i < input.inputs.size(); i += 2) {
		// Field name must be string
		if (input.inputs[i].type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Field name at position " + std::to_string(i) + " must be a string");
		}
		result->field_names.push_back(input.inputs[i].GetValue<string>());
		result->field_values.push_back(input.inputs[i + 1]);
	}

	if (result->field_names.empty()) {
		throw BinderException("firestore_update requires at least one field to update");
	}

	// Process named parameters for credentials
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		}
	}

	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for update operation.");
	}

	// Return type: count of updated documents (1 or 0)
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FirestoreUpdateInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<FirestoreUpdateGlobalState>();
}

static void FirestoreUpdateFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreUpdateBindData>();
	auto &global_state = data.global_state->Cast<FirestoreUpdateGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	int64_t count = 0;

	try {
		// Create Firestore client
		FirestoreClient client(bind_data.credentials);

		// Build fields JSON for update
		json fields;
		for (size_t i = 0; i < bind_data.field_names.size(); i++) {
			fields[bind_data.field_names[i]] =
			    DuckDBValueToFirestore(bind_data.field_values[i], bind_data.field_values[i].type());
		}

		// Perform update
		client.UpdateDocument(bind_data.collection, bind_data.document_id, fields);
		count = 1;
	} catch (const FirestoreNotFoundException &e) {
		// Document not found - return 0
		count = 0;
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore update failed: " + std::string(e.what()));
	}

	FlatVector::GetData<int64_t>(output.data[0])[0] = count;
	output.SetCardinality(1);
	global_state.done = true;
}

// ============================================================================
// DELETE function implementation
// Usage: SELECT * FROM firestore_delete('collection', 'doc_id')
// ============================================================================

struct FirestoreDeleteBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::string document_id;
};

struct FirestoreDeleteGlobalState : public GlobalTableFunctionState {
	bool done;
	FirestoreDeleteGlobalState() : done(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreDeleteBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreDeleteBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();

	// Process named parameters for credentials
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		}
	}

	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for delete operation.");
	}

	// Return type: count of deleted documents (1 or 0)
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FirestoreDeleteInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<FirestoreDeleteGlobalState>();
}

static void FirestoreDeleteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreDeleteBindData>();
	auto &global_state = data.global_state->Cast<FirestoreDeleteGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	int64_t count = 0;

	try {
		// Create Firestore client
		FirestoreClient client(bind_data.credentials);

		// Perform delete
		client.DeleteDocument(bind_data.collection, bind_data.document_id);
		count = 1;
	} catch (const FirestoreNotFoundException &e) {
		// Document not found - return 0
		count = 0;
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore delete failed: " + std::string(e.what()));
	}

	FlatVector::GetData<int64_t>(output.data[0])[0] = count;
	output.SetCardinality(1);
	global_state.done = true;
}

// ============================================================================
// BATCH UPDATE function implementation
// Usage: SELECT * FROM firestore_update_batch('collection', ['id1','id2',...], 'field1', value1, ...)
// ============================================================================

struct FirestoreUpdateBatchBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> document_ids;
	std::vector<std::string> field_names;
	std::vector<Value> field_values;
};

struct FirestoreUpdateBatchGlobalState : public GlobalTableFunctionState {
	bool done;
	FirestoreUpdateBatchGlobalState() : done(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreUpdateBatchBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreUpdateBatchBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document IDs (second arg) - must be a LIST of VARCHAR
	auto &ids_value = input.inputs[1];
	if (ids_value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("firestore_update_batch requires a LIST of document IDs as second argument. "
		                      "Usage: firestore_update_batch('collection', ['id1','id2'], 'field1', value1, ...)");
	}

	// Handle NULL list (e.g., from empty filter result)
	if (!ids_value.IsNull()) {
		auto &id_list = ListValue::GetChildren(ids_value);
		for (auto &id_val : id_list) {
			result->document_ids.push_back(id_val.GetValue<string>());
		}
	}

	// Parse remaining args as field_name/value pairs
	if ((input.inputs.size() - 2) % 2 != 0) {
		throw BinderException("firestore_update_batch requires field name/value pairs after document IDs.");
	}

	for (idx_t i = 2; i < input.inputs.size(); i += 2) {
		if (input.inputs[i].type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Field name at position " + std::to_string(i) + " must be a string");
		}
		result->field_names.push_back(input.inputs[i].GetValue<string>());
		result->field_values.push_back(input.inputs[i + 1]);
	}

	if (result->field_names.empty()) {
		throw BinderException("firestore_update_batch requires at least one field to update");
	}

	// Process named parameters for credentials
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		}
	}

	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for batch update operation.");
	}

	// Return type: count of updated documents
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FirestoreUpdateBatchInitGlobal(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	return make_uniq<FirestoreUpdateBatchGlobalState>();
}

static void FirestoreUpdateBatchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreUpdateBatchBindData>();
	auto &global_state = data.global_state->Cast<FirestoreUpdateBatchGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials);

		// Build fields JSON for update (same for all documents)
		json fields;
		for (size_t i = 0; i < bind_data.field_names.size(); i++) {
			fields[bind_data.field_names[i]] =
			    DuckDBValueToFirestore(bind_data.field_values[i], bind_data.field_values[i].type());
		}

		// Try batch write first, fall back to individual operations if it fails
		// (e.g., when running against emulator with API key auth)
		bool use_individual_ops = false;
		const size_t BATCH_SIZE = 500;
		std::vector<json> writes;

		for (size_t i = 0; i < bind_data.document_ids.size(); i++) {
			const auto &doc_id = bind_data.document_ids[i];

			if (use_individual_ops) {
				// Individual update - fallback mode
				try {
					client.UpdateDocument(bind_data.collection, doc_id, fields);
					count++;
				} catch (const FirestoreNotFoundException &) {
					// Document not found, skip but continue
					FS_LOG_WARN("Document not found during batch operation: " + doc_id);
				}
			} else {
				auto resolved = ResolveDocumentPath(bind_data.collection, doc_id);
				// Build full document path for batch write
				std::string doc_path = "projects/" + bind_data.credentials->project_id + "/databases/" +
				                       bind_data.credentials->database_id + "/documents/" + resolved.document_path;

				json write_op = {{"update", {{"name", doc_path}, {"fields", fields}}},
				                 {"updateMask", {{"fieldPaths", bind_data.field_names}}}};
				writes.push_back(write_op);

				// Execute batch when full or at end
				if (writes.size() >= BATCH_SIZE || i == bind_data.document_ids.size() - 1) {
					try {
						client.BatchWrite(writes);
						count += writes.size();
					} catch (const FirestorePermissionException &) {
						// Batch writes require admin auth - fall back to individual ops
						use_individual_ops = true;
						// Process this batch individually
						for (size_t j = 0; j <= i - (writes.size() - 1); j++) {
							// These were already counted, skip
						}
						// Process remaining items in writes individually
						for (size_t j = i - (writes.size() - 1); j <= i; j++) {
							try {
								client.UpdateDocument(bind_data.collection, bind_data.document_ids[j], fields);
								count++;
							} catch (const FirestoreNotFoundException &) {
								// Document not found, skip
								FS_LOG_WARN("Document not found during batch operation: " + bind_data.document_ids[j]);
							}
						}
					}
					writes.clear();
				}
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore batch update failed: " + std::string(e.what()));
	}

	FlatVector::GetData<int64_t>(output.data[0])[0] = count;
	output.SetCardinality(1);
	global_state.done = true;
}

// ============================================================================
// BATCH DELETE function implementation
// Usage: SELECT * FROM firestore_delete_batch('collection', ['id1','id2',...])
// ============================================================================

struct FirestoreDeleteBatchBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> document_ids;
};

struct FirestoreDeleteBatchGlobalState : public GlobalTableFunctionState {
	bool done;
	FirestoreDeleteBatchGlobalState() : done(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreDeleteBatchBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreDeleteBatchBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document IDs (second arg) - must be a LIST of VARCHAR
	auto &ids_value = input.inputs[1];
	if (ids_value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("firestore_delete_batch requires a LIST of document IDs as second argument. "
		                      "Usage: firestore_delete_batch('collection', ['id1','id2'])");
	}

	// Handle NULL list (e.g., from empty filter result)
	if (!ids_value.IsNull()) {
		auto &id_list = ListValue::GetChildren(ids_value);
		for (auto &id_val : id_list) {
			result->document_ids.push_back(id_val.GetValue<string>());
		}
	}

	// Process named parameters for credentials
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		}
	}

	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for batch delete operation.");
	}

	// Return type: count of deleted documents
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FirestoreDeleteBatchInitGlobal(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	return make_uniq<FirestoreDeleteBatchGlobalState>();
}

static void FirestoreDeleteBatchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreDeleteBatchBindData>();
	auto &global_state = data.global_state->Cast<FirestoreDeleteBatchGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials);

		// Try batch write first, fall back to individual operations if it fails
		// (e.g., when running against emulator with API key auth)
		bool use_individual_ops = false;
		const size_t BATCH_SIZE = 500;
		std::vector<json> writes;

		for (size_t i = 0; i < bind_data.document_ids.size(); i++) {
			const auto &doc_id = bind_data.document_ids[i];

			if (use_individual_ops) {
				// Individual delete - fallback mode
				try {
					client.DeleteDocument(bind_data.collection, doc_id);
					count++;
				} catch (const FirestoreNotFoundException &) {
					// Document not found, skip but continue
					FS_LOG_WARN("Document not found during batch operation: " + doc_id);
				}
			} else {
				auto resolved = ResolveDocumentPath(bind_data.collection, doc_id);
				// Build full document path for batch write
				std::string doc_path = "projects/" + bind_data.credentials->project_id + "/databases/" +
				                       bind_data.credentials->database_id + "/documents/" + resolved.document_path;

				json write_op = {{"delete", doc_path}};
				writes.push_back(write_op);

				// Execute batch when full or at end
				if (writes.size() >= BATCH_SIZE || i == bind_data.document_ids.size() - 1) {
					try {
						client.BatchWrite(writes);
						count += writes.size();
					} catch (const FirestorePermissionException &) {
						// Batch writes require admin auth - fall back to individual ops
						use_individual_ops = true;
						// Process remaining items in writes individually
						for (size_t j = i - (writes.size() - 1); j <= i; j++) {
							try {
								client.DeleteDocument(bind_data.collection, bind_data.document_ids[j]);
								count++;
							} catch (const FirestoreNotFoundException &) {
								// Document not found, skip
								FS_LOG_WARN("Document not found during batch operation: " + bind_data.document_ids[j]);
							}
						}
					}
					writes.clear();
				}
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore batch delete failed: " + std::string(e.what()));
	}

	FlatVector::GetData<int64_t>(output.data[0])[0] = count;
	output.SetCardinality(1);
	global_state.done = true;
}

// ============================================================================
// ARRAY TRANSFORM functions implementation
// Usage: SELECT * FROM firestore_array_union('collection', 'doc_id', 'field', ['val1', 'val2'])
//        SELECT * FROM firestore_array_remove('collection', 'doc_id', 'field', ['val1', 'val2'])
//        SELECT * FROM firestore_array_append('collection', 'doc_id', 'field', ['val1', 'val2'])
// ============================================================================

struct FirestoreArrayTransformBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::string document_id;
	std::string field_name;
	std::vector<Value> elements;
	FirestoreClient::ArrayTransformType transform_type;
};

struct FirestoreArrayTransformGlobalState : public GlobalTableFunctionState {
	bool done;
	FirestoreArrayTransformGlobalState() : done(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreArrayTransformBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names,
                                                            FirestoreClient::ArrayTransformType transform_type) {
	auto result = make_uniq<FirestoreArrayTransformBindData>();
	result->transform_type = transform_type;

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();

	// Get field name (third arg)
	result->field_name = input.inputs[2].GetValue<string>();

	// Get elements to add/remove (fourth arg) - must be a LIST
	auto &elements_value = input.inputs[3];
	if (elements_value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("Array transform requires a LIST of elements as fourth argument.");
	}

	if (!elements_value.IsNull()) {
		auto &element_list = ListValue::GetChildren(elements_value);
		for (auto &elem : element_list) {
			result->elements.push_back(elem);
		}
	}

	// Process named parameters for credentials
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		}
	}

	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for array transform operation.");
	}

	// Return type: count of affected documents (1 or 0)
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<FunctionData> FirestoreArrayUnionBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	return FirestoreArrayTransformBind(context, input, return_types, names,
	                                   FirestoreClient::ArrayTransformType::ARRAY_UNION);
}

static unique_ptr<FunctionData> FirestoreArrayRemoveBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	return FirestoreArrayTransformBind(context, input, return_types, names,
	                                   FirestoreClient::ArrayTransformType::ARRAY_REMOVE);
}

static unique_ptr<FunctionData> FirestoreArrayAppendBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	return FirestoreArrayTransformBind(context, input, return_types, names,
	                                   FirestoreClient::ArrayTransformType::ARRAY_APPEND);
}

static unique_ptr<GlobalTableFunctionState> FirestoreArrayTransformInitGlobal(ClientContext &context,
                                                                              TableFunctionInitInput &input) {
	return make_uniq<FirestoreArrayTransformGlobalState>();
}

static void FirestoreArrayTransformFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreArrayTransformBindData>();
	auto &global_state = data.global_state->Cast<FirestoreArrayTransformGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials);

		// Convert DuckDB values to Firestore format
		json elements = json::array();
		LogicalType element_type = LogicalType::VARCHAR;
		if (!bind_data.elements.empty()) {
			element_type = bind_data.elements[0].type();
		}

		for (const auto &elem : bind_data.elements) {
			elements.push_back(DuckDBValueToFirestore(elem, elem.type()));
		}

		// Perform the array transform
		client.ArrayTransform(bind_data.collection, bind_data.document_id, bind_data.field_name, elements,
		                      bind_data.transform_type);
		count = 1;
	} catch (const FirestoreNotFoundException &e) {
		count = 0;
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore array transform failed: " + std::string(e.what()));
	}

	FlatVector::GetData<int64_t>(output.data[0])[0] = count;
	output.SetCardinality(1);
	global_state.done = true;
}

// ============================================================================
// Table-valued function for bulk insert (via COPY or INSERT...SELECT)
// ============================================================================

// Bind data for firestore output (COPY TO)
struct FirestoreCopyBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	idx_t batch_size;

	FirestoreCopyBindData() : batch_size(500) {
	}
};

// Global state for copy
struct FirestoreCopyGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	std::vector<json> pending_writes;
	idx_t total_written;

	FirestoreCopyGlobalState() : total_written(0) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

// ============================================================================
// Registration
// ============================================================================

void RegisterFirestoreWriteFunctions(ExtensionLoader &loader) {
	// Register firestore_insert table function
	// This allows: INSERT INTO firestore_insert('collection') SELECT ...
	TableFunction insert_func("firestore_insert", {LogicalType::VARCHAR}, // collection
	                          FirestoreInsertFunction, FirestoreInsertBind, FirestoreInsertInitGlobal,
	                          FirestoreInsertInitLocal);

	insert_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	insert_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	insert_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	insert_func.named_parameters["database"] = LogicalType::VARCHAR;
	insert_func.named_parameters["document_id"] = LogicalType::VARCHAR;

	loader.RegisterFunction(insert_func);

	// Register firestore_update table function
	// Usage: SELECT * FROM firestore_update('collection', 'doc_id', 'field1', value1, ...)
	TableFunction update_func("firestore_update",
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR}, // collection, document_id
	                          FirestoreUpdateFunction, FirestoreUpdateBind, FirestoreUpdateInitGlobal);

	update_func.varargs = LogicalType::ANY; // Accept field/value pairs
	update_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	update_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	update_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	update_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(update_func);

	// Register firestore_delete table function
	// Usage: SELECT * FROM firestore_delete('collection', 'doc_id')
	TableFunction delete_func("firestore_delete",
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR}, // collection, document_id
	                          FirestoreDeleteFunction, FirestoreDeleteBind, FirestoreDeleteInitGlobal);

	delete_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	delete_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	delete_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	delete_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(delete_func);

	// Register firestore_update_batch table function
	// Usage: SELECT * FROM firestore_update_batch('collection', ['id1','id2'], 'field1', value1, ...)
	TableFunction update_batch_func(
	    "firestore_update_batch", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    FirestoreUpdateBatchFunction, FirestoreUpdateBatchBind, FirestoreUpdateBatchInitGlobal);

	update_batch_func.varargs = LogicalType::ANY; // Accept field/value pairs
	update_batch_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	update_batch_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	update_batch_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	update_batch_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(update_batch_func);

	// Register firestore_delete_batch table function
	// Usage: SELECT * FROM firestore_delete_batch('collection', ['id1','id2'])
	TableFunction delete_batch_func(
	    "firestore_delete_batch", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    FirestoreDeleteBatchFunction, FirestoreDeleteBatchBind, FirestoreDeleteBatchInitGlobal);

	delete_batch_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	delete_batch_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	delete_batch_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	delete_batch_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(delete_batch_func);

	// Register firestore_array_union table function
	// Usage: SELECT * FROM firestore_array_union('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Adds elements to array without duplicates
	TableFunction array_union_func(
	    "firestore_array_union",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayUnionBind, FirestoreArrayTransformInitGlobal);

	array_union_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	array_union_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	array_union_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	array_union_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(array_union_func);

	// Register firestore_array_remove table function
	// Usage: SELECT * FROM firestore_array_remove('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Removes specified elements from array
	TableFunction array_remove_func(
	    "firestore_array_remove",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayRemoveBind, FirestoreArrayTransformInitGlobal);

	array_remove_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	array_remove_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	array_remove_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	array_remove_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(array_remove_func);

	// Register firestore_array_append table function
	// Usage: SELECT * FROM firestore_array_append('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Appends elements to array (may create duplicates)
	TableFunction array_append_func(
	    "firestore_array_append",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayAppendBind, FirestoreArrayTransformInitGlobal);

	array_append_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	array_append_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	array_append_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	array_append_func.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(array_append_func);
}

void RegisterFirestoreCopyFunction(ExtensionLoader &loader) {
	// COPY function registration would go here
	// This enables: COPY collection FROM 'file.csv' (FORMAT firestore)
	// Deferred to future implementation
}

} // namespace duckdb
