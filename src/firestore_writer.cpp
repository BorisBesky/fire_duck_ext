#include "firestore_writer.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "firestore_logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace duckdb {

// ============================================================================
// INSERT function implementation (table-in-out)
// Usage: SELECT * FROM firestore_insert('collection', (SELECT col1, col2 FROM ...))
//        SELECT * FROM firestore_insert('collection', (SELECT ...), document_id := 'id_col')
// ============================================================================

struct FirestoreInsertBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;
	std::optional<std::string> document_id_param;
	idx_t document_id_column_index;
	bool use_auto_ids;

	FirestoreInsertBindData() : document_id_column_index(DConstants::INVALID_INDEX), use_auto_ids(true) {
	}
};

struct FirestoreInsertGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	idx_t rows_inserted;
	std::vector<json> batch_writes;
	bool use_individual_ops;
	bool count_emitted;

	FirestoreInsertGlobalState() : rows_inserted(0), use_individual_ops(false), count_emitted(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreInsertBindData>();

	// Get collection name (first positional argument)
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

	// Capture input table schema (column names and types from the subquery)
	result->column_names = input.input_table_names;
	result->column_types = input.input_table_types;

	if (result->column_names.empty()) {
		throw BinderException("firestore_insert requires a subquery with at least one column. "
		                      "Usage: SELECT * FROM firestore_insert('collection', (SELECT col1, col2 FROM ...))");
	}

	// Resolve document_id column if specified
	if (result->document_id_param.has_value()) {
		const auto &id_col = result->document_id_param.value();
		bool found = false;
		for (idx_t i = 0; i < result->column_names.size(); i++) {
			if (result->column_names[i] == id_col) {
				result->document_id_column_index = i;
				result->use_auto_ids = false;
				found = true;
				break;
			}
		}
		if (!found) {
			throw BinderException("document_id column '" + id_col + "' not found in input columns.");
		}
	}

	// Return type: single count column
	names.push_back("count");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FirestoreInsertInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<FirestoreInsertBindData>();
	auto global_state = make_uniq<FirestoreInsertGlobalState>();
	global_state->client = make_uniq<FirestoreClient>(bind_data.credentials);
	return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> FirestoreInsertInitLocal(ExecutionContext &context,
                                                                    TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state) {
	return make_uniq<LocalTableFunctionState>();
}

// Flush accumulated batch writes to Firestore
static void FlushInsertBatchWrites(FirestoreInsertBindData &bind_data, FirestoreInsertGlobalState &global_state) {
	if (global_state.batch_writes.empty()) {
		return;
	}

	try {
		global_state.client->BatchWrite(global_state.batch_writes);
		global_state.rows_inserted += global_state.batch_writes.size();
	} catch (const FirestorePermissionException &) {
		// BatchWrite requires admin auth - fall back to individual CreateDocument calls
		FS_LOG_WARN("BatchWrite permission denied for insert, falling back to individual CreateDocument calls");
		global_state.use_individual_ops = true;

		for (auto &write_op : global_state.batch_writes) {
			try {
				std::string doc_path = write_op["update"]["name"].get<std::string>();
				std::string doc_id = doc_path.substr(doc_path.rfind('/') + 1);
				json fields = write_op["update"]["fields"];

				global_state.client->CreateDocument(bind_data.collection, fields, doc_id);
				global_state.rows_inserted++;
			} catch (const std::exception &e) {
				FS_LOG_WARN("Individual insert failed during fallback: " + std::string(e.what()));
			}
		}
	}
	global_state.batch_writes.clear();
}

// Table in-out function: receives input DataChunks and inserts rows into Firestore
static OperatorResultType FirestoreInsertInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                                       DataChunk &input, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreInsertBindData>();
	auto &global_state = data.global_state->Cast<FirestoreInsertGlobalState>();

	const size_t BATCH_SIZE = 500;

	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		// Build Firestore fields JSON for this row
		json fields;
		std::string doc_id_value;

		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			// If this column is the document_id column, extract the ID and skip adding it as a field
			if (col_idx == bind_data.document_id_column_index) {
				Value id_val = input.GetValue(col_idx, row_idx);
				if (id_val.IsNull()) {
					throw InvalidInputException("firestore_insert: document_id column '" +
					                            bind_data.document_id_param.value() + "' cannot be NULL at row " +
					                            std::to_string(global_state.rows_inserted + row_idx));
				}
				doc_id_value = id_val.ToString();
				continue;
			}

			const auto &col_name = bind_data.column_names[col_idx];
			Value val = input.GetValue(col_idx, row_idx);
			fields[col_name] = DuckDBValueToFirestore(val, bind_data.column_types[col_idx]);
		}

		if (bind_data.use_auto_ids) {
			// Auto-generated IDs: must use individual CreateDocument calls
			// (BatchWrite requires a full document path including the ID)
			try {
				global_state.client->CreateDocument(bind_data.collection, fields);
				global_state.rows_inserted++;
			} catch (const std::exception &e) {
				throw InvalidInputException("Firestore insert failed at row " +
				                            std::to_string(global_state.rows_inserted) + ": " +
				                            std::string(e.what()));
			}
		} else if (global_state.use_individual_ops) {
			// Fallback mode: individual CreateDocument calls with explicit ID
			try {
				global_state.client->CreateDocument(bind_data.collection, fields, doc_id_value);
				global_state.rows_inserted++;
			} catch (const std::exception &e) {
				throw InvalidInputException("Firestore insert failed for document '" + doc_id_value +
				                            "': " + std::string(e.what()));
			}
		} else {
			// Batch mode: accumulate write operations
			auto resolved = ResolveDocumentPath(bind_data.collection, doc_id_value);
			std::string doc_path = "projects/" + bind_data.credentials->project_id + "/databases/" +
			                       bind_data.credentials->database_id + "/documents/" + resolved.document_path;

			json write_op = {{"update", {{"name", doc_path}, {"fields", fields}}}};
			global_state.batch_writes.push_back(write_op);

			if (global_state.batch_writes.size() >= BATCH_SIZE) {
				FlushInsertBatchWrites(bind_data, global_state);
			}
		}
	}

	output.SetCardinality(0);
	return OperatorResultType::NEED_MORE_INPUT;
}

// Finalize: flush remaining writes and emit the count
static OperatorFinalizeResultType FirestoreInsertFinal(ExecutionContext &context, TableFunctionInput &data,
                                                       DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreInsertBindData>();
	auto &global_state = data.global_state->Cast<FirestoreInsertGlobalState>();

	if (global_state.count_emitted) {
		output.SetCardinality(0);
		return OperatorFinalizeResultType::FINISHED;
	}

	// Flush any remaining batch writes
	if (!bind_data.use_auto_ids && !global_state.batch_writes.empty()) {
		FlushInsertBatchWrites(bind_data, global_state);
	}

	// Emit the final count
	FlatVector::GetData<int64_t>(output.data[0])[0] = static_cast<int64_t>(global_state.rows_inserted);
	output.SetCardinality(1);
	global_state.count_emitted = true;
	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
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
	// Register firestore_insert table function (table-in-out)
	// Usage: SELECT * FROM firestore_insert('collection', (SELECT col1, col2 FROM ...))
	//        SELECT * FROM firestore_insert('collection', (SELECT ...), document_id := 'id_col')
	TableFunction insert_func("firestore_insert", {LogicalType::VARCHAR, LogicalType::TABLE}, nullptr,
	                          FirestoreInsertBind, FirestoreInsertInitGlobal, FirestoreInsertInitLocal);

	insert_func.in_out_function = FirestoreInsertInOutFunction;
	insert_func.in_out_function_final = FirestoreInsertFinal;

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
