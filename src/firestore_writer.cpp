#include "firestore_writer.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "firestore_logger.hpp"
#include "firestore_function_docs.hpp"
#include "firestore_path_utils.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// ============================================================================
// Collection-group targets ('~orders')
// ============================================================================
// A collection group has no single parent, so each document a write names must
// be its full path ('users/u1/orders/o1') and must sit in the group. Checked at
// bind time wherever the paths are known then, so a bad one fails the
// statement before anything is written -- for a batch, before its first group
// of 500 is sent. ResolveDocumentPath checks again when the request is built.

static void CheckCollectionGroupName(const char *function_name, const std::string &collection) {
	if (!IsFirestoreCollectionGroup(collection)) {
		return;
	}
	auto error = FirestoreCollectionGroupNameError(collection);
	if (!error.empty()) {
		throw BinderException(std::string(function_name) + ": " + error);
	}
}

static void CheckCollectionGroupDocumentPath(const char *function_name, const std::string &collection,
                                             const std::string &document_path) {
	if (!IsFirestoreCollectionGroup(collection)) {
		return;
	}
	std::string resolved;
	auto error = ResolveFirestoreCollectionGroupDocumentPath(collection, document_path, resolved);
	if (!error.empty()) {
		throw BinderException(std::string(function_name) + ": " + error);
	}
}

// Create one document at its resolved path, for the inserts that cannot use
// batchWrite. CreateDocument takes the parent collection and the final id
// separately, so the path is split at its last '/'. For a collection group the
// parent comes from the document's own path: the '~' name is not a collection
// a document can be created in.
static void CreateDocumentAt(FirestoreClient &client, const std::string &document_path, const json &fields) {
	size_t last_slash = document_path.rfind('/');
	if (last_slash == std::string::npos) {
		throw InvalidInputException("firestore_insert: '" + document_path + "' is not a document path");
	}
	client.CreateDocument(document_path.substr(0, last_slash), fields, document_path.substr(last_slash + 1));
}

// ============================================================================
// INSERT function implementation (table-in-out)
// Usage: CALL firestore_insert('collection', (SELECT col1, col2 FROM ...))
//        CALL firestore_insert('collection', (SELECT ...), document_id := 'id_col')
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
	// Document path (relative to /documents/) of each entry in batch_writes,
	// for the fallback that creates them one at a time.
	std::vector<std::string> batch_document_paths;
	bool use_individual_ops;
	bool count_emitted;

	FirestoreInsertGlobalState() : rows_inserted(0), use_individual_ops(false), count_emitted(false) {
	}
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FirestoreInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<FirestoreName> &names) {
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

	// A collection group has no single parent collection, so Firestore cannot
	// generate ids in one: each row has to name its document's full path.
	CheckCollectionGroupName("firestore_insert", result->collection);
	if (IsFirestoreCollectionGroup(result->collection) && !result->document_id_param.has_value()) {
		const std::string group_id = result->collection.substr(1);
		throw BinderException(
		    "firestore_insert: cannot generate document ids in collection group '" + result->collection +
		    "', which has no single parent collection. Pass document_id := a column holding each "
		    "document's full path, such as '<parent path>/" +
		    group_id + "/<document id>', or insert into one collection, such as '<parent path>/" + group_id + "'");
	}

	// Resolve credentials
	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException("No Firestore credentials found for insert operation.");
	}

	// Capture input table schema (column names and types from the subquery)
	result->column_names = FirestoreNameStrings(input.input_table_names);
	result->column_types = input.input_table_types;

	if (result->column_names.empty()) {
		throw BinderException("firestore_insert requires a subquery with at least one column. "
		                      "Usage: CALL firestore_insert('collection', (SELECT col1, col2 FROM ...))");
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
	global_state->client = make_uniq<FirestoreClient>(bind_data.credentials, context);
	return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> FirestoreInsertInitLocal(ExecutionContext &context,
                                                                    TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state) {
	return make_uniq<LocalTableFunctionState>();
}

// Flush accumulated batch writes to Firestore
static void FlushInsertBatchWrites(FirestoreInsertGlobalState &global_state) {
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

		for (size_t i = 0; i < global_state.batch_writes.size(); i++) {
			try {
				const json &fields = global_state.batch_writes[i]["update"]["fields"];
				CreateDocumentAt(*global_state.client, global_state.batch_document_paths[i], fields);
				global_state.rows_inserted++;
			} catch (const std::exception &e) {
				FS_LOG_WARN("Individual insert failed during fallback: " + std::string(e.what()));
			}
		}
	}
	global_state.batch_writes.clear();
	global_state.batch_document_paths.clear();
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
				                            std::to_string(global_state.rows_inserted) + ": " + std::string(e.what()));
			}
			continue;
		}

		// Explicit ids. Into a collection group, the id column holds each
		// document's full path; one that is not a document in the group stops
		// the insert rather than being written somewhere else.
		std::string document_path;
		if (IsFirestoreCollectionGroup(bind_data.collection)) {
			auto error = ResolveFirestoreCollectionGroupDocumentPath(bind_data.collection, doc_id_value, document_path);
			if (!error.empty()) {
				throw InvalidInputException("firestore_insert: " + error);
			}
		} else {
			document_path = ResolveDocumentPath(bind_data.collection, doc_id_value).document_path;
		}

		if (global_state.use_individual_ops) {
			// Fallback mode: individual CreateDocument calls with explicit ID
			try {
				CreateDocumentAt(*global_state.client, document_path, fields);
				global_state.rows_inserted++;
			} catch (const std::exception &e) {
				throw InvalidInputException("Firestore insert failed for document '" + doc_id_value +
				                            "': " + std::string(e.what()));
			}
		} else {
			// Batch mode: accumulate write operations
			std::string doc_path = "projects/" + bind_data.credentials->project_id + "/databases/" +
			                       bind_data.credentials->database_id + "/documents/" + document_path;

			json write_op = {{"update", {{"name", doc_path}, {"fields", fields}}}};
			global_state.batch_writes.push_back(write_op);
			global_state.batch_document_paths.push_back(document_path);

			if (global_state.batch_writes.size() >= BATCH_SIZE) {
				FlushInsertBatchWrites(global_state);
			}
		}
	}

	FirestoreSetCardinality(output, 0);
	return OperatorResultType::NEED_MORE_INPUT;
}

// Finalize: flush remaining writes and emit the count
static OperatorFinalizeResultType FirestoreInsertFinal(ExecutionContext &context, TableFunctionInput &data,
                                                       DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreInsertBindData>();
	auto &global_state = data.global_state->Cast<FirestoreInsertGlobalState>();

	if (global_state.count_emitted) {
		FirestoreSetCardinality(output, 0);
		return OperatorFinalizeResultType::FINISHED;
	}

	// Flush any remaining batch writes
	if (!bind_data.use_auto_ids && !global_state.batch_writes.empty()) {
		FlushInsertBatchWrites(global_state);
	}

	// Emit the final count
	FirestoreMutableData<int64_t>(output.data[0])[0] = static_cast<int64_t>(global_state.rows_inserted);
	FirestoreSetCardinality(output, 1);
	global_state.count_emitted = true;
	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

// ============================================================================
// UPDATE function implementation
// Usage: CALL firestore_update('collection', 'doc_id', 'field1', value1, ...)
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
                                                    vector<LogicalType> &return_types, vector<FirestoreName> &names) {
	auto result = make_uniq<FirestoreUpdateBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();
	CheckCollectionGroupDocumentPath("firestore_update", result->collection, result->document_id);

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
		FirestoreSetCardinality(output, 0);
		return;
	}

	int64_t count = 0;

	try {
		// Create Firestore client
		FirestoreClient client(bind_data.credentials, context);

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

	FirestoreMutableData<int64_t>(output.data[0])[0] = count;
	FirestoreSetCardinality(output, 1);
	global_state.done = true;
}

// ============================================================================
// DELETE function implementation
// Usage: CALL firestore_delete('collection', 'doc_id')
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
                                                    vector<LogicalType> &return_types, vector<FirestoreName> &names) {
	auto result = make_uniq<FirestoreDeleteBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();
	CheckCollectionGroupDocumentPath("firestore_delete", result->collection, result->document_id);

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
		FirestoreSetCardinality(output, 0);
		return;
	}

	int64_t count = 0;

	try {
		// Create Firestore client
		FirestoreClient client(bind_data.credentials, context);

		// Perform delete
		client.DeleteDocument(bind_data.collection, bind_data.document_id);
		count = 1;
	} catch (const FirestoreNotFoundException &e) {
		// Document not found - return 0
		count = 0;
	} catch (const std::exception &e) {
		throw InvalidInputException("Firestore delete failed: " + std::string(e.what()));
	}

	FirestoreMutableData<int64_t>(output.data[0])[0] = count;
	FirestoreSetCardinality(output, 1);
	global_state.done = true;
}

// ============================================================================
// BATCH UPDATE function implementation
// Usage: CALL firestore_update_batch('collection', ['id1','id2',...], 'field1', value1, ...)
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
                                                         vector<LogicalType> &return_types,
                                                         vector<FirestoreName> &names) {
	auto result = make_uniq<FirestoreUpdateBatchBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document IDs (second arg) - must be a LIST of VARCHAR
	auto &ids_value = input.inputs[1];
	if (ids_value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("firestore_update_batch requires a LIST of document IDs as second argument. "
		                      "Usage: firestore_update_batch('collection', ['id1','id2'], 'field1', value1, ...)");
	}

	CheckCollectionGroupName("firestore_update_batch", result->collection);

	// Handle NULL list (e.g., from empty filter result)
	if (!ids_value.IsNull()) {
		auto &id_list = ListValue::GetChildren(ids_value);
		for (auto &id_val : id_list) {
			result->document_ids.push_back(id_val.GetValue<string>());
			// Every path is checked before any batch is sent.
			CheckCollectionGroupDocumentPath("firestore_update_batch", result->collection, result->document_ids.back());
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
		FirestoreSetCardinality(output, 0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials, context);

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

	FirestoreMutableData<int64_t>(output.data[0])[0] = count;
	FirestoreSetCardinality(output, 1);
	global_state.done = true;
}

// ============================================================================
// BATCH DELETE function implementation
// Usage: CALL firestore_delete_batch('collection', ['id1','id2',...])
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
                                                         vector<LogicalType> &return_types,
                                                         vector<FirestoreName> &names) {
	auto result = make_uniq<FirestoreDeleteBatchBindData>();

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document IDs (second arg) - must be a LIST of VARCHAR
	auto &ids_value = input.inputs[1];
	if (ids_value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("firestore_delete_batch requires a LIST of document IDs as second argument. "
		                      "Usage: firestore_delete_batch('collection', ['id1','id2'])");
	}

	CheckCollectionGroupName("firestore_delete_batch", result->collection);

	// Handle NULL list (e.g., from empty filter result)
	if (!ids_value.IsNull()) {
		auto &id_list = ListValue::GetChildren(ids_value);
		for (auto &id_val : id_list) {
			result->document_ids.push_back(id_val.GetValue<string>());
			// Every path is checked before any batch is sent.
			CheckCollectionGroupDocumentPath("firestore_delete_batch", result->collection, result->document_ids.back());
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
		FirestoreSetCardinality(output, 0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials, context);

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

	FirestoreMutableData<int64_t>(output.data[0])[0] = count;
	FirestoreSetCardinality(output, 1);
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

static const char *ArrayTransformFunctionName(FirestoreClient::ArrayTransformType transform_type) {
	switch (transform_type) {
	case FirestoreClient::ArrayTransformType::ARRAY_UNION:
		return "firestore_array_union";
	case FirestoreClient::ArrayTransformType::ARRAY_REMOVE:
		return "firestore_array_remove";
	case FirestoreClient::ArrayTransformType::ARRAY_APPEND:
		return "firestore_array_append";
	}
	return "firestore_array_transform";
}

static unique_ptr<FunctionData> FirestoreArrayTransformBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<FirestoreName> &names,
                                                            FirestoreClient::ArrayTransformType transform_type) {
	auto result = make_uniq<FirestoreArrayTransformBindData>();
	result->transform_type = transform_type;

	// Get collection name (first arg)
	result->collection = input.inputs[0].GetValue<string>();

	// Get document ID (second arg)
	result->document_id = input.inputs[1].GetValue<string>();
	CheckCollectionGroupDocumentPath(ArrayTransformFunctionName(transform_type), result->collection,
	                                 result->document_id);

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
                                                        vector<LogicalType> &return_types,
                                                        vector<FirestoreName> &names) {
	return FirestoreArrayTransformBind(context, input, return_types, names,
	                                   FirestoreClient::ArrayTransformType::ARRAY_UNION);
}

static unique_ptr<FunctionData> FirestoreArrayRemoveBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types,
                                                         vector<FirestoreName> &names) {
	return FirestoreArrayTransformBind(context, input, return_types, names,
	                                   FirestoreClient::ArrayTransformType::ARRAY_REMOVE);
}

static unique_ptr<FunctionData> FirestoreArrayAppendBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types,
                                                         vector<FirestoreName> &names) {
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
		FirestoreSetCardinality(output, 0);
		return;
	}

	int64_t count = 0;

	try {
		FirestoreClient client(bind_data.credentials, context);

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

	FirestoreMutableData<int64_t>(output.data[0])[0] = count;
	FirestoreSetCardinality(output, 1);
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
	// Usage: CALL firestore_insert('collection', (SELECT col1, col2 FROM ...))
	//        CALL firestore_insert('collection', (SELECT ...), document_id := 'id_col')
	TableFunction insert_func("firestore_insert", {LogicalType::VARCHAR, LogicalType::TABLE}, nullptr,
	                          FirestoreInsertBind, FirestoreInsertInitGlobal, FirestoreInsertInitLocal);

	insert_func.in_out_function = FirestoreInsertInOutFunction;
	insert_func.in_out_function_final = FirestoreInsertFinal;

	FirestoreAddNamedParameters(insert_func, {
	                                             {"project_id", LogicalType::VARCHAR},
	                                             {"credentials", LogicalType::VARCHAR},
	                                             {"api_key", LogicalType::VARCHAR},
	                                             {"database", LogicalType::VARCHAR},
	                                             {"document_id", LogicalType::VARCHAR},
	                                         });

	RegisterDocumentedTableFunction(
	    loader, insert_func,
	    {{"collection", "subquery"},
	     "Insert one Firestore document per row of a subquery, with one field per column. document_id := names the "
	     "column that supplies document IDs, which is then left out of the fields; without it Firestore generates "
	     "the IDs. A collection group ('~orders') needs document_id := naming a column of full document paths. "
	     "Returns the number of documents inserted.",
	     "CALL firestore_insert('users', (SELECT 'alice' AS id, 'Alice' AS name, 30 AS age), document_id := 'id');",
	     {"firestore", "write"}});

	// Register firestore_update table function
	// Usage: CALL firestore_update('collection', 'doc_id', 'field1', value1, ...)
	TableFunction update_func("firestore_update",
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR}, // collection, document_id
	                          FirestoreUpdateFunction, FirestoreUpdateBind, FirestoreUpdateInitGlobal);

	FirestoreSetVarArgs(update_func, LogicalType::ANY); // Accept field/value pairs
	FirestoreAddNamedParameters(update_func, {
	                                             {"project_id", LogicalType::VARCHAR},
	                                             {"credentials", LogicalType::VARCHAR},
	                                             {"api_key", LogicalType::VARCHAR},
	                                             {"database", LogicalType::VARCHAR},
	                                         });

	RegisterDocumentedTableFunction(
	    loader, update_func,
	    {{"collection", "document_id"},
	     "Set fields on one Firestore document from the trailing field-name/value pairs, leaving its other fields "
	     "as they are. For a collection group ('~orders'), document_id is the document's full path, as __document_id "
	     "from firestore_scan('~orders') gives it. Returns a one-row count.",
	     "CALL firestore_update('users', 'user123', 'status', 'verified', 'age', 31);",
	     {"firestore", "write"}});

	// Register firestore_delete table function
	// Usage: CALL firestore_delete('collection', 'doc_id')
	TableFunction delete_func("firestore_delete",
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR}, // collection, document_id
	                          FirestoreDeleteFunction, FirestoreDeleteBind, FirestoreDeleteInitGlobal);

	FirestoreAddNamedParameters(delete_func, {
	                                             {"project_id", LogicalType::VARCHAR},
	                                             {"credentials", LogicalType::VARCHAR},
	                                             {"api_key", LogicalType::VARCHAR},
	                                             {"database", LogicalType::VARCHAR},
	                                         });

	RegisterDocumentedTableFunction(
	    loader, delete_func,
	    {{"collection", "document_id"},
	     "Delete one Firestore document by ID. For a collection group ('~orders'), the ID is the document's full "
	     "path, as __document_id from firestore_scan('~orders') gives it. Returns a one-row count.",
	     "CALL firestore_delete('users', 'user123');",
	     {"firestore", "write"}});

	// Register firestore_update_batch table function
	// Usage: CALL firestore_update_batch('collection', ['id1','id2'], 'field1', value1, ...)
	TableFunction update_batch_func(
	    "firestore_update_batch", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    FirestoreUpdateBatchFunction, FirestoreUpdateBatchBind, FirestoreUpdateBatchInitGlobal);

	FirestoreSetVarArgs(update_batch_func, LogicalType::ANY); // Accept field/value pairs
	FirestoreAddNamedParameters(update_batch_func, {
	                                                   {"project_id", LogicalType::VARCHAR},
	                                                   {"credentials", LogicalType::VARCHAR},
	                                                   {"api_key", LogicalType::VARCHAR},
	                                                   {"database", LogicalType::VARCHAR},
	                                               });

	RegisterDocumentedTableFunction(
	    loader, update_batch_func,
	    {{"collection", "document_ids"},
	     "Set the same field-name/value pairs on every Firestore document in a list of IDs, sent through "
	     "batchWrite in groups of up to 500. Batches are not atomic: each write succeeds or fails on its own. For a "
	     "collection group ('~orders'), each ID is a full document path, checked before anything is sent. Returns a "
	     "one-row count.",
	     "CALL firestore_update_batch('users', ['user1', 'user2'], 'status', 'reviewed');",
	     {"firestore", "write"}});

	// Register firestore_delete_batch table function
	// Usage: CALL firestore_delete_batch('collection', ['id1','id2'])
	TableFunction delete_batch_func(
	    "firestore_delete_batch", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    FirestoreDeleteBatchFunction, FirestoreDeleteBatchBind, FirestoreDeleteBatchInitGlobal);

	FirestoreAddNamedParameters(delete_batch_func, {
	                                                   {"project_id", LogicalType::VARCHAR},
	                                                   {"credentials", LogicalType::VARCHAR},
	                                                   {"api_key", LogicalType::VARCHAR},
	                                                   {"database", LogicalType::VARCHAR},
	                                               });

	RegisterDocumentedTableFunction(
	    loader, delete_batch_func,
	    {{"collection", "document_ids"},
	     "Delete every Firestore document in a list of IDs, sent through batchWrite in groups of up to 500. "
	     "Batches are not atomic: each delete succeeds or fails on its own. For a collection group ('~orders'), each "
	     "ID is a full document path, checked before anything is sent. Returns a one-row count.",
	     "CALL firestore_delete_batch('users', ['user1', 'user2']);",
	     {"firestore", "write"}});

	// Register firestore_array_union table function
	// Usage: SELECT * FROM firestore_array_union('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Adds elements to array without duplicates
	TableFunction array_union_func(
	    "firestore_array_union",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayUnionBind, FirestoreArrayTransformInitGlobal);

	FirestoreAddNamedParameters(array_union_func, {
	                                                  {"project_id", LogicalType::VARCHAR},
	                                                  {"credentials", LogicalType::VARCHAR},
	                                                  {"api_key", LogicalType::VARCHAR},
	                                                  {"database", LogicalType::VARCHAR},
	                                              });

	RegisterDocumentedTableFunction(
	    loader, array_union_func,
	    {{"collection", "document_id", "field", "values"},
	     "Add values to an array field of one Firestore document, skipping any the array already holds "
	     "(Firestore's arrayUnion transform). For a collection group ('~orders'), document_id is the document's "
	     "full path. Returns a one-row count.",
	     "CALL firestore_array_union('users', 'user123', 'tags', ['vip', 'active']);",
	     {"firestore", "write"}});

	// Register firestore_array_remove table function
	// Usage: SELECT * FROM firestore_array_remove('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Removes specified elements from array
	TableFunction array_remove_func(
	    "firestore_array_remove",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayRemoveBind, FirestoreArrayTransformInitGlobal);

	FirestoreAddNamedParameters(array_remove_func, {
	                                                   {"project_id", LogicalType::VARCHAR},
	                                                   {"credentials", LogicalType::VARCHAR},
	                                                   {"api_key", LogicalType::VARCHAR},
	                                                   {"database", LogicalType::VARCHAR},
	                                               });

	RegisterDocumentedTableFunction(
	    loader, array_remove_func,
	    {{"collection", "document_id", "field", "values"},
	     "Remove every occurrence of the given values from an array field of one Firestore document (Firestore's "
	     "arrayRemove transform). For a collection group ('~orders'), document_id is the document's full path. "
	     "Returns a one-row count.",
	     "CALL firestore_array_remove('users', 'user123', 'tags', ['inactive']);",
	     {"firestore", "write"}});

	// Register firestore_array_append table function
	// Usage: SELECT * FROM firestore_array_append('collection', 'doc_id', 'field', ['val1', 'val2'])
	// Appends elements to array (may create duplicates)
	TableFunction array_append_func(
	    "firestore_array_append",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    FirestoreArrayTransformFunction, FirestoreArrayAppendBind, FirestoreArrayTransformInitGlobal);

	FirestoreAddNamedParameters(array_append_func, {
	                                                   {"project_id", LogicalType::VARCHAR},
	                                                   {"credentials", LogicalType::VARCHAR},
	                                                   {"api_key", LogicalType::VARCHAR},
	                                                   {"database", LogicalType::VARCHAR},
	                                               });

	RegisterDocumentedTableFunction(
	    loader, array_append_func,
	    {{"collection", "document_id", "field", "values"},
	     "Append values to an array field of one Firestore document, keeping duplicates. The array is read and "
	     "written back whole, so a concurrent write to the same field can be lost. For a collection group "
	     "('~orders'), document_id is the document's full path. Returns a one-row count.",
	     "CALL firestore_array_append('users', 'user123', 'events', ['login']);",
	     {"firestore", "write"}});
}

void RegisterFirestoreCopyFunction(ExtensionLoader &loader) {
	// COPY function registration would go here
	// This enables: COPY collection FROM 'file.csv' (FORMAT firestore)
	// Deferred to future implementation
}

} // namespace duckdb
