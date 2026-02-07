#define DUCKDB_EXTENSION_MAIN

#include "fire_duck_ext_extension.hpp"
#include "firestore_scanner.hpp"
#include "firestore_writer.hpp"
#include "firestore_secrets.hpp"
#include "firestore_settings.hpp"
#include "firestore_logger.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table_function.hpp"
#include <cstdlib>

namespace duckdb {

static void InitializeLogging() {
	const char *log_level = std::getenv("FIRESTORE_LOG_LEVEL");
	if (log_level) {
		FirestoreLogger::Instance().SetLogLevel(ParseLogLevel(log_level));
	}
}

// Global state for one-shot functions (connect, disconnect, clear_cache)
// Ensures the function runs only once per call
struct FirestoreOneShotState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<GlobalTableFunctionState> FirestoreOneShotInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<FirestoreOneShotState>();
}

// Bind data for cache clear function
struct FirestoreClearCacheBindData : public TableFunctionData {
	std::string collection;
};

// Table function to clear the schema cache (all entries)
static void FirestoreClearCacheFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<FirestoreOneShotState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data.bind_data->Cast<FirestoreClearCacheBindData>();
	ClearFirestoreSchemaCache(bind_data.collection);
	FlatVector::GetData<bool>(output.data[0])[0] = true;
	output.SetCardinality(1);
	state.finished = true;
}

// Bind for clear cache with no arguments (clears all)
static unique_ptr<FunctionData> FirestoreClearCacheBindAll(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	// Table functions must return at least one column
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);

	auto result = make_uniq<FirestoreClearCacheBindData>();
	result->collection = ""; // Empty means clear all
	return std::move(result);
}

// Bind for clear cache with collection argument
static unique_ptr<FunctionData> FirestoreClearCacheBindCollection(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	// Table functions must return at least one column
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);

	auto result = make_uniq<FirestoreClearCacheBindData>();
	result->collection = input.inputs[0].GetValue<string>();
	return std::move(result);
}

// ============================================================================
// firestore_connect / firestore_disconnect functions
// ============================================================================

// Bind data for connect function
struct FirestoreConnectBindData : public TableFunctionData {
	std::string database_id;
};

// Bind for firestore_connect(database_id)
static unique_ptr<FunctionData> FirestoreConnectBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);

	auto result = make_uniq<FirestoreConnectBindData>();
	result->database_id = input.inputs[0].GetValue<string>();
	return std::move(result);
}

// Execute firestore_connect - validates credentials and sets session state
static void FirestoreConnectFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<FirestoreOneShotState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data.bind_data->Cast<FirestoreConnectBindData>();

	// Validate that credentials exist for this database
	auto creds = ResolveFirestoreCredentials(context, std::nullopt, std::nullopt, std::nullopt, bind_data.database_id);
	if (!creds) {
		throw InvalidInputException("No Firestore credentials found for database '%s'. "
		                            "Create a secret with DATABASE='%s' or DATABASE='*', "
		                            "or set GOOGLE_APPLICATION_CREDENTIALS environment variable.",
		                            bind_data.database_id.c_str(), bind_data.database_id.c_str());
	}

	// Store in session state
	SetConnectedDatabase(context, bind_data.database_id);

	FlatVector::GetData<bool>(output.data[0])[0] = true;
	output.SetCardinality(1);
	state.finished = true;
}

// Bind for firestore_disconnect()
static unique_ptr<FunctionData> FirestoreDisconnectBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);
	return make_uniq<TableFunctionData>();
}

// Execute firestore_disconnect - clears session state
static void FirestoreDisconnectFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<FirestoreOneShotState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	ClearConnectedDatabase(context);
	FlatVector::GetData<bool>(output.data[0])[0] = true;
	output.SetCardinality(1);
	state.finished = true;
}

static void LoadInternal(ExtensionLoader &loader) {
	// Initialize logging from environment variable
	InitializeLogging();

	// Register extension options
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("firestore_schema_cache_ttl", "Schema cache TTL in seconds (0 to disable caching)",
	                          LogicalType::BIGINT, Value::BIGINT(FirestoreSettings::SchemaCacheTTLSeconds()),
	                          FirestoreSettings::SetSchemaCacheTTLSeconds);

	// Register the firestore secret type for credential management
	RegisterFirestoreSecretType(loader);

	// Register table scan function: firestore_scan('collection')
	RegisterFirestoreScanFunction(loader);

	// Register write functions
	RegisterFirestoreWriteFunctions(loader);

	// Register cache clear function: SELECT * FROM firestore_clear_cache()
	// Overload 1: No arguments - clears entire cache
	TableFunction clear_cache_all("firestore_clear_cache", {}, FirestoreClearCacheFunction, FirestoreClearCacheBindAll,
	                              FirestoreOneShotInit);
	loader.RegisterFunction(clear_cache_all);

	// Overload 2: With collection argument - clears only that collection
	TableFunction clear_cache_collection("firestore_clear_cache", {LogicalType::VARCHAR}, FirestoreClearCacheFunction,
	                                     FirestoreClearCacheBindCollection, FirestoreOneShotInit);
	loader.RegisterFunction(clear_cache_collection);

	// Register firestore_connect(database_id) - sets session-scoped database
	TableFunction connect_func("firestore_connect", {LogicalType::VARCHAR}, FirestoreConnectFunction,
	                           FirestoreConnectBind, FirestoreOneShotInit);
	loader.RegisterFunction(connect_func);

	// Register firestore_disconnect() - clears session-scoped database
	TableFunction disconnect_func("firestore_disconnect", {}, FirestoreDisconnectFunction, FirestoreDisconnectBind,
	                              FirestoreOneShotInit);
	loader.RegisterFunction(disconnect_func);
}

void FireDuckExtExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string FireDuckExtExtension::Name() {
	return "fire_duck_ext";
}

std::string FireDuckExtExtension::Version() const {
#ifdef EXT_VERSION_FIRE_DUCK_EXT
	return EXT_VERSION_FIRE_DUCK_EXT;
#else
	return "v0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(fire_duck_ext, loader) {
	duckdb::LoadInternal(loader);
}
}
