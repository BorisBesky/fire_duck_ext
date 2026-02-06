#define DUCKDB_EXTENSION_MAIN

#include "fire_duck_ext_extension.hpp"
#include "firestore_scanner.hpp"
#include "firestore_writer.hpp"
#include "firestore_secrets.hpp"
#include "firestore_logger.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include <cstdlib>

namespace duckdb {

static void InitializeLogging() {
	const char *log_level = std::getenv("FIRESTORE_LOG_LEVEL");
	if (log_level) {
		FirestoreLogger::Instance().SetLogLevel(ParseLogLevel(log_level));
	}
}

// Bind data for cache clear function
struct FirestoreClearCacheBindData : public TableFunctionData {
	std::string collection;
};

// Table function to clear the schema cache (all entries)
static void FirestoreClearCacheFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<FirestoreClearCacheBindData>();
	ClearFirestoreSchemaCache(bind_data.collection);
	output.SetCardinality(0);
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
static unique_ptr<FunctionData> FirestoreClearCacheBindCollection(ClientContext &context,
                                                                   TableFunctionBindInput &input,
                                                                   vector<LogicalType> &return_types,
                                                                   vector<string> &names) {
	// Table functions must return at least one column
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);

	auto result = make_uniq<FirestoreClearCacheBindData>();
	result->collection = input.inputs[0].GetValue<string>();
	return std::move(result);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Initialize logging from environment variable
	InitializeLogging();

	// Register the firestore secret type for credential management
	RegisterFirestoreSecretType(loader);

	// Register table scan function: firestore_scan('collection')
	RegisterFirestoreScanFunction(loader);

	// Register write functions
	RegisterFirestoreWriteFunctions(loader);

	// Register cache clear function: SELECT * FROM firestore_clear_cache()
	// Overload 1: No arguments - clears entire cache
	TableFunction clear_cache_all("firestore_clear_cache", {}, FirestoreClearCacheFunction, FirestoreClearCacheBindAll);
	loader.RegisterFunction(clear_cache_all);

	// Overload 2: With collection argument - clears only that collection
	TableFunction clear_cache_collection("firestore_clear_cache", {LogicalType::VARCHAR}, FirestoreClearCacheFunction,
	                                     FirestoreClearCacheBindCollection);
	loader.RegisterFunction(clear_cache_collection);
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
