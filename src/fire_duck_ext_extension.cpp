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

// Table function to clear the schema cache
static void FirestoreClearCacheFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ClearFirestoreSchemaCache();
	output.SetCardinality(0);
}

static unique_ptr<FunctionData> FirestoreClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	return nullptr;
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
	TableFunction clear_cache_func("firestore_clear_cache", {}, FirestoreClearCacheFunction, FirestoreClearCacheBind);
	loader.RegisterFunction(clear_cache_func);
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
