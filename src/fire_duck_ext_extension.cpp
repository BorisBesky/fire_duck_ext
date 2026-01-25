#define DUCKDB_EXTENSION_MAIN

#include "fire_duck_ext_extension.hpp"
#include "firestore_scanner.hpp"
#include "firestore_writer.hpp"
#include "firestore_secrets.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    // Register the firestore secret type for credential management
    RegisterFirestoreSecretType(loader);

    // Register table scan function: firestore_scan('collection')
    RegisterFirestoreScanFunction(loader);

    // Register write functions
    RegisterFirestoreWriteFunctions(loader);
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
