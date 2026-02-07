#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

struct FirestoreSettings {
	// Schema cache TTL in seconds (default: 3600 = 60 minutes, 0 = disabled)
	static constexpr int64_t kDefaultSchemaCacheTTLSeconds = 3600;

	static int64_t SchemaCacheTTLSeconds(const ClientContext &context) {
		auto &client_config = ClientConfig::GetConfig(context);
		auto client_it = client_config.set_variables.find("firestore_schema_cache_ttl");
		if (client_it != client_config.set_variables.end()) {
			return NormalizeTTL(client_it->second);
		}
		auto &db_config = DBConfig::GetConfig(context);
		auto db_it = db_config.options.set_variables.find("firestore_schema_cache_ttl");
		if (db_it != db_config.options.set_variables.end()) {
			return NormalizeTTL(db_it->second);
		}
		return kDefaultSchemaCacheTTLSeconds;
	}

	static void SetSchemaCacheTTLSeconds(ClientContext &context, SetScope scope, Value &parameter) {
		auto ttl = BigIntValue::Get(parameter);
		if (ttl < 0) {
			ttl = 0; // 0 means disabled
		}
		parameter = Value::BIGINT(ttl);
	}

private:
	static int64_t NormalizeTTL(const Value &value) {
		auto ttl = BigIntValue::Get(value);
		return ttl < 0 ? 0 : ttl;
	}
};

} // namespace duckdb
