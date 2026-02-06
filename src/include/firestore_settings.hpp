#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct FirestoreSettings {
	// Schema cache TTL in seconds (default: 3600 = 60 minutes, 0 = disabled)
	static int64_t &SchemaCacheTTLSeconds() {
		static int64_t schema_cache_ttl_seconds = 3600;
		return schema_cache_ttl_seconds;
	}

	static void SetSchemaCacheTTLSeconds(ClientContext &context, SetScope scope, Value &parameter) {
		auto ttl = BigIntValue::Get(parameter);
		if (ttl < 0) {
			ttl = 0; // 0 means disabled
		}
		SchemaCacheTTLSeconds() = ttl;
	}
};

} // namespace duckdb
